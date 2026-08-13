package core

import "sort"

// raftCore is the concrete pure Raft state machine implementing RaftCore. It
// holds only in-memory state. Every method is a pure function of that state and
// its arguments — no time, no I/O, no goroutines, no randomness. The driver
// feeds Events and executes the returned Effects.
type raftCore struct {
	// Configuration. voters is the source of truth for the current voting set
	// (includes self); peers/quorum/clusterN are DERIVED from it by
	// recomputeMembership. Static for a node's lifetime unless single-server
	// membership changes (P6, core/config_change.go) mutate voters, at which point
	// the derived fields are recomputed. Every quorum-check site reads the derived
	// fields, so membership changes need no edits there.
	self     NodeID
	voters   map[NodeID]bool // current voting configuration, includes self
	peers    []NodeID        // sorted, excludes self; derived from voters
	quorum   int             // majority of the voting set; derived
	clusterN int             // size of the voting set; derived
	// configBaseline is the voting configuration implied by everything at or below
	// the in-memory log's snapshot base (initially self+Config.Peers). The live
	// config = configBaseline replayed with every KindConfig entry still in the
	// log; adoptConfigFromLog recomputes it, so truncating an uncommitted config
	// entry cleanly reverts membership.
	configBaseline map[NodeID]bool
	// lastConfigIndex is the log index of the most recent KindConfig entry the
	// core has adopted (0 if none); the one-at-a-time safety gate requires it to
	// be committed before another config change is accepted.
	lastConfigIndex Index

	// Persistent state (must be durable before observable actions; the core
	// emits EffectPersistHardState / EffectPersistLog and trusts the driver).
	currentTerm Term
	votedFor    NodeID
	log         *raftLog

	// Volatile state on all servers.
	role        Role
	commitIndex Index
	appliedTo   Index  // highest index handed to the application via EffectApply
	leaderHint  NodeID // best-known current leader (None if unknown)

	// Volatile leader state, reset on election.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index
	// snapPending[p] is true while an InstallSnapshot to peer p is outstanding, so
	// the leader does not re-send the (potentially large) snapshot on every
	// heartbeat while the first is still in flight. Cleared on the peer's
	// InstallSnapshotResp or when the peer's nextIndex rises back into the log.
	snapPending map[NodeID]bool

	// Candidate state, reset on starting an election.
	votesGranted map[NodeID]bool

	// ReadIndex state (leader only, reset on election). pendingReads is a SLICE
	// (never a map) drained in insertion order so the effect stream — and thus the
	// trace hash — is deterministic. readSeq is the monotonically increasing
	// confirmation-round counter stamped on heartbeats; confirmAcks counts, per
	// round, how many peers have echoed a ReadSeq >= that round since it began.
	pendingReads []pendingRead
	readSeq      uint64
	confirmAcks  map[NodeID]uint64 // peer -> highest ReadSeq it has echoed
	noopIndex    Index             // index of this leader's current-term no-op
}

// pendingRead is a linearizable read awaiting (a) heartbeat-confirmed leadership
// as of the round it was enqueued, and (b) the state machine having applied
// through its read index. Ref identifies the client request; readIndex is the
// commit index captured when the read was accepted; round is the readSeq at
// enqueue time; confirmed records whether the leadership round has been acked by
// a quorum yet.
type pendingRead struct {
	ref       ClientRef
	readIndex Index
	round     uint64
	confirmed bool
}

// newCore constructs a fresh core from Config. Peers are sorted and de-duped;
// self is excluded from peers. The node starts as a follower in term 0 with an
// empty log. The driver should arm the election timer immediately (New emits
// no effects; the first EffectResetElectionTimer arrives via the first Step, so
// drivers arm an initial timer themselves — see sim/runtime).
func newCore(cfg Config) *raftCore {
	voters := map[NodeID]bool{cfg.Self: true}
	for _, p := range cfg.Peers {
		voters[p] = true
	}
	peers := make([]NodeID, 0, len(cfg.Peers))
	seen := map[NodeID]bool{cfg.Self: true}
	for _, p := range cfg.Peers {
		if !seen[p] {
			seen[p] = true
			peers = append(peers, p)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	n := len(peers) + 1
	baseline := make(map[NodeID]bool, len(voters))
	for id := range voters {
		baseline[id] = true
	}
	return &raftCore{
		self:           cfg.Self,
		voters:         voters,
		configBaseline: baseline,
		peers:          peers,
		quorum:         n/2 + 1,
		clusterN:       n,
		currentTerm:    0,
		votedFor:       None,
		log:            newRaftLog(),
		role:           Follower,
		nextIndex:      make(map[NodeID]Index, len(peers)),
		matchIndex:     make(map[NodeID]Index, len(peers)),
		snapPending:    make(map[NodeID]bool, len(peers)),
		votesGranted:   make(map[NodeID]bool, len(peers)+1),
		confirmAcks:    make(map[NodeID]uint64, len(peers)),
	}
}

// New returns a fresh RaftCore for the given configuration.
func New(cfg Config) RaftCore { return newCore(cfg) }

// Restore re-installs durable state after a crash, before any events are
// stepped. The node remains a follower (roles are volatile and always start as
// follower on restart — a recovered node must re-earn leadership). It rebases
// the log on the recovered snapshot (snapIndex/snapTerm) and seeds
// commitIndex = appliedTo = snapIndex: those entries are already reflected in
// the recovered application state machine, so they must be neither re-applied
// nor counted as uncommitted. The driver re-applies only the tail (entries
// above snapIndex) as commit re-advances.
func (c *raftCore) Restore(hs HardState, snapIndex Index, snapTerm Term, entries []LogEntry) {
	c.currentTerm = hs.Term
	c.votedFor = hs.VotedFor
	c.log.entries = append(c.log.entries[:0], entries...)
	c.log.snapBase = snapIndex
	c.log.snapTerm = snapTerm
	c.commitIndex = snapIndex
	c.appliedTo = snapIndex
	c.role = Follower
	c.leaderHint = None
	// Re-derive the voting configuration from the recovered log: the baseline is
	// the initial config (seeded at newCore), replayed with every KindConfig entry
	// still present. adoptConfigFromLog updates voters/quorum from the log — the
	// single source of truth for membership across a restart.
	c.adoptConfigFromLog(&effects{}) // discard effects: restore is pre-event, driver re-derives
}

// CompactTo advances the in-memory log base after the driver durably saved a
// snapshot through index (of term). Never compacts uncommitted entries.
//
// Membership: any KindConfig entry at or below `index` is about to be dropped
// from the in-memory log, so its effect must first be folded into
// configBaseline — otherwise a later adoptConfigFromLog (a subsequent config
// change, or restore) would re-derive the voter set WITHOUT the compacted change
// and silently revert committed membership.
func (c *raftCore) CompactTo(index Index, term Term) {
	if index > c.commitIndex {
		index = c.commitIndex
	}
	c.foldConfigIntoBaseline(index)
	c.log.compactTo(index, term)
}

// foldConfigIntoBaseline replays every KindConfig entry at index <= throughIndex
// into configBaseline, so the baseline reflects membership implied by the
// (about-to-be-compacted) prefix. Called before dropping that prefix.
func (c *raftCore) foldConfigIntoBaseline(throughIndex Index) {
	for _, e := range c.log.entries {
		if e.Index > throughIndex {
			break
		}
		if e.Kind != KindConfig {
			continue
		}
		if cc, ok := DecodeConfigChange(e.Command); ok {
			if cc.Add {
				c.configBaseline[cc.Server] = true
			} else {
				delete(c.configBaseline, cc.Server)
			}
		}
	}
}

// --- read-only accessors (invariant monitors, checker, tests) ---

func (c *raftCore) Role() Role          { return c.role }
func (c *raftCore) Term() Term          { return c.currentTerm }
func (c *raftCore) CommitIndex() Index  { return c.commitIndex }
func (c *raftCore) LastLogIndex() Index { return c.log.lastIndex() }
func (c *raftCore) Leader() NodeID      { return c.leaderHint }
func (c *raftCore) ID() NodeID          { return c.self }
func (c *raftCore) SnapBase() Index     { return c.log.snapBase }
func (c *raftCore) SnapTerm() Term      { return c.log.snapTerm }
func (c *raftCore) LogView() []LogEntry { return c.log.clone() }

// Members returns the current voting configuration (sorted, includes self).
func (c *raftCore) Members() []NodeID {
	out := make([]NodeID, 0, len(c.voters))
	for id := range c.voters {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Step processes exactly one event and returns effects in execution order.
// Always returns a non-nil (possibly empty) slice.
func (c *raftCore) Step(ev Event) []Effect {
	eb := &effects{}
	switch ev.Type {
	case EventTickElection:
		c.handleElectionTimeout(eb)
	case EventTickHeartbeat:
		c.handleHeartbeatTimeout(eb)
	case EventDeliver:
		c.handleMessage(ev.Msg, eb)
	case EventPropose:
		c.handlePropose(ev.Ref, ev.Command, eb)
	case EventReadIndex:
		c.handleReadIndex(ev.Ref, eb)
	case EventChangeConfig:
		c.handleChangeConfig(ev.Ref, ev.ConfigAdd, ev.ConfigServer, eb)
	}
	return eb.done()
}

// --- role transitions ---

// becomeFollower steps down to follower in term t, optionally recording the
// known leader. Persists hard state if term or vote changed. Arms the election
// timer (a follower must time out if it stops hearing from a leader).
func (c *raftCore) becomeFollower(t Term, leader NodeID, eb *effects) {
	wasLeader := c.role == Leader
	termChanged := t != c.currentTerm
	if termChanged {
		c.currentTerm = t
		c.votedFor = None
	}
	c.role = Follower
	c.leaderHint = leader
	// A stepping-down leader can no longer confirm its outstanding reads; reject
	// them so the client retries against the new leader (no stale reads served).
	if wasLeader && len(c.pendingReads) > 0 {
		c.dropPendingReads(eb)
	}
	if termChanged {
		eb.persistHardState(c.hardState())
	}
	eb.resetElectionTimer()
}

// becomeCandidate starts a new election: increments term, votes for self,
// persists, and requests votes from all peers. A single-node cluster wins
// immediately.
func (c *raftCore) becomeCandidate(eb *effects) {
	c.currentTerm++
	c.role = Candidate
	c.votedFor = c.self
	c.leaderHint = None
	c.votesGranted = map[NodeID]bool{c.self: true}
	eb.persistHardState(c.hardState())
	eb.resetElectionTimer()

	if c.clusterN == 1 {
		c.becomeLeader(eb)
		return
	}
	li := c.log.lastIndex()
	lt := c.log.lastTerm()
	for _, p := range c.peers {
		eb.send(Message{
			From: c.self, To: p, Type: MsgRequestVote, Term: c.currentTerm,
			LastLogIndex: li, LastLogTerm: lt,
		})
	}
}

// becomeLeader assumes leadership: initializes nextIndex/matchIndex, appends a
// no-op entry in the current term (so the current-term commit can carry prior
// entries), and broadcasts initial heartbeats/appends.
func (c *raftCore) becomeLeader(eb *effects) {
	c.role = Leader
	c.leaderHint = c.self
	c.nextIndex = make(map[NodeID]Index, len(c.peers))
	c.matchIndex = make(map[NodeID]Index, len(c.peers))
	c.snapPending = make(map[NodeID]bool, len(c.peers))
	next := c.log.lastIndex() + 1
	for _, p := range c.peers {
		c.nextIndex[p] = next
		c.matchIndex[p] = 0
	}
	// Reset ReadIndex confirmation state for the new term: any reads pending under
	// a prior leadership are dropped by step-down; a leader confirms reads only
	// with acks gathered during its own term.
	c.pendingReads = nil
	c.readSeq = 0
	c.confirmAcks = make(map[NodeID]uint64, len(c.peers))
	// Append the leader no-op in the current term. Committing this current-term
	// entry is both the Figure-8 prerequisite for carrying prior-term entries AND
	// the ReadIndex precondition (a leader must commit an entry in its own term
	// before it can safely serve reads); noopIndex records it.
	noop := LogEntry{Term: c.currentTerm, Index: c.log.lastIndex() + 1}
	c.log.append(noop)
	c.noopIndex = noop.Index
	eb.persistLog(noop.Index, []LogEntry{noop})
	// matchIndex for self is implicitly lastIndex; single-node commits now.
	c.maybeAdvanceCommit(eb)
	eb.resetHeartbeatTimer()
	c.broadcastAppend(eb)
}

// hardState snapshots the durable subset.
func (c *raftCore) hardState() HardState {
	return HardState{Term: c.currentTerm, VotedFor: c.votedFor}
}

// --- timeouts ---

func (c *raftCore) handleElectionTimeout(eb *effects) {
	// A leader ignores election ticks (it drives heartbeats instead).
	if c.role == Leader {
		return
	}
	c.becomeCandidate(eb)
}

func (c *raftCore) handleHeartbeatTimeout(eb *effects) {
	if c.role != Leader {
		return
	}
	eb.resetHeartbeatTimer()
	c.broadcastAppend(eb)
}

// broadcastAppend sends an AppendEntries to every peer based on nextIndex.
func (c *raftCore) broadcastAppend(eb *effects) {
	for _, p := range c.peers {
		c.sendAppendTo(p, eb)
	}
}

// sendAppendTo emits a single AppendEntries tailored to peer p's nextIndex, or
// an EffectSendSnapshot when the peer has fallen behind the leader's compacted
// prefix (nextIndex <= snapBase). In that case an AppendEntries can never
// succeed — the leader no longer holds the entry at prevIndex — so it must ship
// a snapshot instead (the driver attaches the application bytes). The heartbeat
// carries the current ReadSeq so followers echo it for ReadIndex confirmation.
func (c *raftCore) sendAppendTo(p NodeID, eb *effects) {
	next := c.nextIndex[p]
	if next <= c.log.snapBase {
		// Peer is behind our snapshot: send a snapshot, not entries we no longer
		// have. Suppress re-sends while one is already in flight to this peer.
		if !c.snapPending[p] {
			c.snapPending[p] = true
			eb.sendSnapshot(c.self, p, c.currentTerm, c.log.snapBase, c.log.snapTerm)
		}
		return
	}
	prevIndex := next - 1
	prevTerm, _ := c.log.termAt(prevIndex)
	entries := c.log.sliceFrom(next)
	eb.send(Message{
		From: c.self, To: p, Type: MsgAppendEntries, Term: c.currentTerm,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: c.commitIndex, ReadSeq: c.readSeq,
	})
}

// --- proposals & reads ---

func (c *raftCore) handlePropose(ref ClientRef, command []byte, eb *effects) {
	if c.role != Leader {
		eb.rejectProposal(ref, c.leaderHint)
		return
	}
	e := LogEntry{Term: c.currentTerm, Index: c.log.lastIndex() + 1, Command: command}
	c.log.append(e)
	eb.persistLog(e.Index, []LogEntry{e})
	c.maybeAdvanceCommit(eb) // single-node clusters commit immediately
	c.broadcastAppend(eb)
}

// handleReadIndex accepts a linearizable read (ReadIndex algorithm). A
// non-leader rejects so the client redirects. A leader captures the read index
// as max(commitIndex, noopIndex) — the max folds in the current-term-committed
// safety gate: a read must reflect a point at or after this leader's term began,
// which by Leader Completeness includes every previously-committed entry, so the
// read can never miss a committed write even if this leader has not yet learned
// its full prior-term commit index. The read is then enqueued against a fresh
// leadership-confirmation round and a heartbeat is broadcast so followers echo
// the round; the read is served (EffectReadIndexReady) only once a quorum has
// confirmed leadership as of that round AND the state machine has applied through
// the read index. This is what prevents a deposed leader (asymmetric partition)
// from serving a stale read: it can no longer gather a fresh ack quorum.
func (c *raftCore) handleReadIndex(ref ClientRef, eb *effects) {
	if c.role != Leader {
		eb.rejectProposal(ref, c.leaderHint)
		return
	}
	readIndex := c.commitIndex
	if c.noopIndex > readIndex {
		readIndex = c.noopIndex
	}
	c.readSeq++
	round := c.readSeq
	c.pendingReads = append(c.pendingReads, pendingRead{ref: ref, readIndex: readIndex, round: round})
	// Single-node clusters (quorum 1) are trivially confirmed by self; try to
	// release immediately, otherwise a heartbeat round gathers the acks.
	if c.confirmPending() {
		c.releaseReads(eb)
	}
	if c.clusterN > 1 {
		eb.resetHeartbeatTimer()
		c.broadcastAppend(eb)
	}
}

// confirmPending marks every pending read whose leadership round has now been
// acked by a quorum as confirmed. Returns true if any read became (or already
// was) confirmed, so the caller knows to attempt release. Self trivially acks
// the current round; peers count via confirmAcks (highest ReadSeq each echoed).
func (c *raftCore) confirmPending() bool {
	any := false
	for i := range c.pendingReads {
		if c.pendingReads[i].confirmed {
			any = true
			continue
		}
		round := c.pendingReads[i].round
		count := 1 // self
		for _, p := range c.peers {
			if c.confirmAcks[p] >= round {
				count++
			}
		}
		if count >= c.quorum {
			c.pendingReads[i].confirmed = true
			any = true
		}
	}
	return any
}

// releaseReads emits EffectReadIndexReady for every pending read that is both
// leadership-confirmed and applied-through, draining them from the FRONT of the
// slice in insertion order so the effect stream stays deterministic. Removal
// from the slice is the single source of at-most-once release.
func (c *raftCore) releaseReads(eb *effects) {
	kept := c.pendingReads[:0]
	for _, pr := range c.pendingReads {
		if pr.confirmed && c.appliedTo >= pr.readIndex {
			eb.readIndexReady(pr.ref, pr.readIndex)
			continue
		}
		kept = append(kept, pr)
	}
	// Reset the slice header without aliasing surprises: rebuild if we dropped any.
	if len(kept) != len(c.pendingReads) {
		c.pendingReads = append([]pendingRead(nil), kept...)
	}
}

// dropPendingReads rejects every outstanding read (used on step-down): the
// client must retry against the new leader. Emits EffectRejectProposal per read
// in insertion order.
func (c *raftCore) dropPendingReads(eb *effects) {
	for _, pr := range c.pendingReads {
		eb.rejectProposal(pr.ref, c.leaderHint)
	}
	c.pendingReads = nil
}

// maybeAdvanceCommit recomputes the commit index from matchIndex quorum and the
// current-term safety rule, then emits EffectApply for newly committed entries.
//
// COMMIT RULE (Figure 8): a leader may only mark an entry committed by counting
// replicas if that entry is from the leader's CURRENT term. Prior-term entries
// become committed only transitively, once a current-term entry above them
// reaches quorum. This prevents committing an entry that a future leader could
// still overwrite.
func (c *raftCore) maybeAdvanceCommit(eb *effects) {
	if c.role != Leader {
		return
	}
	// Find the highest index N > commitIndex such that a quorum has matchIndex
	// >= N and log[N].term == currentTerm.
	for n := c.log.lastIndex(); n > c.commitIndex; n-- {
		t, ok := c.log.termAt(n)
		if !ok || t != c.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range c.peers {
			if c.matchIndex[p] >= n {
				count++
			}
		}
		if count >= c.quorum {
			c.commitIndex = n
			break
		}
	}
	c.applyCommitted(eb)
}

// applyCommitted emits EffectApply for entries in (appliedTo, commitIndex].
func (c *raftCore) applyCommitted(eb *effects) {
	if c.commitIndex <= c.appliedTo {
		return
	}
	var out []CommittedEntry
	for i := c.appliedTo + 1; i <= c.commitIndex; i++ {
		e, ok := c.log.entryAt(i)
		if !ok {
			break // inside snapshot; should not happen for newly committed
		}
		out = append(out, CommittedEntry{Index: e.Index, Term: e.Term, Command: e.Command, Kind: e.Kind})
	}
	if len(out) > 0 {
		c.appliedTo = out[len(out)-1].Index
		eb.apply(out)
		// Applying may have satisfied the applied-through condition for confirmed
		// pending reads; try to release them.
		if c.role == Leader && len(c.pendingReads) > 0 {
			c.releaseReads(eb)
		}
	}
}
