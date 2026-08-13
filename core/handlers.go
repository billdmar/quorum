package core

// handleMessage dispatches an incoming Raft RPC. The universal term rule runs
// first: any message with a term greater than ours forces us to adopt it and
// step down before type-specific handling.
func (c *raftCore) handleMessage(m Message, eb *effects) {
	if m.Term > c.currentTerm {
		// A RequestVote does not itself reveal a leader, so leaderHint is None;
		// AppendEntries/InstallSnapshot from a higher term reveal the leader.
		leader := None
		if m.Type == MsgAppendEntries || m.Type == MsgInstallSnapshot {
			leader = m.From
		}
		c.becomeFollower(m.Term, leader, eb)
	}

	switch m.Type {
	case MsgRequestVote:
		c.handleRequestVote(m, eb)
	case MsgRequestVoteResp:
		c.handleRequestVoteResp(m, eb)
	case MsgAppendEntries:
		c.handleAppendEntries(m, eb)
	case MsgAppendEntriesResp:
		c.handleAppendEntriesResp(m, eb)
	case MsgInstallSnapshot:
		c.handleInstallSnapshot(m, eb)
	case MsgInstallSnapshotResp:
		c.handleInstallSnapshotResp(m, eb)
	}
}

// handleRequestVote implements the RequestVote receiver rules. A vote is
// granted iff: the candidate's term is not stale, we have not already voted for
// someone else this term, and the candidate's log is at least as up-to-date as
// ours (the log-comparison rule that guarantees leader completeness).
func (c *raftCore) handleRequestVote(m Message, eb *effects) {
	grant := false
	if m.Term >= c.currentTerm {
		canVote := c.votedFor == None || c.votedFor == m.From
		if canVote && c.candidateUpToDate(m.LastLogIndex, m.LastLogTerm) {
			grant = true
		}
	}
	if grant {
		c.votedFor = m.From
		eb.persistHardState(c.hardState())
		// Granting a vote is hearing from a viable leader-candidate: reset the
		// election timer so we don't immediately start a competing election.
		eb.resetElectionTimer()
	}
	eb.send(Message{
		From: c.self, To: m.From, Type: MsgRequestVoteResp,
		Term: c.currentTerm, VoteGranted: grant,
	})
}

// candidateUpToDate implements the log up-to-date comparison: a log with the
// later last-term is more up-to-date; if terms tie, the longer log wins.
func (c *raftCore) candidateUpToDate(lastIndex Index, lastTerm Term) bool {
	myTerm := c.log.lastTerm()
	myIndex := c.log.lastIndex()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// handleRequestVoteResp tallies a vote. Only a candidate in the matching term
// cares; a majority promotes us to leader. Stale-term responses were already
// handled by the universal term rule (they'd have stepped us down).
func (c *raftCore) handleRequestVoteResp(m Message, eb *effects) {
	if c.role != Candidate || m.Term != c.currentTerm {
		return
	}
	if m.VoteGranted {
		c.votesGranted[m.From] = true
		if c.countVotes() >= c.quorum {
			c.becomeLeader(eb)
		}
	}
}

func (c *raftCore) countVotes() int {
	n := 0
	for _, g := range c.votesGranted {
		if g {
			n++
		}
	}
	return n
}

// handleAppendEntries implements the AppendEntries receiver rules: reject if the
// leader's term is stale; otherwise recognize the leader, reset the election
// timer, run the log-consistency check at prevLogIndex/Term, reconcile entries,
// and advance the commit index to min(leaderCommit, lastNewIndex).
func (c *raftCore) handleAppendEntries(m Message, eb *effects) {
	if m.Term < c.currentTerm {
		c.sendAppendReject(m.From, eb)
		return
	}
	// Valid leader for our term (equal term): ensure follower role, record
	// leader, reset the election timer.
	if c.role != Follower {
		c.role = Follower
	}
	c.leaderHint = m.From
	eb.resetElectionTimer()

	// Log consistency check.
	if !c.log.matches(m.PrevLogIndex, m.PrevLogTerm) {
		ci, ct := c.conflictHint(m.PrevLogIndex)
		eb.send(Message{
			From: c.self, To: m.From, Type: MsgAppendEntriesResp, Term: c.currentTerm,
			Success: false, ConflictIndex: ci, ConflictTerm: ct,
			ReadSeq: m.ReadSeq, // echo the round: this reply still proves reachability
		})
		return
	}

	// Reconcile entries (append/overwrite as needed) and persist only what is new.
	if len(m.Entries) > 0 {
		from, written := c.log.appendFromLeader(m.PrevLogIndex, m.Entries)
		if len(written) > 0 {
			eb.persistLog(from, written)
			// A KindConfig entry may have been appended or an uncommitted one
			// truncated by the reconcile; re-derive membership from the log
			// (adopt-on-append / revert-on-truncation).
			c.adoptConfigFromLog(eb)
		}
	}

	// Advance commit index. The last index this message vouches for is the
	// highest of prevLogIndex and the last appended entry.
	lastNew := m.PrevLogIndex
	if n := len(m.Entries); n > 0 {
		lastNew = m.Entries[n-1].Index
	}
	if m.LeaderCommit > c.commitIndex {
		c.commitIndex = min(m.LeaderCommit, lastNew)
		c.applyCommitted(eb)
	}

	eb.send(Message{
		From: c.self, To: m.From, Type: MsgAppendEntriesResp, Term: c.currentTerm,
		Success: true, MatchIndex: lastNew,
		ReadSeq: m.ReadSeq, // echo the leadership-confirmation round
	})
}

// conflictHint produces the fast-backup hint for a failed consistency check. If
// we have an entry at prevLogIndex with a different term, report that term and
// the first index of that term so the leader can skip the whole term in one
// round. If we are simply too short, report our next index and no term.
func (c *raftCore) conflictHint(prevLogIndex Index) (Index, Term) {
	if prevLogIndex > c.log.lastIndex() {
		return c.log.lastIndex() + 1, 0
	}
	ct, ok := c.log.termAt(prevLogIndex)
	if !ok || ct == 0 {
		return c.log.lastIndex() + 1, 0
	}
	// Walk back to the first index of term ct.
	first := prevLogIndex
	for first > c.log.firstIndex() {
		t, ok := c.log.termAt(first - 1)
		if !ok || t != ct {
			break
		}
		first--
	}
	return first, ct
}

func (c *raftCore) sendAppendReject(to NodeID, eb *effects) {
	eb.send(Message{
		From: c.self, To: to, Type: MsgAppendEntriesResp, Term: c.currentTerm,
		Success: false, ConflictIndex: 0, ConflictTerm: 0,
	})
}

// handleAppendEntriesResp processes a follower's reply on the leader. On success
// it advances matchIndex/nextIndex and may advance the commit index. On failure
// it backs up nextIndex using the conflict hint and retries.
func (c *raftCore) handleAppendEntriesResp(m Message, eb *effects) {
	if c.role != Leader || m.Term != c.currentTerm {
		return
	}
	// Record the leadership-confirmation round this peer echoed (monotonic per
	// peer). Any reply — success or reject — proves the peer heard this leader in
	// this round, which is exactly what ReadIndex confirmation needs.
	if m.ReadSeq > c.confirmAcks[m.From] {
		c.confirmAcks[m.From] = m.ReadSeq
	}
	if len(c.pendingReads) > 0 && c.confirmPending() {
		c.releaseReads(eb)
	}

	if m.Success {
		if m.MatchIndex+1 > c.nextIndex[m.From] {
			c.nextIndex[m.From] = m.MatchIndex + 1
		}
		if m.MatchIndex > c.matchIndex[m.From] {
			c.matchIndex[m.From] = m.MatchIndex
		}
		c.maybeAdvanceCommit(eb)
		return
	}
	// Failure: back up nextIndex using the conflict hint, then retry.
	c.nextIndex[m.From] = c.backupNextIndex(m)
	c.sendAppendTo(m.From, eb)
}

// backupNextIndex computes the new nextIndex for a follower after a rejected
// AppendEntries, using the follower's conflict hint to skip whole terms.
func (c *raftCore) backupNextIndex(m Message) Index {
	if m.ConflictTerm == 0 {
		// Follower was too short (or empty prefix conflict): jump to its hint.
		if m.ConflictIndex == 0 {
			return 1
		}
		return m.ConflictIndex
	}
	// If we have entries of ConflictTerm, set nextIndex to the index just past
	// our last entry of that term; otherwise fall back to the follower's first
	// index of the conflicting term.
	lastOfTerm := Index(0)
	for i := c.log.lastIndex(); i >= c.log.firstIndex(); i-- {
		t, ok := c.log.termAt(i)
		if !ok {
			break
		}
		if t == m.ConflictTerm {
			lastOfTerm = i
			break
		}
		if t < m.ConflictTerm {
			break
		}
	}
	if lastOfTerm != 0 {
		return lastOfTerm + 1
	}
	if m.ConflictIndex == 0 {
		return 1
	}
	return m.ConflictIndex
}

// handleInstallSnapshot is the follower's InstallSnapshot receiver. It rejects a
// stale-term snapshot; otherwise it recognizes the leader, resets the election
// timer, and — unless the snapshot is stale relative to what it already has —
// installs it: rebase the log on (LastIncludedIndex, LastIncludedTerm), advance
// commitIndex/appliedTo to the snapshot base (those entries are now reflected in
// the application state), and emit EffectInstallSnapshot so the driver persists
// the snapshot AND restores the application state machine BEFORE the reply Send.
// A snapshot at or below our commit index is ignored (never roll state back);
// we still reply so the leader can advance its nextIndex for us.
func (c *raftCore) handleInstallSnapshot(m Message, eb *effects) {
	if m.Term < c.currentTerm {
		eb.send(Message{
			From: c.self, To: m.From, Type: MsgInstallSnapshotResp, Term: c.currentTerm,
		})
		return
	}
	// Equal-or-higher term (higher already stepped us down in handleMessage).
	if c.role != Follower {
		c.role = Follower
	}
	c.leaderHint = m.From
	eb.resetElectionTimer()

	if m.LastIncludedIndex > c.commitIndex {
		// Fold any KindConfig entries the snapshot supersedes into configBaseline
		// BEFORE installSnapshot drops them, so membership implied by the compacted
		// prefix survives (same reasoning as CompactTo). NOTE: if the local log
		// conflicts and is discarded wholesale, config entries the follower never
		// held cannot be recovered from bytes here — snapshot-carried membership is
		// a documented boundary (DESIGN §15d); in the tested paths the follower has
		// the matching prefix so this fold is exact.
		c.foldConfigIntoBaseline(m.LastIncludedIndex)
		// Adopt the snapshot. installSnapshot retains a matching tail (Raft §7) or
		// discards a conflicting log.
		c.log.installSnapshot(m.LastIncludedIndex, m.LastIncludedTerm)
		c.commitIndex = m.LastIncludedIndex
		c.appliedTo = m.LastIncludedIndex
		// Re-derive membership: the surviving log tail (post-install) replayed over
		// the now-updated baseline.
		c.adoptConfigFromLog(eb)
		// Persist snapshot + restore application state BEFORE the reply Send.
		eb.installSnapshot(m.LastIncludedIndex, m.LastIncludedTerm, m.SnapshotData)
	}

	eb.send(Message{
		From: c.self, To: m.From, Type: MsgInstallSnapshotResp, Term: c.currentTerm,
		MatchIndex: c.commitIndex, ReadSeq: m.ReadSeq,
	})
}

// handleInstallSnapshotResp is the leader's handler: clear the in-flight flag and
// advance the peer's nextIndex/matchIndex to just past the snapshot it now holds,
// so subsequent AppendEntries resume from the tail.
func (c *raftCore) handleInstallSnapshotResp(m Message, eb *effects) {
	if c.role != Leader || m.Term != c.currentTerm {
		return
	}
	c.snapPending[m.From] = false
	if m.MatchIndex > c.matchIndex[m.From] {
		c.matchIndex[m.From] = m.MatchIndex
	}
	if m.MatchIndex+1 > c.nextIndex[m.From] {
		c.nextIndex[m.From] = m.MatchIndex + 1
	}
	// The peer may still lag the leader's snapshot base if the leader compacted
	// further; the next heartbeat re-evaluates and re-sends a snapshot if needed.
	c.maybeAdvanceCommit(eb)
}
