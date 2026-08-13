package core

import "testing"

// --- test helpers ---

// mkCore builds a core for node "n1" in an n-node cluster (peers n2..nN).
func mkCore(n int) *raftCore {
	self := NodeID("n1")
	var peers []NodeID
	for i := 2; i <= n; i++ {
		peers = append(peers, NodeID(nodeName(i)))
	}
	return newCore(Config{Self: self, Peers: peers})
}

func nodeName(i int) string {
	return "n" + string(rune('0'+i))
}

// countEffect returns how many effects of type t are present.
func countEffect(effs []Effect, t EffectType) int {
	n := 0
	for _, e := range effs {
		if e.Type == t {
			n++
		}
	}
	return n
}

// firstEffect returns the first effect of type t, or false.
func firstEffect(effs []Effect, t EffectType) (Effect, bool) {
	for _, e := range effs {
		if e.Type == t {
			return e, true
		}
	}
	return Effect{}, false
}

// deliverVoteGrants makes c (a candidate) receive granted votes from k peers.
func deliverVoteGrants(c *raftCore, k int) []Effect {
	var all []Effect
	for i := 2; i <= k+1; i++ {
		effs := c.Step(Event{Type: EventDeliver, Msg: Message{
			From: NodeID(nodeName(i)), To: c.self, Type: MsgRequestVoteResp,
			Term: c.currentTerm, VoteGranted: true,
		}})
		all = append(all, effs...)
	}
	return all
}

// --- election ---

func TestElectionTimeoutStartsElection(t *testing.T) {
	c := mkCore(3)
	effs := c.Step(Event{Type: EventTickElection})
	if c.Role() != Candidate {
		t.Fatalf("role = %v, want candidate", c.Role())
	}
	if c.Term() != 1 {
		t.Fatalf("term = %d, want 1", c.Term())
	}
	if c.votedFor != c.self {
		t.Fatalf("votedFor = %q, want self", c.votedFor)
	}
	// Persist hard state before sending RequestVotes; one RequestVote per peer.
	if got := countEffect(effs, EffectPersistHardState); got != 1 {
		t.Errorf("persist hard state = %d, want 1", got)
	}
	if got := countEffect(effs, EffectSend); got != 2 {
		t.Errorf("RequestVote sends = %d, want 2", got)
	}
	// Persist must precede any send.
	assertPersistBeforeSend(t, effs)
}

func TestMajorityVotesElectLeader(t *testing.T) {
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection}) // become candidate, term 1
	effs := deliverVoteGrants(c, 1)        // one grant => 2/3 majority
	if c.Role() != Leader {
		t.Fatalf("role = %v, want leader", c.Role())
	}
	// On becoming leader: append a no-op (persist log) and broadcast appends.
	if got := countEffect(effs, EffectPersistLog); got != 1 {
		t.Errorf("persist log (no-op) = %d, want 1", got)
	}
	if got := countEffect(effs, EffectSend); got == 0 {
		t.Errorf("expected heartbeat/append sends after election")
	}
}

func TestSingleNodeClusterElectsSelfImmediately(t *testing.T) {
	c := mkCore(1)
	c.Step(Event{Type: EventTickElection})
	if c.Role() != Leader {
		t.Fatalf("role = %v, want leader (single node)", c.Role())
	}
	// The no-op should commit immediately (self is a quorum of one).
	if c.CommitIndex() != 1 {
		t.Errorf("commitIndex = %d, want 1 (no-op committed)", c.CommitIndex())
	}
}

func TestLeaderIgnoresElectionTick(t *testing.T) {
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection})
	deliverVoteGrants(c, 1)
	termBefore := c.Term()
	c.Step(Event{Type: EventTickElection})
	if c.Role() != Leader || c.Term() != termBefore {
		t.Errorf("leader should ignore election tick; role=%v term=%d", c.Role(), c.Term())
	}
}

func TestHigherTermStepsDown(t *testing.T) {
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection}) // candidate term 1
	deliverVoteGrants(c, 1)                // leader term 1
	// A message from a higher term forces step-down to follower.
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 5,
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0,
	}})
	if c.Role() != Follower {
		t.Fatalf("role = %v, want follower after higher-term msg", c.Role())
	}
	if c.Term() != 5 {
		t.Fatalf("term = %d, want 5", c.Term())
	}
	if c.Leader() != "n2" {
		t.Errorf("leader hint = %q, want n2", c.Leader())
	}
}

// --- RequestVote receiver rules ---

func TestVoteDeniedToStaleLog(t *testing.T) {
	c := mkCore(3)
	// Give c a log at term 2 so a candidate with an older log is rejected.
	c.currentTerm = 2
	c.log.append(LogEntry{Term: 2, Index: 1}, LogEntry{Term: 2, Index: 2})
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgRequestVote, Term: 3,
		LastLogIndex: 1, LastLogTerm: 1, // stale: older last term
	}})
	resp, ok := firstEffect(effs, EffectSend)
	if !ok || resp.Msg.VoteGranted {
		t.Fatalf("expected vote denied to stale-log candidate; granted=%v", resp.Msg.VoteGranted)
	}
	// Term still advances to 3 (universal term rule) even though vote denied.
	if c.Term() != 3 {
		t.Errorf("term = %d, want 3", c.Term())
	}
}

func TestVoteGrantedOncePerTerm(t *testing.T) {
	c := mkCore(3)
	grant := func(from NodeID) bool {
		effs := c.Step(Event{Type: EventDeliver, Msg: Message{
			From: from, To: c.self, Type: MsgRequestVote, Term: 1,
			LastLogIndex: 0, LastLogTerm: 0,
		}})
		resp, _ := firstEffect(effs, EffectSend)
		return resp.Msg.VoteGranted
	}
	if !grant("n2") {
		t.Fatal("first vote should be granted")
	}
	if grant("n3") {
		t.Fatal("second vote in same term to a different candidate must be denied")
	}
	// Re-request from the same candidate is idempotently granted.
	if !grant("n2") {
		t.Fatal("repeat request from same candidate should still be granted")
	}
}

// --- AppendEntries receiver rules ---

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 5
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 3,
	}})
	resp, ok := firstEffect(effs, EffectSend)
	if !ok || resp.Msg.Success {
		t.Fatalf("stale-term AppendEntries must be rejected; success=%v", resp.Msg.Success)
	}
	if resp.Msg.Term != 5 {
		t.Errorf("resp term = %d, want 5", resp.Msg.Term)
	}
}

func TestAppendEntriesConsistencyCheckFails(t *testing.T) {
	c := mkCore(3)
	// Follower has one entry at term 1; leader claims prevLogIndex=2 which we lack.
	c.currentTerm = 1
	c.log.append(LogEntry{Term: 1, Index: 1})
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 1,
		PrevLogIndex: 2, PrevLogTerm: 1,
	}})
	resp, _ := firstEffect(effs, EffectSend)
	if resp.Msg.Success {
		t.Fatal("consistency check should fail (missing prevLogIndex)")
	}
	// Too-short hint: conflictIndex should point past our last entry, no term.
	if resp.Msg.ConflictIndex != 2 || resp.Msg.ConflictTerm != 0 {
		t.Errorf("conflict hint = (%d,%d), want (2,0)", resp.Msg.ConflictIndex, resp.Msg.ConflictTerm)
	}
}

func TestAppendEntriesAppendsAndPersists(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 1
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
		LeaderCommit: 0,
	}})
	if c.log.lastIndex() != 2 {
		t.Fatalf("lastIndex = %d, want 2", c.log.lastIndex())
	}
	pl, ok := firstEffect(effs, EffectPersistLog)
	if !ok || pl.FromIndex != 1 || len(pl.Entries) != 2 {
		t.Fatalf("persist log = %+v, want from=1 len=2", pl)
	}
	resp, _ := firstEffect(effs, EffectSend)
	if !resp.Msg.Success || resp.Msg.MatchIndex != 2 {
		t.Errorf("resp = success=%v match=%d, want success match=2", resp.Msg.Success, resp.Msg.MatchIndex)
	}
	assertPersistBeforeSend(t, effs)
}

// TestAppendEntriesOverwritesConflict verifies the log-matching reconciliation:
// a conflicting suffix is overwritten, a matching prefix is preserved.
func TestAppendEntriesOverwritesConflict(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 3
	// Local log: [t1, t2(stale), t2(stale)] — indices 1..3.
	c.log.append(
		LogEntry{Term: 1, Index: 1},
		LogEntry{Term: 2, Index: 2},
		LogEntry{Term: 2, Index: 3},
	)
	// Leader (term 3) says: prev=(1,t1); entries index2=t3, index3=t3.
	// Index 1 matches; index 2 conflicts (t2 vs t3) => truncate from 2, append.
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 3,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Index: 2}, {Term: 3, Index: 3}},
	}})
	if c.log.lastIndex() != 3 {
		t.Fatalf("lastIndex = %d, want 3", c.log.lastIndex())
	}
	if term, _ := c.log.termAt(1); term != 1 {
		t.Errorf("index 1 term = %d, want 1 (preserved prefix)", term)
	}
	if term, _ := c.log.termAt(2); term != 3 {
		t.Errorf("index 2 term = %d, want 3 (overwritten)", term)
	}
	pl, ok := firstEffect(effs, EffectPersistLog)
	if !ok || pl.FromIndex != 2 {
		t.Errorf("persist from = %d, want 2 (only rewritten suffix)", pl.FromIndex)
	}
}

// TestAppendEntriesIdempotentReplay verifies a duplicated AppendEntries (same
// entries already present) writes nothing new — needed for the lossy/dup schedules.
func TestAppendEntriesIdempotentReplay(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 1
	msg := Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
	}
	c.Step(Event{Type: EventDeliver, Msg: msg})
	effs := c.Step(Event{Type: EventDeliver, Msg: msg}) // replay
	if got := countEffect(effs, EffectPersistLog); got != 0 {
		t.Errorf("replay persisted %d times, want 0 (idempotent)", got)
	}
	resp, _ := firstEffect(effs, EffectSend)
	if !resp.Msg.Success || resp.Msg.MatchIndex != 2 {
		t.Errorf("replay resp success=%v match=%d, want success match=2", resp.Msg.Success, resp.Msg.MatchIndex)
	}
}

func assertPersistBeforeSend(t *testing.T, effs []Effect) {
	t.Helper()
	sawSend := false
	for _, e := range effs {
		switch e.Type {
		case EffectSend:
			sawSend = true
		case EffectPersistHardState, EffectPersistLog:
			if sawSend {
				t.Errorf("persist effect (%v) emitted after a send — violates persist-before-send", e.Type)
			}
		}
	}
}
