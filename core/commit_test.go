package core

import "testing"

// TestLeaderCommitsOwnTermEntry verifies the basic commit path: a leader
// replicates an entry from its current term to a quorum and marks it committed,
// emitting EffectApply.
func TestLeaderCommitsOwnTermEntry(t *testing.T) {
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection}) // candidate term 1
	deliverVoteGrants(c, 1)                // leader term 1; no-op at index 1

	// Client proposes a command -> index 2, term 1.
	c.Step(Event{Type: EventPropose, Ref: 1, Command: []byte("x=1")})
	if c.log.lastIndex() != 2 {
		t.Fatalf("lastIndex = %d, want 2", c.log.lastIndex())
	}
	// One follower acks index 2 -> quorum (self + 1) => commit.
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1,
		Success: true, MatchIndex: 2,
	}})
	if c.CommitIndex() != 2 {
		t.Fatalf("commitIndex = %d, want 2", c.CommitIndex())
	}
	ap, ok := firstEffect(effs, EffectApply)
	if !ok {
		t.Fatal("expected EffectApply after commit")
	}
	// Applied entries: index 1 (no-op) and index 2 (x=1), in order.
	if len(ap.Committed) != 2 || ap.Committed[1].Index != 2 {
		t.Fatalf("applied = %+v, want indices 1,2", ap.Committed)
	}
	if !IsNoOp(ap.Committed[0].Command) {
		t.Errorf("index 1 should be a no-op")
	}
}

// TestNoDirectCommitOfPriorTermEntry is the Figure-8 safety test: a leader must
// NOT mark a prior-term entry committed merely because it is replicated on a
// quorum. It becomes committed only once a current-term entry above it reaches
// quorum. This is the rule that prevents a committed entry from later being
// overwritten.
func TestNoDirectCommitOfPriorTermEntry(t *testing.T) {
	c := mkCore(5)
	// Construct a leader in term 3 whose log has a prior-term (term 1) entry at
	// index 1 that is NOT yet committed, plus its own no-op at index 2 (term 3).
	c.currentTerm = 3
	c.role = Leader
	c.leaderHint = c.self
	c.log.append(LogEntry{Term: 1, Index: 1}) // prior-term, uncommitted
	// Simulate becoming leader appending a no-op at index 2 (term 3):
	c.nextIndex = map[NodeID]Index{}
	c.matchIndex = map[NodeID]Index{}
	for _, p := range c.peers {
		c.nextIndex[p] = 2
		c.matchIndex[p] = 0
	}
	c.log.append(LogEntry{Term: 3, Index: 2}) // current-term no-op

	// Three followers (of five) ack ONLY index 1 (the prior-term entry). That is
	// a quorum for index 1, but the prior-term rule forbids committing it.
	for _, p := range []NodeID{"n2", "n3", "n4"} {
		c.Step(Event{Type: EventDeliver, Msg: Message{
			From: p, To: c.self, Type: MsgAppendEntriesResp, Term: 3,
			Success: true, MatchIndex: 1,
		}})
	}
	if c.CommitIndex() != 0 {
		t.Fatalf("commitIndex = %d, want 0 — prior-term entry must NOT commit on replica count alone", c.CommitIndex())
	}

	// Now a quorum acks index 2 (the current-term entry). This commits index 2
	// AND, transitively, index 1.
	for _, p := range []NodeID{"n2", "n3", "n4"} {
		c.Step(Event{Type: EventDeliver, Msg: Message{
			From: p, To: c.self, Type: MsgAppendEntriesResp, Term: 3,
			Success: true, MatchIndex: 2,
		}})
	}
	if c.CommitIndex() != 2 {
		t.Fatalf("commitIndex = %d, want 2 — current-term commit carries the prior entry", c.CommitIndex())
	}
}

// TestFollowerAdvancesCommitFromLeaderCommit verifies a follower advances its
// commit index (and applies) based on the leader's LeaderCommit, clamped to the
// last entry it actually has.
func TestFollowerAdvancesCommitFromLeaderCommit(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 2
	// Append two entries first (leaderCommit 0).
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 2,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 2, Index: 1}, {Term: 2, Index: 2}},
	}})
	// Now leader says commit up to 2.
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 2,
		PrevLogIndex: 2, PrevLogTerm: 2, LeaderCommit: 2,
	}})
	if c.CommitIndex() != 2 {
		t.Fatalf("commitIndex = %d, want 2", c.CommitIndex())
	}
	ap, ok := firstEffect(effs, EffectApply)
	if !ok || len(ap.Committed) != 2 {
		t.Fatalf("expected 2 applied entries, got %+v", ap.Committed)
	}
}

// TestFollowerCommitClampedToLocalLog verifies commit index never exceeds the
// entries the follower actually holds, even if the leader's commit is higher.
func TestFollowerCommitClampedToLocalLog(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 2
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 2,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Term: 2, Index: 1}},
		LeaderCommit: 99, // wildly ahead
	}})
	if c.CommitIndex() != 1 {
		t.Fatalf("commitIndex = %d, want 1 (clamped to local last)", c.CommitIndex())
	}
	if ap, ok := firstEffect(effs, EffectApply); !ok || len(ap.Committed) != 1 {
		t.Fatalf("expected 1 applied entry, got ok=%v %+v", ok, ap.Committed)
	}
}

// TestProposeRejectedWhenNotLeader verifies a non-leader rejects proposals with
// a leader hint for redirection.
func TestProposeRejectedWhenNotLeader(t *testing.T) {
	c := mkCore(3)
	c.leaderHint = "n2"
	effs := c.Step(Event{Type: EventPropose, Ref: 7, Command: []byte("x=1")})
	rej, ok := firstEffect(effs, EffectRejectProposal)
	if !ok || rej.Ref != 7 || rej.LeaderHint != "n2" {
		t.Fatalf("reject = %+v, want ref=7 hint=n2", rej)
	}
	if c.log.lastIndex() != 0 {
		t.Errorf("non-leader must not append; lastIndex=%d", c.log.lastIndex())
	}
}

// TestLeaderBacksUpNextIndexOnConflict verifies the fast-backup: a rejected
// AppendEntries with a conflict-term hint moves nextIndex back past that term.
func TestLeaderBacksUpNextIndexOnConflict(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 4
	c.role = Leader
	c.leaderHint = c.self
	// Leader log: index1 t1, index2 t4, index3 t4.
	c.log.append(
		LogEntry{Term: 1, Index: 1},
		LogEntry{Term: 4, Index: 2},
		LogEntry{Term: 4, Index: 3},
	)
	c.nextIndex = map[NodeID]Index{"n2": 4, "n3": 4}
	c.matchIndex = map[NodeID]Index{"n2": 0, "n3": 0}
	// Follower n2 rejects with conflictTerm=2 firstIndex=2 (it has term-2 stuff
	// the leader doesn't). Leader has no term-2 entries, so it should back up to
	// the follower's conflictIndex (2).
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 4,
		Success: false, ConflictTerm: 2, ConflictIndex: 2,
	}})
	if c.nextIndex["n2"] != 2 {
		t.Errorf("nextIndex[n2] = %d, want 2 after conflict backup", c.nextIndex["n2"])
	}
}
