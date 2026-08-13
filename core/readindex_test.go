package core

import "testing"

// leaderCore returns a 3-node core that has won election in term 1 (leader, with
// its no-op committed via one follower ack) and drained the setup effects.
func leaderCore(t *testing.T) *raftCore {
	t.Helper()
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection}) // candidate term 1
	deliverVoteGrants(c, 1)                // leader; no-op at index 1
	// Commit the no-op via a follower ack so ReadIndex's current-term precondition
	// is satisfiable.
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1,
		Success: true, MatchIndex: 1,
	}})
	if c.Role() != Leader {
		t.Fatal("setup: expected leader")
	}
	return c
}

// TestReadIndexRejectedWhenNotLeader verifies a follower rejects a read so the
// client redirects to the leader.
func TestReadIndexRejectedWhenNotLeader(t *testing.T) {
	c := mkCore(3)
	c.leaderHint = "n2"
	effs := c.Step(Event{Type: EventReadIndex, Ref: 9})
	rej, ok := firstEffect(effs, EffectRejectProposal)
	if !ok || rej.Ref != 9 || rej.LeaderHint != "n2" {
		t.Fatalf("reject = %+v, want ref 9 hint n2", rej)
	}
	if _, ok := firstEffect(effs, EffectReadIndexReady); ok {
		t.Fatal("a follower must not serve a read")
	}
}

// TestReadIndexServedAfterQuorumConfirmation verifies the happy path: a leader
// enqueues the read, broadcasts a heartbeat, and once a quorum echoes the
// confirmation round the read is served at the captured index.
func TestReadIndexServedAfterQuorumConfirmation(t *testing.T) {
	c := leaderCore(t)
	effs := c.Step(Event{Type: EventReadIndex, Ref: 42})
	// Not served yet: needs a heartbeat-ack quorum first.
	if _, ok := firstEffect(effs, EffectReadIndexReady); ok {
		t.Fatal("read served before leadership confirmation")
	}
	// The read broadcast a heartbeat carrying a fresh ReadSeq. A follower echoes it.
	round := c.readSeq
	if round == 0 {
		t.Fatal("expected a confirmation round to be opened")
	}
	effs = c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1,
		Success: true, MatchIndex: 1, ReadSeq: round,
	}})
	ready, ok := firstEffect(effs, EffectReadIndexReady)
	if !ok {
		t.Fatal("read not served after a quorum (self + n2) confirmed the round")
	}
	if ready.Ref != 42 {
		t.Fatalf("served ref = %d, want 42", ready.Ref)
	}
}

// TestReadIndexNotServedByStaleAcks is the asymmetric-partition stale-read
// defense: acks that predate the read's confirmation round (ReadSeq below the
// round) must NOT confirm it. A deposed leader that can still hear only OLD acks
// can never gather a fresh quorum, so it never serves a stale read.
func TestReadIndexNotServedByStaleAcks(t *testing.T) {
	c := leaderCore(t)
	c.Step(Event{Type: EventReadIndex, Ref: 7})
	round := c.readSeq
	// A follower reply echoing an OLDER round (round-1) must not confirm.
	stale := round - 1
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1,
		Success: true, MatchIndex: 1, ReadSeq: stale,
	}})
	if len(c.pendingReads) == 0 || c.pendingReads[0].confirmed {
		t.Fatal("a pre-round (stale) ack must not confirm the read")
	}
	// Confirm the current round -> now it serves.
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n3", To: c.self, Type: MsgAppendEntriesResp, Term: 1,
		Success: true, MatchIndex: 1, ReadSeq: round,
	}})
	if _, ok := firstEffect(effs, EffectReadIndexReady); !ok {
		t.Fatal("read should serve once a fresh-round quorum confirms")
	}
}

// TestReadIndexDroppedOnStepDown verifies a leader that steps down (higher term)
// rejects its outstanding reads so the client retries the new leader — never
// serving a read it can no longer vouch for.
func TestReadIndexDroppedOnStepDown(t *testing.T) {
	c := leaderCore(t)
	c.Step(Event{Type: EventReadIndex, Ref: 5})
	if len(c.pendingReads) != 1 {
		t.Fatalf("expected 1 pending read, got %d", len(c.pendingReads))
	}
	// A higher-term AppendEntries forces step-down.
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 9,
		PrevLogIndex: 0, PrevLogTerm: 0,
	}})
	rej, ok := firstEffect(effs, EffectRejectProposal)
	if !ok || rej.Ref != 5 {
		t.Fatalf("stepping-down leader must reject pending read; got %+v ok=%v", rej, ok)
	}
	if len(c.pendingReads) != 0 {
		t.Fatalf("pending reads not cleared on step-down: %d", len(c.pendingReads))
	}
}

// TestReadIndexSingleNodeServedImmediately verifies a single-node cluster serves
// a read without a heartbeat round (self is a quorum).
func TestReadIndexSingleNodeServedImmediately(t *testing.T) {
	c := mkCore(1)
	c.Step(Event{Type: EventTickElection}) // self-elects, commits no-op
	effs := c.Step(Event{Type: EventReadIndex, Ref: 1})
	if _, ok := firstEffect(effs, EffectReadIndexReady); !ok {
		t.Fatal("single-node leader should serve a read immediately")
	}
}
