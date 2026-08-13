package core

import "testing"

// --- snapshot / compaction / InstallSnapshot ---

// TestCompactToAdvancesBase verifies the driver-triggered compaction advances the
// log base and drops the prefix while retaining the tail and all accessors.
func TestCompactToAdvancesBase(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 2
	c.log.append(
		LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		LogEntry{Term: 2, Index: 2, Command: []byte("b")},
		LogEntry{Term: 2, Index: 3, Command: []byte("c")},
	)
	c.commitIndex = 3
	c.CompactTo(2, 2)
	if c.SnapBase() != 2 || c.SnapTerm() != 2 {
		t.Fatalf("snap base/term = %d/%d, want 2/2", c.SnapBase(), c.SnapTerm())
	}
	if c.log.firstIndex() != 3 {
		t.Fatalf("firstIndex = %d, want 3", c.log.firstIndex())
	}
	// Entry 3 is retained; the term at the snapshot base is still resolvable.
	if term, ok := c.log.termAt(3); !ok || term != 2 {
		t.Errorf("term at 3 = %d ok=%v, want 2 true", term, ok)
	}
	if term, ok := c.log.termAt(2); !ok || term != 2 {
		t.Errorf("term at snap base 2 = %d ok=%v, want 2 true", term, ok)
	}
}

// TestCompactToNeverCompactsUncommitted verifies CompactTo clamps to commitIndex.
func TestCompactToNeverCompactsUncommitted(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 1
	c.log.append(LogEntry{Term: 1, Index: 1}, LogEntry{Term: 1, Index: 2}, LogEntry{Term: 1, Index: 3})
	c.commitIndex = 1
	c.CompactTo(3, 1) // asks beyond commit; must clamp to 1
	if c.SnapBase() != 1 {
		t.Fatalf("snap base = %d, want 1 (clamped to commit)", c.SnapBase())
	}
}

// TestLeaderSendsSnapshotToLaggingPeer verifies that when a peer's nextIndex has
// fallen at/below the leader's snapshot base, the leader emits EffectSendSnapshot
// instead of a doomed AppendEntries, and suppresses re-sends while one is pending.
func TestLeaderSendsSnapshotToLaggingPeer(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 5
	c.role = Leader
	c.leaderHint = c.self
	// Leader has compacted through index 10; log holds 11..12.
	c.log.append(LogEntry{Term: 5, Index: 11}, LogEntry{Term: 5, Index: 12})
	c.log.snapBase, c.log.snapTerm = 10, 5
	c.commitIndex = 12
	c.nextIndex = map[NodeID]Index{"n2": 5, "n3": 13} // n2 is behind the snapshot
	c.matchIndex = map[NodeID]Index{"n2": 0, "n3": 12}
	c.snapPending = map[NodeID]bool{}

	eb := &effects{}
	c.sendAppendTo("n2", eb)
	snap, ok := firstEffect(eb.done(), EffectSendSnapshot)
	if !ok {
		t.Fatal("expected EffectSendSnapshot for a peer behind the snapshot base")
	}
	if snap.SnapIndex != 10 || snap.SnapTerm != 5 || snap.Msg.To != "n2" {
		t.Fatalf("snapshot effect = %+v, want SnapIndex 10 SnapTerm 5 To n2", snap)
	}
	if !c.snapPending["n2"] {
		t.Error("snapPending[n2] should be set")
	}
	// A second call while pending must NOT emit another snapshot.
	eb2 := &effects{}
	c.sendAppendTo("n2", eb2)
	if _, ok := firstEffect(eb2.done(), EffectSendSnapshot); ok {
		t.Error("snapshot re-sent while one was already pending")
	}
}

// TestFollowerInstallsSnapshot verifies a follower accepting a fresh snapshot:
// it emits EffectInstallSnapshot (persist+restore) before the reply, advances
// commit/applied to the snapshot base, and rebases its log.
func TestFollowerInstallsSnapshot(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 3
	// Follower has a short, stale log.
	c.log.append(LogEntry{Term: 1, Index: 1})
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgInstallSnapshot, Term: 3,
		LastIncludedIndex: 8, LastIncludedTerm: 3, SnapshotData: []byte("state"),
	}})
	if c.SnapBase() != 8 || c.CommitIndex() != 8 {
		t.Fatalf("after install: snapBase=%d commit=%d, want 8/8", c.SnapBase(), c.CommitIndex())
	}
	inst, ok := firstEffect(effs, EffectInstallSnapshot)
	if !ok || inst.SnapIndex != 8 || string(inst.SnapData) != "state" {
		t.Fatalf("install effect = %+v, want SnapIndex 8 data \"state\"", inst)
	}
	// Persist/restore must precede the reply Send.
	assertInstallBeforeSend(t, effs)
	resp, ok := firstEffect(effs, EffectSend)
	if !ok || resp.Msg.Type != MsgInstallSnapshotResp || resp.Msg.MatchIndex != 8 {
		t.Fatalf("reply = %+v, want InstallSnapshotResp MatchIndex 8", resp.Msg)
	}
}

// TestFollowerIgnoresStaleSnapshot verifies a snapshot at/below the follower's
// commit index never rolls state back, but still replies so the leader advances.
func TestFollowerIgnoresStaleSnapshot(t *testing.T) {
	c := mkCore(3)
	c.currentTerm = 3
	c.log.append(LogEntry{Term: 3, Index: 1}, LogEntry{Term: 3, Index: 2}, LogEntry{Term: 3, Index: 3})
	c.commitIndex = 3
	effs := c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgInstallSnapshot, Term: 3,
		LastIncludedIndex: 2, LastIncludedTerm: 3, SnapshotData: []byte("old"),
	}})
	if c.CommitIndex() != 3 {
		t.Fatalf("stale snapshot rolled commit back to %d, want 3", c.CommitIndex())
	}
	if _, ok := firstEffect(effs, EffectInstallSnapshot); ok {
		t.Error("stale snapshot should not be installed")
	}
	if resp, ok := firstEffect(effs, EffectSend); !ok || resp.Msg.Type != MsgInstallSnapshotResp {
		t.Error("follower should still reply to a stale snapshot")
	}
}

// TestRestoreRebasesOnSnapshot verifies Restore seeds the log base and
// commit/applied from the recovered snapshot bounds.
func TestRestoreRebasesOnSnapshot(t *testing.T) {
	c := mkCore(3)
	tail := []LogEntry{{Term: 4, Index: 21}, {Term: 4, Index: 22}}
	c.Restore(HardState{Term: 4, VotedFor: "n2"}, 20, 4, tail)
	if c.SnapBase() != 20 || c.SnapTerm() != 4 {
		t.Fatalf("snap base/term = %d/%d, want 20/4", c.SnapBase(), c.SnapTerm())
	}
	if c.CommitIndex() != 20 {
		t.Fatalf("commit = %d, want 20 (seeded from snapshot)", c.CommitIndex())
	}
	if c.LastLogIndex() != 22 {
		t.Fatalf("lastIndex = %d, want 22", c.LastLogIndex())
	}
	if c.Term() != 4 || c.Role() != Follower {
		t.Fatalf("term/role = %d/%v, want 4/follower", c.Term(), c.Role())
	}
}

// assertInstallBeforeSend checks EffectInstallSnapshot precedes any EffectSend.
func assertInstallBeforeSend(t *testing.T, effs []Effect) {
	t.Helper()
	sawSend := false
	for _, e := range effs {
		switch e.Type {
		case EffectSend:
			sawSend = true
		case EffectInstallSnapshot:
			if sawSend {
				t.Error("EffectInstallSnapshot emitted after a send — violates persist-before-send")
			}
		}
	}
}
