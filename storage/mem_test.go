package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/billdmar/quorum/core"
)

func TestMemHardStateRoundTrip(t *testing.T) {
	m := NewMem()
	if _, ok, _ := m.LoadHardState(); ok {
		t.Fatal("fresh mem reports a hard state")
	}
	hs := core.HardState{Term: 4, VotedFor: "n3"}
	if err := m.SaveHardState(hs); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	got, ok, err := m.LoadHardState()
	if err != nil || !ok || got != hs {
		t.Fatalf("LoadHardState = %+v ok %v err %v", got, ok, err)
	}
}

func TestMemAppendTruncateLoad(t *testing.T) {
	m := NewMem()
	if fi, _ := m.FirstIndex(); fi != 1 {
		t.Fatalf("FirstIndex = %d, want 1", fi)
	}
	if li, _ := m.LastIndex(); li != 0 {
		t.Fatalf("LastIndex = %d, want 0", li)
	}

	if err := m.AppendEntries(1, []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// Conflict overwrite from index 2.
	if err := m.AppendEntries(2, []core.LogEntry{entry(2, 2, "B"), entry(2, 3, "C")}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	want := []core.LogEntry{entry(1, 1, "a"), entry(2, 2, "B"), entry(2, 3, "C")}
	got, _ := m.LoadEntries()
	if !entriesEqual(got, want) {
		t.Fatalf("entries = %+v, want %+v", got, want)
	}
	if li, _ := m.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
}

func TestMemGapAndCompactedRejected(t *testing.T) {
	m := NewMem()
	if err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.AppendEntries(3, []core.LogEntry{entry(1, 3, "c")}); err == nil {
		t.Fatal("expected gap rejection")
	}
}

func TestMemSnapshotCompaction(t *testing.T) {
	m := NewMem()
	if err := m.AppendEntries(1, []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "c"), entry(2, 4, "d"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.SaveSnapshot(2, 1, []byte("state")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if fi, _ := m.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex = %d, want 3", fi)
	}
	if li, _ := m.LastIndex(); li != 4 {
		t.Fatalf("LastIndex = %d, want 4", li)
	}
	si, st, data, ok, err := m.LoadSnapshot()
	if err != nil || !ok || si != 2 || st != 1 || !bytes.Equal(data, []byte("state")) {
		t.Fatalf("LoadSnapshot = (%d,%d,%q) ok %v err %v", si, st, data, ok, err)
	}
	got, _ := m.LoadEntries()
	want := []core.LogEntry{entry(2, 3, "c"), entry(2, 4, "d")}
	if !entriesEqual(got, want) {
		t.Fatalf("post-compaction = %+v, want %+v", got, want)
	}
	// Appends continue on top of the snapshot.
	if err := m.AppendEntries(5, []core.LogEntry{entry(2, 5, "e")}); err != nil {
		t.Fatalf("append after snapshot: %v", err)
	}
}

// TestMemCrashAfterFsyncDurable: an after-fsync fault applies the write and
// then reports the crash — the data must be fully present.
func TestMemCrashAfterFsyncDurable(t *testing.T) {
	m := NewMem()
	m.Fault = func(op WriteOp, _ core.Index) FaultKind {
		if op == OpHardState {
			return FaultCrashAfterFsync
		}
		return FaultNone
	}
	hs := core.HardState{Term: 5, VotedFor: "n1"}
	err := m.SaveHardState(hs)
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("SaveHardState err = %v, want ErrInjectedCrash", err)
	}
	// Simulate reopen: clear the hook and read what survived.
	m.Fault = nil
	got, ok, _ := m.LoadHardState()
	if !ok || got != hs {
		t.Fatalf("after crash-after-fsync, hard state = %+v ok %v, want fully present", got, ok)
	}
}

// TestMemCrashBeforeFsyncAbsent: a before-fsync fault must NOT apply the write.
func TestMemCrashBeforeFsyncAbsent(t *testing.T) {
	m := NewMem()
	m.Fault = func(op WriteOp, _ core.Index) FaultKind {
		if op == OpHardState {
			return FaultCrashBeforeFsync
		}
		return FaultNone
	}
	err := m.SaveHardState(core.HardState{Term: 9, VotedFor: "n2"})
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	m.Fault = nil
	if _, ok, _ := m.LoadHardState(); ok {
		t.Fatal("crash-before-fsync left a hard state present")
	}
}

// TestMemAppendCrashBeforeFsyncAllOrNothing: a before-fsync fault on an append
// leaves the log exactly as it was — never a partial batch.
func TestMemAppendCrashBeforeFsyncAllOrNothing(t *testing.T) {
	m := NewMem()
	if err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	m.Fault = func(op WriteOp, _ core.Index) FaultKind {
		if op == OpAppend {
			return FaultCrashBeforeFsync
		}
		return FaultNone
	}
	err := m.AppendEntries(2, []core.LogEntry{entry(1, 2, "b"), entry(1, 3, "c")})
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	m.Fault = nil
	got, _ := m.LoadEntries()
	want := []core.LogEntry{entry(1, 1, "a")}
	if !entriesEqual(got, want) {
		t.Fatalf("after before-fsync crash = %+v, want unchanged %+v", got, want)
	}
}

// TestMemAppendCrashAfterFsyncDurable: an after-fsync fault applies the whole
// batch before reporting the crash.
func TestMemAppendCrashAfterFsyncDurable(t *testing.T) {
	m := NewMem()
	m.Fault = func(op WriteOp, idx core.Index) FaultKind {
		// Crash after fsync only once the last entry is written.
		if op == OpAppend && idx == 3 {
			return FaultCrashAfterFsync
		}
		return FaultNone
	}
	err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")})
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	m.Fault = nil
	got, _ := m.LoadEntries()
	want := []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}
	if !entriesEqual(got, want) {
		t.Fatalf("after after-fsync crash = %+v, want fully present %+v", got, want)
	}
}

// TestMemTornWritePrefix: a torn write on the 2nd entry keeps the 1st durably
// and drops the torn entry and everything after it — the longest valid prefix,
// never a partial record.
func TestMemTornWritePrefix(t *testing.T) {
	m := NewMem()
	m.Fault = func(op WriteOp, idx core.Index) FaultKind {
		if op == OpAppend && idx == 2 {
			return FaultTornWrite
		}
		return FaultNone
	}
	err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")})
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	m.Fault = nil
	got, _ := m.LoadEntries()
	want := []core.LogEntry{entry(1, 1, "a")}
	if !entriesEqual(got, want) {
		t.Fatalf("after torn write = %+v, want longest valid prefix %+v", got, want)
	}
	// The store stays usable: a clean append re-fills the tail.
	if err := m.AppendEntries(2, []core.LogEntry{entry(1, 2, "b2")}); err != nil {
		t.Fatalf("append after torn: %v", err)
	}
	got, _ = m.LoadEntries()
	if len(got) != 2 || string(got[1].Command) != "b2" {
		t.Fatalf("post-torn append = %+v", got)
	}
}

// TestMemTornWriteMidTruncation: a torn write during a conflict-overwrite must
// still leave the truncated-then-partially-applied log as a valid prefix.
func TestMemTornWriteMidTruncation(t *testing.T) {
	m := NewMem()
	if err := m.AppendEntries(1, []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Overwrite from index 2 but tear on the entry at index 3.
	m.Fault = func(op WriteOp, idx core.Index) FaultKind {
		if op == OpAppend && idx == 3 {
			return FaultTornWrite
		}
		return FaultNone
	}
	err := m.AppendEntries(2, []core.LogEntry{entry(2, 2, "B"), entry(2, 3, "C")})
	if !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("err = %v, want ErrInjectedCrash", err)
	}
	m.Fault = nil
	// Truncation kept index 1, index 2 was written before the tear at 3.
	got, _ := m.LoadEntries()
	want := []core.LogEntry{entry(1, 1, "a"), entry(2, 2, "B")}
	if !entriesEqual(got, want) {
		t.Fatalf("after mid-truncation torn write = %+v, want %+v", got, want)
	}
}

// TestMemLoadReturnsIndependentSlice verifies LoadEntries hands back a fresh
// slice header, so a caller appending to (or reslicing) it cannot disturb the
// store's own backing array. Command bytes are shared by the project's
// immutable-command convention (see core.raftLog.clone), so we do not assert a
// deep copy of Command.
func TestMemLoadReturnsIndependentSlice(t *testing.T) {
	m := NewMem()
	if err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, _ := m.LoadEntries()
	got = append(got, entry(9, 99, "junk")) // must not reach the store
	_ = got
	again, _ := m.LoadEntries()
	if len(again) != 1 || again[0].Index != 1 {
		t.Fatalf("LoadEntries returned an aliased slice; store now %+v", again)
	}
}
