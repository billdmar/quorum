package kv

import (
	"bytes"
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
)

// apply is a test helper that wraps a Command in a committed entry and applies
// it, returning the Result. idx is only used for entry bookkeeping.
func apply(s *Store, idx core.Index, c Command) Result {
	return s.Apply(core.CommittedEntry{Index: idx, Term: 1, Command: c.Encode()})
}

func TestCommandEncodeDecodeRoundTrip(t *testing.T) {
	cmds := []Command{
		{ClientID: 1, SeqNum: 1, Op: check.OpGet, Key: "k"},
		{ClientID: 2, SeqNum: 7, Op: check.OpPut, Key: "k", Value: "v"},
		{ClientID: 3, SeqNum: 9, Op: check.OpAppend, Key: "k", Value: "tail"},
		{ClientID: 4, SeqNum: 2, Op: check.OpCAS, Key: "k", Value: "new", CompareValue: "old"},
		{ClientID: 5, SeqNum: 3, Op: check.OpPut, Key: "", Value: ""},
	}
	for _, want := range cmds {
		got, err := Decode(want.Encode())
		if err != nil {
			t.Fatalf("Decode(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Fatal("expected error decoding empty input")
	}
	if _, err := Decode([]byte{0x01, 0x02}); err == nil {
		t.Fatal("expected error decoding short input")
	}
	// Valid header but a key length that overruns the buffer.
	bad := make([]byte, commandHeader+4)
	bad[commandHeader] = 0xFF // huge key length
	if _, err := Decode(bad); err == nil {
		t.Fatal("expected error on overrunning key length")
	}
}

func TestDecodeRejectsTruncatedTail(t *testing.T) {
	full := Command{ClientID: 1, SeqNum: 1, Op: check.OpCAS, Key: "k", Value: "v", CompareValue: "c"}.Encode()
	// Chop within the value string (after key) — truncated value.
	keyEnd := commandHeader + 4 + 1 // header + key-len + "k"
	if _, err := Decode(full[:keyEnd+2]); err == nil {
		t.Fatal("expected error on truncated value")
	}
	// Chop within the compare-value string — truncated compare-value.
	if _, err := Decode(full[:len(full)-1]); err == nil {
		t.Fatal("expected error on truncated compare-value")
	}
	// Extra trailing byte — trailing-bytes error.
	if _, err := Decode(append(full, 0x00)); err == nil {
		t.Fatal("expected error on trailing bytes")
	}
}

func TestCASOnAbsentKey(t *testing.T) {
	s := NewStore()
	// Register CAS with absent-key-reads-as-"" semantics: CAS(compare="") on an
	// absent key SUCCEEDS and creates it (an absent key has the empty value). This
	// must match the Porcupine model in check/model.go.
	r := apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpCAS, Key: "nope", Value: "v", CompareValue: ""})
	if !r.OK || r.Value != "v" {
		t.Fatalf("CAS(compare=\"\") on absent key: got %+v, want success with value \"v\"", r)
	}
	if got, ok := s.Get("nope"); !ok || got != "v" {
		t.Fatalf("CAS on absent key should create it = \"v\"; got %q ok=%v", got, ok)
	}
	// CAS with a NON-empty compare against an absent key must fail (absent != "x").
	r2 := apply(s, 2, Command{ClientID: 2, SeqNum: 1, Op: check.OpCAS, Key: "other", Value: "v", CompareValue: "x"})
	if r2.OK || r2.Value != "" {
		t.Fatalf("CAS(compare=\"x\") on absent key: got %+v, want failed/empty", r2)
	}
}

func TestUnknownOpIgnored(t *testing.T) {
	s := NewStore()
	// An out-of-range Op decodes fine but falls through execute's default arm.
	cmd := Command{ClientID: 1, SeqNum: 1, Op: check.OpKind(200), Key: "k", Value: "v"}
	if r := apply(s, 1, cmd); r != (Result{}) {
		t.Fatalf("unknown op should yield zero Result, got %+v", r)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("unknown op mutated state")
	}
}

func TestRestoreRejectsTruncatedAtEveryStage(t *testing.T) {
	// Build a snapshot with both a key and a session so every decode stage runs.
	s := NewStore()
	apply(s, 1, Command{ClientID: 7, SeqNum: 3, Op: check.OpPut, Key: "a", Value: "1"})
	snap := s.Snapshot()

	// Truncating at many prefix lengths must always error, never panic, and
	// leave the target store empty. This exercises the truncation branches for
	// key count, key/value strings, session count, ids, seqs, results, flags.
	for n := 0; n < len(snap); n++ {
		fresh := NewStore()
		if err := fresh.Restore(snap[:n]); err == nil {
			t.Fatalf("expected error restoring %d-byte prefix", n)
		}
		if len(fresh.data) != 0 || len(fresh.sessions) != 0 {
			t.Fatalf("failed restore at prefix %d mutated store", n)
		}
	}
	// The full snapshot restores cleanly.
	fresh := NewStore()
	if err := fresh.Restore(snap); err != nil {
		t.Fatalf("full restore failed: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	s := NewStore()

	// Get of absent key: not found, empty value.
	r := apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpGet, Key: "missing"})
	if r.Found || r.Value != "" {
		t.Fatalf("absent Get: got %+v, want not-found empty", r)
	}

	apply(s, 2, Command{ClientID: 1, SeqNum: 2, Op: check.OpPut, Key: "a", Value: "1"})
	r = apply(s, 3, Command{ClientID: 1, SeqNum: 3, Op: check.OpGet, Key: "a"})
	if !r.Found || r.Value != "1" {
		t.Fatalf("Get after Put: got %+v, want found=1", r)
	}
}

func TestAppendConcatenates(t *testing.T) {
	s := NewStore()
	r := apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpAppend, Key: "k", Value: "foo"})
	if r.Value != "foo" {
		t.Fatalf("first append: got %q want %q", r.Value, "foo")
	}
	r = apply(s, 2, Command{ClientID: 1, SeqNum: 2, Op: check.OpAppend, Key: "k", Value: "bar"})
	if r.Value != "foobar" {
		t.Fatalf("second append: got %q want %q", r.Value, "foobar")
	}
}

func TestCAS(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "old"})

	// Mismatched compare: no swap.
	r := apply(s, 2, Command{ClientID: 1, SeqNum: 2, Op: check.OpCAS, Key: "k", Value: "new", CompareValue: "wrong"})
	if r.OK {
		t.Fatalf("CAS with wrong compare should fail, got %+v", r)
	}
	if v, _ := s.Get("k"); v != "old" {
		t.Fatalf("value changed on failed CAS: %q", v)
	}

	// Matching compare: swap.
	r = apply(s, 3, Command{ClientID: 1, SeqNum: 3, Op: check.OpCAS, Key: "k", Value: "new", CompareValue: "old"})
	if !r.OK || r.Value != "new" {
		t.Fatalf("CAS with right compare should succeed, got %+v", r)
	}
	if v, _ := s.Get("k"); v != "new" {
		t.Fatalf("value not swapped on successful CAS: %q", v)
	}
}

// TestExactlyOnceDuplicate is the core exactly-once guarantee: the SAME command
// (same clientID+seqNum) applied twice takes effect ONCE, and the second apply
// returns the cached result.
func TestExactlyOnceDuplicate(t *testing.T) {
	s := NewStore()
	cmd := Command{ClientID: 1, SeqNum: 1, Op: check.OpAppend, Key: "k", Value: "x"}

	r1 := apply(s, 1, cmd)
	r2 := apply(s, 2, cmd) // retry, re-committed at a later index

	if r1.Value != "x" {
		t.Fatalf("first apply: got %q want %q", r1.Value, "x")
	}
	if r2 != r1 {
		t.Fatalf("duplicate apply returned %+v, want cached %+v", r2, r1)
	}
	if v, _ := s.Get("k"); v != "x" {
		t.Fatalf("append applied twice: value is %q, want %q", v, "x")
	}
}

// TestExactlyOnceOutOfOrderLowerSeq: a lower/stale SeqNum for a client that has
// already advanced is treated as a duplicate/no-op.
func TestExactlyOnceOutOfOrderLowerSeq(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{ClientID: 1, SeqNum: 5, Op: check.OpPut, Key: "k", Value: "current"})

	// A stale command with a lower SeqNum must NOT apply.
	r := apply(s, 2, Command{ClientID: 1, SeqNum: 3, Op: check.OpPut, Key: "k", Value: "stale"})
	if v, _ := s.Get("k"); v != "current" {
		t.Fatalf("stale lower-seq command applied: value is %q, want %q", v, "current")
	}
	// It returns the cached result of the last applied command (seq 5).
	if !r.OK {
		t.Fatalf("stale command should return cached result, got %+v", r)
	}
}

func TestNoOpAndGarbageIgnored(t *testing.T) {
	s := NewStore()
	// Leader no-op (empty command) is ignored.
	if r := s.Apply(core.CommittedEntry{Index: 1, Command: nil}); r != (Result{}) {
		t.Fatalf("no-op should yield zero Result, got %+v", r)
	}
	// Undecodable command is ignored, not fatal.
	if r := s.Apply(core.CommittedEntry{Index: 2, Command: []byte{0x00, 0x01}}); r != (Result{}) {
		t.Fatalf("garbage command should yield zero Result, got %+v", r)
	}
	if len(s.data) != 0 || len(s.sessions) != 0 {
		t.Fatalf("no-op/garbage mutated state: data=%v sessions=%v", s.data, s.sessions)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"})
	apply(s, 2, Command{ClientID: 1, SeqNum: 2, Op: check.OpPut, Key: "b", Value: "2"})
	apply(s, 3, Command{ClientID: 2, SeqNum: 10, Op: check.OpAppend, Key: "c", Value: "z"})

	snap := s.Snapshot()

	restored := NewStore()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Data survived.
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "z"}} {
		if got, _ := restored.Get(kv.k); got != kv.v {
			t.Fatalf("restored[%q] = %q, want %q", kv.k, got, kv.v)
		}
	}

	// Sessions survived: a duplicate command post-restore is still deduped.
	dup := Command{ClientID: 1, SeqNum: 2, Op: check.OpPut, Key: "b", Value: "OVERWRITE"}
	restored.Apply(core.CommittedEntry{Index: 99, Command: dup.Encode()})
	if got, _ := restored.Get("b"); got != "2" {
		t.Fatalf("duplicate applied after restore: b=%q, want %q (session did not survive)", got, "2")
	}
}

// TestSnapshotDeterministic: two stores built to the same logical state via
// different insertion orders produce byte-identical snapshots.
func TestSnapshotDeterministic(t *testing.T) {
	s1 := NewStore()
	apply(s1, 1, Command{ClientID: 2, SeqNum: 1, Op: check.OpPut, Key: "z", Value: "26"})
	apply(s1, 2, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"})
	apply(s1, 3, Command{ClientID: 1, SeqNum: 2, Op: check.OpPut, Key: "m", Value: "13"})

	s2 := NewStore()
	apply(s2, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"})
	apply(s2, 2, Command{ClientID: 1, SeqNum: 2, Op: check.OpPut, Key: "m", Value: "13"})
	apply(s2, 3, Command{ClientID: 2, SeqNum: 1, Op: check.OpPut, Key: "z", Value: "26"})

	if !bytes.Equal(s1.Snapshot(), s2.Snapshot()) {
		t.Fatal("snapshots differ despite identical logical state (non-deterministic)")
	}
}

func TestRestoreRejectsTruncated(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"})
	snap := s.Snapshot()

	fresh := NewStore()
	if err := fresh.Restore(snap[:len(snap)-1]); err == nil {
		t.Fatal("expected error restoring truncated snapshot")
	}
	// A rejected restore leaves the store untouched (still empty).
	if len(fresh.data) != 0 {
		t.Fatalf("failed restore mutated store: %v", fresh.data)
	}
}

func TestExpireBefore(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"})
	apply(s, 2, Command{ClientID: 5, SeqNum: 1, Op: check.OpPut, Key: "b", Value: "2"})

	s.ExpireBefore(5) // drop clients with id < 5

	if _, ok := s.sessions[1]; ok {
		t.Fatal("client 1 session should have been expired")
	}
	if _, ok := s.sessions[5]; !ok {
		t.Fatal("client 5 session should remain")
	}
	// After expiry, client 1 can no longer dedup — a re-sent seq 1 re-applies.
	r := apply(s, 3, Command{ClientID: 1, SeqNum: 1, Op: check.OpAppend, Key: "a", Value: "!"})
	if r.Value != "1!" {
		t.Fatalf("post-expiry re-apply: got %q want %q", r.Value, "1!")
	}
}
