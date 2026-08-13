package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/billdmar/quorum/core"
)

func entry(term core.Term, index core.Index, cmd string) core.LogEntry {
	return core.LogEntry{Term: term, Index: index, Command: []byte(cmd)}
}

func openWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func TestWALHardStateRoundTrip(t *testing.T) {
	w, dir := openWAL(t)

	if _, ok, err := w.LoadHardState(); err != nil || ok {
		t.Fatalf("fresh LoadHardState = ok %v err %v, want ok=false", ok, err)
	}

	hs := core.HardState{Term: 7, VotedFor: core.NodeID("n2")}
	if err := w.SaveHardState(hs); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the hard state must survive.
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	got, ok, err := w2.LoadHardState()
	if err != nil || !ok {
		t.Fatalf("LoadHardState after reopen: ok %v err %v", ok, err)
	}
	if got != hs {
		t.Fatalf("hard state = %+v, want %+v", got, hs)
	}
}

func TestWALAppendAndLoad(t *testing.T) {
	w, dir := openWAL(t)

	if fi, _ := w.FirstIndex(); fi != 1 {
		t.Fatalf("empty FirstIndex = %d, want 1", fi)
	}
	if li, _ := w.LastIndex(); li != 0 {
		t.Fatalf("empty LastIndex = %d, want 0", li)
	}

	ents := []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "")}
	if err := w.AppendEntries(1, ents); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if li, _ := w.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	got, err := w2.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries: %v", err)
	}
	if !entriesEqual(got, ents) {
		t.Fatalf("entries = %+v, want %+v", got, ents)
	}
}

func TestWALEmptyAppendNoOp(t *testing.T) {
	w, _ := openWAL(t)
	if err := w.AppendEntries(1, nil); err != nil {
		t.Fatalf("empty append at 1: %v", err)
	}
	if err := w.AppendEntries(1, []core.LogEntry{entry(1, 1, "x")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// Empty append one past the last entry is a durable no-op.
	if err := w.AppendEntries(2, nil); err != nil {
		t.Fatalf("empty append at lastIndex+1: %v", err)
	}
	if li, _ := w.LastIndex(); li != 1 {
		t.Fatalf("LastIndex = %d, want 1", li)
	}
}

func TestWALTruncateAndOverwrite(t *testing.T) {
	w, dir := openWAL(t)

	if err := w.AppendEntries(1, []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c"),
	}); err != nil {
		t.Fatalf("initial append: %v", err)
	}
	// Overwrite from index 2 with a different-term suffix (conflict overwrite).
	overwrite := []core.LogEntry{entry(2, 2, "B"), entry(2, 3, "C"), entry(2, 4, "D")}
	if err := w.AppendEntries(2, overwrite); err != nil {
		t.Fatalf("overwrite append: %v", err)
	}
	want := []core.LogEntry{entry(1, 1, "a"), entry(2, 2, "B"), entry(2, 3, "C"), entry(2, 4, "D")}

	got, _ := w.LoadEntries()
	if !entriesEqual(got, want) {
		t.Fatalf("after overwrite = %+v, want %+v", got, want)
	}

	// Survives reopen: the truncation must be durable, not just in-memory.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	got, _ = w2.LoadEntries()
	if !entriesEqual(got, want) {
		t.Fatalf("after reopen = %+v, want %+v", got, want)
	}
}

func TestWALRejectsGapAndCompactedAppend(t *testing.T) {
	w, _ := openWAL(t)
	if err := w.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// fromIndex 3 leaves a gap (lastIndex is 1).
	if err := w.AppendEntries(3, []core.LogEntry{entry(1, 3, "c")}); err == nil {
		t.Fatal("expected gap rejection, got nil")
	}
}

func TestWALTornTailRecovery(t *testing.T) {
	w, dir := openWAL(t)
	ents := []core.LogEntry{entry(1, 1, "alpha"), entry(1, 2, "beta"), entry(1, 3, "gamma")}
	if err := w.AppendEntries(1, ents); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the tail: append garbage bytes that look like the start of a
	// record but are truncated — a classic torn write.
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	// A header claiming a 40-byte payload, but we write only 5 bytes after it.
	var hdr [logRecordHeader]byte
	binary.BigEndian.PutUint32(hdr[0:4], 40)
	binary.BigEndian.PutUint32(hdr[4:8], 0xdeadbeef)
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatalf("write torn hdr: %v", err)
	}
	if _, err := f.Write([]byte("short")); err != nil {
		t.Fatalf("write torn body: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close corrupted: %v", err)
	}

	// Recovery must return the valid 3-entry prefix and discard the torn tail.
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	got, _ := w2.LoadEntries()
	if !entriesEqual(got, ents) {
		t.Fatalf("recovered = %+v, want valid prefix %+v", got, ents)
	}
	// And the file must have been repaired so a fresh append lands cleanly.
	if err := w2.AppendEntries(4, []core.LogEntry{entry(1, 4, "delta")}); err != nil {
		t.Fatalf("append after repair: %v", err)
	}
	got, _ = w2.LoadEntries()
	if len(got) != 4 || string(got[3].Command) != "delta" {
		t.Fatalf("post-repair append = %+v", got)
	}
}

func TestWALTornTailFlippedByte(t *testing.T) {
	// A checksum mismatch (flipped byte) in the last record must be discarded.
	w, dir := openWAL(t)
	ents := []core.LogEntry{entry(1, 1, "one"), entry(1, 2, "two")}
	if err := w.AppendEntries(1, ents); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	logPath := filepath.Join(dir, logFileName)
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip the final payload byte.
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(logPath, raw, 0o644); err != nil {
		t.Fatalf("write flipped: %v", err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	got, _ := w2.LoadEntries()
	want := ents[:1] // only the first record is intact
	if !entriesEqual(got, want) {
		t.Fatalf("recovered = %+v, want %+v", got, want)
	}
}

func TestWALSnapshotSaveLoadAndCompaction(t *testing.T) {
	w, dir := openWAL(t)
	ents := []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(2, 3, "c"), entry(2, 4, "d"),
	}
	if err := w.AppendEntries(1, ents); err != nil {
		t.Fatalf("append: %v", err)
	}

	snap := []byte("app-state-v1")
	if err := w.SaveSnapshot(2, 1, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// FirstIndex advances past compacted entries; LastIndex unchanged.
	if fi, _ := w.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex after snapshot = %d, want 3", fi)
	}
	if li, _ := w.LastIndex(); li != 4 {
		t.Fatalf("LastIndex after snapshot = %d, want 4", li)
	}
	got, _ := w.LoadEntries()
	want := []core.LogEntry{entry(2, 3, "c"), entry(2, 4, "d")}
	if !entriesEqual(got, want) {
		t.Fatalf("post-compaction entries = %+v, want %+v", got, want)
	}

	// Snapshot + compacted log both survive a reopen.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()

	si, st, data, ok, err := w2.LoadSnapshot()
	if err != nil || !ok {
		t.Fatalf("LoadSnapshot: ok %v err %v", ok, err)
	}
	if si != 2 || st != 1 || !bytes.Equal(data, snap) {
		t.Fatalf("snapshot = (%d,%d,%q), want (2,1,%q)", si, st, data, snap)
	}
	if fi, _ := w2.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex after reopen = %d, want 3", fi)
	}
	got, _ = w2.LoadEntries()
	if !entriesEqual(got, want) {
		t.Fatalf("post-reopen entries = %+v, want %+v", got, want)
	}

	// Appending on top of the compacted log continues correctly.
	if err := w2.AppendEntries(5, []core.LogEntry{entry(2, 5, "e")}); err != nil {
		t.Fatalf("append after snapshot: %v", err)
	}
	got, _ = w2.LoadEntries()
	if len(got) != 3 || got[2].Index != 5 {
		t.Fatalf("append after snapshot produced %+v", got)
	}
}

func TestWALSnapshotOverwritesPrevious(t *testing.T) {
	w, _ := openWAL(t)
	if err := w.AppendEntries(1, []core.LogEntry{
		entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.SaveSnapshot(1, 1, []byte("s1")); err != nil {
		t.Fatalf("snap1: %v", err)
	}
	if err := w.SaveSnapshot(2, 1, []byte("s2")); err != nil {
		t.Fatalf("snap2: %v", err)
	}
	si, _, data, ok, err := w.LoadSnapshot()
	if err != nil || !ok || si != 2 || string(data) != "s2" {
		t.Fatalf("latest snapshot = (%d,%q) ok %v err %v, want (2,\"s2\")", si, data, ok, err)
	}
	if fi, _ := w.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex = %d, want 3", fi)
	}
}

// TestWALHardStateCorruptionDetected verifies a torn hard-state record is
// reported rather than silently returning a bogus term/vote.
func TestWALHardStateCorruptionDetected(t *testing.T) {
	w, dir := openWAL(t)
	if err := w.SaveHardState(core.HardState{Term: 3, VotedFor: "n1"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	hsPath := filepath.Join(dir, hardStateFileName)
	raw, err := os.ReadFile(hsPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw[len(raw)-1] ^= 0xff // corrupt the payload; CRC no longer matches
	if err := os.WriteFile(hsPath, raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Open loads snapshot meta only (not hard state), so it still succeeds.
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	if _, _, err := w2.LoadHardState(); err == nil {
		t.Fatal("expected checksum-mismatch error, got nil")
	}
}

// TestWALFramingCRC sanity-checks the record framing helpers directly.
func TestWALFramingCRC(t *testing.T) {
	e := entry(9, 42, "payload")
	rec := encodeRecord(encodeEntry(e))
	plen := binary.BigEndian.Uint32(rec[0:4])
	crc := binary.BigEndian.Uint32(rec[4:8])
	payload := rec[logRecordHeader:]
	if int(plen) != len(payload) {
		t.Fatalf("framed length %d != payload %d", plen, len(payload))
	}
	if crc32.ChecksumIEEE(payload) != crc {
		t.Fatal("framed CRC mismatch")
	}
	got, ok := decodeEntry(payload)
	if !ok || got.Term != e.Term || got.Index != e.Index || string(got.Command) != "payload" {
		t.Fatalf("decodeEntry = %+v ok %v", got, ok)
	}
}

func entriesEqual(a, b []core.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || a[i].Index != b[i].Index || !bytes.Equal(a[i].Command, b[i].Command) {
			return false
		}
	}
	return true
}
