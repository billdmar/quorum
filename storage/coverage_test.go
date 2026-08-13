package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/billdmar/quorum/core"
)

func TestFaultKindString(t *testing.T) {
	cases := map[FaultKind]string{
		FaultNone:             "none",
		FaultCrashBeforeFsync: "crash-before-fsync",
		FaultCrashAfterFsync:  "crash-after-fsync",
		FaultTornWrite:        "torn-write",
		FaultKind(99):         "unknown",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("FaultKind(%d).String() = %q, want %q", k, got, want)
		}
	}
}

func TestWriteOpString(t *testing.T) {
	cases := map[WriteOp]string{
		OpHardState: "hardstate",
		OpAppend:    "append",
		OpSnapshot:  "snapshot",
		WriteOp(99): "unknown",
	}
	for o, want := range cases {
		if got := o.String(); got != want {
			t.Errorf("WriteOp(%d).String() = %q, want %q", o, got, want)
		}
	}
}

func TestMemCloseNoop(t *testing.T) {
	if err := NewMem().Close(); err != nil {
		t.Fatalf("mem Close: %v", err)
	}
}

func TestMemEmptySnapshotAndBelowFirstIndex(t *testing.T) {
	m := NewMem()
	if _, _, _, ok, err := m.LoadSnapshot(); ok || err != nil {
		t.Fatalf("fresh LoadSnapshot ok %v err %v", ok, err)
	}
	if err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.SaveSnapshot(1, 1, nil); err != nil {
		t.Fatalf("snap: %v", err)
	}
	// Appending below firstIndex (compacted range) is rejected.
	if err := m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err == nil {
		t.Fatal("expected below-firstIndex rejection")
	}
}

func TestMemSnapshotFaults(t *testing.T) {
	// Before-fsync: snapshot not applied.
	m := NewMem()
	_ = m.AppendEntries(1, []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b")})
	m.Fault = func(op WriteOp, _ core.Index) FaultKind {
		if op == OpSnapshot {
			return FaultCrashBeforeFsync
		}
		return FaultNone
	}
	if err := m.SaveSnapshot(1, 1, []byte("x")); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("snap before-fsync err = %v", err)
	}
	m.Fault = nil
	if _, _, _, ok, _ := m.LoadSnapshot(); ok {
		t.Fatal("before-fsync left a snapshot")
	}
	if fi, _ := m.FirstIndex(); fi != 1 {
		t.Fatalf("FirstIndex advanced despite before-fsync crash: %d", fi)
	}

	// After-fsync: snapshot applied then crash reported.
	m.Fault = func(op WriteOp, _ core.Index) FaultKind {
		if op == OpSnapshot {
			return FaultCrashAfterFsync
		}
		return FaultNone
	}
	if err := m.SaveSnapshot(1, 1, []byte("x")); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("snap after-fsync err = %v", err)
	}
	m.Fault = nil
	if _, _, _, ok, _ := m.LoadSnapshot(); !ok {
		t.Fatal("after-fsync did not persist snapshot")
	}
	if fi, _ := m.FirstIndex(); fi != 2 {
		t.Fatalf("FirstIndex = %d after snapshot, want 2", fi)
	}
}

func TestWALEmptySnapshotLoad(t *testing.T) {
	w, _ := openWAL(t)
	if _, _, _, ok, err := w.LoadSnapshot(); ok || err != nil {
		t.Fatalf("fresh LoadSnapshot ok %v err %v", ok, err)
	}
}

func TestWALSnapshotTooShortAndCorrupt(t *testing.T) {
	w, dir := openWAL(t)
	if err := w.SaveSnapshot(0, 0, []byte("data")); err != nil {
		t.Fatalf("snap: %v", err)
	}
	path := filepath.Join(dir, snapshotFileName)

	// Corrupt the payload -> checksum mismatch.
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, _, _, err := w.LoadSnapshot(); err == nil {
		t.Fatal("expected snapshot checksum error")
	}

	// Truncate below the 4-byte CRC header -> too short.
	if err := os.WriteFile(path, []byte{0x01}, 0o644); err != nil {
		t.Fatalf("write short: %v", err)
	}
	if _, _, _, _, err := w.LoadSnapshot(); err == nil {
		t.Fatal("expected snapshot too-short error")
	}
}

func TestWALHardStateTooShort(t *testing.T) {
	w, dir := openWAL(t)
	if err := w.SaveHardState(core.HardState{Term: 1}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, hardStateFileName), []byte{0x01, 0x02}, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := w.LoadHardState(); err == nil {
		t.Fatal("expected hardstate too-short error")
	}
}

func TestWALAppendBelowFirstIndex(t *testing.T) {
	w, _ := openWAL(t)
	_ = w.AppendEntries(1, []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b")})
	if err := w.SaveSnapshot(1, 1, nil); err != nil {
		t.Fatalf("snap: %v", err)
	}
	if err := w.AppendEntries(1, []core.LogEntry{entry(1, 1, "a")}); err == nil {
		t.Fatal("expected below-firstIndex rejection")
	}
}

func TestOpenErrors(t *testing.T) {
	// mkdir fails when a plain file sits where the dir path points.
	file := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := Open(filepath.Join(file, "sub")); err == nil {
		t.Fatal("expected Open to fail under a non-directory path")
	}
}

func TestWriteAtomicErrorOnReadOnlyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil { // r-x: cannot create temp file
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if err := writeAtomic(dir, "f", []byte("data")); err == nil {
		t.Fatal("expected writeAtomic to fail creating temp in read-only dir")
	}
}

func TestSyncDirMissing(t *testing.T) {
	if err := syncDir(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("expected syncDir to fail on a missing directory")
	}
}

func TestOpenReloadsSnapshotBase(t *testing.T) {
	// Reopening a WAL whose snapshot base is >0 must restore firstIndex from it
	// (exercises Open's snapshot-meta branch and the compacted log reopen path).
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.AppendEntries(1, []core.LogEntry{entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.SaveSnapshot(2, 1, []byte("s")); err != nil {
		t.Fatalf("snap: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	if fi, _ := w2.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex after reopen = %d, want 3", fi)
	}
	got, _ := w2.LoadEntries()
	if len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("entries after reopen = %+v", got)
	}
}

func TestDecodeMalformed(t *testing.T) {
	// Short buffers and length-mismatch buffers must be rejected, not panic.
	if _, ok := decodeEntry([]byte{0x00}); ok {
		t.Error("decodeEntry accepted a short buffer")
	}
	bad := make([]byte, entryPayloadMin+3)
	bad[19] = 0xff // claims a huge command length
	if _, ok := decodeEntry(bad); ok {
		t.Error("decodeEntry accepted a length-mismatched buffer")
	}
	if _, ok := decodeHardState([]byte{0x00}); ok {
		t.Error("decodeHardState accepted a short buffer")
	}
	hsBad := make([]byte, 12)
	hsBad[11] = 0xff // claims a vote length that isn't present
	if _, ok := decodeHardState(hsBad); ok {
		t.Error("decodeHardState accepted a length-mismatched buffer")
	}
	if _, _, _, ok := decodeSnapshot([]byte{0x00}); ok {
		t.Error("decodeSnapshot accepted a short buffer")
	}
	snBad := make([]byte, 20)
	snBad[19] = 0xff
	if _, _, _, ok := decodeSnapshot(snBad); ok {
		t.Error("decodeSnapshot accepted a length-mismatched buffer")
	}
}
