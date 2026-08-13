package storage

// wal.go implements the Storage interface backed by real files with fsync
// discipline. It keeps three artifacts in a directory:
//
//   - hardstate : the durable term+vote, written atomically (temp+rename) so it
//     is always either the old-valid or the new-valid record, never torn.
//   - wal.log   : an append-only sequence of length-prefixed, CRC32-checksummed
//     log-entry records. A crash mid-write leaves a torn trailing record, which
//     recovery detects (bad length or bad checksum) and discards, returning the
//     longest valid prefix.
//   - snapshot  : the most recent snapshot, written atomically. Saving a
//     snapshot compacts the wal.log prefix it supersedes.
//
// Determinism: nothing here reads a clock or randomness. Faults are not injected
// into the WAL itself — the WAL performs honest fsyncs; the crash-recovery
// harness exercises it by killing the process and reopening. The in-memory
// store (mem.go) carries the caller-driven fault hooks.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/billdmar/quorum/core"
)

const (
	logFileName       = "wal.log"
	hardStateFileName = "hardstate"
	snapshotFileName  = "snapshot"

	// logRecordHeader is the fixed per-record prefix in wal.log: a 4-byte
	// big-endian payload length followed by a 4-byte CRC32 of the payload.
	logRecordHeader = 8
	// entryPayloadMin is the smallest valid entry payload: Term(8)+Index(8)+
	// CommandLen(4), with a zero-length command.
	entryPayloadMin = 20
)

// WAL is a file-backed Storage implementation. It keeps an in-memory mirror of
// the durable log (entries plus each record's byte offset) so truncation and
// the log-bound accessors are O(1)/O(log n) and never re-read the file. The
// mirror is always exactly what is fsync'd to disk: no partial record is ever
// admitted, so a reopen reproduces the same state.
type WAL struct {
	dir     string
	logFile *os.File // wal.log, open O_RDWR for append (WriteAt) and truncate

	entries []core.LogEntry // durable log entries, index order, post-snapshot
	offsets []int64         // offsets[i] = byte offset of entries[i]'s record
	tail    int64           // byte offset one past the last record (append point)

	snapIndex core.Index // lastIncludedIndex of the active snapshot; 0 if none
	snapTerm  core.Term  // term of snapIndex; 0 if none
}

// Compile-time assertion that WAL satisfies the frozen contract.
var _ Storage = (*WAL)(nil)

// Open opens (creating if absent) a WAL rooted at dir and recovers durable
// state: it loads any snapshot to establish the log's base index, then scans
// wal.log, discarding a torn trailing record and truncating the file to the
// last valid boundary so subsequent appends stay clean.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir %q: %w", dir, err)
	}
	w := &WAL{dir: dir}

	if idx, term, ok, err := w.loadSnapshotMeta(); err != nil {
		return nil, err
	} else if ok {
		w.snapIndex, w.snapTerm = idx, term
	}

	logPath := filepath.Join(dir, logFileName)
	raw, err := os.ReadFile(logPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("storage: read %q: %w", logPath, err)
	}
	entries, offsets, validEnd := parseLog(raw)
	w.entries, w.offsets, w.tail = entries, offsets, validEnd

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %q: %w", logPath, err)
	}
	w.logFile = f
	// Repair a torn tail: truncate the file to the last valid record boundary
	// and make that durable before we start appending past it.
	if validEnd < int64(len(raw)) {
		if err := f.Truncate(validEnd); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("storage: repair-truncate %q: %w", logPath, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("storage: sync repair %q: %w", logPath, err)
		}
	}
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

// firstIndex is the lowest index the WAL can serve: one past the snapshot base.
func (w *WAL) firstIndex() core.Index { return w.snapIndex + 1 }

// lastIndex is the highest durable index (snapshot base when the tail is empty).
func (w *WAL) lastIndex() core.Index { return w.snapIndex + core.Index(len(w.entries)) }

// SaveHardState atomically persists term+vote and fsyncs before returning.
func (w *WAL) SaveHardState(hs core.HardState) error {
	payload := encodeHardState(hs)
	rec := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], crc32.ChecksumIEEE(payload))
	copy(rec[4:], payload)
	return writeAtomic(w.dir, hardStateFileName, rec)
}

// LoadHardState returns the last persisted HardState, or ok=false on a fresh
// node. A present-but-corrupt record is a hard error (atomic writes make it
// impossible under normal operation).
func (w *WAL) LoadHardState() (core.HardState, bool, error) {
	raw, err := os.ReadFile(filepath.Join(w.dir, hardStateFileName))
	if errors.Is(err, os.ErrNotExist) {
		return core.HardState{}, false, nil
	}
	if err != nil {
		return core.HardState{}, false, fmt.Errorf("storage: read hardstate: %w", err)
	}
	if len(raw) < 4 {
		return core.HardState{}, false, errors.New("storage: hardstate too short")
	}
	crc := binary.BigEndian.Uint32(raw[0:4])
	payload := raw[4:]
	if crc32.ChecksumIEEE(payload) != crc {
		return core.HardState{}, false, errors.New("storage: hardstate checksum mismatch")
	}
	hs, ok := decodeHardState(payload)
	if !ok {
		return core.HardState{}, false, errors.New("storage: hardstate malformed")
	}
	return hs, true, nil
}

// AppendEntries truncates any existing entries at index >= fromIndex, then
// durably appends entries (which begin at fromIndex). Both the truncation and
// the append are fsync'd. An empty append at lastIndex+1 is a durable no-op.
func (w *WAL) AppendEntries(fromIndex core.Index, entries []core.LogEntry) error {
	if fromIndex < w.firstIndex() {
		return fmt.Errorf("storage: fromIndex %d below firstIndex %d", fromIndex, w.firstIndex())
	}
	if fromIndex > w.lastIndex()+1 {
		return fmt.Errorf("storage: fromIndex %d leaves a gap after lastIndex %d", fromIndex, w.lastIndex())
	}

	truncated := false
	if fromIndex <= w.lastIndex() {
		pos := int(fromIndex - w.firstIndex())
		cut := w.offsets[pos]
		if err := w.logFile.Truncate(cut); err != nil {
			return fmt.Errorf("storage: truncate log: %w", err)
		}
		w.entries = w.entries[:pos]
		w.offsets = w.offsets[:pos]
		w.tail = cut
		truncated = true
	}

	for _, e := range entries {
		rec := encodeRecord(encodeEntry(e))
		if _, err := w.logFile.WriteAt(rec, w.tail); err != nil {
			return fmt.Errorf("storage: write entry %d: %w", e.Index, err)
		}
		w.entries = append(w.entries, e)
		w.offsets = append(w.offsets, w.tail)
		w.tail += int64(len(rec))
	}

	// fsync only when something actually changed; a pure no-op stays cheap.
	if truncated || len(entries) > 0 {
		if err := w.logFile.Sync(); err != nil {
			return fmt.Errorf("storage: fsync log: %w", err)
		}
	}
	return nil
}

// LoadEntries returns a copy of the durable log entries in index order.
func (w *WAL) LoadEntries() ([]core.LogEntry, error) {
	out := make([]core.LogEntry, len(w.entries))
	copy(out, w.entries)
	return out, nil
}

// FirstIndex reports the lowest servable index (snapshot base + 1).
func (w *WAL) FirstIndex() (core.Index, error) { return w.firstIndex(), nil }

// LastIndex reports the highest durable index (snapshot base when empty).
func (w *WAL) LastIndex() (core.Index, error) { return w.lastIndex(), nil }

// SaveSnapshot atomically writes a snapshot covering the log through
// lastIncludedIndex/Term, fsyncs it, then compacts the wal.log prefix it
// supersedes (rewriting the file with only entries above lastIncludedIndex).
func (w *WAL) SaveSnapshot(lastIncludedIndex core.Index, lastIncludedTerm core.Term, data []byte) error {
	payload := encodeSnapshot(lastIncludedIndex, lastIncludedTerm, data)
	rec := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], crc32.ChecksumIEEE(payload))
	copy(rec[4:], payload)
	if err := writeAtomic(w.dir, snapshotFileName, rec); err != nil {
		return err
	}
	return w.compact(lastIncludedIndex, lastIncludedTerm)
}

// compact rewrites wal.log with only entries above lastIncludedIndex and
// advances the snapshot base. It rebuilds the file atomically (temp+rename) so
// a crash mid-compaction cannot corrupt the live log.
func (w *WAL) compact(lastIncludedIndex core.Index, lastIncludedTerm core.Term) error {
	keepFrom := lastIncludedIndex + 1
	var kept []core.LogEntry
	if keepFrom <= w.lastIndex() && keepFrom >= w.firstIndex() {
		pos := int(keepFrom - w.firstIndex())
		kept = append(kept, w.entries[pos:]...)
	}

	var buf []byte
	for _, e := range kept {
		buf = append(buf, encodeRecord(encodeEntry(e))...)
	}
	if err := writeAtomic(w.dir, logFileName, buf); err != nil {
		return err
	}

	// Reopen the freshly-rewritten file for continued appends and rebuild the
	// in-memory mirror to match.
	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("storage: close log for compaction: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(w.dir, logFileName), os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("storage: reopen compacted log: %w", err)
	}
	w.logFile = f
	w.snapIndex, w.snapTerm = lastIncludedIndex, lastIncludedTerm
	w.entries = w.entries[:0]
	w.offsets = w.offsets[:0]
	w.tail = 0
	for _, e := range kept {
		rec := encodeRecord(encodeEntry(e))
		w.entries = append(w.entries, e)
		w.offsets = append(w.offsets, w.tail)
		w.tail += int64(len(rec))
	}
	return nil
}

// LoadSnapshot returns the most recent persisted snapshot, if any.
func (w *WAL) LoadSnapshot() (core.Index, core.Term, []byte, bool, error) {
	raw, err := os.ReadFile(filepath.Join(w.dir, snapshotFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, false, nil
	}
	if err != nil {
		return 0, 0, nil, false, fmt.Errorf("storage: read snapshot: %w", err)
	}
	if len(raw) < 4 {
		return 0, 0, nil, false, errors.New("storage: snapshot too short")
	}
	crc := binary.BigEndian.Uint32(raw[0:4])
	payload := raw[4:]
	if crc32.ChecksumIEEE(payload) != crc {
		return 0, 0, nil, false, errors.New("storage: snapshot checksum mismatch")
	}
	idx, term, data, ok := decodeSnapshot(payload)
	if !ok {
		return 0, 0, nil, false, errors.New("storage: snapshot malformed")
	}
	return idx, term, data, true, nil
}

// loadSnapshotMeta reads only the snapshot's index/term (used at Open to
// establish the log base without materializing the data slice for the caller).
func (w *WAL) loadSnapshotMeta() (core.Index, core.Term, bool, error) {
	idx, term, _, ok, err := w.LoadSnapshot()
	return idx, term, ok, err
}

// Close releases the open log file. After Close the WAL must not be used.
func (w *WAL) Close() error {
	if w.logFile == nil {
		return nil
	}
	err := w.logFile.Close()
	w.logFile = nil
	return err
}

// --- framing / encoding ---------------------------------------------------

// encodeRecord frames a payload as [len:uint32][crc:uint32][payload].
func encodeRecord(payload []byte) []byte {
	rec := make([]byte, logRecordHeader+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload))
	copy(rec[logRecordHeader:], payload)
	return rec
}

// encodeEntry serializes a LogEntry payload: Term, Index, CommandLen, Command.
func encodeEntry(e core.LogEntry) []byte {
	buf := make([]byte, entryPayloadMin+len(e.Command))
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.Index))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(e.Command)))
	copy(buf[entryPayloadMin:], e.Command)
	return buf
}

// decodeEntry parses an entry payload; ok=false if the command length does not
// match the buffer (a sign of corruption).
func decodeEntry(payload []byte) (core.LogEntry, bool) {
	if len(payload) < entryPayloadMin {
		return core.LogEntry{}, false
	}
	term := core.Term(binary.BigEndian.Uint64(payload[0:8]))
	index := core.Index(binary.BigEndian.Uint64(payload[8:16]))
	cmdLen := int(binary.BigEndian.Uint32(payload[16:20]))
	if entryPayloadMin+cmdLen != len(payload) {
		return core.LogEntry{}, false
	}
	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
		copy(cmd, payload[entryPayloadMin:])
	}
	return core.LogEntry{Term: term, Index: index, Command: cmd}, true
}

// parseLog walks a wal.log byte image, returning the valid entries, their
// record offsets, and the byte offset just past the last valid record. Any
// trailing bytes beyond validEnd are a torn/corrupt tail to be discarded.
func parseLog(raw []byte) (entries []core.LogEntry, offsets []int64, validEnd int64) {
	off := 0
	for off+logRecordHeader <= len(raw) {
		plen := int(binary.BigEndian.Uint32(raw[off : off+4]))
		crc := binary.BigEndian.Uint32(raw[off+4 : off+8])
		payStart := off + logRecordHeader
		payEnd := payStart + plen
		if plen < entryPayloadMin || payEnd > len(raw) {
			break // torn: header promises more than is present
		}
		payload := raw[payStart:payEnd]
		if crc32.ChecksumIEEE(payload) != crc {
			break // torn/corrupt payload
		}
		e, ok := decodeEntry(payload)
		if !ok {
			break
		}
		entries = append(entries, e)
		offsets = append(offsets, int64(off))
		off = payEnd
	}
	return entries, offsets, int64(off)
}

// encodeHardState serializes Term, VotedForLen, VotedFor.
func encodeHardState(hs core.HardState) []byte {
	vote := []byte(hs.VotedFor)
	buf := make([]byte, 12+len(vote))
	binary.BigEndian.PutUint64(buf[0:8], uint64(hs.Term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(vote)))
	copy(buf[12:], vote)
	return buf
}

func decodeHardState(payload []byte) (core.HardState, bool) {
	if len(payload) < 12 {
		return core.HardState{}, false
	}
	term := core.Term(binary.BigEndian.Uint64(payload[0:8]))
	voteLen := int(binary.BigEndian.Uint32(payload[8:12]))
	if 12+voteLen != len(payload) {
		return core.HardState{}, false
	}
	return core.HardState{Term: term, VotedFor: core.NodeID(payload[12:])}, true
}

// encodeSnapshot serializes LastIncludedIndex, LastIncludedTerm, DataLen, Data.
func encodeSnapshot(idx core.Index, term core.Term, data []byte) []byte {
	buf := make([]byte, 20+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uint64(idx))
	binary.BigEndian.PutUint64(buf[8:16], uint64(term))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(data)))
	copy(buf[20:], data)
	return buf
}

func decodeSnapshot(payload []byte) (core.Index, core.Term, []byte, bool) {
	if len(payload) < 20 {
		return 0, 0, nil, false
	}
	idx := core.Index(binary.BigEndian.Uint64(payload[0:8]))
	term := core.Term(binary.BigEndian.Uint64(payload[8:16]))
	dataLen := int(binary.BigEndian.Uint32(payload[16:20]))
	if 20+dataLen != len(payload) {
		return 0, 0, nil, false
	}
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		copy(data, payload[20:])
	}
	return idx, term, data, true
}

// --- durable file helpers -------------------------------------------------

// writeAtomic writes data to name within dir via a temp file that is fsync'd,
// renamed into place, after which the directory is fsync'd so the rename is
// durable. The result is all-or-nothing: readers see either the old file or the
// new one, never a torn mix.
func writeAtomic(dir, name string, data []byte) (err error) {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: create temp %q: %w", tmp, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("storage: write temp %q: %w", tmp, err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync temp %q: %w", tmp, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("storage: close temp %q: %w", tmp, err)
	}
	if err = os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("storage: rename temp %q: %w", tmp, err)
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so that a create/rename within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open dir %q: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("storage: fsync dir %q: %w", dir, err)
	}
	return d.Close()
}
