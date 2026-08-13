package storage

// mem.go implements the Storage interface entirely in memory. It exists for two
// jobs: fast unit tests that need no real disk, and modeling a node in the
// deterministic simulator without touching a filesystem. It honors the exact
// truncate/append/snapshot semantics of the WAL so the two are interchangeable
// drivers of the same core.
//
// FAULT INJECTION (the reason mem, not WAL, carries the hooks): the crash-
// recovery harness needs to simulate fsync-boundary faults deterministically.
// mem does that via a caller-supplied FaultHook — the CALLER decides when a
// fault fires by returning a FaultKind from the hook. mem itself reads no clock
// and rolls no dice; the (op, index) arguments let the caller drive faults off
// the deterministic event trace. Semantics per FaultKind at a durable-write
// boundary:
//
//   - FaultNone           : write succeeds and is durable.
//   - FaultCrashAfterFsync : write is applied to durable state, THEN the call
//     returns ErrInjectedCrash — the data survives (as if the process died just
//     after fsync).
//   - FaultCrashBeforeFsync : write is NOT applied to durable state and the call
//     returns ErrInjectedCrash — the data is cleanly absent (as if the process
//     died before fsync; a real disk might keep it, but the recovery contract
//     only requires "all or nothing", and absent is a legal outcome).
//   - FaultTornWrite       : for an entry append, all entries before the torn
//     one are applied durably and the torn entry (and everything after) is
//     dropped, then the call returns ErrInjectedCrash — modeling the longest
//     valid prefix a WAL reopen would recover after a torn record.

import (
	"errors"
	"fmt"

	"github.com/billdmar/quorum/core"
)

// ErrInjectedCrash is returned by a Storage call whose durable-write boundary a
// FaultHook chose to fault. It is a sentinel the crash-recovery harness matches
// with errors.Is to distinguish an injected crash from a genuine failure.
var ErrInjectedCrash = errors.New("storage: injected crash at fsync boundary")

// WriteOp identifies the durable-write boundary a FaultHook is being consulted
// about, so the caller can target faults precisely (e.g. "torn write on the
// 3rd log append of this run").
type WriteOp uint8

const (
	// OpHardState: a SaveHardState call.
	OpHardState WriteOp = iota
	// OpAppend: an AppendEntries call (Index is the entry currently being
	// written; the hook is consulted once per entry so a torn write can target a
	// specific entry).
	OpAppend
	// OpSnapshot: a SaveSnapshot call.
	OpSnapshot
)

func (o WriteOp) String() string {
	switch o {
	case OpHardState:
		return "hardstate"
	case OpAppend:
		return "append"
	case OpSnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// FaultHook is consulted at each durable-write boundary. It returns the fault
// to inject for that boundary (FaultNone to proceed normally). op names the
// boundary and index is the log index in play (0 for non-log ops). The hook is
// a pure function of its inputs and whatever deterministic state the CALLER
// holds — MemStorage never calls a clock or RNG to decide a fault.
type FaultHook func(op WriteOp, index core.Index) FaultKind

// MemStorage is an in-memory Storage implementation with caller-driven fault
// injection. It is not safe for concurrent use; drivers step it from a single
// goroutine, matching the pure core's single-threaded contract.
type MemStorage struct {
	hs    core.HardState
	hasHS bool

	entries   []core.LogEntry // durable log, index order, post-snapshot
	snapIndex core.Index      // lastIncludedIndex of active snapshot; 0 if none
	snapTerm  core.Term       // term of snapIndex; 0 if none

	snapData []byte
	hasSnap  bool

	// Fault, if non-nil, is consulted at every durable-write boundary. The
	// caller sets/clears it to drive deterministic fsync-boundary faults.
	Fault FaultHook
}

// Compile-time assertion that MemStorage satisfies the frozen contract.
var _ Storage = (*MemStorage)(nil)

// NewMem returns an empty in-memory store with no fault hook installed.
func NewMem() *MemStorage { return &MemStorage{} }

// fault consults the hook (if any) for the given boundary.
func (m *MemStorage) fault(op WriteOp, index core.Index) FaultKind {
	if m.Fault == nil {
		return FaultNone
	}
	return m.Fault(op, index)
}

func (m *MemStorage) firstIndex() core.Index { return m.snapIndex + 1 }
func (m *MemStorage) lastIndex() core.Index  { return m.snapIndex + core.Index(len(m.entries)) }

// SaveHardState persists term+vote in memory, subject to fault injection.
func (m *MemStorage) SaveHardState(hs core.HardState) error {
	switch m.fault(OpHardState, 0) {
	case FaultCrashBeforeFsync:
		return ErrInjectedCrash // not applied
	case FaultCrashAfterFsync:
		m.hs, m.hasHS = hs, true
		return ErrInjectedCrash // applied, then "crash"
	default:
		m.hs, m.hasHS = hs, true
		return nil
	}
}

// LoadHardState returns the last persisted HardState, or ok=false when none.
func (m *MemStorage) LoadHardState() (core.HardState, bool, error) {
	return m.hs, m.hasHS, nil
}

// AppendEntries truncates at index >= fromIndex then appends, subject to fault
// injection consulted per entry (so FaultTornWrite can target a specific one).
func (m *MemStorage) AppendEntries(fromIndex core.Index, entries []core.LogEntry) error {
	if fromIndex < m.firstIndex() {
		return fmt.Errorf("storage: fromIndex %d below firstIndex %d", fromIndex, m.firstIndex())
	}
	if fromIndex > m.lastIndex()+1 {
		return fmt.Errorf("storage: fromIndex %d leaves a gap after lastIndex %d", fromIndex, m.lastIndex())
	}

	// Truncation is part of the same durable operation; a pre-fsync crash
	// leaves the log untouched. Work on a copy so a fault can cleanly discard.
	pos := int(fromIndex - m.firstIndex())
	next := make([]core.LogEntry, pos)
	copy(next, m.entries[:pos])

	for _, e := range entries {
		switch m.fault(OpAppend, e.Index) {
		case FaultCrashBeforeFsync:
			return ErrInjectedCrash // nothing applied
		case FaultTornWrite:
			// Entries before the torn one are durable; this one and the rest
			// are dropped — the longest valid prefix a WAL reopen would recover.
			m.entries = next
			return ErrInjectedCrash
		case FaultCrashAfterFsync:
			next = append(next, e)
			m.entries = next
			return ErrInjectedCrash // fully applied, then "crash"
		default:
			next = append(next, e)
		}
	}
	m.entries = next
	return nil
}

// LoadEntries returns a copy of the durable log entries in index order.
func (m *MemStorage) LoadEntries() ([]core.LogEntry, error) {
	out := make([]core.LogEntry, len(m.entries))
	copy(out, m.entries)
	return out, nil
}

// FirstIndex reports the lowest servable index (snapshot base + 1).
func (m *MemStorage) FirstIndex() (core.Index, error) { return m.firstIndex(), nil }

// LastIndex reports the highest durable index (snapshot base when empty).
func (m *MemStorage) LastIndex() (core.Index, error) { return m.lastIndex(), nil }

// SaveSnapshot stores a snapshot and compacts the log prefix it supersedes,
// subject to fault injection.
func (m *MemStorage) SaveSnapshot(lastIncludedIndex core.Index, lastIncludedTerm core.Term, data []byte) error {
	fk := m.fault(OpSnapshot, lastIncludedIndex)
	if fk == FaultCrashBeforeFsync {
		return ErrInjectedCrash // nothing applied
	}

	keepFrom := lastIncludedIndex + 1
	var kept []core.LogEntry
	if keepFrom <= m.lastIndex() && keepFrom >= m.firstIndex() {
		pos := int(keepFrom - m.firstIndex())
		kept = append(kept, m.entries[pos:]...)
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	m.entries = kept
	m.snapIndex, m.snapTerm = lastIncludedIndex, lastIncludedTerm
	m.snapData, m.hasSnap = dataCopy, true

	if fk == FaultCrashAfterFsync {
		return ErrInjectedCrash // applied, then "crash"
	}
	return nil
}

// LoadSnapshot returns the most recent persisted snapshot, if any.
func (m *MemStorage) LoadSnapshot() (core.Index, core.Term, []byte, bool, error) {
	if !m.hasSnap {
		return 0, 0, nil, false, nil
	}
	out := make([]byte, len(m.snapData))
	copy(out, m.snapData)
	return m.snapIndex, m.snapTerm, out, true, nil
}

// Close is a no-op for the in-memory store (there is nothing to release).
func (m *MemStorage) Close() error { return nil }
