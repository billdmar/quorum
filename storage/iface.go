// Package storage defines the durable-storage contract for a Raft node and its
// implementations (a write-ahead log with term/vote durability, snapshot files,
// and fsync discipline). The interface is frozen at ; implementations land in
//
//	(SA-storage) and  (snapshots).
//
// FSYNC DISCIPLINE (the correctness heart of this package): every method whose
// name promises durability MUST NOT return until the data is fsync'd to stable
// storage. Raft's safety proof assumes that once a node has persisted its term,
// vote, or a log entry, that fact survives a crash. The core enforces
// persist-before-send at the effect level (see core.EffectType); this interface
// enforces the "persist actually reached the disk" half of that contract.
package storage

import "github.com/billdmar/quorum/core"

// Storage is the durable backing store for one Raft node. All methods are
// synchronous and, where they promise durability, fsync before returning.
// Implementations are driven from OUTSIDE the pure core (by the simulator or
// the production runtime); the core never calls storage directly — it emits
// persistence Effects that a driver translates into these calls.
type Storage interface {
	// SaveHardState durably persists (fsync) the current term and vote. Returns
	// only after the data is stable. Corresponds to core.EffectPersistHardState.
	SaveHardState(hs core.HardState) error

	// AppendEntries truncates any existing entries at index >= fromIndex, then
	// durably (fsync) appends entries (which begin at fromIndex, contiguous and
	// in increasing index order). This single call covers both a plain append
	// and a conflict-overwrite. Corresponds to core.EffectPersistLog. Appending
	// an empty slice with fromIndex one past the last entry is a durable no-op.
	AppendEntries(fromIndex core.Index, entries []core.LogEntry) error

	// LoadHardState returns the last persisted HardState. If none was ever
	// persisted (fresh node), returns the zero HardState (Term 0, VotedFor None)
	// and ok=false.
	LoadHardState() (hs core.HardState, ok bool, err error)

	// LoadEntries returns the persisted log entries in index order. On a fresh
	// node, returns an empty slice. On a node recovering from a torn/truncated
	// write, returns the longest valid prefix (corruption-tail handling): a
	// partial trailing record is discarded, never returned as if complete.
	LoadEntries() ([]core.LogEntry, error)

	// FirstIndex / LastIndex report the durable log bounds. After snapshotting
	// (), FirstIndex advances past compacted entries. On an empty log,
	// FirstIndex==1 and LastIndex==0 (i.e. LastIndex < FirstIndex means empty).
	FirstIndex() (core.Index, error)
	LastIndex() (core.Index, error)

	// --- Snapshot support (). Declared now so the contract is frozen. ---

	// SaveSnapshot durably (fsync) writes a snapshot covering the log through
	// lastIncludedIndex/Term with the given application state bytes, then may
	// compact the log prefix it supersedes. Implemented in .
	SaveSnapshot(lastIncludedIndex core.Index, lastIncludedTerm core.Term, data []byte) error

	// LoadSnapshot returns the most recent persisted snapshot, if any.
	// ok=false when no snapshot exists.
	LoadSnapshot() (lastIncludedIndex core.Index, lastIncludedTerm core.Term, data []byte, ok bool, err error)

	// Close releases any resources (open files). After Close the Storage must
	// not be used. Provided so the crash/restart harness can model a clean
	// process exit distinctly from an adversarial kill.
	Close() error
}

// FaultKind enumerates the fsync-boundary disk faults the crash-recovery
// harness injects. Frozen at  so both the storage kill-point hooks
// (SA-storage) and the fault-injecting simulator (SA-sim) agree on the vocabulary.
type FaultKind uint8

const (
	// FaultNone: no injected fault.
	FaultNone FaultKind = iota
	// FaultCrashBeforeFsync: the process dies after the write reaches the OS
	// but before fsync returns — the data may or may not survive; the recovery
	// code must treat it as possibly lost (tolerated via corruption-tail handling).
	FaultCrashBeforeFsync
	// FaultCrashAfterFsync: the process dies immediately after fsync returns —
	// the data MUST survive and be recovered.
	FaultCrashAfterFsync
	// FaultTornWrite: only a prefix of the record's bytes reached the disk —
	// LoadEntries must discard the torn tail and return the valid prefix.
	FaultTornWrite
)

func (k FaultKind) String() string {
	switch k {
	case FaultNone:
		return "none"
	case FaultCrashBeforeFsync:
		return "crash-before-fsync"
	case FaultCrashAfterFsync:
		return "crash-after-fsync"
	case FaultTornWrite:
		return "torn-write"
	default:
		return "unknown"
	}
}
