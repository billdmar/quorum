package sim

import (
	"errors"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/storage"
)

// node wraps one pure core.RaftCore with its durable storage, its application
// state machine (a kv.Store), the driver-owned timer bookkeeping, and the
// application-side apply stream the invariant monitors read. It is the
// simulator's per-node record; sim.go owns cross-node concerns (routing sends,
// arming timers, the compaction trigger).
//
// CRASH MODEL: a crash drops ALL volatile state (the core, the kv.Store, and the
// apply stream) but keeps durable storage (WAL + snapshot). A restart rebuilds
// the core and kv.Store from the durable snapshot + log tail: load the snapshot
// (kv.Restore), then Restore the core rebased on the snapshot base, then let the
// tail re-apply as commit re-advances. Honest "process died, disk survived".
type node struct {
	idx int         // stable index for network routing
	id  core.NodeID // this node's Raft id
	cfg core.Config // immutable config used to (re)build the core
	rc  core.RaftCore
	st  *storage.MemStorage
	kv  *kv.Store // the replicated application state machine

	// applied is the ordered stream of entries applied to the kv.Store SINCE the
	// current snapshot base (entries at or below snapBase were folded into the
	// snapshot and are not retained here). It feeds NodeView.Applied for the
	// state-machine-safety monitor, which scopes comparisons to the overlapping
	// post-snapshot range. Volatile: cleared on crash, rebuilt on recovery.
	applied []core.CommittedEntry

	// Timer generations. Arming a timer bumps its generation; a fired timer whose
	// generation is stale is ignored. Keeps timer-generation logic OUT of the pure
	// core (which just emits EffectReset*Timer).
	electionGen  uint64
	heartbeatGen uint64

	// diskFaultPPT drives fsync-boundary disk faults via the storage FaultHook.
	diskFaultPPT uint32
	rng          *RNG // shared simulator RNG, for the disk-fault hook

	crashed bool
	// incarnation counts (re)starts; bumped on crash so the invariant monitor can
	// tell a legitimate volatile-state reset from an in-incarnation decrease.
	incarnation uint64
}

// newNode constructs a fresh node: a new core, kv.Store, an empty in-memory
// store with the disk-fault hook installed, and no timers armed yet.
func newNode(idx int, cfg core.Config, rng *RNG, diskFaultPPT uint32) *node {
	n := &node{
		idx:          idx,
		id:           cfg.Self,
		cfg:          cfg,
		rc:           core.New(cfg),
		st:           storage.NewMem(),
		kv:           kv.NewStore(),
		rng:          rng,
		diskFaultPPT: diskFaultPPT,
	}
	n.st.Fault = n.faultHook
	return n
}

// faultHook is consulted by MemStorage at every durable-write boundary. It is a
// pure function of the shared RNG stream (drawn in deterministic write order).
// Only log appends (OpAppend) can suffer a partial/torn fault; hard-state and
// snapshot writes are atomic (temp+rename in the WAL), so for those a fault is
// modeled as a whole-record crash, never a torn write. This keeps the injected
// fault trajectory faithful to the storage layer's real durability guarantees.
func (n *node) faultHook(op storage.WriteOp, _ core.Index) storage.FaultKind {
	if !n.rng.chance(n.diskFaultPPT) {
		return storage.FaultNone
	}
	// Weighted toward torn-writes/before-fsync (data may be lost — the recovery
	// path must tolerate it) over after-fsync crashes. Torn writes only apply to
	// the appendable log; atomic writes degrade a torn draw to before-fsync.
	switch n.rng.Intn(3) {
	case 0:
		return storage.FaultCrashBeforeFsync
	case 1:
		return storage.FaultCrashAfterFsync
	default:
		if op == storage.OpAppend {
			return storage.FaultTornWrite
		}
		return storage.FaultCrashBeforeFsync
	}
}

// persistHardState executes an EffectPersistHardState.
func (n *node) persistHardState(hs core.HardState) (crashed bool) {
	if err := n.st.SaveHardState(hs); err != nil {
		return errors.Is(err, storage.ErrInjectedCrash)
	}
	return false
}

// persistLog executes an EffectPersistLog (truncate-then-append at fromIndex).
func (n *node) persistLog(from core.Index, entries []core.LogEntry) (crashed bool) {
	if err := n.st.AppendEntries(from, entries); err != nil {
		return errors.Is(err, storage.ErrInjectedCrash)
	}
	return false
}

// apply executes an EffectApply: apply each committed entry to the kv.Store
// (which enforces exactly-once via its session/dedup table) and record it in the
// apply stream for the monitor. It returns the per-entry Results aligned 1:1 with
// committed, so the simulator records EACH operation's REAL output into the
// history — a single shared "last result" would misattribute outputs when a
// batch commits several entries at once (a real hazard under retries/loss).
func (n *node) apply(committed []core.CommittedEntry) []kv.Result {
	results := make([]kv.Result, len(committed))
	for i, e := range committed {
		results[i] = n.kv.Apply(e)
		n.applied = append(n.applied, e)
	}
	return results
}

// installSnapshot executes an EffectInstallSnapshot on a follower: durably save
// the received snapshot and restore the kv.Store from it, then rebase the apply
// stream (the snapshot folds in everything through snapIndex, so the retained
// stream starts empty above it). Returns whether an injected disk fault crashed
// the node mid-install.
func (n *node) installSnapshot(snapIndex core.Index, snapTerm core.Term, data []byte) (crashed bool) {
	if err := n.st.SaveSnapshot(snapIndex, snapTerm, data); err != nil {
		return errors.Is(err, storage.ErrInjectedCrash)
	}
	if err := n.kv.Restore(data); err != nil {
		// A malformed snapshot is a real defect, not a tolerated fault: surface it
		// by crashing the node (the recovery suite would then fail to converge).
		return true
	}
	n.applied = n.applied[:0]
	return false
}

// maybeCompact triggers a snapshot+compaction when the applied prefix has grown
// LogCompactionThreshold entries past the current snapshot base. The trigger is
// a pure function of (appliedIndex - snapBase) — no clock, no byte size — so it
// is deterministic. It snapshots the kv.Store, durably saves+compacts storage,
// advances the core's log base (CompactTo), and trims the apply stream to the
// retained tail. Called after each apply on a live node.
func (n *node) maybeCompact() (crashed bool) {
	if n.rc == nil {
		return false
	}
	applied := n.rc.CommitIndex() // committed == applied ceiling in this driver
	base := n.rc.SnapBase()
	if applied < base+config.LogCompactionThreshold {
		return false
	}
	term, ok := n.termAt(applied)
	if !ok {
		return false
	}
	data := n.kv.Snapshot()
	if err := n.st.SaveSnapshot(applied, term, data); err != nil {
		return errors.Is(err, storage.ErrInjectedCrash)
	}
	n.rc.CompactTo(applied, term)
	n.trimAppliedTo(applied)
	return false
}

// termAt returns the term recorded for a committed index by scanning the apply
// stream (which holds the retained post-snapshot committed entries in order).
func (n *node) termAt(index core.Index) (core.Term, bool) {
	for _, e := range n.applied {
		if e.Index == index {
			return e.Term, true
		}
	}
	// Fall back to the log view for entries not in the retained apply stream.
	for _, e := range n.rc.LogView() {
		if e.Index == index {
			return e.Term, true
		}
	}
	return 0, false
}

// trimAppliedTo drops apply-stream entries at or below index (now folded into a
// snapshot), keeping the monitor's Applied slice scoped to the retained range.
func (n *node) trimAppliedTo(index core.Index) {
	kept := n.applied[:0]
	for _, e := range n.applied {
		if e.Index > index {
			kept = append(kept, e)
		}
	}
	n.applied = append([]core.CommittedEntry(nil), kept...)
}

// crash simulates an adversarial kill: volatile state (core, kv.Store, apply
// stream) is discarded, durable storage is kept.
func (n *node) crash() {
	n.crashed = true
	n.rc = nil
	n.kv = nil
	n.applied = n.applied[:0]
	n.electionGen++
	n.heartbeatGen++
	n.incarnation++
}

// restart rebuilds the core and kv.Store from durable storage, modeling a clean
// relaunch. It loads the snapshot (if any) into a fresh kv.Store, then Restores
// the core rebased on the snapshot base with the recovered log tail. Returns
// false (leaving the node crashed) only if storage cannot be read.
func (n *node) restart() bool {
	hs, _, err := n.st.LoadHardState()
	if err != nil {
		return false
	}
	snapIndex, snapTerm, data, haveSnap, err := n.st.LoadSnapshot()
	if err != nil {
		return false
	}
	entries, err := n.st.LoadEntries()
	if err != nil {
		return false
	}
	n.kv = kv.NewStore()
	if haveSnap {
		if err := n.kv.Restore(data); err != nil {
			return false
		}
	}
	n.rc = core.New(n.cfg)
	n.rc.Restore(hs, snapIndex, snapTerm, entries)
	n.applied = n.applied[:0]
	n.crashed = false
	return true
}

// snapshotBytes returns the current kv.Store snapshot for building an outgoing
// InstallSnapshot message (leader side of EffectSendSnapshot).
func (n *node) snapshotBytes() []byte {
	if n.kv == nil {
		return nil
	}
	return n.kv.Snapshot()
}

// view snapshots this node's observable state for the invariant monitors. A
// crashed node reports an empty follower view: it is not participating, so it
// cannot violate a safety invariant.
func (n *node) view() check.NodeView {
	if n.crashed || n.rc == nil {
		return check.NodeView{ID: n.id, Role: core.Follower, Incarnation: n.incarnation}
	}
	applied := make([]core.CommittedEntry, len(n.applied))
	copy(applied, n.applied)
	return check.NodeView{
		ID:          n.id,
		Role:        n.rc.Role(),
		Term:        n.rc.Term(),
		CommitIndex: n.rc.CommitIndex(),
		Incarnation: n.incarnation,
		SnapBase:    n.rc.SnapBase(),
		SnapTerm:    n.rc.SnapTerm(),
		Log:         n.rc.LogView(),
		Applied:     applied,
	}
}
