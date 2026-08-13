package core

// Config is the immutable configuration a Raft node is created with. It carries
// no clocks and no randomness: election-timeout *bounds* live here only so the
// driver can consult them when choosing a randomized duration, but the core
// itself never reads a clock — it reacts to EventTickElection. The core uses
// Peers (sorted, excluding Self) purely to compute quorum sizes and to iterate
// deterministically when broadcasting.
type Config struct {
	Self  NodeID
	Peers []NodeID // sorted, excludes Self; frozen for a node's lifetime (..)

	// Election-timeout bounds, in abstract "ticks", advisory to the driver's
	// timer randomization. The core does not read these to make decisions; they
	// are here so a single Config fully describes a node. Justified/derived
	// bounds live in config/registry.go.
	ElectionTimeoutMinTicks uint64
	ElectionTimeoutMaxTicks uint64
	HeartbeatTicks          uint64
}

// RaftCore is the frozen pure-state-machine contract. A driver constructs a
// core (New), optionally restores durable state after a crash (Restore), then
// repeatedly feeds Events and executes the returned Effects. The contract:
//
//   - Step is a pure function of the core's current in-memory state and the
//     event: no time, no I/O, no goroutines, no randomness. Same state + same
//     event ⇒ identical effects (in identical order) + identical next state.
//   - Step MUST NOT retain references to slices inside the returned Effects that
//     it continues to mutate; effect payloads are safe for the driver to hold.
//   - Effects are returned in execution order; the driver honors the
//     persist-before-send rule documented on EffectType.
//
// There are NO side channels: everything in, everything out, goes through
// Events and Effects. This is the interface the simulator and the real-TCP
// runtime both drive.
type RaftCore interface {
	// Step processes exactly one event and returns the effects the driver must
	// execute, in order. Never returns nil for "no effects" — returns an empty
	// slice — so callers need no nil check.
	Step(ev Event) []Effect

	// Restore re-installs durable state recovered from storage after a crash,
	// BEFORE any events are stepped. hs is the last persisted HardState;
	// snapIndex/snapTerm are the bounds of the most recent durable snapshot (0/0
	// if none); entries is the persisted log TAIL (indices > snapIndex, contiguous
	// and in order). The core rebases its log on the snapshot and seeds
	// commitIndex = appliedTo = snapIndex, since those entries are already
	// reflected in the recovered application state machine and must be neither
	// re-applied nor treated as uncommitted. Calling Restore on a core that has
	// already stepped events is a programming error.
	Restore(hs HardState, snapIndex Index, snapTerm Term, entries []LogEntry)

	// CompactTo advances the core's in-memory log base after the DRIVER has
	// durably saved a snapshot covering the log through index (of term). It drops
	// the compacted prefix; the remaining tail (indices > index) is retained. The
	// core never decides WHEN to snapshot (that would require reading state size /
	// a clock — impurity); the driver triggers it and calls CompactTo. index must
	// be <= CommitIndex() (never compact uncommitted entries).
	CompactTo(index Index, term Term)

	// The following are read-only accessors used by invariant monitors, the
	// history checker, and tests. They MUST NOT mutate state.

	Role() Role
	Term() Term
	CommitIndex() Index
	LastLogIndex() Index
	// Leader returns this node's best-known current leader (None if unknown).
	Leader() NodeID
	// ID returns this node's own NodeID.
	ID() NodeID
	// SnapBase returns the last index covered by the core's installed snapshot
	// (0 if none); the in-memory log begins at SnapBase()+1.
	SnapBase() Index
	// SnapTerm returns the term of SnapBase (0 if none).
	SnapTerm() Term
	// LogView returns a copy of the node's in-memory log entries (committed and
	// uncommitted) for invariant monitors and the history checker to snapshot
	// into a check.ClusterView. The returned slice is safe for the caller to
	// hold; it never aliases the core's backing array.
	LogView() []LogEntry
	// Members returns the node's current voting configuration (sorted, includes
	// self) — the set quorum is computed over. Static (== initial Config.Peers +
	// Self) unless single-server membership changes (P6) are used. The returned
	// slice is a fresh copy.
	Members() []NodeID
}
