// Package check defines the linearizability history-record format and (in ,
// SA-check) the Porcupine model, the Raft safety-invariant monitors, and
// violation reporting. Only the frozen history contract lives in this file.
//
// A linearizability history is a set of client operations, each with an
// invocation time and a response time on a single logical timeline. Porcupine
// searches for a total order of operations that (a) respects real-time
// precedence (an op that returned before another was invoked must be ordered
// first) and (b) is consistent with the KV model's sequential semantics. The
// simulator records one HistoryEvent at invocation and one at response for
// every client operation it issues.
package check

import "github.com/billdmar/quorum/core"

// OpKind is the client operation kind captured in a history. The KV model in
// SA-check interprets these; the format is frozen here so the workload
// generator (), the simulator, and the checker agree.
type OpKind uint8

const (
	OpGet    OpKind = iota // read Key -> Value ("" if absent)
	OpPut                  // set Key = Value, returns nothing meaningful
	OpAppend               // append Value to Key's current value; returns new value
	OpCAS                  // compare-and-swap Key: if == CompareValue, set = Value
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpAppend:
		return "append"
	case OpCAS:
		return "cas"
	default:
		return "unknown"
	}
}

// EventStage marks a HistoryEvent as an operation's invocation or its response.
type EventStage uint8

const (
	StageInvoke EventStage = iota
	StageResponse
)

// HistoryEvent is one record on the linearizability timeline. Every operation
// produces exactly one StageInvoke and (unless it never returns) one
// StageResponse sharing the same OpID. Stamp is the simulator's logical clock
// value (monotonic, deterministic) — NOT wall-clock — so histories are
// reproducible from a seed.
//
// This struct is the frozen wire between the drivers (which emit it) and the
// checker (which consumes it). Adding fields requires an  contract change.
type HistoryEvent struct {
	OpID   uint64     // unique per operation; ties Invoke to Response
	Client uint64     // issuing client/session id
	Stage  EventStage // Invoke or Response
	Stamp  uint64     // logical timeline position (deterministic)

	Kind OpKind

	// Request fields (meaningful on StageInvoke).
	Key          string
	Value        string // Put/Append/CAS new value
	CompareValue string // CAS expected value

	// Response fields (meaningful on StageResponse).
	Output string // Get result / Append result / observed value
	OK     bool   // CAS success; also false if the op is known to have failed
	// Unknown marks a response whose outcome the client never learned (e.g. the
	// request timed out amid a leader change). Porcupine treats such an op as
	// "may or may not have taken effect" by leaving it without a definite
	// linearization point on failure. Drivers set this instead of guessing.
	Unknown bool
}

// History is a full recording for one simulation run, plus the seed and
// schedule that produced it, so any non-linearizable history replays exactly.
type History struct {
	Seed     uint64
	Schedule string
	Events   []HistoryEvent
}

// InvariantID names a Raft safety invariant the monitors check after every
// simulator step. Frozen so the registry (config/registry.go) and the monitors
// (SA-check) refer to the same set.
type InvariantID uint8

const (
	// InvElectionSafety: at most one leader per term across the whole cluster.
	InvElectionSafety InvariantID = iota
	// InvLogMatching: if two logs contain an entry with the same index and
	// term, the logs are identical in all entries up through that index.
	InvLogMatching
	// InvLeaderCompleteness: if an entry is committed in a term, it is present
	// in the logs of all leaders of all higher terms.
	InvLeaderCompleteness
	// InvStateMachineSafety: no two nodes apply different commands at the same
	// log index.
	InvStateMachineSafety
	// InvCommitMonotonicity: a node's commit index never decreases, and applied
	// index never exceeds commit index.
	InvCommitMonotonicity
)

func (i InvariantID) String() string {
	switch i {
	case InvElectionSafety:
		return "election-safety"
	case InvLogMatching:
		return "log-matching"
	case InvLeaderCompleteness:
		return "leader-completeness"
	case InvStateMachineSafety:
		return "state-machine-safety"
	case InvCommitMonotonicity:
		return "commit-monotonicity"
	default:
		return "unknown"
	}
}

// Violation is a machine-checked safety failure. It carries enough context to
// reproduce and diagnose: the seed and schedule that produced it, the logical
// step at which it was detected, and a human-readable detail. A failing seed
// becomes a committed regression test (project rule); Violation is the report
// that drives that.
type Violation struct {
	Invariant InvariantID
	Seed      uint64
	Schedule  string
	Step      uint64
	Detail    string
}

// ClusterView is the read-only snapshot of every node the invariant monitors
// evaluate after a step. Drivers assemble it from the cores' accessors and
// their persisted logs. Kept in check/ so monitors and drivers share the shape.
type ClusterView struct {
	Step  uint64
	Nodes []NodeView
}

// NodeView is one node's observable state for invariant checking. Log is the
// node's in-memory log (committed and uncommitted); Applied is what its
// application state machine has applied, in order, for state-machine-safety.
//
// Incarnation counts how many times this node has (re)started. commitIndex and
// appliedIndex are VOLATILE Raft state (Raft Figure 2): a restart resets them to
// 0 and the node re-derives them from its durable log. Commit-monotonicity is
// therefore an intra-incarnation property — the monitor uses Incarnation to
// reset its per-node baseline across a crash/restart boundary rather than
// mistaking a legitimate volatile-state reset for a safety violation. A driver
// bumps Incarnation on every restart; drivers that never crash a node leave it 0.
type NodeView struct {
	ID          core.NodeID
	Role        core.Role
	Term        core.Term
	CommitIndex core.Index
	Incarnation uint64
	// SnapBase/SnapTerm describe the node's compacted log prefix (0/0 if none).
	// After snapshot install/compaction a node's Log begins at SnapBase+1, so the
	// log-matching and state-machine-safety monitors scope their cross-node
	// comparisons to the index range both nodes physically retain — otherwise a
	// legitimately compacted prefix reads as a divergence (the same false-positive
	// class as the Incarnation fix for commit-monotonicity).
	SnapBase core.Index
	SnapTerm core.Term
	Log      []core.LogEntry
	Applied  []core.CommittedEntry
}
