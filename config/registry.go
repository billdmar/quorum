// Package config is the single, frozen source of truth for the verification
// program: the named fault-schedule matrix, the registered safety invariants,
// the per-gate seed-count floors, and every numeric bound the tests rely on —
// each with a written justification.
//
// REGISTRY RULE (critical): nothing in this file is ever weakened to
// turn a red run green. A violation means investigate the bug, fix it, and
// commit the failing seed as a regression test — never relax a bound, shrink a
// sweep, or drop a schedule. Changes here go through the  only and
// must update the justification alongside the value.
package config

import "github.com/billdmar/quorum/check"

// ScheduleName identifies a named fault schedule. The set is frozen at .
type ScheduleName string

const (
	// ScheduleClean: no faults. Baseline — proves the happy path and that the
	// determinism/trace-hash machinery itself is sound before faults are added.
	ScheduleClean ScheduleName = "clean"
	// ScheduleLossy: independent per-message drop + delay. Stresses retransmit,
	// election liveness under loss, and idempotent handling.
	ScheduleLossy ScheduleName = "lossy"
	// SchedulePartitionHeavy: frequent symmetric network partitions that heal.
	// Stresses leader election across majority/minority splits and re-merge.
	SchedulePartitionHeavy ScheduleName = "partition-heavy"
	// ScheduleAsymmetric: one-way reachability (A hears B, B cannot hear A).
	// Exposes bugs that symmetric partitions hide — e.g. a stuck leader that
	// can send heartbeats but never learns it lost quorum.
	ScheduleAsymmetric ScheduleName = "asymmetric"
	// ScheduleCrashy: frequent node crash/restart with durable-state recovery.
	// Stresses WAL replay, term/vote durability, and rejoin+convergence.
	ScheduleCrashy ScheduleName = "crashy"
	// ScheduleDiskFaulty: fsync-boundary disk faults (crash before/after fsync,
	// torn writes). Stresses corruption-tail handling and the persist-before-send
	// contract. Introduced at  (full matrix).
	ScheduleDiskFaulty ScheduleName = "disk-faulty"
	// ScheduleKitchenSink: all fault classes composed at once — the adversary.
	// Introduced at .
	ScheduleKitchenSink ScheduleName = "kitchen-sink"
)

// FaultParams are the tunable probabilities/rates for a schedule, expressed as
// integer parts-per-thousand (ppt) so they are exact and deterministic — no
// floating point in the fault decisions, which keeps trace hashes stable across
// architectures. All rates are per-message or per-tick as noted.
type FaultParams struct {
	// Per-message network faults (parts per thousand).
	DropPPT    uint32 // probability a message is dropped
	DupPPT     uint32 // probability a message is duplicated
	ReorderPPT uint32 // probability a message is delayed for reordering
	MaxDelay   uint32 // max extra ticks a delayed message waits (uniform [1,MaxDelay])
	// Partition behavior (per-tick).
	PartitionPPT     uint32 // probability a partition event begins on a given tick
	PartitionMaxSpan uint32 // max ticks a partition persists before healing
	Asymmetric       bool   // partitions cut one direction only
	// Node crash behavior (per-tick, per-node).
	CrashPPT       uint32 // probability a running node crashes on a given tick
	RestartMaxSpan uint32 // max ticks a crashed node stays down before restart
	// Disk faults (per durable write).
	DiskFaultPPT uint32 // probability a durable write hits an fsync-boundary fault
}

// Schedule is a named fault schedule: its parameters plus the justification for
// why it exists and why its bounds are set where they are.
type Schedule struct {
	Name          ScheduleName
	Params        FaultParams
	Justification string
}

// Schedules is the frozen fault-schedule matrix. Base matrix ():
// clean, lossy, partition-heavy, asymmetric, crashy. Full matrix () adds
// disk-faulty and kitchen-sink. Bounds are chosen high enough to exercise the
// failure paths hard while keeping runs from being *permanently* unable to
// elect a leader (which would test liveness of the harness, not safety of Raft).
var Schedules = map[ScheduleName]Schedule{
	ScheduleClean: {
		Name:          ScheduleClean,
		Params:        FaultParams{},
		Justification: "Baseline with zero faults: validates the happy path and proves the determinism/trace-hash and history-capture machinery is sound before faults are layered on. Any violation here is a pure logic bug, not a fault-tolerance gap.",
	},
	ScheduleLossy: {
		Name: ScheduleLossy,
		Params: FaultParams{
			DropPPT: 150, DupPPT: 50, ReorderPPT: 200, MaxDelay: 5,
		},
		Justification: "15% drop / 5% dup / 20% reorder (delay up to 5 ticks) stresses retransmission, duplicate-RPC idempotency, and election liveness under loss. Drop kept below the ~33% that would routinely starve a 3-node majority so runs still make progress and exercise post-loss convergence rather than permanent stall.",
	},
	SchedulePartitionHeavy: {
		Name: SchedulePartitionHeavy,
		Params: FaultParams{
			DropPPT: 20, ReorderPPT: 50, MaxDelay: 3,
			PartitionPPT: 40, PartitionMaxSpan: 30,
		},
		Justification: "4%/tick partition onset lasting up to 30 ticks produces frequent majority/minority splits that then heal, exercising election across splits, log divergence in the minority, and reconciliation on merge. Span is bounded so partitions always heal and convergence can be asserted.",
	},
	ScheduleAsymmetric: {
		Name: ScheduleAsymmetric,
		Params: FaultParams{
			DropPPT: 20, ReorderPPT: 50, MaxDelay: 3,
			PartitionPPT: 40, PartitionMaxSpan: 30, Asymmetric: true,
		},
		Justification: "Same rates as partition-heavy but one-directional cuts, which expose bugs symmetric partitions hide: a leader that can still send heartbeats but can no longer receive responses must step down / lose authority correctly (motivates CheckQuorum-style reasoning and the ReadIndex heartbeat confirmation at ).",
	},
	ScheduleCrashy: {
		Name: ScheduleCrashy,
		Params: FaultParams{
			DropPPT: 50, ReorderPPT: 100, MaxDelay: 4,
			CrashPPT: 30, RestartMaxSpan: 25,
		},
		Justification: "3%/tick per-node crash with restart within 25 ticks stresses WAL replay, durability of term/vote/log across restarts, and rejoin+catch-up. Combined with mild loss so recovery happens amid ongoing traffic. Restart is bounded so a crashed node always comes back and convergence is assertable.",
	},
	ScheduleDiskFaulty: {
		Name: ScheduleDiskFaulty,
		Params: FaultParams{
			DropPPT: 50, ReorderPPT: 100, MaxDelay: 4,
			CrashPPT: 20, RestartMaxSpan: 25,
			DiskFaultPPT: 100,
		},
		Justification: "10% of durable writes hit an fsync-boundary fault (crash before/after fsync, torn write). Directly targets corruption-tail handling in LoadEntries and the persist-before-send contract. Introduced at . A crash-before-fsync may lose the last write (tolerated); crash-after-fsync must never lose it (asserted by recovery).",
	},
	ScheduleKitchenSink: {
		Name: ScheduleKitchenSink,
		Params: FaultParams{
			DropPPT: 120, DupPPT: 40, ReorderPPT: 180, MaxDelay: 5,
			PartitionPPT: 35, PartitionMaxSpan: 25, Asymmetric: false,
			CrashPPT: 25, RestartMaxSpan: 25,
			DiskFaultPPT: 80,
		},
		Justification: "All fault classes composed — the adversary. Rates are each pulled slightly below their single-class peaks so the composition still makes progress (a leader can eventually hold quorum long enough to commit) rather than degenerating into permanent unavailability, which would test harness liveness instead of Raft safety. Introduced at .",
	},
}

// BaseMatrix is the schedule set every  seed sweep runs across.
var BaseMatrix = []ScheduleName{
	ScheduleClean, ScheduleLossy, SchedulePartitionHeavy, ScheduleAsymmetric, ScheduleCrashy,
}

// FullMatrix is the schedule set every  seed sweep runs across.
var FullMatrix = []ScheduleName{
	ScheduleClean, ScheduleLossy, SchedulePartitionHeavy, ScheduleAsymmetric,
	ScheduleCrashy, ScheduleDiskFaulty, ScheduleKitchenSink,
}

// RegisteredInvariants is the frozen set of safety invariants the monitors
// evaluate after every simulator step. Zero violations is the bar at every gate.
var RegisteredInvariants = []check.InvariantID{
	check.InvElectionSafety,
	check.InvLogMatching,
	check.InvLeaderCompleteness,
	check.InvStateMachineSafety,
	check.InvCommitMonotonicity,
}

// Seed-count floors per gate. These are FLOORS: a gate's sweep must run at
// least this many seeds across its matrix with zero violations and zero
// non-linearizable histories. Never lowered to pass.
const (
	// SeedFloorG1: the verification. 1,000+ seeds across the base matrix on 3- and
	// 5-node clusters. Large enough that rare interleavings surface, small
	// enough to run in extended runs repeatedly during core development.
	SeedFloorG1 = 1000
	// SeedFloorG2: the full verification gate. 10,000+ seeds across the full
	// matrix (adds disk-faulty + kitchen-sink). An order of magnitude more
	// coverage for the higher-complexity system (snapshots, ReadIndex, sessions).
	SeedFloorG2 = 10000
	// SeedFloorCI: the bounded sweep CI runs on every push. Kept small so CI
	// stays fast; the real assurance comes from the in extended runs / sweeps.
	SeedFloorCI = 200
)

// ClusterSizes are the cluster sizes exercised by the sweeps. Odd sizes only:
// even clusters gain no fault tolerance over the next-lower odd size and
// complicate quorum reasoning.
var ClusterSizes = []int{3, 5}

// Election-timeout and heartbeat bounds, in abstract simulator ticks. The core
// never reads these (it reacts to tick events); the driver uses them to
// randomize timer durations. Justifications below.
const (
	// ElectionTimeoutMinTicks / MaxTicks: the randomized election-timeout window.
	// The [Min,Max] spread must be wide enough that split votes resolve quickly
	// (nodes rarely time out simultaneously) — the standard Raft guidance. Max
	// must comfortably exceed HeartbeatTicks so a live leader's heartbeats
	// normally pre-empt an election.
	ElectionTimeoutMinTicks = 10
	ElectionTimeoutMaxTicks = 20
	// HeartbeatTicks: leader heartbeat period. Well below ElectionTimeoutMin so
	// several heartbeats reach followers per election window, preventing
	// spurious elections under a healthy leader.
	HeartbeatTicks = 3
)

// LogCompactionThreshold is the number of committed-and-applied entries a node
// may accumulate beyond its snapshot base before the driver triggers a snapshot
// and compacts the log prefix. The trigger is a pure function of
// (appliedIndex - SnapBase) >= this bound — no clock, no real byte size — so it
// stays deterministic. Chosen small (12) so that a routine ~40-operation run
// crosses it several times and reliably produces lagging-follower
// InstallSnapshot cases (a follower partitioned across a compaction must be
// caught up by snapshot, not AppendEntries) — the whole point of exercising the
// snapshot path — yet large enough not to churn snapshots every few commits.
const LogCompactionThreshold = 12

// ScheduleBudget is the per-schedule run budget: how many client operations the
// workload issues and the tick ceiling before a run is cut off. It exists ONLY
// to make adversarial schedules' client histories Porcupine-DECIDABLE (enough
// operations actually commit within enough virtual time that the linearizability
// check terminates with a definite verdict instead of timing out as
// "Undetermined"). It DOES NOT weaken any fault rate in Schedules — the faults
// are identical; the adversarial schedules simply get more virtual time and a
// right-sized op count so progress can be observed and judged. A schedule absent
// from this map uses DefaultBudget. All values are constants (never per-seed
// randomized), preserving "a run is a pure function of (seed, schedule, size)".
type ScheduleBudget struct {
	NumClientOps int    // client operations issued across the run
	MaxTicks     uint64 // virtual-tick ceiling; a run also ends early once all ops resolve
}

// DefaultBudget applies to schedules that make steady progress (clean, lossy,
// partition-heavy, asymmetric, crashy commit at or near 40/40 within ~20k ticks
// as measured empirically).
var DefaultBudget = ScheduleBudget{NumClientOps: 40, MaxTicks: 20000}

// ScheduleBudgets overrides DefaultBudget for the highly adversarial full-matrix
// schedules. Empirically (n=5, 40 ops, 20k ticks) kitchen-sink commits ~5/40 and
// disk-faulty ~0/40, leaving Porcupine unable to decide. These schedules get a
// far larger tick ceiling and a smaller op target so that the FEW operations
// that do get through form a short, fully-resolved, decidable history — without
// touching the fault rates. The disk-fault MODEL itself (not just the budget) is
// also right-sized in the simulator so a node makes progress between faults
// rather than crashing on essentially every durable write; see sim notes.
var ScheduleBudgets = map[ScheduleName]ScheduleBudget{
	ScheduleDiskFaulty:  {NumClientOps: 20, MaxTicks: 60000},
	ScheduleKitchenSink: {NumClientOps: 20, MaxTicks: 60000},
}

// BudgetFor returns the run budget for a schedule (its override or the default).
func BudgetFor(s ScheduleName) ScheduleBudget {
	if b, ok := ScheduleBudgets[s]; ok {
		return b
	}
	return DefaultBudget
}
