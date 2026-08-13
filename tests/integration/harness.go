// Package integration wires the deterministic simulator to the verifier army —
// the Raft safety-invariant monitors and the Porcupine linearizability checker —
// and runs them across the registered seed × schedule × cluster-size matrix.
// This is the driver  assembles at  from the frozen contracts and the
// packages; nothing here weakens a registry bound or shrinks a sweep.
package integration

import (
	"fmt"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/sim"
)

// RunResult is the verdict for a single simulated run: the seed/schedule/size
// that produced it, any safety-invariant violations found across the run's
// steps, and the linearizability outcome for the captured client history.
type RunResult struct {
	Seed          uint64
	Schedule      config.ScheduleName
	ClusterSize   int
	Steps         uint64
	OpCount       int
	CommittedOps  int
	Violations    []check.Violation
	Linearizable  bool
	LinDetermined bool
	TraceHash     uint64
}

// OK reports whether a run passed every safety check: zero invariant violations
// and a history Porcupine determined to be linearizable. A run whose
// linearizability could not be determined (timeout) is NOT ok — undecided is
// never treated as safe, per the project.
func (r RunResult) OK() bool {
	return len(r.Violations) == 0 && r.Linearizable && r.LinDetermined
}

// Summary is a one-line human-readable verdict for a run.
func (r RunResult) Summary() string {
	status := "PASS"
	if !r.OK() {
		status = "FAIL"
	}
	lin := "linearizable"
	if !r.Linearizable {
		if !r.LinDetermined {
			lin = "UNDETERMINED"
		} else {
			lin = "NON-LINEARIZABLE"
		}
	}
	return fmt.Sprintf("[%s] seed=%d schedule=%s n=%d steps=%d ops=%d/%d %s violations=%d hash=%016x",
		status, r.Seed, r.Schedule, r.ClusterSize, r.Steps, r.CommittedOps, r.OpCount,
		lin, len(r.Violations), r.TraceHash)
}

// RunOne executes a single fully-verified simulation: it attaches a fresh
// invariant Monitor to the simulator's per-step hook (so every core step is
// checked), runs the workload to completion, then feeds the captured history to
// Porcupine. It returns the aggregate RunResult. The run is a pure function of
// (seed, schedule, size) plus the frozen config — reproducible byte-for-byte.
func RunOne(seed uint64, schedule config.ScheduleName, clusterSize, numOps int) RunResult {
	s := sim.NewSimulator(sim.Params{
		Seed:         seed,
		Schedule:     schedule,
		ClusterSize:  clusterSize,
		NumClientOps: numOps,
	})

	mon := check.NewMonitor()
	var violations []check.Violation
	// Check every registered invariant after every core step. The first step at
	// which any invariant is violated is captured; we keep collecting so the
	// report shows the full set, but a single violation already fails the run.
	s.OnStep = func(view check.ClusterView) {
		for _, v := range mon.CheckAll(view) {
			// Stamp the seed/schedule the monitor cannot know, so a violation is a
			// self-contained, reproducible regression artifact.
			v.Seed = seed
			v.Schedule = string(schedule)
			violations = append(violations, v)
		}
	}

	history := s.Run()

	lin, linRes := check.CheckLinearizable(history)

	return RunResult{
		Seed:          seed,
		Schedule:      schedule,
		ClusterSize:   clusterSize,
		Steps:         stepCount(history),
		OpCount:       linRes.OpCount,
		CommittedOps:  committedOps(history),
		Violations:    violations,
		Linearizable:  lin,
		LinDetermined: linRes.Determined,
		TraceHash:     s.TraceHash(),
	}
}

// stepCount reports how many response events the history recorded (a proxy for
// resolved ops; the precise per-step count lives in the trace, not the history).
func stepCount(h check.History) uint64 {
	var n uint64
	for _, e := range h.Events {
		if e.Stage == check.StageResponse {
			n++
		}
	}
	return n
}

// committedOps counts operations that resolved with a definite (non-Unknown)
// response — i.e. ops the cluster actually committed and applied.
func committedOps(h check.History) int {
	n := 0
	for _, e := range h.Events {
		if e.Stage == check.StageResponse && !e.Unknown {
			n++
		}
	}
	return n
}
