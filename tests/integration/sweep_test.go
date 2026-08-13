package integration

import (
	"flag"
	"testing"

	"github.com/billdmar/quorum/config"
)

// seeds controls how many seeds each sweep runs. It defaults to the  floor
// (config.SeedFloorG1 = 1000) so an unflagged `go test -run TestSeedSweepG1`
// runs the full iron-gate sweep; CI and the verification pass explicit values. It is
// only ever RAISED above a gate's floor, never lowered to pass.
var seeds = flag.Int("seeds", config.SeedFloorG1, "number of seeds per schedule/size in a sweep")

// runSweep drives the fully-verified pipeline (invariant monitors on every step
// + Porcupine on the captured history) for `n` seeds across the given schedule
// matrix and both cluster sizes, requiring every run to pass. Op/tick budgets
// come from the registry per-schedule table (RunOne is called with numOps=0, so
// NewSimulator fills the budget). Returns (runs, failures).
func runSweep(t *testing.T, label string, matrix []config.ScheduleName, n int) (int, int) {
	t.Helper()
	if n < 1 {
		t.Fatalf("seeds must be >= 1, got %d", n)
	}
	var runs, failures int
	committedBySchedule := make(map[config.ScheduleName]int)
	for _, schedule := range matrix {
		for _, size := range config.ClusterSizes {
			for seed := uint64(1); seed <= uint64(n); seed++ {
				r := RunOne(seed, schedule, size, 0) // 0 => registry budget
				runs++
				committedBySchedule[schedule] += r.CommittedOps
				if !r.OK() {
					failures++
					t.Errorf("%s GATE FAILURE: %s", label, r.Summary())
					for _, v := range r.Violations {
						t.Errorf("  violation: invariant=%s step=%d detail=%s",
							v.Invariant, v.Step, v.Detail)
					}
					if failures >= 10 {
						t.Fatalf("stopping after %d failures across %d runs", failures, runs)
					}
				}
			}
		}
	}
	// Progress floor: every schedule must commit SOME operations across its sweep.
	// The harshest schedules (disk-faulty, kitchen-sink) legitimately produce many
	// all-Unknown histories that take the trivial-linearizable short-circuit
	// (Porcupine does not run) — but if a schedule committed ZERO ops across the
	// whole sweep, it would be verified by nothing but the invariant monitors and
	// the gate's "linearizable" claim would be vacuous. This catches a
	// mis-calibration (e.g. a future fault-rate bump) that silently guts coverage.
	for _, schedule := range matrix {
		if committedBySchedule[schedule] == 0 {
			t.Errorf("%s sweep: schedule %q committed ZERO ops across %d seeds × %d sizes — "+
				"linearizability coverage for it is vacuous (progress floor violated)",
				label, schedule, n, len(config.ClusterSizes))
		}
	}
	t.Logf("%s sweep: %d runs across %d schedules × %d sizes × %d seeds — %d failures; committed-by-schedule=%v",
		label, runs, len(matrix), len(config.ClusterSizes), n, failures, committedBySchedule)
	return runs, failures
}

// TestSeedSweepG1 is the verification: the base fault matrix (clean, lossy,
// partition-heavy, asymmetric, crashy) × {3,5} nodes for at least SeedFloorG1
// seeds, zero invariant violations and zero non-linearizable/undetermined
// histories. Any failing seed is reported with full reproduction coordinates and
// must be fixed and committed as a regression, never silenced.
func TestSeedSweepG1(t *testing.T) {
	runSweep(t, "", config.BaseMatrix, *seeds)
	if *seeds < config.SeedFloorG1 {
		t.Logf("NOTE: ran %d seeds < SeedFloorG1=%d (bounded run, e.g. CI); the gate requires the floor",
			*seeds, config.SeedFloorG1)
	}
}

// TestSeedSweepG2 is the full verification gate: the FULL matrix (base + the
// adversarial disk-faulty and kitchen-sink schedules) × {3,5} nodes, exercising
// snapshots/compaction, InstallSnapshot catch-up, ReadIndex reads, and
// exactly-once sessions under the harshest fault composition. Same bar: zero
// violations, zero non-linearizable/undetermined histories. Run explicitly with
// a seed count at or above SeedFloorG2 for the gate; default `-seeds` keeps it
// affordable for iterative checking.
func TestSeedSweepG2(t *testing.T) {
	runSweep(t, "", config.FullMatrix, *seeds)
	if *seeds < config.SeedFloorG2 {
		t.Logf("NOTE: ran %d seeds < SeedFloorG2=%d (bounded run); the full gate requires the floor",
			*seeds, config.SeedFloorG2)
	}
}
