package integration

import (
	"testing"

	"github.com/billdmar/quorum/config"
)

// TestDeterminismGate is the integration-level determinism gate: the same
// (seed, schedule, size) run through the full verified pipeline must produce a
// byte-identical trace hash on every execution. Determinism is law — it is what
// makes a failing seed a reproducible regression and the trace hash a faithful
// fingerprint of the run. This complements sim's own determinism test by
// exercising the exact path the gate uses (RunOne, with monitors attached).
//
// It runs the  BASE matrix (config.BaseMatrix). The  full-matrix schedules
// (kitchen-sink, disk-faulty) are exercised for determinism at the sim level
// (sim.TestDeterminism) and for safety in TestCrashRecoveryConvergence; they are
// deliberately excluded here because kitchen-sink is adversarial enough that many
// runs exhaust Porcupine's timeout (UNDETERMINED) — a  workload-calibration
// concern tracked in docs/DESIGN.md, not a determinism question.
func TestDeterminismGate(t *testing.T) {
	sizes := config.ClusterSizes
	for _, schedule := range config.BaseMatrix {
		for _, size := range sizes {
			for _, seed := range []uint64{1, 42, 507, 9999} {
				r1 := RunOne(seed, schedule, size, 40)
				r2 := RunOne(seed, schedule, size, 40)
				if r1.TraceHash != r2.TraceHash {
					t.Errorf("non-deterministic: seed=%d schedule=%s n=%d hashes %016x != %016x",
						seed, schedule, size, r1.TraceHash, r2.TraceHash)
				}
				// The verified outcome must also be stable across the two runs.
				if r1.OK() != r2.OK() || len(r1.Violations) != len(r2.Violations) {
					t.Errorf("non-deterministic verdict: seed=%d schedule=%s n=%d", seed, schedule, size)
				}
			}
		}
	}
}
