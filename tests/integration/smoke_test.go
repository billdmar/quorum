package integration

import (
	"testing"

	"github.com/billdmar/quorum/config"
)

// TestSmokeOneRun is a fast end-to-end sanity check that the whole pipeline —
// simulator driving the pure core, invariant monitors on every step, Porcupine
// on the captured history — runs and produces a passing verdict on the clean
// schedule. The exhaustive sweep is TestSeedSweepG1.
func TestSmokeOneRun(t *testing.T) {
	r := RunOne(1, config.ScheduleClean, 3, 30)
	t.Log(r.Summary())
	if len(r.Violations) != 0 {
		t.Fatalf("clean-schedule run had %d invariant violations: %+v", len(r.Violations), r.Violations)
	}
	if !r.LinDetermined {
		t.Fatalf("linearizability undetermined (timeout) on a clean 3-node run")
	}
	if !r.Linearizable {
		t.Fatalf("clean-schedule run was non-linearizable")
	}
	if r.CommittedOps == 0 {
		t.Fatalf("clean-schedule run committed zero ops — cluster made no progress")
	}
}
