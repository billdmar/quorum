package sim

import (
	"fmt"

	"github.com/billdmar/quorum/config"
)

// Replay re-runs a single (seed, schedule, size) with the default op/tick
// budgets and returns the trace hash. It is the determinism gate's workhorse:
// the gate calls Replay twice for the same inputs and asserts equal hashes.
// Because a run is a pure function of its Params plus the frozen config, the
// two calls are byte-identical.
func Replay(seed uint64, schedule config.ScheduleName, size int) uint64 {
	s := NewSimulator(Params{Seed: seed, Schedule: schedule, ClusterSize: size})
	s.Run()
	return s.TraceHash()
}

// ReplayString formats a Replay result as a single line, handy for a CLI or
// test log: "seed=42 schedule=lossy size=3 hash=0x...".
func ReplayString(seed uint64, schedule config.ScheduleName, size int) string {
	return fmt.Sprintf("seed=%d schedule=%s size=%d hash=0x%016x",
		seed, schedule, size, Replay(seed, schedule, size))
}
