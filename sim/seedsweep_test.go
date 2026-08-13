package sim

import (
	"flag"
	"testing"

	"github.com/billdmar/quorum/config"
)

// seedsFlag controls how many seeds TestSeedSweep runs. CI invokes it as
//
//	go test ./sim/... -run TestSeedSweep -args -seeds=200
//
// (config.SeedFloorCI). The in extended runs / sweeps pass much larger values.
// Default is small so the plain `go test ./sim/...` stays fast.
var seedsFlag = flag.Int("seeds", 50, "number of seeds TestSeedSweep runs across the base matrix × cluster sizes")

// TestSeedSweep runs -seeds seeds across config.BaseMatrix × config.ClusterSizes
// and asserts each run (a) completes without panicking and (b) reproduces its
// trace hash on an immediate re-run. This proves the SIMULATOR ITSELF is sound
// and deterministic across a broad input space.  layers the invariant
// Monitor and Porcupine linearizability check onto this same sweep at  via
// the OnStep hook and the returned History — those are deliberately NOT asserted
// here, since the invariant monitors and the KV model are  and not
// yet wired.
func TestSeedSweep(t *testing.T) {
	n := *seedsFlag
	if n <= 0 {
		t.Fatalf("-seeds must be positive, got %d", n)
	}
	runs := 0
	for _, sch := range config.BaseMatrix {
		for _, sz := range config.ClusterSizes {
			for seed := uint64(0); seed < uint64(n); seed++ {
				h1 := runOnce(t, seed, sch, sz)
				h2 := Replay(seed, sch, sz)
				if h1 != h2 {
					t.Fatalf("non-reproducible run: seed=%d schedule=%s size=%d: 0x%016x != 0x%016x",
						seed, sch, sz, h1, h2)
				}
				runs++
			}
		}
	}
	t.Logf("seed sweep OK: %d runs (%d seeds × %d schedules × %d sizes), all deterministic",
		runs, n, len(config.BaseMatrix), len(config.ClusterSizes))
}

// runOnce executes a single run, recovering any panic into a test failure with
// full reproduction context, and returns its trace hash.
func runOnce(t *testing.T, seed uint64, sch config.ScheduleName, sz int) (h uint64) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic in run seed=%d schedule=%s size=%d: %v", seed, sch, sz, r)
		}
	}()
	s := NewSimulator(Params{Seed: seed, Schedule: sch, ClusterSize: sz})
	s.Run()
	return s.TraceHash()
}

// TestSeedSweepFullMatrix is a lighter always-on sweep across the FULL matrix
// (adding disk-faulty + kitchen-sink) on a handful of seeds, so the plain
// `go test ./sim/...` still exercises the  schedules for determinism without
// needing the -seeds flag. It never asserts safety — same rationale as above.
func TestSeedSweepFullMatrix(t *testing.T) {
	for _, sch := range config.FullMatrix {
		for _, sz := range config.ClusterSizes {
			for seed := uint64(0); seed < 8; seed++ {
				if runOnce(t, seed, sch, sz) != Replay(seed, sch, sz) {
					t.Fatalf("non-reproducible: seed=%d schedule=%s size=%d", seed, sch, sz)
				}
			}
		}
	}
}
