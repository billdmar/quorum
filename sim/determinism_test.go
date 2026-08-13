package sim

import (
	"testing"

	"github.com/billdmar/quorum/config"
)

// TestDeterminism is THE determinism gate: same (seed, schedule, size) run
// twice MUST yield byte-identical trace hashes. Determinism is law — a run is a
// pure function of its Params plus the frozen config, so any divergence means
// nondeterminism has leaked into the sim (a wall clock, math/rand, unsorted map
// iteration, or a goroutine race) and must be hunted down, never tolerated.
func TestDeterminism(t *testing.T) {
	seeds := []uint64{0, 1, 42, 1337, 999983}
	for _, sch := range config.FullMatrix {
		for _, sz := range config.ClusterSizes {
			for _, seed := range seeds {
				h1 := Replay(seed, sch, sz)
				h2 := Replay(seed, sch, sz)
				if h1 != h2 {
					t.Fatalf("nondeterministic: seed=%d schedule=%s size=%d: 0x%016x != 0x%016x",
						seed, sch, sz, h1, h2)
				}
			}
		}
	}
}

// TestDeterminismHistoryStable asserts the recorded history — not just the
// trace hash — is identical across re-runs: same event count, and every event
// field equal in order. Porcupine consumes this history at , so its
// reproducibility matters as much as the hash's.
func TestDeterminismHistoryStable(t *testing.T) {
	for _, sch := range []config.ScheduleName{config.ScheduleClean, config.ScheduleLossy, config.ScheduleCrashy} {
		s1 := NewSimulator(Params{Seed: 2024, Schedule: sch, ClusterSize: 3})
		s2 := NewSimulator(Params{Seed: 2024, Schedule: sch, ClusterSize: 3})
		h1 := s1.Run()
		h2 := s2.Run()
		if len(h1.Events) != len(h2.Events) {
			t.Fatalf("schedule=%s: history length differs: %d != %d", sch, len(h1.Events), len(h2.Events))
		}
		for i := range h1.Events {
			if h1.Events[i] != h2.Events[i] {
				t.Fatalf("schedule=%s: history event %d differs:\n %+v\n %+v", sch, i, h1.Events[i], h2.Events[i])
			}
		}
	}
}

// TestDistinctSeedsDiffer is a sanity check that the trace hash is actually
// sensitive to the seed — otherwise the determinism gate above would be
// vacuously satisfied by a constant. Not every pair need differ, but across a
// batch the vast majority must, or the hash is not fingerprinting the run.
func TestDistinctSeedsDiffer(t *testing.T) {
	const n = 64
	seen := make(map[uint64]int, n)
	for seed := uint64(0); seed < n; seed++ {
		seen[Replay(seed, config.ScheduleLossy, 3)]++
	}
	if len(seen) < n*3/4 {
		t.Fatalf("trace hash barely varies with seed: only %d distinct hashes over %d seeds", len(seen), n)
	}
}
