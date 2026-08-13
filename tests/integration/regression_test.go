package integration

import (
	"testing"

	"github.com/billdmar/quorum/config"
)

// regressionSeed is one committed reproduction of a bug the seed sweep found.
// Per the project, every failing seed becomes a first-class regression artifact:
// the seed/schedule/size that surfaced the defect, plus a note on what it was.
type regressionSeed struct {
	seed     uint64
	schedule config.ScheduleName
	size     int
	numOps   int
	note     string
}

// regressionSeeds are the sweep-discovered cases that must stay green. Each was
// a genuine finding — a real bug OR (as here) a monitor that was unsound against
// a legal Raft state — fixed without weakening any registry bound.
var regressionSeeds = []regressionSeed{
	{
		seed: 507, schedule: config.SchedulePartitionHeavy, size: 5, numOps: 40,
		note: "leader-completeness false positive: a deposed term-4 leader isolated by " +
			"a partition (unaware of the term-5 leader) coexisted with the true leader; " +
			"the entry was committed in term 5 (not its creation term 1), so the stale " +
			"sub-max-term leader lacking it is legal. Fixed by checking only the max-term " +
			"leader. History was linearizable throughout.",
	},
	{
		seed: 7503, schedule: config.ScheduleKitchenSink, size: 5, numOps: 0,
		note: " checker-cost tail (1 in 140,000): kitchen-sink so adversarial that " +
			"0/20 ops committed — an all-Unknown history, Porcupine's exponential " +
			"worst case, ran 48s to Undetermined (which OK() refuses to treat as safe). " +
			"Fixed by a SOUND short-circuit: a history with no definite operation " +
			"outcome is trivially linearizable (nothing constrains an ordering), so it " +
			"never reaches Porcupine. Not a weakening — a real violation needs a " +
			"definite result, which is still checked (regression tests pin both halves).",
	},
}

// TestRegressionSeeds re-runs every sweep-discovered seed and requires it to
// pass cleanly. A failure here means a fix regressed.
func TestRegressionSeeds(t *testing.T) {
	for _, rs := range regressionSeeds {
		t.Run(string(rs.schedule)+"/"+itoaSeed(rs.seed), func(t *testing.T) {
			r := RunOne(rs.seed, rs.schedule, rs.size, rs.numOps)
			t.Logf("%s\n  finding: %s", r.Summary(), rs.note)
			if !r.OK() {
				t.Errorf("regression seed failed: %s", r.Summary())
				for _, v := range r.Violations {
					t.Errorf("  violation: %s step=%d %s", v.Invariant, v.Step, v.Detail)
				}
			}
		})
	}
}

func itoaSeed(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
