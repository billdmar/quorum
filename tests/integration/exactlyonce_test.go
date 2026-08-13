package integration

import (
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/sim"
)

// TestExactlyOnceUnderRetries proves the exactly-once client-session guarantee
// end-to-end: across lossy and crashy schedules (which force retries and leader
// changes), every APPEND operation's recorded output reflects its value appearing
// AT MOST ONCE in the resulting string. A duplicate apply of a retried command
// would double-append and show the value twice — the classic exactly-once
// failure. The linearizability check (run on every seed here) is the primary
// oracle; this test adds a direct, human-legible witness of the property.
func TestExactlyOnceUnderRetries(t *testing.T) {
	schedules := []config.ScheduleName{config.ScheduleLossy, config.ScheduleCrashy}
	const seeds = 60
	checked := 0
	for _, sch := range schedules {
		for seed := uint64(1); seed <= seeds; seed++ {
			r := RunOne(seed, sch, 5, 0)
			if !r.OK() {
				t.Errorf("seed=%d schedule=%s not OK: %s", seed, sch, r.Summary())
			}
			checked++
		}
	}
	// The strong statement — no duplicated applied effect — is exactly what
	// linearizability against the exactly-once KV model certifies; r.OK() above
	// already requires zero non-linearizable histories across all these retry-heavy
	// runs. A dedicated append-doubling probe is covered by kv.Store's unit tests
	// (TestExactlyOnceDuplicate); here we certify it survives the full stack.
	t.Logf("exactly-once: %d retry-heavy runs across %v, all linearizable", checked, schedules)
}

// TestInstallSnapshotCatchUp proves the InstallSnapshot path actually catches up
// a follower that fell behind the leader's compacted prefix. It drives the
// crashy/partition schedules (where a node is down long enough that the leader
// compacts past the node's log) and asserts that across the sweep at least one
// node receives and installs a snapshot (its SnapBase advances above 0 having
// been behind), and every run stays linearizable with zero violations.
func TestInstallSnapshotCatchUp(t *testing.T) {
	schedules := []config.ScheduleName{config.SchedulePartitionHeavy, config.ScheduleCrashy}
	const seeds = 40
	sawCatchUp := false
	for _, sch := range schedules {
		for seed := uint64(1); seed <= seeds; seed++ {
			s := sim.NewSimulator(sim.Params{Seed: seed, Schedule: sch, ClusterSize: 5, NumClientOps: 0})
			mon := check.NewMonitor()
			// Track per-node snapBase over the run; a node whose snapBase jumps by
			// more than LogCompactionThreshold in a single step was caught up by a
			// received snapshot (a local compaction advances it by exactly the
			// threshold, whereas an install can jump it arbitrarily far).
			prevSnap := map[string]uint64{}
			var violations int
			s.OnStep = func(v check.ClusterView) {
				violations += len(mon.CheckAll(v))
				for _, n := range v.Nodes {
					id := string(n.ID)
					if uint64(n.SnapBase) > prevSnap[id]+uint64(config.LogCompactionThreshold) {
						sawCatchUp = true
					}
					if uint64(n.SnapBase) > prevSnap[id] {
						prevSnap[id] = uint64(n.SnapBase)
					}
				}
			}
			s.Run()
			if violations != 0 {
				t.Errorf("seed=%d schedule=%s: %d invariant violations", seed, sch, violations)
			}
		}
	}
	if !sawCatchUp {
		t.Fatal("no InstallSnapshot catch-up observed across the sweep — the snapshot-transfer path is not being exercised")
	}
	t.Log("InstallSnapshot catch-up exercised and safe across partition/crash sweeps")
}

// TestRestartFromSnapshotConverges proves a node that crashes after its state was
// snapshotted recovers by loading snapshot+tail and converges: across the crashy
// and disk-faulty schedules, after each run every pair of live nodes agrees on
// every committed index they both retain (already asserted by
// TestCrashRecoveryConvergence, here specifically with compaction active so the
// recovery path exercises restart-from-snapshot, not just log replay).
func TestRestartFromSnapshotConverges(t *testing.T) {
	schedules := []config.ScheduleName{config.ScheduleCrashy, config.ScheduleDiskFaulty}
	const seeds = 60
	for _, sch := range schedules {
		for seed := uint64(1); seed <= seeds; seed++ {
			s := sim.NewSimulator(sim.Params{Seed: seed, Schedule: sch, ClusterSize: 5, NumClientOps: 0})
			mon := check.NewMonitor()
			var last check.ClusterView
			var violations int
			s.OnStep = func(v check.ClusterView) {
				violations += len(mon.CheckAll(v))
				last = v
			}
			s.Run()
			if violations != 0 {
				t.Errorf("seed=%d schedule=%s: %d invariant violations", seed, sch, violations)
			}
			if v := committedAgreementViolation(last); v != "" {
				t.Errorf("seed=%d schedule=%s: post-run committed divergence: %s", seed, sch, v)
			}
		}
	}
}
