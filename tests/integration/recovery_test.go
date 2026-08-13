package integration

import (
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/sim"
)

// TestCrashRecoveryConvergence exercises the crash-recovery path across many
// seeds on the crashy and disk-faulty schedules: nodes are killed at arbitrary
// points (including at fsync boundaries on disk-faulty) and restarted, and they
// must rejoin and re-derive committed state from durable storage. The safety
// half — no committed entry lost, no divergent apply — is guarded on every step
// by the invariant monitors and Porcupine (asserted here via r.OK()). This test
// adds the CONVERGENCE half: at the end of a run, every pair of live nodes that
// both consider an index committed must agree on the entry there (no live node
// recovered into a divergent committed prefix).
func TestCrashRecoveryConvergence(t *testing.T) {
	schedules := []config.ScheduleName{config.ScheduleCrashy, config.ScheduleDiskFaulty}
	const seeds = 200

	checked := 0
	for _, sch := range schedules {
		for seed := uint64(1); seed <= seeds; seed++ {
			s := sim.NewSimulator(sim.Params{
				Seed: seed, Schedule: sch, ClusterSize: 5, NumClientOps: 40,
			})
			mon := check.NewMonitor()
			var last check.ClusterView
			var violations int
			s.OnStep = func(v check.ClusterView) {
				violations += len(mon.CheckAll(v))
				last = v
			}
			s.Run()
			if violations != 0 {
				t.Errorf("seed=%d schedule=%s: %d invariant violations during run", seed, sch, violations)
			}
			if v := committedAgreementViolation(last); v != "" {
				t.Errorf("seed=%d schedule=%s: post-run committed divergence: %s", seed, sch, v)
			}
			checked++
		}
	}
	t.Logf("crash-recovery convergence: %d runs across %v × %d seeds, all converged",
		checked, schedules, seeds)
}

// committedAgreementViolation checks that, for every pair of live nodes, at every
// index below both nodes' commit indices, the entries are identical. A committed
// entry is durable and agreed by definition; a live node that recovered into a
// different committed value would be a real safety failure. Returns "" if the
// view is consistent, or a description of the first disagreement.
func committedAgreementViolation(view check.ClusterView) string {
	type liveNode struct {
		id     core.NodeID
		commit core.Index
		byIdx  map[core.Index]core.LogEntry
	}
	var live []liveNode
	for _, n := range view.Nodes {
		// A crashed node reports role=follower, term 0, commit 0, empty log — it is
		// not participating, so it trivially agrees. Only compare nodes with state.
		if n.CommitIndex == 0 && len(n.Log) == 0 {
			continue
		}
		m := make(map[core.Index]core.LogEntry, len(n.Log))
		for _, e := range n.Log {
			m[e.Index] = e
		}
		live = append(live, liveNode{id: n.ID, commit: n.CommitIndex, byIdx: m})
	}
	for i := 0; i < len(live); i++ {
		for j := i + 1; j < len(live); j++ {
			a, b := live[i], live[j]
			hi := a.commit
			if b.commit < hi {
				hi = b.commit
			}
			for idx := core.Index(1); idx <= hi; idx++ {
				ea, aok := a.byIdx[idx]
				eb, bok := b.byIdx[idx]
				if !aok || !bok {
					continue // entry compacted out of one's in-memory log; fine at 
				}
				if ea.Term != eb.Term || string(ea.Command) != string(eb.Command) {
					return "nodes " + string(a.id) + " and " + string(b.id) +
						" disagree at committed index " + itoaSeed(uint64(idx))
				}
			}
		}
	}
	return ""
}
