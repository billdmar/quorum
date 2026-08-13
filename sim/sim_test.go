package sim

import (
	"strings"
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
)

// TestCleanRunCommitsAllOps: on the fault-free schedule every issued client op
// must commit (the happy path), producing a matched invoke+response for each,
// none Unknown. This is the baseline that proves replicate→commit→apply works
// before faults are layered on.
func TestCleanRunCommitsAllOps(t *testing.T) {
	s := NewSimulator(Params{Seed: 1, Schedule: config.ScheduleClean, ClusterSize: 3, NumClientOps: 50})
	h := s.Run()
	if !s.finished() {
		t.Fatalf("clean run did not resolve all ops: pending=%d issued=%d", len(s.clients.pending), s.clients.issued)
	}
	invokes, responses, unknowns := 0, 0, 0
	for _, e := range h.Events {
		switch {
		case e.Stage == check.StageInvoke:
			invokes++
		case e.Unknown:
			unknowns++
			responses++
		default:
			responses++
		}
	}
	if invokes != 50 || responses != 50 {
		t.Fatalf("expected 50 invokes and 50 responses, got %d/%d", invokes, responses)
	}
	if unknowns != 0 {
		t.Fatalf("clean run produced %d Unknown responses (expected 0)", unknowns)
	}
}

// TestOnStepHookFires: the OnStep hook ('s invariant-Monitor attach point)
// is invoked once per core step with a well-formed ClusterView of every node.
func TestOnStepHookFires(t *testing.T) {
	s := NewSimulator(Params{Seed: 2, Schedule: config.ScheduleClean, ClusterSize: 3, NumClientOps: 20})
	calls := 0
	var lastStep uint64
	s.OnStep = func(cv check.ClusterView) {
		calls++
		if len(cv.Nodes) != 3 {
			t.Fatalf("ClusterView has %d nodes, want 3", len(cv.Nodes))
		}
		if cv.Step < lastStep {
			t.Fatalf("ClusterView.Step went backwards: %d < %d", cv.Step, lastStep)
		}
		lastStep = cv.Step
	}
	s.Run()
	if calls == 0 {
		t.Fatal("OnStep never fired")
	}
}

// TestKVStateMachineApplies: committed client commands reach each node's kv.Store
// application state machine. After a clean run, at least one node's kv holds a
// key written by the workload (the leader that applied it, and its followers once
// they apply the same committed entries).
func TestKVStateMachineApplies(t *testing.T) {
	s := NewSimulator(Params{Seed: 3, Schedule: config.ScheduleClean, ClusterSize: 3, NumClientOps: 10})
	s.Run()
	// The workload writes keys "k0".."k{keyspace-1}". At least one must be present
	// on at least one live node's applied KV state after a clean run.
	found := false
	for _, n := range s.nodes {
		if n.crashed || n.kv == nil {
			continue
		}
		for i := 0; i < keyspace; i++ {
			if _, ok := n.kv.Get("k" + string(rune('0'+i))); ok {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no committed client write reached any node's kv.Store")
	}
}

// TestHistoryMatchesRun: the History() accessor ('s Porcupine feed) returns
// the same recording Run() returned, tagged with the run's seed and schedule.
func TestHistoryMatchesRun(t *testing.T) {
	s := NewSimulator(Params{Seed: 8, Schedule: config.ScheduleLossy, ClusterSize: 3, NumClientOps: 15})
	h := s.Run()
	h2 := s.History()
	if len(h.Events) != len(h2.Events) || h2.Seed != 8 || h2.Schedule != string(config.ScheduleLossy) {
		t.Fatalf("History() mismatch: len %d/%d seed=%d schedule=%q", len(h.Events), len(h2.Events), h2.Seed, h2.Schedule)
	}
}

// TestReplayStringFormat: the CLI-able replay line carries the seed, schedule,
// size, and a hex hash — used by the determinism gate's tooling.
func TestReplayStringFormat(t *testing.T) {
	line := ReplayString(42, config.ScheduleLossy, 3)
	for _, want := range []string{"seed=42", "schedule=lossy", "size=3", "hash=0x"} {
		if !strings.Contains(line, want) {
			t.Fatalf("replay line %q missing %q", line, want)
		}
	}
}

// TestCrashRestartConvergence: under the crashy schedule nodes crash and restart
// repeatedly, yet the run still resolves every op — proving the crash/restart
// harness (drop volatile state, keep durable storage, rebuild core via Restore)
// lets the cluster recover and make progress. (Durability/safety invariants are
// 's  assertion; here we prove liveness of the harness itself.)
func TestCrashRestartConvergence(t *testing.T) {
	s := NewSimulator(Params{Seed: 11, Schedule: config.ScheduleCrashy, ClusterSize: 5, NumClientOps: 40})
	s.Run()
	if !s.finished() {
		t.Fatalf("crashy run did not resolve all ops: pending=%d", len(s.clients.pending))
	}
}

// TestSingleNodeCluster: a 1-node cluster elects itself and commits immediately,
// exercising the clusterN==1 fast path through the driver.
func TestSingleNodeCluster(t *testing.T) {
	s := NewSimulator(Params{Seed: 5, Schedule: config.ScheduleClean, ClusterSize: 1, NumClientOps: 10})
	s.Run()
	if !s.finished() {
		t.Fatalf("single-node run did not resolve all ops: pending=%d", len(s.clients.pending))
	}
}

// TestNodeCrashDropsVolatileKeepsDurable directly checks the crash model: after
// a crash the core is gone and the apply stream is cleared, but durable storage
// still holds what was persisted, and restart rebuilds a usable core.
func TestNodeCrashDropsVolatileKeepsDurable(t *testing.T) {
	rng := NewRNG(1)
	cfg := core.Config{Self: "n0"}
	n := newNode(0, cfg, rng, 0)
	// Persist some durable state directly.
	if n.persistHardState(core.HardState{Term: 3, VotedFor: "n0"}) {
		t.Fatal("unexpected injected crash with no fault hook rate")
	}
	n.apply([]core.CommittedEntry{{Index: 1, Term: 1, Command: []byte("x")}})
	if len(n.applied) != 1 {
		t.Fatal("apply did not record entry")
	}
	n.crash()
	if !n.crashed || n.rc != nil || len(n.applied) != 0 {
		t.Fatal("crash did not drop volatile state")
	}
	if !n.restart() {
		t.Fatal("restart failed")
	}
	if n.crashed || n.rc == nil {
		t.Fatal("restart did not rebuild the core")
	}
	if n.rc.Term() != 3 {
		t.Fatalf("durable term not recovered: got %d want 3", n.rc.Term())
	}
}
