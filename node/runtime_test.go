package node

import (
	"sync"
	"testing"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/storage"
)

// nopTransport is a Send sink for the single-node test: with no peers, the core
// emits no meaningful Send effects, but the interface must be satisfied.
type nopTransport struct{}

func (nopTransport) Send(core.Message) {}

// applyRecorder collects committed commands the runtime applies, guarded by a
// mutex because the core loop calls Apply from its own goroutine while the test
// goroutine polls.
type applyRecorder struct {
	mu       sync.Mutex
	commands [][]byte
}

func (a *applyRecorder) apply(entries []core.CommittedEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range entries {
		if core.IsNoOp(e.Command) {
			continue // application state machines ignore leader no-ops
		}
		a.commands = append(a.commands, e.Command)
	}
}

func (a *applyRecorder) snapshot() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([][]byte, len(a.commands))
	copy(out, a.commands)
	return out
}

// TestSingleNodeElectsAndApplies starts a single-node (no-peer) runtime and
// asserts it elects itself leader within a bounded time, then that a proposed
// command is accepted and applied. A single node is its own quorum, so the
// first election timeout makes it leader and any proposal commits immediately.
// This test must be race-clean: all core access is serialized through the one
// core-loop goroutine, and the apply recorder is mutex-guarded.
func TestSingleNodeElectsAndApplies(t *testing.T) {
	cfg := Config{
		Self:        "solo",
		Peers:       nil,
		ElectionMin: 20 * time.Millisecond,
		ElectionMax: 40 * time.Millisecond,
		Heartbeat:   10 * time.Millisecond,
	}
	c := core.New(core.Config{Self: cfg.Self, Peers: cfg.Peers})
	store := storage.NewMem()
	rec := &applyRecorder{}

	rt := New(cfg, c, store, nopTransport{}, rec.apply, 1)
	rt.Start()
	defer rt.Stop()

	// Poll for self-election within a generous window (many election timeouts).
	// Status() is the race-safe observation path; touching c.Role() from the
	// test goroutine would race the core loop.
	if !waitFor(2*time.Second, func() bool { return rt.Status().Role == core.Leader }) {
		t.Fatalf("node did not elect itself leader within timeout; role=%s", rt.Status().Role)
	}

	res := rt.Propose([]byte("set k=v"))
	if !res.Accepted {
		t.Fatalf("proposal not accepted by leader: %+v", res)
	}

	// The command must be applied through the ApplyFunc (commit is immediate for
	// a single-node cluster).
	if !waitFor(2*time.Second, func() bool {
		for _, cmd := range rec.snapshot() {
			if string(cmd) == "set k=v" {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("proposed command was never applied; applied=%v", rec.snapshot())
	}
}

// TestStopIsCleanAndIdempotent confirms Stop unblocks the loop and can be called
// more than once (also implicitly via the deferred Stop in other tests) without
// panicking or leaking goroutines under -race.
func TestStopIsCleanAndIdempotent(t *testing.T) {
	cfg := Config{
		Self:        "solo",
		ElectionMin: 20 * time.Millisecond,
		ElectionMax: 40 * time.Millisecond,
		Heartbeat:   10 * time.Millisecond,
	}
	rt := New(cfg, core.New(core.Config{Self: cfg.Self}), storage.NewMem(), nopTransport{}, nil, 1)
	rt.Start()
	rt.Stop()
	rt.Stop() // idempotent
}

// TestProposeAfterStopReturnsQuickly confirms a Propose on a stopped runtime
// does not block forever.
func TestProposeAfterStopReturnsQuickly(t *testing.T) {
	cfg := Config{Self: "solo", ElectionMin: 20 * time.Millisecond, ElectionMax: 40 * time.Millisecond, Heartbeat: 10 * time.Millisecond}
	rt := New(cfg, core.New(core.Config{Self: cfg.Self}), storage.NewMem(), nopTransport{}, nil, 1)
	rt.Start()
	rt.Stop()

	done := make(chan struct{})
	go func() {
		rt.Propose([]byte("x"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Propose blocked after Stop")
	}
}

// soloConfig is a single-node runtime config with fast timers for tests.
func soloConfig(self core.NodeID) Config {
	return Config{Self: self, ElectionMin: 20 * time.Millisecond,
		ElectionMax: 40 * time.Millisecond, Heartbeat: 10 * time.Millisecond}
}

// TestSingleNodeKVPutThenLinearizableRead drives the full  application path on
// one node: Propose a kv.Command Put, then Read the key back via the ReadIndex
// path and assert the committed value is returned. Exercises kv.Store wiring,
// the propose→apply→dedup path, and EffectReadIndexReady serving a real read.
func TestSingleNodeKVPutThenLinearizableRead(t *testing.T) {
	rt := New(soloConfig("solo"), core.New(core.Config{Self: "solo"}), storage.NewMem(), nopTransport{}, nil, 1)
	rt.Start()
	defer rt.Stop()
	if !waitFor(2*time.Second, func() bool { return rt.Status().Role == core.Leader }) {
		t.Fatalf("no self-election; role=%s", rt.Status().Role)
	}

	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "v1"}
	if res := rt.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("put not accepted: %+v", res)
	}
	// A linearizable read must return the committed value.
	if !waitFor(2*time.Second, func() bool {
		r := rt.Read("k")
		return r.Served && r.Found && r.Value == "v1"
	}) {
		t.Fatalf("linearizable read did not return the committed value; got %+v", rt.Read("k"))
	}
}

// TestRecoverRestoresKVState verifies durable recovery: a runtime writes state,
// stops, and a fresh runtime over the SAME storage recovers the committed KV
// value via Recover (snapshot + log tail) before serving reads.
func TestRecoverRestoresKVState(t *testing.T) {
	store := storage.NewMem()
	rt1 := New(soloConfig("solo"), core.New(core.Config{Self: "solo"}), store, nopTransport{}, nil, 1)
	rt1.Start()
	if !waitFor(2*time.Second, func() bool { return rt1.Status().Role == core.Leader }) {
		t.Fatal("rt1 no self-election")
	}
	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "durable"}
	if res := rt1.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("put not accepted: %+v", res)
	}
	// Wait for it to commit+persist, then stop (storage survives).
	waitFor(2*time.Second, func() bool { return rt1.Status().CommitIndex >= 2 })
	rt1.Stop()

	// A fresh runtime over the same storage recovers the value.
	rt2 := New(soloConfig("solo"), core.New(core.Config{Self: "solo"}), store, nopTransport{}, nil, 2)
	if err := rt2.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	rt2.Start()
	defer rt2.Stop()
	if !waitFor(2*time.Second, func() bool { return rt2.Status().Role == core.Leader }) {
		t.Fatal("rt2 no self-election after recovery")
	}
	if !waitFor(2*time.Second, func() bool {
		r := rt2.Read("k")
		return r.Served && r.Found && r.Value == "durable"
	}) {
		t.Fatalf("recovered runtime lost the committed value; got %+v", rt2.Read("k"))
	}
}

// TestLeaseReadServesAfterGrant verifies the P6 lease-read fast path: after a
// Put and a linearizable Read (which grants the lease), a LeaseRead returns the
// committed value, and the lease is genuinely held (valid()). On a single node
// the value must match what a linearizable read returns.
func TestLeaseReadServesAfterGrant(t *testing.T) {
	rt := New(soloConfig("solo"), core.New(core.Config{Self: "solo"}), storage.NewMem(), nopTransport{}, nil, 1)
	rt.Start()
	defer rt.Stop()
	if !waitFor(2*time.Second, func() bool { return rt.Status().Role == core.Leader }) {
		t.Fatalf("no self-election; role=%s", rt.Status().Role)
	}
	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "leased"}
	if res := rt.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("put not accepted: %+v", res)
	}
	// A LeaseRead: first call runs a ReadIndex round (grants the lease), returns
	// the value; the lease is then held so a second LeaseRead takes the fast path.
	if !waitFor(2*time.Second, func() bool {
		r := rt.LeaseRead("k")
		return r.Served && r.Found && r.Value == "leased"
	}) {
		t.Fatalf("lease read did not return the committed value; got %+v", rt.LeaseRead("k"))
	}
	if !rt.lease.valid(time.Now()) {
		t.Fatal("lease should be held after a successful lease read")
	}
	// Fast-path read returns the same value.
	if r := rt.LeaseRead("k"); !r.Served || r.Value != "leased" {
		t.Fatalf("fast-path lease read wrong: %+v", r)
	}
}

// waitFor polls cond every few ms until it is true or the deadline passes.
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}
