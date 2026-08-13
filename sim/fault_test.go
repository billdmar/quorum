package sim

import (
	"testing"

	"github.com/billdmar/quorum/core"
)

// newTestNet builds a bare network for isolated injector testing: a fresh RNG
// and clock, no partitions active, with the given per-message fault params.
func newTestNet(seed uint64, n int, p paramsView) (*Network, *Clock, *RNG) {
	rng := NewRNG(seed)
	clock := NewClock()
	return NewNetwork(n, rng, clock, p), clock, rng
}

// countDelivered advances the clock far enough to fire every scheduled delivery
// and returns how many delivery items fired (i.e. survived drop). Timers are
// never scheduled here, so every fired item is a delivery.
func drainDeliveries(clock *Clock) int {
	delivered := 0
	// Advance well past any possible delay; deliveries are one tick + MaxDelay.
	for t := uint64(0); t <= 1000 && !clock.empty(); t++ {
		clock.setTick(t)
		for clock.due() {
			if clock.pop().kind == itemDeliver {
				delivered++
			}
		}
	}
	return delivered
}

// TestDropRate: over many independent sends, ~DropPPT/1000 are dropped. We
// assert the observed rate lands in a tolerance band and — crucially — that the
// exact count is deterministic for the fixed seed.
func TestDropRate(t *testing.T) {
	const trials = 10000
	nw, clock, _ := newTestNet(12345, 2, paramsView{dropPPT: 150})
	for i := 0; i < trials; i++ {
		nw.send(0, 1, core.Message{})
	}
	delivered := drainDeliveries(clock)
	dropped := trials - delivered
	// Expected ~1500 drops; allow a generous band for RNG variance.
	if dropped < 1300 || dropped > 1700 {
		t.Fatalf("drop count %d/%d outside expected ~15%% band", dropped, trials)
	}
	// Determinism: a second identical run drops exactly the same count.
	nw2, clock2, _ := newTestNet(12345, 2, paramsView{dropPPT: 150})
	for i := 0; i < trials; i++ {
		nw2.send(0, 1, core.Message{})
	}
	if got := trials - drainDeliveries(clock2); got != dropped {
		t.Fatalf("drop count not deterministic: %d != %d", got, dropped)
	}
}

// TestZeroDropNeverDrops: with DropPPT=0 every message is delivered exactly once
// (and no RNG draw is consumed for the drop decision).
func TestZeroDropNeverDrops(t *testing.T) {
	const trials = 1000
	nw, clock, _ := newTestNet(1, 2, paramsView{})
	for i := 0; i < trials; i++ {
		nw.send(0, 1, core.Message{})
	}
	if got := drainDeliveries(clock); got != trials {
		t.Fatalf("expected %d deliveries with zero faults, got %d", trials, got)
	}
}

// TestDupRate: with DupPPT>0 a fraction of messages are delivered twice, so the
// total delivered count exceeds the send count by ~DupPPT/1000.
func TestDupRate(t *testing.T) {
	const trials = 10000
	nw, clock, _ := newTestNet(555, 2, paramsView{dupPPT: 100})
	for i := 0; i < trials; i++ {
		nw.send(0, 1, core.Message{})
	}
	delivered := drainDeliveries(clock)
	extra := delivered - trials
	// Expected ~1000 extra copies; band for variance.
	if extra < 800 || extra > 1200 {
		t.Fatalf("dup extra-copies %d outside expected ~10%% band", extra)
	}
}

// TestDelayReorder: with ReorderPPT>0 and MaxDelay>1, delayed messages arrive
// later than base latency, and the total spread proves reordering can occur.
// We check that at least some deliveries land beyond tick 1 (base latency) and
// none exceeds the max possible delay (1 + MaxDelay).
func TestDelayReorder(t *testing.T) {
	const trials = 2000
	nw, clock, _ := newTestNet(9, 2, paramsView{reorderPPT: 500, maxDelay: 5})
	for i := 0; i < trials; i++ {
		nw.send(0, 1, core.Message{})
	}
	beyondBase := 0
	maxTick := uint64(0)
	for t := uint64(0); t <= 100 && !clock.empty(); t++ {
		clock.setTick(t)
		for clock.due() {
			it := clock.pop()
			if it.kind == itemDeliver {
				if t > 1 {
					beyondBase++
				}
				if t > maxTick {
					maxTick = t
				}
			}
		}
	}
	if beyondBase == 0 {
		t.Fatal("expected some messages delayed beyond base latency, got none")
	}
	if maxTick > 1+5 {
		t.Fatalf("delivery at tick %d exceeds max possible delay 1+MaxDelay=6", maxTick)
	}
}

// TestPartitionSymmetric: a symmetric partition blocks BOTH directions across
// the cut, and blocks nothing within a side.
func TestPartitionSymmetric(t *testing.T) {
	// PartitionPPT=1000 forces a partition on the first stepPartition call.
	nw, clock, _ := newTestNet(3, 4, paramsView{partitionPPT: 1000, partitionMaxSpan: 50})
	clock.setTick(0)
	nw.stepPartition()
	if !nw.active {
		t.Fatal("expected a partition to be active")
	}
	// Count blocked directed pairs; for a symmetric cut every blocked pair's
	// reverse must also be blocked.
	for p := range nw.blocked {
		if !nw.blocked[pair{p.to, p.from}] {
			t.Fatalf("symmetric partition missing reverse of %v", p)
		}
	}
	// There must be at least one cut link (both sides non-empty is forced).
	if len(nw.blocked) == 0 {
		t.Fatal("symmetric partition cut no links")
	}
}

// TestPartitionAsymmetric: an asymmetric partition cuts exactly one direction —
// there exists a blocked pair whose reverse is NOT blocked.
func TestPartitionAsymmetric(t *testing.T) {
	nw, clock, _ := newTestNet(3, 4, paramsView{partitionPPT: 1000, partitionMaxSpan: 50, asymmetric: true})
	clock.setTick(0)
	nw.stepPartition()
	if !nw.active {
		t.Fatal("expected a partition to be active")
	}
	oneWay := false
	for p := range nw.blocked {
		if !nw.blocked[pair{p.to, p.from}] {
			oneWay = true
			break
		}
	}
	if !oneWay {
		t.Fatal("asymmetric partition blocked no one-directional link")
	}
}

// TestPartitionHeals: a partition with a bounded span clears once its heal tick
// passes, restoring full connectivity. partitionPPT is 0 so stepPartition only
// heals and does not immediately re-partition — isolating the heal behavior
// (in a real run a high onset rate would legitimately re-cut at once).
func TestPartitionHeals(t *testing.T) {
	nw, clock, _ := newTestNet(3, 4, paramsView{partitionPPT: 0, partitionMaxSpan: 5})
	clock.setTick(0)
	nw.begin()
	if !nw.active || len(nw.blocked) == 0 {
		t.Fatal("expected partition active with cut links at tick 0")
	}
	healBy := nw.healTick
	healed := false
	for tk := uint64(1); tk <= healBy+1; tk++ {
		clock.setTick(tk)
		nw.stepPartition()
		if !nw.active && len(nw.blocked) == 0 {
			healed = true
			break
		}
	}
	if !healed {
		t.Fatalf("partition never healed (healTick=%d)", healBy)
	}
}

// TestPartitionBlocksDelivery: a blocked link drops the message entirely (no
// delivery scheduled), while an unblocked link still delivers.
func TestPartitionBlocksDelivery(t *testing.T) {
	nw, clock, _ := newTestNet(1, 3, paramsView{})
	nw.blocked[pair{0, 1}] = true // manually cut 0->1 only
	nw.send(0, 1, core.Message{}) // must be dropped
	nw.send(0, 2, core.Message{}) // must be delivered
	if got := drainDeliveries(clock); got != 1 {
		t.Fatalf("expected exactly 1 delivery across a one-way cut, got %d", got)
	}
}

// TestDiskFaultHook: with diskFaultPPT>0 the node's fault hook returns injected
// faults at ~the configured rate, deterministically for a fixed seed; with 0 it
// never faults and consumes no RNG draws.
func TestDiskFaultHook(t *testing.T) {
	const trials = 10000
	rng := NewRNG(77)
	n := &node{rng: rng, diskFaultPPT: 100}
	faults := 0
	for i := 0; i < trials; i++ {
		if n.faultHook(0, 0) != 0 { // 0 == storage.FaultNone
			faults++
		}
	}
	if faults < 800 || faults > 1200 {
		t.Fatalf("disk-fault count %d outside expected ~10%% band", faults)
	}

	// Zero rate never faults and consumes no draws (RNG position unchanged).
	rng0 := NewRNG(5)
	before := *rng0
	n0 := &node{rng: rng0, diskFaultPPT: 0}
	for i := 0; i < 1000; i++ {
		if n0.faultHook(0, 0) != 0 {
			t.Fatal("zero disk-fault rate produced a fault")
		}
	}
	if *rng0 != before {
		t.Fatal("zero disk-fault rate consumed RNG draws")
	}
}
