package sim

import "github.com/billdmar/quorum/core"

// pair is a directed (from,to) link identified by node indices. Partitions are
// expressed as sets of blocked directed pairs so both symmetric and asymmetric
// cuts share one representation.
type pair struct{ from, to int }

// Network is the fault-injectable message layer between nodes. It implements a
// schedule's per-message faults (drop / duplicate / delay-reorder) and evolving
// network partitions. EVERY fault decision is drawn from the shared simulator
// RNG, so the whole fault trajectory is a pure function of the seed. The
// network never reads a wall clock — it schedules deliveries on the virtual
// Clock and evolves partitions on the tick loop's call to stepPartition.
//
// A message that survives all faults is scheduled for delivery at a future tick
// (never the same tick it is sent — a minimum one-tick latency models the wire
// and prevents unbounded same-tick send/deliver recursion). Delayed messages
// wait extra ticks; duplicates are scheduled independently and may reorder.
type Network struct {
	n      int
	rng    *RNG
	clock  *Clock
	params paramsView

	// In-flight (and delivered) messages. A delivery item carries an index into
	// this slice rather than the Message itself, keeping clock.go free of any
	// core import. Entries are never reclaimed — runs are bounded.
	msgs []core.Message

	// Partition state. blocked holds the currently-cut directed pairs; membership
	// is only ever tested, never iterated, so map order cannot leak into the
	// trace. active/healTick track the single in-progress partition (at most one
	// at a time — composition of partitions buys no extra coverage and would
	// complicate healing).
	blocked  map[pair]bool
	active   bool
	healTick uint64
}

// paramsView is the subset of config.FaultParams the network needs, copied in
// so sim.go owns the config→sim boundary and the network stays decoupled from
// the config package's naming.
type paramsView struct {
	dropPPT          uint32
	dupPPT           uint32
	reorderPPT       uint32
	maxDelay         uint32
	partitionPPT     uint32
	partitionMaxSpan uint32
	asymmetric       bool
}

// NewNetwork builds a network for n nodes driven by the given RNG and clock.
func NewNetwork(n int, rng *RNG, clock *Clock, p paramsView) *Network {
	return &Network{
		n:       n,
		rng:     rng,
		clock:   clock,
		params:  p,
		blocked: make(map[pair]bool),
	}
}

// blockedLink reports whether the directed link from→to is currently cut.
func (nw *Network) blockedLink(from, to int) bool { return nw.blocked[pair{from, to}] }

// send injects a message from node `from` to node `to`. It applies drop, then
// duplication, then delay/reorder, drawing every decision from the RNG in a
// fixed order so the consumed stream is identical across runs of a seed. A
// surviving message (or each surviving copy) is scheduled on the clock.
func (nw *Network) send(from, to int, m core.Message) {
	// Partition / drop: a cut link or a drop draw discards the message outright.
	if nw.blockedLink(from, to) {
		return
	}
	if nw.rng.chance(nw.params.dropPPT) {
		return
	}
	// Duplication: deliver a second independent copy. Decided before delay so the
	// two copies draw their delays independently and may reorder relative to each
	// other. The draw is always consumed (chance handles ppt==0) to keep the
	// stream position fixed regardless of whether a dup occurs.
	copies := 1
	if nw.rng.chance(nw.params.dupPPT) {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		nw.scheduleDelivery(to, m)
	}
}

// scheduleDelivery stores the message and schedules its delivery item. Base
// latency is one tick; a reorder draw adds a uniform [1,MaxDelay] extra ticks.
func (nw *Network) scheduleDelivery(to int, m core.Message) {
	idx := len(nw.msgs)
	nw.msgs = append(nw.msgs, m)
	delay := uint64(1)
	if nw.params.maxDelay > 0 && nw.rng.chance(nw.params.reorderPPT) {
		delay += uint64(nw.rng.between(1, nw.params.maxDelay))
	}
	nw.clock.schedule(nw.clock.Now()+delay, item{kind: itemDeliver, node: to, msgIdx: idx})
}

// message returns the stored message for a delivery item.
func (nw *Network) message(idx int) core.Message { return nw.msgs[idx] }

// stepPartition evolves the partition state for the current tick: it heals an
// expired partition and, if none is active, may begin a new one. Called once
// per tick by the main loop BEFORE timers/deliveries for that tick, so a
// partition that begins on tick T cuts links for messages sent on tick T.
func (nw *Network) stepPartition() {
	if nw.active && nw.clock.Now() >= nw.healTick {
		nw.heal()
	}
	if nw.active {
		return
	}
	if nw.rng.chance(nw.params.partitionPPT) {
		nw.begin()
	}
}

// begin starts a partition: it splits the nodes into two non-empty groups by a
// per-node coin, then cuts every cross-group directed pair (both directions for
// a symmetric cut, only group-A→group-B for an asymmetric one). The heal tick
// is drawn uniformly from [1,PartitionMaxSpan] ticks ahead.
func (nw *Network) begin() {
	if nw.n < 2 || nw.params.partitionMaxSpan == 0 {
		return
	}
	// Assign each node to side A (true) or B (false). Force both sides non-empty
	// by flipping node 0 if the coins came out unanimous — a degenerate split
	// cuts nothing and would waste the partition event.
	side := make([]bool, nw.n)
	allSame := true
	for i := range side {
		side[i] = nw.rng.Intn(2) == 0
		if side[i] != side[0] {
			allSame = false
		}
	}
	if allSame {
		side[0] = !side[0]
	}
	for a := 0; a < nw.n; a++ {
		for b := 0; b < nw.n; b++ {
			if a == b || side[a] == side[b] {
				continue
			}
			// a in A, b in B. Cut A→B always; cut B→A only when symmetric.
			if side[a] && !side[b] {
				nw.blocked[pair{a, b}] = true
				if !nw.params.asymmetric {
					nw.blocked[pair{b, a}] = true
				}
			}
		}
	}
	nw.active = true
	nw.healTick = nw.clock.Now() + uint64(nw.rng.between(1, nw.params.partitionMaxSpan))
}

// heal clears all cut links, restoring full connectivity.
func (nw *Network) heal() {
	nw.blocked = make(map[pair]bool)
	nw.active = false
}
