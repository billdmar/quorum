package sim

import "container/heap"

// timerKind distinguishes the two driver-owned timers. The core never reads a
// clock; it emits EffectResetElectionTimer / EffectResetHeartbeatTimer and the
// simulator arms a virtual timer that later fires as an Event back into the core.
type timerKind uint8

const (
	timerElection timerKind = iota
	timerHeartbeat
)

// itemKind tags what a queued item does when its fire tick arrives.
type itemKind uint8

const (
	itemDeliver itemKind = iota // a Message arrives at node `to`
	itemTimer                   // a node's election/heartbeat timer fires
	itemRestart                 // a crashed node comes back up
)

// item is one scheduled thing on the virtual timeline: either a message
// delivery or a timer firing. Ordering is total and reproducible: primarily by
// fireTick, and among items sharing a tick by seq — a monotonically increasing
// sequence assigned at push time. Because the simulator pushes items from a
// single goroutine in deterministic order, seq is itself deterministic, so two
// runs of the same seed pop items in byte-identical order.
type item struct {
	fireTick uint64
	seq      uint64
	kind     itemKind

	// itemDeliver
	msgIdx int // index into the network's in-flight message table

	// itemTimer
	node  int
	timer timerKind
	gen   uint64 // timer generation; a fire is ignored if it is stale
}

// itemHeap is a min-heap of items ordered by (fireTick, seq). It never compares
// payload fields, so ordering can never depend on message contents — only on
// deterministic scheduling order.
type itemHeap []item

func (h itemHeap) Len() int { return len(h) }
func (h itemHeap) Less(i, j int) bool {
	if h[i].fireTick != h[j].fireTick {
		return h[i].fireTick < h[j].fireTick
	}
	return h[i].seq < h[j].seq
}
func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *itemHeap) Push(x any)   { *h = append(*h, x.(item)) }
func (h *itemHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Clock is the virtual logical clock: a monotonically increasing tick counter
// plus a priority queue of future items. There is NO wall-clock anywhere in the
// simulator — "time" is exactly this counter, advanced by the main loop.
type Clock struct {
	tick uint64
	seq  uint64
	pq   itemHeap
}

// NewClock returns a clock at tick 0 with an empty queue.
func NewClock() *Clock {
	c := &Clock{}
	heap.Init(&c.pq)
	return c
}

// Now returns the current tick.
func (c *Clock) Now() uint64 { return c.tick }

// setTick advances the clock to t. The main loop drives this forward one tick
// at a time; t must not move backwards (asserted by the caller's loop shape).
func (c *Clock) setTick(t uint64) { c.tick = t }

// schedule enqueues it to fire at fireTick, stamping it with the next seq so
// items sharing a tick keep a total, reproducible order.
func (c *Clock) schedule(fireTick uint64, it item) {
	it.fireTick = fireTick
	it.seq = c.seq
	c.seq++
	heap.Push(&c.pq, it)
}

// due reports whether the earliest queued item is ready to fire at the current
// tick (fireTick <= now).
func (c *Clock) due() bool {
	return len(c.pq) > 0 && c.pq[0].fireTick <= c.tick
}

// pop removes and returns the earliest item. Callers guard with due().
func (c *Clock) pop() item { return heap.Pop(&c.pq).(item) }

// empty reports whether the queue holds no items.
func (c *Clock) empty() bool { return len(c.pq) == 0 }
