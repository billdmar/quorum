package sim

import (
	"strconv"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
)

// Params fully describe one simulation run. A run is a pure function of these
// plus the frozen config, so (Seed, Schedule, ClusterSize) with fixed op/tick
// budgets reproduces byte-identically — the determinism gate rests on this.
type Params struct {
	Seed        uint64
	Schedule    config.ScheduleName
	ClusterSize int
	// NumClientOps caps how many client operations the workload issues. The run
	// ends once they have all resolved (committed or timed out) OR MaxTicks is
	// reached, whichever comes first — so a run always terminates even if a
	// pathological fault trajectory stalls progress.
	NumClientOps int
	MaxTicks     uint64
	// NumClients is the count of closed-loop clients (concurrency). Defaults to a
	// small value derived from cluster size when zero.
	NumClients int
}

// Simulator is the single-threaded, quantized-time driver of a whole cluster.
// It owns all nondeterminism (RNG, virtual clock, faults, client workload) and
// drives the pure cores purely through Events/Effects. After every core step it
// assembles a check.ClusterView and invokes OnStep, and it accumulates a
// check.History of client ops — the two hooks  layers invariant monitoring
// and Porcupine linearizability checking onto at .
type Simulator struct {
	params  Params
	sched   config.Schedule
	rng     *RNG
	clock   *Clock
	net     *Network
	nodes   []*node
	clients *clientSet
	trace   *traceHash
	history check.History

	nextRef        uint64
	stepNum        uint64
	crashPPT       uint32
	restartMaxSpan uint32

	// OnStep, if set, is called with a fresh ClusterView after EVERY core step.
	//  attaches the invariant Monitor here at . Nil in the bare sim.
	OnStep func(check.ClusterView)
}

// NewSimulator builds a cluster for the given params. It constructs nodes with
// sorted, stable NodeIDs ("n0".."n{k-1}"), wires the network and workload, and
// arms each node's initial election timer — mirroring a driver that must arm a
// timer at startup since New emits no effects.
func NewSimulator(p Params) *Simulator {
	// Fill unset budgets from the registry's per-schedule table so adversarial
	// schedules get the tick/op budget that makes their histories decidable,
	// without any per-seed randomness (a run stays a pure function of its Params).
	budget := config.BudgetFor(p.Schedule)
	if p.MaxTicks == 0 {
		p.MaxTicks = budget.MaxTicks
	}
	if p.NumClientOps == 0 {
		p.NumClientOps = budget.NumClientOps
	}
	if p.NumClients == 0 {
		p.NumClients = p.ClusterSize // one client per node is a sane default
	}
	sched := config.Schedules[p.Schedule]
	rng := NewRNG(p.Seed)
	clock := NewClock()

	ids := make([]core.NodeID, p.ClusterSize)
	for i := range ids {
		ids[i] = core.NodeID("n" + strconv.Itoa(i))
	}
	s := &Simulator{
		params:         p,
		sched:          sched,
		rng:            rng,
		clock:          clock,
		trace:          newTraceHash(),
		history:        check.History{Seed: p.Seed, Schedule: string(p.Schedule)},
		crashPPT:       sched.Params.CrashPPT,
		restartMaxSpan: sched.Params.RestartMaxSpan,
	}
	s.net = NewNetwork(p.ClusterSize, rng, clock, paramsView{
		dropPPT:          sched.Params.DropPPT,
		dupPPT:           sched.Params.DupPPT,
		reorderPPT:       sched.Params.ReorderPPT,
		maxDelay:         sched.Params.MaxDelay,
		partitionPPT:     sched.Params.PartitionPPT,
		partitionMaxSpan: sched.Params.PartitionMaxSpan,
		asymmetric:       sched.Params.Asymmetric,
	})

	s.nodes = make([]*node, p.ClusterSize)
	for i := range s.nodes {
		peers := make([]core.NodeID, 0, p.ClusterSize-1)
		for j, id := range ids {
			if j != i {
				peers = append(peers, id)
			}
		}
		cfg := core.Config{
			Self: ids[i], Peers: peers,
			ElectionTimeoutMinTicks: config.ElectionTimeoutMinTicks,
			ElectionTimeoutMaxTicks: config.ElectionTimeoutMaxTicks,
			HeartbeatTicks:          config.HeartbeatTicks,
		}
		s.nodes[i] = newNode(i, cfg, rng, sched.Params.DiskFaultPPT)
	}
	s.clients = newClientSet(p.NumClients, p.NumClientOps)

	// Arm each node's initial election timer so the cluster can bootstrap.
	for i := range s.nodes {
		s.armElection(s.nodes[i])
	}
	return s
}

// Run executes the simulation to completion and returns the recorded history.
// The loop is strictly serial: advance the tick, evolve partitions, run the
// workload, then drain every item due at this tick — no goroutines, so the only
// source of ordering is the deterministic clock/RNG.
func (s *Simulator) Run() check.History {
	for s.clock.Now() <= s.params.MaxTicks {
		now := s.clock.Now()
		s.net.stepPartition()
		s.maybeCrash(now)
		s.clients.tick(s, now)

		// Drain all items due at this tick. Newly scheduled items always fire at
		// a strictly later tick (min one-tick latency, timers ≥1 tick), so this
		// inner loop terminates.
		for s.clock.due() {
			s.fire(s.clock.pop())
		}
		if s.finished() {
			break
		}
		s.clock.setTick(now + 1)
	}
	return s.history
}

// finished reports whether the workload is fully resolved: every client op has
// either committed or timed out, and none remain pending.
func (s *Simulator) finished() bool {
	return s.clients.issued >= s.clients.cap && len(s.clients.pending) == 0
}

// fire dispatches one due clock item: a message delivery, a timer firing, or a
// crashed node's restart.
func (s *Simulator) fire(it item) {
	switch it.kind {
	case itemDeliver:
		s.deliver(it)
	case itemTimer:
		s.fireTimer(it)
	case itemRestart:
		s.doRestart(it.node)
	}
}

// deliver hands a message to its destination node as an EventDeliver, unless the
// node is down (a crashed node drops in-flight messages).
func (s *Simulator) deliver(it item) {
	n := s.nodes[it.node]
	if n.crashed {
		return
	}
	m := s.net.message(it.msgIdx)
	s.stepNode(n, core.Event{Type: core.EventDeliver, Msg: m})
}

// fireTimer delivers a timer's Event to its node, ignoring stale generations
// (a reset or crash bumped the generation after this fire was scheduled).
func (s *Simulator) fireTimer(it item) {
	n := s.nodes[it.node]
	if n.crashed {
		return
	}
	switch it.timer {
	case timerElection:
		if it.gen != n.electionGen {
			return
		}
		s.stepNode(n, core.Event{Type: core.EventTickElection})
	case timerHeartbeat:
		if it.gen != n.heartbeatGen {
			return
		}
		s.stepNode(n, core.Event{Type: core.EventTickHeartbeat})
	}
}

// stepNode is the single choke point through which every event reaches a core.
// It folds the event into the trace hash, steps the core, executes the returned
// effects, folds the post-step node summary, then assembles and publishes the
// ClusterView. Routing every event through here is what makes the trace hash a
// faithful fingerprint of the entire run.
func (s *Simulator) stepNode(n *node, ev core.Event) {
	if n.crashed || n.rc == nil {
		return
	}
	s.trace.event(s.clock.Now(), n.idx, ev)
	effs := n.rc.Step(ev)
	s.execEffects(n, effs)
	s.trace.step(n.idx, n.view())
	s.stepNum++
	if s.OnStep != nil {
		s.OnStep(s.clusterView())
	}
}

// execEffects executes a core's returned effects in order, honoring the
// persist-before-send contract implicitly (the core emits persistence effects
// before the sends that depend on them; we execute in that same order). A disk
// fault on a persistence effect crashes the node immediately and abandons the
// remaining effects — modeling a process that died mid-batch at the fsync
// boundary; recovery replays from durable state.
func (s *Simulator) execEffects(n *node, effs []core.Effect) {
	for _, e := range effs {
		switch e.Type {
		case core.EffectSend:
			to := s.indexOf(e.Msg.To)
			if to >= 0 {
				s.net.send(n.idx, to, e.Msg)
			}
		case core.EffectPersistHardState:
			if n.persistHardState(e.HardState) {
				s.crashNode(n)
				return
			}
		case core.EffectPersistLog:
			if n.persistLog(e.FromIndex, e.Entries) {
				s.crashNode(n)
				return
			}
		case core.EffectApply:
			results := n.apply(e.Committed)
			for i, ce := range e.Committed {
				if cid, seq, ok := decodeClientSeq(ce.Command); ok {
					s.clients.onWriteApplied(s, cid, seq, results[i], s.clock.Now())
				}
			}
			// Deterministic compaction trigger: after applying, snapshot+compact if
			// the applied prefix has grown past the registry threshold.
			if n.maybeCompact() {
				s.crashNode(n)
				return
			}
		case core.EffectResetElectionTimer:
			s.armElection(n)
		case core.EffectResetHeartbeatTimer:
			s.armHeartbeat(n)
		case core.EffectRejectProposal:
			// Redirection is handled by the client's retry cadence; a rejected read
			// is resolved so the client can retry it against a new leader.
			s.clients.onRejected(s, e.Ref, s.clock.Now())
		case core.EffectReadIndexReady:
			// The core guarantees the state machine has applied through e.ReadIndex,
			// so reading n.kv now yields a linearizable snapshot at the read point.
			s.clients.onReadReady(s, e.Ref, n, s.clock.Now())
		case core.EffectSendSnapshot:
			to := s.indexOf(e.Msg.To)
			if to >= 0 {
				msg := e.Msg
				msg.LastIncludedIndex = e.SnapIndex
				msg.LastIncludedTerm = e.SnapTerm
				msg.SnapshotData = n.snapshotBytes()
				s.net.send(n.idx, to, msg)
			}
		case core.EffectInstallSnapshot:
			if n.installSnapshot(e.SnapIndex, e.SnapTerm, e.SnapData) {
				s.crashNode(n)
				return
			}
		case core.EffectConfigChanged:
			// Membership changes (P6) are never emitted by the sim's fixed-cluster
			// workload, so this is unreachable here; a dedicated membership harness
			// would react to it (add/remove sim nodes). No-op for the fixed sweep.
		}
	}
}

// armElection arms a fresh election timer with a randomized duration in the
// configured window, bumping the generation so any previously-armed election
// fire for this node becomes stale (honoring "a reset cancels the prior timer").
func (s *Simulator) armElection(n *node) {
	n.electionGen++
	d := s.rng.between(config.ElectionTimeoutMinTicks, config.ElectionTimeoutMaxTicks)
	s.clock.schedule(s.clock.Now()+uint64(d), item{
		kind: itemTimer, node: n.idx, timer: timerElection, gen: n.electionGen,
	})
}

// armHeartbeat arms the leader heartbeat timer at the fixed heartbeat period.
func (s *Simulator) armHeartbeat(n *node) {
	n.heartbeatGen++
	s.clock.schedule(s.clock.Now()+config.HeartbeatTicks, item{
		kind: itemTimer, node: n.idx, timer: timerHeartbeat, gen: n.heartbeatGen,
	})
}

// maybeCrash applies per-node crash faults for the current tick: each running
// node may crash (CrashPPT), and each crash schedules a restart within
// RestartMaxSpan ticks. Draws are only consumed when CrashPPT>0, so fault-free
// schedules' RNG streams are untouched. Nodes are visited in index order.
func (s *Simulator) maybeCrash(now uint64) {
	if s.crashPPT == 0 {
		return
	}
	for _, n := range s.nodes {
		if n.crashed {
			continue
		}
		if s.rng.chance(s.crashPPT) {
			s.crashNode(n)
			span := uint64(1)
			if s.restartMaxSpan > 0 {
				span = uint64(s.rng.between(1, s.restartMaxSpan))
			}
			s.clock.schedule(now+span, item{kind: itemRestart, node: n.idx})
		}
	}
}

// crashNode kills a node and drops any client op that was outstanding on it as
// a proposal is fire-and-forget; the client's retry/timeout logic recovers.
func (s *Simulator) crashNode(n *node) { n.crash() }

// doRestart brings a crashed node back up and re-arms its election timer so it
// can rejoin elections. A node crashed again before its restart fires stays
// down until a fresh restart is scheduled (cannot happen: only one restart is
// scheduled per crash, and a node cannot re-crash while down).
func (s *Simulator) doRestart(idx int) {
	n := s.nodes[idx]
	if !n.crashed {
		return
	}
	if n.restart() {
		s.armElection(n)
	}
}

// findLeader returns a node that currently believes itself leader, or nil. When
// several claim leadership (transiently, across terms) the lowest-index one is
// chosen for determinism. This is a best-effort client router, not a safety
// oracle — the invariant monitors, not this, judge election safety.
func (s *Simulator) findLeader() *node {
	for _, n := range s.nodes {
		if !n.crashed && n.rc != nil && n.rc.Role() == core.Leader {
			return n
		}
	}
	return nil
}

// indexOf maps a NodeID back to its stable index, or -1 if unknown.
func (s *Simulator) indexOf(id core.NodeID) int {
	for i, n := range s.nodes {
		if n.id == id {
			return i
		}
	}
	return -1
}

// clusterView assembles the read-only snapshot the invariant monitors consume.
func (s *Simulator) clusterView() check.ClusterView {
	cv := check.ClusterView{Step: s.stepNum, Nodes: make([]check.NodeView, len(s.nodes))}
	for i, n := range s.nodes {
		cv.Nodes[i] = n.view()
	}
	return cv
}

// TraceHash returns the run's trace hash. Two runs of identical Params MUST
// return the same value — that equality is the determinism gate.
func (s *Simulator) TraceHash() uint64 { return s.trace.sum() }

// History returns the accumulated client-operation history (also returned by
// Run).  feeds this to Porcupine at .
func (s *Simulator) History() check.History { return s.history }
