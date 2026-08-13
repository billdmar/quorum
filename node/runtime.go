// Package node is the production runtime that drives one pure core.RaftCore
// with real time, real goroutines, and a real TCP transport. It is the second
// driver of the sans-I/O core (the first is the deterministic simulator).
//
// RACE-CLEANLINESS BY DESIGN: exactly ONE goroutine — the core loop — ever
// calls core.Step or touches the core. Every source of input (timer fires,
// inbound network messages, client proposals) is funneled to that goroutine
// through channels, so the pure core is never accessed concurrently. This is
// how the production driver stays `go test -race` clean while the core itself
// remains free of any concurrency primitives.
package node

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/storage"
)

// Transport is the outbound side of the network the runtime needs: the concrete
// *rpc.Transport satisfies it. Inbound messages arrive via the handler the
// caller wires into the transport, which should call Runtime.Deliver.
type Transport interface {
	Send(m core.Message)
}

// ApplyFunc is an optional observer invoked with committed entries (in
// increasing index order) AFTER they are applied to the runtime's kv.Store. It
// is a test/observation hook; the authoritative application state is the
// runtime's own kv.Store (see Read). nil is fine.
type ApplyFunc func([]core.CommittedEntry)

// ReadResult reports the outcome of a linearizable Read. Served is true once the
// core has confirmed leadership (ReadIndex) and the state machine has applied
// through the read index, at which point Value/Found reflect the committed value.
// Served is false if this node was not the leader; LeaderHint names the
// best-known leader (empty if unknown) for client redirection.
type ReadResult struct {
	Served     bool
	Value      string
	Found      bool
	LeaderHint core.NodeID
}

// ProposalResult reports the fate of a Propose call. Accepted is true once the
// proposal has been handed to the leader's log; false means the core rejected
// it because this node is not the leader, in which case LeaderHint names the
// best-known leader (empty if unknown) for client redirection.
type ProposalResult struct {
	Accepted   bool
	LeaderHint core.NodeID
}

// Config parameterizes one runtime. ElectionMin/Max bound the randomized
// election timeout; Heartbeat is the leader's heartbeat period. Real wall-clock
// durations (unlike the core's abstract ticks) because this is the production
// driver.
type Config struct {
	Self        core.NodeID
	Peers       []core.NodeID
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
}

// Runtime drives one RaftCore. Construct with New, wire Deliver into the
// transport's inbound handler, then Start. Stop tears everything down cleanly.
type Runtime struct {
	cfg     Config
	core    core.RaftCore
	store   storage.Storage
	tport   Transport
	applyFn ApplyFunc
	kv      *kv.Store // the application state machine (owned by the core loop)
	rng     *rand.Rand

	// appliedIndex is the highest index applied to kv (core-loop owned); used to
	// decide when to trigger a snapshot+compaction.
	appliedIndex core.Index
	appliedTerm  core.Term

	// Channels funneling all input to the single core loop.
	events        chan core.Event
	proposals     chan proposal
	reads         chan readReq
	configChanges chan configChange

	// Real timers, owned and mutated only by the core loop.
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Client proposal bookkeeping, owned only by the core loop: maps the
	// ClientRef attached to an in-flight EventPropose to the waiting caller's
	// result channel, so a later RejectProposal effect can be routed back.
	nextRef uint64
	pending map[core.ClientRef]chan ProposalResult

	// Pending linearizable reads, owned by the core loop: a read's ClientRef maps
	// to the key it wants and the caller's result channel, resolved when the core
	// emits EffectReadIndexReady (served) or EffectRejectProposal (not leader).
	pendingReads map[core.ClientRef]readReq

	// Lease reads (P6): a driver-owned read lease + a channel for lease fast-path
	// reads that the core loop serves directly from kv (no ReadIndex round). See
	// lease.go. The core stays clock-free; the lease clock lives here.
	lease      readLease
	leaseReads chan readReq

	// status is a race-safe snapshot of observable core state, republished by
	// the core loop after every step. Callers (tests, 's CLI at ) read it
	// via Status() instead of touching the core's accessors directly — those
	// accessors are only safe on the core-loop goroutine.
	statusMu sync.Mutex
	status   Status

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startOnce sync.Once
	stopOnce  sync.Once
}

// proposal carries a client command plus the channel the core loop replies on.
type proposal struct {
	command []byte
	result  chan ProposalResult
}

// readReq carries a linearizable-read request: the key to read and the channel
// the core loop replies on once the read index is confirmed and applied.
type readReq struct {
	key    string
	result chan ReadResult
}

// configChange carries a membership-change request to the core loop.
type configChange struct {
	add    bool
	server core.NodeID
	result chan ProposalResult
}

// Status is a race-safe snapshot of a runtime's observable Raft state. It is
// what external observers (tests, and 's CLI at ) read instead of the
// core's own accessors, which are only safe to call on the core-loop goroutine.
type Status struct {
	Role        core.Role
	Term        core.Term
	Leader      core.NodeID
	CommitIndex core.Index
	LastIndex   core.Index
	Members     []core.NodeID // current voting configuration (P6 membership)
}

// New constructs a Runtime over the given core, storage, transport, and apply
// callback. It does not start any goroutines; call Start. seed seeds the
// election-timeout randomization (production uses a real seed; tests pass a
// fixed one for reproducibility — the RANDOMNESS is legitimate here, only the
// core must be deterministic).
func New(cfg Config, c core.RaftCore, store storage.Storage, tport Transport, applyFn ApplyFunc, seed int64) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		cfg:           cfg,
		core:          c,
		store:         store,
		tport:         tport,
		applyFn:       applyFn,
		kv:            kv.NewStore(),
		rng:           rand.New(rand.NewSource(seed)), //nolint:gosec // not security-sensitive; election jitter only
		events:        make(chan core.Event),
		proposals:     make(chan proposal),
		reads:         make(chan readReq),
		configChanges: make(chan configChange),
		leaseReads:    make(chan readReq),
		pending:       make(map[core.ClientRef]chan ProposalResult),
		pendingReads:  make(map[core.ClientRef]readReq),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Recover re-installs durable state into the core and kv.Store from storage
// BEFORE Start. It loads the snapshot (if any) into the kv.Store, then restores
// the core rebased on the snapshot base with the recovered log tail. Call once,
// before Start; a fresh node with empty storage is a no-op. Returns an error if
// storage cannot be read (a real durability fault the caller must surface).
func (r *Runtime) Recover() error {
	hs, _, err := r.store.LoadHardState()
	if err != nil {
		return err
	}
	snapIndex, snapTerm, data, haveSnap, err := r.store.LoadSnapshot()
	if err != nil {
		return err
	}
	entries, err := r.store.LoadEntries()
	if err != nil {
		return err
	}
	if haveSnap {
		if err := r.kv.Restore(data); err != nil {
			return err
		}
	}
	r.core.Restore(hs, snapIndex, snapTerm, entries)
	r.appliedIndex = snapIndex
	r.appliedTerm = snapTerm
	return nil
}

// Deliver hands an inbound Raft message to the core loop. It is safe for
// concurrent use (transport reader goroutines call it) and never touches the
// core directly — it only enqueues an event. It drops the message if the
// runtime is shutting down, which is fine: Raft tolerates loss.
func (r *Runtime) Deliver(m core.Message) {
	select {
	case r.events <- core.Event{Type: core.EventDeliver, Msg: m}:
	case <-r.ctx.Done():
	}
}

// Propose funnels a client command to the core loop and blocks until the core
// accepts it into the leader's log or rejects it (not leader). It returns early
// if the runtime stops. Note: Accepted means "appended by this leader", not yet
// "committed" — commit is observed via the ApplyFunc.
func (r *Runtime) Propose(command []byte) ProposalResult {
	res := make(chan ProposalResult, 1)
	select {
	case r.proposals <- proposal{command: command, result: res}:
	case <-r.ctx.Done():
		return ProposalResult{}
	}
	select {
	case out := <-res:
		return out
	case <-r.ctx.Done():
		return ProposalResult{}
	}
}

// ChangeConfig submits a single-server membership change (P6) to the core loop:
// add or remove `server` from the voting configuration. It returns once the core
// has accepted (appended the config entry, adopted on append) or rejected it
// (not leader / a change already pending / current-term not yet committed /
// trivial). Accepted means appended by this leader; the change commits under the
// new quorum shortly after (observe via Status/Members convergence). Reuses the
// proposal accept/reject plumbing.
func (r *Runtime) ChangeConfig(add bool, server core.NodeID) ProposalResult {
	res := make(chan ProposalResult, 1)
	select {
	case r.configChanges <- configChange{add: add, server: server, result: res}:
	case <-r.ctx.Done():
		return ProposalResult{}
	}
	select {
	case out := <-res:
		return out
	case <-r.ctx.Done():
		return ProposalResult{}
	}
}

// Read performs a linearizable read of key via the core's ReadIndex path: it
// blocks until the leader confirms leadership and the state machine has applied
// through the read index, then returns the committed value. If this node is not
// the leader it returns Served=false with a LeaderHint for redirection. Returns
// early (Served=false) if the runtime stops.
func (r *Runtime) Read(key string) ReadResult {
	res := make(chan ReadResult, 1)
	select {
	case r.reads <- readReq{key: key, result: res}:
	case <-r.ctx.Done():
		return ReadResult{}
	}
	select {
	case out := <-res:
		return out
	case <-r.ctx.Done():
		return ReadResult{}
	}
}

// Start launches the single core-loop goroutine and arms the initial election
// timer. Idempotent: only the first call has an effect.
func (r *Runtime) Start() {
	r.startOnce.Do(func() {
		r.electionTimer = time.NewTimer(r.randElection())
		r.heartbeatTimer = time.NewTimer(r.cfg.Heartbeat)
		// Heartbeat only matters once we are leader; stop it until then so a
		// follower isn't spammed with heartbeat ticks. drainAndStop leaves the
		// timer stopped with an empty channel, ready for a later reset.
		drainAndStop(r.heartbeatTimer)
		r.wg.Add(1)
		go r.loop()
	})
}

// Stop cancels the runtime, unblocks the core loop, and waits for it to exit.
// It does not close the transport or storage — the caller owns those lifetimes
// ( wires and tears them down at ). Idempotent.
func (r *Runtime) Stop() {
	r.stopOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
		r.electionTimer.Stop()
		r.heartbeatTimer.Stop()
	})
}

// loop is the single goroutine that owns the core. It selects over the timers
// and input channels, translates each into exactly one core.Event, steps the
// core, and executes the returned effects in order. Because only this goroutine
// calls Step and mutates the timers/pending map, no locking around the core is
// needed and the design is race-free.
func (r *Runtime) loop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			r.failPending()
			return
		case <-r.electionTimer.C:
			r.runStep(core.Event{Type: core.EventTickElection})
		case <-r.heartbeatTimer.C:
			r.runStep(core.Event{Type: core.EventTickHeartbeat})
		case ev := <-r.events:
			r.runStep(ev)
		case p := <-r.proposals:
			ref := r.registerProposal(p.result)
			r.runStep(core.Event{Type: core.EventPropose, Ref: ref, Command: p.command})
			// If no RejectProposal effect fired, the leader accepted and
			// appended the entry; resolve the still-pending ref as accepted.
			r.resolveProposal(ref, ProposalResult{Accepted: true, LeaderHint: r.core.Leader()})
		case rq := <-r.reads:
			r.nextRef++
			ref := core.ClientRef(r.nextRef)
			r.pendingReads[ref] = rq
			r.runStep(core.Event{Type: core.EventReadIndex, Ref: ref})
			// EffectReadIndexReady/RejectProposal resolves the read from execute();
			// if neither fired synchronously (multi-node: awaiting heartbeat quorum)
			// the read stays pending until a later ack step releases it.
		case cc := <-r.configChanges:
			ref := r.registerProposal(cc.result)
			r.runStep(core.Event{Type: core.EventChangeConfig, Ref: ref,
				ConfigAdd: cc.add, ConfigServer: cc.server})
			// Accepted = no RejectProposal effect fired; resolve the pending ref.
			r.resolveProposal(ref, ProposalResult{Accepted: true, LeaderHint: r.core.Leader()})
		case rq := <-r.leaseReads:
			// Lease fast path: the caller already verified a valid lease + leader
			// role. Serve directly from kv on this (core-loop) goroutine so kv stays
			// single-writer. We re-check leadership here to close the race where the
			// node stepped down between the caller's check and this receive.
			if r.core.Role() == core.Leader {
				v, found := r.kv.Get(rq.key)
				rq.result <- ReadResult{Served: true, Value: v, Found: found}
			} else {
				rq.result <- ReadResult{Served: false, LeaderHint: r.core.Leader()}
			}
		}
	}
}

// runStep steps the core with ev, executes the resulting effects in order, then
// republishes the observable status snapshot. Reading the core's accessors here
// is safe because runStep only ever runs on the core-loop goroutine.
func (r *Runtime) runStep(ev core.Event) {
	for _, eff := range r.core.Step(ev) {
		r.execute(eff)
	}
	r.publishStatus()
}

// publishStatus copies the core's observable state into the mutex-guarded
// snapshot. Called only from the core loop; read via Status.
func (r *Runtime) publishStatus() {
	role := r.core.Role()
	// Any loss of leadership immediately revokes the read lease — a follower or
	// candidate must never serve a lease read (safety over the fast path).
	if role != core.Leader {
		r.lease.revoke()
	}
	s := Status{
		Role:        role,
		Term:        r.core.Term(),
		Leader:      r.core.Leader(),
		CommitIndex: r.core.CommitIndex(),
		LastIndex:   r.core.LastLogIndex(),
		Members:     r.core.Members(),
	}
	r.statusMu.Lock()
	r.status = s
	r.statusMu.Unlock()
}

// Status returns the latest race-safe snapshot of this node's Raft state,
// republished after every core step. Safe for concurrent use.
func (r *Runtime) Status() Status {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	return r.status
}

// execute performs one effect. Effects are executed in the exact order the core
// returned them, which automatically honors persist-before-send: a
// PersistHardState/PersistLog effect emitted before a Send blocks on fsync (via
// storage) before that Send runs, because this loop is single-threaded and
// never reorders or parallelizes within a batch.
func (r *Runtime) execute(eff core.Effect) {
	switch eff.Type {
	case core.EffectSend:
		r.tport.Send(eff.Msg)
	case core.EffectPersistHardState:
		// A failed durable write is a correctness violation, not something to
		// paper over. There is no safe way to continue, so stop the runtime;
		// the crash-recovery harness (/) exercises the recovery path.
		if err := r.store.SaveHardState(eff.HardState); err != nil {
			r.cancel()
		}
	case core.EffectPersistLog:
		if err := r.store.AppendEntries(eff.FromIndex, eff.Entries); err != nil {
			r.cancel()
		}
	case core.EffectApply:
		r.applyCommitted(eff.Committed)
	case core.EffectResetElectionTimer:
		resetTimer(r.electionTimer, r.randElection())
	case core.EffectResetHeartbeatTimer:
		resetTimer(r.heartbeatTimer, r.cfg.Heartbeat)
	case core.EffectRejectProposal:
		// A rejected ref is either a proposal or a read (both use RejectProposal
		// when this node is not leader); resolve whichever is pending.
		r.resolveProposal(eff.Ref, ProposalResult{Accepted: false, LeaderHint: eff.LeaderHint})
		r.resolveRead(eff.Ref, ReadResult{Served: false, LeaderHint: eff.LeaderHint})
	case core.EffectReadIndexReady:
		// The core guarantees the state machine has applied through eff.ReadIndex,
		// so reading kv now yields a linearizable snapshot at the read point.
		if rq, ok := r.pendingReads[eff.Ref]; ok {
			delete(r.pendingReads, eff.Ref)
			val, found := r.kv.Get(rq.key)
			rq.result <- ReadResult{Served: true, Value: val, Found: found}
		}
	case core.EffectSendSnapshot:
		// The leader asks us to ship a snapshot to a lagging peer; attach the kv
		// snapshot bytes and send.
		msg := eff.Msg
		msg.LastIncludedIndex = eff.SnapIndex
		msg.LastIncludedTerm = eff.SnapTerm
		msg.SnapshotData = r.kv.Snapshot()
		r.tport.Send(msg)
	case core.EffectInstallSnapshot:
		// A received snapshot: durably persist it AND restore the kv.Store, as one
		// ordered step BEFORE the reply Send (persist-before-send). A durability
		// fault here is a correctness violation — stop and recover.
		if err := r.store.SaveSnapshot(eff.SnapIndex, eff.SnapTerm, eff.SnapData); err != nil {
			r.cancel()
			return
		}
		if err := r.kv.Restore(eff.SnapData); err != nil {
			r.cancel()
			return
		}
		r.appliedIndex = eff.SnapIndex
		r.appliedTerm = eff.SnapTerm
	case core.EffectConfigChanged:
		// The voting configuration changed (P6). The transport is constructed with
		// every potential member's address, so routes already exist; the change is
		// informational for the runtime (the core has already updated its quorum).
		// A production system would (un)register transport routes here.
	}
}

// applyCommitted applies newly committed entries to the kv.Store (exactly-once
// via its dedup table), advances the applied index/term, notifies the optional
// observer, and triggers a snapshot+compaction when the applied prefix has grown
// past the registry threshold. Runs only on the core loop.
func (r *Runtime) applyCommitted(committed []core.CommittedEntry) {
	if len(committed) == 0 {
		return
	}
	for _, e := range committed {
		r.kv.Apply(e)
		r.appliedIndex = e.Index
		r.appliedTerm = e.Term
	}
	if r.applyFn != nil {
		r.applyFn(committed)
	}
	r.maybeCompact()
}

// maybeCompact snapshots the kv.Store and compacts the log when the applied
// prefix has grown LogCompactionThreshold entries past the core's snapshot base.
// A pure integer trigger (no clock/byte-size), matching the simulator driver.
func (r *Runtime) maybeCompact() {
	if r.appliedIndex < r.core.SnapBase()+config.LogCompactionThreshold {
		return
	}
	data := r.kv.Snapshot()
	if err := r.store.SaveSnapshot(r.appliedIndex, r.appliedTerm, data); err != nil {
		r.cancel()
		return
	}
	r.core.CompactTo(r.appliedIndex, r.appliedTerm)
}

// resolveRead delivers a read result to a waiting Read caller and clears the
// pending entry. Only called from the core loop.
func (r *Runtime) resolveRead(ref core.ClientRef, res ReadResult) {
	if rq, ok := r.pendingReads[ref]; ok {
		delete(r.pendingReads, ref)
		rq.result <- res
	}
}

// registerProposal records a pending proposal under a fresh ClientRef and
// returns the ref. Called only from the core loop, so nextRef/pending need no
// lock. An accepted proposal emits no ref-bearing effect, so the core loop
// resolves the still-pending ref as accepted right after the step (see loop).
func (r *Runtime) registerProposal(result chan ProposalResult) core.ClientRef {
	r.nextRef++
	ref := core.ClientRef(r.nextRef)
	r.pending[ref] = result
	return ref
}

// resolveProposal delivers a result to a waiting Propose caller and clears the
// pending entry. Only called from the core loop.
func (r *Runtime) resolveProposal(ref core.ClientRef, res ProposalResult) {
	if ch, ok := r.pending[ref]; ok {
		delete(r.pending, ref)
		ch <- res
	}
}

// failPending resolves every still-waiting proposal with a not-accepted result
// at shutdown, so no Propose caller blocks forever. Called from the core loop
// on ctx cancellation.
func (r *Runtime) failPending() {
	for ref, ch := range r.pending {
		delete(r.pending, ref)
		select {
		case ch <- ProposalResult{}:
		default:
		}
	}
	for ref, rq := range r.pendingReads {
		delete(r.pendingReads, ref)
		select {
		case rq.result <- ReadResult{}:
		default:
		}
	}
}

// randElection returns a fresh randomized election timeout in [ElectionMin,
// ElectionMax]. Randomizing the timeout is the standard Raft technique for
// avoiding perpetual split votes; the randomness lives here in the driver, not
// in the pure core.
func (r *Runtime) randElection() time.Duration {
	lo := r.cfg.ElectionMin
	hi := r.cfg.ElectionMax
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(r.rng.Int63n(int64(hi-lo)))
}

// resetTimer stops t (draining a pending fire so it can't be observed after the
// reset) and re-arms it for d. Called only from the core loop, which owns the
// timer, so the drain is race-free.
func resetTimer(t *time.Timer, d time.Duration) {
	drainAndStop(t)
	t.Reset(d)
}

// drainAndStop stops t and drains any already-fired value from its channel so a
// stale tick is never delivered after the timer is (re)armed or stopped.
func drainAndStop(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
