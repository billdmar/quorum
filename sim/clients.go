package sim

import (
	"encoding/binary"
	"strconv"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
)

// The workload is a set of closed-loop clients, each a distinct Raft client
// session (a stable ClientID and a monotonically increasing SeqNum). Each client
// holds at most one outstanding operation and drives a mix of writes (Put /
// Append / CAS, replicated via EventPropose) and linearizable reads (Get, via
// EventReadIndex). A write is retried by RE-PROPOSING THE IDENTICAL command
// bytes (same ClientID+SeqNum) so the KV dedup table applies it exactly once
// even across leader changes — this is what the exactly-once property is checked
// against. A read is retried by re-issuing EventReadIndex. Every operation
// records a check.HistoryEvent invoke and (on resolution) a response carrying
// the REAL KV output, so Porcupine checks the actual KV model. Everything is
// driven off the simulator's single RNG / virtual clock, so the whole client
// trajectory is reproducible from the seed.
const (
	// keyspace bounds distinct keys so concurrent clients contend on shared
	// registers (interesting for linearizability) rather than private keys.
	keyspace = 6
	// retryIntervalTicks: ticks a client waits before re-driving an outstanding
	// op. A little above HeartbeatTicks so a healthy leader usually resolves it
	// first, yet a lost op is retried well within an election window.
	retryIntervalTicks = 6
	// opTimeoutTicks: after this long with no resolution the client abandons the
	// op and records an Unknown response (may-or-may-not have taken effect), then
	// moves on. Scaled generously so adversarial schedules still resolve most ops.
	opTimeoutTicks = 400
)

// opKind is the workload's operation kind (a superset view over check.OpKind).
type reqKind uint8

const (
	reqGet reqKind = iota
	reqPut
	reqAppend
	reqCAS
)

// opState is one in-flight client operation.
type opState struct {
	opID     uint64 // unique per operation; ties invoke to response in the history
	client   int
	kind     reqKind
	clientID uint64 // Raft session id (stable per client)
	seqNum   uint64 // session sequence number (stable across retries of THIS op)
	key      string
	value    string // Put/Append/CAS new value
	compare  string // CAS expected value
	// ref is the ClientRef of the CURRENT in-flight EventReadIndex (reads only),
	// so an EffectReadIndexReady/RejectProposal can be matched back to this op.
	ref        core.ClientRef
	invokeTick uint64
	lastDrive  uint64
}

// clientState is one closed-loop client session.
type clientState struct {
	clientID uint64
	nextSeq  uint64
	curOpID  uint64 // 0 == idle
}

// clientSet holds all client sessions and the pending-op tables. pending is keyed
// by opID; refToOp maps an outstanding read's ClientRef back to its opID. Both
// are looked up only by key (never range-iterated for logic), so no map-order
// nondeterminism leaks into the trace.
type clientSet struct {
	clients  []*clientState
	pending  map[uint64]*opState
	refToOp  map[core.ClientRef]uint64
	nextOpID uint64
	issued   int
	cap      int
}

func newClientSet(numClients, cap int) *clientSet {
	cs := &clientSet{
		clients:  make([]*clientState, numClients),
		pending:  make(map[uint64]*opState),
		refToOp:  make(map[core.ClientRef]uint64),
		nextOpID: 1, // 0 is the idle sentinel
		cap:      cap,
	}
	for i := range cs.clients {
		cs.clients[i] = &clientState{clientID: uint64(i + 1)}
	}
	return cs
}

// tick advances every client for the current tick, iterating in slice order.
func (cs *clientSet) tick(s *Simulator, now uint64) {
	for i, c := range cs.clients {
		if c.curOpID == 0 {
			cs.startOp(s, i, c, now)
			continue
		}
		op := cs.pending[c.curOpID]
		if op == nil { // defensive: already resolved
			c.curOpID = 0
			continue
		}
		if now-op.invokeTick > opTimeoutTicks {
			s.recordUnknown(op, now)
			cs.resolve(op)
			c.curOpID = 0
			continue
		}
		if now-op.lastDrive >= retryIntervalTicks {
			if s.drive(op) {
				op.lastDrive = now
			}
		}
	}
}

// startOp begins a fresh op for an idle client session, if the cap allows.
func (cs *clientSet) startOp(s *Simulator, i int, c *clientState, now uint64) {
	if cs.issued >= cs.cap {
		return
	}
	opID := cs.nextOpID
	cs.nextOpID++
	cs.issued++
	c.nextSeq++

	// Choose an op kind deterministically from the RNG. Read-heavy mix with a
	// spread of write kinds so the KV model (Get/Put/Append/CAS) is exercised.
	var kind reqKind
	switch s.rng.Intn(10) {
	case 0, 1, 2, 3:
		kind = reqGet
	case 4, 5, 6:
		kind = reqPut
	case 7, 8:
		kind = reqAppend
	default:
		kind = reqCAS
	}
	key := "k" + strconv.FormatUint(uint64(s.rng.Intn(keyspace)), 10)
	op := &opState{
		opID:       opID,
		client:     i,
		kind:       kind,
		clientID:   c.clientID,
		seqNum:     c.nextSeq,
		key:        key,
		value:      "c" + strconv.FormatUint(c.clientID, 10) + "-" + strconv.FormatUint(opID, 10),
		compare:    "", // CAS expects empty→set on first touch; contention makes it interesting
		invokeTick: now,
	}
	cs.pending[opID] = op
	c.curOpID = opID
	s.recordInvoke(op, now)
	if s.drive(op) {
		op.lastDrive = now
	}
}

// resolve removes an op from the pending tables (idempotent).
func (cs *clientSet) resolve(op *opState) {
	delete(cs.pending, op.opID)
	if op.ref != 0 {
		delete(cs.refToOp, op.ref)
	}
	if cs.clients[op.client].curOpID == op.opID {
		cs.clients[op.client].curOpID = 0
	}
}

// onWriteApplied resolves the write op identified by (clientID, seqNum) when its
// command is applied, recording the real KV Result as the response. Called from
// the apply path. Idempotent: a duplicate apply for an already-resolved op finds
// nothing pending.
func (cs *clientSet) onWriteApplied(s *Simulator, clientID, seqNum uint64, res kv.Result, now uint64) {
	// Find the pending op for this session+seq. Sessions issue seq in order and
	// only one op is outstanding per client, so a linear scan of the small
	// pending set is deterministic and cheap.
	for _, c := range cs.clients {
		if c.clientID != clientID {
			continue
		}
		if c.curOpID == 0 {
			return
		}
		op := cs.pending[c.curOpID]
		if op == nil || op.seqNum != seqNum || op.kind == reqGet {
			return
		}
		s.recordWriteResponse(op, res, now)
		cs.resolve(op)
		return
	}
}

// onReadReady resolves a read op whose ReadIndex is confirmed: read the value
// from the node's kv.Store now (the core guarantees applied>=readIndex) and
// record it as the response.
func (cs *clientSet) onReadReady(s *Simulator, ref core.ClientRef, n *node, now uint64) {
	opID, ok := cs.refToOp[ref]
	if !ok {
		return
	}
	op := cs.pending[opID]
	if op == nil {
		delete(cs.refToOp, ref)
		return
	}
	val, _ := n.kv.Get(op.key)
	s.recordReadResponse(op, val, now)
	cs.resolve(op)
}

// onRejected handles a rejected proposal/read (not leader): the op stays pending
// and will be re-driven to the (hopefully new) leader on the next retry tick.
// We only clear the stale ref mapping for reads so the next drive gets a fresh one.
func (cs *clientSet) onRejected(s *Simulator, ref core.ClientRef, now uint64) {
	if opID, ok := cs.refToOp[ref]; ok {
		delete(cs.refToOp, ref)
		if op := cs.pending[opID]; op != nil {
			op.ref = 0
		}
	}
}

// --- command encoding (kv.Command wire bytes carried opaquely by the core) ---

// decodeClientSeq extracts (clientID, seqNum) from an applied command so the
// apply path can resolve the issuing op. Returns ok=false for a no-op/undecodable
// command.
func decodeClientSeq(cmd []byte) (clientID, seqNum uint64, ok bool) {
	if len(cmd) < 17 {
		return 0, 0, false
	}
	clientID = binary.BigEndian.Uint64(cmd[0:8])
	seqNum = binary.BigEndian.Uint64(cmd[8:16])
	return clientID, seqNum, true
}

// --- Simulator-side driving + history recording ---

// drive sends the op to the current leader: a write via EventPropose (identical
// bytes on every retry so the KV dedup applies once), a read via EventReadIndex.
// Returns false when no leader is observable (leave the op outstanding to retry).
func (s *Simulator) drive(op *opState) bool {
	leader := s.findLeader()
	if leader == nil {
		return false
	}
	ref := core.ClientRef(s.nextRef)
	s.nextRef++
	if op.kind == reqGet {
		op.ref = ref
		s.clients.refToOp[ref] = op.opID
		s.stepNode(leader, core.Event{Type: core.EventReadIndex, Ref: ref})
		return true
	}
	cmd := kv.Command{
		ClientID: op.clientID, SeqNum: op.seqNum,
		Op: op.checkKind(), Key: op.key, Value: op.value, CompareValue: op.compare,
	}
	s.stepNode(leader, core.Event{Type: core.EventPropose, Ref: ref, Command: cmd.Encode()})
	return true
}

// checkKind maps the workload op kind to the check.OpKind the kv.Command uses.
func (op *opState) checkKind() check.OpKind {
	switch op.kind {
	case reqPut:
		return check.OpPut
	case reqAppend:
		return check.OpAppend
	case reqCAS:
		return check.OpCAS
	default:
		return check.OpGet
	}
}

func (s *Simulator) recordInvoke(op *opState, now uint64) {
	s.history.Events = append(s.history.Events, check.HistoryEvent{
		OpID: op.opID, Client: op.clientID, Stage: check.StageInvoke, Stamp: now,
		Kind: op.checkKind(), Key: op.key, Value: op.value, CompareValue: op.compare,
	})
}

func (s *Simulator) recordWriteResponse(op *opState, res kv.Result, now uint64) {
	s.history.Events = append(s.history.Events, check.HistoryEvent{
		OpID: op.opID, Client: op.clientID, Stage: check.StageResponse, Stamp: now,
		Kind: op.checkKind(), Output: res.Value, OK: res.OK,
	})
}

func (s *Simulator) recordReadResponse(op *opState, val string, now uint64) {
	s.history.Events = append(s.history.Events, check.HistoryEvent{
		OpID: op.opID, Client: op.clientID, Stage: check.StageResponse, Stamp: now,
		Kind: check.OpGet, Output: val, OK: true,
	})
}

func (s *Simulator) recordUnknown(op *opState, now uint64) {
	s.history.Events = append(s.history.Events, check.HistoryEvent{
		OpID: op.opID, Client: op.clientID, Stage: check.StageResponse, Stamp: now,
		Kind: op.checkKind(), Unknown: true,
	})
}
