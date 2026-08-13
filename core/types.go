// Package core contains the pure, deterministic Raft state machine and the
// frozen contracts that every driver (the deterministic simulator and the
// real-TCP production runtime) is built against.
//
// PURITY CONTRACT (build-breaking if violated): nothing in this package may
// touch time, I/O, goroutines, or randomness. The Raft core is a pure
// function of (current in-memory state, input event): identical state plus an
// identical event always yields an identical sequence of effects and an
// identical next state. All nondeterminism — timer firing, message arrival,
// randomized election-timeout durations, client requests, crashes — enters the
// core exclusively as Events. All side effects — sending messages, persisting
// durable state, applying committed commands, arming timers — leave the core
// exclusively as Effects, which a driver executes. This "sans-I/O" split is
// what makes deterministic simulation testing possible.
package core

// NodeID identifies a cluster member. Strings mirror real node names/addresses
// in production mode. The empty string is the "no node" sentinel (e.g. no vote
// cast yet, no known leader). The core NEVER ranges over maps of NodeID to
// build ordered effects; peers are held in a sorted slice so effect ordering
// is deterministic.
type NodeID string

// None is the sentinel NodeID meaning "no node" (no vote, no leader hint).
const None NodeID = ""

// Term is a Raft term number. Monotonically non-decreasing per node.
type Term uint64

// Index is a 1-based Raft log index. Index 0 means "before the first entry".
type Index uint64

// ClientRef is an opaque token a driver attaches to a proposal or read request
// so the core can refer back to it (e.g. to reject a proposal when not leader)
// without understanding client identity or command contents. Opaque to core.
type ClientRef uint64

// LogEntry is a single replicated log entry. Command is an opaque byte slice;
// the core never interprets it — interpretation is the application state
// machine's job (see StateMachine). Index is stored redundantly for validation
// and trace clarity.
//
// A LogEntry with a nil/empty Command is a LEADER NO-OP: when a node becomes
// leader it appends one no-op entry in its current term. Committing that
// current-term entry is what lets the leader safely mark prior-term entries
// committed (the Figure-8 commit rule). Application state machines MUST ignore
// no-op entries when applying — use IsNoOp to detect them.
//
// Kind classifies the entry (P6 membership changes). KindNormal (the zero value,
// so every pre-existing entry is unchanged) is a normal client/no-op entry whose
// Command the core never interprets. KindConfig is a single-server membership
// change: the core interprets Command as a ConfigChange and adopts it on append.
type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte
	Kind    EntryKind
}

// EntryKind classifies a LogEntry. Append-only: KindNormal must stay 0 so every
// existing entry (and every existing trace hash) is unaffected.
type EntryKind uint8

const (
	KindNormal EntryKind = iota // normal client command or leader no-op
	KindConfig                  // single-server membership change (P6)
)

// IsNoOp reports whether a command is a leader no-op (nil/empty), which
// application state machines must ignore when applying committed entries.
func IsNoOp(command []byte) bool { return len(command) == 0 }

// CommittedEntry is a log entry the core has determined is committed and hands
// to the driver (via EffectApply) to feed the application state machine, in
// strictly increasing index order. Kind lets the application skip non-Normal
// entries (a KindConfig membership change must not be applied as a KV command).
type CommittedEntry struct {
	Index   Index
	Term    Term
	Command []byte
	Kind    EntryKind
}

// HardState is the subset of Raft state that MUST be durable (fsync'd) before
// the node acts on it in a way another node can observe: the current term and
// the vote cast in that term. Persisted via EffectPersistHardState.
type HardState struct {
	Term     Term
	VotedFor NodeID
}

// Role is a node's current Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// MessageType tags a Raft RPC. A flat message struct (rather than an interface
// hierarchy) is deliberate: it serializes trivially and deterministically for
// both the TCP wire and the simulator's trace hash, and compares cleanly in
// table-driven tests. Fields not relevant to a given type are zero-valued.
type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	// MsgInstallSnapshot carries a snapshot to a follower that has fallen behind
	// the leader's compacted log prefix (snapshot/compaction). The wire shape was
	// declared at  to keep it frozen; it is used by handleInstallSnapshot.
	MsgInstallSnapshot
	MsgInstallSnapshotResp
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResp:
		return "RequestVoteResp"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResp:
		return "AppendEntriesResp"
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResp:
		return "InstallSnapshotResp"
	default:
		return "unknown"
	}
}

// Message is a Raft RPC envelope. From/To carry routing (From doubles as
// candidateId / leaderId). Every field is populated only for the message types
// that use it; see the comments grouping fields by type.
type Message struct {
	From NodeID
	To   NodeID
	Type MessageType
	Term Term

	// RequestVote
	LastLogIndex Index
	LastLogTerm  Term

	// RequestVoteResp
	VoteGranted bool

	// AppendEntries
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index

	// ReadSeq correlates a ReadIndex leadership-confirmation round: a leader
	// stamps the current read-confirmation sequence on the AppendEntries/heartbeat
	// it broadcasts, and a follower echoes it back on AppendEntriesResp. A leader
	// only counts an ack toward confirming a pending read if the echoed ReadSeq is
	// at least the read's round — this is what makes ReadIndex sound under an
	// asymmetric partition (a deposed leader cannot confirm reads with acks that
	// predate its loss of quorum). Zero when no read confirmation is outstanding.
	ReadSeq uint64

	// AppendEntriesResp
	Success       bool
	ConflictIndex Index // fast-backup hint: first index of the conflicting term
	ConflictTerm  Term  // fast-backup hint: the conflicting term (0 if none)
	MatchIndex    Index // follower's highest matching index on success

	// InstallSnapshot () — carry the snapshot's covered prefix and bytes;
	// consumed by handleInstallSnapshot.
	LastIncludedIndex Index
	LastIncludedTerm  Term
	SnapshotData      []byte
}

// EventType tags an input to the core's Step function.
type EventType uint8

const (
	// EventTickElection: the election timer fired. A follower/candidate that
	// receives this starts (or restarts) an election. The DRIVER owns timers
	// and their randomized durations; the core only reacts to the firing.
	EventTickElection EventType = iota
	// EventTickHeartbeat: the heartbeat timer fired. A leader broadcasts
	// AppendEntries (empty or with pending entries) to all peers.
	EventTickHeartbeat
	// EventDeliver: a Raft RPC arrived (Msg is populated).
	EventDeliver
	// EventPropose: a client proposes a command (Command + Ref populated).
	EventPropose
	// EventReadIndex: a client requests a linearizable read (Ref populated).
	// Handled by handleReadIndex, which runs the ReadIndex leadership-confirmation
	// round before the read is served.
	EventReadIndex
	// EventChangeConfig: a single-server membership change request (P6). Populated
	// via the Config* fields. Appended AFTER EventReadIndex so existing numeric
	// event values — folded into existing trace hashes — are unchanged.
	EventChangeConfig
)

func (t EventType) String() string {
	switch t {
	case EventTickElection:
		return "TickElection"
	case EventTickHeartbeat:
		return "TickHeartbeat"
	case EventDeliver:
		return "Deliver"
	case EventPropose:
		return "Propose"
	case EventReadIndex:
		return "ReadIndex"
	case EventChangeConfig:
		return "ChangeConfig"
	default:
		return "unknown"
	}
}

// Event is a single input to Step. Exactly one event is processed per Step
// call. Fields are populated per Type (see EventType docs).
type Event struct {
	Type    EventType
	Msg     Message   // EventDeliver
	Ref     ClientRef // EventPropose, EventReadIndex, EventChangeConfig
	Command []byte    // EventPropose

	// EventChangeConfig: add or remove a single voting server.
	ConfigAdd    bool   // true = add Server, false = remove it
	ConfigServer NodeID // the server to add/remove
}

// ConfigChange is the payload of a KindConfig log entry: a single-server add or
// remove. It is deterministically encoded into LogEntry.Command via
// EncodeConfigChange and decoded via DecodeConfigChange; the core interprets it
// (unlike a KindNormal command), so both drivers and the core agree on the wire.
type ConfigChange struct {
	Add    bool // true = add Server as a voter, false = remove it
	Server NodeID
}

// EffectType tags an output the driver must execute.
type EffectType uint8

const (
	// EffectSend: transmit Msg to Msg.To. The driver MUST NOT execute a Send
	// before completing every durable-persistence effect (PersistHardState,
	// PersistLog) emitted earlier in the same effect batch — Raft correctness
	// depends on "persist before you tell anyone."
	EffectSend EffectType = iota
	// EffectPersistHardState: durably (fsync) store HardState before any later
	// EffectSend in this batch executes.
	EffectPersistHardState
	// EffectPersistLog: truncate the local log so it ends at FromIndex-1, then
	// durably append Entries. Handles both plain appends and conflict
	// overwrites. Must complete before any later EffectSend in this batch.
	EffectPersistLog
	// EffectApply: hand Committed entries (increasing index order) to the
	// application state machine. Emitted only for newly committed entries.
	EffectApply
	// EffectResetElectionTimer: the core changed state such that its election
	// timer should be (re)armed. The driver MUST cancel any previously-armed
	// election timer for this node so no stale EventTickElection is delivered,
	// then arm a fresh one with a randomized duration (seeded in the sim, real
	// randomized timer in production). This driver invariant keeps timer
	// generations out of the core.
	EffectResetElectionTimer
	// EffectResetHeartbeatTimer: (leader) arm/re-arm the heartbeat timer.
	EffectResetHeartbeatTimer
	// EffectRejectProposal: a proposal could not be accepted (this node is not
	// leader). Ref echoes the rejected proposal; LeaderHint is the best-known
	// leader (None if unknown) so the driver can redirect the client.
	EffectRejectProposal
	// EffectReadIndexReady: () a linearizable read identified by Ref may be
	// served once the state machine has applied through ReadIndex.
	EffectReadIndexReady
	// EffectSendSnapshot: () the leader asks the driver to build and send a
	// MsgInstallSnapshot to a lagging peer whose nextIndex has fallen below the
	// leader's first (post-compaction) log index. The core is byte-free: it names
	// the target peer (Msg.To), the snapshot bounds (SnapIndex/SnapTerm), and the
	// current term (Msg.Term); the DRIVER attaches the application snapshot bytes
	// (from its own state-machine snapshot) and transmits. Keeps snapshot payloads
	// out of the pure core.
	EffectSendSnapshot
	// EffectInstallSnapshot: () a follower has accepted a received snapshot. The
	// driver MUST durably persist it (storage.SaveSnapshot) AND restore the
	// application state machine from SnapData, as one ordered step, BEFORE any
	// later EffectSend in this batch (the InstallSnapshot reply). SnapIndex/
	// SnapTerm are the snapshot bounds; SnapData is the opaque application bytes
	// the core carries through untouched (purity preserved).
	EffectInstallSnapshot
	// EffectConfigChanged: (P6) the core's voting configuration changed (a
	// KindConfig entry was appended or reverted). Members carries the new voter
	// set so the driver can add/unwire transport routes / sim nodes. Appended last
	// so existing effect enum values are unchanged.
	EffectConfigChanged
)

func (t EffectType) String() string {
	switch t {
	case EffectSend:
		return "Send"
	case EffectPersistHardState:
		return "PersistHardState"
	case EffectPersistLog:
		return "PersistLog"
	case EffectApply:
		return "Apply"
	case EffectResetElectionTimer:
		return "ResetElectionTimer"
	case EffectResetHeartbeatTimer:
		return "ResetHeartbeatTimer"
	case EffectRejectProposal:
		return "RejectProposal"
	case EffectReadIndexReady:
		return "ReadIndexReady"
	case EffectSendSnapshot:
		return "SendSnapshot"
	case EffectInstallSnapshot:
		return "InstallSnapshot"
	case EffectConfigChanged:
		return "ConfigChanged"
	default:
		return "unknown"
	}
}

// Effect is a single output of Step. The driver executes effects in the exact
// order Step returns them, honoring the persist-before-send rule above. Fields
// are populated per Type.
type Effect struct {
	Type EffectType

	// EffectSend
	Msg Message

	// EffectPersistHardState
	HardState HardState

	// EffectPersistLog
	FromIndex Index
	Entries   []LogEntry

	// EffectApply
	Committed []CommittedEntry

	// EffectRejectProposal / EffectReadIndexReady
	Ref        ClientRef
	LeaderHint NodeID // EffectRejectProposal
	ReadIndex  Index  // EffectReadIndexReady

	// EffectSendSnapshot / EffectInstallSnapshot
	SnapIndex Index  // last index included in the snapshot
	SnapTerm  Term   // term of SnapIndex
	SnapData  []byte // application snapshot bytes (EffectInstallSnapshot only)

	// EffectConfigChanged
	Members []NodeID // the new voting configuration (P6 membership change)
}
