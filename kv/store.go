package kv

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
)

// Result is the outcome of applying one command — enough to answer the client
// and to record a check.HistoryEvent response. Value carries a Get result, the
// new value after an Append, or the observed value on a CAS; Found reports
// whether a Get's key existed; OK reports whether a CAS's compare matched (and
// so whether the swap happened). For non-CAS ops OK is true (the op succeeded).
type Result struct {
	Value string
	OK    bool
	Found bool
}

// session is a client's dedup record: the highest SeqNum applied for the client
// and the Result that application produced. A retried command (SeqNum <=
// lastSeq) returns lastResult WITHOUT re-applying, which is what makes writes
// exactly-once across leader changes and re-commits.
type session struct {
	lastSeq    uint64
	lastResult Result
}

// Store is the replicated key-value state machine. It is driven exclusively by
// Apply (fed committed entries by the driver) and is otherwise pure: no clock,
// no I/O, no goroutines, no randomness. It is NOT safe for concurrent use — the
// driver applies committed entries one at a time, in index order, from a single
// goroutine, matching the core's single-writer discipline.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		data:     make(map[string]string),
		sessions: make(map[uint64]session),
	}
}

// Apply decodes and applies one committed entry, returning the Result. It
// enforces exactly-once: if the command's SeqNum is not newer than the client's
// last applied SeqNum, it is a duplicate (a retry, or a stale re-delivery) and
// the cached Result is returned without mutating state. Leader no-ops and
// entries whose command fails to decode are ignored (a zero Result), so a
// malformed or empty command can never corrupt the store.
func (s *Store) Apply(entry core.CommittedEntry) Result {
	// A KindConfig entry is a membership change (P6), not a KV command — skip it
	// so its payload is never misdecoded as a client op.
	if entry.Kind == core.KindConfig {
		return Result{}
	}
	if core.IsNoOp(entry.Command) {
		return Result{}
	}
	cmd, err := Decode(entry.Command)
	if err != nil {
		return Result{}
	}

	sess := s.sessions[cmd.ClientID]
	if cmd.SeqNum <= sess.lastSeq {
		// Duplicate: already applied (or superseded). Return the cached result
		// and do not touch state — this is the exactly-once guarantee.
		return sess.lastResult
	}

	res := s.execute(cmd)
	s.sessions[cmd.ClientID] = session{lastSeq: cmd.SeqNum, lastResult: res}
	return res
}

// execute performs a command's mutation/read on the data map. It assumes the
// caller has already ruled the command non-duplicate.
func (s *Store) execute(cmd Command) Result {
	switch cmd.Op {
	case check.OpGet:
		v, found := s.data[cmd.Key]
		return Result{Value: v, OK: true, Found: found}
	case check.OpPut:
		s.data[cmd.Key] = cmd.Value
		return Result{OK: true}
	case check.OpAppend:
		v := s.data[cmd.Key] + cmd.Value
		s.data[cmd.Key] = v
		return Result{Value: v, OK: true}
	case check.OpCAS:
		// Register CAS with absent-key-reads-as-"" semantics: an absent key has the
		// empty value, so CAS(compare="") on an absent key succeeds and creates it.
		// This makes CAS a total function of the current value (no separate
		// "not found" case), which is exactly the semantics the linearizability
		// model checks against — the two MUST agree or a correct history reads as
		// non-linearizable.
		cur, found := s.data[cmd.Key]
		if cur == cmd.CompareValue {
			s.data[cmd.Key] = cmd.Value
			return Result{Value: cmd.Value, OK: true, Found: true}
		}
		// Compare failed: no swap; report the observed value.
		return Result{Value: cur, OK: false, Found: found}
	default:
		// Unknown op: treat as a no-op read so a bad command can't corrupt state.
		return Result{}
	}
}

// Get is a direct, non-replicated read of the current committed value, for
// tests and (later) ReadIndex serving after the state machine has caught up. It
// does not touch session state.
func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// ExpireBefore drops dedup sessions for every clientID < minClientID. It exists
// so the session table can be bounded, but is OFF by default (never called by
// Apply): dropping a session that could still retry within a run would break
// exactly-once, so expiry is an explicit, caller-driven policy. A driver may
// call it only for clients it can prove are permanently gone (e.g. IDs below a
// monotonically-assigned low-water mark). We deliberately expose no automatic
// LRU/size cap: clientIDs are assigned densely and a run's client set is small
// and known, so unbounded growth is not a real risk here, and a conservative
// no-expiry default can never violate the safety property.
func (s *Store) ExpireBefore(minClientID uint64) {
	for id := range s.sessions {
		if id < minClientID {
			delete(s.sessions, id)
		}
	}
}

// --- snapshot / restore ( compaction) -----------------------------------

// Snapshot serializes the ENTIRE store — the data map AND the session/dedup
// table — into a deterministic byte image. The session table MUST be included:
// if dedup state were lost across compaction, a command retried after a
// snapshot-install would re-apply and break exactly-once. Keys and client IDs
// are sorted before ranging, so the output is byte-identical for identical
// logical state regardless of map insertion order — required for stable trace
// hashes and cross-store determinism checks.
//
// Layout: [numKeys:u32] then, per key sorted ascending, [key][value] as
// length-prefixed strings; [numSessions:u32] then, per clientID sorted
// ascending, [clientID:u64][lastSeq:u64] and the lastResult as
// [Value(string)][OK:u8][Found:u8].
func (s *Store) Snapshot() []byte {
	buf := make([]byte, 0, 64)

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = appendUint32(buf, uint32(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendString(buf, s.data[k])
	}

	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	buf = appendUint32(buf, uint32(len(ids)))
	for _, id := range ids {
		sess := s.sessions[id]
		buf = appendUint64(buf, id)
		buf = appendUint64(buf, sess.lastSeq)
		buf = appendString(buf, sess.lastResult.Value)
		buf = append(buf, boolByte(sess.lastResult.OK), boolByte(sess.lastResult.Found))
	}
	return buf
}

// Restore replaces the store's state with the contents of a Snapshot image,
// including the session/dedup table, so exactly-once survives compaction. It
// returns an error on any truncation or length mismatch and leaves the store
// untouched in that case (it builds fresh maps and only swaps them in on
// success).
func (s *Store) Restore(data []byte) error {
	rest := data
	nKeys, rest, ok := takeUint32(rest)
	if !ok {
		return errors.New("kv: snapshot truncated at key count")
	}
	newData := make(map[string]string, nKeys)
	for i := uint32(0); i < nKeys; i++ {
		var k, v string
		if k, rest, ok = takeString(rest); !ok {
			return errors.New("kv: snapshot truncated key")
		}
		if v, rest, ok = takeString(rest); !ok {
			return errors.New("kv: snapshot truncated value")
		}
		newData[k] = v
	}

	nSess, rest, ok := takeUint32(rest)
	if !ok {
		return errors.New("kv: snapshot truncated at session count")
	}
	newSessions := make(map[uint64]session, nSess)
	for i := uint32(0); i < nSess; i++ {
		var id, seq uint64
		var val string
		if id, rest, ok = takeUint64(rest); !ok {
			return errors.New("kv: snapshot truncated session id")
		}
		if seq, rest, ok = takeUint64(rest); !ok {
			return errors.New("kv: snapshot truncated session seq")
		}
		if val, rest, ok = takeString(rest); !ok {
			return errors.New("kv: snapshot truncated session result")
		}
		if len(rest) < 2 {
			return errors.New("kv: snapshot truncated session flags")
		}
		okFlag, foundFlag := rest[0] != 0, rest[1] != 0
		rest = rest[2:]
		newSessions[id] = session{
			lastSeq:    seq,
			lastResult: Result{Value: val, OK: okFlag, Found: foundFlag},
		}
	}

	if len(rest) != 0 {
		return errors.New("kv: trailing bytes after snapshot")
	}
	s.data = newData
	s.sessions = newSessions
	return nil
}

// --- fixed-width encode/decode helpers -------------------------------------

func appendUint32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func takeUint32(data []byte) (v uint32, rest []byte, ok bool) {
	if len(data) < 4 {
		return 0, data, false
	}
	return binary.BigEndian.Uint32(data[0:4]), data[4:], true
}

func takeUint64(data []byte) (v uint64, rest []byte, ok bool) {
	if len(data) < 8 {
		return 0, data, false
	}
	return binary.BigEndian.Uint64(data[0:8]), data[8:], true
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
