package sim

import (
	"encoding/binary"
	"hash"
	"hash/fnv"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
)

// traceHash folds every processed event and its salient fields into a single
// running 64-bit FNV-1a hash (stdlib hash/fnv — stable across Go versions and
// architectures). Two runs of the same seed process the same events in the same
// order and therefore MUST produce the same final hash: that equality IS the
// determinism gate. The hash is intentionally cheap and order-sensitive — it is
// a fingerprint of the whole run, not a cryptographic digest.
type traceHash struct {
	h   hash.Hash64
	buf [8]byte
}

func newTraceHash() *traceHash { return &traceHash{h: fnv.New64a()} }

// u64 folds a raw 64-bit value into the hash in fixed big-endian byte order so
// the fold is architecture-independent.
func (t *traceHash) u64(v uint64) {
	binary.BigEndian.PutUint64(t.buf[:], v)
	_, _ = t.h.Write(t.buf[:])
}

// str folds a string's bytes into the hash (used for NodeIDs / command bytes).
func (t *traceHash) str(s string) { _, _ = t.h.Write([]byte(s)) }

// event folds one core-driving event: the tick it fires at, the acting node,
// the event type, and the key fields that distinguish it. For a delivery the
// message's routing, type, term, and the indices that drive log reconciliation
// are folded — enough that any divergence in what the core is asked to do
// changes the hash, without folding every byte of every entry (which the log
// state below already covers transitively).
func (t *traceHash) event(tick uint64, nodeIdx int, ev core.Event) {
	t.u64(tick)
	t.u64(uint64(nodeIdx))
	t.u64(uint64(ev.Type))
	switch ev.Type {
	case core.EventDeliver:
		m := ev.Msg
		t.str(string(m.From))
		t.str(string(m.To))
		t.u64(uint64(m.Type))
		t.u64(uint64(m.Term))
		t.u64(uint64(m.PrevLogIndex))
		t.u64(uint64(m.LeaderCommit))
		t.u64(uint64(m.MatchIndex))
		t.u64(uint64(len(m.Entries)))
		// ReadIndex confirmation round: shifts read-serving behavior, so it must be
		// observable to the hash or read-path nondeterminism could slip past.
		t.u64(m.ReadSeq)
		// InstallSnapshot payload: fold the snapshot bounds and a content hash of
		// the bytes, so a divergent snapshot (different compaction point or state)
		// changes the trace hash even though the routing/type match.
		if m.Type == core.MsgInstallSnapshot {
			t.u64(uint64(m.LastIncludedIndex))
			t.u64(uint64(m.LastIncludedTerm))
			t.u64(fnvBytes(m.SnapshotData))
		}
		if m.VoteGranted {
			t.u64(1)
		}
		if m.Success {
			t.u64(1)
		}
	case core.EventPropose:
		t.u64(uint64(ev.Ref))
		t.str(string(ev.Command))
	case core.EventReadIndex:
		t.u64(uint64(ev.Ref))
	}
}

// step folds a per-step summary of a node's post-step observable state, so any
// divergence in role/term/commit/log — not just in the input events — is caught
// even when the triggering event fields happened to match.
func (t *traceHash) step(nodeIdx int, v check.NodeView) {
	t.u64(uint64(nodeIdx))
	t.u64(uint64(v.Role))
	t.u64(uint64(v.Term))
	t.u64(uint64(v.CommitIndex))
	// Snapshot base: two runs diverging only in what was compacted (identical
	// tails) would otherwise hash identically — a determinism blind spot exactly
	// where snapshots add risk. Fold the base index+term.
	t.u64(uint64(v.SnapBase))
	t.u64(uint64(v.SnapTerm))
	t.u64(uint64(len(v.Log)))
	if n := len(v.Log); n > 0 {
		t.u64(uint64(v.Log[n-1].Term))
		t.u64(uint64(v.Log[n-1].Index))
	}
}

// fnvBytes returns a stable FNV-1a hash of a byte slice, for folding opaque
// snapshot payloads into the trace hash without folding every byte inline.
func fnvBytes(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// sum returns the running hash value.
func (t *traceHash) sum() uint64 { return t.h.Sum64() }
