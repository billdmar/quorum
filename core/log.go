package core

// raftLog is the in-memory view of a node's replicated log. It is pure data
// with pure methods — no I/O, no clocks. Durability is the driver's job (via
// EffectPersistLog); this type only tracks what the core believes the log to
// be. Entries are stored 1-based logically (Raft indices start at 1); index 0
// means "before the first entry" and always has term 0.
//
// After snapshotting () the log may start above index 1; snapBase records the
// last index included in the most recent snapshot (0 when none). entries[i]
// holds log index snapBase+1+i.
type raftLog struct {
	entries  []LogEntry
	snapBase Index // last index covered by a snapshot; 0 if none
	snapTerm Term  // term of the snapBase entry; 0 if none
}

func newRaftLog() *raftLog {
	return &raftLog{}
}

// lastIndex returns the highest index present (snapBase if the log is empty
// beyond the snapshot).
func (l *raftLog) lastIndex() Index {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snapBase
}

// lastTerm returns the term of the last entry (snapTerm if empty beyond snap).
func (l *raftLog) lastTerm() Term {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Term
	}
	return l.snapTerm
}

// firstIndex returns the lowest index physically present in entries (snapBase+1),
// or snapBase+1 when empty (i.e. the next index to be appended).
func (l *raftLog) firstIndex() Index {
	return l.snapBase + 1
}

// termAt returns the term of the entry at index i, and whether it is known.
// Index 0 is term 0 (known). An index at snapBase is snapTerm (known). An index
// inside the compacted range (0 < i < snapBase) is unknown to the in-memory log
// (would require the snapshot). An index beyond lastIndex is unknown.
func (l *raftLog) termAt(i Index) (Term, bool) {
	if i == 0 {
		return 0, true
	}
	if i == l.snapBase {
		return l.snapTerm, true
	}
	if i < l.firstIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.firstIndex()].Term, true
}

// sliceFrom returns a copy of entries with index >= from (clamped to what is
// present). Returns a fresh slice so the caller (and any effect payload) can
// hold it without aliasing the log's backing array.
func (l *raftLog) sliceFrom(from Index) []LogEntry {
	if from < l.firstIndex() {
		from = l.firstIndex()
	}
	if from > l.lastIndex() {
		return nil
	}
	start := int(from - l.firstIndex())
	out := make([]LogEntry, len(l.entries)-start)
	copy(out, l.entries[start:])
	return out
}

// entryAt returns the entry at index i, if physically present.
func (l *raftLog) entryAt(i Index) (LogEntry, bool) {
	if i < l.firstIndex() || i > l.lastIndex() {
		return LogEntry{}, false
	}
	return l.entries[i-l.firstIndex()], true
}

// matches reports whether the log contains an entry at prevIndex whose term is
// prevTerm — the AppendEntries consistency check. prevIndex 0 always matches
// (empty prefix). An index at snapBase matches iff prevTerm == snapTerm.
func (l *raftLog) matches(prevIndex Index, prevTerm Term) bool {
	t, ok := l.termAt(prevIndex)
	return ok && t == prevTerm
}

// append adds entries to the end of the log. Caller guarantees contiguity
// (entries begin at lastIndex()+1). Used when accepting new leader entries or a
// leader appending a client command.
func (l *raftLog) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// appendFromLeader reconciles incoming AppendEntries entries starting at
// prevIndex+1. It walks the incoming entries against the local log: entries
// that already match (same index+term) are kept as-is; at the first conflict
// (same index, different term) it truncates the local log there and appends the
// remainder. Entries wholly before firstIndex (already compacted/committed) are
// skipped. Returns the entries that were actually newly written (for the
// EffectPersistLog payload) and the fromIndex they start at; if nothing new was
// written, returns nil and fromIndex 0.
//
// This is the heart of log matching: it never truncates a prefix the leader did
// not contradict, so committed entries the leader still has are preserved, and
// stale-but-longer suffixes from a previous term are correctly overwritten only
// when the leader's entries actually diverge.
func (l *raftLog) appendFromLeader(prevIndex Index, incoming []LogEntry) (fromIndex Index, written []LogEntry) {
	i := 0
	// Skip incoming entries that are already covered and matching.
	for ; i < len(incoming); i++ {
		e := incoming[i]
		if e.Index < l.firstIndex() {
			continue // already compacted; assumed committed and identical
		}
		if e.Index > l.lastIndex() {
			break // rest are pure appends
		}
		existing := l.entries[e.Index-l.firstIndex()]
		if existing.Term != e.Term {
			// Conflict: truncate here and append the remainder.
			l.entries = l.entries[:e.Index-l.firstIndex()]
			break
		}
	}
	if i >= len(incoming) {
		return 0, nil // everything already present and matching
	}
	rest := incoming[i:]
	l.append(rest...)
	// The written entries begin at rest[0].Index.
	return rest[0].Index, l.sliceFrom(rest[0].Index)
}

// clone returns a copy of the entries slice suitable for building a ClusterView
// snapshot for invariant monitors. The slice is copied; commands are shared
// (they are immutable once appended).
func (l *raftLog) clone() []LogEntry {
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// compactTo discards the log prefix through index (inclusive), advancing the
// snapshot base to (index, term). It is used after the driver durably saves a
// snapshot covering the log through index. Entries with Index > index are
// retained, contiguous. index must be within the physically-present range and
// no greater than the highest present index; a term mismatch at index would be a
// programming error (the driver snapshots its own applied prefix). A compactTo
// at or below the current snapBase is a no-op (idempotent).
func (l *raftLog) compactTo(index Index, term Term) {
	if index <= l.snapBase {
		return
	}
	if index > l.lastIndex() {
		// Cannot compact beyond what we hold; clamp to lastIndex (the driver only
		// ever compacts through an applied index, so this is defensive).
		index = l.lastIndex()
	}
	keep := l.sliceFrom(index + 1)
	l.entries = keep
	l.snapBase = index
	l.snapTerm = term
}

// installSnapshot rebases the log on a snapshot received from the leader,
// covering (index, term). Per Raft §7: if the local log contains an entry at
// index whose term matches term, the snapshot describes a prefix we already have
// — retain the tail after index. Otherwise the local log conflicts with (or is
// shorter than) the snapshot, so discard it entirely and start fresh at the
// snapshot base. Sets snapBase/snapTerm to (index, term).
func (l *raftLog) installSnapshot(index Index, term Term) {
	if t, ok := l.termAt(index); ok && t == term && index <= l.lastIndex() {
		// We have a matching entry at index: keep everything after it.
		l.entries = l.sliceFrom(index + 1)
	} else {
		// Conflict or gap: discard the whole log; the snapshot supersedes it.
		l.entries = nil
	}
	l.snapBase = index
	l.snapTerm = term
}
