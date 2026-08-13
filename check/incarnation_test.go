package check

// Regression tests for the incarnation-aware commit-monotonicity check. The 
// seed sweep surfaced that a crashed node legitimately resets its VOLATILE
// commitIndex to 0 on restart (Raft Figure 2), which the original monitor
// mis-flagged as a monotonicity violation. The monitor now rebaselines across an
// incarnation bump; these tests pin both halves of that behavior so it can never
// silently regress into either a false positive or a lost real violation.

import (
	"testing"

	"github.com/billdmar/quorum/core"
)

// oneNodeView builds a single-node view with an explicit incarnation and commit
// index — enough to drive the stateful commit-monotonicity check in isolation.
func oneNodeView(step uint64, commit core.Index, incarnation uint64) ClusterView {
	// Provide a matching log/applied prefix so the applied<=commit sub-check and
	// the other invariants stay satisfied; we only want to exercise monotonicity.
	var log []core.LogEntry
	var app []core.CommittedEntry
	for i := core.Index(1); i <= commit; i++ {
		log = append(log, entry(i, 1, "x"))
		app = append(app, applied(i, 1, "x"))
	}
	return ClusterView{Step: step, Nodes: []NodeView{{
		ID: "n1", Role: core.Follower, Term: 1,
		CommitIndex: commit, Incarnation: incarnation, Log: log, Applied: app,
	}}}
}

// TestCommitMonotonicity_ResetAcrossIncarnationAllowed proves a commit-index
// reset to 0 that coincides with an incarnation bump (a crash/restart) is NOT a
// violation — the volatile-state reset is legitimate Raft behavior.
func TestCommitMonotonicity_ResetAcrossIncarnationAllowed(t *testing.T) {
	m := NewMonitor()
	// Incarnation 0 climbs to commit 5.
	if vs := m.CheckAll(oneNodeView(1, 5, 0)); len(vs) != 0 {
		t.Fatalf("unexpected violations climbing to 5: %+v", vs)
	}
	// Crash+restart: incarnation bumps to 1, commit resets to 0. Allowed.
	if vs := m.CheckAll(oneNodeView(2, 0, 1)); len(vs) != 0 {
		t.Fatalf("reset to 0 across incarnation bump must NOT be flagged: %+v", vs)
	}
	// Re-climb within incarnation 1 back up to 3. Allowed.
	if vs := m.CheckAll(oneNodeView(3, 3, 1)); len(vs) != 0 {
		t.Fatalf("re-climb after restart must NOT be flagged: %+v", vs)
	}
}

// TestCommitMonotonicity_DecreaseWithinIncarnationStillFlagged proves the
// monitor keeps its teeth: a decrease WITHOUT an incarnation bump — a genuine
// safety violation — is still caught.
func TestCommitMonotonicity_DecreaseWithinIncarnationStillFlagged(t *testing.T) {
	m := NewMonitor()
	if vs := m.CheckAll(oneNodeView(1, 5, 7)); len(vs) != 0 {
		t.Fatalf("unexpected violations at commit 5: %+v", vs)
	}
	// Same incarnation (7), commit drops 5 -> 3: MUST be flagged.
	vs := m.CheckAll(oneNodeView(2, 3, 7))
	if len(vs) == 0 {
		t.Fatal("in-incarnation commit decrease must be flagged")
	}
	if vs[0].Invariant != InvCommitMonotonicity {
		t.Fatalf("wrong invariant flagged: %s", vs[0].Invariant)
	}
}

// TestAllUnknownHistoryTriviallyLinearizable is the regression for  seed 7503
// (kitchen-sink, n=5): a history in which every operation's outcome is Unknown
// carries no observable output for any ordering to contradict, so it is
// trivially linearizable. The checker must short-circuit to Ok (fast, determined)
// rather than hand Porcupine its exponential all-concurrent worst case and time
// out to Undetermined.
func TestAllUnknownHistoryTriviallyLinearizable(t *testing.T) {
	var b hb
	// Several overlapping ops on a shared key, ALL with Unknown responses.
	for i := 0; i < 8; i++ {
		call := uint64(i * 2)
		b.op(uint64(i%3), call, call+100,
			HistoryEvent{Kind: OpAppend, Key: "k", Value: "v"},
			HistoryEvent{Kind: OpAppend, Unknown: true})
	}
	h := History{Seed: 7503, Schedule: "kitchen-sink", Events: b.events}
	lin, res := CheckLinearizable(h)
	if !lin || !res.Determined {
		t.Fatalf("all-Unknown history must be trivially linearizable+determined; got lin=%v determined=%v", lin, res.Determined)
	}
}

// TestDefiniteViolationAmongUnknownsStillFlagged proves the short-circuit does
// NOT mask a real violation: a history that is mostly Unknown but contains ONE
// definite, impossible result must still be flagged non-linearizable.
func TestDefiniteViolationAmongUnknownsStillFlagged(t *testing.T) {
	var b hb
	// A definite Put of "x" that returns before a definite Get that must see it.
	b.op(1, 0, 10,
		HistoryEvent{Kind: OpPut, Key: "k", Value: "x"},
		HistoryEvent{Kind: OpPut, OK: true})
	// Some Unknown noise around it.
	b.op(2, 1, 100, HistoryEvent{Kind: OpAppend, Key: "k", Value: "y"}, HistoryEvent{Kind: OpAppend, Unknown: true})
	// A definite Get AFTER the Put returned that reads the OLD empty value — impossible.
	b.op(3, 20, 30,
		HistoryEvent{Kind: OpGet, Key: "k"},
		HistoryEvent{Kind: OpGet, Output: "", OK: true})
	h := History{Seed: 1, Schedule: "test", Events: b.events}
	lin, res := CheckLinearizable(h)
	if lin || !res.Determined {
		t.Fatalf("a definite stale read among Unknowns must be flagged non-linearizable (determined); got lin=%v determined=%v", lin, res.Determined)
	}
}

// TestLeaderCompleteness_StaleLeaderNotFlagged is the distilled regression for
// the -sweep finding (seed 507, partition-heavy, n=5): a DEPOSED leader at a
// term below the cluster max must NOT be held to leader-completeness. Shape: an
// entry created in term 1 is committed by the max-term (term 5) leader, which
// holds it; a stale term-4 leader — isolated by a partition, unaware of term 5 —
// has its own divergent, UNcommitted branch and lacks the entry. This is legal
// (the stale leader steps down and is overwritten on heal); the monitor must not
// flag it. Keying the old check on the entry's creation term (1 < 4) wrongly did.
func TestLeaderCompleteness_StaleLeaderNotFlagged(t *testing.T) {
	// Committed entry at index 3 (created term 1), held by the true term-5 leader.
	committed := []core.LogEntry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")}
	trueLeaderLog := append(append([]core.LogEntry(nil), committed...),
		entry(4, 5, "d"), entry(5, 5, "e")) // term-5 leader's own suffix
	// Stale term-4 leader: shares the term-1 prefix only up to index 2, then has a
	// divergent UNcommitted term-4 branch (commit stays at 2, below index 3).
	staleLeaderLog := []core.LogEntry{entry(1, 1, "a"), entry(2, 1, "b"),
		entry(3, 4, "X"), entry(4, 4, "Y")}

	view := ClusterView{Step: 1, Nodes: []NodeView{
		{ID: "n3", Role: core.Leader, Term: 5, CommitIndex: 5, Log: trueLeaderLog,
			Applied: []core.CommittedEntry{applied(1, 1, "a"), applied(2, 1, "b"), applied(3, 1, "c"), applied(4, 5, "d"), applied(5, 5, "e")}},
		{ID: "n1", Role: core.Follower, Term: 5, CommitIndex: 5, Log: trueLeaderLog},
		{ID: "n2", Role: core.Follower, Term: 5, CommitIndex: 5, Log: trueLeaderLog},
		{ID: "n4", Role: core.Leader, Term: 4, CommitIndex: 2, Log: staleLeaderLog},
		{ID: "n0", Role: core.Follower, Term: 4, CommitIndex: 2, Log: staleLeaderLog},
	}}
	if v := InvLeaderCompletenessCheck(view); v != nil {
		t.Fatalf("stale sub-max-term leader must NOT be flagged, got: %s", v.Detail)
	}

	// Sanity: if the TRUE (max-term) leader is missing an entry a follower has
	// committed, THAT is a real violation and must still be caught. Corrupt ONLY
	// the leader's log at the committed index 3, leaving followers n1/n2 with the
	// correct committed entry — so a committed "c"@3 exists that the leader lacks.
	bad := view
	bad.Nodes = append([]NodeView(nil), view.Nodes...)
	badLeaderLog := append([]core.LogEntry(nil), trueLeaderLog...)
	badLeaderLog[2] = entry(3, 1, "WRONG")
	bad.Nodes[0].Log = badLeaderLog // n3 (leader) diverges at committed index 3
	if v := InvLeaderCompletenessCheck(bad); v == nil {
		t.Fatal("true max-term leader missing a committed entry MUST be flagged")
	}
}
