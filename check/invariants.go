package check

// invariants.go implements the five registered Raft safety-invariant monitors.
// Each operates on a ClusterView snapshot the driver assembles after a step.
// Four are pure functions of a single view; commit-monotonicity is inherently
// cross-step and so lives on the stateful Monitor, which remembers each node's
// last-seen commit index between CheckAll calls.
//
// All iteration over the view's nodes and over log indices is DETERMINISTIC —
// nodes are visited in sorted NodeID order and indices in ascending order — so
// the same violating view always produces byte-identical Violation output,
// which is what makes a failing seed a stable regression test.
//
// INTERPRETATION NOTE (leader-completeness). The true statement is "an entry
// committed in term T appears in the log of every leader of term > T." A single
// ClusterView cannot observe an entry's COMMIT term — an entry created in term
// E.Term may be committed much later by a higher-term leader via the Figure-8
// rule — so keying the check on E.Term is unsound: it false-positives on a
// deposed stale leader that coexists with the true leader during a partition
// (creation term < stale leader's term < the true commit term). We therefore
// project the invariant onto the only leader a snapshot can trust: the one whose
// term equals the MAXIMUM term present in the cluster. A leader below the max
// term is provably superseded (a higher term exists) and will step down on
// contact, so it is not bound by leader-completeness for the current epoch. The
// max-term leader is the current legitimate leader, and by the election
// restriction it must hold every entry any node has committed. A genuine
// violation — the true leader missing a committed entry — is still caught; the
// deeper net (state-machine-safety + Porcupine) independently guards commit
// agreement. This false positive was found by the  sweep (seed 507,
// partition-heavy, n=5) and is pinned by a regression test.

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/billdmar/quorum/core"
)

// Monitor evaluates all five safety invariants after each step. It is stateful:
// it remembers the highest commit index seen per node so commit-monotonicity can
// detect a decrease across successive CheckAll calls. Construct with NewMonitor.
type Monitor struct {
	// lastCommit maps a node to the highest CommitIndex it has reported WITHIN its
	// current incarnation, so a later view showing a lower value in the SAME
	// incarnation is flagged as a monotonicity violation.
	lastCommit map[core.NodeID]core.Index
	// lastIncarnation maps a node to the incarnation lastCommit was recorded in.
	// commitIndex is volatile Raft state that resets to 0 on restart, so when a
	// node's incarnation advances the baseline is reset rather than treating the
	// legitimate reset as a decrease. See NodeView.Incarnation.
	lastIncarnation map[core.NodeID]uint64
}

// NewMonitor returns a Monitor with empty per-node commit history.
func NewMonitor() *Monitor {
	return &Monitor{
		lastCommit:      make(map[core.NodeID]core.Index),
		lastIncarnation: make(map[core.NodeID]uint64),
	}
}

// CheckAll runs every registered invariant against the view and returns all
// violations found, in a deterministic order (by invariant id, then by the
// order each check emits them). The returned Violations carry Invariant and
// Step; the caller fills Seed and Schedule from the run that produced the view.
func (m *Monitor) CheckAll(view ClusterView) []Violation {
	var out []Violation
	add := func(v *Violation) {
		if v != nil {
			out = append(out, *v)
		}
	}
	add(InvElectionSafetyCheck(view))
	add(InvLogMatchingCheck(view))
	add(InvLeaderCompletenessCheck(view))
	add(InvStateMachineSafetyCheck(view))
	add(m.commitMonotonicity(view))
	return out
}

// sortedNodes returns the view's nodes sorted by NodeID so every monitor
// iterates deterministically.
func sortedNodes(view ClusterView) []NodeView {
	ns := make([]NodeView, len(view.Nodes))
	copy(ns, view.Nodes)
	sort.Slice(ns, func(i, j int) bool { return ns[i].ID < ns[j].ID })
	return ns
}

// InvElectionSafetyCheck reports a violation if more than one node claims to be
// leader in the same term.
func InvElectionSafetyCheck(view ClusterView) *Violation {
	leaderByTerm := make(map[core.Term]core.NodeID)
	for _, n := range sortedNodes(view) {
		if n.Role != core.Leader {
			continue
		}
		if other, ok := leaderByTerm[n.Term]; ok {
			return &Violation{
				Invariant: InvElectionSafety,
				Step:      view.Step,
				Detail: fmt.Sprintf("two leaders in term %d: %s and %s",
					n.Term, other, n.ID),
			}
		}
		leaderByTerm[n.Term] = n.ID
	}
	return nil
}

// logByIndex indexes a node's log entries by their 1-based Index for lookup.
func logByIndex(log []core.LogEntry) map[core.Index]core.LogEntry {
	m := make(map[core.Index]core.LogEntry, len(log))
	for _, e := range log {
		m[e.Index] = e
	}
	return m
}

// entriesEqual reports whether two log entries are identical (term and command).
func entriesEqual(a, b core.LogEntry) bool {
	return a.Term == b.Term && bytes.Equal(a.Command, b.Command)
}

// InvLogMatchingCheck reports a violation if two nodes hold an entry with the
// same (index, term) yet their logs are not identical in every entry up through
// that index (the Raft Log Matching property). We compare each
// unordered node pair once, at the highest index where both agree on the term;
// if the prefix diverges there, that is the violation.
func InvLogMatchingCheck(view ClusterView) *Violation {
	nodes := sortedNodes(view)
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if v := logMatchingPair(view.Step, nodes[i], nodes[j]); v != nil {
				return v
			}
		}
	}
	return nil
}

// logMatchingPair checks the log-matching property for one pair of nodes. The
// comparison is scoped to indices ABOVE both nodes' snapshot bases: below
// max(a.SnapBase, b.SnapBase) at least one node has compacted the entry away, so
// its absence is not a divergence — the snapshot subsumes and (by the install /
// compaction rules) agrees with the committed prefix. Comparing there would be a
// false positive of exactly the class the  review warned about. This does not
// weaken the check: any surviving divergence in the overlapping range is still
// flagged, and state-machine-safety + linearizability independently guard the
// compacted prefix.
func logMatchingPair(step uint64, a, b NodeView) *Violation {
	lo := a.SnapBase
	if b.SnapBase > lo {
		lo = b.SnapBase
	}
	bIdx := logByIndex(b.Log)
	// Highest index where both nodes have an entry sharing the same term.
	var maxMatch core.Index
	found := false
	for _, ea := range a.Log {
		if ea.Index <= lo {
			continue
		}
		if eb, ok := bIdx[ea.Index]; ok && eb.Term == ea.Term {
			if !found || ea.Index > maxMatch {
				maxMatch = ea.Index
				found = true
			}
		}
	}
	if !found {
		return nil
	}
	// Every entry in (lo, maxMatch] must be identical in both logs.
	aIdx := logByIndex(a.Log)
	for idx := lo + 1; idx <= maxMatch; idx++ {
		ea, aok := aIdx[idx]
		eb, bok := bIdx[idx]
		if !aok || !bok || !entriesEqual(ea, eb) {
			return &Violation{
				Invariant: InvLogMatching,
				Step:      step,
				Detail: fmt.Sprintf("logs of %s and %s agree at index %d term %d but diverge at index %d",
					a.ID, b.ID, maxMatch, aIdx[maxMatch].Term, idx),
			}
		}
	}
	return nil
}

// InvLeaderCompletenessCheck reports a violation if an entry committed by any
// node is absent, at the same index, from the current MAX-TERM leader. Only the
// max-term leader is checked: a leader below the max term is provably superseded
// (a higher term exists in the cluster) and steps down on contact, so it is not
// bound by leader-completeness for the current epoch. See the interpretation
// note at the top of the file.
func InvLeaderCompletenessCheck(view ClusterView) *Violation {
	nodes := sortedNodes(view)

	// The maximum term anywhere in the cluster.
	var maxTerm core.Term
	for _, n := range nodes {
		if n.Term > maxTerm {
			maxTerm = n.Term
		}
	}
	// The legitimate current leader is a leader at the max term. If there is none
	// (an election is in progress), there is nothing to hold to completeness yet.
	var leader *NodeView
	for i := range nodes {
		if nodes[i].Role == core.Leader && nodes[i].Term == maxTerm {
			leader = &nodes[i]
			break
		}
	}
	if leader == nil {
		return nil
	}
	leaderIdx := logByIndex(leader.Log)

	// Every entry any node has committed must appear identically in that leader's
	// log. (Election restriction guarantees the max-term leader's log is at least
	// as complete as any committed prefix.) An entry at or below the leader's
	// snapshot base is not in its in-memory log but IS subsumed by its snapshot
	// (the leader could only have snapshotted committed entries), so it is not a
	// counterexample — skip it to avoid a compaction false positive.
	for _, src := range nodes {
		for _, e := range src.Log {
			if e.Index > src.CommitIndex {
				break
			}
			if e.Index <= leader.SnapBase {
				continue // subsumed by the leader's snapshot
			}
			if core.IsNoOp(e.Command) {
				continue // no-ops carry no application command to preserve
			}
			got, ok := leaderIdx[e.Index]
			if !ok || !entriesEqual(got, e) {
				return &Violation{
					Invariant: InvLeaderCompleteness,
					Step:      view.Step,
					Detail: fmt.Sprintf("entry committed at index %d term %d (on %s) missing from max-term leader %s (term %d)",
						e.Index, e.Term, src.ID, leader.ID, leader.Term),
				}
			}
		}
	}
	return nil
}

// InvStateMachineSafetyCheck reports a violation if two nodes apply, or commit,
// different commands at the same log index. We compare both the applied streams
// and the committed
// log prefixes across every node pair, keyed by index.
func InvStateMachineSafetyCheck(view ClusterView) *Violation {
	// applied command per (node,index) collapsed to a per-index witness.
	type witness struct {
		node core.NodeID
		cmd  []byte
	}
	seen := make(map[core.Index]witness)

	record := func(step uint64, node core.NodeID, idx core.Index, cmd []byte) *Violation {
		if w, ok := seen[idx]; ok {
			if !bytes.Equal(w.cmd, cmd) {
				return &Violation{
					Invariant: InvStateMachineSafety,
					Step:      step,
					Detail: fmt.Sprintf("nodes %s and %s applied different commands at index %d",
						w.node, node, idx),
				}
			}
			return nil
		}
		seen[idx] = witness{node: node, cmd: cmd}
		return nil
	}

	for _, n := range sortedNodes(view) {
		// Applied stream: the authoritative record of what reached the state machine.
		for _, a := range n.Applied {
			if v := record(view.Step, n.ID, a.Index, a.Command); v != nil {
				return v
			}
		}
		// Committed log prefix: entries the node considers committed but that may
		// not yet appear in Applied.
		for _, e := range n.Log {
			if e.Index > n.CommitIndex {
				break
			}
			if v := record(view.Step, n.ID, e.Index, e.Command); v != nil {
				return v
			}
		}
	}
	return nil
}

// appliedIndex returns the highest index a node has applied (0 if none). Applied
// is contract-guaranteed to be in increasing index order.
func appliedIndex(n NodeView) core.Index {
	if len(n.Applied) == 0 {
		return 0
	}
	return n.Applied[len(n.Applied)-1].Index
}

// commitMonotonicity: within a single incarnation a node's commit index never
// decreases across steps, and its applied index never exceeds its commit index.
// Stateful — it consults and updates the Monitor's per-node commit history,
// keyed by incarnation. commitIndex is VOLATILE Raft state: a restart resets it
// to 0 and the node re-derives it from its durable log, so a decrease that
// coincides with an incarnation bump is expected and NOT a violation. The real
// cross-crash guarantee (no committed entry is lost or applied differently) is
// enforced by state-machine-safety and by the linearizability checker, not here.
func (m *Monitor) commitMonotonicity(view ClusterView) *Violation {
	for _, n := range sortedNodes(view) {
		sameIncarnation := m.lastIncarnation[n.ID] == n.Incarnation
		if prev, ok := m.lastCommit[n.ID]; ok && sameIncarnation && n.CommitIndex < prev {
			return &Violation{
				Invariant: InvCommitMonotonicity,
				Step:      view.Step,
				Detail: fmt.Sprintf("commit index on %s decreased from %d to %d within incarnation %d",
					n.ID, prev, n.CommitIndex, n.Incarnation),
			}
		}
		if ai := appliedIndex(n); ai > n.CommitIndex {
			return &Violation{
				Invariant: InvCommitMonotonicity,
				Step:      view.Step,
				Detail: fmt.Sprintf("applied index %d exceeds commit index %d on %s",
					ai, n.CommitIndex, n.ID),
			}
		}
	}
	// Record the new highs only after the view passes, so a detected violation
	// reports the true prior value. On an incarnation bump, reset the baseline to
	// the current (post-restart) value rather than carrying the stale high.
	for _, n := range view.Nodes {
		if m.lastIncarnation[n.ID] != n.Incarnation {
			m.lastIncarnation[n.ID] = n.Incarnation
			m.lastCommit[n.ID] = n.CommitIndex
			continue
		}
		if cur, ok := m.lastCommit[n.ID]; !ok || n.CommitIndex > cur {
			m.lastCommit[n.ID] = n.CommitIndex
		}
	}
	return nil
}
