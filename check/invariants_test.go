package check

// Tests for the five safety-invariant monitors. Each invariant is exercised
// twice: a clean view passes, and a crafted violating view is flagged with the
// correct InvariantID.

import (
	"testing"

	"github.com/billdmar/quorum/core"
)

// entry builds a LogEntry with a command derived from cmd (nil cmd => no-op).
func entry(idx core.Index, term core.Term, cmd string) core.LogEntry {
	var c []byte
	if cmd != "" {
		c = []byte(cmd)
	}
	return core.LogEntry{Index: idx, Term: term, Command: c}
}

// applied builds a CommittedEntry.
func applied(idx core.Index, term core.Term, cmd string) core.CommittedEntry {
	return core.CommittedEntry{Index: idx, Term: term, Command: []byte(cmd)}
}

// cleanView is a healthy 3-node view: one leader in term 2, all logs identical,
// all committed and applied through index 2.
func cleanView(step uint64) ClusterView {
	log := []core.LogEntry{entry(1, 1, "a"), entry(2, 2, "b")}
	app := []core.CommittedEntry{applied(1, 1, "a"), applied(2, 2, "b")}
	mk := func(id core.NodeID, role core.Role) NodeView {
		return NodeView{ID: id, Role: role, Term: 2, CommitIndex: 2,
			Log: append([]core.LogEntry(nil), log...), Applied: append([]core.CommittedEntry(nil), app...)}
	}
	return ClusterView{Step: step, Nodes: []NodeView{
		mk("n1", core.Leader), mk("n2", core.Follower), mk("n3", core.Follower),
	}}
}

func TestInvElectionSafety_CleanAndViolation(t *testing.T) {
	if v := InvElectionSafetyCheck(cleanView(1)); v != nil {
		t.Fatalf("clean view flagged: %v", v)
	}
	// Two leaders in term 2.
	bad := cleanView(1)
	bad.Nodes[1].Role = core.Leader
	v := InvElectionSafetyCheck(bad)
	if v == nil || v.Invariant != InvElectionSafety {
		t.Fatalf("expected election-safety violation, got %v", v)
	}
}

func TestInvLogMatching_CleanAndViolation(t *testing.T) {
	if v := InvLogMatchingCheck(cleanView(1)); v != nil {
		t.Fatalf("clean view flagged: %v", v)
	}
	// n2 agrees with n1 at index 2 term 2 but has a different index-1 command:
	// a log-matching violation (same index+term must imply identical prefix).
	bad := cleanView(1)
	bad.Nodes[1].Log = []core.LogEntry{entry(1, 1, "X"), entry(2, 2, "b")}
	v := InvLogMatchingCheck(bad)
	if v == nil || v.Invariant != InvLogMatching {
		t.Fatalf("expected log-matching violation, got %v", v)
	}
}

func TestInvLeaderCompleteness_CleanAndViolation(t *testing.T) {
	if v := InvLeaderCompletenessCheck(cleanView(1)); v != nil {
		t.Fatalf("clean view flagged: %v", v)
	}
	// An entry committed at index 2 term 2 on n1, but a leader of term 3 (n3)
	// is missing it — leader-completeness violation.
	bad := cleanView(1)
	bad.Nodes[2].Role = core.Leader
	bad.Nodes[2].Term = 3
	bad.Nodes[2].Log = []core.LogEntry{entry(1, 1, "a")} // missing index 2
	bad.Nodes[2].CommitIndex = 1
	bad.Nodes[2].Applied = []core.CommittedEntry{applied(1, 1, "a")}
	v := InvLeaderCompletenessCheck(bad)
	if v == nil || v.Invariant != InvLeaderCompleteness {
		t.Fatalf("expected leader-completeness violation, got %v", v)
	}
}

func TestInvStateMachineSafety_CleanAndViolation(t *testing.T) {
	if v := InvStateMachineSafetyCheck(cleanView(1)); v != nil {
		t.Fatalf("clean view flagged: %v", v)
	}
	// n2 applied a different command at index 2.
	bad := cleanView(1)
	bad.Nodes[1].Applied = []core.CommittedEntry{applied(1, 1, "a"), applied(2, 2, "DIFFERENT")}
	bad.Nodes[1].Log = []core.LogEntry{entry(1, 1, "a"), entry(2, 2, "DIFFERENT")}
	v := InvStateMachineSafetyCheck(bad)
	if v == nil || v.Invariant != InvStateMachineSafety {
		t.Fatalf("expected state-machine-safety violation, got %v", v)
	}
}

func TestInvCommitMonotonicity_DecreaseFlagged(t *testing.T) {
	m := NewMonitor()
	// First view: commit index 2 everywhere — clean.
	if vs := m.CheckAll(cleanView(1)); len(vs) != 0 {
		t.Fatalf("clean first view flagged: %v", vs)
	}
	// Second view: n1's commit index drops to 1 — monotonicity violation.
	v2 := cleanView(2)
	v2.Nodes[0].CommitIndex = 1
	vs := m.CheckAll(v2)
	if !hasInvariant(vs, InvCommitMonotonicity) {
		t.Fatalf("expected commit-monotonicity violation, got %v", vs)
	}
}

func TestInvCommitMonotonicity_AppliedExceedsCommit(t *testing.T) {
	m := NewMonitor()
	bad := cleanView(1)
	bad.Nodes[0].CommitIndex = 1 // but Applied goes through index 2
	vs := m.CheckAll(bad)
	if !hasInvariant(vs, InvCommitMonotonicity) {
		t.Fatalf("expected applied>commit violation, got %v", vs)
	}
}

func TestMonitor_CheckAll_CleanNoViolations(t *testing.T) {
	m := NewMonitor()
	for step := uint64(1); step <= 3; step++ {
		if vs := m.CheckAll(cleanView(step)); len(vs) != 0 {
			t.Fatalf("step %d: clean view produced violations: %v", step, vs)
		}
	}
}

// hasInvariant reports whether vs contains a violation for the given invariant.
func hasInvariant(vs []Violation, id InvariantID) bool {
	for _, v := range vs {
		if v.Invariant == id {
			return true
		}
	}
	return false
}
