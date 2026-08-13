package check

// Tests for the human-readable describe/String helpers used in failure output,
// and a few model/monitor edge cases, to keep the checking package's coverage
// high and its diagnostic strings honest.

import (
	"strings"
	"testing"

	"github.com/billdmar/quorum/core"
)

func TestOpKindString(t *testing.T) {
	cases := map[OpKind]string{OpGet: "get", OpPut: "put", OpAppend: "append", OpCAS: "cas", OpKind(99): "unknown"}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Fatalf("OpKind(%d).String() = %q, want %q", k, got, want)
		}
	}
}

func TestInvariantIDString(t *testing.T) {
	cases := map[InvariantID]string{
		InvElectionSafety:     "election-safety",
		InvLogMatching:        "log-matching",
		InvLeaderCompleteness: "leader-completeness",
		InvStateMachineSafety: "state-machine-safety",
		InvCommitMonotonicity: "commit-monotonicity",
		InvariantID(99):       "unknown",
	}
	for id, want := range cases {
		if got := id.String(); got != want {
			t.Fatalf("InvariantID(%d).String() = %q, want %q", id, got, want)
		}
	}
}

func TestDescribeState(t *testing.T) {
	got := describeState(kvState{"y": "2", "x": "1"})
	if got != "{x=1, y=2}" { // keys must be sorted for determinism
		t.Fatalf("describeState = %q, want {x=1, y=2}", got)
	}
	if describeState(kvState{}) != "{}" {
		t.Fatal("empty state should render as {}")
	}
}

func TestDescribeOp(t *testing.T) {
	cases := []struct {
		in   kvInput
		out  kvOutput
		want string
	}{
		{kvInput{kind: OpGet, key: "x"}, kvOutput{value: "1"}, "get(x) -> '1'"},
		{kvInput{kind: OpGet, key: "x"}, kvOutput{unknown: true}, "get(x) -> unknown"},
		{kvInput{kind: OpPut, key: "x", value: "1"}, kvOutput{}, "put(x, '1')"},
		{kvInput{kind: OpAppend, key: "x", value: "b"}, kvOutput{value: "ab"}, "append(x, 'b') -> 'ab'"},
		{kvInput{kind: OpCAS, key: "x", compareValue: "1", value: "2"}, kvOutput{ok: true}, "cas(x, '1'->'2') -> ok"},
		{kvInput{kind: OpCAS, key: "x", compareValue: "1", value: "2"}, kvOutput{ok: false}, "cas(x, '1'->'2') -> fail"},
		{kvInput{kind: OpCAS, key: "x", compareValue: "1", value: "2"}, kvOutput{unknown: true}, "cas(x, '1'->'2') -> unknown"},
		{kvInput{kind: OpKind(99)}, kvOutput{}, "<invalid>"},
	}
	for _, c := range cases {
		if got := describeOp(c.in, c.out); got != c.want {
			t.Fatalf("describeOp(%v,%v) = %q, want %q", c.in, c.out, got, c.want)
		}
	}
}

func TestKVStateEqualAndHash(t *testing.T) {
	a := kvState{"x": "1", "y": "2"}
	b := kvState{"y": "2", "x": "1"}
	if !kvEqual(a, b) {
		t.Fatal("equal states reported unequal")
	}
	if kvHash(a) != kvHash(b) {
		t.Fatal("equal states must share a hash")
	}
	if kvEqual(a, kvState{"x": "1"}) {
		t.Fatal("different-size states reported equal")
	}
	if kvEqual(a, kvState{"x": "1", "y": "9"}) {
		t.Fatal("differing-value states reported equal")
	}
}

func TestStepKV_InvalidKind(t *testing.T) {
	if got := stepKV(kvState{}, kvInput{kind: OpKind(99)}, kvOutput{}); got != nil {
		t.Fatalf("invalid op kind should yield no next state, got %v", got)
	}
}

// TestDescribeOperationInvokedByVisualizer confirms the model's describe hooks
// are wired into the Porcupine model (so failing-seed visualizations render).
func TestModelDescribeHooksWired(t *testing.T) {
	m := KVModel()
	if m.DescribeOperation == nil || m.DescribeState == nil {
		t.Fatal("model must expose describe hooks for visualization")
	}
	// The ToModel wrapper wraps DescribeState over a set of states; ensure it
	// renders our per-state description within.
	s := m.DescribeState(m.Init())
	if !strings.Contains(s, "{") {
		t.Fatalf("wrapped DescribeState looks wrong: %q", s)
	}
}

func TestAppliedIndex_Empty(t *testing.T) {
	if got := appliedIndex(NodeView{}); got != 0 {
		t.Fatalf("appliedIndex of empty node = %d, want 0", got)
	}
	n := NodeView{Applied: []core.CommittedEntry{applied(1, 1, "a"), applied(2, 1, "b")}}
	if got := appliedIndex(n); got != 2 {
		t.Fatalf("appliedIndex = %d, want 2", got)
	}
}
