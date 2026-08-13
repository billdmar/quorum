package viz

import (
	"strings"
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/sim"
)

// runViews executes a bounded simulation and returns its captured ClusterView stream
// plus the node ids, so tests exercise the renderers against real trace shapes (not
// hand-built fixtures that might diverge from what the sim actually emits).
func runViews(t *testing.T, seed uint64, sched config.ScheduleName, size int) ([]check.ClusterView, []core.NodeID) {
	t.Helper()
	var views []check.ClusterView
	s := sim.NewSimulator(sim.Params{Seed: seed, Schedule: sched, ClusterSize: size})
	s.OnStep = func(cv check.ClusterView) { views = append(views, cv) }
	s.Run()
	if len(views) == 0 {
		t.Fatalf("simulation produced no views (seed=%d sched=%s size=%d)", seed, sched, size)
	}
	ids := make([]core.NodeID, size)
	for i := range ids {
		ids[i] = core.NodeID("n" + itoa(i))
	}
	return views, ids
}

// itoa is a tiny local int->string to keep the test import set minimal.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// assertWellFormed checks the structural contract the README depends on: a complete
// <svg>…</svg> document, non-empty, with a rect and a text element per node row.
func assertWellFormed(t *testing.T, svg string, nodeCount int) {
	t.Helper()
	if svg == "" {
		t.Fatal("empty SVG")
	}
	if !strings.HasPrefix(svg, "<svg") {
		t.Errorf("SVG does not start with <svg: %.20q", svg)
	}
	if !strings.HasSuffix(strings.TrimSpace(svg), "</svg>") {
		t.Errorf("SVG does not close with </svg>")
	}
	// At least one node-label <text> and one cell <rect> per node row, plus the
	// title/legend/caption chrome — so counts comfortably exceed nodeCount.
	if got := strings.Count(svg, "<rect"); got < nodeCount {
		t.Errorf("rect count = %d, want >= %d (one cell per node minimum)", got, nodeCount)
	}
	if got := strings.Count(svg, "<text"); got < nodeCount {
		t.Errorf("text count = %d, want >= %d (one label per node minimum)", got, nodeCount)
	}
}

// TestTimelineWellFormed renders the term/leader timeline for a representative
// fault schedule and asserts it is a structurally valid SVG.
func TestTimelineWellFormed(t *testing.T) {
	views, ids := runViews(t, 42, config.ScheduleCrashy, 5)
	assertWellFormed(t, TermLeaderTimeline(views, ids), len(ids))
}

// TestLivenessWellFormed renders the liveness strip and asserts it is valid.
func TestLivenessWellFormed(t *testing.T) {
	views, ids := runViews(t, 42, config.ScheduleCrashy, 5)
	assertWellFormed(t, LivenessStrip(views, ids), len(ids))
}

// TestDeterministicRendering is the core guarantee: rendering the same views in the
// same node order twice yields byte-identical SVG (no map ranged in output order, no
// nondeterministic formatting). Regenerating a figure from a seed is reproducible.
func TestDeterministicRendering(t *testing.T) {
	views, ids := runViews(t, 7, config.SchedulePartitionHeavy, 5)
	cases := map[string]func([]check.ClusterView, []core.NodeID) string{
		"timeline": TermLeaderTimeline,
		"liveness": LivenessStrip,
	}
	for name, render := range cases {
		a := render(views, ids)
		b := render(views, ids)
		if a != b {
			t.Errorf("%s rendering is not deterministic: %d vs %d bytes differ", name, len(a), len(b))
		}
	}
}

// TestDeterministicAcrossRuns confirms the whole pipeline is reproducible from a
// seed: two independent simulations with identical params produce identical figures.
func TestDeterministicAcrossRuns(t *testing.T) {
	v1, ids1 := runViews(t, 99, config.ScheduleLossy, 3)
	v2, ids2 := runViews(t, 99, config.ScheduleLossy, 3)
	if TermLeaderTimeline(v1, ids1) != TermLeaderTimeline(v2, ids2) {
		t.Error("timeline differs across identical seeded runs")
	}
	if LivenessStrip(v1, ids1) != LivenessStrip(v2, ids2) {
		t.Error("liveness strip differs across identical seeded runs")
	}
}

// TestSampleColumns checks the downsampling: it returns every step below the cap,
// caps at max above it, and always spans first..last so the figure covers the run.
func TestSampleColumns(t *testing.T) {
	if got := sampleColumns(0, 10); got != nil {
		t.Errorf("sampleColumns(0)=%v, want nil", got)
	}
	if got := sampleColumns(5, 10); len(got) != 5 || got[0] != 0 || got[4] != 4 {
		t.Errorf("sampleColumns(5,10)=%v, want 0..4", got)
	}
	got := sampleColumns(1000, 180)
	if len(got) != 180 {
		t.Fatalf("sampleColumns(1000,180) len=%d, want 180", len(got))
	}
	if got[0] != 0 || got[179] != 999 {
		t.Errorf("downsample endpoints = %d..%d, want 0..999", got[0], got[179])
	}
	// Columns must be non-decreasing (evenly spaced, no drift backwards).
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("column %d (%d) < previous (%d)", i, got[i], got[i-1])
		}
	}
}

// TestIsDown checks the DERIVED not-participating classification matches how
// sim/node.go view() reports a crashed node (empty follower, term 0, empty log) and
// nothing else.
func TestIsDown(t *testing.T) {
	down := check.NodeView{ID: "n0", Role: core.Follower, Term: 0}
	if !isDown(down) {
		t.Error("empty follower in term 0 should be down")
	}
	activeFollower := check.NodeView{ID: "n0", Role: core.Follower, Term: 3, Log: []core.LogEntry{{Term: 3, Index: 1}}}
	if isDown(activeFollower) {
		t.Error("a follower with a log in term 3 is participating, not down")
	}
	leader := check.NodeView{ID: "n0", Role: core.Leader, Term: 2}
	if isDown(leader) {
		t.Error("a leader is never down")
	}
}
