// Command viz runs one quorum simulation and writes its trace to two SVG figures
// (a term/leader timeline and a node-liveness strip) under an output directory, so
// the README can reference real figures generated from a seed.
//
// Usage:
//
//	go run ./tools/viz/main -seed 42 -schedule crashy -size 5 -out docs/figures/
//
// The run is a pure function of (seed, schedule, size) — the frozen sim contract —
// so the same flags reproduce byte-identical SVG. This is a thin driver: it captures
// the per-step ClusterView stream via Simulator.OnStep and hands it to the pure
// renderers in package viz.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/config"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/sim"
	"github.com/billdmar/quorum/tools/viz"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "viz:", err)
		os.Exit(1)
	}
}

// run parses flags, executes the simulation while capturing every ClusterView, and
// writes the two figures. Kept separate from main so it returns an error rather than
// calling os.Exit, which is easier to reason about.
func run() error {
	seed := flag.Uint64("seed", 42, "simulation seed")
	schedule := flag.String("schedule", "crashy", "fault schedule name (clean|lossy|partition-heavy|asymmetric|crashy|disk-faulty|kitchen-sink)")
	size := flag.Int("size", 5, "cluster size (odd: 3 or 5)")
	out := flag.String("out", "docs/figures/", "output directory for the SVG figures")
	flag.Parse()

	sched, err := parseSchedule(*schedule)
	if err != nil {
		return err
	}

	// Capture the per-step ClusterView stream. The sim publishes a fresh view after
	// every core step; we retain them all (a run is bounded by MaxTicks) and let the
	// renderer downsample for width.
	var views []check.ClusterView
	s := sim.NewSimulator(sim.Params{Seed: *seed, Schedule: sched, ClusterSize: *size})
	s.OnStep = func(cv check.ClusterView) { views = append(views, cv) }
	s.Run()

	if len(views) == 0 {
		return fmt.Errorf("run produced no steps (seed=%d schedule=%s size=%d)", *seed, *schedule, *size)
	}

	ids := nodeIDs(*size)
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	timeline := filepath.Join(*out, "timeline.svg")
	activity := filepath.Join(*out, "activity.svg")
	if err := os.WriteFile(timeline, []byte(viz.TermLeaderTimeline(views, ids)), 0o644); err != nil {
		return fmt.Errorf("write timeline: %w", err)
	}
	if err := os.WriteFile(activity, []byte(viz.LivenessStrip(views, ids)), 0o644); err != nil {
		return fmt.Errorf("write activity: %w", err)
	}

	fmt.Printf("seed=%d schedule=%s size=%d steps=%d trace=%#x\n",
		*seed, *schedule, *size, len(views), s.TraceHash())
	fmt.Println("wrote", timeline)
	fmt.Println("wrote", activity)
	return nil
}

// parseSchedule maps a flag string to a registered ScheduleName, rejecting unknown
// names against the frozen registry so a typo fails loudly rather than silently
// running the clean schedule.
func parseSchedule(name string) (config.ScheduleName, error) {
	sn := config.ScheduleName(name)
	if _, ok := config.Schedules[sn]; !ok {
		return "", fmt.Errorf("unknown schedule %q", name)
	}
	return sn, nil
}

// nodeIDs builds the stable node-id slice "n0".."n{size-1}" that the simulator uses,
// in index order — the fixed row order the renderers draw.
func nodeIDs(size int) []core.NodeID {
	ids := make([]core.NodeID, size)
	for i := range ids {
		ids[i] = core.NodeID(fmt.Sprintf("n%d", i))
	}
	return ids
}
