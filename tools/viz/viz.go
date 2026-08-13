// Package viz renders quorum simulation traces into dependency-free SVG figures
// for the README, so a reviewer can SEE the cluster surviving faults rather than
// reading a table of numbers. It consumes the frozen check.ClusterView stream a
// Simulator publishes via OnStep and emits hand-built SVG strings (no third-party
// deps, stdlib only) that GitHub renders inline.
//
// Two figures are produced:
//
//   - TermLeaderTimeline: per-node rows across logical time, colored by Raft role,
//     annotated at each new leadership with the term — the reader watches
//     leadership change hands and terms advance.
//   - LivenessStrip: per-node rows across logical time, colored by whether the node
//     is PARTICIPATING or DOWN/IDLE — the reader watches nodes drop out (crash or
//     partition) and rejoin while the cluster as a whole keeps a leader.
//
// WHAT THESE FIGURES DO AND DO NOT SHOW (be precise — the sim does not expose its
// partition set in a ClusterView): a node's "down/idle" state is DERIVED from its
// view, not read from a fault flag. sim/node.go view() reports a crashed node as an
// empty follower in term 0 with an empty log; a not-yet-started node looks the same
// at t=0. So the strips render "not participating" = Follower AND term 0 AND empty
// log. This CANNOT distinguish a crash from a network partition from a cold start —
// it only shows that a node is contributing no Raft state at that step. That honest
// limitation is documented on the figure itself as a caption.
//
// DETERMINISM: rendering is a pure function of its inputs. The same views and node
// order always produce byte-identical SVG — every loop iterates a slice in fixed
// index order, no map is ever ranged to produce output, and all numbers are
// formatted with strconv. This mirrors the core's determinism contract so the
// figures can be regenerated reproducibly from a seed.
package viz

import (
	"strconv"
	"strings"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
)

// Layout constants (SVG user units ≈ px). Chosen so a ~5-node, ~180-column figure
// fits comfortably inline on GitHub without horizontal scrolling.
const (
	leftGutter   = 64  // width reserved for node-id labels
	topMargin    = 56  // title + legend band above the rows
	bottomMargin = 40  // caption band below the rows
	rowH         = 26  // height of one node row
	cellW        = 7   // width of one sampled-step cell
	maxCols      = 180 // cap on rendered columns; more steps are downsampled
	rightMargin  = 16
	fontFamily   = "-apple-system,Segoe UI,Roboto,sans-serif"
)

// palette holds the fixed role/liveness colors. Hues are chosen to stay legible for
// the common red-green color-vision deficiency (leader amber vs follower teal vs
// candidate purple are distinguishable by hue AND lightness), and "down" is a light
// neutral gray so gaps read as absence rather than a fourth active state.
var (
	colLeader    = "#d97706" // amber — a node that believes itself leader
	colCandidate = "#7c3aed" // purple — mid-election
	colFollower  = "#0891b2" // teal — active follower
	colDown      = "#e5e7eb" // light gray — not participating (down/idle/cold)
	colUp        = "#059669" // green — participating (liveness strip)
	colText      = "#111827"
	colMuted     = "#6b7280"
	colGrid      = "#ffffff"
)

// TermLeaderTimeline renders per-node role over logical time. Rows are the nodes in
// the given nodeIDs order (fixed, caller-controlled); the x-axis is the sequence of
// (possibly downsampled) simulation steps. Each cell is colored by the node's Raft
// role at that step, and the term of each new leadership is labeled so the reader
// sees leadership changing hands and terms advancing. Returns a complete SVG string.
func TermLeaderTimeline(views []check.ClusterView, nodeIDs []core.NodeID) string {
	cols := sampleColumns(len(views), maxCols)
	width := leftGutter + len(cols)*cellW + rightMargin
	height := topMargin + len(nodeIDs)*rowH + bottomMargin

	var b strings.Builder
	svgHeader(&b, width, height, "Term / leader timeline")
	legend(&b, width, []legendItem{
		{colLeader, "leader"}, {colCandidate, "candidate"},
		{colFollower, "follower"}, {colDown, "down / idle"},
	})

	for row, id := range nodeIDs {
		y := topMargin + row*rowH
		nodeLabel(&b, id, y)
		prevTerm := core.Term(0)
		prevRole := core.Follower
		for c, vi := range cols {
			nv, ok := lookup(views[vi], id)
			x := leftGutter + c*cellW
			fill := colDown
			if ok {
				fill = roleColor(nv)
			}
			rect(&b, x, y+2, cellW, rowH-4, fill)
			// Label the term at each transition INTO leadership: that is exactly the
			// moment leadership changes hands, and stamping its term makes term
			// advancement legible without cluttering every cell.
			if ok && nv.Role == core.Leader && (prevRole != core.Leader || nv.Term != prevTerm) {
				text(&b, x+1, y+rowH-6, 9, colText, "T"+strconv.FormatUint(uint64(nv.Term), 10))
			}
			if ok {
				prevTerm, prevRole = nv.Term, nv.Role
			}
		}
	}

	caption(&b, width, height,
		"x = simulation step (downsampled). Cell color = Raft role; TN marks a node "+
			"taking leadership in term N.")
	b.WriteString("</svg>\n")
	return b.String()
}

// LivenessStrip renders per-node participation over logical time. Rows are the nodes
// in nodeIDs order; each cell is green when the node is participating and gray when
// it is DOWN/IDLE (derived — see package doc: cannot distinguish crash vs partition
// vs cold start). The figure lets a reviewer watch nodes drop out and rejoin while
// the cluster keeps making progress. Returns a complete SVG string.
func LivenessStrip(views []check.ClusterView, nodeIDs []core.NodeID) string {
	cols := sampleColumns(len(views), maxCols)
	width := leftGutter + len(cols)*cellW + rightMargin
	height := topMargin + len(nodeIDs)*rowH + bottomMargin

	var b strings.Builder
	svgHeader(&b, width, height, "Node liveness (fault activity)")
	legend(&b, width, []legendItem{
		{colUp, "participating"}, {colDown, "down / idle"},
	})

	for row, id := range nodeIDs {
		y := topMargin + row*rowH
		nodeLabel(&b, id, y)
		for c, vi := range cols {
			nv, ok := lookup(views[vi], id)
			x := leftGutter + c*cellW
			fill := colDown
			if ok && !isDown(nv) {
				fill = colUp
			}
			rect(&b, x, y+2, cellW, rowH-4, fill)
		}
	}

	caption(&b, width, height,
		"x = simulation step (downsampled). Green = node contributing Raft state; "+
			"gray = down/idle (crash, partition, or pre-start — indistinguishable here).")
	b.WriteString("</svg>\n")
	return b.String()
}

// isDown reports whether a NodeView is "not participating": a follower in term 0
// with an empty log. sim/node.go view() emits exactly this shape for a crashed node,
// and a not-yet-started node is identical, so this classification is honest about
// what it can and cannot tell apart (see package doc).
func isDown(nv check.NodeView) bool {
	return nv.Role == core.Follower && nv.Term == 0 && len(nv.Log) == 0
}

// roleColor maps a participating node's role to its cell color, treating a "down"
// view as gray so the timeline and the liveness strip agree on what a gap looks like.
func roleColor(nv check.NodeView) string {
	if isDown(nv) {
		return colDown
	}
	switch nv.Role {
	case core.Leader:
		return colLeader
	case core.Candidate:
		return colCandidate
	default:
		return colFollower
	}
}

// lookup returns the NodeView for id in cv, scanning the (small, index-ordered)
// Nodes slice — never a map — so output order stays deterministic. ok is false if
// the id is absent (defensive; the sim always emits every node).
func lookup(cv check.ClusterView, id core.NodeID) (check.NodeView, bool) {
	for _, nv := range cv.Nodes {
		if nv.ID == id {
			return nv, true
		}
	}
	return check.NodeView{}, false
}

// sampleColumns returns the indices into a length-n view slice to render as columns.
// When n fits within max it returns every step; otherwise it samples max columns
// evenly across [0,n-1] (first and last always included) so the downsampled figure
// still spans the whole run. Deterministic integer math, no rounding drift.
func sampleColumns(n, max int) []int {
	if n <= 0 {
		return nil
	}
	if n <= max {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}
	out := make([]int, max)
	for c := 0; c < max; c++ {
		out[c] = c * (n - 1) / (max - 1)
	}
	return out
}

// legendItem is one color swatch plus its label in a figure legend.
type legendItem struct {
	color string
	label string
}

// svgHeader writes the opening <svg> tag, a background, and the figure title.
func svgHeader(b *strings.Builder, width, height int, title string) {
	b.WriteString(`<svg xmlns="http://www.w3.org/2000/svg" width="`)
	b.WriteString(strconv.Itoa(width))
	b.WriteString(`" height="`)
	b.WriteString(strconv.Itoa(height))
	b.WriteString(`" viewBox="0 0 `)
	b.WriteString(strconv.Itoa(width))
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(height))
	b.WriteString(`" font-family="`)
	b.WriteString(fontFamily)
	b.WriteString("\">\n")
	rect(b, 0, 0, width, height, "#ffffff")
	text(b, leftGutter, 20, 14, colText, escape(title))
}

// legend writes the color-swatch legend row beneath the title, laid out left to
// right in the given fixed order.
func legend(b *strings.Builder, width int, items []legendItem) {
	_ = width
	x := leftGutter
	y := 32
	for _, it := range items {
		rect(b, x, y, 12, 12, it.color)
		text(b, x+16, y+11, 11, colMuted, escape(it.label))
		x += 20 + len(it.label)*7
	}
}

// nodeLabel writes a node's id in the left gutter, vertically centered on its row.
func nodeLabel(b *strings.Builder, id core.NodeID, y int) {
	text(b, 8, y+rowH/2+4, 12, colText, escape(string(id)))
}

// caption writes the italic explanatory caption in the bottom band, wrapping is left
// to the renderer (the string is kept short enough to fit typical figure widths).
func caption(b *strings.Builder, width, height int, s string) {
	_ = width
	text(b, 8, height-14, 10, colMuted, escape(s))
}

// rect writes one filled rectangle. A hairline stroke in the background color keeps
// adjacent cells visually separated without adding a color of its own.
func rect(b *strings.Builder, x, y, w, h int, fill string) {
	b.WriteString(`<rect x="`)
	b.WriteString(strconv.Itoa(x))
	b.WriteString(`" y="`)
	b.WriteString(strconv.Itoa(y))
	b.WriteString(`" width="`)
	b.WriteString(strconv.Itoa(w))
	b.WriteString(`" height="`)
	b.WriteString(strconv.Itoa(h))
	b.WriteString(`" fill="`)
	b.WriteString(fill)
	b.WriteString(`" stroke="`)
	b.WriteString(colGrid)
	b.WriteString(`" stroke-width="0.5"/>`)
	b.WriteByte('\n')
}

// text writes one text element at (x,y) with the given font size and fill.
func text(b *strings.Builder, x, y, size int, fill, s string) {
	b.WriteString(`<text x="`)
	b.WriteString(strconv.Itoa(x))
	b.WriteString(`" y="`)
	b.WriteString(strconv.Itoa(y))
	b.WriteString(`" font-size="`)
	b.WriteString(strconv.Itoa(size))
	b.WriteString(`" fill="`)
	b.WriteString(fill)
	b.WriteString(`">`)
	b.WriteString(s)
	b.WriteString("</text>\n")
}

// escape replaces the XML metacharacters that can appear in labels/captions so the
// emitted SVG is always well-formed. Node ids are simple ("n0".."nk") but captions
// contain punctuation, so escaping is applied uniformly.
func escape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}
