package check

// convert.go turns a recorded History into the Porcupine input form. We use the
// Operation representation (not the Event one): each operation carries an
// explicit [Call, Return] interval taken from the simulator's logical Stamps, so
// real-time precedence is expressed directly by the deterministic clock rather
// than by relative event order. Operations from one client are naturally
// sequential (their intervals do not overlap); operations from different clients
// overlap exactly when their [Call, Return] intervals do.
//
// UNKNOWN OUTCOMES. Two cases map to kvOutput.unknown = true, letting the
// nondeterministic model place the op as having-happened OR not:
//   - a Response event flagged Unknown (client timed out amid a leader change);
//   - an operation with an Invoke but NO Response at all (still pending at the
//     end of the run). Following Porcupine's own etcd example, we synthesize a
//     Return just past the last stamp so the pending op spans to the end of the
//     timeline instead of being dropped.

import (
	"sort"

	"github.com/anishathalye/porcupine"
)

// clientIDs maps the sparse uint64 client ids in a history to the dense,
// zero-indexed ints Porcupine expects, assigned in sorted order for
// reproducibility. Returned so callers can relate a visualization back to the
// original client.
func clientIDs(h History) map[uint64]int {
	seen := make(map[uint64]struct{})
	for _, e := range h.Events {
		seen[e.Client] = struct{}{}
	}
	ids := make([]uint64, 0, len(seen))
	for c := range seen {
		ids = append(ids, c)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	m := make(map[uint64]int, len(ids))
	for i, c := range ids {
		m[c] = i
	}
	return m
}

// partialOp accumulates the invoke and (optional) response halves of one
// operation as we scan the history, keyed by OpID.
type partialOp struct {
	in       kvInput
	client   uint64
	call     uint64
	out      kvOutput
	returned bool
}

// ToOperations converts a History into the deterministic []porcupine.Operation
// the checker consumes. Operations are sorted by (Call, OpID) so the same
// history always yields the same slice — a failing seed replays identically.
func ToOperations(h History) []porcupine.Operation {
	clients := clientIDs(h)

	// Highest stamp in the run; pending ops get a Return just past it.
	var maxStamp uint64
	for _, e := range h.Events {
		if e.Stamp > maxStamp {
			maxStamp = e.Stamp
		}
	}
	pendingReturn := maxStamp + 1

	// Fold invoke/response pairs together by OpID.
	byID := make(map[uint64]*partialOp)
	order := make([]uint64, 0) // OpIDs in first-seen order, for stable iteration
	for _, e := range h.Events {
		p, ok := byID[e.OpID]
		if !ok {
			if e.Stage != StageInvoke {
				// A response with no matching invoke can't be placed; skip it.
				continue
			}
			p = &partialOp{
				in:     kvInput{kind: e.Kind, key: e.Key, value: e.Value, compareValue: e.CompareValue},
				client: e.Client,
				call:   e.Stamp,
			}
			byID[e.OpID] = p
			order = append(order, e.OpID)
			continue
		}
		if e.Stage == StageResponse {
			p.returned = true
			p.out = kvOutput{value: e.Output, ok: e.OK, unknown: e.Unknown}
		}
	}

	ops := make([]porcupine.Operation, 0, len(order))
	for _, id := range order {
		p := byID[id]
		ret := pendingReturn
		out := p.out
		if p.returned {
			ret = responseStamp(h, id)
		} else {
			// Never learned the outcome: pending to the end, outcome unknown.
			out = kvOutput{unknown: true}
		}
		ops = append(ops, porcupine.Operation{
			ClientId: clients[p.client],
			Input:    p.in,
			Call:     int64(p.call),
			Output:   out,
			Return:   int64(ret),
		})
	}

	sort.Slice(ops, func(i, j int) bool {
		if ops[i].Call != ops[j].Call {
			return ops[i].Call < ops[j].Call
		}
		return ops[i].Return < ops[j].Return
	})
	return ops
}

// responseStamp returns the Stamp of the StageResponse event for opID. The
// caller only invokes this when a response is known to exist.
func responseStamp(h History, opID uint64) uint64 {
	for _, e := range h.Events {
		if e.OpID == opID && e.Stage == StageResponse {
			return e.Stamp
		}
	}
	return 0 // unreachable: caller guarantees a response exists
}
