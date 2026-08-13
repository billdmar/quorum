package check

// linearizable.go is the top-level linearizability entry point. It builds the
// KV model, converts the history to Porcupine operations, and runs the checker.
//
// The bound on checking effort is expressed as Porcupine's OWN timeout (passed
// to CheckOperationsVerbose), NOT as wall-clock logic in this package: the
// checker itself reads no clock and spawns no goroutines, keeping the
// verification path free of the nondeterminism the project forbids. A timeout
// that elapses yields porcupine.Unknown, which we surface honestly as
// "undetermined" rather than silently treating it as linearizable.

import (
	"time"

	"github.com/anishathalye/porcupine"
)

// CheckTimeout bounds the linearizability search. Linearizability checking is
// NP-hard; a run that cannot be decided within this budget returns Unknown
// (LinResult.Determined == false) instead of blocking forever. The value is
// generous for the small per-run histories the sim produces; a genuinely
// non-linearizable history is almost always reported as Illegal well within it.
const CheckTimeout = 30 * time.Second

// LinResult is the verdict for one history, carrying enough context to report
// and reproduce a failing seed. Linearizable is true only for a definite Ok;
// an Unknown (timeout) sets Determined = false and Linearizable = false so a
// caller never mistakes "undecided" for "safe."
type LinResult struct {
	Linearizable bool // true iff Porcupine returned Ok
	Determined   bool // false iff the check timed out (Unknown)
	Result       porcupine.CheckResult
	Seed         uint64
	Schedule     string
	OpCount      int
	// Info is Porcupine's verbose linearization data; on a failure it drives
	// the visualization / partial-linearization report for the failing seed.
	Info porcupine.LinearizationInfo
}

// CheckLinearizable checks whether the recorded history is linearizable against
// the KV model. It returns (linearizable, result); the second value carries the
// seed/schedule and Porcupine's verbose info so a red run can be reproduced and
// visualized. A timeout is reported as not-linearizable AND not-determined —
// investigate, never assume safe.
func CheckLinearizable(h History) (bool, LinResult) {
	ops := ToOperations(h)

	// Trivial-history short-circuit (SOUND, not a weakening): a history in which
	// NO operation has a definite outcome — every response is Unknown (the client
	// never learned whether its op took effect) or the op never returned — carries
	// no observable output for any ordering to contradict. Under the
	// nondeterministic KV model every such op may resolve as "did not happen," so
	// the empty linearization satisfies the whole history: it is trivially Ok. We
	// assert that directly instead of handing Porcupine a maximally-concurrent
	// all-Unknown history, which is its exponential worst case and merely times
	// out to Unknown. A real violation requires at least one DEFINITE result, so
	// this can never mask one. (Found by  seed 7503, kitchen-sink: 20/20 ops
	// Unknown, Porcupine ran 48s to no verdict.)
	if !hasDefiniteOutcome(h) {
		return true, LinResult{
			Result: porcupine.Ok, Determined: true, Linearizable: true,
			Seed: h.Seed, Schedule: h.Schedule, OpCount: len(ops),
		}
	}

	model := KVModel()
	res, info := porcupine.CheckOperationsVerbose(model, ops, CheckTimeout)

	out := LinResult{
		Result:       res,
		Determined:   res != porcupine.Unknown,
		Linearizable: res == porcupine.Ok,
		Seed:         h.Seed,
		Schedule:     h.Schedule,
		OpCount:      len(ops),
		Info:         info,
	}
	return out.Linearizable, out
}

// hasDefiniteOutcome reports whether any operation in the history has a definite
// (non-Unknown) response — the only kind of event that can constrain a
// linearization. A history with none is trivially linearizable (see the
// short-circuit in CheckLinearizable).
func hasDefiniteOutcome(h History) bool {
	for _, e := range h.Events {
		if e.Stage == StageResponse && !e.Unknown {
			return true
		}
	}
	return false
}
