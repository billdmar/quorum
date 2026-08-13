package check

// model.go defines the Porcupine sequential specification of the replicated KV
// store. Porcupine searches for a total order of the recorded client operations
// that respects real-time precedence AND is legal under this model; if none
// exists the history is non-linearizable (a safety bug).
//
// UNKNOWN-OUTCOME OPERATIONS. An operation the client never learned the result
// of — a request that timed out amid a leader change, or one with no recorded
// response at all — "may or may not have taken effect." The only sound way to
// check such a history is a NondeterministicModel: an unknown mutation yields
// BOTH the applied and the not-applied next state, so Porcupine is free to
// place it either way. A deterministic model that force-applied every unknown
// write would raise false non-linearizable verdicts (the primary's Porcupine
// gate demands ZERO false alarms). We build the nondeterministic model and
// convert it to a plain porcupine.Model via ToModel so CheckOperationsVerbose
// can drive it. See convert.go for how the [Call,Return] interval and the
// unknown flag are derived from the history.

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// kvInput is the Porcupine input for one client operation: the op kind plus its
// request arguments, taken verbatim from the HistoryEvent's request fields.
type kvInput struct {
	kind         OpKind
	key          string
	value        string // Put/Append/CAS new value
	compareValue string // CAS expected value
}

// kvOutput is the Porcupine output for one client operation: the value the
// client observed (Get / Append result), the CAS success bit, and whether the
// outcome was unknown. An unknown output never constrains the model — the op is
// modeled as possibly having happened and possibly not.
type kvOutput struct {
	value   string
	ok      bool // CAS success bit (meaningful for OpCAS)
	unknown bool
}

// kvState is the model's KV state: key -> value. Step treats it as immutable
// (each mutating step returns a fresh clone) to honor Porcupine's requirement
// that the model be a pure function of its inputs.
type kvState map[string]string

// clone returns a copy so a mutating step never modifies the state it was given.
func (s kvState) clone() kvState {
	c := make(kvState, len(s)+1)
	for k, v := range s {
		c[k] = v
	}
	return c
}

// KVModel returns the Porcupine model for the replicated KV store. A
// known-outcome operation has exactly one legal next state (or none, if its
// recorded output is impossible under the sequential spec — that "none" is how
// a stale read is caught). An unknown-outcome operation returns every next
// state consistent with it having, or not having, taken effect.
func KVModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init: func() []interface{} { return []interface{}{kvState{}} },
		Step: func(state, input, output interface{}) []interface{} {
			return stepKV(state.(kvState), input.(kvInput), output.(kvOutput))
		},
		Equal:             func(a, b interface{}) bool { return kvEqual(a.(kvState), b.(kvState)) },
		Hash:              func(s interface{}) uint64 { return kvHash(s.(kvState)) },
		DescribeOperation: describeOp,
		DescribeState:     describeState,
	}
	return nm.ToModel()
}

// stepKV returns the set of next states the model may enter given the current
// state, the operation, and its recorded output. An empty result means the
// output is impossible from this state (an illegal step). This is the sequential
// specification: Get reads, Put sets, Append concatenates, CAS swaps iff the
// current value equals CompareValue.
func stepKV(st kvState, in kvInput, out kvOutput) []interface{} {
	switch in.kind {
	case OpGet:
		// A read never changes state. Unknown reads accept any observed value;
		// otherwise the observed value must equal the current one ("" if absent).
		if out.unknown || out.value == st[in.key] {
			return []interface{}{st}
		}
		return nil
	case OpPut:
		// Put's recorded output is not meaningful; success is assumed. Its only
		// effect is the new mapping.
		applied := st.clone()
		applied[in.key] = in.value
		if out.unknown {
			return []interface{}{applied, st} // may or may not have taken effect
		}
		return []interface{}{applied}
	case OpAppend:
		newVal := st[in.key] + in.value
		applied := st.clone()
		applied[in.key] = newVal
		if out.unknown {
			return []interface{}{applied, st}
		}
		// Append returns the resulting value; it must match the model's.
		if out.value == newVal {
			return []interface{}{applied}
		}
		return nil
	case OpCAS:
		swap := st[in.key] == in.compareValue
		applied := st
		if swap {
			applied = st.clone()
			applied[in.key] = in.value
		}
		if out.unknown {
			if swap {
				return []interface{}{applied, st} // ran (swapped) or never ran
			}
			return []interface{}{st} // no swap possible ⇒ state is unchanged either way
		}
		// Known outcome: the success bit must match whether a swap was possible.
		if out.ok == swap {
			return []interface{}{applied}
		}
		return nil
	default:
		return nil
	}
}

// kvEqual reports whether two KV states are identical.
func kvEqual(a, b kvState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// kvHash hashes a KV state deterministically (keys visited in sorted order) so
// equal states always share a hash, as Porcupine's Hash contract requires.
func kvHash(s kvState) uint64 {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(s[k]))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// describeState renders a KV state deterministically for failure visualization,
// e.g. "{x=1, y=2}".
func describeState(state interface{}) string {
	s := state.(kvState)
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, s[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// describeOp renders one operation for failure visualization, e.g.
// "get(x) -> '1'" or "cas(x, 1->2) -> ok".
func describeOp(input, output interface{}) string {
	in := input.(kvInput)
	out := output.(kvOutput)
	res := "'" + out.value + "'"
	if out.unknown {
		res = "unknown"
	}
	switch in.kind {
	case OpGet:
		return fmt.Sprintf("get(%s) -> %s", in.key, res)
	case OpPut:
		return fmt.Sprintf("put(%s, '%s')", in.key, in.value)
	case OpAppend:
		return fmt.Sprintf("append(%s, '%s') -> %s", in.key, in.value, res)
	case OpCAS:
		verdict := "fail"
		if out.unknown {
			verdict = "unknown"
		} else if out.ok {
			verdict = "ok"
		}
		return fmt.Sprintf("cas(%s, '%s'->'%s') -> %s", in.key, in.compareValue, in.value, verdict)
	default:
		return "<invalid>"
	}
}
