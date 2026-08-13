package check

// Tests for the Porcupine KV model and linearizability checking. Histories are
// hand-built so each asserts a specific model behavior; the stale-read case is
// the project-critical one — a Get that returns a value older than a Put which
// linearized before it MUST be flagged non-linearizable.

import "testing"

// hb is a small builder for hand-written histories. Each op is added as an
// invoke at one stamp and a response at a later stamp; concurrency is expressed
// by overlapping the [invoke, response] stamp intervals across clients.
type hb struct {
	events []HistoryEvent
	nextID uint64
}

// op appends an invoke+response pair for one client. call/ret are the logical
// stamps bounding the operation's interval.
func (b *hb) op(client uint64, call, ret uint64, in HistoryEvent, out HistoryEvent) {
	id := b.nextID
	b.nextID++
	in.OpID, in.Client, in.Stage, in.Stamp = id, client, StageInvoke, call
	b.events = append(b.events, in)
	out.OpID, out.Client, out.Stage, out.Stamp = id, client, StageResponse, ret
	b.events = append(b.events, out)
}

// pending appends only an invoke (no response) — an operation the client never
// learned the outcome of.
func (b *hb) pending(client uint64, call uint64, in HistoryEvent) {
	id := b.nextID
	b.nextID++
	in.OpID, in.Client, in.Stage, in.Stamp = id, client, StageInvoke, call
	b.events = append(b.events, in)
}

func (b *hb) history() History { return History{Seed: 1, Schedule: "clean", Events: b.events} }

func put(key, val string) HistoryEvent { return HistoryEvent{Kind: OpPut, Key: key, Value: val} }
func get(key string) HistoryEvent      { return HistoryEvent{Kind: OpGet, Key: key} }
func getRes(val string) HistoryEvent   { return HistoryEvent{Output: val} }
func appendOp(key, val string) HistoryEvent {
	return HistoryEvent{Kind: OpAppend, Key: key, Value: val}
}
func casOp(key, cmp, val string) HistoryEvent {
	return HistoryEvent{Kind: OpCAS, Key: key, CompareValue: cmp, Value: val}
}

func TestLinearizable_SequentialPutGet(t *testing.T) {
	var b hb
	b.op(0, 0, 10, put("x", "1"), HistoryEvent{})
	b.op(0, 11, 20, get("x"), getRes("1"))
	if ok, res := CheckLinearizable(b.history()); !ok {
		t.Fatalf("expected linearizable, got %v (%s)", ok, res.Result)
	}
}

func TestLinearizable_ConcurrentGetSeesEitherValue(t *testing.T) {
	// C0 puts x=1 over [0,30]; concurrently C1 reads x. The read may observe
	// either "" (before the put) or "1" (after) — both are linearizable.
	for _, observed := range []string{"", "1"} {
		var b hb
		b.op(0, 0, 30, put("x", "1"), HistoryEvent{})
		b.op(1, 10, 20, get("x"), getRes(observed))
		if ok, res := CheckLinearizable(b.history()); !ok {
			t.Fatalf("observed %q: expected linearizable, got %s", observed, res.Result)
		}
	}
}

// TestLinearizable_StaleReadFlagged is the project-required case: a Put fully
// completes, then a strictly-later Get returns the OLD value. No total order
// respecting real time can explain it, so it must be non-linearizable.
func TestLinearizable_StaleReadFlagged(t *testing.T) {
	var b hb
	b.op(0, 0, 10, put("x", "new"), HistoryEvent{}) // put returns at stamp 10
	b.op(1, 20, 30, get("x"), getRes(""))           // read starts at 20, sees stale ""
	ok, res := CheckLinearizable(b.history())
	if ok {
		t.Fatalf("stale read was NOT flagged: got linearizable (%s)", res.Result)
	}
	if res.Determined && res.Result != "Illegal" {
		t.Fatalf("expected Illegal, got %s", res.Result)
	}
	t.Logf("stale read correctly flagged non-linearizable: result=%s", res.Result)
}

func TestLinearizable_StaleReadAfterAppend(t *testing.T) {
	// Append makes x="ab"; a later read returning "a" is stale.
	var b hb
	b.op(0, 0, 5, put("x", "a"), HistoryEvent{})
	b.op(0, 6, 10, appendOp("x", "b"), getRes("ab"))
	b.op(1, 20, 30, get("x"), getRes("a"))
	if ok, _ := CheckLinearizable(b.history()); ok {
		t.Fatal("expected non-linearizable stale read after append")
	}
}

func TestLinearizable_CASSuccessAndFailure(t *testing.T) {
	var b hb
	b.op(0, 0, 5, put("x", "1"), HistoryEvent{})
	// CAS 1->2 succeeds.
	b.op(0, 6, 10, casOp("x", "1", "2"), HistoryEvent{OK: true})
	// CAS 1->3 now fails (value is "2").
	b.op(0, 11, 15, casOp("x", "1", "3"), HistoryEvent{OK: false})
	b.op(0, 16, 20, get("x"), getRes("2"))
	if ok, res := CheckLinearizable(b.history()); !ok {
		t.Fatalf("expected linearizable CAS sequence, got %s", res.Result)
	}
}

func TestLinearizable_CASWrongOutcomeFlagged(t *testing.T) {
	// x="1"; a CAS 9->2 cannot succeed (current != 9) yet reports OK=true.
	var b hb
	b.op(0, 0, 5, put("x", "1"), HistoryEvent{})
	b.op(0, 6, 10, casOp("x", "9", "2"), HistoryEvent{OK: true})
	if ok, _ := CheckLinearizable(b.history()); ok {
		t.Fatal("expected non-linearizable: CAS reported success it could not achieve")
	}
}

// TestLinearizable_UnknownOutcomePlacedEitherWay: a Put whose response is
// Unknown may be treated as having happened or not, so a concurrent read seeing
// EITHER value is linearizable.
func TestLinearizable_UnknownOutcomePlacedEitherWay(t *testing.T) {
	for _, observed := range []string{"", "1"} {
		var b hb
		b.op(0, 0, 30, put("x", "1"), HistoryEvent{Unknown: true})
		b.op(1, 40, 50, get("x"), getRes(observed))
		if ok, res := CheckLinearizable(b.history()); !ok {
			t.Fatalf("unknown put, read %q: expected linearizable, got %s", observed, res.Result)
		}
	}
}

// TestLinearizable_PendingOpTreatedAsUnknown: an op with an invoke but no
// response is modeled as unknown and must not, by itself, break linearizability.
func TestLinearizable_PendingOpTreatedAsUnknown(t *testing.T) {
	var b hb
	b.pending(0, 0, put("x", "1"))        // never returns
	b.op(1, 40, 50, get("x"), getRes("")) // reads absent — fine, put may not have run
	if ok, res := CheckLinearizable(b.history()); !ok {
		t.Fatalf("expected linearizable with pending op, got %s", res.Result)
	}
}

func TestToOperations_Deterministic(t *testing.T) {
	var b hb
	b.op(2, 5, 15, get("x"), getRes(""))
	b.op(0, 0, 10, put("x", "1"), HistoryEvent{})
	h := b.history()
	first := ToOperations(h)
	for i := 0; i < 5; i++ {
		got := ToOperations(h)
		if len(got) != len(first) {
			t.Fatalf("nondeterministic length: %d vs %d", len(got), len(first))
		}
		for j := range got {
			if got[j].Call != first[j].Call || got[j].ClientId != first[j].ClientId {
				t.Fatalf("nondeterministic order at %d", j)
			}
		}
	}
	// Sorted by Call: the put (Call 0) must precede the get (Call 5).
	if first[0].Call > first[1].Call {
		t.Fatal("operations not sorted by call stamp")
	}
}
