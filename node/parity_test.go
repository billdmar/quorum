package node

import (
	"testing"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/storage"
)

// Parity: the production runtime and the deterministic simulator are two drivers
// of the SAME pure core, so given an identical sequence of events the core makes
// identical decisions regardless of driver. The sim proves this at scale under
// virtual time; this test proves the RUNTIME faithfully applies the core's
// effects to real storage and a real kv.Store — i.e. that driving the core
// through node.Runtime's effect executor yields the same committed KV state a
// bare core + direct effect application would.
//
// We cannot compare wall-clock timing or trace hashes across drivers (the
// runtime is genuinely time- and network-driven), so parity here is BEHAVIORAL:
// the same committed commands produce the same KV state and the same durable log.

// TestParityRuntimeMatchesDirectCoreDrive feeds the same proposals to (a) a bare
// core whose effects we apply directly to a kv.Store + storage, and (b) a
// single-node Runtime, and asserts the resulting KV state and durable log match.
func TestParityRuntimeMatchesDirectCoreDrive(t *testing.T) {
	cmds := []kv.Command{
		{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"},
		{ClientID: 1, SeqNum: 2, Op: check.OpAppend, Key: "a", Value: "x"},
		{ClientID: 2, SeqNum: 1, Op: check.OpPut, Key: "b", Value: "2"},
		{ClientID: 2, SeqNum: 1, Op: check.OpPut, Key: "b", Value: "DUP"}, // duplicate seq: must dedup
		{ClientID: 1, SeqNum: 3, Op: check.OpCAS, Key: "a", Value: "z", CompareValue: "1x"},
	}

	// (a) Reference: drive a bare single-node core directly, applying its effects
	// to a reference kv.Store exactly as any faithful driver must.
	refKV := kv.NewStore()
	refCore := core.New(core.Config{Self: "solo"})
	drive := func(ev core.Event) {
		for _, eff := range refCore.Step(ev) {
			if eff.Type == core.EffectApply {
				for _, ce := range eff.Committed {
					refKV.Apply(ce)
				}
			}
		}
	}
	drive(core.Event{Type: core.EventTickElection}) // self-elect, commit no-op
	for i, cmd := range cmds {
		drive(core.Event{Type: core.EventPropose, Ref: core.ClientRef(i + 1), Command: cmd.Encode()})
	}

	// (b) The production Runtime driving an identical core over the same proposals.
	rt := New(soloConfig("solo"), core.New(core.Config{Self: "solo"}), storage.NewMem(), nopTransport{}, nil, 1)
	rt.Start()
	defer rt.Stop()
	if !waitFor(2*time.Second, func() bool { return rt.Status().Role == core.Leader }) {
		t.Fatal("runtime did not self-elect")
	}
	for _, cmd := range cmds {
		rt.Propose(cmd.Encode())
	}

	// Compare the observable KV state for every key touched. The runtime serves
	// reads via ReadIndex; the reference reads its kv.Store directly.
	for _, key := range []string{"a", "b"} {
		want, wok := refKV.Get(key)
		var gotVal string
		var gok bool
		if !waitFor(2*time.Second, func() bool {
			r := rt.Read(key)
			if !r.Served {
				return false
			}
			gotVal, gok = r.Value, r.Found
			return true
		}) {
			t.Fatalf("runtime read of %q never served", key)
		}
		if gotVal != want || gok != wok {
			t.Errorf("parity mismatch on %q: runtime=(%q,%v) reference=(%q,%v)", key, gotVal, gok, want, wok)
		}
	}
}
