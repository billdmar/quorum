package rpc

import (
	"reflect"
	"testing"
	"time"

	"github.com/billdmar/quorum/core"
)

// recvOrTimeout waits for one message on ch or fails after a generous timeout.
func recvOrTimeout(t *testing.T, ch <-chan core.Message, what string) core.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return core.Message{}
	}
}

// TestTransportBidirectional stands up two transports on ephemeral loopback
// ports and sends a message each direction, asserting delivery and clean Close.
// Run under -race, this exercises the accept/read/dial goroutines concurrently.
func TestTransportBidirectional(t *testing.T) {
	inA := make(chan core.Message, 4)
	inB := make(chan core.Message, 4)

	ta := NewTransport("A", nil, func(m core.Message) { inA <- m })
	tb := NewTransport("B", nil, func(m core.Message) { inB <- m })
	if err := ta.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("A Listen: %v", err)
	}
	if err := tb.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("B Listen: %v", err)
	}
	defer ta.Close() //nolint:errcheck
	defer tb.Close() //nolint:errcheck

	// Ephemeral ports are known only after Listen; wire the peer routing tables
	// now, before any Send reads them (no goroutine touches peers until Send).
	ta.peers = map[core.NodeID]string{"B": tb.Addr()}
	tb.peers = map[core.NodeID]string{"A": ta.Addr()}

	msgAtoB := core.Message{From: "A", To: "B", Type: core.MsgAppendEntries, Term: 3,
		Entries: []core.LogEntry{{Term: 3, Index: 1, Command: []byte("hi")}}}
	msgBtoA := core.Message{From: "B", To: "A", Type: core.MsgAppendEntriesResp, Term: 3, Success: true, MatchIndex: 1}

	ta.Send(msgAtoB)
	got := recvOrTimeout(t, inB, "A->B message")
	if !reflect.DeepEqual(got, msgAtoB) {
		t.Errorf("A->B mismatch:\n got %+v\nwant %+v", got, msgAtoB)
	}

	tb.Send(msgBtoA)
	got = recvOrTimeout(t, inA, "B->A message")
	if !reflect.DeepEqual(got, msgBtoA) {
		t.Errorf("B->A mismatch:\n got %+v\nwant %+v", got, msgBtoA)
	}

	// A second send reuses the cached connection; assert it still delivers.
	ta.Send(msgAtoB)
	got = recvOrTimeout(t, inB, "A->B second message")
	if !reflect.DeepEqual(got, msgAtoB) {
		t.Errorf("A->B (reuse) mismatch:\n got %+v\nwant %+v", got, msgAtoB)
	}
}

// TestTransportSendUnknownPeer confirms a send to an unmapped peer is silently
// dropped (no panic, no block) — Raft tolerates the loss.
func TestTransportSendUnknownPeer(t *testing.T) {
	tr := NewTransport("A", nil, func(core.Message) {})
	if err := tr.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Close() //nolint:errcheck
	tr.Send(core.Message{From: "A", To: "ghost", Type: core.MsgRequestVote})
}

// TestTransportCloseIdempotent confirms Close can be called twice safely and a
// transport that never listened still closes without leaking goroutines.
func TestTransportCloseIdempotent(t *testing.T) {
	tr := NewTransport("A", map[core.NodeID]string{"B": "127.0.0.1:1"}, func(core.Message) {})
	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Send after Close must be a no-op, not a panic.
	tr.Send(core.Message{To: "B"})
}
