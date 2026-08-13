package rpc

import (
	"bytes"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/billdmar/quorum/core"
)

// sampleMessages returns one representative Message per MessageType, including
// entries and every populated field group, so the round-trip test exercises the
// full wire shape.
func sampleMessages() []core.Message {
	return []core.Message{
		{
			From: "n1", To: "n2", Type: core.MsgRequestVote, Term: 7,
			LastLogIndex: 42, LastLogTerm: 6,
		},
		{
			From: "n2", To: "n1", Type: core.MsgRequestVoteResp, Term: 7,
			VoteGranted: true,
		},
		{
			From: "n1", To: "n3", Type: core.MsgAppendEntries, Term: 7,
			PrevLogIndex: 41, PrevLogTerm: 6, LeaderCommit: 40,
			Entries: []core.LogEntry{
				{Term: 6, Index: 42, Command: []byte("set x=1")},
				{Term: 7, Index: 43, Command: nil}, // leader no-op
			},
		},
		{
			From: "n3", To: "n1", Type: core.MsgAppendEntriesResp, Term: 7,
			Success: true, MatchIndex: 43,
		},
		{
			From: "n3", To: "n1", Type: core.MsgAppendEntriesResp, Term: 7,
			Success: false, ConflictIndex: 40, ConflictTerm: 5,
		},
		{
			From: "n1", To: "n2", Type: core.MsgInstallSnapshot, Term: 7,
			LastIncludedIndex: 40, LastIncludedTerm: 6, SnapshotData: []byte("snap"),
		},
	}
}

func TestCodecRoundTripBuffer(t *testing.T) {
	for _, want := range sampleMessages() {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, want); err != nil {
			t.Fatalf("WriteMessage(%s): %v", want.Type, err)
		}
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage(%s): %v", want.Type, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("round-trip mismatch for %s:\n got %+v\nwant %+v", want.Type, got, want)
		}
	}
}

// TestCodecStreamFraming writes several messages back-to-back onto one stream
// and reads them all back in order, proving the length prefix frames correctly
// on a shared byte stream (as it must over a single TCP connection).
func TestCodecStreamFraming(t *testing.T) {
	msgs := sampleMessages()
	var buf bytes.Buffer
	for _, m := range msgs {
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
	}
	for i, want := range msgs {
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage #%d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("frame #%d mismatch:\n got %+v\nwant %+v", i, got, want)
		}
	}
}

// TestCodecOverPipe round-trips through an in-memory net.Pipe, the same
// io.Reader/io.Writer contract a real TCP conn presents.
func TestCodecOverPipe(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close() //nolint:errcheck
	defer c2.Close() //nolint:errcheck

	want := sampleMessages()[2] // AppendEntries with entries
	go func() {
		if err := WriteMessage(c1, want); err != nil {
			t.Errorf("WriteMessage: %v", err)
		}
	}()
	got, err := ReadMessage(c2)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("pipe mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// TestReadMessageEOF confirms a clean stream end on a frame boundary surfaces as
// io.EOF (unwrapped) so callers can distinguish a closed peer from a real error.
func TestReadMessageEOF(t *testing.T) {
	if _, err := ReadMessage(&bytes.Buffer{}); err != io.EOF {
		t.Errorf("want io.EOF on empty stream, got %v", err)
	}
}
