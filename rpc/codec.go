// Package rpc is the production-runtime network transport: real TCP sockets,
// real goroutines, real timers. It is the SECOND driver of the pure core (the
// first is the deterministic simulator); both drive the identical sans-I/O
// core.RaftCore. All concurrency lives here and in package node — never in the
// core. Wire framing need not be trace-deterministic (only the simulator's
// trace hash is a determinism concern); it need only be a correct,
// self-delimiting encoding of a core.Message over a byte stream.
package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/billdmar/quorum/core"
)

// maxMessageBytes caps a single framed message so a corrupt or hostile length
// prefix cannot make ReadMessage allocate unboundedly. Raft messages carry log
// entries and (at ) snapshot chunks; 64 MiB is far above any legitimate frame
// in this project's local-cluster scope while still bounding the blast radius.
const maxMessageBytes = 64 << 20

// WriteMessage encodes m and writes it to w as a length-prefixed frame: a
// 4-byte big-endian unsigned payload length followed by the gob-encoded
// Message. gob is used because it round-trips the flat core.Message struct
// (including its []LogEntry) without hand-maintained field lists; wire
// determinism is not required here. Encoding to a buffer first is what lets us
// write an accurate length prefix, making the frame self-delimiting on a stream.
func WriteMessage(w io.Writer, m core.Message) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&m); err != nil {
		return fmt.Errorf("rpc: encode message: %w", err)
	}
	if buf.Len() > maxMessageBytes {
		return fmt.Errorf("rpc: message too large: %d bytes", buf.Len())
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(buf.Len()))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("rpc: write length prefix: %w", err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("rpc: write payload: %w", err)
	}
	return nil
}

// ReadMessage reads one length-prefixed frame from r and decodes it into a
// core.Message. It blocks until a full frame is read, returns io.EOF unwrapped
// when the stream ends cleanly on a frame boundary (so callers can detect a
// closed peer), and rejects a length prefix exceeding maxMessageBytes rather
// than trusting it.
func ReadMessage(r io.Reader) (core.Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return core.Message{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxMessageBytes {
		return core.Message{}, fmt.Errorf("rpc: frame length %d exceeds cap %d", n, maxMessageBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return core.Message{}, fmt.Errorf("rpc: read payload: %w", err)
	}
	var m core.Message
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&m); err != nil {
		return core.Message{}, fmt.Errorf("rpc: decode message: %w", err)
	}
	return m, nil
}
