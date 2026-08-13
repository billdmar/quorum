// Package kv is the application state machine layered on top of the Raft core.
// The core replicates opaque []byte commands and, once an entry commits, hands
// it back via EffectApply; this package defines what those bytes mean, applies
// them to a key-value map, and — critically — enforces exactly-once semantics
// under client retries via per-client sessions (request dedup). The exactly-
// once property is proven at : a command retried across a leader change and
// re-committed must take effect at most once.
//
// DETERMINISM CONTRACT: like the rest of quorum, this package touches no clock,
// no I/O, no goroutines, and no randomness. Encode() is a pure function of the
// Command, and Snapshot() is a pure function of the Store's logical state (maps
// are always ranged in sorted key order before serialization), so identical
// state yields byte-identical output and trace hashes stay stable.
package kv

import (
	"encoding/binary"
	"errors"

	"github.com/billdmar/quorum/check"
)

// Command is the decoded form of a replicated KV command — the meaning the
// application assigns to a LogEntry.Command byte slice. ClientID+SeqNum together
// identify a single logical client request so retries of the same request (same
// pair) can be deduplicated to apply exactly once. Op selects the operation;
// Key/Value/CompareValue carry its arguments (fields not used by an Op are
// empty). Op reuses check.OpKind so the KV semantics here match the Porcupine
// model the checker builds against exactly.
type Command struct {
	ClientID     uint64
	SeqNum       uint64
	Op           check.OpKind
	Key          string
	Value        string
	CompareValue string
}

// commandHeader is the fixed prefix of an encoded command: ClientID(8) +
// SeqNum(8) + Op(1). The three arguments follow as length-prefixed strings.
const commandHeader = 17

// Encode serializes a Command into the deterministic wire format the core
// replicates: [ClientID:u64][SeqNum:u64][Op:u8] followed by three
// length-prefixed (u32 length + bytes) strings for Key, Value, CompareValue.
// A struct — not a map — is encoded, so field order is fixed and the output is
// byte-identical for identical inputs (no map-iteration nondeterminism).
func (c Command) Encode() []byte {
	buf := make([]byte, commandHeader)
	binary.BigEndian.PutUint64(buf[0:8], c.ClientID)
	binary.BigEndian.PutUint64(buf[8:16], c.SeqNum)
	buf[16] = byte(c.Op)
	buf = appendString(buf, c.Key)
	buf = appendString(buf, c.Value)
	buf = appendString(buf, c.CompareValue)
	return buf
}

// Decode parses a command wire image produced by Encode. It returns an error
// (rather than a partial value) on any truncation or length mismatch, which is
// how Apply defensively ignores a corrupt or malformed committed entry.
func Decode(data []byte) (Command, error) {
	if len(data) < commandHeader {
		return Command{}, errors.New("kv: command too short")
	}
	c := Command{
		ClientID: binary.BigEndian.Uint64(data[0:8]),
		SeqNum:   binary.BigEndian.Uint64(data[8:16]),
		Op:       check.OpKind(data[16]),
	}
	rest := data[commandHeader:]
	var ok bool
	if c.Key, rest, ok = takeString(rest); !ok {
		return Command{}, errors.New("kv: truncated key")
	}
	if c.Value, rest, ok = takeString(rest); !ok {
		return Command{}, errors.New("kv: truncated value")
	}
	if c.CompareValue, rest, ok = takeString(rest); !ok {
		return Command{}, errors.New("kv: truncated compare-value")
	}
	if len(rest) != 0 {
		return Command{}, errors.New("kv: trailing bytes after command")
	}
	return c, nil
}

// appendString appends a u32-length-prefixed string to buf and returns the
// grown slice. Empty strings encode as a bare zero length.
func appendString(buf []byte, s string) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(s)))
	buf = append(buf, hdr[:]...)
	return append(buf, s...)
}

// takeString reads one u32-length-prefixed string from the front of data,
// returning the string, the remaining bytes, and ok=false on any truncation.
func takeString(data []byte) (s string, rest []byte, ok bool) {
	if len(data) < 4 {
		return "", data, false
	}
	n := int(binary.BigEndian.Uint32(data[0:4]))
	if n < 0 || 4+n > len(data) {
		return "", data, false
	}
	return string(data[4 : 4+n]), data[4+n:], true
}
