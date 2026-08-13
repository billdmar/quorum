package rpc

// This file holds the CLIENT-facing RPC (distinct from the Raft peer transport
// in transport.go): a tiny line-framed request/response protocol a `quorum kv`
// command uses to submit an operation to a cluster node. It is deliberately
// simple — one request, one response, one connection — because it is a demo CLI
// path, not a high-throughput client library.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// ClientRequest is a single KV operation submitted by the CLI. Op is one of
// "put", "get", "append", "cas". ClientID/SeqNum carry the session identity that
// makes retries exactly-once (the CLI assigns a per-process ClientID and an
// increasing SeqNum).
type ClientRequest struct {
	Op           string `json:"op"`
	Key          string `json:"key"`
	Value        string `json:"value,omitempty"`
	CompareValue string `json:"compare,omitempty"`
	ClientID     uint64 `json:"client_id"`
	SeqNum       uint64 `json:"seq_num"`
}

// ClientResponse is the node's reply. Served is true when THIS node handled the
// op; when false, Leader names the best-known leader so the CLI can redirect.
// Value/Found/OK carry the operation result (OK is the CAS success bit).
type ClientResponse struct {
	Served bool   `json:"served"`
	Leader string `json:"leader,omitempty"`
	Value  string `json:"value,omitempty"`
	Found  bool   `json:"found"`
	OK     bool   `json:"ok"`
	Err    string `json:"err,omitempty"`
}

// WriteClientRequest / ReadClientRequest and the response pair use newline-framed
// JSON so a request or response is exactly one line. JSON (not the deterministic
// binary used on the Raft wire) is fine here: the client path is not part of the
// trace-hash determinism domain and human-readable framing eases debugging.

func writeJSONLine(conn net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = conn.Write(b)
	return err
}

func readJSONLine(r *bufio.Reader, v any) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, v)
}

// DoClientRequest dials addr, sends req, and returns the node's response. It is
// the CLI's one-shot client call with a bounded timeout.
func DoClientRequest(addr string, req ClientRequest, timeout time.Duration) (ClientResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return ClientResponse{}, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := writeJSONLine(conn, req); err != nil {
		return ClientResponse{}, fmt.Errorf("send: %w", err)
	}
	var resp ClientResponse
	if err := readJSONLine(bufio.NewReader(conn), &resp); err != nil {
		return ClientResponse{}, fmt.Errorf("recv: %w", err)
	}
	return resp, nil
}

// ClientHandler processes one decoded ClientRequest and returns the response.
// The server (cmd/quorum) supplies this, wiring it to the runtime.
type ClientHandler func(ClientRequest) ClientResponse

// ServeClients accepts client connections on ln and dispatches each request to
// handler, one request/response per connection. It runs until ln is closed.
// Errors on individual connections are isolated (logged by the caller via the
// returned per-conn behavior); a malformed request yields an Err response.
func ServeClients(ln net.Listener, handler ClientHandler) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		go serveClientConn(conn, handler)
	}
}

// maxClientRequestBytes caps a single client request line. A ClientRequest is a
// small JSON object (a few short strings), so this is generous; the cap bounds
// the memory a hostile or buggy client can force the server to buffer from one
// connection — without it, ReadBytes('\n') on a newline-less stream grows without
// limit (a memory-exhaustion DoS). Mirrors the 64 MiB frame cap the Raft wire
// codec already enforces (codec.go), scaled down for the tiny client protocol.
const maxClientRequestBytes = 64 << 10 // 64 KiB

func serveClientConn(conn net.Conn, handler ClientHandler) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	// Bound the bytes read from an untrusted client so one connection cannot
	// exhaust memory with a newline-less stream.
	r := bufio.NewReader(io.LimitReader(conn, maxClientRequestBytes))
	var req ClientRequest
	if err := readJSONLine(r, &req); err != nil {
		_ = writeJSONLine(conn, ClientResponse{Err: "bad request: " + err.Error()})
		return
	}
	_ = writeJSONLine(conn, handler(req))
}
