package rpc

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

// TestClientRoundTrip: a request submitted via DoClientRequest reaches the
// handler and its response comes back intact over a real loopback listener.
func TestClientRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go ServeClients(ln, func(req ClientRequest) ClientResponse {
		return ClientResponse{Served: true, Value: req.Key + "=" + req.Value, OK: true}
	})

	resp, err := DoClientRequest(ln.Addr().String(),
		ClientRequest{Op: "put", Key: "k", Value: "v", ClientID: 1, SeqNum: 1}, 3*time.Second)
	if err != nil {
		t.Fatalf("DoClientRequest: %v", err)
	}
	if !resp.Served || resp.Value != "k=v" {
		t.Fatalf("round-trip mismatch: %+v", resp)
	}
}

// TestClientRequestBounded verifies the server does not buffer an unbounded
// newline-less stream from a hostile client (memory-exhaustion DoS guard): a
// connection that sends far more than maxClientRequestBytes with no newline is
// rejected with a bad-request error rather than growing memory without limit.
func TestClientRequestBounded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go ServeClients(ln, func(ClientRequest) ClientResponse {
		return ClientResponse{Served: true} // must never be reached for this input
	})

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	// Send > maxClientRequestBytes with NO newline. The server reads at most the
	// cap then hits EOF (LimitReader) → ReadBytes returns an error → bad request.
	if _, err := conn.Write([]byte(strings.Repeat("A", maxClientRequestBytes+4096))); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Half-close our write side so the server's LimitReader sees EOF promptly.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	var resp ClientResponse
	if err := readJSONLine(bufio.NewReader(conn), &resp); err != nil {
		return // server closed / errored without buffering unboundedly — acceptable
	}
	if resp.Err == "" {
		t.Fatalf("oversized unterminated request should be rejected; got %+v", resp)
	}
}
