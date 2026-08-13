package rpc

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/billdmar/quorum/core"
)

// dialTimeout bounds a single connection attempt so a dead peer cannot block a
// sender goroutine indefinitely. Sends to an unreachable peer are dropped (Raft
// tolerates message loss and retries via heartbeats), so a short timeout is
// preferable to a long stall.
const dialTimeout = 2 * time.Second

// Transport is a TCP-based, goroutine-safe message transport for one Raft node.
// It listens for inbound connections, delivers received core.Messages to an
// inbound handler, and sends outbound messages to peers identified by NodeID
// (resolved to an address via the peers map). Outbound connections are dialed
// lazily and cached; a broken connection is dropped and re-dialed on the next
// send. Send failures are swallowed by design — the core retries.
type Transport struct {
	self  core.NodeID
	peers map[core.NodeID]string // NodeID -> "host:port"; frozen after New

	// handler receives every successfully-decoded inbound message. It is called
	// from listener goroutines and MUST be safe for concurrent use; package node
	// funnels these onto a single channel, so the handler never touches the core
	// directly.
	handler func(core.Message)

	ln net.Listener

	mu     sync.Mutex
	conns  map[core.NodeID]net.Conn // cached outbound connections
	closed bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTransport constructs a Transport for self, with a static peer address map
// and an inbound-message handler. It does not open any sockets; call Listen to
// begin accepting connections and Send to dial peers. The peers map should map
// every reachable NodeID (including, harmlessly, self) to a "host:port".
func NewTransport(self core.NodeID, peers map[core.NodeID]string, handler func(core.Message)) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	// Copy the peer map so the caller cannot mutate our routing table after
	// construction (it is read without the lock).
	pc := make(map[core.NodeID]string, len(peers))
	for id, addr := range peers {
		pc[id] = addr
	}
	return &Transport{
		self:    self,
		peers:   pc,
		handler: handler,
		conns:   make(map[core.NodeID]net.Conn),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Listen binds a TCP listener on addr (use "127.0.0.1:0" for an ephemeral port)
// and starts the accept loop in a background goroutine. The bound address is
// available via Addr afterward. Listen may be called once per Transport.
func (t *Transport) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.ln = ln
	t.mu.Unlock()
	t.wg.Add(1)
	go t.acceptLoop(ln)
	return nil
}

// Addr returns the actual bound listen address (meaningful after Listen), or
// the empty string if not listening.
func (t *Transport) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return ""
	}
	return t.ln.Addr().String()
}

// acceptLoop accepts inbound connections until the listener is closed, spawning
// a reader goroutine per connection. It exits when Accept fails (which Close
// forces by closing the listener).
func (t *Transport) acceptLoop(ln net.Listener) {
	defer t.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed (Close) or unrecoverable accept error
		}
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

// readLoop reads framed messages off one inbound connection and hands each to
// the handler until the connection closes, errors, or the transport shuts down.
func (t *Transport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close() //nolint:errcheck // best-effort close of a read-side conn
	// Close the connection when the transport is cancelled so a blocked
	// ReadMessage unblocks and this goroutine can exit (no leak under -race).
	stop := t.closeOnCancel(conn)
	defer stop()
	for {
		m, err := ReadMessage(conn)
		if err != nil {
			return
		}
		t.handler(m)
	}
}

// closeOnCancel starts a watcher that closes conn when the transport context is
// cancelled, and returns a stop func that tears the watcher down (called when
// the owning read/connection goroutine exits on its own). This is how blocked
// network reads are unblocked at shutdown without leaking goroutines.
func (t *Transport) closeOnCancel(conn net.Conn) func() {
	done := make(chan struct{})
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		select {
		case <-t.ctx.Done():
			conn.Close() //nolint:errcheck // forcing a blocked Read to unblock
		case <-done:
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// Send transmits m to the peer named by m.To. On any failure (unknown peer,
// dial error, write error) it drops the message and, if a cached connection
// broke, discards it so the next Send re-dials. Send never blocks on the core;
// callers rely on Raft's retransmission for delivery guarantees.
func (t *Transport) Send(m core.Message) {
	conn, ok := t.connFor(m.To)
	if !ok {
		return
	}
	if err := WriteMessage(conn, m); err != nil {
		t.dropConn(m.To, conn)
	}
}

// connFor returns a usable connection to peer, dialing lazily if none is cached.
// Returns ok=false when the peer is unknown, the transport is closed, or the
// dial fails.
func (t *Transport) connFor(peer core.NodeID) (net.Conn, bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false
	}
	if c, ok := t.conns[peer]; ok {
		t.mu.Unlock()
		return c, true
	}
	addr, ok := t.peers[peer]
	t.mu.Unlock()
	if !ok {
		return nil, false
	}

	// Dial outside the lock (it blocks up to dialTimeout).
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(t.ctx, "tcp", addr)
	if err != nil {
		return nil, false
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		conn.Close() //nolint:errcheck // racing a concurrent Close; drop the dial
		return nil, false
	}
	// Another Send may have cached a connection while we dialed; prefer the
	// existing one and discard ours to keep a single conn per peer.
	if existing, ok := t.conns[peer]; ok {
		t.mu.Unlock()
		conn.Close() //nolint:errcheck // duplicate dial lost the race
		return existing, true
	}
	t.conns[peer] = conn
	t.mu.Unlock()
	return conn, true
}

// dropConn removes and closes conn if it is still the cached connection for
// peer, so a later Send re-dials a fresh one. Safe to call with a stale conn.
func (t *Transport) dropConn(peer core.NodeID, conn net.Conn) {
	t.mu.Lock()
	if c, ok := t.conns[peer]; ok && c == conn {
		delete(t.conns, peer)
	}
	t.mu.Unlock()
	conn.Close() //nolint:errcheck // best-effort teardown of a broken conn
}

// Close shuts the transport down: it stops accepting, cancels the context
// (unblocking all in-flight reads), closes the listener and every cached
// outbound connection, and waits for all goroutines to exit. After Close the
// transport must not be used. Close is idempotent.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	ln := t.ln
	conns := t.conns
	t.conns = make(map[core.NodeID]net.Conn)
	t.mu.Unlock()

	t.cancel()
	if ln != nil {
		ln.Close() //nolint:errcheck // unblocks acceptLoop
	}
	for _, c := range conns {
		c.Close() //nolint:errcheck // unblocks any read on this conn
	}
	t.wg.Wait()
	return nil
}
