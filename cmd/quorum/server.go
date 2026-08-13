package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/node"
	"github.com/billdmar/quorum/rpc"
	"github.com/billdmar/quorum/storage"
)

// runServer runs one cluster node until interrupted. It wires the real-TCP Raft
// transport to a node.Runtime driving the pure core, recovers any durable state,
// and serves a client-facing endpoint that submits KV operations to the runtime.
func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	id := fs.String("id", "", "this node's id (must be a key in -peers)")
	peersArg := fs.String("peers", "", "comma-separated id=host:port for all nodes incl. self")
	clientAddr := fs.String("client", "", "address to serve client (kv) requests on, e.g. 127.0.0.1:8001")
	dataDir := fs.String("data", "", "directory for the durable WAL (in-memory if empty)")
	elecMin := fs.Duration("election-min", 150*time.Millisecond, "min election timeout")
	elecMax := fs.Duration("election-max", 300*time.Millisecond, "max election timeout")
	hb := fs.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat period")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *peersArg == "" || *clientAddr == "" {
		return fmt.Errorf("-id, -peers, and -client are required")
	}

	peers, err := parsePeers(*peersArg)
	if err != nil {
		return err
	}
	selfAddr, ok := peers[core.NodeID(*id)]
	if !ok {
		return fmt.Errorf("-id %q not found in -peers", *id)
	}
	peerIDs := make([]core.NodeID, 0, len(peers)-1)
	for pid := range peers {
		if pid != core.NodeID(*id) {
			peerIDs = append(peerIDs, pid)
		}
	}

	// Durable storage: a real WAL when -data is set, else in-memory.
	store, closeStore, err := openStorage(*dataDir)
	if err != nil {
		return err
	}
	defer closeStore()

	c := core.New(core.Config{Self: core.NodeID(*id), Peers: peerIDs})
	cfg := node.Config{
		Self: core.NodeID(*id), Peers: peerIDs,
		ElectionMin: *elecMin, ElectionMax: *elecMax, Heartbeat: *hb,
	}

	var rt *node.Runtime
	// The transport's inbound handler feeds the runtime; construct the transport
	// first with a handler that closes over rt (set just below before Listen).
	tport := rpc.NewTransport(core.NodeID(*id), toAddrMap(peers), func(m core.Message) {
		rt.Deliver(m)
	})
	rt = node.New(cfg, c, store, tport, nil, time.Now().UnixNano())
	if err := rt.Recover(); err != nil {
		return fmt.Errorf("recover durable state: %w", err)
	}
	if err := tport.Listen(selfAddr); err != nil {
		return fmt.Errorf("raft listen %s: %w", selfAddr, err)
	}
	defer func() { _ = tport.Close() }()

	rt.Start()
	defer rt.Stop()

	// Client-facing endpoint.
	cln, err := net.Listen("tcp", *clientAddr)
	if err != nil {
		return fmt.Errorf("client listen %s: %w", *clientAddr, err)
	}
	defer func() { _ = cln.Close() }()
	go rpc.ServeClients(cln, clientHandler(rt))

	fmt.Printf("quorum node %s: raft=%s client=%s peers=%d\n", *id, selfAddr, *clientAddr, len(peers))

	// Run until interrupted.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Printf("quorum node %s: shutting down\n", *id)
	return nil
}

// clientHandler adapts a runtime into a client-request handler: writes go through
// Propose (a sessioned kv.Command), reads through the linearizable Read path. A
// non-leader node returns Served=false with the leader hint for redirection.
func clientHandler(rt *node.Runtime) rpc.ClientHandler {
	return func(req rpc.ClientRequest) rpc.ClientResponse {
		switch req.Op {
		case "status":
			// Report whether THIS node is currently the leader, plus its best-known
			// leader id. Lets a client (e.g. scripts/demo.sh) identify and target the
			// ACTUAL leader rather than guessing.
			st := rt.Status()
			return rpc.ClientResponse{Served: st.Role == core.Leader, Leader: string(st.Leader), OK: true}
		case "get":
			r := rt.Read(req.Key)
			if !r.Served {
				return rpc.ClientResponse{Served: false, Leader: string(r.LeaderHint)}
			}
			return rpc.ClientResponse{Served: true, Value: r.Value, Found: r.Found, OK: true}
		case "put", "append", "cas":
			cmd := kv.Command{
				ClientID: req.ClientID, SeqNum: req.SeqNum,
				Op: opKind(req.Op), Key: req.Key, Value: req.Value, CompareValue: req.CompareValue,
			}
			res := rt.Propose(cmd.Encode())
			if !res.Accepted {
				return rpc.ClientResponse{Served: false, Leader: string(res.LeaderHint)}
			}
			// Accepted means appended by the leader; the write commits shortly. A
			// follow-up linearizable read reflects it, but for the demo CLI we
			// report acceptance (the value is echoed by a subsequent get).
			return rpc.ClientResponse{Served: true, OK: true, Value: req.Value}
		default:
			return rpc.ClientResponse{Err: "unknown op: " + req.Op}
		}
	}
}

func opKind(op string) check.OpKind {
	switch op {
	case "put":
		return check.OpPut
	case "append":
		return check.OpAppend
	case "cas":
		return check.OpCAS
	default:
		return check.OpGet
	}
}

// parsePeers parses "id=host:port,id=host:port,..." into a NodeID->addr map.
func parsePeers(s string) (map[core.NodeID]string, error) {
	out := make(map[core.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad peer spec %q (want id=host:port)", part)
		}
		out[core.NodeID(part[:eq])] = part[eq+1:]
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers parsed from %q", s)
	}
	return out, nil
}

func toAddrMap(peers map[core.NodeID]string) map[core.NodeID]string { return peers }

// openStorage returns a WAL rooted at dir, or an in-memory store when dir is
// empty, plus a close function.
func openStorage(dir string) (storage.Storage, func(), error) {
	if dir == "" {
		s := storage.NewMem()
		return s, func() {}, nil
	}
	w, err := storage.Open(dir)
	if err != nil {
		return nil, nil, err
	}
	return w, func() { _ = w.Close() }, nil
}
