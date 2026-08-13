package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/rpc"
)

// runKV is the client: it submits one KV operation to a node, following a leader
// redirect once if the contacted node is not the leader. Usage:
//
//	quorum kv put    -addr host:port <key> <value>
//	quorum kv get    -addr host:port <key>
//	quorum kv append -addr host:port <key> <value>
//	quorum kv cas    -addr host:port <key> <compare> <value>
//	quorum kv status -addr host:port
func runKV(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected an operation: put|get|append|cas|status")
	}
	op := args[0]
	fs := flag.NewFlagSet("kv "+op, flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8001", "a cluster node's client address")
	// Peers lets the client follow a redirect: id=host:port,... mapping leader ids
	// to client addresses. Optional; without it a redirect is reported, not chased.
	peers := fs.String("peers", "", "optional id=clientaddr,... map to follow leader redirects")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	rest := fs.Args()

	req, err := buildRequest(op, rest)
	if err != nil {
		return err
	}

	resp, err := submit(*addr, *peers, req)
	if err != nil {
		return err
	}
	return printResult(op, req, resp)
}

// buildRequest turns positional args into a ClientRequest with a session identity
// derived deterministically from the args (a fresh CLI invocation is a fresh
// logical op; the ClientID+SeqNum exist so a RETRY of the same invocation dedups).
func buildRequest(op string, rest []string) (rpc.ClientRequest, error) {
	req := rpc.ClientRequest{Op: op}
	switch op {
	case "status":
		if len(rest) != 0 {
			return req, fmt.Errorf("usage: kv status")
		}
		return req, nil
	case "get":
		if len(rest) != 1 {
			return req, fmt.Errorf("usage: kv get <key>")
		}
		req.Key = rest[0]
	case "put", "append":
		if len(rest) != 2 {
			return req, fmt.Errorf("usage: kv %s <key> <value>", op)
		}
		req.Key, req.Value = rest[0], rest[1]
	case "cas":
		if len(rest) != 3 {
			return req, fmt.Errorf("usage: kv cas <key> <compare> <value>")
		}
		req.Key, req.CompareValue, req.Value = rest[0], rest[1], rest[2]
	default:
		return req, fmt.Errorf("unknown op %q (want put|get|append|cas|status)", op)
	}
	// Session identity: a stable ClientID from (op,key,value) + a fixed SeqNum, so
	// that re-running the identical command is deduplicated to exactly-once by the
	// KV session table. Distinct commands get distinct ClientIDs.
	h := fnv.New64a()
	_, _ = h.Write([]byte(op + "\x00" + req.Key + "\x00" + req.Value + "\x00" + req.CompareValue))
	req.ClientID = h.Sum64()
	req.SeqNum = 1
	return req, nil
}

// submit sends the request to addr and follows one leader redirect if peers maps
// the returned leader id to a client address.
func submit(addr, peers string, req rpc.ClientRequest) (rpc.ClientResponse, error) {
	resp, err := rpc.DoClientRequest(addr, req, 3*time.Second)
	if err != nil {
		return resp, err
	}
	// status is a per-node readout, never redirected: each node reports its OWN role.
	if req.Op != "status" && !resp.Served && resp.Leader != "" && peers != "" {
		if m, perr := parsePeers(peers); perr == nil {
			if laddr, ok := m[core.NodeID(resp.Leader)]; ok && laddr != addr {
				return rpc.DoClientRequest(laddr, req, 3*time.Second)
			}
		}
	}
	return resp, nil
}

// printResult renders the response to stdout.
func printResult(op string, req rpc.ClientRequest, resp rpc.ClientResponse) error {
	if resp.Err != "" {
		return fmt.Errorf("%s", resp.Err)
	}
	if op == "status" {
		// Served reports whether the contacted node is itself the leader; it is a
		// state readout, not a redirect. Print "leader" / "follower <leader-id>".
		if resp.Served {
			fmt.Println("leader")
		} else {
			leader := resp.Leader
			if leader == "" {
				leader = "unknown"
			}
			fmt.Printf("follower (leader %s)\n", leader)
		}
		return nil
	}
	if !resp.Served {
		leader := resp.Leader
		if leader == "" {
			leader = "unknown"
		}
		fmt.Printf("not leader; try leader %s\n", leader)
		os.Exit(3)
	}
	switch op {
	case "get":
		if resp.Found {
			fmt.Println(resp.Value)
		} else {
			fmt.Println("(not found)")
		}
	case "cas":
		if resp.OK {
			fmt.Printf("ok (set %s=%s)\n", req.Key, req.Value)
		} else {
			fmt.Printf("cas failed (current != %q)\n", req.CompareValue)
		}
	default:
		fmt.Printf("ok (%s %s)\n", op, req.Key)
	}
	return nil
}
