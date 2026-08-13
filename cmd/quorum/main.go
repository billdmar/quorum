// Command quorum is the production CLI for the quorum Raft KV store. It has two
// modes:
//
//	quorum server   — run one cluster node (real TCP Raft transport + a
//	                   client-facing endpoint), driving the SAME pure core the
//	                   deterministic simulator drives.
//	quorum kv ...    — a client that submits put/get/cas/append operations to a
//	                   running node, following leader redirects.
//
// This is the  production runtime: the second driver of the pure core. All
// concurrency lives in the runtime (node package) and the transport (rpc
// package); the core stays pure.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		if err := runServer(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "quorum server:", err)
			os.Exit(1)
		}
	case "kv":
		if err := runKV(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "quorum kv:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `quorum — Raft-replicated key-value store

Usage:
  quorum server -id N1 -peers N1=127.0.0.1:9001,N2=127.0.0.1:9002,N3=127.0.0.1:9003 -client 127.0.0.1:8001
  quorum kv put   -addr 127.0.0.1:8001 <key> <value>
  quorum kv get   -addr 127.0.0.1:8001 <key>
  quorum kv append-addr 127.0.0.1:8001 <key> <value>
  quorum kv cas   -addr 127.0.0.1:8001 <key> <compare> <value>

Run 'quorum server -h' or 'quorum kv -h' for flags.
`)
}
