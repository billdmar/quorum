package bench

// End-to-end throughput benchmark of the REAL production runtime: a 3-node
// cluster wired over real loopback TCP (rpc.Transport) with real timers, driven
// by N concurrent client goroutines issuing sessioned kv.Command Puts to the
// leader. Unlike the microbenchmarks in bench_test.go (which measure the pure
// core / KV in isolation), this measures the full path a client op travels:
// client -> leader -> replicate to a quorum over TCP -> commit -> apply.
//
// HONEST LABELLING: this is a 3-node LOOPBACK cluster on a SINGLE machine, and
// the runtime does NOT batch client requests — each proposal is its own Raft
// round. Numbers are ops/sec and p50/p99 latency under a given client
// concurrency, reproducible by the command in the file header. It is a bench
// (not a correctness gate) and is excluded from CI.
//
// Run: go test ./bench/ -bench Throughput -run '^$' -benchmem

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/node"
	"github.com/billdmar/quorum/rpc"
	"github.com/billdmar/quorum/storage"
)

// benchNode bundles a runtime with its transport for teardown.
type benchNode struct {
	rt    *node.Runtime
	tport *rpc.Transport
}

// startCluster brings up an n-node cluster on ephemeral loopback ports, wired
// over real TCP, and returns the nodes plus a cleanup func. Mirrors the
// node package's in-process cluster harness (which lives in _test.go and is not
// importable here).
func startCluster(tb testing.TB, n int) ([]*benchNode, func()) {
	tb.Helper()
	ids := make([]core.NodeID, n)
	for i := range ids {
		ids[i] = core.NodeID("n" + string(rune('1'+i)))
	}
	// Phase 1: bind a listener per node to learn its ephemeral address.
	nodes := make([]*benchNode, n)
	addrs := make(map[core.NodeID]string, n)
	for i, id := range ids {
		bn := &benchNode{}
		bn.tport = rpc.NewTransport(id, nil, func(m core.Message) { bn.rt.Deliver(m) })
		if err := bn.tport.Listen("127.0.0.1:0"); err != nil {
			tb.Fatalf("listen %s: %v", id, err)
		}
		addrs[id] = bn.tport.Addr()
		nodes[i] = bn
	}
	// Phase 2: rebuild each transport with the full address map, wire the runtime.
	for i, id := range ids {
		bn := nodes[i]
		_ = bn.tport.Close()
		bn.tport = rpc.NewTransport(id, addrs, func(m core.Message) { bn.rt.Deliver(m) })
		peers := make([]core.NodeID, 0, n-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		cfg := node.Config{Self: id, Peers: peers,
			ElectionMin: 40 * time.Millisecond, ElectionMax: 90 * time.Millisecond,
			Heartbeat: 15 * time.Millisecond}
		bn.rt = node.New(cfg, core.New(core.Config{Self: id, Peers: peers}),
			storage.NewMem(), bn.tport, nil, int64(i+1))
		if err := bn.tport.Listen(addrs[id]); err != nil {
			tb.Fatalf("relisten %s: %v", id, err)
		}
	}
	for _, bn := range nodes {
		bn.rt.Start()
	}
	cleanup := func() {
		for _, bn := range nodes {
			bn.rt.Stop()
			_ = bn.tport.Close()
		}
	}
	return nodes, cleanup
}

// leader returns a node currently reporting leadership, or nil.
func leader(nodes []*benchNode) *node.Runtime {
	for _, bn := range nodes {
		if bn.rt.Status().Role == core.Leader {
			return bn.rt
		}
	}
	return nil
}

// waitLeader blocks until some node reports leadership.
func waitLeader(tb testing.TB, nodes []*benchNode) *node.Runtime {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l := leader(nodes); l != nil {
			return l
		}
		time.Sleep(2 * time.Millisecond)
	}
	tb.Fatal("no leader elected within 5s")
	return nil
}

// BenchmarkClusterThroughput measures end-to-end Put throughput and latency for
// a 3-node loopback cluster under a fixed number of concurrent clients. The
// b.N iterations are distributed across the client goroutines; each op is a
// distinct sessioned kv.Command (unique ClientID+SeqNum), so no dedup masks work.
func BenchmarkClusterThroughput(b *testing.B) {
	for _, clients := range []int{1, 8, 32} {
		b.Run(concurrencyLabel(clients), func(b *testing.B) {
			nodes, cleanup := startCluster(b, 3)
			defer cleanup()
			ldr := waitLeader(b, nodes)

			var seq uint64 // global op counter -> unique (ClientID, SeqNum) per op
			latencies := make([]time.Duration, b.N)
			var idx int64 = -1

			b.ResetTimer()
			start := time.Now()
			var wg sync.WaitGroup
			perClient := (b.N + clients - 1) / clients
			for c := 0; c < clients; c++ {
				wg.Add(1)
				go func(clientID uint64) {
					defer wg.Done()
					for i := 0; i < perClient; i++ {
						n := atomic.AddUint64(&seq, 1)
						if int(n) > b.N {
							return
						}
						cmd := kv.Command{ClientID: clientID, SeqNum: n, Op: check.OpPut,
							Key: "k", Value: "v"}
						t0 := time.Now()
						// Always target the current leader; on a rare election gap,
						// re-resolve. Propose blocks until the leader appends the entry.
						l := ldr
						if l.Status().Role != core.Leader {
							if nl := leader(nodes); nl != nil {
								l = nl
							}
						}
						l.Propose(cmd.Encode())
						d := time.Since(t0)
						slot := atomic.AddInt64(&idx, 1)
						if int(slot) < len(latencies) {
							latencies[slot] = d
						}
					}
				}(uint64(c + 1))
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			reportThroughput(b, elapsed, latencies[:min(int(idx+1), len(latencies))])
		})
	}
}

// reportThroughput attaches ops/sec and p50/p99 latency as custom bench metrics.
func reportThroughput(b *testing.B, elapsed time.Duration, lat []time.Duration) {
	b.Helper()
	if len(lat) == 0 || elapsed <= 0 {
		return
	}
	ops := float64(len(lat)) / elapsed.Seconds()
	b.ReportMetric(ops, "ops/sec")
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p50 := lat[len(lat)*50/100]
	p99 := lat[min(len(lat)*99/100, len(lat)-1)]
	b.ReportMetric(float64(p50.Microseconds())/1000.0, "p50_ms")
	b.ReportMetric(float64(p99.Microseconds())/1000.0, "p99_ms")
}

func concurrencyLabel(n int) string {
	switch n {
	case 1:
		return "clients1"
	case 8:
		return "clients8"
	default:
		return "clients32"
	}
}
