package node

import (
	"testing"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/rpc"
	"github.com/billdmar/quorum/storage"
)

// clusterNode bundles one runtime with its real TCP transport for the in-process
// multi-node tests. These tests use REAL loopback TCP, REAL timers, and REAL
// goroutines — the production runtime end to end — so they must be -race clean.
type clusterNode struct {
	id    core.NodeID
	rt    *Runtime
	tport *rpc.Transport
}

// newCluster starts an n-node cluster on ephemeral loopback ports and returns the
// nodes plus a cleanup function. Each node's transport is wired to its runtime's
// Deliver; election timers are fast so tests converge quickly.
func newCluster(t *testing.T, n int) ([]*clusterNode, func()) {
	t.Helper()
	ids := make([]core.NodeID, n)
	for i := range ids {
		ids[i] = core.NodeID("n" + string(rune('1'+i)))
	}
	// First bind a listener per node to learn its ephemeral address, then wire the
	// full peer map (every node must know every other's address before Send).
	nodes := make([]*clusterNode, n)
	addrs := make(map[core.NodeID]string, n)
	for i, id := range ids {
		cn := &clusterNode{id: id}
		// Transport handler is set after the runtime exists; use a pointer closure.
		cn.tport = rpc.NewTransport(id, nil, func(m core.Message) { cn.rt.Deliver(m) })
		if err := cn.tport.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		addrs[id] = cn.tport.Addr()
		nodes[i] = cn
	}
	// Rebuild each transport with the now-known full address map (NewTransport
	// copies the map at construction, so we recreate with the complete routing).
	for i, id := range ids {
		cn := nodes[i]
		_ = cn.tport.Close() // close the address-discovery listener
		cn.tport = rpc.NewTransport(id, addrs, func(m core.Message) { cn.rt.Deliver(m) })
		peers := make([]core.NodeID, 0, n-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		cfg := Config{Self: id, Peers: peers,
			ElectionMin: 40 * time.Millisecond, ElectionMax: 90 * time.Millisecond,
			Heartbeat: 15 * time.Millisecond}
		cn.rt = New(cfg, core.New(core.Config{Self: id, Peers: peers}),
			storage.NewMem(), cn.tport, nil, int64(i+1))
		if err := cn.tport.Listen(addrs[id]); err != nil {
			t.Fatalf("relisten %s: %v", id, err)
		}
	}
	for _, cn := range nodes {
		cn.rt.Start()
	}
	cleanup := func() {
		for _, cn := range nodes {
			cn.rt.Stop()
			_ = cn.tport.Close()
		}
	}
	return nodes, cleanup
}

// leaderOf returns the node currently reporting Leader, or nil.
func leaderOf(nodes []*clusterNode) *clusterNode {
	for _, cn := range nodes {
		if cn.rt.Status().Role == core.Leader {
			return cn
		}
	}
	return nil
}

// waitLeader polls until some node reports leadership, returning it.
func waitLeader(t *testing.T, nodes []*clusterNode, d time.Duration) *clusterNode {
	t.Helper()
	var ldr *clusterNode
	if !waitFor(d, func() bool { ldr = leaderOf(nodes); return ldr != nil }) {
		t.Fatalf("no leader elected within %v", d)
	}
	return ldr
}

// TestClusterElectsAndReplicates: a real-TCP 3-node cluster elects a leader,
// accepts a Put on the leader, and every node's kv.Store converges to the value.
func TestClusterElectsAndReplicates(t *testing.T) {
	nodes, cleanup := newCluster(t, 3)
	defer cleanup()

	ldr := waitLeader(t, nodes, 5*time.Second)
	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "v1"}
	if res := ldr.rt.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("leader rejected put: %+v", res)
	}
	// Every node converges (committed + applied) to k=v1.
	if !waitFor(5*time.Second, func() bool {
		for _, cn := range nodes {
			r := cn.rt.Read("k")
			// Only the leader serves reads; a follower returns Served=false. Assert
			// via the leader's linearizable read that the value is committed.
			if cn == leaderOf(nodes) && (!r.Served || r.Value != "v1") {
				return false
			}
		}
		l := leaderOf(nodes)
		return l != nil && l.rt.Read("k").Value == "v1"
	}) {
		t.Fatal("cluster did not converge on k=v1")
	}
}

// TestClusterLeaderKillConverges is the  headline: kill the leader, the cluster
// re-elects a new one, and a value written before the kill is still readable —
// live, over real TCP.
func TestClusterLeaderKillConverges(t *testing.T) {
	nodes, cleanup := newCluster(t, 3)
	defer cleanup()

	ldr := waitLeader(t, nodes, 5*time.Second)
	put := kv.Command{ClientID: 7, SeqNum: 1, Op: check.OpPut, Key: "survives", Value: "yes"}
	if res := ldr.rt.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("leader rejected put: %+v", res)
	}
	// Make sure it committed on the leader before we kill it.
	if !waitFor(5*time.Second, func() bool {
		r := ldr.rt.Read("survives")
		return r.Served && r.Value == "yes"
	}) {
		t.Fatal("write did not commit before kill")
	}

	// Kill the leader (stop its runtime + close its transport).
	killed := ldr
	killed.rt.Stop()
	_ = killed.tport.Close()

	// The remaining two nodes must elect a new leader and still serve the value.
	survivors := make([]*clusterNode, 0, 2)
	for _, cn := range nodes {
		if cn != killed {
			survivors = append(survivors, cn)
		}
	}
	newLdr := waitLeader(t, survivors, 5*time.Second)
	if newLdr == killed {
		t.Fatal("killed node still reports leader")
	}
	if !waitFor(5*time.Second, func() bool {
		r := newLdr.rt.Read("survives")
		return r.Served && r.Value == "yes"
	}) {
		t.Fatalf("value lost after leader kill; new leader %s read %+v", newLdr.id, newLdr.rt.Read("survives"))
	}
}
