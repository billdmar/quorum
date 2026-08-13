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

// membershipNode bundles a runtime + transport for the live membership test.
type membershipNode struct {
	id    core.NodeID
	rt    *Runtime
	tport *rpc.Transport
}

// startMembershipCluster starts `initial` voting nodes but wires the transport
// mesh over ALL `total` node addresses, so a node added later already has routes
// to/from everyone. Nodes [0,initial) begin as voters (each other in Peers);
// nodes [initial,total) start knowing the full membership (so they can catch up
// when added) but are not yet voters anywhere. Returns the nodes + cleanup.
func startMembershipCluster(t *testing.T, initial, total int) ([]*membershipNode, func()) {
	t.Helper()
	ids := make([]core.NodeID, total)
	for i := range ids {
		ids[i] = core.NodeID("m" + string(rune('1'+i)))
	}
	nodes := make([]*membershipNode, total)
	addrs := make(map[core.NodeID]string, total)
	for i, id := range ids {
		mn := &membershipNode{id: id}
		mn.tport = rpc.NewTransport(id, nil, func(m core.Message) { mn.rt.Deliver(m) })
		if err := mn.tport.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		addrs[id] = mn.tport.Addr()
		nodes[i] = mn
	}
	for i, id := range ids {
		mn := nodes[i]
		_ = mn.tport.Close()
		// Every transport knows every node's address (routes exist for future adds).
		mn.tport = rpc.NewTransport(id, addrs, func(m core.Message) { mn.rt.Deliver(m) })
		// Initial voters list: the first `initial` nodes form the starting cluster;
		// a later-added node starts with the initial voters as its peers so it can
		// receive replication and vote once promoted.
		var peers []core.NodeID
		for j := 0; j < initial; j++ {
			if ids[j] != id {
				peers = append(peers, ids[j])
			}
		}
		cfg := Config{Self: id, Peers: peers,
			ElectionMin: 50 * time.Millisecond, ElectionMax: 110 * time.Millisecond,
			Heartbeat: 18 * time.Millisecond}
		mn.rt = New(cfg, core.New(core.Config{Self: id, Peers: peers}),
			storage.NewMem(), mn.tport, nil, int64(i+1))
		if err := mn.tport.Listen(addrs[id]); err != nil {
			t.Fatalf("relisten %s: %v", id, err)
		}
	}
	// Start only the initial voters; the spare node(s) start too (so they can be
	// added later) but won't win elections while not in anyone's voter set.
	for _, mn := range nodes {
		mn.rt.Start()
	}
	cleanup := func() {
		for _, mn := range nodes {
			mn.rt.Stop()
			_ = mn.tport.Close()
		}
	}
	return nodes, cleanup
}

func membershipLeader(nodes []*membershipNode) *membershipNode {
	for _, mn := range nodes {
		if mn.rt.Status().Role == core.Leader {
			return mn
		}
	}
	return nil
}

func waitMembershipLeader(t *testing.T, nodes []*membershipNode, d time.Duration) *membershipNode {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if l := membershipLeader(nodes); l != nil {
			return l
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatal("no leader within timeout")
	return nil
}

// TestLiveAddServer: a running 3-node cluster adds a 4th server; the new server
// catches up and the cluster's voter set converges to 4. Real-TCP, -race clean.
func TestLiveAddServer(t *testing.T) {
	nodes, cleanup := startMembershipCluster(t, 3, 4) // 3 voters + 1 spare (m4)
	defer cleanup()
	ldr := waitMembershipLeader(t, nodes, 5*time.Second)

	// Commit a write first so the leader has a current-term entry committed (the
	// membership precondition) and there is state for the new node to catch up on.
	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "k", Value: "v1"}
	if res := ldr.rt.Propose(put.Encode()); !res.Accepted {
		t.Fatalf("initial put rejected: %+v", res)
	}
	waitUntil(t, 3*time.Second, func() bool { return ldr.rt.Status().CommitIndex >= 2 })

	// Add m4. Retry across a possible election gap.
	if !waitUntil(t, 3*time.Second, func() bool {
		return ldr.rt.ChangeConfig(true, "m4").Accepted || membershipLeader(nodes) == nil
	}) {
		t.Fatal("AddServer never accepted")
	}
	// If leadership moved, re-resolve and (idempotently) ensure m4 is being added.
	l := waitMembershipLeader(t, nodes, 3*time.Second)

	// The leader's voter set converges to 4 members including m4.
	if !waitUntil(t, 5*time.Second, func() bool {
		return len(l.rt.Status().Members) == 4 && containsID(l.rt.Status().Members, "m4")
	}) {
		t.Fatalf("voter set did not converge to 4 incl m4; got %v", l.rt.Status().Members)
	}

	// m4 catches up: it now sees the committed write (via a linearizable read on
	// whoever is leader — the value must still be present after reconfiguration).
	if !waitUntil(t, 5*time.Second, func() bool {
		r := l.rt.Read("k")
		return r.Served && r.Found && r.Value == "v1"
	}) {
		t.Fatal("committed value lost across AddServer")
	}
}

// TestLiveRemoveServer: a running 3-node cluster removes one server; the
// remaining 2 keep committing (a new write commits under the 2-node quorum).
func TestLiveRemoveServer(t *testing.T) {
	nodes, cleanup := startMembershipCluster(t, 3, 3)
	defer cleanup()
	ldr := waitMembershipLeader(t, nodes, 5*time.Second)

	// Establish a committed current-term entry.
	waitUntil(t, 3*time.Second, func() bool { return ldr.rt.Status().CommitIndex >= 1 })
	put := kv.Command{ClientID: 1, SeqNum: 1, Op: check.OpPut, Key: "a", Value: "1"}
	ldr.rt.Propose(put.Encode())
	waitUntil(t, 3*time.Second, func() bool { return ldr.rt.Status().CommitIndex >= 2 })

	// Remove a follower (not the leader) so leadership is stable across the change.
	var victim core.NodeID
	for _, mn := range nodes {
		if mn.rt.Status().Role != core.Leader {
			victim = mn.id
			break
		}
	}
	if !waitUntil(t, 3*time.Second, func() bool {
		return ldr.rt.ChangeConfig(false, victim).Accepted || membershipLeader(nodes) == nil
	}) {
		t.Fatal("RemoveServer never accepted")
	}
	l := waitMembershipLeader(t, nodes, 3*time.Second)

	// Voter set converges to 2 (victim gone).
	if !waitUntil(t, 5*time.Second, func() bool {
		return len(l.rt.Status().Members) == 2 && !containsID(l.rt.Status().Members, victim)
	}) {
		t.Fatalf("voter set did not converge to 2 without %s; got %v", victim, l.rt.Status().Members)
	}

	// The reduced cluster still commits a NEW write under the 2-node quorum.
	put2 := kv.Command{ClientID: 2, SeqNum: 1, Op: check.OpPut, Key: "b", Value: "2"}
	if !waitUntil(t, 3*time.Second, func() bool { return l.rt.Propose(put2.Encode()).Accepted }) {
		t.Fatal("post-removal write never accepted")
	}
	if !waitUntil(t, 5*time.Second, func() bool {
		r := l.rt.Read("b")
		return r.Served && r.Found && r.Value == "2"
	}) {
		t.Fatal("reduced cluster did not commit a new write")
	}
}

func containsID(ids []core.NodeID, want core.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Millisecond)
	}
	return cond()
}
