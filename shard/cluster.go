package shard

import (
	"fmt"
	"time"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
	"github.com/billdmar/quorum/node"
	"github.com/billdmar/quorum/rpc"
	"github.com/billdmar/quorum/storage"
)

// group is one Raft replication group — a small independent cluster of
// node.Runtime instances over real loopback TCP, exactly like the single-cluster
// production runtime, but there are numShards of them side by side. Reusing the
// (race-clean, gate-verified) node/rpc stack keeps the sketch honest: each shard
// is a real, working Raft group, not a mock.
type group struct {
	id    int
	nodes []*groupNode
}

type groupNode struct {
	rt    *node.Runtime
	tport *rpc.Transport
}

// ShardedCluster is a multi-raft KV cluster: a Router plus numShards independent
// groups. A client op is routed by key to its owning group and submitted to that
// group's leader. Distinct shards share nothing — no common log, leader, or
// state machine — which is the whole point of the sketch.
type ShardedCluster struct {
	router *Router
	groups []*group
}

// clientSeq assigns unique (ClientID, SeqNum) per op so the KV dedup never masks
// work in the demo; a real client library would own session identity.
type clientSeq struct{ n uint64 }

func (c *clientSeq) next() (uint64, uint64) { c.n++; return 1, c.n }

// NewShardedCluster starts numShards independent groups, each a nodesPerGroup
// cluster over ephemeral loopback ports. Returns the cluster and a cleanup func.
// nodesPerGroup should be odd (3 or 5) for a sensible quorum.
func NewShardedCluster(numShards, nodesPerGroup int) (*ShardedCluster, func(), error) {
	sc := &ShardedCluster{router: NewRouter(numShards)}
	var cleanups []func()
	cleanupAll := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for g := 0; g < numShards; g++ {
		grp, cleanup, err := startGroup(g, nodesPerGroup)
		if err != nil {
			cleanupAll()
			return nil, nil, fmt.Errorf("start group %d: %w", g, err)
		}
		cleanups = append(cleanups, cleanup)
		sc.groups = append(sc.groups, grp)
	}
	return sc, cleanupAll, nil
}

// startGroup brings up one nodesPerGroup Raft cluster on ephemeral loopback
// ports (the same two-phase address-discovery wiring the production cluster and
// bench harness use).
func startGroup(gid, n int) (*group, func(), error) {
	ids := make([]core.NodeID, n)
	for i := range ids {
		ids[i] = core.NodeID(fmt.Sprintf("g%d-n%d", gid, i))
	}
	grp := &group{id: gid, nodes: make([]*groupNode, n)}
	addrs := make(map[core.NodeID]string, n)
	for i, id := range ids {
		gn := &groupNode{}
		gn.tport = rpc.NewTransport(id, nil, func(m core.Message) { gn.rt.Deliver(m) })
		if err := gn.tport.Listen("127.0.0.1:0"); err != nil {
			return nil, nil, err
		}
		addrs[id] = gn.tport.Addr()
		grp.nodes[i] = gn
	}
	for i, id := range ids {
		gn := grp.nodes[i]
		_ = gn.tport.Close()
		gn.tport = rpc.NewTransport(id, addrs, func(m core.Message) { gn.rt.Deliver(m) })
		peers := make([]core.NodeID, 0, n-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		cfg := node.Config{Self: id, Peers: peers,
			ElectionMin: 40 * time.Millisecond, ElectionMax: 90 * time.Millisecond,
			Heartbeat: 15 * time.Millisecond}
		gn.rt = node.New(cfg, core.New(core.Config{Self: id, Peers: peers}),
			storage.NewMem(), gn.tport, nil, int64(gid*100+i+1))
		if err := gn.tport.Listen(addrs[id]); err != nil {
			return nil, nil, err
		}
	}
	for _, gn := range grp.nodes {
		gn.rt.Start()
	}
	cleanup := func() {
		for _, gn := range grp.nodes {
			gn.rt.Stop()
			_ = gn.tport.Close()
		}
	}
	return grp, cleanup, nil
}

// leader returns the current leader runtime of group g, or nil.
func (g *group) leader() *node.Runtime {
	for _, gn := range g.nodes {
		if gn.rt.Status().Role == core.Leader {
			return gn.rt
		}
	}
	return nil
}

// WaitReady blocks until every group has elected a leader, or the deadline
// passes (returning an error naming the first group that never converged).
func (sc *ShardedCluster) WaitReady(d time.Duration) error {
	deadline := time.Now().Add(d)
	for _, g := range sc.groups {
		for g.leader() == nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("group %d elected no leader within %v", g.id, d)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	return nil
}

// ShardOf returns the shard ID that owns key (for tests/inspection).
func (sc *ShardedCluster) ShardOf(key string) int { return sc.router.Shard(key) }

// Put routes key to its owning group and proposes a Put to that group's leader.
// Returns an error if the group has no leader right now (caller may retry).
func (sc *ShardedCluster) Put(seq *clientSeq, key, value string) error {
	g := sc.groups[sc.router.Shard(key)]
	l := g.leader()
	if l == nil {
		return fmt.Errorf("group %d has no leader", g.id)
	}
	cid, sn := seq.next()
	cmd := kv.Command{ClientID: cid, SeqNum: sn, Op: check.OpPut, Key: key, Value: value}
	if res := l.Propose(cmd.Encode()); !res.Accepted {
		return fmt.Errorf("group %d leader rejected put (leader hint %q)", g.id, res.LeaderHint)
	}
	return nil
}

// Get routes key to its owning group and performs a linearizable read on that
// group's leader.
func (sc *ShardedCluster) Get(key string) (value string, found, served bool) {
	g := sc.groups[sc.router.Shard(key)]
	l := g.leader()
	if l == nil {
		return "", false, false
	}
	r := l.Read(key)
	return r.Value, r.Found, r.Served
}

// GroupCommitIndexes returns each group's leader commit index (for tests that
// assert groups advance INDEPENDENTLY — writes to one shard must not move
// another shard's log).
func (sc *ShardedCluster) GroupCommitIndexes() []core.Index {
	out := make([]core.Index, len(sc.groups))
	for i, g := range sc.groups {
		if l := g.leader(); l != nil {
			out[i] = l.Status().CommitIndex
		}
	}
	return out
}
