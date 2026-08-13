# Multi-raft sharding sketch (P6 stretch)

A **sketch** of horizontal scaling: partition the keyspace across N independent
Raft groups, each an independent instance of the same pure core (its own leader,
log, and KV state machine). A deterministic router maps each key to its owning
group.

## What's here

- `router.go` — `Router.Shard(key)`: deterministic FNV-1a-mod-N key→shard
  mapping (same key ⇒ same shard, always; no floats, no map iteration).
- `cluster.go` — `ShardedCluster`: N independent groups, each a real
  `nodesPerGroup`-node cluster built from the **gate-verified** `node.Runtime` +
  `rpc.Transport` stack over loopback TCP. `Put`/`Get` route by key to the owning
  group's leader.
- `shard_test.go` — proves routing determinism/spread and, crucially, **group
  independence**: a write to shard A converges on A while shard B's commit index
  is unchanged, and an unwritten key on B reads absent.

## Why it's a sketch, not production multi-raft

Each shard is a *real* Raft group (not a mock) — the sketch is honest about the
consensus per shard. What a production multi-raft system needs and this
deliberately omits:

- **A shard-placement / configuration manager** — here the router is a fixed
  `key mod N`; real systems track shard→group assignments in a metadata service
  (itself often a Raft group) and can move shards between groups.
- **Dynamic split/merge and rebalancing** — the shard count is fixed at startup.
- **Cross-shard transactions** — an op touching two shards would need a protocol
  across groups (e.g. two-phase commit); this sketch has single-key ops only.
- **Transport multiplexing** — each group here has its own listeners; production
  would multiplex many groups over shared connections.

The point of the sketch is to show the *architecture* — independent consensus
groups behind a deterministic router — reusing quorum's verified core and
runtime, and to make the scaling story concrete for design discussion.
