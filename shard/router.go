// Package shard is a SKETCH of multi-raft sharding on top of quorum's Raft core:
// the keyspace is partitioned across N independent Raft replication groups, each
// an independent instance of the same pure core (one leader, one log, one KV
// state machine per group). A deterministic router maps each key to its owning
// group.
//
// SCOPE (honest): this is an architecture sketch, not a production multi-raft.
// It demonstrates (a) deterministic key→shard routing and (b) that N groups run
// as fully independent consensus instances. It deliberately omits everything a
// real multi-raft system needs — a shard-configuration/placement manager,
// dynamic shard splitting/merging and rebalancing, cross-shard transactions
// (e.g. 2PC over groups), and shared-transport multiplexing. See shard/README.md.
package shard

import "hash/fnv"

// Router maps keys to shard (group) IDs. Routing is a pure, deterministic
// function of the key and the shard count — the same key always routes to the
// same shard — so the mapping is stable and reproducible, the same property the
// rest of quorum relies on.
type Router struct {
	numShards int
}

// NewRouter returns a router over numShards groups (numShards >= 1).
func NewRouter(numShards int) *Router {
	if numShards < 1 {
		numShards = 1
	}
	return &Router{numShards: numShards}
}

// NumShards returns the number of shard groups.
func (r *Router) NumShards() int { return r.numShards }

// Shard returns the shard ID (in [0, numShards)) that owns key. It uses FNV-1a
// (stdlib, stable across architectures) mod numShards — no floating point, no
// map iteration, so the mapping is deterministic.
func (r *Router) Shard(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(r.numShards))
}
