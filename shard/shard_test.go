package shard

import (
	"strconv"
	"testing"
	"time"
)

// TestRouterDeterministic: the same key always routes to the same shard, and
// shards are in range. Determinism is the property the whole system relies on.
func TestRouterDeterministic(t *testing.T) {
	r := NewRouter(4)
	for _, k := range []string{"a", "b", "user:42", "", "shard-key-xyz"} {
		s1, s2 := r.Shard(k), r.Shard(k)
		if s1 != s2 {
			t.Fatalf("router non-deterministic for %q: %d != %d", k, s1, s2)
		}
		if s1 < 0 || s1 >= 4 {
			t.Fatalf("shard %d out of range for %q", s1, k)
		}
	}
}

// TestRouterSpreadsKeys: over many keys, every shard gets at least one key (the
// router actually partitions the space, not maps everything to shard 0).
func TestRouterSpreadsKeys(t *testing.T) {
	r := NewRouter(4)
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[r.Shard("key-"+strconv.Itoa(i))] = true
	}
	if len(seen) < 4 {
		t.Fatalf("router only used %d of 4 shards over 200 distinct keys", len(seen))
	}
}

// TestShardedClusterIndependentGroups is the sketch's core demonstration: two
// independent Raft groups, each a real 3-node loopback cluster, replicate
// writes to their OWN shard without affecting the other. A key routed to group A
// is readable from group A; the two groups' commit indexes advance
// independently (writing only to A's shard does not move B's log).
func TestShardedClusterIndependentGroups(t *testing.T) {
	sc, cleanup, err := NewShardedCluster(2, 3)
	if err != nil {
		t.Fatalf("NewShardedCluster: %v", err)
	}
	defer cleanup()
	if err := sc.WaitReady(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Find two keys that route to different shards, so we exercise both groups.
	var keyA, keyB string
	for i := 0; i < 100 && (keyA == "" || keyB == ""); i++ {
		k := "k" + string(rune('a'+i))
		switch sc.ShardOf(k) {
		case 0:
			if keyA == "" {
				keyA = k
			}
		case 1:
			if keyB == "" {
				keyB = k
			}
		}
	}
	if keyA == "" || keyB == "" {
		t.Fatal("could not find keys mapping to both shards")
	}

	before := sc.GroupCommitIndexes()

	// Write only to shard A's key.
	seq := &clientSeq{}
	if err := writeRetry(t, sc, seq, keyA, "valueA"); err != nil {
		t.Fatalf("put %q: %v", keyA, err)
	}

	// keyA is readable from its group with the written value.
	if !readEventually(t, sc, keyA, "valueA") {
		t.Fatalf("key %q (shard %d) did not converge to valueA", keyA, sc.ShardOf(keyA))
	}

	// The OTHER group's commit index must not have advanced from the A-write
	// (independent logs). We allow the baseline no-op/election entries in
	// `before`, and assert group B did not move because of A's write.
	after := sc.GroupCommitIndexes()
	shardA, shardB := sc.ShardOf(keyA), sc.ShardOf(keyB)
	if after[shardA] <= before[shardA] {
		t.Errorf("shard %d commit did not advance after its write (%d -> %d)", shardA, before[shardA], after[shardA])
	}
	if after[shardB] != before[shardB] {
		t.Errorf("shard %d commit moved (%d -> %d) due to a write on shard %d — groups are not independent",
			shardB, before[shardB], after[shardB], shardA)
	}

	// And keyB, never written, is absent from its (independent) group.
	if v, found, served := sc.Get(keyB); served && found {
		t.Errorf("key %q was never written but read back %q found=%v", keyB, v, found)
	}
}

func writeRetry(t *testing.T, sc *ShardedCluster, seq *clientSeq, key, val string) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = sc.Put(seq, key, val); err == nil {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return err
}

func readEventually(t *testing.T, sc *ShardedCluster, key, want string) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, found, served := sc.Get(key); served && found && v == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
