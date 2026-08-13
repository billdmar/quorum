// Package bench holds honest, reproducible microbenchmarks of the pieces that
// exist at : the pure Raft core's event-processing throughput and the KV
// apply/snapshot paths. These are SINGLE-MACHINE, IN-PROCESS numbers — they
// measure how fast the deterministic core and application state machine process
// work, NOT end-to-end client ops/sec over a network. The end-to-end throughput
// and p99 latency headline (3-node loopback, batched) is produced by the
// real-TCP runtime at /, where a network and real timers actually exist;
// claiming it here would be dishonest.
//
// Run: go test ./bench/ -bench . -benchmem -run '^$'
// Record numbers only on a quiesced machine (see docs/DESIGN.md benchmarking
// note); the CI does not gate on these.
package bench

import (
	"encoding/binary"
	"testing"

	"github.com/billdmar/quorum/check"
	"github.com/billdmar/quorum/core"
	"github.com/billdmar/quorum/kv"
)

// BenchmarkCorePropose measures the leader-side proposal HOT PATH — append +
// persist-effect emission + commit computation + apply — in isolation, using a
// single-node cluster so there is no replication parallelism. (In a multi-node
// cluster a proposal also fans an AppendEntries to each peer; that per-peer
// broadcast cost is measured separately and, for an unacked peer, is a function
// of how far behind it is, not of the propose path itself.) A single-node
// leader commits and applies each entry immediately, so this is the pure
// per-command core cost with no confounding backlog.
func BenchmarkCorePropose(b *testing.B) {
	c := core.New(core.Config{Self: "n1"}) // no peers: single-node
	c.Step(core.Event{Type: core.EventTickElection})
	cmd := make([]byte, 24)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(cmd, uint64(i))
		c.Step(core.Event{Type: core.EventPropose, Ref: core.ClientRef(i), Command: cmd})
	}
}

// BenchmarkCoreAppendEntriesFollower measures follower-side AppendEntries
// processing: the log-consistency check + reconcile + reply for a one-entry
// append, the hot replication path on followers.
func BenchmarkCoreAppendEntriesFollower(b *testing.B) {
	c := core.New(core.Config{Self: "n1", Peers: []core.NodeID{"n2", "n3"}})
	// Prime term.
	c.Step(core.Event{Type: core.EventDeliver, Msg: core.Message{
		From: "n2", To: "n1", Type: core.MsgAppendEntries, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0,
	}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := core.Index(i + 1)
		c.Step(core.Event{Type: core.EventDeliver, Msg: core.Message{
			From: "n2", To: "n1", Type: core.MsgAppendEntries, Term: 1,
			PrevLogIndex: idx - 1, PrevLogTerm: termOf(idx - 1),
			Entries: []core.LogEntry{{Term: 1, Index: idx, Command: []byte("x")}},
		}})
	}
}

func termOf(i core.Index) core.Term {
	if i == 0 {
		return 0
	}
	return 1
}

// BenchmarkKVApply measures application-state-machine throughput: decode + dedup
// + apply one Put per iteration.
func BenchmarkKVApply(b *testing.B) {
	st := kv.NewStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := kv.Command{
			ClientID: 1, SeqNum: uint64(i + 1), Op: check.OpPut,
			Key: "k", Value: "v",
		}
		st.Apply(core.CommittedEntry{Index: core.Index(i + 1), Term: 1, Command: cmd.Encode()})
	}
}

// BenchmarkKVSnapshot measures snapshot serialization throughput at a few state
// sizes — relevant to the compaction path's cost.
func BenchmarkKVSnapshot(b *testing.B) {
	for _, n := range []int{16, 256, 4096} {
		st := kv.NewStore()
		for i := 0; i < n; i++ {
			cmd := kv.Command{ClientID: 1, SeqNum: uint64(i + 1), Op: check.OpPut,
				Key: string(rune('a'+i%26)) + string(rune('0'+i%10)), Value: "value"}
			st.Apply(core.CommittedEntry{Index: core.Index(i + 1), Term: 1, Command: cmd.Encode()})
		}
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = st.Snapshot()
			}
		})
	}
}

func sizeLabel(n int) string {
	switch {
	case n < 100:
		return "keys16"
	case n < 1000:
		return "keys256"
	default:
		return "keys4096"
	}
}
