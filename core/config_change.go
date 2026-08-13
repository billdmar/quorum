package core

import (
	"encoding/binary"
	"sort"
)

// config_change.go implements single-server membership changes (Raft
// dissertation §4.1, no joint consensus). A membership change is a KindConfig
// log entry; the core adopts the new voting configuration the moment the entry
// is APPENDED (not on commit) — the property that makes one-server-at-a-time
// changes safe. Quorum is recomputed from the current voter set, which every
// existing quorum-check site already reads via c.quorum / c.peers.

// EncodeConfigChange serializes a ConfigChange into a KindConfig entry's Command:
// [add:1][serverLen:u16][server]. Deterministic (fixed field order).
func EncodeConfigChange(cc ConfigChange) []byte {
	s := []byte(cc.Server)
	b := make([]byte, 3+len(s))
	if cc.Add {
		b[0] = 1
	}
	binary.BigEndian.PutUint16(b[1:3], uint16(len(s)))
	copy(b[3:], s)
	return b
}

// DecodeConfigChange parses a KindConfig entry's Command; ok=false if malformed.
func DecodeConfigChange(cmd []byte) (cc ConfigChange, ok bool) {
	if len(cmd) < 3 {
		return ConfigChange{}, false
	}
	cc.Add = cmd[0] == 1
	n := int(binary.BigEndian.Uint16(cmd[1:3]))
	if 3+n != len(cmd) {
		return ConfigChange{}, false
	}
	cc.Server = NodeID(cmd[3:])
	return cc, true
}

// handleChangeConfig is the leader-side EventChangeConfig handler. It enforces
// the single-server-change safety preconditions, then appends a KindConfig entry
// (which is adopted immediately on append).
//
// Preconditions (reject via EffectRejectProposal otherwise):
//   - this node is leader;
//   - the previous config change (if any) is COMMITTED (one at a time);
//   - an entry from the current term has committed (noopIndex committed) — the
//     leader must have established its term before reconfiguring;
//   - the change is non-trivial (adding an existing voter / removing an absent
//     one is rejected as a no-op).
func (c *raftCore) handleChangeConfig(ref ClientRef, add bool, server NodeID, eb *effects) {
	if c.role != Leader {
		eb.rejectProposal(ref, c.leaderHint)
		return
	}
	// One-at-a-time: the last adopted config entry must be committed.
	if c.lastConfigIndex > c.commitIndex {
		eb.rejectProposal(ref, c.self)
		return
	}
	// Current-term-committed gate: the leader's no-op (this term) must have committed.
	if c.commitIndex < c.noopIndex {
		eb.rejectProposal(ref, c.self)
		return
	}
	// Reject trivial changes so quorum can't be corrupted by a redundant op.
	if add == c.voters[server] {
		eb.rejectProposal(ref, c.self)
		return
	}
	// Removing the last voter is nonsensical; reject.
	if !add && len(c.voters) <= 1 {
		eb.rejectProposal(ref, c.self)
		return
	}

	e := LogEntry{
		Term:    c.currentTerm,
		Index:   c.log.lastIndex() + 1,
		Kind:    KindConfig,
		Command: EncodeConfigChange(ConfigChange{Add: add, Server: server}),
	}
	c.log.append(e)
	c.adoptConfigFromLog(eb) // adopt-on-append
	eb.persistLog(e.Index, []LogEntry{e})
	c.maybeAdvanceCommit(eb) // single-node clusters commit immediately
	c.broadcastAppend(eb)
}

// adoptConfigFromLog re-derives the current voting configuration from the log:
// it replays every KindConfig entry present (in index order) over the base
// configuration. This is the single source of truth for membership — called
// after any append or truncation — so an uncommitted config entry that is later
// truncated cleanly reverts the voter set (revert-on-truncation). It updates the
// derived quorum and, if the voter set changed, leader per-peer maps, and emits
// EffectConfigChanged.
func (c *raftCore) adoptConfigFromLog(eb *effects) {
	// Rebuild voters from the base (self + entries below the log) then apply every
	// KindConfig entry currently in the log. The base is what's implied by the
	// snapshot + already-compacted config; we conservatively start from the
	// current membership minus the effect of log config entries is hard to invert,
	// so instead we track the base explicitly: start from self and re-apply from a
	// remembered baseline. Simpler + correct: recompute from configBaseline + log.
	newVoters := map[NodeID]bool{}
	for id := range c.configBaseline {
		newVoters[id] = true
	}
	var lastCfgIdx Index
	for _, e := range c.log.entries {
		if e.Kind != KindConfig {
			continue
		}
		if cc, ok := DecodeConfigChange(e.Command); ok {
			if cc.Add {
				newVoters[cc.Server] = true
			} else {
				delete(newVoters, cc.Server)
			}
			lastCfgIdx = e.Index
		}
	}
	c.lastConfigIndex = lastCfgIdx
	if !voterSetEqual(newVoters, c.voters) {
		c.voters = newVoters
		c.recomputeMembership()
		eb.configChanged(c.Members())
	}
}

// recomputeMembership rebuilds the derived peers/quorum/clusterN from voters and
// prunes leader per-peer maps of departed servers (so a removed node no longer
// counts toward quorum and a new one gets replication state on next append).
func (c *raftCore) recomputeMembership() {
	peers := make([]NodeID, 0, len(c.voters))
	for id := range c.voters {
		if id != c.self {
			peers = append(peers, id)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	c.peers = peers
	c.clusterN = len(c.voters)
	c.quorum = c.clusterN/2 + 1

	if c.role == Leader {
		// Drop departed peers from leader maps; new peers are lazily initialized on
		// the next sendAppendTo (nextIndex defaults to 0 -> snapshot/backfill path).
		inSet := func(m map[NodeID]Index) {
			for id := range m {
				if id != c.self && !c.voters[id] {
					delete(m, id)
				}
			}
		}
		inSet(c.nextIndex)
		inSet(c.matchIndex)
		for id := range c.snapPending {
			if !c.voters[id] {
				delete(c.snapPending, id)
			}
		}
		for id := range c.confirmAcks {
			if !c.voters[id] {
				delete(c.confirmAcks, id)
			}
		}
		// Ensure every current peer has replication state.
		for _, p := range c.peers {
			if _, ok := c.nextIndex[p]; !ok {
				c.nextIndex[p] = c.log.lastIndex() + 1
				c.matchIndex[p] = 0
			}
		}
	}
}

func voterSetEqual(a, b map[NodeID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}
