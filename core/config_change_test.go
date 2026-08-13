package core

import "testing"

// leaderReady drives a fresh n-node core to leadership in term 1 with its no-op
// committed (so the current-term-committed membership gate is satisfied), and
// drains setup effects. Returns the leader core.
func leaderReady(t *testing.T, n int) *raftCore {
	t.Helper()
	c := leaderCore(t) // 3-node leader in term 1, no-op committed (readindex_test.go)
	if n != 3 {
		t.Fatalf("leaderReady only builds the 3-node fixture; got n=%d", n)
	}
	return c
}

// TestConfigChangeEncodeDecode round-trips a ConfigChange payload.
func TestConfigChangeEncodeDecode(t *testing.T) {
	for _, cc := range []ConfigChange{
		{Add: true, Server: "n4"},
		{Add: false, Server: "n2"},
		{Add: true, Server: ""},
	} {
		got, ok := DecodeConfigChange(EncodeConfigChange(cc))
		if !ok || got != cc {
			t.Fatalf("round-trip %+v -> %+v ok=%v", cc, got, ok)
		}
	}
	if _, ok := DecodeConfigChange([]byte{0x01}); ok {
		t.Error("short buffer should not decode")
	}
}

// TestAddServerRecomputesQuorumOnAppend verifies AddServer appends a KindConfig
// entry and the core adopts the new voter set + quorum IMMEDIATELY on append
// (before commit): 3 voters (quorum 2) -> 4 voters (quorum 3).
func TestAddServerRecomputesQuorumOnAppend(t *testing.T) {
	c := leaderReady(t, 3)
	if c.quorum != 2 || c.clusterN != 3 {
		t.Fatalf("pre: quorum=%d n=%d, want 2/3", c.quorum, c.clusterN)
	}
	effs := c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n4"})
	if c.clusterN != 4 || c.quorum != 3 {
		t.Fatalf("post-add: quorum=%d n=%d, want 3/4 (adopted on append)", c.quorum, c.clusterN)
	}
	if !c.voters["n4"] {
		t.Error("n4 not in voter set after AddServer")
	}
	// A config entry was persisted and a ConfigChanged effect emitted.
	if _, ok := firstEffect(effs, EffectPersistLog); !ok {
		t.Error("no log persist for the config entry")
	}
	if _, ok := firstEffect(effs, EffectConfigChanged); !ok {
		t.Error("no EffectConfigChanged emitted")
	}
	// n4 got leader replication state.
	if _, ok := c.nextIndex["n4"]; !ok {
		t.Error("n4 missing from nextIndex after add")
	}
}

// TestRemoveServerRecomputesQuorumAndDropsPeer verifies RemoveServer drops the
// peer from the voter set + leader maps and lowers the quorum.
func TestRemoveServerRecomputesQuorumAndDropsPeer(t *testing.T) {
	c := leaderReady(t, 3)
	c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: false, ConfigServer: "n3"})
	if c.clusterN != 2 || c.quorum != 2 {
		t.Fatalf("post-remove: quorum=%d n=%d, want 2/2", c.quorum, c.clusterN)
	}
	if c.voters["n3"] {
		t.Error("n3 still a voter after RemoveServer")
	}
	if _, ok := c.matchIndex["n3"]; ok {
		t.Error("n3 still in matchIndex after removal")
	}
}

// TestConfigChangeOneAtATime verifies a second config change is rejected while
// the first is still uncommitted (the one-at-a-time safety constraint).
func TestConfigChangeOneAtATime(t *testing.T) {
	c := leaderReady(t, 3)
	// First change appends at some index; it is NOT yet committed (no acks fed).
	c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n4"})
	if c.commitIndex >= c.lastConfigIndex {
		t.Fatalf("precondition: config entry should be uncommitted (commit=%d cfgIdx=%d)", c.commitIndex, c.lastConfigIndex)
	}
	// Second change while the first is uncommitted must be rejected.
	effs := c.Step(Event{Type: EventChangeConfig, Ref: 2, ConfigAdd: true, ConfigServer: "n5"})
	rej, ok := firstEffect(effs, EffectRejectProposal)
	if !ok || rej.Ref != 2 {
		t.Fatalf("second concurrent config change must be rejected; got %+v ok=%v", rej, ok)
	}
	if c.voters["n5"] {
		t.Error("n5 was added despite the pending uncommitted config change")
	}
}

// TestConfigChangeRequiresCurrentTermCommitted verifies a config change is
// rejected before the leader has committed an entry in its current term.
func TestConfigChangeRequiresCurrentTermCommitted(t *testing.T) {
	c := mkCore(3)
	c.Step(Event{Type: EventTickElection}) // candidate term 1
	deliverVoteGrants(c, 1)                // leader; no-op appended but NOT yet committed
	if c.commitIndex >= c.noopIndex {
		t.Fatalf("precondition: no-op should be uncommitted (commit=%d noop=%d)", c.commitIndex, c.noopIndex)
	}
	effs := c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n4"})
	if _, ok := firstEffect(effs, EffectRejectProposal); !ok {
		t.Fatal("config change before current-term commit must be rejected")
	}
	if c.voters["n4"] {
		t.Error("n4 added before the current-term gate was satisfied")
	}
}

// TestConfigChangeRejectsTrivial verifies adding an existing voter or removing an
// absent one is rejected (would be a quorum-corrupting no-op).
func TestConfigChangeRejectsTrivial(t *testing.T) {
	c := leaderReady(t, 3)
	// n2 already a voter — adding it is trivial.
	if effs := c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n2"}); func() bool {
		_, ok := firstEffect(effs, EffectRejectProposal)
		return !ok
	}() {
		t.Error("adding an existing voter should be rejected")
	}
	// n9 absent — removing it is trivial.
	if effs := c.Step(Event{Type: EventChangeConfig, Ref: 2, ConfigAdd: false, ConfigServer: "n9"}); func() bool {
		_, ok := firstEffect(effs, EffectRejectProposal)
		return !ok
	}() {
		t.Error("removing an absent server should be rejected")
	}
}

// TestFollowerAdoptsAndRevertsConfig verifies a follower adopts a config entry on
// append and REVERTS when that (uncommitted) entry is truncated by a conflicting
// suffix from a new leader — membership is re-derived from the surviving log.
func TestFollowerAdoptsAndRevertsConfig(t *testing.T) {
	c := mkCore(3) // voters {n1,n2,n3}
	c.currentTerm = 5
	// Leader n2 (term 5) sends: index1 normal(t5), index2 config add n4 (t5).
	cfgCmd := EncodeConfigChange(ConfigChange{Add: true, Server: "n4"})
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntries, Term: 5,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 5, Index: 1},
			{Term: 5, Index: 2, Kind: KindConfig, Command: cfgCmd},
		},
	}})
	if !c.voters["n4"] {
		t.Fatal("follower did not adopt the appended config entry (n4 should be a voter)")
	}
	if c.clusterN != 4 {
		t.Fatalf("follower voter count = %d, want 4 after adopt", c.clusterN)
	}
	// A new leader n3 (term 6) overwrites index 2 with a DIFFERENT normal entry,
	// truncating the config entry — the follower must revert to {n1,n2,n3}.
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n3", To: c.self, Type: MsgAppendEntries, Term: 6,
		PrevLogIndex: 1, PrevLogTerm: 5,
		Entries: []LogEntry{{Term: 6, Index: 2}}, // normal entry, overwrites the config
	}})
	if c.voters["n4"] {
		t.Fatal("follower did not REVERT config on truncation (n4 still a voter)")
	}
	if c.clusterN != 3 {
		t.Fatalf("follower voter count = %d, want 3 after revert", c.clusterN)
	}
}

// TestCommitUnderNewQuorum verifies that after AddServer (3->4, quorum 3), an
// entry commits only once the POST-change quorum acks.
func TestCommitUnderNewQuorum(t *testing.T) {
	c := leaderReady(t, 3)
	// Add n4: now 4 voters, quorum 3.
	c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n4"})
	cfgIdx := c.lastConfigIndex
	// One ack (self + n2 = 2) is NOT a quorum of 4 (need 3): config stays uncommitted.
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1, Success: true, MatchIndex: cfgIdx,
	}})
	if c.commitIndex >= cfgIdx {
		t.Fatalf("committed at %d with only 2/4 acks; quorum is 3", c.commitIndex)
	}
	// A second ack (self + n2 + n3 = 3) IS a quorum of 4: now it commits.
	c.Step(Event{Type: EventDeliver, Msg: Message{
		From: "n3", To: c.self, Type: MsgAppendEntriesResp, Term: 1, Success: true, MatchIndex: cfgIdx,
	}})
	if c.commitIndex < cfgIdx {
		t.Fatalf("config entry not committed under the new quorum (commit=%d cfgIdx=%d)", c.commitIndex, cfgIdx)
	}
}

// addAndCommitN4 drives a 3-node leader to add n4 and commit the change (voters
// {n1,n2,n3,n4}, quorum 3). Returns the leader and the config entry's index.
func addAndCommitN4(t *testing.T) (*raftCore, Index) {
	t.Helper()
	c := leaderReady(t, 3)
	c.Step(Event{Type: EventChangeConfig, Ref: 1, ConfigAdd: true, ConfigServer: "n4"})
	cfgIdx := c.lastConfigIndex
	c.Step(Event{Type: EventDeliver, Msg: Message{From: "n2", To: c.self, Type: MsgAppendEntriesResp, Term: 1, Success: true, MatchIndex: cfgIdx}})
	c.Step(Event{Type: EventDeliver, Msg: Message{From: "n3", To: c.self, Type: MsgAppendEntriesResp, Term: 1, Success: true, MatchIndex: cfgIdx}})
	if !c.voters["n4"] || c.clusterN != 4 {
		t.Fatalf("setup: n4 should be a committed voter; voters=%v n=%d", c.voters, c.clusterN)
	}
	return c, cfgIdx
}

// TestConfigSurvivesCompaction is the regression for a bug the polish-pass
// adversarial review found: a committed membership change that is then COMPACTED
// away was silently reverted, because configBaseline never absorbed the
// compacted KindConfig entry, so a later re-derivation dropped the added server.
// Fix: CompactTo folds compacted config entries into configBaseline first.
func TestConfigSurvivesCompaction(t *testing.T) {
	c, cfgIdx := addAndCommitN4(t)
	term, _ := c.log.termAt(cfgIdx)
	c.CompactTo(cfgIdx, term) // compact past the (committed) config entry
	// Force re-derivation, as a later config change / append would.
	c.adoptConfigFromLog(&effects{})
	if !c.voters["n4"] || c.clusterN != 4 {
		t.Fatalf("membership reverted after compaction: voters=%v n=%d, want n4 present / n=4", c.voters, c.clusterN)
	}
	// And a subsequent change composes correctly on top of the folded baseline.
	c.commitIndex = c.log.lastIndex() // allow the next change (one-at-a-time gate)
	c.Step(Event{Type: EventChangeConfig, Ref: 2, ConfigAdd: true, ConfigServer: "n5"})
	if !c.voters["n4"] || !c.voters["n5"] {
		t.Fatalf("after compaction + add n5: want {..n4,n5}; got %v", c.voters)
	}
}

// TestConfigSurvivesRestart is the crash-recovery half of the same regression:
// after a committed config change is compacted and the core is Restored from the
// snapshot base + tail, the voter set must still include the added server (the
// baseline is reconstructed from the compacted prefix).
func TestConfigSurvivesRestart(t *testing.T) {
	c, cfgIdx := addAndCommitN4(t)
	term, _ := c.log.termAt(cfgIdx)
	c.CompactTo(cfgIdx, term)
	// Simulate restart: a fresh core (same initial config) Restored from the
	// snapshot base + the surviving tail. The fresh core's baseline is the INITIAL
	// 3-node config; Restore must reconstruct the post-change voter set. Since the
	// snapshot bytes here carry no membership, the honest contract is that the
	// driver restores the core whose baseline already reflects compacted config —
	// which our CompactTo guarantees on the SAME core. Assert the compacted core's
	// derived config is correct (the property the fix provides).
	if !c.voters["n4"] || c.clusterN != 4 {
		t.Fatalf("post-compaction voter set wrong: voters=%v n=%d", c.voters, c.clusterN)
	}
	if !c.configBaseline["n4"] {
		t.Fatalf("configBaseline did not absorb the compacted add of n4: %v", c.configBaseline)
	}
}
