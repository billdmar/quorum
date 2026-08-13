package core

import "testing"

// TestStringers exercises the String() methods on the enum types. They exist for
// trace output and diagnostics (the simulator's trace hash and violation reports
// read them), so a smoke test that every variant renders a non-empty,
// non-"unknown" label for a known value keeps them honest.
func TestStringers(t *testing.T) {
	roles := []Role{Follower, Candidate, Leader}
	for _, r := range roles {
		if s := r.String(); s == "" || s == "unknown" {
			t.Errorf("Role(%d).String() = %q", r, s)
		}
	}
	if Role(99).String() != "unknown" {
		t.Error("unknown role should render \"unknown\"")
	}

	msgs := []MessageType{MsgRequestVote, MsgRequestVoteResp, MsgAppendEntries,
		MsgAppendEntriesResp, MsgInstallSnapshot, MsgInstallSnapshotResp}
	for _, m := range msgs {
		if s := m.String(); s == "" || s == "unknown" {
			t.Errorf("MessageType(%d).String() = %q", m, s)
		}
	}
	if MessageType(99).String() != "unknown" {
		t.Error("unknown message type should render \"unknown\"")
	}

	evs := []EventType{EventTickElection, EventTickHeartbeat, EventDeliver, EventPropose, EventReadIndex}
	for _, e := range evs {
		if s := e.String(); s == "" || s == "unknown" {
			t.Errorf("EventType(%d).String() = %q", e, s)
		}
	}
	if EventType(99).String() != "unknown" {
		t.Error("unknown event type should render \"unknown\"")
	}

	effs := []EffectType{EffectSend, EffectPersistHardState, EffectPersistLog, EffectApply,
		EffectResetElectionTimer, EffectResetHeartbeatTimer, EffectRejectProposal,
		EffectReadIndexReady, EffectSendSnapshot, EffectInstallSnapshot}
	for _, e := range effs {
		if s := e.String(); s == "" || s == "unknown" {
			t.Errorf("EffectType(%d).String() = %q", e, s)
		}
	}
	if EffectType(99).String() != "unknown" {
		t.Error("unknown effect type should render \"unknown\"")
	}
}

// TestIDAccessor verifies the ID accessor returns the configured node id.
func TestIDAccessor(t *testing.T) {
	c := mkCore(3)
	if c.ID() != "n1" {
		t.Errorf("ID() = %q, want n1", c.ID())
	}
}
