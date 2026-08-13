package core

// effects accumulates the Effects a single Step produces, in execution order.
// It exists so handler code reads declaratively (eb.persistHardState(...);
// eb.send(...)) while guaranteeing the persist-before-send ordering: callers
// always emit persistence effects before the sends that depend on them, and
// the slice preserves that order for the driver.
type effects struct {
	list []Effect
}

func (e *effects) send(m Message) {
	e.list = append(e.list, Effect{Type: EffectSend, Msg: m})
}

func (e *effects) persistHardState(hs HardState) {
	e.list = append(e.list, Effect{Type: EffectPersistHardState, HardState: hs})
}

func (e *effects) persistLog(from Index, entries []LogEntry) {
	e.list = append(e.list, Effect{Type: EffectPersistLog, FromIndex: from, Entries: entries})
}

func (e *effects) apply(committed []CommittedEntry) {
	e.list = append(e.list, Effect{Type: EffectApply, Committed: committed})
}

func (e *effects) resetElectionTimer() {
	e.list = append(e.list, Effect{Type: EffectResetElectionTimer})
}

func (e *effects) resetHeartbeatTimer() {
	e.list = append(e.list, Effect{Type: EffectResetHeartbeatTimer})
}

func (e *effects) rejectProposal(ref ClientRef, leaderHint NodeID) {
	e.list = append(e.list, Effect{Type: EffectRejectProposal, Ref: ref, LeaderHint: leaderHint})
}

func (e *effects) readIndexReady(ref ClientRef, readIndex Index) {
	e.list = append(e.list, Effect{Type: EffectReadIndexReady, Ref: ref, ReadIndex: readIndex})
}

// sendSnapshot tells the driver to build and transmit a MsgInstallSnapshot from
// `from` to peer `to` covering the log through (snapIndex, snapTerm). The driver
// attaches the application snapshot bytes; the core carries none.
func (e *effects) sendSnapshot(from, to NodeID, term Term, snapIndex Index, snapTerm Term) {
	e.list = append(e.list, Effect{
		Type:      EffectSendSnapshot,
		Msg:       Message{From: from, To: to, Type: MsgInstallSnapshot, Term: term},
		SnapIndex: snapIndex, SnapTerm: snapTerm,
	})
}

// installSnapshot tells the driver to durably persist a received snapshot and
// restore the application state machine from data, as one ordered step BEFORE
// the reply Send later in this batch.
func (e *effects) installSnapshot(snapIndex Index, snapTerm Term, data []byte) {
	e.list = append(e.list, Effect{
		Type: EffectInstallSnapshot, SnapIndex: snapIndex, SnapTerm: snapTerm, SnapData: data,
	})
}

// configChanged tells the driver the voting configuration changed; members is
// the new voter set so the driver can wire/unwire transport routes or sim nodes.
func (e *effects) configChanged(members []NodeID) {
	e.list = append(e.list, Effect{Type: EffectConfigChanged, Members: members})
}

// done returns the accumulated effects, never nil (an empty step yields an
// empty slice so callers need no nil check).
func (e *effects) done() []Effect {
	if e.list == nil {
		return []Effect{}
	}
	return e.list
}
