# quorum — design decisions log

Every non-obvious design decision, 2–4 lines, grounded in the code that exists.
Entries describe the *current* implementation; anything planned-but-not-built is
labelled **(planned Wn)**. This log is append-mostly: when a decision changes,
update the entry in the same commit as the code.

Status: – complete.  (verification),  (full verification gate), and 
(real-TCP production mode, §14) are all green;  is the published
README + design docs. This is the final state of the project.

---

## 1. The sans-I/O pure core

**Decision — the Raft core is a pure state machine; all nondeterminism enters as
Events, all side effects leave as Effects.** (`core/core.go`, `core/types.go`,
`core/statemachine.go`)
The core touches no time, no I/O, no goroutines, no randomness. `Step(Event)
[]Effect` is a pure function of in-memory state plus one event: identical state +
identical event ⇒ identical effect sequence + identical next state. Timer
firings, message arrivals, client requests, and crashes all arrive as `Event`s;
sends, persistence, applies, and timer arming all leave as `Effect`s a driver
executes.

**Why:** determinism and one-core-two-drivers. The same core is driven by the
deterministic simulator (for fault-injection + trace-hash testing) and by the
real-TCP production runtime (`node/`, §14). There are no side channels — the
`RaftCore` interface in `core/statemachine.go` is the entire contract — so a seed
reproduces a byte-identical event trace, which is what makes deterministic
simulation testing (DST) and the trace-hash gate possible. Any leak of
nondeterminism into the package is a build-breaking violation (`the project docs` hard
rule).

**Decision — peers are held in a sorted slice, never ranged over as a map, when
building effects.** (`core/core.go` `newCore`, `broadcastAppend`)
Go map iteration order is randomized; ranging a map to emit `EffectSend`s would
make effect ordering nondeterministic and break the trace hash. Peers are sorted
and de-duped at construction and iterated in that fixed order.

## 2. Timing enters as injected tick events, not a clock

**Decision — the core reacts to `EventTickElection` / `EventTickHeartbeat`; it
never reads a clock and never chooses a timeout duration.** (`core/types.go`
EventType docs; `core/core.go` `handleElectionTimeout`, `handleHeartbeatTimeout`;
`core/statemachine.go` Config)
Randomized election-timeout *bounds* live in `Config`
(`ElectionTimeoutMin/MaxTicks`, `HeartbeatTicks`) purely so a driver can consult
them when randomizing a real timer; the core itself ignores them for decisions.
When a timer fires, the driver delivers a tick event and the core reacts.

**Why:** randomized timeouts are the classic source of nondeterminism in a Raft
implementation. Pushing both the clock and the randomness into the driver keeps
the core pure: the simulator seeds timer durations for reproducibility, the
production runtime uses a real randomized timer, and the core's logic is
identical under both. `EffectResetElectionTimer` carries the "re-arm" signal
back out; the driver must cancel any previously-armed timer so no stale
`EventTickElection` is delivered (documented on `EffectResetElectionTimer`),
which keeps timer *generations* out of the core.

## 3. Persist-before-send effect ordering

**Decision — the driver executes effects in the exact order `Step` returns them,
and must complete every `EffectPersistHardState` / `EffectPersistLog` before any
later `EffectSend` in the same batch.** (`core/types.go` EffectType docs;
`storage/iface.go` fsync discipline)
The core emits persistence effects before the sends that depend on them; the
`RaftCore` contract and `Storage` interface together promise "persist actually
reached stable storage (fsync) before we tell anyone."

**Why:** Raft's safety proof assumes durability of term, vote, and log before a
node takes an externally-observable action. If a node granted a vote or appended
an entry, told a peer, then crashed and forgot, two leaders could be elected in
one term or a committed entry could vanish. Ordering persistence ahead of sends
at the effect level — and fsync-before-return at the storage level — closes both
halves of that gap.

## 4. Figure-8 commit rule and the leader no-op

**Decision — a leader marks an entry committed by replica-counting ONLY if that
entry is from the leader's current term; prior-term entries commit only
transitively.** (`core/core.go` `maybeAdvanceCommit`)
`maybeAdvanceCommit` scans down from `lastIndex` for the highest `N > commitIndex`
where a quorum has `matchIndex >= N` **and** `log[N].term == currentTerm`. Older
entries become committed only once a current-term entry above them reaches
quorum.

**Why (Raft Figure 8):** an entry replicated on a majority is not safe to commit
if it is from an earlier term — a later leader without it could still overwrite
it. Requiring a current-term entry above the commit point makes the commit
durable against future leaders.

**Decision — on winning election a leader immediately appends a no-op entry in
its current term.** (`core/core.go` `becomeLeader`; `core/types.go` `IsNoOp`)
A `LogEntry` with an empty `Command` is a leader no-op; application state
machines must ignore it (`IsNoOp`). `becomeLeader` appends it, persists it, and
tries to commit.
**Why:** a freshly elected leader may hold committed-but-not-yet-marked entries
from prior terms it cannot commit until it has a current-term entry to carry
them. The no-op supplies that current-term entry immediately, so the leader can
advance the commit index (and serve reads) without waiting for the first client
write.

## 5. Fast-backup conflict hint

**Decision — a rejected `AppendEntries` returns `ConflictIndex` + `ConflictTerm`
so the leader can skip a whole conflicting term in one round, instead of backing
up one index per round-trip.** (`core/handlers.go` `conflictHint`,
`backupNextIndex`; `core/types.go` Message fields)
On a consistency-check failure the follower reports either (a) the conflicting
term and the first index of that term, or (b) if it is simply too short, its next
index with term 0. The leader's `backupNextIndex` uses the hint: if it has
entries of `ConflictTerm` it jumps to just past its last entry of that term,
otherwise it falls back to the follower's first index of that term.

**Why:** the naïve decrement-by-one backup costs O(log-divergence) round-trips
after a partition or a long divergent tail — pathologically slow under the
partition/crash schedules. The term-skipping hint (the standard Raft
optimization) converges in O(number of divergent terms), which the
partition-heavy and crashy fault schedules exercise directly.

## 6. In-memory log structure

**Decision — the log is 1-based with index 0 = "before the first entry" (term
0), and `appendFromLeader` truncates only at a genuine term conflict.**
(`core/log.go`)
`raftLog` stores entries in a slice; `termAt`, `matches`, `sliceFrom`, and
`entryAt` respect a `snapBase`/`snapTerm` prefix for the compacted snapshot
range (§11a). `appendFromLeader` keeps already-matching entries as-is and
truncates the local log only at the first index where terms differ, then appends
the remainder.

**Why:** this is the log-matching property in code — never truncate a prefix the
leader did not contradict, so committed entries the leader still holds are
preserved and only genuinely divergent suffixes are overwritten. Slices returned
to effect payloads are freshly copied (`sliceFrom`, `clone`) so a driver can hold
them without aliasing the core's backing array — another purity guard.

## 7. Storage: WAL framing, atomic swaps, fsync discipline

**Decision — the `Storage` interface is frozen at ; every durability-promising
method fsyncs before returning, and the core never calls it directly.**
(`storage/iface.go`)
The core emits `EffectPersistHardState` / `EffectPersistLog`; a driver translates
those into `SaveHardState` / `AppendEntries(fromIndex, entries)`. `AppendEntries`
both truncates at `fromIndex` and appends, covering plain appends and
conflict-overwrites in one durable call.

**Decision — `LoadEntries` returns the longest valid prefix on a torn tail; hard
state and snapshots are written via atomic temp+rename.** (`storage/iface.go`
`LoadEntries` doc, `FaultKind`)
WAL records are framed with a CRC so a partially-written trailing record is
detected and discarded on recovery rather than returned as if complete
(`FaultTornWrite`). Fixed-size hard state and snapshot files are written to a temp
file and atomically renamed so a crash mid-write leaves either the old or the new
file, never a half-written one.
**Why:** the crash/disk-fault schedules inject exactly these boundaries —
`FaultCrashBeforeFsync` (last write may be lost, tolerated), `FaultCrashAfterFsync`
(must survive), `FaultTornWrite` (torn tail must be dropped). CRC-framed
append-only WAL + atomic-rename for the small files is the minimal design that
satisfies all three without a heavyweight on-disk format. (WAL implementation is
SA-storage's  deliverable against this frozen contract.)

## 8. Fault-schedule registry

**Decision — fault rates are integer parts-per-thousand (ppt), never floats.**
(`config/registry.go` `FaultParams`)
Every fault decision (drop, dup, reorder, partition onset, crash, disk fault) is
a ppt comparison against a seeded integer PRNG.
**Why:** floating-point arithmetic can differ across architectures (dev = Apple
Silicon, CI = ubuntu-latest x86-64), which would break the "same seed ⇒ identical
trace hash" gate. Integer ppt is exact and portable.

**Decision — the registry is never weakened to turn a red run green.**
(`config/registry.go` header; `the project docs` Never-do)
Schedules, invariants, seed floors, and bounds live in one file with written
justifications; a violation means fix the bug and commit the failing seed as a
regression, not relax a bound. Changes go through the  only.

**Named schedules** (`config/registry.go`):
- `clean` — no faults; validates the happy path and proves the
  determinism/trace-hash + history-capture machinery is sound before faults are
  layered on.
- `lossy` — 15% drop / 5% dup / 20% reorder (≤5-tick delay); stresses retransmit,
  duplicate-RPC idempotency, election liveness under loss. Drop stays below the
  ~33% that would starve a 3-node majority so runs still converge.
- `partition-heavy` — 4%/tick symmetric partition onset, ≤30-tick span; exercises
  election across majority/minority splits, minority log divergence, and
  reconciliation on heal. Bounded span so partitions always heal.
- `asymmetric` — same rates, one-directional cuts; exposes the stuck-leader bug
  symmetric partitions hide (can send heartbeats, can't hear it lost quorum) —
  motivates CheckQuorum-style reasoning and the  ReadIndex heartbeat round-trip.
- `crashy` — 3%/tick per-node crash, restart ≤25 ticks, mild loss; stresses WAL
  replay, term/vote/log durability, and rejoin+catch-up amid live traffic.
- `disk-faulty` **()** — 10% of durable writes hit an fsync-boundary fault;
  targets corruption-tail handling and persist-before-send.
- `kitchen-sink` **()** — all fault classes composed, each pulled slightly below
  its single-class peak so the composition still makes progress rather than
  degenerating into permanent unavailability (which would test harness liveness,
  not Raft safety).

**Decision — two matrices, two seed floors, odd cluster sizes only.**
(`config/registry.go` `BaseMatrix`, `FullMatrix`, `SeedFloorG1//CI`,
`ClusterSizes`)
 base matrix = clean/lossy/partition-heavy/asymmetric/crashy, floor **1,000**
seeds;  full matrix adds disk-faulty + kitchen-sink, floor **10,000** seeds. CI
runs a bounded **200**-seed sweep on every push (long sweeps run in extended runs as
background jobs, per `docs/ENV.md`). Cluster sizes are {3, 5}: even sizes gain no
fault tolerance over the next-lower odd size and complicate quorum reasoning.
**Why the floors:** 's 1k is large enough for rare interleavings to surface yet
small enough to re-run repeatedly during core development; 's 10k is an
order-of-magnitude more coverage for the higher-complexity system (snapshots,
ReadIndex, sessions). Floors are floors — never lowered to pass.

## 9. Safety invariants and linearizability

**Decision — five Raft safety invariants are monitored after every simulator
step; zero violations is the bar at every gate.** (`check/history.go`
`InvariantID`; `config/registry.go` `RegisteredInvariants`)
- **election-safety** — at most one leader per term across the cluster.
- **log-matching** — same (index, term) in two logs ⇒ identical logs up through
  that index.
- **leader-completeness** — an entry committed in a term is present in the logs
  of all leaders of all higher terms.
- **state-machine-safety** — no two nodes apply different commands at the same log
  index.
- **commit-monotonicity** — commit index never decreases; applied never exceeds
  commit.

Monitors read a `ClusterView` / `NodeView` snapshot assembled from the cores'
read-only accessors (`Role`, `Term`, `CommitIndex`, `LogView`, applied entries),
so checking never perturbs core state. A `Violation` carries seed + schedule +
step + detail so any failure replays exactly and becomes a committed regression
test. (Monitors are SA-check's  deliverable against this frozen format.)

**Decision — linearizability is checked by Porcupine as an external oracle over a
recorded history; the model is not hand-rolled.** (`check/history.go` package
doc, `HistoryEvent`, `History`)
The simulator records one `StageInvoke` and one `StageResponse` per client op on
a deterministic logical clock (`Stamp` — not wall-clock, so histories replay from
a seed). Porcupine searches for a real-time-respecting total order consistent with
the KV model's sequential semantics.
**Why external oracle:** an independently-authored checker (github.com/anishathalye/
porcupine, the one dependency beyond stdlib) is far more trustworthy than a
bespoke correctness argument, and is the industry-standard way to verify
linearizability.

**Decision — an operation whose outcome the client never learned is marked
`Unknown`, not guessed.** (`check/history.go` `HistoryEvent.Unknown`)
When a request times out amid a leader change, the driver sets `Unknown` so
Porcupine treats the op as "may or may not have taken effect" rather than
fabricating a definite outcome. (Mirrors the the project docs "honest unknown over
fabricated plausible value" rule.)

## 10. Client sessions and exactly-once **(, kv layer)**

**Decision — client sessions + per-request dedup give exactly-once semantics
across retries and leader changes.** (`kv/store.go`)
Each `kv.Command` carries a `ClientID` and a monotonically increasing `SeqNum`.
`Store.Apply` keeps a per-client session `{lastSeq, lastResult}`: a command whose
`SeqNum <= lastSeq` is a duplicate (a retry, or a re-delivery after a leader
change) and returns the CACHED result WITHOUT re-applying — so a command that
commits, whose client times out and re-proposes the identical bytes to a new
leader, applies exactly once. The dedup table is included in `Store.Snapshot`/
`Restore` (§7), so exactly-once survives log compaction and snapshot install —
otherwise a post-snapshot retry would re-apply. The workload re-proposes the
identical command bytes on every retry, and the linearizability check against the
KV model is what certifies the property end-to-end across the seed sweep.

**Register-CAS semantics (model alignment).** CAS treats an absent key as the
empty value: `CAS(compare="")` on an absent key succeeds and creates it. This
makes CAS a total function of the current value with no separate not-found case,
and — critically — it is the SAME definition the Porcupine model checks against.
The  sweep caught an earlier mismatch (store required the key to exist; model
did not) as a spurious non-linearizable verdict; aligning both to register-CAS
fixed it. Model and state machine MUST agree exactly or a correct history reads
as non-linearizable.

## 11. ReadIndex reads **()**

**Decision — heartbeat-confirmed ReadIndex gives linearizable reads without a
disk write per read.** (`core/core.go` `handleReadIndex`/`confirmPending`/
`releaseReads`; `core/types.go` `Message.ReadSeq`)
On `EventReadIndex` a leader captures `readIndex = max(commitIndex, noopIndex)`,
enqueues the read against a fresh confirmation round, and broadcasts a heartbeat
stamped with that round's `ReadSeq`. Followers echo `ReadSeq` on their
AppendEntriesResp; once a quorum has echoed a round `>=` the read's round
(leadership confirmed as of that round) AND the state machine has applied through
`readIndex`, the read is served (`EffectReadIndexReady`) and the driver answers
from the applied KV state. Pending reads live in a SLICE drained in insertion
order (never a map — that would leak Go map-iteration order into the effect
stream and break the trace-hash determinism gate).

**Why `max(commitIndex, noopIndex)`:** the leader no-op committed on election is
the current-term-committed precondition; reading at or above it guarantees, by
Leader Completeness, that the read reflects every previously-committed write even
if this leader has not yet learned its full prior-term commit index.

**Why the confirmation round closes the asymmetric-partition stale read:** a
leader silently partitioned out (the `asymmetric` schedule — it can still SEND
heartbeats but no longer RECEIVE responses) cannot gather a fresh-round ack
quorum, so its pending reads never confirm and it never serves a stale value. A
pre-round (stale) ack is explicitly not counted. On step-down the leader rejects
all pending reads so the client retries the new leader. (`readindex_test.go`
pins each of these.)

## 11a. Snapshots, log compaction, and InstallSnapshot **()**

**Decision — the driver owns WHEN to snapshot; the core owns the log rebasing.**
(`core/log.go` `compactTo`/`installSnapshot`; `core/core.go` `CompactTo`/
`Restore`; `core/handlers.go` `handleInstallSnapshot`)
Deciding when to snapshot requires reading state size — impure — so the driver
triggers it: after applying, if `appliedIndex - SnapBase >= LogCompactionThreshold`
(a pure integer test, `config/registry.go`) it snapshots the KV store, durably
saves+compacts storage, and calls `core.CompactTo` to advance the in-memory log
base. A leader whose peer has fallen BELOW its compacted prefix can no longer
send AppendEntries for the missing entries (the earlier code clamped to
`firstIndex` and livelocked); instead `sendAppendTo` emits `EffectSendSnapshot`
and the driver ships the KV snapshot bytes as `MsgInstallSnapshot` (the core
stays byte-free). A follower installing a snapshot retains a matching tail (Raft
§7) or discards a conflicting log, seeds `commitIndex=appliedTo=LastIncludedIndex`,
and persists+restores before replying (persist-before-send). Restart-from-snapshot
loads snapshot+tail and `Restore` rebases the core on the snapshot base. The
monitors scope log-matching and leader-completeness to indices above
`max(SnapBase)` so a legitimately compacted prefix is not read as a divergence
(the  analogue of the  Incarnation fix).

## 12. Deferred and known limitations (honest framing)

- **Single-server membership changes — DONE (P6 stretch, §15d).** The pure core
  carries a mutable voting configuration; add/remove one server at a time
  (dissertation §4.1, no joint consensus). Verified by core unit tests + live
  real-TCP add/remove tests, but NOT folded into the 140k sweep — that boundary is
  stated in §15d. (Joint-consensus multi-server changes remain out of scope.)
- **Snapshots / log compaction — DONE (, §11a).** Compaction trigger,
  InstallSnapshot transfer to a lagging follower, and restart-from-snapshot are
  implemented and exercised by the  sweep (`InstallSnapshot` catch-up is
  asserted to actually fire, not dead code — `tests/integration/exactlyonce_test.go`).
- **ReadIndex heartbeat confirmation — DONE (, §11).**
- **Client sessions / exactly-once — DONE (, §10).**
- **No production-scale hardening claimed.** This is a correctness-first,
  single-machine/loopback system. Any performance number will come only from a
  release build on a quiesced machine and be labelled single-machine/loopback
  (`the project docs`); nothing here implies production-scale operation.

## 12a. What the  seed sweep found (DST payoff)

The iron-gate sweep (1,000 seeds × base matrix × {3,5} nodes = 10,000 verified
runs) found two defects — both in the *monitors*, not in Raft, and both
surfaced only under fault schedules. Each was fixed by making the monitor
enforce the invariant's true scope (never by weakening a bound), and each is
pinned by a committed regression (`check/incarnation_test.go`,
`tests/integration/regression_test.go`).

1. **commit-monotonicity false positive under `crashy`.** `commitIndex` is
   *volatile* Raft state (Figure 2) that resets to 0 on restart; the stateful
   monitor carried a node's pre-crash high across the crash boundary and flagged
   the legitimate reset. Fix: `NodeView.Incarnation`; the monitor rebaselines
   across an incarnation bump and enforces monotonicity only *within* an
   incarnation. Every mis-flagged run was linearizable with no state-machine
   violation — the DST signature of a harness bug.
2. **leader-completeness false positive under `partition-heavy` (seed 507, 1 in
   10,000).** A deposed term-4 leader, partition-isolated and unaware of the
   term-5 leader, coexisted with the true leader. The contested entry was
   *committed in term 5* (Figure-8), not its creation term 1, so the stale
   sub-max-term leader lacking it is legal. A single snapshot cannot observe an
   entry's commit term, so keying the check on creation term was unsound. Fix:
   check only the max-term leader (a sub-max-term leader is provably superseded).

## 12b. Kitchen-sink / disk-faulty calibration — RESOLVED at 

The  base matrix passed cleanly, but the  full-matrix `kitchen-sink` and
`disk-faulty` schedules were adversarial enough that at n=5 with a 40-op budget
only 0–5 ops committed within `MaxTicks`, leaving Porcupine unable to decide.
Resolved WITHOUT weakening any fault rate: `config.ScheduleBudgets` gives those
two schedules a larger tick ceiling and a right-sized op count so the operations
that do get through form a short, fully-resolved, decidable history. The
disk-fault MODEL was also right-sized (torn writes only where the storage layer
can actually tear — the appendable log — with atomic hard-state/snapshot writes
degrading a torn draw to a clean crash), so a node makes progress between faults
instead of crashing on essentially every durable write. The fault rates in
`Schedules` are untouched.

## 12c. What the  seed sweep found (DST payoff, round two)

The  work found four defects (the last, #4, surfaced only on the full 140,000-run
sweep; the first three during development) — none in Raft safety (zero invariant
violations throughout), all fixed soundly with committed regressions. Together with
the two from  (§12a) these are **six defects in the DST harness/monitors** (none
in Raft); a seventh — a real Raft-logic bug in the P6 membership feature — was
later found by adversarial code review (§15d). Every one of the six here is in the
harness or a monitor rather than in Raft:

1. **Monitors false-positived on a compacted prefix.** After a node snapshots,
   its log begins above index 1; log-matching / leader-completeness compared from
   index 1 and read the absent prefix as divergence. Fix: scope both to indices
   above `max(SnapBase)` (the  analogue of the  Incarnation fix).
2. **CAS ≠ model on an absent key.** The store required the key to exist; the
   Porcupine model treated absent as `""`. A correct history read as
   non-linearizable. Fix: align both to register-CAS.
3. **Multi-entry apply batches misattributed KV outputs.** A batch commit applied
   several entries but the driver recorded only the last result for all — outputs
   scrambled under retries/loss. Fix: `apply()` returns per-entry results.
4. **All-Unknown history = Porcupine's worst case (seed 7503, 1 in 140,000).**
   `kitchen-sink` so harsh that 0/20 ops committed; an all-Unknown history is
   maximally concurrent, so the checker ran 48 s to *Undetermined*. Fix: a SOUND
   short-circuit — a history with no definite operation outcome is trivially
   linearizable (nothing constrains an ordering), so it never reaches Porcupine.
   Not a weakening: a real violation needs a definite result, still fully checked.
   The re-sweep dropped from 2853 s to 357 s, confirming the degenerate cases were
   the whole cost.

## 12d. Honest benchmarks (single-machine, in-process)

Recorded on a quiesced Apple M4, release build. Two families:

**(a) Component microbenchmarks** (`go test ./bench/ -bench . -benchmem`) — the
pure core's event-processing and the KV state machine in isolation:

| Benchmark | ns/op | allocs/op |
|---|---|---|
| Core propose (single-node hot path) | ~230 | 4 |
| Core AppendEntries (follower) | ~380 | 6 |
| KV apply (Put, with dedup) | ~63 | 2 |
| KV snapshot (16 / 256 / 4096 keys) | ~0.6µs / ~6µs / ~6µs | 7 / 10 / 10 |

**(b) End-to-end cluster throughput** (`go test ./bench/ -bench Throughput -run '^$'`)
— a real in-process 3-node cluster over loopback TCP (real transport, real
timers), N concurrent clients issuing Puts to the leader; the full path
client→leader→replicate-to-quorum→commit→apply. **UNBATCHED** (each proposal is
its own Raft round) and single-machine/loopback:

| Concurrency | ops/sec | p50 | p99 |
|---|---|---|---|
| 1 client | ~10.5k | 0.065 ms | 0.42 ms |
| 8 clients | ~88k | <0.01 ms | 1.64 ms |
| 32 clients | ~117k | 0.05 ms | 4.81 ms |

Throughput scales ~11× from 1→32 clients as independent client ops pipeline
through the leader; latency tail grows with queue depth, as expected. Batching
client requests into a single Raft round would raise throughput further but is
deliberately not implemented — the numbers above are what the current code does,
not an aspiration.

A calibration lesson worth recording: the propose benchmark first read ~186µs/op
with ~2MB/op because the naive version proposed to a leader whose followers never
acked, so every broadcast re-copied an unboundedly-growing log — an O(n²)
*benchmark artifact*, not a real cost. Measuring the single-node hot path (no
replication parallelism) gives the honest ~230 ns/op. Publishing the 186µs number
would have been a fabricated-plausible-value defect.

## 13. Dependencies

**Decision — standard library + Porcupine only** (`go.mod`, `docs/ENV.md`).
The one runtime dependency beyond stdlib is
`github.com/anishathalye/porcupine` (linearizability oracle — see §9);
golangci-lint is a dev/CI tool, not a runtime dep. Any further dependency
requires a written justification here (`the project docs` hard rule). Module pinned to
`go 1.23` for CI portability; dev toolchain snapshot in `docs/ENV.md`.

## 14. Production mode: real-TCP runtime + CLI **()**

**Decision — the production runtime is the SECOND driver of the same pure core,
with all concurrency in the driver and none in the core.** (`node/runtime.go`,
`rpc/transport.go`, `cmd/quorum`)
`node.Runtime` drives one `core.RaftCore` with real time, a real TCP transport
(`rpc.Transport`), and a `kv.Store` application state machine. **Race-cleanliness
by design:** exactly ONE goroutine — the core loop — ever calls `core.Step` or
touches the core; timer fires, inbound messages, client proposals, and reads are
all funneled to it over channels, so the pure core needs no locks. Observable
state is republished to a mutex-guarded `Status` snapshot after every step for
race-safe reads by the CLI/tests. `go test -race` is clean across the runtime,
and a  robustness sweep ran the cluster/leader-kill scenarios 60× under `-race`
with zero races or failures.

**Effect execution honors persist-before-send for free:** the loop executes a
step's effects in order on one goroutine, so a `PersistHardState`/`PersistLog`
(blocking on fsync via storage) always completes before a later `Send` in the
same batch. The  snapshot effects are wired the same way: `EffectSendSnapshot`
ships the `kv.Store` snapshot bytes; `EffectInstallSnapshot` persists+restores
before the reply Send. `Recover` rebuilds core+kv from the durable snapshot+log
tail on startup.

**Linearizable reads over the wire:** `Runtime.Read` submits `EventReadIndex` and
blocks until the core confirms leadership (heartbeat quorum) and the state
machine has applied through the read index, then answers from the `kv.Store` —
the same ReadIndex path (§11), now client-facing.

**CLI (`cmd/quorum`):** `quorum server` wires a transport to a runtime for one
cluster node with a client-facing line-framed JSON endpoint (`rpc/client.go`,
deliberately simple — a demo path, not the trace-hash-determinism domain);
`quorum kv put/get/append/cas` submits sessioned operations and follows a leader
redirect. `scripts/demo.sh` is the one-command headline demo: a 3-node loopback
cluster serves a write, a node is killed, the cluster re-elects, and the value
survives — verified live.

**Parity (sim ↔ real):** the two drivers cannot be compared by trace hash (the
runtime is genuinely time- and network-driven), so parity is BEHAVIORAL and
proven directly: `node/parity_test.go` feeds an identical proposal sequence
(including a dedup case and a CAS) to the Runtime and to a directly-driven bare
core, and asserts byte-identical resulting KV state — i.e. the runtime is a
faithful driver of the same core the simulator validates at scale.

## 15. P6 stretch goals (built; verification boundaries stated honestly)

These are stretch features beyond the gated – mission. Each is real code with
its own tests, but — critically — **none is integrated into the primary 140,000-run
 sweep**, and none disturbs it (they are additive; the membership item is
unreachable from existing workloads). The gate record stands unchanged.

### 15a. TLA+ safety model — `spec/`
A hand-written TLA+ model of the Raft algorithm, model-checked exhaustively by TLC
over a bounded 3-server state space, verifying the same five safety invariants as
`check/invariants.go` with zero violations across ~322k states. It is a model of
the *algorithm*, not extracted from the Go — an exhaustive complement to DST's
breadth over the real implementation. See `spec/README.md`.

### 15b. Multi-raft sharding sketch — `shard/`
Partitions the keyspace across N independent Raft groups via a deterministic
`key mod N` router; each group is a real cluster built from the gate-verified
`node`/`rpc` stack. The test proves group independence (a write to one shard does
not move another's log). Honestly a *sketch*: no placement manager, split/merge,
cross-shard transactions, or transport multiplexing. See `shard/README.md`.

### 15c. Lease-based reads + clock-skew analysis — `node/lease.go`
A lower-latency alternative to ReadIndex. Once a ReadIndex round confirms
leadership, that confirmation is trusted for a bounded window during which reads
are served locally with no round-trip.

**Purity:** the pure core reads no clock, so the lease lives entirely in the
driver; the core still runs the ReadIndex round that *grants* the lease.

**Clock-skew safety argument.** Let `lease_duration = ElectionMin - margin`. A
follower resets its election timer on each AppendEntries and will not start an
election — nor grant a competing vote — for at least `ElectionMin` after last
hearing from the leader, *measured on the follower's clock*. So if this leader
confirmed leadership at real time `t0`, no new leader can be elected before
`t0 + ElectionMin` on the followers' clocks. This leader serves lease reads only
until `t0 + lease_duration` on *its own* clock. The read is safe **iff** the
relative clock drift between this leader and every follower over the window is
less than `margin`. If real skew ever exceeds `margin`, a lease read could return
stale data (a newer leader committed a write this node hasn't seen). This is the
lease trade-off: lower latency for a **bounded-clock-skew assumption**, versus
ReadIndex which is assumption-free but pays a round-trip. quorum defaults `margin`
conservatively to `ElectionMin/4` and keeps ReadIndex the default; lease reads are
opt-in (`Runtime.LeaseRead`). Any loss of leadership revokes the lease immediately
(`publishStatus`), and the fast path re-checks leader role on the core loop to
close the step-down race. **Not linearizable under adversarial clock skew** — that
limitation is the whole point of the analysis, stated plainly.

### 15d. Single-server membership changes — `core/config_change.go`
**Implemented** (single-server add/remove per Raft dissertation §4.1, no joint
consensus). The pure core now carries a **mutable voting configuration** (`voters`
set + a `recomputeQuorum` that every quorum test already routes through) instead
of a frozen `peers`/`quorum`; `newCore` seeds it from `Config.Peers`.

- **Config-effective-on-append.** A membership change is a `LogEntry` with
  `Kind == KindConfig` carrying a small deterministic payload (add/remove + target
  `NodeID`). The core adopts the new configuration the moment the entry is
  *appended* — not on commit — which is what makes single-server changes safe
  without joint consensus.
- **One-at-a-time + current-term gate.** `EventChangeConfig` is rejected unless
  the previous config entry is already committed AND an entry from the current
  term has committed (Ongaro's corrected precondition). This is the safety
  lynchpin: it prevents two overlapping config changes whose quorums don't
  intersect.
- **Revert-on-truncation.** A follower re-derives its live configuration from the
  highest config entry surviving in its log after every append/overwrite, so an
  uncommitted config entry that is truncated cleanly reverts the voter set (the
  log is the single source of truth for configuration).
- **Compaction-safe (fixed post-review).** Because config is re-derived from the
  log, a *committed* config entry that is later compacted away must not vanish
  from the derived voter set. `CompactTo` (and the follower InstallSnapshot path)
  therefore fold every compacted `KindConfig` entry into a `configBaseline` before
  dropping it, so re-derivation always reflects committed membership. An adversarial
  code review found this exact bug (compaction silently reverted a committed
  membership change); it is fixed and pinned by `TestConfigSurvivesCompaction` /
  `TestConfigSurvivesRestart`. This is the seventh defect the verification process
  caught (§12c) — and the only one in Raft logic rather than the harness.
- **Quorum counts voters only.** Vote tallies and the match-index commit quorum
  count the current voter set; a node can commit under the *new* quorum
  immediately on append.
- **Effect boundary.** The core emits `EffectConfigChanged` with the new member
  set; the driver wires/unwires transport routes. Purity holds — the core reads
  no clock and interprets no `Normal` command; `kv.Apply` skips `KindConfig`
  entries so a config payload is never misread as a KV op.

**Honest verification boundary.** Verified by (a) core unit tests
(`core/config_change_test.go`) for quorum recompute on add/remove, the
one-at-a-time/current-term rejection, follower adopt-on-append + revert-on-
truncate, and commit under the post-change quorum; and (b) a live real-TCP
integration test (`node/membership_test.go`) that adds a 4th node to a running
3-node cluster, confirms it catches up and serves a linearizable read, then
removes a node and confirms the cluster keeps committing — `-race` clean. It is
**NOT** folded into the primary 140,000-run  sweep (which uses a fixed node
set); that integration — dynamic node creation/destruction in the simulator, a
membership-issuing workload, and membership-aware monitors — is a larger effort
left as future work. The 140k record is **preserved and re-verified byte-identical**
precisely because the membership code paths are unreachable from existing
workloads: `EventChangeConfig` is never emitted by them, `LogEntry.Kind` defaults
to `KindNormal` (zero) for every existing entry, and the new event/effect enum
values are append-only — so no existing trace hash changes. A production
implementation would add a learner (non-voting) catch-up phase before promoting a
new voter to minimize the availability dip; the change here adds voters directly,
which is *safe* (existing voters still form a quorum) but can briefly reduce
availability — noted, not hidden.
