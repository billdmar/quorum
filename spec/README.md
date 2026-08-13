# TLA+ safety model (P6 stretch)

`raft.tla` + `raft.cfg` are a small, self-contained TLA+ model of the Raft
algorithm that the Go core (`core/`) implements, model-checked exhaustively by
TLC over a bounded state space.

## What it proves

TLC verifies the five safety invariants — the same set the Go invariant-monitors
check after every simulator step (`check/invariants.go`) — hold across **every
reachable state** of a 3-server model (terms ≤ 3, log length ≤ 3):

- **ElectionSafety** — at most one leader per term.
- **LogMatching** — same index+term ⇒ identical log prefix.
- **LeaderCompleteness** — a committed entry appears in every higher-term leader.
- **StateMachineSafety** — no two servers commit different entries at one index.
- **TypeOK** — structural well-formedness.

Result: **`Model checking completed. No error has been found.`** over ~322k
distinct states.

## Honest scope

This is a model of the **algorithm**, hand-written in TLA+ — it is **not**
mechanically extracted from the Go code, and it intentionally omits snapshots,
membership changes, and client sessions (it models the safety core: terms, votes,
logs, commit). It **complements** the deterministic simulation testing, which
checks the *actual Go implementation* across 140,000 fault schedules: DST gives
breadth over the real code + faults; TLC gives an exhaustive proof over the
algorithm's bounded state space. Two independent lenses on the same safety
properties.

## Run it

Requires Java + [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases/latest)
(TLC 2.19+):

```sh
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config raft.cfg raft.tla
```

`-deadlock` disables TLC's deadlock check: this is a *safety* model, and reaching
the bounded term/log ceiling with no further action enabled is expected (not a
liveness bug). All safety invariants are checked regardless.
