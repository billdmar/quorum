package node

import (
	"sync"
	"time"

	"github.com/billdmar/quorum/core"
)

// leaderRole aliases core.Leader for readability in the lease fast-path guard.
const leaderRole = core.Leader

// Lease-based reads (P6 stretch) — a lower-latency alternative to the
// heartbeat-confirmed ReadIndex path (Runtime.Read). The idea: once a leader has
// confirmed its leadership via a successful ReadIndex round (a quorum of
// followers acknowledged its current-term heartbeat), that confirmation is good
// for a bounded window — because no follower will grant a competing vote until
// its own election timeout elapses. Within that window the leader may serve
// reads LOCALLY from its state machine with no further round-trip.
//
// PURITY: the pure core reads no clock, so the lease lives ENTIRELY in the driver
// (this file). The core's contribution is unchanged: it still runs the ReadIndex
// round that GRANTS the lease. The lease only shortcuts SUBSEQUENT reads.
//
// SAFETY (clock-skew analysis — see docs/DESIGN.md §15):
//
//	lease_duration = ElectionMin - safetyMargin
//
// A follower resets its election timer on every AppendEntries from the leader
// and will not start an election (nor grant a vote to a new candidate) for at
// least ElectionMin after last hearing from the leader. So if this leader
// confirmed leadership at real time t0, no new leader can be elected before
// t0 + ElectionMin as measured on the *followers'* clocks. This leader serves
// lease reads only until t0 + ElectionMin - safetyMargin on its OWN clock. The
// margin must exceed the maximum relative clock drift between this leader and any
// follower over the lease window; if real clock skew ever exceeds the margin, a
// lease read could in principle return stale data (a new leader committed a write
// this node hasn't seen). This is the fundamental lease-read trade-off: lower
// latency in exchange for a bounded-clock-skew ASSUMPTION, versus ReadIndex which
// is assumption-free but pays a round-trip. quorum defaults to a conservative
// margin and, being a correctness-first project, uses ReadIndex by default;
// lease reads are opt-in via LeaseRead.
type readLease struct {
	mu    sync.Mutex
	until time.Time // real-time instant the current lease expires (zero = none)
}

// grant (re)arms the lease to expire `d` from now. Called after a successful
// ReadIndex round confirms leadership.
func (l *readLease) grant(now time.Time, d time.Duration) {
	l.mu.Lock()
	l.until = now.Add(d)
	l.mu.Unlock()
}

// revoke clears the lease (called on any loss/ambiguity of leadership).
func (l *readLease) revoke() {
	l.mu.Lock()
	l.until = time.Time{}
	l.mu.Unlock()
}

// valid reports whether the lease is currently held (now is before expiry).
func (l *readLease) valid(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.until.IsZero() && now.Before(l.until)
}

// leaseDuration is how long a confirmed leadership round is trusted for lease
// reads: the minimum election timeout minus a safety margin covering clock skew.
// Derived from the runtime's own Config so it always tracks the election timing.
func (r *Runtime) leaseDuration() time.Duration {
	d := r.cfg.ElectionMin - r.leaseSafetyMargin()
	if d < 0 {
		return 0 // margin swamps the timeout ⇒ never serve a lease read
	}
	return d
}

// leaseSafetyMargin is the clock-skew guard. Conservative default: a quarter of
// the minimum election timeout. A deployment with tighter clock sync (e.g. PTP)
// could shrink it for lower-latency reads; a looser one must grow it or disable
// lease reads. Kept simple and conservative here (correctness-first).
func (r *Runtime) leaseSafetyMargin() time.Duration {
	return r.cfg.ElectionMin / 4
}

// LeaseRead serves a read of key using the leader read-lease fast path when a
// valid lease is held, falling back to the assumption-free linearizable Read
// (ReadIndex) otherwise. On the fast path it reads the local KV state machine
// directly (no round-trip); the returned ReadResult.Served reflects whether this
// node answered. A lease read is only as linearizable as the clock-skew
// assumption in leaseSafetyMargin holds (see the file header + DESIGN §15).
//
// Every successful linearizable Read (which runs a real ReadIndex round) also
// (re)grants the lease, so a Read followed by LeaseReads amortizes the round-trip
// across the lease window.
func (r *Runtime) LeaseRead(key string) ReadResult {
	if r.lease.valid(time.Now()) && r.Status().Role == leaderRole {
		// Fast path: serve locally. We funnel through the core loop's read of the
		// kv.Store to keep kv single-goroutine (never touch kv from this caller).
		return r.localLeaseRead(key)
	}
	// Slow path: a full ReadIndex round; on success it grants a fresh lease.
	res := r.Read(key)
	if res.Served {
		r.lease.grant(time.Now(), r.leaseDuration())
	}
	return res
}

// localLeaseRead reads key from the kv.Store on the core loop (so kv stays
// single-goroutine) WITHOUT a ReadIndex round, valid only under a held lease.
func (r *Runtime) localLeaseRead(key string) ReadResult {
	res := make(chan ReadResult, 1)
	select {
	case r.leaseReads <- readReq{key: key, result: res}:
	case <-r.ctx.Done():
		return ReadResult{}
	}
	select {
	case out := <-res:
		return out
	case <-r.ctx.Done():
		return ReadResult{}
	}
}
