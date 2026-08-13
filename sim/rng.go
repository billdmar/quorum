// Package sim is the deterministic, seeded, fault-injecting driver of the pure
// Raft core (core.RaftCore). It is the beating heart of the verification
// program: same seed ⇒ byte-identical event trace (the trace-hash gate). The
// simulator is a single-threaded, quantized-time executor — there are NO
// goroutines in the core-driving path — so every interleaving is chosen by the
// seeded PRNG and nothing else. All nondeterminism the core reacts to (timer
// firing, message arrival, randomized election durations, client requests,
// crashes, disk faults) is manufactured HERE, from the RNG in this file.
package sim

// RNG is a deterministic pseudo-random generator seeded from a single uint64.
// It implements splitmix64 — a tiny, well-specified, architecture-independent
// generator — deliberately in preference to math/rand, whose stream is NOT
// guaranteed stable across Go versions. Stability is the whole point: a seed
// must reproduce an identical sequence forever, on any machine, for the
// trace-hash determinism gate to mean anything.
//
// RNG is NOT safe for concurrent use; the simulator drives it from one
// goroutine, matching the pure core's single-threaded contract.
type RNG struct {
	state uint64
}

// NewRNG returns an RNG seeded with seed. Every distinct seed yields an
// independent, reproducible stream.
func NewRNG(seed uint64) *RNG { return &RNG{state: seed} }

// next advances the splitmix64 state and returns the next 64-bit output. The
// three magic constants are the canonical splitmix64 values.
func (r *RNG) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Uint64 returns the next 64-bit pseudo-random value.
func (r *RNG) Uint64() uint64 { return r.next() }

// Intn returns a pseudo-random int in [0,n). It panics for n<=0, matching the
// math/rand contract callers expect. A simple modulo is used: the bias for the
// small n the simulator uses (cluster sizes, key counts) is negligible, and
// determinism — not statistical purity — is the requirement here.
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		panic("sim: Intn requires n > 0")
	}
	return int(r.next() % uint64(n))
}

// chance reports whether a parts-per-thousand probability event fires. ppt is
// integer parts-per-thousand (matching config.FaultParams, which are all ppt),
// so fault decisions use pure integer math with no floating point — that is
// what keeps trace hashes byte-identical across architectures.
//
// ppt==0 short-circuits to false WITHOUT consuming an RNG draw, and ppt>=1000
// short-circuits to true. This is deterministic per (seed, schedule): for a
// fixed schedule the ppt constants are fixed, so which calls consume the stream
// is itself fixed, and the same seed reproduces the same interleaving.
func (r *RNG) chance(ppt uint32) bool {
	if ppt == 0 {
		return false
	}
	if ppt >= 1000 {
		return true
	}
	return r.next()%1000 < uint64(ppt)
}

// between returns a pseudo-random value in the inclusive range [lo,hi]. It
// panics if hi<lo. Used for delay/reorder spans and partition/restart lifetimes.
func (r *RNG) between(lo, hi uint32) uint32 {
	if hi < lo {
		panic("sim: between requires hi >= lo")
	}
	return lo + uint32(r.next()%uint64(hi-lo+1))
}
