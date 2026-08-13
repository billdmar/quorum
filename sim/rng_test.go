package sim

import "testing"

// TestRNGDeterministic: the same seed yields the same stream; determinism is the
// entire reason for a hand-rolled splitmix64 instead of math/rand.
func TestRNGDeterministic(t *testing.T) {
	a, b := NewRNG(42), NewRNG(42)
	for i := 0; i < 1000; i++ {
		if x, y := a.Uint64(), b.Uint64(); x != y {
			t.Fatalf("same-seed streams diverge at %d: %d != %d", i, x, y)
		}
	}
}

// TestRNGSeedIndependence: different seeds produce different streams (the first
// draw already differs for these seeds).
func TestRNGSeedIndependence(t *testing.T) {
	if NewRNG(1).Uint64() == NewRNG(2).Uint64() {
		t.Fatal("distinct seeds produced identical first draw")
	}
}

// TestChancePPTBounds: ppt==0 is never true and consumes no draw; ppt>=1000 is
// always true and consumes no draw; a mid value fires at roughly its rate.
func TestChancePPTBounds(t *testing.T) {
	r := NewRNG(9)
	before := *r
	for i := 0; i < 100; i++ {
		if r.chance(0) {
			t.Fatal("chance(0) returned true")
		}
	}
	if *r != before {
		t.Fatal("chance(0) consumed a draw")
	}
	for i := 0; i < 100; i++ {
		if !r.chance(1000) {
			t.Fatal("chance(1000) returned false")
		}
	}
	if *r != before {
		t.Fatal("chance(1000) consumed a draw")
	}

	const trials = 20000
	hits := 0
	for i := 0; i < trials; i++ {
		if r.chance(250) {
			hits++
		}
	}
	if hits < trials/4-500 || hits > trials/4+500 {
		t.Fatalf("chance(250) fired %d/%d, expected ~%d", hits, trials, trials/4)
	}
}

// TestIntnRange: Intn(n) stays in [0,n) and panics on n<=0.
func TestIntnRange(t *testing.T) {
	r := NewRNG(3)
	for i := 0; i < 10000; i++ {
		if v := r.Intn(7); v < 0 || v >= 7 {
			t.Fatalf("Intn(7)=%d out of range", v)
		}
	}
	assertPanic(t, func() { r.Intn(0) })
	assertPanic(t, func() { r.Intn(-1) })
}

// TestBetweenRange: between(lo,hi) stays inclusive-in-range and panics on hi<lo.
func TestBetweenRange(t *testing.T) {
	r := NewRNG(4)
	sawLo, sawHi := false, false
	for i := 0; i < 20000; i++ {
		v := r.between(3, 8)
		if v < 3 || v > 8 {
			t.Fatalf("between(3,8)=%d out of range", v)
		}
		if v == 3 {
			sawLo = true
		}
		if v == 8 {
			sawHi = true
		}
	}
	if !sawLo || !sawHi {
		t.Fatal("between did not cover its inclusive endpoints")
	}
	// Degenerate range returns the single value without panic.
	if r.between(5, 5) != 5 {
		t.Fatal("between(5,5) must return 5")
	}
	assertPanic(t, func() { r.between(9, 2) })
}

func assertPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	f()
}
