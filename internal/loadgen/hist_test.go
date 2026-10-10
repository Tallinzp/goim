package loadgen

import "testing"

func TestHistPercentiles(t *testing.T) {
	h := newHist()
	for ms := int64(1); ms <= 100; ms++ {
		h.add(ms)
	}
	d := h.dist(100)
	if d.P50 != 50 || d.P90 != 90 || d.P99 != 99 || d.Max != 100 || d.Missing != 0 {
		t.Fatalf("dist %+v", d)
	}
	if !d.Within(99) || d.Within(98) {
		t.Fatal("Within")
	}
}

// Members with no sample are infinitely slow: with 2 of 100 missing, p99
// is not reached.
func TestHistMissingMembersFailThePercentile(t *testing.T) {
	h := newHist()
	for range 98 {
		h.add(10)
	}
	d := h.dist(100)
	if d.P50 != 10 || d.P90 != 10 || d.P99 != -1 || d.Missing != 2 || d.Within(1_000_000) {
		t.Fatalf("dist %+v", d)
	}
	h.add(10)
	if d := h.dist(100); d.P99 != 10 {
		t.Fatalf("99 of 100: p99 %d, want 10", d.P99)
	}
}

// Exact to the millisecond up to 60s (every SLO bound), 100ms above.
func TestHistCoarseAboveAMinute(t *testing.T) {
	h := newHist()
	h.add(30_001)
	h.add(61_234)
	if d := h.dist(2); d.P50 != 30_001 || d.P99 != 61_200 || d.Max != 61_234 {
		t.Fatalf("dist %+v", d)
	}
	h.add(7_200_000) // 2h: the last bucket
	if d := h.dist(3); d.P99 != histValue(histBucket-1) || d.Max != 7_200_000 {
		t.Fatalf("dist %+v", d)
	}
	if empty := newHist().dist(0); empty.P99 != -1 || empty.Max != -1 {
		t.Fatalf("empty %+v", empty)
	}
}
