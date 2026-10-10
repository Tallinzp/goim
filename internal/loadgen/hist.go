package loadgen

import "math"

// Dist is a latency distribution in milliseconds over a population of N.
// Members with no sample (a client that never logged in again) count as
// infinitely slow: a percentile they reach is -1, so it fails any bound.
type Dist struct {
	N       int64 `json:"n"`
	Missing int64 `json:"missing"`
	P50     int64 `json:"p50_ms"`
	P90     int64 `json:"p90_ms"`
	P99     int64 `json:"p99_ms"`
	Max     int64 `json:"max_ms"` // of the samples; -1 with none
}

// Within reports whether p99 is reached and at most limitMs.
func (d Dist) Within(limitMs int64) bool { return d.P99 >= 0 && d.P99 <= limitMs }

// hist counts samples at 1ms resolution below 60s and 100ms above, up to
// 1h (the last bucket holds everything longer): 95,400 buckets, 763KB, so
// memory stays fixed at 600k clients. Every storm bound (30s, and b2's
// detection plus 30s) falls in the exact range: a bucket's lower bound
// above 60s could pass a p99 up to 99ms over the bound.
type hist struct {
	n, max int64
	counts []int64
}

const (
	histFine   = 60_000 // ms at 1ms resolution
	histStep   = 100    // ms per bucket above
	histBucket = histFine + (3_600_000-histFine)/histStep
)

func newHist() *hist { return &hist{max: -1, counts: make([]int64, histBucket)} }

func histIndex(ms int64) int {
	switch {
	case ms < 0:
		return 0
	case ms < histFine:
		return int(ms)
	}
	return min(histFine+int((ms-histFine)/histStep), histBucket-1)
}

// histValue is bucket i's lower bound in ms.
func histValue(i int) int64 {
	if i < histFine {
		return int64(i)
	}
	return histFine + int64(i-histFine)*histStep
}

func (h *hist) add(ms int64) {
	h.counts[histIndex(ms)]++
	h.n++
	h.max = max(h.max, ms)
}

// dist is the distribution over a population of n ≥ h.n members.
func (h *hist) dist(n int64) Dist {
	n = max(n, h.n)
	d := Dist{N: n, Missing: n - h.n, Max: h.max}
	d.P50, d.P90, d.P99 = h.rank(n, 0.50), h.rank(n, 0.90), h.rank(n, 0.99)
	return d
}

// rank is the value at percentile p of n, -1 if it falls on a missing member.
func (h *hist) rank(n int64, p float64) int64 {
	if n == 0 {
		return -1
	}
	want := int64(math.Ceil(p * float64(n)))
	if want > h.n {
		return -1
	}
	var seen int64
	for i, c := range h.counts {
		if seen += c; seen >= want {
			return histValue(i)
		}
	}
	return -1
}
