package sdk

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

const s = time.Second

func ms(n int64) time.Duration { return time.Duration(n) * time.Millisecond }

func pbw(lo, hi time.Duration) *clientpb.Window {
	return &clientpb.Window{LoMs: uint32(lo.Milliseconds()), HiMs: uint32(hi.Milliseconds())}
}

// The clamp table, roadmap choice #13: each window clamped at both ends,
// lo < hi enforced.
func TestServerWindowsAreClamped(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resp   *clientpb.LoginResp
		get    func(ServerWindows) Window
		lo, hi time.Duration
	}{
		{"storm in range", &clientpb.LoginResp{StormWait: pbw(0, 60*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 60 * s},
		{"storm hi below 10s", &clientpb.LoginResp{StormWait: pbw(0, 5*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 10 * s},
		{"storm hi above 120s", &clientpb.LoginResp{StormWait: pbw(0, 600*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 120 * s},
		{"storm lo pinned at 0", &clientpb.LoginResp{StormWait: pbw(5*s, 30*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 30 * s},
		{"local hi zero", &clientpb.LoginResp{LocalTrigger: pbw(0, 0)}, func(w ServerWindows) Window { return w.Local }, 0, s},
		{"local hi above 10s", &clientpb.LoginResp{LocalTrigger: pbw(0, 60*s)}, func(w ServerWindows) Window { return w.Local }, 0, 10 * s},
		{"pre lo below 0.25s", &clientpb.LoginResp{PreHandshakeRetry: pbw(0, 2*s)}, func(w ServerWindows) Window { return w.PreHandshake }, ms(250), 2 * s},
		{"pre hi above 5s", &clientpb.LoginResp{PreHandshakeRetry: pbw(s, 9*s)}, func(w ServerWindows) Window { return w.PreHandshake }, s, 5 * s},
		{"pre lo above clamped hi", &clientpb.LoginResp{PreHandshakeRetry: pbw(6*s, 9*s)}, func(w ServerWindows) Window { return w.PreHandshake }, ms(250), 5 * s},
		{"retryable lo below 0.5s", &clientpb.LoginResp{Retryable: pbw(0, 4*s)}, func(w ServerWindows) Window { return w.Retryable }, ms(500), 4 * s},
		{"retryable hi below 2s", &clientpb.LoginResp{Retryable: pbw(ms(600), ms(700))}, func(w ServerWindows) Window { return w.Retryable }, ms(600), 2 * s},
		{"retryable lo equals hi", &clientpb.LoginResp{Retryable: pbw(3*s, 3*s)}, func(w ServerWindows) Window { return w.Retryable }, ms(500), 3 * s},
		{"retryable hi above 10s", &clientpb.LoginResp{Retryable: pbw(s, 60*s)}, func(w ServerWindows) Window { return w.Retryable }, s, 10 * s},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.get(ServerWindows{}.Merge(tc.resp))
			if got != (Window{tc.lo, tc.hi}) {
				t.Fatalf("got [%v, %v], want [%v, %v]", got.Lo, got.Hi, tc.lo, tc.hi)
			}
			if got.Lo >= got.Hi {
				t.Fatalf("lo %v ≥ hi %v", got.Lo, got.Hi)
			}
		})
	}
}

// An absent window keeps the current value; a present one replaces it.
func TestAbsentWindowsKeepTheirValue(t *testing.T) {
	w := ServerWindows{}.Merge(&clientpb.LoginResp{StormWait: pbw(0, 30*s), Retryable: pbw(2*s, 4*s)})
	w = w.Merge(&clientpb.LoginResp{HeartbeatS: 150, Retryable: pbw(s, 5*s)})
	if w.Storm != (Window{0, 30 * s}) || w.Retryable != (Window{s, 5 * s}) {
		t.Fatalf("got %+v", w)
	}
	eff := w.Over(DefaultWindows())
	if eff.Storm != (Window{0, 30 * s}) || eff.Local != DefaultWindows().Local ||
		eff.PreHandshake != DefaultWindows().PreHandshake || eff.Retryable != (Window{s, 5 * s}) {
		t.Fatalf("effective windows %+v", eff)
	}
}

func TestClampRepairsDamagedWindows(t *testing.T) {
	w := ServerWindows{Storm: Window{0, ms(1)}, Retryable: Window{s, 3 * s}}.Clamp()
	if w.Storm != (Window{0, 10 * s}) || w.Retryable != (Window{s, 3 * s}) || w.Local != (Window{}) {
		t.Fatalf("got %+v", w)
	}
}

// uniform fails t unless 10k draws of sample pass chiSquare's check
// (R4G-O12). The draws use a fixed seed, so the test never flakes.
func uniform(t *testing.T, name string, lo, hi time.Duration, sample func() time.Duration) {
	t.Helper()
	if chi2, count, err := chiSquare(lo, hi, sample); err != nil {
		t.Fatalf("%s: %v", name, err)
	} else if chi2 > 27.88 { // chi-square, 9 degrees of freedom, p = 0.001
		t.Fatalf("%s: chi-square %.1f > 27.88, buckets %v", name, chi2, count)
	}
}

// chiSquare draws 10k samples, buckets them in 10 equal buckets over
// [lo, hi] and returns the chi-square statistic against a uniform spread.
func chiSquare(lo, hi time.Duration, sample func() time.Duration) (float64, [10]int, error) {
	const n, buckets = 10_000, 10
	var count [buckets]int
	for range n {
		d := sample()
		if d < lo || d > hi {
			return 0, count, fmt.Errorf("drew %v outside [%v, %v]", d, lo, hi)
		}
		count[int(float64(d-lo)/float64(hi-lo+1)*buckets)]++
	}
	var chi2 float64
	for _, c := range count {
		diff := float64(c) - n/buckets
		chi2 += diff * diff / (n / buckets)
	}
	return chi2, count, nil
}

func seeded() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestWindowSampleIsUniform(t *testing.T) {
	r := seeded()
	for name, w := range map[string]Window{"storm 0–20s": DefaultWindows().Storm, "local 0–1s": DefaultWindows().Local,
		"pre-handshake 0.5–1.5s": DefaultWindows().PreHandshake, "retryable 1–3s": DefaultWindows().Retryable,
		"Reconnect 0–5s": {0, 5 * s}} {
		uniform(t, name, w.Lo, w.Hi, func() time.Duration { return w.Sample(r) })
	}
}

// The check rejects a skewed distribution, so a passing check means something.
func TestUniformityCheckRejectsASkewedSample(t *testing.T) {
	r := seeded()
	chi2, count, err := chiSquare(0, 20*s, func() time.Duration { u := r.Float64(); return time.Duration(u * u * float64(20*s)) })
	if err != nil || chi2 <= 27.88 {
		t.Fatalf("a u² sample passed: chi-square %.1f, buckets %v, %v", chi2, count, err)
	}
}
