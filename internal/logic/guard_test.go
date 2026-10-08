package logic

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

func newGuard() *Guard {
	cfg := DefaultConfig()
	return &Guard{Recent: cfg.RecentRenewal(), Hold: cfg.Store.LeaseTTL}
}

// sim drives a guard with one surviving gateway that renews every 10s plus
// jitter, observed every 5s (the sweeper cycle). Redis and local time are the
// same simulated clock here.
type sim struct {
	g        *Guard
	t        time.Time
	renewed  int64 // last renewal, ms
	nextRenw time.Time
}

func (s *sim) epochs() []store.EpochScore {
	return []store.EpochScore{
		{EpochRef: store.EpochRef{Gateway: "gw-live", Epoch: 1}, ScoreMs: s.renewed},
		{EpochRef: store.EpochRef{Gateway: "gw-dead", Epoch: 2}, ScoreMs: 1}, // stale
	}
}

// run advances 1s at a time for d, renewing on the gateway's rhythm while
// renew is true, and observing every 5s.
func (s *sim) run(d time.Duration, renew bool, jitter func() time.Duration) {
	for end := s.t.Add(d); s.t.Before(end); {
		s.t = s.t.Add(time.Second)
		if renew && !s.t.Before(s.nextRenw) {
			s.renewed = s.t.UnixMilli()
			s.nextRenw = s.t.Add(10*time.Second + jitter())
		}
		if s.t.Unix()%5 == 0 {
			s.g.Observe(s.t, s.t.UnixMilli(), s.epochs())
		}
	}
}

// Spec (R3-O2): renewing_since is not reset by a lone gateway's 10s rhythm
// with jitter, and the guard releases one lease TTL after renewals resume.
func TestGuardSurvivesTenSecondRhythmWithJitter(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	jitter := func() time.Duration { return time.Duration(rng.IntN(3000)) * time.Millisecond } // RPC latency, 0–3s
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Second, true, jitter)
	start := s.t
	s.run(29*time.Second, true, jitter)
	if s.g.Allows(s.t) {
		t.Fatal("released before one lease TTL of renewals")
	}
	s.run(10*time.Minute, true, jitter)
	if !s.g.Allows(s.t) || s.g.Renewing(s.t) < s.t.Sub(start)-5*time.Second {
		t.Fatalf("renewing_since was reset by the 10s rhythm: renewing %v", s.g.Renewing(s.t))
	}
}

// Spec (R3-O2): renewing_since resets when no other epoch renewed within 15s.
func TestGuardResetsWhenRenewalsStop(t *testing.T) {
	none := func() time.Duration { return 0 }
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Minute, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released after a minute of renewals")
	}
	s.run(20*time.Second, false, none) // > 15s without a renewal
	if s.g.Allows(s.t) || s.g.Renewing(s.t) != 0 {
		t.Fatal("still released after renewals stopped")
	}
	s.run(25*time.Second, true, none)
	if s.g.Allows(s.t) {
		t.Fatal("released less than one lease TTL after renewals resumed")
	}
	s.run(10*time.Second, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released one lease TTL after renewals resumed")
	}
}

func TestGuardNeedsHealthyRedisForOneTTL(t *testing.T) {
	none := func() time.Duration { return 0 }
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Minute, true, none)
	s.g.RedisFailed()
	s.run(25*time.Second, true, none)
	if s.g.Allows(s.t) {
		t.Fatal("released within one lease TTL of a Redis failure")
	}
	s.run(10*time.Second, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released after Redis was healthy for one lease TTL")
	}
}

// A systemic outage: no epoch renews at all, so nothing is ever released.
func TestGuardHoldsWhenNobodyRenews(t *testing.T) {
	s := &sim{g: newGuard(), t: time.Unix(1000, 0), renewed: 1}
	s.run(time.Hour, false, nil)
	if s.g.Allows(s.t) {
		t.Fatal("released with no renewals")
	}
}
