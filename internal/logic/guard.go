package logic

import (
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// Guard is the sweeper's mass-stale guard (R1-O2, R2-O3, R3-O2). A stale
// epoch may be swept only if
//   - some epoch renewed within Recent (1.5 × the renewal interval), and that
//     has held continuously for Hold (one lease TTL): renewing_since; and
//   - the sweeper's own Redis access has been healthy for Hold.
//
// A stale epoch's score is older than 2 × lease TTL, so any epoch that renewed
// within Recent is another epoch, as the spec requires. Durations are measured
// on the local clock; score ages on Redis TIME, the clock the scores use. All
// state is in memory: a restart starts both clocks over, which is conservative.
type Guard struct {
	Recent time.Duration
	Hold   time.Duration

	renewingSince time.Time // zero while no epoch renews
	healthySince  time.Time // zero after a Redis failure
}

// RedisFailed records a failed cycle: Redis health restarts, and with no view
// of the scores, so does renewing_since.
func (g *Guard) RedisFailed() {
	g.healthySince = time.Time{}
	g.renewingSince = time.Time{}
}

// Observe records one successful cycle at local time now.
func (g *Guard) Observe(now time.Time, redisNowMs int64, epochs []store.EpochScore) {
	if g.healthySince.IsZero() {
		g.healthySince = now
	}
	renewing := false
	for _, e := range epochs {
		if e.ScoreMs > 0 && redisNowMs-e.ScoreMs <= g.Recent.Milliseconds() {
			renewing = true
			break
		}
	}
	switch {
	case !renewing:
		g.renewingSince = time.Time{}
	case g.renewingSince.IsZero():
		g.renewingSince = now
	}
}

// Allows reports whether a stale (not ended) epoch may be swept at now.
func (g *Guard) Allows(now time.Time) bool {
	return !g.renewingSince.IsZero() && now.Sub(g.renewingSince) >= g.Hold &&
		!g.healthySince.IsZero() && now.Sub(g.healthySince) >= g.Hold
}

// Renewing returns how long renewals have held continuously (0 if they don't).
func (g *Guard) Renewing(now time.Time) time.Duration {
	if g.renewingSince.IsZero() {
		return 0
	}
	return now.Sub(g.renewingSince)
}
