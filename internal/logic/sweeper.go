package logic

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"golang.org/x/time/rate"
)

// Sweeper deletes the entries of dead gateway epochs (spec: Sweeper). Every
// Logic node runs one; the sweep: claim keeps two nodes off the same epoch.
// Sweeping only cleans up: a dead epoch's entries are already invalid by the
// validity rule, so a held-back sweep never affects correctness.
type Sweeper struct {
	cfg   Config
	st    Store
	m     *Metrics
	now   func() time.Time
	guard Guard
	lim   *rate.Limiter // ~10k uids/s across every epoch this node sweeps
}

// NewSweeper returns a sweeper; now is the local clock (time.Now in production).
func NewSweeper(cfg Config, st Store, m *Metrics, now func() time.Time) *Sweeper {
	return &Sweeper{cfg: cfg, st: st, m: m, now: now,
		guard: Guard{Recent: cfg.RecentRenewal(), Hold: cfg.Store.LeaseTTL},
		lim:   rate.NewLimiter(rate.Limit(cfg.SweepRate), cfg.SweepBatch)}
}

// Run calls Cycle every SweepInterval until ctx ends.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Cycle(ctx)
		}
	}
}

// Cycle runs one sweeper pass: observe renewals for the guard, then sweep
// every epoch that ended or whose score is older than 2 × lease TTL, if the
// guard allows (ended epochs are exempt).
func (s *Sweeper) Cycle(ctx context.Context) {
	local := s.now()
	redisNow, err := s.st.RedisNowMs(ctx)
	var epochs []store.EpochScore
	if err == nil {
		epochs, err = s.st.ListEpochs(ctx)
	}
	if err != nil {
		s.guard.RedisFailed()
		s.m.GuardRenewingSeconds.Set(0)
		slog.Warn("sweeper: redis", "err", err)
		return
	}
	s.guard.Observe(local, redisNow, epochs)
	s.m.GuardRenewingSeconds.Set(s.guard.Renewing(local).Seconds())

	holding := false
	for _, e := range epochs {
		ended := e.ScoreMs == 0
		if !ended && redisNow-e.ScoreMs <= s.cfg.StaleAfter().Milliseconds() {
			continue
		}
		if !ended && !s.guard.Allows(local) {
			holding = true
			s.m.GuardHolds.Inc()
			continue
		}
		s.sweep(ctx, e.EpochRef)
	}
	if holding {
		s.m.GuardHolding.Set(1)
		slog.Warn("sweeper: mass-stale guard holds back stale epochs", "renewing", s.guard.Renewing(local))
	} else {
		s.m.GuardHolding.Set(0)
	}
}

// sweep claims ref, checks its lease key (authoritative, R4M-3), deletes its
// fields page by page and aborts as soon as the epoch renews (R3-O3).
func (s *Sweeper) sweep(ctx context.Context, ref store.EpochRef) {
	result := s.sweepOnce(ctx, ref)
	s.m.Sweeps.WithLabelValues(result).Inc()
	if result != "claimed_elsewhere" {
		slog.Info("sweeper: epoch", "gw", ref.Gateway, "epoch", ref.Epoch, "result", result)
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context, ref store.EpochRef) string {
	ok, err := s.st.ClaimSweep(ctx, ref)
	if err != nil {
		return "error"
	}
	if !ok {
		return "claimed_elsewhere"
	}
	release := func(result string) string {
		if err := s.st.ReleaseSweep(ctx, ref); err != nil {
			slog.Warn("sweeper: release claim", "gw", ref.Gateway, "epoch", ref.Epoch, "err", err)
		}
		return result
	}
	claimed, ok, err := s.st.EpochScoreOf(ctx, ref) // the score at claim time (R3-O3)
	if err != nil {
		return release("error")
	}
	if !ok {
		return release("gone") // another node finished it
	}
	if live, err := s.alive(ctx, ref, claimed); err != nil {
		return release("error")
	} else if live {
		return release("lease_live") // its ZADD failed while its SET succeeded (R4M-3)
	}

	var cursor uint64
	for {
		uids, next, err := s.st.ScanConns(ctx, ref, cursor, int64(s.cfg.SweepBatch))
		if err != nil {
			return release("error")
		}
		for off := 0; off < len(uids); off += s.cfg.SweepBatch {
			part := uids[off:min(off+s.cfg.SweepBatch, len(uids))]
			if err := s.lim.WaitN(ctx, len(part)); err != nil {
				return release("error")
			}
			n, err := s.st.SweepUsers(ctx, ref, part)
			s.m.SweepUIDs.Add(float64(len(part)))
			s.m.SweepDeleted.Add(float64(n))
			if err != nil {
				return release("error")
			}
			// Abort on renewal, between batches: no UNLINK, no ZREM, and the
			// claim is deleted so it can't block a later sweep (R3-OP6). The
			// renewal reported missing, so the gateway's triggered pass restores.
			if live, err := s.alive(ctx, ref, claimed); err != nil {
				return release("error")
			} else if live {
				return release("aborted")
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if err := s.st.FinishSweep(ctx, ref); err != nil {
		return release("error")
	}
	return "done"
}

// alive reports whether ref renewed since the claim: its score is newer than
// claimed, or its lease key exists.
func (s *Sweeper) alive(ctx context.Context, ref store.EpochRef, claimed int64) (bool, error) {
	score, ok, err := s.st.EpochScoreOf(ctx, ref)
	if err != nil {
		return false, err
	}
	if ok && score > claimed {
		return true, nil
	}
	exists, err := s.st.LeasesExist(ctx, []store.EpochRef{ref})
	if err != nil {
		return false, err
	}
	return exists[0], nil
}
