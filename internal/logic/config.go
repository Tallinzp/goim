// Package logic is the Logic service (spec: Logic): the RPC handlers the
// gateways and the account service call, the hardened Kick client, and the
// sweeper. It is the only code that uses internal/store (layer rule).
package logic

import (
	"errors"
	"fmt"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// Config holds Logic's tunables. Every interval is configurable so the test
// timing profile can scale them (spec: Test execution model).
type Config struct {
	Store store.Config // LoginDeadline, SessTTL, LeaseTTL, WaitTimeout

	// RenewInterval is the gateways' lease renewal interval (10s). The
	// mass-stale guard's "recent renewal" window is 1.5 × this.
	RenewInterval time.Duration
	// RedisFailover is the measured shard failover time; LeaseTTL must exceed
	// it plus one RenewInterval (R2-O5). Default 15s until Tier 2 measures it.
	RedisFailover time.Duration
	LeaseCache    time.Duration // how long a lease observation is reused (~1s)

	SweepInterval time.Duration // sweeper cycle (5s)
	SweepRate     int           // sweeper uids/s (10k)
	SweepBatch    int           // uids per SSCAN page and abort check (500)
	DeleteRate    int           // ReconcileEnd delete phase uids/s (10k, R5S-O8)
	DeleteBatch   int           // uids per delete-phase page (500)

	RevokeMaxAhead time.Duration // before_ms may be at most this far past Redis TIME (5m, PI-4)

	Kick KickConfig
}

// KickConfig sizes the hardened Kick client (R2-O6, R5S-O2, roadmap choice #12).
type KickConfig struct {
	Workers      int           // per target gw|epoch (64)
	Queue        int           // per target (1024)
	Deadline     time.Duration // per attempt, connect included (1s)
	Retries      int           // after the first attempt, only while the lease is live (3)
	RetrySpacing time.Duration // between attempts (10s: 3 retries over ~30s)
	IdleTimeout  time.Duration // a target with no work for this long is dropped (1m)
	// AddrFormat turns a gateway id (pod name) into a dial target via the
	// StatefulSet's pod DNS, e.g. "%s.gateway.im.svc.cluster.local:9100".
	AddrFormat string
}

// DefaultConfig returns the spec defaults.
func DefaultConfig() Config {
	return Config{
		Store:          store.DefaultConfig(),
		RenewInterval:  10 * time.Second,
		RedisFailover:  15 * time.Second,
		LeaseCache:     time.Second,
		SweepInterval:  5 * time.Second,
		SweepRate:      10_000,
		SweepBatch:     500,
		DeleteRate:     10_000,
		DeleteBatch:    500,
		RevokeMaxAhead: 5 * time.Minute,
		Kick: KickConfig{
			Workers: 64, Queue: 1024, Deadline: time.Second, Retries: 3,
			RetrySpacing: 10 * time.Second, IdleTimeout: time.Minute,
			AddrFormat: "%s.gateway.im.svc.cluster.local:9100",
		},
	}
}

// StaleAfter is how old a gwepochs score must be before the sweeper considers
// the epoch (2 × lease TTL, R2-O5).
func (c Config) StaleAfter() time.Duration { return 2 * c.Store.LeaseTTL }

// RecentRenewal is the guard's window for "another epoch renewed" (1.5 × the
// renewal interval, R3-O2).
func (c Config) RecentRenewal() time.Duration { return c.RenewInterval * 3 / 2 }

// Validate refuses a configuration that breaks a ratio the design relies on
// (spec: the test timing profile must keep them; Logic refuses to start otherwise).
func (c Config) Validate() error {
	var errs []error
	pos := func(name string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %v", name, d))
		}
	}
	pos("login deadline", c.Store.LoginDeadline)
	pos("sess TTL", c.Store.SessTTL)
	pos("lease TTL", c.Store.LeaseTTL)
	pos("WAIT timeout", c.Store.WaitTimeout)
	pos("renew interval", c.RenewInterval)
	pos("lease cache", c.LeaseCache)
	pos("sweep interval", c.SweepInterval)
	pos("revoke max ahead", c.RevokeMaxAhead)
	pos("kick deadline", c.Kick.Deadline)
	pos("kick retry spacing", c.Kick.RetrySpacing)
	pos("kick idle timeout", c.Kick.IdleTimeout)
	if c.RedisFailover < 0 {
		errs = append(errs, errors.New("redis failover time must not be negative"))
	}
	if c.Store.LeaseTTL <= c.RedisFailover+c.RenewInterval {
		errs = append(errs, fmt.Errorf("lease TTL %v must exceed failover %v + renew interval %v (R2-O5)",
			c.Store.LeaseTTL, c.RedisFailover, c.RenewInterval))
	}
	if c.LeaseCache >= c.RenewInterval {
		errs = append(errs, fmt.Errorf("lease cache %v must be shorter than the renew interval %v", c.LeaseCache, c.RenewInterval))
	}
	for _, r := range []struct {
		name        string
		rate, batch int
	}{{"sweep", c.SweepRate, c.SweepBatch}, {"delete phase", c.DeleteRate, c.DeleteBatch}} {
		if r.rate <= 0 || r.batch <= 0 || r.batch > r.rate {
			errs = append(errs, fmt.Errorf("%s rate %d and batch %d: need 0 < batch ≤ rate", r.name, r.rate, r.batch))
		}
	}
	if c.Kick.Workers <= 0 || c.Kick.Queue <= 0 || c.Kick.Retries < 0 {
		errs = append(errs, fmt.Errorf("kick workers %d, queue %d, retries %d", c.Kick.Workers, c.Kick.Queue, c.Kick.Retries))
	}
	if c.Kick.AddrFormat == "" {
		errs = append(errs, errors.New("kick address format is empty"))
	}
	return errors.Join(errs...)
}
