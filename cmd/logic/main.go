// Command logic runs one Logic pod (spec: Logic): the gateway-facing RPCs,
// Revoke for the account service, the hardened Kick client and the sweeper.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("logic: exit", "err", err)
		os.Exit(1)
	}
}

// options are the flags that are not part of logic.Config.
type options struct {
	redisAddrs   string
	redisCluster bool
	redisReload  time.Duration
	jwksURL      string
	jwksFallback string
	jwks         auth.ManagerConfig // its intervals; URL and fallback come from the two flags above
	kickAddrFile string
	faults       bool
}

// parseFlags returns the validated configuration. Logic refuses to start on
// a configuration that breaks a required ratio (spec: Test execution model).
func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error) {
	cfg, sc := logic.DefaultConfig(), logic.DefaultServerConfig()
	o := options{jwks: auth.DefaultManagerConfig("", "")}
	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
	fs.DurationVar(&o.redisReload, "redis-reload", time.Second, "Redis Cluster: reload the slot map when it is older than this; a failed-over master's slots reach its replica within it (#139)")
	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")
	fs.DurationVar(&o.jwks.Refresh, "jwks-refresh", o.jwks.Refresh, "periodic JWKS refresh")
	fs.DurationVar(&o.jwks.RetryEmpty, "jwks-retry-empty", o.jwks.RetryEmpty, "JWKS retry interval while no key set is held")
	fs.DurationVar(&o.jwks.UnknownKid, "jwks-unknown-kid", o.jwks.UnknownKid, "at most one unknown-kid refresh per this long")
	fs.StringVar(&o.kickAddrFile, "kick-addr-file", "", "JSON file mapping gateway id to Kick address, read at each dial (tests; overrides -kick-addr-format)")
	fs.BoolVar(&o.faults, "faults", false, "serve the test-only fault API on the admin port (race suite only, never in production)")

	fs.StringVar(&sc.GRPCAddr, "grpc-addr", sc.GRPCAddr, "Logic and Revocation gRPC listen address")
	fs.StringVar(&sc.LeaseAddr, "lease-addr", sc.LeaseAddr, "Lease gRPC listen address (dedicated, R1-O2)")
	fs.StringVar(&sc.AdminAddr, "admin-addr", sc.AdminAddr, "/metrics, /healthz, /readyz, /debug/pprof/ listen address")
	fs.IntVar(&sc.Workers, "workers", sc.Workers, "concurrent Login/Logout/Reconcile calls")
	fs.IntVar(&sc.LeaseWorkers, "lease-workers", sc.LeaseWorkers, "concurrent lease calls")
	fs.DurationVar(&sc.MaxConnectionAge, "max-conn-age", sc.MaxConnectionAge, "gRPC MaxConnectionAge")
	fs.DurationVar(&sc.MaxConnectionAgeGrace, "max-conn-age-grace", sc.MaxConnectionAgeGrace, "gRPC MaxConnectionAgeGrace")

	fs.DurationVar(&cfg.Store.LoginDeadline, "login-deadline", cfg.Store.LoginDeadline, "login_deadline (not scaled by the test profile)")
	fs.DurationVar(&cfg.Store.SessTTL, "sess-ttl", cfg.Store.SessTTL, "sess:{uid} and rev:{uid} TTL")
	fs.DurationVar(&cfg.Store.LeaseTTL, "lease-ttl", cfg.Store.LeaseTTL, "gateway lease TTL")
	fs.DurationVar(&cfg.Store.WaitTimeout, "wait-timeout", cfg.Store.WaitTimeout, "Revoke WAIT timeout")
	fs.DurationVar(&cfg.RenewInterval, "renew-interval", cfg.RenewInterval, "gateways' lease renewal interval")
	fs.DurationVar(&cfg.RedisFailover, "redis-failover", cfg.RedisFailover, "measured Redis shard failover time")
	fs.DurationVar(&cfg.LeaseCache, "lease-cache", cfg.LeaseCache, "lease observation cache")
	fs.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "sweeper cycle")
	fs.IntVar(&cfg.SweepRate, "sweep-rate", cfg.SweepRate, "sweeper uids/s")
	fs.IntVar(&cfg.SweepBatch, "sweep-batch", cfg.SweepBatch, "sweeper uids per page and abort check")
	fs.IntVar(&cfg.DeleteRate, "delete-rate", cfg.DeleteRate, "ReconcileEnd delete phase uids/s")
	fs.IntVar(&cfg.DeleteBatch, "delete-batch", cfg.DeleteBatch, "delete phase uids per page")
	fs.DurationVar(&cfg.RevokeMaxAhead, "revoke-max-ahead", cfg.RevokeMaxAhead, "largest before_ms lead over Redis TIME")
	fs.IntVar(&cfg.Kick.Workers, "kick-workers", cfg.Kick.Workers, "Kick workers per target")
	fs.IntVar(&cfg.Kick.Queue, "kick-queue", cfg.Kick.Queue, "Kick queue per target")
	fs.DurationVar(&cfg.Kick.Deadline, "kick-deadline", cfg.Kick.Deadline, "Kick attempt deadline")
	fs.DurationVar(&cfg.Kick.RetrySpacing, "kick-retry-spacing", cfg.Kick.RetrySpacing, "between Kick attempts")
	fs.DurationVar(&cfg.Kick.IdleTimeout, "kick-idle-timeout", cfg.Kick.IdleTimeout, "a Kick target with no work for this long is dropped")
	fs.StringVar(&cfg.Kick.AddrFormat, "kick-addr-format", cfg.Kick.AddrFormat, "gateway pod DNS, %s = pod name")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, o, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, sc, o, fmt.Errorf("config: %w", err)
	}
	if err := sc.Validate(cfg); err != nil {
		return cfg, sc, o, fmt.Errorf("config: %w", err)
	}
	if o.jwksURL == "" && o.jwksFallback == "" {
		return cfg, sc, o, errors.New("config: -jwks-url or -jwks-fallback is required")
	}
	o.jwks.URL, o.jwks.FallbackPath = o.jwksURL, o.jwksFallback
	if o.jwks.Refresh <= 0 || o.jwks.RetryEmpty <= 0 || o.jwks.UnknownKid <= 0 {
		return cfg, sc, o, errors.New("config: the JWKS intervals must be positive")
	}
	if o.redisReload <= 0 { // go-redis reads 0 as its 60s default
		return cfg, sc, o, errors.New("config: -redis-reload must be positive")
	}
	return cfg, sc, o, nil
}

func run(args []string) error {
	cfg, sc, o, err := parseFlags(args)
	if err != nil {
		return err
	}
	addrs := strings.Split(o.redisAddrs, ",")
	var rdb redis.UniversalClient
	if o.redisCluster {
		// The client reloads its slot map after a MOVED, ASK or READONLY
		// reply, or once it is older than ClusterStateReloadInterval (60s by
		// default). A master that died answers nothing, so without a short
		// interval its slots stay on the dead address for up to a minute
		// after its replica took over (Plan 6c2a prototype, #139).
		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, ClusterStateReloadInterval: o.redisReload})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
	}
	defer rdb.Close()
	var faults *fault.Set
	if o.faults {
		faults = fault.New()
		rdb.AddHook(logic.FaultHook(faults))
		slog.Warn("logic: -faults is set: the test-only fault API is on the admin port")
	}
	dial := logic.PodDNSDial(cfg.Kick.AddrFormat)
	if o.kickAddrFile != "" {
		dial = logic.FileDial(o.kickAddrFile)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	m := logic.NewMetrics(reg)
	keys := auth.NewManager(o.jwks, m.AuthObserver())
	srv, err := logic.NewServer(cfg, sc, logic.Deps{Store: store.New(rdb, cfg.Store), Keys: keys,
		Dial: dial, Registry: reg, Metrics: m, Faults: faults})
	if err != nil {
		return err
	}
	// Not Ready until a key set is held; Run keeps retrying both sources.
	if err := keys.Load(ctx); err != nil {
		slog.Error("logic: no key set yet; staying NotReady", "err", err)
	}
	if err := srv.Start(ctx); err != nil {
		return err
	}
	g, l, a := srv.Addrs()
	slog.Info("logic: serving", "grpc", g, "lease", l, "admin", a, "fallback_keys", keys.OnFallback())
	<-ctx.Done()
	slog.Info("logic: shutting down")
	srv.Stop()
	return nil
}
