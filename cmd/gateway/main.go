// Command gateway runs one gateway pod (spec: Gateway): the TCP transport
// with Noise NK on gnet, the gateway core, and its Kick and admin servers.
//
//	gateway [flags]                                serve
//	gateway noise-keygen -key-id 1 -out keys/1     write a Noise static key
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "noise-keygen" {
		if err := keygen(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "gateway:", err)
			os.Exit(2)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, os.Args[1:], nil); err != nil {
		slog.Error("gateway: exit", "err", err)
		os.Exit(1)
	}
}

// keygen writes a new Noise static key's private half to -out (for the
// Secret, file name = key_id) and prints the public half to pin in the app
// (spec: Noise key runbook).
func keygen(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("noise-keygen", flag.ContinueOnError)
	id := fs.Uint("key-id", 0, "key_id (0–65535); the Secret's file name")
	path := fs.String("out", "", "private key file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" || *id > 65535 {
		return errors.New("noise-keygen: -out is required and -key-id must be in 0–65535")
	}
	k, err := wire.GenerateKey()
	if err != nil {
		return err
	}
	priv, pub := wire.EncodeKey(k)
	// O_EXCL: never replace a key whose public half may already be pinned in
	// shipped apps.
	f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(priv + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintf(out, "key_id %d public %s\n", *id, pub)
	return nil
}

// options are the flags that are not part of a Config.
type options struct {
	keyDir       string
	drainTimeout time.Duration
	faults       bool
	// clockOffset shifts the gateway's wall clock for the race suite's
	// clock-skew scenario (R4M-5, P4I-5). It is fixed at start: the clock
	// also drives the timing wheel, and a step at run time would fire
	// timers early.
	clockOffset time.Duration
}

// window is a flag holding a uniform wait "lo-hi", e.g. "1s-3s".
type window struct{ w *gateway.Window }

func (f window) String() string {
	if f.w == nil || *f.w == (gateway.Window{}) {
		return ""
	}
	return f.w.Lo.String() + "-" + f.w.Hi.String()
}

func (f window) Set(s string) error {
	lo, hi, ok := strings.Cut(s, "-")
	if !ok {
		return fmt.Errorf("want lo-hi, e.g. 1s-3s")
	}
	var err error
	if f.w.Lo, err = time.ParseDuration(lo); err != nil {
		return err
	}
	f.w.Hi, err = time.ParseDuration(hi)
	return err
}

// parseFlags returns the validated configuration. The gateway refuses to
// start on a configuration out of range or breaking a required ratio (spec:
// Timeouts, Test execution model).
func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config, options, error) {
	cfg, sc := gateway.DefaultConfig(), gateway.DefaultServerConfig()
	cfg.NumLoops = runtime.GOMAXPROCS(0)
	cfg.GatewayID = os.Getenv("POD_NAME")
	if cfg.GatewayID == "" {
		cfg.GatewayID, _ = os.Hostname()
	}
	tc := tcp.DefaultConfig(cfg.NumLoops)
	o := options{keyDir: "/etc/goim/noise", drainTimeout: 85 * time.Second}

	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&cfg.GatewayID, "id", cfg.GatewayID, "gateway id: the pod name (default $POD_NAME, then the host name)")
	fs.StringVar(&tc.Addr, "listen", tc.Addr, "client TCP listen address (gnet)")
	fs.StringVar(&o.keyDir, "noise-keys", o.keyDir, "Noise key Secret directory: one file per key_id")
	fs.DurationVar(&o.drainTimeout, "drain-timeout", o.drainTimeout, "SIGTERM drain limit; keep below terminationGracePeriodSeconds")
	fs.BoolVar(&o.faults, "faults", false, "serve the test-only fault API on the admin port (race suite only, never in production)")
	fs.DurationVar(&o.clockOffset, "clock-offset", 0, "shift the wall clock by this much (race suite clock skew only)")

	fs.StringVar(&sc.LogicTarget, "logic", sc.LogicTarget, "Logic gRPC target (Login/Logout/Reconcile)")
	fs.StringVar(&sc.LeaseTarget, "lease", sc.LeaseTarget, "Logic Lease gRPC target (dedicated channel)")
	fs.StringVar(&sc.KickAddr, "kick-addr", sc.KickAddr, "Kick gRPC listen address")
	fs.StringVar(&sc.AdminAddr, "admin-addr", sc.AdminAddr, "/metrics, /healthz, /readyz, /debug/pprof/ listen address")

	fs.IntVar(&cfg.NumLoops, "loops", cfg.NumLoops, "gnet event loops (default GOMAXPROCS)")
	fs.IntVar(&cfg.MaxConns, "max-conns", cfg.MaxConns, "connection cap: open sockets in any state")
	fs.IntVar(&cfg.PendingCap, "pending-cap", cfg.PendingCap, "HANDSHAKE + AUTH_PENDING sockets")
	fs.DurationVar(&cfg.RefuseHold, "refuse-hold", cfg.RefuseHold, "a pending_mem or draining refusal stays open until the first tick this long after it (0: close at once)")
	fs.IntVar(&cfg.HeldCap, "held-cap", cfg.HeldCap, "refusals held open at once; over it they close at once (0: no hold)")
	fs.IntVar(&cfg.LoginQueueCap, "login-queue-cap", cfg.LoginQueueCap, "Logins waiting for or inside a Login RPC")
	fs.IntVar(&cfg.LoginRPCs, "login-rpcs", cfg.LoginRPCs, "concurrent Login RPCs to Logic")
	fs.IntVar(&cfg.LoginBatch, "login-batch", cfg.LoginBatch, "entries per Login RPC")
	fs.IntVar(&cfg.LogoutQueue, "logout-queue", cfg.LogoutQueue, "Logouts queued or waiting for a retry (default: -max-conns)")
	fs.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", cfg.HandshakeTimeout, "accept → handshake done (3s–30s)")
	fs.DurationVar(&cfg.AuthTimeout, "auth-timeout", cfg.AuthTimeout, "handshake done → LoginReq (3s–30s)")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "heartbeat interval sent in LoginResp")
	fs.DurationVar(&cfg.HeartbeatMin, "heartbeat-min", cfg.HeartbeatMin, "lowest allowed -heartbeat; the same as the SDK's range")
	fs.DurationVar(&cfg.HeartbeatMax, "heartbeat-max", cfg.HeartbeatMax, "highest allowed -heartbeat; the same as the SDK's range")
	fs.DurationVar(&cfg.LoginDeadline, "login-deadline", cfg.LoginDeadline, "login_deadline, as Logic's (not scaled by the test profile)")
	fs.DurationVar(&cfg.LoginTimeout, "login-timeout", cfg.LoginTimeout, "per-entry Login timeout from recv_ms")
	fs.DurationVar(&cfg.LeaseInterval, "lease-interval", cfg.LeaseInterval, "GatewayLease period")
	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", cfg.LeaseTimeout, "GatewayLease call timeout")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "Logic's lease TTL (the trigger rule)")
	fs.DurationVar(&cfg.RetryFor, "retry-for", cfg.RetryFor, "Logout and ReconcileEnd retry budget")
	fs.DurationVar(&cfg.RetryBackoff, "retry-backoff", cfg.RetryBackoff, "first Logout and ReconcileEnd retry delay, doubled per attempt")
	fs.DurationVar(&cfg.RetryMaxDelay, "retry-max-delay", cfg.RetryMaxDelay, "longest delay between retries")
	fs.DurationVar(&cfg.Reconcile.Cycle, "reconcile-cycle", cfg.Reconcile.Cycle, "periodic Reconcile cycle")
	fs.DurationVar(&cfg.Reconcile.Jitter, "reconcile-jitter", cfg.Reconcile.Jitter, "periodic Reconcile jitter")
	fs.IntVar(&cfg.Reconcile.TriggeredRate, "triggered-rate", cfg.Reconcile.TriggeredRate, "triggered pass entries/s")
	fs.IntVar(&cfg.Reconcile.DeleteRate, "delete-rate", cfg.Reconcile.DeleteRate, "Logic's delete-phase rate, uids/s")
	fs.DurationVar(&cfg.Drain.Window, "drain-window", cfg.Drain.Window, "Reconnect frames spread over this window")
	fs.DurationVar(&cfg.Drain.Jitter, "drain-jitter", cfg.Drain.Jitter, "Reconnect{jitter_ms}")
	fs.DurationVar(&cfg.Drain.Flush, "drain-flush", cfg.Drain.Flush, "after the last close, wait for Logouts")
	fs.BoolVar(&cfg.Limits.Notrack, "conntrack-notrack", cfg.Limits.Notrack, "conntrack is disabled for the gateway port")
	fs.Var(window{&cfg.BusyRetry}, "busy-retry", "Busy{retry_after} window, lo-hi")
	fs.Var(window{&cfg.Windows.StormWait}, "storm-wait", "LoginResp storm wait window, lo-hi (unset: not sent)")
	fs.Var(window{&cfg.Windows.LocalTrigger}, "local-trigger", "LoginResp local trigger window, lo-hi")
	fs.Var(window{&cfg.Windows.PreHandshakeRetry}, "pre-handshake-retry", "LoginResp pre-handshake retry window, lo-hi")
	fs.Var(window{&cfg.Windows.Retryable}, "retryable", "LoginResp retryable reject window, lo-hi")

	fs.IntVar(&tc.MaxClientFrame, "max-client-frame", tc.MaxClientFrame, "largest client frame Length after the handshake")
	fs.IntVar(&tc.MaxOutbound, "max-outbound", tc.MaxOutbound, "bytes queued for a client that does not read before it is closed as slow_consumer")
	concurrency := fs.Int("handshake-concurrency", 0, "handshakes computing at once (default: half the loops)")
	fs.Float64Var(&tc.HandshakeRate, "handshake-rate", tc.HandshakeRate, "handshakes/s, gateway-wide")
	fs.IntVar(&tc.HandshakeBurst, "handshake-burst", tc.HandshakeBurst, "handshake token bucket burst")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, tc, o, err
	}
	// The Logout queue defaults to the cap (#89): when every connection
	// closes at once, each queues one Logout, and a full queue drops it.
	logoutQueueSet := false
	fs.Visit(func(f *flag.Flag) { logoutQueueSet = logoutQueueSet || f.Name == "logout-queue" })
	if !logoutQueueSet {
		cfg.LogoutQueue = cfg.MaxConns
	}
	tc.HandshakeConcurrency = tcp.DefaultConfig(cfg.NumLoops).HandshakeConcurrency
	if *concurrency != 0 {
		tc.HandshakeConcurrency = *concurrency
	}
	cfg.Epoch = gateway.NewEpoch()
	if err := errors.Join(cfg.Validate(), tc.Validate(cfg.NumLoops)); err != nil {
		return cfg, sc, tc, o, fmt.Errorf("config: %w", err)
	}
	return cfg, sc, tc, o, nil
}

// run serves until ctx is done, then drains. started, if set, gets the
// client listen address once the gateway is serving (tests).
func run(ctx context.Context, args []string, started func(addr string)) error {
	cfg, sc, tc, o, err := parseFlags(args)
	if err != nil {
		return err
	}
	keys, err := wire.LoadKeyDir(o.keyDir)
	if err != nil {
		return fmt.Errorf("noise keys: %w", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	var tr *tcp.Transport // set before Start; Ready reads it only after
	d := gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}}
	if o.faults {
		d.Faults = fault.New()
		slog.Warn("gateway: -faults is set: the test-only fault API is on the admin port")
	}
	if off := o.clockOffset; off != 0 {
		d.Clock = func() time.Time { return time.Now().Add(off) }
		slog.Warn("gateway: -clock-offset is set: the wall clock is shifted", "offset", off)
	}
	g, err := gateway.New(cfg, d)
	if err != nil {
		return err
	}
	if tr, err = tcp.New(g, keys, tc, reg); err != nil {
		return err
	}
	// The gateway runs on its own context: ctx ending is the SIGTERM that
	// starts the drain, and the drain still needs the Logout sender, the
	// wheel ticker and the lease connection.
	if err := g.Start(context.Background(), sc); err != nil {
		return err
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		return err
	}
	kick, admin := g.Addrs()
	slog.Info("gateway: serving", "id", cfg.GatewayID, "epoch", fmt.Sprintf("%016x", cfg.Epoch),
		"listen", tr.Addr(), "kick", kick, "admin", admin, "loops", cfg.NumLoops, "keys", len(keys))
	if started != nil {
		started(tr.Addr())
	}
	<-ctx.Done()

	// SIGTERM: drain on a fresh context (ctx is already done), then stop.
	slog.Info("gateway: draining")
	dctx, cancel := context.WithTimeout(context.Background(), o.drainTimeout)
	defer cancel()
	g.Drain(dctx)
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := tr.Stop(sctx); err != nil {
		slog.Warn("gateway: transport stop", "err", err)
	}
	g.Stop()
	return nil
}
