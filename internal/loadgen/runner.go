package loadgen

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/sdk"
)

// AppID and the mobile platform are the slot every loadgen client logs in on.
const AppID = "im"

// connsPerSource is the default number of connections per (source,
// gateway) pair, well inside the 28,232-port ephemeral range (#69), sized
// for one gateway less than the stack has: after /kill its clients log in
// again on the others.
const connsPerSource = 20_000

// Config configures a run.
type Config struct {
	Mode      string        // "light" or "sdk" (choice #80)
	Clients   int           // client i logs in as uid <UIDPrefix>-<i>
	Rate      float64       // clients started per second
	UIDPrefix string        // distinct per loadgen run, so runs never replace each other's sessions
	Sources   int           // source addresses; 0: one per 20,000 connections per gateway, with one gateway gone
	PingEvery time.Duration // light: the Ping interval (cmd/loadgen: 150s, the gateway's default heartbeat)
	Timeout   time.Duration // light: dial + handshake + Login (cmd/loadgen: 10s)
	windows   sdk.Windows   // tests only: the SDK's built-in reconnect windows
}

// Stats is the run's state now: GET /stats and the final result.
type Stats struct {
	Mode     string  `json:"mode"`
	Clients  int     `json:"clients"`
	Sources  int     `json:"sources"`
	Started  int64   `json:"started"`
	RampDone bool    `json:"ramp_done"`
	ElapsedS float64 `json:"elapsed_s"`
	Counts
}

// Runner runs one load: Run starts the clients at Rate and holds them.
type Runner struct {
	cfg      Config
	env      Env
	rec      *Recorder
	bal      *Balancer
	pinger   pingers
	begin    atomic.Int64 // unix ns Run started, 0: not yet
	started  atomic.Int64
	rampDone atomic.Bool

	mu    sync.Mutex
	stops []func() // one per started client
}

// New checks cfg and prepares a run against env's gateways.
func New(cfg Config, env Env) (*Runner, error) {
	switch {
	case cfg.Mode != "light" && cfg.Mode != "sdk":
		return nil, fmt.Errorf("loadgen: mode %q, want light or sdk", cfg.Mode)
	case cfg.Clients < 1 || cfg.Rate <= 0:
		return nil, errors.New("loadgen: Clients and Rate must be positive")
	case cfg.UIDPrefix == "":
		return nil, errors.New("loadgen: UIDPrefix is required")
	case cfg.Mode == "light" && (cfg.PingEvery <= 0 || cfg.Timeout <= 0):
		return nil, errors.New("loadgen: light mode needs PingEvery and Timeout")
	}
	if cfg.Sources == 0 {
		gws := max(1, len(env.GatewayAddrs)-1)
		perGW := (cfg.Clients + gws - 1) / gws
		cfg.Sources = (perGW + connsPerSource - 1) / connsPerSource
	}
	bal, err := NewBalancer(env.GatewayAddrs, cfg.Sources)
	if err != nil {
		return nil, err
	}
	r := &Runner{cfg: cfg, env: env, rec: NewRecorder(cfg.Clients, env.GatewayIDs), bal: bal}
	if cfg.Mode == "light" {
		r.pinger = newPingers(runtime.GOMAXPROCS(0), cfg.PingEvery, r.rec)
	}
	return r, nil
}

// Recorder is the run's recorder.
func (r *Runner) Recorder() *Recorder { return r.rec }

// token signs a token for client i, as the account service would.
func (r *Runner) token(i int) (string, error) {
	now := time.Now().UnixMilli()
	return auth.Sign(r.env.JWTKey, auth.Claims{UID: r.uid(i), AppID: AppID, Platform: "mobile",
		IatMs: now - 1000, ExpMs: now + 24*3_600_000})
}

func (r *Runner) uid(i int) string { return r.cfg.UIDPrefix + "-" + strconv.Itoa(i) }

// Run starts client i at begin + i/Rate, then holds them until ctx ends,
// and returns once every client is stopped.
func (r *Runner) Run(ctx context.Context) {
	begin := time.Now()
	r.begin.Store(begin.UnixNano())
	var wg sync.WaitGroup
	if r.pinger != nil {
		wg.Go(func() { r.pinger.run(ctx) })
	}
	for i := 0; i < r.cfg.Clients && ctx.Err() == nil; i++ {
		due := begin.Add(time.Duration(float64(i) / r.cfg.Rate * float64(time.Second)))
		if d := time.Until(due); d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
			if ctx.Err() != nil {
				break
			}
		}
		r.start(ctx, &wg, i)
		r.started.Add(1)
	}
	r.rampDone.Store(ctx.Err() == nil)
	<-ctx.Done()
	r.mu.Lock()
	stops := r.stops
	r.mu.Unlock()
	// Stop in parallel: an SDK Close waits for its run loop.
	work := make(chan func())
	var sw sync.WaitGroup
	for range 64 {
		sw.Go(func() {
			for f := range work {
				f()
			}
		})
	}
	for _, f := range stops {
		work <- f
	}
	close(work)
	sw.Wait()
	wg.Wait()
}

func (r *Runner) start(ctx context.Context, wg *sync.WaitGroup, i int) {
	var stop func()
	if r.cfg.Mode == "light" {
		lc := &lightConn{i: i}
		stop = lc.stop
		wg.Go(func() { r.light(ctx, lc) })
	} else {
		c, err := r.newSDK(i)
		if err != nil {
			r.rec.Failed("sdk")
			return
		}
		stop = c.Close
		c.Connect()
	}
	r.mu.Lock()
	r.stops = append(r.stops, stop)
	r.mu.Unlock()
}

// ErrKillAfterMark is a SIGKILL that failed after its mark was taken: the
// process ended between Kill's check and the signal. The mark stands.
var ErrKillAfterMark = errors.New("loadgen: SIGKILL failed after the mark")

// Kill is POST /kill: the clients logged in on gateway gw become mark
// name's population, gw leaves the balancer, and pid gets SIGKILL. The mark
// is taken just before the signal is sent: T_death, the moment loadgen
// issues SIGKILL (R4M-17). A pid that is gone or a mark name already taken
// changes nothing: no mark, no gateway leaves the balancer and no signal is
// sent. A dial that picks gw between the mark and SetDown is not logged in
// at the mark, so it is not in the population.
func (r *Runner) Kill(gw, pid int, name string) (int, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("loadgen: pid %d", pid)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return 0, fmt.Errorf("loadgen: pid %d: %w", pid, err)
	}
	n, err := r.Mark(gw, name, true)
	if err != nil {
		return 0, err
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		return n, fmt.Errorf("%w: pid %d: %v", ErrKillAfterMark, pid, err)
	}
	return n, nil
}

// Mark is POST /mark: a named instant on gateway gw (-1: every gateway),
// e.g. b2's DROP; down also takes gw out of the balancer, as an LB's
// health check would, once the mark is taken.
func (r *Runner) Mark(gw int, name string, down bool) (int, error) {
	n, err := r.rec.Mark(name, gw, time.Now())
	if err == nil && down && gw >= 0 {
		r.bal.SetDown(gw)
	}
	return n, err
}

// Stats is the run's state now.
func (r *Runner) Stats() Stats {
	s := Stats{Mode: r.cfg.Mode, Clients: r.cfg.Clients, Sources: r.cfg.Sources, Started: r.started.Load(),
		RampDone: r.rampDone.Load(), Counts: r.rec.Counts()}
	if b := r.begin.Load(); b != 0 {
		s.ElapsedS = time.Since(time.Unix(0, b)).Seconds()
	}
	return s
}
