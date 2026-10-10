# Plan 6a2b: Loadgen Runs (clients, control API, probe, smoke) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put real load on a stack: loadgen's light and SDK clients with a control API that kills a gateway and returns the storm SLO, `cmd/loadgen`, a shell library to drive it, the Tier 1a load probe (≤ 5k connections) and a Tier 2 smoke scenario that proves the whole pipeline on the host before Plan 6b measures anything.

**Architecture:** `Runner` (Task 4) starts clients at a rate through 6a2a's `Balancer`, records them in 6a2a's `Recorder`, and serves `GET /stats`, `POST /kill`, `POST /mark`, `GET /slo` and pprof. `cmd/loadgen` (Task 5) reads `stack.env`, runs until SIGTERM or `-duration`, and writes its result before stopping the clients. `scripts/loadgen.sh` starts, queries and stops it from bash; `scripts/load-probe.sh` is the Tier 1a probe on an unpinned `stack.sh` stack, and `scripts/tier2/smoke.sh` (Task 6) the Tier 2 scenario on `lib.sh` with loadgen pinned to the loadgen cores.

**Tech Stack:** Go 1.27.1, `internal/clientconn`, `internal/sdk`, bash 5.2, curl, python3 (reading JSON), `taskset`, the pinned `redis-server` 7.4.11.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Success criteria → Storm SLO*, *Verification → Test execution model* (Tier 1a probe, *CPU isolation*, *Automation contract*), *Verification → Load tests*. Plan 6a2a (`docs/superpowers/plans/2026-10-09-connection-session-core-06a2a-loadgen-core.md`) holds the choices (#80–#86, read them first), the Review Focus of Tasks 1–3 and the *Execution Notes* of both plans.

**Branch:** `feat/plan6a2-loadgen`, after Plan 6a2a's three commits.

## Global Constraints

- Every constraint of Plan 6a2a's *Global Constraints* holds here.
- `make test` stays under a few minutes on the dev machine: the in-process end-to-end tests here run 30–40 clients; the probe (`make test-load-probe`) is not part of `make test`.
- Loadgen's control API listens on `STACK_PORT_BASE + 5` (17005 on Tier 2, 17205 for the probe).
- On Tier 2 loadgen runs under `taskset -c $T2_CPUS_LOADGEN` (`0-3,12-15`), in a process group of its own registered in `T2_OURS_PGIDS` (6a1 M3).
- Every Tier 2 scenario writes `result.json` through `lib.sh` (schema 1, choice #73), exits 0/1/2, calls `stack_down` before `t2_finish` and records a `processes` check (6a1 notes).

## Review Focus

1. **A ramp faster than the gateways' handshake limit** closes the excess before the handshake reply (gateway `handshakes_total{result="no_slot"}`). A light client must retry that, as the SDK does, or a 600k ramp leaves holes that look like a capacity limit; it gives up after 10 attempts and counts `gave-up` (`TestLightRetriesAPreHandshakeClose`, Task 4).
2. **The SDK storm after a kill must measure the system, not the test**: the killed gateway leaves the balancer at once, so no client redials the dead address into a failure backoff (#81). `TestSDKStormAfterAGatewayStops` and `TestControlAPI` (Task 4) check that every member is back and the gateway is out.
3. **A loadgen that died, or never wrote its result, must still give a valid `result.json`** with a failed check, not a broken file: `lg_py` prints `null` and `lg_true` prints `false` for a missing or half-written file (`scripts/loadgen_test.sh`, Task 5).
4. **The result must survive a slow teardown.** Stopping 200k SDK clients can outlast `lg_stop`'s 60s wait, after which loadgen is SIGKILLed; `cmd/loadgen` writes the result as soon as the run ends and only then cancels the runner's own context, so no client is being stopped while the result is taken (#84 amended; `TestRunWritesTheResultAtTheEnd` checks the file, Task 5; the order is in `run`).
5. **`stack_expect_dead` must run in the scenario's own shell**, never in `$(...)`: in a subshell it cannot reap the gateway and its `stack_forget` is lost, so `stack_down` later reports the killed gateway as a problem. Both scripts call it as a plain command (probe check `processes: …`, Task 5; smoke check 10, Task 6).

---

### Task 4: the runner: light and SDK clients, kill and mark, control API

**Files:**
- Create: `internal/loadgen/runner.go`, `internal/loadgen/light.go`, `internal/loadgen/sdkclient.go`, `internal/loadgen/control.go`, `internal/loadgen/light_test.go`, `internal/loadgen/e2e_test.go`

**Interfaces:**
- Consumes (Plan 6a2a): `Env`, `NewBalancer`, `(*Balancer).Dial/SetDown/Down` (`Down` added at the 6a2a interface review, P6a2aI-1); `NewRecorder`, `(*Recorder).Online/Lost/Failed/Pong/PongMissing/Mark/SLO/Marks/Counts`, `Counts`, `SLO`; `pprofhttp.Register`. Also `clientconn.New/Conn.Handshake/Send/Recv/Close`, `clientconn.ErrClosedBeforeReply`, `sdk.New`, `sdk.Config`, `sdk.MemStore`, `sdk.NewDeviceID`, `sdk.Event`/`EventKind`/`EndKind`, `sdk.Windows`, `auth.Sign`.
- Produces (Task 5 uses all of it):
  - `const AppID = "im"`; `type Config struct { Mode string; Clients int; Rate float64; UIDPrefix string; Sources int; PingEvery, Timeout time.Duration; windows sdk.Windows }`.
  - `func New(cfg Config, env Env) (*Runner, error)`; `(*Runner).Run(ctx context.Context)` (blocks until ctx ends and every client is stopped); `Recorder() *Recorder`; `Kill(gw, pid int, name string) (int, error)`; `Mark(gw int, name string, down bool) (int, error)`; `Stats() Stats`; `Result(within time.Duration) Result`; `Handler() http.Handler`.
  - `type Stats struct { Mode string; Clients, Sources int; Started int64; RampDone bool; ElapsedS float64; Counts }` (JSON `mode`, `clients`, `sources`, `started`, `ramp_done`, `elapsed_s`, then the `Counts` fields inline); `type Result struct { Stats; SLOs map[string]SLO }` (`slos`).
  - Failure reasons in `Counts.Failed`: light `dial`, `pre-handshake`, `handshake`, `token`, `login`, `busy`, `rejected`, `stopped`, `gave-up`; sdk the `sdk.EndKind` names (`pre-handshake`, `busy`, `failed`, …) and `sdk`. Loss reasons in `Counts.Lost`: light `lost`, `kicked`, `reconnect`, `stopped`; sdk the `EndKind` names (`lost`, `pong-timeout`, `reconnect`, `kicked`, …).

- [ ] **Step 1: Write the failing tests**

`internal/loadgen/light_test.go`:

```go
package loadgen

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/clientconn"
)

// A Ping due while the previous one has no Pong is counted missing; a
// removed client gets no more Pings.
func TestPingerCountsMissingPongs(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	rec := NewRecorder(1, []string{"gw-0"})
	p := newPinger(pingTick, rec)                // one slot: every client every tick
	lc := &lightConn{i: 0, c: clientconn.New(a)} // no handshake: Send fails at once, the Ping still counts as sent
	p.add(lc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	waitFor(t, "two missing Pongs", 2*time.Second, func() bool { return rec.Counts().PongMissing >= 2 })
	p.remove(lc)
	lc.pingAt.Store(0)
	time.Sleep(3 * pingTick)
	cancel()
	<-done
	if lc.pingAt.Load() != 0 {
		t.Fatal("a removed client was pinged")
	}
}
```

`internal/loadgen/e2e_test.go` (an in-process stack as in `internal/sdk/e2e_test.go`: Logic on miniredis and real gnet gateways over Noise):

```go
package loadgen

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/sdk"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The end-to-end tests run loadgen over real TCP and Noise against real
// gateways and Logic on miniredis, in process (as internal/sdk's do).

type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

// startStack starts Logic and n gateways; stop[i] stops gateway i's
// transport (its clients see their connections close) and core.
func startStack(t *testing.T, n int) (env Env, stop []func()) {
	t.Helper()
	jwt, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	noise, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	kicks := map[string]string{}
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour
	lsc := logic.DefaultServerConfig()
	lsc.GRPCAddr, lsc.LeaseAddr, lsc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	reg := prometheus.NewRegistry()
	srv, err := logic.NewServer(lcfg, lsc, logic.Deps{Store: store.New(testredis.New(t), lcfg.Store), Keys: staticKeys{jwt},
		Dial: func(gw string) (*grpc.ClientConn, error) {
			mu.Lock()
			defer mu.Unlock()
			return grpc.NewClient(kicks[gw], grpc.WithTransportCredentials(insecure.NewCredentials()))
		}, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	grpcAddr, leaseAddr, _ := srv.Addrs()
	env = Env{NoiseKeyID: 1, NoisePublic: noise.Public, JWTKey: jwt}
	for range n {
		cfg := gateway.DefaultConfig()
		cfg.GatewayID, cfg.Epoch, cfg.NumLoops = testredis.Gateway(), gateway.NewEpoch(), 2
		cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 1000, 1000, 1000, 1000
		cfg.Limits.Notrack = true
		tcfg := tcp.DefaultConfig(cfg.NumLoops)
		tcfg.Addr = "tcp://127.0.0.1:0"
		greg := prometheus.NewRegistry()
		var tr *tcp.Transport
		g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(greg), Registry: greg,
			ReadyChecks: []func() error{func() error { return tr.Ready() }}})
		if err != nil {
			t.Fatal(err)
		}
		if tr, err = tcp.New(g, wire.KeySet{1: noise}, tcfg, greg); err != nil {
			t.Fatal(err)
		}
		if err := g.Start(context.Background(), gateway.ServerConfig{LogicTarget: grpcAddr,
			LeaseTarget: leaseAddr, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		if err := tr.Start(); err != nil {
			g.Stop()
			t.Fatal(err)
		}
		var once sync.Once
		s := func() {
			once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				tr.Stop(ctx)
				g.Stop()
			})
		}
		t.Cleanup(s)
		kick, _ := g.Addrs()
		mu.Lock()
		kicks[cfg.GatewayID] = kick
		mu.Unlock()
		env.GatewayAddrs = append(env.GatewayAddrs, tr.Addr())
		env.GatewayIDs = append(env.GatewayIDs, cfg.GatewayID)
		stop = append(stop, s)
	}
	return env, stop
}

// run starts r in the background; the returned func stops it and waits.
func run(r *Runner) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}

// Light clients log in evenly over the gateways, every Ping is answered,
// and stopping the run closes every connection. Under -race the gateways'
// handshake limit closes some first attempts before the handshake reply;
// those are retried.
func TestLightClientsHoldAndPing(t *testing.T) {
	env, _ := startStack(t, 2)
	r, err := New(Config{Mode: "light", Clients: 40, Rate: 400, UIDPrefix: "light", PingEvery: 200 * time.Millisecond,
		Timeout: 5 * time.Second}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	waitFor(t, "40 online", 10*time.Second, func() bool { s := r.Stats(); return s.RampDone && s.Online == 40 })
	s := r.Stats()
	if d := s.OnlineByGateway[env.GatewayIDs[0]] - s.OnlineByGateway[env.GatewayIDs[1]]; d < -2 || d > 2 {
		t.Fatalf("not balanced: %v", s.OnlineByGateway)
	}
	waitFor(t, "two Pongs per client", 5*time.Second, func() bool { return r.Stats().Pongs >= 80 })
	stop()
	s = r.Stats()
	delete(s.Failed, "pre-handshake")
	if s.PongMissing != 0 || s.Online != 0 || s.Lost["stopped"] != 40 || len(s.Failed) != 0 || s.LoginLatency.N != 40 {
		t.Fatalf("stats after stop: %+v", s)
	}
}

// SDK clients on a gateway that dies log in again elsewhere; the SLO from
// the mark covers every one of them.
func TestSDKStormAfterAGatewayStops(t *testing.T) {
	env, stopGW := startStack(t, 2)
	r, err := New(Config{Mode: "sdk", Clients: 30, Rate: 300, UIDPrefix: "sdk"}, env)
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.windows = sdk.DefaultWindows()
	r.cfg.windows.Storm = sdk.Window{Lo: 0, Hi: time.Second}
	stop := run(r)
	defer stop()
	waitFor(t, "30 online", 10*time.Second, func() bool { s := r.Stats(); return s.RampDone && s.Online == 30 })
	n, err := r.Mark(1, "death", true)
	if err != nil || n != 15 {
		t.Fatalf("mark: %d %v", n, err)
	}
	stopGW[1]()
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("death", time.Second); return s.Back == 15 })
	s, _ := r.Recorder().SLO("death", 5*time.Second)
	if s.Population != 15 || s.Lost != 15 || !s.PassFromMark || !s.PassFromDetection {
		t.Fatalf("slo %+v", s)
	}
	if st := r.Stats(); st.OnlineByGateway[env.GatewayIDs[0]] != 30 || st.Lost["lost"] != 15 {
		t.Fatalf("stats %+v", st)
	}
}

// closeFirst forwards to addr, but closes its first n connections once
// their handshake arrives, before any reply, as a gateway's handshake limit
// does.
func closeFirst(t *testing.T, addr string, n int) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for k := 0; ; k++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			if k < n {
				c.Read(make([]byte, 1)) // after the handshake arrives, as the limiter closes
				c.Close()
				continue
			}
			up, err := net.Dial("tcp", addr)
			if err != nil {
				c.Close()
				continue
			}
			go func() { io.Copy(up, c); up.Close() }()
			go func() { io.Copy(c, up); c.Close() }()
		}
	}()
	return l.Addr().String()
}

// A light client retries a close before the handshake reply after
// 0.5–1.5s, and logs in.
func TestLightRetriesAPreHandshakeClose(t *testing.T) {
	env, _ := startStack(t, 1)
	env.GatewayAddrs[0] = closeFirst(t, env.GatewayAddrs[0], 2)
	r, _ := New(Config{Mode: "light", Clients: 1, Rate: 10, UIDPrefix: "retry", PingEvery: time.Second, Timeout: 5 * time.Second}, env)
	stop := run(r)
	defer stop()
	begin := time.Now()
	waitFor(t, "online", 10*time.Second, func() bool { return r.Stats().Online == 1 })
	if s := r.Stats(); s.Failed["pre-handshake"] != 2 || len(s.Failed) != 1 {
		t.Fatalf("failed %v, want two pre-handshake closes", s.Failed)
	}
	if took := time.Since(begin); took < time.Second {
		t.Fatalf("logged in after %v: retries must wait 0.5–1.5s each", took)
	}
}

func post(t *testing.T, u string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(u, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

// The control API: stats, marks, the SLO, and /kill really sends SIGKILL.
func TestControlAPI(t *testing.T) {
	env, _ := startStack(t, 2)
	r, _ := New(Config{Mode: "light", Clients: 4, Rate: 100, UIDPrefix: "ctl", PingEvery: time.Second, Timeout: 5 * time.Second}, env)
	stop := run(r)
	defer stop()
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	waitFor(t, "4 online", 10*time.Second, func() bool { return r.Stats().Online == 4 })

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var st Stats
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st.Online != 4 || st.Mode != "light" || st.Sources != 1 {
		t.Fatalf("/stats %+v", st)
	}
	if code, _ := post(t, srv.URL+"/mark?gateway=nope&mark=x"); code != http.StatusBadRequest {
		t.Fatalf("/mark unknown gateway: %d", code)
	}
	if code, m := post(t, srv.URL+"/mark?gateway=all&mark=all"); code != http.StatusOK || m["population"] != 4.0 {
		t.Fatalf("/mark all: %d %v", code, m)
	}
	if code, _ := post(t, srv.URL+"/mark?gateway=all&mark=all"); code != http.StatusConflict {
		t.Fatalf("/mark duplicate: %d", code)
	}
	resp, _ = http.Get(srv.URL + "/slo?mark=all&within=30s")
	var s SLO
	json.NewDecoder(resp.Body).Decode(&s)
	resp.Body.Close()
	if s.Population != 4 || s.Gateway != "all" || s.WithinMs != 30_000 {
		t.Fatalf("/slo %+v", s)
	}
	if resp, _ := http.Get(srv.URL + "/slo?mark=nope&within=30s"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/slo unknown mark: %d", resp.StatusCode)
	}

	// /kill: a stand-in process gets SIGKILL; gw-1 leaves the balancer.
	victim := exec.Command("sleep", "30")
	if err := victim.Start(); err != nil {
		t.Fatal(err)
	}
	q := url.Values{"gateway": {env.GatewayIDs[1]}, "pid": {itoa(victim.Process.Pid)}, "mark": {"death"}}
	code, m := post(t, srv.URL+"/kill?"+q.Encode())
	if code != http.StatusOK || m["population"] != 2.0 {
		t.Fatalf("/kill: %d %v", code, m)
	}
	if err := victim.Wait(); err == nil || victim.ProcessState.String() != "signal: killed" {
		t.Fatalf("victim: %v", err)
	}
	if !r.bal.Down(1) {
		t.Fatal("killed gateway still in the balancer")
	}
	// A taken mark name: no signal, gw-0 stays in the balancer.
	other := exec.Command("sleep", "30")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	defer other.Process.Kill()
	q = url.Values{"gateway": {env.GatewayIDs[0]}, "pid": {itoa(other.Process.Pid)}, "mark": {"death"}}
	if code, _ := post(t, srv.URL+"/kill?"+q.Encode()); code != http.StatusConflict {
		t.Fatalf("/kill with a taken mark: %d", code)
	}
	if r.bal.Down(0) || other.Process.Signal(syscall.Signal(0)) != nil {
		t.Fatal("/kill with a taken mark took gw-0 out or signalled")
	}
	if code, _ := post(t, srv.URL+"/kill?gateway="+env.GatewayIDs[0]+"&mark=x"); code != http.StatusBadRequest {
		t.Fatalf("/kill without pid: %d", code)
	}
	if resp, _ := http.Get(srv.URL + "/debug/pprof/goroutine?debug=1"); resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof: %d", resp.StatusCode)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./internal/loadgen/`
Expected: FAIL to build: `undefined: newPinger`, `undefined: lightConn`, `undefined: pingTick`, `undefined: Runner`, `undefined: New`, `undefined: Config`, `undefined: Stats`.

- [ ] **Step 3: Implement**

`internal/loadgen/runner.go`:

```go
package loadgen

import (
	"context"
	"errors"
	"fmt"
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
// gateway) pair, well inside the 28,232-port ephemeral range (#69).
const connsPerSource = 20_000

// Config configures a run.
type Config struct {
	Mode      string        // "light" or "sdk" (choice #80)
	Clients   int           // client i logs in as uid <UIDPrefix>-<i>
	Rate      float64       // clients started per second
	UIDPrefix string        // distinct per loadgen run, so runs never replace each other's sessions
	Sources   int           // source addresses; 0: one per 20,000 connections per gateway
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
	pinger   *pinger
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
		perGW := (cfg.Clients + len(env.GatewayAddrs) - 1) / len(env.GatewayAddrs)
		cfg.Sources = (perGW + connsPerSource - 1) / connsPerSource
	}
	bal, err := NewBalancer(env.GatewayAddrs, cfg.Sources)
	if err != nil {
		return nil, err
	}
	r := &Runner{cfg: cfg, env: env, rec: NewRecorder(cfg.Clients, env.GatewayIDs), bal: bal}
	if cfg.Mode == "light" {
		r.pinger = newPinger(cfg.PingEvery, r.rec)
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

// Kill is POST /kill: the clients logged in on gateway gw become mark
// name's population, gw leaves the balancer, and pid gets SIGKILL. The mark
// is taken just before the signal is sent: T_death, the moment loadgen
// issues SIGKILL (R4M-17). A mark name already taken changes nothing: no
// gateway leaves the balancer and no signal is sent. A dial that picks gw
// between the mark and SetDown is not logged in at the mark, so it is not
// in the population.
func (r *Runner) Kill(gw, pid int, name string) (int, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("loadgen: pid %d", pid)
	}
	n, err := r.Mark(gw, name, true)
	if err != nil {
		return 0, err
	}
	return n, syscall.Kill(pid, syscall.SIGKILL)
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
```

`internal/loadgen/light.go`:

```go
package loadgen

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/sdk"
)

// lightConn is one light client (choice #80): no reconnect once logged in,
// no timers of its own; one goroutine reads, the shared pinger writes Pings.
// Until it logs in, it retries any failure after 0.5–1.5s, at most
// lightAttempts times, as the SDK retries a pre-handshake close: a gateway's
// handshake limit closes the excess of a fast ramp (spec: Admission control).
type lightConn struct {
	i       int
	pingAt  atomic.Int64 // unix ns of the unanswered Ping, 0: none
	reqID   atomic.Uint32
	mu      sync.Mutex
	c       *clientconn.Conn
	stopped bool
}

// stop closes the connection, or keeps it from opening.
func (lc *lightConn) stop() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.stopped = true
	if lc.c != nil {
		lc.c.Close()
	}
}

const lightAttempts = 10

// light logs client lc in and reads until its connection ends.
func (r *Runner) light(ctx context.Context, lc *lightConn) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		gw, why := r.lightConnect(ctx, lc, start.Add(r.cfg.Timeout))
		if why == "" {
			r.rec.Online(lc.i, gw, start, time.Now())
			break
		}
		r.rec.Failed(why)
		if ctx.Err() != nil {
			return
		}
		if attempt == lightAttempts {
			r.rec.Failed("gave-up")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500*time.Millisecond + rand.N(time.Second)):
		}
	}
	defer lc.c.Close()
	r.pinger.add(lc)
	why := lc.read(r.rec)
	r.pinger.remove(lc)
	if ctx.Err() != nil {
		why = "stopped"
	}
	r.rec.Lost(lc.i, why, time.Now())
}

// lightConnect dials, runs the handshake and Login by deadline; why is ""
// once logged in, else what failed (the connection is then closed).
func (r *Runner) lightConnect(ctx context.Context, lc *lightConn, deadline time.Time) (gw int, why string) {
	dctx, cancel := context.WithDeadline(ctx, deadline)
	nc, gw, err := r.bal.Dial(dctx)
	cancel()
	if err != nil {
		return gw, "dial"
	}
	c := clientconn.New(nc)
	lc.mu.Lock()
	lc.c = c
	stopped := lc.stopped
	lc.mu.Unlock()
	if stopped {
		c.Close()
		return gw, "stopped"
	}
	if why = r.lightLogin(c, lc.i, deadline); why != "" {
		c.Close()
	}
	return gw, why
}

// lightLogin runs the handshake and Login; "" means logged in.
func (r *Runner) lightLogin(c *clientconn.Conn, i int, deadline time.Time) string {
	if err := c.Handshake(r.env.NoiseKeyID, r.env.NoisePublic, deadline); errors.Is(err, clientconn.ErrClosedBeforeReply) {
		return "pre-handshake"
	} else if err != nil {
		return "handshake"
	}
	tok, err := r.token(i)
	if err != nil {
		return "token"
	}
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: tok, Appid: AppID,
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: sdk.NewDeviceID(), ConnSeq: 1,
		ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}); err != nil {
		return "login"
	}
	f, err := c.Recv(deadline)
	switch {
	case err != nil:
		return "login"
	case f.Cmd == clientpb.Cmd_CMD_LOGIN_RESP:
		return ""
	case f.Cmd == clientpb.Cmd_CMD_BUSY:
		return "busy"
	case f.Cmd == clientpb.Cmd_CMD_ERROR:
		return "rejected"
	}
	return "login"
}

// read handles frames until the connection ends and says why it ended.
func (lc *lightConn) read(rec *Recorder) string {
	for {
		f, err := lc.c.Recv(time.Time{})
		if err != nil {
			return "lost"
		}
		switch f.Cmd {
		case clientpb.Cmd_CMD_PONG:
			if at := lc.pingAt.Swap(0); at != 0 {
				rec.Pong(time.Duration(time.Now().UnixNano() - at))
			}
		case clientpb.Cmd_CMD_KICK:
			return "kicked"
		case clientpb.Cmd_CMD_RECONNECT:
			return "reconnect"
		}
	}
}

// pinger sends every light client a Ping each PingEvery, spread evenly:
// clients sit in PingEvery/100ms slots by index, and one slot is pinged per
// 100ms tick. A Ping due while the last one is unanswered is counted
// missing.
type pinger struct {
	tick  time.Duration
	rec   *Recorder
	mu    sync.Mutex
	slots []map[*lightConn]struct{}
}

const pingTick = 100 * time.Millisecond

func newPinger(every time.Duration, rec *Recorder) *pinger {
	p := &pinger{tick: pingTick, rec: rec, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick)))}
	for i := range p.slots {
		p.slots[i] = map[*lightConn]struct{}{}
	}
	return p
}

func (p *pinger) add(lc *lightConn) {
	p.mu.Lock()
	p.slots[lc.i%len(p.slots)][lc] = struct{}{}
	p.mu.Unlock()
}

func (p *pinger) remove(lc *lightConn) {
	p.mu.Lock()
	delete(p.slots[lc.i%len(p.slots)], lc)
	p.mu.Unlock()
}

func (p *pinger) run(ctx context.Context) {
	t := time.NewTicker(p.tick)
	defer t.Stop()
	var due []*lightConn
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		due = due[:0]
		for lc := range p.slots[n%len(p.slots)] {
			due = append(due, lc)
		}
		p.mu.Unlock()
		now := time.Now().UnixNano()
		for _, lc := range due {
			if lc.pingAt.Swap(now) != 0 {
				p.rec.PongMissing()
			}
			lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) // a failure ends read
		}
	}
}
```

`internal/loadgen/sdkclient.go`:

```go
package loadgen

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
)

// sdkClient is one sdk-mode client (choice #80): the real SDK with its
// reconnect rules, dialing through the balancer.
type sdkClient struct {
	r      *Runner
	i      int
	gw     atomic.Int32 // gateway of the last dial
	start  time.Time    // the current attempt's start; run loop only
	online bool         // run loop only
}

func (r *Runner) newSDK(i int) (*sdk.Client, error) {
	sc := &sdkClient{r: r, i: i}
	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
		Token: func(context.Context, bool) (string, error) { return r.token(i) },
		Store: &sdk.MemStore{}, OnEvent: sc.event, Windows: r.cfg.windows,
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			nc, gw, err := r.bal.Dial(ctx)
			if err == nil {
				sc.gw.Store(int32(gw))
			}
			return nc, err
		}})
}

// event runs on the client's run loop: it must not block.
func (sc *sdkClient) event(e sdk.Event) {
	now := time.Now()
	switch e.Kind {
	case sdk.EventConnecting:
		sc.start = now
	case sdk.EventOnline:
		sc.online = true
		sc.r.rec.Online(sc.i, int(sc.gw.Load()), sc.start, now)
	case sdk.EventWaiting, sdk.EventIdle, sdk.EventLoginRequired:
		switch {
		case sc.online:
			sc.online = false
			sc.r.rec.Lost(sc.i, e.End.String(), now)
		case e.End != 0:
			sc.r.rec.Failed(e.End.String())
		}
	}
}
```

`internal/loadgen/control.go`:

```go
package loadgen

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Tallinzp/goim/internal/pprofhttp"
)

// Result is what cmd/loadgen writes at exit: the final stats and the SLO of
// every mark.
type Result struct {
	Stats
	SLOs map[string]SLO `json:"slos"`
}

// Result is the final result, SLOs judged against within.
func (r *Runner) Result(within time.Duration) Result {
	res := Result{Stats: r.Stats(), SLOs: map[string]SLO{}}
	for _, name := range r.rec.Marks() {
		res.SLOs[name], _ = r.rec.SLO(name, within)
	}
	return res
}

// Handler is the control API (choice #84), for the scenario scripts:
//
//	GET  /stats                               Stats
//	POST /kill?gateway=gw-1&pid=N&mark=death  SIGKILL and mark T_death  {"mark", "population"}
//	POST /mark?gateway=gw-1|all&mark=drop     a mark; &down=1 also takes the gateway out of the balancer
//	GET  /slo?mark=death&within=30s           SLO
//	/debug/pprof/                             loadgen's own profiles
func (r *Runner) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, r.Stats()) })
	mux.HandleFunc("POST /kill", func(w http.ResponseWriter, q *http.Request) {
		gw := r.env.Gateway(q.FormValue("gateway"))
		pid, err := strconv.Atoi(q.FormValue("pid"))
		if gw < 0 || err != nil || q.FormValue("mark") == "" {
			http.Error(w, "want gateway=<id>&pid=<n>&mark=<name>", http.StatusBadRequest)
			return
		}
		n, err := r.Kill(gw, pid, q.FormValue("mark"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n})
	})
	mux.HandleFunc("POST /mark", func(w http.ResponseWriter, q *http.Request) {
		gw := -1
		if id := q.FormValue("gateway"); id != "all" {
			gw = r.env.Gateway(id)
		}
		if (gw < 0 && q.FormValue("gateway") != "all") || q.FormValue("mark") == "" {
			http.Error(w, "want gateway=<id>|all&mark=<name>", http.StatusBadRequest)
			return
		}
		n, err := r.Mark(gw, q.FormValue("mark"), q.FormValue("down") == "1")
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n})
	})
	mux.HandleFunc("GET /slo", func(w http.ResponseWriter, q *http.Request) {
		within, err := time.ParseDuration(q.FormValue("within"))
		if err != nil || within <= 0 {
			http.Error(w, "want mark=<name>&within=<duration>", http.StatusBadRequest)
			return
		}
		s, err := r.rec.SLO(q.FormValue("mark"), within)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, s)
	})
	pprofhttp.Register(mux)
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `go vet ./internal/loadgen/ && go test -race -count=3 ./internal/loadgen/`
Expected: PASS three times (about 12s each). Under `-race` `TestLightClientsHoldAndPing` typically sees a dozen `pre-handshake` failures that are retried (the gateways' handshake limit at concurrency 1); the test allows them.

- [ ] **Step 5: Mutation check**

Each must fail a test; restore after each: in `Mark`, delete `r.bal.SetDown(gw)` (`TestControlAPI`); in `Mark`, remove the gateway before taking the mark (the order before review) (`TestControlAPI`: a taken mark name must leave gw-0 in the balancer); in `light`, `if attempt == lightAttempts {` → `if true {` (`TestLightRetriesAPreHandshakeClose`); in the pinger, `lc.pingAt.Swap(now) != 0` → `== -1` (`TestPingerCountsMissingPongs`).

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen
git commit -m "feat(loadgen): light and SDK clients, kill/mark and the control API (#80, #84)"
```

---

### Task 5: `cmd/loadgen`, `scripts/loadgen.sh` and the Tier 1a probe

**Files:**
- Create: `cmd/loadgen/main.go`, `cmd/loadgen/main_test.go`, `scripts/loadgen.sh`, `scripts/loadgen_test.sh`, `scripts/load-probe.sh`
- Modify: `scripts/stack.sh` (`stack_build` builds loadgen), `Makefile` (`test-scripts` runs `loadgen_test.sh`; new `test-load-probe`)

**Interfaces:**
- Consumes: Task 4's `loadgen.New`, `Config`, `(*Runner).Run/Handler/Result`, `Result`; `loadgen.LoadEnv`; 6a2a Task 1's `stack_expect_dead`, `stack_snapshot`; 6a1's `stack_build`, `stack_up`, `stack_down`, `stack_health`, `STACK_PIDS`, `T2_OURS_PGIDS`, `T2_CPUS_LOADGEN`.
- Produces: `loadgen -env FILE -mode light|sdk -clients N -rate R -uid-prefix P -sources S -ping-every D -timeout D -control ADDR -duration D -within D -out FILE` (exit 2 on bad flags); shell functions `lg_control`, `lg_start NAME ARGS...`, `lg_py FILE EXPR`, `lg_true FILE EXPR`, `lg_get PATH EXPR`, `lg_post PATH`, `lg_wait_online N SECONDS`, `lg_wait_back MARK SECONDS`, `lg_stop` (sets `LG_STATUS`), `lg_json EXPR`; variables `LG_PID`, `LG_NAME`, `LG_STATUS`; result file `$STACK_DIR/artifacts/loadgen-NAME.json`, log `$STACK_DIR/logs/loadgen-NAME.log`; `make test-load-probe`.

- [ ] **Step 1: Write the failing tests**

`cmd/loadgen/main_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/loadgen"
	"github.com/Tallinzp/goim/internal/wire"
)

func TestRunRefusesBadFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "-env is required"},
		{[]string{"-env", "/nonexistent"}, "no such file"},
		{[]string{"-nope"}, "flag provided but not defined"},
	} {
		err := run(context.Background(), tc.args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
		}
	}
}

// A run against a "gateway" that closes every connection stops at
// -duration and writes a result counting the failed attempts.
func TestRunWritesTheResultAtTheEnd(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Read(make([]byte, 1)) // the handshake has arrived: a close now is before the reply
			c.Close()
		}
	}()
	dir := t.TempDir()
	k, _ := auth.GenerateKey("k1")
	doc, _ := auth.MarshalPrivateJWK(k)
	noise, _ := wire.GenerateKey() // a zero key fails the handshake before any byte is sent
	os.WriteFile(filepath.Join(dir, "jwt.jwk"), doc, 0o600)
	env := "GATEWAY_ADDRS=" + l.Addr().String() + "\nGATEWAY_IDS=gw-0\nNOISE_KEY_ID=1\nNOISE_PUBLIC=" +
		base64.StdEncoding.EncodeToString(noise.Public) + "\nJWT_KEY=" + filepath.Join(dir, "jwt.jwk") + "\n"
	os.WriteFile(filepath.Join(dir, "stack.env"), []byte(env), 0o644)
	out := filepath.Join(dir, "result.json")
	err = run(context.Background(), []string{"-env", filepath.Join(dir, "stack.env"), "-clients", "3", "-rate", "100",
		"-duration", "1s", "-control", "127.0.0.1:0", "-out", out}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var res loadgen.Result
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("result: %v\n%s", err, b)
	}
	if res.Mode != "light" || res.Clients != 3 || res.Started != 3 || res.Online != 0 || res.Failed["pre-handshake"] < 3 {
		t.Fatalf("result %+v", res)
	}
}
```

`scripts/loadgen_test.sh` (make it executable):

```bash
#!/usr/bin/env bash
# Tests for scripts/loadgen.sh's JSON readers on any machine (make
# test-scripts): a result that is missing or broken, e.g. loadgen died
# before writing it, reads as null and false, so a scenario still writes a
# valid result.json with a failed check. Needs python3.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/loadgen.sh
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
fails=0
expect() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got [$2], want [$3]"; fails=$((fails + 1)); fi; }

echo '{"mode": "light", "online": 3, "lost": {"stopped": 3}, "pong_rtt": {"p99_ms": 4}}' >"$dir/r.json"
expect "py: a number" "$(lg_py "$dir/r.json" 'd["online"]')" 3
expect "py: an object as JSON" "$(lg_py "$dir/r.json" 'd["pong_rtt"]')" '{"p99_ms": 4}'
expect "py: a string as JSON" "$(lg_py "$dir/r.json" 'd["mode"]')" '"light"'
expect "py: stdin" "$(lg_py - 'd["online"]' <"$dir/r.json")" 3
expect "py: a missing file is null" "$(lg_py "$dir/none.json" 'd["online"]')" null
echo '{"mode": ' >"$dir/half.json"
expect "py: half a file is null" "$(lg_py "$dir/half.json" 'd["mode"]')" null
expect "py: a missing key is null" "$(lg_py "$dir/r.json" 'd["nope"]')" null
expect "true: a true expression" "$(lg_true "$dir/r.json" 'set(d["lost"]) <= {"stopped"}')" true
expect "true: a false expression" "$(lg_true "$dir/r.json" 'd["online"] > 3')" false
expect "true: a missing file is false" "$(lg_true "$dir/none.json" 'd["online"] == 3')" false
expect "true: a non-bool value is judged by truth" "$(lg_true "$dir/r.json" 'd["online"]')" true

[ "$fails" -eq 0 ] && echo "loadgen_test: all passed" || { echo "loadgen_test: $fails failed"; exit 1; }
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./cmd/loadgen/ ; bash scripts/loadgen_test.sh`
Expected: FAIL: `undefined: run` (no `main.go` yet); `scripts/loadgen.sh: No such file or directory`.

- [ ] **Step 3: Implement**

`cmd/loadgen/main.go`:

```go
// Command loadgen puts load on a goim stack (Plan 6a2). It reads
// scripts/stack.sh's stack.env, starts -clients at -rate through a
// least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
// and serves the control API on -control (GET /stats, POST /kill, POST /mark,
// GET /slo; choice #84) until SIGTERM, Ctrl-C or -duration. It then writes
// the result, the stats at that moment and every mark's SLO against
// -within, as JSON to -out, and only then stops every client: stopping 200k
// SDK clients can outlast the caller's wait, and the result must survive a
// SIGKILL during the teardown.
//
//	loadgen -env $STACK_DIR/stack.env -mode sdk -clients 2000 -rate 500 \
//	    -control 127.0.0.1:17005 -out loadgen.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/loadgen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg loadgen.Config
	envPath := fs.String("env", "", "stack.env written by scripts/stack.sh (required)")
	fs.StringVar(&cfg.Mode, "mode", "light", "light (clientconn + shared pinger) or sdk (one SDK client each)")
	fs.IntVar(&cfg.Clients, "clients", 1000, "clients; client i logs in as <uid-prefix>-<i>")
	fs.Float64Var(&cfg.Rate, "rate", 1000, "clients started per second")
	fs.StringVar(&cfg.UIDPrefix, "uid-prefix", "lg", "uid prefix, distinct per run")
	fs.IntVar(&cfg.Sources, "sources", 0, "source addresses 127.0.0.2…; 0: one per 20,000 connections per gateway")
	fs.DurationVar(&cfg.PingEvery, "ping-every", 150*time.Second, "light: Ping interval (the gateway's -heartbeat)")
	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "light: dial + handshake + Login per attempt")
	control := fs.String("control", "", "control API listen address (none if empty)")
	duration := fs.Duration("duration", 0, "stop after this long (0: at SIGTERM)")
	within := fs.Duration("within", 30*time.Second, "SLO bound for the result's marks")
	out := fs.String("out", "", "result file (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *envPath == "" {
		return errors.New("-env is required")
	}
	env, err := loadgen.LoadEnv(*envPath)
	if err != nil {
		return err
	}
	r, err := loadgen.New(cfg, env)
	if err != nil {
		return err
	}
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	// The runner has its own context, cancelled only once the result is
	// written: the result is the run as it stood, before any client stopped.
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	if *control != "" {
		l, err := net.Listen("tcp", *control)
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: r.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go srv.Serve(l)
		defer srv.Close()
		slog.Info("loadgen: control API", "addr", l.Addr().String())
	}
	slog.Info("loadgen: starting", "mode", cfg.Mode, "clients", cfg.Clients, "rate", cfg.Rate, "gateways", len(env.GatewayAddrs))
	done := make(chan struct{})
	go func() { r.Run(runCtx); close(done) }()
	<-ctx.Done()
	err = writeResult(*out, r.Result(*within))
	stopRun()
	<-done
	return err
}

// writeResult writes res to path (stdout if empty), aside and renamed, so a
// reader never sees half a result.
func writeResult(path string, res loadgen.Result) error {
	doc, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	doc = append(doc, '\n')
	if path == "" {
		_, err = os.Stdout.Write(doc)
		return err
	}
	if err := os.WriteFile(path+".tmp", doc, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
```

`scripts/loadgen.sh`:

```bash
# shellcheck shell=bash
# loadgen.sh: run cmd/loadgen against a scripts/stack.sh stack (Plan 6a2).
# Source it after stack.sh; stack_build has built bin/loadgen. Then:
#
#   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env
#   lg_wait_online N SECONDS    # until the ramp is done and N clients are logged in
#   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
#   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
#   lg_wait_back MARK SECONDS   # until every client of MARK logged in again
#   lg_stop                     # SIGTERM, then wait (60s) for it to write its result; LG_STATUS = exit status
#   lg_json EXPR                # EXPR over the result file of the last loadgen
#   lg_true SOURCE EXPR         # "true" if EXPR is true over SOURCE (a file, or - for stdin), else "false"
#
# Values print as JSON (null when the JSON cannot be read, e.g. loadgen died
# before writing it), so they can go straight into t2_check members.
#
# Logs go to $STACK_DIR/logs/loadgen-NAME.log, the result (final stats and
# every mark's SLO) to $STACK_DIR/artifacts/loadgen-NAME.json. The control
# API listens on STACK_PORT_BASE + 5. With STACK_PIN=1 loadgen is pinned to
# T2_CPUS_LOADGEN (choice #71). Like stack.sh's processes, loadgen runs in
# its own process group, added to T2_OURS_PGIDS.

LG_PID=""
LG_NAME=""
LG_STATUS=""

lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5))"; }

lg_start() {
  LG_NAME=$1
  shift
  local cmd=("$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
    -out "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$@")
  if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "$T2_CPUS_LOADGEN" "${cmd[@]}"); fi
  mkdir -p "$STACK_DIR/artifacts" "$STACK_DIR/logs"
  rm -f "$STACK_DIR/artifacts/loadgen-$LG_NAME.json"
  setsid "${cmd[@]}" >"$STACK_DIR/logs/loadgen-$LG_NAME.log" 2>&1 &
  LG_PID=$!
  T2_OURS_PGIDS+=("$LG_PID")
  local i
  for i in $(seq 100); do
    curl -sf -o /dev/null "http://$(lg_control)/stats" && return 0
    kill -0 "$LG_PID" 2>/dev/null || break
    sleep 0.1
  done
  echo "loadgen: control API not up; last log lines:" >&2
  tail -n 20 "$STACK_DIR/logs/loadgen-$LG_NAME.log" >&2
  return 1
}

# lg_py FILE EXPR: EXPR evaluated with d = the JSON in FILE (- for stdin),
# printed as JSON; null if the file or EXPR fails.
lg_py() {
  python3 -c 'import json, sys
try:
    d = json.load(sys.stdin if sys.argv[1] == "-" else open(sys.argv[1]))
    print(json.dumps(eval(sys.argv[2], {"d": d})))
except Exception:
    print("null")' "$1" "$2"
}

lg_true() { [ "$(lg_py "$1" "bool($2)")" = true ] && echo true || echo false; }

lg_get() { curl -sf "http://$(lg_control)$1" | lg_py - "$2"; }

lg_post() { curl -sf -X POST "http://$(lg_control)$1"; }

lg_wait_online() {
  local i
  for i in $(seq $(($2 * 2))); do
    [ "$(lg_get /stats 'd["ramp_done"] and d["online"] >= '"$1")" = true ] && return 0
    kill -0 "$LG_PID" 2>/dev/null || { echo "loadgen exited" >&2; return 1; }
    sleep 0.5
  done
  echo "loadgen: $(lg_get /stats 'd["online"]') of $1 online after $2s; failed: $(lg_get /stats 'd["failed"]')" >&2
  return 1
}

lg_wait_back() {
  local i
  for i in $(seq $(($2 * 2))); do
    [ "$(lg_get "/slo?mark=$1&within=30s" 'd["back"] == d["population"]')" = true ] && return 0
    sleep 0.5
  done
  return 1
}

# lg_stop sets LG_STATUS rather than printing it: in $(...) it would run in a
# subshell, which cannot wait for the loadgen process.
lg_stop() {
  [ -n "$LG_PID" ] || return 0
  kill -TERM "$LG_PID" 2>/dev/null
  local i
  for i in $(seq 600); do
    kill -0 "$LG_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -KILL "$LG_PID" 2>/dev/null
  wait "$LG_PID" 2>/dev/null
  LG_STATUS=$?
  LG_PID=""
}

lg_json() { lg_py "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$1"; }
```

`stack_build` builds loadgen:

```diff
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -5,7 +5,7 @@
 # core type (choice #71); the Tier 1a load probe (Plan 6a2) runs it unpinned.
 # Source it, set STACK_DIR, then:
 #
-#   stack_build                 # binaries into $STACK_DIR/bin
+#   stack_build                 # binaries into $STACK_DIR/bin (loadgen too, for scripts/loadgen.sh)
 #   stack_up                    # keys, processes, waits until all are Ready
 #   stack_snapshot NAME         # /metrics, heap and goroutine profiles of each process + redis INFO
 #                               #   -> $STACK_DIR/artifacts/NAME/
@@ -44,7 +44,7 @@
 
 stack_build() {
   mkdir -p "$STACK_DIR/bin"
-  go build -o "$STACK_DIR/bin/" ./cmd/gateway ./cmd/logic ./cmd/devtoken
+  go build -o "$STACK_DIR/bin/" ./cmd/gateway ./cmd/logic ./cmd/devtoken ./cmd/loadgen
 }
 
 # stack_start NAME CPUS CMD...: starts CMD in the background in its own
```

`scripts/load-probe.sh` (make it executable):

```bash
#!/usr/bin/env bash
# Tier 1a load probe (spec: Test execution model, "small load probe (≤ 5k
# connections)"; Plan 6a2): a real stack of 2 gateways (scripts/stack.sh,
# unpinned, production timing) and cmd/loadgen, on the dev machine.
#
#   1. light: 3,000 connections log in, Ping every 5s for 15s: every Ping
#      answered, no connection lost
#   2. sdk:   2,000 SDK clients log in; loadgen SIGKILLs gw-1 (POST /kill):
#      99% of its clients log in again within 30s of T_death, and the
#      killed gateway is the only process that did not stop cleanly
#
# Usage: make test-load-probe (needs redis-server from make redis-server,
# curl and python3). Exit 0 if every check passes, else 1. The run's
# directory (logs, loadgen results, snapshots) is kept on failure, or with
# PROBE_KEEP=1.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/stack.sh
. scripts/loadgen.sh
STACK_DIR=$(mktemp -d -t goim-probe.XXXXXX)
STACK_GATEWAYS=2
STACK_PORT_BASE=${STACK_PORT_BASE:-17200}
STACK_GATEWAY_ARGS=(-max-conns 6000 -pending-cap 6000 -logout-queue 6000)
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
check() { if [ "$2" = true ]; then ok "$1"; else bad "$1: $3"; fi; }
cleanup() {
  lg_stop
  stack_down
  if [ "$fails" -eq 0 ] && [ "${PROBE_KEEP:-0}" != 1 ]; then rm -rf "$STACK_DIR"; else echo "probe: results in $STACK_DIR"; fi
}
trap cleanup EXIT

stack_build >"$STACK_DIR/build.log" 2>&1 || { bad "build: see $STACK_DIR/build.log"; exit 1; }
mkdir -p "$STACK_DIR/logs"
stack_up 2>"$STACK_DIR/stack-up.err" || { bad "stack: $(cat "$STACK_DIR/stack-up.err")"; exit 1; }
ok "stack: 2 gateways Ready"

# 1: light.
lg_start light -mode light -clients 3000 -rate 1000 -ping-every 5s -uid-prefix light || { bad "light: loadgen did not start"; exit 1; }
if lg_wait_online 3000 60; then ok "light: 3000 online"; else bad "light: not all online"; fi
check "light: spread over both gateways" "$(lg_get /stats 'min(d["online_by_gateway"].values()) >= 1400')" "$(lg_get /stats 'd["online_by_gateway"]')"
sleep 15
stack_snapshot light
lg_stop
check "light: loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
check "light: every Ping answered" "$(lg_json 'd["pong_missing"] == 0 and d["pongs"] >= 3000 * 2')" \
  "pongs $(lg_json 'd["pongs"]'), missing $(lg_json 'd["pong_missing"]')"
check "light: none lost while held" "$(lg_json 'set(d["lost"]) <= {"stopped"}')" "$(lg_json 'd["lost"]')"
echo "     light: login p99 $(lg_json 'd["login_latency"]["p99_ms"]')ms, Pong p99 $(lg_json 'd["pong_rtt"]["p99_ms"]')ms, failed $(lg_json 'd["failed"]')"

# 2: sdk, and a gateway killed by loadgen.
lg_start sdk -mode sdk -clients 2000 -rate 500 -uid-prefix sdk || { bad "sdk: loadgen did not start"; exit 1; }
if lg_wait_online 2000 60; then ok "sdk: 2000 online"; else bad "sdk: not all online"; fi
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - '"population %d" % d["population"]')
# Not in $(...): stack_expect_dead reaps gw-1 and forgets it in this shell.
if stack_expect_dead gw-1; then ok "sdk: loadgen killed gw-1 ($reply)"; else bad "sdk: gw-1 not killed: $reply"; fi
lg_wait_back death 45 || true
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
check "sdk: 99% of gw-1's clients back within 30s of T_death" "$(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["pass_from_mark"] and d["population"] >= 900')" \
  "$(cat "$STACK_DIR/artifacts/slo-death.json")"
echo "     sdk: population $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["population"]'), from T_death p50 $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["from_mark"]["p50_ms"]')ms p99 $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["from_mark"]["p99_ms"]')ms"
check "sdk: all 2000 online on gw-0 again" "$(lg_get /stats 'd["online_by_gateway"]["gw-0"] == 2000')" "$(lg_get /stats 'd["online_by_gateway"]')"
lg_stop
check "sdk: loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
check "sdk: the result holds the death SLO" "$(lg_json '"death" in d["slos"]')" "no slos.death"

stack_down
problems=$(stack_health)
check "processes: only the killed gateway stopped uncleanly, and it is expected" "$([ -z "$problems" ] && echo true)" "$problems"

[ "$fails" -eq 0 ] && echo "load-probe: all passed" || { echo "load-probe: $fails failed"; exit 1; }
```

`Makefile`:

```diff
--- a/Makefile
+++ b/Makefile
@@ -7,7 +7,7 @@
 COMPOSE_REDIS := docker compose -f deploy/compose/redis-test.yml
 TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM_TEST_TIER=2"; exit 1; }
 
-.PHONY: test test-race-suite test-scripts redis-server test-redis-local proto redis-up redis-down test-redis test-tier2
+.PHONY: test test-race-suite test-scripts test-load-probe redis-server test-redis-local proto redis-up redis-down test-redis test-tier2
 
 # Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
 test:
@@ -20,13 +20,22 @@
 test-race-suite:
 	GOIM_RACE_SUITE=1 GOIM_REDIS_SERVER=$(REDIS_SERVER) go test -count=1 -timeout 30m ./internal/racetest/
 
-# The Tier 2 and load-run scripts (Plan 6a1), on any machine: lib.sh's guard,
-# result.json, recording and host-setup checks, and a real stack.sh stack.
+# The Tier 2 and load-run scripts (Plans 6a1, 6a2), on any machine: lib.sh's
+# guard, result.json, recording and host-setup checks, loadgen.sh's JSON
+# readers, and a real stack.sh stack.
 # Needs python3, curl and redis-server (make redis-server).
 test-scripts:
 	scripts/tier2/lib_test.sh
+	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 
+# The Tier 1a load probe (Plan 6a2, spec: ≤ 5k connections), on the dev
+# machine: a 2-gateway stack.sh stack and cmd/loadgen, light then sdk with a
+# gateway killed by loadgen. Not in make test (about 2 minutes). Needs
+# redis-server (make redis-server), curl and python3.
+test-load-probe:
+	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/load-probe.sh
+
 # The race suite's Redis: redis-server on PATH, else the pinned 7.4.11 that
 # make redis-server builds into .cache/redis (cc and make, no Docker).
 REDIS_SERVER ?= $(or $(shell command -v redis-server 2>/dev/null),$(CURDIR)/.cache/redis/bin/redis-server)
```

- [ ] **Step 4: Run the tests and the probe**

Run: `go vet ./cmd/loadgen/ && go test -race -count=1 ./cmd/loadgen/ && make test-scripts`
Expected: PASS; `loadgen_test: all passed`, `stack_test: all passed`.

Run: `make test-load-probe`
Expected (about 50s on the dev machine): every line `ok`, ending `load-probe: all passed`; the prototype printed `light: login p99 ~200ms, Pong p99 ~45ms, failed {}` and `sdk: population 1000, from T_death p50 ~10s p99 19800ms` (the 0–20s storm window). A bash `Killed` line for gw-1 is the deliberate SIGKILL.

- [ ] **Step 5: Mutation check**

In `scripts/load-probe.sh`, wrap the `stack_expect_dead gw-1` call in `$(...)` (`if [ -n "$(stack_expect_dead gw-1; echo x)" ]; then`): the probe must fail `processes: …` (gw-1 reported). In `cmd/loadgen/main.go`, write the result after `<-done` instead of before: no test catches it on the dev machine (the stop is fast at 3 clients), so check the order by reading `run` (Review Focus 4). Restore.

- [ ] **Step 6: Commit**

```bash
git add cmd/loadgen scripts/loadgen.sh scripts/loadgen_test.sh scripts/load-probe.sh scripts/stack.sh Makefile
git commit -m "feat(loadgen): cmd/loadgen, loadgen.sh and the Tier 1a load probe (#84, #86)"
```

---

### Task 6: the Tier 2 `smoke` scenario

**Files:**
- Create: `scripts/tier2/smoke.sh`
- Modify: `Makefile` (the scenario list in the `test-tier2` comment)

**Interfaces:**
- Consumes: Task 5's `scripts/loadgen.sh`; 6a2a Task 1's `stack_profile`, `stack_expect_dead`, extended `stack_snapshot`; 6a1's `lib.sh` (`t2_init`, `t2_need`, `t2_require_host_setup`, `t2_mem_budget`, `t2_nofile`, `t2_refuse`, `t2_check`, `t2_field`, `t2_duration`, `t2_record_start/end NAME`, `t2_on_exit`, `t2_report_extra`, `t2_section`, `t2_not_covered`, `t2_finish`, `t2_bool`, `t2_json_str`, `t2_cpulist_expand`) and `stack.sh`.
- Produces: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=smoke`; `result.json` with checks `stack`, `light_online`, `pinning`, `no_swap_in_light`, `light_pongs`, `light_held`, `sdk_online`, `slo_b1`, `artifacts`, `processes`, fields `light`, `sdk` (each with `rss_mb`: loadgen and every gateway, #86 amended), `background_light`, durations `setup`, `light_ramp`, `sdk_ramp`. `SMOKE_LIGHT` and `SMOKE_SDK` override the client counts (dry runs).

- [ ] **Step 1: Write the scenario**

`scripts/tier2/smoke.sh` (make it executable):

```bash
#!/usr/bin/env bash
# Tier 2: a smoke load on the Tier 2 host (Plan 6a2), so the load pipeline is
# proven end to end before Plan 6b's measurements: cmd/loadgen pinned to the
# loadgen cores (#71) from 127.0.0.2… (#69), 4 pinned gateways, a recorded
# window with CPU profiles, and the storm SLO computed by loadgen after it
# SIGKILLs a gateway. The scale is a fraction of 6b's; no capacity number is
# judged here.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=smoke
#   Results: tier2-results/smoke-<timestamp>/ (result.json, stack/ with logs,
#   artifacts and loadgen results), plus ONE file to send back:
#   tier2-results/smoke-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. stack:            redis, devtoken, Logic and gw-0..gw-3 become Ready
#   2. pinning:          gateways, Logic + Redis and loadgen on their CPU sets
#   3. light_online:     100,000 light connections log in (25,000 per gateway)
#   4. light_pongs:      a Ping every 10s for 60s: none unanswered
#   5. light_held:       no connection lost during the window
#   6. no_swap_in_light: no page swapped in during the window
#   7. sdk_online:       20,000 SDK clients log in
#   8. slo_b1:           loadgen SIGKILLs gw-3: 99% of its clients log in
#                        again within 30s of T_death
#   9. artifacts:        metrics, heap, goroutine and CPU profiles of every
#                        process, and both loadgen results
#  10. processes:        nothing but the killed gateway died, panicked or
#                        stopped uncleanly
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init smoke
t2_need curl taskset sudo python3
t2_require_host_setup
t2_mem_budget 8
t2_nofile 1048576

export STACK_DIR=$T2_OUT/stack STACK_GATEWAYS=4 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LIGHT=${SMOKE_LIGHT:-100000} SDK=${SMOKE_SDK:-20000}
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# rss_json: resident memory in MB of loadgen and each running gateway, so
# Plan 6b sizes 600k light and 200k SDK clients per process from measured
# bytes per connection (P5I-2), not estimates.
rss_mb() {
  local v
  v=$(awk '/^VmRSS:/ { printf "%d", $2 / 1024 }' "/proc/$1/status" 2>/dev/null)
  echo "${v:-null}"
}
rss_json() {
  local out="\"loadgen\": $(rss_mb "$LG_PID")" name
  for name in "${STACK_ORDER[@]}"; do
    case $name in gw-*) out+=", \"$name\": $(rss_mb "${STACK_PIDS[$name]}")" ;; esac
  done
  echo "{$out}"
}

t2_report_extra() {
  local f
  for f in "$STACK_DIR"/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "$(basename "$f") (last 30 lines)"
    tail -n 30 "$f"
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

# 1: the stack.
busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$T2_OUT/stack-up.err"
up=$?
t2_check stack "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$T2_OUT/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# 3-6: light connections, held through a recorded window.
t=$SECONDS
lg_start light -mode light -clients "$LIGHT" -rate 5000 -ping-every 10s -uid-prefix light
online=$(t2_bool lg_wait_online "$LIGHT" 120)
t2_duration light_ramp $((SECONDS - t))
t2_check light_online "$online" "\"want\": $LIGHT, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"failed\": $(lg_get /stats 'd["failed"]')"

# 2: pinning, loadgen included, while it runs.
wrong=""
for name in "${STACK_ORDER[@]}"; do
  case $name in gw-*) want=$T2_CPUS_GATEWAY ;; *) want=$T2_CPUS_LOGIC ;; esac
  stack_pinned "$name" "$want" || wrong+="${wrong:+ }$name"
done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+="${wrong:+ }loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"wrong\": $(t2_json_str "$wrong")"

t2_record_start light
stack_profile light 30 & wait $! # CPU profiles over the window's first 30s
sleep 30 & wait $!
t2_record_end light
stack_snapshot light
rss=$(rss_json)
lg_stop
t2_field light "{\"exit\": $LG_STATUS, \"rss_mb\": $rss, \"login_latency\": $(lg_json 'd["login_latency"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"failed\": $(lg_json 'd["failed"]'), \"sources\": $(lg_json 'd["sources"]')}"
res=$STACK_DIR/artifacts/loadgen-light.json
t2_check light_pongs "$(lg_true "$res" 'd["pong_missing"] == 0 and d["pongs"] >= '"$LIGHT"' * 5')" "\"pongs\": $(lg_json 'd["pongs"]'), \"missing\": $(lg_json 'd["pong_missing"]')"
t2_check light_held "$(lg_true "$res" 'set(d["lost"]) <= {"stopped"}')" "\"lost\": $(lg_json 'd["lost"]')"

# 7-8: SDK clients, and a gateway killed by loadgen (b1 in small).
t=$SECONDS
lg_start sdk -mode sdk -clients "$SDK" -rate 2000 -uid-prefix sdk
online=$(t2_bool lg_wait_online "$SDK" 60)
t2_duration sdk_ramp $((SECONDS - t))
t2_check sdk_online "$online" "\"want\": $SDK, \"online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]')"
rss=$(rss_json)
reply=$(lg_post "/kill?gateway=gw-3&pid=${STACK_PIDS[gw-3]}&mark=death" | lg_py - d)
stack_expect_dead gw-3 # in this shell: it reaps and forgets gw-3
dead=$?
lg_wait_back death 45
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
slo=$(cat "$STACK_DIR/artifacts/slo-death.json")
pass=false
[ $dead -eq 0 ] && pass=$(lg_true "$STACK_DIR/artifacts/slo-death.json" 'd["pass_from_mark"] and d["population"] >= '"$SDK"' // 5')
t2_check slo_b1 "$pass" "\"kill\": ${reply:-null}, \"slo\": ${slo:-null}"
lg_stop
t2_field sdk "{\"exit\": $LG_STATUS, \"rss_mb\": $rss, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]')}"

# 9: artifacts.
missing=""
for f in logic gw-0 gw-1 gw-2 gw-3; do
  a=$STACK_DIR/artifacts/light
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
  gzip -t "$a/$f.heap.pb.gz" 2>/dev/null || missing+=" $f.heap"
  grep -q 'goroutine profile:' "$a/$f.goroutine.txt" 2>/dev/null || missing+=" $f.goroutine"
  gzip -t "$a/$f.cpu.pb.gz" 2>/dev/null || missing+=" $f.cpu"
done
for f in light sdk; do
  [ "$(lg_true "$STACK_DIR/artifacts/loadgen-$f.json" '"mode" in d')" = true ] || missing+=" loadgen-$f.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

# 10: process health, after a clean stop.
stack_down
problems=$(stack_health)
t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

t2_not_covered "scale: $LIGHT light and $SDK SDK clients, not Plan 6b's 600k and 200k" \
  "b2, storms, floods and slowed Logic (Plan 6b)" "Redis Cluster (Plan 6c)"
t2_finish
```

`Makefile`:

```diff
--- a/Makefile
+++ b/Makefile
@@ -71,7 +71,7 @@
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
```

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, so check the script in a throwaway copy with the guard lowered and the root parts removed (as 6a1 dry-ran `foundation.sh`):

```bash
D=$(mktemp -d); cp scripts/tier2/smoke.sh "$D/smoke.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget 8$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/smoke.sh"
sed -i 's|^export STACK_DIR=$T2_OUT/stack STACK_GATEWAYS=4 STACK_PIN=1$|&; STACK_GATEWAY_ARGS=(-max-conns 3000 -pending-cap 3000 -logout-queue 3000)|' "$D/smoke.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/smoke.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 SMOKE_LIGHT=4000 SMOKE_SDK=2000 bash "$D/smoke.sh"; echo "exit $?"
```

Check the three `sed -i` lines changed what they should (`grep -n '^\. \|^:$\|t2_mem_budget\|STACK_GATEWAY_ARGS' "$D/smoke.sh"`). Expected (prototype, ~95s): `result.json` parses (`python3 -m json.tool tier2-results/smoke-*/result.json`); `stack`, `light_online` (4,000, 1,000 per gateway), `pinning`, `light_pongs`, `light_held`, `sdk_online`, `slo_b1` (population 500, p99 ~19.7s), `artifacts` and `processes` pass; `no_swap_in_light` passes or fails with the dev machine's own swapping (the prototype saw 5 pages in: 1GB available); exit 1 if it fails, else 0. No process is left (`pgrep -f "$PWD/tier2-results/smoke" || echo none`). Remove the dry run's `tier2-results/smoke-*`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/smoke.sh Makefile
git commit -m "feat(tier2): smoke load scenario (#86)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-scripts && make test-load-probe && bash -n scripts/tier2/smoke.sh`
Expected: all pass. Record the commits, the probe's numbers and the dry run in Plan 6a2a's *Execution Notes*; push the branch.

- [ ] **Step 5: Tier 2 run 13 (on the host)**

The user runs, on the Tier 2 host after `git pull`, with the desktop apps closed: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=smoke`, then `SCENARIO=foundation` (the stack helpers changed), and sends back the two `.report.txt` files. Expected: `smoke` 10/10 checks; `light_online` 100,000 with 25,000 per gateway; `slo_b1` population ~5,000 with p99 under 20.5s; `background_light` shows no swap-in and the gateway set's `cpu_clocks` under load (6a1 note: idle clocks mean nothing); `foundation` 6/6. Record the run in 6a2a's *Execution Notes*, including `light.rss_mb` and `sdk.rss_mb` (loadgen's and the gateways' bytes per connection, for 6b's sizing) and the light ramp's `failed` counts (pre-handshake closes at 5,000/s against 4 gateways with handshake concurrency 2 each), which size 6b's ramp rate.

---

## After this plan

- **Plan 6b** builds every Tier 2 load scenario on `smoke.sh`'s steps: (a) light at 600k on one gateway (`-sources` defaults to 30), (b1) `/kill` + `pass_from_mark`, (b2) `goim-netdrop add` + `/mark` + `pass_from_detection`, (b-stress), (c) light with `-ping-every 150s`/`60s` and the gateway's `-heartbeat`, storms, floods, whole-population reconnect (`/mark?gateway=all`), slowed Logic. Its ramp rate comes from run 13's `failed` counts. If 200k SDK clients cost too much memory in one loadgen process (P5I-2: ~25–35KB each; run 13's `sdk.rss_mb` measures it), run two loadgen processes with distinct `-uid-prefix` and control ports, or fold the SDK pinger into the reader (P5I-2's option).
- **Plan 6d:** the admin port carries pprof (#85); keep it off any externally reachable Service.
