package loadgen

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"slices"
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

// gatewayRegs holds each started gateway's registry by its id (stack
// metrics for the flood tests).
var gatewayRegs sync.Map

// stackOpt changes each gateway's core and transport configs (the
// handshake limit is the transport's).
type stackOpt func(*gateway.Config, *tcp.Config)

// startStack starts Logic and n gateways, each config passed through opts;
// stop[i] stops gateway i's transport (its clients see their connections
// close) and core.
func startStack(t *testing.T, n int, opts ...stackOpt) (env Env, stop []func()) {
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
		for _, o := range opts {
			o(&cfg, &tcfg)
		}
		greg := prometheus.NewRegistry()
		gatewayRegs.Store(cfg.GatewayID, greg)
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

// b_stress's ramp (#93): with gw-1 down from the start every client logs in
// on gw-0; after /up the next dials go to gw-1.
func TestRampAloneThenUp(t *testing.T) {
	env, _ := startStack(t, 2)
	r, err := New(Config{Mode: "light", Clients: 4, Rate: 100, UIDPrefix: "alone", PingEvery: time.Second,
		Timeout: 5 * time.Second, Down: []string{env.GatewayIDs[1]}}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	defer stop()
	waitFor(t, "4 online", 10*time.Second, func() bool { return r.Stats().Online == 4 })
	if st := r.Stats(); st.OnlineByGateway[env.GatewayIDs[0]] != 4 {
		t.Fatalf("by gateway %v, want all 4 on gw-0", st.OnlineByGateway)
	}
	r.Up(1)
	c, gw, err := r.bal.Dial(context.Background())
	if c != nil {
		c.Close() // frees its balancer slot
	}
	if err != nil || gw != 1 {
		t.Fatalf("dial after Up: gw %d %v, want gw 1", gw, err)
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

// refuseFirst forwards to addr like closeFirst, but reads each connection's
// whole handshake frame first: it closes the first n after their frame, as
// a gateway's handshake limit does, and sends the rest on with their frame.
// frames returns the refused and the forwarded frames (#109).
func refuseFirst(t *testing.T, addr string, n int) (string, func() (refused, passed [][]byte)) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	var mu sync.Mutex
	var refused, passed [][]byte
	go func() {
		for k := 0; ; k++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(k int) {
				f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
				if _, err := io.ReadFull(c, f); err != nil {
					c.Close()
					return
				}
				mu.Lock()
				if k < n {
					refused = append(refused, f)
				} else {
					passed = append(passed, f)
				}
				mu.Unlock()
				if k < n {
					c.Close()
					return
				}
				up, err := net.Dial("tcp", addr)
				if err != nil {
					c.Close()
					return
				}
				if _, err := up.Write(f); err != nil {
					up.Close()
					c.Close()
					return
				}
				go func() { io.Copy(up, c); up.Close() }()
				io.Copy(c, up)
				c.Close()
			}(k)
		}
	}()
	return l.Addr().String(), func() ([][]byte, [][]byte) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(refused), slices.Clone(passed)
	}
}

// #109, review O4: through the real SDK and clientconn, a client whose
// first handshake the gateway refused sends the same frame again and logs
// in with it. The clients start 33ms apart and retry after 0.5–1.5s, so the
// first three connections are the three clients' first attempts.
func TestSDKResendsARefusedHandshake(t *testing.T) {
	env, _ := startStack(t, 1)
	addr, frames := refuseFirst(t, env.GatewayAddrs[0], 3)
	env.GatewayAddrs[0] = addr
	r, err := New(Config{Mode: "sdk", Clients: 3, Rate: 30, UIDPrefix: "reuse"}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	defer stop()
	waitFor(t, "3 online", 10*time.Second, func() bool { return r.Stats().Online == 3 })
	if s := r.Stats().StormSinceMark; s.HandshakesInline != 3 || s.HandshakesReused != 3 || s.HandshakesPrecomputed != 0 {
		t.Fatalf("storm %+v: want 3 inline, 3 reused", s)
	}
	refused, passed := frames()
	if len(refused) != 3 || len(passed) != 3 {
		t.Fatalf("%d refused, %d passed: want 3 and 3", len(refused), len(passed))
	}
	for _, p := range passed {
		if !slices.ContainsFunc(refused, func(f []byte) bool { return bytes.Equal(f, p) }) {
			t.Fatal("a client logged in with a frame the gateway had not refused")
		}
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
	resp, err = http.Get(srv.URL + "/runtime")
	if err != nil {
		t.Fatal(err)
	}
	var rt Runtime
	json.NewDecoder(resp.Body).Decode(&rt)
	resp.Body.Close()
	if rt.Goroutines == 0 || rt.MappedBytes == 0 {
		t.Fatalf("/runtime %+v", rt)
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
	// /up puts it back (a restarted gateway, or b_stress's survivors).
	if code, m := post(t, srv.URL+"/up?gateway="+env.GatewayIDs[1]); code != http.StatusOK || m["gateway"] != env.GatewayIDs[1] {
		t.Fatalf("/up: %d %v", code, m)
	}
	if r.bal.Down(1) {
		t.Fatal("/up left the gateway down")
	}
	if code, _ := post(t, srv.URL+"/up?gateway=nope"); code != http.StatusBadRequest {
		t.Fatalf("/up unknown gateway: %d", code)
	}
	r.bal.SetDown(1)
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
	// A pid that is gone: no mark, gw-0 stays in the balancer.
	gone := exec.Command("true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	q = url.Values{"gateway": {env.GatewayIDs[0]}, "pid": {itoa(gone.Process.Pid)}, "mark": {"stale"}}
	if code, _ := post(t, srv.URL+"/kill?"+q.Encode()); code != http.StatusConflict {
		t.Fatalf("/kill with a stale pid: %d", code)
	}
	if _, err := r.Recorder().SLO("stale", time.Second); r.bal.Down(0) || err == nil {
		t.Fatal("/kill with a stale pid took gw-0 out or took the mark")
	}
	if code, _ := post(t, srv.URL+"/kill?gateway="+env.GatewayIDs[0]+"&mark=x"); code != http.StatusBadRequest {
		t.Fatalf("/kill without pid: %d", code)
	}
	if resp, _ := http.Get(srv.URL + "/debug/pprof/goroutine?debug=1"); resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof: %d", resp.StatusCode)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// #106, #107: with Precompute every logged-in client holds its next
// handshake frame before the mark; each victim's first attempt after it uses
// its own once, nobody prepares after the mark, every attempt after a wait
// records its lag and every handshake its reply lag.
func TestSDKStormUsesPrecomputedHandshakes(t *testing.T) {
	env, stopGW := startStack(t, 2)
	r, err := New(Config{Mode: "sdk", Clients: 30, Rate: 300, UIDPrefix: "pre", Precompute: true}, env)
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.windows = sdk.DefaultWindows()
	r.cfg.windows.Storm = sdk.Window{Lo: 0, Hi: time.Second}
	stop := run(r)
	defer stop()
	waitFor(t, "30 online and prepared", 10*time.Second, func() bool {
		s := r.Stats()
		return s.RampDone && s.Online == 30 && s.Precompute != nil && s.Precompute.Ready == 30 && s.Precompute.Pending == 0
	})
	n, err := r.Mark(1, "death", true)
	if err != nil || n != 15 {
		t.Fatalf("mark: %d %v", n, err)
	}
	stopGW[1]()
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("death", time.Second); return s.Back == 15 })
	st := r.Stats()
	s := st.StormSinceMark
	if s.Mark != "death" || s.HandshakesPrecomputed != 15 || s.AttemptLag.N < 15 || s.AttemptLag.Max > 1000 ||
		s.ConnectLag.N < 15 || s.ConnectLag.Max > 1000 || s.SendLag.N < 15 || s.SendLag.Max > 1000 || s.ReplyLag.N < 15 || s.ReplyLag.Max > 1000 {
		t.Fatalf("storm since mark %+v", s)
	}
	if !st.Precompute.Stopped || st.Precompute.Ready != 15 {
		t.Fatalf("precompute %+v: want stopped, the survivor's 15 still prepared", st.Precompute)
	}
}
