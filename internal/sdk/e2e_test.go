package sdk

import (
	"context"
	"crypto/ed25519"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The end-to-end tests run the SDK over real TCP and Noise against real
// gateways (gnet) and Plan 2's Logic on miniredis (the real Redis at Tier 2),
// on real time.

// staticKeys is a JWT key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

// stack is Logic and its gateways.
type stack struct {
	t        *testing.T
	rdb      *redis.Client
	jwt      auth.PrivateKey
	noise    wire.Key
	grpcAddr string
	lease    string

	mu    sync.Mutex
	kicks map[string]string // gateway id → Kick address
}

func newStack(t *testing.T) *stack {
	t.Helper()
	st := &stack{t: t, rdb: testredis.New(t), kicks: map[string]string{}}
	var err error
	if st.jwt, err = auth.GenerateKey("k1"); err != nil {
		t.Fatal(err)
	}
	if st.noise, err = wire.GenerateKey(); err != nil {
		t.Fatal(err)
	}
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	lsc := logic.DefaultServerConfig()
	lsc.GRPCAddr, lsc.LeaseAddr, lsc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	reg := prometheus.NewRegistry()
	srv, err := logic.NewServer(lcfg, lsc, logic.Deps{Store: store.New(st.rdb, lcfg.Store), Keys: staticKeys{st.jwt},
		Dial: func(gw string) (*grpc.ClientConn, error) {
			st.mu.Lock()
			addr := st.kicks[gw]
			st.mu.Unlock()
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	st.grpcAddr, st.lease, _ = srv.Addrs()
	return st
}

// gw is one running gateway.
type gw struct {
	id   string
	g    *gateway.Gateway
	tr   *tcp.Transport
	addr string
}

// gateway starts a gateway; mod changes its configuration.
func (st *stack) gateway(mod func(*gateway.Config)) *gw {
	t := st.t
	t.Helper()
	cfg := gateway.DefaultConfig()
	cfg.GatewayID, cfg.Epoch, cfg.NumLoops = testredis.Gateway(), gateway.NewEpoch(), 2
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 100, 100, 100, 100
	cfg.Limits.Notrack = true
	if mod != nil {
		mod(&cfg)
	}
	tcfg := tcp.DefaultConfig(cfg.NumLoops)
	tcfg.Addr = "tcp://127.0.0.1:0"
	reg := prometheus.NewRegistry()
	var tr *tcp.Transport
	g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		t.Fatal(err)
	}
	if tr, err = tcp.New(g, wire.KeySet{1: st.noise}, tcfg, reg); err != nil {
		t.Fatal(err)
	}
	if err := g.Start(context.Background(), gateway.ServerConfig{LogicTarget: st.grpcAddr,
		LeaseTarget: st.lease, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr.Stop(ctx)
		g.Stop()
	})
	kick, _ := g.Addrs()
	st.mu.Lock()
	st.kicks[cfg.GatewayID] = kick
	st.mu.Unlock()
	return &gw{id: cfg.GatewayID, g: g, tr: tr, addr: tr.Addr()}
}

func (st *stack) token(uid string) string {
	st.t.Helper()
	now := time.Now().UnixMilli()
	tok, err := auth.Sign(st.jwt, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
	if err != nil {
		st.t.Fatal(err)
	}
	return tok
}

// field is uid's stored mobile session.
func (st *stack) field(uid string) (session.Entry, bool) {
	st.t.Helper()
	v, err := st.rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		st.t.Fatal(err)
	}
	en, err := session.Decode(v)
	if err != nil {
		st.t.Fatal(err)
	}
	return en, true
}

// liveRec records events on real time.
type liveRec struct {
	mu  sync.Mutex
	evs []Event
	got int
}

func (r *liveRec) add(e Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// next waits up to 5s for an event of kind and returns it; events before
// it are skipped.
func (r *liveRec) next(t *testing.T, kind EventKind) Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for r.got < len(r.evs) {
			e := r.evs[r.got]
			r.got++
			if e.Kind == kind {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %v event within 5s", kind)
	return Event{}
}

// none fails t if an event of kind arrives within d.
func (r *liveRec) none(t *testing.T, kind EventKind, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.evs[r.got:] {
		if e.Kind == kind {
			t.Fatalf("unexpected %v event: %+v", kind, e)
		}
	}
	r.got = len(r.evs)
}

// client starts an SDK client for uid.
func (st *stack) client(uid string, store Store, addrs []string, mod func(*Config)) (*Client, *liveRec) {
	st.t.Helper()
	r := &liveRec{}
	cfg := DefaultConfig()
	cfg.Addrs, cfg.KeyID, cfg.ServerKey = addrs, 1, st.noise.Public
	cfg.AppID, cfg.Platform = "app", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return st.token(uid), nil }
	cfg.Store, cfg.OnEvent = store, r.add
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		st.t.Fatal(err)
	}
	st.t.Cleanup(c.Close)
	return c, r
}

// script logs in a scripted client on device as uid through addr.
func (st *stack) script(addr, uid, device string, connSeq uint64) *clientconn.Conn {
	t := st.t
	t.Helper()
	c, err := clientconn.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, st.noise.Public, time.Now().Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: st.token(uid), Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: device, ConnSeq: connSeq}); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Recv(time.Now().Add(5 * time.Second)); err != nil || f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("scripted login: %+v, %v", f, err)
	}
	return c
}

// Race 9: another device's login kicks the SDK client, which must not
// reconnect on its own.
func TestE2EKickedByOtherDeviceDoesNotReconnect(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	r.next(t, EventOnline)
	st.script(g.addr, uid, "other-device", 1)
	if e := r.next(t, EventIdle); e.Kick != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("idle with %v, want KICKED_BY_OTHER_DEVICE", e.Kick)
	}
	c.NetworkChanged()
	c.Foreground()
	r.none(t, EventConnecting, 2*time.Second)
	if en, ok := st.field(uid); !ok || en.DeviceID != "other-device" {
		t.Fatalf("stored %+v, %v; want the other device's session", en, ok)
	}
}

// Race 10: two instances sharing a device_id replace each other once; the
// replaced one stays idle, so the loop stops.
func TestE2ESharedDeviceIDReplacesOnce(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	s1 := &MemStore{}
	c1, r1 := st.client(uid, s1, []string{g.addr}, nil)
	c1.Connect()
	r1.next(t, EventOnline)
	shared, _ := s1.Load()
	s2 := &MemStore{}
	s2.Save(State{DeviceID: shared.DeviceID, ConnSeq: 100}) // a copy restored elsewhere, further ahead
	c2, r2 := st.client(uid, s2, []string{g.addr}, nil)
	c2.Connect()
	r2.next(t, EventOnline)
	r1.next(t, EventAnomaly)
	if e := r1.next(t, EventIdle); e.Kick != clientpb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("first instance idle with %v, want REPLACED", e.Kick)
	}
	r1.none(t, EventConnecting, 2*time.Second)
	r2.none(t, EventWaiting, 0)
	if en, ok := st.field(uid); !ok || en.ConnSeq != 101 {
		t.Fatalf("stored %+v, %v; want the second instance (conn_seq 101)", en, ok)
	}
}

// A draining gateway sends Reconnect: the client closes, waits within
// [0, jitter_ms] and logs in elsewhere with DRAIN (R4H-O3).
func TestE2EDrainMovesTheClientWithTheDrainReason(t *testing.T) {
	st := newStack(t)
	a := st.gateway(func(c *gateway.Config) {
		c.Drain = gateway.DrainConfig{Window: time.Second, Jitter: 300 * time.Millisecond, Flush: time.Second}
	})
	b := st.gateway(nil)
	var mu sync.Mutex
	target := a.addr // the NLB: A until A is deregistered
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{"nlb"}, func(cfg *Config) {
		cfg.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			mu.Lock()
			addr := target
			mu.Unlock()
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	})
	c.Connect()
	r.next(t, EventOnline)
	if en, _ := st.field(uid); en.Gateway != a.id {
		t.Fatalf("logged in on %s, want A %s", en.Gateway, a.id)
	}
	mu.Lock()
	target = b.addr
	mu.Unlock()
	drained := make(chan struct{})
	go func() { a.g.Drain(context.Background()); close(drained) }()
	t.Cleanup(func() { <-drained })
	w := r.next(t, EventWaiting).Wait
	within(t, "after Reconnect", w, WaitDrain, 0, 300*time.Millisecond, reasonDrain)
	if e := r.next(t, EventConnecting); e.Attempt.Reason != reasonDrain {
		t.Fatalf("attempt %+v, want DRAIN", e.Attempt)
	}
	r.next(t, EventOnline)
	if en, _ := st.field(uid); en.Gateway != b.id {
		t.Fatalf("after the drain the session is on %s, want B %s", en.Gateway, b.id)
	}
}

// A gateway at its cap closes at accept: the SDK sees a pre-handshake close,
// retries after 0.5–1.5s, and logs in once a slot frees.
func TestE2EAtCapacityIsAPreHandshakeClose(t *testing.T) {
	st := newStack(t)
	g := st.gateway(func(c *gateway.Config) { c.MaxConns = 1 })
	holder := st.script(g.addr, testredis.UID(), "holder", 1)
	c, r := st.client(testredis.UID(), &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	within(t, "at capacity", r.next(t, EventWaiting).Wait, WaitPreHandshake, ms(500), ms(1500), clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
	holder.Close()
	r.next(t, EventOnline)
}

// Logout reaches Logic through the gateway's fenced Logout: the field goes.
func TestE2ELogoutDeletesTheSession(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	r.next(t, EventOnline)
	if _, ok := st.field(uid); !ok {
		t.Fatal("no session after login")
	}
	start := time.Now()
	c.Logout()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Logout took %v; the gateway answers LogoutResp at once", d)
	}
	r.next(t, EventLoggedOut)
	deadline := time.Now().Add(5 * time.Second)
	for _, ok := st.field(uid); ok; _, ok = st.field(uid) {
		if time.Now().After(deadline) {
			t.Fatal("session still stored 5s after Logout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
