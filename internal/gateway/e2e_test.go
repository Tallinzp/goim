package gateway

import (
	"context"
	"crypto/ed25519"
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// e2e is this gateway core against a real Logic pod on miniredis (or the
// Tier 2 Redis). Only this test file imports Logic and the store; the
// gateway package itself does not (TestGatewayDoesNotImportTheStore).
type e2e struct {
	*core
	rdb *redis.Client
	key auth.PrivateKey
}

func newE2E(t *testing.T) *e2e {
	t.Helper()
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	rdb := testredis.New(t)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	var kickAddr string // set once the gateway listens
	dial := func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	sc := logic.DefaultServerConfig()
	sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := logic.NewServer(lcfg, sc, logic.Deps{Store: store.New(rdb, lcfg.Store), Keys: staticKeys{key},
		Dial: dial, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	grpcAddr, leaseAddr, _ := srv.Addrs()

	h := newCore(t, func(c *Config) {
		fastConfig(c)
		c.GatewayID = testredis.Gateway()
		c.LeaseTTL = 30 * time.Second // Logic's
	})
	h.g.limits = okLimits
	if err := h.g.Start(context.Background(), ServerConfig{LogicTarget: grpcAddr, LeaseTarget: leaseAddr,
		KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.g.Stop)
	kickAddr, _ = h.g.Addrs()
	waitFor(t, "gateway Ready", func() bool { return h.g.Ready() == nil })
	return &e2e{core: h, rdb: rdb, key: key}
}

// staticKeys is a key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

func (e *e2e) token(t *testing.T, uid string) string {
	t.Helper()
	now := time.Now().UnixMilli()
	tok, err := auth.Sign(e.key, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// loginAs logs a device in on loop li and waits for ONLINE.
func (e *e2e) loginAs(t *testing.T, li int, uid, device string, seq uint64) *fakeLink {
	t.Helper()
	k := e.authed(li)
	req := loginReq(uid, device, seq)
	req.Token = e.token(t, uid)
	e.frame(k, clientpb.Cmd_CMD_LOGIN, 1, req)
	waitFor(t, uid+"/"+device+" ONLINE", func() bool { return onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) })
	return k
}

func (e *e2e) field(t *testing.T, uid string) (session.Entry, bool) {
	t.Helper()
	v, err := e.rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	en, err := session.Decode(v)
	if err != nil {
		t.Fatal(err)
	}
	return en, true
}

func TestE2ELoginKickByOtherDeviceAndLogout(t *testing.T) {
	e := newE2E(t)
	uid := testredis.UID()
	a := e.loginAs(t, 0, uid, "phone-1", 1)
	b := e.loginAs(t, 1, uid, "phone-2", 1) // the same slot from another device
	waitFor(t, "A kicked through Logic's Kick RPC", a.isClosed)
	var m clientpb.Kick
	if !a.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("A's Kick %v", &m)
	}
	if en, ok := e.field(t, uid); !ok || en.ConnID != b.c.id {
		t.Fatalf("stored %+v, want B's conn_id %x", en, b.c.id)
	}
	e.frame(b, clientpb.Cmd_CMD_LOGOUT, 2, nil)
	waitFor(t, "field deleted by the fenced Logout", func() bool { _, ok := e.field(t, uid); return !ok })
}

func TestE2EReconcileRestoresAndDeletesGhosts(t *testing.T) {
	e := newE2E(t)
	ctx := context.Background()
	live := testredis.UID()
	k := e.loginAs(t, 0, live, "d", 1)
	// A lost write: the live connection's field is gone (race 15 / wrongly swept).
	e.rdb.HDel(ctx, "sess:{"+live+"}", "app:mobile")
	// A ghost: a field of this epoch whose connection the gateway doesn't track (race 7).
	ghost := testredis.UID()
	en := session.Entry{Gateway: e.g.cfg.GatewayID, Epoch: e.g.cfg.Epoch, ConnID: 1<<47 | 99,
		DeviceID: "g", ConnSeq: 1, LoginMs: time.Now().Add(-time.Minute).UnixMilli(), TokIatMs: 1}
	e.rdb.HSet(ctx, "sess:{"+ghost+"}", "app:mobile", en.Encode())
	e.rdb.SAdd(ctx, "gwconns:"+e.g.cfg.GatewayID+":{"+strconv.FormatUint(e.g.cfg.Epoch, 10)+"}", ghost)

	before := endsOK(e)
	e.g.recon.Trigger()
	waitFor(t, "ReconcileEnd done", func() bool { return endsOK(e) > before })
	if got, ok := e.field(t, live); !ok || got.ConnID != k.c.id {
		t.Fatalf("live field %+v, %v: want it restored by rule 4", got, ok)
	}
	if _, ok := e.field(t, ghost); ok {
		t.Fatal("ghost not deleted by the delete phase")
	}
}

func endsOK(e *e2e) float64 { return promtest.ToFloat64(e.g.m.ReconcileEnds.WithLabelValues("ok")) }
