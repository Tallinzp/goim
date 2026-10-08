package tcp

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// staticKeys is a JWT key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

// The first vertical slice end to end: a client over TCP + Noise, this
// transport, the gateway core and Plan 2's Logic on miniredis (the real
// Redis at Tier 2). Only test files import Logic and the store.
func TestE2EOverTCP(t *testing.T) {
	rdb := testredis.New(t)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	var kickAddr string            // set once the gateway listens
	reg := prometheus.NewRegistry()
	lsc := logic.DefaultServerConfig()
	lsc.GRPCAddr, lsc.LeaseAddr, lsc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := logic.NewServer(lcfg, lsc, logic.Deps{Store: store.New(rdb, lcfg.Store), Keys: staticKeys{key},
		Dial: func(string) (*grpc.ClientConn, error) {
			return grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	s := startServer(t, func(c *gateway.Config, _ *Config) { c.GatewayID = testredis.Gateway() }, grpcAddr, leaseAddr)
	kickAddr, _ = s.g.Addrs()

	uid := testredis.UID()
	token := func() string {
		now := time.Now().UnixMilli()
		tok, err := auth.Sign(key, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	field := func() (session.Entry, bool) {
		v, err := rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
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

	a := s.login(token(), "phone-a")
	b := s.login(token(), "phone-b") // the same slot from another device
	var k clientpb.Kick
	if cmd, _ := a.recv(&k); cmd != clientpb.Cmd_CMD_KICK || k.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("A got %v %v, want Kick KICKED_BY_OTHER_DEVICE through Logic's Kick RPC", cmd, &k)
	}
	if !a.closedByServer() {
		t.Fatal("A not closed after its Kick")
	}
	if en, ok := field(); !ok || en.DeviceID != "phone-b" || en.Gateway != s.ref.Gateway || en.Epoch != s.ref.Epoch {
		t.Fatalf("stored %+v, %v; want B's session on this gateway epoch", en, ok)
	}
	b.send(clientpb.Cmd_CMD_LOGOUT, 2, nil)
	if cmd, _ := b.recv(nil); cmd != clientpb.Cmd_CMD_LOGOUT_RESP {
		t.Fatalf("B got %v, want LogoutResp", cmd)
	}
	waitFor(t, "field deleted by the fenced Logout", func() bool { _, ok := field(); return !ok })
}
