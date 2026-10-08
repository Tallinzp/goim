package logic

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// staticKeys verifies tokens against a fixed key set at the real time.
type staticKeys struct{ ks auth.KeySet }

func (k staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, k.ks, time.Now().UnixMilli(), 0)
}

// kickRecorder records Kicks instead of sending them.
type kickRecorder struct {
	mu    sync.Mutex
	kicks []recordedKick
}

type recordedKick struct {
	ref    store.EpochRef
	connID uint64
	reason pb.KickReason
}

func (r *kickRecorder) Kick(ref store.EpochRef, connID uint64, reason pb.KickReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kicks = append(r.kicks, recordedKick{ref, connID, reason})
}

func (r *kickRecorder) all() []recordedKick {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedKick(nil), r.kicks...)
}

// env is one Logic service on a fresh store, with a signing key and a gateway.
type env struct {
	t     *testing.T
	cfg   Config
	svc   *Service
	real  *store.Store
	rdb   *redis.Client
	kicks *kickRecorder
	m     *Metrics
	key   auth.PrivateKey
	gw    store.EpochRef
}

func newEnv(t *testing.T) *env {
	return newEnvWith(t, testConfig(), func(s *store.Store) Store { return s })
}

// newEnvWith lets a test wrap the real store.
func newEnvWith(t *testing.T, cfg Config, wrap func(*store.Store) Store) *env {
	t.Helper()
	real, rdb := newStore(t, cfg)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalJWKS(key)
	ks, err := auth.ParseJWKS(doc)
	if err != nil {
		t.Fatal(err)
	}
	st := wrap(real)
	m := newMetrics()
	kicks := &kickRecorder{}
	svc := NewService(cfg, st, staticKeys{ks}, NewLeaseCache(st, cfg.LeaseCache, time.Now), kicks, m, 8)
	return &env{t: t, cfg: cfg, svc: svc, real: real, rdb: rdb, kicks: kicks, m: m, key: key,
		gw: store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 5}}
}

func (e *env) token(uid string, iatMs int64) string {
	e.t.Helper()
	tok, err := auth.Sign(e.key, auth.Claims{UID: uid, AppID: "im", Platform: "mobile",
		IatMs: iatMs, ExpMs: time.Now().UnixMilli() + 3600_000})
	if err != nil {
		e.t.Fatal(err)
	}
	return tok
}

// loginEntry is a valid entry for uid on the mobile slot, received now.
func (e *env) loginEntry(uid string, connID uint64, dev string, seq uint64) *pb.LoginEntry {
	now := redisNow(e.t, e.real)
	return &pb.LoginEntry{ConnId: connID, Token: e.token(uid, now-1000), Appid: "im",
		Platform: pb.Platform_PLATFORM_MOBILE, DeviceId: dev, ConnSeq: seq, RecvMs: now}
}

// dial serves the service on an in-process listener and returns a client.
func (e *env) dial() pb.LogicClient {
	e.t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterLogicServer(srv, e.svc)
	pb.RegisterRevocationServer(srv, e.svc)
	go srv.Serve(lis)
	e.t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cc.Close() })
	return pb.NewLogicClient(cc)
}

// login runs one batch through the gRPC stream and returns results by index.
func (e *env) login(c pb.LogicClient, entries ...*pb.LoginEntry) map[uint32]*pb.LoginResult {
	e.t.Helper()
	stream, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: entries})
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[uint32]*pb.LoginResult{}
	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}
		if _, dup := out[r.Index]; dup {
			e.t.Fatalf("index %d reported twice", r.Index)
		}
		out[r.Index] = r
	}
	if len(out) != len(entries) {
		e.t.Fatalf("%d results for %d entries", len(out), len(entries))
	}
	return out
}
