package logic

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func grpcConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

// TestServerEndToEnd starts a whole Logic pod on local ports: it is NotReady
// until it holds a key set, then serves Login on the main port and
// GatewayLease on the lease port, and exports metrics.
func TestServerEndToEnd(t *testing.T) {
	cfg := testConfig()
	cfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis's gwepochs
	st, _ := newStore(t, cfg)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalJWKS(key)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write(doc) }))
	defer jwks.Close()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	keys := auth.NewManager(auth.DefaultManagerConfig(jwks.URL, ""), m.AuthObserver())

	sc := DefaultServerConfig()
	sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := NewServer(cfg, sc, Deps{Store: st, Keys: keys, Dial: staticDial(nil), Registry: reg, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	if err := srv.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); srv.Stop() }()
	grpcAddr, leaseAddr, adminAddr := srv.Addrs()

	if c := httpStatus(t, "http://"+adminAddr+"/readyz"); c != http.StatusServiceUnavailable {
		t.Fatalf("readyz without a key set: %d", c)
	}
	if c := httpStatus(t, "http://"+adminAddr+"/healthz"); c != http.StatusOK {
		t.Fatalf("healthz: %d", c)
	}
	if c := httpStatus(t, "http://"+adminAddr+"/debug/pprof/goroutine?debug=1"); c != http.StatusOK {
		t.Fatalf("pprof: %d", c) // choice #85
	}
	if err := keys.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if c := httpStatus(t, "http://"+adminAddr+"/readyz"); c != http.StatusOK {
		t.Fatalf("readyz with a key set: %d", c)
	}

	gw := &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1 << 63}
	lease, err := pb.NewLeaseClient(grpcConn(t, leaseAddr)).GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: time.Now().UnixMilli()})
	if err != nil || !lease.Missing {
		t.Fatalf("first lease: %v %v", lease, err)
	}

	now := redisNow(t, st)
	uid := testredis.UID()
	tok, _ := auth.Sign(key, auth.Claims{UID: uid, AppID: "im", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3600_000})
	mdCtx := metadata.AppendToOutgoingContext(ctx, "x-request-id", "req-1")
	stream, err := pb.NewLogicClient(grpcConn(t, grpcAddr)).Login(mdCtx, &pb.LoginBatch{Gw: gw, Entries: []*pb.LoginEntry{{
		ConnId: 1, Token: tok, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, DeviceId: "d", ConnSeq: 1, RecvMs: now}}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := stream.Recv()
	if err != nil || r.Code != pb.LoginCode_LOGIN_CODE_OK || r.Uid != uid {
		t.Fatalf("login: %+v %v", r, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream end: %v", err)
	}

	// P6b1I-1: a ReconcileEnd at the 600k benchmark (4.8MB) is over gRPC's
	// 4MB default receive limit; Logic raises it to pb.MaxMsgBytes.
	_, err = pb.NewLogicClient(grpcConn(t, grpcAddr)).ReconcileEnd(ctx, &pb.ReconcileEndRequest{Gw: gw,
		AsOfMs: time.Now().UnixMilli(), TrackedConnIds: make([]uint64, 600_000)})
	if status.Code(err) == codes.ResourceExhausted {
		t.Fatalf("ReconcileEnd of 600k conn_ids: %v", err)
	}

	resp, err := http.Get("http://" + adminAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`goim_logic_logins_total{result="ok"} 1`,
		`goim_logic_lease_renewals_total{result="missing"} 1`,
		`goim_logic_rpc_seconds_count{code="OK",method="/goim.internal.v1.Logic/Login"} 1`,
		`goim_logic_jwks_fallback 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func TestServerConfigRefusesShortGrace(t *testing.T) {
	sc := DefaultServerConfig()
	sc.MaxConnectionAgeGrace = 6 * time.Second // = login_deadline + 1s
	if err := sc.Validate(DefaultConfig()); err == nil {
		t.Fatal("grace not above the Login timeout accepted")
	}
}

// Spec (Observability): log lines carry the request ID passed in gRPC metadata.
func TestRequestIDFromMetadata(t *testing.T) {
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	in := metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", "abc"))
	logger(withRequestID(in, "/m")).Info("hello", "uid", "u1", "conn_id", 7)
	logger(withRequestID(ctx, "/m")).Info("generated")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "request_id=abc") || !strings.Contains(lines[0], "uid=u1") {
		t.Fatalf("log lines %q", lines)
	}
	if !strings.Contains(lines[1], "request_id=") || strings.Contains(lines[1], "request_id=abc") {
		t.Fatalf("no generated request id: %q", lines[1])
	}
	buf.Reset()
	long := metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", strings.Repeat("x", 1000)))
	logger(withRequestID(long, "/m")).Info("long")
	if out := buf.String(); !strings.Contains(out, "request_id="+strings.Repeat("x", 64)+" ") {
		t.Fatalf("request id not capped at 64 bytes: %q", out)
	}
}
