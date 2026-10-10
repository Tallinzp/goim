package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/pprofhttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerConfig is the process-level configuration of a Logic pod.
type ServerConfig struct {
	GRPCAddr  string // Logic + Revocation services (":9000")
	LeaseAddr string // Lease service, its own listener and pool (":9001", R1-O2)
	AdminAddr string // /metrics, /healthz, /readyz (":9090")

	Workers      int // concurrent Login/Logout/Reconcile calls (64)
	LeaseWorkers int // concurrent lease calls (16)

	// MaxConnectionAge spreads gateways over new Logic pods (≈5m); the grace
	// lets in-flight batches finish after GOAWAY and must exceed the Login
	// timeout, login_deadline + 1s (R1-O12).
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	ShutdownGrace         time.Duration // GracefulStop limit on SIGTERM (30s)
}

// DefaultServerConfig returns the spec defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{GRPCAddr: ":9000", LeaseAddr: ":9001", AdminAddr: ":9090",
		Workers: 64, LeaseWorkers: 16, MaxConnectionAge: 5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second, ShutdownGrace: 30 * time.Second}
}

// Validate checks sc against the service config.
func (sc ServerConfig) Validate(cfg Config) error {
	if sc.Workers <= 0 || sc.LeaseWorkers <= 0 {
		return fmt.Errorf("workers %d and lease workers %d must be positive", sc.Workers, sc.LeaseWorkers)
	}
	if min := cfg.Store.LoginDeadline + time.Second; sc.MaxConnectionAgeGrace <= min {
		return fmt.Errorf("MaxConnectionAgeGrace %v must exceed the Login timeout %v (R1-O12)", sc.MaxConnectionAgeGrace, min)
	}
	return nil
}

// Deps are the parts a Server is built from; tests pass fakes or in-process ones.
type Deps struct {
	Store Store
	Keys  interface {
		Verifier
		Ready() bool
		Run(ctx context.Context)
	}
	Dial     DialFunc
	Registry *prometheus.Registry // served on /metrics
	Metrics  *Metrics             // registered on Registry (NewMetrics)
	// Faults, if set, breaks Store calls on request and serves /fault/ on
	// the admin port (the race suite, -faults). Nil in production.
	Faults *fault.Set
}

// Server is one Logic pod: two gRPC servers, the admin HTTP server, the
// sweeper, the key refresher and the Kick client.
type Server struct {
	cfg  Config
	sc   ServerConfig
	deps Deps
	m    *Metrics

	svc    *Service
	lease  *LeaseService
	kicker *Kicker
	sweep  *Sweeper

	main, leaseSrv *grpc.Server
	admin          *http.Server
	lis            [3]net.Listener
	serving        atomic.Bool
	wg             sync.WaitGroup
}

// NewServer wires a Logic pod. Call Start, then Stop.
func NewServer(cfg Config, sc ServerConfig, d Deps) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := sc.Validate(cfg); err != nil {
		return nil, err
	}
	m := d.Metrics
	if d.Faults != nil {
		d.Store = faultStore{d.Store, d.Faults}
	}
	leases := NewLeaseCache(d.Store, cfg.LeaseCache, time.Now)
	kicker := NewKicker(cfg.Kick, leases, m, d.Dial)
	s := &Server{cfg: cfg, sc: sc, deps: d, m: m, kicker: kicker,
		svc:   NewService(cfg, d.Store, d.Keys, leases, kicker, m, sc.Workers),
		lease: NewLeaseService(d.Store, leases, m, sc.LeaseWorkers),
		sweep: NewSweeper(cfg, d.Store, m, time.Now)}
	unary := []grpc.UnaryServerInterceptor{s.unaryObserve}
	if d.Faults != nil {
		unary = append(unary, faultUnary(d.Faults))
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(s.streamObserve),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	s.main = grpc.NewServer(append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: sc.MaxConnectionAge, MaxConnectionAgeGrace: sc.MaxConnectionAgeGrace}))...)
	pb.RegisterLogicServer(s.main, s.svc)
	pb.RegisterRevocationServer(s.main, s.svc)
	// The lease server keeps its connections: no MaxConnectionAge on the
	// dedicated renewal path.
	s.leaseSrv = grpc.NewServer(opts...)
	pb.RegisterLeaseServer(s.leaseSrv, s.lease)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	pprofhttp.Register(mux)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Ready() {
			http.Error(w, "not ready: no key set or not serving", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if d.Faults != nil {
		mux.Handle("/fault/", d.Faults.Handler(nil, nil))
	}
	s.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// Ready reports whether the pod should receive traffic: it holds a key set
// (spec: JWKS) and its servers are up.
func (s *Server) Ready() bool { return s.serving.Load() && s.deps.Keys.Ready() }

// Start binds the three listeners and starts serving and the background loops.
func (s *Server) Start(ctx context.Context) error {
	for i, addr := range []string{s.sc.GRPCAddr, s.sc.LeaseAddr, s.sc.AdminAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			for _, open := range s.lis[:i] {
				open.Close()
			}
			return err
		}
		s.lis[i] = l
	}
	s.wg.Add(5)
	go func() { defer s.wg.Done(); s.main.Serve(s.lis[0]) }()
	go func() { defer s.wg.Done(); s.leaseSrv.Serve(s.lis[1]) }()
	go func() {
		defer s.wg.Done()
		if err := s.admin.Serve(s.lis[2]); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server", "err", err)
		}
	}()
	go func() { defer s.wg.Done(); s.sweep.Run(ctx) }()
	go func() { defer s.wg.Done(); s.deps.Keys.Run(ctx) }()
	s.serving.Store(true)
	return nil
}

// Addrs returns the bound gRPC, lease and admin addresses.
func (s *Server) Addrs() (grpcAddr, leaseAddr, adminAddr string) {
	return s.lis[0].Addr().String(), s.lis[1].Addr().String(), s.lis[2].Addr().String()
}

// Stop goes NotReady, lets in-flight RPCs finish for up to ShutdownGrace, and
// stops everything. ctx passed to Start must be cancelled too.
func (s *Server) Stop() {
	s.serving.Store(false)
	done := make(chan struct{})
	go func() {
		s.main.GracefulStop()
		s.leaseSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(s.sc.ShutdownGrace):
		s.main.Stop()
		s.leaseSrv.Stop()
	}
	shut, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.admin.Shutdown(shut)
	s.kicker.Close()
	s.wg.Wait()
}

// ---- request IDs, logs and latency ----

// maxRequestID bounds a caller-supplied request ID in every log line.
const maxRequestID = 64

// withRequestID takes x-request-id from the incoming metadata (at most
// maxRequestID bytes), or makes one.
func withRequestID(ctx context.Context, method string) context.Context {
	id := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-request-id"); len(v) > 0 {
			id = v[0][:min(len(v[0]), maxRequestID)]
		}
	}
	if id == "" {
		var b [8]byte
		rand.Read(b[:])
		id = hex.EncodeToString(b[:])
	}
	return context.WithValue(ctx, logKey{}, slog.Default().With("request_id", id, "method", method))
}

func (s *Server) observe(ctx context.Context, method string, start time.Time, err error) {
	code := status.Code(err)
	s.m.RPCSeconds.WithLabelValues(method, code.String()).Observe(time.Since(start).Seconds())
	if err != nil && code != codes.InvalidArgument && code != codes.Canceled {
		logger(ctx).Warn("rpc failed", "code", code.String(), "err", err)
	}
}

func (s *Server) unaryObserve(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	ctx = withRequestID(ctx, info.FullMethod)
	start := time.Now()
	resp, err := h(ctx, req)
	s.observe(ctx, info.FullMethod, start, err)
	return resp, err
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (c ctxStream) Context() context.Context { return c.ctx }

func (s *Server) streamObserve(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	ctx := withRequestID(ss.Context(), info.FullMethod)
	start := time.Now()
	err := h(srv, ctxStream{ss, ctx})
	s.observe(ctx, info.FullMethod, start, err)
	return err
}
