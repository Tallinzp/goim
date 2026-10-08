package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerConfig is where Logic is and what this pod listens on.
type ServerConfig struct {
	LogicTarget string // Login/Logout/Reconcile channel, round_robin over Logic pods
	LeaseTarget string // the dedicated lease channel (R1-O2)
	KickAddr    string // Gateway service; Logic dials <pod>.gateway…:9100
	AdminAddr   string // /metrics, /healthz, /readyz
}

// DefaultServerConfig returns the ports Logic and its Kick client expect
// (roadmap choices #18, #22).
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		LogicTarget: "dns:///logic.im.svc.cluster.local:9000",
		LeaseTarget: "dns:///logic.im.svc.cluster.local:9001",
		KickAddr:    ":9100",
		AdminAddr:   ":9090",
	}
}

// server is the Gateway's process side: gRPC clients and servers, the
// background goroutines, readiness.
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	reqSeq      atomic.Uint64

	recon     *reconciler
	stopRecon context.CancelFunc
	clients   []*grpc.ClientConn
	kick      *grpc.Server
	admin     *http.Server
	lis       [2]net.Listener
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Start dials Logic on two connections, opens the Kick and admin listeners
// and starts the background goroutines.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
		if err != nil {
			g.closeConns()
			return err
		}
		g.clients = append(g.clients, cc)
	}
	for i, addr := range []string{sc.KickAddr, sc.AdminAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			for _, open := range g.lis[:i] {
				open.Close()
			}
			g.closeConns()
			return err
		}
		g.lis[i] = l
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
		if err := g.admin.Serve(g.lis[1]); err != nil && !errors.Is(err, http.ErrServerClosed) {
			g.log.Error("admin server", "err", err)
		}
	})

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	g.recon = newReconciler(g, logic)
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
	return nil
}

// Addrs returns the bound Kick and admin addresses.
func (g *Gateway) Addrs() (kick, admin string) {
	return g.lis[0].Addr().String(), g.lis[1].Addr().String()
}

// Stop ends everything. Call Drain first for a graceful exit.
func (g *Gateway) Stop() {
	g.cancel()
	done := make(chan struct{})
	go func() { g.kick.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		g.kick.Stop()
	}
	shut, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g.admin.Shutdown(shut)
	g.wg.Wait()
	g.recon.ends.Wait()
	g.closeConns()
}

func (g *Gateway) closeConns() {
	for _, cc := range g.clients {
		cc.Close()
	}
}

func (g *Gateway) goRun(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

// runTicker advances every loop's wheel once per tick, through Execute
// (spec: Timers; gnet's engine-wide OnTick is not used).
func (g *Gateway) runTicker(ctx context.Context) {
	t := time.NewTicker(wheelTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.advanceWheels()
		}
	}
}

func (g *Gateway) advanceWheels() {
	for _, ls := range g.loops.bound() {
		_ = ls.loop().Execute(func() { ls.wheel.advance(g.now()) })
	}
}

// dial opens a Logic channel: round_robin over the headless service's
// addresses, fail-fast RPCs, keepalive no faster than Logic's 10s MinTime.
func (g *Gateway) dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
		grpc.WithChainUnaryInterceptor(g.observeUnary),
		grpc.WithChainStreamInterceptor(g.observeStream))
}

// rpcCtx bounds a call to Logic and tags it with a request ID (at most 64
// bytes: epoch in hex, then a sequence number).
func (g *Gateway) rpcCtx(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	id := fmt.Sprintf("%016x-%d", g.cfg.Epoch, g.reqSeq.Add(1))
	return context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "x-request-id", id), timeout)
}

func (g *Gateway) observeUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	g.m.RPCSecs.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return err
}

// observeStream records the time to open a stream; per-entry Login latency
// is login_seconds.
func (g *Gateway) observeStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	start := time.Now()
	s, err := streamer(ctx, desc, cc, method, opts...)
	g.m.RPCSecs.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return s, err
}
