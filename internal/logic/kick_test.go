package logic

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeGateway records Kicks; fail makes every Kick return UNAVAILABLE.
type fakeGateway struct {
	pb.UnimplementedGatewayServer
	mu    sync.Mutex
	kicks []*pb.KickRequest
	fail  bool
	got   chan struct{}
}

func (g *fakeGateway) Kick(_ context.Context, r *pb.KickRequest) (*pb.KickReply, error) {
	g.mu.Lock()
	g.kicks = append(g.kicks, r)
	fail := g.fail
	g.mu.Unlock()
	select {
	case g.got <- struct{}{}:
	default:
	}
	if fail {
		return nil, status.Error(codes.Unavailable, "injected")
	}
	return &pb.KickReply{}, nil
}

func (g *fakeGateway) count() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.kicks) }

// startGateway serves g on a local port and returns its address.
func startGateway(t *testing.T, g *fakeGateway) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterGatewayServer(srv, g)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// blackhole accepts TCP connections and never answers: like a dead pod's IP
// behind a stale DNS record, a connect "succeeds" but no RPC ever completes.
func blackhole(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		lis.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	})
	return lis.Addr().String()
}

// staticDial maps gateway names to addresses.
func staticDial(addrs map[string]string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///"+addrs[gw], grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}

type kickEnv struct {
	k   *Kicker
	st  *store.Store
	m   *Metrics
	cfg KickConfig
}

func newKickEnv(t *testing.T, cfg KickConfig, addrs map[string]string) kickEnv {
	t.Helper()
	full := testConfig()
	st, _ := newStore(t, full)
	m := newMetrics()
	k := NewKicker(cfg, NewLeaseCache(st, 20*time.Millisecond, time.Now), m, staticDial(addrs))
	t.Cleanup(k.Close)
	return kickEnv{k: k, st: st, m: m, cfg: cfg}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestKickReachesGateway(t *testing.T) {
	g := &fakeGateway{got: make(chan struct{}, 1)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	ref := store.EpochRef{Gateway: gw, Epoch: 1 << 63}
	env.k.Kick(ref, 42, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
	g.mu.Lock()
	r := g.kicks[0]
	g.mu.Unlock()
	if r.Gw.Gateway != gw || r.Gw.Epoch != 1<<63 || r.ConnId != 42 || r.Reason != pb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("kick %+v", r)
	}
	waitFor(t, "ok attempt counted", func() bool { return counter(env.m.KickAttempts.WithLabelValues("first", "ok")) == 1 })
}

// R5S-O2: an attempt to a blackholed address ends within its deadline, Kick
// never blocks the caller, and with the lease dead there are no retries.
func TestKickToBlackholeEndsWithinDeadline(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	env := newKickEnv(t, cfg, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 3} // no lease: dead

	start := time.Now()
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("Kick blocked for %v", d)
	}
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	if d := time.Since(start); d > cfg.Deadline+500*time.Millisecond {
		t.Fatalf("attempt took %v, deadline %v", d, cfg.Deadline)
	}
	time.Sleep(3 * cfg.RetrySpacing)
	if n := counter(env.m.KickAttempts.WithLabelValues("retry", "error")); n != 0 {
		t.Fatalf("%v retries to a dead target", n)
	}
}

func TestKickRetriesOnlyWhileLeaseLive(t *testing.T) {
	g := &fakeGateway{fail: true, got: make(chan struct{}, 8)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	ref := store.EpochRef{Gateway: gw, Epoch: 9}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REVOKED)
	waitFor(t, "live give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("live")) == 1 })
	if n := g.count(); n != 1+env.cfg.Retries {
		t.Fatalf("attempts %d, want %d", n, 1+env.cfg.Retries)
	}

	// Same failure with the lease gone: one attempt, no retry.
	if err := env.st.EndEpoch(ctx, ref); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // past the 20ms lease cache
	env.k.Kick(ref, 2, pb.KickReason_KICK_REASON_REVOKED)
	waitFor(t, "dead give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	time.Sleep(3 * env.cfg.RetrySpacing)
	if n := g.count(); n != 1+env.cfg.Retries+1 {
		t.Fatalf("attempts %d, want %d", n, 1+env.cfg.Retries+1)
	}
}

// #97: a Kick refused at connect (nothing listens: a crashed gateway on a
// live node) gives up at once, even while the target's lease is live, which
// it stays for up to a TTL after the crash. Without this every relogin of a
// crash storm is retried until the lease expires (Tier 2 b1 prototype: ~2
// retries per relogin). The give-up counts as dead, so a crash does not
// raise the live give-up alert once per relogin.
func TestKickRefusedDoesNotRetry(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // nothing listens: connects are refused
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
	ref := store.EpochRef{Gateway: gw, Epoch: 11}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	time.Sleep(3 * env.cfg.RetrySpacing)
	if n := counter(env.m.KickAttempts.WithLabelValues("retry", "error")); n != 0 {
		t.Fatalf("%v retries to a refused address with a live lease", n)
	}
	if n := counter(env.m.KickGiveUps.WithLabelValues("live")); n != 0 {
		t.Fatalf("%v live give-ups for a refused address", n)
	}
}

// R5S-O2: a full per-target queue drops and counts instead of growing.
func TestKickQueueFullDrops(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers, cfg.Queue = 1, 2
	env := newKickEnv(t, cfg, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 4}
	for i := range 10 {
		env.k.Kick(ref, uint64(i), pb.KickReason_KICK_REASON_REPLACED)
	}
	// One attempt in flight (or not yet taken) and two queued; the rest dropped.
	// No attempt has failed yet, so the lease was never read: "unknown".
	d := counter(env.m.KickDropped.WithLabelValues("unknown"))
	if d < 7 || d > 8 {
		t.Fatalf("dropped %v, want 7 or 8", d)
	}
	if q := counter(env.m.KickQueueDepth.WithLabelValues(gw + ":4")); q > 2 {
		t.Fatalf("queue depth %v above its size", q)
	}
}

// slowLeases makes every lease read take a second.
type slowLeases struct{ *store.Store }

func (s slowLeases) LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error) {
	time.Sleep(time.Second)
	return s.Store.LeasesExist(ctx, refs)
}

// R5S-O2: Kick never reads Redis on the caller's goroutine, not even to label
// a drop. LoginEach calls done one entry after another per node group, so a
// blocking Kick would hold back the next entries' results.
func TestKickDropDoesNotReadRedis(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers, cfg.Queue = 1, 1
	st, _ := newStore(t, testConfig())
	k := NewKicker(cfg, NewLeaseCache(slowLeases{st}, time.Minute, time.Now), newMetrics(),
		staticDial(map[string]string{gw: blackhole(t)}))
	t.Cleanup(k.Close)
	start := time.Now()
	for i := range 10 {
		k.Kick(store.EpochRef{Gateway: gw, Epoch: 4}, uint64(i), pb.KickReason_KICK_REASON_REPLACED)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("10 Kicks to a full queue took %v", d)
	}
}

// A gateway pod restarts under the same name with a new IP and epoch while the
// old epoch's connection is stuck on the dead IP (node loss, no RST). Kicks to
// the new epoch must not ride the old epoch's transport.
func TestKickNewEpochOfSamePodGetsItsOwnConnection(t *testing.T) {
	gw := testredis.Gateway()
	g := &fakeGateway{got: make(chan struct{}, 1)}
	dead, alive := blackhole(t), startGateway(t, g)
	var mu sync.Mutex
	addr := dead
	dial := func(name string) (*grpc.ClientConn, error) {
		mu.Lock()
		a := addr
		mu.Unlock()
		return grpc.NewClient("passthrough:///"+a, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	st, _ := newStore(t, testConfig())
	m := newMetrics()
	k := NewKicker(testConfig().Kick, NewLeaseCache(st, time.Second, time.Now), m, dial)
	t.Cleanup(k.Close)

	k.Kick(store.EpochRef{Gateway: gw, Epoch: 1}, 1, pb.KickReason_KICK_REASON_REPLACED) // old epoch: keeps the target busy
	mu.Lock()
	addr = alive // DNS now points at the restarted pod
	mu.Unlock()
	k.Kick(store.EpochRef{Gateway: gw, Epoch: 2}, 2, pb.KickReason_KICK_REASON_REPLACED)
	select {
	case <-g.got:
	case <-time.After(2 * time.Second):
		t.Fatal("Kick to the restarted pod's new epoch never arrived")
	}
}

func TestIdleTargetIsRemoved(t *testing.T) {
	g := &fakeGateway{got: make(chan struct{}, 1)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	env.k.Kick(store.EpochRef{Gateway: gw, Epoch: 1}, 1, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
	if env.k.targetCount() != 1 {
		t.Fatal("no target")
	}
	waitFor(t, "idle target removed", func() bool { return env.k.targetCount() == 0 })
	// A later Kick to the same target works again.
	env.k.Kick(store.EpochRef{Gateway: gw, Epoch: 1}, 2, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
}
