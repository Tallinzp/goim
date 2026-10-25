package logic

import (
	"context"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
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
	rdb redis.UniversalClient
	m   *Metrics
	cfg KickConfig
}

func newKickEnv(t *testing.T, cfg KickConfig, addrs map[string]string) kickEnv {
	t.Helper()
	full := testConfig()
	st, rdb := newStore(t, full)
	m := newMetrics()
	k := NewKicker(cfg, NewLeaseCache(st, 20*time.Millisecond, time.Now), m, staticDial(addrs))
	t.Cleanup(k.Close)
	return kickEnv{k: k, st: st, rdb: rdb, m: m, cfg: cfg}
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
	if n := counter(env.m.KickAttempts.WithLabelValues("first", "timeout")); n != 1 {
		t.Fatalf("%v first attempts counted as timeouts, want 1", n)
	}
	time.Sleep(3 * cfg.RetrySpacing)
	if n := attempts(env.m, "retry"); n != 0 {
		t.Fatalf("%v retries to a dead target", n)
	}
}

// R2-O6, #97 (P6b2a-b1I-1): an attempt that times out (a starved but alive
// gateway: its kernel accepts the connect) is not refused, so with the lease
// live it keeps its retries and gives up as live.
func TestKickTimeoutRetriesWhileLeaseLive(t *testing.T) {
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 13}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "live give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("live")) == 1 })
	if n := counter(env.m.KickAttempts.WithLabelValues("retry", "timeout")); n != float64(env.cfg.Retries) {
		t.Fatalf("%v retries to a timing-out address with a live lease, want %d", n, env.cfg.Retries)
	}
	if n := counter(env.m.KickGiveUps.WithLabelValues("dead")); n != 0 {
		t.Fatalf("%v dead give-ups for a timing-out address", n)
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
	if n := counter(env.m.KickAttempts.WithLabelValues("first", "refused")); n != 1 {
		t.Fatalf("%v first attempts counted as refused, want 1", n)
	}
	time.Sleep(3 * env.cfg.RetrySpacing)
	if n := attempts(env.m, "retry"); n != 0 {
		t.Fatalf("%v retries to a refused address with a live lease", n)
	}
	if n := counter(env.m.KickGiveUps.WithLabelValues("live")); n != 0 {
		t.Fatalf("%v live give-ups for a refused address", n)
	}
}

// R1-O7's dead-epoch cross-check during a crash storm (P6b2a-b1I-7): a
// crashed gateway's lease stays live for up to its TTL, so the relogins
// that land meanwhile are not seen by Login's cross-check. Their first Kick
// is refused, and that give-up records now − the epoch's last renewal.
func TestKickRefusedRecordsDeadEpochCrossCheck(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // nothing listens: connects are refused
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
	ref := store.EpochRef{Gateway: gw, Epoch: 13}
	res, err := env.st.RenewLease(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	h := env.m.DeadEpochRelogin
	if n := sampleCount(t, h); n != 1 {
		t.Fatalf("cross-check samples %d, want 1", n)
	}
	if s := sampleSum(t, h); s < 0 || s > float64(time.Now().UnixMilli()-res.RedisNowMs)/1000+0.01 {
		t.Fatalf("cross-check %vs", s)
	}
}

// The cross-check's time is the relogin's Kick() call, not the attempt: a
// refused attempt that waited in the target's queue must not add that wait.
// One worker, and a dial that is refused after 200ms, so the second Kick
// waits ~200ms behind the first.
func TestKickRefusedCrossCheckExcludesQueueWait(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers = 1
	env := newKickEnv(t, cfg, nil)
	env.k.dial = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///refused", grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				time.Sleep(200 * time.Millisecond)
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
			}))
	}
	ref := store.EpochRef{Gateway: gw, Epoch: 15}
	res, err := env.st.RenewLease(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	queued := time.Now().UnixMilli()
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	env.k.Kick(ref, 2, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "give-ups", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 2 })
	h := env.m.DeadEpochRelogin
	if n := sampleCount(t, h); n != 2 {
		t.Fatalf("cross-check samples %d, want 2", n)
	}
	if s, max := sampleSum(t, h), 2*float64(queued+5-res.RedisNowMs)/1000; s > max {
		t.Fatalf("cross-check sum %.3fs > %.3fs: the queue wait was counted", s, max)
	}
}

// A refused Kick to an epoch whose lease is already gone records nothing:
// Login's cross-check has recorded that relogin, and each relogin counts once.
func TestKickRefusedDeadLeaseNoSecondCrossCheck(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
	ref := store.EpochRef{Gateway: gw, Epoch: 14}
	if _, err := env.st.RenewLease(ctx, ref); err != nil { // its last renewal stays in gwepochs
		t.Fatal(err)
	}
	if _, err := env.rdb.Del(ctx, "gwlease:"+gw+":{14}").Result(); err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	if n := sampleCount(t, env.m.DeadEpochRelogin); n != 0 {
		t.Fatalf("cross-check samples %d for an expired lease, want 0", n)
	}
}

// #97 amended (Tier 2 run 20): a SIGKILLed gateway's Kick listener lives on
// in the kernel while its sockets are torn down, then closes with Logic's
// connect still queued: the connection is reset before the server's
// preface. Nothing alive is behind it, so it gives up as refused does, and
// the fail-fast RPCs that carry the same error until gRPC reconnects do too.
func TestKickResetBeforePrefaceDoesNotRetry(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: lis.Addr().String()})
	ref := store.EpochRef{Gateway: gw, Epoch: 12}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	// Nothing accepts: the kernel completes the connect and queues it, and
	// the attempt waits for the server's preface until the listener closes
	// (well within the attempt's deadline), resetting the queued connection.
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	time.Sleep(env.cfg.Deadline / 3)
	lis.Close()
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	if n := counter(env.m.KickAttempts.WithLabelValues("first", "refused")); n != 1 {
		t.Fatalf("%v first attempts counted as refused, want 1", n)
	}
	time.Sleep(3 * env.cfg.RetrySpacing)
	if n := attempts(env.m, "retry"); n != 0 {
		t.Fatalf("%v retries after a reset before the preface, with a live lease", n)
	}
	if n := counter(env.m.KickGiveUps.WithLabelValues("live")); n != 0 {
		t.Fatalf("%v live give-ups after a reset before the preface", n)
	}
}

// attempts sums kind's (first or retry) attempts over every result.
func attempts(m *Metrics, kind string) float64 {
	var n float64
	for _, r := range []string{"ok", "refused", "timeout", "error"} {
		n += counter(m.KickAttempts.WithLabelValues(kind, r))
	}
	return n
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
	d := counter(env.m.KickDropped.WithLabelValues("first", "unknown"))
	if d < 7 || d > 8 {
		t.Fatalf("dropped %v, want 7 or 8", d)
	}
	if q := counter(env.m.KickQueueDepth.WithLabelValues(gw + ":4")); q > 2 {
		t.Fatalf("queue depth %v above its size", q)
	}
}

// P6b2b-b2-run28I-1: a retry dropped by a full queue is counted as a retry,
// apart from the first attempts, and ends its chain: once every chain is over,
// each first attempt is accounted for by a success, a give-up or a dropped
// retry (gwloss's open_chains).
func TestKickRetryDroppedIsCountedAsRetry(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers, cfg.Queue = 1, 1
	env := newKickEnv(t, cfg, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 14}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	// dropped sums kind's drops over every lease label (the test's lease cache
	// expires before a retry comes back, so its label is "unknown").
	dropped := func(kind string) float64 {
		var n float64
		for _, l := range []string{"live", "dead", "unknown"} {
			n += counter(env.m.KickDropped.WithLabelValues(kind, l))
		}
		return n
	}
	// Keep the queue full, so a retry coming back finds no room.
	for i := 0; dropped("retry") == 0; i++ {
		if i == 500 {
			t.Fatal("no retry dropped in 5s")
		}
		env.k.Kick(ref, uint64(i), pb.KickReason_KICK_REASON_REPLACED)
		time.Sleep(10 * time.Millisecond)
	}
	if dropped("first") == 0 {
		t.Fatal("no first attempt dropped by the full queue")
	}
	waitFor(t, "every chain over", func() bool {
		ended := dropped("retry")
		for _, l := range []string{"live", "dead", "unknown"} {
			ended += counter(env.m.KickGiveUps.WithLabelValues(l))
		}
		ended += counter(env.m.KickAttempts.WithLabelValues("first", "ok")) + counter(env.m.KickAttempts.WithLabelValues("retry", "ok"))
		return ended == attempts(env.m, "first")
	})
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
