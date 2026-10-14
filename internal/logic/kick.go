package logic

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DialFunc opens a client connection to a gateway pod by name. It must not
// block (grpc.NewClient connects lazily).
type DialFunc func(gateway string) (*grpc.ClientConn, error)

// PodDNSDial dials gateways by StatefulSet pod DNS through addrFormat
// (spec: Logic→Gateway Kick, no registry).
func PodDNSDial(addrFormat string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		return grpc.NewClient("dns:///"+fmt.Sprintf(addrFormat, gw),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}

// Kicker is the hardened Kick client (R1-O8, R2-O6, R5S-O2). Each target
// gw|epoch has a bounded queue drained by a bounded set of workers; every
// attempt has its own deadline; retries go through the same queue and happen
// only while the target's lease is live. A full queue drops the attempt:
// Reconcile is the backstop (race 8).
type Kicker struct {
	cfg    KickConfig
	leases *LeaseCache
	m      *Metrics
	dial   DialFunc

	mu      sync.Mutex
	targets map[store.EpochRef]*kickTarget
	closed  bool
	done    chan struct{}
	wg      sync.WaitGroup
}

type kickTarget struct {
	ref     store.EpochRef
	label   string
	q       chan kickJob
	cc      *grpc.ClientConn // this epoch's own connection
	client  pb.GatewayClient
	workers int             // running workers; guarded by Kicker.mu
	errs    map[string]bool // error messages already logged; guarded by Kicker.mu
}

// maxLoggedKickErrs bounds the distinct errors logged per target.
const maxLoggedKickErrs = 8

type kickJob struct {
	connID  uint64
	reason  pb.KickReason
	attempt int // 0 = first
}

// NewKicker returns a Kicker; Close stops it.
func NewKicker(cfg KickConfig, leases *LeaseCache, m *Metrics, dial DialFunc) *Kicker {
	return &Kicker{cfg: cfg, leases: leases, m: m, dial: dial,
		targets: map[store.EpochRef]*kickTarget{}, done: make(chan struct{})}
}

// Kick queues the first attempt and returns at once. The first attempt is
// always queued, whatever the target's lease looks like (R2-O6).
func (k *Kicker) Kick(ref store.EpochRef, connID uint64, reason pb.KickReason) {
	k.enqueue(ref, kickJob{connID: connID, reason: reason})
}

func (k *Kicker) enqueue(ref store.EpochRef, j kickJob) {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	t, err := k.targetLocked(ref)
	if err != nil {
		k.mu.Unlock()
		slog.Warn("kick: dial failed", "gw", ref.Gateway, "epoch", ref.Epoch, "err", err)
		k.m.KickGiveUps.WithLabelValues(k.cachedLeaseLabel(ref)).Inc()
		return
	}
	select {
	case t.q <- j:
		k.m.KickQueueDepth.WithLabelValues(t.label).Set(float64(len(t.q)))
		// Workers follow the load up to the cap and retire when idle.
		if t.workers < k.cfg.Workers {
			t.workers++
			k.wg.Add(1)
			go k.work(t)
		}
		k.mu.Unlock()
	default:
		k.mu.Unlock()
		k.m.KickDropped.WithLabelValues(k.cachedLeaseLabel(ref)).Inc()
	}
}

// targetLocked returns ref's target, creating it if needed.
func (k *Kicker) targetLocked(ref store.EpochRef) (*kickTarget, error) {
	if t, ok := k.targets[ref]; ok {
		return t, nil
	}
	// One connection per epoch, not per pod name: a pod that restarts under
	// the same name gets a fresh dial (and DNS lookup) for its new epoch,
	// instead of the old epoch's transport, which may be stuck on a dead IP.
	cc, err := k.dial(ref.Gateway)
	if err != nil {
		return nil, err
	}
	t := &kickTarget{ref: ref, label: ref.Gateway + ":" + strconv.FormatUint(ref.Epoch, 10),
		q: make(chan kickJob, k.cfg.Queue), cc: cc, client: pb.NewGatewayClient(cc)}
	k.targets[ref] = t
	return t, nil
}

// work drains t's queue. A worker idle for IdleTimeout exits; the last one
// out removes the target, so dead epochs don't keep goroutines or connections.
func (k *Kicker) work(t *kickTarget) {
	defer k.wg.Done()
	idle := time.NewTimer(k.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case j := <-t.q:
			k.m.KickQueueDepth.WithLabelValues(t.label).Set(float64(len(t.q)))
			k.attempt(t, j)
			idle.Reset(k.cfg.IdleTimeout)
		case <-idle.C:
			if k.retire(t) {
				return
			}
			idle.Reset(k.cfg.IdleTimeout)
		case <-k.done:
			return
		}
	}
}

// retire lets one idle worker exit unless work arrived meanwhile. The last
// worker out removes the target and closes its connection.
func (k *Kicker) retire(t *kickTarget) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(t.q) > 0 {
		return false
	}
	t.workers--
	if t.workers > 0 {
		return true
	}
	delete(k.targets, t.ref)
	k.m.KickQueueDepth.DeleteLabelValues(t.label)
	t.cc.Close()
	return true
}

func (k *Kicker) attempt(t *kickTarget, j kickJob) {
	kind := "first"
	if j.attempt > 0 {
		kind = "retry"
	}
	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
	start := time.Now()
	// Fail-fast (no WaitForReady): a dead pod's address fails or times out
	// within Deadline instead of blocking on connect (R5S-O2).
	_, err := t.client.Kick(ctx, &pb.KickRequest{Gw: pbRef(t.ref), ConnId: j.connID, Reason: j.reason})
	cancel()
	k.m.KickAttemptSeconds.Observe(time.Since(start).Seconds())
	if err == nil {
		k.m.KickAttempts.WithLabelValues(kind, "ok").Inc()
		return
	}
	result := attemptResult(err)
	k.m.KickAttempts.WithLabelValues(kind, result).Inc()
	k.logErr(t, kind, err)
	if result == "refused" {
		// Nothing listens at the target's address: its process is gone, and
		// with it the connection this Kick targets. A starved but alive
		// gateway still completes TCP connects in its kernel and times out
		// instead, so it keeps its retries (#97, R2-O6). Its lease stays live
		// for up to a TTL after a crash, so the give-up counts as dead: a
		// crash must not raise the live give-up alert once per relogin. The
		// cross-check is recorded first, so a counted give-up has its sample.
		if j.attempt == 0 {
			k.crossCheck(t.ref)
		}
		k.m.KickGiveUps.WithLabelValues("dead").Inc()
		return
	}
	if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || j.attempt >= k.cfg.Retries {
		k.m.KickGiveUps.WithLabelValues(k.leaseLabel(t.ref)).Inc()
		return
	}
	// Retry only while the target's lease is live (R2-O6).
	if k.leaseLabel(t.ref) != "live" {
		k.m.KickGiveUps.WithLabelValues("dead").Inc()
		return
	}
	j.attempt++
	time.AfterFunc(k.cfg.RetrySpacing, func() { k.enqueue(t.ref, j) })
}

// crossCheck records R1-O7's dead-epoch cross-check for a first attempt
// refused while the target's lease is still live: now − the epoch's last
// renewal, the time from gateway death to relogin within the renewal
// interval (P6b2a-b1I-7). A crashed gateway's lease stays live for up to its
// TTL, so Login's cross-check, which needs the lease gone, misses the
// relogins that land meanwhile, nearly all of a crash storm's. A lease
// already gone means Login recorded that relogin, so each counts once. A
// retry comes RetrySpacing after the relogin and is not recorded. It may
// read Redis: call it from a worker only.
func (k *Kicker) crossCheck(ref store.EpochRef) {
	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
	defer cancel()
	if live, err := k.leases.LiveOne(ctx, ref); err != nil || !live {
		return
	}
	renewed, ok, err := k.leases.RenewedMs(ctx, ref)
	if err != nil || !ok || renewed == 0 {
		return
	}
	k.m.DeadEpochRelogin.Observe(float64(time.Now().UnixMilli()-renewed) / 1000)
}

// logErr logs each distinct error once per target (at most
// maxLoggedKickErrs): a storm fails thousands of attempts the same way, and
// the metrics carry no error text.
func (k *Kicker) logErr(t *kickTarget, kind string, err error) {
	msg := status.Code(err).String() + ": " + status.Convert(err).Message()
	k.mu.Lock()
	if t.errs == nil {
		t.errs = map[string]bool{}
	}
	first := !t.errs[msg] && len(t.errs) < maxLoggedKickErrs
	if first {
		t.errs[msg] = true
	}
	k.mu.Unlock()
	if first {
		slog.Warn("kick: attempt failed", "gw", t.ref.Gateway, "epoch", t.ref.Epoch, "attempt", kind, "err", msg)
	}
}

// attemptResult is a failed attempt's result label: refused, timeout or
// error (#97 amended, run 20).
func attemptResult(err error) string {
	switch {
	case refused(err):
		return "refused"
	case status.Code(err) == codes.DeadlineExceeded:
		return "timeout"
	}
	return "error"
}

// refused reports whether a failed Kick reached no live process. Either its
// dial got ECONNREFUSED (nothing listens), or the connection was reset
// before the server's HTTP/2 preface: the listener closed with the connect
// still in its accept queue, as a SIGKILLed gateway's does while the kernel
// tears down its sockets (~1.5s at 200k connections, Tier 2 run 20). A
// starved but alive gateway sends no reset (tcp_abort_on_overflow=0): its
// Kick times out instead. gRPC carries both errors only as text in the
// UNAVAILABLE status, also on the fail-fast RPCs that follow while the
// connection stays in TRANSIENT_FAILURE.
func refused(err error) bool {
	if status.Code(err) != codes.Unavailable {
		return false
	}
	msg := status.Convert(err).Message()
	return strings.Contains(msg, syscall.ECONNREFUSED.Error()) ||
		strings.Contains(msg, "error reading server preface") && strings.Contains(msg, syscall.ECONNRESET.Error())
}

// leaseLabel is the target_lease label: live, dead, or unknown if the lease
// could not be read. It may read Redis: call it from a worker only.
func (k *Kicker) leaseLabel(ref store.EpochRef) string {
	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
	defer cancel()
	live, err := k.leases.LiveOne(ctx, ref)
	switch {
	case err != nil:
		return "unknown"
	case live:
		return "live"
	default:
		return "dead"
	}
}

// cachedLeaseLabel is leaseLabel from the cache only. enqueue runs on the
// caller's goroutine (a Login result callback, Reconcile, Revoke) and must not
// wait on Redis (R5S-O2); a target's workers keep its observation fresh.
func (k *Kicker) cachedLeaseLabel(ref store.EpochRef) string {
	live, ok := k.leases.Peek(ref)
	switch {
	case !ok:
		return "unknown"
	case live:
		return "live"
	default:
		return "dead"
	}
}

// Close stops every worker and closes every connection. Queued Kicks are
// dropped; Reconcile is the backstop.
func (k *Kicker) Close() {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	k.closed = true
	close(k.done)
	k.mu.Unlock()
	k.wg.Wait()
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, t := range k.targets {
		t.cc.Close()
	}
	k.targets = map[store.EpochRef]*kickTarget{}
}

// targetCount is for tests.
func (k *Kicker) targetCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.targets)
}
