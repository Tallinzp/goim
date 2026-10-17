// Package gateway is the transport-independent gateway core (spec: Gateway):
// per-loop connection tables and timing wheels, the connection state
// machine, admission control, Login batching, the Logout fence, the lease
// loop, the Reconcile pass controller, the Kick server, readiness and drain.
// A transport (TCP + Noise in Plan 4, wss later) feeds it decoded frames.
// It never touches Redis: only Logic does (layer rule).
package gateway

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
)

// skewAllowance is the 1s fences add for gateway↔Redis clock skew (spec:
// Clock contract). It is not scaled by the test timing profile.
const skewAllowance = time.Second

// Config holds the gateway's tunables. Every interval is configurable so the
// test timing profile can scale them (spec: Test execution model).
type Config struct {
	GatewayID string // pod name, a DNS-1123 label
	Epoch     uint64 // random per boot (NewEpoch)
	NumLoops  int    // event loops (gnet NumEventLoop); worker_id = loop index

	MaxConns      int // connection cap: open sockets in any state (250k)
	PendingCap    int // HANDSHAKE + AUTH_PENDING sockets, memory only (100k)
	LoginQueueCap int // Logins waiting for or inside a Login RPC (8192)
	// A pending_mem or draining refusal holds its socket until the first
	// wheel tick RefuseHold after it (0.5–1.5s), so a least-connections LB
	// counts the refusals (#131). Over HeldCap held, it closes at once.
	RefuseHold time.Duration // (500ms; 0: no hold)
	HeldCap    int           // (10k; 0: no hold), on top of nofile's MaxConns × (1 + Headroom)

	HandshakeTimeout time.Duration // accept → handshake done (5s, 3–30s)
	AuthTimeout      time.Duration // handshake done → LoginReq (5s, 3–30s)
	Heartbeat        time.Duration // sent in LoginResp; dead after 2× + 10s (150s)
	HeartbeatMin     time.Duration // Heartbeat range, as the SDK's (30s); the test profile lowers it
	HeartbeatMax     time.Duration // (300s, below a 350s LB idle timeout)

	LoginDeadline time.Duration // login_deadline, the same value as Logic's (5s)
	LoginTimeout  time.Duration // per entry from recv_ms, ≥ LoginDeadline + 1s (6s)
	LoginBatch    int           // entries per Login RPC (200)
	LoginLinger   time.Duration // longest wait to fill a batch (5ms)
	LoginRPCs     int           // concurrent Login RPCs (16)
	BusyRetry     Window        // Busy{retry_after}, uniform (1–3s)

	LogoutBatch   int           // entries per Logout RPC (200)
	LogoutLinger  time.Duration // (5ms)
	LogoutRPCs    int           // concurrent Logout RPCs (4)
	LogoutQueue   int           // Logouts queued or waiting for a retry (MaxConns)
	RPCTimeout    time.Duration // Logout RPC deadline (5s)
	RetryFor      time.Duration // Logout and ReconcileEnd retry budget (60s)
	RetryBackoff  time.Duration // first retry delay, doubled per attempt (1s)
	RetryMaxDelay time.Duration // longest delay between retries (10s)

	LeaseInterval time.Duration // GatewayLease period (10s)
	LeaseTimeout  time.Duration // per call (5s)
	LeaseTTL      time.Duration // Logic's lease TTL (30s), for the trigger rule

	Reconcile ReconcileConfig
	Drain     DrainConfig
	Windows   LoginWindows // reconnect windows sent in LoginResp (R5S-O6)

	Limits LimitsConfig
}

// ReconcileConfig sizes the pass controller (spec: Reconcile).
type ReconcileConfig struct {
	Cycle         time.Duration // periodic pass every Cycle ± Jitter (10m)
	Jitter        time.Duration // (1m)
	Chunk         int           // entries per chunk (200)
	TriggeredRate int           // entries/s of a pass after a missing lease (5000)
	MinRate       int           // floor of the periodic pace, entries/s (10)
	MaxInFlight   int           // chunks sent and not yet resolved (4)
	MaxRequeues   int           // per entry and pass (3, R3-O6)
	DeleteRate    int           // Logic's delete-phase rate, sizes the ReconcileEnd deadline (10k)
}

// DrainConfig is the SIGTERM drain (spec: Graceful drain).
type DrainConfig struct {
	Window time.Duration // Reconnect frames are spread over it (60s)
	Jitter time.Duration // Reconnect{jitter_ms} (5s, R4H-O3)
	Flush  time.Duration // after the last close, wait for Logouts (10s)
}

// Window is a uniform random wait [Lo, Hi].
type Window struct{ Lo, Hi time.Duration }

// LoginWindows are the reconnect windows LoginResp carries. A zero Window is
// not sent, so the SDK keeps its current value (R5S-O6).
type LoginWindows struct {
	StormWait, LocalTrigger, PreHandshakeRetry, Retryable Window
}

// LimitsConfig is the startup self-check (spec: Readiness, R5S-O7).
type LimitsConfig struct {
	Notrack       bool    // conntrack disabled for the gateway port
	Headroom      float64 // nofile ≥ MaxConns × (1 + Headroom) (0.10)
	ConntrackPath string  // node-wide nf_conntrack_max
	CountPath     string  // nf_conntrack_count, exported where readable
}

// DefaultConfig returns the spec defaults. GatewayID and Epoch must be set.
func DefaultConfig() Config {
	return Config{
		NumLoops:         16,
		MaxConns:         250_000,
		PendingCap:       100_000,
		LoginQueueCap:    8192,
		RefuseHold:       500 * time.Millisecond,
		HeldCap:          10_000,
		HandshakeTimeout: 5 * time.Second,
		AuthTimeout:      5 * time.Second,
		Heartbeat:        150 * time.Second,
		HeartbeatMin:     30 * time.Second,
		HeartbeatMax:     300 * time.Second,
		LoginDeadline:    5 * time.Second,
		LoginTimeout:     6 * time.Second,
		LoginBatch:       200,
		LoginLinger:      5 * time.Millisecond,
		LoginRPCs:        16,
		BusyRetry:        Window{time.Second, 3 * time.Second},
		LogoutBatch:      200,
		LogoutLinger:     5 * time.Millisecond,
		LogoutRPCs:       4,
		LogoutQueue:      250_000,
		RPCTimeout:       5 * time.Second,
		RetryFor:         60 * time.Second,
		RetryBackoff:     time.Second,
		RetryMaxDelay:    10 * time.Second,
		LeaseInterval:    10 * time.Second,
		LeaseTimeout:     5 * time.Second,
		LeaseTTL:         30 * time.Second,
		Reconcile: ReconcileConfig{Cycle: 10 * time.Minute, Jitter: time.Minute, Chunk: 200,
			TriggeredRate: 5000, MinRate: 10, MaxInFlight: 4, MaxRequeues: 3, DeleteRate: 10_000},
		Drain: DrainConfig{Window: 60 * time.Second, Jitter: 5 * time.Second, Flush: 10 * time.Second},
		Limits: LimitsConfig{Headroom: 0.10,
			ConntrackPath: "/proc/sys/net/netfilter/nf_conntrack_max",
			CountPath:     "/proc/sys/net/netfilter/nf_conntrack_count"},
	}
}

// NewEpoch returns a random non-zero boot epoch.
func NewEpoch() uint64 {
	var b [8]byte
	for {
		rand.Read(b[:])
		if e := binary.BigEndian.Uint64(b[:]); e != 0 {
			return e
		}
	}
}

// DeadAfter is when a silent ONLINE connection is dead: 2 × heartbeat + 10s.
func (c Config) DeadAfter() time.Duration { return 2*c.Heartbeat + 10*time.Second }

// FenceAfter is how long after recv_ms (or sent_ms) no Login (or Reconcile)
// write can still land: login_deadline + the 1s skew allowance.
func (c Config) FenceAfter() time.Duration { return c.LoginDeadline + skewAllowance }

// Validate refuses a configuration the design can't run with. The gateway
// refuses to start on an error (spec: Timeouts, Test execution model).
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	if err := session.ValidGateway(c.GatewayID); err != nil {
		bad("gateway id: %v", err)
	}
	if c.Epoch == 0 {
		bad("epoch must be non-zero (NewEpoch)")
	}
	if c.NumLoops < 1 || c.NumLoops > 1<<16 {
		bad("NumLoops %d must be in [1, 65536]: worker_id is 16 bits", c.NumLoops)
	}
	for name, v := range map[string]int{"MaxConns": c.MaxConns, "PendingCap": c.PendingCap,
		"LoginQueueCap": c.LoginQueueCap, "LoginBatch": c.LoginBatch, "LoginRPCs": c.LoginRPCs,
		"LogoutBatch": c.LogoutBatch, "LogoutRPCs": c.LogoutRPCs, "LogoutQueue": c.LogoutQueue,
		"Reconcile.Chunk": c.Reconcile.Chunk, "Reconcile.TriggeredRate": c.Reconcile.TriggeredRate,
		"Reconcile.MinRate": c.Reconcile.MinRate, "Reconcile.MaxInFlight": c.Reconcile.MaxInFlight,
		"Reconcile.DeleteRate": c.Reconcile.DeleteRate} {
		if v <= 0 {
			bad("%s %d must be positive", name, v)
		}
	}
	// A ReconcileEnd lists every tracked conn_id: open, or closed and
	// awaiting its Logout (P6b1I-1).
	if c.MaxConns+c.LogoutQueue > pb.MaxTrackedConnIDs {
		bad("MaxConns %d + LogoutQueue %d above %d: a ReconcileEnd would exceed Logic's %dMB message limit",
			c.MaxConns, c.LogoutQueue, pb.MaxTrackedConnIDs, pb.MaxMsgBytes>>20)
	}
	if c.HeldCap < 0 {
		bad("HeldCap %d must not be negative", c.HeldCap)
	}
	if c.RefuseHold < 0 || c.RefuseHold >= c.HandshakeTimeout {
		bad("RefuseHold %v must be in [0, HandshakeTimeout %v)", c.RefuseHold, c.HandshakeTimeout)
	}
	if c.Reconcile.MaxRequeues < 0 {
		bad("Reconcile.MaxRequeues %d must not be negative", c.Reconcile.MaxRequeues)
	}
	for name, d := range map[string]time.Duration{"HandshakeTimeout": c.HandshakeTimeout, "AuthTimeout": c.AuthTimeout} {
		if d < 3*time.Second || d > 30*time.Second {
			bad("%s %v outside the allowed 3s–30s", name, d)
		}
	}
	if c.Heartbeat < time.Second || c.Heartbeat%time.Second != 0 {
		bad("Heartbeat %v must be a whole number of seconds (LoginResp carries seconds)", c.Heartbeat)
	}
	if c.HeartbeatMin < time.Second || c.HeartbeatMin > c.HeartbeatMax {
		bad("HeartbeatMin %v and HeartbeatMax %v need 1s ≤ min ≤ max", c.HeartbeatMin, c.HeartbeatMax)
	} else if c.Heartbeat < c.HeartbeatMin || c.Heartbeat > c.HeartbeatMax {
		bad("Heartbeat %v outside [%v, %v]: the SDK would clamp it and disagree on the dead time", c.Heartbeat, c.HeartbeatMin, c.HeartbeatMax)
	}
	if c.LoginDeadline <= 0 {
		bad("LoginDeadline %v must be positive", c.LoginDeadline)
	}
	if c.LoginTimeout < c.FenceAfter() {
		bad("LoginTimeout %v must be at least login_deadline + 1s = %v (R1-O9)", c.LoginTimeout, c.FenceAfter())
	}
	for name, d := range map[string]time.Duration{"LoginLinger": c.LoginLinger, "LogoutLinger": c.LogoutLinger,
		"RPCTimeout": c.RPCTimeout, "RetryFor": c.RetryFor, "RetryBackoff": c.RetryBackoff,
		"RetryMaxDelay": c.RetryMaxDelay, "LeaseInterval": c.LeaseInterval, "LeaseTimeout": c.LeaseTimeout,
		"Reconcile.Cycle": c.Reconcile.Cycle, "Drain.Window": c.Drain.Window, "Drain.Jitter": c.Drain.Jitter} {
		if d <= 0 {
			bad("%s %v must be positive", name, d)
		}
	}
	if c.LeaseTimeout > c.LeaseInterval {
		bad("LeaseTimeout %v must not exceed LeaseInterval %v", c.LeaseTimeout, c.LeaseInterval)
	}
	if c.LeaseTTL <= c.LeaseInterval {
		bad("LeaseTTL %v must exceed LeaseInterval %v", c.LeaseTTL, c.LeaseInterval)
	}
	if c.Reconcile.Jitter < 0 || c.Reconcile.Jitter >= c.Reconcile.Cycle/2 {
		bad("Reconcile.Jitter %v must be in [0, Cycle/2)", c.Reconcile.Jitter)
	}
	if c.Drain.Flush < 0 {
		bad("Drain.Flush %v must not be negative", c.Drain.Flush)
	}
	if err := c.BusyRetry.check(false); err != nil {
		bad("BusyRetry: %v", err)
	}
	for name, w := range map[string]Window{"StormWait": c.Windows.StormWait, "LocalTrigger": c.Windows.LocalTrigger,
		"PreHandshakeRetry": c.Windows.PreHandshakeRetry, "Retryable": c.Windows.Retryable} {
		if err := w.check(true); err != nil {
			bad("Windows.%s: %v", name, err)
		}
	}
	if c.Limits.Headroom < 0 {
		bad("Limits.Headroom %v must not be negative", c.Limits.Headroom)
	}
	return errors.Join(errs...)
}

func (w Window) check(zeroOK bool) error {
	switch {
	case zeroOK && w == Window{}:
		return nil
	case w.Lo < 0 || w.Hi <= 0 || w.Lo >= w.Hi:
		return fmt.Errorf("[%v, %v] needs 0 ≤ lo < hi", w.Lo, w.Hi)
	case w.Hi > time.Duration(1<<32-1)*time.Millisecond:
		return fmt.Errorf("hi %v does not fit in uint32 ms", w.Hi)
	}
	return nil
}
