package loadgen

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	mrand "math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/wire"
)

// The flood modes (Plan 6b3a; spec: Load tests, "handshake flood" and
// "slow-client flood"; Failure handling, "Handshake flood", "Slow clients").
// None logs in: each attempt dials through the balancer and waits for the
// gateway to close the connection, so the gateway, not loadgen, closes
// first and the TIME_WAIT is the gateway's, not a loadgen source port's.
const (
	// ModeGarbage sends one well-formed handshake frame under the stack's
	// key_id with a random Noise message: the gateway admits it through the
	// handshake limit, computes the DH, fails the handshake and closes
	// (handshakes_total{result="failed"}), or refuses it at the limit
	// (handshake_rate). Attempts start at Rate per second, open loop, at
	// most Clients in flight.
	ModeGarbage = "garbage"
	// ModeSlowHandshake holds Clients connections that send nothing: each
	// sits in HANDSHAKE until the gateway's handshake timeout closes it.
	ModeSlowHandshake = "slow-handshake"
	// ModeSlowAuth holds Clients connections that complete the handshake
	// and send no Login: each sits in AUTH_PENDING until the auth timeout.
	ModeSlowAuth = "slow-auth"
)

func floodMode(mode string) bool {
	return mode == ModeGarbage || mode == ModeSlowHandshake || mode == ModeSlowAuth
}

// Flood outcomes, one per attempt. Dial failures use dialReason's names.
const (
	FloodClosed       = "closed"        // the gateway closed (FIN or RST): the outcome the spec expects
	FloodAnswered     = "answered"      // the gateway sent bytes where it should only close
	FloodTimeout      = "timeout"       // no close within Timeout: loadgen closed it
	FloodPreHandshake = "pre-handshake" // slow-auth: closed before the handshake reply (the limit)
	FloodHandshake    = "handshake"     // slow-auth: the handshake failed otherwise
	FloodError        = "error"         // another read error
)

// slowHeld is the shortest hold that counts as the gateway's timeout (its
// configured range is 3–30s): a slow client closed sooner (at capacity, at
// the handshake limit) waits 0.5–1.5s before it dials again, as the SDK
// does after a pre-handshake close. One closed by the timeout waits 0–1s,
// one tick of the gateway's timer wheel: dialing again at once would put
// every client the wheel closed in one tick back into one tick's interval,
// so the cohort would be closed and redial together every round, a burst
// of handshakes each tick (Plan 6b3a, O1). The flood still keeps ~90% of
// its slots.
const slowHeld = 3 * time.Second

// FloodStats are a flood run's counts (Stats.Flood). Held is from the
// garbage frame's write, the slow-handshake dial or the slow-auth
// handshake to the gateway's close, over the attempts it closed.
type FloodStats struct {
	Attempts int64            `json:"attempts"` // dials started
	Skipped  int64            `json:"skipped"`  // garbage: due while Clients attempts were in flight
	Open     int64            `json:"open"`     // connections open now
	Outcomes map[string]int64 `json:"outcomes"`
	Held     Dist             `json:"held"`
}

type floodRec struct {
	mu       sync.Mutex
	attempts int64
	skipped  int64
	open     int64
	outcomes map[string]int64
	held     *hist
}

func newFloodRec() *floodRec { return &floodRec{outcomes: map[string]int64{}, held: newHist()} }

func (f *floodRec) attempt() {
	f.mu.Lock()
	f.attempts++
	f.mu.Unlock()
}

func (f *floodRec) skip() {
	f.mu.Lock()
	f.skipped++
	f.mu.Unlock()
}

func (f *floodRec) opened(d int64) {
	f.mu.Lock()
	f.open += d
	f.mu.Unlock()
}

// outcome counts one attempt's end; held < 0 records no hold.
func (f *floodRec) outcome(why string, held time.Duration) {
	f.mu.Lock()
	f.outcomes[why]++
	if held >= 0 {
		f.held.add(held.Milliseconds())
	}
	f.mu.Unlock()
}

func (f *floodRec) stats() *FloodStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &FloodStats{Attempts: f.attempts, Skipped: f.skipped, Open: f.open, Outcomes: map[string]int64{},
		Held: f.held.dist(f.held.n)}
	for k, v := range f.outcomes {
		s.Outcomes[k] = v
	}
	return s
}

// garbageHandshake is a client handshake frame under keyID whose Noise
// message is random bytes: it passes the gateway's frame and key_id checks,
// and fails only once the gateway has computed the DH.
func garbageHandshake(keyID uint16) []byte {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	wire.Header{Version: wire.Version, Cmd: uint16(clientpb.Cmd_CMD_HANDSHAKE), Length: wire.HandshakeLen}.Put(f)
	binary.BigEndian.PutUint16(f[wire.HeaderLen:], keyID)
	rand.Read(f[wire.HeaderLen+wire.KeyIDLen:])
	return f
}

// garbage starts ModeGarbage's attempts at Rate per second until ctx ends.
// An attempt due while Clients are in flight is skipped and counted: the
// gateway closes slower than the rate asks, or loadgen cannot keep up.
func (r *Runner) garbage(ctx context.Context, wg *sync.WaitGroup) {
	sem := make(chan struct{}, r.cfg.Clients)
	begin := time.Now()
	for i := 0; ; i++ {
		due := begin.Add(time.Duration(float64(i) / r.cfg.Rate * float64(time.Second)))
		if d := time.Until(due); d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case sem <- struct{}{}:
			r.started.Add(1)
			wg.Go(func() {
				defer func() { <-sem }()
				r.garbageOnce(ctx)
			})
		default:
			r.flood.skip()
		}
	}
}

// garbageOnce is one ModeGarbage attempt.
func (r *Runner) garbageOnce(ctx context.Context) {
	nc, why := r.floodDial(ctx)
	if nc == nil {
		if why != "" {
			r.flood.outcome(why, -1)
		}
		return
	}
	defer r.floodClose(nc)
	sent := time.Now()
	if _, err := nc.Write(garbageHandshake(r.env.NoiseKeyID)); err != nil {
		if clientconn.IsClosed(err) && ctx.Err() == nil {
			r.flood.outcome(FloodClosed, time.Since(sent))
		}
		return
	}
	r.awaitClose(ctx, nc, sent)
}

// slow keeps one ModeSlowHandshake or ModeSlowAuth connection open, dialing
// again after redialWait whenever the gateway closes it, until ctx ends.
func (r *Runner) slow(ctx context.Context) {
	for ctx.Err() == nil {
		held, ok := r.slowOnce(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(redialWait(held, ok)):
		}
	}
}

// redialWait is a slow client's wait before it dials again (slowHeld).
func redialWait(held time.Duration, ok bool) time.Duration {
	if ok && held >= slowHeld {
		return mrand.N(time.Second)
	}
	return 500*time.Millisecond + mrand.N(time.Second)
}

// slowOnce is one slow connection: dial, the handshake in ModeSlowAuth,
// then silence until the gateway closes it. ok: the gateway closed it, held
// after the dial or the handshake.
func (r *Runner) slowOnce(ctx context.Context) (held time.Duration, ok bool) {
	nc, why := r.floodDial(ctx)
	if nc == nil {
		if why != "" {
			r.flood.outcome(why, -1)
		}
		return 0, false
	}
	defer r.floodClose(nc)
	if r.cfg.Mode == ModeSlowAuth {
		err := clientconn.New(nc).Handshake(r.env.NoiseKeyID, r.env.NoisePublic, time.Now().Add(r.cfg.Timeout))
		switch {
		case ctx.Err() != nil:
			return 0, false
		case errors.Is(err, clientconn.ErrClosedBeforeReply):
			r.flood.outcome(FloodPreHandshake, -1)
			return 0, false
		case err != nil:
			r.flood.outcome(FloodHandshake, -1)
			return 0, false
		}
	}
	return r.awaitClose(ctx, nc, time.Now())
}

// floodDial dials through the balancer by Timeout and counts the attempt
// and the open connection; why is "" when ctx ended.
func (r *Runner) floodDial(ctx context.Context) (net.Conn, string) {
	r.flood.attempt()
	dctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	nc, _, err := r.bal.Dial(dctx)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ""
		}
		return nil, dialReason(err)
	}
	r.flood.opened(1)
	return nc, ""
}

func (r *Runner) floodClose(nc net.Conn) {
	nc.Close()
	r.flood.opened(-1)
}

// awaitClose waits up to Timeout after from for the gateway to close nc,
// counts the outcome, and returns the hold when the gateway closed it.
// When ctx ends first the attempt is not counted.
func (r *Runner) awaitClose(ctx context.Context, nc net.Conn, from time.Time) (time.Duration, bool) {
	stop := context.AfterFunc(ctx, func() { nc.SetReadDeadline(time.Now()) })
	defer stop()
	if err := nc.SetReadDeadline(from.Add(r.cfg.Timeout)); err != nil {
		return 0, false
	}
	var b [64]byte
	n, err := nc.Read(b[:])
	held := time.Since(from)
	switch {
	case ctx.Err() != nil:
		return 0, false
	case n > 0:
		r.flood.outcome(FloodAnswered, -1)
	case clientconn.IsClosed(err):
		r.flood.outcome(FloodClosed, held)
		return held, true
	case errors.Is(err, os.ErrDeadlineExceeded):
		r.flood.outcome(FloodTimeout, -1)
	default:
		r.flood.outcome(FloodError, -1)
	}
	return 0, false
}
