// Package tcp is the gateway's TCP transport (spec: Transport seam): gnet
// event loops, the 6-byte frame codec and Noise NK. It turns sockets into the
// core's events (Open, HandshakeDone, Frame, Closed) on the connection's
// loop, and implements gateway.Loop and gateway.Link on top of gnet.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// maxLoops is gnet v2.10.0's limit on event loops (gfd.EventLoopIndexMax).
const maxLoops = 256

// Config is the transport's own configuration. The number of event loops is
// the core's NumLoops.
type Config struct {
	Addr string // gnet listen address, e.g. "tcp://:8000"
	// MaxClientFrame is the largest Length a client frame may have after the
	// handshake, checked from the header before the payload is buffered. A
	// LoginReq with a 4096-byte token fits in 8192 (roadmap choice #16).
	MaxClientFrame int
	// MaxOutbound caps the bytes queued for a client whose socket won't take
	// them; a frame that would pass it closes the connection as
	// slow_consumer, so a client that never reads can't grow gateway memory.
	// The default is one max frame (choice #46): only control frames are
	// sent in sub-project 1, so a client this far behind has stopped reading.
	MaxOutbound int
	// The handshake limit: handshakes computing at once, and a token bucket
	// on handshakes/s (spec: Admission control).
	HandshakeConcurrency int
	HandshakeRate        float64
	HandshakeBurst       int
}

// DefaultConfig returns the defaults for a gateway with numLoops loops.
// Plan 6 re-derives the handshake limit from the measured NK cost.
func DefaultConfig(numLoops int) Config {
	return Config{Addr: "tcp://:8000", MaxClientFrame: 8192, MaxOutbound: wire.HeaderLen + wire.MaxLength,
		HandshakeConcurrency: max(1, (numLoops+1)/2), HandshakeRate: 25_000, HandshakeBurst: 2_500}
}

// Validate refuses a configuration the transport can't run with.
func (c Config) Validate(numLoops int) error {
	var errs []error
	if numLoops > maxLoops {
		errs = append(errs, fmt.Errorf("NumLoops %d exceeds gnet's %d event loops", numLoops, maxLoops))
	}
	if c.MaxClientFrame < wire.HandshakeLen || c.MaxClientFrame > wire.MaxLength {
		errs = append(errs, fmt.Errorf("MaxClientFrame %d must be in [%d, %d]", c.MaxClientFrame, wire.HandshakeLen, wire.MaxLength))
	}
	if c.MaxOutbound < wire.HeaderLen+wire.MaxLength {
		errs = append(errs, fmt.Errorf("MaxOutbound %d must be at least %d (one frame)", c.MaxOutbound, wire.HeaderLen+wire.MaxLength))
	}
	if c.HandshakeConcurrency < 1 || c.HandshakeRate <= 0 || c.HandshakeBurst < 1 {
		errs = append(errs, errors.New("HandshakeConcurrency, HandshakeRate and HandshakeBurst must be positive"))
	}
	return errors.Join(errs...)
}

// Transport is the gnet event handler for one gateway.
type Transport struct {
	gnet.BuiltinEventEngine

	g     *gateway.Gateway
	cfg   Config
	loops int
	keys  wire.KeySet
	lim   *limiter
	hs    *prometheus.CounterVec // handshakes_total{result=ok|failed|no_slot|no_token}
	log   *slog.Logger

	byLoop sync.Map // gnet.EventLoop → *loop
	eng    gnet.Engine
	addr   string
	booted chan struct{}
	up     atomic.Bool
	done   chan error // gnet.Run returned

	// onLoop, if set, is called on the loop at every transport event and
	// Link call (the loop-ownership test).
	onLoop func(c *conn, site string)
}

// New builds the transport for g, with the Noise key set ks.
func New(g *gateway.Gateway, ks wire.KeySet, cfg Config, reg prometheus.Registerer) (*Transport, error) {
	n := g.Config().NumLoops
	if err := cfg.Validate(n); err != nil {
		return nil, err
	}
	if len(ks) == 0 {
		return nil, errors.New("tcp: empty Noise key set")
	}
	hs := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "goim_gateway", Name: "handshakes_total",
		Help: "Noise handshakes by result: computed (ok, failed) or refused by the limiter (no_slot, no_token)."}, []string{"result"})
	reg.MustRegister(hs)
	return &Transport{g: g, cfg: cfg, loops: n, keys: ks, hs: hs,
		lim:    newLimiter(cfg.HandshakeConcurrency, cfg.HandshakeRate, cfg.HandshakeBurst, time.Now),
		log:    slog.Default().With("transport", "tcp"),
		booted: make(chan struct{}), done: make(chan error, 1)}, nil
}

// Start runs gnet and returns once it listens. The listener stays open while
// the gateway drains: the core rejects new connections as draining, and the
// NLB stops sending them once the pod is NotReady.
func (t *Transport) Start() error {
	go func() {
		t.done <- gnet.Run(t, t.cfg.Addr,
			gnet.WithMulticore(true),
			gnet.WithNumEventLoop(t.loops),
			gnet.WithLoadBalancing(gnet.RoundRobin), // spec: Pre-allocation, chosen explicitly
			gnet.WithReusePort(false),               // one acceptor, so RoundRobin applies
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
			gnet.WithLogger(gnetLogger{t.log}))
	}()
	select {
	case <-t.booted:
		return nil
	case err := <-t.done:
		return fmt.Errorf("tcp: %w", err)
	}
}

// Addr is the bound listen address, once started.
func (t *Transport) Addr() string { return t.addr }

// Ready is the transport's readiness condition: listening, with a key set.
func (t *Transport) Ready() error {
	if !t.up.Load() {
		return errors.New("TCP listener not started")
	}
	return nil
}

// Stop shuts gnet down; every socket still open is closed and reported to
// the core through Closed.
func (t *Transport) Stop(ctx context.Context) error {
	if !t.up.Load() {
		return nil
	}
	if err := t.eng.Stop(ctx); err != nil {
		return err
	}
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OnBoot records the engine and the bound address.
func (t *Transport) OnBoot(eng gnet.Engine) gnet.Action {
	t.eng = eng
	if fd, err := eng.Dup(); err == nil {
		if sa, err := syscall.Getsockname(fd); err == nil {
			t.addr = sockaddr(sa)
		}
		syscall.Close(fd)
	}
	t.up.Store(true)
	close(t.booted)
	return gnet.None
}

func sockaddr(sa syscall.Sockaddr) string {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return (&net.TCPAddr{IP: a.Addr[:], Port: a.Port}).String()
	case *syscall.SockaddrInet6:
		return (&net.TCPAddr{IP: a.Addr[:], Port: a.Port}).String()
	}
	return ""
}

// loopOf returns the transport's state for gnet loop el, created the first
// time el is seen (on el's goroutine).
func (t *Transport) loopOf(el gnet.EventLoop) *loop {
	if l, ok := t.byLoop.Load(el); ok {
		return l.(*loop)
	}
	l := &loop{el: el, g: t.g}
	t.byLoop.Store(el, l)
	return l
}

// OnOpen admits the socket in the core, before any crypto (spec: Admission
// control). A rejected socket is closed at once; its OnClose finds no conn.
func (t *Transport) OnOpen(gc gnet.Conn) ([]byte, gnet.Action) {
	l := t.loopOf(gc.EventLoop())
	c := &conn{t: t, l: l, gc: gc}
	var reason string
	l.run(func() { c.c, reason = t.g.Open(l, c) })
	if reason != "" {
		return nil, gnet.Close
	}
	gc.SetContext(c)
	c.hook("open")
	return nil, gnet.None
}

// OnTraffic cuts and handles every whole frame buffered.
func (t *Transport) OnTraffic(gc gnet.Conn) gnet.Action {
	c, ok := gc.Context().(*conn)
	if !ok {
		return gnet.Close
	}
	c.hook("traffic")
	c.l.run(c.read)
	return gnet.None
}

// OnClose reports the close to the core, once per admitted connection.
func (t *Transport) OnClose(gc gnet.Conn, _ error) gnet.Action {
	if c, ok := gc.Context().(*conn); ok {
		c.hook("close")
		c.l.closed(c)
	}
	return gnet.None
}

// gnetLogger sends gnet's own log lines to slog.
type gnetLogger struct{ l *slog.Logger }

func (g gnetLogger) Debugf(f string, a ...any) { g.l.Debug(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Infof(f string, a ...any)  { g.l.Info(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Warnf(f string, a ...any)  { g.l.Warn(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Errorf(f string, a ...any) { g.l.Error(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Fatalf(f string, a ...any) {
	g.l.Error(fmt.Sprintf(f, a...))
	panic(fmt.Sprintf(f, a...))
}
