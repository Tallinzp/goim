package gateway

import (
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// Gateway is one gateway instance (pod name + boot epoch). A transport calls
// Open, HandshakeDone, Frame and Closed on the connection's loop goroutine.
type Gateway struct {
	cfg   Config
	m     *Metrics
	now   func() time.Time // wall clock: recv_ms, sent_ms, as_of_ms, fences
	ref   *pb.GatewayRef
	loops *loops
	log   *slog.Logger

	open        atomic.Int64 // sockets in any state (connection cap)
	pending     atomic.Int64 // HANDSHAKE + AUTH_PENDING sockets (pending cap)
	tracked     atomic.Int64 // table entries
	loginQueued atomic.Int64 // Logins waiting for or inside a Login RPC
	draining    atomic.Bool

	loginCh  chan *loginItem
	logoutCh chan *logoutItem
	gauges   [3]stateGauges
}

type stateGauges struct{ inc, dec func() }

// Deps are what a Gateway is built with; tests pass fakes.
type Deps struct {
	Metrics  *Metrics             // registered on Registry (NewMetrics)
	Registry *prometheus.Registry // served on /metrics
	// Clock is the wall clock (nil: time.Now). Plan 5 injects a skewed one.
	Clock func() time.Time
	// ReadyChecks are the transport's readiness conditions, e.g. the Noise
	// key set is loaded (Plan 4).
	ReadyChecks []func() error
}

// New builds the gateway core. Call Start to connect it to Logic.
func New(cfg Config, d Deps) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	now, m := d.Clock, d.Metrics
	if now == nil {
		now = time.Now
	}
	g := &Gateway{cfg: cfg, m: m, now: now,
		ref:      &pb.GatewayRef{Gateway: cfg.GatewayID, Epoch: cfg.Epoch},
		loops:    newLoops(cfg, now()),
		log:      slog.Default().With("gw", cfg.GatewayID, "epoch", cfg.Epoch),
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
	}
	return g, nil
}

// Open admits a new socket on loop l (spec: Admission control). It returns
// the reject reason, already counted, when the transport must close the
// socket before any crypto.
func (g *Gateway) Open(l Loop, link Link) (*Conn, string) {
	ls := g.loops.bind(l)
	switch {
	case g.draining.Load():
		return nil, g.reject(RejectDraining)
	case g.open.Add(1) > int64(g.cfg.MaxConns):
		g.open.Add(-1)
		return nil, g.reject(RejectAtCapacity)
	case g.pending.Add(1) > int64(g.cfg.PendingCap):
		g.pending.Add(-1)
		g.open.Add(-1)
		return nil, g.reject(RejectPendingMem)
	}
	c := &Conn{id: ls.nextID(), ls: ls, link: link, state: stateHandshake}
	c.onLife = func() { g.lifeExpired(c) }
	c.onFence = func() { g.fenceExpired(c) }
	ls.conns[c.id] = c
	g.tracked.Add(1)
	g.m.Tracked.Inc()
	g.gauges[stateHandshake].inc()
	ls.wheel.schedule(&c.life, g.now().Add(g.cfg.HandshakeTimeout), c.onLife)
	return c, ""
}

func (g *Gateway) reject(reason string) string {
	g.m.Rejects.WithLabelValues(reason).Inc()
	return reason
}

// HandshakeDone moves c from HANDSHAKE to AUTH_PENDING.
func (g *Gateway) HandshakeDone(c *Conn) {
	if c.state != stateHandshake || c.closing {
		return
	}
	g.setState(c, stateAuthPending)
	c.ls.wheel.schedule(&c.life, g.now().Add(g.cfg.AuthTimeout), c.onLife)
}

// Frame handles one decoded frame from the client.
func (g *Gateway) Frame(c *Conn, cmd clientpb.Cmd, reqID uint32, body []byte) {
	if c.closing || c.state == stateClosed {
		return
	}
	switch c.state {
	case stateAuthPending:
		// AUTH_PENDING accepts one LoginReq and nothing else.
		if cmd != clientpb.Cmd_CMD_LOGIN || c.outcome != loginNone || c.login != nil {
			g.closeConn(c, "protocol")
			return
		}
		g.loginFrame(c, reqID, body)
	case stateOnline:
		c.lastActive = g.now().UnixMilli() // any frame resets the heartbeat, lazily
		switch cmd {
		case clientpb.Cmd_CMD_PING:
			c.link.Send(clientpb.Cmd_CMD_PONG, reqID, nil)
		case clientpb.Cmd_CMD_LOGOUT:
			// Reply at once; the Logout RPC goes through the fence (R4H-O8).
			c.link.Send(clientpb.Cmd_CMD_LOGOUT_RESP, reqID, nil)
			g.closeConn(c, "logout")
		default:
			g.closeConn(c, "protocol")
		}
	default: // HANDSHAKE: the transport delivers no frames before the handshake
		g.closeConn(c, "protocol")
	}
}

// Closed is called once per accepted connection, after its socket closed.
func (g *Gateway) Closed(c *Conn) {
	if c.state == stateClosed {
		return
	}
	if !c.closing {
		g.m.Closes.WithLabelValues("client").Inc()
	}
	prev := c.state
	g.gauges[prev].dec()
	g.open.Add(-1)
	if prev == stateHandshake || prev == stateAuthPending {
		g.pending.Add(-1)
	}
	c.state, c.closing, c.link = stateClosed, false, nil
	c.ls.wheel.stop(&c.life)
	g.m.AwaitingLogout.Inc()
	if c.outcome == loginPending && c.login.state.CompareAndSwap(itemQueued, itemCancelled) {
		// Never sent: it can't reach Logic, so it needs no Logout.
		c.outcome, c.login = loginNone, nil
		c.ls.wheel.stop(&c.fence)
		g.loginFinished()
	}
	g.settle(c)
}

func (g *Gateway) setState(c *Conn, s state) {
	g.gauges[c.state].dec()
	g.gauges[s].inc()
	if c.state == stateAuthPending && s == stateOnline {
		g.pending.Add(-1)
	}
	c.state = s
}

// closeConn asks the transport to close c; Closed follows.
func (g *Gateway) closeConn(c *Conn, reason string) {
	if c.closing || c.state == stateClosed {
		return
	}
	c.closing = true
	g.m.Closes.WithLabelValues(reason).Inc()
	c.link.Close()
}

func (g *Gateway) send(c *Conn, cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	body, err := proto.Marshal(msg)
	if err != nil {
		g.log.Error("marshal client frame", "cmd", cmd, "err", err)
		return
	}
	c.link.Send(cmd, reqID, body)
}

// lifeExpired is c's HANDSHAKE, AUTH_PENDING or heartbeat timer (spec:
// Timeouts, Heartbeat). Timeouts close silently.
func (g *Gateway) lifeExpired(c *Conn) {
	if c.closing {
		return
	}
	switch c.state {
	case stateHandshake:
		g.reject(RejectHandshakeTimeout)
		g.closeConn(c, "timeout")
	case stateAuthPending:
		if c.outcome == loginNone { // the timer stops when LoginReq arrives
			g.reject(RejectAuthTimeout)
			g.closeConn(c, "timeout")
		}
	case stateOnline:
		dead := time.UnixMilli(c.lastActive).Add(g.cfg.DeadAfter())
		if g.now().Before(dead) {
			c.ls.wheel.schedule(&c.life, dead, c.onLife) // lazy re-arm
			return
		}
		g.closeConn(c, "heartbeat")
	}
}

// busy sends Busy{retry_after}, uniform in BusyRetry (R4G-O12), and closes.
func (g *Gateway) busy(c *Conn, reqID uint32) {
	w := g.cfg.BusyRetry
	after := w.Lo + rand.N(w.Hi-w.Lo+1)
	g.send(c, clientpb.Cmd_CMD_BUSY, reqID, &clientpb.Busy{RetryAfterMs: uint32(after.Milliseconds())})
	g.closeConn(c, "busy")
}

// kickConn sends the Kick frame and closes (spec: Kick handling).
func (g *Gateway) kickConn(c *Conn, reason clientpb.KickReason, metric string) {
	g.send(c, clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: reason})
	g.closeConn(c, metric)
}

// ---- Logout fence (spec: Logout fence) ----

// fenceExpired is fence (b): recv_ms + login_deadline + 1s has passed, so no
// write for this Login can still land.
func (g *Gateway) fenceExpired(c *Conn) {
	c.fenceB = true
	g.settle(c)
}

// settle decides what a closed connection still needs: nothing (untrack it),
// a fenced Logout, or more waiting. It runs whenever a fence condition may
// have changed.
func (g *Gateway) settle(c *Conn) {
	if c.state != stateClosed || c.logoutQueued {
		return
	}
	if c.inChunk { // fence (c): its Reconcile chunk is unresolved
		return
	}
	uid := g.logoutUID(c)
	if uid == "" || (c.outcome != loginPending && c.outcome != loginOK && c.outcome != loginUnknown) {
		g.untrack(c) // no Login reached Logic, or it definitively wrote nothing
		return
	}
	if c.outcome == loginOK || c.fenceB { // fence (a) or (b)
		c.logoutQueued = true
		g.queueLogout(c, uid)
	}
}

// logoutUID is the uid a Logout for c must name: Logic's verified uid, or,
// with no definitive reply, the token's unverified sub claim. A wrong sub only
// makes the compare-and-delete miss; an unreadable token can't have been
// accepted, so it needs no Logout.
func (g *Gateway) logoutUID(c *Conn) string {
	if c.uid != "" || c.login == nil {
		return c.uid
	}
	c.uid = tokenSubject(c.login.entry.GetToken())
	return c.uid
}

// untrack removes a closed connection from its loop's table: it has passed
// its Logout fence for good (spec: Tracked connections).
func (g *Gateway) untrack(c *Conn) {
	if _, ok := c.ls.conns[c.id]; !ok {
		return
	}
	delete(c.ls.conns, c.id)
	c.ls.wheel.stop(&c.fence)
	c.login = nil
	g.tracked.Add(-1)
	g.m.Tracked.Dec()
	g.m.AwaitingLogout.Dec()
}
