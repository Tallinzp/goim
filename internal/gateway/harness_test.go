package gateway

import (
	"encoding/base64"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// ---- fake transport ----

// fakeLoop is an event loop: one goroutine running queued funcs. Unlike gnet
// it keeps order, so sync can wait for everything queued before it.
type fakeLoop struct {
	mu     sync.Mutex
	cond   *sync.Cond
	q      []func()
	closed bool
	done   chan struct{}
}

func newFakeLoop() *fakeLoop {
	l := &fakeLoop{done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	go l.run()
	return l
}

var errLoopClosed = errors.New("fake loop closed")

func (l *fakeLoop) Execute(fn func()) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLoopClosed
	}
	l.q = append(l.q, fn)
	l.cond.Signal()
	return nil
}

func (l *fakeLoop) run() {
	defer close(l.done)
	for {
		l.mu.Lock()
		for len(l.q) == 0 && !l.closed {
			l.cond.Wait()
		}
		if len(l.q) == 0 {
			l.mu.Unlock()
			return
		}
		fn := l.q[0]
		l.q = l.q[1:]
		l.mu.Unlock()
		fn()
	}
}

// do runs fn on the loop and waits for it.
func (l *fakeLoop) do(fn func()) {
	done := make(chan struct{})
	if l.Execute(func() { fn(); close(done) }) != nil {
		return
	}
	<-done
}

func (l *fakeLoop) close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Signal()
	l.mu.Unlock()
	<-l.done
}

type frame struct {
	cmd   clientpb.Cmd
	reqID uint32
	body  []byte
}

// fakeLink is one connection's transport side. Close calls Gateway.Closed
// later, on the loop, as gnet's OnClose does.
type fakeLink struct {
	g    *Gateway
	loop *fakeLoop
	c    *Conn // set on the loop by open

	mu     sync.Mutex
	frames []frame
	closed bool
}

func (k *fakeLink) Send(cmd clientpb.Cmd, reqID uint32, body []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.closed {
		k.frames = append(k.frames, frame{cmd, reqID, append([]byte(nil), body...)})
	}
}

func (k *fakeLink) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return
	}
	k.closed = true
	k.loop.Execute(func() { k.g.Closed(k.c) })
}

// clientClose is the client hanging up (RST/FIN).
func (k *fakeLink) clientClose() { k.Close() }

func (k *fakeLink) isClosed() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.closed
}

func (k *fakeLink) sent() []frame {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]frame(nil), k.frames...)
}

// last returns the last frame with cmd, decoded into msg (if not nil).
func (k *fakeLink) last(t *testing.T, cmd clientpb.Cmd, msg proto.Message) bool {
	t.Helper()
	fs := k.sent()
	for i := len(fs) - 1; i >= 0; i-- {
		if fs[i].cmd == cmd {
			if msg != nil {
				if err := proto.Unmarshal(fs[i].body, msg); err != nil {
					t.Fatalf("decode %v: %v", cmd, err)
				}
			}
			return true
		}
	}
	return false
}

// ---- clock ----

// fakeClock is real time plus an offset tests can move forward.
type fakeClock struct{ off atomic.Int64 }

func (c *fakeClock) now() time.Time       { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *fakeClock) jump(d time.Duration) { c.off.Add(int64(d)) }

// ---- core harness (no Logic) ----

// core is a Gateway on fake loops, not started: tests drive the state
// machine and feed Login replies by hand.
type core struct {
	t     *testing.T
	g     *Gateway
	loops []*fakeLoop
	clock *fakeClock
}

func newCore(t *testing.T, mod func(*Config)) *core {
	t.Helper()
	cfg := testConfig()
	if mod != nil {
		mod(&cfg)
	}
	clock := &fakeClock{}
	reg := prometheus.NewRegistry()
	g, err := New(cfg, Deps{Metrics: NewMetrics(reg), Registry: reg, Clock: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	h := &core{t: t, g: g, clock: clock}
	for range cfg.NumLoops {
		h.loops = append(h.loops, newFakeLoop())
	}
	t.Cleanup(func() {
		for _, l := range h.loops {
			l.close()
		}
	})
	return h
}

// open accepts a connection on loop li; it returns nil link on a reject.
func (h *core) open(li int) (*fakeLink, string) {
	l := h.loops[li]
	k := &fakeLink{g: h.g, loop: l}
	var reason string
	l.do(func() {
		k.c, reason = h.g.Open(l, k)
	})
	if reason != "" {
		return nil, reason
	}
	return k, ""
}

// authed opens a connection and completes its handshake.
func (h *core) authed(li int) *fakeLink {
	h.t.Helper()
	k, reason := h.open(li)
	if reason != "" {
		h.t.Fatalf("open rejected: %s", reason)
	}
	k.loop.do(func() { h.g.HandshakeDone(k.c) })
	return k
}

func (h *core) frame(k *fakeLink, cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			h.t.Fatal(err)
		}
	}
	k.loop.do(func() { h.g.Frame(k.c, cmd, reqID, body) })
}

// login sends a LoginReq for uid and returns the queued item.
func (h *core) login(k *fakeLink, uid string) *loginItem {
	h.t.Helper()
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 7, loginReq(uid, "dev-"+uid, 1))
	select {
	case it := <-h.g.loginCh:
		return it
	case <-time.After(time.Second):
		h.t.Fatal("LoginReq was not queued")
		return nil
	}
}

// reply hands a worker's reply to the loop, as post does.
func (h *core) reply(it *loginItem, r loginReply) {
	it.ls.loop().(*fakeLoop).do(func() { h.g.applyLogin(it, r) })
}

// online logs a connection in with an OK reply.
func (h *core) online(li int, uid string) *fakeLink {
	h.t.Helper()
	k := h.authed(li)
	it := h.login(k, uid)
	it.state.Store(itemSent)
	h.reply(it, loginReply{outcome: loginOK, label: "ok", uid: uid, tokIatMs: 1})
	if k.c.state != stateOnline {
		h.t.Fatalf("state %s, want online", stateLabel[k.c.state])
	}
	return k
}

// advance moves the clock and runs every loop's wheel up to it.
func (h *core) advance(d time.Duration) {
	h.clock.jump(d)
	for i, l := range h.loops {
		ls := h.g.loops.states[i]
		l.do(func() { ls.wheel.advance(h.g.now()) })
	}
	h.sync()
}

// sync waits until every loop has run what was queued before it.
func (h *core) sync() {
	for _, l := range h.loops {
		l.do(func() {})
	}
}

// onLoop reads loop state on its goroutine.
func onLoop[T any](k *fakeLink, fn func(c *Conn) T) T {
	var v T
	k.loop.do(func() { v = fn(k.c) })
	return v
}

func (h *core) tracked(k *fakeLink) bool {
	return onLoop(k, func(c *Conn) bool { _, ok := c.ls.conns[c.id]; return ok })
}

// takeLogout returns the next queued Logout, or nil.
func (h *core) takeLogout() *logoutItem {
	select {
	case it := <-h.g.logoutCh:
		return it
	default:
		return nil
	}
}

func loginReq(uid, device string, seq uint64) *clientpb.LoginReq {
	return &clientpb.LoginReq{Token: fakeToken(uid), Appid: "app", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: device, ConnSeq: seq, ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// fakeToken is a JWS-shaped token with the given sub; only Logic verifies.
func fakeToken(sub string) string {
	return "eyJhbGciOiJFZERTQSJ9." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"`+sub+`"}`)) + ".c2ln"
}

// waitFor polls cond for up to 5s.
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
