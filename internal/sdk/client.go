package sdk

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// Config configures a Client.
type Config struct {
	// Addrs are the gateway addresses; each attempt picks one at random. In
	// production it is the one NLB address; tests list several gateways.
	Addrs     []string
	KeyID     uint16 // key_id of the pinned server key
	ServerKey []byte // the pinned Noise static public key
	AppID     string
	Platform  clientpb.Platform
	// Token returns the access token. It is called before every attempt and
	// must honour ctx (TokenTimeout). refresh is true while the last Login
	// rejection was TOKEN_INVALID: return a new token, not a cached one.
	Token func(ctx context.Context, refresh bool) (string, error)
	// Store holds the persisted State. A Store that is a Locker is locked
	// by New, so a second Client on it fails with ErrStoreInUse.
	Store Store
	// OnEvent receives every event in order, on the client's goroutine. It
	// must not block or call the Client.
	OnEvent func(Event)

	Windows        Windows        // built-in defaults (DefaultWindows)
	Backoff        Backoff        // (DefaultBackoff)
	Heartbeat      HeartbeatRange // accepted LoginResp heartbeat (DefaultHeartbeatRange)
	TokenTimeout   time.Duration  // Token call (10s)
	PongTimeout    time.Duration  // no frame within this of a Ping: dead server (10s)
	ConnectTimeout time.Duration  // dial + handshake (10s)
	LoginTimeout   time.Duration  // LoginReq → reply (10s)
	LogoutTimeout  time.Duration  // Logout → LogoutResp (2s)

	Dial func(ctx context.Context, addr string) (net.Conn, error) // nil: TCP
	Rand *rand.Rand                                               // nil: randomly seeded
}

// DefaultConfig returns the timing defaults; the caller sets the rest. New
// gives any zero timing field its default.
func DefaultConfig() Config {
	return Config{Windows: DefaultWindows(), Backoff: DefaultBackoff(), Heartbeat: DefaultHeartbeatRange(),
		TokenTimeout: 10 * time.Second, PongTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second,
		LoginTimeout: 10 * time.Second, LogoutTimeout: 2 * time.Second}
}

// EventKind says what happened.
type EventKind int

const (
	EventConnecting    EventKind = iota + 1 // an attempt starts (Attempt)
	EventOnline                             // logged in (Resp)
	EventWaiting                            // the next attempt waits (Wait; Code or Kick: why)
	EventIdle                               // no attempt until Connect, or Foreground after REPLACED (Kick)
	EventLoginRequired                      // REVOKED: drop the token, show the login screen
	EventLoggedOut                          // Logout or SwitchAccount finished
	EventAnomaly                            // worth logging (Err): REPLACED, BAD_REQUEST, an unknown code, a capped server value, a store or Save failure
)

var eventNames = map[EventKind]string{EventConnecting: "connecting", EventOnline: "online", EventWaiting: "waiting",
	EventIdle: "idle", EventLoginRequired: "login-required", EventLoggedOut: "logged-out", EventAnomaly: "anomaly"}

func (k EventKind) String() string { return eventNames[k] }

// Attempt describes one connection attempt.
type Attempt struct {
	Addr      string
	Reason    clientpb.ReconnectReason
	ConnSeq   uint64
	OfflineMs int64
}

// Event is one step of the client's life.
type Event struct {
	Kind    EventKind
	Attempt Attempt             // EventConnecting
	Resp    *clientpb.LoginResp // EventOnline
	Wait    Wait                // EventWaiting
	Kick    clientpb.KickReason // the Kick that caused it, if any
	Code    clientpb.ErrorCode  // the Login error that caused it, if any
	Err     error               // the failure that caused it, if any
}

// Client holds at most one connection for one device_id (spec: device_id)
// and keeps it logged in by the reconnect rules. Its methods may be called
// from any goroutine.
type Client struct {
	cfg      Config
	deviceID atomic.Value // string; "" until the store is read
	unlock   func()       // releases the store's lock, or nil
	cmds     chan func()
	msgs     chan attemptMsg
	quit     chan struct{}
	done     chan struct{}

	// Owned by the run loop.
	pending  []Event // New's events, emitted first by the run loop
	pol      *policy
	st       State
	loaded   bool                // st was read from the store, or started fresh
	refresh  bool                // the last Login rejection was TOKEN_INVALID
	cur      *attempt            // in flight or online
	online   bool                // cur logged in
	timer    *time.Timer         // the pending wait, or nil
	wait     Wait                // what timer is for
	idleKick clientpb.KickReason // REPLACED: Foreground reconnects
	lostAt   time.Time           // the last established connection's loss; zero: none since login
}

// New loads the persisted state, creating device_id on a fresh install or
// when the record is corrupt, and starts the client idle: call Connect. A
// store that can't be read yet is kept: the client reads it again before
// each attempt. Store problems are reported as the first events.
func New(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 || cfg.ServerKey == nil || cfg.Token == nil || cfg.Store == nil {
		return nil, errors.New("sdk: Addrs, ServerKey, Token and Store are required")
	}
	def := DefaultConfig()
	for _, f := range []struct{ v, d *time.Duration }{{&cfg.PongTimeout, &def.PongTimeout},
		{&cfg.ConnectTimeout, &def.ConnectTimeout}, {&cfg.LoginTimeout, &def.LoginTimeout},
		{&cfg.LogoutTimeout, &def.LogoutTimeout}, {&cfg.TokenTimeout, &def.TokenTimeout},
		{&cfg.Backoff.Base, &def.Backoff.Base}, {&cfg.Backoff.Max, &def.Backoff.Max}} {
		if *f.v <= 0 {
			*f.v = *f.d
		}
	}
	if cfg.Windows == (Windows{}) {
		cfg.Windows = def.Windows
	}
	if cfg.Heartbeat == (HeartbeatRange{}) {
		cfg.Heartbeat = def.Heartbeat
	}
	if cfg.Heartbeat.Min <= 0 || cfg.Heartbeat.Min > cfg.Heartbeat.Max {
		return nil, errors.New("sdk: Heartbeat needs 0 < Min ≤ Max")
	}
	if cfg.Dial == nil {
		cfg.Dial = tcpDial
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	if cfg.OnEvent == nil {
		cfg.OnEvent = func(Event) {}
	}
	c := &Client{cfg: cfg, cmds: make(chan func()), msgs: make(chan attemptMsg),
		quit: make(chan struct{}), done: make(chan struct{}),
		pol: newPolicy(cfg.Windows, cfg.Backoff, cfg.Rand)}
	c.deviceID.Store("")
	if l, ok := cfg.Store.(Locker); ok {
		unlock, err := l.Lock()
		if err != nil {
			return nil, err
		}
		c.unlock = unlock
	}
	if err := c.load(); err != nil {
		c.pending = append(c.pending, Event{Kind: EventAnomaly, Err: err})
	}
	go c.run()
	return c, nil
}

// load reads the store. A corrupt record starts a fresh install: a new
// device_id is safe (the server sees another device on the slot), and
// conn_seq restarts with it; the corruption is queued as an anomaly. Any
// other error leaves the client unloaded and the record untouched.
func (c *Client) load() error {
	st, err := c.cfg.Store.Load()
	switch {
	case errors.Is(err, ErrCorrupt):
		c.pending = append(c.pending, Event{Kind: EventAnomaly, Err: err})
		st = State{}
	case err != nil:
		return err
	}
	st.Windows = st.Windows.Clamp()
	if st.DeviceID == "" {
		st.DeviceID = NewDeviceID()
		if err := c.cfg.Store.Save(st); err != nil {
			return err // never stored, so never used
		}
	}
	c.st, c.loaded = st, true
	c.deviceID.Store(st.DeviceID)
	c.pol.w = st.Windows.Over(c.cfg.Windows)
	return nil
}

// DeviceID is this install's device_id, or "" while the store can't be read.
func (c *Client) DeviceID() string { return c.deviceID.Load().(string) }

// Connect starts an attempt now if the client is idle: the first connect,
// or the user asking to go online after a Kick or a Logout.
func (c *Client) Connect() { c.do(func() { c.connect() }) }

// NetworkChanged reports an OS network change: it cuts the storm wait
// short (spec: Local trigger).
func (c *Client) NetworkChanged() { c.do(func() { c.local(false) }) }

// Foreground reports the app returning to the foreground: like a network
// change, and it also ends the idle state after a REPLACED Kick.
func (c *Client) Foreground() { c.do(func() { c.local(true) }) }

// Logout cancels every pending reconnect and retry, sends Logout if logged
// in and waits up to LogoutTimeout for LogoutResp, then closes. The client
// stays idle until Connect.
func (c *Client) Logout() { c.do(func() { c.logout() }) }

// SwitchAccount logs out, then connects with the new account's tokens.
func (c *Client) SwitchAccount(token func(ctx context.Context, refresh bool) (string, error)) {
	c.do(func() {
		c.logout()
		c.cfg.Token = token
		c.connect()
	})
}

// Close is the app closing: it cancels everything, closes any connection
// without a Logout, and stops the client.
func (c *Client) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
	}
	<-c.done
}

// do runs f on the run loop and waits for it.
func (c *Client) do(f func()) {
	ran := make(chan struct{})
	select {
	case c.cmds <- func() { f(); close(ran) }:
		<-ran
	case <-c.done:
	}
}

func (c *Client) run() {
	defer close(c.done)
	for _, e := range c.pending {
		c.emit(e)
	}
	c.pending = nil
	for {
		var fire <-chan time.Time
		if c.timer != nil {
			fire = c.timer.C
		}
		select {
		case <-c.quit:
			c.stopWait()
			c.drop()
			if c.unlock != nil {
				c.unlock()
			}
			return
		case f := <-c.cmds:
			f()
		case <-fire:
			c.timer = nil
			c.start(c.wait.Reason)
		case m := <-c.msgs:
			if m.a == c.cur {
				c.handle(m)
			}
		}
	}
}

func (c *Client) emit(e Event) { c.cfg.OnEvent(e) }

// connect starts an attempt if nothing is in flight or pending.
func (c *Client) connect() {
	if c.cur == nil && c.timer == nil {
		c.idleKick = 0
		c.start(c.pol.first())
	}
}

// local is a local trigger (spec: Precedence).
func (c *Client) local(foreground bool) {
	switch {
	case c.timer != nil:
		if w, ok := c.pol.local(c.wait); ok {
			c.stopWait()
			c.schedule(w, Event{})
		}
	case c.cur == nil && foreground && c.idleKick == clientpb.KickReason_KICK_REASON_REPLACED:
		c.idleKick = 0
		c.schedule(c.pol.localWait(), Event{})
	}
}

// start persists the next conn_seq, then starts an attempt (R1-O5: persist
// before each connection attempt). A store not read yet is read first.
func (c *Client) start(reason clientpb.ReconnectReason) {
	if !c.loaded {
		if err := c.load(); err != nil {
			c.fail(err)
			return
		}
		for _, e := range c.pending { // a corrupt record found now
			c.emit(e)
		}
		c.pending = nil
	}
	c.st.ConnSeq++
	if err := c.cfg.Store.Save(c.st); err != nil {
		c.st.ConnSeq--
		c.fail(err)
		return
	}
	var offline int64
	if !c.lostAt.IsZero() {
		offline = time.Since(c.lostAt).Milliseconds()
	}
	at := Attempt{Addr: c.cfg.Addrs[c.cfg.Rand.IntN(len(c.cfg.Addrs))], Reason: reason, ConnSeq: c.st.ConnSeq, OfflineMs: offline}
	req := &clientpb.LoginReq{Appid: c.cfg.AppID, Platform: c.cfg.Platform, DeviceId: c.st.DeviceID,
		ConnSeq: at.ConnSeq, ReconnectReason: reason, OfflineMs: offline}
	ctx, cancel := context.WithCancel(context.Background())
	a := &attempt{cancel: cancel, refresh: c.refresh}
	c.cur, c.online = a, false
	c.emit(Event{Kind: EventConnecting, Attempt: at})
	cfg := c.cfg // the attempt reads its own copy: SwitchAccount may change Token
	go a.run(ctx, &cfg, at.Addr, req, c.msgs)
}

// fail is a failure before any attempt: an anomaly, then a failure backoff.
func (c *Client) fail(err error) {
	c.emit(Event{Kind: EventAnomaly, Err: err})
	w, _ := c.pol.after(end{kind: endFailed, err: err})
	c.schedule(w, Event{Err: err})
}

func (c *Client) handle(m attemptMsg) {
	if m.note != nil {
		c.emit(Event{Kind: EventAnomaly, Err: m.note})
		return
	}
	if m.resp != nil {
		c.online = true
		c.lostAt = time.Time{}
		c.refresh = false
		c.pol.loggedIn()
		if hb, clamped := c.cfg.Heartbeat.Of(m.resp); clamped {
			c.emit(Event{Kind: EventAnomaly, Err: fmt.Errorf("sdk: heartbeat %ds clamped to %v", m.resp.GetHeartbeatS(), hb)})
		}
		if w := c.st.Windows.Merge(m.resp); w != c.st.Windows {
			c.st.Windows = w
			if err := c.cfg.Store.Save(c.st); err != nil {
				c.emit(Event{Kind: EventAnomaly, Err: err})
			}
			c.pol.w = w.Over(c.cfg.Windows)
		}
		c.emit(Event{Kind: EventOnline, Resp: m.resp})
		return
	}
	e := m.end
	if c.online {
		c.lostAt = time.Now() // the client detected the loss now (offline_ms)
	}
	c.cur, c.online = nil, false
	ev := Event{Kick: e.kick, Code: e.code, Err: e.err}
	if e.kind == endRejected {
		c.refresh = e.code == clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID
	}
	if e.kind == endRejected && e.code == clientpb.ErrorCode_ERROR_CODE_STALE {
		// Set the counter to max(local, stored); the next attempt adds 1.
		c.st.ConnSeq = max(c.st.ConnSeq, e.storedSeq)
		if err := c.cfg.Store.Save(c.st); err != nil {
			c.emit(Event{Kind: EventAnomaly, Err: err})
		}
	}
	if err := c.pol.anomaly(e); err != nil {
		c.emit(Event{Kind: EventAnomaly, Kick: e.kick, Code: e.code, Err: err})
	}
	revoked := e.kick == clientpb.KickReason_KICK_REASON_REVOKED ||
		(e.kind == endRejected && e.code == clientpb.ErrorCode_ERROR_CODE_REVOKED)
	if w, ok := c.pol.after(e); ok {
		c.schedule(w, ev)
		return
	}
	if revoked {
		ev.Kind = EventLoginRequired
	} else {
		ev.Kind = EventIdle
		c.idleKick = e.kick
	}
	c.emit(ev)
}

// schedule arms the wait; ev carries why.
func (c *Client) schedule(w Wait, ev Event) {
	c.wait, c.timer = w, time.NewTimer(w.D)
	ev.Kind, ev.Wait = EventWaiting, w
	c.emit(ev)
}

func (c *Client) stopWait() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

// drop aborts the attempt in flight or closes the connection, with no Logout.
func (c *Client) drop() {
	if c.cur != nil {
		c.cur.cancel()
		c.cur, c.online = nil, false
	}
}

// logout is the stop condition (spec: Stop conditions).
func (c *Client) logout() {
	c.stopWait()
	c.idleKick = 0
	c.lostAt = time.Time{}
	c.refresh = false
	if c.cur != nil && c.online {
		a := c.cur
		a.loggingOut.Store(true)
		if a.cc.Send(clientpb.Cmd_CMD_LOGOUT, 1, nil) == nil {
			t := time.NewTimer(c.cfg.LogoutTimeout)
		wait:
			for {
				select {
				case m := <-c.msgs:
					switch {
					case m.a != a:
					case m.note != nil:
						c.emit(Event{Kind: EventAnomaly, Err: m.note})
					case m.resp == nil:
						break wait // LogoutResp, or the connection ended
					}
				case <-t.C:
					break wait
				}
			}
			t.Stop()
		}
	}
	c.drop()
	c.emit(Event{Kind: EventLoggedOut})
}
