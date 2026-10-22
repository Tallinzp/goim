package loadgen

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
	"github.com/Tallinzp/goim/internal/wire"
)

// sdkClient is one sdk-mode client (choice #80): the real SDK with its
// reconnect rules, dialing through the balancer.
type sdkClient struct {
	r      *Runner
	i      int
	c      *sdk.Client
	conn   atomic.Pointer[replyConn] // the newest connection dialed, for Reset
	gw     atomic.Int32              // gateway of the last dial
	tok    atomic.Pointer[string]    // the last token, reused until a refresh (spec: Client SDK rules)
	pre    atomic.Pointer[prepared]  // the next attempt's first message (#106)
	queued atomic.Bool               // waiting for a precompute worker
	held   atomic.Pointer[handshake] // the last attempt's first message while no reply answered it (#109)
	dials  atomic.Uint64             // connections dialed; the latest is the attempt that runs (#109)
	due    time.Time                 // the next attempt's due time (its wait's end), zero: no wait (#107); run loop only
	start  time.Time                 // the current attempt's start; run loop only
	online bool                      // run loop only
}

func (r *Runner) newSDK(i int) (*sdkClient, error) {
	sc := &sdkClient{r: r, i: i}
	c, err := sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
		Token: func(_ context.Context, refresh bool) (string, error) {
			if t := sc.tok.Load(); t != nil && !refresh {
				return *t, nil // a phone's cached token
			}
			t, err := r.token(i)
			if err == nil {
				sc.tok.Store(&t)
			}
			return t, err
		},
		Store: &sdk.MemStore{}, OnEvent: sc.event, Windows: r.cfg.windows,
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			// Numbered when the dial starts: a cancelled attempt's dial that
			// returns after the next attempt's stays older (review
			// P6b2a-reuseI-1).
			seq := sc.dials.Add(1)
			nc, gw, err := r.bal.Dial(ctx)
			if err != nil {
				return nil, err
			}
			if seq == sc.dials.Load() {
				sc.gw.Store(int32(gw))
			}
			rc := sc.track(nc, seq)
			rc.gw = gw
			if seq == sc.dials.Load() {
				sc.conn.Store(rc)
			}
			r.rec.ConnectLag(rc.dialed.Sub(nc.(*conn).connecting))
			return rc, nil
		},
		Initiate: sc.initiate})
	sc.c = c
	return sc, err
}

// abort closes c with an RST, not a FIN (SO_LINGER 0): an LB resetting
// the flow (Reset, #134). Only an open connection: a ghosted one's socket
// is the keeper's (6b3c2 review M2).
func (c *replyConn) abort() {
	if !c.state.CompareAndSwap(connOpen, connClosed) {
		return
	}
	if tc := c.tcp(); tc != nil {
		tc.SetLinger(0)
	}
	c.Conn.Close()
}

// track wraps a connection the client just dialed; seq is its dial number,
// taken when its dial started (#107, #109).
func (sc *sdkClient) track(nc net.Conn, seq uint64) *replyConn {
	return &replyConn{Conn: nc, sc: sc, gw: -1, seq: seq, dialed: time.Now()}
}

// initiate is the client's Config.Initiate. It sends the last attempt's
// first message again while no reply byte answered it (#109): the gateway
// refuses a handshake over its limit before any X25519, so it costs the
// server the same, and loadgen is spared the two X25519 a phone makes on its
// own CPU. Otherwise it takes the precomputed one (#106) or makes one. A
// prepared or held frame is for the env's key, which is this Config's KeyID
// and ServerKey: the SDK pins one key (P6b2a-stormI-3). It assumes the
// gateway refuses by closing with no byte sent (a refusal frame would mark
// the handshake answered: reuse would stop) and keeps no replay check on a
// first message, which Noise NK lets anyone replay (a check would fail
// every resend) (review P6b2a-reuseI-3).
func (sc *sdkClient) initiate(keyID uint16, pub []byte) (*wire.Initiator, []byte, error) {
	r := sc.r
	if h := sc.held.Load(); h != nil && h.reusable() {
		r.rec.Handshake(HandshakeReused)
		return h.pr.in, h.pr.frame, nil
	}
	var pr *prepared
	how := HandshakePrecomputed
	if r.pre != nil {
		pr = r.pre.take(sc)
	}
	if pr == nil {
		in, frame, err := wire.Initiate(keyID, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		if err != nil {
			return nil, nil, err
		}
		pr, how = &prepared{in: in, frame: frame}, HandshakeInline
	}
	r.rec.Handshake(how)
	sc.held.Store(&handshake{pr: pr})
	return pr.in, pr.frame, nil
}

// event runs on the client's run loop: it must not block.
func (sc *sdkClient) event(e sdk.Event) {
	now := time.Now()
	if e.Kind == sdk.EventWaiting {
		sc.r.rec.Wait(e.Wait.Kind.String())
		if e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure {
			sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
		}
	}
	switch e.Kind {
	case sdk.EventConnecting:
		sc.start = now
		if !sc.due.IsZero() { // how late loadgen starts the attempt (#107)
			sc.r.rec.AttemptLag(now.Sub(sc.due))
			sc.due = time.Time{}
		}
	case sdk.EventOnline:
		sc.online = true
		sc.r.rec.Online(sc.i, int(sc.gw.Load()), sc.start, now)
		if sc.r.pre != nil {
			sc.r.pre.enqueue(sc)
		}
	case sdk.EventWaiting, sdk.EventIdle, sdk.EventLoginRequired:
		if e.Kind == sdk.EventWaiting {
			sc.due = now.Add(e.Wait.D)
		}
		switch {
		case sc.online:
			sc.online = false
			sc.r.rec.Lost(sc.i, e.End.String(), now)
		case e.End != 0:
			sc.r.rec.Failed(e.End.String())
		}
	}
}

// handshake is a client's last first message (#109). An Initiator serves
// one handshake, so it is sent again only while no reply byte has come back
// for it. The SDK runs one attempt at a time and dials before it initiates;
// an attempt it cancels (drop) keeps running until its connection's close
// lands, so it can write or read after the next attempt dialed. Only the
// client's newest connection may send the handshake or finish it, so no two
// attempts can finish it (review O3, M1, M2). The sender is kept as its
// dial number, not the connection (review M3).
type handshake struct {
	pr       *prepared
	mu       sync.Mutex
	owner    uint64 // the dial number of the connection that sent it last
	answered bool   // a reply byte came back on owner: never sent again
}

func (h *handshake) reusable() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.answered
}

// send: c sends h's frame. False if a reply answered h meanwhile, or a newer
// connection was dialed: c must not use it.
func (h *handshake) send(c *replyConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.answered || c.seq != c.sc.dials.Load() {
		return false
	}
	h.owner = c.seq
	return true
}

// reply: a reply byte for h came back on c. False if c did not send h last,
// or a newer connection was dialed: c must not finish it.
func (h *handshake) reply(c *replyConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.answered || h.owner != c.seq || c.seq != c.sc.dials.Load() {
		return false
	}
	h.answered = true
	return true
}

// errHandshakeTaken fails a connection that may not use the client's held
// handshake: a newer connection was dialed, or another one sent it since
// (#109).
var errHandshakeTaken = errors.New("loadgen: handshake used by a later attempt")

// replyConn measures loadgen's own lags on one connection: from the dial
// returning to the handshake frame, its first write (SendLag: making or
// taking the frame and the scheduling wait, run 25), and from the read that
// completed the server's handshake reply to the Login frame, its second
// write (ReplyLag: Finish and the Login's seal) (#107). It also tells the
// client's held handshake which connection may finish it (#109).
type replyConn struct {
	net.Conn
	sc     *sdkClient
	gw     int          // the gateway dialed
	seq    uint64       // its dial number (#109)
	state  atomic.Int32 // connOpen, connGhost, connClosed (Ghost)
	dialed time.Time
	read   atomic.Int64 // unix ns the latest Read returned
	writes atomic.Int32
	hs     atomic.Pointer[handshake] // the held handshake this connection sent (#109)
}

func (c *replyConn) Read(b []byte) (int, error) {
	if c.state.Load() == connGhost {
		return 0, errGhost
	}
	n, err := c.Conn.Read(b)
	if c.state.Load() == connGhost {
		return 0, errGhost
	}
	c.read.Store(time.Now().UnixNano())
	if h := c.hs.Load(); h != nil && n > 0 {
		if !h.reply(c) {
			return 0, errHandshakeTaken
		}
		c.hs.Store(nil)
		c.sc.held.CompareAndSwap(h, nil) // answered: the client keeps no Initiator
	}
	return n, err
}

// Write's first call is the handshake frame, recognized as the held one by
// its backing array: the SDK writes it unchanged in one Write (#109, review
// O4; TestSDKResendsARefusedHandshake). A connection older than the newest
// is a cancelled attempt's: its first message, held or one answered since,
// is not sent (review M1).
func (c *replyConn) Write(b []byte) (int, error) {
	if c.state.Load() == connGhost {
		return 0, errGhost
	}
	switch c.writes.Add(1) {
	case 1:
		c.sc.r.rec.SendLag(time.Since(c.dialed))
		if c.seq != c.sc.dials.Load() {
			return 0, errHandshakeTaken
		}
		if h := c.sc.held.Load(); h != nil && len(b) > 0 && len(h.pr.frame) > 0 && &b[0] == &h.pr.frame[0] {
			if !h.send(c) {
				return 0, errHandshakeTaken
			}
			c.hs.Store(h)
		}
	case 2:
		if at := c.read.Load(); at != 0 {
			c.sc.r.rec.ReplyLag(time.Duration(time.Now().UnixNano() - at))
		}
	}
	return c.Conn.Write(b)
}
