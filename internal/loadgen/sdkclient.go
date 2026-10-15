package loadgen

import (
	"context"
	"net"
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
	gw     atomic.Int32             // gateway of the last dial
	tok    atomic.Pointer[string]   // the last token, reused until a refresh (spec: Client SDK rules)
	pre    atomic.Pointer[prepared] // the next attempt's first message (#106)
	queued atomic.Bool              // waiting for a precompute worker
	due    time.Time                // the next attempt's due time (its wait's end), zero: no wait (#107); run loop only
	start  time.Time                // the current attempt's start; run loop only
	online bool                     // run loop only
}

func (r *Runner) newSDK(i int) (*sdk.Client, error) {
	sc := &sdkClient{r: r, i: i}
	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
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
			nc, gw, err := r.bal.Dial(ctx)
			if err != nil {
				return nil, err
			}
			sc.gw.Store(int32(gw))
			dialed := time.Now()
			r.rec.ConnectLag(dialed.Sub(nc.(*conn).connecting))
			return &replyConn{Conn: nc, rec: r.rec, dialed: dialed}, nil
		},
		// A prepared frame is for the env's key, which is this Config's
		// KeyID and ServerKey: the SDK pins one key (P6b2a-stormI-3).
		Initiate: func(keyID uint16, pub []byte) (*wire.Initiator, []byte, error) {
			if r.pre != nil {
				if pr := r.pre.take(sc); pr != nil {
					r.rec.Handshake(true)
					return pr.in, pr.frame, nil
				}
			}
			r.rec.Handshake(false)
			return wire.Initiate(keyID, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		}})
}

// event runs on the client's run loop: it must not block.
func (sc *sdkClient) event(e sdk.Event) {
	now := time.Now()
	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
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

// replyConn measures loadgen's own lags on one connection: from the dial
// returning to the handshake frame, its first write (SendLag: making or
// taking the frame and the scheduling wait, run 25), and from the read that
// completed the server's handshake reply to the Login frame, its second
// write (ReplyLag: Finish and the Login's seal) (#107).
type replyConn struct {
	net.Conn
	rec    *Recorder
	dialed time.Time
	read   atomic.Int64 // unix ns the latest Read returned
	writes atomic.Int32
}

func (c *replyConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.read.Store(time.Now().UnixNano())
	return n, err
}

func (c *replyConn) Write(b []byte) (int, error) {
	switch c.writes.Add(1) {
	case 1:
		c.rec.SendLag(time.Since(c.dialed))
	case 2:
		if at := c.read.Load(); at != 0 {
			c.rec.ReplyLag(time.Duration(time.Now().UnixNano() - at))
		}
	}
	return c.Conn.Write(b)
}
