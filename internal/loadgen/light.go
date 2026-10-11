package loadgen

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/sdk"
)

// lightConn is one light client (choice #80): no reconnect once logged in,
// no timers of its own; one goroutine reads, the shared pinger writes Pings.
// Until it logs in, it retries any failure after 0.5–1.5s, at most
// lightAttempts times, as the SDK retries a pre-handshake close: a gateway's
// handshake limit closes the excess of a fast ramp (spec: Admission control).
type lightConn struct {
	i       int
	pingAt  atomic.Int64 // unix ns of the unanswered Ping, 0: none
	reqID   atomic.Uint32
	mu      sync.Mutex
	c       *clientconn.Conn
	stopped bool
}

// stop closes the connection, or keeps it from opening.
func (lc *lightConn) stop() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.stopped = true
	if lc.c != nil {
		lc.c.Close()
	}
}

const lightAttempts = 10

// light logs client lc in and reads until its connection ends.
func (r *Runner) light(ctx context.Context, lc *lightConn) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		gw, why := r.lightConnect(ctx, lc, start.Add(r.cfg.Timeout))
		if why == "" {
			r.rec.Online(lc.i, gw, start, time.Now())
			break
		}
		r.rec.Failed(why)
		if ctx.Err() != nil {
			return
		}
		if attempt == lightAttempts {
			r.rec.Failed("gave-up")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500*time.Millisecond + rand.N(time.Second)):
		}
	}
	defer lc.c.Close()
	r.pinger.add(lc)
	why := lc.read(r.rec)
	r.pinger.remove(lc)
	if ctx.Err() != nil {
		why = "stopped"
	}
	r.rec.Lost(lc.i, why, time.Now())
}

// lightConnect dials, runs the handshake and Login by deadline; why is ""
// once logged in, else what failed (the connection is then closed).
func (r *Runner) lightConnect(ctx context.Context, lc *lightConn, deadline time.Time) (gw int, why string) {
	dctx, cancel := context.WithDeadline(ctx, deadline)
	nc, gw, err := r.bal.Dial(dctx)
	cancel()
	if err != nil {
		return gw, dialReason(err)
	}
	c := clientconn.New(nc)
	lc.mu.Lock()
	lc.c = c
	stopped := lc.stopped
	lc.mu.Unlock()
	if stopped {
		c.Close()
		return gw, "stopped"
	}
	if why = r.lightLogin(c, lc.i, deadline); why != "" {
		c.Close()
	}
	return gw, why
}

// dialReason names a failed dial for the failed counts: dial-timeout (no
// SYN-ACK by the attempt's deadline: SYNs lost, e.g. loopback drops in run
// 14), dial-refused, dial-no-port (EADDRNOTAVAIL: the source's ports are
// used up), else dial.
func dialReason(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "dial-timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "dial-refused"
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return "dial-no-port"
	}
	return "dial"
}

// lightLogin runs the handshake and Login; "" means logged in.
func (r *Runner) lightLogin(c *clientconn.Conn, i int, deadline time.Time) string {
	if err := c.Handshake(r.env.NoiseKeyID, r.env.NoisePublic, deadline); errors.Is(err, clientconn.ErrClosedBeforeReply) {
		return "pre-handshake"
	} else if err != nil {
		return "handshake"
	}
	tok, err := r.token(i)
	if err != nil {
		return "token"
	}
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: tok, Appid: AppID,
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: sdk.NewDeviceID(), ConnSeq: 1,
		ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}); err != nil {
		return "login"
	}
	f, err := c.Recv(deadline)
	switch {
	case err != nil:
		return "login"
	case f.Cmd == clientpb.Cmd_CMD_LOGIN_RESP:
		return ""
	case f.Cmd == clientpb.Cmd_CMD_BUSY:
		return "busy"
	case f.Cmd == clientpb.Cmd_CMD_ERROR:
		return "rejected"
	}
	return "login"
}

// read handles frames until the connection ends and says why it ended.
func (lc *lightConn) read(rec *Recorder) string {
	for {
		f, err := lc.c.Recv(time.Time{})
		if err != nil {
			return "lost"
		}
		switch f.Cmd {
		case clientpb.Cmd_CMD_PONG:
			if at := lc.pingAt.Swap(0); at != 0 {
				rec.Pong(time.Duration(time.Now().UnixNano() - at))
			}
		case clientpb.Cmd_CMD_KICK:
			return "kicked"
		case clientpb.Cmd_CMD_RECONNECT:
			return "reconnect"
		}
	}
}

// pinger sends every light client a Ping each PingEvery, spread evenly:
// clients sit in PingEvery/100ms slots by index, and one slot is pinged per
// 100ms tick. A Ping due while the last one is unanswered is counted
// missing.
type pinger struct {
	tick  time.Duration
	rec   *Recorder
	mu    sync.Mutex
	slots []map[*lightConn]struct{}
}

const pingTick = 100 * time.Millisecond

func newPinger(every time.Duration, rec *Recorder) *pinger {
	p := &pinger{tick: pingTick, rec: rec, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick)))}
	for i := range p.slots {
		p.slots[i] = map[*lightConn]struct{}{}
	}
	return p
}

func (p *pinger) add(lc *lightConn) {
	p.mu.Lock()
	p.slots[lc.i%len(p.slots)][lc] = struct{}{}
	p.mu.Unlock()
}

func (p *pinger) remove(lc *lightConn) {
	p.mu.Lock()
	delete(p.slots[lc.i%len(p.slots)], lc)
	p.mu.Unlock()
}

func (p *pinger) run(ctx context.Context) {
	t := time.NewTicker(p.tick)
	defer t.Stop()
	var due []*lightConn
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		due = due[:0]
		for lc := range p.slots[n%len(p.slots)] {
			due = append(due, lc)
		}
		p.mu.Unlock()
		now := time.Now().UnixNano()
		for _, lc := range due {
			if lc.pingAt.Swap(now) != 0 {
				p.rec.PongMissing()
			}
			lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) // a failure ends read
		}
	}
}
