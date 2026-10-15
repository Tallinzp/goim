package sdk

import (
	"sync"
	"sync/atomic"
	"time"
)

// pinger sends a Ping every hb and reports a timeout when no frame has
// arrived within pongTimeout of the oldest Ping sent since the last frame
// (spec: Dead-server detection). It runs on a timer, not on a goroutine of
// its own: an online client holds one goroutine for its connection, not
// two (plan 6b2a-pinger: b-stress's loadgen runs 600k SDK clients).
type pinger struct {
	hb, pongTimeout time.Duration
	start           time.Time
	lastRecv        *atomic.Int64 // ns since start of the last frame received
	send            func(reqID uint32) error
	timeout         func()

	// mu guards the state below, not send or timeout: stop never waits for
	// a firing (a blocked Send must not hold up serve), and a firing checks
	// stopped before it re-arms, so none runs after the one in progress.
	mu         sync.Mutex
	t          *time.Timer
	stopped    bool
	reqID      uint32
	next       time.Duration // next Ping, since start
	unanswered time.Duration // send time of the oldest Ping no frame has followed; -1: none
}

// startPinger arms the first Ping one heartbeat after start.
func startPinger(hb, pongTimeout time.Duration, start time.Time, lastRecv *atomic.Int64, send func(reqID uint32) error, timeout func()) *pinger {
	p := &pinger{hb: hb, pongTimeout: pongTimeout, start: start, lastRecv: lastRecv,
		send: send, timeout: timeout, reqID: 1, next: hb, unanswered: -1}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.t = time.AfterFunc(hb-time.Since(start), p.fire)
	return p
}

// fire sends a Ping when one is due, reports a timeout when the oldest
// unanswered Ping is pongTimeout old, and otherwise arms the next firing.
func (p *pinger) fire() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	now := time.Since(p.start)
	if p.unanswered >= 0 && p.lastRecv.Load() >= int64(p.unanswered) {
		p.unanswered = -1
	}
	if p.unanswered >= 0 && now >= p.unanswered+p.pongTimeout {
		p.mu.Unlock()
		p.timeout()
		return
	}
	ping := now >= p.next
	if ping {
		p.reqID++
	}
	reqID := p.reqID
	p.mu.Unlock()

	if ping && p.send(reqID) != nil {
		return // the reader sees the broken connection
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return // stopped during the Send: do not re-arm
	}
	if ping {
		if p.unanswered < 0 {
			p.unanswered = now
		}
		p.next = now + p.hb
	}
	at := p.next
	if p.unanswered >= 0 {
		at = min(at, p.unanswered+p.pongTimeout)
	}
	p.t.Reset(at - time.Since(p.start))
}

// stop ends the pinger without waiting for a firing in progress: that
// firing may still finish its Send or timeout, harmlessly (serve closes the
// connection first), but it does not re-arm, and none runs after it.
func (p *pinger) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.t.Stop()
}
