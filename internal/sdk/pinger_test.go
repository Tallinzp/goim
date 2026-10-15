package sdk

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// pingLog records a pinger's Pings and timeouts, as offsets from start.
type pingLog struct {
	mu       sync.Mutex
	start    time.Time
	pings    []time.Duration
	timeouts []time.Duration
	failSend bool
}

func (l *pingLog) send(uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failSend {
		return errors.New("broken")
	}
	l.pings = append(l.pings, time.Since(l.start))
	return nil
}

func (l *pingLog) timeout() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.timeouts = append(l.timeouts, time.Since(l.start))
}

func (l *pingLog) got() (pings, timeouts []time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Duration(nil), l.pings...), append([]time.Duration(nil), l.timeouts...)
}

// Review Focus 2: with no frame after the first Ping, the pinger reports a
// timeout PongTimeout after it, once, and sends nothing more.
func TestPingerTimesOutOncePongTimeoutAfterAPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &pingLog{start: time.Now()}
		var lastRecv atomic.Int64
		p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
		defer p.stop()
		time.Sleep(10 * time.Minute)
		pings, timeouts := l.got()
		if len(pings) != 1 || pings[0] != 30*time.Second {
			t.Fatalf("pings %v, want [30s]", pings)
		}
		if len(timeouts) != 1 || timeouts[0] != 40*time.Second {
			t.Fatalf("timeouts %v, want [40s]", timeouts)
		}
	})
}

// Review Focus 5: a frame after each Ping keeps the pinger going, one Ping
// per heartbeat, with no timeout.
func TestPingerKeepsPingingWhileAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &pingLog{start: time.Now()}
		var lastRecv atomic.Int64
		p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, func(id uint32) error {
			err := l.send(id)
			go func() { // the Pong, 1s later
				time.Sleep(time.Second)
				lastRecv.Store(int64(time.Since(l.start)))
			}()
			return err
		}, l.timeout)
		defer p.stop()
		time.Sleep(10*time.Minute + time.Second)
		pings, timeouts := l.got()
		if len(pings) != 20 || pings[19] != 600*time.Second {
			t.Fatalf("pings %v, want 20, the last at 600s", pings)
		}
		if len(timeouts) != 0 {
			t.Fatalf("timeouts %v, want none", timeouts)
		}
	})
}

// Review Focus 1: after stop, no firing re-arms, whether stop comes between
// firings or during one (stop does not wait for it), and stop twice is
// harmless.
func TestPingerStopsForGood(t *testing.T) {
	t.Run("between firings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now()}
			var lastRecv atomic.Int64
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
			time.Sleep(35 * time.Second) // one Ping out, its timeout due at 40s
			p.stop()
			p.stop()
			time.Sleep(10 * time.Minute)
			if pings, timeouts := l.got(); len(pings) != 1 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want one Ping and no timeout", pings, timeouts)
			}
		})
	})
	t.Run("during a firing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now()}
			var lastRecv atomic.Int64
			inSend, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, func(id uint32) error {
				once.Do(func() {
					close(inSend)
					<-release // a Send blocked on a full socket buffer
				})
				return l.send(id)
			}, l.timeout)
			<-inSend
			p.stop() // returns at once: it does not wait for the firing (review O2)
			close(release)
			time.Sleep(10 * time.Minute)
			// The released firing finishes its Ping but does not re-arm: no
			// timeout at 40s, no second Ping.
			if pings, timeouts := l.got(); len(pings) != 1 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want the one Ping and no timeout", pings, timeouts)
			}
		})
	})
	t.Run("a failed Send stops the Pings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now(), failSend: true}
			var lastRecv atomic.Int64
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
			defer p.stop()
			time.Sleep(10 * time.Minute)
			if pings, timeouts := l.got(); len(pings) != 0 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want none: the reader sees the broken connection", pings, timeouts)
			}
		})
	})
}

// Review Focus 4 (plan 6b2a-pinger): an online client holds one goroutine
// in its attempt (serve), not a second one for the pinger. b-stress's
// loadgen runs 600k SDK clients; each goroutine costs ~5.5KB of stack.
func TestAnOnlineClientHoldsOneAttemptGoroutine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(&clientpb.LoginResp{HeartbeatS: 30})
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(time.Second)
		expect(t, r.take(), EventConnecting, EventOnline)
		synctest.Wait()
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		n := 0
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "internal/sdk.(*attempt).") {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%d goroutines in an attempt, want 1 (serve)\n%s", n, buf)
		}
	})
}
