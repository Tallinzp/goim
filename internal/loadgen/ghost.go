package loadgen

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// errGhost fails a ghosted connection's reads and writes: the SDK sees a
// lost connection, and nothing it writes reaches the gateway (Ghost).
var errGhost = errors.New("loadgen: ghost: the network dropped the connection")

// ghosts keeps the sockets of POST /ghost open and unread, so their
// gateways see only silence, as when a carrier's network drops every flow
// (Plan 6b3c, review O1). A gateway closes each one when a relogin's Kick
// reaches it or its heartbeat sweep finds it dead (DeadAfter). The keeper
// waits for that close in its own epoll set (EPOLLRDHUP, no goroutine per
// socket), then drains and closes the socket, so none stays in CLOSE_WAIT,
// and counts the close per gateway: "kick" when a frame came first (the
// Kick), "silent" when none did (the sweep). A ghost keeps its balancer slot
// until then: the LB still holds the flow.
type ghosts struct {
	once   sync.Once
	ep     int
	err    error
	mu     sync.Mutex
	byFD   map[int32]*replyConn
	work   chan *replyConn
	done   chan struct{}
	open   atomic.Int64
	kick   []atomic.Int64 // per gateway
	silent []atomic.Int64 // per gateway
}

// GhostCloses is one gateway's ghosts closed since the run started.
type GhostCloses struct {
	Kick   int64 `json:"kick"`
	Silent int64 `json:"silent"`
}

func newGhosts(gateways int) *ghosts {
	return &ghosts{byFD: map[int32]*replyConn{}, done: make(chan struct{}),
		kick: make([]atomic.Int64, gateways), silent: make([]atomic.Int64, gateways)}
}

// start opens the epoll set and its loop on first use.
func (g *ghosts) start() error {
	g.once.Do(func() {
		if g.ep, g.err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); g.err != nil {
			return
		}
		g.work = make(chan *replyConn, 4096)
		for range 4 {
			go func() {
				for c := range g.work {
					g.close(c)
				}
			}()
		}
		go g.loop()
	})
	return g.err
}

// keep takes c's socket: it stays open, unread, until its gateway closes it.
func (g *ghosts) keep(c *replyConn) {
	tc := c.tcp()
	if tc == nil || g.start() != nil {
		c.Conn.Close()
		return
	}
	rc, err := tc.SyscallConn()
	if err != nil {
		c.Conn.Close()
		return
	}
	g.open.Add(1)
	var added bool
	rc.Control(func(fd uintptr) {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.byFD[int32(fd)] = c
		ev := unix.EpollEvent{Events: unix.EPOLLRDHUP, Fd: int32(fd)}
		if unix.EpollCtl(g.ep, unix.EPOLL_CTL_ADD, int(fd), &ev) == nil {
			added = true
		} else {
			delete(g.byFD, int32(fd))
		}
	})
	if !added {
		g.close(c)
	}
}

// loop hands every socket its gateway closed (or reset) to a closer.
func (g *ghosts) loop() {
	evs := make([]unix.EpollEvent, 256)
	for {
		n, err := unix.EpollWait(g.ep, evs, 1000)
		select {
		case <-g.done:
			return
		default:
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return
		}
		for _, ev := range evs[:n] {
			g.mu.Lock()
			c := g.byFD[ev.Fd]
			delete(g.byFD, ev.Fd)
			if c != nil {
				unix.EpollCtl(g.ep, unix.EPOLL_CTL_DEL, int(ev.Fd), nil)
			}
			g.mu.Unlock()
			if c != nil {
				g.work <- c
			}
		}
	}
}

// close drains what the gateway sent, then closes c's socket (a FIN, as
// nothing is left unread) and frees its balancer slot.
func (g *ghosts) close(c *replyConn) {
	kicked := false
	if tc := c.tcp(); tc != nil {
		tc.SetReadDeadline(time.Now().Add(time.Second))
		var buf [512]byte
		for {
			n, err := tc.Read(buf[:])
			kicked = kicked || n > 0
			if err != nil {
				break
			}
		}
	}
	c.Conn.Close()
	g.open.Add(-1)
	if c.gw >= 0 && c.gw < len(g.kick) {
		if kicked {
			g.kick[c.gw].Add(1)
		} else {
			g.silent[c.gw].Add(1)
		}
	}
}

// stop closes every ghost still kept, at the run's end.
func (g *ghosts) stop() {
	g.mu.Lock()
	left := g.byFD
	g.byFD = map[int32]*replyConn{}
	g.mu.Unlock()
	for _, c := range left {
		c.Conn.Close()
		g.open.Add(-1)
	}
	close(g.done)
}

// tcp is c's TCP socket under the balancer's wrapper.
func (c *replyConn) tcp() *net.TCPConn {
	if lc, ok := c.Conn.(*conn); ok {
		if tc, ok := lc.Conn.(*net.TCPConn); ok {
			return tc
		}
	}
	return nil
}

// Connection states for Ghost: a connection is ghosted only while open, and
// a ghosted one's Close hands the socket to the keeper.
const (
	connOpen int32 = iota
	connGhost
	connClosed
)

// ghost makes c's reads and writes fail at once while its socket stays
// open: Read and Write check the state, and the past deadline wakes a Read
// in progress. False if c was already closed.
func (c *replyConn) ghost() bool {
	if !c.state.CompareAndSwap(connOpen, connGhost) {
		return false
	}
	c.Conn.SetReadDeadline(aLongTimeAgo) // a blocked Read returns now
	return true
}

var aLongTimeAgo = time.Unix(1, 0)

func (c *replyConn) Close() error {
	if c.state.CompareAndSwap(connGhost, connClosed) {
		c.sc.r.ghosts.keep(c)
		return nil
	}
	if c.state.Swap(connClosed) == connClosed {
		return nil
	}
	return c.Conn.Close()
}

// Ghost is POST /ghost (Plan 6b3c, review O1): mark name on every gateway,
// its population every client logged in now, then every SDK client's
// newest connection is lost to the client (its read fails: EndLost, and its
// storm wait) while its socket stays open and unread, so each gateway still
// holds it ONLINE: a carrier's or a region's network that drops and then
// recovers. The relogins then meet the old connections on the gateways, the
// cap and Logic's Kicks. took is how long that took after the mark. A taken
// mark name changes nothing.
func (r *Runner) Ghost(name string) (n int, took time.Duration, err error) {
	if r.cfg.Mode != "sdk" {
		return 0, 0, ErrNotSDK
	}
	if err := r.ghosts.start(); err != nil {
		return 0, 0, err
	}
	if n, err = r.Mark(-1, name, false); err != nil {
		return 0, 0, err
	}
	begin := time.Now()
	r.eachSDK(func(sc *sdkClient) {
		if c := sc.conn.Load(); c != nil {
			c.ghost()
		}
	})
	return n, time.Since(begin), nil
}
