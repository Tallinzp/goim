package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// ErrNoGateway is a dial while every gateway is marked down.
var ErrNoGateway = errors.New("loadgen: every gateway is down")

// Balancer stands in for the NLB (choice #81): each dial goes to the live
// gateway with the fewest open connections (the least-connections algorithm
// the spec asks of the cloud LB, R5S-O4), from that gateway's next source
// address in turn. A gateway marked down (loadgen killed or silenced it) gets no new
// dials, as an LB removes a failed target.
type Balancer struct {
	addrs []string
	srcs  []net.IP
	mu    sync.Mutex
	open  []int
	down  []bool
	// next is each gateway's source turn. One turn shared by all gateways
	// falls into step with least connections, which visits the gateways in
	// order: with 4 gateways and 10 sources each gateway would get only 5
	// sources, and twice the connections per (source, destination) pair.
	next []int
}

// Sources are loadgen's source addresses 127.0.0.2, 127.0.0.3, … (choice
// #69): Linux answers on all of 127/8 with no setup.
func Sources(n int) ([]net.IP, error) {
	if n < 1 || n > 253 {
		return nil, fmt.Errorf("loadgen: %d sources, want 1–253", n)
	}
	ips := make([]net.IP, n)
	for i := range ips {
		ips[i] = net.IPv4(127, 0, 0, byte(2+i))
	}
	return ips, nil
}

// NewBalancer balances over addrs from n source addresses.
func NewBalancer(addrs []string, sources int) (*Balancer, error) {
	srcs, err := Sources(sources)
	if err != nil {
		return nil, err
	}
	return &Balancer{addrs: addrs, srcs: srcs, open: make([]int, len(addrs)), down: make([]bool, len(addrs)),
		next: make([]int, len(addrs))}, nil
}

// SetDown takes gateway gw out of rotation for new dials. It is permanent:
// a scenario that restarts a gateway adds a SetUp (P6a2aI-1).
func (b *Balancer) SetDown(gw int) {
	b.mu.Lock()
	b.down[gw] = true
	b.mu.Unlock()
}

// Down reports whether gateway gw is out of rotation.
func (b *Balancer) Down(gw int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.down[gw]
}

// Open is the number of open connections per gateway.
func (b *Balancer) Open() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.open...)
}

// pick takes a slot on the chosen gateway and that gateway's next source.
func (b *Balancer) pick() (int, net.IP, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	best := -1
	for i := range b.addrs {
		if !b.down[i] && (best < 0 || b.open[i] < b.open[best]) {
			best = i
		}
	}
	if best < 0 {
		return -1, nil, ErrNoGateway
	}
	b.open[best]++
	src := b.srcs[b.next[best]]
	b.next[best] = (b.next[best] + 1) % len(b.srcs)
	return best, src, nil
}

func (b *Balancer) release(gw int) {
	b.mu.Lock()
	b.open[gw]--
	b.mu.Unlock()
}

// Dial connects to the chosen gateway and returns its index. The
// connection counts as open on that gateway until it is closed.
func (b *Balancer) Dial(ctx context.Context) (net.Conn, int, error) {
	gw, src, err := b.pick()
	if err != nil {
		return nil, -1, err
	}
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: src}, Control: bindNoPort}
	nc, err := d.DialContext(ctx, "tcp", b.addrs[gw])
	if err != nil {
		b.release(gw)
		return nil, gw, err
	}
	return &conn{Conn: nc, release: func() { b.release(gw) }}, gw, nil
}

// bindNoPort sets IP_BIND_ADDRESS_NO_PORT: the kernel picks the source port
// at connect, per (source, destination) pair. Without it, bind() picks a
// port unique to the source address across every destination, and each
// source gives 28,232 connections in total instead of per gateway (#69).
func bindNoPort(_, _ string, rc syscall.RawConn) error {
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT, 1)
	}); err != nil {
		return err
	}
	return serr
}

// conn releases its balancer slot once, on the first Close.
type conn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *conn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
