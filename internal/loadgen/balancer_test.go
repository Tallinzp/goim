package loadgen

import (
	"context"
	"errors"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// listeners starts n TCP listeners that accept and hold connections.
func listeners(t *testing.T, n int) []string {
	t.Helper()
	var addrs []string
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				t.Cleanup(func() { c.Close() })
			}
		}()
		addrs = append(addrs, l.Addr().String())
	}
	return addrs
}

func TestBalancerLeastConnectionsAndSources(t *testing.T) {
	b, err := NewBalancer(listeners(t, 3), 2)
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	srcs := map[string]int{}
	for range 6 {
		c, _, err := b.Dial(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		srcs[c.LocalAddr().(*net.TCPAddr).IP.String()]++
	}
	if got := b.Open(); got[0] != 2 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("open per gateway %v, want 2 each", got)
	}
	if srcs["127.0.0.2"] != 3 || srcs["127.0.0.3"] != 3 {
		t.Fatalf("sources %v, want 127.0.0.2 and .3 three times each", srcs)
	}
	// Closing frees the slot once: a second Close does not count again.
	conns[0].Close()
	conns[0].Close()
	if got := b.Open(); got[0]+got[1]+got[2] != 5 {
		t.Fatalf("open after one close %v", got)
	}
	// The next dial goes to the gateway that lost a connection.
	_, gw, err := b.Dial(context.Background())
	if err != nil || b.Open()[gw] != 2 {
		t.Fatalf("dial after close: gw %d %v %v", gw, b.Open(), err)
	}
}

func TestBalancerSkipsDownGateways(t *testing.T) {
	b, _ := NewBalancer(listeners(t, 2), 1)
	b.SetDown(0)
	for range 3 {
		if _, gw, err := b.Dial(context.Background()); err != nil || gw != 1 {
			t.Fatalf("dial: gw %d %v, want gw 1", gw, err)
		}
	}
	b.SetDown(1)
	if _, _, err := b.Dial(context.Background()); !errors.Is(err, ErrNoGateway) {
		t.Fatalf("all down: %v", err)
	}
}

func TestBalancerFailedDialFreesTheSlot(t *testing.T) {
	addrs := listeners(t, 1)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := l.Addr().String()
	l.Close()
	b, _ := NewBalancer([]string{dead, addrs[0]}, 1)
	if _, gw, err := b.Dial(context.Background()); err == nil || gw != 0 {
		t.Fatalf("dial to a closed port: gw %d %v", gw, err)
	}
	if got := b.Open(); got[0] != 0 {
		t.Fatalf("a failed dial still counts: %v", got)
	}
}

// Each socket has IP_BIND_ADDRESS_NO_PORT, so ports are per (source,
// destination) pair (#69).
func TestBalancerBindsWithoutPort(t *testing.T) {
	b, _ := NewBalancer(listeners(t, 1), 1)
	c, _, err := b.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.(*conn).Conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	raw.Control(func(fd uintptr) { v, gerr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT) })
	if gerr != nil || v != 1 {
		t.Fatalf("IP_BIND_ADDRESS_NO_PORT = %d %v, want 1", v, gerr)
	}
}

func TestSourcesRange(t *testing.T) {
	if _, err := Sources(0); err == nil {
		t.Fatal("0 sources accepted")
	}
	if _, err := Sources(254); err == nil {
		t.Fatal("254 sources accepted (127.0.0.255 is past the range)")
	}
	ips, _ := Sources(253)
	if ips[252].String() != "127.0.0.254" {
		t.Fatalf("last source %v", ips[252])
	}
}

// Sources turn per gateway: least connections visits the gateways in order,
// so one shared turn with as many sources as gateways would give each
// gateway one source, and its 28,232 ports per (source, destination) pair
// would run out at a fraction of the planned load (#69, final review).
func TestBalancerEveryGatewayUsesEverySource(t *testing.T) {
	b, _ := NewBalancer(listeners(t, 2), 2)
	pairs := map[[2]string]int{}
	for range 8 {
		c, gw, err := b.Dial(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pairs[[2]string{b.addrs[gw], c.LocalAddr().(*net.TCPAddr).IP.String()}]++
	}
	if len(pairs) != 4 {
		t.Fatalf("(gateway, source) pairs %v, want all 4 twice each", pairs)
	}
	for p, n := range pairs {
		if n != 2 {
			t.Fatalf("pair %v used %d times, want 2: %v", p, n, pairs)
		}
	}
}
