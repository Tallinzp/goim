package loadgen

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/internal/clientconn"
)

// A Ping due while the previous one has no Pong is counted missing; a
// removed client gets no more Pings.
func TestPingerCountsMissingPongs(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	rec := NewRecorder(1, []string{"gw-0"})
	p := newPinger(pingTick, rec)                // one slot: every client every tick
	lc := &lightConn{i: 0, c: clientconn.New(a)} // no handshake: Send fails at once, the Ping still counts as sent
	p.add(lc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	waitFor(t, "two missing Pongs", 2*time.Second, func() bool { return rec.Counts().PongMissing >= 2 })
	p.remove(lc)
	lc.pingAt.Store(0)
	time.Sleep(3 * pingTick)
	cancel()
	<-done
	if lc.pingAt.Load() != 0 {
		t.Fatal("a removed client was pinged")
	}
}

// A failed dial is counted by why it failed, so a run's report tells lost
// SYNs (timeout) from refused connections and exhausted source ports (run 14).
func TestDialFailuresAreCountedByReason(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	_, refused := net.Dial("tcp", closed)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	_, timeout := (&net.Dialer{}).DialContext(ctx, "tcp", closed)
	for _, c := range []struct {
		err  error
		want string
	}{
		{refused, "dial-refused"},
		{timeout, "dial-timeout"},
		{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.EADDRNOTAVAIL)}, "dial-no-port"},
		{errors.New("no gateway up"), "dial"},
	} {
		if got := dialReason(c.err); got != c.want {
			t.Errorf("%v: %q, want %q", c.err, got, c.want)
		}
	}
}

// The longest gap between two Pings to one client is recorded: a pinger that
// falls behind its ticks gets its clients closed by the gateway's heartbeat
// check while no Pong is ever missing (run 15: 146,836 heartbeat closes,
// pong_missing 0).
func TestPingerRecordsTheLongestPingGap(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	rec := NewRecorder(1, []string{"gw-0"})
	p := newPinger(2*pingTick, rec) // two slots: client 0 every 200ms
	lc := &lightConn{i: 0, c: clientconn.New(a)}
	p.add(lc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(7 * pingTick)
	if g := rec.Counts().PingGapMaxMs; g < 150 || g > 400 {
		t.Fatalf("on time: longest gap %dms, want ~200ms", g)
	}
	p.mu.Lock() // the pinger stalls for 1s
	time.Sleep(10 * pingTick)
	p.mu.Unlock()
	time.Sleep(5 * pingTick)
	if g := rec.Counts().PingGapMaxMs; g < 1000 {
		t.Fatalf("after a 1s stall: longest gap %dms, want >= 1000ms", g)
	}
}

// Clients are spread over pinger shards, each with its own goroutine, and
// every shard pings its clients.
func TestPingerShardsPingEveryClient(t *testing.T) {
	rec := NewRecorder(4, []string{"gw-0"})
	ps := newPingers(4, pingTick, rec)
	var lcs []*lightConn
	for i := range 4 {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		lc := &lightConn{i: i, c: clientconn.New(a)}
		ps.add(lc)
		lcs = append(lcs, lc)
	}
	for i, p := range ps {
		if n := len(p.slots[0]); n != 1 {
			t.Fatalf("shard %d holds %d clients, want 1", i, n)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ps.run(ctx); close(done) }()
	waitFor(t, "every client pinged", 2*time.Second, func() bool {
		for _, lc := range lcs {
			if lc.pingAt.Load() == 0 {
				return false
			}
		}
		return true
	})
	cancel()
	<-done
}

// A light client whose every dial fails gives up after lightAttempts: one
// failure counted per attempt, "gave-up" once, never online, and a 0.5-1.5s
// wait between attempts (6a2b minor 3). The waits run on synctest's clock;
// the dials are real and refused at once.
func TestLightGivesUpAfterItsAttempts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	synctest.Test(t, func(t *testing.T) {
		r, err := New(Config{Mode: "light", Clients: 1, Rate: 1, UIDPrefix: "u", PingEvery: time.Minute, Timeout: time.Second},
			Env{GatewayAddrs: []string{closed}, GatewayIDs: []string{"gw-0"}})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		r.light(t.Context(), &lightConn{i: 0})
		waited := time.Since(start)
		c := r.Recorder().Counts()
		if c.Failed["dial-refused"] != lightAttempts || c.Failed["gave-up"] != 1 || len(c.Failed) != 2 {
			t.Errorf("failed %v, want %d dial-refused and 1 gave-up", c.Failed, lightAttempts)
		}
		if c.Online != 0 || c.Logins != 0 {
			t.Errorf("online %d, logins %d, want none", c.Online, c.Logins)
		}
		if lo, hi := (lightAttempts-1)*500*time.Millisecond, (lightAttempts-1)*1500*time.Millisecond; waited < lo || waited > hi {
			t.Errorf("gave up after %v of waits, want %v-%v", waited, lo, hi)
		}
	})
}
