package loadgen

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/wire"
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

// A pinger that falls behind pings, at its next tick, every slot whose time
// has passed, each once however long the stall: slots follow the clock, not
// the ticks it got. Run 30's pinger left clients unpinged for 126s and the
// gateways closed 177,743 for their heartbeat; whether ticks were dropped or
// a Send blocked is open, so this covers the first and the Send deadline the
// second. How late it was and its slowest Send are recorded. The Send stub
// sleeps on synctest's clock.
func TestPingerCatchesUpOnMissedSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := NewRecorder(5, []string{"gw-0"})
		p := newPinger(5*pingTick, rec) // five slots, one client each
		var pings [5]int
		p.send = func(lc *lightConn, _ time.Time) error {
			pings[lc.i]++
			if lc.i == 0 && pings[0] == 1 {
				time.Sleep(10 * pingTick) // the first Send blocks for 1s
			}
			return nil
		}
		for i := range 5 {
			p.add(&lightConn{i: i})
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { p.run(ctx); close(done) }()
		time.Sleep(12*pingTick + pingTick/2) // the stall ends at 1.1s
		synctest.Wait()
		for i, n := range pings {
			if n < 1 || n > 2 {
				t.Errorf("client %d pinged %d times by 1.25s, want 1 or 2", i, n)
			}
		}
		c := rec.Counts()
		if c.PingTickLagMaxMs < 900 {
			t.Errorf("ping_tick_lag_max_ms %d, want >= 900", c.PingTickLagMaxMs)
		}
		if c.PingSendMaxMs < 1000 {
			t.Errorf("ping_send_max_ms %d, want >= 1000", c.PingSendMaxMs)
		}
		cancel()
		<-done
	})
}

// A Ping Send that misses its deadline closes its client, so its read ends
// lost, and is counted; the clients after it in the same visit are pinged,
// each stamped at its own Send, not at the blocked one's (6b3b2 review O1,
// O9).
func TestPingerTimesOutABlockedSend(t *testing.T) {
	t.Run("the real Send", testPingerRealSendDeadline)
	t.Run("stubbed", testPingerTimesOutABlockedSend)
}

// The pinger's own send sets the write deadline: a peer that never reads
// (a pipe, like a full socket buffer) times the Ping out and closes the
// client, instead of holding the shard (6b3b2 final review I-1).
func testPingerRealSendDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := NewRecorder(1, []string{"gw-0"})
		p := newPinger(pingTick, rec)
		a, b := net.Pipe()
		defer b.Close()
		key, err := wire.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		go func() { // the gateway's side: answer the handshake, then read nothing
			f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
			if _, err := io.ReadFull(b, f); err != nil {
				return
			}
			if _, reply, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE)); err == nil {
				b.Write(reply)
			}
		}()
		c := clientconn.New(a)
		if err := c.Handshake(1, key.Public, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		p.add(&lightConn{c: c})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { p.run(ctx); close(done) }()
		time.Sleep(pingTick + pingSendTimeout + pingTick/2)
		synctest.Wait()
		if c := rec.Counts(); c.PingSendTimeouts != 1 {
			t.Errorf("ping_send_timeouts %d, want 1", c.PingSendTimeouts)
		}
		b.SetReadDeadline(time.Now().Add(pingTick))
		if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Errorf("the blocked client's connection: read %v, want EOF (closed)", err)
		}
		cancel()
		a.Close() // a Send still blocked without a deadline returns now
		<-done
	})
}

func testPingerTimesOutABlockedSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := NewRecorder(2, []string{"gw-0"})
		p := newPinger(pingTick, rec) // one slot: every client every tick
		var calls int
		var blocked *lightConn
		var stamped int64 // the second client's pingAt at its Send
		p.send = func(lc *lightConn, deadline time.Time) error {
			calls++
			switch calls {
			case 1:
				blocked = lc
				time.Sleep(time.Until(deadline))
				return os.ErrDeadlineExceeded
			case 2:
				stamped = lc.pingAt.Load()
			}
			return nil
		}
		var peers [2]net.Conn
		for i := range 2 {
			a, b := net.Pipe()
			defer b.Close()
			peers[i] = b
			p.add(&lightConn{i: i, c: clientconn.New(a)})
		}
		t0 := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { p.run(ctx); close(done) }()
		time.Sleep(pingTick + pingSendTimeout + pingTick/2)
		synctest.Wait()
		if c := rec.Counts(); c.PingSendTimeouts != 1 {
			t.Errorf("ping_send_timeouts %d, want 1", c.PingSendTimeouts)
		}
		if want := t0.Add(pingTick + pingSendTimeout).UnixNano(); stamped < want {
			t.Errorf("the second client stamped %v before the blocked Send ended", time.Duration(want-stamped))
		}
		peers[blocked.i].SetReadDeadline(time.Now().Add(pingTick))
		if _, err := peers[blocked.i].Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Errorf("the blocked client's connection: read %v, want EOF (closed)", err)
		}
		cancel()
		<-done
	})
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
