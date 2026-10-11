package loadgen

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
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
