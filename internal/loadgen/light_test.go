package loadgen

import (
	"context"
	"net"
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
