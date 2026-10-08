package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

func TestDrainSendsReconnectClosesTheRestAndEndsTheEpoch(t *testing.T) {
	l := newLive(t, func(c *Config) {
		c.Drain.Window, c.Drain.Jitter, c.Drain.Flush = 2*time.Second, 5*time.Second, time.Second
	})
	kc := l.kickClient(t)
	ks := l.onlineMany(t, 3)
	auth := l.authed(0) // never logs in
	done := make(chan struct{})
	go func() { l.g.Drain(context.Background()); close(done) }()
	waitFor(t, "NotReady", func() bool { return l.g.Ready() != nil })
	if _, reason := l.open(1); reason != RejectDraining {
		t.Fatalf("open during drain: %q, want draining", reason)
	}
	kick(t, kc, "gw-1", 0xE1, ks[0].c.id, pb.KickReason_KICK_REASON_REPLACED) // Kicks still reach a draining gateway
	waitFor(t, "kicked", ks[0].isClosed)
	<-done
	for _, k := range ks[1:] {
		var r clientpb.Reconnect
		if !k.last(t, clientpb.Cmd_CMD_RECONNECT, &r) || r.JitterMs != 5000 {
			t.Fatalf("conn %x: Reconnect %v, want jitter 5000ms", k.c.id, &r)
		}
		if !k.isClosed() {
			t.Fatal("still open after the drain")
		}
	}
	if !auth.isClosed() || l.g.tracked.Load() != 0 {
		t.Fatalf("after drain: auth closed=%v, %d still tracked", auth.isClosed(), l.g.tracked.Load())
	}
	if n := get(l.logic, func() int { return l.logic.epochEnded }); n != 1 {
		t.Fatalf("EpochEnded called %d times", n)
	}
}
