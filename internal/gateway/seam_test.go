package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func closes(h *core, reason string) int {
	return int(testutil.ToFloat64(h.g.m.Closes.WithLabelValues(reason)))
}

func TestTransportRejectAndCloseAreCounted(t *testing.T) {
	h := newCore(t, nil)
	for _, reason := range []string{RejectBadVersion, RejectNoiseUnknownKey, RejectHandshakeRate} {
		k, _ := h.open(0)
		k.loop.do(func() { h.g.Reject(k.c, reason) })
		h.sync()
		if !k.isClosed() || rejects(h, reason) != 1 || h.tracked(k) {
			t.Fatalf("%s: closed=%v rejects=%d tracked=%v", reason, k.isClosed(), rejects(h, reason), h.tracked(k))
		}
	}
	if n := closes(h, "rejected"); n != 3 {
		t.Fatalf("closes_total{rejected} = %d, want 3", n)
	}
	k := h.online(1, "u1")
	k.loop.do(func() { h.g.Close(k.c, "decrypt") })
	h.sync()
	if !k.isClosed() || closes(h, "decrypt") != 1 || closes(h, "client") != 0 {
		t.Fatal("a transport close must be counted under its reason, not as a client close")
	}
	if h.g.Config().NumLoops != len(h.loops) {
		t.Fatal("Config does not return the gateway's configuration")
	}
}

// cmd/gateway may stop or drain a gateway whose Start failed.
func TestStopAndDrainBeforeStartDoNotPanic(t *testing.T) {
	h := newCore(t, func(c *Config) { c.Drain.Window = 10 * time.Millisecond })
	h.g.Drain(context.Background())
	h.g.Stop()
}
