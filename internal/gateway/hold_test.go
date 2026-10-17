package gateway

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// refusal opens a socket on loop li that must be refused for reason; held
// reports whether the core kept it (#131).
func (h *core) refusal(li int, reason string) (k *fakeLink, held bool) {
	h.t.Helper()
	l := h.loops[li]
	k = &fakeLink{g: h.g, loop: l}
	var got string
	l.do(func() { k.c, got = h.g.Open(l, k) })
	if got != reason {
		h.t.Fatalf("open: reason %q, want %q", got, reason)
	}
	return k, k.c != nil
}

// alignTo moves the clock to at past a whole wheel tick, so a hold test
// knows how far the next tick is.
func (h *core) alignTo(at time.Duration) {
	ms := h.g.now().UnixMilli() % 1000
	h.advance((at - time.Duration(ms)*time.Millisecond + time.Second) % time.Second)
}

func heldNow(h *core) int { return int(testutil.ToFloat64(h.g.m.Held)) }
func heldTotal(h *core, result string) int {
	return int(testutil.ToFloat64(h.g.m.HeldTotal.WithLabelValues(result)))
}

func TestPendingMemRefusalIsHeldUntilTheTickAfterRefuseHold(t *testing.T) {
	h := newCore(t, func(c *Config) { c.PendingCap = 1 })
	h.open(0)
	h.alignTo(900 * time.Millisecond) // the next tick is 100ms away: a hold must outlast it
	k, held := h.refusal(1, RejectPendingMem)
	if !held || k.isClosed() {
		t.Fatalf("held %v closed %v, want the refused socket held open", held, k.isClosed())
	}
	if heldNow(h) != 1 || heldTotal(h, "held") != 1 || rejects(h, RejectPendingMem) != 1 {
		t.Fatalf("refusals_held %d, held_total %d, pending_mem %d; want 1, 1, 1", heldNow(h), heldTotal(h, "held"), rejects(h, RejectPendingMem))
	}
	if h.g.open.Load() != 1 || h.g.pending.Load() != 1 || h.g.tracked.Load() != 1 {
		t.Fatalf("open %d pending %d tracked %d, want 1 1 1: a held refusal counts in no cap and no table",
			h.g.open.Load(), h.g.pending.Load(), h.g.tracked.Load())
	}
	h.advance(400 * time.Millisecond)
	if k.isClosed() {
		t.Fatal("closed before RefuseHold (500ms)")
	}
	h.advance(800 * time.Millisecond) // 1.2s: the first tick after 500ms (1.1s)
	if !k.isClosed() || len(k.sent()) != 0 {
		t.Fatalf("closed %v frames %d, want a silent close by 1.5s", k.isClosed(), len(k.sent()))
	}
	h.sync()
	if heldNow(h) != 0 || closes(h, "held") != 1 {
		t.Fatalf("after the close: refusals_held %d, closes{held} %d; want 0, 1", heldNow(h), closes(h, "held"))
	}
}

func TestDrainingRefusalIsHeld(t *testing.T) {
	h := newCore(t, nil)
	h.g.draining.Store(true)
	if _, held := h.refusal(0, RejectDraining); !held {
		t.Fatal("a draining refusal must be held")
	}
}

func TestAtCapacityRefusalIsNotHeld(t *testing.T) {
	h := newCore(t, func(c *Config) { c.MaxConns = 1 })
	h.open(0)
	if _, held := h.refusal(1, RejectAtCapacity); held || heldTotal(h, "held") != 0 {
		t.Fatal("at_capacity must close at once: a gateway at the cap already has the most sockets")
	}
}

func TestHeldCapClosesTheRestAtOnce(t *testing.T) {
	h := newCore(t, func(c *Config) { c.PendingCap, c.HeldCap = 1, 2 })
	h.open(0)
	h.refusal(0, RejectPendingMem)
	h.refusal(1, RejectPendingMem)
	if _, held := h.refusal(2, RejectPendingMem); held {
		t.Fatal("third refusal held over HeldCap 2")
	}
	if heldNow(h) != 2 || heldTotal(h, "held") != 2 || heldTotal(h, "full") != 1 {
		t.Fatalf("refusals_held %d, held %d, full %d; want 2, 2, 1", heldNow(h), heldTotal(h, "held"), heldTotal(h, "full"))
	}
	h.advance(1500 * time.Millisecond)
	if _, held := h.refusal(2, RejectPendingMem); !held {
		t.Fatal("a refusal after the held ones closed must be held again")
	}
}

func TestClientCloseFreesAHeldRefusal(t *testing.T) {
	h := newCore(t, func(c *Config) { c.PendingCap, c.HeldCap = 1, 1 })
	h.open(0)
	k, _ := h.refusal(1, RejectPendingMem)
	k.clientClose()
	h.sync()
	if heldNow(h) != 0 || closes(h, "client") != 0 {
		t.Fatalf("refusals_held %d, closes{client} %d; want 0, 0", heldNow(h), closes(h, "client"))
	}
	if n := onLoop(k, func(c *Conn) int { return c.ls.wheel.n }); n != 0 {
		t.Fatalf("%d timers armed on the loop, want 0: the hold's timer must stop with the close", n)
	}
	if _, held := h.refusal(1, RejectPendingMem); !held {
		t.Fatal("the slot a client freed must be reused")
	}
	h.advance(1500 * time.Millisecond)
	if closes(h, "held") != 1 {
		t.Fatalf("closes{held} %d, want 1: the first refusal's timer must be stopped", closes(h, "held"))
	}
}

func TestHoldOff(t *testing.T) {
	for name, mod := range map[string]func(*Config){
		"RefuseHold 0": func(c *Config) { c.PendingCap, c.RefuseHold = 1, 0 },
		"HeldCap 0":    func(c *Config) { c.PendingCap, c.HeldCap = 1, 0 },
	} {
		t.Run(name, func(t *testing.T) {
			h := newCore(t, mod)
			h.open(0)
			if _, held := h.refusal(1, RejectPendingMem); held || heldTotal(h, "full") != 0 {
				t.Fatal("hold off: the refusal must close at once, not counted as full")
			}
		})
	}
}
