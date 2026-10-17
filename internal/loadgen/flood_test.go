package loadgen

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
)

// A garbage handshake passes every check the gateway makes before the DH
// (header, length, key_id), so the flood costs the gateway its handshake
// computation, and fails the DH's AEAD check; no two are alike.
func TestGarbageHandshakeFailsOnlyAfterTheDH(t *testing.T) {
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	f := garbageHandshake(7)
	h := wire.ParseHeader(f)
	if err := h.Check(); err != nil || h.Cmd != uint16(clientpb.Cmd_CMD_HANDSHAKE) || h.Length != wire.HandshakeLen ||
		len(f) != wire.HeaderLen+wire.HandshakeLen {
		t.Fatalf("header %+v (%v), %d bytes", h, err, len(f))
	}
	if id := wire.KeyID(f); id != 7 {
		t.Fatalf("key_id %d, want 7", id)
	}
	if _, _, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE)); !errors.Is(err, wire.ErrHandshake) {
		t.Fatalf("Respond: %v, want ErrHandshake", err)
	}
	if bytes.Equal(f, garbageHandshake(7)) {
		t.Fatal("two garbage handshakes are equal")
	}
}

// Flood modes dial from all 253 sources by default ("from many sources"),
// in their own 127.0.SourceNet.x, and need Timeout.
func TestNewFloodModes(t *testing.T) {
	env := Env{GatewayIDs: []string{"gw-0"}, GatewayAddrs: []string{"127.0.0.1:1"}}
	for _, mode := range []string{ModeGarbage, ModeSlowHandshake, ModeSlowAuth} {
		r, err := New(Config{Mode: mode, Clients: 10, Rate: 1, UIDPrefix: "x", Timeout: time.Second, SourceNet: 3}, env)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if r.cfg.Sources != 253 || r.bal.srcs[0].String() != "127.0.3.2" || r.bal.srcs[252].String() != "127.0.3.254" {
			t.Fatalf("%s: %d sources from %v", mode, r.cfg.Sources, r.bal.srcs[0])
		}
		if r.Stats().Flood == nil {
			t.Fatalf("%s: no flood stats", mode)
		}
	}
	for _, tc := range []struct {
		cfg  Config
		want string
	}{
		{Config{Mode: ModeSlowAuth, Clients: 1, Rate: 1, UIDPrefix: "x"}, "Timeout"},
		{Config{Mode: ModeGarbage, Clients: 1, Rate: 1, UIDPrefix: "x", Timeout: time.Second, SourceNet: 256}, "source net"},
		{Config{Mode: ModeGarbage, Clients: 1, Rate: 1, UIDPrefix: "x", Timeout: time.Second, Precompute: true}, "precompute"},
		{Config{Mode: "flood", Clients: 1, Rate: 1, UIDPrefix: "x"}, "mode"},
	} {
		if _, err := New(tc.cfg, env); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err %v, want %q", tc.cfg, err, tc.want)
		}
	}
}

// metric is the value of name{label=value} in gateway id's registry.
func metric(t *testing.T, id, name, label, value string) float64 {
	t.Helper()
	reg, _ := gatewayRegs.Load(id)
	mfs, err := reg.(*prometheus.Registry).Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// The gateway computes and fails every garbage handshake and closes the
// connection itself, quickly; loadgen counts each attempt closed, none
// answered or timed out, and leaves no connection open when stopped. An
// attempt still in flight at the stop is not counted.
func TestGarbageFloodIsClosedByTheGateway(t *testing.T) {
	env, _ := startStack(t, 1)
	r, err := New(Config{Mode: ModeGarbage, Clients: 50, Rate: 200, UIDPrefix: "g", Timeout: 5 * time.Second}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	waitFor(t, "100 garbage handshakes closed", 10*time.Second, func() bool { return r.Stats().Flood.Outcomes[FloodClosed] >= 100 })
	stop()
	f := r.Stats().Flood
	closed := f.Outcomes[FloodClosed]
	if closed < f.Attempts-50 || len(f.Outcomes) != 1 || f.Open != 0 {
		t.Fatalf("flood %+v, want every attempt but those in flight at the stop closed, and none open", f)
	}
	if f.Held.P99 < 0 || f.Held.P99 > 1000 {
		t.Fatalf("held %+v, want closed within 1s", f.Held)
	}
	id := env.GatewayIDs[0]
	failed := metric(t, id, "goim_gateway_handshakes_total", "result", "failed")
	limited := metric(t, id, "goim_gateway_admission_rejects_total", "reason", gateway.RejectHandshakeRate)
	if n := int64(failed + limited); n < closed || n > f.Attempts || failed == 0 {
		t.Fatalf("gateway: %v failed + %v handshake_rate, loadgen %d attempts", failed, limited, f.Attempts)
	}
}

// A slow client holds its connection until the gateway's timeout for its
// state closes it (3s here, the shortest allowed; rounded up to the
// wheel's 1s tick), then dials again at once, so the flood keeps Clients
// connections open.
func TestSlowFloodHeldUntilTheGatewaysTimeout(t *testing.T) {
	for _, tc := range []struct{ mode, reason string }{
		{ModeSlowHandshake, gateway.RejectHandshakeTimeout},
		{ModeSlowAuth, gateway.RejectAuthTimeout},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			env, _ := startStack(t, 1, func(c *gateway.Config, _ *tcp.Config) {
				c.HandshakeTimeout, c.AuthTimeout = 3*time.Second, 3*time.Second
			})
			r, err := New(Config{Mode: tc.mode, Clients: 5, Rate: 100, UIDPrefix: "s", Timeout: 10 * time.Second}, env)
			if err != nil {
				t.Fatal(err)
			}
			stop := run(r)
			defer stop()
			waitFor(t, "5 slow connections open", 5*time.Second, func() bool { return r.Stats().Flood.Open == 5 })
			waitFor(t, "5 closed by the gateway", 10*time.Second, func() bool { return r.Stats().Flood.Outcomes[FloodClosed] >= 5 })
			waitFor(t, "5 open again", 5*time.Second, func() bool { return r.Stats().Flood.Open == 5 })
			f := r.Stats().Flood
			if f.Held.P50 < 3000 || f.Held.Max > 4500 || f.Outcomes[FloodTimeout] != 0 || f.Outcomes[FloodAnswered] != 0 {
				t.Fatalf("flood %+v, want held 3–4.5s, no timeout or answer", f)
			}
			if got := metric(t, env.GatewayIDs[0], "goim_gateway_admission_rejects_total", "reason", tc.reason); got < 5 {
				t.Fatalf("gateway %s rejects %v, want ≥ 5", tc.reason, got)
			}
		})
	}
}

// A slow client the gateway's timeout closed redials within one wheel tick,
// spread over it, so a cohort closed in one tick does not redial together;
// one closed sooner waits 0.5–1.5s, as the SDK does (O1).
func TestRedialWaitSpreadsOverATick(t *testing.T) {
	lo, hi := time.Hour, time.Duration(0)
	for range 200 {
		w := redialWait(5*time.Second, true)
		if w < 0 || w >= time.Second {
			t.Fatalf("after a timeout: %v, want 0–1s", w)
		}
		lo, hi = min(lo, w), max(hi, w)
		if w := redialWait(10*time.Millisecond, true); w < 500*time.Millisecond || w >= 1500*time.Millisecond {
			t.Fatalf("after an early close: %v, want 0.5–1.5s", w)
		}
		if w := redialWait(0, false); w < 500*time.Millisecond || w >= 1500*time.Millisecond {
			t.Fatalf("after a failure: %v, want 0.5–1.5s", w)
		}
	}
	if hi-lo < 500*time.Millisecond {
		t.Fatalf("after a timeout: %v–%v, want spread over the tick", lo, hi)
	}
}

// Against a gateway whose handshake bucket is nearly empty, slow-auth's
// handshakes are refused at the limit: loadgen counts them pre-handshake
// and backs off 0.5–1.5s each time, so it does not spin (O2).
func TestSlowAuthRefusedAtTheLimitBacksOff(t *testing.T) {
	env, _ := startStack(t, 1, func(_ *gateway.Config, tc *tcp.Config) { tc.HandshakeRate, tc.HandshakeBurst = 2, 1 })
	const clients = 20
	r, err := New(Config{Mode: ModeSlowAuth, Clients: clients, Rate: 1000, UIDPrefix: "l", Timeout: 10 * time.Second}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	begin := time.Now()
	time.Sleep(3 * time.Second)
	stop()
	elapsed := time.Since(begin)
	f := r.Stats().Flood
	// Each client refused waits ≥ 0.5s; the few admitted hold (5s timeout).
	if bound := int64(clients * (elapsed/(500*time.Millisecond) + 1)); f.Attempts > bound {
		t.Fatalf("flood %+v in %v: %d attempts, want ≤ %d (no spin)", f, elapsed, f.Attempts, bound)
	}
	if f.Outcomes[FloodPreHandshake] < clients || len(f.Outcomes) != 1 {
		t.Fatalf("flood %+v, want only pre-handshake outcomes, ≥ %d", f, clients)
	}
	id := env.GatewayIDs[0]
	if got := metric(t, id, "goim_gateway_admission_rejects_total", "reason", gateway.RejectHandshakeRate); got < clients {
		t.Fatalf("gateway handshake_rate %v, want ≥ %d", got, clients)
	}
	if got := metric(t, id, "goim_gateway_handshakes_total", "result", "no_token"); got < clients {
		t.Fatalf("gateway no_token %v, want ≥ %d", got, clients)
	}
}
