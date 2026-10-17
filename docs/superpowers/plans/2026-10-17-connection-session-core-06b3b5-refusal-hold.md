# Plan 6b3b5: Hold a Refused Socket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a least-connections LB from pouring every new dial onto a gateway that refuses at once (#131). On a `pending_mem` or `draining` refusal, the gateway keeps the socket open for a short hold before it closes it, so the LB's count includes the refusals. Then repeat the handshake-concurrency-4 `b_stress` trial that found the problem.

**Architecture:** Two packages change, `internal/gateway` (the core) and `internal/gateway/tcp` (the transport), plus one flag pair and three readings in `gwloss.sh`.
- **The core holds the refusal.** `Open` keeps its signature `(*Conn, string)` with a wider contract. A reason with no `Conn` means close at once, as today. A reason **with** a `Conn` means the refusal is held. That `Conn` is in a new state `stateHeld`. It is not in the loop's table and counts in neither `open` (the connection cap) nor `pending` (the pending cap). Its `life` timer is armed on the loop's existing wheel at `now + RefuseHold` and closes it through `closeConn(c, "held")`. `Closed` on a held `Conn` stops the timer and gives back its place under `HeldCap`, and nothing else: no gauge, no Logout, no settle.
- **The hold is the wheel's.** The wheel ticks once a second and never fires early, so `RefuseHold` 500ms gives a hold of 0.5–1.5s, 1s on average, spread by when the refusal arrived. A loop's closes come in one batch per tick. There is no random draw (the user's ruling).
- **A cap bounds it.** `HeldCap` (default 10,000) is one atomic count across the loops. A refusal over it closes at once and is counted as `full`. `RefuseHold` 0 or `HeldCap` 0 turns the hold off. The startup nofile self-check now needs `MaxConns × (1 + Headroom) + HeldCap`.
- **`at_capacity` is not held.** A gateway at the connection cap already has the most sockets, so least-connections does not pick it.
- **The transport keeps a held socket and reads nothing from it.** `OnOpen` closes the socket only when `Open` returned no `Conn`. A held conn's `OnTraffic` discards whatever the client sent (its first handshake message), so no DH is ever computed for it. Its close takes the usual path (`OnClose` → `Closed`).
- **Loadgen needs no change.** Its balancer (`internal/loadgen/balancer.go`) counts a connection until the client closes it, and a client closes when it sees the gateway's close. So a held refusal counts there for the hold, as it would in a real LB.

**Tech Stack:** Go 1.27.1, gnet v2.10.0, Prometheus client, bash (`gwloss.sh`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Load balancer algorithm* (R5S-O4, ~l.67): least-connections where the cloud LB offers it.
- *Admission control* (~l.199–201): the memory-only pending cap (`pending_mem`) refuses before any crypto.
- *Client SDK rules*, *Pre-handshake closes* (~l.132, l.141): the client waits 0.5–1.5s after a close before the handshake reply, up to 10 in a row. A held refusal is such a close, arriving one hold later.
- *Readiness*, the limits self-check (R5S-O7): nofile ≥ cap × (1 + headroom).
- *Observability* (~l.449): admission rejects by reason.

The spec text is amended with rev7, not in place (#131). Task 3 records the amendment text from this plan's *Spec follow-up (rev7)* section in the plan's dispositions file.

Inputs:
- The roadmap's #131: the problem, the decision and what was not chosen.
- Plan 6b3b4's *Execution Notes*, *Handshake-concurrency-4 trial at the new cost* (~l.941–962). These are the numbers this plan's Task 3 compares with: survivors' `pending_mem` 100,602 / 2,251,091, gw-1 frozen from t=29s, recovery p50/p90/p99 41.4/71.0/74.8s against 23.2/40.8/54.6s at 2 slots, and the storm profiles (gw-2 30.0s in system calls against gw-1's 16.2s).

**Decisions:**
- #131 (the user, 2026-10-17, with AskUserQuestion): the gateway holds a `pending_mem` (and `draining`) refusal briefly, on the loop's wheel, under a cap on held sockets. The spec is amended with rev7. *Not chosen:* readiness off near the cap, refusals spread before the cliff, recording it as a limit.
- The sizing #131 left to the plan (the user, 2026-10-17, with AskUserQuestion, before the prototype):
  1. **Hold:** the wheel, at `now + 500ms` (`-refuse-hold`), so 0.5–1.5s. Not an exact random 0.5–1s on a second, finer timer per loop.
  2. **Cap:** `-held-cap` 10,000 fixed per gateway, added to the nofile self-check. Not a cap that follows `-max-conns`.
  3. **Verification:** the concurrency-4 `b_stress` trial is repeated and recorded against an expectation, not gated. No new `gwloss` check.

  These belong to #131. No new choice number.

**Branch:** `feat/plan6b3`. Execute from its head, this plan's own commit or later.

**Size:** 3 tasks.
- Task 1, core and transport: about 70 lines of code and 190 of tests.
- Task 2: two flags, a flag test, three readings in `gwloss.sh`, and a dev dry run.
- Task 3: the user's Tier 2 trial and the record.

The prototype `proto-6b3b5` (`d12b258`, from `335eacc`) ran every step. Its files are replayed here byte for byte: `hold_test.go` in full, everything else as `git apply` patches. On the dev machine (2 cores):
- `go test -race` passes for `./internal/gateway/...` and `./cmd/gateway/`.
- `make test`, `make test-race-suite` (280s), `make test-scripts` and `make test-load-probe` all pass.
- 9 mutations, all caught (Task 1 Step 6).
- The `b_stress` dry run gave 14 of 15 checks. Only `no_swap_in_b_stress` failed, from the dev machine's own swap. The new readings are present, all 0.

---

## Global Constraints

- Go 1.27.1. No new dependency.
- Defaults: `RefuseHold` 500ms, `HeldCap` 10,000. `Validate` refuses `HeldCap` < 0, and a `RefuseHold` outside `[0, HandshakeTimeout)`.
- Only `pending_mem` and `draining` refusals are held. `at_capacity`, `handshake_rate`, `bad_version` and `noise_unknown_key` close as before.
- A held socket is never in a loop's table. It counts in neither the connection cap nor the pending cap, has no Logout, and is never read.
- Metric names: `goim_gateway_refusals_held` (gauge), `goim_gateway_refusals_held_total{result="held"|"full"}` (both series exported at 0), and `goim_gateway_closes_total{reason="held"}`.
- Flags: `-refuse-hold` (duration), `-held-cap` (int).
- nofile self-check: `ceil(MaxConns × (1 + Headroom)) + HeldCap`. Every Tier 2 scenario raises nofile to 1,048,576 (`t2_nofile`), and the largest cap is 650,000 (715,000 + 10,000).
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A refused client's retry comes one hold later.** A held client sees the close 0.5–1.5s after its handshake message, then waits the SDK's 0.5–1.5s. Ten held refusals in a row now take ~20s rather than ~10s before backoff. The spec's rolling-restart sizing (R5S-O4) is about `at_capacity`, which is not held, so that sizing stands. *Checked by:* Task 3's recovery percentiles against the 4-slot trial, and the spec follow-up text.
2. **Closes in a batch per tick.** At ~50k refusals/s (gw-2's rate in the first trial), a loop could close up to 10,000 sockets in one wheel advance, and the LB's count then dips once a second. *Checked by:* Task 3's socket samples (`srv_est` per survivor, `refusals_held` every 10s) and the gateways' storm profiles (`Syscall6`, the accepts and closes).
3. **The cap binding.** If `refusals_held_full` is above 0, part of the loop returns for that gateway. *Checked by:* `TestHeldCapClosesTheRestAtOnce` (the mechanism), and in Task 3 `refusals_held_full` per survivor, with outcome (b).
4. **No change where the hold doesn't fire.** At the default 2 slots the survivors refuse with `no_slot`, which is not held: `pending_mem` was 0/0 in run 30j's rerun. The floods phases had `pending_mem` 0 from run 30c on. So no gated result moves. The only cost is the startup nofile need, +10,000. *Checked by:* `TestAtCapacityRefusalIsNotHeld` and the existing transport tests (immediate closes), `make test-load-probe`, the race suite, and the nofile arithmetic in Global Constraints.
5. **A held socket must never reach the frame reader or the state machine.** If it did, the gateway would compute a DH for a refused client, and a held `Conn`'s timer could close an unrelated socket. *Checked by:* `TestHeldRefusalIsAPreHandshakeCloseAfterTheHold` (the client gets no handshake reply; the mutation that drops the transport's held branch is caught) and `TestClientCloseFreesAHeldRefusal` (the timer stops with the close).

## Implementation choices this plan makes

- **A held refusal is a `Conn` in `stateHeld`**, so the transport keeps one code path (`SetContext`, `OnClose` → `Closed`). Using `Conn` costs ~250 bytes per held socket, at most ~2.5MB at the cap. `stateHeld` comes after `stateClosed`, so `gauges [3]` (handshake, auth_pending, online) never indexes it. `Closed` handles it before any gauge.
- **Closes:** the core's close of a held socket counts `closes_total{reason="held"}`. A client that hangs up first counts nothing in `closes_total`, because the connection was never admitted. Its `pending_mem` or `draining` reject was counted at the refusal.
- **`RefuseHold` < `HandshakeTimeout`.** A hold must be shorter than a real handshake may take. Within that limit, the hold plus one tick stays below the SDK's 10s `ConnectTimeout` and loadgen's 10s `-timeout`.
- **The flags sit next to `-pending-cap`** in `cmd/gateway/main.go`.
- **`gwloss.sh` reads two counters and one gauge.** `refusals_held` and `refusals_held_full` go in `GW_COUNTERS`, so they appear per survivor in `<phase>_lease` and the phase's `survivors`. The gauge `refusals_held` goes in each socket sample (`GWLOSS_SS_EVERY`). No check is added.

## File Structure

| File | Change |
|---|---|
| `internal/gateway/config.go` | `RefuseHold`, `HeldCap`: fields, defaults, `Validate` |
| `internal/gateway/conn.go` | `stateHeld` |
| `internal/gateway/gateway.go` | `held` count, `Open` → `refuse`, `Closed`'s held branch |
| `internal/gateway/limits.go` | nofile need + `HeldCap` |
| `internal/gateway/metrics.go` | `Held`, `HeldTotal` |
| `internal/gateway/tcp/conn.go` | `held` flag |
| `internal/gateway/tcp/transport.go` | `OnOpen` keeps a held socket, `OnTraffic` discards its bytes |
| `internal/gateway/hold_test.go` | new: the core's hold tests |
| `internal/gateway/config_test.go`, `server_test.go` | validation; the self-check with `HeldCap` |
| `internal/gateway/tcp/transport_test.go` | a held refusal over TCP |
| `cmd/gateway/main.go`, `main_test.go` | `-refuse-hold`, `-held-cap` |
| `scripts/tier2/gwloss.sh` | the held readings |

---

### Task 1: The core holds a `pending_mem` or `draining` refusal; the transport keeps it unread

**Files:**
- Create: `internal/gateway/hold_test.go`
- Modify: `internal/gateway/config.go`, `conn.go`, `gateway.go`, `limits.go`, `metrics.go`, `internal/gateway/tcp/conn.go`, `internal/gateway/tcp/transport.go`
- Test: `internal/gateway/hold_test.go`, `config_test.go`, `server_test.go`, `internal/gateway/tcp/transport_test.go`

**Interfaces:**
- Consumes: the wheel (`ls.wheel.schedule/stop`, `timer`), `closeConn`, `reject`, and the test harness's `core`, `fakeLink`, `advance`, `sync`, `onLoop`, `rejects`, `closes` (in `harness_test.go`, `conn_test.go` and `seam_test.go`).
- Produces:
  - `Config.RefuseHold time.Duration`, `Config.HeldCap int`.
  - `Metrics.Held prometheus.Gauge`, `Metrics.HeldTotal *prometheus.CounterVec` (label `result`).
  - `Open(l Loop, link Link) (*Conn, string)`. A non-nil `Conn` together with a reason means the refusal is held.
  - The close reason `"held"`.

- [ ] **Step 1: The failing tests**

Create `internal/gateway/hold_test.go`:

```go
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
```

Then apply the other tests: the validation test, the self-check with `HeldCap` 100, and the TCP test.

```bash
git apply <<'PATCH'
diff --git a/internal/gateway/config_test.go b/internal/gateway/config_test.go
index 64d0c6c..f774116 100644
--- a/internal/gateway/config_test.go
+++ b/internal/gateway/config_test.go
@@ -118,3 +118,17 @@ func TestRejectReasonsExportedAtZero(t *testing.T) {
 		t.Fatalf("%d admission_rejects_total series before any reject, want 9 (err %v)", n, err)
 	}
 }
+
+func TestRefusalHoldIsValidated(t *testing.T) {
+	for name, mod := range map[string]func(*Config){
+		"negative hold":          func(c *Config) { c.RefuseHold = -time.Millisecond },
+		"hold ≥ handshake limit": func(c *Config) { c.RefuseHold = c.HandshakeTimeout },
+		"negative cap":           func(c *Config) { c.HeldCap = -1 },
+	} {
+		cfg := testConfig()
+		mod(&cfg)
+		if cfg.Validate() == nil {
+			t.Errorf("%s: accepted", name)
+		}
+	}
+}
diff --git a/internal/gateway/server_test.go b/internal/gateway/server_test.go
index a3125af..f9f57b4 100644
--- a/internal/gateway/server_test.go
+++ b/internal/gateway/server_test.go
@@ -17,7 +17,7 @@ import (
 func TestLimitsSelfCheck(t *testing.T) {
 	m := NewMetrics(prometheus.NewRegistry())
 	cfg := testConfig()
-	cfg.MaxConns = 1000 // needs nofile ≥ 1100, nf_conntrack_max ≥ 2000
+	cfg.MaxConns, cfg.HeldCap = 1000, 100 // needs nofile ≥ 1100 + 100, nf_conntrack_max ≥ 2000
 	src := func(nofile uint64, conntrack string, err error) limitSource {
 		return limitSource{
 			nofile:   func() (uint64, error) { return nofile, nil },
@@ -30,12 +30,12 @@ func TestLimitsSelfCheck(t *testing.T) {
 		notrack bool
 		fail    string
 	}{
-		{"enough", src(1100, "2000\n", nil), false, ""},
-		{"nofile too low", src(1099, "2000\n", nil), false, "RLIMIT_NOFILE"},
-		{"conntrack too low", src(1100, "1999\n", nil), false, "nf_conntrack_max"},
-		{"conntrack too low but notrack", src(1100, "10\n", nil), true, ""},
-		{"conntrack not loaded", src(1100, "", fs.ErrNotExist), false, ""},
-		{"conntrack unreadable", src(1100, "", errors.New("permission denied")), false, "nf_conntrack_max"},
+		{"enough", src(1200, "2000\n", nil), false, ""},
+		{"nofile too low", src(1199, "2000\n", nil), false, "RLIMIT_NOFILE"},
+		{"conntrack too low", src(1200, "1999\n", nil), false, "nf_conntrack_max"},
+		{"conntrack too low but notrack", src(1200, "10\n", nil), true, ""},
+		{"conntrack not loaded", src(1200, "", fs.ErrNotExist), false, ""},
+		{"conntrack unreadable", src(1200, "", errors.New("permission denied")), false, "nf_conntrack_max"},
 	} {
 		t.Run(tc.name, func(t *testing.T) {
 			c := cfg
diff --git a/internal/gateway/tcp/transport_test.go b/internal/gateway/tcp/transport_test.go
index 61b9c2b..f5519c8 100644
--- a/internal/gateway/tcp/transport_test.go
+++ b/internal/gateway/tcp/transport_test.go
@@ -307,6 +307,27 @@ func TestRejectAtAcceptIsAPreHandshakeCloseForTheClient(t *testing.T) {
 	}
 }
 
+// A pending_mem refusal is held open (#131): the client's handshake goes
+// unread and it sees a pre-handshake close no sooner than RefuseHold.
+func TestHeldRefusalIsAPreHandshakeCloseAfterTheHold(t *testing.T) {
+	s := newServer(t, func(c *gateway.Config, _ *Config) { c.PendingCap = 1 })
+	s.hello() // AUTH_PENDING: the one pending socket
+	c := s.dial()
+	start := time.Now()
+	_, err := wire.ClientHandshake(c.nc, 1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
+	took := time.Since(start)
+	if !errors.Is(err, wire.ErrClosedBeforeReply) {
+		t.Fatalf("err %v, want ErrClosedBeforeReply", err)
+	}
+	if took < 450*time.Millisecond || took > 3*time.Second {
+		t.Fatalf("closed after %v, want the hold: 0.5s to the next tick (≤ 1.5s, slack to 3s)", took)
+	}
+	if s.rejects(gateway.RejectPendingMem) != 1 || s.closes("held") != 1 {
+		t.Fatalf("pending_mem %d, closes{held} %d; want 1, 1", s.rejects(gateway.RejectPendingMem), s.closes("held"))
+	}
+	waitFor(t, "refusals_held back to 0", func() bool { return testutil.ToFloat64(s.m.Held) == 0 })
+}
+
 // TestListenBesideTimeWait: a gateway restarted on its port while the old
 // process's closed connections sit in TIME_WAIT (a SIGKILL closes them with
 // a FIN, as does a drain) must still bind: a container restarted in the same
PATCH
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/gateway/... 2>&1 | head`
Expected: `[build failed]` for both packages: `c.RefuseHold undefined`, `c.HeldCap undefined`, `h.g.m.Held undefined`, `h.g.m.HeldTotal undefined`.

- [ ] **Step 3: The core and the transport**

```bash
git apply <<'PATCH'
diff --git a/internal/gateway/config.go b/internal/gateway/config.go
index c2f4a11..9ee0cda 100644
--- a/internal/gateway/config.go
+++ b/internal/gateway/config.go
@@ -31,6 +31,11 @@ type Config struct {
 	MaxConns      int // connection cap: open sockets in any state (250k)
 	PendingCap    int // HANDSHAKE + AUTH_PENDING sockets, memory only (100k)
 	LoginQueueCap int // Logins waiting for or inside a Login RPC (8192)
+	// A pending_mem or draining refusal holds its socket until the first
+	// wheel tick RefuseHold after it (0.5–1.5s), so a least-connections LB
+	// counts the refusals (#131). Over HeldCap held, it closes at once.
+	RefuseHold time.Duration // (500ms; 0: no hold)
+	HeldCap    int           // (10k; 0: no hold), on top of nofile's MaxConns × (1 + Headroom)
 
 	HandshakeTimeout time.Duration // accept → handshake done (5s, 3–30s)
 	AuthTimeout      time.Duration // handshake done → LoginReq (5s, 3–30s)
@@ -108,6 +113,8 @@ func DefaultConfig() Config {
 		MaxConns:         250_000,
 		PendingCap:       100_000,
 		LoginQueueCap:    8192,
+		RefuseHold:       500 * time.Millisecond,
+		HeldCap:          10_000,
 		HandshakeTimeout: 5 * time.Second,
 		AuthTimeout:      5 * time.Second,
 		Heartbeat:        150 * time.Second,
@@ -187,6 +194,12 @@ func (c Config) Validate() error {
 		bad("MaxConns %d + LogoutQueue %d above %d: a ReconcileEnd would exceed Logic's %dMB message limit",
 			c.MaxConns, c.LogoutQueue, pb.MaxTrackedConnIDs, pb.MaxMsgBytes>>20)
 	}
+	if c.HeldCap < 0 {
+		bad("HeldCap %d must not be negative", c.HeldCap)
+	}
+	if c.RefuseHold < 0 || c.RefuseHold >= c.HandshakeTimeout {
+		bad("RefuseHold %v must be in [0, HandshakeTimeout %v)", c.RefuseHold, c.HandshakeTimeout)
+	}
 	if c.Reconcile.MaxRequeues < 0 {
 		bad("Reconcile.MaxRequeues %d must not be negative", c.Reconcile.MaxRequeues)
 	}
diff --git a/internal/gateway/conn.go b/internal/gateway/conn.go
index ffd8542..14d0e30 100644
--- a/internal/gateway/conn.go
+++ b/internal/gateway/conn.go
@@ -25,9 +25,10 @@ const (
 	stateAuthPending
 	stateOnline
 	stateClosed
+	stateHeld // a refusal held open (#131): not tracked, in neither cap
 )
 
-var stateLabel = [...]string{"handshake", "auth_pending", "online", "closed"}
+var stateLabel = [...]string{"handshake", "auth_pending", "online", "closed", "held"}
 
 // outcome is what the gateway knows about a connection's Login.
 type outcome uint8
diff --git a/internal/gateway/gateway.go b/internal/gateway/gateway.go
index 9a28058..a2968df 100644
--- a/internal/gateway/gateway.go
+++ b/internal/gateway/gateway.go
@@ -27,6 +27,7 @@ type Gateway struct {
 	pending     atomic.Int64 // HANDSHAKE + AUTH_PENDING sockets (pending cap)
 	tracked     atomic.Int64 // table entries
 	loginQueued atomic.Int64 // Logins waiting for or inside a Login RPC
+	held        atomic.Int64 // refusals held open (#131)
 	draining    atomic.Bool
 
 	loginCh  chan *loginItem
@@ -77,21 +78,23 @@ func New(cfg Config, d Deps) (*Gateway, error) {
 	return g, nil
 }
 
-// Open admits a new socket on loop l (spec: Admission control). It returns
-// the reject reason, already counted, when the transport must close the
-// socket before any crypto.
+// Open admits a new socket on loop l (spec: Admission control). On a refusal
+// it returns the reason, already counted. With no Conn, the transport closes
+// the socket before any crypto. With a Conn, the refusal is held (#131): the
+// transport keeps the socket, discards what it reads, and reports its close
+// through Closed; the core closes it after the hold.
 func (g *Gateway) Open(l Loop, link Link) (*Conn, string) {
 	ls := g.loops.bind(l)
 	switch {
 	case g.draining.Load():
-		return nil, g.reject(RejectDraining)
+		return g.refuse(ls, link, RejectDraining)
 	case g.open.Add(1) > int64(g.cfg.MaxConns):
 		g.open.Add(-1)
 		return nil, g.reject(RejectAtCapacity)
 	case g.pending.Add(1) > int64(g.cfg.PendingCap):
 		g.pending.Add(-1)
 		g.open.Add(-1)
-		return nil, g.reject(RejectPendingMem)
+		return g.refuse(ls, link, RejectPendingMem)
 	}
 	c := &Conn{id: ls.nextID(), ls: ls, link: link, state: stateHandshake}
 	c.onLife = func() { g.lifeExpired(c) }
@@ -104,6 +107,30 @@ func (g *Gateway) Open(l Loop, link Link) (*Conn, string) {
 	return c, ""
 }
 
+// refuse counts a pending_mem or draining refusal and, while fewer than
+// HeldCap are held, holds the socket open until the first wheel tick
+// RefuseHold after now (#131). A refusing gateway's open sockets then grow
+// with its refusals, so a least-connections LB turns to the others, instead
+// of seeing its count fall and sending it every new dial. at_capacity is not
+// held: a gateway at the cap already has the most sockets.
+func (g *Gateway) refuse(ls *loopState, link Link, reason string) (*Conn, string) {
+	g.reject(reason)
+	if g.cfg.RefuseHold <= 0 || g.cfg.HeldCap <= 0 {
+		return nil, reason
+	}
+	if g.held.Add(1) > int64(g.cfg.HeldCap) {
+		g.held.Add(-1)
+		g.m.HeldTotal.WithLabelValues("full").Inc()
+		return nil, reason
+	}
+	g.m.HeldTotal.WithLabelValues("held").Inc()
+	g.m.Held.Inc()
+	c := &Conn{ls: ls, link: link, state: stateHeld}
+	c.onLife = func() { g.closeConn(c, "held") }
+	ls.wheel.schedule(&c.life, g.now().Add(g.cfg.RefuseHold), c.onLife)
+	return c, reason
+}
+
 func (g *Gateway) reject(reason string) string {
 	g.m.Rejects.WithLabelValues(reason).Inc()
 	return reason
@@ -169,6 +196,13 @@ func (g *Gateway) Closed(c *Conn) {
 	if c.state == stateClosed {
 		return
 	}
+	if c.state == stateHeld {
+		c.ls.wheel.stop(&c.life)
+		c.state, c.closing, c.link = stateClosed, false, nil
+		g.held.Add(-1)
+		g.m.Held.Dec()
+		return
+	}
 	if !c.closing {
 		g.m.Closes.WithLabelValues("client").Inc()
 	}
diff --git a/internal/gateway/limits.go b/internal/gateway/limits.go
index 05c6b29..47f08fd 100644
--- a/internal/gateway/limits.go
+++ b/internal/gateway/limits.go
@@ -30,20 +30,20 @@ var hostLimits = limitSource{
 }
 
 // checkLimits returns why the gateway must stay NotReady, or nil:
-// RLIMIT_NOFILE ≥ cap × (1 + headroom), and, unless conntrack is disabled for
-// the gateway port, nf_conntrack_max ≥ 2 × cap. An absent conntrack file means
-// conntrack isn't loaded, which passes.
+// RLIMIT_NOFILE ≥ cap × (1 + headroom) + HeldCap (#131), and, unless
+// conntrack is disabled for the gateway port, nf_conntrack_max ≥ 2 × cap. An
+// absent conntrack file means conntrack isn't loaded, which passes.
 func checkLimits(cfg Config, src limitSource, m *Metrics) error {
 	var errs []error
-	needFiles := uint64(math.Ceil(float64(cfg.MaxConns) * (1 + cfg.Limits.Headroom)))
+	needFiles := uint64(math.Ceil(float64(cfg.MaxConns)*(1+cfg.Limits.Headroom))) + uint64(cfg.HeldCap)
 	switch n, err := src.nofile(); {
 	case err != nil:
 		errs = append(errs, fmt.Errorf("read RLIMIT_NOFILE: %w", err))
 	default:
 		m.Limits.WithLabelValues("nofile").Set(float64(n))
 		if n < needFiles {
-			errs = append(errs, fmt.Errorf("RLIMIT_NOFILE %d is below %d (cap %d + %.0f%% headroom)",
-				n, needFiles, cfg.MaxConns, cfg.Limits.Headroom*100))
+			errs = append(errs, fmt.Errorf("RLIMIT_NOFILE %d is below %d (cap %d + %.0f%% headroom + %d held)",
+				n, needFiles, cfg.MaxConns, cfg.Limits.Headroom*100, cfg.HeldCap))
 		}
 	}
 	if !cfg.Limits.Notrack {
diff --git a/internal/gateway/metrics.go b/internal/gateway/metrics.go
index a68c560..b9a736a 100644
--- a/internal/gateway/metrics.go
+++ b/internal/gateway/metrics.go
@@ -27,6 +27,8 @@ type Metrics struct {
 	Tracked        prometheus.Gauge       // every table entry
 	Rejects        *prometheus.CounterVec // reason
 	Closes         *prometheus.CounterVec // reason: client | heartbeat | kick | not_owner | logout | protocol | drain
+	Held           prometheus.Gauge       // refused sockets held open (#131)
+	HeldTotal      *prometheus.CounterVec // result: held | full (closed at once: HeldCap reached)
 
 	Logins       *prometheus.CounterVec // result
 	LoginQueue   prometheus.Gauge       // Logins waiting for or inside a Login RPC
@@ -97,6 +99,8 @@ func NewMetrics(reg prometheus.Registerer) *Metrics {
 		Tracked:        g("tracked", "Connection table entries (the tracked set)."),
 		Rejects:        cv("admission_rejects_total", "Connections rejected or closed by admission control.", "reason"),
 		Closes:         cv("closes_total", "Connections the gateway closed, by reason.", "reason"),
+		Held:           g("refusals_held", "pending_mem and draining refusals held open until their close."),
+		HeldTotal:      cv("refusals_held_total", "pending_mem and draining refusals: held, or closed at once with HeldCap reached (full).", "result"),
 
 		Logins:       cv("logins_total", "Logins by result, as the client sees it.", "result"),
 		LoginQueue:   g("login_queue", "Logins waiting for or inside a Login RPC."),
@@ -134,5 +138,7 @@ func NewMetrics(reg prometheus.Registerer) *Metrics {
 		RejectHandshakeTimeout, RejectAuthTimeout, RejectHandshakeRate, RejectBadVersion, RejectNoiseUnknownKey} {
 		m.Rejects.WithLabelValues(r)
 	}
+	m.HeldTotal.WithLabelValues("held")
+	m.HeldTotal.WithLabelValues("full")
 	return m
 }
diff --git a/internal/gateway/tcp/conn.go b/internal/gateway/tcp/conn.go
index 587f638..eebba59 100644
--- a/internal/gateway/tcp/conn.go
+++ b/internal/gateway/tcp/conn.go
@@ -29,6 +29,7 @@ type conn struct {
 	c       *gateway.Conn
 	sess    *wire.Session // nil until the handshake completes
 	closing bool          // Close requested
+	held    bool          // a refusal the core holds open (#131): nothing is read
 }
 
 func (c *conn) hook(site string) {
diff --git a/internal/gateway/tcp/transport.go b/internal/gateway/tcp/transport.go
index b3f6c0b..1a7f2da 100644
--- a/internal/gateway/tcp/transport.go
+++ b/internal/gateway/tcp/transport.go
@@ -199,26 +199,33 @@ func (t *Transport) loopOf(el gnet.EventLoop) *loop {
 
 // OnOpen admits the socket in the core, before any crypto (spec: Admission
 // control). A rejected socket is closed at once; its OnClose finds no conn.
+// A held refusal (#131) stays open until the core closes it.
 func (t *Transport) OnOpen(gc gnet.Conn) ([]byte, gnet.Action) {
 	l := t.loopOf(gc.EventLoop())
 	c := &conn{t: t, l: l, gc: gc}
 	var reason string
 	l.run(func() { c.c, reason = t.g.Open(l, c) })
-	if reason != "" {
+	if c.c == nil {
 		return nil, gnet.Close
 	}
+	c.held = reason != ""
 	gc.SetContext(c)
 	c.hook("open")
 	return nil, gnet.None
 }
 
-// OnTraffic cuts and handles every whole frame buffered.
+// OnTraffic cuts and handles every whole frame buffered. A held refusal's
+// bytes are dropped unread.
 func (t *Transport) OnTraffic(gc gnet.Conn) gnet.Action {
 	c, ok := gc.Context().(*conn)
 	if !ok {
 		return gnet.Close
 	}
 	c.hook("traffic")
+	if c.held {
+		gc.Discard(-1)
+		return gnet.None
+	}
 	c.l.run(c.read)
 	return gnet.None
 }
PATCH
gofmt -l internal/gateway   # prints nothing
```

- [ ] **Step 4: Run the tests**

Run: `go test -race -count=1 ./internal/gateway/...`
Expected: `ok` for `internal/gateway` and `internal/gateway/tcp`.

Run: `go test -count=5 -run 'Held|Hold' ./internal/gateway/ ./internal/gateway/tcp/`
Expected: `ok`. The TCP test's bounds, at least 450ms and at most 3s, have slack against a 2-core machine's scheduling.

- [ ] **Step 5: The whole suite**

Run: `make test`
Expected: every package `ok`.

- [ ] **Step 6: Mutations**

Apply each change alone, run the named package's tests, and expect a failure, then restore the file with `git checkout -- <file>`. Commit the task's files first, or use `git diff > /tmp/x.patch` to keep them. The prototype caught all 9:

| # | File | Change | Package | Caught by |
|---|---|---|---|---|
| 1 | `gateway.go` | `return g.refuse(ls, link, RejectPendingMem)` → `return nil, g.reject(RejectPendingMem)` | `./internal/gateway/` | `TestPendingMemRefusalIs…` |
| 2 | `gateway.go` | `return g.refuse(ls, link, RejectDraining)` → `return nil, g.reject(RejectDraining)` | same | `TestDrainingRefusalIsHeld` |
| 3 | `gateway.go` | `return nil, g.reject(RejectAtCapacity)` → `return g.refuse(ls, link, RejectAtCapacity)` | same | `TestAtCapacityRefusalIsNotHeld` |
| 4 | `gateway.go` | drop `g.held.Add(-1)` from `Closed`'s held branch | same | `TestHeldCapClosesTheRestAtOnce` |
| 5 | `gateway.go` | drop `c.ls.wheel.stop(&c.life)` from `Closed`'s held branch | same | `TestClientCloseFreesAHeldRefusal` (armed timers) |
| 6 | `gateway.go` | `g.now().Add(g.cfg.RefuseHold)` → `g.now()` | same | `TestPendingMemRefusalIs…` (`alignTo` 900ms) |
| 7 | `limits.go` | drop ` + uint64(cfg.HeldCap)` | same | `TestLimitsSelfCheck/nofile_too_low` |
| 8 | `transport.go` | `if c.c == nil {` → `if reason != "" {` in `OnOpen` | `./internal/gateway/tcp/` | `TestHeldRefusalIs…` |
| 9 | `transport.go` | delete the `if c.held { … }` block in `OnTraffic` | same | `TestHeldRefusalIs…` (the handshake gets a reply) |

- [ ] **Step 7: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): hold a pending_mem or draining refusal until the tick after RefuseHold, under HeldCap, so least-connections sees it (#131)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The flags and `gwloss`' readings

**Files:**
- Modify: `cmd/gateway/main.go`, `scripts/tier2/gwloss.sh`
- Test: `cmd/gateway/main_test.go`

**Interfaces:**
- Consumes: `Config.RefuseHold`, `Config.HeldCap` and the metric names (Task 1).
- Produces: the flags `-refuse-hold` and `-held-cap`. In gwloss, the per-survivor counters `refusals_held` and `refusals_held_full` (`GW_COUNTERS`), and `gateways.<gw>.refusals_held` (the gauge) in each `ss_<phase>` sample. Task 3 reads them.

- [ ] **Step 1: The failing flag test**

```bash
git apply <<'PATCH'
diff --git a/cmd/gateway/main_test.go b/cmd/gateway/main_test.go
index 5dcd18b..6984822 100644
--- a/cmd/gateway/main_test.go
+++ b/cmd/gateway/main_test.go
@@ -72,6 +72,21 @@ func TestLogoutQueueFollowsTheCap(t *testing.T) {
 	}
 }
 
+// The refusal hold's flags (#131).
+func TestRefuseHoldFlags(t *testing.T) {
+	cfg, _, _, _, err := parseFlags([]string{"-id", "gw-0"})
+	if err != nil || cfg.RefuseHold != 500*time.Millisecond || cfg.HeldCap != 10_000 {
+		t.Fatalf("defaults: hold %v, cap %d, err %v; want 500ms, 10000", cfg.RefuseHold, cfg.HeldCap, err)
+	}
+	cfg, _, _, _, err = parseFlags([]string{"-id", "gw-0", "-refuse-hold", "0", "-held-cap", "0"})
+	if err != nil || cfg.RefuseHold != 0 || cfg.HeldCap != 0 {
+		t.Fatalf("hold %v, cap %d, err %v; want 0, 0", cfg.RefuseHold, cfg.HeldCap, err)
+	}
+	if _, _, _, _, err := parseFlags([]string{"-id", "gw-0", "-refuse-hold", "5s"}); err == nil {
+		t.Fatal("-refuse-hold 5s accepted: it must stay below -handshake-timeout (5s)")
+	}
+}
+
 // The race suite's flags (Plan 5b).
 func TestRaceSuiteFlags(t *testing.T) {
 	cfg, _, _, o, err := parseFlags([]string{"-id", "gw-0", "-faults", "-clock-offset", "-5s",
PATCH
go test ./cmd/gateway/ -run TestRefuseHoldFlags
```

Expected: FAIL. The first `parseFlags` passes, then `flag provided but not defined: -refuse-hold`.

- [ ] **Step 2: The flags and the readings**

```bash
git apply <<'PATCH'
diff --git a/cmd/gateway/main.go b/cmd/gateway/main.go
index b95e23d..84bce9a 100644
--- a/cmd/gateway/main.go
+++ b/cmd/gateway/main.go
@@ -142,6 +142,8 @@ func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config
 	fs.IntVar(&cfg.NumLoops, "loops", cfg.NumLoops, "gnet event loops (default GOMAXPROCS)")
 	fs.IntVar(&cfg.MaxConns, "max-conns", cfg.MaxConns, "connection cap: open sockets in any state")
 	fs.IntVar(&cfg.PendingCap, "pending-cap", cfg.PendingCap, "HANDSHAKE + AUTH_PENDING sockets")
+	fs.DurationVar(&cfg.RefuseHold, "refuse-hold", cfg.RefuseHold, "a pending_mem or draining refusal stays open until the first tick this long after it (0: close at once)")
+	fs.IntVar(&cfg.HeldCap, "held-cap", cfg.HeldCap, "refusals held open at once; over it they close at once (0: no hold)")
 	fs.IntVar(&cfg.LoginQueueCap, "login-queue-cap", cfg.LoginQueueCap, "Logins waiting for or inside a Login RPC")
 	fs.IntVar(&cfg.LoginRPCs, "login-rpcs", cfg.LoginRPCs, "concurrent Login RPCs to Logic")
 	fs.IntVar(&cfg.LoginBatch, "login-batch", cfg.LoginBatch, "entries per Login RPC")
diff --git a/scripts/tier2/gwloss.sh b/scripts/tier2/gwloss.sh
index 5537cec..97c59c4 100755
--- a/scripts/tier2/gwloss.sh
+++ b/scripts/tier2/gwloss.sh
@@ -342,6 +342,8 @@ GW_COUNTERS=(
   rejects_draining='goim_gateway_admission_rejects_total:reason="draining"'
   rejects_bad_version='goim_gateway_admission_rejects_total:reason="bad_version"'
   rejects_noise_unknown_key='goim_gateway_admission_rejects_total:reason="noise_unknown_key"'
+  refusals_held='goim_gateway_refusals_held_total:result="held"'
+  refusals_held_full='goim_gateway_refusals_held_total:result="full"'
   handshakes_ok='goim_gateway_handshakes_total:result="ok"'
   handshakes_failed='goim_gateway_handshakes_total:result="failed"'
   handshakes_no_slot='goim_gateway_handshakes_total:result="no_slot"'
@@ -395,6 +397,7 @@ ss_sample() {
     gw+="${gw:+, }\"$g\": $(stack_counters "$g" handshake_conns='goim_gateway_connections:state="handshake"' \
       handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"' \
       pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"' \
+      refusals_held=goim_gateway_refusals_held \
       handshakes_ok='goim_gateway_handshakes_total:result="ok"' handshakes_no_slot='goim_gateway_handshakes_total:result="no_slot"')"
   done
   echo "{\"t\": $t, \"ss_ms\": $ms, \"sockets\": {$est}, \"listeners\": {$lst}, \"gateways\": {$gw}, \"loadgen\": $(lg_get /runtime d)}"
PATCH
go test -race -count=1 ./cmd/gateway/ && bash -n scripts/tier2/gwloss.sh && make test-scripts
```

Expected: `ok`, then `make test-scripts` passes.

- [ ] **Step 3: Dry run on the dev machine**

This is the 6b2a-stress recipe (that plan's Task 4 Step 2), `b_stress` only, with the trial's settings at dev scale. The throwaway copy also passes `-pending-cap 20`, so a refusal could be held. At 1,500 relogins spread by the 0–20s jitter it usually isn't, which is why the core's tests, not this run, cover the held path.

```bash
D=$(mktemp -d); cp scripts/tier2/gwloss.sh "$D/gwloss.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget \$mem$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/gwloss.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/gwloss.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/gwloss.sh"
sed -i 's|^  STACK_GATEWAY_ARGS=(-max-conns "$cap")$|  STACK_GATEWAY_ARGS=(-max-conns "$cap" -pending-cap 20)|' "$D/gwloss.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need\|pending-cap 20' "$D/gwloss.sh"
GOIM_TEST_TIER=2 GWLOSS_PHASES=b_stress GWLOSS_STRESS_CPUS_GATEWAY=1 GWLOSS_STRESS_CPUS_LOGIC=0 GWLOSS_STRESS_CPUS_LOADGEN=0-1 \
  GWLOSS_STRESS_HANDSHAKE_CONCURRENCY=4 GWLOSS_SS_EVERY=10 \
  GWLOSS_STRESS_CLIENTS=1500 GWLOSS_STRESS_RATE=500 GWLOSS_STRESS_CAP=5000 bash "$D/gwloss.sh"; echo "exit $?"
```

The `grep` must show:
- three `. ` lines naming `scripts/`;
- a `:` line;
- `t2_mem_budget 0`;
- the redis path;
- `if ! true`;
- `t2_need` without sudo;
- `-pending-cap 20`.

If `redis-server` is on `PATH` (the dev machine has `~/.local/bin/redis-server`), `R` may name that instead.

Expected (prototype, ~2.5 min): 15 checks, all passing except possibly `no_swap_in_b_stress` (dev swap; the prototype saw 172 pages in). The exit code is 1 if a check fails. Values from the prototype:
- `b_stress.gateway_args` is `-max-conns 5000 -pending-cap 20 -handshake-concurrency 4`.
- `b_stress_lease.survivors.gw-1` and `.gw-2` each have `logins_ok` 750, `refusals_held` 0 and `refusals_held_full` 0.
- Each `ss_b_stress[].gateways.<gw>` has `refusals_held` 0.
- `b_stress_back` has `back_all` true and `back_s` ≈ 21.

Afterwards, `pgrep -f "$PWD/tier2-results/gwloss"` finds nothing. Remove `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 4: Commit**

```bash
git add cmd/gateway scripts/tier2/gwloss.sh
git commit -m "feat(gateway): -refuse-hold, -held-cap; gwloss reads the held refusals per survivor (#131)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push
```

---

### Task 3: The concurrency-4 `b_stress` trial again, and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (`docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`: plan 6 row, #131), and the plan's dispositions file from the review (`docs/superpowers/objections/connection-session-core-06b3b5-refusal-hold-dispositions.md`).

- [ ] **Step 1: The trial (the user, on the Tier 2 host, ~5 min)**

After `git fetch && git switch feat/plan6b3 && git pull`, with the desktop apps closed:

```bash
GOIM_TEST_TIER=2 GWLOSS_PHASES=b_stress GWLOSS_STRESS_HANDSHAKE_CONCURRENCY=4 GWLOSS_SS_EVERY=10 make test-tier2 SCENARIO=gwloss
```

Send back the `.report.txt`. These are exactly the first trial's settings (`gwloss-20261017-104939`, `6bbcdf9`), so the two runs compare one to one. That trial passed 14 of 15, failing only `b_stress_storm_real`, which is not a #131 reading.

- [ ] **Step 2: Read it against the first trial**

| Reading | Where in the report | First trial (no hold) | Expected with the hold |
|---|---|---|---|
| survivors' `rejects_pending_mem` | `b_stress.survivors.<gw>` | 100,602 / 2,251,091 | the two within about 2× of each other, total well under 2.35M |
| `refusals_held` / `refusals_held_full` | same | — | `held` about equal to `pending_mem` on each; `full` 0 |
| open sockets per survivor over time | `ss_b_stress[].sockets.<port>.srv_est` | gw-2 always lower (156k vs 177k at 16s) | no survivor lower at every sample |
| the gauge | `ss_b_stress[].gateways.<gw>.refusals_held` | — | a few thousand at most, under 10,000 |
| handshakes in progress at t ≥ 56s | `ss_b_stress[].gateways.<gw>.handshake_conns` | gw-1 0 (frozen), gw-2 14,644 | both survivors above 0 while relogins remain |
| handshakes done | `survivors.<gw>.handshakes_ok` | 606,008 / 884,295 | about equal |
| recovery p50 / p90 / p99, `back_s` | `b_stress_back.slo.from_mark`, `back_s` | 41.4 / 71.0 / 74.8s, 107s | lower than the first trial, and the p50 and p99 compared with 2 slots (23.2 / 40.8 / 54.6s, 127s) |
| connections opened in the storm | `b_stress.tcp.storm.ActiveOpens` | 3,843,286 | lower |
| loadgen `busy` / `no-reply` | `b_stress.failed` | 605,993 / 284,097 | recorded |
| gateways' storm profiles | `b_stress/artifacts/storm/<gw>.cpu.pb.gz` (`go tool pprof -top`) | `Syscall6` 16.2s / 30.0s | about even |

Outcomes:
- **(a) Even, no frozen survivor:** #131 is fixed for least-connections. Record it. Whether to revisit the concurrency default (#111, #116) with this result is the user's call.
- **(b) Uneven with `refusals_held_full` > 0:** the cap binds. Record it, with the peak gauge from the samples. The user decides whether to raise `-held-cap` or rerun with a longer `-refuse-hold`. Either is a flag change through `STACK_GATEWAY_ARGS`, which the throwaway copy can set, so no code change is needed.
- **(c) Uneven with `full` 0:** the hold does not carry the count. Diagnose from the samples (`srv_est` against `refusals_held` per survivor) before anything else, with superpowers:systematic-debugging.

- [ ] **Step 3: Record**

- **This plan's *Execution Notes*:**
  - Tasks 1–2 executed, with commits and the dry run.
  - The trial: commit, `dirty`, duration, the run directory, the table above filled in, and the outcome.
- **The roadmap:**
  - #131: "executed (6b3b5)", the trial's outcome in one sentence.
  - The plan 6 row: "6b3b5 complete" with the trial's reading, then "next: 6b3c".
- **The dispositions file:** append this plan's *Spec follow-up (rev7)* section, unchanged, under the heading `#131 spec follow-up (rev7)`, for the next spec revision.

Commit (`docs: 6b3b5 trial recorded (#131)`, with the trailer) and push. Fast-forwarding `main` is the user's call.

---

## Spec follow-up (rev7)

The text below goes into rev7 (en and zh-CN). The spec is not edited in place (#131).

1. *Admission control*, after the memory-only pending cap bullet: "**Held refusals** *(#131)*: a `pending_mem` refusal, and a `draining` one, keeps the socket open, reading nothing, until the first timer-wheel tick at least `refuse_hold` (default 500ms) after it, so 0.5–1.5s, then closes it. At most `held_cap` (default 10k) are held at once. Beyond that a refusal closes at once. Held sockets count in neither cap. Why: a least-connections LB counts open connections. An instant close lowers the refusing gateway's count, so the LB sends it every new dial while the other gateways go idle (found in Tier 2, the handshake-concurrency-4 trial: 2.25M refusals on one survivor, the other frozen). Holding the socket makes the count include the refusals (refusal rate × hold), whatever the LB's algorithm. `at_capacity` is not held: a gateway at the cap already has the most connections."
2. *Load balancer algorithm* (R5S-O4): append "A gateway's refusals before the handshake are held briefly so that the LB's count includes them *(#131)*."
3. *Client SDK rules*, *Pre-handshake closes*: append "A held refusal reaches the client 0.5–1.5s after its handshake message, and the retry wait starts from that close. So 10 held refusals in a row take ~20s. The rolling-restart sizing above is about `at_capacity`, which is not held."
4. *Readiness*, limits self-check (R5S-O7): nofile ≥ cap × (1 + headroom) **+ held_cap**.
5. *Observability*: the held refusals now (gauge), refusals held or closed at once with the cap full (`held` / `full`), and the `held` close reason.

## After this plan

Plan 6b3c (`/reset`, `/network-change`, scenario `reconnect`, run 31), with its inputs listed in the roadmap row. #111 (defer a handshake instead of refusing it) stays parked. This plan was the precondition #131 named for unparking it.

## Execution Notes
