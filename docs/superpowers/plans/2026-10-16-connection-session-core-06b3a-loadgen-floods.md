# Plan 6b3a: Loadgen Instances and Flood Modes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give loadgen what the spec's flood tests need, so 6b3b only writes the `floods` scenario. Loadgen gains three flood modes: garbage handshakes, slow clients that never handshake, and slow clients that never log in. `scripts/loadgen.sh` can run several named loadgen instances on one stack, so a flood can run beside a legitimate population (P6a2bI-1). This plan has no Tier 2 run. Its checks are unit and in-process tests plus a `floods` phase in `make test-load-probe` on the dev machine.

**Architecture:** The flood modes are new `Config.Mode` values on the existing `Runner`, so there is still one mode per process. They log nothing in and keep their own counts in `Stats.Flood`. Every attempt dials through the existing least-connections `Balancer` and waits for the gateway to close the socket. That way the gateway closes first, and TIME_WAIT lands on the gateway, not on a loadgen source port. A new `-source-net K` flag gives each instance its own source addresses `127.0.K.2…`, so a flood never spends a legitimate instance's ports per (source, gateway) pair. In `loadgen.sh`, every existing function acts on the *current* instance: `main` by default, or whichever `lg_in INST` names. Instance k listens on control port `STACK_PORT_BASE + 5 + k` (k < 4), and `stack_ports` covers all four ports. Existing scenarios work unchanged because they only use `main`.

**Tech Stack:** Go 1.27.1 (`internal/loadgen`, `cmd/loadgen`), bash 5.2, curl, python3 (reads JSON only), the pinned `redis-server` 7.4.11 for the load probe.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Verification → Load tests*: "handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket"; "slow-client flood: slots are freed by the timeouts and legitimate clients get no `Busy`".
- *Storms, drain, failures*: "Handshake flood | The global handshake concurrency limit and token bucket protect established connections; excess handshakes are closed and counted"; "Slow clients | `HANDSHAKE` / `AUTH_PENDING` 5s timeouts free their slots".
- *Gateway → Admission control*: the Noise handshake limit is per gateway and global, never per IP; when it is exhausted, the gateway closes the connection and counts `handshake_rate`.
- The SDK's pre-handshake retry: uniform 0.5–1.5s.

Inputs: roadmap #118 (the 6b3 split), P6a2bI-1 (6a2a plan, *Execution Notes*).

**Decisions:** #118 (the user, 2026-10-16) puts instances and flood modes in this plan, with no Tier 2 run. **Proposed with this plan, decided by the review (2026-10-16):** #119 (the flood modes) and #120 (the instances), both amended, and #121 (the reading of the spec's 20k/s storm clause), under *Implementation choices* below. **Reviewed 2026-10-16:** objections O1–O7 all accepted (`docs/superpowers/objections/connection-session-core-06b3a-loadgen-floods.md`, decisions in `…-dispositions.md`) and applied here.

**Branch:** `feat/plan6b3`, which already holds #118. Execute from its head, `1b6f64c` or later.

**Size:** 3 tasks, about 760 lines changed. The prototype (`proto-6b3a`, from `1b6f64c`; the review's changes are `eb2639c`) ran every block. Its code is replayed here byte for byte: new Go files in full, every other change as a `git apply` patch against `1b6f64c`. On the dev machine the prototype passed `go test -race` for `internal/loadgen` (3 runs) and `cmd/loadgen`, `go vet ./...`, `gofmt`, `scripts/loadgen_test.sh`, `scripts/stack_test.sh` and the full `make test-load-probe`.

## Global Constraints

- Go 1.27.1. No change outside `internal/loadgen`, `cmd/loadgen` and the scripts named below. The gateway, SDK and Logic are untouched.
- `make test`, `make test-scripts` and `make test-load-probe` stay green on the dev machine (2 cores / 2GB, no Docker; the probe runs `redis-server` from `make redis-server`).
- Scripts use bash with `set -uo pipefail`. JSON is written without jq. python3 only reads JSON and does arithmetic.
- Ports (#76): `STACK_PORT_BASE` 17000 on Tier 2, 17200 in the load probe, 17500 in `stack_test.sh`. Gateway ports start at base + 100, so base + 6 … + 8 are free for loadgen instances.
- Source addresses (#69): `127.0.0.2…127.0.0.254` stays the default (`-source-net 0`). Linux answers on all of 127/8 with no setup, so `127.0.K.x` needs none either.
- Spec numbers: gateway `HandshakeTimeout` and `AuthTimeout` are 5s by default (config range 3–30s). The gateway's timer wheel has a 1s tick and rounds deadlines up, so a slow connection closes 5–6s after its state began. Handshake bucket defaults are 25,000/s with a burst of 2,500, and concurrency is half the loops. The SDK's pre-handshake retry waits 0.5–1.5s.
- Existing scenarios (`smoke`, `capacity`, `gwloss`, `slowlogic`) and their `LG_PID`, `LG_NAME`, `LG_STATUS` and `lg_*` calls keep working unchanged on instance `main`.
- Comments and names match the surrounding code. New behaviour cites its choice number (#119, #120) or P6a2bI-1.

## Review Focus

1. **Existing scenarios under the new `lg_start` guard.** `lg_start` now refuses an instance that is still running. Before, it silently overwrote `LG_PID`, and the old loadgen kept the control port, so the new one's `/stats` check passed against the old process. Expected: every existing scenario calls `lg_stop` (directly, or through `slowlogic`'s `down`) before its next `lg_start`. This was checked by grep for `smoke.sh` (lines 92/113, 122/136), `capacity.sh` (158/278), `gwloss.sh` (473, then 503/559/804) and `slowlogic.sh` (320, `down`'s `lg_stop`, 397). The guard is pinned by `inst: a second start of a running instance is refused` (Task 2). A `gwloss` or `slowlogic` dry run is not part of this plan. If the executor has the time, `GWLOSS_PHASES=b1` with the 2 × 1,000 dry-run settings is the cheapest end-to-end check.
2. **The garbage rate loadgen actually reaches, and the rate that reaches a DH.** The garbage schedule is open loop: when loadgen falls behind, it starts the overdue attempts back to back. `skipped` counts only attempts refused at the `-clients` in-flight cap, not lateness. `flood.attempts / elapsed_s` is the *schedule* rate: it says loadgen kept up. It counts dials started, failed dials and attempts cut off at the stop included, and `closed` is the same for a failed DH, `handshake_rate`, `noise_unknown_key`, `at_capacity` and `pending_mem`. The rate *delivered* to the limiter is the gateways' `handshakes_total{result="failed"}` delta over the flood window plus `{no_slot, no_token}` (O3). The DH rate is the `failed` delta alone, because `no_slot`/`no_token` are refused before any DH (wording corrected after the final review). 6b3b states which of the two it gates. Expected: 6b3b gates on the delivered rate (e.g. ≥ 0.95 × rate × duration) before reading any gateway number as the effect of that rate, as the probe's `failed + limited ≥ closed` check does here. A stale `NOISE_KEY_ID` or a full gateway would otherwise turn the run into the cheap flood and still pass. In the dry runs, 20,009–20,018 attempts were made in 10s at 2,000/s, with 0 skipped, and the gateways failed every one.
3. **TIME_WAIT at Tier 2 rates.** The gateway closes first, so loadgen keeps no TIME_WAIT, and the gateway's side holds one per garbage attempt for 60s. At 20,000/s that is ~1.2M, beyond the kernel's default `net.ipv4.tcp_max_tw_buckets` (262,144). `scripts/tier2/sysctl.conf` does not raise it, so the kernel drops the excess TIME_WAIT entries and logs `TCP: time wait bucket table overflow`. Expected: harmless to the gateway, and no `dial-no-port` in loadgen's outcomes. Not exercised here (the dry run made 20k attempts in total). 6b3b records `ss -s`'s `timewait` and checks the outcomes.
4. **Instances named inside `$(...)`.** `lg_in` registers a new instance in the shell that runs it. A first `$(lg_in x …)` registers `x` only in that subshell, so the parent may later give another instance that index. Expected: harmless, because ports come from the parent's order, and `lg_start` and `lg_stop` must run in the parent anyway (a subshell cannot keep or wait for the pid). The `loadgen.sh` header and `lg_in`'s comment say so. Pinned by `inst: a 5th instance is refused` (Task 2), whose count leaves out the instance named only in a subshell.
5. **Slow-auth spends the handshake bucket.** Each slow-auth connection runs a real Noise handshake on both sides before it goes quiet. Each one is admitted by the same limiter that legitimate clients need, and it costs loadgen an X25519. At the probe's 500 connections every ~6s this is ~85/s. At Tier 2 scale (100k slow-auth connections ≈ 16k handshakes/s) it would by itself near the 25k/s bucket, which would make "legitimate clients get no `Busy`" fail for the wrong reason. A redial at once would have been worse: every client the wheel closed in one tick would be accepted again within one tick's interval and closed together every round, so the bucket would see per-tick blocks of about the ramp rate (e.g. 20k within milliseconds against a 2,500 burst). The 0–1s redial jitter (choice 1, O1) spreads each tick's cohort over the tick, so the arrival rate is the average. Expected: 6b3b sizes the slow-auth share against the bucket and records `handshakes_total{result=no_token|no_slot}`. Flagged, not gated, here.
6. **The limiter path.** The probe's dry run never makes the limiter refuse (`handshake_rate 0`: one loop per gateway, concurrency 1, 2,000/s against 25k/s). `TestSlowAuthRefusedAtTheLimitBacksOff` (Task 1) covers it in process: a gateway with a 2/s bucket and a burst of 1 refuses slow-auth's handshakes, loadgen counts them `pre-handshake` and backs off 0.5–1.5s each time, with no spin (O2). 6b3b's Tier 2 run is its first test at scale.

## Implementation choices this plan makes

1. **#119 (decided, amended): three flood modes on the existing `Runner`, each counted in `Stats.Flood`.**
   - **`garbage`** sends one well-formed client handshake frame: the right version, `CMD_HANDSHAKE`, `HandshakeLen` and the stack's `key_id`, followed by 48 random bytes as the Noise message. It passes every check the gateway makes before any crypto. The gateway therefore admits it through the handshake limiter, computes the DH and fails the AEAD check (`handshakes_total{result="failed"}`), or refuses it at the limit (`handshake_rate`). This is the expensive garbage, the kind the spec's limiter exists for. A frame with an unknown key_id or a bad header would be closed before any crypto and test nothing. Loadgen makes no X25519 for it, so it can flood far faster than it can log in. Attempts start at `-rate` per second, open loop, with at most `-clients` in flight. An attempt due while the cap is full is counted as `skipped`.
   - **`slow-handshake`** holds `-clients` connections that send nothing (`HANDSHAKE` state).
   - **`slow-auth`** holds `-clients` connections that complete the handshake and send no Login (`AUTH_PENDING` state).
   - **The gateway always closes first.** Every attempt waits up to `-timeout` (default 10s) for the gateway to close.
   - **`held`** is a distribution over the attempts the gateway closed: from the garbage frame's write, the slow-handshake dial or the slow-auth handshake, until the close.
   - **Outcomes:** `closed`, `answered` (the gateway sent bytes where it should only close), `timeout`, `pre-handshake`/`handshake` (slow-auth's own handshake failed), `error`, and dial failures under light mode's names.
   - **Redialing:** a slow client closed after ≥ 3s (the config range's lower bound, so by its timeout) dials again after a uniform 0–1s, one tick of the gateway's timer wheel. Dialing at once would keep each tick's cohort phase-locked to the wheel, redialing as one burst every round (Review Focus 5, O1). With the jitter, the flood keeps ~90% of its slots. One closed sooner (at capacity, at the limit) waits 0.5–1.5s, as the SDK does after a pre-handshake close, so it never spins.
   - **Stopping:** attempts in flight at the stop are not counted. Flood modes default to all 253 sources ("from many sources").
   - **Alternatives:** a separate flood binary would duplicate `Env`, the balancer and the control API. A flood inside the light runner would break "one mode per process" (P6a2bI-1).
2. **#119 (decided): `-source-net K`, sources `127.0.K.2…`.** Each (source, gateway) pair has 28,232 ports (#69). A flood on the legitimate instance's sources would take ports from it (and from its relogins in 6b3c) and could produce `dial-no-port` failures that are not the gateway's doing. Separate /24s make the two populations' ports independent. They also stay distinguishable in `ss` output. `NewBalancer` and `Sources` keep their signatures. `NewBalancerIn` and `SourcesIn` take the octet.
3. **#120 (decided, amended): instances in `loadgen.sh`, selected by `lg_in INST CMD…`, at most 4.**
   - **Selection:** every existing function acts on the current instance, so no call site changes and `$LG_PID` still names the current instance's process. `lg_in` saves the current instance's `LG_PID`/`LG_NAME`/`LG_STATUS`, loads INST's, runs CMD, and restores all three (O5). Instance k's control port is `STACK_PORT_BASE + 5 + k`.
   - **Why at most 4:** main plus three running at once, enough for 6b3b (legitimate + garbage, or + slow-handshake + slow-auth) and 6b3c (SDK storm + light heartbeat probe). `stack_ports` adds base + 6 … + 8, so a leftover loadgen on any of them is refused before the stack starts.
   - **Names are slots for the whole script (O4):** the cap counts every instance name a script uses, and a name is never freed, so indexes (and ports) never change (Review Focus 4 depends on that). Scenarios name instances by role (`flood`, `flood2`), not by phase, and start a stopped instance again for the next phase, under a new NAME (its own log and result file). The probe does this: `flood` runs garbage and then slow-handshake.
   - **`lg_start` guards:** it refuses an instance that still runs, and a NAME a running instance already holds (the two would share `loadgen-NAME.json`).
   - **Per-instance settings:** `LG_CPUS` (default `T2_CPUS_LOADGEN`) pins each instance under `STACK_PIN=1`. `LG_ENV` is read at each `lg_start`, as before.
   - **New helpers:** `lg_stop_all` stops every instance (scenario cleanup and phase ends), with each status in `LG_STATUSES[INST]`. `lg_wait EXPR SECONDS` is the general wait over `/stats` (a flood has no `online`).
   - **Alternative:** a function per instance (`lg_start_flood`, …) would fix the scenario's shape into the library.
4. **The dry run is a load-probe phase, not a Tier 2 scenario** (#118). Between the probe's light and SDK phases, `floods` runs 2,000 light clients. Beside them, instance `flood` runs garbage at 2,000/s for 10s. Then `flood` runs again with 500 slow-handshake connections, beside `flood2`'s 500 slow-auth ones, so 3 instances are up at once and one name stays free. It gates every outcome and the gateways' own counters, the slow holds (p50 ≥ 5s, p99 ≤ 6.5s = 5s + one tick + 0.5s, max ≤ 7.5s, so a contended 2-core machine does not flake it, O7), and the light clients' Pings. That runs on every `make test-load-probe`, so a regression in the modes or the instances shows on the dev machine before 6b3b reaches the host.
5. **#121 (decided): the spec's handshake-flood bullet holds two tests** (O6). (1) "Garbage handshakes from many sources leave heartbeats answered on time": a flood beside a legitimate population. (2) "A legitimate 20k/s storm is not rejected by the bucket": the bucket's sizing check, run without garbage, because 20k/s of garbage plus a 20k/s storm cannot fit under a 25k/s bucket. 6b3b runs (2) as a light instance ramping at 20,000/s and judges it by `failed["pre-handshake"]` and the gateways' `handshake_rate` / `handshakes_total{no_slot, no_token}`.

## File Structure

- Create `internal/loadgen/flood.go`: the modes, `FloodStats`, `garbageHandshake`, the garbage schedule, the slow loop, `awaitClose` (Task 1).
- Create `internal/loadgen/flood_test.go`: unit and in-process tests (Task 1).
- Modify `internal/loadgen/runner.go` (`Config.SourceNet`, `Stats.Flood`, validation, `Run`, `start`), `internal/loadgen/balancer.go` (`SourcesIn`, `NewBalancerIn`), `internal/loadgen/e2e_test.go` (`startStack` options, `gatewayRegs`), `cmd/loadgen/main.go` (flags), `cmd/loadgen/main_test.go` (Task 1).
- Modify `scripts/loadgen.sh` (instances, `lg_in`, `lg_wait`, `lg_stop_all`), `scripts/stack.sh` (`stack_ports`), `scripts/loadgen_test.sh`, `scripts/stack_test.sh` (Task 2).
- Modify `scripts/load-probe.sh` (phase `floods`), `README.md`, the roadmap, this plan's *Execution Notes* (Task 3).

---

### Task 1: Flood modes in loadgen (#119)

**Files:**
- Create: `internal/loadgen/flood.go`, `internal/loadgen/flood_test.go`
- Modify: `internal/loadgen/runner.go`, `internal/loadgen/balancer.go`, `cmd/loadgen/main.go`
- Test: `internal/loadgen/flood_test.go`, `internal/loadgen/e2e_test.go`, `cmd/loadgen/main_test.go`

**Interfaces:**
- Consumes: `Runner` (`cfg`, `env`, `bal`, `started`, `rampDone`, `ramped`), `Balancer.Dial(ctx) (net.Conn, int, error)`, `dialReason(error) string`, `newHist`/`hist.dist`, `clientconn.New(nc).Handshake`, `clientconn.IsClosed`, `clientconn.ErrClosedBeforeReply`, `wire.Header.Put`, `wire.HeaderLen`/`HandshakeLen`/`KeyIDLen`; in tests `startStack`, `run`, `waitFor`.
- Produces:
  - Modes: `ModeGarbage = "garbage"`, `ModeSlowHandshake = "slow-handshake"`, `ModeSlowAuth = "slow-auth"`.
  - Outcomes: `FloodClosed`, `FloodAnswered`, `FloodTimeout`, `FloodPreHandshake`, `FloodHandshake`, `FloodError` (`"closed"`, `"answered"`, `"timeout"`, `"pre-handshake"`, `"handshake"`, `"error"`).
  - `type FloodStats struct { Attempts, Skipped, Open int64; Outcomes map[string]int64; Held Dist }`, served as `/stats`'s and the result's `"flood"`, with keys `attempts`, `skipped`, `open`, `outcomes`, `held`.
  - `Config.SourceNet int`, and the flag `-source-net`.
  - `SourcesIn(netOctet, n int) ([]net.IP, error)` and `NewBalancerIn(addrs []string, netOctet, sources int) (*Balancer, error)`.
  - `redialWait(held, ok) time.Duration`, a slow client's wait before it dials again (O1).
  - Test helpers `type stackOpt func(*gateway.Config, *tcp.Config)` and `startStack(t, n, opts ...stackOpt)`, applied after both defaults so a test can set the handshake limit (O2), `gatewayRegs` (gateway id → `*prometheus.Registry`) and `metric(t, id, name, label, value) float64`.

- [ ] **Step 1: Write the failing tests**

Create `internal/loadgen/flood_test.go`:

```go
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
```

Apply the test-helper and flag-test changes (`git apply --check` first):

```diff
diff --git a/cmd/loadgen/main_test.go b/cmd/loadgen/main_test.go
index 10ae8f4..80bb48b 100644
--- a/cmd/loadgen/main_test.go
+++ b/cmd/loadgen/main_test.go
@@ -26,6 +26,8 @@ func TestRunRefusesBadFlags(t *testing.T) {
 		{[]string{"-nope"}, "flag provided but not defined"},
 		{[]string{"-env", "ENV", "-down", "gw-9"}, "down"},
 		{[]string{"-env", "ENV", "-precompute"}, "sdk mode"},
+		{[]string{"-env", "ENV", "-mode", "slow-auth", "-timeout", "0s"}, "Timeout"},
+		{[]string{"-env", "ENV", "-mode", "garbage", "-source-net", "256"}, "source net"},
 	} {
 		for i, a := range tc.args {
 			if a == "ENV" {
diff --git a/internal/loadgen/e2e_test.go b/internal/loadgen/e2e_test.go
index 49dcb76..22795c7 100644
--- a/internal/loadgen/e2e_test.go
+++ b/internal/loadgen/e2e_test.go
@@ -42,9 +42,18 @@ func (s staticKeys) Verify(tok string) (auth.Claims, error) {
 func (staticKeys) Ready() bool         { return true }
 func (staticKeys) Run(context.Context) {}
 
-// startStack starts Logic and n gateways; stop[i] stops gateway i's
-// transport (its clients see their connections close) and core.
-func startStack(t *testing.T, n int) (env Env, stop []func()) {
+// gatewayRegs holds each started gateway's registry by its id (stack
+// metrics for the flood tests).
+var gatewayRegs sync.Map
+
+// stackOpt changes each gateway's core and transport configs (the
+// handshake limit is the transport's).
+type stackOpt func(*gateway.Config, *tcp.Config)
+
+// startStack starts Logic and n gateways, each config passed through opts;
+// stop[i] stops gateway i's transport (its clients see their connections
+// close) and core.
+func startStack(t *testing.T, n int, opts ...stackOpt) (env Env, stop []func()) {
 	t.Helper()
 	jwt, err := auth.GenerateKey("k1")
 	if err != nil {
@@ -84,7 +93,11 @@ func startStack(t *testing.T, n int) (env Env, stop []func()) {
 		cfg.Limits.Notrack = true
 		tcfg := tcp.DefaultConfig(cfg.NumLoops)
 		tcfg.Addr = "tcp://127.0.0.1:0"
+		for _, o := range opts {
+			o(&cfg, &tcfg)
+		}
 		greg := prometheus.NewRegistry()
+		gatewayRegs.Store(cfg.GatewayID, greg)
 		var tr *tcp.Transport
 		g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(greg), Registry: greg,
 			ReadyChecks: []func() error{func() error { return tr.Ready() }}})
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./internal/loadgen/ ./cmd/loadgen/`
Expected: `internal/loadgen` fails to build (`undefined: garbageHandshake`, `undefined: ModeGarbage`, `undefined: redialWait`, `unknown field SourceNet`, …). Then `cmd/loadgen`'s two new cases fail: `-mode slow-auth` gives `mode "slow-auth", want light or sdk`, not `Timeout`, and `-source-net` gives `flag provided but not defined`.

- [ ] **Step 3: Implement**

Create `internal/loadgen/flood.go`:

```go
package loadgen

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	mrand "math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/wire"
)

// The flood modes (Plan 6b3a; spec: Load tests, "handshake flood" and
// "slow-client flood"; Failure handling, "Handshake flood", "Slow clients").
// None logs in: each attempt dials through the balancer and waits for the
// gateway to close the connection, so the gateway, not loadgen, closes
// first and the TIME_WAIT is the gateway's, not a loadgen source port's.
const (
	// ModeGarbage sends one well-formed handshake frame under the stack's
	// key_id with a random Noise message: the gateway admits it through the
	// handshake limit, computes the DH, fails the handshake and closes
	// (handshakes_total{result="failed"}), or refuses it at the limit
	// (handshake_rate). Attempts start at Rate per second, open loop, at
	// most Clients in flight.
	ModeGarbage = "garbage"
	// ModeSlowHandshake holds Clients connections that send nothing: each
	// sits in HANDSHAKE until the gateway's handshake timeout closes it.
	ModeSlowHandshake = "slow-handshake"
	// ModeSlowAuth holds Clients connections that complete the handshake
	// and send no Login: each sits in AUTH_PENDING until the auth timeout.
	ModeSlowAuth = "slow-auth"
)

func floodMode(mode string) bool {
	return mode == ModeGarbage || mode == ModeSlowHandshake || mode == ModeSlowAuth
}

// Flood outcomes, one per attempt. Dial failures use dialReason's names.
const (
	FloodClosed       = "closed"        // the gateway closed (FIN or RST): the outcome the spec expects
	FloodAnswered     = "answered"      // the gateway sent bytes where it should only close
	FloodTimeout      = "timeout"       // no close within Timeout: loadgen closed it
	FloodPreHandshake = "pre-handshake" // slow-auth: closed before the handshake reply (the limit)
	FloodHandshake    = "handshake"     // slow-auth: the handshake failed otherwise
	FloodError        = "error"         // another read error
)

// slowHeld is the shortest hold that counts as the gateway's timeout (its
// configured range is 3–30s): a slow client closed sooner (at capacity, at
// the handshake limit) waits 0.5–1.5s before it dials again, as the SDK
// does after a pre-handshake close. One closed by the timeout waits 0–1s,
// one tick of the gateway's timer wheel: dialing again at once would put
// every client the wheel closed in one tick back into one tick's interval,
// so the cohort would be closed and redial together every round, a burst
// of handshakes each tick (Plan 6b3a, O1). The flood still keeps ~90% of
// its slots.
const slowHeld = 3 * time.Second

// FloodStats are a flood run's counts (Stats.Flood). Held is from the
// garbage frame's write, the slow-handshake dial or the slow-auth
// handshake to the gateway's close, over the attempts it closed.
type FloodStats struct {
	Attempts int64            `json:"attempts"` // dials started
	Skipped  int64            `json:"skipped"`  // garbage: due while Clients attempts were in flight
	Open     int64            `json:"open"`     // connections open now
	Outcomes map[string]int64 `json:"outcomes"`
	Held     Dist             `json:"held"`
}

type floodRec struct {
	mu       sync.Mutex
	attempts int64
	skipped  int64
	open     int64
	outcomes map[string]int64
	held     *hist
}

func newFloodRec() *floodRec { return &floodRec{outcomes: map[string]int64{}, held: newHist()} }

func (f *floodRec) attempt() {
	f.mu.Lock()
	f.attempts++
	f.mu.Unlock()
}

func (f *floodRec) skip() {
	f.mu.Lock()
	f.skipped++
	f.mu.Unlock()
}

func (f *floodRec) opened(d int64) {
	f.mu.Lock()
	f.open += d
	f.mu.Unlock()
}

// outcome counts one attempt's end; held < 0 records no hold.
func (f *floodRec) outcome(why string, held time.Duration) {
	f.mu.Lock()
	f.outcomes[why]++
	if held >= 0 {
		f.held.add(held.Milliseconds())
	}
	f.mu.Unlock()
}

func (f *floodRec) stats() *FloodStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &FloodStats{Attempts: f.attempts, Skipped: f.skipped, Open: f.open, Outcomes: map[string]int64{},
		Held: f.held.dist(f.held.n)}
	for k, v := range f.outcomes {
		s.Outcomes[k] = v
	}
	return s
}

// garbageHandshake is a client handshake frame under keyID whose Noise
// message is random bytes: it passes the gateway's frame and key_id checks,
// and fails only once the gateway has computed the DH.
func garbageHandshake(keyID uint16) []byte {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	wire.Header{Version: wire.Version, Cmd: uint16(clientpb.Cmd_CMD_HANDSHAKE), Length: wire.HandshakeLen}.Put(f)
	binary.BigEndian.PutUint16(f[wire.HeaderLen:], keyID)
	rand.Read(f[wire.HeaderLen+wire.KeyIDLen:])
	return f
}

// garbage starts ModeGarbage's attempts at Rate per second until ctx ends.
// An attempt due while Clients are in flight is skipped and counted: the
// gateway closes slower than the rate asks, or loadgen cannot keep up.
func (r *Runner) garbage(ctx context.Context, wg *sync.WaitGroup) {
	sem := make(chan struct{}, r.cfg.Clients)
	begin := time.Now()
	for i := 0; ; i++ {
		due := begin.Add(time.Duration(float64(i) / r.cfg.Rate * float64(time.Second)))
		if d := time.Until(due); d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case sem <- struct{}{}:
			r.started.Add(1)
			wg.Go(func() {
				defer func() { <-sem }()
				r.garbageOnce(ctx)
			})
		default:
			r.flood.skip()
		}
	}
}

// garbageOnce is one ModeGarbage attempt.
func (r *Runner) garbageOnce(ctx context.Context) {
	nc, why := r.floodDial(ctx)
	if nc == nil {
		if why != "" {
			r.flood.outcome(why, -1)
		}
		return
	}
	defer r.floodClose(nc)
	sent := time.Now()
	if _, err := nc.Write(garbageHandshake(r.env.NoiseKeyID)); err != nil {
		if clientconn.IsClosed(err) && ctx.Err() == nil {
			r.flood.outcome(FloodClosed, time.Since(sent))
		}
		return
	}
	r.awaitClose(ctx, nc, sent)
}

// slow keeps one ModeSlowHandshake or ModeSlowAuth connection open, dialing
// again after redialWait whenever the gateway closes it, until ctx ends.
func (r *Runner) slow(ctx context.Context) {
	for ctx.Err() == nil {
		held, ok := r.slowOnce(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(redialWait(held, ok)):
		}
	}
}

// redialWait is a slow client's wait before it dials again (slowHeld).
func redialWait(held time.Duration, ok bool) time.Duration {
	if ok && held >= slowHeld {
		return mrand.N(time.Second)
	}
	return 500*time.Millisecond + mrand.N(time.Second)
}

// slowOnce is one slow connection: dial, the handshake in ModeSlowAuth,
// then silence until the gateway closes it. ok: the gateway closed it, held
// after the dial or the handshake.
func (r *Runner) slowOnce(ctx context.Context) (held time.Duration, ok bool) {
	nc, why := r.floodDial(ctx)
	if nc == nil {
		if why != "" {
			r.flood.outcome(why, -1)
		}
		return 0, false
	}
	defer r.floodClose(nc)
	if r.cfg.Mode == ModeSlowAuth {
		err := clientconn.New(nc).Handshake(r.env.NoiseKeyID, r.env.NoisePublic, time.Now().Add(r.cfg.Timeout))
		switch {
		case ctx.Err() != nil:
			return 0, false
		case errors.Is(err, clientconn.ErrClosedBeforeReply):
			r.flood.outcome(FloodPreHandshake, -1)
			return 0, false
		case err != nil:
			r.flood.outcome(FloodHandshake, -1)
			return 0, false
		}
	}
	return r.awaitClose(ctx, nc, time.Now())
}

// floodDial dials through the balancer by Timeout and counts the attempt
// and the open connection; why is "" when ctx ended.
func (r *Runner) floodDial(ctx context.Context) (net.Conn, string) {
	r.flood.attempt()
	dctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	nc, _, err := r.bal.Dial(dctx)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ""
		}
		return nil, dialReason(err)
	}
	r.flood.opened(1)
	return nc, ""
}

func (r *Runner) floodClose(nc net.Conn) {
	nc.Close()
	r.flood.opened(-1)
}

// awaitClose waits up to Timeout after from for the gateway to close nc,
// counts the outcome, and returns the hold when the gateway closed it.
// When ctx ends first the attempt is not counted.
func (r *Runner) awaitClose(ctx context.Context, nc net.Conn, from time.Time) (time.Duration, bool) {
	stop := context.AfterFunc(ctx, func() { nc.SetReadDeadline(time.Now()) })
	defer stop()
	if err := nc.SetReadDeadline(from.Add(r.cfg.Timeout)); err != nil {
		return 0, false
	}
	var b [64]byte
	n, err := nc.Read(b[:])
	held := time.Since(from)
	switch {
	case ctx.Err() != nil:
		return 0, false
	case n > 0:
		r.flood.outcome(FloodAnswered, -1)
	case clientconn.IsClosed(err):
		r.flood.outcome(FloodClosed, held)
		return held, true
	case errors.Is(err, os.ErrDeadlineExceeded):
		r.flood.outcome(FloodTimeout, -1)
	default:
		r.flood.outcome(FloodError, -1)
	}
	return 0, false
}
```

Apply the runner, balancer and flag changes:

```diff
diff --git a/cmd/loadgen/main.go b/cmd/loadgen/main.go
index bb9934d..faaab1f 100644
--- a/cmd/loadgen/main.go
+++ b/cmd/loadgen/main.go
@@ -3,7 +3,10 @@
 // least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
 // and serves the control API on -control (GET /stats, POST /kill, POST /mark,
 // POST /up, GET /slo, GET /runtime; choice #84) until SIGTERM, Ctrl-C or -duration.
-// -down keeps gateways out of the balancer until POST /up (#93). It then writes
+// -down keeps gateways out of the balancer until POST /up (#93). The flood
+// modes (garbage, slow-handshake, slow-auth; Plan 6b3a) log nothing in: they
+// put a handshake flood or a slow-client flood beside another instance's
+// legitimate clients, from their own -source-net. It then writes
 // the result, the stats at that moment and every mark's SLO against
 // -within, as JSON to -out, and only then stops every client: stopping 200k
 // SDK clients can outlast the caller's wait, and the result must survive a
@@ -46,13 +49,14 @@ func run(ctx context.Context, args []string, stderr io.Writer) error {
 	fs.SetOutput(stderr)
 	var cfg loadgen.Config
 	envPath := fs.String("env", "", "stack.env written by scripts/stack.sh (required)")
-	fs.StringVar(&cfg.Mode, "mode", "light", "light (clientconn + shared pinger) or sdk (one SDK client each)")
-	fs.IntVar(&cfg.Clients, "clients", 1000, "clients; client i logs in as <uid-prefix>-<i>")
-	fs.Float64Var(&cfg.Rate, "rate", 1000, "clients started per second")
+	fs.StringVar(&cfg.Mode, "mode", "light", "light (clientconn + shared pinger), sdk (one SDK client each), or a flood: garbage, slow-handshake, slow-auth")
+	fs.IntVar(&cfg.Clients, "clients", 1000, "clients; client i logs in as <uid-prefix>-<i>; garbage: attempts in flight at most")
+	fs.Float64Var(&cfg.Rate, "rate", 1000, "clients started per second; garbage: attempts per second")
 	fs.StringVar(&cfg.UIDPrefix, "uid-prefix", "lg", "uid prefix, distinct per run")
-	fs.IntVar(&cfg.Sources, "sources", 0, "source addresses 127.0.0.2…; 0: one per 5,000 connections per gateway")
+	fs.IntVar(&cfg.Sources, "sources", 0, "source addresses 127.0.<source-net>.2…; 0: one per 5,000 connections per gateway (floods: 253)")
+	fs.IntVar(&cfg.SourceNet, "source-net", 0, "third octet of the source addresses, one per instance on a stack")
 	fs.DurationVar(&cfg.PingEvery, "ping-every", 150*time.Second, "light: Ping interval (the gateway's -heartbeat)")
-	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "light: dial + handshake + Login per attempt")
+	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "light: dial + handshake + Login per attempt; floods: dial, then the wait for the gateway's close")
 	down := fs.String("down", "", "gateway ids out of the balancer until POST /up, comma-separated")
 	fs.BoolVar(&cfg.Precompute, "precompute", false, "sdk: prepare each logged-in client's next handshake until the first mark (#106)")
 	control := fs.String("control", "", "control API listen address (none if empty)")
diff --git a/internal/loadgen/balancer.go b/internal/loadgen/balancer.go
index 39be408..3967f7d 100644
--- a/internal/loadgen/balancer.go
+++ b/internal/loadgen/balancer.go
@@ -38,20 +38,31 @@ const maxSources = 253
 
 // Sources are loadgen's source addresses 127.0.0.2, 127.0.0.3, … (choice
 // #69): Linux answers on all of 127/8 with no setup.
-func Sources(n int) ([]net.IP, error) {
+func Sources(n int) ([]net.IP, error) { return SourcesIn(0, n) }
+
+// SourcesIn are n source addresses 127.0.netOctet.2, 127.0.netOctet.3, …:
+// loadgen instances on one stack each take their own (Plan 6b3a), so a
+// flood's connections never use a legitimate instance's ports per
+// (source, gateway) pair.
+func SourcesIn(netOctet, n int) ([]net.IP, error) {
 	if n < 1 || n > maxSources {
 		return nil, fmt.Errorf("loadgen: %d sources, want 1–%d", n, maxSources)
 	}
 	ips := make([]net.IP, n)
 	for i := range ips {
-		ips[i] = net.IPv4(127, 0, 0, byte(2+i))
+		ips[i] = net.IPv4(127, 0, byte(netOctet), byte(2+i))
 	}
 	return ips, nil
 }
 
 // NewBalancer balances over addrs from n source addresses.
 func NewBalancer(addrs []string, sources int) (*Balancer, error) {
-	srcs, err := Sources(sources)
+	return NewBalancerIn(addrs, 0, sources)
+}
+
+// NewBalancerIn balances over addrs from SourcesIn(netOctet, sources).
+func NewBalancerIn(addrs []string, netOctet, sources int) (*Balancer, error) {
+	srcs, err := SourcesIn(netOctet, sources)
 	if err != nil {
 		return nil, err
 	}
diff --git a/internal/loadgen/runner.go b/internal/loadgen/runner.go
index c731ea5..ec37395 100644
--- a/internal/loadgen/runner.go
+++ b/internal/loadgen/runner.go
@@ -29,15 +29,16 @@ const connsPerSource = 5_000
 
 // Config configures a run.
 type Config struct {
-	Mode       string        // "light" or "sdk" (choice #80)
-	Clients    int           // client i logs in as uid <UIDPrefix>-<i>
-	Rate       float64       // clients started per second
+	Mode       string        // "light" or "sdk" (choice #80), or a flood mode: ModeGarbage, ModeSlowHandshake, ModeSlowAuth
+	Clients    int           // client i logs in as uid <UIDPrefix>-<i>; ModeGarbage: attempts in flight at most
+	Rate       float64       // clients started per second; ModeGarbage: attempts per second
+	SourceNet  int           // source addresses 127.0.SourceNet.2…, so instances keep apart (Plan 6b3a)
 	UIDPrefix  string        // distinct per loadgen run, so runs never replace each other's sessions
-	Sources    int           // source addresses; 0: one per 5,000 connections per gateway, with one gateway gone or Down
+	Sources    int           // source addresses; 0: one per 5,000 connections per gateway, with one gateway gone or Down; flood modes: all 253
 	Down       []string      // gateway ids out of the balancer from the start, until POST /up (b_stress, #93)
 	Precompute bool          // sdk: prepare each logged-in client's next handshake until the first mark (#106)
 	PingEvery  time.Duration // light: the Ping interval (cmd/loadgen: 150s, the gateway's default heartbeat)
-	Timeout    time.Duration // light: dial + handshake + Login (cmd/loadgen: 10s)
+	Timeout    time.Duration // light: dial + handshake + Login; flood modes: dial, and then the wait for the close (cmd/loadgen: 10s)
 	windows    sdk.Windows   // tests only: the SDK's built-in reconnect windows
 }
 
@@ -50,6 +51,7 @@ type Stats struct {
 	RampDone   bool             `json:"ramp_done"`
 	ElapsedS   float64          `json:"elapsed_s"`
 	Precompute *PrecomputeStats `json:"precompute,omitempty"`
+	Flood      *FloodStats      `json:"flood,omitempty"`
 	Counts
 }
 
@@ -61,6 +63,7 @@ type Runner struct {
 	bal      *Balancer
 	pinger   pingers
 	pre      *precomputer // nil without Precompute
+	flood    *floodRec    // nil unless a flood mode
 	begin    atomic.Int64 // unix ns Run started, 0: not yet
 	started  atomic.Int64
 	rampDone atomic.Bool
@@ -73,8 +76,8 @@ type Runner struct {
 // New checks cfg and prepares a run against env's gateways.
 func New(cfg Config, env Env) (*Runner, error) {
 	switch {
-	case cfg.Mode != "light" && cfg.Mode != "sdk":
-		return nil, fmt.Errorf("loadgen: mode %q, want light or sdk", cfg.Mode)
+	case cfg.Mode != "light" && cfg.Mode != "sdk" && !floodMode(cfg.Mode):
+		return nil, fmt.Errorf("loadgen: mode %q, want light, sdk, %s, %s or %s", cfg.Mode, ModeGarbage, ModeSlowHandshake, ModeSlowAuth)
 	case cfg.Clients < 1 || cfg.Rate <= 0:
 		return nil, errors.New("loadgen: Clients and Rate must be positive")
 	case cfg.Precompute && cfg.Mode != "sdk":
@@ -83,6 +86,10 @@ func New(cfg Config, env Env) (*Runner, error) {
 		return nil, errors.New("loadgen: UIDPrefix is required")
 	case cfg.Mode == "light" && (cfg.PingEvery <= 0 || cfg.Timeout <= 0):
 		return nil, errors.New("loadgen: light mode needs PingEvery and Timeout")
+	case floodMode(cfg.Mode) && cfg.Timeout <= 0:
+		return nil, errors.New("loadgen: flood modes need Timeout")
+	case cfg.SourceNet < 0 || cfg.SourceNet > 255:
+		return nil, fmt.Errorf("loadgen: source net %d, want 0–255", cfg.SourceNet)
 	}
 	var down []int
 	for _, id := range cfg.Down {
@@ -97,6 +104,9 @@ func New(cfg Config, env Env) (*Runner, error) {
 	if len(down) >= len(env.GatewayAddrs) {
 		return nil, errors.New("loadgen: down: every gateway")
 	}
+	if cfg.Sources == 0 && floodMode(cfg.Mode) {
+		cfg.Sources = maxSources // "garbage handshakes from many sources" (spec: Load tests)
+	}
 	if cfg.Sources == 0 {
 		// Sized for the busiest gateway: one gateway gone, or every client on
 		// the gateways up at the start.
@@ -104,7 +114,7 @@ func New(cfg Config, env Env) (*Runner, error) {
 		perGW := (cfg.Clients + gws - 1) / gws
 		cfg.Sources = min(maxSources, (perGW+connsPerSource-1)/connsPerSource)
 	}
-	bal, err := NewBalancer(env.GatewayAddrs, cfg.Sources)
+	bal, err := NewBalancerIn(env.GatewayAddrs, cfg.SourceNet, cfg.Sources)
 	if err != nil {
 		return nil, err
 	}
@@ -118,6 +128,9 @@ func New(cfg Config, env Env) (*Runner, error) {
 	if cfg.Precompute {
 		r.pre = newPrecomputer(r, cfg.Clients, runtime.GOMAXPROCS(0)/2)
 	}
+	if floodMode(cfg.Mode) {
+		r.flood = newFloodRec()
+	}
 	return r, nil
 }
 
@@ -134,7 +147,8 @@ func (r *Runner) token(i int) (string, error) {
 func (r *Runner) uid(i int) string { return r.cfg.UIDPrefix + "-" + strconv.Itoa(i) }
 
 // Run starts client i at begin + i/Rate, then holds them until ctx ends,
-// and returns once every client is stopped.
+// and returns once every client is stopped. ModeGarbage has no ramp: its
+// attempts run at Rate until ctx ends.
 func (r *Runner) Run(ctx context.Context) {
 	begin := time.Now()
 	r.begin.Store(begin.UnixNano())
@@ -147,6 +161,13 @@ func (r *Runner) Run(ctx context.Context) {
 			wg.Go(func() { r.pre.run(ctx) })
 		}
 	}
+	if r.cfg.Mode == ModeGarbage {
+		r.rampDone.Store(true)
+		close(r.ramped)
+		r.garbage(ctx, &wg)
+		wg.Wait()
+		return
+	}
 	for i := 0; i < r.cfg.Clients && ctx.Err() == nil; i++ {
 		due := begin.Add(time.Duration(float64(i) / r.cfg.Rate * float64(time.Second)))
 		if d := time.Until(due); d > 0 {
@@ -187,11 +208,15 @@ func (r *Runner) Run(ctx context.Context) {
 
 func (r *Runner) start(ctx context.Context, wg *sync.WaitGroup, i int) {
 	var stop func()
-	if r.cfg.Mode == "light" {
+	switch {
+	case floodMode(r.cfg.Mode): // a slow client; ctx's end closes its connection
+		wg.Go(func() { r.slow(ctx) })
+		return
+	case r.cfg.Mode == "light":
 		lc := &lightConn{i: i}
 		stop = lc.stop
 		wg.Go(func() { r.light(ctx, lc) })
-	} else {
+	default:
 		c, err := r.newSDK(i)
 		if err != nil {
 			r.rec.Failed("sdk")
@@ -266,5 +291,8 @@ func (r *Runner) Stats() Stats {
 	if r.pre != nil {
 		s.Precompute = r.pre.stats()
 	}
+	if r.flood != nil {
+		s.Flood = r.flood.stats()
+	}
 	return s
 }
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `gofmt -l internal/ cmd/ && go vet ./internal/loadgen/ ./cmd/loadgen/ && go test -race -count=3 ./internal/loadgen/ && go test -race -count=1 ./cmd/loadgen/`
Expected: no gofmt output, vet clean, `ok` for both packages. The prototype took ~36s per `internal/loadgen` run. `TestSlowFloodHeldUntilTheGatewaysTimeout` takes ~5s per mode, and `TestSlowAuthRefusedAtTheLimitBacksOff` 3.5s. `TestGarbageFloodIsClosedByTheGateway` showed `held` p99 ≤ 16ms in every run. The first prototype of that test asked for every attempt to be `closed` and failed 4 runs out of 4: 1–3 attempts in flight at the stop go uncounted by design. The bound is now `closed ≥ attempts − 50` (the `-clients` cap).

- [ ] **Step 5: Mutation checks**

Each mutation must make the named test fail, as it did in the prototype. Revert each one before the next (`git checkout -- internal/`).
- `garbageHandshake` writes `keyID+1`: `TestGarbageHandshakeFailsOnlyAfterTheDH` fails with `key_id 8, want 7`.
- `Run`'s `if r.cfg.Mode == ModeGarbage {` becomes `if false {`, so the garbage runner ramps slow-handshake clients instead: `TestGarbageFloodIsClosedByTheGateway` fails with `timed out after 10s waiting for 100 garbage handshakes closed`.
- `New` drops `cfg.Sources = maxSources` for flood modes: `TestNewFloodModes` fails with `garbage: 1 sources from 127.0.3.2`.
- `slow`'s loop body becomes `r.slowOnce(ctx); return`, so a slow client never redials: `TestSlowFloodHeldUntilTheGatewaysTimeout` fails, timing out on `5 open again`.
- `redialWait`'s timeout branch returns `0` (no jitter, O1): `TestRedialWaitSpreadsOverATick` fails with `after a timeout: 0s–0s, want spread over the tick`.
- `redialWait`'s last line returns `0` (no backoff after an early close, O2): `TestSlowAuthRefusedAtTheLimitBacksOff` fails with `9238 attempts, want ≤ 140 (no spin)` (and `TestRedialWaitSpreadsOverATick` with `after an early close: 0s`).
- `slowOnce`'s `case errors.Is(err, clientconn.ErrClosedBeforeReply):` becomes `case false:`: `TestSlowAuthRefusedAtTheLimitBacksOff` fails with `Outcomes:map[handshake:57] …, want only pre-handshake outcomes`.

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen/flood.go internal/loadgen/flood_test.go internal/loadgen/runner.go internal/loadgen/balancer.go internal/loadgen/e2e_test.go cmd/loadgen/main.go cmd/loadgen/main_test.go
git commit -m "feat: loadgen flood modes garbage, slow-handshake, slow-auth and -source-net (#119)"
```

---

### Task 2: Loadgen instances in `loadgen.sh` (#120, P6a2bI-1)

**Files:**
- Modify: `scripts/loadgen.sh`, `scripts/stack.sh`
- Test: `scripts/loadgen_test.sh`, `scripts/stack_test.sh`

**Interfaces:**
- Consumes: `STACK_DIR`, `STACK_PORT_BASE`, `STACK_PIN`, `T2_CPUS_LOADGEN`, `T2_OURS_PGIDS`, `lg_py`.
- Produces:
  - `lg_in INST CMD…`
  - `lg_wait EXPR SECONDS` (EXPR in python over `GET /stats` as `d`)
  - `lg_stop_all`
  - `LG_INST` (current instance, default `main`), `LG_INSTANCES` (by index), `LG_MAX_INSTANCES=4`
  - `LG_PIDS`, `LG_NAMES` and `LG_STATUSES`, associative arrays by instance
  - `LG_CPUS`, read by `lg_start`
  - `lg_k INST` (index)
  - `lg_control` (now per instance)
  - `stack_ports`, now through base + 8
- Unchanged: `lg_start`, `lg_get`, `lg_post`, `lg_wait_online`, `lg_wait_back`, `lg_profile`, `lg_stop`, `lg_json` and `lg_true`, on the current instance. `LG_PID`, `LG_NAME` and `LG_STATUS` keep their meaning, for the current instance.

- [ ] **Step 1: Write the failing tests**

Apply (the fake loadgen is a python3 HTTP server that serves `/stats` on its `-control` port and writes its `-out` at SIGTERM):

```diff
diff --git a/scripts/loadgen_test.sh b/scripts/loadgen_test.sh
index 527cd7d..0cf42fa 100755
--- a/scripts/loadgen_test.sh
+++ b/scripts/loadgen_test.sh
@@ -64,4 +64,68 @@ expect "wait_online: a 2s wait ends by its deadline" "$((SECONDS - start <= 5))"
 expect "wait_online: gave up on the deadline, not on loadgen" "$(grep -c "online and failed after" "$dir/online.err")" 1
 kill $slow 2>/dev/null; wait $slow 2>/dev/null
 
+# Instances (P6a2bI-1), with a fake loadgen that serves /stats on its
+# -control port, and at SIGTERM writes its -out and exits $FAKE_EXIT.
+(
+  STACK_DIR=$dir/stack
+  mkdir -p "$STACK_DIR/bin"
+  cat >"$STACK_DIR/bin/loadgen" <<'PY'
+#!/usr/bin/env python3
+import http.server, json, os, signal, sys
+a = sys.argv[1:]
+host, port = a[a.index("-control") + 1].rsplit(":", 1)
+out = a[a.index("-out") + 1]
+doc = json.dumps({"out": os.path.basename(out), "mode": a[a.index("-mode") + 1]}).encode()
+class H(http.server.BaseHTTPRequestHandler):
+    def do_GET(self):
+        self.send_response(200); self.send_header("Content-Length", str(len(doc))); self.end_headers(); self.wfile.write(doc)
+    def log_message(self, *x): pass
+def term(*_):
+    open(out, "wb").write(doc)
+    os._exit(int(os.environ.get("FAKE_EXIT", "0")))
+signal.signal(signal.SIGTERM, term)
+http.server.HTTPServer((host, int(port)), H).serve_forever()
+PY
+  chmod +x "$STACK_DIR/bin/loadgen"
+  STACK_PORT_BASE=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
+  T2_OURS_PGIDS=()
+  lg_start a -mode light || echo "FAIL inst: main did not start"
+  main_pid=$LG_PID
+  LG_ENV=(FAKE_EXIT=3)
+  STACK_PIN=1 T2_CPUS_LOADGEN=0 LG_CPUS=1 lg_in flood lg_start b -mode garbage || echo "FAIL inst: flood did not start"
+  expect "inst: main's control port is base + 5" "$(lg_control)" "127.0.0.1:$((STACK_PORT_BASE + 5))"
+  expect "inst: flood's control port is base + 6" "$(lg_in flood lg_control)" "127.0.0.1:$((STACK_PORT_BASE + 6))"
+  expect "inst: main answers as main" "$(lg_get /stats 'd["out"]')" '"loadgen-a.json"'
+  expect "inst: flood answers as flood" "$(lg_in flood lg_get /stats 'd["mode"]')" '"garbage"'
+  expect "inst: LG_PID is main's again after lg_in" "$LG_PID" "$main_pid"
+  expect "inst: LG_CPUS pins the flood" "$(taskset -pc "${LG_PIDS[flood]}" | awk '{print $NF}')" 1
+  out=$(lg_start c -mode light 2>&1)
+  expect "inst: a second start of a running instance is refused" "$? $out" "1 loadgen: instance main already runs (a)"
+  out=$(lg_in probe lg_start a -mode light 2>&1)
+  expect "inst: another instance's running name is refused" "$? $out" "1 loadgen: name a is instance main's"
+  lg_in x3 true && lg_in x4 true # main, flood, x3, x4 (probe was named in a subshell only)
+  out=$(lg_in x5 true 2>&1)
+  expect "inst: a 5th instance is refused" "$? $out" "1 loadgen: instance x5: already 4 instances"
+  lg_wait 'd["mode"] == "light"' 2
+  expect "wait: a true expression" "$?" 0
+  start=$SECONDS
+  lg_wait 'd["mode"] == "sdk"' 1
+  expect "wait: a false one fails by its deadline" "$? $((SECONDS - start <= 2))" "1 1"
+  flood_pid=${LG_PIDS[flood]}
+  lg_in flood lg_stop
+  expect "inst: the flood stopped" "$(kill -0 "$flood_pid" 2>/dev/null && echo running || echo stopped) ${LG_STATUSES[flood]}" "stopped 3"
+  expect "inst: LG_STATUS is main's after lg_in" "[$LG_STATUS]" "[]"
+  expect "inst: main still runs" "$(kill -0 "$LG_PID" 2>/dev/null && echo running)" running
+  expect "inst: the flood's result" "$(lg_in flood lg_json 'd["out"]')" '"loadgen-b.json"'
+  lg_in flood lg_start b2 -mode slow-auth || echo "FAIL inst: a stopped instance did not start again"
+  LG_ENV=()
+  expect "inst: a stopped instance runs again under a new name" "$(lg_in flood lg_get /stats 'd["mode"]')" '"slow-auth"'
+  lg_stop_all
+  expect "inst: lg_stop_all stopped main" "$(kill -0 "$main_pid" 2>/dev/null && echo running || echo stopped) ${LG_STATUSES[main]} [$LG_PID]" "stopped 0 []"
+  expect "inst: LG_STATUS is main's after lg_stop_all" "$LG_STATUS ${LG_STATUSES[flood]}" "0 3"
+  expect "inst: the restarted instance's result" "$(lg_in flood lg_json 'd["out"]')" '"loadgen-b2.json"'
+  expect "inst: main's result" "$(lg_json 'd["out"]')" '"loadgen-a.json"'
+) | tee "$dir/inst.out"
+fails=$((fails + $(grep -c '^FAIL' "$dir/inst.out")))
+
 [ "$fails" -eq 0 ] && echo "loadgen_test: all passed" || { echo "loadgen_test: $fails failed"; exit 1; }
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index e7f9e2b..d1e03b4 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -29,6 +29,7 @@ procs=$((ncpu / 2 > 1 ? ncpu / 2 : 1))
 
 # The ports checked before stack_up include loadgen's control port.
 grep -qx $((STACK_PORT_BASE + 5)) <<<"$(stack_ports)" && ok "ports: loadgen's control port" || bad "ports: loadgen's control port missing"
+grep -qx $((STACK_PORT_BASE + 8)) <<<"$(stack_ports)" && ok "ports: the 4th loadgen instance's control port" || bad "ports: the 4th loadgen instance's control port missing"
 
 # stack_metric prints byte-sized sums whole, as Prometheus writes them
 # (5.36870912e+09), also under an awk whose %d stops at 2^31 (busybox; 6b1
```

- [ ] **Step 2: Run them to see them fail**

Run: `scripts/loadgen_test.sh; STACK_REDIS_SERVER=$HOME/.local/bin/redis-server scripts/stack_test.sh 2>&1 | grep -v '^ok'`
Expected: `loadgen_test` fails from `inst: flood did not start` on (`lg_in: command not found`; `LG_PIDS[flood]` unbound), and `inst: a second start of a running instance is refused` gets `0`, because the old `lg_start` started a second loadgen on main's port. The checks the review added (a stopped instance started again under a new name, O4; `LG_STATUS` as main's after `lg_in` and after `lg_stop_all`, O5) fail as well. `stack_test` fails `ports: the 4th loadgen instance's control port missing`. The `kill: … No such process` and `Killed … setsid` lines from `stack_test.sh` are its own expected kills; they print on `1b6f64c` too.

- [ ] **Step 3: Implement**

```diff
diff --git a/scripts/loadgen.sh b/scripts/loadgen.sh
index 27faac0..d5bf983 100644
--- a/scripts/loadgen.sh
+++ b/scripts/loadgen.sh
@@ -5,6 +5,7 @@
 #   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env,
 #                               #   with LG_ENV (VAR=VALUE..., e.g. GOMEMLIMIT=16GiB, #93) in its environment
 #   lg_wait_online N SECONDS    # until the ramp is done and N clients are logged in
+#   lg_wait EXPR SECONDS        # until EXPR over GET /stats is true
 #   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
 #   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
 #   lg_wait_back MARK SECONDS   # until every client of MARK logged in again
@@ -12,30 +13,96 @@
 #   lg_stop                     # SIGTERM, then wait (60s) for it to write its result; LG_STATUS = exit status
 #   lg_json EXPR                # EXPR over the result file of the last loadgen
 #   lg_true SOURCE EXPR         # "true" if EXPR is true over SOURCE (a file, or - for stdin), else "false"
+#   lg_in INST CMD...           # CMD (one of the above) on loadgen instance INST
+#   lg_stop_all                 # lg_stop on every instance; LG_STATUSES[INST] = each exit status
 #
 # Values print as JSON (null when the JSON cannot be read, e.g. loadgen died
 # before writing it), so they can go straight into t2_check members.
 #
 # Logs go to $STACK_DIR/logs/loadgen-NAME.log, the result (final stats and
-# every mark's SLO) to $STACK_DIR/artifacts/loadgen-NAME.json. The control
-# API listens on STACK_PORT_BASE + 5, one of stack_ports, so a leftover
-# loadgen there is refused before the stack starts. With STACK_PIN=1
-# loadgen is pinned to T2_CPUS_LOADGEN (choice #71). Like stack.sh's processes, loadgen runs in
-# its own process group, added to T2_OURS_PGIDS.
+# every mark's SLO) to $STACK_DIR/artifacts/loadgen-NAME.json. With
+# STACK_PIN=1 loadgen is pinned to LG_CPUS, default T2_CPUS_LOADGEN (choice
+# #71). Like stack.sh's processes, loadgen runs in its own process group,
+# added to T2_OURS_PGIDS.
+#
+# Instances (P6a2bI-1): several loadgens can run on one stack, one mode
+# each, e.g. a flood beside a legitimate population, or a light instance
+# answering heartbeats beside an SDK storm. Every function above acts on the
+# current instance, "main" unless inside lg_in; LG_PID, LG_NAME and
+# LG_STATUS are the current instance's. Instance k (main is 0, the others
+# numbered in the order lg_in first names them) has its control API on
+# STACK_PORT_BASE + 5 + k, among stack_ports, so a leftover loadgen there is
+# refused before the stack starts. A script names at most LG_MAX_INSTANCES
+# instances in all, and a name is never freed: name instances by role
+# (flood, flood2), not by phase, and start a stopped one again for the next
+# phase. Each lg_start takes its own NAME (its log and result file), and
+# each instance its own -uid-prefix and, for a flood, -source-net. LG_ENV
+# and LG_CPUS are read by lg_start, so set them before each:
+#
+#   lg_start legit -mode light -clients 20000 -uid-prefix legit
+#   LG_CPUS=0-1 lg_in flood lg_start garbage -mode garbage -rate 20000 -clients 5000 -source-net 1
+#   lg_in flood lg_get /stats 'd["flood"]["outcomes"]'
+#   lg_in flood lg_stop
+#   lg_in flood lg_start slowhs -mode slow-handshake -clients 50000 -source-net 1
+#   lg_stop_all
 
 LG_PID=""
 LG_NAME=""
 LG_STATUS=""
 LG_ENV=()
+LG_INST=main
+LG_INSTANCES=(main)
+LG_MAX_INSTANCES=4 # control ports STACK_PORT_BASE + 5 … + 8 (stack_ports)
+declare -A LG_PIDS=() LG_NAMES=() LG_STATUSES=()
+
+# lg_k INST: the instance's index; fails for an instance never named.
+lg_k() {
+  local i
+  for i in "${!LG_INSTANCES[@]}"; do
+    [ "${LG_INSTANCES[$i]}" = "$1" ] && { echo "$i"; return 0; }
+  done
+  return 1
+}
+
+lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5 + $(lg_k "$LG_INST")))"; }
 
-lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5))"; }
+# lg_in INST CMD...: CMD with INST as the current instance, then the
+# previous one again. A name first seen here takes the next index, for
+# good (#120, O4). Not in $(...) for lg_start or lg_stop: a subshell cannot
+# keep the pid or wait for it.
+lg_in() {
+  local inst=$1 prev=$LG_INST rc
+  shift
+  if ! lg_k "$inst" >/dev/null; then
+    [ ${#LG_INSTANCES[@]} -lt "$LG_MAX_INSTANCES" ] || { echo "loadgen: instance $inst: already $LG_MAX_INSTANCES instances" >&2; return 1; }
+    LG_INSTANCES+=("$inst")
+  fi
+  LG_PIDS[$prev]=$LG_PID LG_NAMES[$prev]=$LG_NAME LG_STATUSES[$prev]=$LG_STATUS
+  LG_INST=$inst LG_PID=${LG_PIDS[$inst]:-} LG_NAME=${LG_NAMES[$inst]:-} LG_STATUS=${LG_STATUSES[$inst]:-}
+  "$@"
+  rc=$?
+  LG_PIDS[$inst]=$LG_PID LG_NAMES[$inst]=$LG_NAME LG_STATUSES[$inst]=$LG_STATUS
+  LG_INST=$prev LG_PID=${LG_PIDS[$prev]:-} LG_NAME=${LG_NAMES[$prev]:-} LG_STATUS=${LG_STATUSES[$prev]:-}
+  return $rc
+}
 
 lg_start() {
+  local inst
+  if [ -n "$LG_PID" ] && kill -0 "$LG_PID" 2>/dev/null; then
+    echo "loadgen: instance $LG_INST already runs ($LG_NAME)" >&2
+    return 1
+  fi
+  for inst in "${!LG_PIDS[@]}"; do
+    if [ "$inst" != "$LG_INST" ] && [ "${LG_NAMES[$inst]:-}" = "$1" ] && kill -0 "${LG_PIDS[$inst]:-0}" 2>/dev/null; then
+      echo "loadgen: name $1 is instance $inst's" >&2
+      return 1
+    fi
+  done
   LG_NAME=$1
   shift
   local cmd=(env "${LG_ENV[@]}" "$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
     -out "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$@")
-  if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "$T2_CPUS_LOADGEN" "${cmd[@]}"); fi
+  if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "${LG_CPUS:-$T2_CPUS_LOADGEN}" "${cmd[@]}"); fi
   mkdir -p "$STACK_DIR/artifacts" "$STACK_DIR/logs"
   rm -f "$STACK_DIR/artifacts/loadgen-$LG_NAME.json"
   setsid "${cmd[@]}" >"$STACK_DIR/logs/loadgen-$LG_NAME.log" 2>&1 &
@@ -84,6 +151,15 @@ lg_wait_online() {
   return 1
 }
 
+lg_wait() {
+  local end=$((SECONDS + $2))
+  while [ $SECONDS -lt $end ]; do
+    [ "$(lg_get /stats "bool($1)")" = true ] && return 0
+    sleep 0.5
+  done
+  return 1
+}
+
 lg_wait_back() {
   local end=$((SECONDS + $2))
   while [ $SECONDS -lt $end ]; do
@@ -114,7 +190,13 @@ lg_stop() {
   kill -KILL "$LG_PID" 2>/dev/null
   wait "$LG_PID" 2>/dev/null
   LG_STATUS=$?
+  LG_STATUSES[$LG_INST]=$LG_STATUS
   LG_PID=""
 }
 
+lg_stop_all() {
+  local inst
+  for inst in "${LG_INSTANCES[@]}"; do lg_in "$inst" lg_stop; done
+}
+
 lg_json() { lg_py "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$1"; }
diff --git a/scripts/stack.sh b/scripts/stack.sh
index d793bbb..0a9487f 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -104,12 +104,12 @@ stack_wait() {
 # that hangs (a DROP rule) counts as busy instead of blocking for minutes.
 stack_port_free() { timeout 2 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$0"' "$1" 2>/dev/null; [ $? -eq 1 ]; }
 
-# stack_ports: every port stack_up listens on, and loadgen's control port
-# (base + 5, scripts/loadgen.sh): a leftover loadgen there would answer
-# lg_start's /stats in place of the new one.
+# stack_ports: every port stack_up listens on, and loadgen's control ports
+# (base + 5 … + 8, one per instance, scripts/loadgen.sh): a leftover loadgen
+# there would answer lg_start's /stats in place of the new one.
 stack_ports() {
   local base=${STACK_PORT_BASE:-17000} i
-  seq "$base" $((base + 5))
+  seq "$base" $((base + 8))
   for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
     echo $((base + 100 + 10 * i)) $((base + 101 + 10 * i)) $((base + 102 + 10 * i))
   done
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `make test-scripts`
Expected: `lib_test: all passed`, `loadgen_test: all passed`, `stack_test: all passed`. The prototype's first test run failed four checks, all faults in the test itself, fixed in the patch above:
- two compared a message carrying a newline;
- `a 5th instance` counted an instance named only in a subshell (Review Focus 4);
- `the flood stopped` ran `kill -0` on the emptied `LG_PIDS[flood]`, which is `kill -0 0`, which always succeeds.

The fake loadgen exits `$FAKE_EXIT` (3 for the flood, through `LG_ENV`), so main's and the flood's statuses differ. With `lg_in`'s `LG_STATUS` swap removed, the prototype failed exactly `LG_STATUS is main's after lg_in` (`got [[3]], want [[]]`) and `LG_STATUS is main's after lg_stop_all` (`got [3 3], want [0 3]`).

- [ ] **Step 5: Commit**

```bash
git add scripts/loadgen.sh scripts/stack.sh scripts/loadgen_test.sh scripts/stack_test.sh
git commit -m "feat: loadgen.sh instances: lg_in, lg_wait, lg_stop_all, control ports base+5..+8 (#120, P6a2bI-1)"
```

---

### Task 3: The `floods` phase in the load probe, and the record

**Files:**
- Modify: `scripts/load-probe.sh`, `README.md`, `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`, this plan's *Execution Notes*

**Interfaces:**
- Consumes: Task 1's modes and `"flood"` stats, Task 2's `lg_in`, `lg_wait`, `lg_stop_all` and `LG_STATUSES`, `stack_metric NAME METRIC MATCH`.
- Produces: `make test-load-probe` phase 2 `floods` (the SDK phase becomes 3).

- [ ] **Step 1: Add the phase**

```diff
diff --git a/scripts/load-probe.sh b/scripts/load-probe.sh
index 098d56e..b59d79d 100755
--- a/scripts/load-probe.sh
+++ b/scripts/load-probe.sh
@@ -5,7 +5,13 @@
 #
 #   1. light: 3,000 connections log in, Ping every 5s for 15s: every Ping
 #      answered, no connection lost
-#   2. sdk:   2,000 SDK clients log in; loadgen SIGKILLs gw-1 (POST /kill):
+#   2. floods (Plan 6b3a): beside 2,000 light clients, loadgen instance
+#      flood sends garbage handshakes at 2,000/s for 10s, then runs again
+#      holding 500 slow-handshake connections beside flood2's 500 slow-auth
+#      ones: the gateways fail and close every garbage handshake, close
+#      every slow connection by its 5s timeout (held p99 ≤ 6.5s), and the
+#      light clients keep every Ping answered
+#   3. sdk:   2,000 SDK clients log in; loadgen SIGKILLs gw-1 (POST /kill):
 #      99% of its clients log in again within 30s of T_death, and the
 #      killed gateway is the only process that did not stop cleanly
 #
@@ -26,7 +32,7 @@ ok() { echo "ok   $1"; }
 bad() { echo "FAIL $1"; fails=$((fails + 1)); }
 check() { if [ "$2" = true ]; then ok "$1"; else bad "$1: $3"; fi; }
 cleanup() {
-  lg_stop
+  lg_stop_all
   stack_down
   if [ "$fails" -eq 0 ] && [ "${PROBE_KEEP:-0}" != 1 ]; then rm -rf "$STACK_DIR"; else echo "probe: results in $STACK_DIR"; fi
 }
@@ -50,7 +56,55 @@ check "light: every Ping answered" "$(lg_json 'd["pong_missing"] == 0 and d["pon
 check "light: none lost while held" "$(lg_json 'd["lost"] == {}')" "$(lg_json 'd["lost"]')"
 echo "     light: login p99 $(lg_json 'd["login_latency"]["p99_ms"]')ms, Pong p99 $(lg_json 'd["pong_rtt"]["p99_ms"]')ms, failed $(lg_json 'd["failed"]')"
 
-# 2: sdk, and a gateway killed by loadgen.
+# 2: floods beside light clients, each flood on its own loadgen instance
+# from its own source addresses; instance flood runs garbage, then
+# slow-handshake (instances are named by role and reused, #120).
+# gw_sum METRIC MATCH: over both gateways; a labelled counter has no sample
+# before its first increment, which counts as 0.
+gw_sum() {
+  python3 -c 'import sys; print(sum(0 if x == "null" else int(float(x)) for x in sys.argv[1:]))' \
+    "$(stack_metric gw-0 "$1" "$2")" "$(stack_metric gw-1 "$1" "$2")"
+}
+lg_start floods-light -mode light -clients 2000 -rate 1000 -ping-every 5s -uid-prefix floods || { bad "floods: light loadgen did not start"; exit 1; }
+if lg_wait_online 2000 60; then ok "floods: 2000 light clients online"; else bad "floods: light clients not all online"; fi
+failed0=$(gw_sum goim_gateway_handshakes_total 'result="failed"')
+limited0=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_rate"')
+lg_in flood lg_start garbage -mode garbage -rate 2000 -clients 500 -source-net 1 || bad "garbage: loadgen did not start"
+sleep 10
+lg_in flood lg_stop
+check "garbage: loadgen exited 0" "$([ "${LG_STATUSES[flood]}" = 0 ] && echo true)" "status ${LG_STATUSES[flood]}"
+outcomes=$(lg_in flood lg_json 'd["flood"]["outcomes"]')
+closed=$(lg_in flood lg_json 'd["flood"]["outcomes"].get("closed", 0)')
+check "garbage: ≥ 15000 attempts, every one counted closed by a gateway" "$(lg_in flood lg_true "$STACK_DIR/artifacts/loadgen-garbage.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["outcomes"]["closed"] >= 15000')" "$outcomes"
+failed=$(($(gw_sum goim_gateway_handshakes_total 'result="failed"') - failed0))
+limited=$(($(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_rate"') - limited0))
+check "garbage: the gateways failed (or limited) as many handshakes" "$([ $((failed + limited)) -ge "$closed" ] && [ "$failed" -gt 0 ] && echo true)" "failed $failed + handshake_rate $limited, closed $closed"
+echo "     garbage: $(lg_in flood lg_json 'd["flood"]["attempts"]') attempts, $(lg_in flood lg_json 'd["flood"]["skipped"]') skipped, held p50 $(lg_in flood lg_json 'd["flood"]["held"]["p50_ms"]')ms p99 $(lg_in flood lg_json 'd["flood"]["held"]["p99_ms"]')ms; gateways: failed $failed, handshake_rate $limited"
+hst0=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_timeout"')
+aut0=$(gw_sum goim_gateway_admission_rejects_total 'reason="auth_timeout"')
+lg_in flood lg_start slowhs -mode slow-handshake -clients 500 -rate 500 -source-net 2 || bad "slow-handshake: loadgen did not start"
+lg_in flood2 lg_start slowauth -mode slow-auth -clients 500 -rate 500 -source-net 3 || bad "slow-auth: loadgen did not start"
+for inst in flood flood2; do
+  lg_in "$inst" lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= 500 and d["flood"]["open"] >= 450' 30 ||
+    bad "$inst: not 500 closed with 450 open again: $(lg_in "$inst" lg_get /stats 'd["flood"]')"
+done
+lg_in flood lg_stop
+lg_in flood2 lg_stop
+for inst in slowhs slowauth; do
+  check "$inst: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway" "$(lg_true "$STACK_DIR/artifacts/loadgen-$inst.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" \
+    "$(lg_py "$STACK_DIR/artifacts/loadgen-$inst.json" 'd["flood"]')"
+done
+echo "     slow: held p50/p99/max: slow-handshake $(lg_in flood lg_json '[d["flood"]["held"][k] for k in ("p50_ms", "p99_ms", "max_ms")]')ms, slow-auth $(lg_in flood2 lg_json '[d["flood"]["held"][k] for k in ("p50_ms", "p99_ms", "max_ms")]')ms"
+hst=$(($(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_timeout"') - hst0))
+aut=$(($(gw_sum goim_gateway_admission_rejects_total 'reason="auth_timeout"') - aut0))
+check "slow: the gateways closed them by their timeouts" "$([ "$hst" -ge 500 ] && [ "$aut" -ge 500 ] && echo true)" "handshake_timeout $hst, auth_timeout $aut"
+lg_stop
+check "floods: light loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
+check "floods: every light Ping answered through the floods" "$(lg_json 'd["pong_missing"] == 0 and d["pongs"] >= 2000 * 3 and d["lost"] == {}')" \
+  "pongs $(lg_json 'd["pongs"]'), missing $(lg_json 'd["pong_missing"]'), lost $(lg_json 'd["lost"]')"
+echo "     floods: light Pong p99 $(lg_json 'd["pong_rtt"]["p99_ms"]')ms, max $(lg_json 'd["pong_rtt"]["max_ms"]')ms"
+
+# 3: sdk, and a gateway killed by loadgen.
 lg_start sdk -mode sdk -clients 2000 -rate 500 -uid-prefix sdk || { bad "sdk: loadgen did not start"; exit 1; }
 if lg_wait_online 2000 60; then ok "sdk: 2000 online"; else bad "sdk: not all online"; fi
 reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - '"population %d" % d["population"]')
```

- [ ] **Step 2: Run the probe**

Run: `make test-load-probe 2>&1 | tail -30`
Expected: `load-probe: all passed`. Under the prototype (`eb2639c`, after the review) the phase printed:

```
ok   floods: 2000 light clients online
ok   garbage: loadgen exited 0
ok   garbage: ≥ 15000 attempts, every one counted closed by a gateway
ok   garbage: the gateways failed (or limited) as many handshakes
     garbage: 20009 attempts, 0 skipped, held p50 0ms p99 1ms; gateways: failed 20009, handshake_rate 0
ok   slowhs: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway
ok   slowauth: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway
     slow: held p50/p99/max: slow-handshake [5506, 5996, 6007]ms, slow-auth [5507, 5998, 6010]ms
ok   slow: the gateways closed them by their timeouts
ok   floods: light loadgen exited 0
ok   floods: every light Ping answered through the floods
     floods: light Pong p99 8ms, max 27ms
```

In the prototype's first two runs, two mistakes in the phase were found and fixed in the patch above:
- The gateways' baseline read `null`, because a labelled counter has no sample before its first increment. `gw_sum` counts it as 0.
- The Ping bound was `2000 × 4`, but the floods last ~18s at a 5s interval, so pongs came to 7,580. It is `2000 × 3` now.

Before the review, the slow holds' max reached 6,120ms against the old 6.5s max gate (O7). The light Pong p99 under the floods was 8ms in that run, and 24–52ms in earlier runs (phase 1: 6–9ms), with none missing. That is recorded only (`echo`), not gated: Tier 2's numbers are 6b3b's.

- [ ] **Step 3: The record**

- `README.md`, *What's in it*, the `cmd/loadgen` row: English `Load generator: light (raw connections) and SDK modes, garbage-handshake and slow-client flood modes, several instances per stack, histograms exact to 1 ms, kill/mark control API`; 中文 `压测工具：light（裸连接）和 SDK 两种模式，垃圾握手和慢客户端洪泛模式，同一栈上可运行多个实例，精确到 1 ms 的直方图，kill/mark 控制接口`. Recount the *Tests* row in both languages with `git ls-files '*_test.go' | xargs grep -c '^func Test' | awk -F: '{s += $2} END {print s}'` (the prototype counted 479; the README still says 467) and write that number.
- Roadmap: in plan 6's row, after the 6b3a entry written with this plan, record its review and execution as they happened, and "next 6b3b". #119, #120 and #121 are already listed as decided (the review, 2026-10-16); note there only that they are executed.
- This plan's *Execution Notes*: each task's commit, the test output, the mutation results, the probe's floods lines.

- [ ] **Step 4: Commit**

```bash
git add scripts/load-probe.sh README.md docs/superpowers/plans/
git commit -m "test: load probe floods phase: garbage, slow-handshake, slow-auth beside light clients (Plan 6b3a)"
```

## After this plan

- **6b3b** (scenario `floods`, Tier 2 run 30) builds on: `lg_in` with a light or SDK `main` and up to three floods; `"flood"` stats; `-source-net` per flood. It decides the rates and sizes. It names instances by role and reuses them across phases (#120). It gates the delivered garbage rate on the gateways' handshake counters, as the probe's `failed + limited ≥ closed` check does (Review Focus 2), sizes slow-auth against the bucket (Review Focus 5), and records the gateway's TIME_WAIT (Review Focus 3). It runs the spec's 20k/s storm clause without a flood (#121). The limiter path has an in-process test here (Review Focus 6); run 30 is its first test at scale.
- **6b3c** (`/reset`, `/network-change`, scenario `reconnect`) uses instances for its light heartbeat probe beside the SDK storm (P6a2bI-2). The 6b2a-pinger ruling applies there: an SDK Ping `Send` blocked on a full socket buffer holds back the Pong-timeout check. Heartbeats are checked from the light instance, but whether a blocked write matters at that scale should be checked, not assumed.

## Execution Notes

Executed inline 2026-10-16 (superpowers:executing-plans) on `feat/plan6b3` from `4e2dfa3`. Every block applied as written (`git apply --check` clean), with no ruling needed.

- **Task 1** (`6b661e9`). RED as expected: `internal/loadgen` failed to build (`undefined: garbageHandshake`, `ModeGarbage`, `unknown field SourceNet`, …), and `cmd/loadgen` failed its two new cases (`mode "slow-auth", want light or sdk`; `flag provided but not defined: -source-net`). GREEN: `gofmt` and `go vet` clean, `go test -race -count=3 ./internal/loadgen/` ok (106s for the three runs), `./cmd/loadgen/` ok. All 7 mutations failed their named test as the plan says: `key_id 8, want 7`; `timed out after 10s waiting for 100 garbage handshakes closed`; `garbage: 1 sources from 127.0.3.2`; `timed out after 5s waiting for 5 open again` (both modes); `after a timeout: 0s–0s, want spread over the tick`; `9649 attempts, want ≤ 140 (no spin)` plus `after an early close: 0s`; `Outcomes:map[handshake:59] …, want only pre-handshake outcomes`.
- **Task 2** (`8925e3a`). RED as expected: `loadgen_test: 9 failed` (`lg_in: command not found`, `a second start of a running instance is refused: got [0 ]`, `lg_wait: command not found`, …), and `stack_test` failed `ports: the 4th loadgen instance's control port missing`. `stack_test` printed `4 failed` for that one `FAIL` line: its subshell blocks `exit $fails` with the count they inherited, so a failure before them is counted again. That was already so before this plan and is harmless. GREEN: `make test-scripts` gave `lib_test`, `loadgen_test` and `stack_test: all passed`. The optional `GWLOSS_PHASES=b1` dry run (Review Focus 1) was not run.
- **Task 3** (this commit). `make test-load-probe`: `load-probe: all passed`. The floods phase printed:

```
ok   floods: 2000 light clients online
ok   garbage: loadgen exited 0
ok   garbage: ≥ 15000 attempts, every one counted closed by a gateway
ok   garbage: the gateways failed (or limited) as many handshakes
     garbage: 20013 attempts, 0 skipped, held p50 0ms p99 4ms; gateways: failed 20011, handshake_rate 0
ok   slowhs: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway
ok   slowauth: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway
     slow: held p50/p99/max: slow-handshake [5512, 5992, 6000]ms, slow-auth [5513, 5995, 6004]ms
ok   slow: the gateways closed them by their timeouts
ok   floods: light loadgen exited 0
ok   floods: every light Ping answered through the floods
     floods: light Pong p99 9ms, max 28ms
```

  The other phases: light login p99 180ms, Pong p99 10ms. SDK: gw-1's population of 1,000 was back from T_death at p50 10,359ms and p99 19,841ms. The README's test count was recounted to 479.

**Final review** (fresh Opus reviewer, 2026-10-16, `4e2dfa3..9942e3d`): no Critical or Important findings, ready to merge. All six Review Focus items hold as the plan expects. For item 1, every `lg_start` in `smoke`, `capacity`, `gwloss` and `slowlogic` is preceded by an `lg_stop` on every path. Six minors, all fixed on the user's request (2026-10-16, see below):
1. `awaitClose` registers its `context.AfterFunc` before `SetReadDeadline(from + Timeout)`. A stop that lands between the two calls has its deadline pushed back, so `Run` can return up to `Timeout` late. The slow-auth `Handshake` has a fixed deadline too. The result is written before then.
2. In garbage mode, a `Write` error that is not `IsClosed` (e.g. `ETIMEDOUT`) is counted in `attempts` but in no outcome. It could be counted as `error`.
3. `lg_wait` has no `kill -0` liveness check, unlike `lg_wait_online`. A crashed instance waits out the whole deadline and then shows as a `null` stat.
4. The plan's wording: `failed + {no_slot, no_token}` is the rate that reaches the limiter. The DH rate is `failed` alone. 6b3b should say which of the two it gates on.
5. A stale test comment, `flood_test.go`'s "then dials again at once", predates the O1 jitter.
6. If a gateway's metrics endpoint is unreadable, the probe's `gw_sum` causes a bash arithmetic error instead of a clean FAIL.

The reviewer's notes for 6b3b: register `t2_on_exit 'lg_stop_all'` rather than `'lg_stop'` in any scenario with flood instances; always pass `-source-net` to a flood instance, since nothing enforces it; raise loadgen's `-timeout` (10s) if the gateway's timeouts are set above ~9s.

**Minors fixed** (2026-10-16, on the user's request), each with a test that failed first except where noted:
1. `awaitClose` now sets its read deadline before it registers the stop hook. `slowOnce` closes the connection when the stop comes during slow-auth's `Handshake`. RED→GREEN: `TestSlowAuthHandshakeEndsAtTheStop` took 5.0s before the fix (`slowOnce after the stop took 5.001s`) and 0.2s after. Ruling: `TestAwaitCloseEndsAtAStopAlreadyMade` passed before the fix too. The race needs the AfterFunc goroutine to run between two adjacent calls, and 50 tries never hit it. It is kept as a guard for the stop semantics, and the reorder is verified by reading the code.
2. A garbage `Write` error that is not a close is counted as `error`. `garbageOnce` was split so that `garbageOn` writes and waits on a given connection. RED→GREEN: `TestGarbageWriteErrorIsCounted` (`outcomes map[], want one error`).
3. `lg_wait` fails at once, with `loadgen exited`, when the instance's process is gone. RED→GREEN: `wait: a stopped instance fails at once` (`got [1 0 0], want [1 1 1]`).
4. The plan's wording in Review Focus 2 is corrected: the rate delivered to the limiter is `failed + no_slot + no_token`, and the DH rate is `failed` alone.
5. The stale comment in `flood_test.go` now says the client redials within a tick.
6. `gw_sum` reads each gateway's metrics with `stack_metrics`. When a gateway cannot be read, it prints 0 and fails, and every call site counts a FAIL. Before, `stack_metric` printed `null` for a failed scrape, so an unreadable gateway silently counted as 0: the reviewer's "arithmetic error" was in fact a wrong number. Ruling: there is no unit test for `load-probe.sh`, so this was checked RED→GREEN with a stub script that sources the function with gw-1 unreadable (`rc=0 out=[5]`, then `rc=1 out=[0]`).

After the fixes: `go test -race -count=3 ./internal/loadgen/` and `./cmd/loadgen/` ok, `make test-scripts` all passed, `make test-load-probe` all passed (garbage 20,015 attempts, all failed by the gateways; slow holds p99 6,000ms each; light Pong p99 8ms), and `make test` ok. The README's test count is now 482.
