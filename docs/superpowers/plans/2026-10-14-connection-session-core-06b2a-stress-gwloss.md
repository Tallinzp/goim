# Plan 6b2a-stress: Killing a 600k Gateway (Tier 2 (b-stress)) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's load test (b-stress) on the Tier 2 host with SDK clients: SIGKILL a gateway holding 600k clients and measure the relogin storm on two empty survivors, the 600k `gwconns` sweep (`SSCAN` + `UNLINK`), Kick behaviour, lease renewals, Redis latency and the share of clients that reach backoff. b-stress has no SLO. The plan also measures R1-O7's dead-epoch cross-check during crash storms, in b1 and b-stress.

**Architecture:** `scripts/tier2/gwloss.sh` becomes a loop over phases (`GWLOSS_PHASES`, default `b1 b_stress`), each on a fresh stack, as `capacity.sh` does. b1 is unchanged apart from three new things: the cross-check, Redis latency and the backoff count. `b_stress` runs 3 gateways. Loadgen starts with `-down gw-1,gw-2`, ramps 600k SDK clients onto `gw-0` alone, sends `POST /up` for the other two, then SIGKILLs `gw-0`. Loadgen gets `-down`, `POST /up` (`Balancer.SetUp`) and a count of clients that reach backoff. `stack.sh` gets Redis per-command latency from `INFO latencystats`, and `stack_wait_idle` now also requires each running gateway's Logouts to settle (P6b1I-4). `loadgen.sh` gets `LG_ENV`, which runs loadgen with `GOMEMLIMIT`. Logic records the dead-epoch cross-check when a relogin's Kick is refused while the dead gateway's lease is still live (P6b2a-b1I-7, R1-O7 option (a)).

**Tech Stack:** Go 1.27.1, bash 5.2, the pinned `redis-server` 7.4.11 (`make redis-server`; `INFO latencystats` is Redis ≥ 7.0), curl, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Verification → Load tests* (b-stress: "kill a 600k gateway: measured, no SLO; includes a 600k-member `gwconns` `SSCAN` + `UNLINK`"; b1; "storm: the other gateways never miss a lease renewal"), *Observability* ("dead-epoch cross-check (R1-O7): when a relogin finds the old entry pointing at a dead epoch, record `login_ms − that epoch's last lease renewal`"), *Failure modes* ("Gateway process crash / pod kill"), *Client SDK rules* (backoff), *Verification → Test execution model* (*Automation contract*). Roadmap choices #69, #71, #79, #87, #93, #94 (amended 2026-10-14), #95–#97, and the interface reviews P6a2bI-2, P6b1I-4, P6b2a-b1I-7.

**Branch:** `feat/plan6b2a-stress`, from `main` (`74bdafc`, which holds every earlier plan, Plan 6b2a-b1 complete, and the README).

**Size:** 4 tasks, ~104KB. Above the ~80KB guideline because Task 4 gives `gwloss.sh` whole (it is rewritten as a loop over phases). The prototype has already run every block, so execution is copying and checking, and fits one session.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`), `make test-race-suite` and `make test-scripts` stay green on the dev machine (2 cores / 2GB), loopback only.
- Scripts: bash with `set -uo pipefail`; `scripts/tier2/lib.sh` writes JSON without jq; python3 only reads JSON (and does arithmetic on it). The host's awk is mawk: no gawk-only features.
- Ports (choice #76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`; loadgen's control API is `STACK_PORT_BASE + 5`.
- CPU sets (#71): gateways `4-9,16-21`, shared by every gateway of a phase (`GOMAXPROCS` = 12 / gateways by #79: 6 each in b1, 4 each in b_stress), Logic + Redis `10,11,22,23`, loadgen `0-3,12-15` (8 CPUs).
- Loadgen sources 127.0.0.2… (#69, amended): one per 5,000 connections per (source, gateway) pair. Sized for the busiest gateway, which is the victim alone in b_stress's ramp: 120 sources for 600k.
- SDK memory budget (#93): b1 runs 2 gateways × 200k SDK clients (loadgen peaked at 10.6–11.5GB in runs 20 and 21, ~26.4–28.7KB per client). b_stress runs 3 gateways, ramps 600k onto the victim alone, then `POST /up` for the others. Loadgen gets `GOMEMLIMIT` (16GiB by default). A check before the run refuses it when `MemAvailable` is below the phases' budget. `rss_peak_mb` is recorded (`VmHWM`), and the swap-in check (#70) catches real memory trouble.
- Spec numbers: lease TTL 30s, renewed every 10s; the sweeper starts on an epoch 2 × TTL after its last renewal and sweeps 10,000 uids/s; the dead-epoch cross-check is within ±10s (the renewal interval); Kick retries go up to 3, 10s apart, only while the target's lease is live; per-target Kick queue 1024, 64 workers, 1s deadline per attempt; the SDK backs off after 10 pre-handshake closes or 5 other retryable rejects in a row (2.5–5s, doubling to 5 min).
- Swap during a measured window fails it (#70); the user closes the desktop apps before the run.
- Comments and names match the surrounding code; new behaviour cites its choice number (#98–#101) or interface-review id.

## Review Focus

1. **A crash-storm relogin must be counted once by the cross-check, and twice only at the lease's expiry.** Login records it when the old epoch's lease is gone, and the Kicker records it when the first Kick is refused while the lease is still live. If both fire for one relogin, the histogram double counts and `cross_check`'s upper bound (1.1 per relogin) catches it, but only at scale. One double count is possible and accepted (review M3): Login queues the Kick before its own cross-check reads the lease, so a relogin whose Kicker read sees the lease live and whose Login read, milliseconds later, sees it expired is recorded by both. Only relogins inside that window around the TTL's end can be counted twice, far below the 1.1 bound. Test: `TestKickRefusedDeadLeaseNoSecondCrossCheck` (Task 1: lease gone → the refused Kick records nothing). Retries are not recorded: they come 10s after the relogin, and the code checks `j.attempt == 0`.
2. **`-down` must leave at least one gateway up, and must not name an unknown gateway.** A typo such as `gw-3` on a 3-gateway stack would otherwise ramp every client over all three gateways, and b_stress's victim would hold a third of the clients. Test: `TestNewSizesSourcesForTheGatewaysUp` (Task 2: `gw-9` and "every gateway" refused), `TestRunRefusesBadFlags` (`-down gw-9`). The scenario's `b_stress_online` also requires all 600k on `gw-0`.
3. **Sources sized for the ramp, not for the survivors.** With 2 of 3 gateways down, the default would give 60 sources (sized for 300k on each of two survivors), which puts 10,000 connections on each (source, victim) pair. That is 35% of the port range, where runs 14–16 slowed the ramp. Test: `TestNewSizesSourcesForTheGatewaysUp` (`gw-1,gw-2` down → 120).
4. **A gateway that dropped a Logout fails the phase boundary at once, not after its wait.** A dropped Logout never settles, so `stack_wait_idle` would otherwise burn its full wait (600s in b_stress). Test: `idle: a dropped Logout fails at once` (Task 3).
5. **A counter family with no sample yet reads 0 in `stack_wait_idle`, and a gateway that does not answer reads busy.** A fresh gateway has no `logouts_total` sample until its first Logout. Reading that as null would keep it busy forever, and reading a failed scrape as 0 would let it pass. Tests: `idle: no login and no Logout yet settle` and `idle: a silent gateway keeps it busy` / `idle: names the silent gateway` (Task 3). In `stack_wait_idle` the scrape's own failure is the "busy" signal (`no answer`).

## Implementation choices this plan makes

Proposed 2026-10-14 with this plan, for the user's review. R1-O7 option (a) was decided in the 6b2a-b1 interface review (P6b2a-b1I-7); #101 records how it is implemented.

98. **The `gwloss` scenario's `b_stress` phase** (#93). A fresh stack with 3 gateways sharing the gateway CPU set (4 `GOMAXPROCS` each, #79), each with `-max-conns 650000` (the victim's 600k plus clients in flight), and Go's default GC. Loadgen runs with `GOMEMLIMIT=16GiB` (`GWLOSS_STRESS_LG_MEMLIMIT`) and `-down gw-1,gw-2`. It ramps 600k SDK clients at 5,000/s (~120s, uid prefix `b_stress`) onto `gw-0` from 120 sources, then sends `POST /up` for `gw-1` and `gw-2`, then SIGKILLs `gw-0` (`/kill`, mark `death`). The storm window runs from the kill until the sweeper reports the dead epoch `done` (at most `GWLOSS_STRESS_SWEEP_WAIT`, 300s: 60s stale plus 60s for 600k uids, with room for a slow Redis) and every victim is back. Every victim must be back within `GWLOSS_STRESS_BACK_WAIT` (600s, amended in review (I3): a client refused at the survivors' handshake limit (2 concurrent handshakes each at 4 `GOMAXPROCS`) more than 10 times in a row steps through the SDK's doubling backoff, up to 5 min, so a slow tail can pass 300s while still converging). The sweep is watched in the background from the kill, alongside the relogins, so a storm slower than `GWLOSS_STRESS_SWEEP_WAIT` neither hides a finished sweep nor delays its `sweep_s` (review I1: in the proposal the sweep wait began after the relogin wait, with both at 300s). The survivors drain within 600s after loadgen stops. **Checks** (12): `b_stress_stack`; `b_stress_online` (all 600k on `gw-0`, and both `POST /up` calls answered); `b_stress_back` (every victim back within the wait, the survivors rejected none at capacity, and the kill succeeded; no SLO); `b_stress_kicks` and `b_stress_cross_check` (as b1's; in both phases the Kick lower bound is 0.9 × (relogins − `sweep_deleted`), amended in review (I2): a victim whose entry the sweep deleted before it came back replaces nothing and Kicks nothing); `b_stress_lease` (each survivor); `b_stress_swept`; `no_swap_in_b_stress`; `no_lo_drops_in_b_stress`; `b_stress_loadgen_headroom`; `b_stress_idle`; `b_stress_processes`. `pinning` and `artifacts` are checked once for the run. **Recorded:** the SLO distribution from the mark and from detection (`slo-death.json`, a record, not a verdict), the backoff count, Redis per-command latency for the ramp and the storm, the survivors' `handshake_rate` rejects, the per-process cores, RSS and peak RSS, loadgen's live heap per client at the ramp's end after a forced GC (`loadgen_heap`, `ramp/loadgen.heap.txt`; review M4, for #93's budget, which estimates it from RSS), Kick drops, sweep uids and deleted fields, and login latency. **Memory check:** `t2_mem_budget` 21GB when b_stress runs (loadgen's 17.2GB limit plus the victim ~1.4GB, the survivors ~0.7GB each and Redis ~0.4GB), 14GB for b1 alone. **`not_covered`:** per-shard Redis numbers (6c), and survivors that carry load of their own through the 600k storm, since they start empty (#93). The `gwloss` scenario becomes a loop over `GWLOSS_PHASES`, each phase on a fresh stack. A phase whose ramp fails stops that phase and goes on to the next. **Changed for b1:** a new `b1_cross_check` check (b1 has 12 checks of its own, plus `pinning` and `artifacts` for the run); `pinning` now names every gateway of its phase; the result's single `survivor` becomes a `survivors` map, in `b1_lease` and the `b1` field; `b1_idle` also requires the survivor's Logouts to settle (#100); and the `b1` field gains `redis_latency`, `backoff` and the survivor's `rejects_handshake_rate`. Rejected: running b_stress's survivors at a share of the load (#93 fixed the memory budget at one 600k population).
99. **Loadgen: `-down`, `POST /up`, the backoff count.** `Config.Down` (`-down gw-1,gw-2`) names gateways out of the balancer from the start, until `POST /up?gateway=`. A repeated id counts once (review M1). An unknown id is refused, and so is a list that covers every gateway. Default sources are sized for the busiest gateway: `min(N − 1, gateways up at the start)`, at least 1. `Runner.Up` and `Balancer.SetUp` put a gateway back. The SDK client counts `EventWaiting` with `Wait.Kind` `WaitBackoff` or `WaitFailure` as backoff (P6a2bI-2). `Counts` gains `backoff_clients`, each client counted once in the run, and `backoff_waits` by kind. `POST /up` is not refused for a killed gateway: a scenario that restarts one needs it, and the scenario owns the order.
100. **`stack_wait_idle` settles Logouts per running gateway** (P6b1I-4): from one scrape, `logouts_total{result="done"}` ≥ `logins_total{result="ok"}` and no `dropped`, besides 0 connections and 0 awaiting Logout. In a successful scrape a counter with no sample counts as 0. A dropped Logout fails at once. `capacity.sh`'s `logouts_settled` moves here (same rule, now on every running gateway). **Redis latency:** `stack_redis_resetstat` (`CONFIG RESETSTAT`) at a window's start, and `stack_redis_latency` at its end, which prints `{"CMD": {"calls", "p50_us", "p99_us", "p999_us"}}` from `INFO latencystats` and `commandstats`. Commands that Lua scripts run are listed too (`hset`, `sadd`, …, next to `evalsha`), and the readers' own `info` and `config` are left out. Redis CPU stays `stack_usage_json`'s `redis` cores. **`LG_ENV`**: `VAR=VALUE` entries for loadgen's environment (`env` in `lg_start`).
101. **R1-O7's cross-check on a refused Kick** (P6b2a-b1I-7, option (a)). When a Kick's **first** attempt is refused (#97) and the target's lease is still **live**, the Kicker observes `now − RenewedMs(target)` in `dead_epoch_relogin_seconds`. A lease already gone means Login's own cross-check recorded that relogin, so each counts once, apart from relogins at the lease's expiry (Review Focus 1). A retry comes 10s after its relogin and is not recorded. A first attempt dropped by a full queue or timed out records nothing. Both reads go through the `LeaseCache`, so a storm reads Redis about once per cache TTL. **New check in both phases:** `<phase>_cross_check`: samples ≥ 0.9 × (first attempts − first-attempt timeouts) and ≤ 1.1 × relogins, with the mean recorded. The time is the give-up's, not `login_ms`: a refused attempt fails within milliseconds of its dequeue, and the queue holds at most 1,024 attempts. Rejected: carrying `login_ms` through `Kick()`, which would change the `Kicker` interface for a few milliseconds of accuracy against a ±10s spec.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `internal/logic/kick.go` | modify | `Kicker.crossCheck`, called on a refused first attempt (#101) |
| `internal/logic/metrics.go` | modify | `dead_epoch_relogin_seconds` help text |
| `internal/logic/kick_test.go` | modify | two cross-check tests; `kickEnv.rdb` |
| `internal/loadgen/balancer.go` | modify | `Balancer.SetUp` (#99) |
| `internal/loadgen/runner.go` | modify | `Config.Down`, source sizing, `Runner.Up` |
| `internal/loadgen/control.go` | modify | `POST /up` |
| `internal/loadgen/recorder.go` | modify | `Recorder.Backoff`; `Counts.BackoffClients`, `.BackoffWaits` |
| `internal/loadgen/sdkclient.go` | modify | backoff waits → `Recorder.Backoff` |
| `cmd/loadgen/main.go` | modify | `-down` |
| `internal/loadgen/*_test.go`, `cmd/loadgen/main_test.go` | modify/create | tests for the above (`sdkclient_test.go` is new) |
| `scripts/stack.sh` | modify | `stack_wait_idle` settles Logouts; `stack_redis_cli`, `stack_redis_resetstat`, `stack_redis_latency` (#100) |
| `scripts/loadgen.sh` | modify | `LG_ENV` |
| `scripts/tier2/capacity.sh` | modify | `logouts_settled` removed (now in `stack_wait_idle`) |
| `scripts/stack_test.sh` | modify | tests for the above |
| `scripts/tier2/gwloss.sh` | rewrite | phases `b1` and `b_stress` (#98) |

Every code block below was run in a prototype (see *Execution Notes*). Patches apply with `git apply` from the repo root (save each to a file first, e.g. `git apply /tmp/t1-test.patch`), and new or rewritten files are given whole.

---

### Task 1: the dead-epoch cross-check on a refused Kick (#101)

**Files:**
- Modify: `internal/logic/kick.go` (`Kicker.attempt`, new `Kicker.crossCheck`), `internal/logic/metrics.go` (help text)
- Test: `internal/logic/kick_test.go`

**Interfaces:**
- Consumes: `LeaseCache.LiveOne(ctx, ref) (bool, error)`, `LeaseCache.RenewedMs(ctx, ref) (int64, bool, error)`, `Metrics.DeadEpochRelogin`; in tests `newKickEnv`, `newStore(t, cfg) (*store.Store, *redis.Client)`, `sampleCount`, `sampleSum`, `waitFor`, `counter`.
- Produces: `goim_logic_dead_epoch_relogin_seconds` gets one sample per refused first attempt while the target's lease is live. Task 4's `<phase>_cross_check` reads its `_count` and `_sum`.

- [ ] **Step 1: Write the failing tests**

Apply to `internal/logic/kick_test.go`:

```diff
diff --git a/internal/logic/kick_test.go b/internal/logic/kick_test.go
index 7d700a2..fff2a68 100644
--- a/internal/logic/kick_test.go
+++ b/internal/logic/kick_test.go
@@ -10,6 +10,7 @@ import (
 	pb "github.com/Tallinzp/goim/api/internalpb"
 	"github.com/Tallinzp/goim/internal/store"
 	"github.com/Tallinzp/goim/internal/testredis"
+	"github.com/redis/go-redis/v9"
 	"google.golang.org/grpc"
 	"google.golang.org/grpc/codes"
 	"google.golang.org/grpc/credentials/insecure"
@@ -98,6 +99,7 @@ func staticDial(addrs map[string]string) DialFunc {
 type kickEnv struct {
 	k   *Kicker
 	st  *store.Store
+	rdb *redis.Client
 	m   *Metrics
 	cfg KickConfig
 }
@@ -105,11 +107,11 @@ type kickEnv struct {
 func newKickEnv(t *testing.T, cfg KickConfig, addrs map[string]string) kickEnv {
 	t.Helper()
 	full := testConfig()
-	st, _ := newStore(t, full)
+	st, rdb := newStore(t, full)
 	m := newMetrics()
 	k := NewKicker(cfg, NewLeaseCache(st, 20*time.Millisecond, time.Now), m, staticDial(addrs))
 	t.Cleanup(k.Close)
-	return kickEnv{k: k, st: st, m: m, cfg: cfg}
+	return kickEnv{k: k, st: st, rdb: rdb, m: m, cfg: cfg}
 }
 
 func waitFor(t *testing.T, what string, cond func() bool) {
@@ -245,6 +247,60 @@ func TestKickRefusedDoesNotRetry(t *testing.T) {
 	}
 }
 
+// R1-O7's dead-epoch cross-check during a crash storm (P6b2a-b1I-7): a
+// crashed gateway's lease stays live for up to its TTL, so the relogins
+// that land meanwhile are not seen by Login's cross-check. Their first Kick
+// is refused, and that give-up records now − the epoch's last renewal.
+func TestKickRefusedRecordsDeadEpochCrossCheck(t *testing.T) {
+	lis, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	addr := lis.Addr().String()
+	lis.Close() // nothing listens: connects are refused
+	gw := testredis.Gateway()
+	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
+	ref := store.EpochRef{Gateway: gw, Epoch: 13}
+	res, err := env.st.RenewLease(ctx, ref)
+	if err != nil {
+		t.Fatal(err)
+	}
+	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
+	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
+	h := env.m.DeadEpochRelogin
+	if n := sampleCount(t, h); n != 1 {
+		t.Fatalf("cross-check samples %d, want 1", n)
+	}
+	if s := sampleSum(t, h); s < 0 || s > float64(time.Now().UnixMilli()-res.RedisNowMs)/1000+0.01 {
+		t.Fatalf("cross-check %vs", s)
+	}
+}
+
+// A refused Kick to an epoch whose lease is already gone records nothing:
+// Login's cross-check has recorded that relogin, and each relogin counts once.
+func TestKickRefusedDeadLeaseNoSecondCrossCheck(t *testing.T) {
+	lis, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	addr := lis.Addr().String()
+	lis.Close()
+	gw := testredis.Gateway()
+	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
+	ref := store.EpochRef{Gateway: gw, Epoch: 14}
+	if _, err := env.st.RenewLease(ctx, ref); err != nil { // its last renewal stays in gwepochs
+		t.Fatal(err)
+	}
+	if _, err := env.rdb.Del(ctx, "gwlease:"+gw+":{14}").Result(); err != nil {
+		t.Fatal(err)
+	}
+	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
+	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
+	if n := sampleCount(t, env.m.DeadEpochRelogin); n != 0 {
+		t.Fatalf("cross-check samples %d for an expired lease, want 0", n)
+	}
+}
+
 // #97 amended (Tier 2 run 20): a SIGKILLed gateway's Kick listener lives on
 // in the kernel while its sockets are torn down, then closes with Logic's
 // connect still queued: the connection is reset before the server's
```

- [ ] **Step 2: Run them to verify the first fails**

Run: `go test -count=1 -run 'CrossCheck' ./internal/logic/`
Expected: FAIL: `TestKickRefusedRecordsDeadEpochCrossCheck` with `cross-check samples 0, want 1`. `TestKickRefusedDeadLeaseNoSecondCrossCheck` already passes: it pins that the new code does not record a relogin that Login has already counted (Review Focus 1).

- [ ] **Step 3: Implement**

Apply:

```diff
diff --git a/internal/logic/kick.go b/internal/logic/kick.go
index 0319e25..1b9be95 100644
--- a/internal/logic/kick.go
+++ b/internal/logic/kick.go
@@ -195,6 +195,9 @@ func (k *Kicker) attempt(t *kickTarget, j kickJob) {
 		// for up to a TTL after a crash, so the give-up counts as dead: a
 		// crash must not raise the live give-up alert once per relogin.
 		k.m.KickGiveUps.WithLabelValues("dead").Inc()
+		if j.attempt == 0 {
+			k.crossCheck(t.ref)
+		}
 		return
 	}
 	if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || j.attempt >= k.cfg.Retries {
@@ -210,6 +213,28 @@ func (k *Kicker) attempt(t *kickTarget, j kickJob) {
 	time.AfterFunc(k.cfg.RetrySpacing, func() { k.enqueue(t.ref, j) })
 }
 
+// crossCheck records R1-O7's dead-epoch cross-check for a first attempt
+// refused while the target's lease is still live: now − the epoch's last
+// renewal, the time from gateway death to relogin within the renewal
+// interval (P6b2a-b1I-7). A crashed gateway's lease stays live for up to its
+// TTL, so Login's cross-check, which needs the lease gone, misses the
+// relogins that land meanwhile, nearly all of a crash storm's. A lease
+// already gone means Login recorded that relogin, so each counts once. A
+// retry comes RetrySpacing after the relogin and is not recorded. It may
+// read Redis: call it from a worker only.
+func (k *Kicker) crossCheck(ref store.EpochRef) {
+	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
+	defer cancel()
+	if live, err := k.leases.LiveOne(ctx, ref); err != nil || !live {
+		return
+	}
+	renewed, ok, err := k.leases.RenewedMs(ctx, ref)
+	if err != nil || !ok || renewed == 0 {
+		return
+	}
+	k.m.DeadEpochRelogin.Observe(float64(time.Now().UnixMilli()-renewed) / 1000)
+}
+
 // logErr logs each distinct error once per target (at most
 // maxLoggedKickErrs): a storm fails thousands of attempts the same way, and
 // the metrics carry no error text.
diff --git a/internal/logic/metrics.go b/internal/logic/metrics.go
index 35bd25b..b11379c 100644
--- a/internal/logic/metrics.go
+++ b/internal/logic/metrics.go
@@ -13,7 +13,7 @@ type Metrics struct {
 	Logins           *prometheus.CounterVec // result
 	ConnSeqRegressed prometheus.Counter
 	Undecodable      *prometheus.CounterVec // op: stored entries that did not decode (PI-3)
-	DeadEpochRelogin prometheus.Histogram   // login_ms − dead epoch's last renewal (R1-O7)
+	DeadEpochRelogin prometheus.Histogram   // login_ms (or a refused Kick) − dead epoch's last renewal (R1-O7)
 	StoreSeconds     *prometheus.HistogramVec
 	RPCSeconds       *prometheus.HistogramVec // method, code
 	Logouts          *prometheus.CounterVec   // result
@@ -89,7 +89,7 @@ func NewMetrics(reg prometheus.Registerer) *Metrics {
 		Logins:           cv("logins_total", "Logins by result.", "result"),
 		ConnSeqRegressed: c("conn_seq_regressed_total", "Logins accepted as a device counter regression."),
 		Undecodable:      cv("undecodable_entries_total", "Stored sess values that did not decode, by operation.", "op"),
-		DeadEpochRelogin: h("dead_epoch_relogin_seconds", "Relogin login_ms minus the dead epoch's last lease renewal.",
+		DeadEpochRelogin: h("dead_epoch_relogin_seconds", "Relogin login_ms, or the give-up of its refused Kick while the lease was live, minus the dead epoch's last lease renewal.",
 			prometheus.LinearBuckets(0, 5, 24)),
 		StoreSeconds:     hv("store_seconds", "Session store call latency.", lat, "op"),
 		RPCSeconds:       hv("rpc_seconds", "gRPC handler latency.", lat, "method", "code"),
```

- [ ] **Step 4: Run the tests**

Run: `gofmt -l internal/logic; go test -race -count=1 ./internal/logic/`
Expected: no gofmt output, `ok`. The other Kick tests are unchanged: `crossCheck` runs only after a refused give-up.

- [ ] **Step 5: Commit**

```bash
git add internal/logic/kick.go internal/logic/kick_test.go internal/logic/metrics.go
git commit -m "feat(logic): a refused Kick with a live lease records the dead-epoch cross-check (P6b2a-b1I-7)"
```

---

### Task 2: loadgen `-down`, `POST /up` and the backoff count (#99)

**Files:**
- Modify: `internal/loadgen/balancer.go`, `internal/loadgen/runner.go`, `internal/loadgen/control.go`, `internal/loadgen/recorder.go`, `internal/loadgen/sdkclient.go`, `cmd/loadgen/main.go`
- Test: `internal/loadgen/balancer_test.go`, `internal/loadgen/runner_test.go`, `internal/loadgen/recorder_test.go`, `internal/loadgen/e2e_test.go`, `cmd/loadgen/main_test.go`; create `internal/loadgen/sdkclient_test.go`

**Interfaces:**
- Consumes: `Env.Gateway(id string) int`, `sdk.Event`, `sdk.WaitKind` (`WaitBackoff`, `WaitFailure`, `String()`).
- Produces: `Balancer.SetUp(gw int)`; `Config.Down []string`; `Runner.Up(gw int)`; `POST /up?gateway=<id>` → `{"gateway": "<id>"}` (400 for an unknown id); `Recorder.Backoff(i int, kind string)`; `Counts.BackoffClients` (`backoff_clients`), `Counts.BackoffWaits` (`backoff_waits`); `cmd/loadgen -down gw-1,gw-2`. Task 4 uses `-down`, `POST /up` and `/stats`' `backoff_clients`/`backoff_waits`.

- [ ] **Step 1: Write the failing tests**

Apply:

```diff
diff --git a/cmd/loadgen/main_test.go b/cmd/loadgen/main_test.go
index 3b5cd80..347edc4 100644
--- a/cmd/loadgen/main_test.go
+++ b/cmd/loadgen/main_test.go
@@ -24,7 +24,13 @@ func TestRunRefusesBadFlags(t *testing.T) {
 		{nil, "-env is required"},
 		{[]string{"-env", "/nonexistent"}, "no such file"},
 		{[]string{"-nope"}, "flag provided but not defined"},
+		{[]string{"-env", "ENV", "-down", "gw-9"}, "down"},
 	} {
+		for i, a := range tc.args {
+			if a == "ENV" {
+				tc.args[i] = writeEnv(t, "127.0.0.1:1")
+			}
+		}
 		err := run(context.Background(), tc.args, &bytes.Buffer{})
 		if err == nil || !strings.Contains(err.Error(), tc.want) {
 			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
@@ -32,6 +38,20 @@ func TestRunRefusesBadFlags(t *testing.T) {
 	}
 }
 
+// writeEnv writes a stack.env for one gateway gw-0 at addr and returns its path.
+func writeEnv(t *testing.T, addr string) string {
+	t.Helper()
+	dir := t.TempDir()
+	k, _ := auth.GenerateKey("k1")
+	doc, _ := auth.MarshalPrivateJWK(k)
+	noise, _ := wire.GenerateKey() // a zero key fails the handshake before any byte is sent
+	os.WriteFile(filepath.Join(dir, "jwt.jwk"), doc, 0o600)
+	env := "GATEWAY_ADDRS=" + addr + "\nGATEWAY_IDS=gw-0\nNOISE_KEY_ID=1\nNOISE_PUBLIC=" +
+		base64.StdEncoding.EncodeToString(noise.Public) + "\nJWT_KEY=" + filepath.Join(dir, "jwt.jwk") + "\n"
+	os.WriteFile(filepath.Join(dir, "stack.env"), []byte(env), 0o644)
+	return filepath.Join(dir, "stack.env")
+}
+
 // A run against a "gateway" that closes every connection stops at
 // -duration and writes a result counting the failed attempts.
 func TestRunWritesTheResultAtTheEnd(t *testing.T) {
@@ -50,16 +70,9 @@ func TestRunWritesTheResultAtTheEnd(t *testing.T) {
 			c.Close()
 		}
 	}()
-	dir := t.TempDir()
-	k, _ := auth.GenerateKey("k1")
-	doc, _ := auth.MarshalPrivateJWK(k)
-	noise, _ := wire.GenerateKey() // a zero key fails the handshake before any byte is sent
-	os.WriteFile(filepath.Join(dir, "jwt.jwk"), doc, 0o600)
-	env := "GATEWAY_ADDRS=" + l.Addr().String() + "\nGATEWAY_IDS=gw-0\nNOISE_KEY_ID=1\nNOISE_PUBLIC=" +
-		base64.StdEncoding.EncodeToString(noise.Public) + "\nJWT_KEY=" + filepath.Join(dir, "jwt.jwk") + "\n"
-	os.WriteFile(filepath.Join(dir, "stack.env"), []byte(env), 0o644)
-	out := filepath.Join(dir, "result.json")
-	err = run(context.Background(), []string{"-env", filepath.Join(dir, "stack.env"), "-clients", "3", "-rate", "100",
+	envPath := writeEnv(t, l.Addr().String())
+	out := filepath.Join(filepath.Dir(envPath), "result.json")
+	err = run(context.Background(), []string{"-env", envPath, "-clients", "3", "-rate", "100",
 		"-duration", "1s", "-control", "127.0.0.1:0", "-out", out}, &bytes.Buffer{})
 	if err != nil {
 		t.Fatal(err)
diff --git a/internal/loadgen/balancer_test.go b/internal/loadgen/balancer_test.go
index 877e20a..c0326ea 100644
--- a/internal/loadgen/balancer_test.go
+++ b/internal/loadgen/balancer_test.go
@@ -84,6 +84,27 @@ func TestBalancerSkipsDownGateways(t *testing.T) {
 	}
 }
 
+// SetUp puts a gateway back in rotation (b_stress: the victim ramps alone,
+// then the others come up, #93); least connections then fills it first.
+func TestBalancerSetUpReturnsAGateway(t *testing.T) {
+	b, _ := NewBalancer(listeners(t, 2), 1)
+	b.SetDown(1)
+	for range 3 {
+		if _, gw, err := b.Dial(context.Background()); err != nil || gw != 0 {
+			t.Fatalf("dial while gw-1 is down: gw %d %v, want gw 0", gw, err)
+		}
+	}
+	b.SetUp(1)
+	if b.Down(1) {
+		t.Fatal("gw-1 still down after SetUp")
+	}
+	for range 3 {
+		if _, gw, err := b.Dial(context.Background()); err != nil || gw != 1 {
+			t.Fatalf("dial after SetUp: gw %d %v, want gw 1", gw, err)
+		}
+	}
+}
+
 func TestBalancerFailedDialFreesTheSlot(t *testing.T) {
 	addrs := listeners(t, 1)
 	l, _ := net.Listen("tcp", "127.0.0.1:0")
diff --git a/internal/loadgen/e2e_test.go b/internal/loadgen/e2e_test.go
index e43e1e4..0551079 100644
--- a/internal/loadgen/e2e_test.go
+++ b/internal/loadgen/e2e_test.go
@@ -193,6 +193,27 @@ func TestSDKStormAfterAGatewayStops(t *testing.T) {
 	}
 }
 
+// b_stress's ramp (#93): with gw-1 down from the start every client logs in
+// on gw-0; after /up the next dials go to gw-1.
+func TestRampAloneThenUp(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, err := New(Config{Mode: "light", Clients: 4, Rate: 100, UIDPrefix: "alone", PingEvery: time.Second,
+		Timeout: 5 * time.Second, Down: []string{env.GatewayIDs[1]}}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	stop := run(r)
+	defer stop()
+	waitFor(t, "4 online", 10*time.Second, func() bool { return r.Stats().Online == 4 })
+	if st := r.Stats(); st.OnlineByGateway[env.GatewayIDs[0]] != 4 {
+		t.Fatalf("by gateway %v, want all 4 on gw-0", st.OnlineByGateway)
+	}
+	r.Up(1)
+	if _, gw, err := r.bal.Dial(context.Background()); err != nil || gw != 1 {
+		t.Fatalf("dial after Up: gw %d %v, want gw 1", gw, err)
+	}
+}
+
 // closeFirst forwards to addr, but closes its first n connections once
 // their handshake arrives, before any reply, as a gateway's handshake limit
 // does.
@@ -312,6 +333,17 @@ func TestControlAPI(t *testing.T) {
 	if !r.bal.Down(1) {
 		t.Fatal("killed gateway still in the balancer")
 	}
+	// /up puts it back (a restarted gateway, or b_stress's survivors).
+	if code, m := post(t, srv.URL+"/up?gateway="+env.GatewayIDs[1]); code != http.StatusOK || m["gateway"] != env.GatewayIDs[1] {
+		t.Fatalf("/up: %d %v", code, m)
+	}
+	if r.bal.Down(1) {
+		t.Fatal("/up left the gateway down")
+	}
+	if code, _ := post(t, srv.URL+"/up?gateway=nope"); code != http.StatusBadRequest {
+		t.Fatalf("/up unknown gateway: %d", code)
+	}
+	r.bal.SetDown(1)
 	// A taken mark name: no signal, gw-0 stays in the balancer.
 	other := exec.Command("sleep", "30")
 	if err := other.Start(); err != nil {
diff --git a/internal/loadgen/recorder_test.go b/internal/loadgen/recorder_test.go
index fd958e4..0a80bfd 100644
--- a/internal/loadgen/recorder_test.go
+++ b/internal/loadgen/recorder_test.go
@@ -141,3 +141,15 @@ func TestRecorderSLOReportsReassignedMembers(t *testing.T) {
 		t.Fatalf("slo b %+v", s)
 	}
 }
+
+// Clients that reach backoff (P6a2bI-2): each client once, every wait by kind.
+func TestRecorderCountsBackoffClients(t *testing.T) {
+	r := NewRecorder(3, []string{"gw-0"})
+	r.Backoff(0, "backoff")
+	r.Backoff(0, "backoff")
+	r.Backoff(1, "failure")
+	c := r.Counts()
+	if c.BackoffClients != 2 || c.BackoffWaits["backoff"] != 2 || c.BackoffWaits["failure"] != 1 || len(c.BackoffWaits) != 2 {
+		t.Fatalf("backoff: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
+	}
+}
diff --git a/internal/loadgen/runner_test.go b/internal/loadgen/runner_test.go
index bec36bd..06dccd4 100644
--- a/internal/loadgen/runner_test.go
+++ b/internal/loadgen/runner_test.go
@@ -1,6 +1,9 @@
 package loadgen
 
-import "testing"
+import (
+	"strings"
+	"testing"
+)
 
 // The default source count holds a gateway's share after another gateway
 // is killed: /kill moves its clients onto N−1 gateways (6a2b minor 1).
@@ -28,3 +31,42 @@ func TestNewSizesSourcesForOneGatewayLess(t *testing.T) {
 		}
 	}
 }
+
+// With gateways down at the start (b_stress's victim ramps alone, #93), the
+// default sources hold every client on the gateways that are up: 600k on
+// one gateway needs 120, not the 60 sized for two survivors.
+func TestNewSizesSourcesForTheGatewaysUp(t *testing.T) {
+	env := Env{GatewayAddrs: []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}, GatewayIDs: []string{"gw-0", "gw-1", "gw-2"}}
+	for _, tc := range []struct {
+		down []string
+		want int
+	}{
+		{nil, 60},                       // 300,000 on each of 2 after a kill
+		{[]string{"gw-1", "gw-2"}, 120}, // 600,000 on gw-0 alone
+		{[]string{"gw-2"}, 60},          // 300,000 on each of 2
+		{[]string{"gw-2", "gw-2"}, 60},  // a repeated id is one gateway
+	} {
+		r, err := New(Config{Mode: "sdk", Clients: 600_000, Rate: 1, UIDPrefix: "x", Down: tc.down}, env)
+		if err != nil {
+			t.Fatal(err)
+		}
+		if r.cfg.Sources != tc.want {
+			t.Errorf("down %v: %d sources, want %d", tc.down, r.cfg.Sources, tc.want)
+		}
+		for i, id := range env.GatewayIDs {
+			want := false
+			for _, d := range tc.down {
+				want = want || d == id
+			}
+			if r.bal.Down(i) != want {
+				t.Errorf("down %v: %s down %v", tc.down, id, r.bal.Down(i))
+			}
+		}
+	}
+	for _, down := range [][]string{{"gw-9"}, {"gw-0", "gw-1", "gw-2"}} {
+		_, err := New(Config{Mode: "sdk", Clients: 1, Rate: 1, UIDPrefix: "x", Down: down}, env)
+		if err == nil || !strings.Contains(err.Error(), "down") {
+			t.Errorf("down %v: err %v", down, err)
+		}
+	}
+}
diff --git a/internal/loadgen/sdkclient_test.go b/internal/loadgen/sdkclient_test.go
new file mode 100644
index 0000000..ef3ad8f
--- /dev/null
+++ b/internal/loadgen/sdkclient_test.go
@@ -0,0 +1,24 @@
+package loadgen
+
+import (
+	"testing"
+
+	"github.com/Tallinzp/goim/internal/sdk"
+)
+
+// An SDK client's backoff and failure waits count as backoff; the quick
+// retries before them do not (P6a2bI-2).
+func TestSDKClientCountsBackoffWaits(t *testing.T) {
+	r, err := New(Config{Mode: "sdk", Clients: 2, Rate: 1, UIDPrefix: "x"}, Env{GatewayAddrs: []string{"127.0.0.1:1"}, GatewayIDs: []string{"gw-0"}})
+	if err != nil {
+		t.Fatal(err)
+	}
+	sc := &sdkClient{r: r, i: 1}
+	for _, k := range []sdk.WaitKind{sdk.WaitPreHandshake, sdk.WaitRetryable, sdk.WaitStorm, sdk.WaitBackoff, sdk.WaitFailure} {
+		sc.event(sdk.Event{Kind: sdk.EventWaiting, Wait: sdk.Wait{Kind: k}, End: sdk.EndFailed})
+	}
+	c := r.Stats().Counts
+	if c.BackoffClients != 1 || c.BackoffWaits["backoff"] != 1 || c.BackoffWaits["failure"] != 1 || len(c.BackoffWaits) != 2 {
+		t.Fatalf("backoff: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
+	}
+}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go vet ./internal/loadgen/ ./cmd/loadgen/`
Expected: FAIL to compile: `b.SetUp undefined (type *Balancer has no field or method SetUp)` (and, once that is fixed, `Down`, `Up`, `Backoff`, `BackoffClients`).

- [ ] **Step 3: Implement**

Apply:

```diff
diff --git a/cmd/loadgen/main.go b/cmd/loadgen/main.go
index 9ca1994..1af7a19 100644
--- a/cmd/loadgen/main.go
+++ b/cmd/loadgen/main.go
@@ -2,7 +2,8 @@
 // scripts/stack.sh's stack.env, starts -clients at -rate through a
 // least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
 // and serves the control API on -control (GET /stats, POST /kill, POST /mark,
-// GET /slo; choice #84) until SIGTERM, Ctrl-C or -duration. It then writes
+// POST /up, GET /slo; choice #84) until SIGTERM, Ctrl-C or -duration.
+// -down keeps gateways out of the balancer until POST /up (#93). It then writes
 // the result, the stats at that moment and every mark's SLO against
 // -within, as JSON to -out, and only then stops every client: stopping 200k
 // SDK clients can outlast the caller's wait, and the result must survive a
@@ -24,6 +25,7 @@ import (
 	"net/http"
 	"os"
 	"os/signal"
+	"strings"
 	"syscall"
 	"time"
 
@@ -51,6 +53,7 @@ func run(ctx context.Context, args []string, stderr io.Writer) error {
 	fs.IntVar(&cfg.Sources, "sources", 0, "source addresses 127.0.0.2…; 0: one per 5,000 connections per gateway")
 	fs.DurationVar(&cfg.PingEvery, "ping-every", 150*time.Second, "light: Ping interval (the gateway's -heartbeat)")
 	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "light: dial + handshake + Login per attempt")
+	down := fs.String("down", "", "gateway ids out of the balancer until POST /up, comma-separated")
 	control := fs.String("control", "", "control API listen address (none if empty)")
 	duration := fs.Duration("duration", 0, "stop after this long (0: at SIGTERM)")
 	within := fs.Duration("within", 30*time.Second, "SLO bound for the result's marks")
@@ -61,6 +64,9 @@ func run(ctx context.Context, args []string, stderr io.Writer) error {
 	if *envPath == "" {
 		return errors.New("-env is required")
 	}
+	if *down != "" {
+		cfg.Down = strings.Split(*down, ",")
+	}
 	env, err := loadgen.LoadEnv(*envPath)
 	if err != nil {
 		return err
@@ -88,7 +94,7 @@ func run(ctx context.Context, args []string, stderr io.Writer) error {
 		defer srv.Close()
 		slog.Info("loadgen: control API", "addr", l.Addr().String())
 	}
-	slog.Info("loadgen: starting", "mode", cfg.Mode, "clients", cfg.Clients, "rate", cfg.Rate, "gateways", len(env.GatewayAddrs))
+	slog.Info("loadgen: starting", "mode", cfg.Mode, "clients", cfg.Clients, "rate", cfg.Rate, "gateways", len(env.GatewayAddrs), "down", cfg.Down)
 	done := make(chan struct{})
 	go func() { r.Run(runCtx); close(done) }()
 	<-ctx.Done()
diff --git a/internal/loadgen/balancer.go b/internal/loadgen/balancer.go
index a074f9f..7619063 100644
--- a/internal/loadgen/balancer.go
+++ b/internal/loadgen/balancer.go
@@ -58,14 +58,22 @@ func NewBalancer(addrs []string, sources int) (*Balancer, error) {
 		next: make([]int, len(addrs))}, nil
 }
 
-// SetDown takes gateway gw out of rotation for new dials. It is permanent:
-// a scenario that restarts a gateway adds a SetUp (P6a2aI-1).
+// SetDown takes gateway gw out of rotation for new dials, until a SetUp.
 func (b *Balancer) SetDown(gw int) {
 	b.mu.Lock()
 	b.down[gw] = true
 	b.mu.Unlock()
 }
 
+// SetUp puts gateway gw back in rotation (P6a2aI-1): b_stress's survivors
+// after the victim's ramp (#93), or a restarted gateway. Least connections
+// then sends it the next dials until it catches up.
+func (b *Balancer) SetUp(gw int) {
+	b.mu.Lock()
+	b.down[gw] = false
+	b.mu.Unlock()
+}
+
 // Down reports whether gateway gw is out of rotation.
 func (b *Balancer) Down(gw int) bool {
 	b.mu.Lock()
diff --git a/internal/loadgen/control.go b/internal/loadgen/control.go
index f3df4aa..82decd4 100644
--- a/internal/loadgen/control.go
+++ b/internal/loadgen/control.go
@@ -32,6 +32,7 @@ func (r *Runner) Result(within time.Duration) Result {
 //	POST /kill?gateway=gw-1&pid=N&mark=death  SIGKILL and mark T_death  {"mark", "population"}
 //	                                          409: pid gone or mark taken, nothing changed; 500: ErrKillAfterMark
 //	POST /mark?gateway=gw-1|all&mark=drop     a mark; &down=1 also takes the gateway out of the balancer
+//	POST /up?gateway=gw-1                     the gateway is back in the balancer  {"gateway"}
 //	GET  /slo?mark=death&within=30s           SLO
 //	/debug/pprof/                             loadgen's own profiles
 func (r *Runner) Handler() http.Handler {
@@ -70,6 +71,15 @@ func (r *Runner) Handler() http.Handler {
 		}
 		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n})
 	})
+	mux.HandleFunc("POST /up", func(w http.ResponseWriter, q *http.Request) {
+		gw := r.env.Gateway(q.FormValue("gateway"))
+		if gw < 0 {
+			http.Error(w, "want gateway=<id>", http.StatusBadRequest)
+			return
+		}
+		r.Up(gw)
+		writeJSON(w, map[string]any{"gateway": q.FormValue("gateway")})
+	})
 	mux.HandleFunc("GET /slo", func(w http.ResponseWriter, q *http.Request) {
 		within, err := time.ParseDuration(q.FormValue("within"))
 		if err != nil || within <= 0 {
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index 889d679..72974b4 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -13,25 +13,30 @@ import (
 // below what a mutex serves; callers pass the times in, so a wait for the
 // lock never shows in a sample (P6a2aI-4).
 type Recorder struct {
-	mu      sync.Mutex
-	ids     []string
-	clients []clientState
-	online  []int
-	logins  int64
-	login   *hist
-	lost    map[string]int64
-	failed  map[string]int64
-	pongs   int64
-	missing int64
-	gapMax  time.Duration
-	pongRTT *hist
-	marks   []*mark // by index
-	byName  map[string]*mark
+	mu             sync.Mutex
+	ids            []string
+	clients        []clientState
+	online         []int
+	logins         int64
+	login          *hist
+	lost           map[string]int64
+	failed         map[string]int64
+	backoff        map[string]int64 // waits by kind
+	backoffClients int64
+	pongs          int64
+	missing        int64
+	gapMax         time.Duration
+	pongRTT        *hist
+	marks          []*mark // by index
+	byName         map[string]*mark
 }
 
 // clientState: gw is the gateway it is logged in on (-1: none); mark and
-// pos place it in its latest mark (-1: none).
-type clientState struct{ gw, mark, pos int32 }
+// pos place it in its latest mark (-1: none); backoff: it has reached backoff.
+type clientState struct {
+	gw, mark, pos int32
+	backoff       bool
+}
 
 // mark is a named instant and the clients logged in on its gateway then
 // (the population of an SLO), with when each lost its connection and
@@ -48,7 +53,7 @@ type mark struct {
 // NewRecorder records clients 0..n-1 over gateways ids.
 func NewRecorder(n int, ids []string) *Recorder {
 	r := &Recorder{ids: ids, clients: make([]clientState, n), online: make([]int, len(ids)),
-		login: newHist(), pongRTT: newHist(), lost: map[string]int64{}, failed: map[string]int64{}, byName: map[string]*mark{}}
+		login: newHist(), pongRTT: newHist(), lost: map[string]int64{}, failed: map[string]int64{}, backoff: map[string]int64{}, byName: map[string]*mark{}}
 	for i := range r.clients {
 		r.clients[i] = clientState{gw: -1, mark: -1}
 	}
@@ -103,6 +108,18 @@ func (r *Recorder) Failed(why string) {
 	r.mu.Unlock()
 }
 
+// Backoff: client i's next attempt waits by exponential backoff, of kind
+// (an sdk.WaitKind name: backoff or failure; P6a2bI-2).
+func (r *Recorder) Backoff(i int, kind string) {
+	r.mu.Lock()
+	defer r.mu.Unlock()
+	r.backoff[kind]++
+	if c := &r.clients[i]; !c.backoff {
+		c.backoff = true
+		r.backoffClients++
+	}
+}
+
 // Pong: a Ping was answered after rtt.
 func (r *Recorder) Pong(rtt time.Duration) {
 	r.mu.Lock()
@@ -226,6 +243,8 @@ type Counts struct {
 	Logins          int64            `json:"logins"`
 	Lost            map[string]int64 `json:"lost"`
 	Failed          map[string]int64 `json:"failed"`
+	BackoffClients  int64            `json:"backoff_clients"` // clients that reached backoff, each once
+	BackoffWaits    map[string]int64 `json:"backoff_waits"`   // backoff waits by kind
 	LoginLatency    Dist             `json:"login_latency"`
 	Pongs           int64            `json:"pongs"`
 	PongMissing     int64            `json:"pong_missing"`
@@ -238,6 +257,7 @@ func (r *Recorder) Counts() Counts {
 	r.mu.Lock()
 	defer r.mu.Unlock()
 	c := Counts{OnlineByGateway: map[string]int{}, Logins: r.logins, Lost: map[string]int64{}, Failed: map[string]int64{},
+		BackoffClients: r.backoffClients, BackoffWaits: map[string]int64{},
 		LoginLatency: r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(), PongRTT: r.pongRTT.dist(r.pongRTT.n)}
 	for i, n := range r.online {
 		c.Online += n
@@ -249,5 +269,8 @@ func (r *Recorder) Counts() Counts {
 	for k, v := range r.failed {
 		c.Failed[k] = v
 	}
+	for k, v := range r.backoff {
+		c.BackoffWaits[k] = v
+	}
 	return c
 }
diff --git a/internal/loadgen/runner.go b/internal/loadgen/runner.go
index 82e78d1..590674b 100644
--- a/internal/loadgen/runner.go
+++ b/internal/loadgen/runner.go
@@ -5,6 +5,7 @@ import (
 	"errors"
 	"fmt"
 	"runtime"
+	"slices"
 	"strconv"
 	"sync"
 	"sync/atomic"
@@ -32,7 +33,8 @@ type Config struct {
 	Clients   int           // client i logs in as uid <UIDPrefix>-<i>
 	Rate      float64       // clients started per second
 	UIDPrefix string        // distinct per loadgen run, so runs never replace each other's sessions
-	Sources   int           // source addresses; 0: one per 5,000 connections per gateway, with one gateway gone
+	Sources   int           // source addresses; 0: one per 5,000 connections per gateway, with one gateway gone or Down
+	Down      []string      // gateway ids out of the balancer from the start, until POST /up (b_stress, #93)
 	PingEvery time.Duration // light: the Ping interval (cmd/loadgen: 150s, the gateway's default heartbeat)
 	Timeout   time.Duration // light: dial + handshake + Login (cmd/loadgen: 10s)
 	windows   sdk.Windows   // tests only: the SDK's built-in reconnect windows
@@ -76,8 +78,23 @@ func New(cfg Config, env Env) (*Runner, error) {
 	case cfg.Mode == "light" && (cfg.PingEvery <= 0 || cfg.Timeout <= 0):
 		return nil, errors.New("loadgen: light mode needs PingEvery and Timeout")
 	}
+	var down []int
+	for _, id := range cfg.Down {
+		gw := env.Gateway(id)
+		if gw < 0 {
+			return nil, fmt.Errorf("loadgen: down: no gateway %q", id)
+		}
+		if !slices.Contains(down, gw) { // a repeated id is one gateway
+			down = append(down, gw)
+		}
+	}
+	if len(down) >= len(env.GatewayAddrs) {
+		return nil, errors.New("loadgen: down: every gateway")
+	}
 	if cfg.Sources == 0 {
-		gws := max(1, len(env.GatewayAddrs)-1)
+		// Sized for the busiest gateway: one gateway gone, or every client on
+		// the gateways up at the start.
+		gws := min(max(1, len(env.GatewayAddrs)-1), len(env.GatewayAddrs)-len(down))
 		perGW := (cfg.Clients + gws - 1) / gws
 		cfg.Sources = min(maxSources, (perGW+connsPerSource-1)/connsPerSource)
 	}
@@ -85,6 +102,9 @@ func New(cfg Config, env Env) (*Runner, error) {
 	if err != nil {
 		return nil, err
 	}
+	for _, gw := range down {
+		bal.SetDown(gw)
+	}
 	r := &Runner{cfg: cfg, env: env, rec: NewRecorder(cfg.Clients, env.GatewayIDs), bal: bal}
 	if cfg.Mode == "light" {
 		r.pinger = newPingers(runtime.GOMAXPROCS(0), cfg.PingEvery, r.rec)
@@ -209,6 +229,9 @@ func (r *Runner) Mark(gw int, name string, down bool) (int, error) {
 	return n, err
 }
 
+// Up is POST /up: gateway gw is back in the balancer.
+func (r *Runner) Up(gw int) { r.bal.SetUp(gw) }
+
 // Stats is the run's state now.
 func (r *Runner) Stats() Stats {
 	s := Stats{Mode: r.cfg.Mode, Clients: r.cfg.Clients, Sources: r.cfg.Sources, Started: r.started.Load(),
diff --git a/internal/loadgen/sdkclient.go b/internal/loadgen/sdkclient.go
index f08e438..62acd57 100644
--- a/internal/loadgen/sdkclient.go
+++ b/internal/loadgen/sdkclient.go
@@ -38,6 +38,9 @@ func (r *Runner) newSDK(i int) (*sdk.Client, error) {
 // event runs on the client's run loop: it must not block.
 func (sc *sdkClient) event(e sdk.Event) {
 	now := time.Now()
+	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
+		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
+	}
 	switch e.Kind {
 	case sdk.EventConnecting:
 		sc.start = now
```

- [ ] **Step 4: Run the tests**

Run: `gofmt -l internal/loadgen cmd/loadgen; go test -race -count=1 ./internal/loadgen/ ./cmd/loadgen/`
Expected: no gofmt output, both `ok`. `TestNewSizesSourcesForOneGatewayLess` is unchanged: with nothing down, the sizing is `max(1, N − 1)` as before.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen cmd/loadgen
git commit -m "feat(loadgen): -down, POST /up and the backoff count (#93, P6a2bI-2)"
```

---

### Task 3: Logouts settle per gateway, Redis latency, `LG_ENV` (#100)

**Files:**
- Modify: `scripts/stack.sh` (`stack_wait_idle`, `stack_snapshot`; new `stack_redis_cli`, `stack_redis_resetstat`, `stack_redis_latency`), `scripts/loadgen.sh` (`LG_ENV`), `scripts/tier2/capacity.sh` (`logouts_settled` removed)
- Test: `scripts/stack_test.sh`

**Interfaces:**
- Consumes: `stack_metrics`, `stack_metric_in`, `stack_admins`; Task 2's loadgen (built by `stack_build`).
- Produces: `stack_wait_idle SECONDS` (now also one Logout done per login and none dropped, per running gateway; fails at once on a dropped Logout); `stack_redis_resetstat`; `stack_redis_latency` → `{"CMD": {"calls", "p50_us", "p99_us", "p999_us"}}` or `null`; `LG_ENV=(VAR=VALUE…)` read by `lg_start`. Task 4 uses all of them.

- [ ] **Step 1: Write the failing tests**

Apply to `scripts/stack_test.sh`:

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index c8da1b1..6b41ace 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -7,6 +7,7 @@ set -uo pipefail
 cd "$(git rev-parse --show-toplevel)"
 . scripts/tier2/lib.sh
 . scripts/stack.sh
+. scripts/loadgen.sh
 STACK_DIR=$(mktemp -d)
 trap 'stack_down; rm -rf "$STACK_DIR"' EXIT
 fails=0
@@ -50,6 +51,37 @@ else
   echo "skip metric: bytes past 2^31 (no busybox)"
 fi
 
+# stack_wait_idle also waits for each running gateway's Logouts to settle
+# (P6b1I-4): one done per login, none dropped, read from one scrape. A fake
+# gw-0 /metrics, before the real stack.
+(
+  mkdir -p "$STACK_DIR/fake-idle"
+  fake() { printf '%s\n' goim_gateway_connections\ 0 goim_gateway_awaiting_logout\ 0 "$@" >"$STACK_DIR/fake-idle/metrics"; }
+  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 4'
+  python3 -m http.server --bind 127.0.0.1 -d "$STACK_DIR/fake-idle" $((STACK_PORT_BASE + 102)) >/dev/null 2>&1 &
+  srv=$!
+  trap 'kill $srv' EXIT
+  STACK_GATEWAYS=1
+  declare -A STACK_PIDS=([gw-0]=$srv)
+  for i in $(seq 50); do curl -sf -o /dev/null "http://127.0.0.1:$((STACK_PORT_BASE + 102))/metrics" && break; sleep 0.1; done
+  out=$(stack_wait_idle 1 2>&1) && bad "idle: a Logout short must keep it busy" || ok "idle: a Logout short keeps it busy"
+  case $out in *"gw-0: connections 0, awaiting_logout 0, logouts done 4 of 5 logins"*) ok "idle: names the Logouts" ;; *) bad "idle: Logouts not named: [$out]" ;; esac
+  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 6'
+  stack_wait_idle 2 && ok "idle: more Logouts than logins settle (retried logins)" || bad "idle: done 6 of 5 logins"
+  fake
+  stack_wait_idle 2 && ok "idle: no login and no Logout yet settle" || bad "idle: a fresh gateway"
+  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 4' 'goim_gateway_logouts_total{result="dropped"} 1'
+  start=$SECONDS
+  out=$(stack_wait_idle 10 2>&1) && bad "idle: a dropped Logout must fail" || ok "idle: a dropped Logout fails"
+  [ $((SECONDS - start)) -lt 3 ] && ok "idle: a dropped Logout fails at once" || bad "idle: a dropped Logout waited $((SECONDS - start))s"
+  case $out in *"dropped 1"*) ok "idle: names the dropped Logout" ;; *) bad "idle: dropped not named: [$out]" ;; esac
+  # A gateway that does not answer is busy, never idle.
+  kill $srv; wait $srv 2>/dev/null
+  out=$(stack_wait_idle 1 2>&1) && bad "idle: a silent gateway must keep it busy" || ok "idle: a silent gateway keeps it busy"
+  case $out in *"gw-0: no answer"*) ok "idle: names the silent gateway" ;; *) bad "idle: silent gateway not named: [$out]" ;; esac
+  exit $fails
+) || fails=$((fails + $?))
+
 # stack_term records what wait reports, "killed" only for a process our
 # SIGKILL ended (M1): a kill -KILL that lands on an exited, unreaped process
 # (stubbed here) must not turn a clean exit 0 into "killed".
@@ -165,6 +197,23 @@ contains "idle: names the gateway" "$out" "gw-0: connections 1"
 wait $holder
 stack_wait_idle 10 && ok "idle: after the socket closed" || bad "idle: after the socket closed"
 
+# Loadgen runs with LG_ENV (GOMEMLIMIT, #93); its logins settle on the
+# gateways after it stops (P6b1I-4); Redis's per-command latency since a
+# reset covers the run's commands and none of the readers' own.
+stack_redis_resetstat && ok "redis: resetstat" || bad "redis: resetstat"
+LG_ENV=(GOMEMLIMIT=1GiB)
+STACK_PIN=0 # loadgen's CPU set is the Tier 2 host's
+lg_start t -mode light -clients 4 -rate 100 -uid-prefix st && lg_wait_online 4 20 && ok "loadgen: 4 online" || bad "loadgen: not online"
+STACK_PIN=1
+expect "loadgen: runs with LG_ENV" "$(tr '\0' '\n' <"/proc/$LG_PID/environ" | grep '^GOMEMLIMIT=')" GOMEMLIMIT=1GiB
+lat=$(stack_redis_latency)
+expect "redis: latency of the logins' commands" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(any(v["calls"] > 0 and v["p99_us"] > 0 for v in d.values()), "info" in d, any(k.startswith("config") for k in d))' "$lat")" "True False False"
+expect "redis: no answer is null" "$(STACK_PORT_BASE=1; stack_redis_latency)" null
+lg_stop
+stack_wait_idle 30 && ok "idle: the logins' Logouts settle" || bad "idle: after loadgen"
+expect "idle: one Logout done per login" "$(stack_metric gw-0 goim_gateway_logouts_total 'result="done"')" "$(stack_metric gw-0 goim_gateway_logins_total 'result="ok"')"
+LG_ENV=()
+
 # stack_usage: CPU and RSS per process over a window, extras included; a
 # process gone since is null.
 sleep 30 & extra=$!
```

- [ ] **Step 2: Run them to verify they fail**

Run: `make redis-server >/dev/null && STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_test.sh 2>&1 | grep -E '^(FAIL|stack_test)'`
Expected: `stack_test: 9 failed`: `idle: a Logout short must keep it busy`, `idle: Logouts not named`, `idle: a dropped Logout must fail`, `idle: dropped not named`, `idle: silent gateway not named` (the old code says `connections null`), `redis: resetstat`, `loadgen: runs with LG_ENV`, `redis: latency of the logins' commands`, `redis: no answer is null`. The `idle: … settle` tests and `idle: a silent gateway keeps it busy` already pass: the old rule ignored Logouts and read a silent gateway as busy too.

- [ ] **Step 3: Implement**

Apply:

```diff
diff --git a/scripts/loadgen.sh b/scripts/loadgen.sh
index decaadb..186c528 100644
--- a/scripts/loadgen.sh
+++ b/scripts/loadgen.sh
@@ -2,7 +2,8 @@
 # loadgen.sh: run cmd/loadgen against a scripts/stack.sh stack (Plan 6a2).
 # Source it after stack.sh; stack_build has built bin/loadgen. Then:
 #
-#   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env
+#   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env,
+#                               #   with LG_ENV (VAR=VALUE..., e.g. GOMEMLIMIT=16GiB, #93) in its environment
 #   lg_wait_online N SECONDS    # until the ramp is done and N clients are logged in
 #   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
 #   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
@@ -25,13 +26,14 @@
 LG_PID=""
 LG_NAME=""
 LG_STATUS=""
+LG_ENV=()
 
 lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5))"; }
 
 lg_start() {
   LG_NAME=$1
   shift
-  local cmd=("$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
+  local cmd=(env "${LG_ENV[@]}" "$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
     -out "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$@")
   if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "$T2_CPUS_LOADGEN" "${cmd[@]}"); fi
   mkdir -p "$STACK_DIR/artifacts" "$STACK_DIR/logs"
diff --git a/scripts/stack.sh b/scripts/stack.sh
index 402ddc6..ecba9e6 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -13,11 +13,14 @@
 #   stack_expect_dead NAME      # NAME was killed on purpose (loadgen /kill): reap and record it
 #   stack_metric NAME METRIC [MATCH]  # sum of METRIC's samples (with MATCH in the line) on NAME's /metrics
 #   stack_metrics NAME          # NAME's /metrics once; stack_metric_in BODY METRIC [MATCH] reads it
-#   stack_wait_idle SECONDS     # phase boundary: every gateway at 0 connections and 0 awaiting Logout
+#   stack_wait_idle SECONDS     # phase boundary: every gateway at 0 connections, 0 awaiting Logout,
+#                               #   its Logouts settled (one done per login, none dropped)
 #   stack_usage_start [N=PID..] # start a CPU window over the stack's processes (and N=PID extras)
 #   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb", "rss_peak_mb"}...} since the start
 #   stack_counters NAME KEY=METRIC[:MATCH]...  # {"KEY": sum, ...} from one scrape of NAME (null: no answer)
 #   stack_counters_delta A B    # {"KEY": B - A, ...} of two stack_counters readings
+#   stack_redis_resetstat       # Redis CONFIG RESETSTAT: a latency window starts
+#   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer)
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -228,14 +231,44 @@ stack_admins() {
 # gateway killed on purpose (stack_expect_dead) is skipped.
 stack_snapshot() {
   local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} name admin
-  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
   mkdir -p "$dir"
   while read -r name admin; do
     curl -sf "http://$admin/metrics" >"$dir/$name.metrics"
     curl -sf "http://$admin/debug/pprof/heap" >"$dir/$name.heap.pb.gz"
     curl -sf "http://$admin/debug/pprof/goroutine?debug=1" >"$dir/$name.goroutine.txt"
   done < <(stack_admins)
-  "$(dirname "$redis")/redis-cli" -p "$base" info all >"$dir/redis.info" 2>&1
+  stack_redis_cli info all >"$dir/redis.info" 2>&1
+}
+
+# stack_redis_cli ARGS...: redis-cli against the stack's Redis.
+stack_redis_cli() {
+  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
+  "$(dirname "$redis")/redis-cli" -p "${STACK_PORT_BASE:-17000}" "$@"
+}
+
+# stack_redis_resetstat: CONFIG RESETSTAT, so Redis's per-command latency
+# percentiles and call counts cover a window from now (Plan 6b2a-stress).
+stack_redis_resetstat() { [ "$(stack_redis_cli config resetstat 2>/dev/null)" = OK ]; }
+
+# stack_redis_latency: Redis's own per-command latency since the last
+# resetstat, from INFO latencystats (p50, p99 and p99.9 in µs, Redis 7's
+# defaults) and the calls from INFO commandstats, as {"CMD": {"calls",
+# "p50_us", "p99_us", "p999_us"}, ...}: the single Redis's p99 for the
+# spec's per-shard numbers (R4M-9), which need a Cluster (6c). The readers'
+# own INFO and CONFIG are left out. Prints null if Redis does not answer.
+stack_redis_latency() {
+  local info
+  info=$(stack_redis_cli info latencystats 2>/dev/null && stack_redis_cli info commandstats 2>/dev/null) &&
+    grep -q '^# Latencystats' <<<"$info" || { echo null; return; }
+  awk -F'[:,=]' '{ sub(/\r$/, "") }
+    /^latency_percentiles_usec_/ { c = substr($1, 26); p50[c] = $3; p99[c] = $5; p999[c] = $7; cmd[++n] = c }
+    /^cmdstat_/ { calls[substr($1, 9)] = $3 }
+    END { out = ""
+          for (i = 1; i <= n; i++) {
+            c = cmd[i]
+            if (c == "info" || index(c, "config") == 1) continue
+            out = out (out == "" ? "" : ", ") sprintf("\"%s\": {\"calls\": %d, \"p50_us\": %s, \"p99_us\": %s, \"p999_us\": %s}", c, calls[c], p50[c], p99[c], p999[c]) }
+          print "{" out "}" }' <<<"$info"
 }
 
 # stack_profile NAME SECONDS: a CPU profile of Logic and every running
@@ -283,19 +316,38 @@ stack_metric_in() {
 }
 
 # stack_wait_idle SECONDS: the phase boundary on a shared stack (P6a2bI-3):
-# waits until every running gateway has no open connection and no closed
-# one still awaiting its fenced Logout, so a stopped loadgen's closes and
-# Logouts do not spill into the next phase. Fails, naming what is left, after
-# SECONDS. A gateway that does not answer counts as busy.
+# waits until every running gateway has no open connection, no closed one
+# still awaiting its fenced Logout, and its Logouts settled: at least one
+# done per login, none dropped (P6b1I-4). A client that retried after a
+# timeout has two sessions, and a Login with an unknown outcome gets one
+# too, so done can exceed the logins. So a stopped loadgen's closes and
+# Logouts do not spill into the next phase. Each gateway is read from one
+# scrape, where a counter with no sample yet is 0; a gateway that does not
+# answer counts as busy. Only running gateways count: a killed one's
+# sessions end by EpochEnded, not Logouts, and it is gone from stack_admins
+# (stack_expect_dead). Fails, naming what is left, after SECONDS, or at once
+# on a dropped Logout, which never settles.
 stack_wait_idle() {
-  local end=$((SECONDS + $1)) name admin busy c a
+  local end=$((SECONDS + $1)) name admin busy body c a done logins dropped
   while :; do
     busy=""
     while read -r name admin; do
       [[ $name == gw-* ]] || continue
-      c=$(stack_metric "$name" goim_gateway_connections)
-      a=$(stack_metric "$name" goim_gateway_awaiting_logout)
-      [ "$c" = 0 ] && [ "$a" = 0 ] || busy+=" $name: connections $c, awaiting_logout $a;"
+      body=$(stack_metrics "$name") || { busy+=" $name: no answer;"; continue; }
+      c=$(stack_metric_in "$body" goim_gateway_connections)
+      a=$(stack_metric_in "$body" goim_gateway_awaiting_logout)
+      done=$(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"')
+      logins=$(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"')
+      dropped=$(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"')
+      [ "$done" = null ] && done=0
+      [ "$logins" = null ] && logins=0
+      [ "$dropped" = null ] && dropped=0
+      if [ "$dropped" != 0 ]; then
+        echo "stack: $name dropped $dropped Logouts: they never settle (logouts done $done of $logins logins, dropped $dropped)" >&2
+        return 1
+      fi
+      [ "$c" = 0 ] && [ "$a" = 0 ] && [ "$done" -ge "$logins" ] ||
+        busy+=" $name: connections $c, awaiting_logout $a, logouts done $done of $logins logins;"
     done < <(stack_admins)
     [ -z "$busy" ] && return 0
     if [ $SECONDS -ge $end ]; then
diff --git a/scripts/tier2/capacity.sh b/scripts/tier2/capacity.sh
index 6ed8768..6b6cdf5 100755
--- a/scripts/tier2/capacity.sh
+++ b/scripts/tier2/capacity.sh
@@ -62,6 +62,7 @@
 #                      awaiting Logout within 300s; then, from one scrape, a
 #                      Logout done for every Login that succeeded (more for
 #                      Logins with an unknown outcome) and none dropped
+#                      (in stack_wait_idle since 6b2a-stress, P6b1I-4)
 #   P_reconcile_end    only with CAPACITY_RECONCILE_CYCLE set: after the window,
 #                      a ReconcileEnd listing at least every client succeeds
 #                      (4.8MB at 600k, over gRPC's 4MB default; P6b1I-1) and
@@ -113,15 +114,6 @@ gc_mark_cpu_pct() {
 }
 
 # usage_ok JSON: loadgen used under 3/4 of its CPU set.
-# logouts_settled DONE DROPPED LOGINS_OK, read from one scrape: every
-# session that logged in got its fenced Logout (a client that retried after
-# a timeout has two sessions, and a Login with an unknown outcome gets one
-# too, so DONE can exceed LOGINS_OK), and none was dropped. DONE and
-# LOGINS_OK must be read; an absent DROPPED in the same scrape means none.
-logouts_settled() {
-  [ "$1" != null ] && [ "$3" != null ] && [ "$1" -ge "$3" ] && [ "$2" = null -o "$2" = 0 ] && echo true || echo false
-}
-
 # lo_dropped: packets loopback dropped on input since boot (host-wide).
 lo_dropped() { cat /sys/class/net/lo/statistics/rx_dropped 2>/dev/null || echo null; }
 
@@ -302,7 +294,6 @@ print(round(w["gw-0"]["cpu_s"] / p * 1e6, 1) if p > 0 and w.get("gw-0") else "nu
   dropped=$(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"')
   logins_ok=$(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"')
   heartbeat_closes=$(stack_metric_in "$body" goim_gateway_closes_total 'reason="heartbeat"')
-  [ "$(logouts_settled "$done_logouts" "$dropped" "$logins_ok")" = true ] || idle=false
   t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"done_logouts\": $done_logouts, \"logins_ok\": $logins_ok, \"dropped_logouts\": $dropped, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
   stack_down
   problems=$(stack_health)
```

- [ ] **Step 4: Run the tests, and check that they catch a broken rule**

Run: `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_test.sh 2>&1 | grep -E '^(FAIL|stack_test)'; scripts/loadgen_test.sh | tail -1; bash -n scripts/tier2/capacity.sh && echo ok`
Expected: `stack_test: all passed`, `loadgen_test: all passed`, `ok`.

Then a mutant: drop the Logout comparison and check that the tests catch it.

```bash
cp scripts/stack.sh /tmp/stack.sh.bak
sed -i 's|\[ "\$c" = 0 \] && \[ "\$a" = 0 \] && \[ "\$done" -ge "\$logins" \] \|\||[ "$c" = 0 ] \&\& [ "$a" = 0 ] \|\||' scripts/stack.sh
grep -c '"$done" -ge' scripts/stack.sh   # 0: the mutant is in
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_test.sh 2>&1 | grep -E '^(FAIL|stack_test)'
cp /tmp/stack.sh.bak scripts/stack.sh
```

Expected: `FAIL idle: a Logout short must keep it busy`, `FAIL idle: Logouts not named: []`, `stack_test: 2 failed`. Then `git diff --stat` shows the Step 3 change only.

- [ ] **Step 5: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh scripts/loadgen.sh scripts/tier2/capacity.sh
git commit -m "feat(scripts): stack_wait_idle settles Logouts per gateway, Redis latency readers, LG_ENV (P6b1I-4, #93)"
```

---

### Task 4: the `b_stress` phase of `gwloss`, and Tier 2 run 22 (#98)

**Files:**
- Rewrite: `scripts/tier2/gwloss.sh` (`Makefile` already lists `gwloss`)

**Interfaces:**
- Consumes: Task 1's cross-check samples; Task 2's `-down`, `POST /up`, `backoff_clients`/`backoff_waits`; Task 3's `stack_wait_idle`, `stack_redis_resetstat`, `stack_redis_latency`, `LG_ENV`; the existing `stack_counters`, `stack_counters_delta`, `stack_usage_*`, `t2_*`, `lg_*`.
- Produces: `result.json` with 26 checks when both phases run: b1 12 (`b1_cross_check` new), b_stress 12, and `pinning` and `artifacts` once; fields `b1` and `b_stress`.

- [ ] **Step 1: Replace `scripts/tier2/gwloss.sh` with:**

```bash
#!/usr/bin/env bash
# Tier 2: gwloss (Plans 6b2a-b1 and 6b2a-stress), the spec's load tests (b1)
# and (b-stress) with SDK clients, each phase on a fresh stack:
#   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
#        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
#        about one per relogin, with no retries (R2-O6);
#   (b-stress) kill a 600k gateway: measured, no SLO; includes a 600k-member
#        gwconns SSCAN + UNLINK (the dead epoch's sweep; R4M-9);
#   storm: the other gateways never miss a lease renewal.
# The gateways share the gateway CPU set (GOMAXPROCS = its CPUs / gateways,
# #79), with Go's default GC (6b1). Logic is one process throughout
# (minReplicas, no HPA), so each storm runs at Logic's minimum.
#   b1: 2 gateways, 2 x 200k SDK clients up from the start (#93): the
#       survivor gw-0 carries its own 200k through the storm and ends with
#       400k.
#   b_stress: 3 gateways. Loadgen starts with gw-1 and gw-2 out of its
#       balancer, ramps 600k SDK clients onto gw-0 alone, puts the others
#       back (POST /up), then SIGKILLs gw-0: the survivors start empty and
#       take the 600k relogins (#93). Loadgen runs with GOMEMLIMIT.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss
#   Results: tier2-results/gwloss-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/gwloss-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 25 minutes (b1 ~8, b_stress ~17; up to ~40 if b_stress's relogins
#   use their whole wait).
#
# Settings:
#   GWLOSS_PHASES      phases to run, in order (default "b1 b_stress")
#   GWLOSS_CLIENTS     b1: SDK clients over both gateways (default 400000)
#   GWLOSS_RATE        b1: clients started per second (default 5000)
#   GWLOSS_CAP         b1: each gateway's -max-conns (default 450000: the
#                      survivor's 400k plus room for clients in flight)
#   GWLOSS_SWEEP_WAIT  b1: seconds after the kill to wait for the dead epoch's
#                      sweep (default 180: stale 60s after its last renewal,
#                      then 200k uids at the sweeper's 10k/s)
#   GWLOSS_SOURCES     b1: loadgen's source addresses (default: loadgen's, one
#                      per 5,000 connections per gateway, sized for the survivor)
#   GWLOSS_STRESS_CLIENTS  b_stress: SDK clients, all on the victim (default 600000)
#   GWLOSS_STRESS_RATE     b_stress: clients started per second (default 5000)
#   GWLOSS_STRESS_CAP      b_stress: each gateway's -max-conns (default 650000:
#                          the victim's 600k plus room for clients in flight)
#   GWLOSS_STRESS_SWEEP_WAIT  b_stress: as GWLOSS_SWEEP_WAIT (default 300: 60s
#                          stale, then 600k uids at 10k/s, and slower relogins)
#   GWLOSS_STRESS_BACK_WAIT   b_stress: seconds after the kill to wait for every
#                          victim to log in again (default 600: the SDK's backoff
#                          doubles to 5 min, and a client refused at the
#                          survivors' handshake limit keeps stepping up)
#   GWLOSS_STRESS_SOURCES  b_stress: loadgen's source addresses (default:
#                          loadgen's, sized for the victim alone: 120 for 600k)
#   GWLOSS_STRESS_LG_MEMLIMIT b_stress: loadgen's GOMEMLIMIT (default 16GiB, #93)
#
# Checks (b1; b_stress has the same with its own prefix, unless noted):
#   b1_stack           redis, devtoken, Logic and the gateways become Ready
#   pinning            (first phase) gateways, Logic + Redis and loadgen on their CPU sets
#   b1_online          every client logs in, each gateway holds 45-55% of
#                      them; b_stress: every client on gw-0, then gw-1 and
#                      gw-2 are back in loadgen's balancer
#   b1_slo             (b1 only) loadgen SIGKILLs gw-1: 99% of its clients log
#                      in again within 30s of T_death (R4M-17), and the
#                      survivor rejected none at capacity (SDK clients retry a
#                      reject without counting it, P6b2a-b1I-2)
#   b_stress_back      (b_stress only) loadgen SIGKILLs gw-0: every one of its
#                      clients logs in again within GWLOSS_STRESS_BACK_WAIT, and
#                      the survivors rejected none at capacity; no SLO
#   b1_kicks           Kick first attempts in the storm, sent or dropped by a
#                      full queue (R5S-O2), are 0.9-1.1 per relogin (at least 0.9
#                      per relogin whose entry the sweep had not deleted: such a
#                      relogin replaces nothing, so it Kicks nothing), and none
#                      gives up as live. A refused Kick is not retried (#97):
#                      only first attempts that timed out while the victim's
#                      sockets were torn down are, once each, and refused
#                      (R2-O6; #97 amended, run 20)
#   b1_cross_check     Logic's dead-epoch cross-check (R1-O7) has a sample for
#                      at least 0.9 of the relogins whose first Kick was sent
#                      and did not time out (by Login once the lease is gone,
#                      or by the refused Kick while it is live, P6b2a-b1I-7),
#                      and at most 1.1 per relogin
#   b1_lease           every survivor renews its lease every interval from
#                      the kill to the sweep: no call failed or found the
#                      lease missing, and Logic saw no renewal error
#   b1_swept           the sweeper finishes the dead epoch within the sweep wait
#   no_swap_in_b1      no page swapped in from the kill to the sweep (#70)
#   no_lo_drops_in_b1  loopback dropped no packet from the ramp to the sweep
#   b1_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp and
#                      the storm (P6a2aI-4)
#   b1_idle            after loadgen stops, every survivor reaches 0
#                      connections, 0 awaiting Logout and one Logout done per
#                      login, none dropped (stack_wait_idle, P6b1I-4)
#   b1_processes       nothing but the victim died, panicked or stopped uncleanly
#   artifacts          (once) every phase's storm CPU profiles, metrics before
#                      and after, loadgen's heap at the ramp's end, the SLO and
#                      loadgen's result
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init gwloss
t2_need curl taskset sudo python3
t2_require_host_setup

PHASES=${GWLOSS_PHASES:-b1 b_stress}
for p in $PHASES; do
  case $p in b1 | b_stress) ;; *) echo "gwloss: unknown phase '$p' (b1, b_stress)" >&2; exit 1 ;; esac
done
# The memory each phase needs available (#93): b1's loadgen peaked at
# 10.6-11.5GB with 400k SDK clients (runs 20, 21); b_stress's is held to
# GWLOSS_STRESS_LG_MEMLIMIT (16GiB = 17.2GB), plus the victim at 600k
# (~1.4GB), the survivors (~0.7GB each) and Redis (~0.4GB).
mem=14
case " $PHASES " in *" b_stress "*) mem=21 ;; esac
t2_mem_budget $mem
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# The storm's counters (Plan 6b2a): Logic's Kicks, lease renewals, sweeps
# and relogins onto a dead epoch; each survivor's own lease calls and rejects.
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  kick_first='goim_logic_kick_attempts_total:attempt="first"'
  kick_retry='goim_logic_kick_attempts_total:attempt="retry"'
  kick_first_timeout='goim_logic_kick_attempts_total:attempt="first",result="timeout"'
  kick_retry_refused='goim_logic_kick_attempts_total:attempt="retry",result="refused"'
  kick_giveups_dead='goim_logic_kick_giveups_total:target_lease="dead"'
  kick_giveups_live='goim_logic_kick_giveups_total:target_lease="live"'
  kick_dropped=goim_logic_kick_dropped_total
  dead_epoch_relogins=goim_logic_dead_epoch_relogin_seconds_count
  dead_epoch_relogin_s=goim_logic_dead_epoch_relogin_seconds_sum
  lease_live='goim_logic_lease_renewals_total:result="live"'
  lease_missing='goim_logic_lease_renewals_total:result="missing"'
  lease_error='goim_logic_lease_renewals_total:result="error"'
  sweeps_done='goim_logic_sweeps_total:result="done"'
  sweeps=goim_logic_sweeps_total
  sweep_uids=goim_logic_sweep_uids_total
  sweep_deleted=goim_logic_sweep_deleted_total
  guard_holds=goim_logic_sweep_guard_holds_total
)
GW_COUNTERS=(
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_handshake_rate='goim_gateway_admission_rejects_total:reason="handshake_rate"'
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# lo_dropped: packets loopback dropped on input since boot (host-wide); as capacity.sh.
lo_dropped() { cat /sys/class/net/lo/statistics/rx_dropped 2>/dev/null || echo null; }
# usage_ok JSON: loadgen used under 3/4 of its CPU set.
usage_ok() { [ "$(echo "$1" | lg_true - 'd["loadgen"]["cores"] < '"$LG_CPUS"' * 0.75')" = true ]; }

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

first=1
missing=""
for name in $PHASES; do
  # Each phase's stack, victim and waits.
  case $name in
    b1)
      export STACK_GATEWAYS=2
      clients=${GWLOSS_CLIENTS:-400000} rate=${GWLOSS_RATE:-5000} cap=${GWLOSS_CAP:-450000}
      sweep_wait=${GWLOSS_SWEEP_WAIT:-180} back_wait=60 idle_wait=300 sources=${GWLOSS_SOURCES:-}
      victim=gw-1 survivors=(gw-0) down="" LG_ENV=()
      # Every client up from the start, split by the balancer.
      online_rule='d["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45'
      ;;
    b_stress)
      export STACK_GATEWAYS=3
      clients=${GWLOSS_STRESS_CLIENTS:-600000} rate=${GWLOSS_STRESS_RATE:-5000} cap=${GWLOSS_STRESS_CAP:-650000}
      sweep_wait=${GWLOSS_STRESS_SWEEP_WAIT:-300} back_wait=${GWLOSS_STRESS_BACK_WAIT:-600} idle_wait=600
      sources=${GWLOSS_STRESS_SOURCES:-}
      victim=gw-0 survivors=(gw-1 gw-2) down=gw-1,gw-2 LG_ENV=(GOMEMLIMIT="${GWLOSS_STRESS_LG_MEMLIMIT:-16GiB}")
      # The victim ramped alone (#93).
      online_rule='d["online"] == '"$clients"' and d["online_by_gateway"].get("gw-0") == '"$clients"
      ;;
  esac
  export STACK_DIR=$T2_OUT/$name
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  STACK_GATEWAY_ARGS=(-max-conns "$cap")
  t0=$SECONDS

  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  up=$?
  t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  [ $up -eq 0 ] || t2_finish

  # The ramp.
  lo0=$(lo_dropped)
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-0"
  stack_redis_resetstat
  lg_start "$name" -mode sdk -clients "$clients" -rate "$rate" -uid-prefix "$name" ${sources:+-sources "$sources"} ${down:+-down "$down"}
  stack_usage_start loadgen="$LG_PID"
  t=$SECONDS
  online=false
  lg_wait_online "$clients" $((clients / rate * 3 + 60)) && online=$(lg_get /stats "$online_rule")
  [ "$online" = true ] || online=false # null: /stats unreadable, a failed run, not a stopped one
  ramp_s=$((SECONDS - t))
  ramp=$(stack_usage_json)
  redis_ramp=$(stack_redis_latency)
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-1"
  by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
  # Loadgen's live heap per client at the ramp's end, after a forced GC (#93's
  # memory budget estimates it from RSS, which includes GC headroom).
  mkdir -p "$STACK_DIR/artifacts/ramp"
  curl -sf -m 60 "http://$(lg_control)/debug/pprof/heap?gc=1&debug=1" >"$STACK_DIR/artifacts/ramp/loadgen.heap.txt"
  lg_heap=$(awk -v n="$clients" '/^# HeapAlloc = / { a = $4 } /^# Sys = / { s = $4 }
    END { if (a == "") print "null"; else printf "{\"live_mb\": %d, \"sys_mb\": %d, \"live_bytes_per_client\": %d}\n", a / 1048576, s / 1048576, a / n }' \
    "$STACK_DIR/artifacts/ramp/loadgen.heap.txt")
  # b_stress: the survivors back in the balancer before the kill.
  ups=""
  for g in ${down//,/ }; do
    lg_post "/up?gateway=$g" >/dev/null || ups+=" $g"
  done
  [ -z "$ups" ] || online=false
  t2_check "${name}_online" "$online" "\"want\": $clients, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $by_gateway, \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]'), \"up_failed\": $(t2_json_str "${ups# }")"

  if [ -n "$first" ]; then
    first=""
    wrong=""
    for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
    for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
    have=$(taskset -pc "$LG_PID" 2>/dev/null)
    [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
    t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
  fi
  if [ "$online" != true ]; then
    lg_stop
    stack_down
    t2_duration "$name" $((SECONDS - t0))
    continue
  fi

  # The storm: counters before, SIGKILL, CPU profiles of its first 30s.
  stack_snapshot before
  logic0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  declare -A sv0=() sv1=()
  for g in "${survivors[@]}"; do sv0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
  stack_redis_resetstat
  t2_record_start "$name"
  t2_cpustat_snap "$STACK_DIR/cpustat-2"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
  stack_usage_start loadgen="$LG_PID"
  reply=$(lg_post "/kill?gateway=$victim&pid=${STACK_PIDS[$victim]}&mark=death" | lg_py - d)
  kill_t=$SECONDS
  stack_expect_dead "$victim" # in this shell: it reaps and forgets the victim
  dead=$?
  (stack_profile storm 30 & lg_profile storm 30; wait) >/dev/null 2>&1 &
  prof=$!
  # The dead epoch's sweep (2 x TTL after its last renewal, then its uids at
  # the sweeper's rate), watched from the kill alongside the relogins: a
  # storm slower than the sweep must not hide it or delay its time. The
  # storm's window ends when both are done. A sweep done since the kill, not
  # before it (P6b2a-b1I-3).
  done0=$(echo "$logic0" | lg_py - 'd["sweeps_done"] or 0')
  (
    while [ $((SECONDS - kill_t)) -lt "$sweep_wait" ]; do
      d=$(stack_counters logic sweeps_done='goim_logic_sweeps_total:result="done"')
      [ "$(echo "$d" | lg_true - 'd["sweeps_done"] > '"$done0")" = true ] && { echo "true $((SECONDS - kill_t))"; exit; }
      sleep 1
    done
    echo "false $((SECONDS - kill_t))"
  ) >"$STACK_DIR/sweep.out" 2>/dev/null &
  sweeping=$!
  back=$(t2_bool lg_wait_back death "$back_wait")
  back_s=$((SECONDS - kill_t))
  lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
  wait "$sweeping"
  read -r swept sweep_s <"$STACK_DIR/sweep.out" || { swept=false sweep_s=null; }
  wait "$prof"
  storm=$(stack_usage_json)
  redis_storm=$(stack_redis_latency)
  logic1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  for g in "${survivors[@]}"; do sv1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
  t2_cpustat_snap "$STACK_DIR/cpustat-3"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
  t2_record_end "$name"
  lo1=$(lo_dropped)
  stack_snapshot after
  logic_d=$(stack_counters_delta "$logic0" "$logic1")
  sv_d=""
  for g in "${survivors[@]}"; do sv_d+="${sv_d:+, }\"$g\": $(stack_counters_delta "${sv0[$g]}" "${sv1[$g]}")"; done
  sv_d="{$sv_d}"
  window_s=$(echo "$storm" | lg_py - 'd["window_s"]')
  backoff=$(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')

  slo=$STACK_DIR/artifacts/slo-death.json
  pop=$(lg_py "$slo" 'd["population"]')
  # Rejects at capacity over every survivor; null if any reading failed.
  rejects=$(echo "$sv_d" | lg_py - 'sum(v["rejects_at_capacity"] for v in d.values())')
  pass=false
  if [ "$name" = b1 ]; then
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$clients"' * 0.45')
    t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
  else
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && [ "$back" = true ] && pass=$(lg_true "$slo" 'd["population"] == '"$clients")
    t2_check "${name}_back" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"rejects_at_capacity\": ${rejects:-null}, \"backoff\": $backoff, \"slo\": $(cat "$slo")"
  fi
  t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - 'd["d"]["kick_retry"] <= d["d"]["kick_first_timeout"] and d["d"]["kick_retry_refused"] == d["d"]["kick_retry"] and d["d"]["kick_giveups_live"] == 0 and 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0)) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]')" \
    "\"relogins\": $pop, \"swept_first\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]'), \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"first_timeouts\": $(echo "$logic_d" | lg_py - 'd["kick_first_timeout"]'), \"retries_refused\": $(echo "$logic_d" | lg_py - 'd["kick_retry_refused"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
  # R1-O7: one sample per relogin whose first Kick was sent and did not time
  # out, by Login (lease gone) or by the refused Kick (lease live).
  t2_check "${name}_cross_check" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - '0.9 * (d["d"]["kick_first"] - d["d"]["kick_first_timeout"]) <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]')" \
    "\"relogins\": $pop, \"samples\": $(echo "$logic_d" | lg_py - 'd["dead_epoch_relogins"]'), \"mean_s\": $(echo "$logic_d" | lg_py - 'round(d["dead_epoch_relogin_s"] / d["dead_epoch_relogins"], 1)')"
  # One renewal per 10s interval (the gateway's -lease-interval default): the
  # window holds at least window_s / 10 - 1 whole intervals.
  t2_check "${name}_lease" "$(echo "{\"g\": $sv_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= d["w"] // 10 - 1 for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
    "\"window_s\": $window_s, \"survivors\": $sv_d, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
  t2_check "${name}_swept" "$swept" "\"sweep_s\": $sweep_s, \"wait_s\": $sweep_wait, \"sweeps\": $(echo "$logic_d" | lg_py - 'd["sweeps"]'), \"uids\": $(echo "$logic_d" | lg_py - 'd["sweep_uids"]'), \"deleted\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]')"
  lo=null
  [ "$lo0" = null ] || [ "$lo1" = null ] || lo=$((lo1 - lo0))
  t2_check "no_lo_drops_in_$name" "$(t2_bool [ "$lo" = 0 ])" "\"dropped\": $lo, \"netdev_max_backlog\": $(cat /proc/sys/net/core/netdev_max_backlog 2>/dev/null || echo null)"
  headroom=false
  usage_ok "$ramp" && usage_ok "$storm" && headroom=true
  t2_check "${name}_loadgen_headroom" "$headroom" \
    "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"storm_cores\": $(echo "$storm" | lg_py - 'd["loadgen"]["cores"]')"

  # The end: the result is written before any client stops (#84), then the
  # survivors drain to idle before the stack stops.
  lg_stop
  res=$STACK_DIR/artifacts/loadgen-$name.json
  t=$SECONDS
  idle=$(t2_bool stack_wait_idle "$idle_wait" 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  logouts=""
  for g in "${survivors[@]}"; do
    body=$(stack_metrics "$g") || body=""
    logouts+="${logouts:+, }\"$g\": {\"done\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"logins_ok\": $(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"'), \"dropped\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"')}"
  done
  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"wait_s\": $idle_wait, \"logouts\": {$logouts}, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

  cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
  tcp="{\"ramp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-0" "$STACK_DIR/tcpstat-1"), \"storm\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3")}"
  t2_field "$name" "{\"clients\": $clients, \"victim\": \"$victim\", \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"loadgen_heap\": $lg_heap, \"storm\": $storm, \"back_s\": $back_s, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivors\": $sv_d, \"redis_latency\": {\"ramp\": $redis_ramp, \"storm\": $redis_storm}, \"backoff\": $backoff, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
  t2_duration "$name" $((SECONDS - t0))

  # Artifacts: metrics before and after the storm, its CPU profiles, the SLO
  # and loadgen's result.
  a=$STACK_DIR/artifacts
  for f in before/logic before/"$victim" after/logic; do
    grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
  done
  for g in "${survivors[@]}"; do
    for f in before/"$g" after/"$g"; do
      grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
    done
  done
  for f in logic "${survivors[@]}" loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/storm/$f.cpu"; done
  grep -q '^# HeapAlloc = ' "$a/ramp/loadgen.heap.txt" 2>/dev/null || missing+=" $name/ramp/loadgen.heap.txt"
  [ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" $name/slo-death.json"
  [ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
  "b_stress's survivors start empty: no survivor carries its own load through the 600k storm (#93)" \
  "b2 and slowed Logic (Plan 6b2b)"
t2_finish
```

Run: `bash -n scripts/tier2/gwloss.sh && echo ok`
Expected: `ok`.

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, so check the script in a throwaway copy with the guard lowered, the root parts removed and the scale cut, as 6b2a-b1 did: b1 at 2 × 1,000 SDK clients, b_stress at 1,500 on the victim, both at 500/s.

```bash
D=$(mktemp -d); cp scripts/tier2/gwloss.sh "$D/gwloss.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget \$mem$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/gwloss.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/gwloss.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/gwloss.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/gwloss.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 \
  GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=5000 \
  GWLOSS_STRESS_CLIENTS=1500 GWLOSS_STRESS_RATE=500 GWLOSS_STRESS_CAP=5000 bash "$D/gwloss.sh"; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype, ~2.5 min): `result.json` parses (`python3 -m json.tool tier2-results/gwloss-*/result.json >/dev/null`) and has 26 checks. All pass except possibly `no_swap_in_b1` and `no_swap_in_b_stress`, from the dev machine's own swapping (the first prototype run saw 5 and 3 pages in; the run after the plan review passed all 26). The exit code is 1 if a check fails, else 0. Values from the prototype:
- `b1_online`: `by_gateway` `{"gw-0": 1000, "gw-1": 1000}`; `b_stress_online`: `{"gw-0": 1500, "gw-1": 0, "gw-2": 0}`, `up_failed` `""`.
- `b1_slo`: `population` 1000, `back_s` ≈ 21; `b_stress_back`: `population` 1500, `back_all` true, `back_s` ≈ 20, `wait_s` 600, `backoff` `{"clients": 0, "waits": {}}`.
- `b1_kicks`: `swept_first` 0, `first` 1000, `retries` 0, `giveups_dead` 1000, `dropped` null; `b_stress_kicks`: `swept_first` 0, `first` 1500, `retries` 0, `giveups_dead` 1500.
- `b1_cross_check`: `samples` 1000, `mean_s` ≈ 15; `b_stress_cross_check`: `samples` 1500, `mean_s` ≈ 14.
- `b1_lease`: `survivors` `gw-0` with `lease_live` 6 in a ~61s window; `b_stress_lease`: `gw-1` and `gw-2`, each `lease_live` 6, `logins_ok` 750, `rejects_handshake_rate` 0.
- `b1_swept` and `b_stress_swept`: `sweep_s` ≈ 61, `uids` 1000 and 1500, `deleted` 0.
- `b1_idle`: `logouts` `{"gw-0": {"done": 2000, "logins_ok": 2000, "dropped": null}}`; `b_stress_idle`: `gw-1` and `gw-2` each `done` 750 of 750.
- The `b_stress` field: `redis_latency.storm` lists `sscan` and `unlink` (the dead epoch's `gwconns` sweep) next to `evalsha`, `hset` and the other commands; `by_gateway_at_kill` `{"gw-0": 1500, "gw-1": 0, "gw-2": 0}`; `loadgen_env` `"GOMEMLIMIT=16GiB"`; each phase's `loadgen_heap`, e.g. `{"live_mb": 9, "sys_mb": 50, "live_bytes_per_client": 6916}` (at this scale loadgen's fixed heap is a large share; the host's 600k gives the per-client number).

`b1/artifacts/` and `b_stress/artifacts/` hold `before/` and `after/` metrics, `storm/{logic,<survivors>,loadgen}.cpu.pb.gz`, `slo-death.json` and `loadgen-<phase>.json`. No process is left (`pgrep -f "$PWD/tier2-results/gwloss" || echo none`). An unknown phase is refused: `GWLOSS_PHASES="b1 nope"` prints `gwloss: unknown phase 'nope' (b1, b_stress)` and exits 1 before any stack starts. Afterwards, remove the dry run's `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/gwloss.sh
git commit -m "feat(tier2): gwloss b_stress phase, Tier 2 (b-stress) (#98)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-race-suite && make test-scripts && make test-load-probe && bash -n scripts/tier2/gwloss.sh`
Expected: all pass. Record the commits and the dry run in this plan's *Execution Notes*, record #98–#101 as decided in the roadmap (with the 6b2a-stress status), and push the branch.

- [ ] **Step 5: Tier 2 run 22 (on the host)**

On the Tier 2 host, after `git fetch && git switch feat/plan6b2a-stress`, with the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss` (~25 min, both phases; up to ~40 if b_stress's relogins use their whole wait), then sends back the `.report.txt`. Logic changed only in the Kick give-up path (one extra cached lease read), which the race suite's Kick tests cover on the dev machine. The race suite at production scale is therefore not rerun, unless the user wants it (`SCENARIO=race`). Expected: `gwloss` 26/26. What to read from the run, for the *Execution Notes*, the roadmap and the README's *Measured results* (both languages):
- **(b-stress):** `back_s` and the SLO distribution from the mark and from detection; `backoff.clients` (the share of the 600k that reached backoff, and why: `backoff.waits`, `failed`); the survivors' `rejects_handshake_rate`; Kick first attempts, drops and retries; `sweep_s` with `uids` and `deleted` (victims swept before they came back, `b_stress_kicks`' `swept_first`; the 600k `gwconns` `SSCAN` + `UNLINK`); `redis_latency.storm` (`evalsha` p99, and the `sscan`/`unlink` rows); the survivors' lease calls.
- **Storm cost:** Logic, Redis and survivor cores in the storm against the ramp, with the storm CPU profiles (`b_stress/artifacts/storm/`).
- **Memory (#93):** loadgen's `rss_peak_mb` at 600k under `GOMEMLIMIT` against `loadgen_heap.live_bytes_per_client` (the live heap, for the next memory budget), and whether loadgen's GC showed in its cores (`b_stress_loadgen_headroom`, `storm/loadgen.cpu.pb.gz`); the victim's peak at 600k with 4 `GOMAXPROCS`.
- **(b1) again:** `b1_cross_check` (`samples` close to the relogins, `mean_s` around 10–20s: 0–10s since the last renewal, plus ~1.6s detection and the 0–20s jitter), and the rest as run 21.
- If a check fails: `b_stress_online` → `failed` (pre-handshake closes at 5,000/s on 4 `GOMAXPROCS`: lower `GWLOSS_STRESS_RATE`); `b_stress_back` → `backoff` and `slo.missing`, and raise `GWLOSS_STRESS_BACK_WAIT` if clients are still in backoff steps; `b_stress_loadgen_headroom` → loadgen's GC under `GOMEMLIMIT` (its CPU profile), then raise `GWLOSS_STRESS_LG_MEMLIMIT` if `MemAvailable` allows; `<phase>_cross_check` → `samples` against `first − first_timeouts`, and Logic's `kick:` log lines; `<phase>_idle` → the error names the gateway and its Logouts.

---

## After this plan

- **Plan 6b2b:** b2 (silent loss: DROP on the victim's client and Kick ports), where Kicks time out instead of being refused and keep their retries while the lease is live, and slowed Logic (P5bI-2). It reuses `gwloss.sh`'s phase loop, the counter lists and `stack_redis_latency`.
- **Spec:** #97 (refused Kick) is recorded as a follow-up in the rev5 dispositions file. #101 refines R1-O7's cross-check ("or the refused Kick's give-up while the lease is live") and goes into the same follow-up, for the next spec revision.

## Execution Notes

- **Prototype (2026-10-14, while writing the plan):** every block was written and run in a scratch worktree of `main` `74bdafc` (branch `proto-6b2a-stress`, commits `fc3569f`, `b4a3726`, `3cb6941`, `e59910e`, one per task). A render script then copied it into this plan, with modified files as `git diff` against the previous task's commit and `gwloss.sh` whole. Results on the dev machine:
  - Task 1: `TestKickRefusedRecordsDeadEpochCrossCheck` failed first (`cross-check samples 0, want 1`), then passed; `go test -race -count=1 ./internal/logic/` green.
  - Task 2: the tests failed to compile first (`b.SetUp undefined`), then `go test -race -count=1 ./internal/loadgen/ ./cmd/loadgen/` was green.
  - Task 3: `stack_test: 9 failed` first, then all passed; `loadgen_test` green; the mutant (no Logout comparison) was caught with 2 failures.
  - Task 4: the dry run (b1 2 × 1,000, b_stress 1,500, 500/s) gave 26 checks, 24 passing. Only `no_swap_in_b1` (5 pages) and `no_swap_in_b_stress` (3 pages) failed, from the dev machine's own swap. The values are in Task 4 Step 2. No process was left.
- **Found while prototyping:**
  1. `INFO latencystats` counts the commands that Lua scripts run (`hset`, `hmget`, `sadd`, …) as well as `evalsha` itself, so the per-command rows show where a script's time goes. `CONFIG RESETSTAT` clears both `latencystats` and `commandstats` (checked on 7.4.11), so each window starts at zero.
  2. With both cross-check paths, the dry runs saw exactly one sample per relogin (1,000 of 1,000 and 1,500 of 1,500). Nearly all came from the refused Kick, because the 0–20s jitter lands inside the dead lease's 30s TTL.
- **Self-review (2026-10-14):** Review Focus 5 claimed a test for "a gateway that does not answer reads busy", but none existed. `idle: a silent gateway keeps it busy` and `idle: names the silent gateway` were added to Task 3 (with the old code the second fails), the prototype's Task 3 commit was amended, and the plan was rendered and replayed again.
- **Plan review (2026-10-14, Claude; decided by the user, "apply all"):** #98 amended (I1–I3, M4), #99 amended (M1), #100 and #101 as proposed (#101's wording and Review Focus 1 per M3). The 21GB memory check and `GOMEMLIMIT=16GiB` stay. `b_stress_back` stays a check, with a 600s wait.
  - **I1 (fixed):** the sweep wait started only after `lg_wait_back` returned, but counted from the kill. With both at 300s in b_stress, relogins that took the whole wait left the sweep loop no iteration (`swept` false although the sweep finished at ~120s), and `sweep_s` read as the relogin time whenever the relogins were slower. The sweep is now watched by a background subshell from the kill, which writes `swept` and `sweep_s` to `sweep.out`. A stub run (sweep done at 2s, relogins 6s, sweep wait 4s) gave `swept=true sweep_s=2`; the old loop gives false.
  - **I2 (fixed):** the sweeper deletes the entries of victims not back yet (from ~50–60s after the kill). Such a victim's relogin replaces nothing and queues no Kick, but still counted in `b_stress_kicks`' lower bound of 0.9 × relogins. With more than ~10% of the 600k in backoff at their turn in the sweep, the check would fail on a healthy storm. The lower bound is now 0.9 × (relogins − `sweep_deleted`), in both phases (`swept_first` in the check's detail). `cross_check` was already right: its lower bound counts Kicks.
  - **I3 (fixed):** `GWLOSS_STRESS_BACK_WAIT` defaults to 600s, not 300s. Each survivor admits `(4+1)/2 = 2` concurrent handshakes at 4 `GOMAXPROCS` (b1's survivor had 3 for 200k and refused 186k handshakes). A client refused more than 10 times in a row then steps through the SDK's doubling backoff (2.5–5s up to 5 min), so six unlucky steps add up to ~315s. The wait still catches a stuck client.
  - **M1 (fixed):** a repeated `-down` id counted twice: `-down gw-1,gw-1` was refused as "every gateway" on 2 gateways and sized 120 sources instead of 60 on 3. `New` now skips repeats; `TestNewSizesSourcesForTheGatewaysUp` has the case (`gw-2,gw-2` → 60), which fails without the fix (`120 sources, want 60`).
  - **M2 (noted, no change):** b_stress's ramp sends 5,000/s to one gateway with 2 handshake slots, about 4× b1's ramp rate per slot, so handshake rejects in the ramp are expected; Step 5's fallback (lower `GWLOSS_STRESS_RATE`) covers a failed ramp.
  - **M3 (wording):** Review Focus 1 and #101 now say that a relogin at the lease's expiry can be counted twice: Login queues the Kick before its own cross-check reads the lease. The window is milliseconds, far below the 1.1 bound.
  - **M4 (added):** at the ramp's end each phase saves loadgen's `/debug/pprof/heap?gc=1&debug=1` (`ramp/loadgen.heap.txt`, checked by `artifacts`) and records `loadgen_heap` (`live_mb`, `sys_mb`, `live_bytes_per_client`) from its `HeapAlloc` after the forced GC. #93 sized the budget from RSS, which includes GC headroom; this gives the live heap per SDK client. The forced GC runs after the ramp's CPU window and before the storm's.
  - The prototype's Task 2 and Task 4 commits were amended (`1b502ce`, `0876f07` on `proto-6b2a-stress`), `go test -race ./internal/loadgen/ ./cmd/loadgen/` is green, and the dry run (Task 4 Step 2) gave 26 of 26 (no swap this time): `sweep_s` 60 and 61 from the watcher, `swept_first` 0, `b_stress_back` `wait_s` 600, `loadgen_heap` 6.6–6.9KB live per client at 1,000–2,000 clients. The plan was rendered again and replayed on a clean `74bdafc` (6 patches with `git apply`, `gwloss.sh` whole): the tree is byte for byte the prototype's.
- **Execution (2026-10-14, inline, in a fresh session):** commits `bc4c942` (Task 1), `11f8d5d` (Task 2), `81b31dd` (Task 3), `d7b8f68` (Task 4). Every RED step failed as expected (Task 1 `cross-check samples 0, want 1`; Task 2 `b.SetUp undefined`; Task 3 `stack_test: 9 failed`; mutant caught with 2 failures). `make test`, `make test-race-suite`, `make test-scripts`, `make test-load-probe` green.
  - **Ruling (Task 1):** `crossCheck` is called *before* `KickGiveUps("dead").Inc()`, not after. With the plan's order, `TestKickRefusedRecordsDeadEpochCrossCheck` raced (it waits for the give-up, then reads the histogram) and failed once under `-race`. Now a counted give-up always has its sample. Metrics lose nothing; the give-up is counted a few ms later.
  - **Dry run (Task 4 Step 2):** 26 checks, 24 pass; only `no_swap_in_b1` (4 pages) and `no_swap_in_b_stress` (1 page) failed, from the dev machine's swap. Values as in Step 2: `b1_cross_check` 1000/1000 mean 14.5s, `b_stress_cross_check` 1500/1500 mean 14.0s, `sweep_s` 61/62, `loadgen_heap` 6.6–6.9KB live per client, `redis_latency.storm` lists `sscan` and `unlink`. No process left; `GWLOSS_PHASES="b1 nope"` refused.
  - **Final review (fresh reviewer):** no Critical or Important; all five Review Focus items met and tested. Minors deferred: (1) the Kicker's sample is `time.Now()` at the refused attempt (includes the Kick queue wait), not `login_ms`, and the mirror window (Login reads the lease live, the Kicker reads it gone: neither records) sits next to M3 inside the 0.9 bound; (2) `TestRampAloneThenUp` does not close the connection from `r.bal.Dial`; (3) `POST /up` has no guard for a killed gateway (allowed by #99); (4) `gwloss.sh` ignores `stack_redis_resetstat`'s result, so a failed reset would silently widen `redis_latency.storm`; (5) `stack_wait_idle` names a variable `done` (a bash keyword).
- **Final review minors fixed (2026-10-14, user: "fix the deferred minors"):**
  - M1 (`dc49480`): the Kicker's cross-check now uses the relogin's `Kick()` time (`kickJob.kickMs`), not the attempt's, so a job's queue wait is left out (#101's "the time is the give-up's" no longer holds; the `Kicker` interface is unchanged). `TestKickRefusedCrossCheckExcludesQueueWait` (1 worker, a dial refused after 200ms) failed first with `cross-check sum 0.404s > 0.010s`, then passed. The mirror of M3's double count is noted here: Login reads the lease live, then the Kicker reads it gone after a short queue wait, so neither records that relogin. It needs the same few-millisecond window at the TTL's end and sits inside the 0.9 lower bound.
  - M2, M3 (`21b4eda`): `TestRampAloneThenUp` closes its probe connection. `POST /up` stays allowed after `/kill` (#99); `Runner.Up` and the control API's doc now say the caller sends it only once the gateway listens again.
  - M4, M5 (`88c6c08`): the phase field's `redis_latency.reset` (`ramp`, `storm`) records whether each `CONFIG RESETSTAT` succeeded, and the header says that `after/`'s Redis INFO covers the storm only. `stack_wait_idle`'s `done` is now `ldone`. A b1-only dry run gave 14 checks, only `no_swap_in_b1` failing (the dev machine's swap), with `reset` `{"ramp": true, "storm": true}`.
  - `make test`, `make test-race-suite`, `make test-scripts`, `make test-load-probe` green.
- **Interface review (2026-10-14, Claude; decided as proposed by the user, "apply all"):** P6b2a-stressI-1..4. **Checked, unchanged:** `LG_ENV`'s `env` (and `taskset`) exec loadgen, so `LG_PID` is loadgen's own pid. `failure` waits come from an attempt that never reached a gateway's logic (`EndFailed`: dial or handshake failure or timeout), which goes straight to a backoff step; `backoff` waits come from the retry limits (10 pre-handshake closes, 5 other retryable rejects) or a backoff code. `/kill` takes the victim out of the balancer before its SIGKILL, so no client redials the dead address; in run 22, `failure` waits are handshake failures or timeouts at the survivors. `stack_redis_latency`'s awk fields match Redis 7's `latency_percentiles_usec_<cmd>:p50=…,p99=…,p99.9=…`; with latency tracking off it prints `{}`. The cross-check's bounds are unchanged by the final review's M1.
  - **P6b2a-stressI-1 (fixed, `c2972b9`):** `backoff_clients` counts each client once per run, and `gwloss.sh` read it once after the storm, so clients that backed off in the 600k ramp (handshake rejects at 2 slots, M2) were in the storm's number and not counted again there. The `Recorder` now also counts since the latest mark (`Counts.BackoffSinceMark`, `/stats`' `backoff_since_mark` `{mark, clients, waits}`: each client once per mark), and `gwloss.sh`'s `backoff` (in `<phase>_back`/`b1_slo` and the phase field) is `{"ramp", "storm", "run"}`: since loadgen's start read just before `/kill`, since the `death` mark, and the run's totals. `TestRecorderCountsBackoffSinceMark` failed first (`c.BackoffSinceMark undefined`), then passed. Task 4 Step 5's `backoff.clients` now reads `backoff.storm.clients` (and `backoff.ramp` for the ramp). Dry run: 26 checks, only the two swap checks failing (the dev machine), `backoff` present in both phases.
  - **P6b2a-stressI-2 (for 6b2b):** `stack_wait_idle` fails a phase boundary at once on any dropped Logout. Right for b1 and b_stress; a slowed Logic may drop Logouts legitimately (Reconcile repairs them, R1-O3), so 6b2b decides whether its phase records dropped Logouts instead of failing on them.
  - **P6b2a-stressI-3 (fixed, `c2972b9`):** `capacity.sh`'s `# usage_ok JSON: …` comment, left above `lo_dropped` when `logouts_settled` moved out, is back above `usage_ok()`.
  - **P6b2a-stressI-4 (noted):** the cross-check compares three clocks: Login's sample is the gateway's `login_ms`, the Kicker's is Logic's clock (`kickMs`), and the renewal is Redis's `TIME`. One host in Tier 2; in production, NTP skew is far below the spec's ±10s.
- **Tier 2 run 22 (2026-10-14, `ca93323`, dirty 0, 1048s): reported 24/26, really 23/26.** Failed: `no_swap_in_b1` (94 pages), `no_swap_in_b_stress` (77,723 pages in, 271,437 out); `b_stress_back` passed only because `lg_wait_back` counted polls (#103).
  - **b1 (valid, as run 21):** all 200,000 back, from the mark p50 12.0s, p99 22.9s, max 37.6s; detection p99 1.67s. Kicks: `first` 194,818 + `dropped` 5,182, 64 retries = 64 first timeouts, all refused. **`b1_cross_check`: 194,754 samples = first − first timeouts, mean 15.4s.** Survivor: 8 lease renewals, none missed; 180,027 `handshake_rate` rejects. Sweep 82s (200k uids). Loadgen 11.5GB, 5.7KB live heap per client.
  - **b_stress:** 600k on `gw-0` in a 124s ramp (265k pre-handshake closes, retried), victim 1.35GB, loadgen 16.1GB (live heap 3.25GB, 5,683 B per client; the rest mostly goroutine stacks). After the kill: sweep done at 120s (600k uids, 500,324 entries deleted before their victims came back; SSCAN p99 2.6ms, UNLINK 0.53ms), Kicks `first` 73,228 + `dropped` 26,448 (0.9 × (600k − 500,324) ✓), cross-check 72,972 samples, mean 16.5s; survivors 74 renewals each, none missed; Logic 0.10 cores, Redis 0.03, `evalsha` p99 60µs. **Relogins loadgen-bound:** p50 275s, p99 441s, max 739s; 518,054 clients reached backoff (414,642 `backoff` and 2,256,290 `failure` waits). The survivors' `after/` metrics (read on the host): per survivor ~3.2M `handshake_timeout`, ~183k `pending_mem`, ~166k `handshake_rate` (all `no_slot`), ~19k `auth_timeout`, only ~342k handshakes ok; their CPU sets 92% idle. Loadgen's storm CPU profile (first 30s): 791% of 8 CPUs, `crypto/ecdh.x25519ScalarMult` 46% cumulative, syscalls 17%, GC minor. The client side of the handshakes, not the gateways, set the pace.
  - **Decided by the user (option A) and fixed:** #102 (`36d3438`: headroom on the storm's first 30s; b_stress records `loadgen_bound`; back wait 900s), #103 (`3378b69`: wall-clock waits; `loadgen_test` failed first on both waits, 2 failures, then passed), #104 (`36d3438`: every reject reason and handshake result per survivor), #105 (`36d3438`: b_stress budget 24GB; stop the host's other services before the run). Dry run 26 checks, only the two swap checks failing (the dev machine); `make test`, `make test-race-suite`, `make test-scripts`, `make test-load-probe` green. Next: Tier 2 run 23 with docker/mongod, VS Code and gopls stopped.
  - **#105 amended (2026-10-14, the user):** the budget is 23GiB, not 24: `t2_mem_budget` counts GiB (`MemAvailable` / 1024²), and the host reaches ~23GiB available with a desktop session (24,066,832 kB = 23.0GiB). Before run 23 the host's swap was emptied (1.5GiB left from run 22) and its other services stopped.

