# Plan 6b2a-b1: Gateway Kill at 200k (Tier 2 (b1)) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's load test (b1) on the Tier 2 host with SDK clients: SIGKILL one of two 200k gateways, then check that 99% of its clients are logged in again within 30s of `T_death`, that Kick attempts to the dead gateway are about one per relogin with no retries, that the surviving gateway misses no lease renewal, and that the dead epoch is swept. Each process's CPU and peak memory are recorded for 6b2a-stress's sizing.

**Architecture:** A new Tier 2 scenario, `scripts/tier2/gwloss.sh`, has one phase, `b1`, on a fresh two-gateway stack. Loadgen ramps 400k SDK clients split over both gateways, then SIGKILLs `gw-1` (`POST /kill`, mark `death`). Logic's and the survivor's counters are read before the kill and again when the sweeper has finished the dead epoch, and the checks come from those deltas. `scripts/stack.sh` gains the readers this needs: `stack_counters` and `stack_counters_delta` (several counters from one scrape, with a missing metric read as null and never as 0), and `rss_peak_mb` from `VmHWM` in `stack_usage_json`. The prototype found that every relogin's Kick was retried about twice: a SIGKILLed gateway's lease stays live for up to its 30s TTL, and Logic retries while the lease is live. Logic therefore no longer retries a Kick whose connect was refused (#97).

**Tech Stack:** Go 1.27.1 (grpc-go status codes), bash 5.2, the pinned `redis-server` 7.4.11 (`make redis-server`), curl, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Verification → Load tests* (b1) and "storm: the other gateways never miss a lease renewal"; *Failure modes* ("Gateway process crash / pod kill": one Kick attempt per relogin reaches the dead epoch, with no retries; "Kick can't reach the gateway": retried while the target lease is live); *Kick* (R2-O6: the first attempt is always sent; R5S-O2: bounded per-target queue); *Sweeper* (2 × TTL, then 10k uids/s); *Verification → Test execution model* (*Automation contract*). Roadmap choices #69, #71, #79, #87, #93, #94 (amended 2026-10-14), and Plan 6b1's *Execution Notes* (P6b1I-3: `rss_peak_mb`).

**Branch:** `feat/plan6b2a-b1-gwloss`, from `main` (`7cb0295`, which holds every earlier plan, the README and roadmap #93/#94).

**Split (#94, amended):** 6b2a-b1 (this plan: 3 tasks, ~50KB) → 6b2a-stress (`b_stress`, `POST /up`, the memory check, the loadgen backoff count, the per-gateway `stack_wait_idle`, Redis p99 from `INFO`) → 6b2b. Each plan is written and executed in its own session, and each ends with its own Tier 2 run.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`), `make test-race-suite` and `make test-scripts` stay green on the dev machine (2 cores / 2GB), loopback only.
- Scripts: bash with `set -uo pipefail`; `scripts/tier2/lib.sh` writes JSON without jq; python3 only reads JSON (and does arithmetic on it). The host's awk is mawk: no gawk-only features.
- Ports (choice #76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`; loadgen's control API is `STACK_PORT_BASE + 5`.
- CPU sets (#71): gateways `4-9,16-21` (shared by both gateways, `GOMAXPROCS` 6 each by #79), Logic + Redis `10,11,22,23`, loadgen `0-3,12-15` (8 CPUs).
- Loadgen sources 127.0.0.2… (#69, amended): by default one per 5,000 connections per gateway, sized for `max(1, N−1)` gateways, so 80 sources for 400k on 2 gateways. After the kill, the survivor's 400k come from 5,000 per source.
- SDK memory budget (#93): b1 runs 2 gateways × 200k SDK clients, all up from the start, and loadgen needs ≈ 9.5GB (~23.8KB per SDK client, `ca7c6e9`). No `POST /up`. Gateways run Go's default GC (6b1 run 18).
- Spec numbers: 99% logged in again within 30s of `T_death` (R4M-17); lease TTL 30s, renewed every 10s; the sweeper starts on an epoch 2 × TTL after its last renewal and sweeps 10,000 uids/s; Kick retries go up to 3, 10s apart, only while the target's lease is live; per-target Kick queue 1024, 64 workers, 1s deadline per attempt.
- Swap during the measured window fails it (#70); the user closes the desktop apps before the run.
- Comments and names match the surrounding code; new behaviour cites its choice number (#95–#97).

## Review Focus

1. **A refused Kick and a timed-out Kick must be told apart.** #97 stops retries only when the connect is refused, meaning nothing listens at the target's address. A starved but alive gateway still accepts TCP in its kernel and its Kick times out, so it keeps R2-O6's retries. Three tests pin this: `TestKickRefusedDoesNotRetry` (Task 1, refused with a live lease: no retry, counted `dead`), the existing `TestKickRetriesOnlyWhileLeaseLive` (an error from a listening gateway with a live lease: 3 retries) and `TestKickToBlackholeEndsWithinDeadline` (a timeout).
2. **A mistyped counter must never read 0.** `b1_kicks` passes when `kick_retry` is 0, so a misspelt metric name read as 0 would be a false pass. `stack_counters` prints null for a metric with no sample in the scrape, and the check's python then fails. Test: `counters: one scrape as JSON` (`typo` → None; Task 2).
3. **A label value with no sample yet is 0, not null.** `kick_attempts_total{attempt="retry"}` has no sample until the first retry, so after this plan it has none in a passing run. Reading that as null would fail `b1_kicks` on the success it checks for. Test: the same check (`never` → 0); the dry run shows `"retries": 0` (Task 3).
4. **The survivor ends with 400k connections, above the default 250k cap.** At the default cap the survivor would refuse half of the relogins, those clients would back off, and the SLO would fail for a reason that is not the storm. `GWLOSS_CAP` defaults to 450,000 (the Logout queue follows it, #89), and `b1_slo` fails if any gave up (Task 3).
5. **A failed scrape around the storm must fail the checks, not pass them.** If Logic does not answer at either end, `stack_counters` prints null, `stack_counters_delta` prints null, and every check that reads the delta evaluates to false. Test: `counters: delta of a failed scrape` (Task 2).

## Implementation choices this plan makes

Proposed 2026-10-14 with this plan. **Decided 2026-10-14:** the user decided #97 while prototyping ("No retry on refused"). Claude decided #95 and #96 at the user's request in the plan review: #96 as proposed, #95 with one amendment (`b1_kicks` counts dropped attempts; see *Execution Notes → Plan review*). Execution: inline (superpowers:executing-plans), in a fresh session.

95. **The `gwloss` scenario's `b1` phase.** A fresh stack with 2 gateways sharing the gateway CPU set, `-max-conns 450000` (the survivor's 400k plus clients in flight; nofile 900k of 1,048,576, conntrack fine), and Go's default GC. 400k SDK clients at 5,000/s (ramp ~80s), uid prefix `b1`. Each gateway must hold 45–55% of the clients before the kill. Loadgen then SIGKILLs `gw-1` (`/kill`, mark `death`; T_death is loadgen's clock). The storm window runs from the kill until the sweeper reports the dead epoch `done`, at most `GWLOSS_SWEEP_WAIT` (180s; expected ~60s stale plus 20s for 200k uids). It is the window for swap, CPU (`stack_usage_*`), the CPU-set and TCP counters, and the counter deltas. CPU profiles of Logic, `gw-0` and loadgen cover the storm's first 30s (`artifacts/storm/`), taken inside the window and recorded as `profile_in_storm_s`, because the storm happens only there. Metrics and heap snapshots are taken before (all processes) and after (Logic, `gw-0`). **Checks:** `b1_slo` (99% within 30s of the mark, with the victim holding ≥45% of the clients); `b1_kicks` (0.9–1.1 first attempts per relogin, counting those a full per-target queue dropped before sending (R5S-O2 allows them; Reconcile is the backstop); 0 retries; 0 `live` give-ups); `b1_lease` (the survivor's lease calls in the window: no `error`, no `missing`, at least `window_s // 10 − 1` `live`, and no Logic `lease_renewals_total{result="error"}`); `b1_swept`; `no_swap_in_b1`; `no_lo_drops_in_b1`; `b1_loadgen_headroom` (under ¾ of loadgen's CPU set in the ramp and the storm, #92); `b1_idle` (`stack_wait_idle 300` on the survivor after loadgen stops); `b1_processes`; `pinning`; `artifacts`. Logic stays one process (minReplicas, no HPA), so the storm runs at Logic's minimum. **Recorded, not checked:** Logic's Kick give-ups and drops, `dead_epoch_relogin_seconds` count, logins `ok`/`replaced`, sweeps, swept uids and deleted fields, mass-stale guard holds, per-process cores, RSS and peak RSS for the ramp and the storm, login latency, `lost`, and the SLO distribution from the mark and from detection. **`not_covered`:** per-shard Redis CPU and p99 with the `gwconns` shards marked (one `redis-server`; Redis Cluster is Plan 6c; Redis p99 from `INFO` is 6b2a-stress), `b_stress`, and b2 with slowed Logic.
96. **Counter readers.** `stack_counters NAME KEY=METRIC[:MATCH]...` takes one scrape and prints `{"KEY": sum}`. It prints null for the whole object if NAME does not answer. A KEY is null if METRIC has no sample at all (a typo, or a counter family that has never been incremented), and 0 if METRIC has samples but none contains MATCH (a label value not yet seen). `stack_counters_delta A B` prints B − A per key. A key null in a successful A counts as 0, because a family absent before the storm had no increments yet. A key null in B stays null, and if either reading is null the whole delta is null. So a family that never appears at all, such as `kick_dropped_total` with no drop, reads `null` in the result's fields. That is "no sample", which the field documentation says. **`rss_peak_mb`** in `stack_usage_json` is `VmHWM`: the process's peak since start (P6b1I-3), not the window's, because the memory budget (#93) is the peak.
97. **A Kick refused at connect is not retried, and counts as a `dead` give-up** (decided by the user 2026-10-14 while prototyping). The spec's *Failure modes* says a crash gets "one Kick attempt per relogin, with no retries", and also that a Kick that can't reach the gateway is "retried while the target lease is live". After a SIGKILL the lease stays live for up to its TTL (30s), so in the prototype (2 × 1,000 SDK clients) the 1,000 relogins got 1,000 first attempts and 2,049 retries, all `connection refused`. At 200k that is ~400k extra attempts through the 1,024-slot per-target queue. R2-O6's retries protect a starved but alive gateway, whose kernel still completes the TCP connect, so its Kick times out (`DeadlineExceeded`) and keeps its retries. A refused connect means nothing listens at the address: the process is gone, and with it the connection the Kick targets. Logic now gives up at once on `UNAVAILABLE` whose message carries `connection refused` (gRPC passes the dial error only as text). The give-up counts as `target_lease="dead"`, so a crash does not raise the `live` give-up alert once per relogin. In Kubernetes a SIGKILLed container restarts in the same pod, on the same IP, and is refused until it listens again, so the change applies there too. A deleted pod's IP times out instead and keeps the retries (b2 measures that case). Rejected: amending the spec to allow up to 3 retries per relogin, and keeping the check failing as a known open item.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `internal/logic/kick.go` | modify | `refused(err)`; a refused attempt gives up as `dead`, no retry (#97) |
| `internal/logic/kick_test.go` | modify | `TestKickRefusedDoesNotRetry` |
| `scripts/stack.sh` | modify | `rss_peak_mb` in `stack_usage_json`; `stack_counters`, `stack_counters_delta` (#96) |
| `scripts/stack_test.sh` | modify | tests for the above |
| `scripts/tier2/gwloss.sh` | create | the Tier 2 `gwloss` scenario, phase `b1` (#95) |
| `Makefile` | modify | `gwloss` in the `test-tier2` scenario list |

Every code block below was run in a prototype (see *Execution Notes*). Patches apply with `git apply` from the repo root; new files are given whole.

---

### Task 1: a refused Kick is not retried (#97)

**Files:**
- Modify: `internal/logic/kick.go` (`Kicker.attempt`, new `refused`)
- Test: `internal/logic/kick_test.go`

**Interfaces:**
- Consumes: `newKickEnv(t, KickConfig, map[string]string) kickEnv`, `testConfig()`, `waitFor`, `counter`, `env.st.RenewLease(ctx, ref)`, `Metrics.KickGiveUps` / `.KickAttempts` (labels `target_lease`; `attempt`, `result`).
- Produces: `refused(err error) bool`. For a refused attempt, `goim_logic_kick_attempts_total{attempt="first",result="error"}` +1, `goim_logic_kick_giveups_total{target_lease="dead"}` +1, no retry. Task 3's `b1_kicks` reads these counters.

- [ ] **Step 1: Write the failing test**

Apply to `internal/logic/kick_test.go`:

```diff
diff --git a/internal/logic/kick_test.go b/internal/logic/kick_test.go
index f26e7d3..aa95397 100644
--- a/internal/logic/kick_test.go
+++ b/internal/logic/kick_test.go
@@ -189,6 +189,36 @@ func TestKickRetriesOnlyWhileLeaseLive(t *testing.T) {
 	}
 }
 
+// #97: a Kick refused at connect (nothing listens: a crashed gateway on a
+// live node) gives up at once, even while the target's lease is live, which
+// it stays for up to a TTL after the crash. Without this every relogin of a
+// crash storm is retried until the lease expires (Tier 2 b1 prototype: ~2
+// retries per relogin). The give-up counts as dead, so a crash does not
+// raise the live give-up alert once per relogin.
+func TestKickRefusedDoesNotRetry(t *testing.T) {
+	lis, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	addr := lis.Addr().String()
+	lis.Close() // nothing listens: connects are refused
+	gw := testredis.Gateway()
+	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: addr})
+	ref := store.EpochRef{Gateway: gw, Epoch: 11}
+	if _, err := env.st.RenewLease(ctx, ref); err != nil {
+		t.Fatal(err)
+	}
+	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
+	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
+	time.Sleep(3 * env.cfg.RetrySpacing)
+	if n := counter(env.m.KickAttempts.WithLabelValues("retry", "error")); n != 0 {
+		t.Fatalf("%v retries to a refused address with a live lease", n)
+	}
+	if n := counter(env.m.KickGiveUps.WithLabelValues("live")); n != 0 {
+		t.Fatalf("%v live give-ups for a refused address", n)
+	}
+}
+
 // R5S-O2: a full per-target queue drops and counts instead of growing.
 func TestKickQueueFullDrops(t *testing.T) {
 	gw := testredis.Gateway()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -race -count=1 -run TestKickRefusedDoesNotRetry ./internal/logic/`
Expected: FAIL with `timed out waiting for give-up`. With the lease live, the refused attempt is retried and finally counted as a `live` give-up, never as `dead`.

- [ ] **Step 3: Implement**

Apply to `internal/logic/kick.go`:

```diff
diff --git a/internal/logic/kick.go b/internal/logic/kick.go
index 4464f0d..9343ea3 100644
--- a/internal/logic/kick.go
+++ b/internal/logic/kick.go
@@ -5,7 +5,9 @@ import (
 	"fmt"
 	"log/slog"
 	"strconv"
+	"strings"
 	"sync"
+	"syscall"
 	"time"
 
 	pb "github.com/Tallinzp/goim/api/internalpb"
@@ -179,6 +181,16 @@ func (k *Kicker) attempt(t *kickTarget, j kickJob) {
 		return
 	}
 	k.m.KickAttempts.WithLabelValues(kind, "error").Inc()
+	if refused(err) {
+		// Nothing listens at the target's address: its process is gone, and
+		// with it the connection this Kick targets. A starved but alive
+		// gateway still completes TCP connects in its kernel and times out
+		// instead, so it keeps its retries (#97, R2-O6). Its lease stays live
+		// for up to a TTL after a crash, so the give-up counts as dead: a
+		// crash must not raise the live give-up alert once per relogin.
+		k.m.KickGiveUps.WithLabelValues("dead").Inc()
+		return
+	}
 	if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || j.attempt >= k.cfg.Retries {
 		k.m.KickGiveUps.WithLabelValues(k.leaseLabel(t.ref)).Inc()
 		return
@@ -192,6 +204,14 @@ func (k *Kicker) attempt(t *kickTarget, j kickJob) {
 	time.AfterFunc(k.cfg.RetrySpacing, func() { k.enqueue(t.ref, j) })
 }
 
+// refused reports whether a failed Kick never reached a listener: its dial
+// got ECONNREFUSED. gRPC carries the dial error only as text in the
+// UNAVAILABLE status ("connect: connection refused"), also on the fail-fast
+// RPCs that follow while the connection stays in TRANSIENT_FAILURE.
+func refused(err error) bool {
+	return status.Code(err) == codes.Unavailable && strings.Contains(status.Convert(err).Message(), syscall.ECONNREFUSED.Error())
+}
+
 // leaseLabel is the target_lease label: live, dead, or unknown if the lease
 // could not be read. It may read Redis: call it from a worker only.
 func (k *Kicker) leaseLabel(ref store.EpochRef) string {
```

- [ ] **Step 4: Run the tests**

Run: `gofmt -l internal/logic; go test -race -count=1 ./internal/logic/ && GOIM_RACE_SUITE=1 GOIM_REDIS_SERVER=$(command -v redis-server || echo .cache/redis/bin/redis-server) go test -count=1 -timeout 20m -run 'Kick' ./internal/racetest/`
Expected: no gofmt output and both `ok`. `TestKickRetriesOnlyWhileLeaseLive` still sees 3 retries: its gateway listens and returns `UNAVAILABLE` from the handler, which is not a refused connect. The race suite's `TestDroppedKickIsRetriedThenCaughtByReconcile` still sees 3 retries, because its Kick fault fails inside a listening gateway.

- [ ] **Step 5: Commit**

```bash
git add internal/logic/kick.go internal/logic/kick_test.go
git commit -m "fix(logic): a Kick refused at connect is not retried (#97)"
```

---

### Task 2: counter readers and peak RSS in stack.sh (#96)

**Files:**
- Modify: `scripts/stack.sh` (header, `stack_usage_json`, new `stack_counters`, `stack_counters_delta`)
- Test: `scripts/stack_test.sh`

**Interfaces:**
- Consumes: `stack_metrics NAME` (one scrape; fails if NAME does not answer), `stack_usage_start`, `STACK_USAGE_PIDS`.
- Produces:
  - `stack_counters NAME KEY=METRIC[:MATCH]...` → `{"KEY": number|null, ...}` or `null`. MATCH is a substring of the sample line, e.g. `'goim_logic_kick_attempts_total:attempt="retry"'`. Specs must not contain spaces.
  - `stack_counters_delta A B` → `{"KEY": number|null, ...}` or `null`.
  - `stack_usage_json` → each process now also has `"rss_peak_mb"` (VmHWM).
  Task 3 uses all three.

- [ ] **Step 1: Write the failing tests**

Apply to `scripts/stack_test.sh`:

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index a115e02..c8da1b1 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -140,6 +140,19 @@ expect "metrics: a label filter" "$(stack_metric_in 'goim_x{r="done"} 5
 goim_x{r="dropped"} 2' goim_x 'r="done"')" 5
 stack_metrics gw-9 >/dev/null && bad "metrics: no such process must fail" || ok "metrics: no such process fails"
 
+# stack_counters: several counters from one scrape, as JSON (Plan 6b2a). A
+# metric with no sample is null (a typo must not read 0); a label value
+# with no sample of a metric that has others is 0 (a retry counter before
+# the first retry); a process that does not answer is null.
+c=$(stack_counters gw-0 conns=goim_gateway_connections first='goim_gateway_lease_calls_total:result="first"' \
+  never='goim_gateway_lease_calls_total:result="never"' typo=goim_gateway_lease_calls_totl)
+expect "counters: one scrape as JSON" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(sorted(d), d["conns"], d["first"], d["never"], d["typo"])' "$c")" \
+  "['conns', 'first', 'never', 'typo'] 0 1 0 None"
+expect "counters: no such process" "$(stack_counters gw-9 conns=goim_gateway_connections)" null
+expect "counters: delta, an absent sample before is 0" \
+  "$(stack_counters_delta '{"a": 1, "b": null, "c": 2, "d": 0.5}' '{"a": 4, "b": 3, "c": null, "d": 0.75}')" '{"a": 3, "b": 3, "c": null, "d": 0.25}'
+expect "counters: delta of a failed scrape" "$(stack_counters_delta null '{"a": 1}')" null
+
 # The phase boundary (P6a2bI-3): idle at once on an idle stack; a socket open
 # on gw-0 keeps it busy, named, until the socket closes.
 stack_wait_idle 5 && ok "idle: an idle stack" || bad "idle: an idle stack"
@@ -161,6 +174,8 @@ kill $extra; wait $extra 2>/dev/null
 usage=$(stack_usage_json)
 expect "usage: every process, the extras, and the window" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(sorted(d), 0.9 < d["window_s"] < 3, d["gw-0"]["rss_mb"] > 0, d["gw-0"]["cpu_s"] >= 0, d["gone"], d["extra"])' "$usage")" \
   "['devtoken', 'extra', 'gone', 'gw-0', 'gw-1', 'logic', 'redis', 'window_s'] True True True None None"
+# The peak (VmHWM, P6b1I-3) is at least the current RSS.
+expect "usage: peak RSS" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(all(d[n]["rss_peak_mb"] >= d[n]["rss_mb"] > 0 for n in ("gw-0", "logic", "redis")))' "$usage")" True
 
 stack_profile cpu 1
 # A failed profile fails stack_profile, even when it is not the last one
```

- [ ] **Step 2: Run them to verify they fail**

Run: `make test-scripts 2>&1 | grep -E 'FAIL|passed|failed'`
Expected: `FAIL counters: ...` (`stack_counters: command not found` makes the readings empty) and `FAIL usage: peak RSS`, then `stack_test: 5 failed`.

- [ ] **Step 3: Implement**

Apply to `scripts/stack.sh`:

```diff
diff --git a/scripts/stack.sh b/scripts/stack.sh
index 6664c08..402ddc6 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -15,7 +15,9 @@
 #   stack_metrics NAME          # NAME's /metrics once; stack_metric_in BODY METRIC [MATCH] reads it
 #   stack_wait_idle SECONDS     # phase boundary: every gateway at 0 connections and 0 awaiting Logout
 #   stack_usage_start [N=PID..] # start a CPU window over the stack's processes (and N=PID extras)
-#   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb"}...} since the start
+#   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb", "rss_peak_mb"}...} since the start
+#   stack_counters NAME KEY=METRIC[:MATCH]...  # {"KEY": sum, ...} from one scrape of NAME (null: no answer)
+#   stack_counters_delta A B    # {"KEY": B - A, ...} of two stack_counters readings
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -326,8 +328,11 @@ stack_usage_start() {
   STACK_USAGE_T0=$(date +%s%N)
 }
 
-# stack_usage_json: {"window_s": W, NAME: {"cpu_s", "cores", "rss_mb"}, ...}
-# since stack_usage_start (cores = cpu_s / W); a process gone since is null.
+# stack_usage_json: {"window_s": W, NAME: {"cpu_s", "cores", "rss_mb",
+# "rss_peak_mb"}, ...} since stack_usage_start (cores = cpu_s / W); a process
+# gone since is null. rss_peak_mb is the process's peak resident memory since
+# it started (VmHWM, P6b1I-3), not the window's: the memory budget of a run
+# (#93) is its peak, which a reading at the window's end can miss.
 stack_usage_json() {
   local hz e name pid t1 out
   hz=$(getconf CLK_TCK)
@@ -336,14 +341,53 @@ stack_usage_json() {
   for e in "${STACK_USAGE_PIDS[@]}"; do
     name=${e%%=*} pid=${e#*=}
     out+=", \"$name\": $(awk -v t0="${STACK_USAGE0[$name]:-}" -v t1="$(stack_ticks "$pid")" -v hz="$hz" \
-      -v w=$((t1 - STACK_USAGE_T0)) -v rss="$(awk '/^VmRSS:/ { print $2 }' "/proc/$pid/status" 2>/dev/null)" '
-      BEGIN { if (t0 == "" || t1 == "" || rss == "") { print "null"; exit }
+      -v w=$((t1 - STACK_USAGE_T0)) -v mem="$(awk '/^Vm(RSS|HWM):/ { printf "%s ", $2 }' "/proc/$pid/status" 2>/dev/null)" '
+      BEGIN { if (t0 == "" || t1 == "" || split(mem, m, " ") != 2) { print "null"; exit }
               c = (t1 - t0) / hz
-              printf "{\"cpu_s\": %.2f, \"cores\": %.3f, \"rss_mb\": %d}", c, c / (w / 1e9), rss / 1024 }')"
+              printf "{\"cpu_s\": %.2f, \"cores\": %.3f, \"rss_mb\": %d, \"rss_peak_mb\": %d}", c, c / (w / 1e9), m[2] / 1024, m[1] / 1024 }')"
   done
   echo "{$out}"
 }
 
+# stack_counters NAME KEY=METRIC[:MATCH]...: one scrape of NAME's /metrics
+# as {"KEY": the sum of METRIC's samples containing MATCH, ...}, for counters
+# read before and after an event (Plan 6b2a: the storm's Kicks, lease
+# renewals and sweeps). Prints null if NAME does not answer. A KEY is null
+# when the scrape has no sample of METRIC at all (a mistyped name must not
+# read as 0), and 0 when METRIC has samples but none contains MATCH: a
+# labelled counter has no sample for a label value until its first
+# increment, e.g. kick_attempts_total{attempt="retry"} while no Kick was
+# retried.
+stack_counters() {
+  local name=$1 body
+  shift
+  body=$(stack_metrics "$name") || { echo null; return; }
+  awk -v specs="$*" '
+    BEGIN { n = split(specs, sp, " ")
+            for (i = 1; i <= n; i++) {
+              e = index(sp[i], "="); key[i] = substr(sp[i], 1, e - 1); rest = substr(sp[i], e + 1)
+              c = index(rest, ":"); met[i] = c ? substr(rest, 1, c - 1) : rest; want[i] = c ? substr(rest, c + 1) : "" } }
+    { for (i = 1; i <= n; i++)
+        if ($1 == met[i] || index($1, met[i] "{") == 1) { seen[i] = 1; if (want[i] == "" || index($0, want[i])) s[i] += $NF } }
+    END { out = ""
+          for (i = 1; i <= n; i++)
+            out = out (i > 1 ? ", " : "") "\"" key[i] "\": " (!seen[i] ? "null" : (s[i] == int(s[i]) ? sprintf("%.0f", s[i]) : sprintf("%.6g", s[i])))
+          print "{" out "}" }' <<<"$body"
+}
+
+# stack_counters_delta A B: {"KEY": B - A, ...} over two stack_counters
+# readings of the same process. A KEY null in A counts as 0 there (the
+# counter had no sample yet; a failed scrape makes all of A null instead),
+# and is null in the delta when B lacks it or either reading is null.
+stack_counters_delta() {
+  python3 -c 'import json, sys
+a, b = (json.loads(x) for x in sys.argv[1:3])
+if a is None or b is None:
+    print("null")
+else:
+    print(json.dumps({k: None if v is None else round(v - (a.get(k) or 0), 6) for k, v in b.items()}))' "$1" "$2" 2>/dev/null || echo null
+}
+
 # stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
 stack_pinned() {
   local have
```

- [ ] **Step 4: Run the tests**

Run: `make test-scripts 2>&1 | grep -E 'FAIL|counters|peak|passed|failed'`
Expected: no `FAIL`. The four `counters:` lines and `usage: peak RSS` are `ok`, and `lib_test`, `loadgen_test` and `stack_test` each print `all passed`.

- [ ] **Step 5: Check that the test catches a typo read as 0**

```bash
cp scripts/stack.sh /tmp/stack.sh.good
sed -i 's/(!seen\[i\] ? "null" : /(0 ? "null" : /' scripts/stack.sh
STACK_REDIS_SERVER=$(command -v redis-server || echo .cache/redis/bin/redis-server) scripts/stack_test.sh 2>&1 | grep -E '^FAIL|failed'
cp /tmp/stack.sh.good scripts/stack.sh && rm /tmp/stack.sh.good
```

Expected: `FAIL counters: one scrape as JSON: got [... 0 1 0 0], want [... 0 1 0 None]` and `stack_test: 1 failed`. `git diff --stat` then shows only Task 2's two files changed since Task 1.

- [ ] **Step 6: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh
git commit -m "feat(stack): counter readers and peak RSS for storm phases (#96)"
```

---

### Task 3: the Tier 2 `gwloss` scenario (b1), and Tier 2 run 20

**Files:**
- Create: `scripts/tier2/gwloss.sh`
- Modify: `Makefile` (scenario list comment)

**Interfaces:**
- Consumes: Task 1's counters; Task 2's `stack_counters`, `stack_counters_delta` and `rss_peak_mb`; `stack.sh` (`stack_build`, `stack_up`, `stack_busy_ports`, `stack_pinned`, `stack_snapshot`, `stack_profile`, `stack_expect_dead`, `stack_metrics`, `stack_metric_in`, `stack_usage_start`/`_json`, `stack_wait_idle`, `stack_down`, `stack_health`); `loadgen.sh` (`lg_start`, `lg_wait_online`, `lg_get`, `lg_post`, `lg_wait_back`, `lg_profile`, `lg_stop`, `lg_json`, `lg_py`, `lg_true`); `lib.sh` (`t2_*`, including `t2_cpustat_snap`/`t2_cpusets_json` and `t2_tcpstat_snap`/`t2_tcpstat_json`); loadgen's `POST /kill?gateway=&pid=&mark=` → `{"mark", "population"}` and `GET /slo?mark=&within=`.
- Produces: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss` → `tier2-results/gwloss-<ts>/result.json` with 13 checks and the `b1` field. 6b2a-stress adds the `b_stress` phase to this file.

- [ ] **Step 1: Write the scenario**

Create `scripts/tier2/gwloss.sh`:

```bash
#!/usr/bin/env bash
# Tier 2: gwloss (Plan 6b2a-b1), the spec's load test (b1) with SDK clients:
#   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
#        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
#        about one per relogin, with no retries (R2-O6);
#   storm: the other gateway never misses a lease renewal.
# Two gateways share the gateway CPU set (GOMAXPROCS 6 each, #79), with Go's
# default GC (6b1), and 2 x 200k SDK clients up from the start (#93): the
# survivor carries its own 200k through the storm and ends with 400k. Logic
# is one process throughout (minReplicas, no HPA), so the storm runs at
# Logic's minimum. Plan 6b2a-stress adds the b_stress phase (a 600k victim).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss
#   Results: tier2-results/gwloss-<timestamp>/ (result.json, b1/ with logs,
#   artifacts and loadgen results), plus ONE file to send back:
#   tier2-results/gwloss-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 8 minutes.
#
# Settings:
#   GWLOSS_CLIENTS     SDK clients over both gateways (default 400000)
#   GWLOSS_RATE        clients started per second (default 5000)
#   GWLOSS_CAP         each gateway's -max-conns (default 450000: the
#                      survivor's 400k plus room for clients in flight)
#   GWLOSS_SWEEP_WAIT  seconds after the kill to wait for the dead epoch's
#                      sweep (default 180: stale 60s after its last renewal,
#                      then 200k uids at the sweeper's 10k/s)
#   GWLOSS_SOURCES     loadgen's source addresses (default: loadgen's, one per
#                      5,000 connections per gateway, sized for the survivor)
#
# Checks:
#   b1_stack           redis, devtoken, Logic, gw-0 and gw-1 become Ready
#   pinning            gateways, Logic + Redis and loadgen on their CPU sets
#   b1_online          every client logs in, each gateway holds 45-55% of
#                      them, none gave up
#   b1_slo             loadgen SIGKILLs gw-1: 99% of its clients log in again
#                      within 30s of T_death (R4M-17)
#   b1_kicks           Kick first attempts in the storm, sent or dropped by a
#                      full queue (R5S-O2), are 0.9-1.1 per relogin, none is
#                      retried and none gives up as live: a refused Kick is
#                      not retried (R2-O6, #97)
#   b1_lease           the survivor gw-0 renews its lease every interval from
#                      the kill to the sweep: no call failed or found the
#                      lease missing, and Logic saw no renewal error
#   b1_swept           the sweeper finishes the dead epoch within GWLOSS_SWEEP_WAIT
#   no_swap_in_b1      no page swapped in from the kill to the sweep (#70)
#   no_lo_drops_in_b1  loopback dropped no packet from the ramp to the sweep
#   b1_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp and
#                      the storm (P6a2aI-4)
#   b1_idle            after loadgen stops, gw-0 reaches 0 connections and 0
#                      awaiting Logout within 300s
#   b1_processes       nothing but gw-1 died, panicked or stopped uncleanly
#   artifacts          storm CPU profiles, metrics before and after, loadgen's result
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init gwloss
t2_need curl taskset sudo python3
t2_require_host_setup
t2_mem_budget 14
t2_nofile 1048576

CLIENTS=${GWLOSS_CLIENTS:-400000} RATE=${GWLOSS_RATE:-5000} CAP=${GWLOSS_CAP:-450000}
SWEEP_WAIT=${GWLOSS_SWEEP_WAIT:-180} SOURCES=${GWLOSS_SOURCES:-}
export STACK_GATEWAYS=2 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# The storm's counters (Plan 6b2a): Logic's Kicks, lease renewals, sweeps
# and relogins onto a dead epoch; the survivor's own lease calls.
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  kick_first='goim_logic_kick_attempts_total:attempt="first"'
  kick_retry='goim_logic_kick_attempts_total:attempt="retry"'
  kick_giveups_dead='goim_logic_kick_giveups_total:target_lease="dead"'
  kick_giveups_live='goim_logic_kick_giveups_total:target_lease="live"'
  kick_dropped=goim_logic_kick_dropped_total
  dead_epoch_relogins=goim_logic_dead_epoch_relogin_seconds_count
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

# ---- b1: SIGKILL a 200k gateway ----
name=b1
export STACK_DIR=$T2_OUT/$name
mkdir -p "$STACK_DIR"
ln -sfn ../bin "$STACK_DIR/bin"
STACK_GATEWAY_ARGS=(-max-conns "$CAP")
t0=$SECONDS

busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$STACK_DIR/stack-up.err"
up=$?
t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# The ramp: every client up from the start, split by the balancer.
lo0=$(lo_dropped)
t2_cpustat_snap "$STACK_DIR/cpustat-0"
t2_tcpstat_snap "$STACK_DIR/tcpstat-0"
lg_start "$name" -mode sdk -clients "$CLIENTS" -rate "$RATE" -uid-prefix "$name" ${SOURCES:+-sources "$SOURCES"}
stack_usage_start loadgen="$LG_PID"
t=$SECONDS
online=false
lg_wait_online "$CLIENTS" $((CLIENTS / RATE * 3 + 60)) &&
  online=$(lg_get /stats 'd["online"] == '"$CLIENTS"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$CLIENTS"' * 0.45 and "gave-up" not in d["failed"]')
[ "$online" = true ] || online=false # null: /stats unreadable, a failed run, not a stopped one
ramp_s=$((SECONDS - t))
ramp=$(stack_usage_json)
t2_cpustat_snap "$STACK_DIR/cpustat-1"
t2_tcpstat_snap "$STACK_DIR/tcpstat-1"
by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
t2_check "${name}_online" "$online" "\"want\": $CLIENTS, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $by_gateway, \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"

wrong=""
for n in gw-0 gw-1; do stack_pinned "$n" "$T2_CPUS_GATEWAY" || wrong+=" $n"; done
for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
[ "$online" = true ] || t2_finish

# The storm: counters before, SIGKILL, CPU profiles of its first 30s.
stack_snapshot before
logic0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
gw0=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_record_start "$name"
t2_cpustat_snap "$STACK_DIR/cpustat-2"
t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
stack_usage_start loadgen="$LG_PID"
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - d)
kill_t=$SECONDS
stack_expect_dead gw-1 # in this shell: it reaps and forgets gw-1
dead=$?
(stack_profile storm 30 & lg_profile storm 30; wait) >/dev/null 2>&1 &
prof=$!
back=$(t2_bool lg_wait_back death 60)
back_s=$((SECONDS - kill_t))
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
# The dead epoch's sweep (2 x TTL after its last renewal, then its uids at
# the sweeper's rate): the storm's window ends when it is done.
swept=false
while [ $((SECONDS - kill_t)) -lt "$SWEEP_WAIT" ]; do
  d=$(stack_counters logic sweeps_done='goim_logic_sweeps_total:result="done"')
  [ "$(echo "$d" | lg_true - 'd["sweeps_done"] >= 1')" = true ] && { swept=true; break; }
  sleep 1
done
sweep_s=$((SECONDS - kill_t))
wait "$prof"
storm=$(stack_usage_json)
logic1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
gw1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_cpustat_snap "$STACK_DIR/cpustat-3"
t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
t2_record_end "$name"
lo1=$(lo_dropped)
stack_snapshot after
logic_d=$(stack_counters_delta "$logic0" "$logic1")
gw_d=$(stack_counters_delta "$gw0" "$gw1")
window_s=$(echo "$storm" | lg_py - 'd["window_s"]')

slo=$STACK_DIR/artifacts/slo-death.json
pop=$(lg_py "$slo" 'd["population"]')
pass=false
[ $dead -eq 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$CLIENTS"' * 0.45')
t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"slo\": $(cat "$slo")"
t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - 'd["d"]["kick_retry"] == 0 and d["d"]["kick_giveups_live"] == 0 and 0.9 * d["pop"] <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]')" \
  "\"relogins\": $pop, \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
# One renewal per 10s interval (the gateway's -lease-interval default): the
# window holds at least window_s / 10 - 1 whole intervals.
t2_check "${name}_lease" "$(echo "{\"g\": $gw_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'd["g"]["lease_error"] == 0 and d["g"]["lease_missing"] == 0 and d["g"]["lease_live"] >= d["w"] // 10 - 1 and d["l"]["lease_error"] == 0')" \
  "\"window_s\": $window_s, \"survivor\": $gw_d, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
t2_check "${name}_swept" "$swept" "\"sweep_s\": $sweep_s, \"wait_s\": $SWEEP_WAIT, \"sweeps\": $(echo "$logic_d" | lg_py - 'd["sweeps"]'), \"uids\": $(echo "$logic_d" | lg_py - 'd["sweep_uids"]'), \"deleted\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]')"
lo=null
[ "$lo0" = null ] || [ "$lo1" = null ] || lo=$((lo1 - lo0))
t2_check "no_lo_drops_in_$name" "$(t2_bool [ "$lo" = 0 ])" "\"dropped\": $lo, \"netdev_max_backlog\": $(cat /proc/sys/net/core/netdev_max_backlog 2>/dev/null || echo null)"
headroom=false
usage_ok "$ramp" && usage_ok "$storm" && headroom=true
t2_check "${name}_loadgen_headroom" "$headroom" \
  "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"storm_cores\": $(echo "$storm" | lg_py - 'd["loadgen"]["cores"]')"

# The end: the result is written before any client stops (#84), then gw-0
# drains to idle before the stack stops.
lg_stop
res=$STACK_DIR/artifacts/loadgen-$name.json
t=$SECONDS
idle=$(t2_bool stack_wait_idle 300 2>"$STACK_DIR/idle.err")
stop_s=$((SECONDS - t))
body=$(stack_metrics gw-0) || body=""
t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"done_logouts\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"dropped_logouts\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"'), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
stack_down
problems=$(stack_health)
t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
tcp="{\"ramp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-0" "$STACK_DIR/tcpstat-1"), \"storm\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3")}"
t2_field "$name" "{\"clients\": $CLIENTS, \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"storm\": $storm, \"back_s\": $back_s, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivor\": $gw_d, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
t2_duration "$name" $((SECONDS - t0))

# Artifacts: metrics before and after the storm, its CPU profiles, the SLO
# and loadgen's result.
missing=""
a=$STACK_DIR/artifacts
for f in before/logic before/gw-0 before/gw-1 after/logic after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
done
for f in logic gw-0 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm/$f.cpu"; done
[ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" slo-death.json"
[ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" loadgen-$name.json"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
  "b_stress, the 600k victim (Plan 6b2a-stress)" "b2 and slowed Logic (Plan 6b2b)"
t2_finish
```

Apply to `Makefile`:

```diff
diff --git a/Makefile b/Makefile
index 344c33a..f3be91d 100644
--- a/Makefile
+++ b/Makefile
@@ -71,7 +71,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, capacity, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, gwloss, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
```

Run: `chmod +x scripts/tier2/gwloss.sh && bash -n scripts/tier2/gwloss.sh && echo ok`
Expected: `ok`.

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, so check the script in a throwaway copy with the guard lowered, the root parts removed and the scale cut, as 6b1 dry-ran `capacity.sh`: 2 × 1,000 SDK clients at 500/s.

```bash
D=$(mktemp -d); cp scripts/tier2/gwloss.sh "$D/gwloss.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget 14$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/gwloss.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/gwloss.sh"
R=$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/gwloss.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 \
  GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=5000 bash "$D/gwloss.sh"; echo "exit $?"
```

Check that the `sed -i` lines changed what they should: `grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/gwloss.sh"` should show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype, ~2.5 min): `result.json` parses (`python3 -m json.tool tier2-results/gwloss-*/result.json >/dev/null`) and has 13 checks. All pass except possibly `no_swap_in_b1`, from the dev machine's own swapping (the prototype saw 0 pages in on its first run and 91 on its second). The exit code is 1 if a check fails, else 0. Values from the prototype:
- `b1_online`: `by_gateway` `{"gw-0": 1000, "gw-1": 1000}`.
- `b1_slo`: `population` 1000, `back` 1000, `from_mark.p99_ms` ≈ 19,700 (the SDK's 0–20s reconnect jitter).
- `b1_kicks`: `first` 1000 (plus `dropped`, which is null here), `retries` 0, `giveups_dead` 1000, `giveups_live` 0, `dropped` null (no drop, so no sample).
- `b1_lease`: `window_s` ≈ 60, `survivor.lease_live` 6, `lease_missing` 0, `lease_error` 0.
- `b1_swept`: `sweep_s` ≈ 60, `uids` 1000, `deleted` 0 (every victim relogged in, so their fields already point at `gw-0`).
- `b1_idle`: `stop_s` 0, `done_logouts` 2000.
- The `b1` field: `logic.logins_replaced` 1000, `logic.dead_epoch_relogins` 0 (see *Execution Notes*), and every process in `ramp`/`storm` has `rss_peak_mb` ≥ `rss_mb`, with `storm."gw-1"` null.

`b1/artifacts/` holds `before/{logic,gw-0,gw-1}.metrics`, `after/{logic,gw-0}.metrics`, `storm/{logic,gw-0,loadgen}.cpu.pb.gz`, `slo-death.json` and `loadgen-b1.json`. No process is left (`pgrep -f "$PWD/tier2-results/gwloss" || echo none`). Afterwards, remove the dry run's `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/gwloss.sh Makefile
git commit -m "feat(tier2): gwloss scenario, Tier 2 (b1) (#95)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-race-suite && make test-scripts && make test-load-probe && bash -n scripts/tier2/gwloss.sh`
Expected: all pass. Record the commits and the dry run in this plan's *Execution Notes*, record #95–#97 as decided in the roadmap (with the 6b2a-b1 status), and push the branch.

- [ ] **Step 5: Tier 2 run 20 (on the host)**

On the Tier 2 host, after `git fetch && git switch feat/plan6b2a-b1-gwloss`, with the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss` (~8 min). Logic changed, so the user also runs `SCENARIO=race` (the race suite at production scale, as in earlier Logic changes), then sends back both `.report.txt` files. Expected: `gwloss` 13/13, `race` passes. What to read from the run, for the *Execution Notes*, the roadmap and the README's *Measured results* (both languages):
- **(b1):** the SLO distribution from the mark (p50/p99/max) and from detection; `back_s`; Kick first attempts per relogin, give-ups, drops (`kick_dropped` non-null means the 1,024-slot queue overflowed at ~10k relogins/s); `sweep_s`, uids and deleted fields; the survivor's lease calls.
- **Storm cost:** Logic, Redis and `gw-0` cores in the storm against the ramp (5,000 logins/s), with the storm CPU profiles (`artifacts/storm/`). Read them knowing the profiles run inside the window.
- **For 6b2a-stress (#93):** loadgen's `rss_peak_mb` at 400k SDK clients (bytes per SDK client, which predicts b-stress's 600k at ~14.3GB), the survivor's `rss_peak_mb` at 400k, Redis's peak, and loadgen's cores in the ramp (headroom for a 600k SDK ramp).
- If a check fails: `b1_online` with `gave-up` → look at `failed` and `admission_rejects_total` in `before/gw-*.metrics`; `b1_kicks` with retries → check whether the Kick errors were refused (Logic's log) or timeouts; `b1_lease` → read `survivor` and Logic's `lease_renewals_total`; `b1_swept` → `guard_holds` (the mass-stale guard) and Logic's `sweeper:` log lines; `b1_loadgen_headroom` → lower `GWLOSS_RATE`.

---

## After this plan

- **Plan 6b2a-stress** (its own session): adds the `b_stress` phase to `gwloss.sh` (3 gateways, the 600k victim ramped alone, then `POST /up` with `Balancer.SetUp`), the memory check before the run, the loadgen backoff count, the logouts-settled check counted per running gateway in `stack_wait_idle` (P6b1I-4), and Redis CPU and p99 from `INFO`. It sizes loadgen's `GOMEMLIMIT` from run 20's `rss_peak_mb`, and reuses `stack_counters` with the same counter lists.
- **Plan 6b2b:** b2 (silent loss), where Kicks time out instead of being refused and keep their retries while the lease is live. Its Kick checks read the same counters.
- **R1-O7's dead-epoch cross-check** (see *Execution Notes*): to decide in the plan review whether it belongs to a later plan.

## Execution Notes

- **Prototype (2026-10-14, while writing the plan):** every block was written and run in a scratch worktree of `main` `7cb0295` (branch `proto-6b2a-b1`, commit `c88dd2a`). A render script then copied it into this plan, new files whole and modified files as `git diff` against `7cb0295`. Results on the dev machine:
  - `TestKickRefusedDoesNotRetry` failed first (`timed out waiting for give-up`), then passed.
  - `go test -race -count=1 ./...` green; the race suite's Kick tests green; `make test-scripts` green; Task 2 Step 5's mutant caught.
  - First dry run (before #97): 12 of 13 checks passed; `b1_kicks` failed with `first` 1000, `retries` 2049, `giveups_dead` 1000.
  - Second dry run (with #97): 12 of 13 passed. The failure was `no_swap_in_b1` (91 pages, the dev machine's own swap with 1GB available). `b1_kicks` showed `first` 1000, `retries` 0, `giveups_dead` 1000, `giveups_live` 0; SLO p99 19.7s from the mark; `sweep_s` 60.
- **Found while prototyping:**
  1. The Kick retries after a crash (#97, decided by the user).
  2. Logic counts a relogin that replaces a session as `logins_total{result="replaced"}`, not `ok`. The `b1` field records both.
  3. **`dead_epoch_relogin_seconds` (R1-O7's dead-epoch cross-check) stayed at 0** for all 1,000 relogins. Logic records it only when the old entry's epoch is already dead by its lease, and a crashed gateway's lease is live for up to 30s, which covers nearly every relogin (0–20s jitter). The spec's "time from gateway death to relogin, ±10s, no client data needed" is therefore not measured for a crash storm. Loadgen's SLO measures the same thing from client data, so b1 is not blocked. Raised for the plan review (not changed here).
  4. A counter family that was never incremented (`kick_dropped_total`) is absent from `/metrics`, so its delta is null. #96 documents this as "no sample".
- **Plan review (2026-10-14, Claude at the user's request, "review as usual"):** #96 as proposed. #95 has one amendment, made in the prototype (`proto-6b2a-b1` commit `c88dd2a`), dry-run, and copied back into Task 3 byte for byte. `b1_kicks` counted only first attempts that were sent. A full per-target queue (1,024) drops attempts before they are sent, and R5S-O2 allows that with Reconcile as the backstop, so at 200k relogins in ~20s a few drops would have failed the check for allowed behaviour while hiding the count. The check now counts `first + dropped` per relogin, with an absent `kick_dropped_total` read as 0 (Logic's scrape succeeded, so the family has no sample), and `dropped` stays in the check's fields. Re-verified with Task 3 Step 2's dry run: 13 checks, all passing except `no_swap_in_b1` (the dev machine's own swap). `b1_kicks` showed `first` 1000, `retries` 0, `giveups_dead` 1000; SLO p99 19.8s; `sweep_s` 61; no process left. The rendered plan was replayed on a fresh branch from `7cb0295` (Task 1's and Task 2's failing steps failed as written: `timed out waiting for give-up`, `stack_test: 5 failed`; the mutant was caught), and the tree matched the prototype byte for byte. **Considered and not changed:**
  - The storm's CPU profiles sit inside the window (a few percent of sampling cost, recorded as `profile_in_storm_s`), because the storm is only there.
  - The swap window covers only the storm. The ramp's memory peak shows in `rss_peak_mb`, and a ramp pushed into swap would fail `b1_online` or the headroom check.
  - The ±10% band holds clients whose relogin times out client-side and logs in again.
  - The `dead` label for refused give-ups (#97) keeps the label set the spec names (`live|dead`) rather than adding `refused`.

  **For the user (not decided here):** R1-O7's dead-epoch cross-check (*Found while prototyping* 3). It is not measured for crash storms, because nearly every relogin lands while the lease is still live. Options: (a) Logic records it on the refused Kick's give-up (#97), where it knows the target is gone; (b) accept that the client-side SLO is the only crash-storm measure and say so in the spec. The recommendation is to decide it in 6b2a-stress, which measures a 600k storm whose relogins outlast the TTL.
- **Execution (2026-10-14, inline, superpowers:executing-plans):** the plan's code was applied as written, with no rulings needed. Commits: `85bb353` (Task 1, #97; `TestKickRefusedDoesNotRetry` failed with `timed out waiting for give-up`, then passed; the logic and race-suite Kick tests are green), `5abf7d0` (Task 2, #96; `stack_test: 5 failed`, then all passed; the mutant was caught), and `f793371` (Task 3, #95). Dry run on the dev machine (2 × 1,000 SDK clients, 500/s): 13 checks, 12 pass, and only `no_swap_in_b1` failed (5 pages, the dev machine's own swap). `b1_kicks`: `first` 1000, `retries` 0, `giveups_dead` 1000, `giveups_live` 0, `dropped` null. SLO from the mark: p99 19.8s. `sweep_s` 60. `b1_lease`: 6 `live`, window 60.5s. No process was left. Branch check: `make test`, `make test-race-suite`, `make test-scripts`, `make test-load-probe` and `bash -n` all green.
- **Final review (2026-10-14, fresh reviewer):** no Critical or Important findings, and all five Review Focus items hold. Minors deferred to the user:
  1. No test covers "a timeout with a live lease keeps its retries". `TestKickToBlackholeEndsWithinDeadline` runs with no lease, so widening `refused()` would go unnoticed.
  2. SDK clients never record `gave-up` (only light clients do), so a relogin refused at the cap is not seen. Under 1% such relogins would still pass `b1_slo`. Review Focus 4's wording "b1_slo fails if any gave up" is imprecise. Possible fix: record the survivor's admission rejects in the storm delta.
  3. `b1_lease` allows `window_s // 10 − 1` renewals, so one missed renewal can pass; lease expiry is still caught by `missing == 0`. Possible fix: record the largest gap between renewals.
  4. The spec's row "Kick can't reach the gateway" still says refused Kicks are retried while the lease is live. #97 overrides that for a refused connect, and this should be recorded in the dispositions file or the next spec revision.
  5. `TestKickRefusedDoesNotRetry` relies on a closed `:0` port staying unbound, a very low flake risk under parallel `go test ./...`.
- **Interface review (2026-10-14, Claude; decided as proposed by the user, "yes"):** P6b2a-b1I-1..7, covering the final review's five minors. **Checked, unchanged:** #97 cannot hit a restarted gateway, because Logic connects to each epoch separately (`kick.go`: `kickTarget.cc`), so a new epoch on the same address does not inherit the old epoch's cached `connection refused`. A live gateway never refuses a Kick while it holds sessions, because `g.Start` opens the Kick listener before `tr.Start` accepts a client. Every metric name in `LOGIC_COUNTERS` and `GW_COUNTERS` matches `metrics.go`, including the recorded-only ones, where a typo would read null unnoticed. `VmHWM` precedes `VmRSS` in `/proc/PID/status`, so `m[1]` is the peak.
  - **P6b2a-b1I-1 (fixed, minor 1):** `TestKickTimeoutRetriesWhileLeaseLive`: a blackhole address with a live lease makes `Retries` retries and gives up `live`, with no `dead` give-up. A `refused()` widened to `DEADLINE_EXCEEDED` fails it (`timed out waiting for live give-up`), checked.
  - **P6b2a-b1I-2 (fixed, minor 2):** SDK clients never record `gave-up`, so `b1_online`'s `"gave-up" not in failed` could not fail, and Review Focus 4's "b1_slo fails if any gave up" did not hold. `b1_slo` now also requires the survivor's `admission_rejects_total{reason="at_capacity"}` delta over the storm to be 0 (`GW_COUNTERS.rejects_at_capacity`, shown in the check), and the vacuous test is removed from `b1_online`. Because the counter would have no sample before its first reject, which would read as null and fail a passing run, the gateway now exports every reject reason at 0 from the start (`NewMetrics`; `TestRejectReasonsExportedAtZero`, which fails without the fix).
  - **P6b2a-b1I-3 (fixed):** the sweep wait compared the total `sweeps_done` with 1. It now compares with `logic0`'s value, so a sweep from before the kill cannot end the storm window (harmless on b1's fresh stack, but 6b2a-stress reuses the loop).
  - **P6b2a-b1I-4 (accepted, minor 3):** `b1_lease` keeps `window_s // 10 − 1`. `missing == 0` catches an expired lease, and requiring all `window_s // 10` renewals would fail on how the 10s intervals line up with the window.
  - **P6b2a-b1I-5 (recorded, minor 4):** #97 is a follow-up in `objections/connection-session-core-design-rev5-dispositions.md`, for the next spec revision. It is not yet applied to the rev6 spec.
  - **P6b2a-b1I-6 (accepted, minor 5):** the closed `:0` port in `TestKickRefusedDoesNotRetry` stays. The flake risk is very low.
  - **P6b2a-b1I-7 (R1-O7, for 6b2a-stress Task 1):** option (a). On a refused Kick's give-up, the Kicker observes `now − RenewedMs(target)` in `dead_epoch_relogin_seconds`; the worker may read Redis. That measures death to relogin without client data, to within the spec's ±10s, during a crash storm whose relogins land while the lease is still live.
  - Verified: `go test -race` on `internal/logic` (Kick tests) and `internal/gateway/...` green. Dry run as Task 3 Step 2 (2 × 1,000): 13 checks, 12 pass; only `no_swap_in_b1` failed (8 pages, the dev machine's own swap). `rejects_at_capacity` 0, `b1_kicks` `first` 1000 / `retries` 0, `sweep_s` 61. Negative dry run with `GWLOSS_CAP=1500`: the survivor rejected 7,002 at capacity, and `b1_slo` failed. No process was left.
- **Tier 2 run 20 (2026-10-14, `ca57fee`, dirty 0): `gwloss` 12/13, `b1_kicks` failed; `race` passed** (58/58, mutation caught). The SLO passed: 200,000 victims all back, from the mark p50 12.0s, p99 23.0s, max 31.0s; detection p50 0.88s, p99 1.62s (the 200k-socket process takes ~1.6s to exit). `b1_kicks`: `first` 194,689 + `dropped` 5,311 = 200,000 exactly, but `retries` 8,612; every sent first attempt ended in one `dead` give-up, `giveups_live` 0. Logic's `kick_attempt_seconds`: 203,173 of 203,301 attempts under 4ms, 128 at 0.25–1.0s, so the retries were fast non-refused errors, and the 128 slow ones (two rounds of 64 workers) filled the queue. Lease: 8 `live`, none missing; swept 83s after the kill (200,000 uids, 0 fields deleted: every victim had relogged in). The survivor rejected 185,409 handshakes as `handshake_rate` (3 concurrent handshakes at 6 loops against ~10k relogins/s): the storm's `pre-handshake` failures (186,641), retried by the clients, inside the SLO; login p99 1.2s. `TCPTimeWaitOverflow` 253,831 in the storm. Loadgen RSS peak 10.6GB (ramp 9.4GB) at 400k SDK clients; survivor 891MB at 400k; Logic 0.44 cores ramp, 0.22 storm; Redis 0.07.
- **Rerun with Kick error logs (2026-10-14, `2e94429`, 12/13: `b1_kicks` again, 8,310 retries, 5,233 drops; `no_swap_in_b1` also failed: 182 pages in, 6,043 out, loadgen peak 11.5GB):** Logic logged, in order: `DeadlineExceeded: context deadline exceeded while waiting for connections to become ready` (:32, kill at :31.9), `Unavailable: … error reading server preface: connection reset by peer` (:33), then `connection refused` (:34). The victim's Kick listener lived ~1.5s in the kernel while its sockets were torn down: connects completed, nothing answered, the listener closed and reset them, and gRPC failed the next second of Kicks fast with that cached error, none matching `refused()`. **#97 amended (the user, "yes"; roadmap #97):** (a) `refused()` also matches a reset before the server preface (`5a11677`; `TestKickResetBeforePrefaceDoesNotRetry`, a listener that never accepts and closes with the connect queued, reproduces the run's exact error; without the clause it fails); (b) the teardown's timeouts keep their retries, so `b1_kicks` now requires retries ≤ first-attempt timeouts, every retry refused, no `live` give-up; (c) `kick_attempts_total{result}` is `ok|refused|timeout|error` (tests updated; the race suite's injected `UNAVAILABLE` stays `error`). Logic also logs each distinct Kick error once per target (`2e94429`). Dry run (2 × 1,000): 13 checks, only `no_swap_in_b1` failed (dev machine); `b1_kicks` `first_timeouts` 0, `retries` 0. Expected in run 21: ~128 retries (the teardown's timeouts, each then refused), drops about as before (~5k: the 1s blocking remains), `b1_kicks` passing. **For 6b2a-stress:** loadgen 10.6–11.5GB at 400k (26.5–28.7KB per SDK client), so 600k needs ~17GB of 26GB; the survivor's handshake limit shapes a 600k storm's relogins.
