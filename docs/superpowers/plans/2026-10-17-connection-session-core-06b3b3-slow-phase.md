# Plan 6b3b3: The Slow Phase on Its Own CPUs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `floods`' slow phase a valid test on the Tier 2 host, then rerun the scenario as run 30c. This is #126's follow-up to run 30b, which failed 28/30 on `slow_legit` and `slow_heartbeats` with loadgen's CPU set saturated.

**Architecture:** One script, `scripts/tier2/floods.sh`, changes, and only in its slow phase:
- **The floods move.** The slow-handshake instance is pinned to Logic's CPU set (`FLOODS_SLOW_HS_CPUS`, default `T2_CPUS_LOGIC`), shared with Logic and Redis. The slow-auth instance is pinned to the gateways' set (`FLOODS_SLOW_AUTH_CPUS`, default `T2_CPUS_GATEWAY`), shared with the gateways. Each gets a small GOMAXPROCS (`FLOODS_SLOW_HS_PROCS` 2, `FLOODS_SLOW_AUTH_PROCS` 4), not one per CPU of its set. Loadgen's 8 CPUs are left to the light clients.
- **A per-second series.** From the light clients' start to the end of the hold, a sampler writes one JSON line a second to `slow-series.jsonl`. Each line holds the three loadgens' CPU and the three CPU sets' busy share. It holds the light instance's online, gave-up and failed counts. Per gateway, it holds the run-queue delay summed over its threads, plus the pending sockets, `pending_mem` rejects, `no_slot` refusals, logins and heartbeat closes. `slow.series` turns it into rows plus `first_s`, the second each sign of trouble first showed, so cause and effect can be ordered.
- **A verdict.** The verdict reads loadgen's, the gateways' and Logic's CPU sets over two windows, the light clients' ramp and the hold. It is `valid`, `loadgen-saturated`, `gateway-set-saturated`, `logic-set-saturated` or `unread`. `slow_legit` and `slow_heartbeats` pass only when it is `valid`. Any other verdict fails them, labelled, with a `not_covered` line. The verdict names the first saturated set. The order in `first_s` says which came first, and the ruling follows that order.

**Tech Stack:** bash 5.2, python3 (JSON and arithmetic only), `taskset`, `/proc/stat`.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Verification → Load tests* (~l.546–548): the slow-client flood bullet, which `floods`' slow phase runs (#122).
- *Storms, drain, failures* (~l.425–429), row *Slow clients*: legitimate clients are not refused beside slow ones. `slow_legit` gates this: no Busy, no `login_queue`, `pending_mem` or `at_capacity` reject, no bucket refusal.
- *Admission control* (~l.199): `PendingCap`, the HANDSHAKE + AUTH_PENDING sockets per gateway (100k), which `pending_mem` rejects enforce.

Inputs:
- Plan 6b3b2 (`2026-10-16-connection-session-core-06b3b2-run30-followups.md`): its *Execution Notes*, with run 30b's record and its reading of the slow phase.
- The roadmap's #122 (the `floods` scenario), #124 (`storm_rate`'s verdict, which this plan's verdict follows in form) and #126.

**Decisions:**
- #126 (the user, 2026-10-17) made this plan. Its three parts: the slow floods get CPUs of their own rather than sharing loadgen's 8, a per-second series for the slow phase, and a loadgen-saturation verdict like `storm_rate`'s.
- Before writing, the user ruled on two open points (2026-10-17, with AskUserQuestion). They are **#127**, decided as amended by the review (2026-10-17, O1–O5 accepted; `docs/superpowers/objections/connection-session-core-06b3b3-slow-phase.md` and `-dispositions.md`):
  1. **Where the floods run:** written as both floods on the gateway set. **Amended by review O1:** slow-handshake (0.83 cores in run 30b, no DH) runs on Logic's set and only slow-auth on the gateway set (Review Focus 1 has the arithmetic). The alternatives were carving the floods 2 cores out of the gateway set, which changes the gateways under test (6b3b2's O5 argument), and Logic's set for both floods, which is too small at 4 CPUs.
  2. **What a saturated verdict does:** `slow_legit` and `slow_heartbeats` **fail**, labelled with the verdict. They do not pass the way `storm_rate`'s `loadgen-bound` does. Both checks gate gateway-side behaviour (`pending_mem`, heartbeat closes), and passing them while loadgen was the bound could hide a real gateway problem.

**Branch:** `feat/plan6b3`. Execute from its head, `a082079` or later (this plan's own commit).

**Size:** 2 tasks, about 205 lines added to one file. The prototype `proto-6b3b3` (`4ba4eba`, from `a082079`; the review's dispositions in `f338cbe`) ran every block. Its diff is replayed here byte for byte as one `git apply` patch. On the dev machine it passed `make test-scripts` and `bash -n`. Its full `floods` dry run had 30 checks, all passing except a dev-machine swap check (`no_swap_in_slow` in the last run, `no_swap_in_storm` before it). A forced `loadgen-saturated` run failed both slow checks with the label and the `not_covered` line. In that run each flood ran with its own GOMAXPROCS and CPU list, read from `/proc`. The other verdicts were each reached on JSON, and the settings check refused out-of-set CPU lists.

## Global Constraints

- `make test`, `make test-scripts` and `go vet ./...` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Scripts: bash with `set -uo pipefail`, JSON without jq, python3 only for reading JSON and arithmetic. `t2_bool A || B` runs `||` outside `t2_bool`, so compound conditions go through `lg_true`.
- `floods`' phases, sizes and defaults are unchanged (#122), and so are the storm and garbage phases. The check count stays at 30: the verdict gates two existing checks and adds none.
- No Go change. No change to the gateway's flags, limits or `PendingCap`. If the slow phase is valid and still shows `pending_mem` rejects, that is a finding for the record and the user, not something this plan tunes.
- The 85% busy threshold is the one `storm_rate` uses (#124). Busy means user, nice, system, irq, softirq and steal, which is `100 - idle_pct` in `t2_cpusets_json`, idle and iowait being the rest.

## Review Focus

1. **The floods share the gateways' and Logic's CPUs (review O1, O2).** In run 30b the floods used 3.5 Zen 5 cores: slow-auth 2.68, slow-handshake 0.83. Zen 5c runs at 3.29 GHz against 5.16, so the same work costs about 1.57 times as many CPUs there, and more on busy SMT pairs. Run 30b's 69.5% idle gateway set was a 244s average with 90k of 400k online, so it says little about a healthy ramp. In a healthy ramp the gateways take the light clients' 10k handshakes/s plus slow-auth's ~7k/s. At run 30b storm's ~0.26 CPU per 1k handshakes/s (gw-0 5.31 CPUs at 20.8k/s seen), that is ~4.4 CPUs. Both floods there would add ~5.5, about 10 of 12 (~83%) against the 85% threshold. Slow-auth alone adds ~4.2, about 72%. Slow-handshake's ~1.3 CPUs go to Logic's set (94.6% idle in run 30b, ~3.8 CPUs free). The floods' GOMAXPROCS (2 and 4) keeps their threads from crowding the gateways' 2 × 6 Ps. Without it, each would run 12 Ps on the 12-CPU mask. Interference below 85% shows as run-queue delay and longer slot holds, not busy share. The series' `gw_runq_ms_per_s` (the larger gateway's wait per second over its threads) and `flood2_cores` beside `gateway_set_busy_pct` show it. The record reads them before calling a gateway refusal the gateway's own.
2. **The light instance alone may still saturate loadgen.** Run 30b's light instance used 4.42 cores, retries included. Run 30's storm saturated the same 8 CPUs at about 16k new clients/s. The slow phase ramps 400k at 10k/s and also Pings, which should fit, but a retry cascade would not. In that case the verdict is `loadgen-saturated` and both checks fail, labelled. That is a valid outcome of run 30c. What it means depends on the order in `slow.series.first_s` (review O3), and the user rules on it. If loadgen saturated before any gateway refusal, the slow phase needs a smaller light population or more loadgen CPUs. If a `pending_mem` refusal, a rise in `pending_max` or a jump in `lg_refused_per_s` came first, the retries that followed it saturated loadgen, and it is a possible gateway finding. `first_s.no_slot` and `first_s.light_refused` don't order it: the concurrency limit's loss (#111) is there from the first seconds of a healthy run, slow-auth's included (P6b3b3I-1).
3. **The window average can hide a burst.** The verdict reads the ramp and the hold as two windows. A 10s spike inside a 40s ramp can stay under 85% on average and give `valid`. The series reads per second. `first_s.loadgen_saturated` non-null under a `valid` verdict means "read the rows around it", above all when `slow_legit` fails. A per-second gate would be too noisy (the dev machine's first second is 93% busy), so it is recorded, not gated.
4. **The sampler's own cost and timing.** Each second it scrapes both gateways' `/metrics`, asks the light instance for `/stats` with a 1s timeout, and reads `/proc/stat`, three `/proc/PID/stat` files and each gateway's `/proc/PID/task/*/schedstat`. With `STACK_PIN=1` it is pinned to Logic's set (94.6% idle in run 30b), off the gateways' and loadgen's. Slow-handshake's ~1.3 CPUs now share that set. The sampler costs a few tens of ms per second, and `logic_set_busy_pct` shows the set's total. The clock and CPU readings are taken together, first. The scrapes follow within about 1s, so events less than about a second apart can't be ordered. An unanswered `/stats` gives a row with `lg_online` null, and `first_s.stats_unanswered` records when that first happened. That is itself a saturation sign: run 30b's pinger fell 123s behind.
5. **`pending_mem` in a valid run.** About 60k flood sockets sit pending on each gateway (120k over two) against `PendingCap` 100k. Legitimate clients in HANDSHAKE or AUTH_PENDING add to that. A healthy loadgen completes a handshake in milliseconds, so the legitimate pending count should be in the hundreds. A starved one leaves its clients pending for seconds, which at 5k/s per gateway fills the remaining 40k. The series' `pending_max` (the larger gateway's HANDSHAKE + AUTH_PENDING) against `pending_mem_per_s` shows which happened. A valid run with `pending_mem` rejects is a candidate finding against the spec's *Slow clients* row. It is read against `gw_runq_ms_per_s` first (review O2). A refusal that comes with a high run-queue delay may be the co-located flood's doing, and needs a rerun without it before it becomes a spec question. The record names it, and the user decides (e.g. `PendingCap` sizing, a spec follow-up).

## Implementation choices this plan makes

1. **The floods' CPU lists and GOMAXPROCS** (#127.1 as amended, review O1, O2, O5). `FLOODS_SLOW_HS_CPUS` defaults to `T2_CPUS_LOGIC` and `FLOODS_SLOW_AUTH_CPUS` to `T2_CPUS_GATEWAY`. Each must be a CPU list whose CPUs all lie inside one of the three sets, so the verdict reads every CPU a flood runs on. Anything else is refused at the start. `FLOODS_SLOW_HS_PROCS` (2) and `FLOODS_SLOW_AUTH_PROCS` (4) are run 30b's cores times 1.57, rounded. The floods take them through `lg_start`'s existing `LG_CPUS` and `LG_ENV`: `GOMAXPROCS=N` is appended to `LG_ENV` for each flood and removed afterwards, so the light instance is unchanged. `loadgen.sh` doesn't change. The garbage phase's flood stays on loadgen's set: garbage passed in run 30b, and moving it would change a passing phase. `slow.flood_cpus` and `slow.flood_procs` record both.
2. **The verdict's windows** are the light clients' ramp (from `lg_start slow-light` until they settle) and the hold. Three `t2_cpustat_snap` snapshots (`cpustat-light0/1/2`) feed `t2_cpusets_json` for each window, recorded as `slow.window.ramp` and `slow.window.hold`. The order of the tests is `unread` first (a snapshot missing), then `loadgen-saturated`, `gateway-set-saturated`, `logic-set-saturated`, then `valid`. Loadgen comes first because its saturation invalidates both checks' readings, whatever the gateways did. The order picks the label, not the cause: retries after gateway refusals saturate loadgen too, so a non-valid verdict is ruled on by the order in `first_s` (review O3).
3. **The verdict fails, labelled** (#127.2). `slow_legit` and `slow_heartbeats` each carry `verdict` and pass only with `"valid"`. Any other verdict writes the `not_covered` line `slow: legitimate logins and heartbeats beside the floods; verdict "<verdict>", ruled by the order in slow.series.first_s (slow.window, slow.series)`. `heartbeats` gains an optional 4th argument for it. The garbage phase passes none, so `garbage_heartbeats` is unchanged and has no `verdict` member.
4. **The series' columns** (`slow.series.columns`) are: `t_s`, `lg_online`, `lg_gave_up`, `lg_failed_per_s`, `light_cores`, `flood_cores`, `flood2_cores`, `loadgen_set_busy_pct`, `gateway_set_busy_pct`, `logic_set_busy_pct`, `gw_runq_ms_per_s`, `pending_max`, `online_conns`, `pending_mem_per_s`, `no_slot_per_s`, `logins_ok_per_s`, `closes_heartbeat_per_s`. `gw_runq_ms_per_s` is the larger gateway's run-queue wait in ms per second, summed over its threads (`/proc/PID/task/*/schedstat`, field 2; `/proc/PID/schedstat` is the main thread only). It is recorded, not gated: no threshold has a basis yet. There is one row per interval between samples, about 250 in run 30c's slow phase (~20KB in the report). The raw lines stay in `artifacts/slow-series.jsonl`, which the `artifacts` check requires. `first_s` holds `loadgen_saturated`, `gateway_set_saturated`, `logic_set_saturated`, `stats_unanswered`, `pending_mem`, `no_slot`, `gave_up` and `closes_heartbeat`, each in seconds since the light clients' start, or null if it never happened.

## File Structure

- `scripts/tier2/floods.sh`: the header (settings and checks), the floods' settings and their validation (`in_set`), `heartbeats`' verdict argument, the new `set_ticks`, `runq_ns`, `SERIES_COUNTERS`, `slow_sampler`, `slow_series` and `slow_verdict`, and `phase_slow` (Task 1).
- This plan's *Execution Notes*, the roadmap, and Plan 6b3b2's *Execution Notes* (Task 2).
- `floods.sh` has no unit tests (as in 6b3b and 6b3b2). It is checked by the dry run and the forced cases in Task 1 Step 3.

---

### Task 1: `floods`: the slow floods off loadgen's set, the series and the verdict

**Files:**
- Modify: `scripts/tier2/floods.sh` (header, settings check, `heartbeats`, new helpers before the build, `phase_slow`)

**Interfaces:**
- Consumes: `t2_cpustat_snap`, `t2_cpusets_json`, `t2_cpulist_expand`, `T2_CPUS_GATEWAY`/`T2_CPUS_LOGIC`/`T2_CPUS_LOADGEN` (`lib.sh`). `lg_start`'s `LG_CPUS` and `LG_ENV`, `lg_in`, `lg_get` with `LG_CURL_TIMEOUT`, `LG_PIDS`, `lg_py`/`lg_true` (`loadgen.sh`). `stack_ticks`, `stack_admins`, `stack_counters`, `STACK_PIDS` (`stack.sh`). `/proc/PID/task/*/schedstat`. The gateway's `goim_gateway_connections{state}`, `goim_gateway_admission_rejects_total{reason="pending_mem"}`, `goim_gateway_handshakes_total{result="no_slot"}`, `goim_gateway_logins_total{result="ok"}` and `goim_gateway_closes_total{reason="heartbeat"}`.
- Produces: the settings `FLOODS_SLOW_HS_CPUS`, `FLOODS_SLOW_AUTH_CPUS`, `FLOODS_SLOW_HS_PROCS`, `FLOODS_SLOW_AUTH_PROCS`. `slow_legit.verdict` and `slow_heartbeats.verdict` (JSON strings: `"valid"`, `"loadgen-saturated"`, `"gateway-set-saturated"`, `"logic-set-saturated"`, `"unread"`). `slow.flood_cpus` and `slow.flood_procs` (`{"slowhs", "slowauth"}`), `slow.verdict`, `slow.window.{ramp,hold}` (each `t2_cpusets_json`), `slow.series` (`columns`, `rows`, `samples`, `first_s`) and `artifacts/slow-series.jsonl`. `heartbeats PHASE N DELTA [VERDICT]`. Still 30 checks.

- [ ] **Step 1: Apply the change**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index bee907d..d5dbdf0 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -24,6 +24,15 @@
 #       400k light clients log in beside them and Ping: no Busy, no reject,
 #       no bucket refusal (slow-auth's ~7k handshakes/s fit the buckets,
 #       6b3a Review Focus 5); the concurrency limit's refusals are recorded.
+#       The floods run off loadgen's CPU set: in run 30b the three loadgens
+#       saturated loadgen's 8 CPUs while the gateway set sat 69.5% idle and
+#       Logic's 94.6% (#126). Slow-handshake (no DH) runs on Logic's set,
+#       slow-auth on the gateways', each with a small GOMAXPROCS so its
+#       threads don't crowd the gateways' (#127, 6b3b3 review O1, O2). A
+#       per-second series from the light clients' start orders cause and
+#       effect (the CPU sets, the gateways' run-queue delay, pending
+#       sockets and refusals, logins), and a verdict on the three CPU sets
+#       tells a saturated run from a gateway failure.
 # Logic is one process (minReplicas, no HPA), unthrottled.
 #
 # Usage (on the Tier 2 host, from the repo root), after the one-time
@@ -56,6 +65,13 @@
 #   FLOODS_SLOW_AUTH       slow: slow-auth connections (default 40000)
 #   FLOODS_SLOW_RATE       slow: each flood's connections started per second (default 10000)
 #   FLOODS_SLOW_HOLD       slow: seconds held once the light clients are online (default 60)
+#   FLOODS_SLOW_HS_CPUS    slow: the CPU list the slow-handshake flood is pinned
+#                          to (default T2_CPUS_LOGIC, shared with Logic and
+#                          Redis, #127); inside one of the three CPU sets
+#   FLOODS_SLOW_AUTH_CPUS  slow: the slow-auth flood's (default
+#                          T2_CPUS_GATEWAY, shared with the gateways)
+#   FLOODS_SLOW_HS_PROCS   slow: the slow-handshake flood's GOMAXPROCS (default 2)
+#   FLOODS_SLOW_AUTH_PROCS slow: the slow-auth flood's GOMAXPROCS (default 4)
 #
 # Checks (per phase run):
 #   storm_stack           redis, devtoken, Logic and the gateway become Ready
@@ -128,8 +144,22 @@
 #                         slots, at_capacity) and no bucket refusal
 #                         (no_token); the concurrency limit's (no_slot,
 #                         retried as in storm) and loadgen's failed are
-#                         recorded
-#   slow_heartbeats, slow_lease   as garbage's, over the floods
+#                         recorded. Only a valid run passes: verdict
+#                         "valid" when none of loadgen's, the gateways' and
+#                         Logic's CPU sets was >= 85% busy over the light
+#                         clients' ramp or over the hold, else
+#                         "loadgen-saturated", "gateway-set-saturated" or
+#                         "logic-set-saturated" (the floods share the last
+#                         two), which fails with a not_covered line (#126:
+#                         run 30b's pending_mem rejects came with a starved
+#                         loadgen, and which came first is unknown). The
+#                         verdict names the first saturated set, not the
+#                         cause: retries after gateway refusals saturate
+#                         loadgen too, so the order in slow.series.first_s
+#                         rules (6b3b3 review O3)
+#   slow_heartbeats       as garbage's, over the floods, and only with
+#                         verdict "valid", as slow_legit
+#   slow_lease            as garbage's, over the floods
 #   no_swap_in_slow       no page swapped in from the floods' start to their stop
 #   <phase>_conntrack     the host's nf_conntrack_count, sampled every second
 #                         from the stack up to its stop, peaked under 80% of
@@ -142,6 +172,7 @@
 #                         connections and settles its Logouts
 #   <phase>_processes     nothing died, panicked or stopped uncleanly
 #   artifacts             metrics before and after each phase's window, the
+#                         slow phase's series (slow-series.jsonl), the
 #                         garbage flood's CPU profiles, loadgen's results
 # Each phase also records its CPU sets' shares and softnet drops over its
 # window (cpu_sets: run 30's garbage SYN loss had none snapshotted), and
@@ -166,9 +197,23 @@ for p in $PHASES; do
 done
 for v in FLOODS_STORM_CLIENTS FLOODS_STORM_RATE FLOODS_CLIENTS FLOODS_RATE FLOODS_HEARTBEAT \
   FLOODS_GARBAGE_RATE FLOODS_GARBAGE_INFLIGHT FLOODS_GARBAGE_SECONDS FLOODS_GARBAGE_BUCKET \
-  FLOODS_SLOW_HANDSHAKE FLOODS_SLOW_AUTH FLOODS_SLOW_RATE FLOODS_SLOW_HOLD; do
+  FLOODS_SLOW_HANDSHAKE FLOODS_SLOW_AUTH FLOODS_SLOW_RATE FLOODS_SLOW_HOLD \
+  FLOODS_SLOW_HS_PROCS FLOODS_SLOW_AUTH_PROCS; do
   case ${!v-1} in ''|*[!0-9]*|0*) echo "floods: $v must be a positive whole number" >&2; exit 1 ;; esac
 done
+# in_set LIST SET: every CPU of LIST is in SET.
+in_set() {
+  local c set=" $(t2_cpulist_expand "$2") "
+  for c in $(t2_cpulist_expand "$1"); do [[ $set == *" $c "* ]] || return 1; done
+}
+# The slow floods' CPU lists, each inside one of the three sets, so the
+# verdict reads every CPU a flood runs on (6b3b3 review O5).
+HS_CPUS=${FLOODS_SLOW_HS_CPUS:-$T2_CPUS_LOGIC} AUTH_CPUS=${FLOODS_SLOW_AUTH_CPUS:-$T2_CPUS_GATEWAY}
+for l in "$HS_CPUS" "$AUTH_CPUS"; do
+  [[ $l =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] &&
+    { in_set "$l" "$T2_CPUS_GATEWAY" || in_set "$l" "$T2_CPUS_LOGIC" || in_set "$l" "$T2_CPUS_LOADGEN"; } ||
+    { echo "floods: FLOODS_SLOW_HS_CPUS and FLOODS_SLOW_AUTH_CPUS must each be a CPU list inside T2_CPUS_GATEWAY, T2_CPUS_LOGIC or T2_CPUS_LOADGEN" >&2; exit 1; }
+done
 [ "${FLOODS_GARBAGE_SECONDS:-90}" -ge 45 ] || { echo "floods: FLOODS_GARBAGE_SECONDS must be at least 45 (the flood's profile)" >&2; exit 1; }
 [ "${FLOODS_HEARTBEAT:-30}" -ge 30 ] && [ "${FLOODS_HEARTBEAT:-30}" -le 300 ] ||
   { echo "floods: FLOODS_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
@@ -423,18 +468,143 @@ lease() {
     "\"window_s\": $3, \"live_floor\": $floor, \"gateways\": $(echo "$2" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic_lease_error\": $(echo "$4" | lg_py - 'd["lease_error"]')"
 }
 
-# heartbeats PHASE N DELTA: checks PHASE_heartbeats over the light result of
-# N clients online (instance main, stopped) and the gateways' counter delta.
+# heartbeats PHASE N DELTA [VERDICT]: checks PHASE_heartbeats over the light
+# result of N clients online (instance main, stopped) and the gateways'
+# counter delta; with VERDICT (slow_verdict's, a JSON string) only "valid"
+# passes.
 # The gateway closes a client 2 x interval + 10s after its last frame, so a
 # Ping more than the interval + 5s late is a pinger that fell behind.
 # Pongs >= N is a floor over the whole light run, not per client: pong_missing
 # and lost catch a Ping left unanswered in the flood (O5).
 heartbeats() {
-  local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json
-  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3}" | lg_true - 'd["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values()) and d["r"]["ping_gap_max_ms"] <= '"$(((HB + 5) * 1000))")" \
-    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]'), \"ping_send_timeouts\": $(lg_json 'd["ping_send_timeouts"]')"
+  local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json v=${4:-null}
+  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3, \"v\": $v}" | lg_true - 'd["v"] in (None, "valid") and d["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values()) and d["r"]["ping_gap_max_ms"] <= '"$(((HB + 5) * 1000))")" \
+    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]'), \"ping_send_timeouts\": $(lg_json 'd["ping_send_timeouts"]')${4:+, \"verdict\": $4}"
+}
+
+# set_ticks LIST: "busy total" clock ticks of LIST's CPUs since boot
+# (/proc/stat; busy: user, nice, system, irq, softirq, steal; total adds
+# idle and iowait), as t2_cpusets_json counts them.
+set_ticks() {
+  awk -v cpus=" $(t2_cpulist_expand "$1") " '$1 ~ /^cpu[0-9]+$/ && index(cpus, " " substr($1, 4) " ") {
+    b += $2 + $3 + $4 + $7 + $8 + $9; t += $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 } END { print b + 0, t + 0 }' /proc/stat
+}
+
+# runq_ns PID: nanoseconds PID's threads have waited on a run queue
+# (/proc/PID/task/*/schedstat, field 2; the process's own schedstat is its
+# main thread only); empty if PID is gone.
+runq_ns() {
+  [ -d "/proc/$1/task" ] || return 0
+  cat /proc/"$1"/task/*/schedstat 2>/dev/null | awk '{ s += $2 } END { if (NR) printf "%.0f\n", s }'
+}
+
+# slow_sampler FILE LIGHT FLOOD FLOOD2: appends one JSON line a second to
+# FILE until killed (#126): {"ms", "ticks": [the three loadgens' CPU
+# ticks], "sets": {"loadgen", "gateway", "logic": [busy, total]}, "lg":
+# the light instance's {"online", "gave_up", "failed"} (null when /stats
+# does not answer within 1s: a saturated loadgen answers late), "gw":
+# {gateway: SERIES_COUNTERS plus "runq_ns"}}. The clock, CPU and run-queue
+# readings come first, side by side; the scrapes after them may trail by
+# up to ~1s.
+SERIES_COUNTERS=(
+  handshake='goim_gateway_connections:state="handshake"'
+  auth_pending='goim_gateway_connections:state="auth_pending"'
+  online='goim_gateway_connections:state="online"'
+  pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
+  no_slot='goim_gateway_handshakes_total:result="no_slot"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
+)
+slow_sampler() {
+  local ms ticks sets lg gw g p t
+  local -A rq
+  while :; do
+    ms=$(date +%s%3N) ticks=""
+    for p in "$2" "$3" "$4"; do t=$(stack_ticks "$p"); ticks+="${ticks:+, }${t:-null}"; done
+    sets="\"loadgen\": [$(set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
+    rq=()
+    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do rq[$g]=$(runq_ns "${STACK_PIDS[$g]}"); done
+    lg=$(LG_CURL_TIMEOUT=1 lg_get /stats '{"online": d["online"], "gave_up": d["failed"].get("gave-up", 0), "failed": sum(d["failed"].values())}')
+    gw=""
+    for g in "${!rq[@]}"; do
+      t=$(stack_counters "$g" "${SERIES_COUNTERS[@]}")
+      [ "$t" = null ] || t="${t%\}}, \"runq_ns\": ${rq[$g]:-null}}"
+      gw+="${gw:+, }\"$g\": $t"
+    done
+    echo "{\"ms\": $ms, \"ticks\": [$ticks], \"sets\": {$sets}, \"lg\": ${lg:-null}, \"gw\": {$gw}}" >>"$1"
+    sleep 1
+  done
+}
+
+# slow_series FILE: slow_sampler's lines as {"columns", "rows" (one per
+# interval between samples, t_s since the first; a column null where a
+# reading is missing), "first_s" (when each sign of trouble first showed,
+# null if never: a CPU set at >= 85% busy, /stats unanswered, a
+# pending_mem reject, a no_slot refusal, a client given up, a heartbeat
+# close), "samples"}; null if unreadable. gw_runq_ms_per_s is the larger
+# gateway's run-queue wait per second, summed over its threads: recorded,
+# not gated (6b3b3 review O2).
+slow_series() {
+  python3 -c 'import json, os, sys
+rows = []
+for l in open(sys.argv[1]):
+    try:
+        rows.append(json.loads(l))
+    except ValueError:
+        pass
+hz = os.sysconf("SC_CLK_TCK")
+cols = ["t_s", "lg_online", "lg_gave_up", "lg_failed_per_s", "light_cores", "flood_cores", "flood2_cores",
+        "loadgen_set_busy_pct", "gateway_set_busy_pct", "logic_set_busy_pct", "gw_runq_ms_per_s", "pending_max",
+        "online_conns", "pending_mem_per_s", "no_slot_per_s", "logins_ok_per_s", "closes_heartbeat_per_s"]
+def busy(a, b, k):
+    x, y = a["sets"].get(k, []), b["sets"].get(k, [])
+    return round(100 * (y[0] - x[0]) / (y[1] - x[1]), 1) if len(x) == 2 and len(y) == 2 and y[1] > x[1] else None
+def gsum(b, k):
+    v = [g.get(k) for g in b["gw"].values() if g]
+    return None if not v or None in v else sum(v)
+def per_s(a, b, k, dt):
+    x, y = gsum(a, k), gsum(b, k)
+    return None if x is None or y is None else round((y - x) / dt, 1)
+def runq(a, b, dt):
+    v = [(b["gw"][g]["runq_ns"] - a["gw"][g]["runq_ns"]) / 1e6 / dt for g in b["gw"]
+         if b["gw"][g] and (a["gw"].get(g) or {}).get("runq_ns") is not None and b["gw"][g].get("runq_ns") is not None]
+    return round(max(v), 1) if v else None
+out = []
+for a, b in zip(rows, rows[1:]):
+    dt = (b["ms"] - a["ms"]) / 1000
+    if dt <= 0:
+        continue
+    cores = [None if x is None or y is None else round((y - x) / hz / dt, 2) for x, y in zip(a["ticks"], b["ticks"])]
+    pend = [g["handshake"] + g["auth_pending"] for g in b["gw"].values() if g and g.get("handshake") is not None and g.get("auth_pending") is not None]
+    lg = b["lg"]
+    out.append([round((b["ms"] - rows[0]["ms"]) / 1000, 1), lg and lg["online"], lg and lg["gave_up"],
+                round((lg["failed"] - a["lg"]["failed"]) / dt, 1) if lg and a["lg"] else None] + cores +
+               [busy(a, b, "loadgen"), busy(a, b, "gateway"), busy(a, b, "logic"), runq(a, b, dt),
+                max(pend) if pend else None, gsum(b, "online"),
+                per_s(a, b, "pending_mem", dt), per_s(a, b, "no_slot", dt), per_s(a, b, "logins_ok", dt), per_s(a, b, "closes_heartbeat", dt)])
+c = {k: i for i, k in enumerate(cols)}
+def first(f):
+    return next((r[0] for r in out if f(r)), None)
+pos = lambda k: lambda r: (r[c[k]] or 0) > 0
+sat = lambda k: lambda r: (r[c[k]] or 0) >= 85
+print(json.dumps({"columns": cols, "rows": out, "samples": len(rows), "first_s": {
+    "loadgen_saturated": first(sat("loadgen_set_busy_pct")), "gateway_set_saturated": first(sat("gateway_set_busy_pct")),
+    "logic_set_saturated": first(sat("logic_set_busy_pct")),
+    "stats_unanswered": first(lambda r: r[c["lg_online"]] is None),
+    "pending_mem": first(pos("pending_mem_per_s")), "no_slot": first(pos("no_slot_per_s")),
+    "gave_up": first(pos("lg_gave_up")), "closes_heartbeat": first(pos("closes_heartbeat_per_s"))}}))' "$1" 2>/dev/null || echo null
 }
 
+# slow_verdict: the lg_py expression for the slow phase's verdict, on
+# {"ramp", "hold": t2_cpusets_json of the light clients' ramp and of the
+# hold}: the first of loadgen's, the gateways' (with slow-auth) and Logic's
+# (with slow-handshake) sets >= 85% busy in either window, as
+# loadgen-saturated, gateway-set-saturated or logic-set-saturated; else
+# valid; unread if a snapshot is missing. Only valid lets slow_legit and
+# slow_heartbeats pass (#126). The order names a set, not a cause: which
+# came first is read in slow.series.first_s (6b3b3 review O3).
+slow_verdict='"unread" if not (d["ramp"] and d["hold"]) else next((k + "-saturated" if k == "loadgen" else k + "-set-saturated" for k in ("loadgen", "gateway", "logic") if any(100 - w[k]["idle_pct"] >= 85 for w in (d["ramp"], d["hold"]))), "valid")'
+
 t=$SECONDS
 if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
   echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
@@ -554,7 +724,7 @@ phase_garbage() {
 phase_slow() {
   local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} hs=${FLOODS_SLOW_HANDSHAKE:-80000}
   local au=${FLOODS_SLOW_AUTH:-40000} srate=${FLOODS_SLOW_RATE:-10000} hold=${FLOODS_SLOW_HOLD:-60}
-  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f
+  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win verdict
   STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
   up slow || { stack_down; return; }
   stack_snapshot before
@@ -563,18 +733,33 @@ phase_slow() {
   t2_cpustat_snap "$STACK_DIR/cpustat-0"
   ct_stat_snap "$STACK_DIR/ctstat-0"
   t0=$SECONDS
-  lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
-  lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
+  # The floods off loadgen's CPU set, left to the light clients (#126):
+  # slow-handshake on Logic's, slow-auth on the gateways', each with a
+  # GOMAXPROCS of its own, not one per CPU of the set (#127, review O1, O2).
+  LG_ENV+=(GOMAXPROCS=${FLOODS_SLOW_HS_PROCS:-2})
+  LG_CPUS=$HS_CPUS lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
+  LG_ENV[-1]=GOMAXPROCS=${FLOODS_SLOW_AUTH_PROCS:-4}
+  LG_CPUS=$AUTH_CPUS lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
+  unset 'LG_ENV[-1]'
   lg_in flood lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$hs"' and d["flood"]["open"] >= 0.85 * '"$hs" $((hs / srate + 60)) || flooding=false
   lg_in flood2 lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$au"' and d["flood"]["open"] >= 0.85 * '"$au" $((au / srate + 60)) || flooding=false
   t2_check slow_flood "$flooding" "\"slowhs\": $(lg_in flood lg_get /stats 'd["flood"]'), \"slowauth\": $(lg_in flood2 lg_get /stats 'd["flood"]')"
   [ "$flooding" = true ] || { t2_record_end slow; down slow $((n + hs + au)); return; }
   lg_start slow-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix slow
   stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}" flood2="${LG_PIDS[flood2]}"
+  t2_cpustat_snap "$STACK_DIR/cpustat-light0"
+  slow_sampler "$STACK_DIR/artifacts/slow-series.jsonl" "$LG_PID" "${LG_PIDS[flood]}" "${LG_PIDS[flood2]}" &
+  smp=$!
+  # On Logic's set (94.6% idle in run 30b, slow-handshake's ~1.3 CPUs
+  # beside it now), off the gateways' and loadgen's.
+  [ "${STACK_PIN:-0}" = 1 ] && taskset -apc "$T2_CPUS_LOGIC" "$smp" >/dev/null
   online=false
   lg_wait "$(settled "$n")" $((n / rate * 3 + 60)) && online=$(lg_get /stats "$(logged_in "$n")")
   on=$(lg_get /stats 'd["online"]')
+  t2_cpustat_snap "$STACK_DIR/cpustat-light1"
   sleep "$hold"
+  t2_cpustat_snap "$STACK_DIR/cpustat-light2"
+  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
   usage=$(stack_usage_json)
   lg_in flood lg_stop
   lg_in flood2 lg_stop
@@ -586,6 +771,10 @@ phase_slow() {
   stack_snapshot after
   lg_stop
   d=$(gws_delta "$s0" "$s1")
+  win="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-light0" "$STACK_DIR/cpustat-light1"), \"hold\": $(t2_cpusets_json "$STACK_DIR/cpustat-light1" "$STACK_DIR/cpustat-light2")}"
+  verdict=$(echo "$win" | lg_py - "$slow_verdict")
+  [ "$verdict" = '"valid"' ] ||
+    t2_not_covered "slow: legitimate logins and heartbeats beside the floods; verdict $verdict, ruled by the order in slow.series.first_s (slow.window, slow.series)"
   # Slow-auth's handshakes meet the limiter: a refused one ends pre-handshake,
   # with no hold, and the bounds are on the holds the timeouts ended (O1).
   held=true
@@ -595,15 +784,16 @@ phase_slow() {
   t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}')"
   t2_check slow_freed "$(echo "{\"g\": $d, \"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')}" | lg_true - 'sum(v["rejects_handshake_timeout"] for v in d["g"].values()) >= d["h"] and sum(v["rejects_auth_timeout"] for v in d["g"].values()) >= d["a"]')" \
     "\"handshake_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_handshake_timeout"] for v in d.values())'), \"slowhs_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"auth_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_auth_timeout"] for v in d.values())'), \"slowauth_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')"
-  t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}}" | lg_true - 'd["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
-    "\"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
-  heartbeats slow "$on" "$d"
+  t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}, \"v\": $verdict}" | lg_true - 'd["v"] == "valid" and d["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
+    "\"verdict\": $verdict, \"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
+  heartbeats slow "$on" "$d" "$verdict"
   lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
+  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"flood_cpus\": {\"slowhs\": $(t2_json_str "$HS_CPUS"), \"slowauth\": $(t2_json_str "$AUTH_CPUS")}, \"flood_procs\": {\"slowhs\": ${FLOODS_SLOW_HS_PROCS:-2}, \"slowauth\": ${FLOODS_SLOW_AUTH_PROCS:-4}}, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"verdict\": $verdict, \"window\": $win, \"series\": $(slow_series "$STACK_DIR/artifacts/slow-series.jsonl"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
   down slow $((n + hs + au))
   for f in before/gw-0 after/gw-0; do
     grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
   done
+  [ -s "$STACK_DIR/artifacts/slow-series.jsonl" ] || missing+=" slow:slow-series.jsonl"
 }
 
 for p in $PHASES; do
PATCH
```

- [ ] **Step 2: Check the syntax and the `||` rule**

Run: `bash -n scripts/tier2/floods.sh && echo ok; grep -c 't2_bool [^)]*||' scripts/tier2/floods.sh`
Expected: `ok` and `0`.

- [ ] **Step 3: Dry run on the dev machine**

The same throwaway copy as Plan 6b3b2's Task 4 Step 3: the guard lowered, the root parts removed, the scale cut. The copy lives in the session scratchpad (`$S`), not `/tmp`. **`FLOODS_RATE` is 500, not 1,000:** on the dev machine the three CPU sets are the same 2 CPUs, so the verdict reads the whole machine. At 1,000/s the prototype's light ramp read 92.7% busy and gave `loadgen-saturated`, correctly, and both slow checks failed. At 500/s it read 73.2% (`4ba4eba`) and 65.9% (`f338cbe`).

```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/floods.sh "$D/floods.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 ss$|t2_need curl taskset python3 ss|' "$D/floods.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/floods.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/floods.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/floods.sh"
rm -rf tier2-results/floods-*
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  FLOODS_STORM_CLIENTS=2000 FLOODS_STORM_RATE=1000 FLOODS_CLIENTS=2000 FLOODS_RATE=500 \
  FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500 \
  FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35 \
  bash "$D/floods.sh" >"$D/out.txt" 2>&1; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo. On the dev machine the three sets are the same CPUs, so the floods' default lists (Logic's set, the gateways') pass the settings check.

Expected (prototype `f338cbe`, ~2 min): `result.json` parses and has **30** checks. All pass except `no_swap_in_*` checks failing from the dev machine's own swapping (the prototype: only `no_swap_in_slow`; `4ba4eba`: only `no_swap_in_storm`). Values from the prototype:
- `slow_legit.verdict` and `slow_heartbeats.verdict`: `"valid"`. `garbage_heartbeats` has no `verdict` member.
- `slow.flood_cpus`: `{"slowhs": "0-1", "slowauth": "0-1"}`. `slow.flood_procs`: `{"slowhs": 2, "slowauth": 4}`. `slow.window`: ramp 65.9% busy and hold 18.4% busy, the same for all three sets (`100 - idle_pct`).
- `slow.series`: `samples` 36, 35 rows, 17 `columns`, all eight `first_s` members null. `gw_runq_ms_per_s` is non-null in every row (the prototype: ~600 in the first seconds on 2 CPUs, under 10 once settled).
- `not_covered` has 3 lines, none starting with `slow:`.
- `storm_rate.verdict` `"met"`. `garbage_limited` both `bound_by` `"bucket"`. `ping_gap_max_ms` 30,002 (garbage) and 30,001 (slow).

Then the forced cases (`rm -rf tier2-results/floods-*` before each run):
- **A saturated loadgen fails the slow checks, labelled.** Run with `T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=1 T2_CPUS_LOADGEN=0 FLOODS_PHASES=slow FLOODS_CLIENTS=20000 FLOODS_RATE=5000 FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35` (and `GOIM_TEST_TIER=2`). That is 12 checks. While it runs, read each flood's `/proc/PID/environ` and `taskset -pc PID`, for example once a second in a background loop over `pgrep -f "loadgen.*slow-(handshake|auth)"`. Expected (prototype):
  - The floods run as `GOMAXPROCS=2` on CPU `1` (slow-handshake) and `GOMAXPROCS=4` on CPU `1` (slow-auth).
  - `slow_legit` and `slow_heartbeats` fail with `verdict` `"loadgen-saturated"`.
  - `slow.window.ramp.loadgen` and `slow.window.hold.loadgen` read `idle_pct` 0.0.
  - `not_covered` starts with `slow: legitimate logins and heartbeats beside the floods; verdict "loadgen-saturated", ruled by the order in slow.series.first_s (slow.window, slow.series)`.
  - `slow.series.first_s`: `loadgen_saturated` and `stats_unanswered` at the first row (1.5s), `gave_up` 94.0, and some early rows have `lg_online` null. `gateway_set_saturated` and `logic_set_saturated` are 1.5 too: here both sets are CPU 1, where the floods and the stack ran. The prototype got 781 of 20,000 online.
  - `slow_held` and `no_swap_in_slow` may fail too (the starved CPU). The rest pass.
- **The verdict's other branches**, on JSON, after sourcing `scripts/tier2/lib.sh` and `scripts/loadgen.sh` and `eval "$(grep '^slow_verdict=' scripts/tier2/floods.sh)"`. With `w() { echo "{\"loadgen\": {\"idle_pct\": $1}, \"gateway\": {\"idle_pct\": $2}, \"logic\": {\"idle_pct\": $3}}"; }` and `chk() { echo "{\"ramp\": $1, \"hold\": $2}" | lg_py - "$slow_verdict"; }`:

| call | prints |
|---|---|
| `chk "$(w 50 50 50)" "$(w 50 50 50)"` | `"valid"` |
| `chk "$(w 50 50 50)" "$(w 14 50 50)"` | `"loadgen-saturated"` (the hold alone) |
| `chk "$(w 50 14 50)" "$(w 50 50 50)"` | `"gateway-set-saturated"` |
| `chk "$(w 50 50 14)" "$(w 50 50 50)"` | `"logic-set-saturated"` |
| `chk "$(w 10 10 10)" "$(w 50 50 50)"` | `"loadgen-saturated"` (loadgen first) |
| `chk "$(w 50 10 10)" "$(w 50 50 50)"` | `"gateway-set-saturated"` (the gateways before Logic) |
| `chk "$(w 15.1 15.1 15.1)" "$(w 15 50 50)"` | `"loadgen-saturated"` (85.0% busy is in) |
| `chk null "$(w 50 50 50)"` | `"unread"` |
- **The settings check refuses**, each before anything starts, with `T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=1 T2_CPUS_LOADGEN=0 FLOODS_PHASES=slow`: `FLOODS_SLOW_HS_CPUS=5` (outside every set) and `FLOODS_SLOW_AUTH_CPUS=0,x` (not a list) print `floods: FLOODS_SLOW_HS_CPUS and FLOODS_SLOW_AUTH_CPUS must each be a CPU list inside T2_CPUS_GATEWAY, T2_CPUS_LOGIC or T2_CPUS_LOADGEN`. `FLOODS_SLOW_AUTH_PROCS=0` prints `floods: FLOODS_SLOW_AUTH_PROCS must be a positive whole number`.

Afterwards, remove `tier2-results/floods-*` and `$D`, and check that no process is left (`pgrep -af "scratchpad/dry" | grep -v pgrep || echo none`).

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/floods.sh
git commit -m "feat(tier2): floods: the slow floods off loadgen's set (slow-handshake on Logic's, slow-auth on the gateways', small GOMAXPROCS), a per-second slow-phase series with the gateways' run-queue delay, and a CPU-saturation verdict on the three sets that slow_legit and slow_heartbeats need valid (#126, #127)"
```

- [ ] **Step 5: Verify the whole branch**

Run: `make test-scripts && make test && go vet ./... && bash -n scripts/tier2/*.sh`
Expected: `lib_test`, `loadgen_test` and `stack_test` print `all passed`, and `make test` is ok. Record the commit and the dry run in this plan's *Execution Notes*, then push the branch.

---

### Task 2: Tier 2 run 30c and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row, #126, #127), Plan 6b3b2's *Execution Notes* (one line pointing here), and `README.md` (both languages) only if run 30c passes.

- [ ] **Step 1: Tier 2 run 30c (the user, on the host, ~12 min)**

After `git fetch && git switch feat/plan6b3 && git pull`, with the desktop apps closed:
1. If the host rebooted since run 30b, rerun `sudo scripts/tier2/host-setup.sh` first. The guard refuses the run until the NOTRACK rules are back.
2. Clear the kernel log so a `table full` line can be dated to this run: `sudo dmesg -C`.
3. Run `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods` and send back the `.report.txt` and the output of `dmesg | grep -i conntrack`.

There are 30 checks. Expected:
- Run 30b's 28 passing checks pass again. Storm and garbage are unchanged, and `storm_rate` stays `loadgen-bound`.
- `slow_flood`, `slow_held` and `slow_freed` pass, now with the floods on Zen 5c cores and a capped GOMAXPROCS. A `slow_held` max near 7.5s would mean the floods' timing suffers on the shared sets or under the cap (Review Focus 1).
- `slow_legit` and `slow_heartbeats` **should pass with verdict `valid`**. Three other outcomes are still valid records:
  - Verdict `loadgen-saturated`, `gateway-set-saturated` or `logic-set-saturated`: both checks fail, labelled, and the order in `first_s` rules on it (Review Focus 2, 1).
  - Verdict `valid` with `pending_mem` rejects: a candidate gateway finding, read against `gw_runq_ms_per_s` (Review Focus 5).
  - Verdict `valid`, but `slow_legit` fails with no `pending_mem` rejects (clients given up after `no_slot` or `login` failures), or `slow_heartbeats` fails alone (review O4). A failure from `no_slot` alone is input to #111.

  Each one goes to the user before anything changes.

What to read, for the *Execution Notes* and the roadmap:
- **Order of events, first for any verdict other than `valid`:** `slow.series.first_s`, all ten members. Did `loadgen_set_busy_pct` reach 85, or `stats_unanswered` show, before `pending_mem` began? Did `pending_max` rise toward 100k, `lg_gave_up` rise, or `lg_refused_per_s` jump above its level of the first seconds before it? The answer decides the ruling (Review Focus 2). `first_s.no_slot` and `first_s.light_refused` are recorded but don't order it: the concurrency limit refuses some handshakes (slow-auth's and the light clients') from the first seconds of a healthy run (run 30: slow-auth's `pre-handshake` 244,786; P6b3b3I-1). Read `first_s.scrape_late` first: a row after a late or failed scrape (`scrape_ms` over 1,500 or null) has no gateway readings, and the next on-time row's rates cover the gap, so a refusal there is dated late, never early (P6b3b3I-3). Under a `valid` verdict, a non-null `loadgen_saturated` means reading the rows around it (Review Focus 3). With any failure, read the rows from the first sign onward.
- **Verdict:** `slow.verdict`, then `slow.window.ramp` and `.hold`: busy (`100 - idle_pct`) and softirq of `loadgen`, `gateway` and `logic`.
- **Where the CPU went:** `slow.usage` (cores of `loadgen`, `flood`, `flood2`, `gw-0`, `gw-1`, `logic`) against run 30b's light 4.42, slow-auth 2.68 and slow-handshake 0.83. Read `flood` (slow-handshake) against Logic's busy share and `slow_legit.login_latency`, for slow-handshake's effect on Logic.
- **Interference:** `gw_runq_ms_per_s` over the ramp and the hold, above all in the seconds around any refusal (Review Focus 1, 5). It is read beside the scrape, so a failed scrape keeps it (P6b3b3I-2).
- **The checks:** `slow_legit` (`online` of 400,000, `failed`, `gave_up`, `hs_no_slot` and `rejects_pending_mem` per gateway). `slow_heartbeats` (`ping_gap_max_ms` under 35,000, `ping_tick_lag_max_ms`, `ping_send_max_ms`, `ping_send_timeouts`, `lost`, `closes_heartbeat`).
- **`dmesg`:** no `table full`.

- [ ] **Step 2: Record**

- This plan's *Execution Notes*: run 30c's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #126 and #127 each get one line with run 30c's result. The plan 6 row says "6b3b3 executed, run 30c …; next 6b3c (fresh session)", or the follow-up the run calls for.
- Plan 6b3b2's *Execution Notes*: one line, "Run 30c: see Plan 6b3b3".
- If run 30c passes in full (30/30), add the README *Measured results* row in both languages, as Plan 6b3b2's Task 5 Step 3 describes, and move "floods" from *Next* to *Done* in the status paragraph. Include the runs' comparability note: from run 30c on, the slow floods ran on Logic's set (slow-handshake) and the gateway set (slow-auth), not loadgen's.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b2-run30-followups.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3b3-slow-phase.md
git commit -m "docs: Tier 2 run 30c, floods recorded"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b3c** (`POST /reset`, `POST /network-change`, scenario `reconnect`, run 31) takes `floods`' helpers to `lib.sh` (P6b3bI-3). `slow_sampler`/`slow_series` are written for the slow phase. If `reconnect` needs a series, 6b3c generalizes them then, not here (YAGNI).
- **#111 (parked):** run 30c's `slow.series` `no_slot_per_s` beside the logins adds to its input, as does a valid run failed by `no_slot` alone (Task 2's fourth outcome, review O4).
- **The spec:** nothing here changes its text. A valid run with `pending_mem` rejects would be a spec question (`PendingCap` against a slow-client flood of this size). It would get a follow-up with #97, #101 and #125.

## Execution Notes

**Task 1 executed 2026-10-17** (`6ae87bf`, from `19261bc`). The plan's patch applied cleanly (205 lines added, 15 removed in `floods.sh`). `bash -n` ok, no `t2_bool … ||`. `make test-scripts` (`lib_test`, `loadgen_test`, `stack_test` all passed), `make test` and `go vet ./...` green.

Dry run on the dev machine (the plan's throwaway copy, `FLOODS_RATE=500`): 30 checks. Three failed, none in the slow checks this plan changes:
- `no_swap_in_garbage` (37 pages) and `no_swap_in_slow` (2 pages): the dev machine's own swapping, as expected.
- `storm_rate`: verdict `short` (`rate_per_s` 661 from a single sample, `new_per_s` null). The storm phase is untouched by this diff. A storm-only rerun gave `met` (`new_per_s` 989, 2 samples), so this was dev-machine noise. Ruled so; run 30c gates it on the host.
- The slow phase as the prototype had it:
  - `slow_legit` and `slow_heartbeats` passed with `verdict` `"valid"`. `garbage_heartbeats` has no `verdict` member.
  - `slow.flood_cpus` `{"slowhs": "0-1", "slowauth": "0-1"}`. `slow.flood_procs` `{2, 4}`.
  - `slow.window`: ramp 74.1% busy, hold 18.2% busy.
  - `slow.series`: 35 samples, 34 rows, 17 columns. All eight `first_s` members are null. `gw_runq_ms_per_s` is non-null in every row (~570–650 in the ramp, under 10 once settled).
  - `not_covered` has 3 lines, none starting with `slow:`.
  - `garbage_limited` gave both `bound_by` `"bucket"`. `ping_gap_max_ms` was 30,002 (garbage) and 30,001 (slow).

Forced `loadgen-saturated` case (12 checks): the probe read `GOMAXPROCS=2` on CPU 1 (slow-handshake) and `GOMAXPROCS=4` on CPU 1 (slow-auth) throughout.
- `slow_legit` and `slow_heartbeats` failed with `"loadgen-saturated"`. `no_swap_in_slow` and `slow_held` failed too, as allowed.
- Loadgen `idle_pct` was 0.0 in both windows, and the `not_covered` label is as specified.
- `first_s`: `loadgen_saturated`, `gateway_set_saturated`, `logic_set_saturated` and `stats_unanswered` all at 1.5. `gave_up` was null, unlike the prototype's 94.0: here 2,669 of 20,000 got online against the prototype's 781. That is timing on the starved CPU, and `gave_up` is recorded, not gated.
- Early rows have `lg_online` null.

The verdict's other branches gave all 8 table rows as expected on JSON. The settings check refused `FLOODS_SLOW_HS_CPUS=5`, `FLOODS_SLOW_AUTH_CPUS=0,x` and `FLOODS_SLOW_AUTH_PROCS=0` with the specified messages, before any stack started. No process was left behind.

**Final review 2026-10-17** (fresh Opus reviewer, `19261bc..8e0c315`): 0 Critical, 1 Important, 6 Minor. Verdict "ready with fixes".
- **I1, fixed in `42f9763`.** `slow_series`' `gsum` skipped a gateway whose scrape came back null, so one failed `/metrics` gave a row of negative rates, then a spike of that gateway's whole total. That planted a false `first_s.no_slot` or `first_s.pending_mem`, the very order the review-O3 ruling reads. Now a missing gateway nulls that row's sums, and the rates are null unless both samples hold the same gateways. A synthetic three-sample test (gw-1 null in the middle sample) gave −5000/+5000 and `first_s.no_slot` 2.0 before the fix, and nulls after it. Suite green; slow-only dry run 12 checks, only `no_swap_in_slow` (dev) failing, verdict `valid`, `first_s` all null.
- **Minors, left for the user's call:**
  - M1: a line's counters can trail its `ms` stamp by up to ~21s, because `stack_metrics` uses `curl -m 10` serially. Suggested: an `ms_end` field, or a short timeout for the sampler. The reviewer thought it worth taking before run 30c.
  - M2: a counter family with no sample yet reads null to N at its first rise, so `first_s` can miss or delay it (`closes_heartbeat`). Suggested: treat null as 0 when the later reading is non-null.
  - M3: `in_set` accepts a reversed range such as `5-3` (it expands to nothing), so `taskset` fails later inside `lg_start`.
  - M4: `heartbeats` gates `v in (None, "valid")`, so a verdict computed as null would pass `slow_heartbeats`. No path to it today.
  - M5: `first_s.no_slot` and `no_slot_per_s` include slow-auth's own refusals. This is a caveat for reading run 30c.
  - M6: `gw_runq_ms_per_s` can go negative if a gateway thread exits or the gateway restarts.

**Minors M1–M6 fixed 2026-10-17 on the user's request** (`e12f3de`). New `scripts/tier2/floods_test.sh` is in `make test-scripts`. It covers `in_set`, `heartbeats`' gate, `slow_verdict` and `slow_series` on synthetic sampler lines, and failed 14 of 27 checks before the fixes.
- **M1.** `stack_metrics` now takes `STACK_CURL_TIMEOUT` (default 10, unchanged elsewhere), with a new `stack_test` case: a silent gateway reads null within 1s; the old code was cut off by the test's 10s `timeout`. The sampler scrapes each gateway with 1s, stamps `ms_end` right after the gateways, and asks `/stats` last.
  - A sample whose scrapes trailed its clock by over 1.5s, or that lacks a gateway, gives no gateway readings. The next on-time sample's rates are taken against the last on-time one, so a refusal is dated late, never early (this also covers I1's lost interval).
  - New column `scrape_ms`; new `first_s.scrape_late`.
  - In the forced loadgen-saturated dry run the skew stayed ≤440ms. With `/stats` first, as before the reorder, it was ~1.3s.
- **M2.** A gateway counter null in the earlier sample counts 0 there, once the later one has it.
- **M3.** `in_set` refuses a list that expands to no CPU (`5-3`).
- **M4.** `heartbeats` gates on `$# -ge 4`: with a verdict argument, even an empty one, only `"valid"` passes, and the `verdict` member is always written.
- **M5.** The sampler reads the light instance's own `pre-handshake` failures (a gateway refusing it). New column `lg_refused_per_s`; new `first_s.light_refused`. `no_slot_per_s` still counts slow-auth's refusals too, as the header now says.
- **M6.** A gateway whose run-queue total went down gives no reading that row, never a negative one.

The series now has **19 columns** (`scrape_ms` after `t_s`, `lg_refused_per_s` after `lg_failed_per_s`) and `first_s` has **10 members**. Task 2's "all eight members" reads as all ten. Still 30 checks (17 `t2_check` calls, as before), no Go change. Suite green (`lib_test`, `floods_test`, `loadgen_test` and `stack_test` all passed; `make test`, `go vet`). Slow-only dry runs: valid 11/12 (only `no_swap_in_slow`, dev), skew max 183ms, `first_s` all null. Forced case: `loadgen-saturated`, labelled as before.

**Interface review** (2026-10-17, Claude; decided by the user, "apply all"): P6b3b3I-1..4, over what run 30c's record and 6b3c build on (`slow_sampler`/`slow_series`, `slow_verdict`, `heartbeats`' verdict argument, `in_set`, `STACK_CURL_TIMEOUT`, the `result.json` fields Task 2 reads, Task 2's reading). **Checked, unchanged:** every `SERIES_COUNTERS` name exists with its label (`connections{state}` is `handshake`, `auth_pending`, `online`); `set_ticks` counts busy as `t2_cpusets_json` does (busy: user, nice, system, irq, softirq, steal), so the series and the verdict share their 85%; the fields Task 2 reads under `slow.usage` (`loadgen`, `flood`, `flood2`, `gw-0`, `gw-1`, `logic`) and `slow.window.*` (`idle_pct`, `softirq_pct`) exist; `LG_CURL_TIMEOUT` was already in `loadgen.sh`; the report carries `slow.series` (~250 rows, ~30KB); still 30 checks.
- **P6b3b3I-1 (fixed, plan):** on the host, `first_s.no_slot` and `first_s.light_refused` show in the first seconds of a healthy run: the concurrency limit's loss (#111) is recorded, not gated, and slow-auth alone had 244,786 `pre-handshake` closes in run 30 (storm's `no_slot_share` 27%). The dry run had them null only at its small size. Task 2 and Review Focus 2 ordered loadgen's saturation against "`pending_mem` or `no_slot`", which read literally would make every `loadgen-saturated` verdict a possible gateway finding. They now order it against `pending_mem`, a rise of `pending_max` or `lg_gave_up`, and a jump in `lg_refused_per_s`; the two first seconds are recorded only.
- **P6b3b3I-2 (fixed, code):** `slow_sampler` stored a gateway's `runq_ns` inside its scrape, so a failed `/metrics` (null) lost that gateway's run-queue wait for the two rows beside it, and the column's max then showed the other gateway's: the seconds Review Focus 1 and 5 read, when a struggling gateway misses the 1s scrape. The sampler now writes `"rq": {gateway: ns}` beside `"gw"`, and `runq()` reads it. New `floods_test.sh` case (gw-1 waits 1s/s, its middle scrape fails): `[1000.0, 1000.0]`; against the old `floods.sh` it and the M6 case (now on `rq`) failed with `[None, None]`. A stubbed sampler run wrote gw-1's `rq` with its scrape null. `make test-scripts` green.
- **P6b3b3I-3 (fixed, plan):** Task 2 still read "all eight members" and didn't name M1's and M5's additions. It now reads all ten, and reads `first_s.scrape_late` before the order: a row after a late or failed scrape has no gateway readings and the next on-time row's rates cover the gap, so a refusal there is dated late.
- **P6b3b3I-4 (noted, for 6b3c):** `in_set`, `set_ticks`, `runq_ns` and `heartbeats`' optional verdict argument are generic, and `STACK_CURL_TIMEOUT` is now `stack.sh`'s. As P6b3bI-3, they move to `lib.sh` with their second user.

**Run 30c** (2026-10-17, the user, Tier 2 host; commit `a8c1d80`, `dirty` 0, 293s; `floods-20261017-060707`). **Failed 28/30**: `slow_legit` and `slow_heartbeats`, both on verdict `loadgen-saturated` alone. `dmesg | grep -i conntrack` after the run (buffer cleared before it): empty, no `table full`; conntrack peaked at 33 of 4,194,304 (`slow_conntrack`).
- **Storm and garbage as in run 30b.** `storm_rate` passes `loadgen-bound`: `new_per_s` 15,962, `loadgen_cores_ramp` 7.939 of 8, `no_slot_share` 0.2738. `garbage_delivered` 7,207,756 attempts, `TCPSynRetrans` 3. `garbage_limited` `bound_by` `slots` on both (`dh_per_s` 21,629 / 21,256). `garbage_heartbeats` Pong p99 4ms, none missing. No swap in any phase.
- **The floods** (`slow_flood`, `slow_held`, `slow_freed` pass): slow-handshake on Logic's set `10,11,22,23` (GOMAXPROCS 2, 0.91 cores), slow-auth on the gateway set `4-9,16-21` (GOMAXPROCS 4, 3.29 cores). Holds max 6,588ms (slowhs) and 6,880ms (slowauth; `pre-handshake` 268,757 of 988,945 attempts, 27%).
- **`slow_legit`: fails on the verdict only.** 398,991 online and 1,009 gave up (400,000), failures `pre-handshake` 537,444 and `gave-up` only (`logged_in` true). Both gateways: `rejects_pending_mem` 0 (run 30b: 63k / 66k), `logins_busy` 0, `rejects_at_capacity` 0, `rejects_login_queue` 0, `hs_no_token` 0. `hs_no_slot` 402,720 / 403,481. Login p50/p99/max 1,924 / 5,053 / 5,123ms.
- **`slow_heartbeats`: fails on the verdict only.** 1,185,090 Pongs, `pong_missing` 0, Pong p99/max 1,822 / 1,896ms, `lost` {}, `closes_heartbeat` 0. `ping_gap_max_ms` 30,415 (run 30b: 155,087), `ping_tick_lag_max_ms` 414, `ping_send_max_ms` 95, `ping_send_timeouts` 0.
- **Verdict and windows:** ramp: loadgen 15.0% idle (85.0% busy, at the threshold), gateway set 10.4% idle (89.6%), Logic 52.7%. Hold: loadgen 95.8% idle, gateway 44.7%, Logic 76.3%. Both loadgen and the gateway set saturated in the ramp; the label is `loadgen-saturated` because loadgen is tested first.
- **Order (`first_s`):** `gateway_set_saturated` 1.1, `no_slot` 1.1, `light_refused` 1.1, `loadgen_saturated` 5.5, `gave_up` 9.0, `stats_unanswered` 35.7 (three rows without `/stats` between 35.7 and 41.7); `pending_mem`, `closes_heartbeat`, `scrape_late`, `logic_set_saturated` null. `pending_max` peaked at 78,183 (39.7s) of the 100k `PendingCap`. `lg_refused_per_s` rose with the ramp (4.5k → ~12k/s by 5.5s, 8–15k/s after), no jump. `scrape_ms` ≤ 102.
- **Reading:** the light clients lost **57% of their handshakes to the concurrency limit** (537,444 refusals against 398,991 logins; storm 27%, slow-auth 27%), so with retries loadgen made ~20k handshakes/s for 10k new clients/s, the ~21k/s ceiling the storm phase measured on its 8 CPUs, and saturated from 5.5s. The gateway set ran hotter than Review Focus 1's ~72%: 89.6% over the ramp with slow-auth's 3.7 cores, and the gateways' run-queue wait `gw_runq_ms_per_s` was 1,500–2,500 in the ramp against ~250–700 in the hold, so slow-auth's co-location slowed them. By Task 2's rule (P6b3b3I-1), loadgen saturated before any `pending_mem` refusal, and none came: not a gateway finding against the spec's *Slow clients* row. The 57% `no_slot` loss and the 1,009 gave-ups are #111's input.
- **Decided (the user, 2026-10-17), #128:** rerun the slow phase alone as run 30d at half the light rate (`FLOODS_PHASES=slow FLOODS_RATE=5000`, no code change), so the slots' offered load, the refusals and the retries drop. If it is valid and clean, runs 30c (storm, garbage) and 30d (slow) together are the floods result, and the README row names the slow phase's light rate.

**Run 30d** (2026-10-17, the user, Tier 2 host; commit `d064dc3`, `dirty` 0, 165s; `floods-20261017-062610`; `FLOODS_PHASES=slow FLOODS_RATE=5000`, #128). **Passed 12/12, verdict `valid`.** `dmesg | grep -i conntrack` after the run (buffer cleared before it): empty, no `table full`; `slow_conntrack` peak 31.
- **`slow_legit`:** 400,000 online, `gave_up` 0, failures `pre-handshake` 150,445 only (27.3% of the light handshakes, against run 30c's 57%). Both gateways: `rejects_pending_mem` 0, `logins_busy` 0, `at_capacity` 0, `login_queue` 0, `no_token` 0; `hs_no_slot` 187,539 / 188,415. Login p50/p99/max 6 / 56 / 122ms.
- **`slow_heartbeats`:** 1,402,329 Pongs, none missing, Pong p99/max 18 / 49ms, `lost` {}, no heartbeat close, `ping_gap_max_ms` 30,035, `ping_tick_lag_max_ms` 33, `ping_send_max_ms` 9.
- **The floods:** `slow_held` slowhs p99/max 6,226 / 6,584ms, slowauth 6,025 / 6,318ms (`pre-handshake` 225,509 of 1,232,065, 18%). `slow_freed`, `slow_flood`, `slow_lease` (15 renewals each) pass.
- **Windows:** ramp: gateway set 82.2% busy (17.8% idle), loadgen 26.0%, Logic 40.1%. Hold: gateway 55.0%, loadgen 4.1%, Logic 23.4%. `slow.usage` over 144s: gw-0 and gw-1 2.52 cores each, loadgen (light) 1.26, slow-auth 3.37, slow-handshake 0.89, Logic 0.28.
- **Series:** `first_s.gateway_set_saturated` 22.8 under a `valid` verdict (Review Focus 3): through the ramp the gateway set ran 78–88% per second, with no refusal jump, no gave-up and `pending_max` ≤ 58,761 around those seconds; `gw_runq_ms_per_s` 1,000–1,800 in the ramp against ~250–650 in the hold, slow-auth's co-location. `loadgen_saturated`, `stats_unanswered`, `scrape_late`, `pending_mem`, `gave_up`, `closes_heartbeat` all null. Light `lg_refused_per_s` ~1.7–2.1k/s, flat through the ramp.
- **Result (#128):** runs 30c (storm, garbage) and 30d (slow) together are the floods result: README *Measured results* row added in both languages, "floods" moved to *Done*. The gateway set at 82% over the ramp sits near the 85% threshold with slow-auth on it; a later slow-phase run at 10,000/s would need more loadgen CPUs and the floods off the gateways' set. **6b3b3 complete.**

**Run 30e, a rate-edge probe** (2026-10-17, the user, Tier 2 host; commit `d064dc3`, `dirty` 0, 140s; `floods-20261017-064000`; `FLOODS_PHASES=slow FLOODS_RATE=7500`; #129). **Failed 10/12**: `slow_legit` and `slow_heartbeats`, on verdict `gateway-set-saturated` alone. `dmesg | grep -i conntrack` empty; `slow_conntrack` peak 24.
- **Gated readings clean:** 399,522 online and 478 gave up (400,000), failures `pre-handshake` 409,633 and `gave-up` only. Both gateways: `rejects_pending_mem` 0, `logins_busy` 0, `at_capacity` 0, `login_queue` 0, `no_token` 0; `hs_no_slot` 330,602 / 331,385. Login p50/p99/max 9 / 220 / 368ms. 1,230,857 Pongs, none missing, Pong p99/max 55 / 124ms, no heartbeat close, `ping_gap_max_ms` 30,550, `ping_tick_lag_max_ms` 548.
- **Windows:** ramp: gateway set 88.1% busy (11.9% idle), loadgen 55.3%, Logic 45.6%. Hold: gateway 55.2%, loadgen 4.2%. `slow.usage` over 119s: gw-0 and gw-1 2.58 cores each, light 2.30, slow-auth 3.34, slow-handshake 0.90.
- **Order (`first_s`):** `gateway_set_saturated` 1.1 (85.6% in the first row, 88–93% through the ramp), `no_slot` and `light_refused` 1.1, `gave_up` 10.9, `loadgen_saturated` 39.5 (single-second spikes to 87–90% every ~5s in an otherwise ~55% set); `pending_mem`, `stats_unanswered`, `scrape_late`, `closes_heartbeat` null. `gw_runq_ms_per_s` 1,450–2,420 in the ramp. `pending_max` ≤ 55,518.
- **The edge** (runs 30d, 30e, 30c beside the same floods):

  | light rate | verdict | gateway set (ramp) | loadgen (ramp) | refused share of light handshakes | gave up | login p99 |
  |---|---|---|---|---|---|---|
  | 5,000/s (30d) | `valid` | 82.2% | 26.0% | 27.3% | 0 | 56ms |
  | 5,500/s (30g) | `valid` | 84.6% | 29.5% | 32.9% | 8 | 59ms |
  | 6,000/s (30f) | `gateway-set-saturated` | 85.0% | 31.6% | 37.0% | 20 | 62ms |
  | 7,500/s (30e) | `gateway-set-saturated` | 88.1% | 55.3% | 50.6% | 478 | 220ms |
  | 10,000/s (30c) | `loadgen-saturated` | 89.6% | 85.0% | 57.3% | 1,009 | 5,053ms |

  On this host the edge lies between 5,000 and 7,500/s, and it is the gateways' CPU set, which slow-auth shares (3.3 cores of its 12), that binds first. At every rate no legitimate client was refused for memory, capacity or the Login queue and every Ping was answered; what grows is the concurrency limit's loss, which doubles past 5,000/s and makes clients give up in light mode's 10 attempts (the SDK backs off instead). That loss is #111's input; the spec has the client treat it as a `Busy` retry (*Admission control*), which the slow-client flood bullet's "no `Busy`" does not gate (#122, review O2).

**Run 30f, the edge narrowed** (2026-10-17, the user, Tier 2 host; commit `d064dc3`, `dirty` 0, 153s; `floods-20261017-064445`; `FLOODS_PHASES=slow FLOODS_RATE=6000`, the user's choice over the suggested 6,250; #129). **Failed 10/12**: `slow_legit` and `slow_heartbeats` on verdict `gateway-set-saturated` alone, the gateway set at 85.0% busy over the ramp (15.0% idle), exactly the threshold. `dmesg | grep -i conntrack` empty; `slow_conntrack` peak 29.
- **Gated readings clean:** 399,980 online and 20 gave up, failures `pre-handshake` 235,304 and `gave-up` only; `rejects_pending_mem`, `logins_busy`, `at_capacity`, `login_queue`, `no_token` 0 on both; `hs_no_slot` 239,019 / 240,857. Login p50/p99/max 7 / 62 / 193ms. 1,329,727 Pongs, none missing, Pong p99/max 6 / 86ms, no heartbeat close, `ping_gap_max_ms` 30,032.
- **Windows:** ramp: gateway 85.0%, loadgen 31.6%, Logic 42.3%. Hold: gateway 55.2%, loadgen 4.2%. `slow.usage` over 132s: gw-0 and gw-1 2.55 cores each, light 1.56, slow-auth 3.38, slow-handshake 0.90.
- **Order (`first_s`):** `gateway_set_saturated` 4.4 (85–93% per second through the ramp), `no_slot` and `light_refused` 1.1, `gave_up` 12.0; `loadgen_saturated`, `pending_mem`, `stats_unanswered`, `scrape_late`, `closes_heartbeat` null. `gw_runq_ms_per_s` 1,070–2,380 in the ramp.
- **The edge (#129):** with these floods on this host, 5,000/s is valid and 6,000/s sits on the 85% line, so the slow phase's edge is about 5,000–6,000 legitimate logins/s (2,500–3,000 per gateway), bound by the gateways' CPU set with slow-auth's ~3.4 cores on it. The concurrency limit's loss grows with the rate (27% → 37% → 51% → 57%) and light clients start giving up from 6,000/s.

**Run 30g, the edge narrowed again** (2026-10-17, the user, Tier 2 host; commit `d064dc3`, `dirty` 0, 158s; `floods-20261017-064907`; `FLOODS_PHASES=slow FLOODS_RATE=5500`; #129). **Passed 12/12, verdict `valid`**, the gateway set at 84.6% busy over the ramp (15.4% idle). `dmesg | grep -i conntrack` empty; `slow_conntrack` peak 22.
- `slow_legit`: 399,992 online and 8 gave up (allowed: failures `pre-handshake` 196,348 and `gave-up` only), `pending_mem`, Busy, `at_capacity`, `login_queue`, `no_token` 0; `hs_no_slot` 217,773 / 218,956. Login p50/p99/max 7 / 59 / 152ms. `slow_heartbeats`: 1,349,641 Pongs, none missing, Pong p99/max 17 / 53ms, `ping_gap_max_ms` 30,019.
- Windows: ramp gateway 84.6%, loadgen 29.5%, Logic 41.5%; hold gateway 55.0%. `slow.usage` over 137s: gw-0 and gw-1 2.54 / 2.55 cores, light 1.41, slow-auth 3.40, slow-handshake 0.90. `first_s.gateway_set_saturated` 7.6 (per-second 84–90% through the ramp, under a valid window), `gave_up` 18.5; `loadgen_saturated`, `pending_mem`, `stats_unanswered`, `scrape_late`, `closes_heartbeat` null.
- **The edge (#129): 5,500/s passes at 84.6%, 6,000/s fails at 85.0%.** With these floods on this host, the slow phase holds about 5,500 legitimate logins/s (2,750 per gateway), bound by the gateways' CPU set with slow-auth's ~3.4 cores on it. Light clients begin to give up at 5,500/s (8 of 400,000), which the gate allows.

**Run 30h, slow-auth off the gateways' CPUs** (2026-10-17, the user, Tier 2 host; commit `d064dc3`, `dirty` 0, 140s; `floods-20261017-065901`; `FLOODS_PHASES=slow FLOODS_RATE=7500 FLOODS_SLOW_AUTH_CPUS=0-3,12-15`, slow-auth on loadgen's set; #129). **Failed 10/12**: `slow_legit` and `slow_heartbeats` on verdict `loadgen-saturated` alone (loadgen 85.0% busy over the ramp). `dmesg` not sent.
- **Against run 30e (7,500/s, slow-auth on the gateway set):**

  | | 30e | 30h |
  |---|---|---|
  | gateway set busy, ramp | 88.1% | **51.5%** |
  | `gw_runq_ms_per_s`, ramp | 1,450–2,420 | **100–270** |
  | light handshakes refused | 50.6% | **28.1%** |
  | gave up | 478 | **2** |
  | gw-0 / gw-1 cores | 2.58 / 2.58 | 2.32 / 2.32 |
  | slow-auth cores | 3.34 (gateway set) | 2.43 (loadgen set, mean 4,313 MHz) |
  | loadgen busy, ramp | 55.3% | 85.0% |
  | login p99 / max | 220 / 368ms | 530 / 777ms |
  | Pong p99 / max | 55 / 124ms | 155 / 309ms |

- Gated readings clean: 399,998 online, 2 gave up, failures `pre-handshake` 156,535 and `gave-up` only; `pending_mem`, Busy, `at_capacity`, `login_queue`, `no_token` 0; no heartbeat close, none missing, `ping_gap_max_ms` 30,772, `ping_tick_lag_max_ms` 764. `first_s`: `loadgen_saturated` 2.2, `gateway_set_saturated` null, `gave_up` 52.6.
- **Reading:** runs 30e–30g measured the shared CPU set, not the gateways. With the gateways alone on their set, 7,500/s used about half of it, the run-queue wait fell tenfold, and the concurrency limit's loss fell to 28%, the same as 5,000/s with slow-auth beside them (27.3%): that ~27% is the three slots' own loss at these rates (#111), not CPU contention. The slower logins and Pongs are loadgen's, now the bound. The gateways' own edge beside these floods is above 7,500/s and is not measurable on this host with all three loadgens on 8 CPUs; Tier 3 (Plan 6d) is where it can be.

**Gateway CPU profile: every X25519 is done twice** (2026-10-17; the user ran `go tool pprof -top` on run 30c's 30s garbage-phase profile of gw-0; #129).
- `crypto/ecdh.x25519ScalarMult` is 67.7% of the gateway's CPU (73.0 of 107.9 CPU-s), `edwards25519/field.feMul` alone 27.7%; `ecdh.(*PrivateKey).ECDH` is only 36.5s of it. Syscalls (`internal/runtime/syscall/linux.Syscall6`) 18.7%, GC ~5%, gnet's accept 4.8%.
- **Cause (read in the sources):** flynn/noise v1.1.0's `dh25519` computes each DH, and each `GenerateKeypair`, with `golang.org/x/crypto/curve25519.X25519` (v0.54.0). That calls `ecdh.X25519().NewPrivateKey(scalar)`, which in Go 1.27 computes the public key eagerly (a full scalar multiplication, `crypto/ecdh/x25519.go:56-58`), then `ECDH`. So a DH costs two scalar multiplications and key generation two (it multiplies the basepoint twice). The server side of a legitimate NK handshake (es, e, ee) does 6 where 3 suffice; a garbage one (es, then a failed decrypt) 2 where 1 suffices. The clients (`internal/sdk`, `internal/loadgen`, `internal/clientconn`) use the same `internal/wire`, so they pay twice too.
- **Benchmark (dev machine, AMD EPYC 7742):** one DH through `curve25519.X25519` 135µs, the same DH on a cached `*ecdh.PrivateKey` 68µs; `wire.Initiate` (e, es) 286µs, four multiplications.
- **Why it matters:** the measured slot hold of ~300–500µs (6b3b review O2) is these 6 multiplications, not the spec's ~100µs NK estimate (*Admission control*), and it is what makes the three slots per gateway refuse ~27% at these rates. A DH function for flynn/noise that keeps `*ecdh.PrivateKey` objects (the static key parsed once; each handshake's ephemeral kept from generation to use) would halve the handshake's crypto on both sides with no change to the bytes on the wire.

