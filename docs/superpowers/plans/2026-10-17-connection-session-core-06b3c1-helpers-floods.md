# Plan 6b3c1: Shared Helpers and the Slow Floods' Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the open notes the earlier 6b3 plans left before the whole-population reconnect (Plan 6b3c2): the shared helpers to `lib.sh` (P6b3bI-3, P6b3b3I-4, P6b3b4I-2), the slow floods' `refused` outcome (#135, run 30n) with a bound on it (review O7), and the slow verdict's blind spot (run 30m).

**Architecture:** Two parts, one task each, then an optional run:
- **Helpers to `lib.sh`** (Task 1). The helpers that now have a second user move to `scripts/tier2/lib.sh` with the `t2_` prefix: `t2_kill_tree` (gwloss, floods), `t2_up` (slowlogic, floods, reconnect), `t2_gws_counters`/`t2_gws_delta`, `t2_lease`, `t2_set_ticks` and `t2_runq_ns` (floods, reconnect). No behaviour changes.
- **Floods' slow checks** (Task 2). Loadgen's slow modes count a gateway close before `slowHeld` (3s) as `refused`, with no hold (#135), so `slow_freed` compares the gateways' timeouts with the closes that were timeouts. `slow_held` bounds the floods' `refused` by the gateways' refusals over the phase (review O7). `slow_verdict` also reads `slow.series`: a CPU set at ≥ 95% busy for 5s in a row before every light client is online fails the verdict (#136.2), so a long settle can no longer average a saturated ramp under 85% (run 30m).
- **Run 30o** (Task 3, optional, the user's call): the slow phase at 20,000/s again, to see the new verdict.

**Tech Stack:** Go 1.27.1 (`internal/loadgen`), bash 5.2, python3 (JSON and arithmetic only), `/proc/stat`.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Load tests* (~l.546–547: handshake flood, slow-client flood). Nothing here changes it.

Inputs:
- The roadmap's #135 (`refused`) and #136 (the rulings, amended by the review).
- P6b3bI-3 (the 6b3b plan, ~l.808), P6b3b3I-4 (the 6b3b3 plan, ~l.571), P6b3b4I-2 and runs 30m/30n (the 6b3b4 plan's *Execution Notes*, ~l.895–910).
- The review of the joint Plan 6b3c: `docs/superpowers/objections/connection-session-core-06b3c-reconnect.md` and `-dispositions.md` (O7 applies here; O1–O6 and O8 to Plan 6b3c2).

**Decisions:**
- #135 (the user, 2026-10-17, before writing).
- **#136 (the user, 2026-10-17):** (1) one plan, **amended 2026-10-18**: after the review's O1 the plan grew to ~170KB, and the user split it after its Task 2: this plan (6b3c1) and Plan 6b3c2 (`2026-10-17-connection-session-core-06b3c2-reconnect.md`); (2) the verdict's series rule is ≥ 95% busy for ≥ 5s in a row (not ≥ 85%: run 30l's gateway set was at 84.8–87.6% from 2.2s to 9.0s in a valid run). (3)–(5) are 6b3c2's.
- Review O7 (accepted): `slow_held` also needs the two floods' `refused` to be at most the gateways' refusals over the phase. No new check: floods stays at 30.

**Branch:** `feat/plan6b3`. Execute from its head, this plan's own commit or later. Plan 6b3c2 is executed after this one.

**Size:** 3 tasks. Task 1 moves ~70 lines into `lib.sh` (+32 test lines). Task 2 changes ~10 lines of `flood.go`, ~65 of `floods.sh`, +55 test lines. Task 3 is the user's optional run and the record. The prototype `proto-6b3c` (from `9841396`; tasks `f2546aa`, `4658e13`) ran every step. Its diff is replayed here byte for byte: the changed files as `git apply` patches, one per task. On the dev machine (AMD EPYC 7742, 2 cores):
- `make test` (race), `go vet ./...` and `make test-scripts` green.
- Mutations: 1 caught in Task 2 (the `refused` case); the series rule's by the script tests.
- `floods` slow-phase dry run (2,000 light clients at 500/s): 12 checks, only the dev swap check failing; verdict `valid`, `sat_run_s` 0 on every set; `slow_freed` 3,500 timeouts against 3,500 `closed`; `slow_held` `refused` 0 against the gateways' 0 refusals. A forced run (20,000 at 4,000/s, before O7) failed `slow_legit` and `slow_heartbeats` on verdict `loadgen-saturated`, with `sat_run_s` 9.9s on each set (its ramp window was also over 85%, 92.8%, so that run does not separate the two rules; `floods_test.sh` does).

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- No change to the gateway, Logic, the SDK or the wire. Go changes only in `internal/loadgen`'s flood code.
- Loadgen's existing endpoints, flags and result fields keep their meaning. New: flood outcome `refused`.
- No check of `floods`, `gwloss` or `slowlogic` changes except `floods`' `slow_held` (allows `refused`, bounded by the gateways' refusals, O7), `slow_freed` (via loadgen's `closed`) and `slow_verdict` (reads the series). Their check counts stay (`floods` 30).

## Review Focus

1. **The series rule (#136.2).** `slow_verdict` fails a set at ≥ 95% busy for ≥ 5s in a row before every light client is online, beside the old ramp and hold averages. `floods_test.sh` separates the rule from the 85% average (a 5s run fails, a 4.9s run passes); no dev run separates them.
2. **The `refused` bound (O7).** The gateways' refusals (`rejects_pending_mem` + `rejects_at_capacity` + `hs_no_slot` + `hs_no_token`, from `t2_gws_delta`) include the light clients', so the bound is loose: it fails only a close that no counter explains. At the dev scale both sides are 0.

## Implementation choices this plan makes

1. **Which helpers move.** Only those with a second user: `kill_tree`, `up` (floods keeps a wrapper that also starts its conntrack sampler), `gws_counters`/`gws_delta` (now with the counters as arguments), `lease`, `set_ticks` (with an optional stat file, for its test) and `runq_ns`. `heartbeats`, `settled`, `logged_in` and `in_set` stay in `floods.sh`: they read light clients' results, and `reconnect` runs SDK clients. `down` stays per scenario (P6b3bI-3). `new_per_s` is not reused (P6b3bI-2): `reconnect` gates no rate.
2. **The O7 bound stays in `floods.sh`**, not a helper: the dry run shows it, and `floods_test.sh` does not change for it.

## File Structure

- `scripts/tier2/lib.sh`: `t2_kill_tree`, `t2_up`, `t2_gws_counters`, `t2_gws_delta`, `t2_lease`, `t2_set_ticks`, `t2_runq_ns` (Task 1).
- `scripts/tier2/lib_test.sh`: their tests (Task 1).
- `scripts/tier2/floods.sh`: the moved helpers' callers (Task 1); `slow_held` (with the O7 bound), `slow_freed`'s doc, `slow_series` N and `sat_run_s`, `slow_verdict` (Task 2).
- `scripts/tier2/floods_test.sh`: `kill_tree`'s test moves out (Task 1); the verdict's and `sat_run_s`' tests (Task 2).
- `scripts/tier2/gwloss.sh`, `scripts/tier2/slowlogic.sh`: callers only (Task 1).
- `internal/loadgen/flood.go`, `flood_test.go`: `FloodRefused` (Task 2).
- This plan's *Execution Notes*, the roadmap (Task 3).

---

### Task 1: Shared helpers to `lib.sh`

**Files:**
- Modify: `scripts/tier2/lib.sh`, `scripts/tier2/lib_test.sh`, `scripts/tier2/floods.sh`, `scripts/tier2/floods_test.sh`, `scripts/tier2/gwloss.sh`, `scripts/tier2/slowlogic.sh`

**Interfaces:**
- Produces (in `lib.sh`, for Task 2 and Plan 6b3c2): `t2_kill_tree PID`; `t2_up NAME` (exports `STACK_DIR=$T2_OUT/NAME`, checks `NAME_stack`, returns `stack_up`'s status); `t2_gws_counters SPEC...` (`{"gw-k": stack_counters SPEC...}`); `t2_gws_delta A B`; `t2_lease PHASE DELTA SECONDS LOGIC_DELTA` (checks `PHASE_lease`; DELTA needs `lease_live`, `lease_missing`, `lease_error`, LOGIC_DELTA `lease_error`); `t2_set_ticks LIST [STAT]` (`"busy total"`); `t2_runq_ns PID`.
- Consumes: `stack_*` (`scripts/stack.sh`), `lg_py`/`lg_true` (`scripts/loadgen.sh`), `t2_lease_floor`, `t2_check`, `t2_bool`, `t2_json_str`, `t2_refuse`.

- [ ] **Step 1: The patch**

Apply from the repository root with `git apply` (the patch below, saved to a file in the session scratchpad):

```diff
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index 736b899..95c7a7e 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -234,10 +234,8 @@ t2_on_exit 'stack_down'
 t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the stack
 smp="" prof=""
 t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
-# kill_tree PID: PID and its descendants, so the exit trap stops a profile
-# subshell's curls too (6b3b4 final review M4).
-kill_tree() { local c; for c in $(pgrep -P "$1"); do kill_tree "$c"; done; kill "$1" 2>/dev/null; }
-t2_on_exit '[ -n "$prof" ] && kill_tree "$prof"'
+# The profile subshell with its curls (6b3b4 final review M4).
+t2_on_exit '[ -n "$prof" ] && t2_kill_tree "$prof"'
 cts=""
 t2_on_exit '[ -n "$cts" ] && kill "$cts" 2>/dev/null'
 
@@ -271,19 +269,10 @@ t2_report_extra() {
   done
 }
 
-# up NAME: a fresh stack in $T2_OUT/NAME; checks NAME_stack.
+# up NAME: t2_up, then the conntrack sampler.
 up() {
-  export STACK_DIR=$T2_OUT/$1
-  mkdir -p "$STACK_DIR"
-  ln -sfn ../bin "$STACK_DIR/bin"
-  local busy
-  busy=$(stack_busy_ports)
-  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
-  stack_up 2>"$STACK_DIR/stack-up.err"
-  local rc=$?
-  t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
-  [ $rc -eq 0 ] && ct_start
-  return $rc
+  t2_up "$1" || return
+  ct_start
 }
 
 # down NAME CONNS: every loadgen stopped, the gateways idle, the stack
@@ -340,26 +329,6 @@ a, b = read(sys.argv[1]), read(sys.argv[2])
 print(json.dumps({k: b[k] - a[k] for k in ("insert_failed", "drop", "early_drop", "invalid") if k in a and k in b}))' "$1" "$2" 2>/dev/null || echo null
 }
 
-# gws_counters: {"gw-0": stack_counters, ...} of every gateway; gws_delta A B
-# the same of two readings.
-gws_counters() {
-  local g out=""
-  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
-    out+="${out:+, }\"$g\": $(stack_counters "$g" "${GW_COUNTERS[@]}")"
-  done
-  echo "{$out}"
-}
-# (stack_counters_delta per gateway: a key null in A counts as 0).
-gws_delta() {
-  python3 -c 'import json, sys
-a, b = (json.loads(x) for x in sys.argv[1:3])
-def delta(x, y):
-    if x is None or y is None:
-        return None
-    return {k: None if v is None else round(v - (x.get(k) or 0), 6) for k, v in y.items()}
-print(json.dumps({g: delta(a.get(g), y) for g, y in b.items()}))' "$1" "$2" 2>/dev/null || echo null
-}
-
 # hs_sampler NAME FILE PID: appends "unix_ms ok refused pre cpu" every
 # second, NAME's completed handshakes, the limiter's refusals (no_slot +
 # no_token, a label with no sample yet read as 0), the current loadgen's
@@ -466,15 +435,6 @@ logged_in() {
 # timewait: the host's TIME_WAIT sockets now (ss -s).
 timewait() { ss -s 2>/dev/null | sed -n 's/^TCP:.*timewait \([0-9]*\).*/\1/p' | head -n 1 | grep . || echo null; }
 
-# lease PHASE DELTA SECONDS LOGIC_DELTA: checks PHASE_lease over the gateways'
-# counter delta of a window of SECONDS.
-lease() {
-  local floor
-  floor=$(t2_lease_floor "$3")
-  t2_check "${1}_lease" "$(echo "{\"g\": $2, \"l\": $4}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
-    "\"window_s\": $3, \"live_floor\": $floor, \"gateways\": $(echo "$2" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic_lease_error\": $(echo "$4" | lg_py - 'd["lease_error"]')"
-}
-
 # heartbeats PHASE N DELTA [VERDICT]: checks PHASE_heartbeats over the light
 # result of N clients online (instance main, stopped) and the gateways'
 # counter delta; with VERDICT (slow_verdict's, a JSON string) only "valid"
@@ -491,22 +451,6 @@ heartbeats() {
     "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]'), \"ping_send_timeouts\": $(lg_json 'd["ping_send_timeouts"]')$([ $# -ge 4 ] && echo ", \"verdict\": $v")"
 }
 
-# set_ticks LIST: "busy total" clock ticks of LIST's CPUs since boot
-# (/proc/stat; busy: user, nice, system, irq, softirq, steal; total adds
-# idle and iowait), as t2_cpusets_json counts them.
-set_ticks() {
-  awk -v cpus=" $(t2_cpulist_expand "$1") " '$1 ~ /^cpu[0-9]+$/ && index(cpus, " " substr($1, 4) " ") {
-    b += $2 + $3 + $4 + $7 + $8 + $9; t += $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 } END { print b + 0, t + 0 }' /proc/stat
-}
-
-# runq_ns PID: nanoseconds PID's threads have waited on a run queue
-# (/proc/PID/task/*/schedstat, field 2; the process's own schedstat is its
-# main thread only); empty if PID is gone.
-runq_ns() {
-  [ -d "/proc/$1/task" ] || return 0
-  cat /proc/"$1"/task/*/schedstat 2>/dev/null | awk '{ s += $2 } END { if (NR) printf "%.0f\n", s }'
-}
-
 # slow_sampler FILE LIGHT FLOOD FLOOD2: appends one JSON line a second to
 # FILE until killed (#126): {"ms", "ticks": [the three loadgens' CPU
 # ticks], "sets": {"loadgen", "gateway", "logic": [busy, total]}, "lg":
@@ -534,9 +478,9 @@ slow_sampler() {
   while :; do
     ms=$(date +%s%3N) ticks=""
     for p in "$2" "$3" "$4"; do t=$(stack_ticks "$p"); ticks+="${ticks:+, }${t:-null}"; done
-    sets="\"loadgen\": [$(set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
+    sets="\"loadgen\": [$(t2_set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(t2_set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(t2_set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
     rq=()
-    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do rq[$g]=$(runq_ns "${STACK_PIDS[$g]}"); done
+    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do rq[$g]=$(t2_runq_ns "${STACK_PIDS[$g]}"); done
     gw="" rqj=""
     for g in "${!rq[@]}"; do
       t=$(STACK_CURL_TIMEOUT=1 stack_counters "$g" "${SERIES_COUNTERS[@]}")
@@ -654,7 +598,7 @@ phase_storm() {
   STACK_GATEWAYS=1 STACK_GATEWAY_ARGS=(-max-conns $((n + n / 10)))
   up storm || { stack_down; return; }
   stack_snapshot before
-  s0=$(gws_counters)
+  s0=$(t2_gws_counters "${GW_COUNTERS[@]}")
   t2_record_start storm
   t2_cpustat_snap "$STACK_DIR/cpustat-0"
   ct_stat_snap "$STACK_DIR/ctstat-0"
@@ -673,12 +617,12 @@ phase_storm() {
   usage=$(stack_usage_json)
   t2_cpustat_snap "$STACK_DIR/cpustat-1"
   ct_stat_snap "$STACK_DIR/ctstat-1"
-  s1=$(gws_counters)
+  s1=$(t2_gws_counters "${GW_COUNTERS[@]}")
   wait "$prof"
   prof=""
   t2_record_end storm
   stack_snapshot after
-  d=$(gws_delta "$s0" "$s1")
+  d=$(t2_gws_delta "$s0" "$s1")
   hs=$(hs_rate "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$n")
   [ "$online" = true ] && online=$(lg_get /stats "$(logged_in "$n")")
   [ "$online" = true ] || online=false # a null /stats reading fails the check, not the run
@@ -717,7 +661,7 @@ phase_garbage() {
   t2_check garbage_online "$online" "\"want\": $n, \"online\": $on, \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]')"
   [ "$online" = true ] || { down garbage $((n + inflight)); return; }
   stack_snapshot before
-  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
+  s0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_tcpstat_snap "$STACK_DIR/tcp0"
   t2_cpustat_snap "$STACK_DIR/cpustat-0"
   ct_stat_snap "$STACK_DIR/ctstat-0"
@@ -731,7 +675,7 @@ phase_garbage() {
   prof=$!
   sleep "$secs"
   usage=$(stack_usage_json)
-  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
+  s1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
   t2_tcpstat_snap "$STACK_DIR/tcp1"
   t2_cpustat_snap "$STACK_DIR/cpustat-1"
@@ -743,7 +687,7 @@ phase_garbage() {
   t2_record_end garbage
   stack_snapshot after
   lg_stop
-  d=$(gws_delta "$s0" "$s1")
+  d=$(t2_gws_delta "$s0" "$s1")
   flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
   want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
   t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
@@ -754,7 +698,7 @@ phase_garbage() {
     t2_not_covered "garbage: the bucket (${FLOODS_GARBAGE_BUCKET:-25000}/s) refused nothing on $g; the concurrency limit bound first (garbage_limited.dh_per_s)"
   done
   heartbeats garbage "$on" "$d"
-  lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
+  t2_lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
   t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d, \"flood\": $flood}"
   down garbage $((n + inflight))
   for f in before/gw-0 after/gw-0 garbage/gw-0.cpu.pb.gz garbage/gw-1.cpu.pb.gz garbage/loadgen-flood.cpu.pb.gz; do
@@ -773,7 +717,7 @@ phase_slow() {
   STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
   up slow || { stack_down; return; }
   stack_snapshot before
-  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
+  s0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_record_start slow
   t2_cpustat_snap "$STACK_DIR/cpustat-0"
   ct_stat_snap "$STACK_DIR/ctstat-0"
@@ -808,14 +752,14 @@ phase_slow() {
   usage=$(stack_usage_json)
   lg_in flood lg_stop
   lg_in flood2 lg_stop
-  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
+  s1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
   t2_cpustat_snap "$STACK_DIR/cpustat-1"
   ct_stat_snap "$STACK_DIR/ctstat-1"
   t2_record_end slow
   stack_snapshot after
   lg_stop
-  d=$(gws_delta "$s0" "$s1")
+  d=$(t2_gws_delta "$s0" "$s1")
   win="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-light0" "$STACK_DIR/cpustat-light1"), \"hold\": $(t2_cpusets_json "$STACK_DIR/cpustat-light1" "$STACK_DIR/cpustat-light2")}"
   verdict=$(echo "$win" | lg_py - "$slow_verdict")
   [ "$verdict" = '"valid"' ] ||
@@ -832,7 +776,7 @@ phase_slow() {
   t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}, \"v\": $verdict}" | lg_true - 'd["v"] == "valid" and d["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
     "\"verdict\": $verdict, \"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
   heartbeats slow "$on" "$d" "$verdict"
-  lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
+  t2_lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
   t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"flood_cpus\": {\"slowhs\": $(t2_json_str "$HS_CPUS"), \"slowauth\": $(t2_json_str "$AUTH_CPUS")}, \"flood_procs\": {\"slowhs\": ${FLOODS_SLOW_HS_PROCS:-2}, \"slowauth\": ${FLOODS_SLOW_AUTH_PROCS:-4}}, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"verdict\": $verdict, \"window\": $win, \"series\": $(slow_series "$STACK_DIR/artifacts/slow-series.jsonl"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
   down slow $((n + hs + au))
   for f in before/gw-0 after/gw-0; do
diff --git a/scripts/tier2/floods_test.sh b/scripts/tier2/floods_test.sh
index 3cb2856..97a0e35 100755
--- a/scripts/tier2/floods_test.sh
+++ b/scripts/tier2/floods_test.sh
@@ -2,7 +2,8 @@
 # Tests for scripts/tier2/floods.sh's pure parts on any machine (make
 # test-scripts): in_set, heartbeats' verdict gate, slow_verdict and
 # slow_series over synthetic slow_sampler lines (6b3b3 final review I1,
-# M1-M6). The scenario itself runs on the Tier 2 host.
+# M1-M6). The scenario itself runs on the Tier 2 host. t2_kill_tree, now
+# lib.sh's, is tested in lib_test.sh.
 set -uo pipefail
 cd "$(git rev-parse --show-toplevel)"
 . scripts/tier2/lib.sh
@@ -13,7 +14,7 @@ fails=0
 ok() { echo "ok   $1"; }
 bad() { echo "FAIL $1"; fails=$((fails + 1)); }
 expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
-for f in in_set heartbeats slow_series kill_tree; do
+for f in in_set heartbeats slow_series; do
   eval "$(sed -n "/^$f() {/,/^}/p" scripts/tier2/floods.sh)"
 done
 eval "$(grep '^slow_verdict=' scripts/tier2/floods.sh)"
@@ -106,15 +107,7 @@ s=$(slow_series "$tmp/i2.jsonl")
 expect "series: run-queue wait through a failed scrape" "$(col "$s" gw_runq_ms_per_s)" "[1000.0, 1000.0]"
 expect "series: columns" "$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])["columns"]))' "$s")" 19
 
-# kill_tree: the exit trap stops a phase's profile subshell and the curls
-# inside it, not only the subshell (6b3b4 final review M4).
-(sleep 30 & (sleep 30 & wait) & wait) >/dev/null 2>&1 & tree=$!
-sleep 0.2
-kids=$(pgrep -P "$tree"); kids="$kids $(for p in $kids; do pgrep -P "$p"; done)"
-kill_tree "$tree"; sleep 0.2
-left=0; for p in $tree $kids; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
-[ "$(echo $kids | wc -w)" -eq 3 ] && expect "kill_tree: no process of the tree is left" "$left" 0 || bad "kill_tree: the test tree isn't 4 processes"
-kill $tree $kids 2>/dev/null
-grep -q "^t2_on_exit '\[ -n \"\$prof\" \] && kill_tree \"\$prof\"'$" scripts/tier2/floods.sh && ok "kill_tree: the exit trap uses it" || bad "kill_tree: the exit trap doesn't use it"
+# The exit trap stops the profile subshell with its curls (M4).
+grep -q "^t2_on_exit '\[ -n \"\$prof\" \] && t2_kill_tree \"\$prof\"'$" scripts/tier2/floods.sh && ok "kill_tree: the exit trap uses it" || bad "kill_tree: the exit trap doesn't use it"
 
 [ "$fails" -eq 0 ] && echo "floods_test: all passed" || { echo "floods_test: $fails failed"; exit 1; }
diff --git a/scripts/tier2/gwloss.sh b/scripts/tier2/gwloss.sh
index 5537cec..dce5ff1 100755
--- a/scripts/tier2/gwloss.sh
+++ b/scripts/tier2/gwloss.sh
@@ -286,8 +286,7 @@ t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack
 # started from a non-interactive shell they ignore Ctrl-C's SIGINT, and the
 # goroutine count never ends on its own (6b2b-b2 review I-2).
 storm_helpers=()
-kill_tree() { local c; for c in $(pgrep -P "$1"); do kill_tree "$c"; done; kill "$1" 2>/dev/null; }
-t2_on_exit 'for p in "${storm_helpers[@]}"; do kill_tree "$p"; done'
+t2_on_exit 'for p in "${storm_helpers[@]}"; do t2_kill_tree "$p"; done'
 
 # The storm's counters (Plan 6b2a): Logic's Kicks, lease renewals, sweeps
 # and relogins onto a dead epoch; each survivor's own lease calls and rejects.
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index 25d81f6..8badaad 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -443,6 +443,76 @@ t2_lease_floor() {
   echo $((s / 10 - 1 > (s - 6) / 10 ? s / 10 - 1 : (s - 6) / 10))
 }
 
+# The helpers below were each written for one scenario and moved here with
+# their second user (Plan 6b3c, P6b3bI-3, P6b3b3I-4, P6b3b4I-2). They need
+# stack.sh and loadgen.sh sourced, as every scenario that calls them does.
+
+# t2_kill_tree PID: PID and its descendants, so an exit trap stops a
+# background helper's curls too, not only its subshell (6b3b4 final review
+# M4).
+t2_kill_tree() { local c; for c in $(pgrep -P "$1"); do t2_kill_tree "$c"; done; kill "$1" 2>/dev/null; }
+
+# t2_up NAME: a fresh stack in $T2_OUT/NAME, STACK_DIR exported; checks
+# NAME_stack. Refuses the run if the stack's ports are in use.
+t2_up() {
+  export STACK_DIR=$T2_OUT/$1
+  mkdir -p "$STACK_DIR"
+  ln -sfn ../bin "$STACK_DIR/bin"
+  local busy
+  busy=$(stack_busy_ports)
+  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
+  stack_up 2>"$STACK_DIR/stack-up.err"
+  local rc=$?
+  t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
+  return $rc
+}
+
+# t2_gws_counters SPEC...: {"gw-0": stack_counters SPEC..., ...} of every
+# gateway; t2_gws_delta A B the same of two readings (stack_counters_delta
+# per gateway: a key null in A counts as 0).
+t2_gws_counters() {
+  local g out=""
+  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
+    out+="${out:+, }\"$g\": $(stack_counters "$g" "$@")"
+  done
+  echo "{$out}"
+}
+t2_gws_delta() {
+  python3 -c 'import json, sys
+a, b = (json.loads(x) for x in sys.argv[1:3])
+def delta(x, y):
+    if x is None or y is None:
+        return None
+    return {k: None if v is None else round(v - (x.get(k) or 0), 6) for k, v in y.items()}
+print(json.dumps({g: delta(a.get(g), y) for g, y in b.items()}))' "$1" "$2" 2>/dev/null || echo null
+}
+
+# t2_lease PHASE DELTA SECONDS LOGIC_DELTA: checks PHASE_lease over the
+# gateways' counter delta (t2_gws_delta, with lease_live, lease_missing and
+# lease_error) of a window of SECONDS, and Logic's (lease_error).
+t2_lease() {
+  local floor
+  floor=$(t2_lease_floor "$3")
+  t2_check "${1}_lease" "$(echo "{\"g\": $2, \"l\": $4}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
+    "\"window_s\": $3, \"live_floor\": $floor, \"gateways\": $(echo "$2" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic_lease_error\": $(echo "$4" | lg_py - 'd["lease_error"]')"
+}
+
+# t2_set_ticks LIST [STAT]: "busy total" clock ticks of LIST's CPUs since
+# boot (STAT, default /proc/stat; busy: user, nice, system, irq, softirq,
+# steal; total adds idle and iowait), as t2_cpusets_json counts them.
+t2_set_ticks() {
+  awk -v cpus=" $(t2_cpulist_expand "$1") " '$1 ~ /^cpu[0-9]+$/ && index(cpus, " " substr($1, 4) " ") {
+    b += $2 + $3 + $4 + $7 + $8 + $9; t += $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 } END { print b + 0, t + 0 }' "${2:-/proc/stat}"
+}
+
+# t2_runq_ns PID: nanoseconds PID's threads have waited on a run queue
+# (/proc/PID/task/*/schedstat, field 2; the process's own schedstat is its
+# main thread only); empty if PID is gone.
+t2_runq_ns() {
+  [ -d "/proc/$1/task" ] || return 0
+  cat /proc/"$1"/task/*/schedstat 2>/dev/null | awk '{ s += $2 } END { if (NR) printf "%.0f\n", s }'
+}
+
 t2_swap_pages() { awk '/^pswpin / { i = $2 } /^pswpout / { o = $2 } END { print i + 0, o + 0 }' /proc/vmstat; }
 
 # t2_clock_sampler FILE: appends "unix_s cpu khz" for every CPU every 5s.
diff --git a/scripts/tier2/lib_test.sh b/scripts/tier2/lib_test.sh
index 20923a4..9e3df28 100755
--- a/scripts/tier2/lib_test.sh
+++ b/scripts/tier2/lib_test.sh
@@ -299,6 +299,38 @@ expect "tcpstat: a missing snapshot is null" "$(t2_tcpstat_json "$tmp/none" "$tm
 # up to 5s after its tick, and a short window still needs its one renewal.
 expect "lease floor: 16s, 53s, 66s, 5s" "$(t2_lease_floor 16) $(t2_lease_floor 53) $(t2_lease_floor 66) $(t2_lease_floor 5)" "1 4 6 0"
 
+# The helpers moved here from floods.sh (Plan 6b3c). t2_kill_tree: no
+# process of a subshell tree is left (6b3b4 final review M4).
+(sleep 30 & (sleep 30 & wait) & wait) >/dev/null 2>&1 & tree=$!
+sleep 0.2
+kids=$(pgrep -P "$tree"); kids="$kids $(for p in $kids; do pgrep -P "$p"; done)"
+t2_kill_tree "$tree"; sleep 0.2
+left=0; for p in $tree $kids; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
+[ "$(echo $kids | wc -w)" -eq 3 ] && expect "kill_tree: no process of the tree is left" "$left" 0 || bad "kill_tree: the test tree isn't 4 processes"
+kill $tree $kids 2>/dev/null
+# t2_gws_delta: per gateway; a key null in A counts as 0, one null in B
+# stays null, a gateway missing in A is null.
+expect "gws_delta: per gateway" "$(t2_gws_delta '{"gw-0": {"a": 1, "b": null, "c": 5}}' '{"gw-0": {"a": 4, "b": 2, "c": null}, "gw-1": {"a": 1}}')" \
+  '{"gw-0": {"a": 3, "b": 2, "c": null}, "gw-1": null}'
+expect "gws_delta: unreadable" "$(t2_gws_delta '{' '{}')" null
+# t2_set_ticks: busy and total of the listed CPUs only.
+printf '%s\n' 'cpu  9 9 9 9 9 9 9 9' 'cpu0 1 2 3 4 5 6 7 8' 'cpu1 10 20 30 40 50 60 70 80' 'cpu2 100 0 0 100 0 0 0 0' >"$tmp/stat"
+expect "set_ticks: the listed CPUs" "$(t2_set_ticks 0,2 "$tmp/stat")" "127 236"
+expect "set_ticks: no CPU" "$(t2_set_ticks 5 "$tmp/stat")" "0 0"
+expect "runq_ns: this shell waits a whole number of ns" "$(t2_runq_ns $$ | grep -cE '^[0-9]+$')" 1
+expect "runq_ns: a pid that is gone" "$(t2_runq_ns 999999999)" ""
+# t2_lease: the floor of a 66s window is 6 renewals; Logic's errors fail it.
+f0=$fails
+(
+  . scripts/loadgen.sh
+  t2_check() { echo "$1 $2"; }
+  g='{"gw-0": {"lease_live": 6, "lease_missing": 0, "lease_error": 0}}'
+  expect "lease: 6 renewals in 66s" "$(t2_lease p "$g" 66 '{"lease_error": 0}')" "p_lease true"
+  expect "lease: 5 renewals in 66s" "$(t2_lease p "${g/6/5}" 66 '{"lease_error": 0}')" "p_lease false"
+  expect "lease: Logic's error" "$(t2_lease p "$g" 66 '{"lease_error": 1}')" "p_lease false"
+  [ "$fails" -eq "$f0" ]
+) || fails=$((fails + 1))
+
 # make test-tier2 shows the script's own exit code (make itself exits 2 on any failure).
 out=$(GOIM_TEST_TIER= make -s test-tier2 SCENARIO=race 2>&1)
 contains "make: shows the scenario's exit code" "$out" "test-tier2: race exited 2"
diff --git a/scripts/tier2/slowlogic.sh b/scripts/tier2/slowlogic.sh
index a758397..72fac11 100755
--- a/scripts/tier2/slowlogic.sh
+++ b/scripts/tier2/slowlogic.sh
@@ -243,20 +243,6 @@ t2_report_extra() {
   done
 }
 
-# up NAME: a fresh stack in $T2_OUT/NAME; checks NAME_stack.
-up() {
-  export STACK_DIR=$T2_OUT/$1
-  mkdir -p "$STACK_DIR"
-  ln -sfn ../bin "$STACK_DIR/bin"
-  local busy
-  busy=$(stack_busy_ports)
-  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
-  stack_up 2>"$STACK_DIR/stack-up.err"
-  local rc=$?
-  t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
-  return $rc
-}
-
 # down NAME: loadgen stopped, the gateways idle (dropped Logouts settled and
 # recorded), the stack stopped; checks NAME_idle and NAME_processes.
 down() {
@@ -292,7 +278,7 @@ cal_clients=${SLOWLOGIC_CAL_CLIENTS:-120000} cal_rate=${SLOWLOGIC_CAL_RATE:-8000
 cal_cap=${SLOWLOGIC_CAL_QUEUE_CAP:-1000}
 skip=${SLOWLOGIC_CAL_SKIP:-5} win=${SLOWLOGIC_CAL_WINDOW:-10}
 STACK_GATEWAY_ARGS=(-max-conns $((cal_clients + 10000)) -login-queue-cap "$cal_cap")
-up calibrate || t2_finish
+t2_up calibrate || t2_finish
 cpu_max=$(stack_cpu_max logic)
 want_max="$(python3 -c 'import sys; print(round(float(sys.argv[1]) * 100000))' "$quota_cores") 100000"
 gomaxprocs=$(stack_metric logic go_sched_gomaxprocs_threads)
@@ -384,7 +370,7 @@ back_wait=${SLOWLOGIC_BACK_WAIT:-600}
 busy_win=${SLOWLOGIC_BUSY_WINDOW:-20}
 STACK_GATEWAY_ARGS=(-max-conns $((clients + clients / 10)) -login-queue-cap "$qcap"
   ${SLOWLOGIC_LOGIN_RPCS:+-login-rpcs "$SLOWLOGIC_LOGIN_RPCS"} ${SLOWLOGIC_LOGIN_BATCH:+-login-batch "$SLOWLOGIC_LOGIN_BATCH"})
-up storm || t2_finish
+t2_up storm || t2_finish
 lg_start storm -mode sdk -clients "$clients" -rate "$rate" -uid-prefix slow
 online=false
 lg_wait_online "$clients" $((clients / rate * 3 + 60)) &&
```

- [ ] **Step 2: Check the old names are gone and the scripts parse**

Run:
```bash
grep -nE '(^|[^_a-z])(gws_counters|gws_delta|set_ticks|runq_ns|kill_tree)\b|(^|\s)lease (garbage|slow)|(^|\s)up (calibrate|storm)' scripts/tier2/*.sh scripts/*.sh | grep -v t2_
for f in scripts/tier2/*.sh; do bash -n "$f" || echo "BAD $f"; done
make test-scripts
```
Expected: the `grep` prints only `floods.sh`'s `  up storm || { stack_down; return; }` (floods' own `up` wrapper) and `lib_test.sh`'s `kill_tree` test line; no `BAD`; `lib_test`, `floods_test`, `loadgen_test` and `stack_test` print `all passed` (`lib_test` now with `kill_tree`, `gws_delta`, `set_ticks`, `runq_ns` and `lease` lines).

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/lib.sh scripts/tier2/lib_test.sh scripts/tier2/floods.sh scripts/tier2/floods_test.sh scripts/tier2/gwloss.sh scripts/tier2/slowlogic.sh
git commit -m "refactor: move the shared Tier 2 helpers to lib.sh" -m "kill_tree, up, gws_counters, gws_delta, lease, set_ticks and runq_ns
now have a second user. They move to lib.sh with the t2_ prefix.
No behaviour changes." -m "Refs: P6b3bI-3, P6b3b3I-4, P6b3b4I-2 (Plan 6b3c1 Task 1)."
```

---

### Task 2: Floods: the `refused` outcome and the verdict from the series

**Files:**
- Modify: `internal/loadgen/flood.go`, `internal/loadgen/flood_test.go`, `scripts/tier2/floods.sh`, `scripts/tier2/floods_test.sh`

**Interfaces:**
- Produces: flood outcome `FloodRefused = "refused"` (slow modes: closed by the gateway before `slowHeld`, no hold); `slow_held`'s detail `refused`, `gateway_refusals` (O7); `slow_series FILE N` with `"sat_run_s": {"loadgen", "gateway", "logic": seconds}`; `slow_verdict` over `{"ramp", "hold", "sat"}`.
- Consumes: `t2_gws_*`, `t2_lease`, `t2_set_ticks`, `t2_runq_ns` (Task 1).

- [ ] **Step 1: The patch**

```diff
diff --git a/internal/loadgen/flood.go b/internal/loadgen/flood.go
index e50f258..2af2edc 100644
--- a/internal/loadgen/flood.go
+++ b/internal/loadgen/flood.go
@@ -44,6 +44,7 @@ func floodMode(mode string) bool {
 // Flood outcomes, one per attempt. Dial failures use dialReason's names.
 const (
 	FloodClosed       = "closed"        // the gateway closed (FIN or RST): the outcome the spec expects
+	FloodRefused      = "refused"       // slow modes: the gateway closed before slowHeld, not by its timeout (#135)
 	FloodAnswered     = "answered"      // the gateway sent bytes where it should only close
 	FloodTimeout      = "timeout"       // no close within Timeout: loadgen closed it
 	FloodPreHandshake = "pre-handshake" // slow-auth: closed before the handshake reply (the limit)
@@ -59,12 +60,16 @@ const (
 // every client the wheel closed in one tick back into one tick's interval,
 // so the cohort would be closed and redial together every round, a burst
 // of handshakes each tick (Plan 6b3a, O1). The flood still keeps ~90% of
-// its slots.
+// its slots. A slow client closed sooner is counted FloodRefused, not
+// FloodClosed, with no hold: the gateway's timeouts are the closes floods'
+// slow_freed compares with, and a pending_mem refusal is not one (#135,
+// run 30n).
 const slowHeld = 3 * time.Second
 
 // FloodStats are a flood run's counts (Stats.Flood). Held is from the
 // garbage frame's write, the slow-handshake dial or the slow-auth
-// handshake to the gateway's close, over the attempts it closed.
+// handshake to the gateway's close, over the attempts it closed
+// (FloodClosed; a slow client's FloodRefused has no hold).
 //
 // The rest of a flood's Stats (P6b3aI-4): clients is Config.Clients (for
 // ModeGarbage the in-flight cap), started counts ModeGarbage's attempts
@@ -290,6 +295,9 @@ func (r *Runner) awaitClose(ctx context.Context, nc net.Conn, from time.Time) (t
 		return 0, false
 	case n > 0:
 		r.flood.outcome(FloodAnswered, -1)
+	case clientconn.IsClosed(err) && r.cfg.Mode != ModeGarbage && held < slowHeld:
+		r.flood.outcome(FloodRefused, -1)
+		return held, true
 	case clientconn.IsClosed(err):
 		r.flood.outcome(FloodClosed, held)
 		return held, true
diff --git a/internal/loadgen/flood_test.go b/internal/loadgen/flood_test.go
index 5c6cafd..126b04a 100644
--- a/internal/loadgen/flood_test.go
+++ b/internal/loadgen/flood_test.go
@@ -161,6 +161,30 @@ func TestSlowFloodHeldUntilTheGatewaysTimeout(t *testing.T) {
 	}
 }
 
+// A slow client the gateway closes before slowHeld, here at its pending
+// cap (pending_mem), is counted refused with no hold, not closed: only the
+// gateway's timeouts are closed, so floods' slow_freed compares like with
+// like (#135, run 30n).
+func TestSlowFloodClosedEarlyIsRefused(t *testing.T) {
+	env, _ := startStack(t, 1, func(c *gateway.Config, _ *tcp.Config) {
+		c.HandshakeTimeout, c.PendingCap = 3*time.Second, 2
+	})
+	r, err := New(Config{Mode: ModeSlowHandshake, Clients: 5, Rate: 100, UIDPrefix: "p", Timeout: 10 * time.Second}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	stop := run(r)
+	defer stop()
+	waitFor(t, "2 closed by the timeout", 10*time.Second, func() bool { return r.Stats().Flood.Outcomes[FloodClosed] >= 2 })
+	f := r.Stats().Flood
+	if f.Outcomes[FloodRefused] < 3 || f.Held.P50 < 3000 || f.Held.N != f.Outcomes[FloodClosed] {
+		t.Fatalf("flood %+v, want ≥ 3 refused, and one hold per closed, a timeout's (p50 ≥ 3s)", f)
+	}
+	if got := metric(t, env.GatewayIDs[0], "goim_gateway_admission_rejects_total", "reason", gateway.RejectPendingMem); got < 3 {
+		t.Fatalf("gateway pending_mem %v, want ≥ 3", got)
+	}
+}
+
 // A slow client the gateway's timeout closed redials within one wheel tick,
 // spread over it, so a cohort closed in one tick does not redial together;
 // one closed sooner waits 0.5–1.5s, as the SDK does (O1).
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index 95c7a7e..c013718 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -135,9 +135,19 @@
 #                         p50 >= 5s, p99 <= 6.5s, max <= 7.5s (5s + one tick of
 #                         the wheel, plus scheduling slack, 6b3a O7); a
 #                         slow-auth handshake the limiter refused ends
-#                         pre-handshake, with no hold, and is counted (O1)
+#                         pre-handshake, with no hold, and is counted (O1);
+#                         a connection the gateway closed before 3s (at
+#                         capacity, a limit, pending_mem) is refused, with
+#                         no hold, and is counted (#135); the two floods'
+#                         refused are at most the gateways' refusals over
+#                         the phase (pending_mem + at_capacity + no_slot +
+#                         no_token, the light clients' included: a loose
+#                         bound, but a close no counter explains fails it,
+#                         review O7)
 #   slow_freed            the gateways counted at least as many
-#                         handshake_timeout and auth_timeout closes
+#                         handshake_timeout and auth_timeout closes as the
+#                         floods' closed outcomes, the closes held >= 3s
+#                         (refused ones are not timeouts, #135, run 30n)
 #   slow_legit            every light client logs in beside the floods (or
 #                         gave up as in storm_online): no
 #                         Busy, no reject (login_queue, pending_mem: the
@@ -147,7 +157,10 @@
 #                         recorded. Only a valid run passes: verdict
 #                         "valid" when none of loadgen's, the gateways' and
 #                         Logic's CPU sets was >= 85% busy over the light
-#                         clients' ramp or over the hold, else
+#                         clients' ramp or over the hold, and none was >= 95%
+#                         busy for 5s in a row in slow.series before every
+#                         light client was online (the ramp's average hid
+#                         run 30m's 16s at 98-100%, Plan 6b3c), else
 #                         "loadgen-saturated", "gateway-set-saturated" or
 #                         "logic-set-saturated" (the floods share the last
 #                         two), which fails with a not_covered line (#126:
@@ -493,7 +506,7 @@ slow_sampler() {
   done
 }
 
-# slow_series FILE: slow_sampler's lines as {"columns", "rows" (one per
+# slow_series FILE N: slow_sampler's lines as {"columns", "rows" (one per
 # interval between samples, t_s since the first; a column null where a
 # reading is missing), "first_s" (when each sign of trouble first showed,
 # null if never: a CPU set at >= 85% busy, /stats unanswered, a gateway
@@ -507,7 +520,10 @@ slow_sampler() {
 # alone (M5). gw_runq_ms_per_s is the larger gateway's run-queue wait per
 # second, summed over its threads, null for a gateway whose total went
 # down (a thread gone, M6), kept through a failed scrape (P6b3b3I-2):
-# recorded, not gated (6b3b3 review O2).
+# recorded, not gated (6b3b3 review O2). "sat_run_s": per CPU set, the
+# longest run of rows at >= 95% busy while fewer than N light clients were
+# online (or /stats went unanswered), in seconds: what slow_verdict reads
+# beside the windows' averages (Plan 6b3c, run 30m).
 slow_series() {
   python3 -c 'import json, os, sys
 rows = []
@@ -563,25 +579,39 @@ def first(f):
     return next((r[0] for r in out if f(r)), None)
 pos = lambda k: lambda r: (r[c[k]] or 0) > 0
 sat = lambda k: lambda r: (r[c[k]] or 0) >= 85
-print(json.dumps({"columns": cols, "rows": out, "samples": len(rows), "first_s": {
+def sat_run(k):
+    best = run = 0.0
+    prev = 0.0
+    for r in out:
+        dt, prev = r[0] - prev, r[0]
+        if (r[c["lg_online"]] is None or r[c["lg_online"]] < int(sys.argv[2])) and (r[c[k + "_set_busy_pct"]] or 0) >= 95:
+            run += dt
+            best = max(best, run)
+        else:
+            run = 0.0
+    return round(best, 1)
+print(json.dumps({"columns": cols, "rows": out, "samples": len(rows),
+    "sat_run_s": {k: sat_run(k) for k in ("loadgen", "gateway", "logic")}, "first_s": {
     "loadgen_saturated": first(sat("loadgen_set_busy_pct")), "gateway_set_saturated": first(sat("gateway_set_busy_pct")),
     "logic_set_saturated": first(sat("logic_set_busy_pct")),
     "stats_unanswered": first(lambda r: r[c["lg_online"]] is None),
     "scrape_late": first(lambda r: r[c["scrape_ms"]] is None or r[c["scrape_ms"]] > 1500),
     "pending_mem": first(pos("pending_mem_per_s")), "no_slot": first(pos("no_slot_per_s")),
     "light_refused": first(pos("lg_refused_per_s")),
-    "gave_up": first(pos("lg_gave_up")), "closes_heartbeat": first(pos("closes_heartbeat_per_s"))}}))' "$1" 2>/dev/null || echo null
+    "gave_up": first(pos("lg_gave_up")), "closes_heartbeat": first(pos("closes_heartbeat_per_s"))}}))' "$1" "$2" 2>/dev/null || echo null
 }
 
 # slow_verdict: the lg_py expression for the slow phase's verdict, on
 # {"ramp", "hold": t2_cpusets_json of the light clients' ramp and of the
-# hold}: the first of loadgen's, the gateways' (with slow-auth) and Logic's
-# (with slow-handshake) sets >= 85% busy in either window, as
-# loadgen-saturated, gateway-set-saturated or logic-set-saturated; else
-# valid; unread if a snapshot is missing. Only valid lets slow_legit and
+# hold, "sat": slow_series' sat_run_s}: the first of loadgen's, the
+# gateways' (with slow-auth) and Logic's (with slow-handshake) sets >= 85%
+# busy in either window, or >= 95% for 5s in a row before the light
+# clients were online, as loadgen-saturated, gateway-set-saturated or
+# logic-set-saturated; else valid; unread if a snapshot or the series is
+# missing. Only valid lets slow_legit and
 # slow_heartbeats pass (#126). The order names a set, not a cause: which
 # came first is read in slow.series.first_s (6b3b3 review O3).
-slow_verdict='"unread" if not (d["ramp"] and d["hold"]) else next((k + "-saturated" if k == "loadgen" else k + "-set-saturated" for k in ("loadgen", "gateway", "logic") if any(100 - w[k]["idle_pct"] >= 85 for w in (d["ramp"], d["hold"]))), "valid")'
+slow_verdict='"unread" if not (d["ramp"] and d["hold"] and d["sat"]) else next((k + "-saturated" if k == "loadgen" else k + "-set-saturated" for k in ("loadgen", "gateway", "logic") if any(100 - w[k]["idle_pct"] >= 85 for w in (d["ramp"], d["hold"])) or d["sat"][k] >= 5), "valid")'
 
 t=$SECONDS
 if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
@@ -713,7 +743,7 @@ phase_garbage() {
 phase_slow() {
   local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} hs=${FLOODS_SLOW_HANDSHAKE:-80000}
   local au=${FLOODS_SLOW_AUTH:-40000} srate=${FLOODS_SLOW_RATE:-10000} hold=${FLOODS_SLOW_HOLD:-60}
-  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win verdict
+  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win series verdict
   STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
   up slow || { stack_down; return; }
   stack_snapshot before
@@ -761,23 +791,29 @@ phase_slow() {
   lg_stop
   d=$(t2_gws_delta "$s0" "$s1")
   win="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-light0" "$STACK_DIR/cpustat-light1"), \"hold\": $(t2_cpusets_json "$STACK_DIR/cpustat-light1" "$STACK_DIR/cpustat-light2")}"
-  verdict=$(echo "$win" | lg_py - "$slow_verdict")
+  series=$(slow_series "$STACK_DIR/artifacts/slow-series.jsonl" "$n")
+  verdict=$(echo "{\"w\": $win, \"s\": $series}" | lg_py - '{"ramp": d["w"]["ramp"], "hold": d["w"]["hold"], "sat": (d["s"] or {}).get("sat_run_s")}' | lg_py - "$slow_verdict")
   [ "$verdict" = '"valid"' ] ||
     t2_not_covered "slow: legitimate logins and heartbeats beside the floods; verdict $verdict, ruled by the order in slow.series.first_s (slow.window, slow.series)"
   # Slow-auth's handshakes meet the limiter: a refused one ends pre-handshake,
-  # with no hold, and the bounds are on the holds the timeouts ended (O1).
+  # with no hold, and the bounds are on the holds the timeouts ended (O1). A
+  # close before 3s is refused, with no hold, in both floods (#135).
   held=true
-  for inst in slowhs:'{"closed"}' slowauth:'{"closed", "pre-handshake"}'; do
+  for inst in slowhs:'{"closed", "refused"}' slowauth:'{"closed", "pre-handshake", "refused"}'; do
     [ "$(lg_true "$STACK_DIR/artifacts/loadgen-${inst%%:*}.json" 'set(d["flood"]["outcomes"]) <= '"${inst#*:}"' and "closed" in d["flood"]["outcomes"] and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" = true ] || held=false
   done
-  t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}')"
+  # Each refused close needs a gateway refusal to explain it (review O7).
+  refused=$(echo "{\"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("refused", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("refused", 0)')}" | lg_py - 'd["h"] + d["a"]')
+  refusals=$(echo "$d" | lg_py - 'sum(v[k] for v in d.values() for k in ("rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token"))')
+  [ "$(echo "{\"r\": ${refused:-null}, \"g\": ${refusals:-null}}" | lg_true - 'd["r"] <= d["g"]')" = true ] || held=false
+  t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"refused\": ${refused:-null}, \"gateway_refusals\": ${refusals:-null}"
   t2_check slow_freed "$(echo "{\"g\": $d, \"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')}" | lg_true - 'sum(v["rejects_handshake_timeout"] for v in d["g"].values()) >= d["h"] and sum(v["rejects_auth_timeout"] for v in d["g"].values()) >= d["a"]')" \
     "\"handshake_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_handshake_timeout"] for v in d.values())'), \"slowhs_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"auth_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_auth_timeout"] for v in d.values())'), \"slowauth_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')"
   t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}, \"v\": $verdict}" | lg_true - 'd["v"] == "valid" and d["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
     "\"verdict\": $verdict, \"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
   heartbeats slow "$on" "$d" "$verdict"
   t2_lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"flood_cpus\": {\"slowhs\": $(t2_json_str "$HS_CPUS"), \"slowauth\": $(t2_json_str "$AUTH_CPUS")}, \"flood_procs\": {\"slowhs\": ${FLOODS_SLOW_HS_PROCS:-2}, \"slowauth\": ${FLOODS_SLOW_AUTH_PROCS:-4}}, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"verdict\": $verdict, \"window\": $win, \"series\": $(slow_series "$STACK_DIR/artifacts/slow-series.jsonl"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
+  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"flood_cpus\": {\"slowhs\": $(t2_json_str "$HS_CPUS"), \"slowauth\": $(t2_json_str "$AUTH_CPUS")}, \"flood_procs\": {\"slowhs\": ${FLOODS_SLOW_HS_PROCS:-2}, \"slowauth\": ${FLOODS_SLOW_AUTH_PROCS:-4}}, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"verdict\": $verdict, \"window\": $win, \"series\": $series, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
   down slow $((n + hs + au))
   for f in before/gw-0 after/gw-0; do
     grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
diff --git a/scripts/tier2/floods_test.sh b/scripts/tier2/floods_test.sh
index 97a0e35..de25da5 100755
--- a/scripts/tier2/floods_test.sh
+++ b/scripts/tier2/floods_test.sh
@@ -42,12 +42,20 @@ expect "heartbeats: a null verdict fails" "$(hb null)" "false None"
 
 # slow_verdict: the order and the threshold.
 w() { echo "{\"loadgen\": {\"idle_pct\": $1}, \"gateway\": {\"idle_pct\": $2}, \"logic\": {\"idle_pct\": $3}}"; }
-chk() { echo "{\"ramp\": $1, \"hold\": $2}" | lg_py - "$slow_verdict"; }
+sat() { echo "{\"loadgen\": $1, \"gateway\": $2, \"logic\": $3}"; }
+chk() { echo "{\"ramp\": $1, \"hold\": $2, \"sat\": ${3:-$(sat 0 0 0)}}" | lg_py - "$slow_verdict"; }
 expect "verdict: valid" "$(chk "$(w 50 50 50)" "$(w 50 50 50)")" '"valid"'
 expect "verdict: loadgen first" "$(chk "$(w 10 10 10)" "$(w 50 50 50)")" '"loadgen-saturated"'
 expect "verdict: gateways before Logic" "$(chk "$(w 50 10 10)" "$(w 50 50 50)")" '"gateway-set-saturated"'
 expect "verdict: 85.0% busy is in" "$(chk "$(w 15.1 15.1 15.1)" "$(w 15 50 50)")" '"loadgen-saturated"'
 expect "verdict: unread" "$(chk null "$(w 50 50 50)")" '"unread"'
+# The series' runs at >= 95% before every light client was online: 5s in
+# a row fails a valid pair of windows (run 30m), 4.9s does not, and a
+# missing series is unread (Plan 6b3c).
+expect "verdict: a 5s run" "$(chk "$(w 50 50 50)" "$(w 50 50 50)" "$(sat 5 0 0)")" '"loadgen-saturated"'
+expect "verdict: a 4.9s run" "$(chk "$(w 50 50 50)" "$(w 50 50 50)" "$(sat 4.9 4.9 4.9)")" '"valid"'
+expect "verdict: a gateway run" "$(chk "$(w 50 50 50)" "$(w 50 50 50)" "$(sat 0 6 9)")" '"gateway-set-saturated"'
+expect "verdict: no series" "$(chk "$(w 50 50 50)" "$(w 50 50 50)" null)" '"unread"'
 
 # slow_series over synthetic lines. gwj NO_SLOT [CLOSES] is one gateway's
 # reading; line MS SKEW_MS GW0 GW1 [LG [RQ0 RQ1]] one sample.
@@ -60,7 +68,7 @@ first() { python3 -c 'import json,sys; print(json.loads(sys.argv[1])["first_s"][
 # row's rates are taken from the last whole sample, not one gateway's
 # total against two (I1).
 { line 0 10 "$(gwj 5000)" "$(gwj 5000)"; line 1000 10 "$(gwj 5000)" null; line 2000 10 "$(gwj 5000)" "$(gwj 5000)"; } >"$tmp/i1.jsonl"
-s=$(slow_series "$tmp/i1.jsonl")
+s=$(slow_series "$tmp/i1.jsonl" 10)
 expect "series: a missing gateway nulls its row" "$(col "$s" no_slot_per_s)" "[None, 0.0]"
 expect "series: a missing gateway's online" "$(col "$s" online_conns)" "[None, 200]"
 expect "series: a missing gateway is no refusal" "$(first "$s" no_slot)" None
@@ -69,7 +77,7 @@ expect "series: a missing gateway is no refusal" "$(first "$s" no_slot)" None
 # gateway readings, and the refusals it saw are dated by the next on-time
 # sample, against the last on-time one (M1).
 { line 0 10 "$(gwj 0)" "$(gwj 0)"; line 1000 2500 "$(gwj 1500)" "$(gwj 1500)"; line 4000 10 "$(gwj 1500)" "$(gwj 1500)"; line 5000 10 "$(gwj 1500)" "$(gwj 1500)"; } >"$tmp/m1.jsonl"
-s=$(slow_series "$tmp/m1.jsonl")
+s=$(slow_series "$tmp/m1.jsonl" 10)
 expect "series: scrape_ms" "$(col "$s" scrape_ms)" "[2500, 10, 10]"
 expect "series: a late scrape nulls its row" "$(col "$s" no_slot_per_s)" "[None, 750.0, 0.0]"
 expect "series: a late scrape is dated" "$(first "$s" scrape_late)" 1.0
@@ -78,10 +86,10 @@ expect "series: refusals dated by the next on-time sample" "$(first "$s" no_slot
 # A counter with no sample yet counts 0 before its first rise (M2).
 nc() { gwj 0 | sed 's/"closes_heartbeat": 0/"closes_heartbeat": null/'; }
 { line 0 10 "$(nc)" "$(nc)"; line 1000 10 "$(nc)" "$(nc)"; line 2000 10 "$(gwj 0 4)" "$(nc)"; } >"$tmp/m2.jsonl"
-s=$(slow_series "$tmp/m2.jsonl")
+s=$(slow_series "$tmp/m2.jsonl" 10)
 expect "series: a first rise from no sample" "$(col "$s" closes_heartbeat_per_s)" "[None, None]"
 { line 0 10 "$(nc)" "$(nc)"; line 1000 10 "$(nc)" "$(nc)"; line 2000 10 "$(gwj 0 4)" "$(gwj 0 0)"; } >"$tmp/m2.jsonl"
-s=$(slow_series "$tmp/m2.jsonl")
+s=$(slow_series "$tmp/m2.jsonl" 10)
 expect "series: a first rise from no sample counts" "$(col "$s" closes_heartbeat_per_s)" "[None, 4.0]"
 expect "series: a first rise is dated" "$(first "$s" closes_heartbeat)" 2.0
 
@@ -89,7 +97,7 @@ expect "series: a first rise is dated" "$(first "$s" closes_heartbeat)" 2.0
 # no_slot (M5).
 lg() { echo "{\"online\": 10, \"gave_up\": 0, \"failed\": $1, \"refused\": $2}"; }
 { line 0 10 "$(gwj 0)" "$(gwj 0)" "$(lg 0 0)"; line 1000 10 "$(gwj 900)" "$(gwj 900)" "$(lg 0 0)"; line 2000 10 "$(gwj 1800)" "$(gwj 1800)" "$(lg 6 6)"; } >"$tmp/m5.jsonl"
-s=$(slow_series "$tmp/m5.jsonl")
+s=$(slow_series "$tmp/m5.jsonl" 10)
 expect "series: lg_refused_per_s" "$(col "$s" lg_refused_per_s)" "[0.0, 6.0]"
 expect "series: the light clients' first refusal" "$(first "$s" light_refused)" 2.0
 expect "series: the gateways' first no_slot" "$(first "$s" no_slot)" 1.0
@@ -97,14 +105,23 @@ expect "series: the gateways' first no_slot" "$(first "$s" no_slot)" 1.0
 # A run-queue total that went down (a thread gone, a restarted gateway) is
 # no reading, never a negative wait (M6).
 { line 0 10 "$(gwj 0)" "$(gwj 0)" null 5000000000 5000000000; line 1000 10 "$(gwj 0)" "$(gwj 0)" null 1000000000 1000000000; line 2000 10 "$(gwj 0)" "$(gwj 0)" null 1100000000 1000000000; } >"$tmp/m6.jsonl"
-s=$(slow_series "$tmp/m6.jsonl")
+s=$(slow_series "$tmp/m6.jsonl" 10)
 expect "series: no negative run-queue wait" "$(col "$s" gw_runq_ms_per_s)" "[None, 100.0]"
 
 # A gateway's failed scrape keeps its run-queue wait, which is read beside
 # it, not inside it (P6b3b3I-2): gw-1 waits 1s/s and its middle scrape fails.
 { line 0 10 "$(gwj 0)" "$(gwj 0)" null 0 0; line 1000 10 "$(gwj 0)" null null 100000000 1000000000; line 2000 10 "$(gwj 0)" "$(gwj 0)" null 200000000 2000000000; } >"$tmp/i2.jsonl"
-s=$(slow_series "$tmp/i2.jsonl")
+s=$(slow_series "$tmp/i2.jsonl" 10)
 expect "series: run-queue wait through a failed scrape" "$(col "$s" gw_runq_ms_per_s)" "[1000.0, 1000.0]"
+# sat_run_s: the longest run at >= 95% while fewer than N were online (or
+# /stats went unanswered); a run ends at a row under 95% or once all N are
+# online. Sets as [busy, total] ticks: 100 a second total; lg's 10 online.
+sl() { echo "{\"ms\": $1, \"ticks\": [0, 0, 0], \"sets\": {\"loadgen\": [$2, $(($1 / 10))], \"gateway\": [0, $(($1 / 10))], \"logic\": [0, $(($1 / 10))]}, \"lg\": $3, \"rq\": {}, \"gw\": {\"gw-0\": $(gwj 0)}, \"ms_end\": $(($1 + 10))}"; }
+{ sl 0 0 "$(lg 0 0)"; sl 1000 96 "$(lg 0 0)"; sl 2000 192 null; sl 3000 288 "$(lg 0 0)"; sl 4000 300 "$(lg 0 0)"; sl 5000 396 "$(lg 0 0)"; sl 6000 492 "$(lg 0 0)"; } >"$tmp/sat.jsonl"
+s=$(slow_series "$tmp/sat.jsonl" 20)
+expect "series: sat_run_s" "$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["sat_run_s"])' "$s")" "{'loadgen': 3.0, 'gateway': 0.0, 'logic': 0.0}"
+s=$(slow_series "$tmp/sat.jsonl" 10)
+expect "series: all online, only the unanswered second counts" "$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["sat_run_s"]["loadgen"])' "$s")" 1.0
 expect "series: columns" "$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])["columns"]))' "$s")" 19
 
 # The exit trap stops the profile subshell with its curls (M4).
```

- [ ] **Step 2: The new test fails without the change, then passes**

Run (the mutation turns the new case off; `$S` is the session scratchpad):
```bash
cp internal/loadgen/flood.go "$S/flood.go"
sed -i 's/r.cfg.Mode != ModeGarbage \&\& held < slowHeld:/false:/' internal/loadgen/flood.go
go test -count=1 -run TestSlowFloodClosedEarlyIsRefused ./internal/loadgen/; cp "$S/flood.go" internal/loadgen/flood.go
go test -race -count=1 -run 'TestSlowFlood|TestRedial|TestSlowAuth|TestGarbage' ./internal/loadgen/
```
Expected: first `FAIL` (`Outcomes:map[closed:2]`, no `refused`), then `ok`.

- [ ] **Step 3: Script tests and a slow-phase dry run**

Run `make test-scripts`. Expected: `floods_test` passes with the new `verdict: a 5s run`, `a 4.9s run`, `a gateway run`, `no series`, `series: sat_run_s` and `all online, only the unanswered second counts` lines.

Then the slow phase alone on the dev machine, with the throwaway copy of Plan 6b3b4's Task 2 Step 2 (`$S` is the session scratchpad):
```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/floods.sh "$D/floods.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 ss$|t2_need curl taskset python3 ss|' "$D/floods.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/floods.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/floods.sh"
rm -rf tier2-results/floods-*
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 FLOODS_PHASES=slow \
  FLOODS_CLIENTS=2000 FLOODS_RATE=500 FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35 \
  bash "$D/floods.sh" >"$D/out.txt" 2>&1; echo "exit $?"
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/floods-*/result.json")[0])); print({k: v["pass"] for k, v in d["checks"].items()}); print(d["slow"]["verdict"], d["slow"]["series"]["sat_run_s"]); print({k: d["checks"]["slow_held"][k] for k in ("refused", "gateway_refusals")})'
```
Expected (prototype, ~2 min): `exit 1`; 12 checks, failing only `no_swap_in_slow` (the dev machine's swap, if it swaps); `valid` with `sat_run_s` 0.0 on every set. `slow_held`'s outcomes are `closed` only at this scale (no early close), its `refused` 0 and `gateway_refusals` 0 (O7: at most), and `slow_freed` passes. Then `rm -rf tier2-results/floods-*`.

- [ ] **Step 4: Commit**

```bash
git add internal/loadgen/flood.go internal/loadgen/flood_test.go scripts/tier2/floods.sh scripts/tier2/floods_test.sh
git commit -m "fix: count early slow closes as refused, gate saturated runs" -m "A slow flood connection that the gateway closes before 3s is now
counted as refused, with no hold. slow_freed then compares the
timeouts with the closes that were timeouts.
slow_verdict also reads slow.series. A CPU set at 95% busy or more for
5s in a row, before every light client is online, fails the verdict.
slow_held also needs the refused closes to be at most the gateways'
refusals over the phase." -m "Refs: #135, #136, runs 30m and 30n, review O7 (Plan 6b3c1 Task 2)."
```

---

### Task 3: Run 30o (optional) and the record

- [ ] **Step 1 (optional, the user's call): run 30o, the slow phase at 20,000/s**

```bash
git pull && GOIM_TEST_TIER=2 FLOODS_PHASES=slow FLOODS_RATE=20000 make test-tier2 SCENARIO=floods
```
Expected: run 30m's shape (loadgen's set at 98–100% for ~16s in the ramp), now verdict `loadgen-saturated` with `slow.series.sat_run_s.loadgen` ≥ 5, so `slow_legit` and `slow_heartbeats` fail labelled, as the user ruled. A `valid` verdict here would mean the rule missed it. Read `slow_held`'s `refused` against `gateway_refusals` (O7).

- [ ] **Step 2: Record**

In this plan's *Execution Notes*: the commits, and run 30o's commit, duration, checks and verdict if it ran. In the roadmap: the plan 6 row (6b3c1 executed).

```bash
git add docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3c1-helpers-floods.md
git commit -m "docs: record Plan 6b3c1 execution" -m "Refs: #135, #136 (Plan 6b3c1 Task 3)."
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **Plan 6b3c2** (`2026-10-17-connection-session-core-06b3c2-reconnect.md`): loadgen `/reset`, `/network-change`, `/ghost`, `/clients`; scenario `reconnect` (phases `reset`, `netchange`, `ghost`); run 31. It uses Task 1's helpers.
- Then **6c** (Redis Cluster), **6d** (Tier 3, k8s).

## Execution Notes

**Executed 2026-10-18** (inline, superpowers:executing-plans), on `feat/plan6b3` from `69dfa80`:
- Task 1: `5fc44fa` (helpers to `lib.sh`). The new `lib_test` lines failed first (7 fails), then `make test-scripts` passed. The `grep` printed only floods' `up storm` and `lib_test`'s `kill_tree` line, as expected.
- Task 2: `c367041` (`refused`, the O7 bound, the series verdict). The new `floods_test` lines failed first (5 fails). The mutation failed `TestSlowFloodClosedEarlyIsRefused` with `Outcomes:map[closed:3]` (the plan expected `closed:2`; the same shape, no `refused`). Dry run (slow phase, 2,000 at 500/s): `exit 1`, 12 checks, only `no_swap_in_slow` failing; verdict `valid`, `sat_run_s` 0.0 on every set; `slow_held` outcomes `closed` 3,500 each, `refused` 0 against `gateway_refusals` 0; `slow_freed` 3,500 timeouts against 3,500 `closed`. `make test` (race), `go vet ./...` and `make test-scripts` green.
- Task 3: run 30o (optional) not run; the user's call.

**Final review** (fresh Opus reviewer, whole branch `69dfa80..c367041`): 0 Critical, 0 Important, 3 Minor. Both Review Focus items hold: the 5s/4.9s edges are tested; every key the O7 bound reads is in `GW_COUNTERS`, and a null fails safe. Task 1's "no behaviour change" holds; Plan 6b3c2's calls match the moved helpers.
- **M1, re-graded Important and fixed** (`e5f401d`): `sat_run_s` counted rows while fewer than N light clients were online. A client that gave up never comes online, so a few gave-ups (run 30g had 8) stretched the window over the whole hold. A short burst at ≥ 95% there would give a false `*-saturated` verdict. The window now ends at online + gave-up ≥ N, as `settled` does. Test `series: gave-up clients end the ramp` failed first (6.0 against 0.0), then passed.
- M2 (deferred, the user's call): `refused` and `refusals` are not in `phase_slow`'s `local` line, so they are globals. No clash now.
- M3 (deferred, the user's call): `slowHeld` (3s) equals the smallest allowed auth timeout. With `-auth-timeout 3s`, a slow-auth timeout close can read 2.99s (loadgen starts its clock after the handshake reply) and count `refused`. Not at floods' 5s defaults.

