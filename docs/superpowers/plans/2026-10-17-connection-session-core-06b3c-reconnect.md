# Plan 6b3c: Whole-Population Reconnect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's last Tier 2 population test, the whole-population reconnect (R5S-O6), as scenario `reconnect` and Tier 2 run 31. Before it, close the open notes the earlier 6b3 plans left for this plan: the shared helpers to `lib.sh` (P6b3bI-3, P6b3b3I-4, P6b3b4I-2), the slow floods' `refused` outcome (#135, run 30n), and the slow verdict's blind spot (run 30m).

**Architecture:** Four parts, one task each, then the run:
- **Helpers to `lib.sh`** (Task 1). The helpers that now have a second user move to `scripts/tier2/lib.sh` with the `t2_` prefix: `t2_kill_tree` (gwloss, floods), `t2_up` (slowlogic, floods, reconnect), `t2_gws_counters`/`t2_gws_delta`, `t2_lease`, `t2_set_ticks` and `t2_runq_ns` (floods, reconnect). No behaviour changes.
- **Floods' slow checks** (Task 2). Loadgen's slow modes count a gateway close before `slowHeld` (3s) as `refused`, with no hold (#135), so `slow_freed` compares the gateways' timeouts with the closes that were timeouts. `slow_verdict` also reads `slow.series`: a CPU set at ≥ 95% busy for 5s in a row before every light client is online fails the verdict (#136.2), so a long settle can no longer average a saturated ramp under 85% (run 30m).
- **Loadgen `POST /reset` and `POST /network-change`** (Task 3, P6a2bI-2, #134). `/reset` marks every gateway (`gw=all`) and then closes every SDK client's newest connection with `SO_LINGER 0`: each gateway gets an RST, each client loses its connection and waits its 0–20s storm window. `/network-change` calls `sdk.Client.NetworkChanged` on every client. Both are sdk mode only. Loadgen also counts every SDK wait by kind since the latest mark (`storm_since_mark.waits`) and reports its balancer's open connections per gateway (`balancer_open`).
- **Scenario `reconnect`** (Task 4). Phases `reset` and `netchange`, each on a fresh stack: 600,000 SDK clients over 3 gateways, as gwloss b_stress runs them (`-mode sdk -precompute`, b_stress's CPU sets, 4 GOMAXPROCS per gateway), the spec's 250k cap and a 30s heartbeat (#136.3, #136.4). Then `/reset` (and, in `netchange`, `/network-change` once every client has lost its connection). It measures the time until 99% are back and the share that reached backoff, with no SLO, and checks that every client comes back, that the clients already back lose no connection to a Pong timeout, and that no component fails. A per-second series per gateway records #133's readings and a funnel reading, not gated.
- **Run 31** (Task 5): the user runs `reconnect` on the Tier 2 host, and (optional) the slow phase at 20,000/s again to see the new verdict.

**Tech Stack:** Go 1.27.1 (`internal/loadgen`, `cmd/loadgen`), bash 5.2, python3 (JSON and arithmetic only), `taskset`, `/proc/stat`.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Success criteria* (~l.28): whole-population reconnect is best effort, no SLO; recovery ≈ users ÷ admitted login rate plus the backoff tail; admission control shapes it; no component may fail or fall over.
- *Load tests* (~l.548): drop every connection on all gateways at once (RST from loadgen in one instant); measured: the time until 99% are logged in again and the share of clients reaching backoff; heartbeats of already-reconnected clients answered on time; no component restarts. The same with every client raising a network-change event at once.
- *Client SDK rules*, *Local trigger* and *Precedence* (~l.128–129): a network change cuts only the 0–20s storm wait, to 0–1s.
- *Storms, drain, failures*, row *Whole-population reconnect* (~l.428).

Inputs:
- The roadmap's #133 (series, funnel, not gated), #134 (600k SDK clients, `/reset`), #135 (`refused`), and #136 (this plan's four rulings, below).
- P6a2bI-2 and P6a2bI-3 (the 6a2a plan's *Execution Notes*, ~l.1539): `/reset`, `/network-change`, the backoff count (already in loadgen: `backoff_since_mark`), a phase boundary on a fresh stack.
- P6b3bI-2/-3 (the 6b3b plan, ~l.808), P6b3b3I-4 (the 6b3b3 plan, ~l.571), P6b3b4I-2 and runs 30m/30n (the 6b3b4 plan's *Execution Notes*, ~l.895–910).
- gwloss b_stress's 30j rerun (6b3b4 *Execution Notes*, ~l.919): 600,000 back in 127s from one gateway's SIGKILL, loadgen-bound, `pending_mem` 0, `login_queue` 17,713 + 16,898. Run 31 is the first with every gateway's clients reconnecting at once.

**Decisions:**
- #133, #134, #135 (the user, 2026-10-17, before writing).
- **#136 (the user, 2026-10-17, with AskUserQuestion, before writing):** (1) one plan, not split into 6b3c1/6b3c2; (2) the verdict's series rule is ≥ 95% busy for ≥ 5s in a row (not ≥ 85%: run 30l's gateway set was at 84.8–87.6% from 2.2s to 9.0s in a valid run); (3) `reconnect`'s gateways send a 30s heartbeat (not the 150s default), so the clients back send Pings during the storm; (4) the spec's 250k cap (not b_stress's 650k), so the cap may shape the storm, as the spec says; `at_capacity` is recorded, not gated. #136 is in the roadmap with this plan's commit.

**Branch:** `feat/plan6b3`. Execute from its head, this plan's own commit or later.

**Size:** 5 tasks. Task 1 moves ~70 lines into `lib.sh` (+32 test lines). Task 2 changes ~10 lines of `flood.go`, ~60 of `floods.sh`, +55 test lines. Task 3 adds ~140 lines to `internal/loadgen` and a 139-line test file. Task 4 adds `reconnect.sh` (439 lines) and its 42-line test. Task 5 is the user's run and the record. The prototype `proto-6b3c` (from `9841396`; tasks `f2546aa`, `eb1a2e4`, `460f8be`, `f00f4f5`) ran every step. Its diff is replayed here byte for byte: the changed files as `git apply` patches, one per task, the new files inside them. On the dev machine (AMD EPYC 7742, 2 cores):
- `make test` (race), `go vet ./...` and `make test-scripts` green.
- Mutations: 1 caught in Task 2 (the `refused` case), 5 caught in Task 3; the series rule's and the funnel's by the script tests.
- `reconnect` dry run (3,000 clients): 21 checks, only the two dev swap checks failing. `reset`: every client lost the connection within 133ms (`close_ms` 132), all back in 20s (p99 19.9s, the 0–20s storm wait). `netchange`: the network change sent 399ms after the mark, 2,970 of 3,000 waits cut to local, all back in 1.8s.
- `floods` slow-phase dry run (2,000 light clients at 500/s): 12 checks, only the dev swap check failing; verdict `valid`, `sat_run_s` 0 on every set; `slow_freed` 3,500 timeouts against 3,500 `closed`. A forced run (20,000 at 4,000/s) failed `slow_legit` and `slow_heartbeats` on verdict `loadgen-saturated`, with `sat_run_s` 9.9s on each set (its ramp window was also over 85%, 92.8%, so that run does not separate the two rules; `floods_test.sh` does).

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- No change to the gateway, Logic, the SDK or the wire. Go changes only in `internal/loadgen` and `cmd/loadgen`'s doc comment.
- Loadgen's existing endpoints, flags and result fields keep their meaning. New: `POST /reset`, `POST /network-change`, `Stats.balancer_open`, `storm_since_mark.waits`, flood outcome `refused`.
- No check of `floods`, `gwloss` or `slowlogic` changes except `floods`' `slow_held` (allows `refused`), `slow_freed` (via loadgen's `closed`) and `slow_verdict` (reads the series). Their check counts stay (`floods` 30).
- `reconnect` gates no SLO (the spec has none): it gates only that every client comes back within `RECONNECT_BACK_WAIT`, the drop reached everyone, no Pong timeout, lease renewals, idle, processes, swap and artifacts. `pending_mem`, the funnel, `at_capacity`, backoff and recovery times are recorded (#133).

## Review Focus

1. **"One instant" is 600k closes.** `/reset` takes the mark, then 64 workers close every connection (`eachSDK`). In the dry run 3,000 took 132ms; on the host 600k may take seconds, each close also running the gateway's side of the RST on loopback (the 30j profile: closes were 15% of loadgen's storm). A client closed late starts its storm wait late, so its relogin time from the mark includes it. `P_drop` bounds every client's detection by `RECONNECT_DROP_MAX_S` (10s) and records `close_ms` and the detection distribution beside the recovery times.
2. **A network change to an online client does nothing.** `sdk.Client.local` acts only in a wait or a failing attempt. So `netchange` posts `/network-change` only once every client has lost its connection (`slo.lost == population`), and a client whose 0–20s wait ended before that keeps its attempt: `waits.local` counts the waits cut, `network_change_after_ms` the delay (dry run: 2,970 of 3,000 at 399ms).
3. **The logout queue is the cap's size.** At the reset each gateway queues ~200k Logouts at once into a 250k queue (`-logout-queue` follows `-max-conns`), while the relogins replace those sessions, and again at the stop. A full queue drops Logouts (reconciled later by design), so `P_idle` allows dropped ones (`stack_wait_idle … dropped-ok`) and records them per gateway, and the series has `logouts_dropped_per_s`.
4. **12 gateway threads on 8 CPUs.** As #133 sized it (2 handshake slots each), every gateway has 4 GOMAXPROCS on b_stress's 8-CPU gateway set, and all three handshake at once (in b_stress only two did, in the storm). The gateway set may saturate before Logic does; the series' `gateway_set_busy_pct` beside `pending_mem_per_s` and `busy_per_s` says which came first. `RECONNECT_GATEWAY_PROCS` changes it for a rerun.
5. **The heartbeat check is by absence.** SDK clients record no Pong RTT, only a Pong timeout (10s) as `lost["pong-timeout"]`. `P_heartbeats` passes only if every connection that ended after the mark was the reset's. With a 30s heartbeat and a hold of the heartbeat + 15s, every client back sends at least one Ping after its relogin. In a loadgen-bound storm a late read on loadgen's side could also cause a Pong timeout: the check still fails, and `P_back.loadgen_bound` is beside it to read.

## Implementation choices this plan makes

1. **Which helpers move.** Only those with a second user: `kill_tree`, `up` (floods keeps a wrapper that also starts its conntrack sampler), `gws_counters`/`gws_delta` (now with the counters as arguments), `lease`, `set_ticks` (with an optional stat file, for its test) and `runq_ns`. `heartbeats`, `settled`, `logged_in` and `in_set` stay in `floods.sh`: they read light clients' results, and `reconnect` runs SDK clients. `down` stays per scenario (P6b3bI-3): `reconnect` ends its phase as gwloss does. `new_per_s` is not reused (P6b3bI-2): `reconnect` gates no rate.
2. **The reset closes from loadgen's side.** Each SDK client keeps its newest connection (`sdkClient.conn`, set in its `Dial` wrapper), and `replyConn.abort` sets `SO_LINGER 0` on the TCP socket and closes it. The client's read fails, so the SDK ends the connection as `EndLost` and waits its storm window, as after an RST from the network. The mark comes first, so the population is every client online at the mark.
3. **`eachSDK`** runs a function on every started SDK client with 64 workers, as `Run`'s stop does: `NetworkChanged` waits for the client's run loop.
4. **Waits since the mark by kind** (`StormCount.Waits`), from every `EventWaiting`, so `netchange` shows how many storm waits it cut (`local`). The existing backoff counts stay as they are.
5. **`reconnect` has its own sampler and series** (`rc_sampler`, `rc_series`), per gateway. `floods`' `slow_sampler`/`slow_series` sum the gateways, and the funnel needs each gateway apart (6b3b3's *After this plan*: generalize only if needed; the two shapes differ). Both use the moved `t2_set_ticks`.
6. **The funnel reading** (#133): a row where one gateway refused at `pending_mem` while another, under the cap, had no socket in HANDSHAKE. `rc_series` sums those rows' seconds and gives the first, the longest run and the first 20.
7. **A fresh stack per phase** (P6a2bI-3): `netchange` does not start on `reset`'s Logouts.

## File Structure

- `scripts/tier2/lib.sh`: `t2_kill_tree`, `t2_up`, `t2_gws_counters`, `t2_gws_delta`, `t2_lease`, `t2_set_ticks`, `t2_runq_ns` (Task 1).
- `scripts/tier2/lib_test.sh`: their tests (Task 1).
- `scripts/tier2/floods.sh`: the moved helpers' callers (Task 1); `slow_held`, `slow_freed`'s doc, `slow_series` N and `sat_run_s`, `slow_verdict` (Task 2).
- `scripts/tier2/floods_test.sh`: `kill_tree`'s test moves out (Task 1); the verdict's and `sat_run_s`' tests (Task 2).
- `scripts/tier2/gwloss.sh`, `scripts/tier2/slowlogic.sh`: callers only (Task 1).
- `internal/loadgen/flood.go`, `flood_test.go`: `FloodRefused` (Task 2).
- `internal/loadgen/runner.go`, `sdkclient.go`, `recorder.go`, `control.go`, new `reset_test.go`; `cmd/loadgen/main.go` (doc) (Task 3).
- `scripts/tier2/reconnect.sh` (new), `scripts/tier2/reconnect_test.sh` (new), `Makefile` (Task 4).
- This plan's *Execution Notes*, the roadmap, the README (only if the user asks) (Task 5).

---

### Task 1: Shared helpers to `lib.sh`

**Files:**
- Modify: `scripts/tier2/lib.sh`, `scripts/tier2/lib_test.sh`, `scripts/tier2/floods.sh`, `scripts/tier2/floods_test.sh`, `scripts/tier2/gwloss.sh`, `scripts/tier2/slowlogic.sh`

**Interfaces:**
- Produces (in `lib.sh`, for Tasks 2 and 4): `t2_kill_tree PID`; `t2_up NAME` (exports `STACK_DIR=$T2_OUT/NAME`, checks `NAME_stack`, returns `stack_up`'s status); `t2_gws_counters SPEC...` (`{"gw-k": stack_counters SPEC...}`); `t2_gws_delta A B`; `t2_lease PHASE DELTA SECONDS LOGIC_DELTA` (checks `PHASE_lease`; DELTA needs `lease_live`, `lease_missing`, `lease_error`, LOGIC_DELTA `lease_error`); `t2_set_ticks LIST [STAT]` (`"busy total"`); `t2_runq_ns PID`.
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
No behaviour changes." -m "Refs: P6b3bI-3, P6b3b3I-4, P6b3b4I-2 (Plan 6b3c Task 1)."
```

---

### Task 2: Floods: the `refused` outcome and the verdict from the series

**Files:**
- Modify: `internal/loadgen/flood.go`, `internal/loadgen/flood_test.go`, `scripts/tier2/floods.sh`, `scripts/tier2/floods_test.sh`

**Interfaces:**
- Produces: flood outcome `FloodRefused = "refused"` (slow modes: closed by the gateway before `slowHeld`, no hold); `slow_series FILE N` with `"sat_run_s": {"loadgen", "gateway", "logic": seconds}`; `slow_verdict` over `{"ramp", "hold", "sat"}`.
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
index 95c7a7e..7d6ccff 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -135,9 +135,14 @@
 #                         p50 >= 5s, p99 <= 6.5s, max <= 7.5s (5s + one tick of
 #                         the wheel, plus scheduling slack, 6b3a O7); a
 #                         slow-auth handshake the limiter refused ends
-#                         pre-handshake, with no hold, and is counted (O1)
+#                         pre-handshake, with no hold, and is counted (O1);
+#                         a connection the gateway closed before 3s (at
+#                         capacity, a limit, pending_mem) is refused, with
+#                         no hold, and is counted (#135)
 #   slow_freed            the gateways counted at least as many
-#                         handshake_timeout and auth_timeout closes
+#                         handshake_timeout and auth_timeout closes as the
+#                         floods' closed outcomes, the closes held >= 3s
+#                         (refused ones are not timeouts, #135, run 30n)
 #   slow_legit            every light client logs in beside the floods (or
 #                         gave up as in storm_online): no
 #                         Busy, no reject (login_queue, pending_mem: the
@@ -147,7 +152,10 @@
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
@@ -493,7 +501,7 @@ slow_sampler() {
   done
 }
 
-# slow_series FILE: slow_sampler's lines as {"columns", "rows" (one per
+# slow_series FILE N: slow_sampler's lines as {"columns", "rows" (one per
 # interval between samples, t_s since the first; a column null where a
 # reading is missing), "first_s" (when each sign of trouble first showed,
 # null if never: a CPU set at >= 85% busy, /stats unanswered, a gateway
@@ -507,7 +515,10 @@ slow_sampler() {
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
@@ -563,25 +574,39 @@ def first(f):
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
@@ -713,7 +738,7 @@ phase_garbage() {
 phase_slow() {
   local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} hs=${FLOODS_SLOW_HANDSHAKE:-80000}
   local au=${FLOODS_SLOW_AUTH:-40000} srate=${FLOODS_SLOW_RATE:-10000} hold=${FLOODS_SLOW_HOLD:-60}
-  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win verdict
+  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win series verdict
   STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
   up slow || { stack_down; return; }
   stack_snapshot before
@@ -761,13 +786,15 @@ phase_slow() {
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
   t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}')"
@@ -777,7 +804,7 @@ phase_slow() {
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
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/floods-*/result.json")[0])); print({k: v["pass"] for k, v in d["checks"].items()}); print(d["slow"]["verdict"], d["slow"]["series"]["sat_run_s"])'
```
Expected (prototype, ~2 min): `exit 1`; 12 checks, failing only `no_swap_in_slow` (the dev machine's swap, if it swaps); `valid` with `sat_run_s` 0.0 on every set. `slow_held`'s outcomes are `closed` only at this scale (no early close), and `slow_freed` passes. Then `rm -rf tier2-results/floods-*`.

- [ ] **Step 4: Commit**

```bash
git add internal/loadgen/flood.go internal/loadgen/flood_test.go scripts/tier2/floods.sh scripts/tier2/floods_test.sh
git commit -m "fix: count early slow closes as refused, gate saturated runs" -m "A slow flood connection that the gateway closes before 3s is now
counted as refused, with no hold. slow_freed then compares the
timeouts with the closes that were timeouts.
slow_verdict also reads slow.series. A CPU set at 95% busy or more for
5s in a row, before every light client is online, fails the verdict." -m "Refs: #135, #136, runs 30m and 30n (Plan 6b3c Task 2)."
```

---

### Task 3: Loadgen `POST /reset` and `POST /network-change`

**Files:**
- Modify: `internal/loadgen/runner.go`, `internal/loadgen/sdkclient.go`, `internal/loadgen/recorder.go`, `internal/loadgen/control.go`, `cmd/loadgen/main.go`
- Create: `internal/loadgen/reset_test.go`

**Interfaces:**
- Produces: `(*Runner).Reset(name string) (n int, took time.Duration, err error)`; `(*Runner).NetworkChange() (n int, took time.Duration, err error)`; `ErrNotSDK`; `POST /reset?mark=NAME` → `{"mark", "population", "close_ms"}` (400 without a mark, 409 outside sdk mode or with a taken mark, nothing changed); `POST /network-change` → `{"clients", "ms"}` (409 outside sdk mode); `Stats.BalancerOpen` (`balancer_open`: `{"gw-k": open}`); `StormCount.Waits` (`storm_since_mark.waits`: `{"storm": n, "local": n, ...}`, every SDK wait by kind since the latest mark); `(*Recorder).Wait(kind string)`.
- Consumes: `(*Runner).Mark`, `Balancer.Open`, `sdk.Client.NetworkChanged`, `sdk.WaitKind.String`.

- [ ] **Step 1: The patch**

The tests are `reset_test.go` (new, in the patch): `TestResetDropsEveryConnection`, `TestAbortSendsAnRST`, `TestNetworkChangeCutsTheStormWait`, `TestResetAndNetworkChangeNeedSDKMode`.

```diff
diff --git a/cmd/loadgen/main.go b/cmd/loadgen/main.go
index 6b6f8f5..3131984 100644
--- a/cmd/loadgen/main.go
+++ b/cmd/loadgen/main.go
@@ -2,7 +2,8 @@
 // scripts/stack.sh's stack.env, starts -clients at -rate through a
 // least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
 // and serves the control API on -control (GET /stats, POST /kill, POST /mark,
-// POST /up, GET /slo, GET /runtime; choice #84) until SIGTERM, Ctrl-C or -duration.
+// POST /up, GET /slo, GET /runtime; choice #84; sdk mode also POST /reset and
+// POST /network-change, Plan 6b3c) until SIGTERM, Ctrl-C or -duration.
 // -down keeps gateways out of the balancer until POST /up (#93). The flood
 // modes (garbage, slow-handshake, slow-auth; Plan 6b3a) log nothing in: they
 // put a handshake flood or a slow-client flood beside another instance's
diff --git a/internal/loadgen/control.go b/internal/loadgen/control.go
index a126b04..d2f69bd 100644
--- a/internal/loadgen/control.go
+++ b/internal/loadgen/control.go
@@ -34,6 +34,9 @@ func (r *Runner) Result(within time.Duration) Result {
 //	POST /mark?gateway=gw-1|all&mark=drop     a mark; &down=1 also takes the gateway out of the balancer
 //	POST /up?gateway=gw-1                     the gateway is back in the balancer  {"gateway"}
 //	                                          also after /kill: send it once the gateway listens again
+//	POST /reset?mark=reset                    sdk: mark every gateway, then RST every connection  {"mark", "population", "close_ms"}
+//	                                          409: not sdk mode or the mark taken, nothing changed
+//	POST /network-change                      sdk: NetworkChanged on every client  {"clients", "ms"}; 409: not sdk mode
 //	GET  /slo?mark=death&within=30s           SLO
 //	GET  /runtime                             Runtime: loadgen's GC, memory and scheduling counters
 //	/debug/pprof/                             loadgen's own profiles
@@ -83,6 +86,26 @@ func (r *Runner) Handler() http.Handler {
 		r.Up(gw)
 		writeJSON(w, map[string]any{"gateway": q.FormValue("gateway")})
 	})
+	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, q *http.Request) {
+		if q.FormValue("mark") == "" {
+			http.Error(w, "want mark=<name>", http.StatusBadRequest)
+			return
+		}
+		n, took, err := r.Reset(q.FormValue("mark"))
+		if err != nil {
+			http.Error(w, err.Error(), http.StatusConflict)
+			return
+		}
+		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n, "close_ms": took.Milliseconds()})
+	})
+	mux.HandleFunc("POST /network-change", func(w http.ResponseWriter, _ *http.Request) {
+		n, took, err := r.NetworkChange()
+		if err != nil {
+			http.Error(w, err.Error(), http.StatusConflict)
+			return
+		}
+		writeJSON(w, map[string]any{"clients": n, "ms": took.Milliseconds()})
+	})
 	mux.HandleFunc("GET /slo", func(w http.ResponseWriter, q *http.Request) {
 		within, err := time.ParseDuration(q.FormValue("within"))
 		if err != nil || within <= 0 {
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index eca1422..779d40b 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -195,6 +195,18 @@ func (r *Recorder) Handshake(how string) {
 	r.mu.Unlock()
 }
 
+// Wait: an SDK client's next attempt waits, of kind (an sdk.WaitKind
+// name), counted since the latest mark: after /network-change the storm
+// waits it cut show as local ones (Plan 6b3c).
+func (r *Recorder) Wait(kind string) {
+	r.mu.Lock()
+	if r.storm.Waits == nil {
+		r.storm.Waits = map[string]int64{}
+	}
+	r.storm.Waits[kind]++
+	r.mu.Unlock()
+}
+
 // Pong: a Ping was answered after rtt.
 func (r *Recorder) Pong(rtt time.Duration) {
 	r.mu.Lock()
@@ -355,16 +367,17 @@ type BackoffCount struct {
 // (SendLag, ms), how long each handshake took from the
 // server's reply to the Login frame (ReplyLag, ms), and where its first
 // message came from: made ahead of time, when it dialed, or the last attempt's
-// again (#106, #107, #109).
+// again (#106, #107, #109); and every SDK wait by kind (Plan 6b3c).
 type StormCount struct {
-	Mark                  string `json:"mark"`
-	AttemptLag            Dist   `json:"attempt_lag"`
-	ConnectLag            Dist   `json:"connect_lag"`
-	SendLag               Dist   `json:"send_lag"`
-	ReplyLag              Dist   `json:"reply_lag"`
-	HandshakesPrecomputed int64  `json:"handshakes_precomputed"`
-	HandshakesInline      int64  `json:"handshakes_inline"`
-	HandshakesReused      int64  `json:"handshakes_reused"`
+	Mark                  string           `json:"mark"`
+	AttemptLag            Dist             `json:"attempt_lag"`
+	ConnectLag            Dist             `json:"connect_lag"`
+	SendLag               Dist             `json:"send_lag"`
+	ReplyLag              Dist             `json:"reply_lag"`
+	HandshakesPrecomputed int64            `json:"handshakes_precomputed"`
+	HandshakesInline      int64            `json:"handshakes_inline"`
+	HandshakesReused      int64            `json:"handshakes_reused"`
+	Waits                 map[string]int64 `json:"waits"`
 }
 
 // Counts returns a snapshot of the counters.
@@ -398,5 +411,9 @@ func (r *Recorder) Counts() Counts {
 	c.StormSinceMark.ReplyLag = r.reply.dist(r.reply.n)
 	c.StormSinceMark.ConnectLag = r.connect.dist(r.connect.n)
 	c.StormSinceMark.SendLag = r.send.dist(r.send.n)
+	c.StormSinceMark.Waits = map[string]int64{}
+	for k, v := range r.storm.Waits {
+		c.StormSinceMark.Waits[k] = v
+	}
 	return c
 }
diff --git a/internal/loadgen/reset_test.go b/internal/loadgen/reset_test.go
new file mode 100644
index 0000000..39ba785
--- /dev/null
+++ b/internal/loadgen/reset_test.go
@@ -0,0 +1,139 @@
+package loadgen
+
+import (
+	"errors"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"syscall"
+	"testing"
+	"time"
+
+	"github.com/Tallinzp/goim/internal/sdk"
+)
+
+// sdkRun starts n SDK clients on env with the storm window storm, serves
+// their control API, and waits until all are online.
+func sdkRun(t *testing.T, env Env, n int, storm sdk.Window) (*Runner, *httptest.Server) {
+	t.Helper()
+	r, err := New(Config{Mode: "sdk", Clients: n, Rate: 300, UIDPrefix: "rst"}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	r.cfg.windows = sdk.DefaultWindows()
+	r.cfg.windows.Storm = storm
+	t.Cleanup(run(r))
+	srv := httptest.NewServer(r.Handler())
+	t.Cleanup(srv.Close)
+	waitFor(t, "all online", 10*time.Second, func() bool { s := r.Stats(); return s.RampDone && s.Online == n })
+	return r, srv
+}
+
+// POST /reset marks every gateway and closes every SDK connection at once:
+// every client loses it and logs in again after its storm wait, and each
+// gateway sees its clients close (#134). A taken mark closes nothing.
+func TestResetDropsEveryConnection(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 0, Hi: time.Second})
+	code, m := post(t, srv.URL+"/reset?mark=reset")
+	if code != http.StatusOK || m["population"] != 20.0 || m["close_ms"] == nil {
+		t.Fatalf("/reset: %d %v", code, m)
+	}
+	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
+	s, _ := r.Recorder().SLO("reset", 5*time.Second)
+	st := r.Stats()
+	if s.Gateway != "all" || s.Population != 20 || s.Lost != 20 || st.Lost["lost"] != 20 || st.StormSinceMark.Waits["storm"] != 20 {
+		t.Fatalf("slo %+v, lost %v, waits %v: want all 20 lost, each after one storm wait", s, st.Lost, st.StormSinceMark.Waits)
+	}
+	if open := st.BalancerOpen[env.GatewayIDs[0]] + st.BalancerOpen[env.GatewayIDs[1]]; open != 20 {
+		t.Fatalf("balancer open %v, want the 20 back", st.BalancerOpen)
+	}
+	waitFor(t, "the gateways count 20 client closes", 5*time.Second, func() bool {
+		return metric(t, env.GatewayIDs[0], "goim_gateway_closes_total", "reason", "client")+
+			metric(t, env.GatewayIDs[1], "goim_gateway_closes_total", "reason", "client") >= 20
+	})
+	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusConflict {
+		t.Fatalf("/reset with a taken mark: %d", code)
+	}
+	if code, _ := post(t, srv.URL+"/reset"); code != http.StatusBadRequest {
+		t.Fatalf("/reset without a mark: %d", code)
+	}
+	if st := r.Stats(); st.Online != 20 || st.Lost["lost"] != 20 {
+		t.Fatalf("a refused /reset closed connections: %+v", st.Counts)
+	}
+}
+
+// abort closes with an RST: the peer's read fails with ECONNRESET, not
+// EOF (SO_LINGER 0, #134).
+func TestAbortSendsAnRST(t *testing.T) {
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer ln.Close()
+	b, err := NewBalancer([]string{ln.Addr().String()}, 1)
+	if err != nil {
+		t.Fatal(err)
+	}
+	nc, _, err := b.Dial(t.Context())
+	if err != nil {
+		t.Fatal(err)
+	}
+	peer, err := ln.Accept()
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer peer.Close()
+	(&replyConn{Conn: nc}).abort()
+	peer.SetReadDeadline(time.Now().Add(5 * time.Second))
+	var buf [1]byte
+	if _, err := peer.Read(buf[:]); !errors.Is(err, syscall.ECONNRESET) {
+		t.Fatalf("peer read: %v, want ECONNRESET", err)
+	}
+	if b.Open()[0] != 0 {
+		t.Fatalf("balancer open %v, want the slot freed", b.Open())
+	}
+}
+
+// POST /network-change after a reset cuts every client's storm wait to the
+// local trigger's 0–1s (spec: Local trigger): with a 30–60s storm window
+// all are back within seconds, each after one storm and one local wait.
+func TestNetworkChangeCutsTheStormWait(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 30 * time.Second, Hi: 60 * time.Second})
+	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusOK {
+		t.Fatalf("/reset: %d", code)
+	}
+	waitFor(t, "all lost", 5*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Lost == 20 })
+	waitFor(t, "all in their storm wait", 5*time.Second, func() bool { return r.Stats().StormSinceMark.Waits["storm"] == 20 })
+	code, m := post(t, srv.URL+"/network-change")
+	if code != http.StatusOK || m["clients"] != 20.0 {
+		t.Fatalf("/network-change: %d %v", code, m)
+	}
+	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
+	if w := r.Stats().StormSinceMark.Waits; w["local"] != 20 {
+		t.Fatalf("waits since the mark %v, want 20 local", w)
+	}
+}
+
+// Outside sdk mode /reset and /network-change are refused, and no mark is
+// taken: light clients don't follow the SDK's reconnect rules (#134).
+func TestResetAndNetworkChangeNeedSDKMode(t *testing.T) {
+	env, _ := startStack(t, 1)
+	r, err := New(Config{Mode: "light", Clients: 2, Rate: 100, UIDPrefix: "nr", PingEvery: time.Second, Timeout: 5 * time.Second}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer run(r)()
+	srv := httptest.NewServer(r.Handler())
+	defer srv.Close()
+	waitFor(t, "2 online", 10*time.Second, func() bool { return r.Stats().Online == 2 })
+	for _, p := range []string{"/reset?mark=reset", "/network-change"} {
+		if code, _ := post(t, srv.URL+p); code != http.StatusConflict {
+			t.Fatalf("%s in light mode: %d", p, code)
+		}
+	}
+	if _, err := r.Recorder().SLO("reset", time.Second); err == nil || r.Stats().Online != 2 {
+		t.Fatal("a refused /reset took a mark or closed connections")
+	}
+}
diff --git a/internal/loadgen/runner.go b/internal/loadgen/runner.go
index 122ba8b..14747e8 100644
--- a/internal/loadgen/runner.go
+++ b/internal/loadgen/runner.go
@@ -52,6 +52,9 @@ type Stats struct {
 	ElapsedS   float64          `json:"elapsed_s"`
 	Precompute *PrecomputeStats `json:"precompute,omitempty"`
 	Flood      *FloodStats      `json:"flood,omitempty"` // flood modes only
+	// BalancerOpen is the balancer's open connections per gateway, the
+	// count least connections picks by (#133: the funnel).
+	BalancerOpen map[string]int `json:"balancer_open"`
 	Counts
 }
 
@@ -70,7 +73,8 @@ type Runner struct {
 	ramped   chan struct{} // closed when every client has started
 
 	mu    sync.Mutex
-	stops []func() // one per started client
+	stops []func()     // one per started client
+	sdks  []*sdkClient // sdk mode: every started client (Reset, NetworkChange)
 }
 
 // New checks cfg and prepares a run against env's gateways.
@@ -217,13 +221,16 @@ func (r *Runner) start(ctx context.Context, wg *sync.WaitGroup, i int) {
 		stop = lc.stop
 		wg.Go(func() { r.light(ctx, lc) })
 	default:
-		c, err := r.newSDK(i)
+		sc, err := r.newSDK(i)
 		if err != nil {
 			r.rec.Failed("sdk")
 			return
 		}
-		stop = c.Close
-		c.Connect()
+		stop = sc.c.Close
+		sc.c.Connect()
+		r.mu.Lock()
+		r.sdks = append(r.sdks, sc)
+		r.mu.Unlock()
 	}
 	r.mu.Lock()
 	r.stops = append(r.stops, stop)
@@ -283,6 +290,73 @@ func (r *Runner) Mark(gw int, name string, down bool) (int, error) {
 	return n, err
 }
 
+// ErrNotSDK refuses /reset and /network-change outside sdk mode: only SDK
+// clients follow the spec's reconnect rules (#134). Nothing changes.
+var ErrNotSDK = errors.New("loadgen: /reset and /network-change need sdk mode")
+
+// Reset is POST /reset (P6a2bI-2, #134): mark name on every gateway, its
+// population every client logged in now, then close every SDK client's
+// newest connection with SO_LINGER 0, so each gateway gets an RST and each
+// client loses its connection at once, as when an LB resets every flow
+// (the spec's whole-population reconnect, "RST from loadgen in one
+// instant"). took is how long the closes took after the mark. A taken mark
+// name closes nothing.
+func (r *Runner) Reset(name string) (n int, took time.Duration, err error) {
+	if r.cfg.Mode != "sdk" {
+		return 0, 0, ErrNotSDK
+	}
+	if n, err = r.Mark(-1, name, false); err != nil {
+		return 0, 0, err
+	}
+	begin := time.Now()
+	r.eachSDK(func(sc *sdkClient) {
+		if c := sc.conn.Load(); c != nil {
+			c.abort()
+		}
+	})
+	return n, time.Since(begin), nil
+}
+
+// NetworkChange is POST /network-change (P6a2bI-2): every SDK client gets
+// sdk.Client.NetworkChanged, as when a city's network recovers. A client
+// in its 0–20s storm wait then attempts within the 0–1s local window; one
+// online, or in a wait the server set, keeps it (spec: Precedence). n is
+// the clients told, took how long that took.
+func (r *Runner) NetworkChange() (n int, took time.Duration, err error) {
+	if r.cfg.Mode != "sdk" {
+		return 0, 0, ErrNotSDK
+	}
+	begin := time.Now()
+	var told atomic.Int64
+	r.eachSDK(func(sc *sdkClient) {
+		sc.c.NetworkChanged()
+		told.Add(1)
+	})
+	return int(told.Load()), time.Since(begin), nil
+}
+
+// eachSDK runs f on every started SDK client, 64 at a time: a call into a
+// client waits for its run loop.
+func (r *Runner) eachSDK(f func(*sdkClient)) {
+	r.mu.Lock()
+	scs := r.sdks
+	r.mu.Unlock()
+	work := make(chan *sdkClient)
+	var wg sync.WaitGroup
+	for range 64 {
+		wg.Go(func() {
+			for sc := range work {
+				f(sc)
+			}
+		})
+	}
+	for _, sc := range scs {
+		work <- sc
+	}
+	close(work)
+	wg.Wait()
+}
+
 // Up is POST /up: gateway gw is back in the balancer. It is not refused for
 // a gateway that /kill or /mark&down=1 took out (#99): a scenario that
 // restarts one needs it. The caller sends it only once gw listens again:
@@ -292,7 +366,10 @@ func (r *Runner) Up(gw int) { r.bal.SetUp(gw) }
 // Stats is the run's state now.
 func (r *Runner) Stats() Stats {
 	s := Stats{Mode: r.cfg.Mode, Clients: r.cfg.Clients, Sources: r.cfg.Sources, Started: r.started.Load(),
-		RampDone: r.rampDone.Load(), Counts: r.rec.Counts()}
+		RampDone: r.rampDone.Load(), BalancerOpen: map[string]int{}, Counts: r.rec.Counts()}
+	for i, n := range r.bal.Open() {
+		s.BalancerOpen[r.env.GatewayIDs[i]] = n
+	}
 	if b := r.begin.Load(); b != 0 {
 		s.ElapsedS = time.Since(time.Unix(0, b)).Seconds()
 	}
diff --git a/internal/loadgen/sdkclient.go b/internal/loadgen/sdkclient.go
index 267e2be..d593244 100644
--- a/internal/loadgen/sdkclient.go
+++ b/internal/loadgen/sdkclient.go
@@ -18,6 +18,8 @@ import (
 type sdkClient struct {
 	r      *Runner
 	i      int
+	c      *sdk.Client
+	conn   atomic.Pointer[replyConn] // the newest connection dialed, for Reset
 	gw     atomic.Int32              // gateway of the last dial
 	tok    atomic.Pointer[string]    // the last token, reused until a refresh (spec: Client SDK rules)
 	pre    atomic.Pointer[prepared]  // the next attempt's first message (#106)
@@ -29,9 +31,9 @@ type sdkClient struct {
 	online bool                      // run loop only
 }
 
-func (r *Runner) newSDK(i int) (*sdk.Client, error) {
+func (r *Runner) newSDK(i int) (*sdkClient, error) {
 	sc := &sdkClient{r: r, i: i}
-	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
+	c, err := sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
 		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
 		Token: func(_ context.Context, refresh bool) (string, error) {
 			if t := sc.tok.Load(); t != nil && !refresh {
@@ -57,10 +59,26 @@ func (r *Runner) newSDK(i int) (*sdk.Client, error) {
 				sc.gw.Store(int32(gw))
 			}
 			rc := sc.track(nc, seq)
+			if seq == sc.dials.Load() {
+				sc.conn.Store(rc)
+			}
 			r.rec.ConnectLag(rc.dialed.Sub(nc.(*conn).connecting))
 			return rc, nil
 		},
 		Initiate: sc.initiate})
+	sc.c = c
+	return sc, err
+}
+
+// abort closes c with an RST, not a FIN (SO_LINGER 0): an LB resetting
+// the flow (Reset, #134).
+func (c *replyConn) abort() {
+	if lc, ok := c.Conn.(*conn); ok {
+		if tc, ok := lc.Conn.(*net.TCPConn); ok {
+			tc.SetLinger(0)
+		}
+	}
+	c.Conn.Close()
 }
 
 // track wraps a connection the client just dialed; seq is its dial number,
@@ -106,8 +124,11 @@ func (sc *sdkClient) initiate(keyID uint16, pub []byte) (*wire.Initiator, []byte
 // event runs on the client's run loop: it must not block.
 func (sc *sdkClient) event(e sdk.Event) {
 	now := time.Now()
-	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
-		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
+	if e.Kind == sdk.EventWaiting {
+		sc.r.rec.Wait(e.Wait.Kind.String())
+		if e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure {
+			sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
+		}
 	}
 	switch e.Kind {
 	case sdk.EventConnecting:
```

- [ ] **Step 2: Run the tests**

Run: `go test -race -count=1 -run 'TestReset|TestAbort|TestNetworkChange' ./internal/loadgen/`
Expected: `ok`.

- [ ] **Step 3: Mutations**

Each change below, alone, must fail the named test (then undo it):

| change | test that fails |
|---|---|
| `tc.SetLinger(0)` → `_ = tc` (`sdkclient.go`) | `TestAbortSendsAnRST` (EOF, not ECONNRESET) |
| `c.abort()` → `_ = c` (`runner.go`, `Reset`) | `TestResetDropsEveryConnection` |
| `sc.c.NetworkChanged()` → `_ = sc` (`runner.go`) | `TestNetworkChangeCutsTheStormWait` |
| delete `sc.r.rec.Wait(e.Wait.Kind.String())` (`sdkclient.go`) | `TestResetDropsEveryConnection`, `TestNetworkChangeCutsTheStormWait` |
| delete `Reset`'s `if r.cfg.Mode != "sdk" { … }` | `TestResetAndNetworkChangeNeedSDKMode` |

- [ ] **Step 4: The whole suite**

Run: `gofmt -l . ; go vet ./... && make test`
Expected: no file listed, `ok` everywhere.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/runner.go internal/loadgen/sdkclient.go internal/loadgen/recorder.go internal/loadgen/control.go internal/loadgen/reset_test.go cmd/loadgen/main.go
git commit -m "feat: add loadgen POST /reset and POST /network-change" -m "/reset marks every gateway, then closes every SDK connection with
SO_LINGER 0. Each gateway gets an RST, and each client waits its
storm window. /network-change calls NetworkChanged on every client.
Both work in sdk mode only.
Loadgen also counts every SDK wait by kind since the latest mark, and
reports its balancer's open connections per gateway." -m "Refs: P6a2bI-2, #133, #134 (Plan 6b3c Task 3)."
```

---

### Task 4: Scenario `reconnect`

**Files:**
- Create: `scripts/tier2/reconnect.sh`, `scripts/tier2/reconnect_test.sh`
- Modify: `Makefile` (`test-scripts`, the scenario list)

**Interfaces:**
- Consumes: Task 1's `t2_up`, `t2_gws_counters`, `t2_gws_delta`, `t2_lease`, `t2_set_ticks`, `t2_kill_tree`; Task 3's `POST /reset?mark=reset`, `POST /network-change`, `balancer_open`, `storm_since_mark.waits`; loadgen's `/slo?mark=reset` (`population`, `lost`, `back`, `detection`, `from_mark`), `backoff_since_mark`.
- Produces: `result.json` checks `P_stack`, `P_online`, `P_pinning`, `no_swap_in_P`, `P_drop`, `P_back`, `P_heartbeats`, `P_lease`, `P_idle`, `P_processes` for `P` in `reset`, `netchange`, and `artifacts` (21 checks with both phases); field `P` with `slo`, `loadgen` (backoff, storm lags, waits), `gateways` (counter deltas), `series` (`rc_series`: rows from the mark, per gateway `gw_columns`, `funnel`), `storm_first30`, `loadgen_bound`, `reset`, `network_change`, `network_change_after_ms`.

- [ ] **Step 1: The patch**

```diff
diff --git a/Makefile b/Makefile
index e6124d3..9afe218 100644
--- a/Makefile
+++ b/Makefile
@@ -27,6 +27,7 @@ test-race-suite:
 test-scripts:
 	scripts/tier2/lib_test.sh
 	scripts/tier2/floods_test.sh
+	scripts/tier2/reconnect_test.sh
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 
@@ -72,7 +73,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, reconnect, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
diff --git a/scripts/tier2/reconnect.sh b/scripts/tier2/reconnect.sh
new file mode 100755
index 0000000..da423c1
--- /dev/null
+++ b/scripts/tier2/reconnect.sh
@@ -0,0 +1,439 @@
+#!/usr/bin/env bash
+# Tier 2: reconnect (Plan 6b3c), the spec's load test "whole-population
+# reconnect" (R5S-O6): drop every connection on all gateways at once and
+# measure, with no SLO, the time until 99% are logged in again and the
+# share of clients that reach backoff; the heartbeats of clients already
+# back are answered on time and no component restarts. Each phase on a
+# fresh stack:
+#   reset: 600k SDK clients over 3 gateways (as gwloss b_stress runs them:
+#       -mode sdk -precompute, #134), then POST /reset: loadgen marks every
+#       gateway and closes every connection with an RST in one instant
+#       (P6a2bI-2). The clients log in again after the SDK's 0-20s storm
+#       wait, its pre-handshake retries and backoff.
+#   netchange: the same, then POST /network-change once every client has
+#       lost its connection: each cuts its storm wait to the local
+#       trigger's 0-1s (spec: "the same with every client raising a
+#       network-change event at once").
+# The gateways run the spec's cap (250k, #136) and a 30s heartbeat (#136),
+# so the clients back send Pings during the storm. A per-second series
+# per gateway records pending sockets, pending_mem refusals, open
+# connections, handshakes in progress, Busy, the login queue and the
+# Logouts awaiting, beside loadgen's balancer counts, and a funnel reading
+# (one gateway refusing at pending_mem while another, under its cap, has
+# no handshake in progress) is recorded, not gated (#133). CPU sets as
+# b_stress's (#108), 4 GOMAXPROCS per gateway: 2 handshake slots each, as
+# #133 sized it. Logic is one process (minReplicas, no HPA).
+#
+# Usage (on the Tier 2 host, from the repo root), after the one-time
+#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
+#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=reconnect
+#   Results: tier2-results/reconnect-<timestamp>/ (result.json, one
+#   directory per phase with logs, artifacts and loadgen results), plus ONE
+#   file to send back: tier2-results/reconnect-<timestamp>.report.txt
+#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
+#   About 20 minutes (each phase ~10: the ramp 120s, the storm up to
+#   RECONNECT_BACK_WAIT, the hold, the stop).
+#
+# Settings (every number whole and positive, refused otherwise before
+# anything starts):
+#   RECONNECT_PHASES      phases to run, in order (default "reset netchange")
+#   RECONNECT_CLIENTS     SDK clients over the 3 gateways (default 600000)
+#   RECONNECT_RATE        clients started per second (default 5000)
+#   RECONNECT_HEARTBEAT   the gateways' -heartbeat, seconds (default 30, the
+#                         SDK's lowest: most Pings in the storm; 30-300)
+#   RECONNECT_CAP         each gateway's -max-conns (default: the gateway's
+#                         250,000, the spec's cap)
+#   RECONNECT_DROP_MAX_S  every client must notice the drop within this
+#                         many seconds of the mark (default 10)
+#   RECONNECT_BACK_WAIT   seconds after the mark to wait for every client to
+#                         log in again (default 900: as b_stress, the
+#                         SDK's backoff doubles to 5 min)
+#   RECONNECT_HOLD        seconds held once every client is back (default
+#                         the heartbeat + 15: each sends a Ping and gets its
+#                         Pong, or its 10s Pong timeout)
+#   RECONNECT_LG_MEMLIMIT loadgen's GOMEMLIMIT (default 16GiB, as b_stress)
+#   RECONNECT_CPUS_LOADGEN, RECONNECT_CPUS_GATEWAY, RECONNECT_CPUS_LOGIC
+#                         the CPU sets (default b_stress's: 0-5,10,12-17,22;
+#                         6-9,18-21; 11,23)
+#   RECONNECT_GATEWAY_PROCS  each gateway's GOMAXPROCS (default 4)
+#
+# Checks (per phase P):
+#   P_stack        redis, devtoken, Logic and the 3 gateways become Ready
+#   P_online       every client logs in, each gateway holds >= 30% of them
+#   P_pinning      the gateways, Logic + Redis and loadgen on their CPU sets;
+#                  a failure ends the run
+#   P_drop         POST /reset marked every client (population = clients),
+#                  every one noticed the loss within RECONNECT_DROP_MAX_S,
+#                  and the gateways counted a client close for each; for
+#                  netchange also POST /network-change reached every
+#                  client. The reset's close time and the mark-to-network-
+#                  change time are recorded
+#   P_back         every client logged in again within RECONNECT_BACK_WAIT;
+#                  no SLO: the time until 99% are back (slo.from_mark p99),
+#                  the share that reached backoff, the waits since the mark
+#                  by kind, the rejects by reason, and whether loadgen was
+#                  the bound (>= 3/4 of its CPUs over the storm's first
+#                  30s, #102) are recorded
+#   P_heartbeats   from the mark to the hold's end every connection that
+#                  ended was the reset's (lost "lost" = population, no
+#                  Pong timeout, no other reason) and no gateway closed one
+#                  for its heartbeat: the clients back were answered on
+#                  time while the others logged in (R5S-O6)
+#   P_lease        every gateway renewed its lease every interval from the
+#                  mark to the hold's end, and Logic saw no renewal error
+#   no_swap_in_P   no page swapped in from the mark to the hold's end
+#   P_idle         after loadgen stops, every gateway reaches 0 connections
+#                  and settles its Logouts (dropped ones allowed, counted:
+#                  the logout queue is the cap's size, and every gateway
+#                  queues ~200k at once twice)
+#   P_processes    nothing died, panicked or stopped uncleanly ("no
+#                  component restarts")
+#   artifacts      (once) every phase's metrics before and after, the
+#                  storm's CPU profiles (gateways, Logic, loadgen), the
+#                  series, the SLO and loadgen's result
+set -uo pipefail
+
+. "$(dirname "$0")/lib.sh"
+. "$(dirname "$0")/../stack.sh"
+. "$(dirname "$0")/../loadgen.sh"
+t2_init reconnect
+t2_need curl taskset sudo python3
+t2_require_host_setup
+
+PHASES=${RECONNECT_PHASES:-reset netchange}
+named=" "
+for p in $PHASES; do
+  case $p in reset | netchange) ;; *) echo "reconnect: unknown phase '$p' (reset, netchange)" >&2; exit 1 ;; esac
+  case $named in *" $p "*) echo "reconnect: phase '$p' named twice in RECONNECT_PHASES" >&2; exit 1 ;; esac
+  named+="$p "
+done
+for v in RECONNECT_CLIENTS RECONNECT_RATE RECONNECT_HEARTBEAT RECONNECT_CAP RECONNECT_DROP_MAX_S \
+  RECONNECT_BACK_WAIT RECONNECT_HOLD RECONNECT_GATEWAY_PROCS; do
+  case ${!v-1} in ''|*[!0-9]*|0*) echo "reconnect: $v must be a positive whole number" >&2; exit 1 ;; esac
+done
+HB=${RECONNECT_HEARTBEAT:-30}
+[ "$HB" -ge 30 ] && [ "$HB" -le 300 ] || { echo "reconnect: RECONNECT_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
+N=${RECONNECT_CLIENTS:-600000} RATE=${RECONNECT_RATE:-5000}
+DROP_MAX=${RECONNECT_DROP_MAX_S:-10} BACK_WAIT=${RECONNECT_BACK_WAIT:-900} HOLD=${RECONNECT_HOLD:-$((HB + 15))}
+# As gwloss b_stress (#93, #105): 600k SDK clients' loadgen peaks at ~16GB
+# under its 16GiB GOMEMLIMIT, beside 3 gateways, Redis and the kernel's
+# socket memory.
+t2_mem_budget 23
+t2_nofile 1048576
+
+export STACK_PIN=1 STACK_GATEWAYS=3
+export T2_CPUS_GATEWAY=${RECONNECT_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${RECONNECT_CPUS_LOADGEN:-0-5,10,12-17,22}
+export T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}
+export STACK_GATEWAY_PROCS=${RECONNECT_GATEWAY_PROCS:-4}
+LG_NCPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
+LG_ENV=(GOMEMLIMIT="${RECONNECT_LG_MEMLIMIT:-16GiB}")
+STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
+STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+STACK_GATEWAY_ARGS=(-heartbeat "${HB}s" ${RECONNECT_CAP:+-max-conns "$RECONNECT_CAP"})
+CAP=${RECONNECT_CAP:-250000}
+t2_on_exit 'stack_down'
+t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack
+smp="" prof=""
+t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
+t2_on_exit '[ -n "$prof" ] && t2_kill_tree "$prof"'
+
+GW_COUNTERS=(
+  closes_client='goim_gateway_closes_total:reason="client"'
+  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
+  hs_ok='goim_gateway_handshakes_total:result="ok"'
+  hs_no_slot='goim_gateway_handshakes_total:result="no_slot"'
+  hs_no_token='goim_gateway_handshakes_total:result="no_token"'
+  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
+  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
+  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
+  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
+  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  logins_busy='goim_gateway_logins_total:result="busy"'
+  logouts_done='goim_gateway_logouts_total:result="done"'
+  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
+  logouts_retried='goim_gateway_logouts_total:result="retried"'
+  lease_live='goim_gateway_lease_calls_total:result="live"'
+  lease_missing='goim_gateway_lease_calls_total:result="missing"'
+  lease_error='goim_gateway_lease_calls_total:result="error"'
+)
+LOGIC_COUNTERS=(
+  lease_error='goim_logic_lease_renewals_total:result="error"'
+  logins_ok='goim_logic_logins_total:result="ok"'
+  logins_replaced='goim_logic_logins_total:result="replaced"'
+  logouts_deleted='goim_logic_logouts_total:result="deleted"'
+  logouts_unknown='goim_logic_logouts_total:result="unknown"'
+  logouts_not_owner='goim_logic_logouts_total:result="not_owner"'
+)
+
+t2_report_extra() {
+  local f
+  for f in "$T2_OUT"/*/logs/*.log; do
+    [ -f "$f" ] || continue
+    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
+    tail -n 20 "$f"
+  done
+}
+
+# rc_sampler FILE: appends one JSON line a second to FILE until killed
+# (#133): {"ms", "sets": {"loadgen", "gateway", "logic": [busy, total]},
+# "lg": {"online", "open": the balancer's per gateway} (null when /stats
+# does not answer within 1s), "gw": {gateway: SERIES_COUNTERS, null if its
+# /metrics does not answer within 1s}, "ms_end": the clock after the
+# gateways' scrapes}.
+SERIES_COUNTERS=(
+  handshake='goim_gateway_connections:state="handshake"'
+  auth_pending='goim_gateway_connections:state="auth_pending"'
+  online='goim_gateway_connections:state="online"'
+  pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
+  at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
+  no_slot='goim_gateway_handshakes_total:result="no_slot"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  busy='goim_gateway_logins_total:result="busy"'
+  login_queue=goim_gateway_login_queue
+  awaiting_logout=goim_gateway_awaiting_logout
+  logouts_done='goim_gateway_logouts_total:result="done"'
+  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
+)
+rc_sampler() {
+  local ms ms_end sets lg gw g t
+  while :; do
+    ms=$(date +%s%3N)
+    sets="\"loadgen\": [$(t2_set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(t2_set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(t2_set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
+    gw=""
+    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
+      t=$(STACK_CURL_TIMEOUT=1 stack_counters "$g" "${SERIES_COUNTERS[@]}")
+      gw+="${gw:+, }\"$g\": ${t:-null}"
+    done
+    ms_end=$(date +%s%3N)
+    lg=$(LG_CURL_TIMEOUT=1 lg_get /stats '{"online": d["online"], "open": d["balancer_open"]}')
+    echo "{\"ms\": $ms, \"sets\": {$sets}, \"lg\": ${lg:-null}, \"gw\": {$gw}, \"ms_end\": $ms_end}" >>"$1"
+    sleep 1
+  done
+}
+
+# rc_series FILE MARK_MS CAP: rc_sampler's lines as {"columns", "gw_columns",
+# "rows": [[t_s since the mark, scrape_ms, lg_online, the three sets' busy %,
+# {gateway: [gw_columns]}], ...], "funnel", "samples"}; null if unreadable.
+# A sample whose gateway scrapes trailed its clock by over 1.5s, or missed
+# a gateway, has no gateway readings, and the next on-time sample's rates
+# are taken from the last on-time one (as floods' slow_series). The funnel
+# (#131, #133): the rows where a gateway refused at pending_mem while
+# another, under CAP open connections, had no socket in HANDSHAKE;
+# "seconds" (their intervals summed), "first_s", "longest_s" (in a row)
+# and the first 20 as [t_s, refusing, idle]. Recorded, not gated.
+rc_series() {
+  python3 -c 'import json, sys
+rows = []
+for l in open(sys.argv[1]):
+    try:
+        rows.append(json.loads(l))
+    except ValueError:
+        pass
+mark, cap = int(sys.argv[2]), int(sys.argv[3])
+cols = ["t_s", "scrape_ms", "lg_online", "loadgen_set_busy_pct", "gateway_set_busy_pct", "logic_set_busy_pct", "gw"]
+gcols = ["pending", "handshake", "open", "lb_open", "pending_mem_per_s", "at_capacity_per_s", "no_slot_per_s",
+         "logins_ok_per_s", "busy_per_s", "login_queue", "awaiting_logout", "logouts_done_per_s", "logouts_dropped_per_s"]
+rates = ("pending_mem", "at_capacity", "no_slot", "logins_ok", "busy", "logouts_done", "logouts_dropped")
+def busy(a, b, k):
+    x, y = a["sets"].get(k, []), b["sets"].get(k, [])
+    return round(100 * (y[0] - x[0]) / (y[1] - x[1]), 1) if len(x) == 2 and len(y) == 2 and y[1] > x[1] else None
+def on_time(b):
+    return b.get("ms_end") is not None and b["ms_end"] - b["ms"] <= 1500 and len(b["gw"]) > 0 and all(b["gw"].values())
+def rate(p, b, g, k):
+    if p is None or g not in p["gw"] or b["gw"][g].get(k) is None:
+        return None
+    return round((b["gw"][g][k] - (p["gw"][g].get(k) or 0)) / ((b["ms"] - p["ms"]) / 1000), 1)
+def gauge(v, k):
+    return v.get(k)
+out, ids = [], sorted({g for r in rows for g in r["gw"]})
+last = rows[0] if rows and on_time(rows[0]) else None
+for a, b in zip(rows, rows[1:]):
+    if b["ms"] <= a["ms"]:
+        continue
+    lg = b["lg"]
+    gw = None
+    if on_time(b):
+        gw = {}
+        for g, v in sorted(b["gw"].items()):
+            h, ap, on = gauge(v, "handshake"), gauge(v, "auth_pending"), gauge(v, "online")
+            pend = None if h is None or ap is None else h + ap
+            gw[g] = [pend, h, None if pend is None or on is None else pend + on, (lg or {}).get("open", {}).get(g) if lg else None] + \
+                [rate(last, b, g, k) for k in rates[:5]] + [gauge(v, "login_queue"), gauge(v, "awaiting_logout")] + [rate(last, b, g, k) for k in rates[5:]]
+        last = b
+    out.append([round((b["ms"] - mark) / 1000, 1), b["ms_end"] - b["ms"] if b.get("ms_end") is not None else None,
+                lg and lg["online"], busy(a, b, "loadgen"), busy(a, b, "gateway"), busy(a, b, "logic"), gw])
+gi = {k: i for i, k in enumerate(gcols)}
+def funnel(r):
+    gw = r[6]
+    if not gw:
+        return None
+    refusing = [g for g, v in gw.items() if (v[gi["pending_mem_per_s"]] or 0) > 0]
+    idle = [g for g, v in gw.items() if v[gi["handshake"]] == 0 and v[gi["open"]] is not None and v[gi["open"]] < cap]
+    pair = next(([a, b] for a in refusing for b in idle if a != b), None)
+    return pair
+hits, longest, run, prev = [], 0.0, 0.0, None
+total = 0.0
+for r in out:
+    dt = r[0] - prev if prev is not None else 0.0
+    prev = r[0]
+    p = funnel(r)
+    if p:
+        hits.append([r[0]] + p)
+        total += dt
+        run += dt
+        longest = max(longest, run)
+    else:
+        run = 0.0
+print(json.dumps({"columns": cols, "gw_columns": gcols, "gateways": ids, "rows": out, "samples": len(rows),
+    "funnel": {"seconds": round(total, 1), "first_s": hits[0][0] if hits else None, "longest_s": round(longest, 1), "rows": hits[:20]}}))' "$1" "$2" "$3" 2>/dev/null || echo null
+}
+
+t=$SECONDS
+if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
+  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
+  exit 1
+fi
+STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
+t2_duration setup $((SECONDS - t))
+missing=""
+
+for name in $PHASES; do
+  t0=$SECONDS
+  t2_up "$name" || { stack_down; t2_duration "$name" $((SECONDS - t0)); continue; }
+
+  # The ramp: as b_stress's, over every gateway.
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
+  lg_start "$name" -mode sdk -precompute -clients "$N" -rate "$RATE" -uid-prefix "$name"
+  stack_usage_start loadgen="$LG_PID"
+  t=$SECONDS
+  online=false
+  lg_wait_online "$N" $((N / RATE * 3 + 60)) &&
+    online=$(lg_get /stats 'd["online"] == '"$N"' and min(d["online_by_gateway"].values()) >= '"$N"' * 0.3')
+  [ "$online" = true ] || online=false # null: /stats unreadable
+  ramp_s=$((SECONDS - t))
+  ramp=$(stack_usage_json)
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
+  t2_check "${name}_online" "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
+  wrong=""
+  for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
+  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+  have=$(taskset -pc "$LG_PID" 2>/dev/null)
+  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
+  t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
+  if [ -n "$wrong" ]; then
+    lg_stop
+    stack_down
+    t2_duration "$name" $((SECONDS - t0))
+    t2_finish
+  fi
+  if [ "$online" != true ]; then
+    lg_stop
+    stack_down
+    t2_duration "$name" $((SECONDS - t0))
+    continue
+  fi
+  # #106: every client holds its next handshake frame at the mark, as in
+  # b_stress, so the first attempts are the spec's, not loadgen's X25519.
+  t=$SECONDS
+  until [ "$(lg_get /stats '(d.get("precompute") or {}).get("pending") == 0 and (d.get("precompute") or {}).get("ready") == d["online"]')" = true ]; do
+    [ $((SECONDS - t)) -lt 120 ] || break
+    sleep 1
+  done
+  precompute=$(lg_get /stats '{"pending": (d.get("precompute") or {}).get("pending"), "ready": (d.get("precompute") or {}).get("ready"), "online": d["online"], "wait_s": '"$((SECONDS - t))"'}')
+
+  # The storm: counters before, the reset, then the series and profiles.
+  stack_snapshot before
+  g0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  lost0=$(lg_get /stats 'd["lost"]')
+  t2_record_start "$name"
+  t2_cpustat_snap "$STACK_DIR/cpustat-2"
+  t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
+  stack_usage_start loadgen="$LG_PID"
+  mark_ms=$(date +%s%3N)
+  rc_sampler "$STACK_DIR/artifacts/series.jsonl" &
+  smp=$!
+  taskset -apc "$T2_CPUS_LOGIC" "$smp" >/dev/null
+  (stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
+  prof=$!
+  reply=$(lg_post "/reset?mark=reset" | lg_py - d)
+  mark_t=$SECONDS
+  # Every client notices the RST at once (its read fails); until then a
+  # network change would find it online and do nothing.
+  dropped=false
+  until [ "$(lg_get '/slo?mark=reset&within=30s' 'd["lost"] == d["population"]')" = true ]; do
+    [ $((SECONDS - mark_t)) -lt "$DROP_MAX" ] || break
+    sleep 0.2
+  done
+  [ "$(lg_get '/slo?mark=reset&within=30s' 'd["lost"] == d["population"]')" = true ] && dropped=true
+  nc=null nc_after_ms=null
+  if [ "$name" = netchange ]; then
+    nc_after_ms=$(($(date +%s%3N) - mark_ms))
+    nc=$(lg_post /network-change | lg_py - d)
+  fi
+  back=$(t2_bool lg_wait_back reset "$BACK_WAIT")
+  back_s=$((SECONDS - mark_t))
+  sleep "$HOLD"
+  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
+  wait "$prof"; prof=""
+  storm=$(stack_usage_json)
+  storm30=$(cat "$STACK_DIR/storm30.json" 2>/dev/null) || storm30=null
+  g1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  lost1=$(lg_get /stats 'd["lost"]')
+  window_s=$((SECONDS - mark_t))
+  t2_cpustat_snap "$STACK_DIR/cpustat-3"
+  t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
+  t2_record_end "$name"
+  stack_snapshot after
+  lg_get '/slo?mark=reset&within=30s' d >"$STACK_DIR/artifacts/slo-reset.json"
+  slo=$STACK_DIR/artifacts/slo-reset.json
+  d=$(t2_gws_delta "$g0" "$g1")
+  ld=$(stack_counters_delta "$l0" "$l1")
+  st=$(lg_get /stats '{"backoff": d["backoff_since_mark"], "storm": d["storm_since_mark"], "login_latency": d["login_latency"], "failed": d["failed"]}')
+  series=$(rc_series "$STACK_DIR/artifacts/series.jsonl" "$mark_ms" "$CAP")
+  loadgen_bound=$(echo "$storm30" | lg_true - 'd["loadgen"]["cores"] >= '"$LG_NCPUS"' * 0.75')
+
+  pop=$(lg_py "$slo" 'd["population"]')
+  rule='d["r"]["population"] == '"$N"' and d["s"]["lost"] == d["s"]["population"] and 0 <= d["s"]["detection"]["max_ms"] <= '"$DROP_MAX"' * 1000 and sum(v["closes_client"] for v in d["g"].values()) >= d["s"]["population"]'
+  [ "$name" = netchange ] && rule+=' and d["nc"] is not None and d["nc"]["clients"] == '"$N"
+  t2_check "${name}_drop" "$([ "$dropped" = true ] && echo "{\"r\": ${reply:-null}, \"s\": $(cat "$slo"), \"g\": $d, \"nc\": $nc}" | lg_true - "$rule" || echo false)" \
+    "\"reset\": ${reply:-null}, \"lost\": $(lg_py "$slo" 'd["lost"]'), \"detection\": $(lg_py "$slo" 'd["detection"]'), \"max_s\": $DROP_MAX, \"closes_client\": $(echo "$d" | lg_py - '{g: v["closes_client"] for g, v in d.items()}'), \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms"
+  t2_check "${name}_back" "$([ "$back" = true ] && lg_true "$slo" 'd["back"] == d["population"] == '"$N" || echo false)" \
+    "\"back_s\": $back_s, \"wait_s\": $BACK_WAIT, \"from_mark\": $(lg_py "$slo" 'd["from_mark"]'), \"backoff\": $(echo "$st" | lg_py - 'd["backoff"]'), \"backoff_share\": $(echo "$st" | lg_py - 'round(d["backoff"]["clients"] / '"${pop:-0}"', 4) if '"${pop:-0}"' else None'), \"waits\": $(echo "$st" | lg_py - 'd["storm"]["waits"]'), \"loadgen_bound\": $loadgen_bound, \"rejects\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_no_slot", "hs_no_token", "rejects_at_capacity", "rejects_pending_mem", "rejects_handshake_timeout", "rejects_auth_timeout", "rejects_login_queue", "logins_busy")} for g, v in d.items()}'), \"funnel\": $(echo "$series" | lg_py - 'd["funnel"]')"
+  lost=$(echo "{\"a\": ${lost0:-null}, \"b\": ${lost1:-null}}" | lg_py - '{k: v - d["a"].get(k, 0) for k, v in d["b"].items() if v - d["a"].get(k, 0)}')
+  t2_check "${name}_heartbeats" "$(echo "{\"l\": ${lost:-null}, \"g\": $d, \"p\": ${pop:-null}}" | lg_true - 'd["l"] == {"lost": d["p"]} and all(v["closes_heartbeat"] == 0 for v in d["g"].values())')" \
+    "\"lost_since_mark\": ${lost:-null}, \"population\": ${pop:-null}, \"closes_heartbeat\": $(echo "$d" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"heartbeat_s\": $HB, \"hold_s\": $HOLD"
+  t2_lease "$name" "$d" "$window_s" "$ld"
+
+  # The end: the result is written before any client stops (#84), then the
+  # gateways drain to idle (dropped Logouts allowed and counted).
+  lg_stop
+  t=$SECONDS
+  idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
+  stop_s=$((SECONDS - t))
+  logouts=$(t2_gws_counters done='goim_gateway_logouts_total:result="done"' dropped='goim_gateway_logouts_total:result="dropped"' logins_ok='goim_gateway_logins_total:result="ok"')
+  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"logouts\": $logouts, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
+  stack_down
+  problems=$(stack_health)
+  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
+
+  cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
+  t2_field "$name" "{\"clients\": $N, \"rate\": $RATE, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"precompute_at_mark\": $precompute, \"reset\": ${reply:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms, \"back_s\": $back_s, \"hold_s\": $HOLD, \"window_s\": $window_s, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"slo\": $(cat "$slo"), \"loadgen\": $st, \"gateways\": $d, \"logic\": $ld, \"cpu_sets\": $cpu_sets, \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3"), \"series\": $series, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS}"
+  t2_duration "$name" $((SECONDS - t0))
+
+  a=$STACK_DIR/artifacts
+  for f in before/logic after/logic before/gw-0 after/gw-0 before/gw-1 after/gw-1 before/gw-2 after/gw-2; do
+    grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
+  done
+  for f in logic gw-0 gw-1 gw-2 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/storm/$f.cpu"; done
+  [ -s "$a/series.jsonl" ] || missing+=" $name/series.jsonl"
+  [ "$(lg_true "$slo" '"population" in d')" = true ] || missing+=" $name/slo-reset.json"
+  [ "$(lg_true "$a/loadgen-$name.json" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
+done
+t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
+
+t2_not_covered "the drop as an LB restart or failover: Tier 2's LB is loadgen's in-process balancer (#134); Tier 3 (Plan 6d)" \
+  "a carrier or regional network recovering for part of the population: every client is reset (the spec's worst case)" \
+  "heartbeat Pong times: SDK clients record a Pong only by its absence (the 10s Pong timeout), not its delay" \
+  "per-shard Redis load with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)"
+t2_finish
diff --git a/scripts/tier2/reconnect_test.sh b/scripts/tier2/reconnect_test.sh
new file mode 100755
index 0000000..2e94d40
--- /dev/null
+++ b/scripts/tier2/reconnect_test.sh
@@ -0,0 +1,42 @@
+#!/usr/bin/env bash
+# Tests for scripts/tier2/reconnect.sh's rc_series over synthetic
+# rc_sampler lines (make test-scripts): the per-gateway rates, a late
+# scrape, and the funnel reading (#133). The scenario itself runs on the
+# Tier 2 host.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/loadgen.sh
+tmp=$(mktemp -d)
+trap 'rm -rf "$tmp"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+eval "$(sed -n '/^rc_series() {/,/^}/p' scripts/tier2/reconnect.sh)"
+
+# gw HANDSHAKE ONLINE PENDING_MEM: one gateway's reading; line MS SKEW GW0
+# GW1 one sample (loadgen 10 online, the balancer's 5 and 6 open).
+gw() { echo "{\"handshake\": $1, \"auth_pending\": 0, \"online\": $2, \"pending_mem\": $3, \"at_capacity\": 0, \"no_slot\": 0, \"logins_ok\": $2, \"busy\": 0, \"login_queue\": 0, \"awaiting_logout\": 0, \"logouts_done\": 0, \"logouts_dropped\": 0}"; }
+line() { echo "{\"ms\": $1, \"sets\": {}, \"lg\": {\"online\": 10, \"open\": {\"gw-0\": 5, \"gw-1\": 6}}, \"gw\": {\"gw-0\": $3, \"gw-1\": $4}, \"ms_end\": $(($1 + $2))}"; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+
+# gw-0 refuses at pending_mem from 2s while gw-1, under the cap (100), has
+# no handshake in progress: the funnel, for 2s in a row; at 4s gw-1 starts
+# a handshake and it ends. A late scrape (2.5s) at 5s has no gateway
+# readings, and the next sample's rates are taken from 5s's; then gw-1
+# is at its cap: full, not idle.
+{ line 1000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"; line 2000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"
+  line 3000 10 "$(gw 5 10 100)" "$(gw 0 10 0)"; line 4000 10 "$(gw 5 10 200)" "$(gw 0 10 0)"
+  line 5000 10 "$(gw 5 10 300)" "$(gw 1 10 0)"; line 6000 2500 "$(gw 5 10 900)" "$(gw 0 10 0)"
+  line 9000 10 "$(gw 5 10 900)" "$(gw 0 100 0)"; } >"$tmp/f.jsonl"
+s=$(rc_series "$tmp/f.jsonl" 1000 100)
+expect "series: rows from the mark" "$(get "$s" '[r[0] for r in d["rows"]]')" "[1.0, 2.0, 3.0, 4.0, 5.0, 8.0]"
+expect "series: gw-0 pending_mem per s" "$(get "$s" '[r[6] and r[6]["gw-0"][d["gw_columns"].index("pending_mem_per_s")] for r in d["rows"]]')" "[0.0, 100.0, 100.0, 100.0, None, 150.0]"
+expect "series: open and lb_open" "$(get "$s" '[d["rows"][0][6]["gw-1"][d["gw_columns"].index(k)] for k in ("pending", "open", "lb_open")]')" "[1, 11, 6]"
+expect "series: the funnel" "$(get "$s" 'd["funnel"]')" "{'seconds': 2.0, 'first_s': 2.0, 'longest_s': 2.0, 'rows': [[2.0, 'gw-0', 'gw-1'], [3.0, 'gw-0', 'gw-1']]}"
+# At its cap a gateway with no handshake in progress is full, not idle.
+expect "series: a full gateway is no funnel" "$(get "$(rc_series "$tmp/f.jsonl" 1000 10)" 'd["funnel"]["seconds"]')" 0.0
+expect "series: unreadable" "$(rc_series "$tmp/none.jsonl" 0 100)" null
+
+[ "$fails" -eq 0 ] && echo "reconnect_test: all passed" || { echo "reconnect_test: $fails failed"; exit 1; }
```

- [ ] **Step 2: Script tests**

Run: `bash -n scripts/tier2/reconnect.sh && make test-scripts`
Expected: `reconnect_test: all passed` (rates from the mark, a late scrape, the funnel for 2s, a full gateway is no funnel), and the other four `all passed`.

- [ ] **Step 3: Dry run on the dev machine**

A throwaway copy as in Task 2 Step 3, with the CPU sets on the dev machine's two CPUs and the scale cut (~2.5 min):
```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/reconnect.sh "$D/reconnect.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/reconnect.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/reconnect.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/reconnect.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/reconnect.sh"
rm -rf tier2-results/reconnect-*
GOIM_TEST_TIER=2 RECONNECT_CPUS_GATEWAY=0-1 RECONNECT_CPUS_LOGIC=0-1 RECONNECT_CPUS_LOADGEN=0-1 RECONNECT_GATEWAY_PROCS=1 \
  RECONNECT_CLIENTS=3000 RECONNECT_RATE=500 RECONNECT_BACK_WAIT=120 RECONNECT_LG_MEMLIMIT=512MiB \
  bash "$D/reconnect.sh" >"$D/out.txt" 2>&1; echo "exit $?"
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/reconnect-*/result.json")[0])); print(len(d["checks"]), [k for k, v in d["checks"].items() if not v["pass"]]); [print(p, d["checks"][p + "_back"]["from_mark"]["p99_ms"], d["checks"][p + "_back"]["waits"], d["checks"][p + "_drop"]["network_change_after_ms"]) for p in ("reset", "netchange")]'
```
The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype): `exit 1`; **21** checks; failing only `no_swap_in_reset` and `no_swap_in_netchange` (the dev machine's swap; none, one or both). `reset`: p99 under 20,500ms (the 0–20s storm wait), `waits` `{"storm": 3000}`, `network_change_after_ms` `None`. `netchange`: p99 under 3,000ms, `waits` with `local` near 3,000 (prototype 2,970) beside `storm` 3,000, `network_change_after_ms` under 1,000 (prototype 399). Then `rm -rf tier2-results/reconnect-*`.

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/reconnect.sh scripts/tier2/reconnect_test.sh Makefile
git commit -m "test: add Tier 2 scenario reconnect" -m "Phases reset and netchange drop 600k SDK clients on 3 gateways at
once, with no SLO. The scenario records the time until 99% are back,
the backoff share, and a per-second series per gateway with a funnel
reading. It checks that every client comes back, no Pong times out,
and no component fails." -m "Refs: R5S-O6, #133, #134, #136 (Plan 6b3c Task 4)."
```

Push the branch.

---

### Task 5: Tier 2 run 31 and the record

- [ ] **Step 1: Run 31, `reconnect` (the user, on the host, ~20 min)**

```bash
git pull && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=reconnect
```
Send back `tier2-results/reconnect-<timestamp>.report.txt`, and `dmesg | tail -20` if a check fails.

What to read, per phase, against gwloss b_stress's 30j rerun (127s from one gateway's kill, loadgen-bound, `pending_mem` 0):
- `P_back.from_mark` p99 (the spec's "time until 99% are back") and `back_s`; the spec's estimate is users ÷ admitted login rate plus the backoff tail. `P_back.backoff_share` (Tier 3's < 1% is a later bound, not gated here) and `waits`.
- `P_back.loadgen_bound`: if true, the times are an upper bound, as b_stress's (#110).
- #133: `P_back.rejects` (`rejects_pending_mem`, `logins_busy`, `rejects_login_queue`, `rejects_at_capacity` per gateway) and `P_back.funnel`. A funnel with `seconds` > 0 is the evidence #133 names for unparking 6b3b5 and #111: the user's call.
- `P.series` per second: which came first of `gateway_set_busy_pct` ≥ 85, `logic_set_busy_pct` ≥ 85, `pending_mem_per_s` > 0 and `busy_per_s` > 0 (Review Focus 4); `logouts_dropped_per_s` (Review Focus 3).
- `netchange` against `reset`: the 0–20s storm wait cut to 0–1s puts the arrivals into ~1s, so the admission control, not the jitter, shapes it.

- [ ] **Step 2 (optional, the user's call): run 30o, the slow phase at 20,000/s**

```bash
GOIM_TEST_TIER=2 FLOODS_PHASES=slow FLOODS_RATE=20000 make test-tier2 SCENARIO=floods
```
Expected: run 30m's shape (loadgen's set at 98–100% for ~16s in the ramp), now verdict `loadgen-saturated` with `slow.series.sat_run_s.loadgen` ≥ 5, so `slow_legit` and `slow_heartbeats` fail labelled, as the user ruled. A `valid` verdict here would mean the rule missed it.

- [ ] **Step 3: Record**

In this plan's *Execution Notes*: each run's commit, duration, checks, and the readings of Step 1. In the roadmap: the plan 6 row (6b3c executed, run 31), and #133's outcome (funnel seen or not). The README's rows (English and 中文) only if the user asks: a new row for the whole-population reconnect.

```bash
git add docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3c-reconnect.md
git commit -m "docs: record Tier 2 run 31 (reconnect)" -m "Refs: #133, #136 (Plan 6b3c Task 5)."
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6c** (Redis Cluster, `gwconns` shard load, ReconcileEnd/drain deadline sizing), then **6d** (Tier 3, k8s; #132's fresh-pod funnel and the LB-side remedies; the drop as a real LB restart).
- **#111 / 6b3b5 (parked):** run 31's `pending_mem` and funnel readings are their input (#133).
- **The spec:** nothing here changes its text. The measured recovery and backoff share go with #97, #101, #125 to the next revision if they differ from the spec's estimate.

## Execution Notes

(none yet)
