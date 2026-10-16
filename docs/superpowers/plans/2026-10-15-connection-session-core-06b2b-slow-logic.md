# Plan 6b2b-slow: Logic Slowed by a CPU Quota (Tier 2 slowlogic) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's load test "Logic slowed down" on the Tier 2 host with SDK clients. Logic runs under a 25% CPU quota. With each gateway's Login-queue cap sized by the spec's rule from Logic's measured throughput, a relogin storm faster than Logic must be answered `Busy`, not `EXPIRED`. Every client comes back, and the gateways' lease renewals stay on time.

**Architecture:** `stack.sh` can start Logic inside a transient systemd user scope with `CPUQuota=` (`STACK_LOGIC_CPU_QUOTA`), which is cgroup v2 `cpu.max`, the mechanism behind a Kubernetes CPU limit. `stack_cpu_max` reads the quota back. `stack_wait_idle` gains a `dropped-ok` mode, because a slowed Logic may legitimately drop Logouts (roadmap #94, amended). A new scenario, `scripts/tier2/slowlogic.sh`, runs two phases on fresh stacks. **calibrate** offers far more logins than Logic can serve, behind a small queue cap (1,000 per gateway, so no queued Login expires), and measures Logic's saturated throughput T. **storm** sets `-login-queue-cap` = T × 4s per gateway (the rule for the lone survivor, which Logic then serves at T; review O1), ramps 2 × 30T SDK clients at T/2 per second (victims capped by `SLOWLOGIC_MAX_VICTIMS`, refused below 27T; O6), then SIGKILLs `gw-1`, so 30T relogins arrive at ~1.5T/s on `gw-0` (#113). Logic's own pace in the storm, T', is recorded beside T (O3). No Go change.

**Tech Stack:** bash 5.2, `systemd-run --user --scope` (systemd 255 on the host; the user manager delegates `cpu memory pids`), the pinned `redis-server` 7.4.11, curl, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here: *Verification → Load tests* ("Logic slowed down: the Login-queue cap triggers `Busy` and clients retry within the SLO; with the queue cap sized by the rule above, logins are rejected as `Busy`, not `EXPIRED` (`expired` stays near zero) *(R4H-O2)*"); *Gateway → Admission control* ("Login-queue cap … **Sizing** *(R4H-O2)*: `login_queue_cap ≤ measured Logic login throughput per gateway × (login_deadline − margin)`"; `Busy{retry_after}` 1–3s); *Failure handling* ("Logic saturated *(R1-O2)*: Lease RPCs keep their own connection and worker pool, so renewals continue. The gateway's Login-queue cap replies `Busy` (1–3s)."); *Client SDK rules* (`Busy`, `EXPIRED` and `KEY_UNKNOWN` retried after 1–3s, 5 in a row, then exponential backoff). Inputs: roadmap #94 and its amendments (quota, not `taskset`; 25%; calibration; dropped Logouts recorded), P5bI-2 (slow Logic by CPU, not by the `login` fault), P6b2a-stressI-2.

**Decisions (2026-10-15, the user, with this plan):** #113 (the load is a kill storm sized from the calibration; the 30s SLO is recorded, not gated) and #114 (6b2b split; this is its second plan). **Proposed with this plan, for review (#113):** a host that cannot set a quota is refused, instead of #94's `taskset` fallback. **Review (2026-10-15):** O1–O7 accepted (`docs/superpowers/objections/connection-session-core-06b2b-slow-logic-dispositions.md`): the storm's cap T × 4 (O1); `storm_busy` ≥ 0.1 × victims (O2); T' over the storm's first 20s (O3); `calibrate_saturated` also gates wasted logins ≤ 1% (O4); `calibrate_lease`, and the 5s bound stated (O5); `SLOWLOGIC_MAX_VICTIMS` and `storm_size` (O6); two numbers corrected (O7). Re-prototyped (dry run below).

**Branch:** `feat/plan6b2b-slow`, from the head of `feat/plan6b2b-b2` once 6b2b-b2 is executed. This plan's patches apply on top of 6b2b-b2's (`stack.sh` changes from both). Execute 6b2b-b2 first.

**Size:** 3 tasks. The prototype (a scratch worktree with 6b2b-b2 applied) ran every block. Task 1 is given as `git apply` patches and Task 2 as a whole new file. A dry run at a 5% quota on the dev machine exercised calibration, the sized cap and real `Busy` (Task 2, Step 2).

## Global Constraints

- Go 1.27.1. No Go change in this plan.
- `make test`, `make test-race-suite` and `make test-scripts` stay green on the dev machine (2 cores / 2GB; its user manager also delegates `cpu`, so the quota tests run there; elsewhere they print `skip quota: …`).
- Scripts: bash with `set -uo pipefail`; JSON written without jq; python3 only reads JSON and does arithmetic; mawk-compatible awk.
- Ports (#76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`.
- CPU sets (#71): gateways `4-9,16-21` (2 gateways, 6 `GOMAXPROCS` each), Logic + Redis `10,11,22,23`, loadgen `0-3,12-15`. The quota applies to Logic alone, on top of its CPU set. Redis is not slowed.
- Spec numbers: `login_deadline` 5s (margin 1s, so the rule's factor is 4s); `Busy{retry_after}` 1–3s; the SDK hands a client to exponential backoff (cap 5 min) after 5 retryable rejects in a row; the SDK waits a uniform 0–20s after losing an established connection; lease renewal every 10s, TTL 30s, call timeout 5s.
- Run 18 measured Logic at 0.41 cores for 5,000 logins/s (~0.08 cores per 1,000/s), so 25% should sustain ~3,000 logins/s. The calibration replaces this estimate with a measurement.
- Swap during a measured window fails it (#70).
- Comments and names match the surrounding code; new behaviour cites its choice number (#113) or the spec's id.

## Review Focus

1. **The lease under the quota.** Lease RPCs have their own connection and worker pool, but share Logic's CPU quota. With GOMAXPROCS 2, the 25ms budget of each 100ms period is spent in 12.5ms, so CFS can throttle the whole cgroup for up to 87.5ms of every period while Login work saturates it. Expected: every renewal still completes inside the gateway's 5s lease timeout, so no gateway sees `missing` or `error`, and each renews once per 10s, both gateways through the calibration and `gw-0` through the storm. Pinned by `calibrate_lease` and `storm_lease`. **Only the 5s bound is checked:** renewal latency is not measured (no lease histogram, and this plan makes no Go change), so a renewal that took 3s passes. A margin needs a gateway lease histogram, a later choice (review O5). If it fails, compare Logic's storm CPU profile (`storm/artifacts/storm/logic.cpu.pb.gz`) and `cpu.stat`'s `nr_throttled` (recorded as `cpu_stat` in `calibrate_saturated`, `storm.busy_window` and `storm`, P6b2b-slowI-1). That would be a finding against R1-O2's "renewals continue", not a harness bug.
2. **A calibration that did not saturate Logic, or saturated it with waste.** If the offered load is too low (or loadgen is the bound), T measures the offered load. If queued logins expire, Logic's CPU partly goes to logins that do not count, and T comes out low (the prototype's first version, ~30% low). Either way the storm's cap and size come out wrong. Expected: `calibrate_saturated` requires Logic's CPU in the window ≥ 0.9 × the quota **and** wasted logins (Logic's `expired`, the gateways' `expired_queued` and `error`) ≤ 1% of Logic's ok logins, since waste burns the quota too and the CPU alone passes it (review O4). When it fails, the storm phase does not run, so no misleading storm is reported. Behind the 1,000 cap the waste is 0. Pinned by `calibrate_saturated`, and by the scenario's early `t2_finish` (dry run: a calibration at 600/s on a 5% quota saturates; the code path for a failed one is the `[ "$saturated" = true ] ||` line). Two further ways the storm comes out mis-sized are recorded or refused: **T' below T** (storm relogins replace gw-1's entries and each queues a Kick to the dead address, inside the same quota), recorded as `storm.busy_window.throughput_per_s` (Logic's `ok + replaced` per second over the kill's first `SLOWLOGIC_BUSY_WINDOW` = 20s) beside T (O3); and **T far above the estimate**, where 30T outgrows the memory budget: victims are capped by `SLOWLOGIC_MAX_VICTIMS` (120,000), and below 27T, where Busy cannot reach its floor, `storm_size` fails and the storm is not run (O6).
3. **Logic's GOMAXPROCS under a quota.** Go ≥ 1.25 derives GOMAXPROCS from the cgroup's CPU limit, rounded up with a floor of 2. Logic's CPU set has 4 CPUs, so it should be 2, not 4. Expected: recorded in `slow_scope.logic_gomaxprocs` (from `go_sched_gomaxprocs_threads`, which the prototype found exported), not gated. With 4 instead, Logic would burst into throttling more often, which shows as worse login latency at the same T.
4. **The end of each phase under a slowed Logic.** 2 × 30T sessions log out when loadgen stops, through the same quota, and a gateway's Logout queue may drop some (the spec allows it; Reconcile repairs them). Expected: `stack_wait_idle 900 dropped-ok` settles, and the dropped count is recorded per gateway in `<phase>_idle.logouts`. A dropped Logout fails b1, b2 and b_stress (`stack_wait_idle` without `dropped-ok`), never this scenario. Pinned by `idle: dropped-ok …` in `stack_test.sh` (Task 1).
5. **Clients handed to exponential backoff by Busy.** A victim answered `Busy` five times in a row backs off exponentially (up to 5 min). Expected: every victim is back within 600s (`storm_back`). The share that reached backoff is recorded (`storm_back.backoff`, the storm field's `backoff`), with the from-kill SLO distribution, which may miss 30s by design (#113). If `storm_back` fails, look at `slo.missing` and `backoff.waits` before raising `SLOWLOGIC_BACK_WAIT`.

## Implementation choices this plan makes

1. **#113: a kill storm sized from a calibration; the 30s SLO recorded, not gated** (decided). Busy needs relogins arriving faster than T. A backlog drained at T for ~25s, plus Busy retries and the backoff after 5 in a row, sits right at 30s, so a gated SLO would be fragile either way. At 30T victims over the SDK's 0–20s storm wait, arrivals are ~1.5T/s. `gw-0`'s cap is the spec's rule for the share in force during the storm: gw-0 is alone and Logic serves it at T, so T × 4s = 4T (review O1; T/2 × 4 = 2T would leave half the rule's budget unused and never test its 1s margin). The queue fills ~8s in and Busy follows. A queued Login waits up to 4T / T = 4s, the rule's bound, so an `EXPIRED` above the gate is a finding against R4H-O2's margin, not a reason to lower the cap. First attempts turned away come to ~20T(V/20T − 1) − 4T for V victims: ~6T of 30T (~20%) before retries. Meeting the Busy floor (0.1 × V) needs V ≥ ~27T. Gates: Busy ≥ 0.1 × victims (O2); EXPIRED (Logic's `expired` and the gateway queue's `expired_queued`, both on `gw-0`'s `logins_total`) ≤ 0.1% of its ok logins; every victim back within 600s; no reject at capacity; no missed lease renewal, in the calibration and the storm.
2. **The calibration measures T as Logic's ok logins per second** over a 10s window, 5s after loadgen starts offering 8,000/s (120k SDK clients over 15s), behind a 1,000 Login-queue cap per gateway (`SLOWLOGIC_CAL_QUEUE_CAP`). The cap is under 1s of Logic's pace per gateway, so no queued Login waits out its deadline, the excess is answered `Busy` at the gateway, and Logic's CPU goes only to logins that count. It still allows up to 5 Login RPCs of 200 in flight per gateway (the gateway's `LoginRPCs` 16 × `LoginBatch` 200), enough to keep 0.25 cores busy, which `calibrate_saturated` checks. The prototype first calibrated at the default cap of 8,192. There Logic spent CPU on logins that came back `EXPIRED`, and gateway entries timed out. T came out ~30% low (165–244/s against 338–346/s at a 5% quota), and the storm's arrivals at 1.5 × T barely outran Logic: in one run 25 Busy instead of ~20,000.
3. **`slowlogic` is its own scenario**, not a `gwloss` phase. It needs a calibration phase whose result sizes the next one, and none of `gwloss`'s sweep, precompute, socket-sampler or Kick gates. About 355 lines, against `gwloss.sh`'s ~740 after 6b2b-b2.
4. **`STACK_LOGIC_CPU_QUOTA` wraps Logic in `systemd-run --user --scope --quiet -p CPUQuota=…`.** `--scope` moves systemd-run itself into a new scope and execs the command. Logic keeps the PID `stack_start` recorded, its process group (from `setsid`) and its CPU affinity (from `taskset`, which execs `systemd-run`). Checked in the prototype: `/proc/PID/comm` is `logic`, its cgroup is `…/app.slice/run-….scope`, `cpu.max` is `50000 100000` for 50%.
5. **No `taskset` fallback** (proposed, #113). #94 kept `taskset` to one CPU as a fallback for a host without cpu delegation. The Tier 2 host has the delegation, and the fallback would test a different slowdown: fewer CPUs, not a throttled quota. A host that cannot set a quota is refused (exit 2) with the file to check.
6. **`stack_wait_idle SECONDS dropped-ok`:** a dropped Logout counts as settled (done + dropped ≥ logins), and the error names both counts (`logouts done 3 + dropped 1 of 5 logins`). Without the argument nothing changes.

## File Structure

- Modify `scripts/stack.sh`: `STACK_LOGIC_CPU_QUOTA`, `stack_cpu_max NAME`, `stack_wait_idle SECONDS [dropped-ok]` (Task 1).
- Modify `scripts/stack_test.sh`: their tests (Task 1).
- Create `scripts/tier2/slowlogic.sh`; modify `Makefile` (the scenario list in `test-tier2`'s comment) (Task 2).
- Modify `README.md` (both languages), the roadmap, this plan's *Execution Notes* (Task 3).

---

### Task 1: Logic under a CPU quota, and dropped Logouts as settled

**Files:**
- Modify: `scripts/stack.sh`
- Test: `scripts/stack_test.sh`

**Interfaces:**
- Consumes: `stack_start NAME CPUS CMD...` (taskset when pinned, setsid, records the PID); `stack_metrics`, `stack_metric_in`, `stack_admins`.
- Produces: `STACK_LOGIC_CPU_QUOTA` (e.g. `25%`; empty: no quota), read by `stack_up`; `stack_cpu_max NAME` prints the `cpu.max` line of NAME's cgroup (`"25000 100000"`), or `null`; `stack_wait_idle SECONDS [dropped-ok]`.

- [ ] **Step 1: Write the failing tests**

Apply (on top of 6b2b-b2's `scripts/stack_test.sh`; `git apply --check` first):

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index 3e5d5e6..f04d0b3 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -75,6 +75,11 @@ fi
   out=$(stack_wait_idle 10 2>&1) && bad "idle: a dropped Logout must fail" || ok "idle: a dropped Logout fails"
   [ $((SECONDS - start)) -lt 3 ] && ok "idle: a dropped Logout fails at once" || bad "idle: a dropped Logout waited $((SECONDS - start))s"
   case $out in *"dropped 1"*) ok "idle: names the dropped Logout" ;; *) bad "idle: dropped not named: [$out]" ;; esac
+  # dropped-ok (Plan 6b2b-slow): a dropped Logout counts as settled.
+  stack_wait_idle 2 dropped-ok && ok "idle: dropped-ok settles done + dropped" || bad "idle: dropped-ok, done 4 + dropped 1 of 5"
+  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 3' 'goim_gateway_logouts_total{result="dropped"} 1'
+  out=$(stack_wait_idle 1 dropped-ok 2>&1) && bad "idle: dropped-ok, a Logout short must keep it busy" || ok "idle: dropped-ok, a Logout short keeps it busy"
+  case $out in *"logouts done 3 + dropped 1 of 5 logins"*) ok "idle: dropped-ok names done and dropped" ;; *) bad "idle: dropped-ok not named: [$out]" ;; esac
   # A gateway that does not answer is busy, never idle.
   kill $srv; wait $srv 2>/dev/null
   out=$(stack_wait_idle 1 2>&1) && bad "idle: a silent gateway must keep it busy" || ok "idle: a silent gateway keeps it busy"
@@ -280,12 +285,25 @@ for name in gw-0 gw-1; do
 done
 
 # STACK_GATEWAY_PROCS overrides the share (one gateway alone would get every CPU).
-STACK_GATEWAYS=1 STACK_GATEWAY_PROCS=1
+# STACK_LOGIC_CPU_QUOTA runs Logic under a CPU quota (Plan 6b2b-slow), where
+# a systemd user manager delegates the cpu controller.
+quota=""
+systemd-run --user --scope --quiet -p CPUQuota=50% true 2>/dev/null && quota=50%
+STACK_GATEWAYS=1 STACK_GATEWAY_PROCS=1 STACK_LOGIC_CPU_QUOTA=$quota
 if stack_up; then
   grep -q " loops=1 " "$STACK_DIR/logs/gw-0.log" && ok "procs: STACK_GATEWAY_PROCS=1 runs 1 loop" || bad "procs: override: $(grep -o ' loops=[0-9]* ' "$STACK_DIR/logs/gw-0.log")"
+  if [ -n "$quota" ]; then
+    expect "quota: Logic's cpu.max" "$(stack_cpu_max logic)" "50000 100000"
+    expect "quota: the PID is Logic's" "$(cat "/proc/${STACK_PIDS[logic]}/comm")" logic
+    stack_pinned logic "$T2_CPUS_LOGIC" && ok "quota: Logic still pinned" || bad "quota: Logic not pinned"
+  else
+    echo "skip quota: no systemd user manager delegating the cpu controller"
+  fi
 else
   bad "up: second stack"
 fi
+STACK_LOGIC_CPU_QUOTA=
+expect "quota: no such process" "$(stack_cpu_max gw-9)" null
 # A gateway that dies during the run is reported, and stays reported after the stop.
 kill -KILL "${STACK_PIDS[gw-0]}"
 sleep 0.2
```

- [ ] **Step 2: Run them to see them fail**

Run: `make test-scripts 2>&1 | grep -E 'FAIL|stack_test:'`
Expected, with `stack_test: 4 failed` as the last line:

```
FAIL idle: dropped-ok, done 4 + dropped 1 of 5
FAIL idle: dropped-ok not named: [stack: gw-0 dropped 1 Logouts: they never settle (logouts done 3 of 5 logins, dropped 1)]
FAIL quota: Logic's cpu.max: got [], want [50000 100000]
FAIL quota: no such process: got [], want [null]
```

Without the mode, a dropped Logout fails at once. `stack_cpu_max: command not found` appears on stderr. `quota: the PID is Logic's` and `quota: Logic still pinned` already pass, since Logic starts without the quota. They pin that the wrapper keeps both.

- [ ] **Step 3: Implement**

Apply:

```diff
diff --git a/scripts/stack.sh b/scripts/stack.sh
index 6d2b62d..c0c45c9 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -13,8 +13,10 @@
 #   stack_expect_dead NAME [SECONDS]  # NAME was killed on purpose (loadgen /kill): reap and record it
 #   stack_metric NAME METRIC [MATCH]  # sum of METRIC's samples (with MATCH in the line) on NAME's /metrics
 #   stack_metrics NAME          # NAME's /metrics once; stack_metric_in BODY METRIC [MATCH] reads it
-#   stack_wait_idle SECONDS     # phase boundary: every gateway at 0 connections, 0 awaiting Logout,
-#                               #   its Logouts settled (one done per login, none dropped)
+#   stack_wait_idle SECONDS [dropped-ok]  # phase boundary: every gateway at 0 connections, 0 awaiting
+#                               #   Logout, its Logouts settled (one done per login, none dropped;
+#                               #   dropped-ok: a dropped Logout counts as settled)
+#   stack_cpu_max NAME          # NAME's cgroup cpu.max, e.g. "25000 100000" (null: unreadable)
 #   stack_usage_start [N=PID..] # start a CPU window over the stack's processes (and N=PID extras)
 #   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb", "rss_peak_mb"}...} since the start
 #   stack_counters NAME KEY=METRIC[:MATCH]...  # {"KEY": sum, ...} from one scrape of NAME (null: no answer)
@@ -38,6 +40,9 @@
 #   STACK_GATEWAY_PROCS  each gateway's GOMAXPROCS (default: max(1, CPUs / STACK_GATEWAYS),
 #                        CPUs = T2_CPUS_GATEWAY when pinned, else all online; #79)
 #   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
+#   STACK_LOGIC_CPU_QUOTA  e.g. 25%: Logic runs in a transient systemd user scope with
+#                        that CPUQuota (cgroup cpu.max, as a Kubernetes CPU limit; Plan
+#                        6b2b-slow); needs the user manager to delegate the cpu controller
 #   STACK_GATEWAY_ENV    array of VAR=VALUE for every gateway, e.g. (GOGC=off GOMEMLIMIT=4GiB) (#88)
 #   STACK_DRAIN_WINDOW   gateway -drain-window (default 1s, so stack_down's drain ends and
 #                        exits 0 well inside STACK_DOWN_WAIT; a measured drain sets 60s)
@@ -185,7 +190,13 @@ stack_up() {
   fi
   stack_start devtoken "$logic_cpus" "$bin/devtoken" serve -addr "$devtoken" -jwks "$STACK_DIR/keys/jwks.json"
   stack_wait devtoken "http://$devtoken/" || return 1
-  stack_start logic "$logic_cpus" "$bin/logic" -redis "$redis_addr" -redis-cluster=false \
+  # A CPU quota (Plan 6b2b-slow): systemd-run --scope moves itself into a
+  # new scope and execs Logic, so the PID, the process group and the
+  # affinity stay as stack_start set them.
+  local logic=("$bin/logic")
+  [ -n "${STACK_LOGIC_CPU_QUOTA:-}" ] &&
+    logic=(systemd-run --user --scope --quiet -p "CPUQuota=$STACK_LOGIC_CPU_QUOTA" -- "$bin/logic")
+  stack_start logic "$logic_cpus" "${logic[@]}" -redis "$redis_addr" -redis-cluster=false \
     -grpc-addr "$grpc" -lease-addr "$lease" -admin-addr "$admin" \
     -jwks-url "http://$devtoken/" -jwks-fallback "$STACK_DIR/keys/jwks.json" \
     -kick-addr-file "$STACK_DIR/kick.json" "${STACK_LOGIC_ARGS[@]}"
@@ -328,9 +339,12 @@ stack_metric_in() {
 # answer counts as busy. Only running gateways count: a killed one's
 # sessions end by EpochEnded, not Logouts, and it is gone from stack_admins
 # (stack_expect_dead). Fails, naming what is left, after SECONDS, or at once
-# on a dropped Logout, which never settles.
+# on a dropped Logout, which never settles. With dropped-ok, a dropped
+# Logout counts as settled instead (done + dropped >= logins): a slowed
+# Logic may fill a gateway's Logout queue, which the spec allows, Reconcile
+# repairing the session (R1-O3, R3-O1; roadmap #94, P6b2a-stressI-2).
 stack_wait_idle() {
-  local end=$((SECONDS + $1)) name admin busy body c a ldone logins dropped
+  local end=$((SECONDS + $1)) ok=${2:-} name admin busy body c a ldone logins dropped
   while :; do
     busy=""
     while read -r name admin; do
@@ -344,12 +358,13 @@ stack_wait_idle() {
       [ "$ldone" = null ] && ldone=0
       [ "$logins" = null ] && logins=0
       [ "$dropped" = null ] && dropped=0
-      if [ "$dropped" != 0 ]; then
+      if [ "$dropped" != 0 ] && [ "$ok" != dropped-ok ]; then
         echo "stack: $name dropped $dropped Logouts: they never settle (logouts done $ldone of $logins logins, dropped $dropped)" >&2
         return 1
       fi
-      [ "$c" = 0 ] && [ "$a" = 0 ] && [ "$ldone" -ge "$logins" ] ||
-        busy+=" $name: connections $c, awaiting_logout $a, logouts done $ldone of $logins logins;"
+      [ "$ok" = dropped-ok ] || dropped=0
+      [ "$c" = 0 ] && [ "$a" = 0 ] && [ $((ldone + dropped)) -ge "$logins" ] ||
+        busy+=" $name: connections $c, awaiting_logout $a, logouts done $ldone$([ "$dropped" = 0 ] || echo " + dropped $dropped") of $logins logins;"
     done < <(stack_admins)
     [ -z "$busy" ] && return 0
     if [ $SECONDS -ge $end ]; then
@@ -488,6 +503,16 @@ for le in les:
     print("null") # Q above 1 (P6b2b-b2I-5)' "$1" "$2" "$3" 2>/dev/null || echo null
 }
 
+# stack_cpu_max NAME: the cpu.max of NAME's cgroup (cgroup v2), e.g.
+# "25000 100000" for a 25% quota, "max 100000" for none; null if NAME is
+# not running or its cgroup has no cpu.max (the cpu controller is not
+# enabled there).
+stack_cpu_max() {
+  local pid=${STACK_PIDS[$1]:-} path
+  path=$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null)
+  [ -n "$pid" ] && [ -n "$path" ] && cat "/sys/fs/cgroup$path/cpu.max" 2>/dev/null || echo null
+}
+
 # stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
 stack_pinned() {
   local have
```

- [ ] **Step 4: Run the tests**

Run: `make test-scripts 2>&1 | grep -E 'FAIL|skip|quota|dropped-ok|stack_test:'`
Expected (dev machine, which delegates `cpu`):

```
ok   idle: dropped-ok settles done + dropped
ok   idle: dropped-ok, a Logout short keeps it busy
ok   idle: dropped-ok names done and dropped
ok   quota: Logic's cpu.max
ok   quota: the PID is Logic's
ok   quota: Logic still pinned
ok   quota: no such process
stack_test: all passed
```

On a machine without a systemd user manager, the three Logic quota lines are replaced by `skip quota: no systemd user manager delegating the cpu controller`.

Check that a broken rule is caught: in `stack_wait_idle`, change `[ "$ok" = dropped-ok ] || dropped=0` to `dropped=0`. Expected: `FAIL idle: dropped-ok, done 4 + dropped 1 of 5` and `FAIL idle: dropped-ok not named: …`. Revert.

- [ ] **Step 5: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh
git commit -m "feat(stack): Logic under a CPU quota (STACK_LOGIC_CPU_QUOTA, stack_cpu_max); stack_wait_idle dropped-ok (6b2b-slow, #94)"
```

---

### Task 2: the `slowlogic` scenario (#113)

**Files:**
- Create: `scripts/tier2/slowlogic.sh` (mode 0755)
- Modify: `Makefile` (comment of `test-tier2`)

**Interfaces:**
- Consumes: Task 1 (`STACK_LOGIC_CPU_QUOTA`, `stack_cpu_max`, `stack_wait_idle … dropped-ok`); `stack_up`, `stack_counters`, `stack_counters_delta`, `stack_usage_start`/`_json`, `stack_snapshot`, `stack_profile`, `stack_expect_dead`, `stack_pinned`, `stack_health`; loadgen `lg_start`, `lg_wait_online`, `lg_wait_back`, `lg_get`, `lg_post /kill`, `lg_profile`, `lg_stop`; `lib.sh`'s `t2_*`. Gateway metrics: `goim_gateway_logins_total{result="ok|busy|expired|expired_queued|error"}`, `goim_gateway_admission_rejects_total{reason="at_capacity|login_queue"}`, `goim_gateway_lease_calls_total{result}`; Logic's `goim_logic_logins_total{result}`, `goim_logic_lease_renewals_total{result}`, `go_sched_gomaxprocs_threads`.
- Produces: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=slowlogic`; result fields `calibrate` (`throughput_per_s` = T) and `storm` (`busy_window.throughput_per_s` = T', `clamped`); 17 checks on a full run. Logic's Kick counters: `goim_logic_kick_attempts_total`, `goim_logic_kick_dropped_total`.

- [ ] **Step 1: Create the scenario**

`scripts/tier2/slowlogic.sh`:

```bash
#!/usr/bin/env bash
# Tier 2: slowlogic (Plan 6b2b-slow), the spec's load test "Logic slowed
# down" (P5bI-2) with SDK clients: Logic runs under a CPU quota (cgroup
# cpu.max through a systemd user scope, as a Kubernetes CPU limit; roadmap
# #94, amended), on two gateways, each phase on a fresh stack:
#   calibrate: SDK clients offered well above Logic's capacity, behind a
#       small Login-queue cap, so no queued Login expires; Logic's saturated
#       login throughput T is the rate of its ok logins while its CPU sits
#       at the quota.
#   storm: each gateway's -login-queue-cap set by the spec's rule for the
#       gateway that takes the storm, T per gateway x (login_deadline - 1s
#       margin) = T x 4s, since gw-0 is then alone and Logic serves it at T;
#       2 x 30T clients ramped at T/2 per second (Logic at half load), then
#       gw-1 SIGKILLed: its 30T clients come back over the SDK's 0-20s storm
#       wait, at 1.5T per second, onto gw-0 (#113). Logins beyond Logic's
#       pace queue up to the cap (~8s in; a queued Login waits up to 4s, the
#       rule's bound), and the rest are answered Busy: rejected as Busy, not
#       EXPIRED (R4H-O2). At 30T, Logic needs >= 30s to serve them, so the
#       30s SLO is recorded, not gated (#113). Busy needs >= ~27T victims
#       to reach its floor (0.1 x victims), so a population capped below
#       that is refused, not run.
# Logic is one process (minReplicas, no HPA).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=slowlogic
#   Results: tier2-results/slowlogic-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/slowlogic-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 15 minutes (at T ~3,000/s).
#
# Settings:
#   SLOWLOGIC_QUOTA        Logic's CPUQuota (default 25%: ~3,000 logins/s from run
#                          18's 0.41 cores at 5,000/s; roadmap #94, amended)
#   SLOWLOGIC_CAL_CLIENTS  calibrate: SDK clients (default 120000)
#   SLOWLOGIC_CAL_RATE     calibrate: clients started per second (default 8000,
#                          above Logic's ~3,000/s at 25%)
#   SLOWLOGIC_CAL_QUEUE_CAP calibrate: each gateway's -login-queue-cap (default
#                          1000: under 1s of Logic's pace per gateway at ~1,500/s,
#                          so no Login waits out its deadline and Logic spends no
#                          CPU on logins that come back EXPIRED; up to 5 Login RPCs
#                          of 200 in flight per gateway, enough to keep 0.25 cores
#                          busy, which calibrate_saturated checks). The default
#                          8,192 made T ~30% low in the prototype, and the storm's
#                          Busy marginal
#   SLOWLOGIC_CAL_SKIP     calibrate: seconds from loadgen's start to the window
#                          (default 5: the Login queues fill first)
#   SLOWLOGIC_CAL_WINDOW   calibrate: the measured window, seconds (default 10)
#   SLOWLOGIC_VICTIMS      storm: clients on each gateway (default 30 x T, at most
#                          SLOWLOGIC_MAX_VICTIMS)
#   SLOWLOGIC_MAX_VICTIMS  storm: the most victims the memory budget covers (default
#                          120000: 240k SDK clients, ~5GB in loadgen); below 27 x T
#                          the storm is refused (storm_size): lower SLOWLOGIC_QUOTA
#   SLOWLOGIC_RATE         storm: clients started per second (default T/2)
#   SLOWLOGIC_QUEUE_CAP    storm: each gateway's -login-queue-cap (default
#                          T x 4, the spec's rule for the lone survivor)
#   SLOWLOGIC_BUSY_WINDOW  storm: seconds from the kill over which Logic's own
#                          pace T' (ok + replaced logins per second) and cores
#                          are recorded (default 20, the SDK's storm wait)
#   SLOWLOGIC_BACK_WAIT    storm: seconds after the kill to wait for every victim
#                          (default 600: Busy 5 times in a row hands a client to
#                          the SDK's exponential backoff, capped at 5 min)
#
# Checks:
#   calibrate_stack     redis, devtoken, Logic and both gateways become Ready
#   slow_scope          Logic runs under the quota (its cgroup's cpu.max) and
#                       on its CPU set (once, in calibrate); Logic's GOMAXPROCS
#                       is recorded (Go derives it from the quota)
#   calibrate_saturated Logic's CPU in the window was at least 0.9 x the
#                       quota, so T is Logic's pace, not the offered load's,
#                       and wasted logins (Logic's expired, the gateways'
#                       expired_queued and error) were at most 1% of its ok
#   calibrate_lease     both gateways renewed their leases every interval
#                       while Logic was saturated (no missing or error; the
#                       5s call timeout is the bound, latency not measured),
#                       and Logic saw no renewal error
#   calibrate_idle      after loadgen stops, every gateway reaches 0
#                       connections and settles its Logouts; dropped Logouts
#                       count as settled and are recorded (roadmap #94)
#   calibrate_processes nothing died, panicked or stopped uncleanly
#   storm_size          30 x T victims (or the capped number) is at least
#                       27 x T, so Busy can reach its floor; recorded with
#                       whether the cap clamped it
#   storm_stack         as calibrate_stack, with the queue cap set
#   storm_online        every client logs in, each gateway holds 45-55%
#   storm_busy          gw-0 answered Busy at its Login-queue cap in the storm,
#                       at least 0.1 x victims times
#   storm_expired       gw-0's EXPIRED logins in the storm (Logic's and the
#                       queue's, expired_queued) are at most 0.1% of its ok
#                       logins: Busy, not EXPIRED (R4H-O2)
#   storm_back          every victim logs in again within SLOWLOGIC_BACK_WAIT,
#                       and gw-0 rejected none at capacity; the 30s SLO from
#                       the kill is recorded (#113)
#   storm_lease         gw-0 renews its lease every interval through the
#                       storm (own connection and worker pool, same quota,
#                       R1-O2; the 5s call timeout is the bound), and Logic
#                       saw no renewal error
#   no_swap_in_storm    no page swapped in from the kill to the last victim (#70)
#   storm_idle          as calibrate_idle
#   storm_processes     as calibrate_processes
#   artifacts           metrics before and after each window, the storm's CPU
#                       profiles, the SLO and loadgen's results
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init slowlogic
t2_need curl taskset sudo python3 systemd-run
t2_require_host_setup

QUOTA=${SLOWLOGIC_QUOTA:-25%}
case $QUOTA in
  [1-9]% | [1-9][0-9]% | 100%) ;;
  *) echo "slowlogic: SLOWLOGIC_QUOTA must be 1%-100%, got '$QUOTA'" >&2; exit 1 ;;
esac
# The quota needs the user manager to delegate the cpu controller (the Tier 2
# host's does: user@UID.service/cgroup.controllers lists cpu).
systemd-run --user --scope --quiet -p "CPUQuota=$QUOTA" true 2>/dev/null ||
  t2_refuse "refused: systemd-run --user --scope cannot set CPUQuota=$QUOTA (does /sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers list cpu?)"
t2_mem_budget 10
t2_nofile 1048576

export STACK_PIN=1 STACK_GATEWAYS=2 STACK_LOGIC_CPU_QUOTA=$QUOTA
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

GW_COUNTERS=(
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  logins_expired='goim_gateway_logins_total:result="expired"'
  logins_expired_queued='goim_gateway_logins_total:result="expired_queued"'
  logins_error='goim_gateway_logins_total:result="error"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
)
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  logins_expired='goim_logic_logins_total:result="expired"'
  lease_error='goim_logic_lease_renewals_total:result="error"'
  kick_attempts='goim_logic_kick_attempts_total'
  kick_dropped='goim_logic_kick_dropped_total'
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# up NAME: a fresh stack in $T2_OUT/NAME; checks NAME_stack.
up() {
  export STACK_DIR=$T2_OUT/$1
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  local busy
  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  local rc=$?
  t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  return $rc
}

# down NAME: loadgen stopped, the gateways idle (dropped Logouts settled and
# recorded), the stack stopped; checks NAME_idle and NAME_processes.
down() {
  lg_stop
  local t=$SECONDS idle stop_s logouts="" g body problems
  idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
    body=$(stack_metrics "$g") || body=""
    logouts+="${logouts:+, }\"$g\": {\"done\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"dropped\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"'), \"logins_ok\": $(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"')}"
  done
  t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"logouts\": {$logouts}, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
quota_cores=$(python3 -c 'import sys; print(int(sys.argv[1].rstrip("%")) / 100)' "$QUOTA")
missing=""

# Calibrate: Logic's saturated login throughput at the quota. A small queue
# cap keeps every queued Login inside its deadline: the excess is answered
# Busy at the gateway, and Logic's CPU goes only to logins that count.
t0=$SECONDS
cal_clients=${SLOWLOGIC_CAL_CLIENTS:-120000} cal_rate=${SLOWLOGIC_CAL_RATE:-8000}
cal_cap=${SLOWLOGIC_CAL_QUEUE_CAP:-1000}
skip=${SLOWLOGIC_CAL_SKIP:-5} win=${SLOWLOGIC_CAL_WINDOW:-10}
STACK_GATEWAY_ARGS=(-max-conns $((cal_clients + 10000)) -login-queue-cap "$cal_cap")
up calibrate || t2_finish
cpu_max=$(stack_cpu_max logic)
want_max="$(python3 -c 'import sys; print(round(float(sys.argv[1]) * 100000))' "$quota_cores") 100000"
gomaxprocs=$(stack_metric logic go_sched_gomaxprocs_threads)
wrong=""
for g in gw-0 gw-1; do stack_pinned "$g" "$T2_CPUS_GATEWAY" || wrong+=" $g"; done
for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
scope=false
[ "$cpu_max" = "$want_max" ] && [ -z "$wrong" ] && scope=true
t2_check slow_scope "$scope" \
  "\"quota\": $(t2_json_str "$QUOTA"), \"cpu_max\": $(t2_json_str "$cpu_max"), \"want\": $(t2_json_str "$want_max"), \"logic_gomaxprocs\": $gomaxprocs, \"wrong_pinning\": $(t2_json_str "${wrong# }")"
# calibrate_lease's counters cover the whole saturated stretch, from
# loadgen's start to its stop.
lease0=$(stack_counters logic "${LOGIC_COUNTERS[@]}") lease_t=$SECONDS
declare -A lg0=() lg1=() g0=() g1=()
for g in gw-0 gw-1; do lg0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
lg_start calibrate -mode sdk -clients "$cal_clients" -rate "$cal_rate" -uid-prefix cal
sleep "$skip"
stack_snapshot cal_before
l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
for g in gw-0 gw-1; do g0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
stack_usage_start loadgen="$LG_PID"
sleep "$win"
usage=$(stack_usage_json)
l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
for g in gw-0 gw-1; do g1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
stack_snapshot cal_after
logic_d=$(stack_counters_delta "$l0" "$l1")
gws_d="{\"gw-0\": $(stack_counters_delta "${g0[gw-0]}" "${g1[gw-0]}"), \"gw-1\": $(stack_counters_delta "${g0[gw-1]}" "${g1[gw-1]}")}"
# T: Logic's ok logins per second over the window, whole.
T=$(echo "{\"l\": $logic_d, \"u\": $usage}" | lg_py - 'int(d["l"]["logins_ok"] / d["u"]["window_s"])' 2>/dev/null)
logic_cores=$(echo "$usage" | lg_py - 'd["logic"]["cores"]' 2>/dev/null)
# Wasted logins burn the quota too, so the CPU alone does not prove T:
# Logic's expired and the gateways' expired_queued and error are at most 1%
# of its ok logins (O4 of the plan's review).
waste=$(echo "{\"l\": $logic_d, \"g\": $gws_d}" | lg_py - '{"logic_expired": d["l"]["logins_expired"] or 0, "expired_queued": sum(d["g"][g]["logins_expired_queued"] or 0 for g in d["g"]), "error": sum(d["g"][g]["logins_error"] or 0 for g in d["g"])}' 2>/dev/null)
saturated=$(echo "{\"c\": ${logic_cores:-null}, \"w\": ${waste:-null}, \"l\": $logic_d}" | lg_true - 'd["c"] is not None and d["c"] >= 0.9 * '"$quota_cores"' and d["w"] is not None and sum(d["w"].values()) <= 0.01 * (d["l"]["logins_ok"] or 0)')
[ -n "$T" ] && [ "$T" -gt 0 ] 2>/dev/null || { T=null saturated=false; }
t2_check calibrate_saturated "$saturated" "\"throughput_per_s\": $T, \"logic_cores\": ${logic_cores:-null}, \"quota_cores\": $quota_cores, \"waste\": ${waste:-null}, \"waste_max\": $(echo "$logic_d" | lg_py - '0.01 * (d["logins_ok"] or 0)'), \"window_s\": $(echo "$usage" | lg_py - 'd["window_s"]')"
t2_field calibrate "{\"clients\": $cal_clients, \"rate\": $cal_rate, \"queue_cap\": $cal_cap, \"skip_s\": $skip, \"throughput_per_s\": $T, \"logic_gomaxprocs\": $gomaxprocs, \"usage\": $usage, \"logic\": $logic_d, \"gateways\": $gws_d, \"loadgen_online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
lease1=$(stack_counters logic "${LOGIC_COUNTERS[@]}") lease_s=$((SECONDS - lease_t))
for g in gw-0 gw-1; do lg1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
lease_d="{\"gw-0\": $(stack_counters_delta "${lg0[gw-0]}" "${lg1[gw-0]}"), \"gw-1\": $(stack_counters_delta "${lg0[gw-1]}" "${lg1[gw-1]}"), \"logic\": $(stack_counters_delta "$lease0" "$lease1")}"
t2_check calibrate_lease "$(echo "$lease_d" | lg_true - 'all(d[g]["lease_error"] == 0 and d[g]["lease_missing"] == 0 and d[g]["lease_live"] >= '"$lease_s"' // 10 - 1 for g in ("gw-0", "gw-1")) and d["logic"]["lease_error"] == 0')" \
  "\"elapsed_s\": $lease_s, \"gateways\": $(echo "$lease_d" | lg_py - '{g: {"live": d[g]["lease_live"], "missing": d[g]["lease_missing"], "error": d[g]["lease_error"]} for g in ("gw-0", "gw-1")}'), \"logic_lease_error\": $(echo "$lease_d" | lg_py - 'd["logic"]["lease_error"]')"
down calibrate
t2_duration calibrate $((SECONDS - t0))
for f in cal_before/logic cal_after/logic; do
  grep -q '^# TYPE' "$T2_OUT/calibrate/artifacts/$f.metrics" 2>/dev/null || missing+=" calibrate/$f.metrics"
done
[ "$saturated" = true ] || { t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"; t2_finish; }

# The storm, sized from T (#113).
t0=$SECONDS
# The population, capped by the memory budget (SLOWLOGIC_MAX_VICTIMS). Busy
# reaches its floor only from ~27T victims: over the 0-20s storm wait they
# arrive at V/20 per second against a drain of T, the 4T queue fills after
# 4T / (V/20 - T) seconds, and first attempts turned away after that come
# to 20T(V/20T - 1) - 4T >= 0.1V for V >= ~26.7T.
max_victims=${SLOWLOGIC_MAX_VICTIMS:-120000} clamped=false
victims=${SLOWLOGIC_VICTIMS:-$((30 * T))}
[ -z "${SLOWLOGIC_VICTIMS:-}" ] && [ "$victims" -gt "$max_victims" ] && victims=$max_victims clamped=true
need=$((27 * T))
t2_check storm_size "$(t2_bool [ "$victims" -ge "$need" ])" "\"throughput_per_s\": $T, \"victims\": $victims, \"need\": $need, \"max_victims\": $max_victims, \"clamped\": $clamped, \"victims_per_t\": $(python3 -c 'import sys; print(round(int(sys.argv[1]) / int(sys.argv[2]), 1))' "$victims" "$T")"
[ "$victims" -ge "$need" ] || { t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"; t2_finish; }
clients=$((2 * victims))
rate=${SLOWLOGIC_RATE:-$((T / 2))}
qcap=${SLOWLOGIC_QUEUE_CAP:-$((T * 4))}
back_wait=${SLOWLOGIC_BACK_WAIT:-600}
busy_win=${SLOWLOGIC_BUSY_WINDOW:-20}
STACK_GATEWAY_ARGS=(-max-conns $((clients + clients / 10)) -login-queue-cap "$qcap")
up storm || t2_finish
lg_start storm -mode sdk -clients "$clients" -rate "$rate" -uid-prefix slow
online=false
lg_wait_online "$clients" $((clients / rate * 3 + 60)) &&
  online=$(lg_get /stats 'd["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45')
[ "$online" = true ] || online=false
by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
ramp_gw=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_check storm_online "$online" "\"want\": $clients, \"by_gateway\": $by_gateway, \"rate\": $rate, \"failed\": $(lg_get /stats 'd["failed"]'), \"gw0_ramp\": $ramp_gw"
if [ "$online" != true ]; then
  down storm
  t2_duration storm $((SECONDS - t0))
  t2_finish
fi

stack_snapshot before
l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
s0=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_record_start storm
stack_usage_start loadgen="$LG_PID"
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - d)
kill_t=$SECONDS
# T': Logic's own pace while the storm keeps it saturated (relogins that
# replace gw-1's entries, each with a Kick to the dead address), against the
# calibration's fresh logins. Its own usage window, in a subshell, leaves
# the storm's whole-window reading alone.
(b0=$(stack_counters logic "${LOGIC_COUNTERS[@]}"); stack_usage_start; sleep "$busy_win"
  u=$(stack_usage_json); b1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  echo "{\"u\": $u, \"d\": $(stack_counters_delta "$b0" "$b1")}" >"$STACK_DIR/busy.json") >/dev/null 2>&1 &
busy_pid=$!
stack_expect_dead gw-1 60
dead=$?
(stack_profile storm 30 & lg_profile storm 30 & wait) >/dev/null 2>&1 &
prof=$!
back=$(t2_bool lg_wait_back death "$back_wait")
back_s=$((SECONDS - kill_t))
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
wait "$prof" "$busy_pid"
usage=$(stack_usage_json)
busy_window=$(lg_py "$STACK_DIR/busy.json" '{"window_s": d["u"]["window_s"], "throughput_per_s": int(((d["d"]["logins_ok"] or 0) + (d["d"]["logins_replaced"] or 0)) / d["u"]["window_s"]), "logic_cores": d["u"]["logic"]["cores"], "logic": d["d"]}' 2>/dev/null) || busy_window=null
[ -n "$busy_window" ] || busy_window=null
l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
s1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_record_end storm
stack_snapshot after
logic_d=$(stack_counters_delta "$l0" "$l1")
gw_d=$(stack_counters_delta "$s0" "$s1")
window_s=$(echo "$usage" | lg_py - 'd["window_s"]')
slo=$STACK_DIR/artifacts/slo-death.json

t2_check storm_busy "$(echo "$gw_d" | lg_true - '(d["logins_busy"] or 0) >= 0.1 * '"$victims")" \
  "\"busy\": $(echo "$gw_d" | lg_py - 'd["logins_busy"]'), \"floor\": $((victims / 10)), \"rejects_login_queue\": $(echo "$gw_d" | lg_py - 'd["rejects_login_queue"]'), \"queue_cap\": $qcap"
t2_check storm_expired "$(echo "$gw_d" | lg_true - '(d["logins_expired"] or 0) + (d["logins_expired_queued"] or 0) <= 0.001 * d["logins_ok"]')" \
  "\"expired\": $(echo "$gw_d" | lg_py - 'd["logins_expired"]'), \"expired_queued\": $(echo "$gw_d" | lg_py - 'd["logins_expired_queued"]'), \"ok\": $(echo "$gw_d" | lg_py - 'd["logins_ok"]'), \"logic_expired\": $(echo "$logic_d" | lg_py - 'd["logins_expired"]')"
pass=false
[ $dead -eq 0 ] && [ "$back" = true ] && [ "$(echo "$gw_d" | lg_py - 'd["rejects_at_capacity"]')" = 0 ] &&
  pass=$(lg_true "$slo" 'd["population"] == '"$victims"' and d["back"] == d["population"]')
t2_check storm_back "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"backoff\": $(lg_get /stats 'd["backoff_since_mark"]'), \"slo\": $(cat "$slo")"
t2_check storm_lease "$(echo "{\"g\": $gw_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'd["g"]["lease_error"] == 0 and d["g"]["lease_missing"] == 0 and d["g"]["lease_live"] >= d["w"] // 10 - 1 and d["l"]["lease_error"] == 0')" \
  "\"window_s\": $window_s, \"gw-0\": {\"live\": $(echo "$gw_d" | lg_py - 'd["lease_live"]'), \"missing\": $(echo "$gw_d" | lg_py - 'd["lease_missing"]'), \"error\": $(echo "$gw_d" | lg_py - 'd["lease_error"]')}, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
t2_field storm "{\"throughput_per_s\": $T, \"busy_window\": $busy_window, \"victims\": $victims, \"clamped\": $clamped, \"clients\": $clients, \"rate\": $rate, \"queue_cap\": $qcap, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"by_gateway_at_kill\": $by_gateway, \"back_s\": $back_s, \"usage\": $usage, \"logic\": $logic_d, \"gw-0\": $gw_d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]'), \"backoff\": $(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')}"
down storm
t2_duration storm $((SECONDS - t0))

a=$T2_OUT/storm/artifacts
for f in before/logic before/gw-0 after/logic after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" storm/$f.metrics"
done
for f in logic gw-0 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm/$f.cpu"; done
[ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" storm/slo-death.json"
[ "$(lg_true "$a/loadgen-storm.json" '"mode" in d')" = true ] || missing+=" storm/loadgen-storm.json"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "Logic scaled out under load (HPA from minReplicas): one Logic process here (Tier 3, Plan 6d)" \
  "a slowed Redis, or Logic slowed by something other than CPU (Redis Cluster failover in Plan 6c)" \
  "the 30s SLO under a slowed Logic: recorded, not gated (#113)"
t2_finish
```

Then `chmod +x scripts/tier2/slowlogic.sh` and apply:

```diff
diff --git a/Makefile b/Makefile
index f3be91d..a4a55e8 100644
--- a/Makefile
+++ b/Makefile
@@ -71,7 +71,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, capacity, gwloss, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
```

Run: `bash -n scripts/tier2/slowlogic.sh && echo ok`
Expected: `ok`.

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, so the script is checked in a throwaway copy, as `gwloss` is: the guard lowered, the root parts removed, the scale cut. The dev machine delegates `cpu`, so the quota is real here. A 5% quota makes T small enough for this machine and lets Busy really happen.

```bash
D=$(mktemp -d); cp scripts/tier2/slowlogic.sh "$D/slowlogic.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget 10$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 systemd-run$|t2_need curl taskset python3 systemd-run|' "$D/slowlogic.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/slowlogic.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/slowlogic.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/slowlogic.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  SLOWLOGIC_QUOTA=5% SLOWLOGIC_CAL_CLIENTS=6000 SLOWLOGIC_CAL_RATE=600 SLOWLOGIC_CAL_QUEUE_CAP=100 bash "$D/slowlogic.sh"; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype after the review, ~4 min): `exit 1`. `result.json` parses and has 17 checks. All pass except `storm_expired` (see below) and possibly `no_swap_in_storm` from the dev machine's own swapping (the prototype saw 3–17 pages in, in every run). Values from the re-prototyped run (2026-10-15, after review O1–O7):
- `slow_scope`: `cpu_max` `"5000 100000"` = `want`, `logic_gomaxprocs` 2, `wrong_pinning` `""`.
- `calibrate_saturated`: `throughput_per_s` 347 (338–346 in the first prototype), `logic_cores` 0.050 of `quota_cores` 0.05, `waste` all 0 (`waste_max` ≈ 35), `window_s` 10. In the `calibrate` field: gateways `logins_busy` 3,600–4,300 each.
- `calibrate_lease`: `elapsed_s` 19, each gateway `live` 1, `missing` 0, `error` 0.
- `storm_size`: `victims` 10,410 ≥ `need` 9,369 (27T), `clamped` false, `victims_per_t` 30.
- `storm_stack`: `gateway_args` `"-max-conns 22902 -login-queue-cap 1388"` (4T); `storm_online`: 20,820 clients at 173/s, half per gateway, `gw0_ramp.logins_busy` 0.
- `storm_busy`: `busy` 18,505 = `rejects_login_queue`, `floor` 1,041.
- **`storm_expired` fails:** `expired` 889, `expired_queued` 0, `ok` 10,410, `logic_expired` 962 (~9%); gw-0 `logins_error` 219; login latency p99 6.4s. The storm's T' (`storm.busy_window`) was 285/s, 82% of T, with Logic at its quota (0.049 cores): every storm login was `replaced` (5,717 in 20s) with a Kick to the dead gw-1 (5,700). The 4T cap (1,388) held 4.9s of work at T', past the rule's 4s. This is the result the review's O1/O3 asked the run to expose. Under the 2T cap, the first prototype had 0 EXPIRED. The user chose to keep the cap and gates (dispositions, follow-up F1). Run 29 may fail `storm_expired` the same way, which is a finding against R4H-O2.
- `storm_back`: `back_all` true, `back_s` 54, `backoff` `{"clients": 1044, "waits": {"backoff": 1518}}` (~10% of the victims), `slo.from_mark` p50 19.9s, p99 37.2s, max 53.4s: over 30s, as #113 expects at 30T.
- `storm_lease`: gw-0 `live` 5 over `window_s` 53.8, `missing` 0, `error` 0.
- `calibrate_idle` and `storm_idle`: `stop_s` a few seconds, no Logout dropped at this scale.

`calibrate/artifacts/` holds `cal_before/` and `cal_after/`; `storm/artifacts/` holds `before/`, `after/`, `storm/{logic,gw-0,loadgen}.cpu.pb.gz` (the dead gw-1 is no longer profiled), `slo-death.json` and `loadgen-storm.json`. No process is left (`pgrep -f "$PWD/tier2-results/slowlogic" || echo none`). A bad quota is refused before anything starts: `GOIM_TEST_TIER=2 SLOWLOGIC_QUOTA=0% bash "$D/slowlogic.sh"` prints `slowlogic: SLOWLOGIC_QUOTA must be 1%-100%, got '0%'` and exits 1. Afterwards, remove `tier2-results/slowlogic-*` and `$D`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/slowlogic.sh Makefile
git commit -m "feat(tier2): slowlogic, Logic under a CPU quota: calibrated queue cap, Busy not EXPIRED in a kill storm (#113)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-race-suite && make test-scripts && make test-load-probe && bash -n scripts/tier2/slowlogic.sh`
Expected: all pass. Record the commits and the dry run in this plan's *Execution Notes* and push the branch.

---

### Task 3: Tier 2 run 29 and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row; #113), `README.md` (both languages)

- [ ] **Step 1: Tier 2 run 29 (on the host, ~15 min)**

On the Tier 2 host, after `git fetch && git switch feat/plan6b2b-slow`, with the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=slowlogic` and sends back the `.report.txt`. Expected: 17/17, or 16/17 with `storm_expired` failing as in the dry run (follow-up F1: a finding, not a harness bug) (`calibrate_stack`, `slow_scope`, `calibrate_saturated`, `calibrate_lease`, `calibrate_idle`, `calibrate_processes`, `storm_size`, `storm_stack`, `storm_online`, `storm_busy`, `storm_expired`, `storm_back`, `storm_lease`, `no_swap_in_storm`, `storm_idle`, `storm_processes`, `artifacts`).

What to read, for the *Execution Notes*, the roadmap and the README:
- **Calibration:** T (`calibrate.throughput_per_s`; ~3,000/s expected), Logic's cores in the window against the 0.25 quota, its `cpu_stat` (`nr_throttled` of `nr_periods`, `throttled_usec`), `slow_scope.logic_gomaxprocs` (expect 2), and the calibration's `busy` (large) against `calibrate_saturated.waste` (0 behind the small cap) and `logins_other`; `calibrate_lease` per gateway. `calibrate_lease` covers the ~16–19s of the calibration, so one renewal per gateway (`live_floor` 1); `storm_lease` is the longer check of R1-O2, on gw-0 alone (P6b2b-slowI-6).
- **Storm:** `storm_size` (`victims_per_t`, `clamped`); `storm.queue_cap` (4T), `victims`, `rate`; T' (`storm.busy_window.throughput_per_s`, its `logic_cores`, `cpu_stat`, `logins_other` and Kick counters) against T; `storm_busy.busy` against its floor; `storm_expired` (expired, expired_queued, ok); `storm_back.back_s`, `slo.from_mark` p50/p99/max (the recorded SLO), `backoff`; `storm_lease`; Logic's and gw-0's cores (`storm.usage`); `storm_idle.logouts` (dropped Logouts at the end).
- If a check fails: `calibrate_saturated` → `logic_cores` against the quota, and loadgen's `failed` (pre-handshake closes at 8,000/s), then raise `SLOWLOGIC_CAL_RATE` (or `SLOWLOGIC_CAL_QUEUE_CAP`, if Logic ran out of queued work); its `waste` above 1% → lower `SLOWLOGIC_CAL_QUEUE_CAP`; `calibrate_lease` or `storm_lease` → Review Focus 1; `storm_size` → T far above the estimate: lower `SLOWLOGIC_QUOTA` (smaller T), not the storm; `storm_busy` under its floor → arrivals did not outrun Logic (T overestimated or loadgen short: T' and Logic's cores in `busy_window`); `storm_expired` → whether the EXPIRED come from Logic (`logic_expired`) or the gateway queue, and T' against T. Below T, the 4T cap held more than 4s of the storm's work. At T' ≈ T, the rule's 1s margin is too small, a finding against R4H-O2, not a reason to lower the cap; `storm_back` → Review Focus 5.

- [ ] **Step 2: Record**

- This plan's *Execution Notes*: the run's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #113 gets one line with run 29's result. The plan 6 row: "6b2b-slow executed, run 29 …; 6b2b complete; next 6b3 (fresh session)".
- README *Measured results*, both languages, a row after the b2 row: **Logic slowed to a 25% CPU quota**: its measured login throughput T (and T' in the storm), the queue cap from the spec's rule (4T for the lone survivor), Busy count against EXPIRED (≈ 0), every client back in N s (p99 from the kill, no SLO gate), lease renewals missed: 0 (the 5s bound; latency not measured). In the status paragraph, move "slowed Logic" from *Next* to *Done*.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-15-connection-session-core-06b2b-slow-logic.md
git commit -m "docs: Tier 2 run 29, Logic slowed to a CPU quota recorded; README row"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b2b complete.** Next: Plan 6b3 (population events: whole-population reconnect, floods), in a fresh session.
- **Spec:** the calibration and the 30T sizing are how one host tests R4H-O2's rule. If run 29's `storm_expired` fails as the dry run did (T' ≈ 0.82 T: relogins that replace a dead gateway's session and Kick it cost more than fresh logins), R4H-O2's sizing rule needs a follow-up: measure throughput on relogins, or widen the margin. That decision is the user's, after run 29 (dispositions F1).

## Execution Notes

- 2026-10-16, executed inline on `feat/plan6b2b-slow` (from `feat/plan6b2b-b2` at 1ce22ea). Task 1: b6aa84c (both patches applied cleanly; RED as Step 2 expects, 4 failed; GREEN; the `dropped=0` mutation caught by 2 checks). Task 2: 4f91d2c (the scenario as given, 357 lines; Makefile comment).
- Dry run (dev machine, 5% quota, Step 2's settings, at b6aa84c + the uncommitted scenario): `exit 1`, 17 checks, 15 pass. Failures: `storm_expired` (as F1 expects) and `no_swap_in_storm` (1 page in, the dev machine). `slow_scope` cpu_max `5000 100000`, `logic_gomaxprocs` 2. `calibrate_saturated` T 345/s, Logic 0.050 of 0.05 cores, waste 0. `calibrate_lease` both gateways live 1. `storm_size` 10,350 victims ≥ 9,315, not clamped; cap 1,380 (4T). `storm_busy` 18,184 (floor 1,035). `storm_expired`: expired 545, expired_queued 0, ok 10,350, logic_expired 697; gw-0 `logins_error` 267; login latency p99 5.7s. T' (busy_window) 284/s = 82% of T, Logic at its quota, every storm login `replaced` with a Kick (5,331 in 20s). `storm_back` 47s, backoff 872 clients; `slo.from_mark` p50 18.5s, p99 34.0s, max 46.1s. `storm_lease` gw-0 live 4 in 46.4s, missing 0. No Logout dropped; no process left; `SLOWLOGIC_QUOTA=0%` refused.
- Whole branch: `make test && make test-race-suite && make test-scripts && make test-load-probe && bash -n scripts/tier2/slowlogic.sh` passed.
- Final review (opus): no Critical/Important. Minors, fixed on the user's request: `calibrate_lease`'s renewal floor was `lease_s // 10 - 1` = 0 over the ~16s window, so a gateway that stopped renewing passed; it is now also `(lease_s - 6) // 10` (a renewal ends ≤5s after its tick), 1 in calibration, recorded as `live_floor`. `storm_expired` records gw-0's `error` (Login timeouts, answered Busy; not gated). The third (a failed scrape making `result.json` invalid) did not hold: `lg_py` and `stack_counters_delta` print `null` on any failure. Dry run after the fix: 17 checks, the same two failing (`storm_expired`: expired 279, error 0; dev swap); `calibrate_lease` `live_floor` 1, both gateways live 1.
- **Interface review (2026-10-16, Claude; decided by the user, "apply all"):** P6b2b-slowI-1..6. **Checked, unchanged:** every metric and label slowlogic reads exists (Logic's `replaced` is apart from `ok`, so T' = ok + replaced holds; `kick_dropped_total`'s new `attempt` label is summed); loadgen's fields (`backoff_since_mark`, `backoff_clients`/`backoff_waits`, `online_by_gateway`, `login_latency`) exist; least-connections splits 2V clients V/V once all are online, so `storm_back`'s `population == victims` holds; the busy-window subshell's usage window leaves the storm's alone and reads the dead gw-1 as null; a failed scrape fails every gate it feeds, never passes it; `dropped-ok` changes nothing without the argument; an explicit `SLOWLOGIC_VICTIMS` bypasses the cap, as the header says.
  - **P6b2b-slowI-1 (fixed):** throttling was not recorded, though Review Focus 1 and Task 3 point to `nr_throttled`, and the scope is gone after the run. New `stack_cpu_stat NAME` (`usage_usec`, `nr_periods`, `nr_throttled`, `throttled_usec` of NAME's cgroup, null when unreadable); its delta is recorded as `cpu_stat` over the calibration window (`calibrate_saturated`, `calibrate`), the busy window (`storm.busy_window`) and the whole storm (`storm`), and the storm's stack records its own `cpu_max` (`slow_scope` checks the calibration's only). New tests `quota: Logic's cpu.stat` and `quota: no such process's cpu.stat`.
  - **P6b2b-slowI-2 (fixed):** the 11 number settings are refused before anything starts unless whole (`SLOWLOGIC_CAL_SKIP` may be 0, the rest positive): they land in arithmetic and in `result.json` unquoted (as gwloss's P6b2b-b2I-2). A failed `lg_start calibrate` is recorded as `calibrate_saturated` false with an error and ends the run (it was ignored: T=0, read as unsaturated).
  - **P6b2b-slowI-3 (fixed):** T's count spanned more than its `window_s` (two gateway scrapes, and Logic's own scrape under the quota, before the usage window opened), so T read a little high, and with it the 4T cap. The gateways are read first, then Logic's `cpu.stat` and counters right before the window opens; after it, Logic first.
  - **P6b2b-slowI-4 (fixed):** Logic's other login results (`superseded`, `stale`, `kicked`, `error`) burn the quota too but were not recorded. `LOGIC_COUNTERS` gains `logins_all`, and `logins_other` (all − ok − replaced − expired) is recorded in `calibrate_saturated`, `storm.busy_window` and `storm`. The 1% waste gate (O4) is unchanged.
  - **P6b2b-slowI-5 (fixed):** `storm_lease` used the old floor (`window // 10 − 1`). One helper, `lease_floor`, serves both lease checks; `storm_lease` records `live_floor`, and an unreadable window fails it.
  - **P6b2b-slowI-6 (noted):** `calibrate_lease` covers ~16–19s of saturation, one renewal per gateway; Task 3's reading guide says so, and `storm_lease` is the longer check. No `SLOWLOGIC_CAL_HOLD`.
  - `make test-scripts` and `bash -n` pass; `SLOWLOGIC_CAL_RATE=8k` and `SLOWLOGIC_RATE=0` are refused at once. Dry run (Step 2's settings, 5% quota): `exit 1`, 17 checks, failing only `storm_expired` (F1: expired 141, logic_expired 541, error 800) and `no_swap_in_storm` (dev swap). T 343/s, Logic 0.048 of 0.05 cores, `cpu_stat` throttled in 108 of 108 periods (17.9s throttled), `logins_other` 0; `calibrate_lease` `live_floor` 1, both live 1; storm cap 1,372 (4T), `storm_busy` 15,081 (floor 1,029); T' 318/s (93% of T), throttled 207 of 208 periods, `logins_other` 200 (6,361 replaced, 292 expired, 6,348 Kicks); storm `cpu_max` `5000 100000`; `storm_lease` `live_floor` 4, live 4 in 46.8s; back in 46s, 679 clients to backoff. No process left.
- **Tier 2 run 29** (2026-10-16, `04dc99a`, dirty 0): 16/17, only `storm_expired`, as F1 expected. T 3,550/s (Logic 0.250 of 0.25 cores, throttled in 105 of 105 periods, waste 0, GOMAXPROCS 2); `calibrate_lease` both gateways live 1. Storm: 106,500 victims (30T, not clamped), cap 14,200 (4T); T' 3,116/s (88% of T), every storm login `replaced` with a Kick (61,692 attempts, 636 dropped in the first 20s), `logins_other` 0. `storm_busy` 99,911 (floor 10,650). **`storm_expired` failed:** 3,701 EXPIRED (3.5%), all Logic's (reached Redis > 5s after recv_ms), `expired_queued` 0, `error` 0; login latency p90 4.48s, p99 5.07s, max 5.9s. All back in 45s (from the kill p50 17.7s, p99 35.8s, max 45.3s), 162 clients to backoff. `storm_lease` gw-0 live 4 (floor 3), missing 0, throttled 356 of 404 periods. No swap, no dropped Logout. The cap held 14,200 / 3,116 = 4.56s of work at T', leaving 0.44s of the 1s margin for Logic's own pipeline and the throttling.
- **Run 29b** (`04dc99a`, `SLOWLOGIC_QUEUE_CAP=12464` ≈ run 29's T' × 4, testing "measure T on relogins, 1s margin"): 16/17, `storm_expired` failed again: T 3,710, T' 2,814 (76% of T: T' moved 12 points between runs), so 12,464 held 4.43s; EXPIRED 2,775 (2.5%), all Logic's; latency max 5.75s; Busy 158,492; back in 53s, 998 clients to backoff.
- **Run 29c** (`04dc99a`, `SLOWLOGIC_QUEUE_CAP=9275`, 2.69 × its T): **17/17 passed**. T 3,443, T' 2,970 (86%), so 3.12s nominal; EXPIRED 0; login latency p90 3.03s, p99 3.96s, max 4.8s; Busy 128,001 (floor 10,329); all 103,290 back in 46s (from the kill p50 17.8s, p99 34.5s, max 45.4s), 256 clients to backoff; Kicks 99,129, 4,161 dropped; `storm_lease` gw-0 live 4 (floor 3); throttled 342 of 411 periods; no swap, no dropped Logout.
- **Reading:** in all three, the latency past the nominal wait (cap / T') was 1.3–1.7s (5.9 − 4.56, 5.75 − 4.43, 4.8 − 3.12), and T' was 76–88% of T. In runs 29 and 29b no login expired in the first 20s after the kill: all came later, while Busy retries kept the queue full; not explained (the dead epoch's sweep starts ≥ 60s after its last renewal, after the storm). Logic's storm CPU profile on the host covers kill to +30s.
- **Decision #115 (the user, 2026-10-16):** the sizing rule's margin is 3s, `cap ≤ T × (login_deadline − 3s)` = 2T at a 5s deadline, T measured on fresh logins (dispositions F2). At the worst seen (T' 0.76T, tail 1.7s): 2.6s + 1.7s ≈ 4.3s. `slowlogic`'s default cap is now T × 2 (`SLOWLOGIC_QUEUE_CAP`); 27T victims still reach the Busy floor (2T needs ~24.4T). No run at the 2T default itself: it is below the passing 2.69T, with more Busy and so more clients to backoff than run 29c.
- **Run 29d** (2026-10-16, `04dc99a`: `main` not yet at `1758ddc`, so the old 4T default, not #115's 2T): **17/17 passed**. T 2,696/s (low: 3,443–3,710 in runs 29–29c), so the 4T cap was 10,784; T' 2,804/s (104% of T), 3.85s nominal; EXPIRED 0; login latency p90 3.30s, p99 4.65s, max 5.02s (tail 1.17s); Busy 34,432 (floor 8,088), 11 clients to backoff; all 80,880 back in 28s (from the kill p99 27.6s: the 30s SLO met); `storm_lease` gw-0 live 3 (floor 2); throttled 274 of 283 periods. Logic's CPU clock in the storm averaged 2,440 MHz against 1,613–1,673 in runs 29–29c: at 0.25 cores its CPUs are mostly idle and the governor clocks them differently between phases and runs, so T varies by ~38% and T'/T (76–104%) mixes frequency with the relogin and Kick cost. The clocks are recorded for the storm only, not the calibration window. #115 stands: what kept the latency under 5s in all four runs is cap / T' plus a 1.2–1.7s tail, and the 3s margin also absorbs a noisy T. Next: a run at `1758ddc` (2T default).
- **Run 29e** (2026-10-16, `da54bd3`, dirty 0, #115's 2T default): **17/17 passed**. T 3,503/s, cap 7,006 (2T); T' 2,717/s (78% of T), so 2.58s nominal; EXPIRED 0; login latency p90 2.39s, p99 3.10s, max 3.66s (tail 1.08s, 1.34s under the deadline); Busy 160,504 (floor 10,509), 910 clients to backoff (0.9%); all 105,090 back in 51s (from the kill p50 19.6s, p99 35.8s, max 51.1s; the SLO recorded, not gated); `storm_lease` gw-0 live 5 (floor 4), missing 0, Logic throttled in 355 of 458 periods; no swap, no dropped Logout; Logic's storm clock 1,788 MHz. The rule's default holds with ~1.3s to spare; the price of the smaller cap is more Busy and backoff, not a slower drain (back in 51s, as 29c's 46s and 29b's 53s).
- **#115 on hold (the user, 2026-10-16):** the 3s margin is not acceptable; the spec keeps 1s (4T), and the system must meet it. Diagnostic added (dispositions F2): gateway flags `-login-rpcs`/`-login-batch` (new test `TestLoginWindowFlags`), Logic histograms `login_age_seconds` and `login_prepare_seconds` (new test `TestLoginObservesEntryAgeAndPrepare`, RED on count 0), and in `slowlogic` the knobs `SLOWLOGIC_LOGIN_RPCS`/`SLOWLOGIC_LOGIN_BATCH` (storm only), `login_age` (p50/p99/max, bucket bounds) and `prepare_ms` in the calibration, the busy window and the storm, the calibration window's `cpu_clocks`; the storm's default cap back to 4T. `make test`, `go vet`, `make test-scripts` pass. Dev dry run (5% quota, `SLOWLOGIC_LOGIN_RPCS=4`): 17 checks, failing `storm_expired` (484 Logic EXPIRED of 10,710) and `no_swap_in_storm` (dev swap); `-login-rpcs 4` reached the gateways; calibration `login_age` p99 1.0s, `prepare_ms` 28.9; storm `login_age` p50 3.5s, p99 6s, `prepare_ms` 227 (743 in the busy window); `cpu_clocks` null (the dev VM has no cpufreq). The dev machine shares 2 CPUs among everything, so it says nothing about the host. Next: host runs at 4T, `SLOWLOGIC_LOGIN_RPCS` unset (16) and 4.
- **Runs 29f/29g** (2026-10-16, `841a8c3`, dirty 0, 4T cap): both 16/17, `storm_expired` failing. **29f** (`SLOWLOGIC_LOGIN_RPCS` unset, 16): T 3,097/s, cap 12,388; busy-window T' 3,109/s (100% of T), so 3.98s nominal; EXPIRED 3,103 + 310 unknown (Logic 3,413, 3.7% of 92,910); storm `login_age` p50 3.5s, p99/max in the 5–6s bucket; login latency p90 3.78s, p99 4.99s, max 5.50s; `prepare_ms` 64 → 114; all back in 39s. **29g** (`-login-rpcs 4`): T 3,578/s, cap 14,312; T' 2,461/s (69% of T), so 5.82s nominal; EXPIRED 19,170 + 1,861 `expired_queued` (19.6%); `login_age` p50 5.0s; back in 61s. The in-flight-window hypothesis is refuted: the gateway's `loginQueued` counts a Login from admission to its reply (`loginFinished` in `resolve`), so the window is inside the cap, not added to it, and by Little's law a Login's wait is cap / T' however the cap splits between queue and Logic; 4 RPCs did not shorten the wait and T' fell. What sets the latency is T' against the T the cap was sized from: T'/T was 0.69–1.04 over runs 29–29g (Logic's storm CPU clock 1,656–1,749 MHz against 2,968–3,040 in the calibration, but 29f kept T' = T at the lower clock, so the clock alone does not predict it), and even at T' = T the tail ran ~1.5s past nominal. A cap fixed from one calibration therefore cannot hold a 1s margin on this host; (B) smaller store pieces does not change cap / T' either.
- **Tail diagnostic** (2026-10-16, after 29f/29g): even at T' = T (29f) logins ran ~1–1.5s past cap / T', and the calibration (no Kicks, high clock, cap below the in-flight window) showed `login_age` p99 1.5s, max 2.0s against 0.65s nominal, so the tail is in the gateway → Logic path. Hypotheses: Logic serves its up to 3,200 in-flight Logins in no order (16 batches on 2 Ps throttled to 25%, plus GC), or T' dips within the storm. Added: gateway histograms `login_queue_wait_seconds` (RPC send − recv_ms, per entry sent) and `login_logic_wait_seconds` (definitive reply − send; new test `TestLoginObservesQueueAndLogicWait`, RED without the change); Logic's `login_age_seconds` buckets now every 0.25s to 7s; in `slowlogic`, `gw0_wait` (`queue`/`logic`, p50/p90/p99/max) in the calibration and the storm, `login_age` gains p90, and the storm's `logins_per_s` (gw-0's replies per second and its `login_queue`, sampled each second from the kill until all are back; `artifacts/logins-gw-0.tsv`). `make test`, `go vet`, `make test-scripts` pass; the new shell helpers checked on fake data. No dev dry run: the dev VM (2 cores, 1GB) is refused by the host guard. Next: a host run at 4T, `SLOWLOGIC_LOGIN_RPCS` unset.
- **Run 29h** (2026-10-16, `0477088`, dirty 0, 4T, 16 Login RPCs): 16/17, `storm_expired` failing (2,733 Logic EXPIRED, 2.7%; none queued). T 3,379/s, cap 13,516; busy-window T' 3,031/s (90%); the per-second series with the queue at the cap (s 9–30) averaged 2,888/s (85% of T), sd 706, one-second dips to 1,180–1,761 each followed by a catch-up second (4,134 after 1,180); the lowest 5s mean 2,440/s (0.85 of the span's mean, 0.72 T, s 21–26), where cap / rate = 5.5s, the run's max. gw-0's split in the storm: `queue` p50 3.5s, p90 4.0, p99 4.25, max 4.5; `logic` p50 1.25s, p90 1.5, p99 1.75, max 2.0 (≈ the 3,200 in flight / ~2,900/s, so Logic's unordered service adds ~0.5s spread at most); calibration `queue` ≤ 0.5s, `logic` p50 0.5s, p99 1.5s (2,000 in flight / 3,379/s = 0.59s nominal). Finding: the ~1.5s "tail" of runs 29–29g was mostly the rate measured over a 20s window; against a 5s rolling rate the wait is cap / rate within ~0.5s. What exceeds the deadline is Logic's pace over a few seconds: its mean T'/T (0.69–1.04 across runs) and seconds-long dips of ~15% below that mean.
- **#115 decided again (the user, 2026-10-16, after 29h):** the 3s margin; applied to rev6 in place (*Sizing* *(R4H-O2, #115)*, English and Chinese), `slowlogic`'s default cap back to T × 2, README row amended; option D parked as roadmap #116. The diagnostic metrics and knobs stay. Run 29e (`da54bd3`, 2T default) is 6b2b-slow's passing run.
- **Logic's pace (2026-10-16, after #115):** run 29h's storm CPU profile of Logic: token verification 77% (Ed25519 70%), Kicks 10%, GC negligible; options (a verified-token cache, cheaper Kicks) recorded and parked by the user as roadmap #117.
- **Review of the tail diagnostic `0477088` (2026-10-16, Claude, on `feat/plan6b3`):** no Critical/Important. Checked, unchanged: the gateway's queue wait is observed once per entry sent; its Logic wait only for a definitive reply (a timeout, a failed RPC or a stream that ended are not), Logic's EXPIRED included; the RPC's send time is set before the RPC and read after it in the same goroutine; buckets 0.25–7s cover the 5s deadline and the 6s per-entry timeout; `t2_on_exit` appends, so the sampler's hook keeps the others. Minors, fixed on the user's request: `login_sampler` counted every `logins_total` result but `busy`, so `expired_queued` (never reached Logic) and `error` (a timeout, counted when it fires) would have inflated "Logic's pace"; it now leaves both out (run 29h, `slowlogic-20261016-093106`, had none queued and gw-0 `error` 0, so its series stands); five comments still said "#115 on hold". Left as is: Logic's `login_age_seconds` lost its 0.1s bucket, so an unsaturated p50 reads ≤250ms.
- 6b2b-slow complete; with run 28, 6b2b complete.
