# Plan 6b3b: The `floods` Scenario Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Tier 2 scenario `floods` that runs the spec's handshake-flood load test as two tests (#121) and its slow-client flood test, each in its own phase on a fresh stack. Then Tier 2 run 30 on the host.

**Architecture:** One new script, `scripts/tier2/floods.sh`, built on 6b3a's loadgen instances and flood modes. It follows `slowlogic`'s shape: `up`/`down` per phase, `stack_counters` deltas per gateway, `t2_record_start NAME` windows. It has three phases. `storm` ramps light clients at 20,000/s onto one gateway, with no flood. `garbage` runs a light population that Pings every 30s, beside instance `flood` sending garbage handshakes above both gateways' buckets. `slow` runs instances `flood` (slow-handshake) and `flood2` (slow-auth), with a light population logging in beside them. Each flood instance has its own `-source-net`. The scenario registers `t2_on_exit 'lg_stop_all'`. Every gateway number is read per gateway from the gateways' own counters. `slowlogic`'s `lease_floor` moves to `lib.sh` as `t2_lease_floor`, so both scenarios use it.

**Tech Stack:** bash 5.2, curl, python3 (reads JSON, arithmetic), `ss`, the pinned `redis-server` 7.4.11. No Go change.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Verification → Load tests*: "handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket"; "slow-client flood: slots are freed by the timeouts and legitimate clients get no `Busy`".
- *Storms, drain, failures*: "Handshake flood | The global handshake concurrency limit and token bucket protect established connections; excess handshakes are closed and counted"; "Slow clients | `HANDSHAKE` / `AUTH_PENDING` 5s timeouts free their slots".
- *Gateway → Admission control*: the Noise handshake limit is "a concurrency limit on handshake computations running at once (a semaphore sized to the cores dedicated to handshakes) plus a gateway-wide token bucket on handshakes/s … The bucket must allow at least the 20k/s storm peak with margin; in a legitimate storm it shapes load rather than rejecting it. When either is exhausted the gateway does not start the handshake: it closes the connection and counts a `handshake_rate` reject." The memory-only pending cap "must not trigger in a normal storm".
- *Client SDK rules*: a pre-handshake close is retried after a uniform 0.5–1.5s. The Pong timeout is one heartbeat interval + 10s.

Inputs: Plan 6b3a (`2026-10-16-connection-session-core-06b3a-loadgen-floods.md`): its *Review Focus* 2, 3 and 5, its *After this plan*, and the final reviewer's notes and P6b3aI-6/7 in its *Execution Notes*.

**Decisions:** #118 (the user, 2026-10-16) makes this plan scenario `floods` and Tier 2 run 30. #121 (decided 2026-10-16) splits the handshake-flood bullet into two tests. **Proposed with this plan, for review:** #122 (the phases, their sizes and their gates), under *Implementation choices* below.

**Branch:** `feat/plan6b3`. Execute from its head, `cc7c636` or later.

**Size:** 3 tasks, about 440 lines changed (one new script of 415 lines). The prototype `proto-6b3b` (from `cc7c636`: `f1ce460`, then `c99b8bc` after its own dry run) ran every block. Its code is replayed here byte for byte: `floods.sh` in full, every other change as a `git apply` patch against `cc7c636`. On the dev machine the prototype passed `make test-scripts`, and the `floods` dry run (Task 2, Step 3) ran all three phases at small scale.

## Global Constraints

- No change outside `scripts/tier2/` and the `Makefile` comment. The gateway, SDK, Logic and loadgen are untouched.
- `make test-scripts` stays green on the dev machine (2 cores, ~1GB, no Docker). The Tier 2 guard refuses the dev machine, so the scenario is checked there in a throwaway copy with the guard lowered, as `slowlogic` was.
- Scripts use bash with `set -uo pipefail`. JSON is written without jq. python3 only reads JSON and does arithmetic. Every number setting is checked before anything starts (as P6b2b-slowI-2).
- Ports: `STACK_PORT_BASE` 17000. Loadgen instances `main`, `flood`, `flood2` take control ports base + 5, + 6, + 7 (#120).
- Sources: the light instance uses `-source-net 0` (the default). `flood` uses 1 and `flood2` uses 2. A flood without one is refused by `cmd/loadgen` (P6b3aI-3).
- Spec numbers the gates use: `HandshakeTimeout` and `AuthTimeout` are 5s. The timer wheel ticks every 1s and rounds deadlines up. The handshake bucket is 25,000/s with a burst of 2,500. Concurrency is half the loops, and the loops follow `STACK_GATEWAY_PROCS` = gateway CPUs ÷ gateways. That is 12 CPUs on the host, so 6 slots for one gateway and 3 each for two. The pending cap is 100,000 and the connection cap 250,000 per gateway. `-heartbeat` is 30–300s. The SDK's Pong timeout is heartbeat + 10s.
- Tier 2 host: 26GB RAM (the UMA change is on hold), loadgen CPUs `0-3,12-15`, gateways `4-9,16-21`, Logic and Redis `10,11,22,23`.
- Comments and names match the surrounding code. New behaviour cites #121 or #122.

## Review Focus

1. **The concurrency limit refuses legitimate handshakes.** The limiter's semaphore has no wait queue: a handshake that finds every slot busy is closed (`no_slot`, counted as `handshake_rate`), and the client retries after 0.5–1.5s. That is an Erlang loss. In `storm`, one gateway has 6 slots, and at 20,000/s × ~100µs that is 2 Erlangs, so ~1.2% are refused. In `slow`, each gateway has 3 slots at ~8.6k handshakes/s (slow-auth's redials plus the light ramp): ~0.9 Erlang, ~4.5%. The dev dry run, with 1 slot, refused 33–38% at 1,000/s and 6% at 200/s. Expected: every client still logs in (light retries up to 10 times), and `storm_bucket`/`slow_legit` gate only the **bucket** (`no_token` 0), which is the spec's clause. `no_slot`, its share (`storm_bucket.no_slot_share`) and loadgen's `failed["pre-handshake"]` are recorded, not gated (#122 (2)). If run 30's share is large, the spec's "shapes load rather than rejecting it" does not hold for the semaphore. Whether to size it differently or let a handshake wait briefly for a slot is the user's decision after the run, not this plan's.
2. **Loadgen as the bound.** `storm` needs ~20,000 X25519 handshakes/s from loadgen's 8 CPUs, and `garbage` needs 80,000 TCP connects per second with no crypto. If loadgen falls short, the gateway sees less than the test claims. Expected: `storm_rate` (the gateway's completed handshakes ≥ 0.9 × rate, between 5% and 95% of the clients) and `garbage_delivered` (the gateways' `failed + no_slot + no_token` ≥ 0.95 × rate × seconds, 6b3a Review Focus 2) fail rather than pass on a trickle. On a failure, read `usage.loadgen`/`usage.flood` cores and `garbage/loadgen-flood.cpu.pb.gz`. Lower `FLOODS_GARBAGE_RATE` only while `garbage_limited` still passes.
3. **Least connections splits the garbage unevenly.** A refused attempt closes sooner than one that runs a DH, so its gateway has fewer open connections and gets more of the flood. The dry run split it 65/35 (gw-0 refused 34,737, gw-1 5,389, each computing at its bucket's rate). At 60,000/s that split would leave one gateway at ~21,000/s, under its 25,000/s bucket. Expected: the default of 80,000/s keeps the smaller share at ~28,000/s, so `garbage_limited`, which is gated per gateway, passes. If it fails for one gateway while the other refused, raise `FLOODS_GARBAGE_RATE`.
4. **TIME_WAIT on the gateway's side.** The gateway closes every garbage attempt first (6b3a), so the gateway holds one TIME_WAIT per attempt for 60s. At 80,000/s that is ~4.8M, far past `net.ipv4.tcp_max_tw_buckets`, so the kernel drops the excess (`TCPTimeWaitOverflow`). Expected: harmless. The flood's outcomes stay `closed` only, with no `dial-timeout` or `dial-no-port`, and `ListenOverflows` stays 0. All of it is recorded in `garbage_delivered` (`timewait` before and after, `tcp` deltas, `outcomes`), not gated. The dev dry run hit the dev machine's 8,192 buckets with 65k overflows and every outcome `closed`.
5. **"On time" for a heartbeat.** The light client's Pong RTT is one distribution over its whole run. It covers its ramp as well as the flood, so a phase has no clean baseline. Expected: `<phase>_heartbeats` gates what the spec makes a failure. No Pong is missing (a Ping was due while the last one was unanswered). Every client was Pinged at least once. The slowest Pong is under 10s, the SDK's Pong timeout above the heartbeat. No connection was lost, and no gateway closed one for its heartbeat. p50/p99/max are recorded, for the README. The 6b2a-pinger ruling (an SDK Ping write with no deadline) does not apply: these are light clients, and the gateways keep reading them.

## Implementation choices this plan makes

1. **#122 (proposed): three phases, each on a fresh stack.** `FLOODS_PHASES` (default `storm garbage slow`) picks them, as `GWLOSS_PHASES` does, so one phase can be rerun alone. A phase's counters, swap window and loadgen results are its own, and no flood's TIME_WAIT or half-open state reaches the next phase's numbers.
2. **#122 (proposed): `storm` is #121 (2) on one gateway, with the bucket gated and the semaphore recorded.** 200,000 light clients ramp at 20,000/s onto gw-0, so 10s at the spec's peak. That stays under the 250,000 connection cap. The gateway runs its default limiter with 6 slots. The bucket is per gateway, so with two gateways each would see only 10,000/s, and the test would not test the bucket. `storm_bucket` passes on `no_token` 0. `no_slot` is recorded with its share (Review Focus 1). *Alternative:* gate `no_slot` 0 too. That turns the semaphore's Erlang loss into a failure of this plan, when it is a question for the spec.
3. **#122 (proposed): `garbage` is #121 (1) at 80,000/s for 90s beside 400,000 light clients.** That is the operating point, 200,000 per gateway. The clients Ping every 30s, the gateway's lowest heartbeat, so each client Pings three times during the flood. The gates are: loadgen kept the schedule and the gateways saw it (`garbage_delivered`), each gateway's limiter refused part of it (`garbage_limited`, so the flood ran past the limit), heartbeats (Review Focus 5), and each gateway renewed its lease through the flood (`garbage_lease`, `t2_lease_floor`, as `slowlogic`). The gateways' and the flood's CPU are profiled from 10s to 40s into the flood. `FLOODS_GARBAGE_BUCKET` lowers `-handshake-rate` for this phase only, so the dev dry run's small flood reaches the limiter. On the host it stays unset.
4. **#122 (proposed): `slow` holds 80,000 slow-handshake and 40,000 slow-auth connections while 400,000 light clients log in.** That is 60,000 pending per gateway, under the 100,000 pending cap, so the light clients find slots. Slow-auth's redials cost ~7,300 handshakes/s (40,000 every ~5.5s). With the light ramp's 10,000/s, that is ~8,600/s per gateway, a third of the bucket (6b3a Review Focus 5). The light clients start only once both floods hold ≥ 85% of their connections and have had every slot freed once (`slow_flood`). The phase then holds 60s. The gates are: the holds by the timeouts, with the probe's bounds (p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, 6b3a O7), the gateways' `handshake_timeout`/`auth_timeout` counts (`slow_freed`), the legitimate clients (`slow_legit`: all online, no Busy, no `login_queue`/`pending_mem`/`at_capacity` reject, no `no_token`; `no_slot` recorded), heartbeats and leases. `-max-conns` is raised to fit the floods and the clients.
5. **#122 (proposed): "answered on time" means under the Pong timeout,** gated as Review Focus 5 says, with the RTT quantiles recorded. *Alternative:* a baseline window before each flood. That needs a second light instance per phase, because a run's `pong_rtt` is cumulative.
6. **`t2_lease_floor` in `lib.sh`.** `slowlogic`'s `lease_floor` (P6b2b-slowI-5) moves unchanged, with a `lib_test` case, because `floods` needs the same floor.

## File Structure

- Modify `scripts/tier2/lib.sh` (`t2_lease_floor`), `scripts/tier2/lib_test.sh` (its test), `scripts/tier2/slowlogic.sh` (uses it) (Task 1).
- Create `scripts/tier2/floods.sh` (mode 0755). Modify the `Makefile`'s `test-tier2` comment (Task 2).
- Modify this plan's *Execution Notes*, the roadmap (plan 6 row, #122) and `README.md` (both languages) (Task 3).

---

### Task 1: `t2_lease_floor` in `lib.sh`

**Files:**
- Modify: `scripts/tier2/lib.sh` (after `t2_tcpstat_json`)
- Modify: `scripts/tier2/slowlogic.sh:180-187` (removed), `:364`, `:457`
- Test: `scripts/tier2/lib_test.sh`

**Interfaces:**
- Produces: `t2_lease_floor SECONDS`, which prints the fewest lease renewals a whole-second window must hold: `max(SECONDS // 10 - 1, (SECONDS - 6) // 10)`, using bash integer division. Task 2's `lease` uses it.

- [ ] **Step 1: Write the failing test**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/lib_test.sh b/scripts/tier2/lib_test.sh
index 46b2e7a..d46ef2b 100755
--- a/scripts/tier2/lib_test.sh
+++ b/scripts/tier2/lib_test.sh
@@ -277,6 +277,10 @@ expect "tcpstat: deltas of the named counters" "$(T2_TCPSTAT_NAMES="TCPSynRetran
   '{"TCPSynRetrans": 250, "PAWSActive": 4, "ListenOverflows": 0, "AttemptFails": 15, "EstabResets": 0, "NoSuchCounter": null}'
 expect "tcpstat: a missing snapshot is null" "$(t2_tcpstat_json "$tmp/none" "$tmp/tcp1")" null
 
+# The lease renewals a window must hold (P6b2b-slowI-5): one call can end
+# up to 5s after its tick, and a short window still needs its one renewal.
+expect "lease floor: 16s, 53s, 66s, 5s" "$(t2_lease_floor 16) $(t2_lease_floor 53) $(t2_lease_floor 66) $(t2_lease_floor 5)" "1 4 6 0"
+
 # make test-tier2 shows the script's own exit code (make itself exits 2 on any failure).
 out=$(GOIM_TEST_TIER= make -s test-tier2 SCENARIO=race 2>&1)
 contains "make: shows the scenario's exit code" "$out" "test-tier2: race exited 2"
PATCH
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `scripts/tier2/lib_test.sh 2>&1 | grep -E "lease|lib_test:"`
Expected: four `t2_lease_floor: command not found` lines, then `FAIL lease floor: 16s, 53s, 66s, 5s: got [   ], want [1 4 6 0]` and `lib_test: 1 failed`.

- [ ] **Step 3: Move `lease_floor` to `lib.sh` as `t2_lease_floor`**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index 153d167..f5b961d 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -428,6 +428,16 @@ t2_tcpstat_json() {
           print "{" out "}" }' "$1" "$2"
 }
 
+# t2_lease_floor SECONDS: the fewest lease renewals a gateway must make in a
+# window of SECONDS (whole). A renewal counts when its call ends, at most 5s
+# (the call timeout) after its 10s tick, so (SECONDS - 6) // 10 of them (1s
+# for SECONDS' resolution), and never fewer than SECONDS // 10 - 1
+# (P6b2b-slowI-5; shared with floods, Plan 6b3b).
+t2_lease_floor() {
+  local s=$1
+  echo $((s / 10 - 1 > (s - 6) / 10 ? s / 10 - 1 : (s - 6) / 10))
+}
+
 t2_swap_pages() { awk '/^pswpin / { i = $2 } /^pswpout / { o = $2 } END { print i + 0, o + 0 }' /proc/vmstat; }
 
 # t2_clock_sampler FILE: appends "unix_s cpu khz" for every CPU every 5s.
diff --git a/scripts/tier2/slowlogic.sh b/scripts/tier2/slowlogic.sh
index 6f81bf7..a758397 100755
--- a/scripts/tier2/slowlogic.sh
+++ b/scripts/tier2/slowlogic.sh
@@ -177,15 +177,6 @@ LOGIC_COUNTERS=(
   kick_dropped='goim_logic_kick_dropped_total'
 )
 
-# lease_floor SECONDS: the fewest renewals a window of SECONDS (whole) must
-# hold. A renewal counts when its call ends, at most 5s (the call timeout)
-# after its 10s tick, so (SECONDS - 6) // 10 of them (1s for SECONDS'
-# resolution), and never fewer than SECONDS // 10 - 1 (P6b2b-slowI-5).
-lease_floor() {
-  local s=$1
-  echo $((s / 10 - 1 > (s - 6) / 10 ? s / 10 - 1 : (s - 6) / 10))
-}
-
 # The other Logic login results (superseded, stale, kicked, error...) in a
 # stack_counters_delta of LOGIC_COUNTERS: Logic work that is neither T's nor
 # T''s (P6b2b-slowI-4).
@@ -361,7 +352,7 @@ t2_field calibrate "{\"clients\": $cal_clients, \"rate\": $cal_rate, \"queue_cap
 lease1=$(stack_counters logic "${LOGIC_COUNTERS[@]}") lease_s=$((SECONDS - lease_t))
 for g in gw-0 gw-1; do lg1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
 # 1 over the ~16s calibration, where lease_s // 10 - 1 alone would be 0.
-lease_floor=$(lease_floor "$lease_s")
+lease_floor=$(t2_lease_floor "$lease_s")
 lease_d="{\"gw-0\": $(stack_counters_delta "${lg0[gw-0]}" "${lg1[gw-0]}"), \"gw-1\": $(stack_counters_delta "${lg0[gw-1]}" "${lg1[gw-1]}"), \"logic\": $(stack_counters_delta "$lease0" "$lease1")}"
 t2_check calibrate_lease "$(echo "$lease_d" | lg_true - 'all(d[g]["lease_error"] == 0 and d[g]["lease_missing"] == 0 and d[g]["lease_live"] >= '"$lease_floor"' for g in ("gw-0", "gw-1")) and d["logic"]["lease_error"] == 0')" \
   "\"elapsed_s\": $lease_s, \"live_floor\": $lease_floor, \"gateways\": $(echo "$lease_d" | lg_py - '{g: {"live": d[g]["lease_live"], "missing": d[g]["lease_missing"], "error": d[g]["lease_error"]} for g in ("gw-0", "gw-1")}'), \"logic_lease_error\": $(echo "$lease_d" | lg_py - 'd["logic"]["lease_error"]')"
@@ -454,7 +445,7 @@ gw_d=$(stack_counters_delta "$s0" "$s1")
 window_s=$(echo "$usage" | lg_py - 'd["window_s"]')
 slo=$STACK_DIR/artifacts/slo-death.json
 storm_floor=null # an unreadable window fails storm_lease
-[[ $window_s =~ ^[0-9]+(\.[0-9]+)?$ ]] && storm_floor=$(lease_floor "${window_s%.*}")
+[[ $window_s =~ ^[0-9]+(\.[0-9]+)?$ ]] && storm_floor=$(t2_lease_floor "${window_s%.*}")
 
 t2_check storm_busy "$(echo "$gw_d" | lg_true - '(d["logins_busy"] or 0) >= 0.1 * '"$victims")" \
   "\"busy\": $(echo "$gw_d" | lg_py - 'd["logins_busy"]'), \"floor\": $((victims / 10)), \"rejects_login_queue\": $(echo "$gw_d" | lg_py - 'd["rejects_login_queue"]'), \"queue_cap\": $qcap"
PATCH
```

- [ ] **Step 4: Run the tests to make sure they pass**

Run: `scripts/tier2/lib_test.sh 2>&1 | grep -E "lease|lib_test:" && bash -n scripts/tier2/slowlogic.sh && ! grep -n '[^_]lease_floor "' scripts/tier2/slowlogic.sh`
Expected: `ok   lease floor: 16s, 53s, 66s, 5s`, `lib_test: all passed`. The final grep finds no call to the old name.

- [ ] **Step 5: Commit**

```bash
git add scripts/tier2/lib.sh scripts/tier2/lib_test.sh scripts/tier2/slowlogic.sh
git commit -m "refactor(tier2): lease_floor into lib.sh as t2_lease_floor, for floods (Plan 6b3b)"
```

---

### Task 2: The `floods` scenario (#121, #122)

**Files:**
- Create: `scripts/tier2/floods.sh` (mode 0755)
- Modify: `Makefile:74` (the `test-tier2` comment)

**Interfaces:**
- Consumes: Task 1's `t2_lease_floor`. From `stack.sh`: `stack_up`, `stack_down`, `stack_admins`, `stack_counters`, `stack_counters_delta`, `stack_metric`, `stack_snapshot`, `stack_profile`, `stack_usage_start`/`_json`, `stack_wait_idle`, `stack_health`, `stack_busy_ports`. From `loadgen.sh`: `lg_start`, `lg_in`, `lg_wait`, `lg_wait_online`, `lg_get`, `lg_json`, `lg_py`, `lg_true`, `lg_profile`, `lg_stop`, `lg_stop_all`, `LG_PIDS`. From `lib.sh`: `t2_*`, including `t2_tcpstat_snap`/`_json` and `t2_record_start/end NAME`. Gateway metrics: `goim_gateway_handshakes_total{result="ok|failed|no_slot|no_token"}`, `goim_gateway_admission_rejects_total{reason=...}` (every reason exported at 0), `goim_gateway_logins_total{result}`, `goim_gateway_closes_total{reason="heartbeat"}`, `goim_gateway_lease_calls_total{result}`. Logic's `goim_logic_lease_renewals_total{result="error"}`. Loadgen's result: `flood.{attempts, skipped, open, outcomes, held}`, `pongs`, `pong_missing`, `pong_rtt`, `lost`, `failed`, `login_latency`.
- Produces: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods`, with 27 checks on a full run, and result fields `storm`, `garbage` and `slow` (see the script's header).

- [ ] **Step 1: Create the scenario**

`scripts/tier2/floods.sh`:

```bash
#!/usr/bin/env bash
# Tier 2: floods (Plan 6b3b), the spec's load tests "handshake flood" (two
# tests, #121) and "slow-client flood", each phase on a fresh stack:
#   storm: a legitimate 20k/s storm on one gateway, no flood: 200k light
#       clients ramped at 20,000/s against the gateway's default handshake
#       limit (25,000/s bucket, burst 2,500, half the loops computing): the
#       bucket shapes, it does not reject (#121 (2)). One gateway, since the
#       bucket is per gateway and the spec's 20k/s is a storm's peak.
#   garbage: 2 x 200k light clients (the operating point per gateway) Ping
#       every 30s, then loadgen instance flood sends garbage handshakes (a
#       valid header and key_id, a random Noise message: each costs the
#       gateway a DH) at 80,000/s for 90s from 253 sources, above both
#       gateways' buckets (2 x 25,000/s) even when least-connections splits
#       it 65/35 (the dry run's split: a refused attempt closes sooner, so
#       its gateway gets more), so each limiter refuses part of it:
#       every Ping is still answered on time and the gateways renew their
#       leases (#121 (1)).
#   slow: instances flood and flood2 hold 80k slow-handshake and 40k
#       slow-auth connections (sends nothing; handshakes, then sends
#       nothing), freed by the gateways' 5s timeouts and redialed, while
#       400k light clients log in beside them and Ping: no Busy, no reject,
#       no bucket refusal (slow-auth's ~7k handshakes/s fit the buckets,
#       6b3a Review Focus 5).
# Logic is one process (minReplicas, no HPA), unthrottled.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods
#   Results: tier2-results/floods-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/floods-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 12 minutes.
#
# Settings (every number whole and positive, refused otherwise before
# anything starts):
#   FLOODS_PHASES          phases to run, in order (default "storm garbage slow")
#   FLOODS_STORM_CLIENTS   storm: light clients (default 200000: 10s at the rate)
#   FLOODS_STORM_RATE      storm: clients started per second (default 20000)
#   FLOODS_CLIENTS         garbage, slow: light clients over both gateways (default 400000)
#   FLOODS_RATE            garbage, slow: their clients started per second (default 10000)
#   FLOODS_HEARTBEAT       garbage, slow: the gateways' -heartbeat and the light
#                          clients' Ping interval, seconds (default 30, the
#                          lowest the gateway takes: most Pings per flood)
#   FLOODS_GARBAGE_RATE    garbage attempts per second (default 80000)
#   FLOODS_GARBAGE_INFLIGHT  garbage attempts in flight at most (default 20000)
#   FLOODS_GARBAGE_SECONDS the garbage flood's length (default 90: 3 Pings each;
#                          at least 45, so the profile from 10s to 40s ends inside it)
#   FLOODS_GARBAGE_BUCKET  garbage: each gateway's -handshake-rate (default: the
#                          gateway's 25,000/s; a dry run lowers it so its small
#                          flood reaches the limiter)
#   FLOODS_SLOW_HANDSHAKE  slow: slow-handshake connections (default 80000)
#   FLOODS_SLOW_AUTH       slow: slow-auth connections (default 40000)
#   FLOODS_SLOW_RATE       slow: each flood's connections started per second (default 10000)
#   FLOODS_SLOW_HOLD       slow: seconds held once the light clients are online (default 60)
#
# Checks (per phase run):
#   storm_stack           redis, devtoken, Logic and the gateway become Ready
#   storm_online          every client logs in, none gives up
#   storm_rate            the gateway completed handshakes at >= 0.9 x the
#                         rate, between 5% and 95% of the clients (so the
#                         bucket saw the storm, not a loadgen-bound trickle);
#                         its per-second series is recorded
#   storm_bucket          the bucket refused nothing (no_token 0, #121);
#                         the concurrency limit's refusals (no_slot, each a
#                         pre-handshake close the client retries after
#                         0.5-1.5s), their share of the handshakes and
#                         Busy are recorded
#   no_swap_in_storm      no page swapped in from the ramp to the last login
#   garbage_stack, slow_stack   as storm_stack, two gateways
#   garbage_online        every light client logs in, each gateway holds 45-55%
#   garbage_delivered     loadgen kept the schedule (attempts >= 0.95 x rate x
#                         seconds) and the gateways saw it: failed + no_slot +
#                         no_token >= 0.95 x rate x seconds (6b3a Review
#                         Focus 2); outcomes, TIME_WAIT and kernel TCP
#                         counters are recorded (Review Focus 3)
#   garbage_limited       each gateway's limiter refused part of the flood
#                         (no_slot + no_token > 0): it ran past the bucket
#   garbage_heartbeats    every Ping answered (no Pong missing, every client
#                         Pinged at least once), the slowest Pong under 10s
#                         (the SDK's Pong timeout), no connection lost, and no
#                         gateway closed one for its heartbeat
#   garbage_lease         each gateway renewed its lease every interval
#                         through the flood, and Logic saw no renewal error
#   no_swap_in_garbage    no page swapped in over the flood
#   slow_flood            both floods held >= 85% of their connections, each
#                         slot freed at least once, before the light clients start
#   slow_held             every slow connection was closed by a gateway, held
#                         p50 >= 5s, p99 <= 6.5s, max <= 7.5s (5s + one tick of
#                         the wheel, plus scheduling slack, 6b3a O7)
#   slow_freed            the gateways counted at least as many
#                         handshake_timeout and auth_timeout closes
#   slow_legit            every light client logs in beside the floods: no
#                         Busy, no reject (login_queue, pending_mem: the
#                         slots, at_capacity) and no bucket refusal
#                         (no_token); the concurrency limit's (no_slot,
#                         retried as in storm) and loadgen's failed are
#                         recorded
#   slow_heartbeats, slow_lease   as garbage's, over the floods
#   no_swap_in_slow       no page swapped in from the floods' start to their stop
#   <phase>_idle          after loadgen stops, every gateway reaches 0
#                         connections and settles its Logouts
#   <phase>_processes     nothing died, panicked or stopped uncleanly
#   artifacts             metrics before and after each phase's window, the
#                         garbage flood's CPU profiles, loadgen's results
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init floods
t2_need curl taskset sudo python3 ss
t2_require_host_setup

PHASES=${FLOODS_PHASES:-storm garbage slow}
for p in $PHASES; do
  case $p in storm | garbage | slow) ;; *) echo "floods: unknown phase '$p' (storm, garbage, slow)" >&2; exit 1 ;; esac
done
for v in FLOODS_STORM_CLIENTS FLOODS_STORM_RATE FLOODS_CLIENTS FLOODS_RATE FLOODS_HEARTBEAT \
  FLOODS_GARBAGE_RATE FLOODS_GARBAGE_INFLIGHT FLOODS_GARBAGE_SECONDS FLOODS_GARBAGE_BUCKET \
  FLOODS_SLOW_HANDSHAKE FLOODS_SLOW_AUTH FLOODS_SLOW_RATE FLOODS_SLOW_HOLD; do
  case ${!v-1} in ''|*[!0-9]*|0) echo "floods: $v must be a positive whole number" >&2; exit 1 ;; esac
done
[ "${FLOODS_GARBAGE_SECONDS:-90}" -ge 45 ] || { echo "floods: FLOODS_GARBAGE_SECONDS must be at least 45 (the flood's profile)" >&2; exit 1; }
# Light clients cost loadgen ~10KB each and the gateway less; the garbage
# phase's 400k are the most at once (capacity's 600k on one gateway ran
# within 16).
t2_mem_budget 12
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the stack
smp=""
t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'

GW_COUNTERS=(
  hs_ok='goim_gateway_handshakes_total:result="ok"'
  hs_failed='goim_gateway_handshakes_total:result="failed"'
  hs_no_slot='goim_gateway_handshakes_total:result="no_slot"'
  hs_no_token='goim_gateway_handshakes_total:result="no_token"'
  rejects_handshake_rate='goim_gateway_admission_rejects_total:reason="handshake_rate"'
  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
)
LOGIC_LEASE_ERROR='goim_logic_lease_renewals_total:result="error"'
HB=${FLOODS_HEARTBEAT:-30}

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

# down NAME: every loadgen stopped, the gateways idle, the stack stopped;
# checks NAME_idle and NAME_processes.
down() {
  lg_stop_all
  local t=$SECONDS idle stop_s problems
  idle=$(t2_bool stack_wait_idle 900 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
}

# gws_counters: {"gw-0": stack_counters, ...} of every gateway; gws_delta A B
# the same of two readings.
gws_counters() {
  local g out=""
  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
    out+="${out:+, }\"$g\": $(stack_counters "$g" "${GW_COUNTERS[@]}")"
  done
  echo "{$out}"
}
# (stack_counters_delta per gateway: a key null in A counts as 0).
gws_delta() {
  python3 -c 'import json, sys
a, b = (json.loads(x) for x in sys.argv[1:3])
def delta(x, y):
    if x is None or y is None:
        return None
    return {k: None if v is None else round(v - (x.get(k) or 0), 6) for k, v in y.items()}
print(json.dumps({g: delta(a.get(g), y) for g, y in b.items()}))' "$1" "$2" 2>/dev/null || echo null
}

# hs_sampler NAME FILE: appends "unix_ms ok" every second, NAME's completed
# handshakes, until killed.
hs_sampler() {
  local n
  while :; do
    n=$(stack_metric "$1" goim_gateway_handshakes_total 'result="ok"') &&
      echo "$(date +%s%3N) $n" >>"$2"
    sleep 1
  done
}
# hs_rate FILE N: {"rate_per_s" (handshakes per second between the samples
# where 5% and 95% of N had completed: the ramp's body, without the retries'
# tail), "ramp_s", "peak_per_s", "series": [[s since the first sample,
# handshakes/s], ...] (first 120)}; null when the samples never reach 95% of N.
hs_rate() {
  python3 -c 'import json, sys
rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 2 and l.split()[1].isdigit()]
n = int(sys.argv[2])
lo = next((r for r in rows if r[1] >= 0.05 * n), None)
hi = next((r for r in rows if r[1] >= 0.95 * n), None)
if not lo or not hi or hi[0] <= lo[0]:
    print("null"); sys.exit()
pts = [((t1 - rows[0][0]) / 1000, (c1 - c0) * 1000 / (t1 - t0)) for (t0, c0), (t1, c1) in zip(rows, rows[1:]) if t1 > t0]
print(json.dumps({"rate_per_s": round((hi[1] - lo[1]) * 1000 / (hi[0] - lo[0])), "ramp_s": round((hi[0] - lo[0]) / 1000, 1),
       "peak_per_s": round(max(p[1] for p in pts)), "series": [[round(t, 1), round(v)] for t, v in pts[:120]]}))' "$1" "$2" 2>/dev/null || echo null
}

# timewait: the host's TIME_WAIT sockets now (ss -s).
timewait() { ss -s 2>/dev/null | sed -n 's/^TCP:.*timewait \([0-9]*\).*/\1/p' | head -n 1 | grep . || echo null; }

# lease PHASE DELTA SECONDS LOGIC_DELTA: checks PHASE_lease over the gateways'
# counter delta of a window of SECONDS.
lease() {
  local floor
  floor=$(t2_lease_floor "$3")
  t2_check "${1}_lease" "$(echo "{\"g\": $2, \"l\": $4}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
    "\"window_s\": $3, \"live_floor\": $floor, \"gateways\": $(echo "$2" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic_lease_error\": $(echo "$4" | lg_py - 'd["lease_error"]')"
}

# heartbeats PHASE N DELTA: checks PHASE_heartbeats over the light result of
# N clients (instance main, stopped) and the gateways' counter delta.
heartbeats() {
  local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json
  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3}" | lg_true - 'd["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values())')" \
    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}')"
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
missing=""

# storm: #121 (2), a legitimate 20k/s storm on one gateway's default limiter.
phase_storm() {
  local n=${FLOODS_STORM_CLIENTS:-200000} rate=${FLOODS_STORM_RATE:-20000} s0 s1 d usage online hs
  STACK_GATEWAYS=1 STACK_GATEWAY_ARGS=(-max-conns $((n + n / 10)))
  up storm || { stack_down; return; }
  stack_snapshot before
  s0=$(gws_counters)
  t2_record_start storm
  lg_start storm -mode light -clients "$n" -rate "$rate" -uid-prefix storm
  stack_usage_start loadgen="$LG_PID"
  hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" &
  smp=$!
  online=$(t2_bool lg_wait_online "$n" $((n / rate * 3 + 60)))
  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
  usage=$(stack_usage_json)
  s1=$(gws_counters)
  t2_record_end storm
  stack_snapshot after
  d=$(gws_delta "$s0" "$s1")
  hs=$(hs_rate "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$n")
  [ "$online" = true ] && online=$(lg_get /stats 'd["online"] == '"$n"' and "gave-up" not in d["failed"]')
  t2_check storm_online "${online:-false}" "\"want\": $n, \"online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]'), \"login_latency\": $(lg_get /stats 'd["login_latency"]')"
  t2_check storm_rate "$(echo "$hs" | lg_true - 'd["rate_per_s"] >= 0.9 * '"$rate")" \
    "\"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
  t2_check storm_bucket "$(echo "$d" | lg_true - 'd["gw-0"]["hs_no_token"] == 0')" \
    "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
  down storm
}

# garbage: #121 (1), garbage handshakes beside a Pinging population.
phase_garbage() {
  local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} grate=${FLOODS_GARBAGE_RATE:-80000}
  local inflight=${FLOODS_GARBAGE_INFLIGHT:-20000} secs=${FLOODS_GARBAGE_SECONDS:-90}
  local online s0 s1 d l0 l1 tw0 tw1 usage want flood prof t0 lease_s f
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $((n / 2 + n / 10 + inflight)) -heartbeat "${HB}s"
    ${FLOODS_GARBAGE_BUCKET:+-handshake-rate "$FLOODS_GARBAGE_BUCKET"})
  up garbage || { stack_down; return; }
  lg_start garbage-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix garbage
  online=false
  lg_wait_online "$n" $((n / rate * 3 + 60)) &&
    online=$(lg_get /stats 'd["online"] == '"$n"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$n"' * 0.45')
  t2_check garbage_online "${online:-false}" "\"want\": $n, \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"failed\": $(lg_get /stats 'd["failed"]')"
  [ "$online" = true ] || { down garbage; return; }
  stack_snapshot before
  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_tcpstat_snap "$STACK_DIR/tcp0"
  tw0=$(timewait)
  t2_record_start garbage
  t0=$SECONDS
  lg_in flood lg_start garbage -mode garbage -rate "$grate" -clients "$inflight" -source-net 1
  stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}"
  # The gateways' and the flood's CPU over 30s of the flood, from 10s in.
  (sleep 10; stack_profile garbage 30 & lg_in flood lg_profile garbage 30 & wait) >/dev/null 2>&1 &
  prof=$!
  sleep "$secs"
  usage=$(stack_usage_json)
  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_tcpstat_snap "$STACK_DIR/tcp1"
  tw1=$(timewait)
  lg_in flood lg_stop
  wait "$prof"
  t2_record_end garbage
  stack_snapshot after
  lg_stop
  d=$(gws_delta "$s0" "$s1")
  flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
  want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
  t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1")"
  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_slot"] + v["hs_no_token"] > 0 for v in d.values())')" \
    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"]} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
  heartbeats garbage "$n" "$d"
  lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d, \"flood\": $flood}"
  down garbage
  for f in before/gw-0 after/gw-0 garbage/gw-0.cpu.pb.gz garbage/gw-1.cpu.pb.gz garbage/loadgen-flood.cpu.pb.gz; do
    case $f in
      *.gz) gzip -t "$STACK_DIR/artifacts/$f" 2>/dev/null || missing+=" garbage:$f" ;;
      *) grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" garbage:$f.metrics" ;;
    esac
  done
}

# slow: slow-handshake and slow-auth floods beside light clients logging in.
phase_slow() {
  local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} hs=${FLOODS_SLOW_HANDSHAKE:-80000}
  local au=${FLOODS_SLOW_AUTH:-40000} srate=${FLOODS_SLOW_RATE:-10000} hold=${FLOODS_SLOW_HOLD:-60}
  local s0 s1 d l0 l1 t0 lease_s flooding=true online usage inst held f
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
  up slow || { stack_down; return; }
  stack_snapshot before
  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_record_start slow
  t0=$SECONDS
  lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
  lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
  lg_in flood lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$hs"' and d["flood"]["open"] >= 0.85 * '"$hs" $((hs / srate + 60)) || flooding=false
  lg_in flood2 lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$au"' and d["flood"]["open"] >= 0.85 * '"$au" $((au / srate + 60)) || flooding=false
  t2_check slow_flood "$flooding" "\"slowhs\": $(lg_in flood lg_get /stats 'd["flood"]'), \"slowauth\": $(lg_in flood2 lg_get /stats 'd["flood"]')"
  [ "$flooding" = true ] || { t2_record_end slow; down slow; return; }
  lg_start slow-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix slow
  stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}" flood2="${LG_PIDS[flood2]}"
  online=false
  lg_wait_online "$n" $((n / rate * 3 + 60)) && online=$(lg_get /stats 'd["online"] == '"$n")
  sleep "$hold"
  usage=$(stack_usage_json)
  lg_in flood lg_stop
  lg_in flood2 lg_stop
  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_record_end slow
  stack_snapshot after
  lg_stop
  d=$(gws_delta "$s0" "$s1")
  held=true
  for inst in slowhs slowauth; do
    [ "$(lg_true "$STACK_DIR/artifacts/loadgen-$inst.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" = true ] || held=false
  done
  t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}')"
  t2_check slow_freed "$(echo "{\"g\": $d, \"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')}" | lg_true - 'sum(v["rejects_handshake_timeout"] for v in d["g"].values()) >= d["h"] and sum(v["rejects_auth_timeout"] for v in d["g"].values()) >= d["a"]')" \
    "\"handshake_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_handshake_timeout"] for v in d.values())'), \"slowhs_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"auth_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_auth_timeout"] for v in d.values())'), \"slowauth_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')"
  t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}}" | lg_true - 'd["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
    "\"want\": $n, \"online\": ${online:-false}, \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
  heartbeats slow "$n" "$d"
  lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d}"
  down slow
  for f in before/gw-0 after/gw-0; do
    grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
  done
}

for p in $PHASES; do
  t0=$SECONDS
  "phase_$p"
  t2_duration "$p" $((SECONDS - t0))
  for f in "$T2_OUT/$p"/artifacts/loadgen-*.json; do
    [ -e "$f" ] || continue
    [ "$(lg_true "$f" '"mode" in d')" = true ] || missing+=" ${f#"$T2_OUT"/}"
  done
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "garbage from distinct hosts: 253 loopback sources of one loadgen (the limiter is global, never per IP)" \
  "the handshake flood across several gateways behind an LB (Tier 3, Plan 6d)" \
  "a storm's lease renewals on the other gateways: storm runs one gateway (gwloss b1 covers it)"
t2_finish
```

```bash
chmod 0755 scripts/tier2/floods.sh
git apply <<'PATCH'
diff --git a/Makefile b/Makefile
index a4a55e8..9bc1e72 100644
--- a/Makefile
+++ b/Makefile
@@ -71,7 +71,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
PATCH
```

- [ ] **Step 2: Check the syntax and the refusals**

Run: `bash -n scripts/tier2/floods.sh && echo syntax-ok`
Expected: `syntax-ok`. The refusals are checked in the dry-run copy (Step 3), because the guard refuses the dev machine first.

- [ ] **Step 3: Dry run on the dev machine**

The guard refuses the dev machine, so the script runs in a throwaway copy, as `slowlogic` did: the guard lowered, the root parts removed, the scale cut. `FLOODS_GARBAGE_BUCKET=500` lets a 2,000/s flood reach the limiter.

```bash
D=$(mktemp -d); cp scripts/tier2/floods.sh "$D/floods.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget 12$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 ss$|t2_need curl taskset python3 ss|' "$D/floods.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/floods.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/floods.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/floods.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  FLOODS_STORM_CLIENTS=2000 FLOODS_STORM_RATE=1000 FLOODS_CLIENTS=2000 FLOODS_RATE=1000 \
  FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500 \
  FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35 \
  bash "$D/floods.sh" >"$D/out.txt" 2>&1; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype, ~2 min): `exit 1`. `result.json` parses and has 27 checks. All pass except `storm_rate`, and possibly the `no_swap_in_*` checks from the dev machine's own swapping (the prototype saw 1–22 pages in some runs and 0 in others). Values from the prototype (`c99b8bc`):
- `storm_rate` fails: `rate_per_s` 385 of 1,000. The dev gateway has 2 CPUs, so 1 handshake slot, and `storm_bucket` (which passes) shows why: `hs_no_token` 0, `hs_no_slot` 993, `no_slot_share` 0.33, loadgen `failed` `{"pre-handshake": 993}`. `storm_online` passes: all 2,000 logged in through the retries.
- `garbage_delivered`: `attempts` 90,373 against `want` 85,500, `skipped` 0, `outcomes` `{"closed": 90373}`. Gateways: gw-0 `hs_failed` 25,057 and `hs_no_token` 34,737, gw-1 25,067 and 5,389 (the split of Review Focus 3). `timewait` `[2895, 8192]`. `tcp.TCPTimeWaitOverflow` ~65k, `ListenOverflows` 0.
- `garbage_limited`: both gateways refused, `dh_per_s` 557 each against `bucket` 500 (the burst of 2,500 included).
- `garbage_heartbeats` and `slow_heartbeats`: `pong_missing` 0, `pongs` 3,208 and 2,567, `pong_rtt.max_ms` 19 and 52, `lost` `{}`, no heartbeat closes. `garbage_lease` and `slow_lease`: `live` 3–4 per gateway, at the floor, no `missing` or `error`.
- `slow_flood`: each flood `open` 500, `closed` 500. `slow_held`: 3,500 closed each, p50 ~5,510ms, p99 ~5,995ms, max ~6,007ms. `slow_freed`: `handshake_timeout` 3,500 = `slowhs_closed`, `auth_timeout` 3,500 = `slowauth_closed`. `slow_legit`: 2,000 online, `failed` `{}`, every gateway count 0.
- `artifacts`: `missing` `""`. `garbage/artifacts/garbage/` holds `gw-0`, `gw-1`, `logic` and `loadgen-flood` `.cpu.pb.gz`.

Then the pass path of `storm`, at a rate one slot carries:

```bash
rm -rf tier2-results/floods-*
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 FLOODS_PHASES=storm \
  FLOODS_STORM_CLIENTS=1000 FLOODS_STORM_RATE=200 bash "$D/floods.sh" >"$D/out2.txt" 2>&1; echo "exit $?"
```

Expected: `exit 0`, 8 checks. `storm_rate` `rate_per_s` 184 of 200. `storm_bucket` `hs_no_slot` 67 (`no_slot_share` 0.06), `hs_no_token` 0.

And the refusals, each before anything starts (`exit 1`, with these messages): `FLOODS_PHASES=bogus` prints `floods: unknown phase 'bogus' (storm, garbage, slow)`. `FLOODS_GARBAGE_SECONDS=30` prints `floods: FLOODS_GARBAGE_SECONDS must be at least 45 (the flood's profile)`. `FLOODS_RATE=1.5` prints `floods: FLOODS_RATE must be a positive whole number`. Each is run as `GOIM_TEST_TIER=2 VAR=VALUE bash "$D/floods.sh"`. No process is left (`pgrep -af "tier2-results/floods" | grep -v pgrep || echo none`). Afterwards, remove `tier2-results/floods-*` and `$D`.

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/floods.sh Makefile
git commit -m "feat(tier2): floods: a 20k/s storm on the bucket, garbage handshakes beside Pinging clients, slow clients beside logins (#121, #122)"
```

- [ ] **Step 5: Verify the whole branch**

Run: `make test-scripts && make test && bash -n scripts/tier2/floods.sh scripts/tier2/slowlogic.sh`
Expected: `lib_test`, `loadgen_test` and `stack_test` print `all passed`, and `make test` is ok. Record the commits and the dry run in this plan's *Execution Notes* and push the branch.

---

### Task 3: Tier 2 run 30 and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row, #122), `README.md` (both languages)

- [ ] **Step 1: Tier 2 run 30 (on the host, ~10 min)**

On the Tier 2 host, after `git fetch && git switch feat/plan6b3 && git pull`, with the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods` and sends back the `.report.txt`. Expected: 27/27 (`storm_stack`, `no_swap_in_storm`, `storm_online`, `storm_rate`, `storm_bucket`, `storm_idle`, `storm_processes`, `garbage_stack`, `garbage_online`, `no_swap_in_garbage`, `garbage_delivered`, `garbage_limited`, `garbage_heartbeats`, `garbage_lease`, `garbage_idle`, `garbage_processes`, `slow_stack`, `slow_flood`, `no_swap_in_slow`, `slow_held`, `slow_freed`, `slow_legit`, `slow_heartbeats`, `slow_lease`, `slow_idle`, `slow_processes`, `artifacts`).

What to read, for the *Execution Notes*, the roadmap and the README:
- **storm:** `storm_rate.handshakes` (`rate_per_s`, `peak_per_s`, `ramp_s`) and `loadgen_cores`, `storm_bucket` (`hs_no_token` 0, `hs_no_slot` and `no_slot_share`, Review Focus 1, against the ~1.2% estimate), `storm.login_latency`, `storm.usage` (gw-0's and Logic's cores at 20k/s).
- **garbage:** `garbage_delivered` (`attempts`/`want`, the gateways' `hs_failed` and `hs_no_token`/`hs_no_slot` each, `outcomes`, `timewait`, `tcp.TCPTimeWaitOverflow`, `tcp.ListenOverflows`), `garbage_limited.dh_per_s` against 25,000/s, the gateways' cores (`garbage.usage`) and the flood's, the heartbeats' `pong_rtt` p50/p99/max, `garbage_lease`. The gateways' profiles (`garbage/artifacts/garbage/gw-*.cpu.pb.gz`) show how much of their CPU the failed DH took.
- **slow:** `slow_flood` (`open` against 85%), `slow_held` quantiles, `slow_freed`, `slow_legit` (`login_latency`, loadgen `failed`, each gateway's `hs_no_slot`), the heartbeats, `slow_lease`.
- If a check fails: `storm_rate` or `garbage_delivered` → Review Focus 2. `garbage_limited` on one gateway → Review Focus 3. `slow_legit` with `rejects_pending_mem` → the floods filled the pending cap (lower `FLOODS_SLOW_HANDSHAKE`), with `hs_no_token` → slow-auth's share is too high for the bucket (lower `FLOODS_SLOW_AUTH`). `slow_held` past 6.5s → the gateways' wheel was late under the floods, which is a finding. `<phase>_heartbeats` → the limiter did not protect established connections, the spec's claim: read `pong_rtt` and the gateways' profiles.

- [ ] **Step 2: Record**

- This plan's *Execution Notes*: run 30's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #122 gets one line with run 30's result and its decision status. The plan 6 row: "6b3b executed, run 30 …; next 6b3c (fresh session)".
- README *Measured results*, both languages, a row after the slowed-Logic row: **Floods**: a 20k/s storm on one gateway with the bucket refusing 0 (the concurrency limit's share), garbage at N/s beside 400k Pinging clients (Pong p99, none missing), and slow clients freed in ~5.5s with 400k legitimate logins and no Busy. In the status paragraph, move "floods" from *Next* to *Done*.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b-floods.md
git commit -m "docs: Tier 2 run 30, floods recorded; README row"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b3c** (`POST /reset`, `POST /network-change`, scenario `reconnect`, run 31) reuses `floods`' pattern: instances named by role, `lg_stop_all`, per-gateway counters, `<phase>_heartbeats` from a light instance beside an SDK storm. The SDK's pre-handshake retries meet the same concurrency limit in a whole-population reconnect. Run 30's `no_slot_share` (Review Focus 1) is the measured input for that.
- **Spec:** if run 30 shows a large `no_slot` share in a legitimate storm, *Admission control*'s "shapes load rather than rejecting it" holds for the bucket but not for the semaphore. Deciding that is the user's call, after the run.

## Execution Notes

(Filled in during execution.)
