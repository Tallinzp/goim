# Plan 6c3: gwconns Shard Load and the ReconcileEnd Deadline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure a triggered Reconcile pass at the cap on a Redis Cluster: the `gwconns` set's master must stay within the rule against the other masters (R4M-9), and the ReconcileEnd delete phase must keep its rate, with its time against the 30s graces recorded (R5S-O8, Plan 2's deferred minor). The new Tier 2 scenario `gwconns` runs it as run 36.

**Architecture:** Two tasks:
- **The scenario `gwconns`** (Task 1, `scripts/tier2/gwconns.sh`, its readers' tests `gwconns_test.sh`, one `Makefile` line). One stack: a Redis Cluster (3 masters, 3 replicas), Logic, one gateway with `-faults` at `-max-conns` 250,000, 250k light clients. The test fault API (`POST /fault/action/reconcile`) starts the triggered pass. Two windows, each from a `CONFIG RESETSTAT` on every node: the pass and the delete phase. In each, the set's master's p99 of one per-uid command (pass: `pexpire`; delete: `hgetall`) is compared with each other master's.
- **Dry run and run 36** (Task 2): the dev dry run, then the user runs the scenario on the Tier 2 host; the report is recorded.

No production code changes. The deadlines are measured, not changed (#147.3).

**Tech Stack:** bash, python3 (the readers), the pinned `redis-server` 7.4.11 (`make redis-server`), `scripts/stack.sh`, `scripts/loadgen.sh`, `scripts/tier2/lib.sh`.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Verification* → *Load tests*, the `gwconns` shard load bullet (~l.541: a triggered Reconcile pass at the cap, per-shard Redis CPU and p99 recorded, the `gwconns` shard's p99 within 2× the other shards'); *Reconcile* → *Pass end* (~l.227: ReconcileEnd's deadline covers the rate-limited phase) and *Delete phase* (~l.286: ~10k uids/s, the reply after the phase). Nothing here changes the spec; its next revision names the compared commands and the floor (#148.5–6).

Inputs:
- The roadmap's **#147** (the shape, decided by the user 2026-11-25; #147.2 amended by #148.5, #147.3 amended) and **#148** (this plan's choices, decided by the user 2026-11-25).
- Plan 2's deferred minor (`2026-10-08-connection-session-core-02-logic.md`, *Deferred minors*: a ReconcileEnd longer than `MaxConnectionAgeGrace` or `ShutdownGrace`, 30s, about 280k members at 10k/s, is cut and retried).
- Plan 6c1's interface review P6c1I-4..6 (the 6c1 plan's *Execution Notes*: the shared Logic set as the baseline; record each epoch key's master at the start; run 33's per-master `evalsha` 119–124µs).
- `scripts/tier2/failover.sh` as the pattern (one Cluster stack, `stack_redis_latency` per node, the checks' style); `scripts/tier2/capacity.sh` `P_reconcile_end` (the ReconcileEnd counters).

**Decisions (the user, 2026-11-25, with AskUserQuestion; #147, #148):**
1. **The trigger** is the test fault API (gateway `-faults`, `POST /fault/action/reconcile`), not a deleted lease key (#147.1).
2. **One gateway** at the cap, 250k light clients (#148.1).
3. **Two windows**, pass and delete phase, both gated (#148.2).
4. **The rule:** in each window the set's master's p99 ≤ max(2 ×, + floor) **each** other master's p99 (#147.2, #148.3, #148.6). **Amended at the final review (#148.7):** the p99 is a client-side probe's round trip (`PING` to each master every 10ms from loadgen's CPUs), floor 200µs; Redis's own service-time p99s (`pexpire` in the pass, `hgetall` in the delete phase, #148.5) are recorded only. The set's own commands (`sadd`, `sscan`, `srem`) are recorded, not gated.
5. **The delete phase** is gated on its rate: uids scanned ÷ the phase's time ≥ 90% of `-delete-rate`, one ReconcileEnd, none retried or dropped. The time and its margin to 30s are recorded only (#147.3 amended).
6. **No Logic SIGTERM phase** (#147.4).
7. **Logic and the 6 Redis nodes share Logic's CPU set** (11,23), as runs 33–35 (#148.4).

**Branch:** `feat/plan6c3` (from `main` `b2942a1`; #147 is `51474b3`). Execute from its head, this plan's own commit or later.

**Size:** 2 tasks. Task 1: ~390 lines of script (the scenario ~315, its tests ~76, the `Makefile` 1). Task 2: the dry run, run 36 and the record. The prototype `proto-6c3` (from `51474b3`; `2ea2234`, `0c747c1`) ran every step; its diff is replayed here byte for byte as one `git apply` patch. On the dev machine (AMD EPYC 7742, 2 cores, ~1GB):
- `make test-scripts` green; `gwconns_test.sh` 27 checks pass; without `gwconns.sh` 26 fail.
- 6 mutations of the readers, each caught (Task 1 Step 4).
- The dry run (Task 2 Step 1, 5,000 clients, triggered rate 150/s, delete rate 500/s): 14 checks, only `no_swap_in` fails (the dev machine's swap). Pass 32.2s, `pexpire` p99 5.0µs on the set's master against 6.0 and 7.0µs; the delete phase 9.0s at 555 uids/s (bound 450), `hgetall` 8.0µs against 12.0 and 6.0µs; 5,000 sessions intact, 0 deleted. The set's master: `sadd` 25 calls p99 811µs, `sscan` 8 pages p99 365µs.
- **Replayed 2026-11-25** in a fresh worktree from `feat/plan6c3`: the patch taken from this file, every step of Task 1 run as written; the tree after Task 1 equals `proto-6c3`'s (empty `git diff`).

## Global Constraints

- `make test-scripts` stays green on the dev machine (2 cores, ~1GB, no Docker, no root). `make test`, `go vet ./...` are untouched (no Go change).
- No change to any Go code, the wire, any Lua script, `scripts/stack.sh`, `scripts/loadgen.sh` or `scripts/tier2/lib.sh`. New files only: `scripts/tier2/gwconns.sh`, `scripts/tier2/gwconns_test.sh`; one line in the `Makefile` (`test-scripts`).
- Run 36 is `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwconns` at its defaults: 250,000 clients, `-max-conns` 250,000, ramp 5,000/s, `-triggered-rate` 5,000, `-delete-rate` 10,000 (Logic and the gateway), floor 200µs (the probe's, #148.7), graces 30s; CPU sets gateway `6-9,18-21` (GOMAXPROCS 8), Logic + Redis `11,23`, loadgen `0-5,10,12-17,22`; `-reconcile-cycle 24h` (no periodic pass in the run, capacity's choice).
- The gateway's `-delete-rate` equals Logic's: it sizes ReconcileEnd's deadline (2 × tracked ÷ rate + 10s = 60s at 250k).
- `result.json` member names must not repeat lib.sh's own (`pass`, `checks`, …): the windows' fields are `pass_window` and `delete_window`.

## Review Focus

1. **The pass's end leaks ~1s of the delete phase into the pass window.** The pass window's numbers are read (6 nodes × 3 `redis-cli` calls) before the delete window's `RESETSTAT`. The pass's gate (`pexpire`) is unaffected (the delete phase runs no `pexpire`); its `hgetall` and `sscan` numbers carry that second. Run 36: read the delete window's `hgetall` calls against the clients (~250k, less what the first second took).
2. **A periodic pass at a random time.** `-reconcile-cycle 24h` starts the first periodic pass at a random point of 24h (~0.4% in a 6-minute run). It runs at `MinRate` 10/s, so it cannot end or send a ReconcileEnd in the run (and Logic's `ends` counts `pace="triggered"` only). A trigger during it cancels it; its chunks in flight may resolve after the first reading, so `pass_entries` reads above the clients and fails (the safe direction). `passes_periodic` in `pass` shows the cause. Run 36: read `passes_periodic` (0 expected). (Corrected at the final review, M-1.)
3. **ReconcileEnd across Logic's `MaxConnectionAge` (5m).** The gateway's channel to Logic gets a GOAWAY every ~5m; a phase in flight then has the 30s grace, longer than the ~25s phase. A cut phase would be retried (`end` fails on `retried`). Run 36: if `end` fails on a retry, read Logic's log for the GOAWAY before any change.
4. **The p99 of a few hundred SSCAN pages.** At 250k the set's master runs ~500 `sscan` pages and ~1,250 `sadd`s (one per chunk); each blocks that master for its time (dry run: p99 365µs and 811µs at 5,000 members). Redis's `INFO latencystats` times a command's own run only, so a per-uid command queued behind a page does not show there (final review I-1); the probe's round trip does, and it is the gate (#148.7). A page lands under ~1% of the probes (500 × ~0.5ms over ~25s), near the p99. Run 36: record `sadd`/`sscan` p99 and p99.9, the probe's p50/p99/p99.9 per master, and the set's master's CPU against the others'.
5. **The rate gate's burst.** Logic's limiter allows a burst of one page (500 uids) at once, so the measured rate is a little above `-delete-rate` (dry run: 555 against 500/s); at 250k it is ~0.2% above. The gate is a floor (≥ 90%), so the burst cannot make it fail.

Each line's test: 1, 2, 3, 4 are run 36's readings (Task 2 Step 3); 2's `end` rule (two phases fail) and 5's floor are pinned by `gwconns_test.sh` (`end: an earlier phase in Logic's window`, `end: 9,000 uids/s is 90%`).

## File Structure

- `scripts/tier2/gwconns.sh` (new): the scenario; readers `gc_shards`, `gc_end`, `gc_wait_inc` (Task 1).
- `scripts/tier2/gwconns_test.sh` (new): the readers' tests (Task 1).
- `Makefile`: `test-scripts` runs `gwconns_test.sh` (Task 1).
- The roadmap (the plan 6 row, #148's outcome), this plan's *Execution Notes* (Tasks 1–2).

---

### Task 1: The scenario `gwconns`

**Files:**
- Create: `scripts/tier2/gwconns.sh`, `scripts/tier2/gwconns_test.sh`
- Modify: `Makefile` (`test-scripts`)
- Test: `scripts/tier2/gwconns_test.sh`

**Interfaces:**
- Consumes (unchanged): `stack.sh` (`stack_up` with `STACK_REDIS=cluster`, `STACK_GATEWAY_ARGS`, `STACK_LOGIC_ARGS`; `stack_lease_key`, `stack_redis_owner`, `stack_redis_node_cli`, `stack_redis_resetstat`, `stack_redis_latency`, `stack_redis_roles`, `stack_counters`, `stack_counters_delta`, `stack_metric`, `stack_metrics`, `stack_usage_start`/`_json`, `stack_snapshot`, `stack_wait_idle`, `stack_health`); `loadgen.sh` (`lg_start`, `lg_wait_online`, `lg_get`, `lg_py`, `lg_true`, `lg_stop_all`); `lib.sh` (`t2_check`, `t2_field`, `t2_record_start`/`_end`, `t2_sessions`); the gateway's `-faults` admin API `POST /fault/action/reconcile` (`internal/gateway/server.go`).
- Produces (readers, tested): `gc_shards LATENCY OWNER CMD FLOOR_US` → `{"cmd", "owner", "p99_us": {master: µs}, "calls": {master: n}, "bound_us", "ok"}` or `null`; `gc_end GW_DELTA LOGIC_DELTA CLIENTS RATE GRACE_S` → `{"ends_ok", "retried", "dropped", "scanned", "duration_s", "rate", "want_rate", "margin_s", "deadline_s", "deleted", "ok"}` or `null`; `gc_wait_inc NAME METRIC MATCH FROM SECONDS` (status 0 once the sum exceeds FROM, 1 after SECONDS).
- Produces (result.json): checks `stack`, `online`, `pinning`, `gwconns_set`, `pass`, `pass_entries`, `pass_shards`, `end`, `end_shards`, `sessions`, `no_swap_in`, `idle`, `processes`, `artifacts` (14); fields `pass_window`, `delete_window` (each `redis_latency` per node and `usage` per process), `background`, `run`.
- Consumed by: Task 2.

- [ ] **Step 1: The tests**

Save the patch below to a file in the session scratchpad (`PATCH`) and apply its `Makefile` and test file from the repository root: `git apply --include=Makefile --include='*_test.sh' PATCH`. `git status --short` shows ` M Makefile` and `?? scripts/tier2/gwconns_test.sh`.

````diff
diff --git a/Makefile b/Makefile
index 6a089ca..fea69c6 100644
--- a/Makefile
+++ b/Makefile
@@ -29,6 +29,7 @@ test-scripts:
 	scripts/tier2/floods_test.sh
 	scripts/tier2/reconnect_test.sh
 	scripts/tier2/failover_test.sh
+	scripts/tier2/gwconns_test.sh
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_cluster_test.sh
diff --git a/scripts/tier2/gwconns.sh b/scripts/tier2/gwconns.sh
new file mode 100755
index 0000000..679b271
--- /dev/null
+++ b/scripts/tier2/gwconns.sh
@@ -0,0 +1,316 @@
+#!/usr/bin/env bash
+# Tier 2: gwconns (Plan 6c3), the spec's gwconns shard load test (R4M-9)
+# and the ReconcileEnd deadline (R5S-O8, Plan 2's deferred minor): a
+# triggered Reconcile pass at the cap (250k connections on one gateway),
+# started through the test fault API (gateway -faults, POST
+# /fault/action/reconcile, #147.1), then its ReconcileEnd's delete phase.
+# On a Redis Cluster (3 masters, 3 replicas, stack.sh STACK_REDIS=cluster);
+# Logic and the 6 nodes share Logic's CPU set, as runs 33-35 (#148.4).
+# One gateway (#148.1): its gwconns:gw-0:{EPOCH} set is on one master (the
+# set and the lease key share the epoch's hash tag), the other two hold
+# none. Two windows (#148.2), each from a CONFIG RESETSTAT on every node:
+#   pass:   from the trigger to the pass's end (every entry resolved):
+#           per entry an HGET, HMGET and PEXPIRE on its uid's master (an
+#           unchanged session runs no compare-and-set script) and a SADD
+#           per chunk on the set's master;
+#   delete: from the pass's end to ReconcileEnd's reply: the delete phase,
+#           an SSCAN page of the set and an HGETALL per uid on its master.
+# The rule (#147.2 amended, #148.3, #148.5-6): in each window, the set's
+# master's p99 of the window's per-uid command (pass: pexpire; delete:
+# hgetall) is at most max(2x, + GWCONNS_FLOOR_US) each other master's
+# (Redis's percentiles are buckets: at a few µs one bucket doubles a p99).
+# The set's own commands (sadd, sscan, srem) run on its master only; they
+# are recorded, not gated. Reading the pass window's
+# numbers takes ~1s of the delete phase into the pass window.
+#
+# Usage (on the Tier 2 host, from the repo root), after the one-time
+#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
+#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwconns
+#   Results: tier2-results/gwconns-<timestamp>/ (result.json, the stack's
+#   logs and artifacts), plus ONE file to send back:
+#   tier2-results/gwconns-<timestamp>.report.txt
+#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
+#   About 5 minutes (the ramp 50s, the pass 50s, the delete phase 25s).
+#
+# Settings (every number whole and positive, refused otherwise before
+# anything starts):
+#   GWCONNS_CLIENTS        light clients online on gw-0 (default 250000, the cap)
+#   GWCONNS_CAP            gw-0's -max-conns (default 250000, the spec's cap)
+#   GWCONNS_RATE           the ramp, clients per second (default 5000)
+#   GWCONNS_TRIGGERED_RATE gw-0's -triggered-rate, entries/s (default 5000)
+#   GWCONNS_DELETE_RATE    the delete phase's rate, uids/s: Logic's and
+#                          gw-0's -delete-rate (default 10000)
+#   GWCONNS_FLOOR_US       the rule's floor over the other masters' p99, µs
+#                          (default 50, #148.6)
+#   GWCONNS_GRACE_S        MaxConnectionAgeGrace and ShutdownGrace, for the
+#                          recorded margin (default 30, Logic's defaults)
+#   GWCONNS_LG_MEMLIMIT    loadgen's GOMEMLIMIT (default 4GiB)
+#   GWCONNS_CPUS_LOADGEN, GWCONNS_CPUS_GATEWAY, GWCONNS_CPUS_LOGIC
+#                          the CPU sets (default failover's: 0-5,10,12-17,22;
+#                          6-9,18-21; 11,23)
+#   GWCONNS_GATEWAY_PROCS  gw-0's GOMAXPROCS (default 8, its CPU set)
+#
+# Checks:
+#   stack          redis (6 nodes), devtoken, Logic and gw-0 Ready
+#   online         every client logs in on gw-0
+#   pinning        gw-0, Logic + Redis and loadgen on their CPU sets; a
+#                  failure ends the run
+#   gwconns_set    gw-0's gwconns set has every client (SCARD); its master
+#                  and the lease key's master recorded (P6c1I-6)
+#   pass           the trigger is accepted and the triggered pass ends
+#                  (result done) within 2 x clients / rate + 60s; its
+#                  time, the entries by result, the re-queues and the
+#                  not_owner entries by reason (Kicks: a reading, run 31
+#                  had 4,119) recorded
+#   pass_entries   every client's entry got a definitive result once:
+#                  written + unchanged + not_owner = clients, none dropped
+#                  at the re-queue limit
+#   pass_shards    the pass window's pexpire p99 on the set's master is at
+#                  most max(2x, + FLOOR_US) each other master's; every
+#                  master ran pexpire;
+#                  every node's commands and CPU recorded
+#   end            ReconcileEnd succeeds at its first attempt (none
+#                  retried or dropped), the phase scanned every client,
+#                  and its rate (uids scanned / the phase's time, from
+#                  Logic's histograms) is at least 90% of the delete rate
+#                  (#147.3); the time, its margin to GRACE_S and gw-0's
+#                  deadline (2 x tracked / delete rate + 10s) recorded only
+#   end_shards     the delete window's hgetall p99 on the set's master is at
+#                  most max(2x, + FLOOR_US) each other master's; every
+#                  master ran hgetall
+#   sessions       after the phase, every client's session is in the store,
+#                  on gw-0 (the phase deletes no live session); the fields
+#                  it deleted recorded (0 expected: no ghost)
+#   no_swap_in     no page swapped in from the trigger to the sessions check
+#   idle           after loadgen stops, gw-0 reaches 0 connections
+#   processes      nothing died, panicked or stopped uncleanly
+#   artifacts      the metrics before and after
+set -uo pipefail
+
+. "$(dirname "$0")/lib.sh"
+. "$(dirname "$0")/../stack.sh"
+. "$(dirname "$0")/../loadgen.sh"
+t2_init gwconns
+t2_need curl taskset sudo python3
+t2_require_host_setup
+
+for v in GWCONNS_CLIENTS GWCONNS_CAP GWCONNS_RATE GWCONNS_TRIGGERED_RATE GWCONNS_DELETE_RATE GWCONNS_FLOOR_US GWCONNS_GRACE_S GWCONNS_GATEWAY_PROCS; do
+  case ${!v-1} in ''|*[!0-9]*|0*) echo "gwconns: $v must be a positive whole number" >&2; exit 1 ;; esac
+done
+N=${GWCONNS_CLIENTS:-250000} CAP=${GWCONNS_CAP:-250000} RATE=${GWCONNS_RATE:-5000}
+TR=${GWCONNS_TRIGGERED_RATE:-5000} DR=${GWCONNS_DELETE_RATE:-10000} GRACE=${GWCONNS_GRACE_S:-30} FLOOR=${GWCONNS_FLOOR_US:-50}
+[ "$N" -le "$CAP" ] || { echo "gwconns: GWCONNS_CLIENTS must not exceed GWCONNS_CAP" >&2; exit 1; }
+# 250k light clients: under capacity's 600k budget.
+t2_mem_budget 12
+t2_nofile 1048576
+
+export STACK_PIN=1 STACK_GATEWAYS=1 STACK_REDIS=cluster
+export T2_CPUS_GATEWAY=${GWCONNS_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${GWCONNS_CPUS_LOADGEN:-0-5,10,12-17,22}
+export T2_CPUS_LOGIC=${GWCONNS_CPUS_LOGIC:-11,23}
+export STACK_GATEWAY_PROCS=${GWCONNS_GATEWAY_PROCS:-8}
+# The production cycle (10m) would start a periodic pass at a random time;
+# 24h keeps it out of the run (capacity's choice), its count recorded.
+STACK_GATEWAY_ARGS=(-faults -max-conns "$CAP" -reconcile-cycle 24h -reconcile-jitter 0 -triggered-rate "$TR" -delete-rate "$DR")
+STACK_LOGIC_ARGS=(-delete-rate "$DR")
+LG_ENV=(GOMEMLIMIT="${GWCONNS_LG_MEMLIMIT:-4GiB}")
+STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
+STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+t2_on_exit 'stack_down'
+t2_on_exit 'lg_stop_all' # newest first: loadgen stops before the stack
+
+TRIG='pace="PACE_TRIGGERED"'
+GW_COUNTERS=(
+  passes_done="goim_gateway_reconcile_passes_total:$TRIG,result=\"done\""
+  passes_incomplete="goim_gateway_reconcile_passes_total:$TRIG,result=\"incomplete\""
+  passes_periodic='goim_gateway_reconcile_passes_total:pace="PACE_PERIODIC"'
+  pass_s="goim_gateway_reconcile_pass_seconds_sum:$TRIG"
+  written='goim_gateway_reconcile_entries_total:result="written"'
+  unchanged='goim_gateway_reconcile_entries_total:result="unchanged"'
+  refused='goim_gateway_reconcile_entries_total:result="refused"'
+  not_owner='goim_gateway_reconcile_entries_total:result="not_owner"'
+  no_reply='goim_gateway_reconcile_entries_total:result="no_reply"'
+  requeued=goim_gateway_reconcile_requeued_total
+  requeue_limit=goim_gateway_reconcile_requeue_limit_total
+  ends_ok='goim_gateway_reconcile_end_total:result="ok"'
+  ends_retried='goim_gateway_reconcile_end_total:result="retried"'
+  ends_dropped='goim_gateway_reconcile_end_total:result="dropped"'
+  end_deleted=goim_gateway_reconcile_end_deleted_total
+)
+LOGIC_COUNTERS=(
+  end_s='goim_logic_reconcile_end_seconds_sum:pace="triggered"'
+  ends='goim_logic_reconcile_end_seconds_count:pace="triggered"'
+  scanned='goim_logic_reconcile_end_scanned_uids_sum:pace="triggered"'
+  deleted=goim_logic_reconcile_end_deleted_total
+)
+
+t2_report_extra() {
+  local f
+  for f in "$T2_OUT"/stack/logs/*.log; do
+    [ -f "$f" ] || continue
+    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
+    tail -n 20 "$f"
+  done
+}
+
+# gc_shards LATENCY OWNER CMD FLOOR_US: the rule over one window's
+# stack_redis_latency (#147.2, #148.3, #148.6): {"cmd", "owner", "p99_us"
+# and "calls" per master, "bound_us": max(2 x, + FLOOR_US) the lowest other
+# master's p99, "ok"}. ok: 3 masters
+# answered, OWNER is one, each ran CMD, and OWNER's p99 <= bound_us. null if
+# LATENCY does not parse.
+gc_shards() {
+  python3 -c 'import json, sys
+lat, owner, cmd, floor = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
+m = {n: v["cmds"].get(cmd) or {} for n, v in lat.items() if v and v["role"] == "master"}
+p99 = {n: c.get("p99_us") for n, c in m.items()}
+calls = {n: c.get("calls", 0) for n, c in m.items()}
+others = [p99[n] for n in m if n != owner]
+bound = max(2 * min(others), min(others) + floor) if others and None not in others else None
+ok = len(m) == 3 and owner in m and all(calls.values()) and bound is not None and p99[owner] is not None and p99[owner] <= bound
+print(json.dumps({"cmd": cmd, "owner": owner, "p99_us": p99, "calls": calls, "bound_us": bound, "ok": ok}))' "$@" 2>/dev/null || echo null
+}
+
+# gc_end GW LOGIC CLIENTS RATE GRACE_S: the end check over gw-0's and
+# Logic's counter deltas (#147.3): {"ends_ok", "retried", "dropped",
+# "scanned", "duration_s", "rate", "want_rate" (90% of RATE), "margin_s"
+# (GRACE_S - duration_s), "deadline_s" (gw-0's: 2 x CLIENTS / RATE + 10),
+# "deleted", "ok"}. ok: one ReconcileEnd ok, none retried or dropped, at
+# least CLIENTS scanned, and rate >= want_rate. null if a delta is unreadable.
+gc_end() {
+  python3 -c 'import json, sys
+g, l = json.loads(sys.argv[1]), json.loads(sys.argv[2])
+n, rate, grace = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
+z = lambda d, k: d.get(k) or 0
+dur, scanned = l.get("end_s"), z(l, "scanned")
+r = round(scanned / dur, 1) if dur else None
+ok = z(g, "ends_ok") == 1 and z(g, "ends_retried") == 0 and z(g, "ends_dropped") == 0 and z(l, "ends") == 1 and scanned >= n and r is not None and r >= 0.9 * rate
+print(json.dumps({"ends_ok": z(g, "ends_ok"), "retried": z(g, "ends_retried"), "dropped": z(g, "ends_dropped"),
+                  "scanned": scanned, "duration_s": None if dur is None else round(dur, 3), "rate": r, "want_rate": 0.9 * rate,
+                  "margin_s": None if dur is None else round(grace - dur, 3), "deadline_s": 2 * n / rate + 10,
+                  "deleted": l.get("deleted"), "ok": ok}))' "$@" 2>/dev/null || echo null
+}
+
+# gc_wait_inc NAME METRIC MATCH FROM SECONDS: polls (every 0.2s) NAME's
+# METRIC samples with MATCH (stack_metric) until their sum exceeds FROM
+# (null counts as 0); fails after SECONDS.
+gc_wait_inc() {
+  local end=$((SECONDS + $5)) from=${4/null/0} v
+  while :; do
+    v=$(stack_metric "$1" "$2" "$3")
+    [ "$v" != null ] && [ "$(awk -v a="$v" -v b="$from" 'BEGIN { print (a > b) }')" = 1 ] && return 0
+    [ $SECONDS -lt "$end" ] || return 1
+    sleep 0.2
+  done
+}
+
+t=$SECONDS
+if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
+  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
+  exit 1
+fi
+STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
+t2_duration setup $((SECONDS - t))
+
+t0=$SECONDS
+export STACK_DIR=$T2_OUT/stack
+mkdir -p "$STACK_DIR"
+ln -sfn ../bin "$STACK_DIR/bin"
+busy=$(stack_busy_ports)
+[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
+stack_up 2>"$STACK_DIR/stack-up.err"
+rc=$?
+t2_check stack "$(t2_bool [ $rc -eq 0 ])" "\"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"logic_args\": $(t2_json_str "${STACK_LOGIC_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
+[ $rc -eq 0 ] || { stack_down; t2_finish; }
+lg_start main -mode light -clients "$N" -rate "$RATE" -uid-prefix main
+t=$SECONDS
+online=false
+lg_wait_online "$N" $((N / RATE * 3 + 60)) && online=$(lg_get /stats 'd["online_by_gateway"].get("gw-0") == '"$N")
+[ "$online" = true ] || online=false
+ramp_s=$((SECONDS - t))
+t2_check online "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
+wrong=""
+stack_pinned gw-0 "$T2_CPUS_GATEWAY" || wrong+=" gw-0"
+for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+have=$(taskset -pc "$LG_PID" 2>/dev/null)
+[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
+t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
+[ -z "$wrong" ] && [ "$online" = true ] || { lg_stop_all; stack_down; t2_finish; }
+t2_duration ramp $((SECONDS - t0))
+
+# The set: its master (the lease key's: one hash tag, the epoch) and size.
+lease=$(stack_lease_key gw-0) set=${lease/#gwlease:/gwconns:}
+owner=$(stack_redis_owner "$set") lease_owner=$(stack_redis_owner "$lease")
+scard=$(STACK_REDIS_TIMEOUT=5 stack_redis_node_cli "${owner:-redis-0}" scard "$set" 2>/dev/null)
+case $scard in '' | *[!0-9]*) scard=null ;; esac
+t2_check gwconns_set "$(t2_bool [ -n "$owner" ] && [ "$scard" = "$N" ])" \
+  "\"key\": \"$set\", \"owner\": \"$owner\", \"lease_owner\": \"$lease_owner\", \"members\": $scard, \"want\": $N, \"roles\": $(stack_redis_roles)"
+stack_snapshot before
+admin=$(stack_admins | awk '$1 == "gw-0" { print $2 }')
+
+# The pass window, from the trigger (#147.1).
+t2_record_start
+g0=$(stack_counters gw-0 "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+redis_reset=true; stack_redis_resetstat || redis_reset=false
+stack_usage_start loadgen="$LG_PID"
+trig_ms=$(date +%s%3N)
+trigger=$(t2_bool curl -sf -m 5 -X POST "http://$admin/fault/action/reconcile" -o /dev/null)
+pass_wait=$((2 * N / TR + 60))
+done=false pass_ms=null
+[ "$trigger" = true ] && gc_wait_inc gw-0 goim_gateway_reconcile_passes_total "$TRIG,result=\"done\"" \
+  "$(echo "$g0" | lg_py - 'd["passes_done"]')" "$pass_wait" && { done=true; pass_ms=$(($(date +%s%3N) - trig_ms)); }
+pass_lat=$(stack_redis_latency)
+pass_use=$(stack_usage_json)
+# The delete window, from the pass's end.
+redis_reset2=true; stack_redis_resetstat || redis_reset2=false
+stack_usage_start loadgen="$LG_PID"
+end_t0=$(date +%s%3N)
+g1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
+end_wait=$((2 * N / DR + 10 + 90)) # gw-0's deadline, then its retries (60s) and a margin
+replied=false end_ms=null
+[ "$done" = true ] && gc_wait_inc gw-0 goim_gateway_reconcile_end_total 'result="ok"' \
+  "$(echo "$g1" | lg_py - 'd["ends_ok"]')" "$end_wait" && { replied=true; end_ms=$(($(date +%s%3N) - end_t0)); }
+end_lat=$(stack_redis_latency)
+end_use=$(stack_usage_json)
+g2=$(stack_counters gw-0 "${GW_COUNTERS[@]}") l2=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+gp=$(stack_counters_delta "$g0" "$g1") gd=$(stack_counters_delta "$g1" "$g2") ld=$(stack_counters_delta "$l0" "$l2")
+not_owner=$(stack_metrics gw-0 | awk '$1 ~ /^goim_gateway_reconcile_not_owner_total\{/ { r = $1; sub(/.*reason="/, "", r); sub(/".*/, "", r); out = out (out == "" ? "" : ", ") "\"" r "\": " $NF } END { print "{" out "}" }')
+
+t2_check pass "$(t2_bool [ "$done" = true ])" \
+  "\"trigger\": $trigger, \"pass_ms\": $pass_ms, \"wait_s\": $pass_wait, \"triggered_rate\": $TR, \"counters\": $gp, \"not_owner_by_reason\": ${not_owner:-null}"
+t2_check pass_entries "$(echo "${gp:-null}" | lg_true - '(d["written"] or 0) + (d["unchanged"] or 0) + (d["not_owner"] or 0) == '"$N"' and (d["requeue_limit"] or 0) == 0')" \
+  "\"want\": $N, \"entries\": $(echo "${gp:-null}" | lg_py - '{k: d[k] for k in ("written", "unchanged", "refused", "not_owner", "no_reply", "requeued", "requeue_limit")}')"
+ps=$(gc_shards "${pass_lat:-null}" "$owner" pexpire "$FLOOR")
+t2_check pass_shards "$(echo "${ps:-null}" | lg_true - 'd["ok"]')" "\"rule\": ${ps:-null}, \"redis_reset\": $redis_reset"
+end=$(gc_end "${gd:-null}" "${ld:-null}" "$N" "$DR" "$GRACE")
+t2_check end "$(echo "${end:-null}" | lg_true - 'd["ok"]')" "\"replied\": $replied, \"reply_ms\": $end_ms, \"wait_s\": $end_wait, \"delete_rate\": $DR, \"grace_s\": $GRACE, \"reading\": ${end:-null}"
+es=$(gc_shards "${end_lat:-null}" "$owner" hgetall "$FLOOR")
+t2_check end_shards "$(echo "${es:-null}" | lg_true - 'd["ok"]')" "\"rule\": ${es:-null}, \"redis_reset\": $redis_reset2"
+sessions=$(t2_sessions 0)
+t2_check sessions "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
+  "\"sessions\": ${sessions:-null}, \"deleted\": $(echo "${ld:-null}" | lg_py - 'd["deleted"]')"
+t2_record_end
+t2_field pass_window "{\"owner\": \"$owner\", \"pass_ms\": $pass_ms, \"counters\": ${gp:-null}, \"redis_latency\": ${pass_lat:-null}, \"usage\": ${pass_use:-null}}"
+t2_field delete_window "{\"owner\": \"$owner\", \"reply_ms\": $end_ms, \"gateway\": ${gd:-null}, \"logic\": ${ld:-null}, \"redis_latency\": ${end_lat:-null}, \"usage\": ${end_use:-null}}"
+t2_duration reconcile $((SECONDS - t0))
+
+stack_snapshot after
+lg_stop_all
+t=$SECONDS
+idle=$(t2_bool stack_wait_idle 600 2>"$STACK_DIR/idle.err")
+t2_check idle "$idle" "\"stop_s\": $((SECONDS - t)), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
+stack_down
+problems=$(stack_health)
+t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
+missing=""
+a=$STACK_DIR/artifacts
+for f in before/logic after/logic before/gw-0 after/gw-0; do
+  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
+done
+t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
+t2_field run "{\"clients\": $N, \"cap\": $CAP, \"rate\": $RATE, \"triggered_rate\": $TR, \"delete_rate\": $DR, \"grace_s\": $GRACE, \"floor_us\": $FLOOR, \"ramp_s\": $ramp_s, \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}")}"
+
+t2_not_covered "a delete phase cut at a grace (MaxConnectionAgeGrace, ShutdownGrace): no Logic SIGTERM phase (#147.4); the race suite pins a cut ReconcileEnd's retry and the idempotent deletes" \
+  "ghosts: every client is live, so the delete phase deletes nothing and runs no compare-and-delete script" \
+  "the 600k benchmark's ReconcileEnd (4.8MB): capacity's P_reconcile_end (CAPACITY_RECONCILE_CYCLE set)" \
+  "several gateways' passes at once: one gateway (#148.1)"
+t2_finish
diff --git a/scripts/tier2/gwconns_test.sh b/scripts/tier2/gwconns_test.sh
new file mode 100755
index 0000000..79514eb
--- /dev/null
+++ b/scripts/tier2/gwconns_test.sh
@@ -0,0 +1,76 @@
+#!/usr/bin/env bash
+# Tests for scripts/tier2/gwconns.sh's readers (make test-scripts): the
+# shard rule (gc_shards: the set's master's p99 at most max(2x, + floor)
+# each other master's, #147.2, #148.3, #148.6), the end check (gc_end: one ReconcileEnd at the
+# first attempt, every uid scanned, the rate at least 90% of the delete
+# rate, #147.3) and the counter wait (gc_wait_inc). The scenario itself
+# runs on the Tier 2 host.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/loadgen.sh
+tmp=$(mktemp -d)
+trap 'rm -rf "$tmp"' EXIT
+T2_TMP_READS=$tmp/reads
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+eval "$(sed -n '/^gc_shards() {/,/^}/p; /^gc_end() {/,/^}/p; /^gc_wait_inc() {/,/^}/p' scripts/tier2/gwconns.sh)"
+
+# stack_redis_latency on a Cluster: masters redis-0..2 with evalsha p99
+# A, B, C (µs), replicas redis-3..5; redis-1 holds the set.
+lat() {
+  python3 -c 'import json, sys
+a, b, c = (float(x) if "." in x else int(x) for x in sys.argv[1:4])
+calls, down = int(sys.argv[4]), sys.argv[5] == "down"
+m = lambda z, p, n=9: {"role": "master", "zone": z, "cmds": {"evalsha": {"calls": n, "p50_us": 10, "p99_us": p, "p999_us": 900}}}
+d = {"redis-0": m("z0", a), "redis-1": m("z1", b, calls), "redis-2": None if down else m("z2", c),
+     "redis-3": {"role": "replica", "zone": "z1", "cmds": {"evalsha": {"calls": 9, "p50_us": 1, "p99_us": 9999, "p999_us": 9999}}},
+     "redis-4": {"role": "replica", "zone": "z2", "cmds": {}}, "redis-5": None}
+d["redis-1"]["cmds"]["sadd"] = {"calls": 9, "p50_us": 1, "p99_us": 400, "p999_us": 900}
+print(json.dumps(d))' "$1" "$2" "$3" "${4:-9}" "${5:-up}"
+}
+s=$(gc_shards "$(lat 120 240 150)" redis-1 evalsha 50)
+expect "shards: 240 within 2 x the lower other (120)" "$(get "$s" 'd["ok"], d["bound_us"]')" "(True, 240)"
+expect "shards: masters only, per master" "$(get "$s" 'd["p99_us"], d["calls"]')" "({'redis-0': 120, 'redis-1': 240, 'redis-2': 150}, {'redis-0': 9, 'redis-1': 9, 'redis-2': 9})"
+expect "shards: 241 over 2 x 120" "$(get "$(gc_shards "$(lat 120 241 150)" redis-1 evalsha 50)" 'd["ok"]')" False
+expect "shards: against each other master, not the higher" "$(get "$(gc_shards "$(lat 120 290 150)" redis-1 evalsha 50)" 'd["ok"]')" False
+expect "shards: the owner lower than the others" "$(get "$(gc_shards "$(lat 120 50 150)" redis-1 evalsha 50)" 'd["ok"]')" True
+expect "shards: the owner ran no evalsha" "$(get "$(gc_shards "$(lat 120 100 150 0)" redis-1 evalsha 50)" 'd["ok"]')" False
+expect "shards: a master without the command" "$(get "$(gc_shards "$(lat 120 100 150)" redis-1 sadd 50)" 'd["ok"], d["p99_us"]')" "(False, {'redis-0': None, 'redis-1': 400, 'redis-2': None})"
+expect "shards: a master that did not answer" "$(get "$(gc_shards "$(lat 120 100 150 9 down)" redis-1 evalsha 50)" 'd["ok"], sorted(d["p99_us"])')" "(False, ['redis-0', 'redis-1'])"
+expect "shards: the owner not a master" "$(get "$(gc_shards "$(lat 120 100 150)" redis-3 evalsha 50)" 'd["ok"]')" False
+expect "shards: one bucket over 2x, within the floor" "$(get "$(gc_shards "$(lat 7.007 14.015 7.007)" redis-1 evalsha 50)" 'd["ok"], d["bound_us"]')" "(True, 57.007)"
+expect "shards: over the floor" "$(get "$(gc_shards "$(lat 7.007 57.008 9.0)" redis-1 evalsha 50)" 'd["ok"]')" False
+expect "shards: no floor, one bucket over 2x" "$(get "$(gc_shards "$(lat 7.007 14.015 7.007)" redis-1 evalsha 0)" 'd["ok"]')" False
+expect "shards: an unreadable reading" "$(gc_shards null redis-1 evalsha 50)" null
+
+# gc_end over gw-0's and Logic's deltas: 250k at 10k/s, a 26s phase.
+gw() { echo "{\"ends_ok\": ${1:-1}, \"ends_retried\": ${2:-0}, \"ends_dropped\": ${3:-null}}"; }
+lg() { echo "{\"end_s\": ${1:-26.0}, \"ends\": ${2:-1}, \"scanned\": ${3:-250000}, \"deleted\": 0}"; }
+e=$(gc_end "$(gw)" "$(lg)" 250000 10000 30)
+expect "end: 250k in 26s" "$(get "$e" '[d[k] for k in ("ok", "rate", "want_rate", "margin_s", "deadline_s", "deleted")]')" "[True, 9615.4, 9000.0, 4.0, 60.0, 0]"
+expect "end: 9,000 uids/s is 90%" "$(get "$(gc_end "$(gw)" "$(lg 27.7778)" 250000 10000 30)" 'd["ok"]')" True
+expect "end: below 90%" "$(get "$(gc_end "$(gw)" "$(lg 27.8)" 250000 10000 30)" 'd["ok"], d["rate"]')" "(False, 8992.8)"
+expect "end: a retry fails" "$(get "$(gc_end "$(gw 1 1)" "$(lg)" 250000 10000 30)" 'd["ok"], d["retried"]')" "(False, 1)"
+expect "end: a drop fails" "$(get "$(gc_end "$(gw 0 0 1)" "$(lg)" 250000 10000 30)" 'd["ok"]')" False
+expect "end: no reply" "$(get "$(gc_end "$(gw null)" "$(lg null 0 0)" 250000 10000 30)" 'd["ok"], d["rate"], d["margin_s"]')" "(False, None, None)"
+expect "end: two phases" "$(get "$(gc_end "$(gw 2)" "$(lg 26.0 2 500000)" 250000 10000 30)" 'd["ok"]')" False
+expect "end: an earlier phase in Logic's window" "$(get "$(gc_end "$(gw)" "$(lg 52.0 2 500000)" 250000 10000 30)" 'd["ok"]')" False
+expect "end: not every uid scanned" "$(get "$(gc_end "$(gw)" "$(lg 20.0 1 249999)" 250000 10000 30)" 'd["ok"]')" False
+expect "end: over the grace is recorded only" "$(get "$(gc_end "$(gw)" "$(lg)" 250000 10000 25)" 'd["ok"], d["margin_s"]')" "(True, -1.0)"
+expect "end: an unreadable delta" "$(gc_end null "$(lg)" 250000 10000 30)" null
+
+# gc_wait_inc: readings null, 3, 3, then 4 over a start of 3.
+printf '%s\n' null 3 3 4 >"$T2_TMP_READS"
+stack_metric() { local v; v=$(head -n 1 "$T2_TMP_READS"); sed -i 1d "$T2_TMP_READS"; echo "${v:-null}"; }
+sleep() { :; }
+expect "wait: until the sum exceeds the start" "$(gc_wait_inc gw-0 m x 3 5 && echo done; cat "$T2_TMP_READS")" done
+printf '%s\n' null 1 >"$T2_TMP_READS"
+expect "wait: a null start counts as 0" "$(gc_wait_inc gw-0 m x null 5 && echo done)" done
+SECONDS=0
+expect "wait: fails after SECONDS" "$(gc_wait_inc gw-0 m x 3 0 || echo timeout)" timeout
+
+if [ "$fails" -eq 0 ]; then echo "gwconns_test: all passed"; else echo "gwconns_test: $fails failure(s)"; exit 1; fi
````

- [ ] **Step 2: Run the tests to see them fail**

Run: `scripts/tier2/gwconns_test.sh; echo "exit $?"`
Expected: `sed: can't read scripts/tier2/gwconns.sh`, `gc_shards: command not found`; the last line `gwconns_test: 26 failure(s)`, `exit 1` (of 27 checks only `wait: fails after SECONDS` passes: a missing function also "fails").

- [ ] **Step 3: The scenario**

Apply the rest of the same patch: `git apply --exclude=Makefile --exclude='*_test.sh' PATCH`, then `chmod +x scripts/tier2/gwconns.sh scripts/tier2/gwconns_test.sh` (the patch's mode is `100755`; `git apply` sets it, the `chmod` is a no-op then). `bash -n scripts/tier2/gwconns.sh` prints nothing.

- [ ] **Step 4: Run the tests to see them pass; the mutations**

Run: `scripts/tier2/gwconns_test.sh`
Expected: 27 `ok` lines, the last `gwconns_test: all passed`.

Then each mutation below on `scripts/tier2/gwconns.sh` alone, the tests run, the file restored (`git checkout scripts/tier2/gwconns.sh` is not possible before the commit: copy it to the scratchpad first and copy it back). Each must give at least one `FAIL`:

| sed on `gwconns.sh` | what it breaks | FAIL lines |
|---|---|---|
| `s/bound = max(2 \* min(others), min(others) + floor)/bound = 2 * min(others)/` | the floor (#148.6) | 1 |
| `s/max(2 \* min(others), min(others) + floor)/max(2 * max(others), max(others) + floor)/` | against the higher master, not each (#148.3) | 4 |
| `s/r >= 0.9 \* rate/r >= 0.8 * rate/` | the 90% rate (#147.3) | 1 |
| `s/z(g, "ends_retried") == 0 and //` | a retried ReconcileEnd passes | 1 |
| `s/z(l, "ends") == 1/z(l, "ends") >= 1/` | two delete phases pass (Review Focus 2) | 1 |
| `s/(a > b)/(a >= b)/` | the wait returns at the start value | 1 |

- [ ] **Step 5: The suites**

Run: `make test-scripts`
Expected: every script's last line `all passed` or `0 failure(s)`, `gwconns_test: all passed` among them; exit 0.

- [ ] **Step 6: Commit**

```bash
git add Makefile scripts/tier2/gwconns.sh scripts/tier2/gwconns_test.sh
git commit -m "test: add the Tier 2 scenario gwconns" -m "One gateway at the cap runs a triggered Reconcile pass. The
scenario starts it through the test fault API. It compares the
set's master with the other masters in two windows: pexpire in
the pass, hgetall in the delete phase. The delete phase is gated
on its rate; its time and margin are recorded." -m "Refs: #147, #148, R4M-9, R5S-O8"
```
(with the session's attribution line). `git diff proto-6c3 -- Makefile scripts/` is empty.

---

### Task 2: Dry run, run 36, and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (the plan 6 row, #148's outcome)

**Interfaces:**
- Consumes: Task 1 on `feat/plan6c3`, pushed.

- [ ] **Step 1: Dry run on the dev machine**

The failover recipe (Plan 6c2b's Task 3 Step 5) on `gwconns.sh`: a throwaway copy on the dev machine's 2 CPUs, 5,000 clients, rates scaled so the pass takes ~33s and the delete phase ~10s (Logic refuses a delete rate below its 500-uid page); ~2 minutes. With `S` the session scratchpad:
```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/gwconns.sh "$D/gwconns.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/gwconns.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/gwconns.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/gwconns.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/gwconns.sh"
rm -rf tier2-results/gwconns-*
GOIM_TEST_TIER=2 GWCONNS_CPUS_GATEWAY=0-1 GWCONNS_CPUS_LOGIC=0-1 GWCONNS_CPUS_LOADGEN=0-1 GWCONNS_GATEWAY_PROCS=1 \
  GWCONNS_CLIENTS=5000 GWCONNS_RATE=500 GWCONNS_TRIGGERED_RATE=150 GWCONNS_DELETE_RATE=500 GWCONNS_LG_MEMLIMIT=512MiB \
  bash "$D/gwconns.sh" >"$D/out.txt" 2>&1; echo "exit $?"
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/gwconns-*/result.json")[0])); c=d["checks"]; print(len(c), [k for k, v in c.items() if not v["pass"]]); print(c["pass_shards"]["rule"]); print(c["end_shards"]["rule"]); print(c["end"]["reading"], c["pass"]["pass_ms"])'
```
The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo. (`.cache/redis` must hold the pinned build: `make redis-server` once, or in a worktree a link to the main checkout's `.cache/redis`.)

Expected (prototype): `exit 1`; **14** checks; failing only `no_swap_in` (the dev machine's swap; it may also pass). `pass_ms` ~32,000–34,000; `pass_shards` and `end_shards` `ok` with 3 masters and ~1,650 (`pexpire`) and ~1,500 (`hgetall`) calls each; `end`: `scanned` 5,000, `rate` 500–600, `deleted` 0, `retried` 0. Then `rm -rf tier2-results/gwconns-*`.

- [ ] **Step 2: The user runs run 36**

Ask the user to run, on the Tier 2 host, from a fresh pull of `feat/plan6c3`, with the desktop apps closed (after `sudo scripts/tier2/host-setup.sh` if the host rebooted since the last run):
```bash
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwconns
```
and to send back `tier2-results/gwconns-<timestamp>.report.txt`. About 5 minutes.

- [ ] **Step 3: Read the report**

Expected: exit 0, **14/14** checks. Read and record:
- `gwconns_set`: the set's master and the lease key's (the same: one hash tag), `members` 250,000.
- `pass`: `pass_ms` (~50s at 5,000/s), the entries by result, `requeued`, `not_owner_by_reason` (a reading; run 31 had 4,119 on gw-0 after a reset storm; 0 expected at rest), `passes_periodic` (Review Focus 2).
- `pass_shards`, `end_shards`: the probe's p99 and calls per master, `bound_us`, `probe_errors`, `service_time`; beside them from `pass_window`/`delete_window`: the set's master's `sadd`, `sscan`, `srem` (calls, p99, p99.9), `hget`/`hmget` per master, and each node's CPU (`usage`) (Review Focus 1, 4).
- `end`: `duration_s` (~25s), `rate`, `margin_s` to 30s, `deadline_s` (60), `reply_ms`, `deleted`.
- `sessions`: 250,000 found.
- Run 33's per-master `evalsha` p99 (119–124µs) is from a login storm; this run's commands differ (#148.5), so it is context, not a baseline.

A failing check: record it with its numbers and ask the user (AskUserQuestion) before any change. Whether the margin to 30s is enough, or the graces change, is the user's decision from this run (#147.3).

- [ ] **Step 4: Record and commit**

In this plan's *Execution Notes*: the dry run and run 36's numbers as in Step 3. In the roadmap: the plan 6 row (6c3 executed, run 36 and its result) and #148's outcome; the margin's decision as the user takes it. Commit (docs), push.

---

## After this plan

- The spec's next revision: the compared commands (`pexpire`, `hgetall`) and the floor for the `gwconns` shard rule (#148.5–6), beside #139–#142 and #145.
- Plan 2's deferred minor (the ReconcileEnd and drain deadlines against the graces) is closed by run 36's margin, or reopened by the user's decision on it.
- Then **6d** (Tier 3, k8s: rolling restart, the LB).

## Execution Notes

**Executed 2026-11-25** (superpowers:subagent-driven-development, the user's choice after a start inline was stopped before any step ran), on `feat/plan6c3` from `c492283`.
- Task 1: `2c8742f`, byte for byte the plan's patch (`git diff proto-6c3` empty). Step 2: 26 failures; Step 4: 27 ok; the 6 mutations 1, 4, 1, 1, 1, 1 FAIL; `make test-scripts` green. Task review (Sonnet): spec ✅, approved; minors deferred: ~1s of the delete phase in the pass window (Review Focus 1); `t2_sessions 0` never flags `stale` (no re-login in the window).
- Task 2 Step 1 (dry run on `2c8742f`): exit 1, 14 checks, only `no_swap_in` (151 pages); pass 32,155ms; `pexpire` 6.0/6.0/8.0µs, `hgetall` 7.0/8.0/12.0µs; delete phase 9.0s at 555 uids/s, deleted 0, retried 0.
- **Final review (Opus): 0 Critical, 1 Important, 3 Minor.**
  - **I-1: the gate read Redis's service time.** `INFO latencystats` times a command's own run; a command queued behind an `sscan` page or a `sadd` still shows a few µs, so the gate could not see the head-of-line blocking R4M-9 is about. The user chose (#148.7): a client-side probe, gated. Fixed `a845331` (`gc_probe`, `gc_probe_latency`, `gc_probe_errors`; 8 new tests failed first). The scoped re-review found the probe outliving its kill (python a child of `bash -c`); fixed `23050a1` (`exec python3`; test `probe: killing its PID stops it` failed first), with each master probed once (`sort -u`) and the floor 200µs (the user's choice: the dev machine's probe p99 is ~1.4–2.0ms on every master, scheduler noise on 2 shared CPUs, so it gives no number; re-checked after run 36).
  - M-1: Review Focus 2's premise (corrected above). M-2 (`redis_reset` not gated): not fixed, moot once the gate reads the probe. M-3 (an empty lease key named slot 0's master as the owner): fixed in `a845331`.
  - The patch in Task 1 is the code before these fixes; the code on the branch (`23050a1`) supersedes it.
- Dry run on `23050a1`: exit 1, 14 checks, only `no_swap_in`; probe p99 pass 1,799/2,039/1,885µs (bound 3,598), delete 1,843/1,944/1,425µs (bound 3,686), 0 probe errors; no probe left after the run.
- **Next: the user runs run 36** (Task 2 Steps 2–4).
