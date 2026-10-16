# Plan 6b2b-b2: Silent Loss of a 200k Gateway (Tier 2 (b2)) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's load test (b2) on the Tier 2 host with SDK clients. A gateway holding 200k clients goes silent: iptables DROP on its client and Kick ports, then SIGKILL behind the DROP. Its gateways send a 30s heartbeat (review O1). Check that 99% of its clients log in again within 30s of detecting the loss, that detection stays within one heartbeat plus the Pong timeout, that the survivor's Login p99 is unaffected by Kicks that time out, and that Logic's goroutine count stays bounded.

**Architecture:** `scripts/tier2/gwloss.sh` gains a third phase, `b2`, between `b1` and `b_stress` in the default order. It uses b1's layout (2 gateways × 200k, `gw-1` the victim, b1's knobs), with the gateways at `-heartbeat 30s`: the SDK's Pings keep their login's phase, so at 150s the relogins would come in one ~80s band set by the script's timing, after the lease and often after the sweep (review O1). At the kill, `goim-netdrop add` puts DROP rules on the victim's client port and Kick port. Loadgen's `/kill` then takes the mark (`drop`) and SIGKILLs the process. The dying kernel's FINs and RSTs never reach a client, and Logic's Kick connects get no answer, so clients find the loss only by Pong timeout and the lease ends as in a crash (#112). After the storm window the rules come out, so the dead port's leftover sockets end by RST. `stack.sh` gains a histogram reader, so the scenario can compare the survivor's Login p99 in the storm with its p99 in the ramp. Loadgen needs no change: `/kill` takes any mark name, and `/slo` already reports `from_detection`, `detection` and `pass_from_detection`.

**Tech Stack:** bash 5.2, the pinned `redis-server` 7.4.11, curl, python3 (reads JSON only), iptables through the root-owned `/usr/local/sbin/goim-netdrop` (#68; installed by `sudo scripts/tier2/host-setup.sh`, already done on the Tier 2 host).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here: *Verification → Load tests* ("(b2) iptables DROP on a 200k gateway (silent node loss): 99% logged in again within 30s of client detection; Login p99 is unaffected by Kicks to the dropped gateway, and Logic's goroutine count stays bounded *(R5S-O2)*"); *Goals* (node loss / silent partition: "Detection is bounded by design: at most one heartbeat interval plus the Pong timeout"); *Failure handling* (node loss / silent partition: "Server side as for a crash. Kicks to the lost pod time out after ~1s each and never delay relogins; overflow is dropped"); *Logic → Hardened Kick*. Inputs: roadmap #94 (6b2b's scope, amended 2026-10-14), plan 6b2a-pinger's *Execution Notes* ("For 6b2b": b2 is the first use at scale of the pinger's timeout path).

**Decisions (2026-10-15, the user, with this plan):** #112 (b2's victim: DROP, then SIGKILL) and #114 (6b2b split into 6b2b-b2 and 6b2b-slow, both written now, each executed in its own session). See *Implementation choices*.

**Review (2026-10-15):** objections O1–O7 (`docs/superpowers/objections/connection-session-core-06b2b-b2-silent-loss.md`) decided and applied (`…-06b2b-b2-silent-loss-dispositions.md`): a 30s heartbeat in b2 so relogins meet a live lease and come before the sweep, with a floor on the Kicks exercised (O1); Redis latency beside the Login p99 (O2); expected numbers recomputed (O3); storm profiles 15s after the mark (O4); the interrupt check cues on the rules (O5); a cross-check floor (O6); no rules, no kill (O7).

**Branch:** `feat/plan6b2b-b2`, from `main` (`8a9a5e4`: 6b2a closed, README current). The plan files are committed on `feat/plan6b2b`, which branches from `8a9a5e4` too; start the execution branch from that branch's head, so the plan is on it.

**Size:** 3 tasks. The prototype (a scratch worktree at `8a9a5e4`) ran every block. Tasks 1 and 2 give each change as a `git apply` patch, so execution replays it byte for byte. A prototype dry run with a fake `goim-netdrop` passed every check that can pass without a real DROP (see Task 2, Step 2).

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`. No Go change in this plan.
- `make test`, `make test-race-suite` and `make test-scripts` stay green on the dev machine (2 cores / 2GB), loopback only.
- Scripts: bash with `set -uo pipefail`; `scripts/tier2/lib.sh` writes JSON without jq; python3 only reads JSON and does arithmetic on it. The host's awk is mawk: no gawk-only features (`stack_buckets` uses `match`/`RSTART`/`RLENGTH` only).
- Ports (#76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`. Gateway `i` listens on base+100+10i, Kick on base+101+10i. Loadgen's control API is base+5.
- CPU sets (#71) for b2, as b1: gateways `4-9,16-21` (6 `GOMAXPROCS` each), Logic + Redis `10,11,22,23`, loadgen `0-3,12-15`.
- The one root action is `sudo -n /usr/local/sbin/goim-netdrop add|del|flush|list` (#68). It takes a port and DROPs TCP to and from that port on `lo` (INPUT, by `--dport` and by `--sport`). Nothing else runs as root.
- Spec numbers: heartbeat 150s (gateway `-heartbeat` default, sent in `LoginResp`; b2's gateways send 30s, the SDK's and gateway's lowest, review O1), Pong timeout 10s (SDK), lease TTL 30s renewed every 10s, sweep 2 × TTL after the last renewal at 10,000 uids/s. Kick: 1s deadline per attempt, up to 3 retries 10s apart, only while the target's lease is live; queue 1024 and 64 workers per target; a full queue drops the attempt (`kick_dropped`). The SDK waits a uniform 0–20s after losing an established connection.
- b2 runs on a fresh stack like every phase, so a phase before it never leaves DROP rules: the rules come out at the end of b2's storm, and the run's exit hook flushes every `goim-netdrop` rule however the run ends.
- Swap during a measured window fails it (#70); the user closes the desktop apps before the run.
- Comments and names match the surrounding code; new behaviour cites its choice number (#112) or the spec's id.

## Review Focus

1. **A run interrupted inside b2 must not leave DROP rules.** Rules left behind make the next run's `stack_busy_ports` refuse the stack (a connect to a DROPped port hangs, so it counts as busy), and they silence that port for anything else on the host. Expected: the run's exit hook (`sudo -n goim-netdrop flush`) removes them on Ctrl-C, on a failed check that ends the run early, and on a normal end. Pinned by Task 3, Step 1 (interrupt a small b2 after the kill, then `goim-netdrop list` is empty) and by `b2_netdrop`'s `rules_left` 0 on a normal end.
2. **The victims must detect the loss by Pong timeout, never by a FIN or RST.** If a FIN or RST leaks past the DROP (the rules going in after the SIGKILL, or a rule missing one direction), detection takes milliseconds and the from-detection SLO passes trivially. Expected: every victim's end is `pong-timeout`, and detection lies between ~10s and 40s after the mark (its next Ping, at most one 30s heartbeat away, plus 10s). Pinned by `b2_detection` (`lost["pong-timeout"]` ≥ the population, `max_ms` ≤ 45s) and by `b2_netdrop` (4 rules in before the kill). The dev dry run cannot show it (no iptables there): it fails `b2_detection` on purpose, and Task 3's Tier 2 run is the test.
3. **Kicks to a silent gateway must neither delay Logins nor pile up goroutines.** Each first attempt waits up to the 1s deadline: gRPC's fail-fast RPCs wait while a connection is CONNECTING, and fail at once in TRANSIENT_FAILURE (result `error`). While CONNECTING, 64 workers serve ~64 attempts/s against relogins peaking at ~6,700/s (detection 10–40s plus the SDK's 0–20s wait: 10–60s, flat from 30s to 40s), so most attempts are dropped by the full queue; in TRANSIENT_FAILURE they fail at once and drain it. The lease ends 20–30s after the kill, so 8–33% of relogins Kick a live lease and keep their retries. Nearly all (~92%) come before the sweep. Expected: Login p99 in the storm is within one histogram bucket of the ramp's, Logic's goroutines grow by at most 500, no Kick is refused, some are retried, none gives up as live. Pinned by `b2_login_p99`, `b2_goroutines` and `b2_kicks`.
4. **About 400k orphaned sockets after the DROP.** The killed gateway's 200k sockets and loadgen's 200k victim sockets, closed at Pong timeout, keep sending FINs or retransmissions into the DROP until the rules come out. Expected: the survivor and its 200k clients are unaffected (other ports). Once `goim-netdrop del` runs, the retransmissions get RSTs and the sockets end before the next phase's stack starts. If the kernel hits `tcp_max_orphans` it resets orphans itself ("too many orphaned sockets" in `dmesg`), which is harmless here. Pinned by `b2_lease` and `b2_idle` (the survivor), and by b_stress's `b_stress_stack` in the full run (ports free after b2).
5. **`no_lo_drops_in_b2` must not count the DROP rules.** It reads `/sys/class/net/lo/statistics/rx_dropped`, the netdev input drops. Netfilter drops are not counted there, so the check stays meaningful (loopback backlog overflow, run 14). Expected: 0. If Task 3's run shows drops on `lo` only in b2, compare `tcp.storm` with b1's before suspecting the backlog. A netfilter drop showing up in `rx_dropped` would be a finding to record, not a fix to make in this plan.

## Implementation choices this plan makes

Decided 2026-10-15 by the user (#112, #114), or proposed with this plan for the user's review:

1. **#112: the victim is DROPped, then SIGKILLed** (decided). `goim-netdrop` matches ports only, so it can silence the victim's client and Kick ports but not its outbound lease connection to Logic. With the process alive, the victim would keep its lease, Kicks would retry until they give up as live, and the victim would close its own idle clients and send their Logouts. That is an asymmetric partition, not the spec's node loss ("server side as for a crash"). So the rules go in first and loadgen's `/kill` SIGKILLs behind them. The lease ends 20–30s after the kill, Kicks time out instead of being refused, and the dead epoch is swept as in b1. Loadgen takes the mark at the SIGKILL, a few ms after the DROP. That gap is noise next to the ≥10s detection, and b2's SLO counts from detection.
2. **b2 reuses b1's layout and knobs** (`GWLOSS_CLIENTS`, `_RATE`, `_CAP`, `_SWEEP_WAIT`, `_SOURCES`), so b1 and b2 differ in how the victim dies and in the heartbeat (choice 7). b2 adds five knobs: the heartbeat (30s), the back wait (120s: detection ≤ 45s, then the 0–20s storm wait), the detection bound (45s), the profile offset (15s, choice 8), and the goroutine growth bound (500).
3. **Login p99 is compared by histogram buckets.** The survivor's `goim_gateway_login_seconds` (LoginResp sent − recv_ms; buckets 0.5ms × 2ⁿ) is read at the DROP and after the storm. Its p99 is the bound of the bucket that holds it, and the storm's may be at most one bucket (2×) above the ramp's. The ramp logs in 5,000/s over two gateways (2,500/s each); the storm peaks at ~6,700/s on one (b1's survivor took ~10k/s). So the one-bucket allowance also absorbs the storm's own load, and a 1s Kick delay would still land ≥ 5 buckets above a ramp p99 of tens of ms. The dead epoch's sweep may overlap the storm's tail (~8% of relogins come after 50s), so the check's detail carries the storm's Redis latency (review O2). No interpolation: a bucket bound is what the histogram knows.
4. **Logic's goroutines are sampled every second** (`go_goroutines`, from the Go collector Logic registers) from the DROP until the victims are back. Allowed growth over the DROP's count is 500. The Kicker holds at most 64 workers per target, and retry timers start a goroutine only when they fire. A goroutine per Kick would be about 1,300 per second of 1s deadlines.
5. **b2's Kick and cross-check gates differ from b1's** (the same counters, more of them read). No Kick may be refused, since nothing answers. Retries are more than 0 (relogins under a live lease, review O1) and at most 3 per first attempt (the spec's limit), and none gives up as live: the lease is dead before a relogin's third retry. First attempts are at most 1.1 per relogin, and first attempts plus drops are at least max(0.5, 0.9 × the share not swept first) per relogin, so a run that exercised no Kick fails. `kick_dropped` also counts dropped retries, so only the first attempts carry the upper bound. Only Login records R1-O7's cross-check in b2 (no refused Kick), for relogins between the lease's end and the sweep (~0.6–0.9 of them), so the gate is ≥ 0.1 per relogin (a cautious floor, review O6) and ≤ 1.1, and the mean is recorded.
6. **#114: 6b2b is split** (decided): this plan, then `2026-10-15-connection-session-core-06b2b-slow-logic.md` (scenario `slowlogic`), each executed in its own fresh session and ending with its own Tier 2 run.
7. **b2's gateways send a 30s heartbeat** (review O1, decided). The SDK sends its first Ping one heartbeat after login and then every heartbeat, with no jitter, so Ping phases are the ramp's. At 150s an 80s ramp puts every Ping in one 80s band, and the kill's place in it depends on the script's timing. Most relogins would then come after the lease has died, and often after the sweep (no entry, no Kick), so the Kick checks could pass without testing R5S-O2. At 30s the ramp covers the phase ~2.7 times. The spec's rule (one heartbeat plus the Pong timeout) is checked at 30 + 10s; the 150s value is not measured (see *After this plan*).
8. **b2's storm profiles start 15s after the mark** (review O4, `GWLOSS_B2_PROFILE_AT`): the 30s CPU profiles and `storm30` (hence `loadgen_bound`, `b2_loadgen_headroom` and the lag gating) cover the relogins, not the quiet ~10s before the first Pong timeout. The usage window restarts inside the profiling subshell; the whole-storm reading is unchanged.
9. **No rules, no kill** (review O7): if `rules_at_kill` is not 4, b2 records `b2_netdrop` failed and ends the phase before the SIGKILL, as a failed `online` check does, so a broken `goim-netdrop` never runs a b1 under b2's name.

## File Structure

- Modify `scripts/stack.sh`: `stack_buckets NAME METRIC`, `stack_hist_quantile A B Q` (Task 1).
- Modify `scripts/stack_test.sh`: their tests (Task 1).
- Modify `scripts/tier2/gwloss.sh`: the `b2` phase (Task 2).
- Modify `README.md` (both languages), the roadmap, this plan's *Execution Notes* (Task 3).

---

### Task 1: a histogram reader for `stack.sh`

**Files:**
- Modify: `scripts/stack.sh` (header list; two functions after `stack_counters_delta`)
- Test: `scripts/stack_test.sh`

**Interfaces:**
- Consumes: `stack_metrics NAME` (one scrape; fails if NAME does not answer).
- Produces: `stack_buckets NAME METRIC` prints `{"0.0005": n, ..., "+Inf": n}`, the cumulative counts of `METRIC_bucket` summed over other labels, in scrape order. It prints `null` if NAME does not answer or has no such histogram. `stack_hist_quantile A B Q` prints the upper bound in ms of the bucket holding quantile Q of the samples in B − A, as an integer when whole (`1`, `0.5`). It prints `null` for a null reading, no samples, or Q in `+Inf`.

- [ ] **Step 1: Write the failing tests**

Apply (from the repo root, `git apply --check` first; it must apply cleanly to `8a9a5e4`'s `scripts/stack_test.sh`):

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index 9443fd6..3e5d5e6 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -185,6 +185,17 @@ expect "counters: delta, an absent sample before is 0" \
   "$(stack_counters_delta '{"a": 1, "b": null, "c": 2, "d": 0.5}' '{"a": 4, "b": 3, "c": null, "d": 0.75}')" '{"a": 3, "b": 3, "c": null, "d": 0.25}'
 expect "counters: delta of a failed scrape" "$(stack_counters_delta null '{"a": 1}')" null
 
+# A histogram's quantile is the bound of the bucket that holds it, over the
+# samples between two readings (Plan 6b2b: b2's Login p99).
+h0='{"0.0005": 50, "0.001": 50, "0.002": 50, "+Inf": 50}'
+h1='{"0.0005": 90, "0.001": 99, "0.002": 100, "+Inf": 100}'
+expect "hist: p99's bucket bound in ms" "$(stack_hist_quantile '{}' "$h1" 0.99)" 1
+expect "hist: a sub-ms bound" "$(stack_hist_quantile '{}' "$h1" 0.5)" 0.5
+expect "hist: only the samples between the readings" "$(stack_hist_quantile "$h0" "$h1" 0.99)" 2
+expect "hist: no sample between the readings" "$(stack_hist_quantile "$h1" "$h1" 0.99)" null
+expect "hist: a quantile in +Inf" "$(stack_hist_quantile '{}' '{"0.001": 1, "+Inf": 3}' 0.9)" null
+expect "hist: a failed reading" "$(stack_hist_quantile null "$h1" 0.99)" null
+
 # The phase boundary (P6a2bI-3): idle at once on an idle stack; a socket open
 # on gw-0 keeps it busy, named, until the socket closes.
 stack_wait_idle 5 && ok "idle: an idle stack" || bad "idle: an idle stack"
@@ -206,6 +217,12 @@ STACK_PIN=0 # loadgen's CPU set is the Tier 2 host's
 lg_start t -mode light -clients 4 -rate 100 -uid-prefix st && lg_wait_online 4 20 && ok "loadgen: 4 online" || bad "loadgen: not online"
 STACK_PIN=1
 expect "loadgen: runs with LG_ENV" "$(tr '\0' '\n' <"/proc/$LG_PID/environ" | grep '^GOMEMLIMIT=')" GOMEMLIMIT=1GiB
+# Each login is one sample of a gateway's login_seconds histogram, its
+# buckets in Prometheus's order.
+b0=$(stack_buckets gw-0 goim_gateway_login_seconds) b1=$(stack_buckets gw-1 goim_gateway_login_seconds)
+expect "buckets: one sample per login, in order" "$(python3 -c 'import json,sys; a, b = (json.loads(x) for x in sys.argv[1:3]); print(a["+Inf"] + b["+Inf"], list(a)[0], list(a)[-1])' "$b0" "$b1")" "4 0.0005 +Inf"
+expect "buckets: no such histogram" "$(stack_buckets gw-0 goim_gateway_no_such_seconds)" null
+expect "buckets: no such process" "$(stack_buckets gw-9 goim_gateway_login_seconds)" null
 lat=$(stack_redis_latency)
 expect "redis: latency of the logins' commands" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(any(v["calls"] > 0 and v["p99_us"] > 0 for v in d.values()), "info" in d, any(k.startswith("config") for k in d))' "$lat")" "True False False"
 expect "redis: no answer is null" "$(STACK_PORT_BASE=1; stack_redis_latency)" null
```

- [ ] **Step 2: Run them to see them fail**

Run: `make test-scripts 2>&1 | grep -E 'FAIL|command not found' | head`
Expected: `FAIL hist: ...` and `FAIL buckets: ...` lines, with `stack_hist_quantile: command not found` and `stack_buckets: command not found` on stderr; `stack_test: 9 failed` at its end (6 hist, 3 buckets). The first `buckets:` expectation fails inside python on empty input.

- [ ] **Step 3: Implement**

Apply:

```diff
diff --git a/scripts/stack.sh b/scripts/stack.sh
index f845e6b..6d2b62d 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -19,6 +19,8 @@
 #   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb", "rss_peak_mb"}...} since the start
 #   stack_counters NAME KEY=METRIC[:MATCH]...  # {"KEY": sum, ...} from one scrape of NAME (null: no answer)
 #   stack_counters_delta A B    # {"KEY": B - A, ...} of two stack_counters readings
+#   stack_buckets NAME METRIC   # {"LE": cumulative count, ...} of histogram METRIC on NAME (null: no answer)
+#   stack_hist_quantile A B Q   # Q's bucket bound in ms over two stack_buckets readings (null: no sample)
 #   stack_redis_resetstat       # Redis CONFIG RESETSTAT: a latency window starts
 #   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer)
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
@@ -440,6 +442,49 @@ else:
     print(json.dumps({k: None if v is None else round(v - (a.get(k) or 0), 6) for k, v in b.items()}))' "$1" "$2" 2>/dev/null || echo null
 }
 
+# stack_buckets NAME METRIC: histogram METRIC's cumulative bucket counts on
+# NAME from one scrape, as {"LE": count, ...} with LE as Prometheus writes it
+# ("0.0005", ..., "+Inf"), summed over the other labels (Plan 6b2b: the
+# survivor's Login latency before and during b2's storm). Prints null if
+# NAME does not answer or has no METRIC_bucket sample.
+stack_buckets() {
+  local body
+  body=$(stack_metrics "$1") || { echo null; return; }
+  awk -v m="$2_bucket" '
+    index($1, m "{") == 1 && match($1, /le="[^"]*"/) {
+      le = substr($1, RSTART + 4, RLENGTH - 5)
+      if (!(le in s)) order[++n] = le
+      s[le] += $NF }
+    END { if (!n) { print "null"; exit }
+          out = ""
+          for (i = 1; i <= n; i++) out = out (i > 1 ? ", " : "") "\"" order[i] "\": " sprintf("%.0f", s[order[i]])
+          print "{" out "}" }' <<<"$body"
+}
+
+# stack_hist_quantile A B Q: the upper bound, in ms, of the bucket that
+# holds quantile Q (0-1) of the samples between two stack_buckets readings
+# of the same process (B - A); a bucket's bound, not an interpolation, so a
+# p99 reads as one of the histogram's bounds. Prints null when either
+# reading is null, there is no sample between them, or Q falls in +Inf.
+stack_hist_quantile() {
+  python3 -c 'import json, math, sys
+a, b = (json.loads(x) for x in sys.argv[1:3])
+q = float(sys.argv[3])
+if a is None or b is None:
+    print("null"); sys.exit()
+les = sorted(b, key=lambda le: math.inf if le == "+Inf" else float(le))
+d = {le: b[le] - a.get(le, 0) for le in les}
+total = d.get("+Inf", 0)
+if total <= 0:
+    print("null"); sys.exit()
+rank = math.ceil(q * total)
+for le in les:
+    if d[le] >= rank:
+        ms = None if le == "+Inf" else round(float(le) * 1000, 3)
+        print(json.dumps(int(ms) if ms is not None and ms == int(ms) else ms))
+        break' "$1" "$2" "$3" 2>/dev/null || echo null
+}
+
 # stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
 stack_pinned() {
   local have
```

- [ ] **Step 4: Run the tests**

Run: `make test-scripts`
Expected: `lib_test`, `loadgen_test` and `stack_test` all pass (`stack_test: all passed`). The new lines appear as `ok   hist: ...` (6) and `ok   buckets: ...` (3). The two shell notices the suite always prints (`kill: ... No such process` from its first fake server's trap, and `Killed setsid ...` from a deliberate SIGKILL) are not failures.

Check that a broken rule is caught: change `rank = math.ceil(q * total)` to `rank = math.floor(q * total)` in `stack.sh` and run `make test-scripts`. Expected: `FAIL hist: only the samples between the readings: got [1], want [2]` (49.5 of the 50 samples between the readings floors to 49, which the 1ms bucket's 49 reaches). Then, separately, change `if d[le] >= rank:` to `if d[le] > rank:`. Expected: `FAIL hist: p99's bucket bound in ms: got [2], want [1]`, plus the `between` and `+Inf` cases. Revert both mutations.

- [ ] **Step 5: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh
git commit -m "feat(stack): stack_buckets and stack_hist_quantile, a histogram's quantile between two readings (6b2b-b2)"
```

---

### Task 2: the `b2` phase of `gwloss` (#112)

**Files:**
- Modify: `scripts/tier2/gwloss.sh`

**Interfaces:**
- Consumes: `stack_buckets`, `stack_hist_quantile` (Task 1); `stack_metric`, `stack_counters`, `stack_counters_delta`, `stack_expect_dead NAME SECONDS`, `stack_wait_idle`; loadgen's `POST /kill?gateway=&pid=&mark=` (any mark name), `GET /slo?mark=&within=30s` (`population`, `lost`, `detection`, `from_detection`, `pass_from_detection`), `GET /stats` (`lost` by end reason: `pong-timeout`, `lost`, …); `lg_wait_back MARK SECONDS`; `sudo -n /usr/local/sbin/goim-netdrop add|del|flush|list`.
- Produces: phase `b2` in `GWLOSS_PHASES` (default now `b1 b2 b_stress`), its gateways at `-heartbeat ${GWLOSS_B2_HEARTBEAT:-30s}`; checks `b2_netdrop`, `b2_slo`, `b2_detection`, `b2_login_p99`, `b2_goroutines` and b2's versions of the per-phase checks; the phase field gains `goroutines` (every phase: null outside b2) and `profile_at_s` (0 outside b2); artifacts `b2/artifacts/slo-drop.json`, `b2/goroutines.txt`. The other phases keep their mark `death` and artifact `slo-death.json`.

- [ ] **Step 1: Apply the change**

Apply:

```diff
diff --git a/scripts/tier2/gwloss.sh b/scripts/tier2/gwloss.sh
index cf7a9c7..638a300 100755
--- a/scripts/tier2/gwloss.sh
+++ b/scripts/tier2/gwloss.sh
@@ -1,9 +1,19 @@
 #!/usr/bin/env bash
-# Tier 2: gwloss (Plans 6b2a-b1 and 6b2a-stress), the spec's load tests (b1)
-# and (b-stress) with SDK clients, each phase on a fresh stack:
+# Tier 2: gwloss (Plans 6b2a-b1, 6b2a-stress and 6b2b-b2), the spec's load
+# tests (b1), (b2) and (b-stress) with SDK clients, each phase on a fresh stack:
 #   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
 #        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
 #        about one per relogin, with no retries (R2-O6);
+#   (b2) silent node loss of a 200k gateway: iptables DROP on its client and
+#        Kick ports (goim-netdrop, #68), then SIGKILL behind the DROP, so no
+#        FIN or RST reaches a client and its lease ends as in a crash (#112):
+#        99% logged in again within 30s of the client detecting the loss
+#        (Pong timeout); detection within one heartbeat plus the Pong
+#        timeout; the survivor's Login p99 unaffected by the Kicks that time
+#        out; Logic's goroutines bounded (R5S-O2). Its gateways send a 30s
+#        heartbeat: the SDK's Pings keep their login's phase, so at 150s the
+#        relogins would come in one band set by the script's timing, after
+#        the lease and often after the sweep (6b2b-b2 review O1);
 #   (b-stress) kill a 600k gateway: measured, no SLO; includes a 600k-member
 #        gwconns SSCAN + UNLINK (the dead epoch's sweep; R4M-9);
 #   storm: the other gateways never miss a lease renewal.
@@ -25,20 +35,38 @@
 #   per phase with logs, artifacts and loadgen results), plus ONE file to
 #   send back: tier2-results/gwloss-<timestamp>.report.txt
 #   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
-#   About 25 minutes (b1 ~8, b_stress ~17; up to ~40 if b_stress's relogins
-#   use their whole wait).
+#   About 32 minutes (b1 ~8, b2 ~7, b_stress ~17; up to ~50 if b_stress's
+#   relogins use their whole wait).
 #
 # Settings:
-#   GWLOSS_PHASES      phases to run, in order (default "b1 b_stress")
-#   GWLOSS_CLIENTS     b1: SDK clients over both gateways (default 400000)
-#   GWLOSS_RATE        b1: clients started per second (default 5000)
-#   GWLOSS_CAP         b1: each gateway's -max-conns (default 450000: the
+#   GWLOSS_PHASES      phases to run, in order (default "b1 b2 b_stress")
+#   GWLOSS_CLIENTS     b1, b2: SDK clients over both gateways (default 400000)
+#   GWLOSS_RATE        b1, b2: clients started per second (default 5000)
+#   GWLOSS_CAP         b1, b2: each gateway's -max-conns (default 450000: the
 #                      survivor's 400k plus room for clients in flight)
-#   GWLOSS_SWEEP_WAIT  b1: seconds after the kill to wait for the dead epoch's
-#                      sweep (default 180: stale 60s after its last renewal,
-#                      then 200k uids at the sweeper's 10k/s)
-#   GWLOSS_SOURCES     b1: loadgen's source addresses (default: loadgen's, one
-#                      per 5,000 connections per gateway, sized for the survivor)
+#   GWLOSS_SWEEP_WAIT  b1, b2: seconds after the kill to wait for the dead
+#                      epoch's sweep (default 180: stale 60s after its last
+#                      renewal, then 200k uids at the sweeper's 10k/s)
+#   GWLOSS_SOURCES     b1, b2: loadgen's source addresses (default: loadgen's,
+#                      one per 5,000 connections per gateway, sized for the
+#                      survivor)
+#   GWLOSS_B2_HEARTBEAT  b2: the gateways' -heartbeat (default 30s, the
+#                      SDK's lowest): Ping phases spread evenly over the 80s
+#                      ramp, so some relogins meet a live lease (review O1)
+#   GWLOSS_B2_BACK_WAIT  b2: seconds after the DROP to wait for every victim
+#                      to log in again (default 120: detection within 45s,
+#                      then the SDK's 0-20s storm wait and the login)
+#   GWLOSS_B2_DETECT_MAX_S  b2: the bound on every victim's detection, from
+#                      the DROP to its Pong timeout (default 45: the 30s
+#                      heartbeat plus the 10s Pong timeout, plus 5s for the
+#                      timers' and loadgen's slack)
+#   GWLOSS_B2_PROFILE_AT  b2: seconds after the DROP that the storm's 30s CPU
+#                      profiles and loadgen-bound reading start (default 15:
+#                      its relogins run from ~10s to ~60s; review O4)
+#   GWLOSS_B2_GOROUTINE_GROWTH  b2: the most Logic's goroutines may grow over
+#                      their count at the DROP while the victims log in again
+#                      (default 500; a goroutine per Kick to the silent
+#                      gateway would be thousands, R5S-O2)
 #   GWLOSS_STRESS_CLIENTS  b_stress: SDK clients, all on the victim (default 600000)
 #   GWLOSS_STRESS_RATE     b_stress: clients started per second (default 5000)
 #   GWLOSS_STRESS_CAP      b_stress: each gateway's -max-conns (default 650000:
@@ -90,7 +118,8 @@
 #                      clients were left to log in); taken only if the
 #                      victims are not back by then.
 #
-# Checks (b1; b_stress has the same with its own prefix, unless noted):
+# Checks (b1; b2 and b_stress have the same with their own prefix, unless
+# noted):
 #   b1_stack           redis, devtoken, Logic and the gateways become Ready
 #   b1_pinning         gateways, Logic + Redis and loadgen on their phase's CPU
 #                      sets (b_stress's differ, #108)
@@ -101,6 +130,24 @@
 #                      in again within 30s of T_death (R4M-17), and the
 #                      survivor rejected none at capacity (SDK clients retry a
 #                      reject without counting it, P6b2a-b1I-2)
+#   b2_netdrop         (b2 only) both of gw-1's DROP rules were in before the
+#                      SIGKILL, and none is left after the storm; without
+#                      them there is no kill and no storm (review O7)
+#   b2_slo             (b2 only) every victim detected the loss, and 99% of them
+#                      logged in again within 30s of their detection (the
+#                      spec's b2 SLO); the survivor rejected none at capacity
+#   b2_detection       (b2 only) every victim's detection, from the DROP mark,
+#                      within GWLOSS_B2_DETECT_MAX_S, and each by Pong timeout
+#                      (loadgen's lost["pong-timeout"]), not by a FIN or RST
+#   b2_login_p99       (b2 only) the survivor's Login p99 in the storm is at
+#                      most one histogram bucket (2x) above its p99 in the
+#                      ramp: Kicks to the silent gateway, each up to Logic's
+#                      1s Kick deadline, never delay a Login (R5S-O2); the
+#                      storm's Redis latency beside it (the sweep may
+#                      overlap the storm's tail, review O2)
+#   b2_goroutines      (b2 only) Logic's goroutines, sampled every second from
+#                      the DROP until the victims are back, never grow more
+#                      than GWLOSS_B2_GOROUTINE_GROWTH over the DROP's count
 #   b_stress_back      (b_stress only) loadgen SIGKILLs gw-0: every one of its
 #                      clients logs in again within GWLOSS_STRESS_BACK_WAIT, and
 #                      the survivors rejected none at capacity; no SLO.
@@ -113,12 +160,22 @@
 #                      gives up as live. A refused Kick is not retried (#97):
 #                      only first attempts that timed out while the victim's
 #                      sockets were torn down are, once each, and refused
-#                      (R2-O6; #97 amended, run 20)
+#                      (R2-O6; #97 amended, run 20). b2: the same bounds (first
+#                      attempts at most 1.1 per relogin, sent or dropped at
+#                      least 0.9), no Kick refused (the DROP answers nothing:
+#                      they time out or fail fast, and keep their retries while
+#                      the lease is live), at most 3 retries per first attempt,
+#                      none gives up as live; and Kicks were exercised: some
+#                      retried, sent or dropped first attempts for at least
+#                      half the relogins (review O1)
 #   b1_cross_check     Logic's dead-epoch cross-check (R1-O7) has a sample for
 #                      at least 0.9 of the relogins whose first Kick was sent
 #                      and did not time out (by Login once the lease is gone,
 #                      or by the refused Kick while it is live, P6b2a-b1I-7),
-#                      and at most 1.1 per relogin
+#                      and at most 1.1 per relogin. b2: no Kick is refused, so
+#                      only Login records it, for the relogins between the
+#                      lease's end and the sweep: at least 0.1 per relogin
+#                      (a cautious floor, review O6), at most 1.1
 #   b1_storm_real      the storm was the spec's, not loadgen's (#106, #107):
 #                      every logged-in client held its next handshake frame
 #                      at the kill (loadgen -precompute); after the kill,
@@ -162,9 +219,9 @@ t2_init gwloss
 t2_need curl taskset sudo python3
 t2_require_host_setup
 
-PHASES=${GWLOSS_PHASES:-b1 b_stress}
+PHASES=${GWLOSS_PHASES:-b1 b2 b_stress}
 for p in $PHASES; do
-  case $p in b1 | b_stress) ;; *) echo "gwloss: unknown phase '$p' (b1, b_stress)" >&2; exit 1 ;; esac
+  case $p in b1 | b2 | b_stress) ;; *) echo "gwloss: unknown phase '$p' (b1, b2, b_stress)" >&2; exit 1 ;; esac
 done
 # The tail profile starts after the storm's own 30s profile: loadgen serves
 # one CPU profile at a time.
@@ -195,6 +252,8 @@ t2_nofile 1048576
 export STACK_PIN=1
 STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
 STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+NETDROP=/usr/local/sbin/goim-netdrop
+t2_on_exit "sudo -n $NETDROP flush" # b2's DROP rules, whatever stopped the run
 t2_on_exit 'stack_down'
 t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack
 
@@ -207,6 +266,12 @@ LOGIC_COUNTERS=(
   kick_retry='goim_logic_kick_attempts_total:attempt="retry"'
   kick_first_timeout='goim_logic_kick_attempts_total:attempt="first",result="timeout"'
   kick_retry_refused='goim_logic_kick_attempts_total:attempt="retry",result="refused"'
+  # b2: a silent target refuses nothing; its attempts time out or fail fast
+  # (gRPC's fail-fast RPCs on a connection in TRANSIENT_FAILURE: error).
+  kick_first_refused='goim_logic_kick_attempts_total:attempt="first",result="refused"'
+  kick_first_error='goim_logic_kick_attempts_total:attempt="first",result="error"'
+  kick_retry_timeout='goim_logic_kick_attempts_total:attempt="retry",result="timeout"'
+  kick_retry_error='goim_logic_kick_attempts_total:attempt="retry",result="error"'
   kick_giveups_dead='goim_logic_kick_giveups_total:target_lease="dead"'
   kick_giveups_live='goim_logic_kick_giveups_total:target_lease="live"'
   kick_dropped=goim_logic_kick_dropped_total
@@ -312,13 +377,16 @@ lag_max=${GWLOSS_LAG_P99_MS:-100}
 for name in $PHASES; do
   # Each phase's stack, victim and waits.
   case $name in
-    b1)
+    b1 | b2)
       export STACK_GATEWAYS=2
       export T2_CPUS_GATEWAY=$cpus_gateway T2_CPUS_LOADGEN=$cpus_loadgen T2_CPUS_LOGIC=$cpus_logic
       unset STACK_GATEWAY_PROCS
       clients=${GWLOSS_CLIENTS:-400000} rate=${GWLOSS_RATE:-5000} cap=${GWLOSS_CAP:-450000}
       sweep_wait=${GWLOSS_SWEEP_WAIT:-180} back_wait=60 idle_wait=300 sources=${GWLOSS_SOURCES:-}
-      victim=gw-1 survivors=(gw-0) down="" LG_ENV=()
+      victim=gw-1 survivors=(gw-0) down="" LG_ENV=() mark=death
+      # b2: the loss is the DROP; its clients detect it by Pong timeout,
+      # within one heartbeat (30s, GWLOSS_B2_HEARTBEAT) plus 10s.
+      [ "$name" = b2 ] && back_wait=${GWLOSS_B2_BACK_WAIT:-120} mark=drop
       # Every client up from the start, split by the balancer.
       online_rule='d["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45'
       ;;
@@ -333,7 +401,7 @@ for name in $PHASES; do
       clients=${GWLOSS_STRESS_CLIENTS:-600000} rate=${GWLOSS_STRESS_RATE:-5000} cap=${GWLOSS_STRESS_CAP:-650000}
       sweep_wait=${GWLOSS_STRESS_SWEEP_WAIT:-300} back_wait=${GWLOSS_STRESS_BACK_WAIT:-900} idle_wait=600
       sources=${GWLOSS_STRESS_SOURCES:-}
-      victim=gw-0 survivors=(gw-1 gw-2) down=gw-1,gw-2 LG_ENV=(GOMEMLIMIT="${GWLOSS_STRESS_LG_MEMLIMIT:-16GiB}")
+      victim=gw-0 survivors=(gw-1 gw-2) down=gw-1,gw-2 LG_ENV=(GOMEMLIMIT="${GWLOSS_STRESS_LG_MEMLIMIT:-16GiB}") mark=death
       # The victim ramped alone (#93).
       online_rule='d["online"] == '"$clients"' and d["online_by_gateway"].get("gw-0") == '"$clients"
       ;;
@@ -345,6 +413,7 @@ for name in $PHASES; do
   STACK_GATEWAY_ARGS=(-max-conns "$cap")
   [ "$name" = b_stress ] && [ -n "${GWLOSS_STRESS_HANDSHAKE_CONCURRENCY:-}" ] &&
     STACK_GATEWAY_ARGS+=(-handshake-concurrency "$GWLOSS_STRESS_HANDSHAKE_CONCURRENCY")
+  [ "$name" = b2 ] && STACK_GATEWAY_ARGS+=(-heartbeat "${GWLOSS_B2_HEARTBEAT:-30s}")
   t0=$SECONDS
 
   busy=$(stack_busy_ports)
@@ -410,6 +479,13 @@ for name in $PHASES; do
 
   # The storm: counters before, SIGKILL, CPU profiles of its first 30s.
   stack_snapshot before
+  # b2: the survivor's Login latency so far (the ramp's; a fresh stack) and
+  # Logic's goroutines, both at the DROP.
+  login_h0=null goroutines0=null
+  if [ "$name" = b2 ]; then
+    login_h0=$(stack_buckets gw-0 goim_gateway_login_seconds)
+    goroutines0=$(stack_metric logic go_goroutines)
+  fi
   # The ramp's backoff (no mark yet: since loadgen started), apart from the
   # storm's, which counts since the death mark (P6b2a-stressI-1).
   backoff_ramp=$(lg_get /stats 'd["backoff_since_mark"]')
@@ -421,7 +497,27 @@ for name in $PHASES; do
   t2_cpustat_snap "$STACK_DIR/cpustat-2"
   t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
   stack_usage_start loadgen="$LG_PID"
-  reply=$(lg_post "/kill?gateway=$victim&pid=${STACK_PIDS[$victim]}&mark=death" | lg_py - d)
+  # b2 (#112): the DROP goes in first, on the victim's client and Kick ports,
+  # so the SIGKILL's FINs and RSTs never reach a client and a Kick's connect
+  # is never refused: the node is silent, then gone, as in a node loss. The
+  # mark is taken at the SIGKILL, a few ms after the DROP.
+  drop_rules=null
+  if [ "$name" = b2 ]; then
+    vport=$((STACK_PORT_BASE + 100 + 10 * ${victim#gw-}))
+    sudo -n "$NETDROP" add "$vport" && sudo -n "$NETDROP" add $((vport + 1))
+    drop_rules=$(sudo -n "$NETDROP" list | wc -l)
+    # Without both rules the storm would be b1's (RSTs reach the clients):
+    # no kill, no storm (review O7).
+    if [ "$drop_rules" != 4 ]; then
+      t2_check "${name}_netdrop" false "\"ports\": [$vport, $((vport + 1))], \"rules_at_kill\": $drop_rules, \"rules_left\": null"
+      sudo -n "$NETDROP" flush
+      lg_stop
+      stack_down
+      t2_duration "$name" $((SECONDS - t0))
+      continue
+    fi
+  fi
+  reply=$(lg_post "/kill?gateway=$victim&pid=${STACK_PIDS[$victim]}&mark=$mark" | lg_py - d)
   kill_t=$SECONDS
   # In this shell: it reaps and forgets the victim. 60s: a 600k gateway
   # exits only once the kernel has closed its sockets (~5.3s in run 25).
@@ -430,7 +526,12 @@ for name in $PHASES; do
   # The storm's first 30s, where its relogins peak: loadgen's cores there
   # say whether the storm was loadgen-bound (#102); the whole storm's average
   # hid it in run 22 (3.2 of 8 cores, 7.9 in the first 30s).
-  (stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
+  # b2: from GWLOSS_B2_PROFILE_AT after the mark, its relogins starting at
+  # the first Pong timeouts (review O4).
+  prof_at=0
+  [ "$name" = b2 ] && prof_at=${GWLOSS_B2_PROFILE_AT:-15}
+  (sleep "$prof_at"; [ "$prof_at" = 0 ] || stack_usage_start loadgen="$LG_PID"
+    stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
   prof=$!
   # GWLOSS_TAIL_PROFILE_AT: loadgen's CPU in the storm's tail (run 27), if
   # the victims are not back by then; stopped with them.
@@ -462,8 +563,25 @@ for name in $PHASES; do
     echo "false $((SECONDS - kill_t))"
   ) >"$STACK_DIR/sweep.out" 2>/dev/null &
   sweeping=$!
-  back=$(t2_bool lg_wait_back death "$back_wait")
+  # b2: Logic's goroutines every second until the victims are back (R5S-O2),
+  # on the Logic CPU set.
+  counting=""
+  if [ "$name" = b2 ]; then
+    (while :; do stack_metric logic go_goroutines; sleep 1; done) >"$STACK_DIR/goroutines.txt" 2>/dev/null &
+    counting=$!
+    taskset -a -p -c "$T2_CPUS_LOGIC" "$counting" >/dev/null 2>&1
+  fi
+  back=$(t2_bool lg_wait_back "$mark" "$back_wait")
   back_s=$((SECONDS - kill_t))
+  goroutines=null
+  if [ -n "$counting" ]; then
+    kill "$counting" 2>/dev/null
+    wait "$counting" 2>/dev/null
+    goroutines=$(python3 -c 'import json, sys
+n = [int(l) for l in open(sys.argv[1]) if l.strip().isdigit()]
+print(json.dumps({"at_drop": int(sys.argv[2]) if sys.argv[2].isdigit() else None, "max": max(n) if n else None, "samples": len(n)}))' \
+      "$STACK_DIR/goroutines.txt" "$goroutines0" 2>/dev/null || echo null)
+  fi
   # Loadgen's runtime once the victims are back (or the wait ran out), even
   # with GWLOSS_SS_EVERY off: its memory against GOMEMLIMIT and the GC
   # limiter at the storm's end (P6b2a-pingerI-1). Recorded, not a check.
@@ -488,7 +606,7 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
     [ $((SECONDS - kill_t)) -lt "$tail_at" ] && { pkill -P "$tailprof" 2>/dev/null; kill "$tailprof" 2>/dev/null; }
     wait "$tailprof" 2>/dev/null
   fi
-  lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
+  lg_get "/slo?mark=$mark&within=30s" d >"$STACK_DIR/artifacts/slo-$mark.json"
   wait "$sweeping"
   read -r swept sweep_s <"$STACK_DIR/sweep.out" || { swept=false sweep_s=null; }
   wait "$prof"
@@ -503,6 +621,15 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
   t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
   t2_record_end "$name"
   lo1=$(lo_dropped)
+  # b2: the survivor's Login latency in the storm; then the DROP rules come
+  # out, so the dead port's leftover sockets on both sides end by RST.
+  login_h1=null drop_left=null
+  if [ "$name" = b2 ]; then
+    login_h1=$(stack_buckets gw-0 goim_gateway_login_seconds)
+    sudo -n "$NETDROP" del "$vport"
+    sudo -n "$NETDROP" del $((vport + 1))
+    drop_left=$(sudo -n "$NETDROP" list | wc -l)
+  fi
   stack_snapshot after
   logic_d=$(stack_counters_delta "$logic0" "$logic1")
   sv_d=""
@@ -511,7 +638,7 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
   window_s=$(echo "$storm" | lg_py - 'd["window_s"]')
   backoff="{\"ramp\": $backoff_ramp, \"storm\": $(lg_get /stats 'd["backoff_since_mark"]'), \"run\": $(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')}"
 
-  slo=$STACK_DIR/artifacts/slo-death.json
+  slo=$STACK_DIR/artifacts/slo-$mark.json
   pop=$(lg_py "$slo" 'd["population"]')
   # Rejects at capacity over every survivor; null if any reading failed.
   rejects=$(echo "$sv_d" | lg_py - 'sum(v["rejects_at_capacity"] for v in d.values())')
@@ -519,15 +646,47 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
   if [ "$name" = b1 ]; then
     [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$clients"' * 0.45')
     t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
+  elif [ "$name" = b2 ]; then
+    t2_check "${name}_netdrop" "$(t2_bool [ "$drop_rules:$drop_left" = 4:0 ])" "\"ports\": [$vport, $((vport + 1))], \"rules_at_kill\": $drop_rules, \"rules_left\": $drop_left"
+    # The spec's b2 SLO: from each client's detection; every victim detected.
+    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_detection"] and d["lost"] == d["population"] and d["population"] >= '"$clients"' * 0.45')
+    t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
+    lost=$(lg_get /stats 'd["lost"]')
+    detect_max=${GWLOSS_B2_DETECT_MAX_S:-45}
+    t2_check "${name}_detection" "$(echo "{\"s\": $(cat "$slo"), \"lost\": $lost}" | lg_true - 'd["s"]["detection"]["n"] == d["s"]["population"] and 0 <= d["s"]["detection"]["max_ms"] <= '"$detect_max"' * 1000 and d["lost"].get("pong-timeout", 0) >= d["s"]["population"]')" \
+      "\"max_s\": $detect_max, \"detection\": $(lg_py "$slo" 'd["detection"]'), \"lost\": $lost"
+    p99_ramp=$(stack_hist_quantile '{}' "$login_h0" 0.99) p99_storm=$(stack_hist_quantile "$login_h0" "$login_h1" 0.99)
+    t2_check "${name}_login_p99" "$(echo "{\"r\": $p99_ramp, \"s\": $p99_storm}" | lg_true - 'd["r"] is not None and d["s"] is not None and d["s"] <= 2 * d["r"]')" \
+      "\"ramp_p99_ms\": $p99_ramp, \"storm_p99_ms\": $p99_storm, \"bucket_bounds\": true, \"redis_latency_storm\": $redis_storm"
+    growth=${GWLOSS_B2_GOROUTINE_GROWTH:-500}
+    t2_check "${name}_goroutines" "$(echo "$goroutines" | lg_true - 'd is not None and d["at_drop"] is not None and d["max"] is not None and d["max"] - d["at_drop"] <= '"$growth")" \
+      "\"max_growth\": $growth, \"logic\": $goroutines"
   else
     [ $dead -eq 0 ] && [ "$rejects" = 0 ] && [ "$back" = true ] && pass=$(lg_true "$slo" 'd["population"] == '"$clients")
     t2_check "${name}_back" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"loadgen_bound\": $loadgen_bound, \"rejects_at_capacity\": ${rejects:-null}, \"backoff\": $backoff, \"slo\": $(cat "$slo")"
   fi
-  t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - 'd["d"]["kick_retry"] <= d["d"]["kick_first_timeout"] and d["d"]["kick_retry_refused"] == d["d"]["kick_retry"] and d["d"]["kick_giveups_live"] == 0 and 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0)) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]')" \
-    "\"relogins\": $pop, \"swept_first\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]'), \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"first_timeouts\": $(echo "$logic_d" | lg_py - 'd["kick_first_timeout"]'), \"retries_refused\": $(echo "$logic_d" | lg_py - 'd["kick_retry_refused"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
-  # R1-O7: one sample per relogin whose first Kick was sent and did not time
-  # out, by Login (lease gone) or by the refused Kick (lease live).
-  t2_check "${name}_cross_check" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - '0.9 * (d["d"]["kick_first"] - d["d"]["kick_first_timeout"]) <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]')" \
+  if [ "$name" = b2 ]; then
+    # A silent target (#112): no Kick refused; retries kept while the lease
+    # is live, at most 3 per first attempt (R2-O6); the sent-or-dropped
+    # bounds as b1's (dropped also counts retries a full queue dropped, so
+    # only the first attempts have the upper bound). Kicks were exercised
+    # (review O1): some retried while the lease was live, and first attempts
+    # sent or dropped for at least half the relogins (at a 30s heartbeat
+    # nearly all relogins come before the sweep).
+    kicks_rule='d["d"]["kick_first_refused"] == 0 and d["d"]["kick_retry_refused"] == 0 and d["d"]["kick_giveups_live"] == 0 and 0 < d["d"]["kick_retry"] <= 3 * d["d"]["kick_first"] and d["d"]["kick_first"] <= 1.1 * d["pop"] and max(0.5 * d["pop"], 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0))) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0)'
+    # R1-O7: only Login records the cross-check here (no refused Kick), for
+    # relogins between the lease's end and the sweep: most of them at a 30s
+    # heartbeat; 0.1 per relogin is a cautious floor (review O6).
+    cross_rule='0.1 * d["pop"] <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]'
+  else
+    kicks_rule='d["d"]["kick_retry"] <= d["d"]["kick_first_timeout"] and d["d"]["kick_retry_refused"] == d["d"]["kick_retry"] and d["d"]["kick_giveups_live"] == 0 and 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0)) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]'
+    # R1-O7: one sample per relogin whose first Kick was sent and did not time
+    # out, by Login (lease gone) or by the refused Kick (lease live).
+    cross_rule='0.9 * (d["d"]["kick_first"] - d["d"]["kick_first_timeout"]) <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]'
+  fi
+  t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - "$kicks_rule")" \
+    "\"relogins\": $pop, \"swept_first\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]'), \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"first_timeouts\": $(echo "$logic_d" | lg_py - 'd["kick_first_timeout"]'), \"first_errors\": $(echo "$logic_d" | lg_py - 'd["kick_first_error"]'), \"first_refused\": $(echo "$logic_d" | lg_py - 'd["kick_first_refused"]'), \"retries_refused\": $(echo "$logic_d" | lg_py - 'd["kick_retry_refused"]'), \"retries_timeout\": $(echo "$logic_d" | lg_py - 'd["kick_retry_timeout"]'), \"retries_error\": $(echo "$logic_d" | lg_py - 'd["kick_retry_error"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
+  t2_check "${name}_cross_check" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - "$cross_rule")" \
     "\"relogins\": $pop, \"samples\": $(echo "$logic_d" | lg_py - 'd["dead_epoch_relogins"]'), \"mean_s\": $(echo "$logic_d" | lg_py - 'round(d["dead_epoch_relogin_s"] / d["dead_epoch_relogins"], 1)')"
   # #107: the storm was the spec's: every logged-in client was prepared at
   # the kill, and after it loadgen started each attempt, got each dial back
@@ -586,7 +745,7 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
 
   cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
   tcp="{\"ramp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-0" "$STACK_DIR/tcpstat-1"), \"storm\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3")}"
-  t2_field "$name" "{\"clients\": $clients, \"victim\": \"$victim\", \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"loadgen_heap\": {\"ramp\": $heap_ramp, \"at_kill\": $heap_kill}, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"precompute_at_kill\": $precompute, \"storm_since_mark\": $storm_d, \"back_s\": $back_s, \"loadgen_at_back\": $rt_back, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivors\": $sv_d, \"redis_latency\": {\"ramp\": $redis_ramp, \"storm\": $redis_storm, \"reset\": {\"ramp\": $reset_ramp, \"storm\": $reset_storm}}, \"backoff\": $backoff, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
+  t2_field "$name" "{\"clients\": $clients, \"victim\": \"$victim\", \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"loadgen_heap\": {\"ramp\": $heap_ramp, \"at_kill\": $heap_kill}, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"precompute_at_kill\": $precompute, \"storm_since_mark\": $storm_d, \"back_s\": $back_s, \"loadgen_at_back\": $rt_back, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivors\": $sv_d, \"redis_latency\": {\"ramp\": $redis_ramp, \"storm\": $redis_storm, \"reset\": {\"ramp\": $reset_ramp, \"storm\": $reset_storm}}, \"backoff\": $backoff, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"profile_at_s\": $prof_at, \"stop_s\": $stop_s, \"goroutines\": $goroutines, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
   t2_duration "$name" $((SECONDS - t0))
 
   # Artifacts: metrics before and after the storm, its CPU profiles, the SLO
@@ -604,12 +763,13 @@ print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || ech
   for w in ramp at_kill; do
     grep -q '^# HeapAlloc = ' "$a/$w/loadgen.heap.txt" 2>/dev/null || missing+=" $name/$w/loadgen.heap.txt"
   done
-  [ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" $name/slo-death.json"
+  [ "$(lg_true "$a/slo-$mark.json" '"population" in d')" = true ] || missing+=" $name/slo-$mark.json"
   [ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
 done
 t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
 
 t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
   "b_stress's survivors start empty: no survivor carries its own load through the 600k storm (#93)" \
-  "b2 and slowed Logic (Plan 6b2b)"
+  "b2: the lost node's recovery (out-of-service taint, gw-k recreated elsewhere, R5S-O3) is Tier 3 (Plan 6d)" \
+  "slowed Logic (scenario slowlogic, Plan 6b2b-slow)"
 t2_finish
```

Run: `bash -n scripts/tier2/gwloss.sh && grep -c 'NETDROP' scripts/tier2/gwloss.sh`
Expected: no syntax error; `8`.

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, and the dev machine has no `goim-netdrop` or iptables rules. So check the script in a throwaway copy, as 6b2a-stress did: the guard lowered, sudo removed, `NETDROP` pointed at a fake that only keeps the rule list in a file, and the scale cut. Without a real DROP the SIGKILL's RSTs reach the clients, so `b2_detection` (ends by `lost`, detection ~30ms) and `b2_kicks` (every first attempt `refused`) fail by design. Everything else must pass, which checks the plumbing and the JSON. b1 runs too, since its code paths now go through `$mark` and `kicks_rule`.

```bash
D=$(mktemp -d); cp scripts/tier2/gwloss.sh "$D/gwloss.sh"
cat >"$D/fakedrop" <<FAKE
#!/bin/bash
f=$D/rules; touch \$f
case \$1 in
  add) echo "dport \$2" >>\$f; echo "sport \$2" >>\$f ;;
  del) grep -v " \$2\\\$" \$f >\$f.n; mv \$f.n \$f ;;
  flush) : >\$f ;;
  list) cat \$f ;;
esac
FAKE
chmod +x "$D/fakedrop"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget \$mem$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/gwloss.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/gwloss.sh"
sed -i "s|^NETDROP=.*|NETDROP=$D/fakedrop|; s|sudo -n ||g" "$D/gwloss.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/gwloss.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need\|^NETDROP=\|sudo' "$D/gwloss.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  GWLOSS_PHASES="b1 b2" GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=3000 GWLOSS_B2_BACK_WAIT=60 bash "$D/gwloss.sh"; echo "exit $?"
cat "$D/rules"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, `t2_need` without sudo, `NETDROP=$D/fakedrop`, and `sudo` only in the usage comment (line 29).

First, an unknown phase is still refused: `GOIM_TEST_TIER=2 GWLOSS_PHASES="b2 nope" bash "$D/gwloss.sh"; echo "exit $?"` prints `gwloss: unknown phase 'nope' (b1, b2, b_stress)` and `exit 1` before any stack starts. (The real script's host guard refuses the dev machine before that check, so it is checked in the copy.)

Expected (~5 min): `exit 1`. `result.json` parses and has 33 checks (b1 14, b2 18, `artifacts`). All pass except `b2_detection` and `b2_kicks`, and possibly `no_swap_in_b1`/`no_swap_in_b2` from the dev machine's own swapping. `cat "$D/rules"` prints nothing. Values from the prototype's b2 (re-run after the review):
- `b2_netdrop`: `ports` `[17110, 17111]`, `rules_at_kill` 4, `rules_left` 0.
- `b2_slo`: `kill` `{"mark": "drop", "population": 1000}`, `back_all` true, `back_s` ≈ 21, `slo.lost` 1000, `slo.pass_from_detection` true.
- `b2_detection` (fails here): `max_s` 45, `detection.max_ms` ≈ 44, `lost` `{"lost": 1000}`.
- `b2_login_p99`: `ramp_p99_ms` 64, `storm_p99_ms` 16 (two shared CPUs), `redis_latency_storm` an object.
- `b2_goroutines`: `logic` `{"at_drop": 29, "max": 93, "samples": 20}`.
- `b2_kicks` (fails here): `first` 1000, `first_refused` 1000, `retries` 0, `giveups_dead` 1000, `dropped` null.
- `b2_cross_check`: `samples` 1000, `mean_s` ≈ 16 (refused Kicks record it here; on the host only Login will).
- `b2_swept`: `sweep_s` ≈ 60, `uids` 1000. `b2_loadgen_headroom` passes (`storm_first30_cores` ≈ 0.03: the window 15–45s after the mark). The `b2` field has `"gateway_args": "-max-conns 3000 -heartbeat 30s"`, `"profile_at_s": 15` and `"goroutines": {...}`; the b1 field has `"profile_at_s": 0` and `"goroutines": null`.
- b1's 14 checks pass with the values listed in 6b2a-stress Task 4 Step 2 (`b1_slo` `population` 1000, `b1_kicks` `first` 1000, `b1_cross_check` `samples` 1000).

`b2/artifacts/` holds `slo-drop.json`, and `b1/artifacts/` holds `slo-death.json`. No process is left (`pgrep -f "$PWD/tier2-results/gwloss" || echo none`).

Then no rules, no kill (review O7): make the fake's `add` fail (`sed -i 's|^  add) |  add) exit 1; |' "$D/fakedrop"`) and run `GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 GWLOSS_PHASES=b2 GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=3000 bash "$D/gwloss.sh"; echo "exit $?"`. Expected (~1 min; the prototype's b2 took 8s after setup): `exit 1`, 5 checks: `b2_stack`, `b2_online`, `b2_pinning`, `artifacts` pass, `b2_netdrop` fails with `rules_at_kill` 0 and `rules_left` null; no process left. Afterwards, remove the dry runs' `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/gwloss.sh
git commit -m "feat(tier2): gwloss b2 phase, silent loss of a 200k gateway: DROP then SIGKILL (#112)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-race-suite && make test-scripts && make test-load-probe && bash -n scripts/tier2/gwloss.sh`
Expected: all pass. Record the commits and the dry run in this plan's *Execution Notes* and push the branch. (#112–#114 are already in the roadmap, recorded with the plans; Task 3 adds the run.)

---

### Task 3: Tier 2 run 28 (b2) and the record

**Files:**
- Modify: `docs/superpowers/plans/2026-10-15-connection-session-core-06b2b-b2-silent-loss.md` (*Execution Notes*)
- Modify: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md` (plan 6 row; #112)
- Modify: `README.md` (*Measured results*, both languages; the status paragraph)

- [ ] **Step 1: The interrupt check (on the host, ~3 min)**

On the Tier 2 host, after `git fetch && git switch feat/plan6b2b-b2`, the user runs a small b2 and, in a second terminal, presses Ctrl-C in the first once the DROP rules are in (`watch -n1 sudo -n /usr/local/sbin/goim-netdrop list` shows 4 lines; the kill follows at once). The cue is the rules, not a Kick: the check is about the rules (review O5):

```bash
GOIM_TEST_TIER=2 GWLOSS_PHASES=b2 GWLOSS_CLIENTS=20000 GWLOSS_RATE=2000 make test-tier2 SCENARIO=gwloss
# Ctrl-C after the kill, then:
sudo -n /usr/local/sbin/goim-netdrop list; echo "rules: $?"; pgrep -f tier2-results/gwloss || echo "no process"
```

Expected: `goim-netdrop list` prints nothing (exit 0), and no process is left (Review Focus 1). If a rule is left: `sudo -n /usr/local/sbin/goim-netdrop flush`, and record it as a finding (the exit hook did not run).

- [ ] **Step 2: Tier 2 run 28 (on the host, ~8 min)**

With the desktop apps closed, the user runs `GOIM_TEST_TIER=2 GWLOSS_PHASES=b2 make test-tier2 SCENARIO=gwloss` and sends back the `.report.txt`. Expected: 19/19 (b2's 18 checks and `artifacts`). The full default run (`b1 b2 b_stress`, 47 checks, ~35 min) is not required: b1 and b_stress are unchanged apart from reading their mark and Kick rule through variables, which the dry run covers. The user may run it if they want the README's rows from one run.

What to read from the run, for the *Execution Notes*, the roadmap and the README:
- **SLO:** `b2_slo.slo`: `from_detection` p50/p99/max (the spec's SLO, ≤ 30s at p99), `from_mark` (detection plus relogin), `detection` p50/p99/max (expect 10–40s, close to uniform: the next Ping, at most one 30s heartbeat away, plus 10s; the 80s ramp covers the phase ~2.7 times, so some phases hold 3 logins' worth and some 2), `back_s`.
- **Kicks:** `b2_kicks`: `first`, `first_timeouts` against `first_errors` (fail-fast while TRANSIENT_FAILURE), `dropped`, `retries` (> 0: relogins under a live lease) and their results, `giveups_dead`; Logic's `kick:` log lines. `b2_cross_check`: `samples` and `mean_s`.
- **Login and goroutines:** `b2_login_p99` (`ramp_p99_ms`, `storm_p99_ms`, `redis_latency_storm`); `b2_goroutines.logic` (`at_drop`, `max`).
- **Lease and sweep:** `b2_lease` (survivor renewals), `b2_swept.sweep_s` against b1's 82s (run 21).
- **Host:** `tcp.storm` (`TCPAbortOnTimeout`, `RetransSegs`: the orphans retransmitting into the DROP), `no_lo_drops_in_b2` (Review Focus 5), `b2_loadgen_headroom`.
- If a check fails: `b2_netdrop` → `rules_at_kill` (a failed `add`: the sudoers line, `iptables` missing); `b2_detection` with `lost` other than `pong-timeout` → a FIN/RST leaked (rules after the kill? `tcp.storm.OutRsts`); detection over 45s → the heartbeat the clients got (`gateway_args` has `-heartbeat 30s`; an `EventAnomaly` "heartbeat clamped" in loadgen's log) or pinger timing; `b2_login_p99` → first `redis_latency_storm` against `redis_latency.ramp` (the sweep overlapping the storm's tail, review O2) and the storm's own rate (~6,700/s at its peak against the ramp's 2,500/s per gateway), then the survivor's storm CPU profile (`b2/artifacts/storm/gw-0.cpu.pb.gz`, 15–45s after the mark) and Logic's; `b2_goroutines` → `b2/goroutines.txt` (the per-second counts: when they rose) and `b2/artifacts/after/logic.goroutine.txt` (taken after the storm, so it shows the stacks only if the count stayed high; otherwise take `curl 127.0.0.1:17004/debug/pprof/goroutine?debug=1` at the peak in a rerun); `b2_kicks` with refused > 0 → a rule missing on the Kick port (17111); with `retries` 0 or first attempts plus drops under half the relogins → when the relogins came (`detection`) against the lease's end and `b2_swept.sweep_s`.

- [ ] **Step 3: Record**

- This plan's *Execution Notes*: Step 1's result and run 28's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #112 gets one line with run 28's result. The plan 6 row: "6b2b-b2 executed, run 28 …; next 6b2b-slow (fresh session)".
- README *Measured results*, both languages, a row after "A 200k gateway SIGKILLed": **A 200k gateway silently lost (iptables DROP; 30s heartbeat)**: the share back within 30s of detection (p99), detection p50/p99/max, Login p99 storm vs ramp, Kicks timed out or dropped and none delaying a Login, Logic's goroutines `at_drop` → `max`, lease renewals missed (0), sweep time. In the status paragraph (line 84 and its Chinese twin), move "silent gateway loss" from *Next* to *Done*.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-15-connection-session-core-06b2b-b2-silent-loss.md
git commit -m "docs: Tier 2 run 28, b2 silent gateway loss recorded; README row"
```

Push the branch. Fast-forwarding `main` is the user's call, as with every earlier plan.

---

## After this plan

- **Plan 6b2b-slow** (`2026-10-15-connection-session-core-06b2b-slow-logic.md`, fresh session): Logic in a `CPUQuota=25%` scope, a calibration of its saturated login throughput, then a kill storm sized from it (#113).
- **Spec:** nothing new. #112 is how the Tier 2 test models node loss on one host; the spec's wording ("iptables DROP … silent node loss") stands.
- **b2 at the spec's 150s heartbeat** is not run (choice 7). With Ping phases spread over the whole interval, as a real population's are, it would need loadgen to stagger logins over at least 150s (a ~150s ramp at ~1,300/s for 200k on the victim). Not planned; the 30s run tests the same rule and the same Kick paths.

## Execution Notes

- 2026-10-15, executed inline (superpowers:executing-plans) on `feat/plan6b2b-b2` from `feat/plan6b2b` (`a80b11a`). Both patches applied cleanly.
- Task 1 (`e7ead47`): RED 9 failed (6 hist, 3 buckets) as planned; GREEN `make test-scripts` all passed; both mutations (`floor`, `>`) caught with the planned FAIL lines, reverted.
- Task 2 (`04746ae`): `bash -n` clean, 8 `NETDROP`. Dev dry run (`b1 b2`, 2,000 clients, fake `goim-netdrop`): exit 1, 33 checks, failed only `b2_detection` (`lost` `{"lost": 1000}`, detection max 40ms) and `b2_kicks` (`first_refused` 1000) by design, plus `no_swap_in_b1` (dev swap, 54 pages). `b2_netdrop` 4/0; `b2_slo` back_s 20, `pass_from_detection` true; `b2_login_p99` ramp 16ms, storm 16ms (the prototype's ramp was 64ms; two shared CPUs); `b2_goroutines` at_drop 28, max 93, 20 samples; `b2_cross_check` 1000 samples, mean 15.9s; `b2_swept` 60s. Fields: b2 `-max-conns 3000 -heartbeat 30s`, `profile_at_s` 15, goroutines set; b1 `profile_at_s` 0, goroutines null. Artifacts `b2/artifacts/slo-drop.json`, `b1/artifacts/slo-death.json`. Fake rules empty after the run; no process left. Unknown phase refused (`exit 1`). No rules, no kill: 5 checks, `b2_netdrop` failed with `rules_at_kill` 0, `rules_left` null, the others passed. Whole branch: `make test`, `test-race-suite`, `test-scripts`, `test-load-probe` all passed.
- Next: Task 3 on the Tier 2 host (the user): the interrupt check, then run 28 (`GWLOSS_PHASES=b2`, 19 checks).
