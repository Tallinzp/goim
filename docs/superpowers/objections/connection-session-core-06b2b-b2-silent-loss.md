---
spec: docs/superpowers/plans/2026-10-15-connection-session-core-06b2b-b2-silent-loss.md
date: 2026-10-15
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: implementation
    severity: high
    claim: "The SDK's Pings are phase-locked to each client's login, so b2's detections fall in one ~80s band whose place after the kill is set by how long the script takes between ramp and kill; nearly every relogin then lands after the victim's lease has died, and many after the sweep, so the Kick checks (R5S-O2) can pass without Kicks to a live lease, and maybe with almost no Kicks."
    evidence: "Review Focus 3: \"64 workers then serve ~64 attempts/s against ~1,300 relogins/s\"; Task 3: \"detection p50/p99/max (expect roughly uniform 10–160s: the next Ping's time plus 10s)\"; internal/sdk/pinger.go: \"startPinger arms the first Ping one heartbeat after start\" (no jitter; start = the attempt's start)"
    disposition: accepted
    disposition_rationale: "b2's gateways at -heartbeat 30s (Ping phases even over the ramp; 8–33% of relogins under a live lease, nearly all before the sweep); detection bound 45s; b2_kicks floor: retries > 0 and first attempts plus drops ≥ max(0.5, 0.9 × not swept first) × relogins"
  - id: O2
    category: risk
    severity: medium
    claim: "In b2 the dead epoch's sweep (stale ~50–60s after the kill, done ~82s in b1) runs while victims are logging in again, which b1's never did; b2_login_p99 compares against a ramp without a sweep, so a failure (or a near miss) cannot be told apart from sweep load on Redis."
    evidence: "Implementation choice 3: \"a storm p99 above the ramp's can only come from something else, which is what the spec rules out\"; Global Constraints: \"sweep 2 × TTL after the last renewal at 10,000 uids/s\"; Task 3: \"b2_swept.sweep_s against b1's 82s (run 21)\""
    disposition: accepted
    disposition_rationale: "mostly answered by O1; the storm's Redis latency recorded beside b2_login_p99; the sweep and the storm's rate named as causes in the failure guide"
  - id: O3
    category: specification quality
    severity: medium
    claim: "The plan's rates and expected shapes assume uniform heartbeat phases: ~1,300 relogins/s and detection uniform over 10–160s; with an 80s ramp the band is ~80s wide, ~2,500 relogins/s on the survivor, the same per-gateway rate as the ramp, and these numbers would go into the Execution Notes and README as stated."
    evidence: "Review Focus 3 (\"~1,300 relogins/s\"); implementation choice 3 (\"The ramp logs in 5,000/s over two gateways and the storm ~1,300/s on one\"); Task 3 (\"expect roughly uniform 10–160s\")"
    disposition: accepted
    disposition_rationale: "every expected number recomputed from the 30s heartbeat (detection 10–40s, relogins peak ~6,700/s on gw-0); choice 3 restated"
  - id: O4
    category: specification quality
    severity: medium
    claim: "The storm's 30s measurements (CPU profiles, storm30 usage, hence loadgen_bound and b2_loadgen_headroom) start at the mark, but b2's relogins start tens of seconds later, so they measure a quiet window; Task 3's failure guide sends the reader to those profiles for b2_login_p99."
    evidence: "gwloss.sh: \"The storm: counters before, SIGKILL, CPU profiles of its first 30s\"; \"usage_ok \\\"$storm30\\\" && loadgen_bound=false\"; Task 3: \"b2_login_p99 → the survivor's storm CPU profile (b2/artifacts/storm/gw-0.cpu.pb.gz)\""
    disposition: accepted
    disposition_rationale: "GWLOSS_B2_PROFILE_AT (default 15): b2's 30s storm profiles and storm30 start 15s after the mark"
  - id: O5
    category: specification quality
    severity: medium
    claim: "Task 3 Step 1 tells the user to press Ctrl-C when Logic logs its first `kick: attempt failed`, 'about 30s after the ramp'; in a 20k-client b2 the first relogin comes ~150s after the first login, likely after the sweep, so the line may come minutes later or never."
    evidence: "Task 3 Step 1: \"presses Ctrl-C once the kill is logged (about 30s after the ramp; tail -f tier2-results/gwloss-*/b2/logs/logic.log shows the first `kick: attempt failed` line)\"; GWLOSS_CLIENTS=20000 GWLOSS_RATE=2000 (a 10s ramp)"
    disposition: accepted
    disposition_rationale: "the interrupt check's cue is goim-netdrop list showing 4 rules"
  - id: O6
    category: risk
    severity: low
    claim: "b2's cross-check gate (at least 1 sample) and the Kick lower bound (0.9 × (relogins − swept first)) pass whatever share of relogins is exercised, so they cannot catch a regression in Login's cross-check or a run that exercised no Kick."
    evidence: "Implementation choice 5: \"the gate is ≥ 1 sample and ≤ 1.1 per relogin\"; cross_rule='1 <= d[\"d\"][\"dead_epoch_relogins\"] <= 1.1 * d[\"pop\"]'"
    disposition: accepted
    disposition_rationale: "cross-check floor 0.1 × relogins; revisit after run 28"
  - id: O7
    category: risk
    severity: low
    claim: "If `goim-netdrop add` fails, the phase still SIGKILLs and runs ~8 minutes as a copy of b1, and b2_slo passes on RST detection; only b2_netdrop and b2_detection say it was not b2."
    evidence: "Task 2 diff: \"sudo -n \\\"$NETDROP\\\" add \\\"$vport\\\" && sudo -n \\\"$NETDROP\\\" add $((vport + 1))\" followed unconditionally by \"reply=$(lg_post \\\"/kill?...\")\""
    disposition: accepted
    disposition_rationale: "no rules, no kill: b2_netdrop fails and the phase ends before the SIGKILL"
---

# Objections: Plan 6b2b-b2 (silent loss of a 200k gateway)

## O1 — implementation — high

### Claim

Each SDK client sends its first Ping one heartbeat (150s) after its connection started, and every 150s after that, with no jitter. b2's clients log in during an 80s ramp (400k at 5,000/s), so all their Pings fall in one 80s band per 150s. A victim detects the loss at its first Ping after the DROP plus the 10s Pong timeout, so the detections (and the relogins, plus the SDK's 0–20s wait) fill an ~80s band. Where that band sits after the kill depends only on how long the script takes between the first login and the kill: the ramp, the forced-GC heap dump, the precompute wait, the heap dump at the kill and `stack_snapshot` (heap and goroutine dumps of every process).

With T the time from the first login to the kill, detection starts at about 160 − T seconds after the kill and ends ~80s later. The victim's lease is dead 20–30s after the kill, and its entries are swept between ~50–60s and ~82s (b1, run 21). For any T from ~80s to ~130s (overhead from 0 to 50s after the ramp), every relogin comes after the lease is dead, and the early part of the range puts most of them after the sweep too, when Login finds no entry and queues no Kick at all. A Kick is retried only while the lease is live, so the retry path is never used.

So the run can pass `b2_kicks`, `b2_login_p99` and `b2_goroutines` with Kicks only to a dead lease, a few thousand of them, or almost none. The spec's claim under test ("Kicks to the lost pod time out after ~1s each and never delay relogins") is about the relogins that happen while the pod looks alive. Real clients' Ping phases are spread over the whole heartbeat, and that spread is what puts relogins in that window. The plan's numbers assume that spread (O3).

### Evidence

> Review Focus 3: "64 workers then serve ~64 attempts/s against ~1,300 relogins/s, so most attempts are dropped by the full queue."

> Task 3: "`detection` p50/p99/max (expect roughly uniform 10–160s: the next Ping's time plus 10s)"

`internal/sdk/pinger.go`: "startPinger arms the first Ping one heartbeat after start", `next: hb`, then `p.next = now + p.hb`; `attempt.go` passes `start := time.Now()`. `internal/logic/kick.go`: "The first attempt is always queued, whatever the target's lease looks like", but "Retry only while the target's lease is live". Login queues a Kick only when `r.Prev != nil`, which a swept entry is not.

### Why this matters

Run 28 could report 19/19 and the README would say "Kicks timed out or dropped and none delayed a Login" when no Kick met a live lease and possibly none was sent during the relogins that matter. The dev dry run cannot show it (RST detection in ~30ms). Ways out, for the disposition:
- **b2 gateways at `-heartbeat 30s`** (the SDK's and gateway's minimum). An 80s ramp then covers the 30s phase ~2.7 times, so phases are close to even (2:3). Detection is 10–40s, roughly two thirds of relogins come while the lease is live (retries exercised), and all come before the sweep (also answers O2). The detection gate becomes 30 + 10 + 5s, so the spec's 150s number is not the one measured, though its rule ("one heartbeat plus the Pong timeout") is.
- **Time the kill**: SIGKILL at first login + 150s − a few seconds, waiting if the script got there early. Detection then starts ~10s after the kill and runs ~80s, with ~15–25% of relogins under a live lease. The sweep still overlaps the band (O2), and the phase waits ~50s longer.
- **Either way, a floor**: gate that Kicks were exercised (e.g. retries > 0, first attempts plus drops ≥ some share of relogins), so a run that exercised none fails instead of passing.

## O2 — risk — medium

### Claim

In b1 every relogin finished within ~20s, well before the sweep started (~60s). In b2 the relogins run for ~80s at a time the script does not control (O1), and the sweep (SSCAN plus deletes of 200k uids at 10k/s on the one Redis) will often run at the same time. `b2_login_p99` compares the storm with the ramp, which had no sweep, and allows one bucket (2×). A storm p99 two buckets up from sweep contention would fail the check and send the reader to Kicks. A pass would not show that Kicks were harmless when the sweep was the bigger load.

### Evidence

> Implementation choice 3: "so a storm p99 above the ramp's can only come from something else, which is what the spec rules out."

> Global Constraints: "sweep 2 × TTL after the last renewal at 10,000 uids/s."

### Why this matters

The check's conclusion ("Kicks to a silent gateway never delay a Login") rests on there being no other cause in the window. Either keep the sweep out of the relogin window (the 30s heartbeat in O1 does), or record Redis latency (`redis_latency.storm`, already in the phase field) beside the check and name the sweep as a possible cause in Task 3's failure guide.

## O3 — specification quality — medium

### Claim

The plan's rates and expected shapes assume Ping phases spread evenly over 150s: ~1,300 relogins/s, and detection "roughly uniform 10–160s". With an 80s ramp the band is ~80s wide: ~2,500 relogins/s on the survivor (200k in ~80s, smoothed by the 0–20s wait), the same per-gateway rate as the ramp (5,000/s over two). Choice 3's argument still holds (the storm is no faster than the ramp), but by a margin of 1×, not ~2×. The Kick queue drops a larger share than Review Focus 3 says.

### Evidence

> Review Focus 3: "~1,300 relogins/s". Implementation choice 3: "The ramp logs in 5,000/s over two gateways and the storm ~1,300/s on one". Task 3: "expect roughly uniform 10–160s".

### Why this matters

The executor copies these expectations into the Execution Notes and README, and would read a two-hump or 80s-wide detection band as a pinger fault. Whatever O1 decides, the expected shape should follow from it.

## O4 — specification quality — medium

### Claim

The storm's first-30s measurements start at the mark: the CPU profiles of every process (`stack_profile storm 30`, `lg_profile storm 30`), and `storm30.json`, from which `loadgen_bound` is computed and `b2_loadgen_headroom` and `b2_storm_real`'s lag gating follow. In b2 the first detection comes at the earliest ~10s after the mark and, per O1, more likely 30–80s after it. These measurements then cover a quiet window: `loadgen_bound` is false by construction, and the profiles show idle processes.

### Evidence

> gwloss.sh: "The storm: counters before, SIGKILL, CPU profiles of its first 30s." `usage_ok "$storm30" && loadgen_bound=false`.

> Task 3: "`b2_login_p99` → the survivor's storm CPU profile (`b2/artifacts/storm/gw-0.cpu.pb.gz`) and Logic's".

### Why this matters

The headroom check would pass without having measured the storm, and the profile the failure guide names would not contain the failure. b2 could start the 30s profiles at the first detection (or when `/stats` shows the first relogin) or, with O1's 30s heartbeat, at mark + 10s. Or the plan could say that these are not meaningful in b2 and point the failure guide at a rerun profile.

## O5 — specification quality — medium

### Claim

Task 3 Step 1 tells the user to press Ctrl-C when Logic logs its first `kick: attempt failed`, "about 30s after the ramp". In that small run (20,000 clients at 2,000/s, a 10s ramp) the first Ping is 150s after the first login, so the first relogin comes ~150s after the ramp starts. The sweep of 20k uids is done ~60s after the kill. If the kill comes within ~90s of the ramp, as it will, every relogin finds no entry, Logic queues no Kick, and the line never appears.

### Evidence

> Task 3 Step 1: "presses Ctrl-C once the kill is logged (about 30s after the ramp; `tail -f tier2-results/gwloss-*/b2/logs/logic.log` shows the first `kick: attempt failed` line)".

### Why this matters

The user waits for a cue that does not come. The check is about the DROP rules, which are in from the kill, so the cue should be the kill itself: `sudo -n goim-netdrop list` showing 4 rules, or the phase's kill line in the gwloss output.

## O6 — risk — low

### Claim

b2's cross-check gate needs at least one sample, and the Kick lower bound is 0.9 × (relogins − entries swept first). Both pass whatever share of relogins met an entry: a run where the sweep came first passes them with near-zero counts. Neither would catch Login's cross-check recording, say, 10% of what it should.

### Evidence

> Implementation choice 5: "so the gate is ≥ 1 sample and ≤ 1.1 per relogin, and the mean is recorded." `cross_rule='1 <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]'`.

### Why this matters

These checks look like coverage of R1-O7 and R2-O6 in b2 but only guard the upper side. An honest lower bound needs the count of relogins that replaced an entry of the dead epoch after its lease ended (Logic's `logins` by result, or `kick_first` attempts whose lease label was dead). Failing that, the plan should say these are record-only below the upper bound. O1's floor would cover the Kick side.

## O7 — risk — low

### Claim

If either `goim-netdrop add` fails (a sudoers line gone, iptables missing), the phase still SIGKILLs the victim. It then runs its ~8 minutes as a copy of b1: clients detect by RST, and `b2_slo` passes from detection. `b2_netdrop` and `b2_detection` fail, so the run fails, but only after the time is spent, and the report shows a passing `b2_slo` from a test that was not b2.

### Evidence

> `sudo -n "$NETDROP" add "$vport" && sudo -n "$NETDROP" add $((vport + 1))`, then `drop_rules=$(...)` and, unconditionally, `reply=$(lg_post "/kill?...")`.

### Why this matters

This is cheap to fix: if `drop_rules` is not 4, skip the kill and end the phase with `b2_netdrop` failed, as a failed `online` check does. Otherwise it is acceptable as is, since the run fails anyway.

## Explicitly not objecting to

- **#112, DROP then SIGKILL instead of a DROP on a live process**: the plan explains why a port-only DROP on a live process becomes an asymmetric partition, and the user decided it.
- **The exit hook's `goim-netdrop flush`**: `t2_traps` runs the hooks on EXIT and on INT (`exit 130`). The flush removes rules by spec and not by line number, and `list` prints only tagged rules, so `rules_at_kill` 4 matches the real script's output.
- **The Kicker under a DROP**: lazy `grpc.NewClient`, 1s per-attempt deadline, fail-fast in TRANSIENT_FAILURE and a bounded queue and workers make goroutine growth from Kicks bounded by design. The 500 bound and the sampler are reasonable.
- **Bucket-bound p99 instead of interpolation**: the histogram tops out at ~16s (0.5ms × 2¹⁵), so a 1s Kick delay lands in a finite bucket far above the ramp's p99. The method can see the failure it is for (O2 is about confounders, not the method).
- **Running only b2 in run 28**: b1 and b_stress change only through `$mark` and the rule variables, and the dry run covers both b1 and b2's plumbing.
- **The orphaned sockets behind the DROP (Review Focus 4)**: they are on the victim's port only, end by RST once the rules come out, and b_stress's `stack` check catches leftovers in a full run.
- **`no_lo_drops_in_b2` and netfilter drops (Review Focus 5)**: netfilter drops are not netdev `rx_dropped`, and the plan says to record rather than fix a surprise there.
