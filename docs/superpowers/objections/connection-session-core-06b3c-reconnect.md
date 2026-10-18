---
spec: docs/superpowers/plans/2026-10-17-connection-session-core-06b3c-reconnect.md
date: 2026-10-17
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: premise
    severity: high
    claim: "Both phases drop the clients with an RST that the gateways see at once, so the carrier/network-recovery case (the gateways keep the old connections as ghosts) is never run, and netchange tests only the SDK's timing."
    evidence: "Spec Success criteria: 'Whole-population reconnect (R5S-O6) (LB failover or reset, a regional or carrier network recovering)'. Plan Task 4: 'netchange: the same, then POST /network-change'; Implementation choice 2: 'each gateway gets an RST'; not_covered: 'a carrier or regional network recovering for part of the population: every client is reset'."
    disposition: accepted
    disposition_rationale: "Add phase ghost: loadgen silent drop (socket kept open, unread), then /network-change; the cap and Kicks shape it (#136 amended)."
  - id: O2
    category: premise
    severity: medium
    claim: "#136.4's reason for the 250k cap ('so the cap may shape the storm') does not hold at the defaults: the RST frees every slot at once and 600k ÷ 3 = 200k per gateway, so at_capacity fires only on a > 50k imbalance that least connections prevents."
    evidence: "Plan Decisions #136 (4): 'the spec's 250k cap (not b_stress's 650k), so the cap may shape the storm, as the spec says; at_capacity is recorded, not gated'. Task 4: 'RECONNECT_CLIENTS ... (default 600000)', 'STACK_GATEWAYS=3'; Implementation choice 2: the reset closes from loadgen's side."
    disposition: accepted
    disposition_rationale: "Follows O1: keep 250k; #136.4's reason reworded (cap binds in ghost, not in the RST phases)."
  - id: O3
    category: risk
    severity: high
    claim: "P_heartbeats fails on a Pong timeout from either side, and in a loadgen-bound storm (as every b_stress run was) the run has no rule to tell a gateway that answered late from a loadgen that read late, unlike floods' verdict (#126, #127)."
    evidence: "Review Focus 5: 'In a loadgen-bound storm a late read on loadgen's side could also cause a Pong timeout: the check still fails, and P_back.loadgen_bound is beside it to read.' Inputs: 'gwloss b_stress's 30j rerun ... 600,000 back in 127s ..., loadgen-bound'. Review Focus 4: 'all three handshake at once (in b_stress only two did)'."
    disposition: accepted
    disposition_rationale: "Labelled fail when loadgen_bound, as floods (#126/#127); loadgen's run-queue wait in the series."
  - id: O4
    category: risk
    severity: medium
    claim: "No check reads the session state after the storm: 600k Logouts race 600k relogins and some Logouts are dropped, but nothing checks that Logic/Redis holds exactly one live session per client on the gateway that holds it, so a Logout that deletes a new session passes every check."
    evidence: "Review Focus 3: 'each gateway queues ~200k Logouts at once into a 250k queue ... while the relogins replace those sessions ... A full queue drops Logouts (reconciled later by design)'. Task 4 LOGIC_COUNTERS (logins_replaced, logouts_deleted, logouts_not_owner) go only into the field 'logic'; P_idle allows 'dropped-ok'."
    disposition: accepted
    disposition_rationale: "New check P_sessions in the hold: one live session per client, on the gateway that holds it; form settled in the prototype."
  - id: O5
    category: scope
    severity: medium
    claim: "reconnect records no Redis reading, though gwloss records Redis latency and command stats for its storm, and a whole-population relogin is Redis's worst write burst; Plan 6c (Redis Cluster) then has no single-instance baseline from it."
    evidence: "gwloss.sh:475/550/685 (stack_redis_resetstat, stack_redis_latency for ramp and storm). reconnect.sh has no stack_redis_* call; its not_covered says only 'per-shard Redis load with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)'. After this plan: '6c (Redis Cluster, gwconns shard load ...)'."
    disposition: accepted
    disposition_rationale: "Record stack_redis_resetstat/latency per phase as gwloss, into P.redis; not gated."
  - id: O6
    category: specification quality
    severity: medium
    claim: "The funnel is named as the evidence to unpark 6b3b5 and #111 at 'seconds > 0', so one sampled second can unpark a parked plan; and it pairs an instant gauge (handshake == 0) with a rate over the interval (pending_mem_per_s > 0)."
    evidence: "Task 5 Step 1: 'A funnel with seconds > 0 is the evidence #133 names for unparking 6b3b5 and #111: the user's call.' rc_series: 'refusing = ... pending_mem_per_s > 0', 'idle = ... v[gi[\"handshake\"]] == 0 and ... open < cap'."
    disposition: accepted
    disposition_rationale: "Name the funnel's bar before the run (e.g. longest_s >= 5s); under it recorded as seen, under the bar."
  - id: O7
    category: risk
    severity: medium
    claim: "#135's refused outcome has no bound and no cross-check, so a gateway fault that closes most slow clients early now passes slow_held and slow_freed while a few still reach the timeout."
    evidence: "Task 2 floods.sh: 'for inst in slowhs:'{\"closed\", \"refused\"}' slowauth:'{\"closed\", \"pre-handshake\", \"refused\"}'' with 'set(...) <= ... and \"closed\" in d[\"flood\"][\"outcomes\"]'; flood.go: 'case clientconn.IsClosed(err) && r.cfg.Mode != ModeGarbage && held < slowHeld: r.flood.outcome(FloodRefused, -1)'; the doc: 'a connection the gateway closed before 3s (at capacity, a limit, pending_mem) is refused'."
    disposition: accepted
    disposition_rationale: "slow_held also needs refused <= the gateways' refusals over the phase; floods stays at 30 checks."
  - id: O8
    category: risk
    severity: low
    claim: "The per-second sampler (4 curls and 3 /proc/stat reads each second) runs on Logic's 2-CPU set, the set most likely to bound a login storm, so logic_set_busy_pct includes the sampler's own cost."
    evidence: "reconnect.sh: 'taskset -apc \"$T2_CPUS_LOGIC\" \"$smp\"' with 'T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}'; Review Focus 4: 'the series' gateway_set_busy_pct beside pending_mem_per_s and busy_per_s says which came first'."
    disposition: accepted
    disposition_rationale: "Pin rc_sampler to loadgen's set."
---

# Objections: Plan 6b3c (whole-population reconnect)

Mode: spec time (the plan, before execution). The plan is mostly patches.
I read the prose, Task 2's and Task 3's patches, and all of `reconnect.sh`.
I checked facts in the spec (rev6 ~l.28, ~l.128–131, ~l.428, ~l.548), the SDK
(`internal/sdk/policy.go`, `client.go`), the gateway config and
`gwloss.sh`.

## O1 — premise — high

### Claim

The spec names three causes of a whole-population reconnect: an LB failover
or reset, and a regional or carrier network that recovers. The plan runs
only one server-side shape: loadgen closes every socket with `SO_LINGER 0`,
so every gateway gets an RST and frees the connection at once. In a network
recovery the gateways get nothing. Each old connection stays ONLINE on its
gateway until `DeadAfter` (2 × heartbeat + 10s: 70s at 30s, 310s at the
default 150s). During that time the relogins arrive beside 600k ghosts.

The `netchange` phase is the spec's "every client raising a network-change
event at once", which is the network-recovery case. But it runs on top of
the same RST. So it tests the SDK's 0–1s local window, and not the server
state that comes with a network recovery.

### Evidence

> **Whole-population reconnect** *(R5S-O6)* (LB failover or reset, a
> regional or carrier network recovering) — spec, *Success criteria*

> `netchange`: the same, then POST /network-change once every client has
> lost its connection — Task 4, `reconnect.sh` header

> The reset closes from loadgen's side … each gateway gets an RST —
> Implementation choice 2

The `not_covered` line covers only "part of the population", not the ghost
connections.

### Why this matters

With ghosts, the paths that are not tested are the expensive ones:

- **The cap binds.** 200k ghosts plus 200k relogins per gateway is 400k
  against a 250k cap. So `at_capacity` refuses ~150k per gateway until the
  heartbeat sweep. This is the "connection cap shapes it" that the spec
  promises (see O2).
- **Every relogin replaces a live session.** Logic Kicks the old connection
  on its gateway. That is 600k Kicks in one burst, the path that parked #117
  ("cheaper Kicks") says is costly.
- **The sweep comes in one burst.** The ghosts all die within one wheel
  tick of `DeadAfter`, which gives 600k close-and-Logout events at once, in
  the middle of the storm.

If run 31 passes, it will be read as "whole-population reconnect: fine",
but the run tested the easiest of the spec's three causes. A ghost drop
needs no root. Loadgen can make the SDK's `Read` fail (the client sees a
loss and starts its wait) while it keeps the real fd open and unread, so the
gateway sees only silence. `goim-netdrop` (gwloss b2) is the precedent for a
silent drop, but it drops by port, so it would also block the relogins.

## O2 — premise — medium

### Claim

#136.4 picks the spec's 250k cap instead of b_stress's 650k "so the cap may
shape the storm". At the plan's defaults the cap cannot shape it. The RST
frees each gateway's connections at once. 600,000 ÷ 3 = 200,000 per gateway.
`at_capacity` fires only if one gateway gets more than 50k more than its
share, and loadgen's least-connections balancer steers away from the fuller
gateway. With 250k and with 650k you get the same run.

### Evidence

> (4) the spec's 250k cap (not b_stress's 650k), so the cap may shape the
> storm, as the spec says; `at_capacity` is recorded, not gated — Decisions,
> #136

`RECONNECT_CLIENTS` default 600000, `STACK_GATEWAYS=3`; the reset closes
from loadgen's side (Implementation choice 2).

### Why this matters

The decision is recorded as a reason that the run then seems to test. If
`at_capacity` stays at 0, the record must not say "the cap was tested
and held". The cap binds only with ghosts (O1), with fewer gateways, or with
more clients per gateway. If O1 is rejected, the reason in #136.4 needs
different words, or one of the other two changes.

## O3 — risk — high

### Claim

`P_heartbeats` passes only if no connection ended for a reason other than
the reset. A Pong timeout can come from the gateway (it answered late) or
from loadgen (it read the Pong late, or sent the Ping late). The plan says
that the check "still fails", and puts `loadgen_bound` beside it. But it
gives no rule for how to read a failure. Every b_stress run was
loadgen-bound, and this storm is larger (three gateways' clients, not
one). So a failed `P_heartbeats` with `loadgen_bound: true` is the likely
outcome, and it cannot be read either way.

### Evidence

> In a loadgen-bound storm a late read on loadgen's side could also cause a
> Pong timeout: the check still fails, and `P_back.loadgen_bound` is beside
> it to read. — Review Focus 5

> 600,000 back in 127s from one gateway's SIGKILL, loadgen-bound — Inputs

### Why this matters

For floods, the project already decided this question twice (#126, #127):
a run that loadgen saturates fails labelled, with a verdict, and the
ruling order is in the series. Here the same failure mode has no verdict.
A 20-minute run on the host would then need a ruling after the fact, and
maybe a rerun. Also, the check is by absence of a 10s Pong timeout, so a
Pong at 9.9s passes as "on time". The plan's `not_covered` names this, but
it does not record any loadgen-side lag (the light client's
`ping_tick_lag_max_ms` / `ping_send_max_ms` have no SDK counterpart here)
that would let a reader separate the two sides.

## O4 — risk — medium

### Claim

At the reset each gateway queues ~200k Logouts while the same users log in
again. The order of a Logout (old connection) and a Login (new connection)
in Logic decides the end state: replaced, deleted, or not_owner. Some
Logouts are dropped at the full queue. The plan records Logic's counters
but no check reads the session state. A Logout that deletes the new session
leaves a client online at its gateway with no session in Logic. No
message can reach that client, and the SDK does not see anything wrong.
`P_heartbeats` and `P_back` both pass in that case.

### Evidence

> At the reset each gateway queues ~200k Logouts at once into a 250k queue
> … while the relogins replace those sessions … A full queue drops Logouts
> (reconciled later by design) — Review Focus 3

`LOGIC_COUNTERS` (`logins_replaced`, `logouts_deleted`, `logouts_not_owner`)
go only into the field `logic`. `P_idle` allows `dropped-ok`.

### Why this matters

The `race` scenario tests the ordering fences one at a time, at small scale.
This run is the only one where 600k such races happen at once, with queue
drops. A cheap check during the hold would close the gap: for example, the
count of live sessions in Redis equal to N, or Logic's counter arithmetic
(every relogin is `replaced` or follows a `deleted`). If no such check is
added, the record must say that the run does not show session consistency.

## O5 — scope — medium

### Claim

`gwloss` records Redis's latency and command stats for its ramp and its
storm. `reconnect` records nothing from Redis. A whole-population relogin
is the largest write burst Redis gets: 600k logins and 600k Logouts in
seconds. The plan's next step, 6c, moves to Redis Cluster and sizes the
`gwconns` shards. Without a single-instance reading from this run, 6c has
no baseline for the worst burst.

### Evidence

`gwloss.sh:475`, `:550`, `:685` (`stack_redis_resetstat`,
`stack_redis_latency`). `reconnect.sh` has no `stack_redis_*` call. Its
`not_covered` says: "per-shard Redis load with the gwconns shards marked
(one redis-server; Redis Cluster in Plan 6c)". *After this plan*: "6c (Redis
Cluster, `gwconns` shard load, ReconcileEnd/drain deadline sizing)".

### Why this matters

Redis shares Logic's 2-CPU set. If the storm is Logic-bound, the series
cannot say whether Logic's CPU or Redis's latency came first. The helper
already exists, so the cost is a few lines. To get it later needs a new
20-minute run.

## O6 — specification quality — medium

### Claim

Task 5 makes the funnel the evidence to unpark 6b3b5 and #111, with
`seconds > 0` as the bar. One row (one sampled second) is enough. Also the
row pairs two readings of different kinds: `handshake == 0` is a gauge at
one instant, and `pending_mem_per_s > 0` is a rate over the interval
(~1s or more). A gateway can read 0 at the instant of the scrape and still
be busy for most of the second.

### Evidence

> A funnel with `seconds` > 0 is the evidence #133 names for unparking
> 6b3b5 and #111: the user's call. — Task 5 Step 1

`rc_series`: `refusing = [g … if (v[gi["pending_mem_per_s"]] or 0) > 0]`,
`idle = [g … if v[gi["handshake"]] == 0 and … open < cap]`.

### Why this matters

6b3b5 was parked after review because the trigger did not occur at the
defaults (the 30j `pending_mem` was 0). A threshold of one sampled second
lets chance decide a parked plan. The record should name a bar before the
run, for example `longest_s` ≥ some seconds, or the refusals' share during
the funnel rows, so the "user's call" reads a number set in advance.

## O7 — risk — medium

### Claim

Before #135, a slow client that the gateway closed early pulled down the
held p50, so `slow_held` failed. Now such a close is `refused`, with no hold
and no bound. `slow_held` requires only that the set of outcomes is
allowed and that `closed` occurs. So a gateway fault that closes 90% of the
slow clients at 1s (a wrong timeout, a limit at the wrong size) passes
`slow_held` and `slow_freed` if 10% still reach the 5s timeout.

### Evidence

> `slowhs:'{"closed", "refused"}' slowauth:'{"closed", "pre-handshake", "refused"}'`
> … `set(d["flood"]["outcomes"]) <= … and "closed" in d["flood"]["outcomes"]`
> — Task 2, `floods.sh`

> `case clientconn.IsClosed(err) && r.cfg.Mode != ModeGarbage && held < slowHeld:`
> — Task 2, `flood.go`

The doc names the causes "(at capacity, a limit, pending_mem)", but nothing
checks that the refusals came from those.

### Why this matters

#135 fixed a false fail, and it also removed a true one. A cross-check
keeps both: the floods' `refused` count must not exceed the gateways'
refusal counters over the phase (`rejects_pending_mem` +
`rejects_at_capacity` + the limiter's), which `t2_gws_delta` already
reads. Without it, a refused share is recorded but nothing gates it.

## O8 — risk — low

### Claim

`rc_sampler` runs one loop per second with four curls and three `/proc/stat`
reads, and the plan pins it to Logic's set (2 CPUs, `11,23`). In a login
storm that set is a likely bound (Logic and Redis share it). The sampler's
own CPU time is then in `logic_set_busy_pct`, the reading that Review
Focus 4 uses to say which set saturated first.

### Evidence

`taskset -apc "$T2_CPUS_LOGIC" "$smp"`; `T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}`.
Review Focus 4: "the series' `gateway_set_busy_pct` beside
`pending_mem_per_s` and `busy_per_s` says which came first".

### Why this matters

The cost is probably small (a few % of one CPU). Floods does the same. But
loadgen's set has 14 CPUs and is already the "measuring" side. If Logic's
set reads 86% with the sampler in it, the ruling can change. Either move it,
or record its CPU time so the reader can subtract it.

## Explicitly not objecting to

- **Task 1 (helpers to `lib.sh`)**: a mechanical move with tests and a
  check that the old names are gone; no behaviour changes and nothing to
  falsify.
- **The 95% for 5s series rule (#136.2)**: the user decided it with run 30l's
  84.8–87.6% as evidence, and `floods_test.sh` separates the two rules; I
  have no reading that contradicts it.
- **No SLO on recovery time**: the spec says "best effort, no SLO", so to
  gate only "every client back within 900s" follows the spec.
- **`/network-change` sent only after every client lost its connection**
  (Review Focus 2): it follows the SDK's documented `local` semantics
  (`policy.go:230`, `client.go:320`), and the delay and the cut waits are
  recorded.
- **"One instant" is 600k closes over seconds** (Review Focus 1): the spec
  allows "RST from loadgen", and `close_ms` and the detection distribution
  are recorded and bounded by `RECONNECT_DROP_MAX_S`.
- **A 30s heartbeat instead of 150s (#136.3)**: it is the SDK's lowest
  allowed value, decided by the user, and it is what makes the heartbeat
  check mean something during the storm.
- **One plan, not split (#136.1)**: the user's call; the size is large
  (~125KB) but the per-task patches keep each session's reading small.
- **`eachSDK`'s 64 workers and the `r.sdks` slice**: the same pattern as
  `Run`'s stop, and 600k pointers is small beside loadgen's 16GiB.
