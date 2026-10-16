---
spec: docs/superpowers/plans/2026-10-15-connection-session-core-06b2b-slow-logic.md
date: 2026-10-15
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: implementation
    severity: high
    claim: "The storm's queue cap is half of what the spec's rule allows for the gateway that actually takes the storm, so storm_expired is close to guaranteed and the rule's factor (deadline − margin) is never put under load."
    evidence: "storm sets `-login-queue-cap` = T/2 × 4s per gateway … SIGKILLs `gw-1`, so 30T relogins arrive at ~1.5T/s on `gw-0`; \"A queued Login waits at most 2T / T = 2s, well inside the 5s deadline, so `EXPIRED` should be near zero.\""
    disposition: accepted
    disposition_rationale: "cap = T x 4 (the rule for the lone survivor) (see dispositions file)"
  - id: O2
    category: risk
    severity: high
    claim: "storm_busy passes on a single Busy, so it does not catch the failure the prototype itself hit (25 Busy instead of ~20,000, arrivals barely outrunning Logic)."
    evidence: "Gates: Busy > 0 …; \"the storm's arrivals at 1.5 × T barely outran Logic: in one run 25 Busy instead of ~20,000.\"; `'(d[\"logins_busy\"] or 0) > 0'`"
    disposition: accepted
    disposition_rationale: "storm_busy needs Busy >= 0.1 x victims (see dispositions file)"
  - id: O3
    category: implementation
    severity: medium
    claim: "T is measured on fresh logins (Logic `ok`), but the storm serves relogins that replace a dead gateway's entries (Logic `replaced`, a Kick each to gw-1 under its live lease), and the storm's own Logic pace is never measured, so the run cannot tell whether T held."
    evidence: "\"The calibration measures T as Logic's ok logins per second\"; LOGIC_COUNTERS has `logins_replaced` but T uses only `logins_ok`; the storm's `logic_d` and `usage` span the kill to the last victim (up to 600s)."
    disposition: accepted
    disposition_rationale: "busy-period window records T' (recorded, not gated) (see dispositions file)"
  - id: O4
    category: risk
    severity: medium
    claim: "calibrate_saturated gates only Logic's CPU; the waste the plan names as the other way T comes out wrong (expired, expired_queued, error in calibration) is recorded but not gated, so a mis-sized storm still runs."
    evidence: "Review Focus 2: \"If queued logins expire, Logic's CPU partly goes to logins that do not count, and T comes out low … Either way the storm's cap and size come out wrong. Expected: `calibrate_saturated` requires Logic's CPU in the window ≥ 0.9 × the quota.\"; \"`expired`, `expired_queued` and `error` are recorded\""
    disposition: accepted
    disposition_rationale: "calibrate_saturated also gates waste <= 1% of ok (see dispositions file)"
  - id: O5
    category: risk
    severity: medium
    claim: "storm_lease can only fail when a renewal exceeds the 5s call timeout, records no renewal latency, and is not checked in calibration, where Logic is saturated hardest and longest."
    evidence: "Review Focus 1: \"every renewal still completes well inside the gateway's 5s lease timeout\"; storm_lease reads only `lease_live`, `lease_missing`, `lease_error` (counters) from the kill onward; the calibrate phase has no lease check."
    disposition: accepted
    disposition_rationale: "calibrate_lease added; 5s-bound limit stated (see dispositions file)"
  - id: O6
    category: risk
    severity: medium
    claim: "The storm's population is 60T with no ceiling, while the memory budget (10GB) is fixed before T is known; a T above the ~3,000/s estimate grows loadgen and gw-0 past what was budgeted on a host that already sat at its memory edge."
    evidence: "`t2_mem_budget 10` (before calibration); `victims=${SLOWLOGIC_VICTIMS:-$((30 * T))}`, `clients=$((2 * victims))`; Global Constraints: run 18's 0.41 cores at 5,000/s is an estimate the calibration replaces."
    disposition: accepted
    disposition_rationale: "SLOWLOGIC_MAX_VICTIMS 120000, clamp recorded; storm refused below 27T (corrected from 13T) (see dispositions file)"
  - id: O7
    category: specification quality
    severity: low
    claim: "Two numbers in the plan's reasoning are wrong: Logic's 64 workers are the Kick queue's, not Login's, and with GOMAXPROCS 2 a 25% quota throttles up to 87.5ms of each 100ms period, not 75ms."
    evidence: "SLOWLOGIC_CAL_QUEUE_CAP comment: \"enough to keep its 64 workers busy\" (internal/logic/config.go:40: `Workers int // per target gw|epoch (64)`, the Kick queue; Login concurrency is the gateway's LoginRPCs 16 × LoginBatch 200); Review Focus 1: \"CFS can throttle the whole cgroup for up to 75ms of every 100ms period\" while Review Focus 3 expects GOMAXPROCS 2."
    disposition: accepted
    disposition_rationale: "wording fixed (see dispositions file)"
---

## O1 — implementation — high

### Claim

The spec's rule bounds the cap by Logic's throughput *per gateway* × 4s. The plan uses the steady-state share (T/2, two gateways) and sets 2T. But the storm lands on gw-0 alone, and Logic then serves gw-0 at its full T. A full queue therefore drains in 2s, half the rule's 4s budget. `storm_expired` can only fail if the storm's real pace drops below T/2. The test passes whether or not the rule's factor is right. It tests a cap well under the rule, not "the queue cap sized by the rule above".

### Evidence

> storm sets `-login-queue-cap` = T/2 × 4s per gateway … SIGKILLs `gw-1`, so 30T relogins arrive at ~1.5T/s on `gw-0` (#113).

> A queued Login waits at most 2T / T = 2s, well inside the 5s deadline, so `EXPIRED` should be near zero.

The spec text the plan cites: "with the queue cap sized by the rule above, logins are rejected as `Busy`, not `EXPIRED`". In `internal/gateway/login.go:35` the cap counts logins "waiting for or inside a Login RPC", so the 1s margin also covers batching (200 per RPC) and the RPC itself. That margin is what a cap at the bound would test.

### Why this matters

R4H-O2's claim is that the factor (deadline − margin) is safe at the bound. A cap at half the bound leaves 3s of slack, which hides a margin that is too small (batch wait, an RPC slowed by CFS throttling, Redis). Ways to test the bound: set the storm's cap from the share in force during the storm (T × 4 on the lone survivor: it fills in ~8s at 1.5T in, T out, so Busy still comes). Or keep T/2 × 4 and build a storm that both gateways take at once, each draining T/2. The plan names neither, and does not say the gate is conservative by 2×.

## O2 — risk — high

### Claim

The plan's own prototype found the main way this scenario fails: T is underestimated, arrivals barely outrun Logic, and Busy almost never happens. One run had 25 Busy instead of ~20,000. With `storm_busy` at `> 0`, that run passes. The Busy path, the backoff after 5 in a row and the queue at its cap would get almost no exercise, and the report would show 15/15.

### Evidence

> The prototype first calibrated at the default cap of 8,192 … the storm's arrivals at 1.5 × T barely outran Logic: in one run 25 Busy instead of ~20,000.

> `t2_check storm_busy "$(echo "$gw_d" | lg_true - '(d["logins_busy"] or 0) > 0')"`

### Why this matters

The plan's arithmetic predicts a floor. The queue fills ~4s in, then ~0.5T/s of first attempts are turned away for ~16s, about 8T of 30T victims (~27%) before retries. The dry run saw 2× the victims. A floor such as busy ≥ 0.1 × victims (or the backoff count > 0) fails a storm that did not outrun Logic. Without one, the check that proves "the Login-queue cap triggers Busy" accepts noise. The b2 review added Kick floors for the same reason (O1, O6 there).

## O3 — implementation — medium

### Claim

Calibration logins are all first logins: no prior entry, Logic counts them `ok`. Storm logins replace entries of the SIGKILLed gw-1. Logic counts them `replaced` (`internal/logic/login.go:157`), and while gw-1's lease lives (20–30s, i.e. most of the 0–20s storm wait) each one queues a Kick to the dead address, with a refused connect and the dead-epoch cross-check. All of that runs inside the same 0.25-core quota, so the storm's pace T' is below T. The plan never measures T'. The storm's `logic_d` and `usage` cover kill → last victim (≈50s in the dry run, up to 600s), mostly idle time, so neither T' nor Logic's cores while the queue was full can be read from the result.

### Evidence

> The calibration measures T as Logic's ok logins per second …

`LOGIC_COUNTERS` includes `logins_replaced`, but T is `d["l"]["logins_ok"] / d["u"]["window_s"]`. The storm window: `stack_usage_start` at the kill, `stack_usage_json` after `lg_wait_back` and the profiles.

Task 3's failure guide: "`storm_busy` false → arrivals did not outrun Logic (T underestimated: compare Logic's cores in the storm with the quota)". That comparison needs a reading over the busy period, which the window averages away.

### Why this matters

If T' is well below T, the cap (2T) holds more than 2s of work and the storm is mis-sized in a direction the run cannot show. It also leaves O1's question open: is the 2× slack real? A short window over the storm's busy period (e.g., 5–20s after the kill, as b2's `storm30`), counting `ok + replaced` and Logic's cores, would record T' beside T.

## O4 — risk — medium

### Claim

Review Focus 2 names two ways the calibration goes wrong: under-offered load, and waste (queued logins that expire). Either way the storm is mis-sized. Only the first is gated: Logic's CPU ≥ 0.9 × quota is true in both cases, since wasted work also burns the quota. The waste counters are written to the `calibrate` field and left there. The storm then runs on a T that may be low, which is the prototype's 30%-low case.

### Evidence

> If queued logins expire, Logic's CPU partly goes to logins that do not count, and T comes out low (the prototype's first version, ~30% low). Either way the storm's cap and size come out wrong. Expected: `calibrate_saturated` requires Logic's CPU in the window ≥ 0.9 × the quota. When it fails, the storm phase does not run …

> Calibration's `expired`, `expired_queued` and `error` are recorded in the `calibrate` field and are 0 behind the 1,000 cap.

### Why this matters

The plan already expects these counters to be 0. Gating them in `calibrate_saturated` (or a sibling check that also skips the storm) costs one expression. Without it, a host where 1,000 per gateway is still too much (a T lower than expected) gives a storm that is too small and a low `storm_busy`. With O2 unaddressed, that still passes.

## O5 — risk — medium

### Claim

Review Focus 1 frames the lease as *the* risk of a CPU quota: lease and Login RPCs share 2 Ps and the throttled cgroup. Yet the gate can only fail once a renewal passes the 5s call timeout (`error`) or the lease is lost (`missing`). The run records no renewal latency, so a renewal that took 3s under throttling looks like one that took 3ms. Logic is saturated hardest in calibration (8,000/s offered, ~15s), and calibration has no lease check.

### Evidence

> Expected: every renewal still completes well inside the gateway's 5s lease timeout, so `gw-0` never sees `missing` or `error` …

`GW_COUNTERS` has only `lease_live`, `lease_missing`, `lease_error` (`goim_gateway_lease_calls_total`; `internal/gateway/metrics.go` has no lease latency histogram). `storm_lease` reads deltas from the kill onward. The calibrate phase has no lease check.

### Why this matters

R1-O2's "renewals continue" passes as long as the margin is above zero. The plan cannot report how close it came, which is the number a capacity decision needs. Options: add the lease check to calibration (cheap, same counters). Read renewal latency from what exists (gateway logs, Logic's gRPC server metrics if exported). Or say plainly that only the 5s bound is checked. "No Go change" rules out a new histogram; that limit should be stated, not left implicit.

## O6 — risk — medium

### Claim

The population comes from a measurement made at run time: 2 × 30T clients, no ceiling. The memory budget is a constant set before T exists. At ~20.65KB per SDK client (smoke 100k), T = 3,000 gives 180k clients ≈ 3.7GB in loadgen, plus 180k connections on gw-0 after the kill. But T is an estimate from a run (run 18) where Logic was not saturated. If batching at saturation makes Logic cheaper per login, T and the population double, and the 10GB budget and the swap check (#70) meet a load nobody sized. 6b2a showed this host near its memory edge.

### Evidence

> `t2_mem_budget 10` (line 370, before calibration)

> `victims=${SLOWLOGIC_VICTIMS:-$((30 * T))}`, `clients=$((2 * victims))`, `-max-conns $((clients + clients / 10))`

> Global Constraints: "Run 18 measured Logic at 0.41 cores for 5,000 logins/s … so 25% should sustain ~3,000 logins/s. The calibration replaces this estimate with a measurement."

### Why this matters

A swap in the storm fails `no_swap_in_storm`, but only after a 15-minute run. A host OOM takes the run down without a clean report. A cap on `victims` (refuse, or clamp and record, when 30T exceeds what the budget covers) or a budget check after calibration closes the gap.

## O7 — specification quality — low

### Claim

Two lines of reasoning use numbers that are not this system's. (1) The calibration cap's comment says 1,000 is "enough to keep its 64 workers busy". Logic's 64 workers are the Kick queue's, per target. Login concurrency is set by the gateway: 16 concurrent RPCs × 200 per batch, and a 1,000 cap allows at most 5 RPCs per gateway. (2) With GOMAXPROCS 2 (Review Focus 3's expectation), a 25ms-per-100ms budget burns in 12.5ms of wall time, so throttling lasts up to 87.5ms, not 75ms.

### Evidence

> SLOWLOGIC_CAL_QUEUE_CAP comment: "enough to keep its 64 workers busy"

`internal/logic/config.go:40`: `Workers int // per target gw|epoch (64)`, `Queue: 1024`, the Kick queue. `internal/gateway/config.go`: `LoginBatch 200`, `LoginRPCs 16`.

> Review Focus 1: "CFS can throttle the whole cgroup for up to 75ms of every 100ms period"

### Why this matters

Neither changes a gate. But an executor or a reader of run 29 who uses them to explain a result (Logic not saturating behind the cap, lease latency) starts from the wrong model. The cap's real constraint is "enough in flight to keep 0.25 cores busy across ≤ 5 batches per gateway", and `calibrate_saturated` is what checks it.

## Explicitly not objecting to

- **The 30s SLO recorded, not gated**: decided by the user as #113, and the plan's arithmetic (a ~25s backlog plus retries) supports that a gate would be fragile either way.
- **Refusing a host without cpu delegation instead of #94's `taskset` fallback**: the plan argues it well. One CPU is a different slowdown from a throttled quota, and the Tier 2 host delegates `cpu`, so the fallback would test something else on a host that is never used.
- **`slowlogic` as its own scenario rather than a `gwloss` phase**: its calibration feeds the next phase's sizing, and gwloss's sweep, precompute and Kick gates do not apply. Separate scripts are cheaper than flags.
- **`stack_wait_idle … dropped-ok` accepting dropped Logouts without checking that Reconcile repairs them**: #94 (amended) decided dropped Logouts are recorded, the spec allows them, and each phase starts on a fresh stack, so leftover entries cannot leak into a later check.
- **The `systemd-run --user --scope` wrapper keeping PID, process group and affinity**: the prototype checked `/proc/PID/comm`, the cgroup path and `cpu.max`, and `stack_test.sh` pins all three.
- **Calibration offer level and window (8,000/s for 15s, window 5–15s)**: ~75k Busy'd clients are still retrying at 1–3s when the window ends, so offered load stays above T, and `calibrate_saturated` catches it if not.
- **Loadgen being the bound in the storm**: arrivals at ~1.5T ≈ 4,500/s plus Busy retries are well under the ~10k/s that b1's 200k storm drove from the same 8 CPUs. O2's floor would catch a storm loadgen could not deliver anyway.
- **Logic's GOMAXPROCS recorded, not gated**: Go's cgroup-aware default is a runtime fact, not a property of the system under test. Recording it is enough to explain latency.
