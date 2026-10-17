---
spec: docs/superpowers/plans/2026-10-17-connection-session-core-06b3b3-slow-phase.md
date: 2026-10-17
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: risk
    severity: high
    claim: "Review Focus 1's room estimate for the gateway set uses run 30b's whole-phase average, in which only 90k of 400k clients got online. In a healthy run the light clients' ramp puts about 17k handshakes/s on the gateways while the floods, now on Zen 5c, take about 5.5 CPUs. That is about 10 of 12 CPUs, near the 85% threshold, so run 30c could easily come back gateway-set-saturated and fail both checks again."
    evidence: "Review Focus 1: \"On the gateway set's Zen 5c cores they will cost more, perhaps 4.5 of its 12 CPUs. The set was 69.5% idle, about 8.3 CPUs free\"; run 30b slow: \"90,334 of 400,000 online\", the gateway set 69.5% idle over 244s; run 30b storm: gw-0 used 5.31 cores at seen_per_s 20,780; slow-auth's \"~7k handshakes/s\" (floods.sh header, 6b3a Review Focus 5); roadmap #71/#108: Zen 5 5.16 GHz and Zen 5c 3.29 GHz, SMT siblings n and n+12; Implementation choice 2: the verdict reads the ramp window on its own."
    disposition: accepted
    disposition_rationale: "split the floods: slow-handshake on Logic's set, slow-auth on the gateway set; healthy-ramp arithmetic in Review Focus 1; amends #127.1 (see dispositions file)"
  - id: O2
    category: risk
    severity: medium
    claim: "Below 85% busy, the verdict can't see how much the co-located floods slow the gateways. The two flood processes start with GOMAXPROCS 12 each, which adds 24 Ps beside the gateways' 12, on SMT pairs. Their effect shows as run-queue delay, longer handshake-slot holds and so more no_slot refusals, not as busy share. Review Focus 1 says the record must decide whether a refusal was the gateway's own, but gives it no measure to decide with. Review Focus 5 would call a valid run with pending_mem rejects a finding against the spec."
    evidence: "Review Focus 1: \"a gateway that is slow beside the floods would now be slow in part because of them ... The record must read them before calling a gateway refusal the gateway's own\"; Review Focus 5: \"A valid run with pending_mem rejects is a real finding against the spec's Slow clients row\"; Implementation choice 1: `LG_CPUS=$FLOOD_CPUS lg_in flood lg_start …` with no LG_ENV; loadgen.sh:111 `taskset -c`, so Go sizes GOMAXPROCS from the 12-CPU mask; roadmap #79: each gateway's GOMAXPROCS is its share (6 with STACK_GATEWAYS=2)."
    disposition: accepted
    disposition_rationale: "floods' GOMAXPROCS capped via LG_ENV; per-second gateway run-queue delay (schedstat over threads) in slow.series (see dispositions file)"
  - id: O3
    category: implementation
    severity: medium
    claim: "The verdict checks loadgen first, and Review Focus 2 reads loadgen-saturated as 'the slow phase needs a smaller light population or more loadgen CPUs'. A loadgen saturated by retries is also what a gateway refusing legitimate clients produces, which is run 30b's open question. The verdict ignores the series' ordering, and the not_covered line files a possible gateway failure as a harness gap."
    evidence: "Implementation choice 2: \"Loadgen comes first because its saturation invalidates both checks' readings, whatever the gateways did\"; Review Focus 2: \"Run 30b's light instance used 4.42 cores, retries included ... a retry cascade would not [fit] ... it would say the slow phase needs a smaller light population or more loadgen CPUs\"; run 30b reading: \"how much is loadgen's saturation feeding retries. The run has no time series to order the two\"; not_covered text: \"slow: legitimate logins and heartbeats beside the floods; verdict \\\"<verdict>\\\"\"."
    disposition: accepted
    disposition_rationale: "verdict order kept; Review Focus 2, not_covered and Task 2 rule a non-valid verdict by the order in first_s (see dispositions file)"
  - id: O4
    category: specification quality
    severity: low
    claim: "Task 2 lists the expected outcomes as a pass, a non-valid verdict, or valid with pending_mem rejects. It doesn't cover a valid verdict where slow_legit fails without pending_mem (clients given up after no_slot or login failures) or slow_heartbeats fails alone, so the record has no guidance for those cases."
    evidence: "Task 2 Step 1: \"Two other outcomes are still valid records: Verdict loadgen-saturated or gateway-set-saturated ... Verdict valid with pending_mem rejects: a gateway finding\"; slow_legit gates `d[\"o\"] is True` (every client online) and hs_no_slot is recorded, not gated; run 30b: hs_no_slot 157,018 and 154,271."
    disposition: accepted
    disposition_rationale: "Task 2 names the fourth outcome (valid verdict, failure without pending_mem); no_slot-only failure is #111 input (see dispositions file)"
  - id: O5
    category: implementation
    severity: low
    claim: "FLOODS_SLOW_FLOOD_CPUS can be set to any CPU list, but the verdict and the series always read T2_CPUS_LOADGEN and T2_CPUS_GATEWAY. If the setting is moved, for example to Logic's set or back to loadgen's to reproduce run 30b, a saturated flood set goes unread and the verdict can say valid."
    evidence: "Implementation choice 1: \"It is validated as a CPU list, not a number\"; slow_sampler: `sets=\"\\\"loadgen\\\": [$(set_ticks \"$T2_CPUS_LOADGEN\" …)], \\\"gateway\\\": [$(set_ticks \"$T2_CPUS_GATEWAY\" …)]\"`; slow_verdict reads only `w[\"loadgen\"]` and `w[\"gateway\"]`."
    disposition: accepted
    disposition_rationale: "series and verdict read all three sets, new logic-set-saturated; each flood's CPU list must lie in a known set (see dispositions file)"
---

# Objections: Plan 6b3b3 (the slow phase on its own CPUs)

## O1 — risk — high

### Claim

Review Focus 1 checks that the gateway set has room for the floods against run 30b's slow phase: 69.5% idle over 244s, so about 8.3 CPUs free. That phase was not a healthy run. Only 90,334 of 400,000 light clients got online, and the gateways were refusing. In the run the plan aims for, the gateways handle the light ramp's 10,000 handshakes/s plus slow-auth's ~7,000/s, about 17,000 DH/s over two gateways. Run 30b's storm cost gw-0 5.31 CPUs at ~20,800 handshakes/s seen, about 0.26 CPU per 1,000/s, so this is about 4.4 CPUs before logins and Pings. The floods used 3.51 Zen 5 cores. Zen 5c runs at 3.29 GHz against 5.16, so the same work takes about 5.5 CPUs, and more when SMT siblings are busy. That adds up to about 10 of 12 CPUs, roughly 83% busy, over the ramp window the verdict reads separately. The verdict's threshold is 85%.

### Evidence

> On the gateway set's Zen 5c cores they will cost more, perhaps 4.5 of its 12 CPUs. The set was 69.5% idle, about 8.3 CPUs free, so the gateways should keep room. (Review Focus 1)

Run 30b: slow phase 90,334 of 400,000 online; storm `gw-0 used 5.31 cores`, `seen_per_s 20,780`. Roadmap #71/#108: cores 4–11 are Zen 5c at 3.29 GHz, SMT siblings n and n+12. Implementation choice 2: the ramp window has its own `t2_cpusets_json`.

### Why this matters

If run 30c comes back `gateway-set-saturated` over the ramp, both slow checks fail labelled, and the user needs another plan and another run (30d). The estimate in Review Focus 1, which a reviewer would rely on, understates that risk. The 85% threshold is also measured on logical CPUs. On SMT pairs, 80% logical busy already means most physical cores have both threads loaded. Ways to answer it:
- Accept the risk as stated, with the arithmetic written down.
- Lower the floods' share, for example slow-handshake (0.83 cores, no DH) on Logic's set, which had ~3.8 CPUs free, and only slow-auth on the gateway set.
- Settle it with a short calibration on the host before the full run.

## O2 — risk — medium

### Claim

The floods are Go processes pinned to the 12-CPU gateway set with no `LG_ENV`, so each runs 12 Ps. Beside the two gateways' 6 Ps each, that is 36 Ps on 12 CPUs, on SMT pairs. Interference of this kind shows up first as scheduler delay for the gateways' loop threads. A slower handshake holds its concurrency slot longer, which turns into more `no_slot` refusals and more legitimate clients left pending. Average busy share below 85% doesn't show any of this. So a `valid` verdict doesn't establish that the gateways ran as they would alone. Review Focus 1 asks the record to decide whether a refusal was the gateway's own, but neither the series nor the verdict gives it a number to decide with. Review Focus 5 then treats a valid run with `pending_mem` rejects as a finding against the spec.

### Evidence

> But a gateway that is slow beside the floods would now be slow in part because of them … The record must read them before calling a gateway refusal the gateway's own. (Review Focus 1)

> A valid run with `pending_mem` rejects is a real finding against the spec's *Slow clients* row. (Review Focus 5)

`loadgen.sh:111` wraps loadgen in `taskset -c "${LG_CPUS…}"`, so Go sizes GOMAXPROCS from that mask. Roadmap #79: each gateway's GOMAXPROCS is its share of the set.

### Why this matters

A spec follow-up such as `PendingCap` sizing could rest on refusals that the harness caused by co-locating the floods. Ways to answer it:
- Cap the floods' GOMAXPROCS through `LG_ENV`.
- Add the gateways' run-queue delay (`/proc/PID/schedstat`, field 2) to the per-second series, so "the gateway's own" has a measure.
- Accept that a valid run with refusals needs a rerun without floods on the set before it becomes a spec question.

## O3 — implementation — medium

### Claim

The verdict checks `loadgen-saturated` before anything else. Review Focus 2 reads that outcome as the light population being too big for loadgen. But run 30b's open question is the reverse: gateway refusals (`pending_mem`, `no_slot`) make light clients retry, and the retries, Noise handshakes that are expensive on the client too, saturate loadgen. The verdict can't tell the two apart. The series' `first_s` can, but it feeds only the record, not the verdict or the `not_covered` wording.

### Evidence

> Loadgen comes first because its saturation invalidates both checks' readings, whatever the gateways did. (Implementation choice 2)

> Run 30b's light instance used 4.42 cores, retries included … a retry cascade would not [fit] … it would say the slow phase needs a smaller light population or more loadgen CPUs, a ruling for the user. (Review Focus 2)

Run 30b's reading: "how much is loadgen's saturation feeding retries. The run has no time series to order the two."

### Why this matters

If run 30c's gateways refuse first, the run is labelled a harness gap ("not covered"), and the remedy Review Focus 2 suggests (fewer clients, more loadgen CPUs) would remove the load that exposed the gateway problem. Task 2's reading list does ask the ordering question, which helps, but the verdict and the Review Focus text both point the other way. Ways to answer it:
- Make the verdict or its label depend on the order: for example `loadgen-saturated` only when `first_s.loadgen_saturated` comes before `first_s.pending_mem`/`no_slot`, otherwise `gateway-first`.
- Reword Review Focus 2 and the `not_covered` line to say the order decides the ruling.

## O4 — specification quality — low

### Claim

Task 2 Step 1 names three outcomes. A fourth is plausible and isn't named: a valid verdict where `slow_legit` fails with no `pending_mem` rejects, or `slow_heartbeats` fails alone. `slow_legit` needs every client online, and `no_slot` refusals are recorded but not gated, so a run with clients given up after repeated `no_slot` or `login` failures fails with nothing in the plan to say how to read it.

### Evidence

> Two other outcomes are still valid records: Verdict `loadgen-saturated` or `gateway-set-saturated` … Verdict `valid` with `pending_mem` rejects: a gateway finding. (Task 2 Step 1)

`slow_legit`: `d["o"] is True`. Run 30b: `hs_no_slot` 157,018 and 154,271.

### Why this matters

This is minor. The record of an unnamed outcome would be written without guidance, and without saying that it goes to the user before anything changes, as the named outcomes do. It also feeds #111 (parked): a valid run failed by `no_slot` alone is direct evidence for it.

## O5 — implementation — low

### Claim

`FLOODS_SLOW_FLOOD_CPUS` is a free CPU list, but the sampler and the verdict always read `T2_CPUS_LOADGEN` and `T2_CPUS_GATEWAY`. If the setting points elsewhere, the floods' own set is never read and the verdict can say `valid` while they are starved. Moving it to Logic's set is the obvious next try if O1's risk comes true. Moving it back to loadgen's set reproduces run 30b.

### Evidence

Implementation choice 1: "It is validated as a CPU list, not a number." `slow_sampler` reads `set_ticks "$T2_CPUS_LOADGEN"` and `set_ticks "$T2_CPUS_GATEWAY"`. `slow_verdict` reads `w["loadgen"]` and `w["gateway"]` only.

### Why this matters

A knob the plan provides gives a wrong verdict when it is used. Ways to answer it:
- Refuse a list that isn't inside the gateway set.
- Have the verdict read the floods' list as its own set.
- Document it as gateway-set-only.

## Explicitly not objecting to

- **Failing labelled rather than passing like `storm_rate` (#127.2):** the user ruled on it with a stated reason (both checks gate gateway behaviour). Nothing in the plan undercuts that reason.
- **Not moving the garbage flood:** garbage passed in run 30b, and the plan argues that changing a passing phase costs comparability.
- **Window averages and not a per-second gate (Review Focus 3):** the plan names the burst risk and records `first_s` for it. The dev machine's 93% first second is real evidence that a per-second gate would be noisy.
- **The sampler's own cost and the ~1s skew between CPU and scrape readings (Review Focus 4):** it is pinned to Logic's mostly idle set. `/stats` is a few counters (`Runner.Stats`), not a scan of clients. One-second ordering is enough for causes run 30b separated by tens of seconds.
- **`taskset -apc` after the sampler's first fork:** at most the first second's children run unpinned, which can't change a 60s+ window.
- **The sampler outliving an aborted run:** it reuses the global `smp`, which the existing `t2_on_exit` hook already kills.
- **`pending_max` as a stand-in for `g.pending`:** the gateway's HANDSHAKE and AUTH_PENDING gauges move exactly where `pending` does (`gateway.go` `Open`, `Closed`, `setState`).
- **No unit tests for `floods.sh`:** this follows 6b3b/6b3b2. The dry run and the forced `loadgen-saturated` case cover the new gates, and the other verdict branches are table-checked on JSON.
