# Dispositions: Plan 6b3b3 review (2026-10-17)

Objections: `connection-session-core-06b3b3-slow-phase.md`. The user decided them with AskUserQuestion: O1–O4 first, then O5, which was asked after O1's split made it matter more. All five are accepted. O1 amends #127.1. The changes below go into the plan and are re-verified in `proto-6b3b3`, then replayed into the plan byte for byte, before execution.

## O1 — accepted: split the floods, slow-handshake on Logic's set

**Decision:** the slow-handshake flood (0.83 Zen 5 cores in run 30b, no DH) runs on Logic's set, which was 94.6% idle in run 30b (~3.8 of 4 CPUs free), beside Logic and Redis. Only the slow-auth flood (2.68 cores, ~7k DH/s) shares the gateway set. Review Focus 1 gets the healthy-ramp arithmetic in place of the whole-phase average.

**Rationale:** Review Focus 1 estimated room from run 30b's 244s average, when 90k of 400k were online. A healthy ramp puts about 17k handshakes/s on the gateways (~4.4 CPUs at run 30b storm's 0.26 CPU per 1k/s). Both floods on Zen 5c (3.29 against 5.16 GHz) add ~5.5 CPUs, which is ~10 of 12, about 83% against the 85% threshold, and on SMT pairs. With slow-auth alone (~4.2 CPUs) the estimate is ~8.6 of 12, about 72%. Logic's set, rejected in #127.1 as too small for both floods' 3.5 cores, has room for slow-handshake's ~1.3 Zen 5c CPUs.

**Plan changes to apply:**
- Settings: `FLOODS_SLOW_HS_CPUS` (default `T2_CPUS_LOGIC`) and `FLOODS_SLOW_AUTH_CPUS` (default `T2_CPUS_GATEWAY`) replace `FLOODS_SLOW_FLOOD_CPUS`. `slow.flood_cpus` records both lists. Validation as in O5.
- Header, Architecture, Implementation choice 1, Review Focus 1: the split, and the arithmetic above (healthy ramp, Zen 5c factor, SMT caveat).
- Review Focus 4: the sampler stays on Logic's set, now beside slow-handshake. Check its cost there, or move it, and settle this in the prototype.
- Roadmap #127.1: "decided as amended by review O1" (the split; the alternatives as before, Logic's set too small for *both* floods).
- Task 2's reading: `slow.usage` `flood` against Logic's set's busy share, and Logic's own latency (login latency) for interference from slow-handshake.

## O2 — accepted: cap the floods' GOMAXPROCS and measure the gateways' run-queue delay

**Decision:** each flood instance starts with `LG_ENV=(GOMAXPROCS=N)`. N is sized in the prototype from run 30b's cores on Zen 5c (proposed: slow-auth 4, slow-handshake 2) and stated in Implementation choice 1. The series gains each gateway's run-queue delay per second, summed over its threads (`/proc/PID/task/*/schedstat`, field 2, ns waiting on a run queue; `/proc/PID/schedstat` alone is the main thread only). The column is the larger gateway's ms per second, e.g. `gw_runq_ms_per_s`, with `first_s` and a row of reading guidance.

**Rationale:** without `LG_ENV`, Go sizes each flood's GOMAXPROCS from its 12-CPU mask, which puts 24 flood Ps beside the gateways' 12 on SMT pairs. Their effect is run-queue delay and longer slot holds, not busy share. Review Focus 1 asks the record to decide whether a refusal was the gateway's own, and Review Focus 5 would treat a valid run's `pending_mem` rejects as a spec finding. Neither has a measure to go on.

**Plan changes to apply:**
- `phase_slow`: `LG_ENV` for both floods (restored afterwards, so the light instance is unchanged).
- `slow_sampler`/`slow_series`: the run-queue reading for `gw-0`/`gw-1` (PIDs from `STACK_PIDS`), the new column, and `first_s` if a threshold is defined (settle in the prototype; else record only).
- Review Focus 1 and 5: a valid run with refusals is read against `gw_runq_ms_per_s`. A refusal that comes with a high run-queue delay is not yet a spec question.
- Dry run: the new column is present and non-null.

## O3 — accepted: the order of events decides the ruling, not the verdict's precedence

**Decision:** the verdict keeps its order (`unread`, `loadgen-saturated`, …). Review Focus 2 and the `not_covered` line say that a non-valid verdict is ruled on from `slow.series.first_s`. If loadgen saturated first, it is a harness limit (smaller light population or more loadgen CPUs). If a gateway refusal (`pending_mem`, `no_slot`) or a rise in `pending_max` came first, it is a possible gateway finding, with the retries as its consequence.

**Rationale:** gateway refusals make light clients retry, and those retries alone can saturate loadgen. That was run 30b's open question. Review Focus 2's reading ("needs a smaller light population or more loadgen CPUs") assumes the cause runs one way only.

**Plan changes to apply:**
- Review Focus 2: the remedy depends on the order in `first_s`, as above.
- `not_covered` text: add "rule by slow.series.first_s order", e.g. `slow: legitimate logins and heartbeats beside the floods; verdict "<v>", rule by the order in slow.series.first_s`.
- Implementation choice 2: one sentence saying the precedence orders the label, not the cause.
- Task 2's reading: the ordering question comes first for any non-valid verdict.

## O4 — accepted: name the fourth outcome in Task 2

**Decision:** Task 2 Step 1 adds a fourth outcome: verdict `valid` with `slow_legit` failing and no `pending_mem` rejects (clients given up after `no_slot` or `login` failures), or `slow_heartbeats` failing alone. It goes to the user before anything changes, like the others. A failure from `no_slot` alone is recorded as #111's input.

**Plan changes to apply:**
- Task 2 Step 1: the outcome above. *After this plan* → #111: point to it.

## O5 — accepted: the verdict reads every set a flood runs on

**Decision:** the series and the verdict read all three sets: loadgen's, the gateways' and Logic's. A new verdict, `logic-set-saturated`, covers Logic's set at ≥ 85% busy in either window. Order: `unread`, `loadgen-saturated`, `gateway-set-saturated`, `logic-set-saturated`, `valid`. Each flood's CPU list must lie inside one of the three sets (`T2_CPUS_GATEWAY`, `T2_CPUS_LOGIC`, `T2_CPUS_LOADGEN`), or the script refuses it with a message. That way every flood's CPUs are read by the verdict.

**Rationale:** after O1, slow-handshake shares Logic's set with Logic and Redis. A saturated Logic set would slow logins (AUTH_PENDING held longer, which feeds `pending_mem`) while the verdict says `valid`.

**Plan changes to apply:**
- `slow_sampler`: a `logic` member in `sets`. `slow_series`: a `logic_set_busy_pct` column and `first_s.logic_set_saturated`.
- `slow_verdict`: the `logic-set-saturated` branch. The JSON table in Task 1 Step 3 gets a row for it and one checking that loadgen still comes first.
- Settings check: each of `FLOODS_SLOW_HS_CPUS`/`FLOODS_SLOW_AUTH_CPUS` is a CPU list whose CPUs all lie in one of the three sets (`t2_cpulist_expand`).
- Header and Produces: the verdict's values, now five.
- `t2_cpusets_json` already reports `logic`, so `slow.window` needs no change.
