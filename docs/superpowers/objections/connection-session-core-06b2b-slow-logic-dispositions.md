# Dispositions: Plan 6b2b-slow review (2026-10-15)

Objections: `connection-session-core-06b2b-slow-logic.md`. Decided by the user, one at a time.

## O1 — accepted: the storm's cap is T × 4

**Decision:** `SLOWLOGIC_QUEUE_CAP` defaults to T × 4. That is the spec's rule applied to the share in force during the storm: gw-0 is alone, and Logic serves it at the full T.

**Rationale:** at T/2 × 4 = 2T, a full queue drains in 2s, half the rule's 4s budget. `storm_expired` could then fail only if the storm's pace fell below T/2, so the rule's margin (1s for batching and the RPC) was never under load. At 4T, queued waits reach ~4s. With arrivals at ~1.5T and a drain of T, the queue fills ~8s in, and Busy follows for the rest of the 0–20s storm wait.

**Plan changes to apply:**
- Goal, Architecture, choice 1, the script's header and settings comment, `qcap` default `$((T * 4))`, and roadmap #113: the cap is "T × 4s (the spec's rule for the lone survivor, which Logic serves at T)". Queued waits are ≤ 4s, not 2s. The queue fills ~8s in, not ~4s.
- Task 2 Step 2's expected values: `-login-queue-cap` = 4T (≈ 1,384 at T 346). Re-run the dry run; record the new Busy, EXPIRED and back numbers.
- Task 3's failure guide: an `expired_queued` or Logic `expired` above the gate means the rule's 1s margin is too small at this cap. That is a finding against R4H-O2, not a reason to lower the cap.

## O2 — accepted: storm_busy needs Busy ≥ 0.1 × victims

**Decision:** `storm_busy` passes when gw-0's Busy in the storm is ≥ 0.1 × victims, not > 0.

**Rationale:** the prototype's run with an underestimated T had 25 Busy instead of ~20,000, and `> 0` passes it. At the 4T cap (O1), the queue fills ~8s in. First attempts are then turned away at ~0.5T/s for the remaining ~12s of the storm wait: about 6T of 30T victims (~20%), before retries. 0.1 × victims sits well under that, and still fails a storm that barely outran Logic, or one loadgen could not deliver.

**Plan changes to apply:**
- `storm_busy`'s expression: `(d["logins_busy"] or 0) >= 0.1 * victims`. Its detail gains `"floor": 0.1 × victims`.
- Choice 1's gates, Review Focus (a new line, or in choice 1), the header's check comment, and roadmap #113: "Busy ≥ 0.1 × victims".
- Task 2 Step 2: re-check the dry run's Busy against the floor at the 4T cap. Task 3's failure guide: `storm_busy` under the floor → T overestimated or loadgen short (compare Logic's cores in the storm, O3).

## O3 — accepted: the storm's own pace T' recorded over its busy period

**Decision:** a second window inside the storm, from the kill to +20s (`SLOWLOGIC_BUSY_WINDOW`, default 20), records T' (Logic's `ok + replaced` per second), Logic's cores, and its Kick counters, beside T in the `storm` field (`busy_window`). Recorded, not gated.

**Rationale:** T is measured on fresh logins (all `ok`). Storm relogins replace the dead gw-1's entries (`replaced`), and while gw-1's lease lives each one queues a Kick to a refused address, plus the dead-epoch cross-check, all inside the same 0.25-core quota. The storm's whole window (kill → last victim, up to 600s) averages the busy period away, so neither T' nor Logic's cores while the queue was full could be read. Task 3's failure guide already asks for "Logic's cores in the storm"; this provides them.

**Plan changes to apply:**
- Task 2: after the kill, a background subshell reads Logic's counters (`logins_ok`, `logins_replaced`, and the Kick counters `stack_counters` can name) and starts a usage window (`stack_usage_start`), sleeps `SLOWLOGIC_BUSY_WINDOW`, reads them again, and writes `busy_window` (`window_s`, `throughput_per_s` = (ok + replaced) / window, `logic_cores`, `kicks`) to an artifact the main shell reads. Placed like b2's `storm30`: it must not restart the storm's whole-window usage reading.
- The `storm` field gains `busy_window` (T' beside `throughput_per_s` = T). `LOGIC_COUNTERS` gains the Kick counters it needs.
- Review Focus 2 (or a new line): T' against T says whether the calibration represented the storm. T' well below T means the cap held more than 4s of the storm's work, which also explains an `expired_queued` (O1).
- Task 3's "What to read" and failure guide name `busy_window`. The dry run records it.

## O4 — accepted: calibrate_saturated also gates wasted work ≤ 1%

**Decision:** `calibrate_saturated` requires Logic's CPU ≥ 0.9 × the quota **and** waste ≤ 1% of Logic's ok logins in the window. Waste is Logic's `expired` plus both gateways' `expired_queued` and `error`. On a fail, the storm is skipped, as for a CPU miss.

**Rationale:** Review Focus 2 names two ways T comes out wrong, under-offered load and wasted work, and only the first was gated. Wasted work burns the quota too, so the CPU gate passes it (the prototype's ~30%-low T at the 8,192 cap). The plan expects 0 behind the 1,000 cap; 1% leaves room for a stray error without accepting the prototype's case.

**Plan changes to apply:**
- `saturated` = CPU condition and `(logic expired + Σ gw expired_queued + Σ gw error) ≤ 0.01 × logic logins_ok`. The check's detail gains `"waste"` (the sum and its parts) and `"waste_max"`.
- Review Focus 2 and the header's check comment: "… and wasted logins ≤ 1% of ok". Task 3's failure guide: waste above 1% → lower `SLOWLOGIC_CAL_QUEUE_CAP`.

## O5 — accepted: calibrate_lease added; the 5s-bound limit stated

**Decision:** a new check `calibrate_lease`, on the same counters as `storm_lease`. Both gateways read from loadgen's start in calibrate to just before `down calibrate`: no `missing`, no `error`, `live` ≥ elapsed // 10 − 1 per gateway, and Logic's `lease_error` 0. Review Focus 1 and the README row say that only the 5s call timeout is checked: renewal latency is not measured, since this plan makes no Go change.

**Rationale:** calibration saturates Logic hardest (8,000/s offered behind two queues) and longest, yet had no lease check. The counters are already read. A latency histogram would give the real margin but means a Go change and a task. Stating the limit makes R1-O2's "renewals continue" a checked bound, not a measured margin, on the record.

**Plan changes to apply:**
- Task 2: gateway counters for both gateways and Logic's `lease_error` at `lg_start calibrate` (before the skip) and before `down calibrate`. Then `t2_check calibrate_lease …` with per-gateway `live`/`missing`/`error`, `elapsed_s`, `logic_lease_error`.
- Checks: 16 on a full run (header comment, Produces, Task 2 Step 2's "15 checks", Task 3 Step 1's list and "16/16").
- Review Focus 1: "checked against the 5s timeout only; renewal latency is not measured (no Go change); a margin needs a gateway lease histogram, a later choice". The README row: "lease renewals missed: 0 (5s bound; latency not measured)".

## O6 — accepted: victims capped by `SLOWLOGIC_MAX_VICTIMS`, the clamp recorded

**Decision:** `SLOWLOGIC_MAX_VICTIMS` (default 120,000: 240k SDK clients ≈ 5GB in loadgen at ~20.65KB each, inside the 10GB budget with the gateways). Victims = min(30T, the max). A clamp is recorded in the `storm` field (`clamped`, `victims_per_t` = victims / T). Below the size at which the Busy floor can be met (see the correction), the storm is refused (not run), and a failed `storm_size` check names T, the max and the size it needed.

**Rationale:** the population comes from T, measured at run time, and the memory budget is fixed before T exists. Run 18's 0.41 cores at 5,000/s was not a saturated measurement, so T could come in well above ~3,000/s.

**Correction (while applying):** the question said Busy still comes at ≥ ~13T victims. That was wrong. With victims V arriving over the 0–20s storm wait at V/20 per second, a drain of T and the 4T cap (O1), the queue fills only if (V/20 − T) × 20 > 4T, i.e. V > 24T. First-attempt Busy ≈ 20T(r − 1) − 4T with r = V/20T. Meeting O2's floor (Busy ≥ 0.1 V) needs r ≥ 1.33, i.e. **V ≥ ~27T**. So a clamp leaves a narrow band (27T–30T): the default max only clamps for T in 4,000–4,444/s, and above ~4,444/s the storm is refused. The refusal threshold is 27T, not 13T. If run 29 refuses, the remedy is a lower `SLOWLOGIC_QUOTA` (smaller T), recorded, not a smaller storm.

**Plan changes to apply:**
- Task 2: `max_victims=${SLOWLOGIC_MAX_VICTIMS:-120000}`. `victims` = min(30T, max) unless `SLOWLOGIC_VICTIMS` is set. If victims < 27T, `t2_check storm_size false` with `{"throughput_per_s", "max_victims", "need": 27T}`, then `t2_finish` before `up storm`. On a full run, `storm_size` passes and records the same fields plus `clamped` (17 checks with O5's `calibrate_lease`).
- Settings comment, choice 1 (the 27T bound and why), Review Focus 2 (a T far above the estimate → `storm_size`), roadmap #113, Task 3's failure guide (`storm_size` → lower `SLOWLOGIC_QUOTA`).

## O7 — accepted: two wrong numbers fixed

**Decision:** the calibration cap's comment says the 1,000 cap allows ≤ 5 Login RPCs of 200 per gateway (the gateway's `LoginRPCs` 16 × `LoginBatch` 200), enough to keep 0.25 cores busy, which `calibrate_saturated` checks. Logic's 64 workers are its Kick queue's and are not named. Review Focus 1 says throttling lasts up to 87.5ms of each 100ms period (25ms of budget spent in 12.5ms on GOMAXPROCS 2), not 75ms.

**Plan changes to apply:** the `SLOWLOGIC_CAL_QUEUE_CAP` comment in the script (and choice 2 if it repeats it); Review Focus 1's throttle figure.

## Follow-up F1 — the re-prototype at the 4T cap: EXPIRED ~9%; cap and gates kept

**Finding (dry run, 5% quota, 2026-10-15):** T = 347/s on fresh logins. In the storm's first 20s, T' = 285/s (82%), with Logic at its quota. Every storm login was `replaced`, each with a Kick to the dead gw-1 (5,700 in 20s). The 4T cap (1,388) therefore held 4.9s of work at T', past the rule's 4s: Logic `expired` 962 (~9% of 10,410 ok), gw-0 `error` 219, login p99 6.4s. `storm_busy` passed (18,505 against the 1,041 floor). Under the old 2T cap, the first prototype had 0 EXPIRED.

**Decision:** keep the 4T cap and the gates. The plan records the dry run as the expected result: `storm_expired` may fail in run 29, which is a finding against R4H-O2 (the rule's "measured Logic login throughput" must be relogin throughput, or the margin must be larger), not a harness bug. The spec question is decided after run 29.

**Rejected:** calibrating on relogins (a small kill storm), which costs more script, a longer run and a re-prototype; and reverting to 2T, which hides the gap behind half the rule's budget (O1).

## Follow-up F2 — run 29: the rule's 1s margin is too small; margin 3s (#115)

**Finding (Tier 2, 2026-10-16, `04dc99a`, three runs):**

| run | T (fresh) | T' (storm) | cap | cap / T' | EXPIRED | latency max |
|---|---|---|---|---|---|---|
| 29 | 3,550 | 3,116 (88%) | 14,200 (4T) | 4.56s | 3,701 (3.5%) | 5.9s |
| 29b | 3,710 | 2,814 (76%) | 12,464 (≈ run 29's T' × 4) | 4.43s | 2,775 (2.5%) | 5.75s |
| 29c | 3,443 | 2,970 (86%) | 9,275 (2.69T) | 3.12s | 0 | 4.8s |

Every EXPIRED was Logic's (the login reached Redis more than 5s after the gateway received it); the gateway queue expired none. Two things break the 1s margin: T' (relogins that each Kick a dead gateway) is 76–88% of T and varies by 12 points between runs, so sizing on a measured T' is fragile; and logins wait 1.3–1.7s beyond cap / T' (Logic's pipeline under CFS throttling).

**Decision (the user):** margin 3s: `login_queue_cap ≤ T × (login_deadline − 3s)`, 2T at the spec's 5s deadline, with T measured on fresh logins. Worst seen: 2.6s nominal + 1.7s ≈ 4.3s. `slowlogic`'s default cap becomes T × 2 (#115).

**Rejected:** (a) T measured on relogins with the 1s margin (run 29b failed; T' moves); 2.5T, 2.5s margin (worst seen ≈ 4.97s, at the deadline); keeping the rule and accepting EXPIRED.

**Spec changes to apply** (rev6, *Gateway → Admission control*, Login-queue cap, **Sizing** *(R4H-O2)*): `login_queue_cap ≤ measured Logic login throughput per gateway × (login_deadline − margin)` gains: the margin is 3s at the 5s `login_deadline` (2 × throughput), since under a CPU limit relogins with Kicks run at 76–88% of fresh logins and queued logins wait 1.3–1.7s beyond the queue's nominal drain (Tier 2 run 29, #115). Not applied to the spec yet (the user decides).

**On hold (the user, 2026-10-16, after run 29e):** a 3s margin is not acceptable; the spec keeps `login_queue_cap ≤ T × (login_deadline − 1s)`, and the *Spec changes to apply* above are withdrawn. The question becomes how the system meets the 1s margin. Hypothesis from the runs and the code: the gateway keeps `LoginRPCs` 16 × `LoginBatch` 200 = 3,200 Logins inside Logic at once, fixed whatever Logic's speed, and Logic checks a batch's tokens one after another before its store call (`internal/logic/login.go`), so under a 25% quota each batch spends ~1s inside Logic, about the observed 1.1–1.7s past cap / T'. Diagnostic (`slowlogic`): gateway flags `-login-rpcs`/`-login-batch` (`SLOWLOGIC_LOGIN_RPCS`/`SLOWLOGIC_LOGIN_BATCH`), Logic histograms `login_age_seconds` (each entry's age as its batch goes to the store) and `login_prepare_seconds` (a batch's token checks), and the calibration window's CPU clocks; the storm's default cap back to 4T. Next: host runs at 4T with 16 and with 4 Login RPCs; then (A) size or bound the in-flight window, (B) Logic stores in smaller pieces, or (C) a steadier T.

