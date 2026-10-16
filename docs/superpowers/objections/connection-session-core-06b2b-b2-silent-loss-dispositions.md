# Dispositions: Plan 6b2b-b2 review (2026-10-15)

Objections: `connection-session-core-06b2b-b2-silent-loss.md`. Decided by the user, one at a time.

## O1 — accepted: b2's gateways at `-heartbeat 30s`, plus a floor on the Kicks exercised

**Decision:** b2 runs its gateways with `-heartbeat 30s` (the SDK's and the gateway's minimum) and adds a check that Kicks were exercised.

**Rationale:** the SDK's Pings are phase-locked to login (no jitter), so with 150s the relogins come in one ~80s band placed by the script's own timing, mostly after the lease has died and often after the sweep. At 30s an 80s ramp covers the phase ~2.7 times, so phases are close to even. Detection is 10–40s, some relogins come while the lease is live (8–33%, see the correction below; retries exercised), and nearly all come before the sweep. The spec's rule (one heartbeat plus the Pong timeout) is what is checked. The 150s value is not, and the plan says so.

**Plan changes to apply:**
- b2's `STACK_GATEWAY_ARGS` gain `-heartbeat 30s` (b1 and b_stress unchanged). Record it in the phase field (`gateway_args` already does).
- `GWLOSS_B2_DETECT_MAX_S` default 45 (30 + 10 + 5). `GWLOSS_B2_BACK_WAIT` shrinks to fit: detection ≤ 45s, plus the 0–20s wait and the login (120s, see the correction).
- A floor on Kicks exercised in `b2_kicks`: `kick_retry` > 0, and first attempts plus drops at least a stated share of relogins (set from the 30s arithmetic below), so a run that exercised no Kick fails.
- Review Focus 2/3, implementation choices 2–4, Task 3's expected shapes and the README row reflect the 30s heartbeat: detection 10–40s; relogin rate on the survivor; the survivor's 200k clients also Ping every 30s (~6.7k Pings/s on gw-0 in steady state).
- Note in *After this plan* / Execution Notes: b2 at the spec's 150s heartbeat with phases spread over the whole interval would need loadgen to stagger logins over ≥150s; not done here.

**Correction (while applying):** the "roughly two thirds of relogins while the lease is live" in the question left out the SDK's 0–20s wait. With detection D ~ U(10, 40)s and the wait W ~ U(0, 20)s, a relogin comes at D + W (10–60s, flat at ~6,700/s from 30s to 40s). The lease ends L = 20–30s after the kill, so the share under a live lease is P(D + W < L) = 8% (L = 20) to 33% (L = 30). That is still thousands of first attempts against a live lease, so retries are exercised, against ~0 at 150s. P(D + W > 50s) ≈ 8%, so nearly every relogin still meets its entry before the sweep. The floor is therefore `kick_retry` > 0 and first attempts plus drops ≥ max(0.5 × pop, 0.9 × (pop − swept first)). `GWLOSS_B2_BACK_WAIT` defaults to 120 (45 + 20 + slack), not 90.

## O2 — accepted: mostly answered by O1; Redis latency recorded beside the check

**Decision:** rely on O1 to keep the sweep out of nearly all of the relogin window. `b2_login_p99`'s detail gains `redis_latency_storm` (`redis_storm`, already read), and Task 3's failure guide names the sweep (and the storm's own rate, above the ramp's per gateway) as possible causes before Kicks.

## O3 — accepted: every expected number recomputed from the 30s heartbeat

**Decision:** detection 10–40s (`max_ms` ≤ 45s). Relogins 10–60s, peaking at ~6,700/s on gw-0, which is above the ramp's 2,500/s per gateway (b1's survivor took ~10k/s). Choice 3 is restated: the storm is faster than the ramp, so the one-bucket allowance also absorbs load, and a 1s Kick delay still lands ≥ 5 buckets up. Review Focus 3's queue arithmetic: 64 attempts/s while CONNECTING (1s each); fail-fast in TRANSIENT_FAILURE drains the queue at once, so most first attempts are sent or dropped.

## O4 — accepted: b2's storm profiles and loadgen-bound reading start 15s after the mark

**Decision:** `GWLOSS_B2_PROFILE_AT` (default 15). In b2 the 30s CPU profiles and `storm30` (hence `loadgen_bound`, `b2_loadgen_headroom`, the lag gating) cover 15–45s after the mark. The phase field gains `profile_at_s` (0 in b1 and b_stress). The usage window restarts inside the profiling subshell, so the whole-storm `storm` reading is unchanged.

## O5 — accepted: the interrupt check's cue is the rules

**Decision:** Task 3 Step 1: press Ctrl-C once `sudo -n /usr/local/sbin/goim-netdrop list` shows 4 rules (the kill follows at once).

## O6 — accepted: cross-check floor 0.1 × relogins

**Decision:** `cross_rule` becomes 0.1 × pop ≤ samples ≤ 1.1 × pop. The expected share is ~0.6–0.9 (relogins after the lease's end and before the sweep). Revisit after run 28.

## O7 — accepted: no rules, no kill

**Decision:** if `rules_at_kill` is not 4, b2 records `b2_netdrop` failed, flushes, stops loadgen and the stack, and goes to the next phase without killing, as a failed `online` check does.
