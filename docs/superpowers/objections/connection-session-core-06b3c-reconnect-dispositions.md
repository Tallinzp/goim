# Dispositions: Plan 6b3c review (2026-10-17)

Objections: `connection-session-core-06b3c-reconnect.md`. The user decided them with AskUserQuestion: O1–O4 first, then O5–O8. All eight are accepted. No new numbered choice: **#136 is amended** (a fifth ruling, the `ghost` phase, from O1; #136.4's reason reworded, from O2). The changes below go into the plan. They are re-verified in `proto-6b3c` and replayed into the plan byte for byte before execution, as usual.

Size note: O1 adds a loadgen mode and a third phase to a plan that is already ~125KB. #136.1 (one plan) was the user's ruling before this review. When the dispositions are applied, the plan's *Size* states the new size. If it is near half a session's context ([[keep-sessions-small]]), the split is the user's call then.

## O1 — accepted: add phase `ghost`

**Decision:** loadgen gets a silent drop, and `reconnect` gets a third phase, `ghost`, on a fresh stack. Phases become `reset netchange ghost`. `reset` and `netchange` stay as they are. `ghost` is the spec's carrier or network recovery: every client loses its connection, the gateways see nothing, and every client raises a network change.

**Rationale:** the spec names three causes of a whole-population reconnect. The RST drop is the easiest for the servers: each gateway frees the connection at once. In a network recovery each gateway keeps its ~200k old connections ONLINE until `DeadAfter` (2 × 30s + 10s = 70s at the phase's heartbeat), so the relogins meet the cap (~400k against 250k), every admitted relogin replaces a live session (a Kick to the old connection, the cost behind parked #117), and the old connections that no Kick reached are closed by the heartbeat sweep in the storm. Loadgen can do this with no root. `goim-netdrop` is not used: it drops by port, so it would also block the relogins.

**Plan changes to apply:**
- Loadgen (Task 3): `POST /ghost?mark=NAME` (name settled in the prototype), sdk mode only, as `/reset`. It takes the mark, then for every SDK client makes the SDK-facing wrapper fail its `Read` (the SDK sees a lost connection: `EndLost`, storm wait 0–20s) and keeps the real TCP socket open and unread. The SDK's later writes (Pings) must not reach the old socket. Loadgen closes a kept socket when the gateway closes it (FIN from a Kick or the sweep), so no socket stays in CLOSE_WAIT, and counts those closes. A reply like `/reset`'s (`population`, `ms`), plus `ghosts_open` in `/stats`. Tests: the SDK client is lost and back while the gateway still holds the old connection; the gateway's Kick or `DeadAfter` closes it; a mutation that closes the socket fails the test.
- `reconnect.sh` (Task 4): phase `ghost` = `/ghost`, then `/network-change` once every client has lost its connection (as `netchange`). Its hold lasts at least `DeadAfter` after the mark plus one wheel tick, so every ghost is gone before the stop.
- Checks for `ghost` that differ from the RST phases, settled in the prototype: `P_drop` (every client lost; the gateways' closes of the old connections, by Kick and by heartbeat, reach the population by the hold's end, instead of `closes_client`); `P_heartbeats` (only the clients' live connections count: loadgen's lost reasons since the mark are the ghost drop's alone, and gateway heartbeat closes are allowed up to the ghosts that no Kick closed, recorded per gateway).
- Recorded for `ghost`: `rejects_at_capacity` per gateway and per second (the cap shapes it here, O2), Logic's Kicks, the sweep's closes per second, `ghosts_open` per second in the series.
- Resources: up to 600k ghosts and 600k live connections on loadgen at once. `t2_nofile 1048576` is too small; raise it and check the host's limits and `t2_mem_budget` (both ends on loopback: ~2.4M sockets at the peak) in the prototype. Name the peak in Review Focus.
- Header, Goal, Architecture, Review Focus (a new item: the ghost phase's peak and why the hold is ≥ DeadAfter), checks list (21 → 21 + ghost's), duration (~30 min), Task 5's *What to read* (`ghost` against `netchange`: the cap and the Kicks, not the jitter, shape it).
- Roadmap: #136 gets a fifth ruling (the `ghost` phase, O1).

## O2 — accepted: follows O1, #136.4's reason reworded

**Decision:** keep the 250k cap. Amend #136.4's reason: in the RST phases the cap does not bind (200k per gateway, every slot freed at once; `at_capacity` expected 0); in `ghost` it binds (the ghosts plus the relogins).

**Rationale:** with O1's phase the cap's choice has a run that tests it. The record must not say that `reset` tested the cap.

**Plan changes to apply:**
- Decisions, #136 (4): the new reason, as above.
- Task 5 *What to read*: `at_capacity` in `reset`/`netchange` expected 0; in `ghost` read it with the Kicks.
- Roadmap #136.4: the same words.

## O3 — accepted: labelled fail, and loadgen's run-queue wait

**Decision:** when `P_back.loadgen_bound` is true, a failed `P_heartbeats` fails labelled, as floods does (#126, #127): a `not_covered` line says that loadgen was the bound and names where to read it. Also `rc_sampler` records loadgen's run-queue wait (`t2_runq_ns` over its threads) per second, and `rc_series` gives it as a column, as floods' `gw_runq_ms_per_s`.

**Rationale:** every b_stress storm was loadgen-bound, and this storm is larger. Without a rule, a failed `P_heartbeats` cannot be read as a gateway fault or a loadgen fault. The run-queue wait shows when loadgen's reads were late, beside the Pong timeouts.

**Plan changes to apply:**
- `reconnect.sh`: the `not_covered` line on a failed `P_heartbeats` with `loadgen_bound` true; the `P_heartbeats` detail carries `loadgen_bound` and the run-queue maximum.
- `rc_sampler`/`rc_series`: `lg_runq_ms_per_s` (a failed reading as null, a total that went down as null, as M6); a test in `reconnect_test.sh`.
- Review Focus 5: the rule. Task 5's *What to read*: the rule.
- The check still fails in both cases (as floods' non-valid verdict); only the label differs.

## O4 — accepted: a session check in the hold

**Decision:** a new check per phase, `P_sessions`: during the hold, with every client back, Logic's store holds one live session per client, and each one names the gateway that holds the client's connection (from loadgen's per-client gateway). The prototype settles the form: a full count, or a fixed random sample of clients read one by one, and how to read it without a large load on the one Redis in the hold (a count read after the hold and before `lg_stop` is acceptable).

**Rationale:** at the reset each gateway queues ~200k Logouts while the same users log in again, and some Logouts are dropped at the full queue. A Logout that deletes the new session leaves a client online with no session. No message reaches it, and no other check sees it. `ghost` adds 600k replacing logins with Kicks, which is the same race in another order.

**Plan changes to apply:**
- `reconnect.sh`: `P_sessions` with the detail (sessions found, missing, on a wrong gateway, extra; Logic's `logins_replaced`, `logouts_deleted`, `logouts_not_owner` beside it). Check count +1 per phase.
- Loadgen, only if the prototype needs it: a way to get the client → gateway map (e.g. a `/clients` sample endpoint). Settle in the prototype.
- Header checks list and Global Constraints (what `reconnect` gates).

## O5 — accepted: record Redis as gwloss does

**Decision:** `stack_redis_resetstat` at the mark and `stack_redis_latency` after the hold, into field `P.redis`, per phase. Recorded, not gated.

**Rationale:** the whole-population relogin is Redis's largest write burst, and Plan 6c (Redis Cluster) needs a single-instance baseline from it. The helpers exist (`gwloss.sh` ~l.475, ~l.550, ~l.685).

**Plan changes to apply:**
- `reconnect.sh`: the two calls and the field, as gwloss (a failed reset records `null`, as gwloss's `reset_storm`).
- Task 5 *What to read*: Redis's p99 per command beside `logic_set_busy_pct` (Redis shares Logic's set).
- *After this plan*, 6c: run 31's `P.redis` is its input.

## O6 — accepted: name the funnel's bar before the run

**Decision:** the plan names a bar before run 31, for example `funnel.longest_s` ≥ 5s in a row. Only a funnel at or over the bar is the evidence for unparking 6b3b5 and #111. A funnel under the bar is recorded as "seen, under the bar". The prototype picks the number (and says why), from the sampler's real row interval.

**Rationale:** one sampled second must not decide a parked plan. 6b3b5 was parked because its trigger did not occur at the defaults.

**Plan changes to apply:**
- `rc_series`: `funnel.over_bar` (bool) beside `seconds`, `first_s`, `longest_s`; a test case under and at the bar.
- Task 5 Step 1: the bar replaces "seconds > 0".
- Implementation choice 6: the bar and the gauge-against-rate caveat (O6's second half is recorded, not changed).

## O7 — accepted: cross-check `refused` against the gateways

**Decision:** `slow_held` also needs the two floods' `refused` count to be at most the gateways' refusals over the phase (`rejects_pending_mem` + `rejects_at_capacity` + the limiter's `hs_no_slot` + `hs_no_token`, from `t2_gws_delta`). No new check: floods stays at 30.

**Rationale:** #135 fixed a false fail, but it also removed a true one: a fault that closes most slow clients early would pass. The gateways' counters include the light clients' refusals, so the bound is loose, but a close that no counter explains fails it.

**Plan changes to apply:**
- Task 2 `floods.sh`: the condition in `slow_held` and the numbers in its detail (refused, the gateways' sum).
- The doc block of `slow_held`: the bound.
- `floods_test.sh`: no change unless the condition is pulled out into a testable helper (the prototype decides); the dry run shows it.
- Global Constraints: `slow_held` changes also by the bound.

## O8 — accepted: the sampler on loadgen's set

**Decision:** pin `rc_sampler` to loadgen's set (`T2_CPUS_LOADGEN`, 14 CPUs), not Logic's. Floods keeps its own pinning.

**Rationale:** Logic and Redis share 2 CPUs, the set most likely to bound a login storm, and Review Focus 4 reads `logic_set_busy_pct` to rule which set saturated first. On loadgen's 14 CPUs the sampler's cost is a small share, and `loadgen_bound` reads loadgen's process cores, not the set.

**Plan changes to apply:**
- `reconnect.sh`: `taskset -apc "$T2_CPUS_LOADGEN" "$smp"`, with a comment saying why.
- Review Focus 4: one sentence.

## Applied (2026-10-18)

All eight are applied and re-verified in `proto-6b3c` (`f2546aa`, `4658e13`, `d1d4c06`, `fd1dc04`; replayed byte for byte). O1's fd peak: up to 1.2M loadgen sockets, so the user chose a nofile hard limit of 2,097,152 (`host-setup.sh`) over a smaller `ghost`. The plan then measured ~170KB, and the user split it after its Task 2: O7 is in `2026-10-17-connection-session-core-06b3c1-helpers-floods.md`; O1–O6 and O8 are in `2026-10-17-connection-session-core-06b3c2-reconnect.md` (the plan file this review names, renamed).
