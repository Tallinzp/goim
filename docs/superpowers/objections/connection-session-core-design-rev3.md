---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev3.md
date: 2026-10-03
mode: spec
focus: final round — application of R1/R2 dispositions, interactions between them, open points 4–6, Reconcile delete-phase gap
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: implementation
    severity: high
    blocking: true
    claim: "The Reconcile delete phase can't be implemented on stateless Logic as written. Its chunk-membership rule also deletes live sessions whose Login was in flight when the pass started, and leaves ghosts for up to two cycles, not the one the contract states."
    evidence: "\"After the pass, Logic SSCANs gwconns:gw:{epoch}. For each field that points at this gw|epoch, whose conn_id was not in any chunk of the pass, and whose login_ms < cutoff_ms, it compare-and-deletes.\" / \"Logic: Stateless. Every node serves every RPC\" / \"Gateway→Logic: dns:/// headless service + round_robin\" / contract: \"Ghost ... ≤ 1 Reconcile cycle (~10 min + jitter)\""
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: risk
    severity: high
    blocking: true
    claim: "The mass-stale guard releases on the first renewal after a systemic outage. It then sweeps every healthy gateway that hasn't renewed yet, which is the R1-O2 failure that the R2-O3 rule was meant to prevent."
    evidence: "\"sweep a stale epoch only if at least one other epoch renewed within the last renewal interval (10s), and the sweeper's own Redis access has been healthy for one full lease TTL\" / \"it releases on its own once renewals resume\" / race 12: \"a systemic outage ... stops every renewal, so the guard holds and nothing is swept\""
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: risk
    severity: high
    blocking: true
    claim: "If a gateway's lease renewal lands while the sweeper is sweeping its epoch, the renewal never reports `missing`. The sweeper keeps deleting the gateway's sessions (and anything already restored), and the ~1 min restore promised by race 12 and the contract does not happen."
    evidence: "\"The reply says whether the epoch was found live or missing (lease key absent and gwepochs member gone, i.e. swept)\" / Sweeper: \"Claim each with sweep: SET NX, run SSCAN gwconns, ... Then UNLINK the set ... and remove the ZSET member\" / \"Rate-limited to about 10k users/s\" / race 12: \"its lease reply then reports missing and a triggered Reconcile restores its sessions within ~1 min\""
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: implementation
    severity: medium
    blocking: false
    claim: "Reconcile rule 3 copies only part of the login table's ordering. It leaves out the counter-regression row, so after a failover rollback it can close the legitimate (regressed-counter) connection with REPLACED, which never reconnects on its own. It also has no tie-break for equal login_ms."
    evidence: "Rule 3: \"compare the two with the login table's ordering (R2-O4): same device_id, higher conn_seq wins; different device_id, later login_ms wins\" / login table: \"same device_id, new conn_seq ≤ stored, recv_ms > stored login_ms + login_deadline | counter regression: overwrite\""
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: specification quality
    severity: medium
    blocking: false
    claim: "A client closed at accept for at_capacity, or at the handshake limit, restarts the 0–20s reconnect jitter. That can't fit the 30s SLO that the R2-O2 rolling-restart test requires, and the rolling-restart arithmetic shows this happens often."
    evidence: "\"A connection closed at accept (at_capacity) is an unexpected close and follows the same rule\" (0–20s, then exponential backoff) / \"Busy{retry_after} uses a short value, 1–3s with jitter, so rejected clients still fit within the SLO\" / test: \"rolling restart of all gateways at 1M total: ... clients rejected at the cap log in elsewhere within the SLO\""
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: specification quality
    severity: medium
    blocking: false
    claim: "Open point 6 retries deadline-refused Reconcile writes only in the next pass, which breaks the ~1 min bound for a triggered pass. Rule 3 also doesn't say that the kick of the stored session depends on the compare-and-set succeeding."
    evidence: "Open point 6: \"A refused entry counts as a definitive reply for fence (c) and is retried by the next pass.\" / contract: \"≤ ~1 min after a missing lease reply\" / rule 3: \"compare-and-set write it, refresh the TTL, ensure the uid is in gwconns, and kick the stored session\""
    disposition: pending
    disposition_rationale: null
  - id: O7
    category: specification quality
    severity: low
    blocking: false
    claim: "The compose rig has 2 gateways, so it can't run the R2-O3 test \"kill 2 gateways while the others renew\"."
    evidence: "\"Race integration tests (deploy/compose: Redis, Logic, 2 gateways and a scripted client)\" / \"kill 2 gateways at once: both epochs are swept while the others renew (R2-O3)\""
    disposition: pending
    disposition_rationale: null
---

# Objection record: Connection & Session Core, revision 3 (final round)

Scope as requested: (1) settled R1/R2 decisions are not reopened. Where an objection touches one, it names the disposition and shows either a misapplication or a failure the earlier round did not cover. (2) The disposition application check comes first. (3) Rulings on open points 4–6. (4) The Reconcile delete-phase gap (→ O1). (5) New failures only. (6) Blocking objections first.

## Application check

**R2 dispositions: every "spec change to apply" is present in rev3.**

| R2 | Applied in rev3 | Consistent with the rest? |
|---|---|---|
| O1 live chunks + fence (c) + `sent_ms` deadline + race 14 + test | Gateway → Logout fence (c); Reconcile "Live chunks"; Reconcile handling compare-and-set deadline; race 14; test | Fence (c) and the Lua deadline use the same `sent_ms` and the same `login_deadline + 1s` margin as fence (b): **consistent**. The delete phase that R2-O1 left in place is not (O1). |
| O2 connection cap, density note, `at_capacity`, skew metric, rolling-restart test | All present | The test conflicts with the SDK retry rule (O5). |
| O3 topology spread, SLO scoped to one gateway, "others renewing" guard, race 12, tests | All present | Guard release edge (O2); compose rig too small for its test (O7). |
| O4 replication note, Revoke `max` + `WAIT`, rule 3 by login ordering, open point removed, tests | All present | Rule 3 reproduces the disposition's simplified ordering, which leaves out the regression row (O4). |
| O5 configurable TTL with failover requirement, sweeper 2 × TTL, guard uses TTL, interface constraint, failover test, carried outage | All present | — |
| O6 first Kick always sent, retries only while the lease is live, fail-fast, labelled metric, failure row, tests | All present | Consistent with the contract: a Kick lost to a live gateway is retried for ≤ ~30s; anything else falls to Reconcile `not_owner` / rule 1, ≤ 1 cycle. |
| O7 `minReplicas` for peak, readiness on JWKS, ConfigMap fallback, runbook, tests | All present | — |
| O8 Consistency contract, two paces, test | All present | The ghost bound is wrong under the current delete rule (O1); the triggered-pass bound is broken by open point 6 (O6) and by O3. |
| O9 `UNLINK`, `gwconns` note | Both present | — |

**R1 mechanisms that R2 modified or overlapped.**
- **R1-O9 Logout fence**: (a)/(b) unchanged; (c) added as a conjunction ("**both** of the following hold"). Correct.
- **R1-O8 hardened Kick**: R2-O6 restricts retries, not the first attempt, so R1-O8's "always send" still holds. Correct.
- **R1-O3 Reconcile**: rule order 1→4 kept; rule 3 changed by R2-O4 (see O4); the delete phase was not adapted to live chunks (see O1).
- **R1-O2 lease / guard**: dedicated connection and pool kept; R1-O2's "wait one full lease period after Redis returns" survives as guard condition 2. However, the R2-O3 condition 1 adds a release edge that condition 2 does not cover (see O2), and R1-O2 layer 3's "missing" detection has a hole during a sweep (see O3).

## Rulings on open points 4–6

- **Open point 4 (the cap counts all open connections): sound, and it is already the smallest option.** One clarification: count **sockets** (incremented on accept, decremented on close), not connection-table entries. A connection kept after close only to fire a fenced Logout must not count, or a Logic outage (Logouts parked for up to 60s) would push the gateway into `at_capacity`.
- **Open point 5 (one pass at a time; a `missing` reply cancels the periodic pass and waits ≤ 6s): sound.** Two clarifications, no new mechanism. A cancelled pass sends no further chunks, and none are built ahead of sending: the spec says chunks are "read ... at the moment it is sent", so no chunk is ever built but unsent with its mark never cleared. A cancelled pass also runs no delete phase. An equally small alternative is to make `in_reconcile_chunk` a small counter instead of a flag, which removes the wait. It isn't needed.
- **Open point 6 (the deadline refuses only writes that create or replace a field): sound for safety.** Late TTL refreshes and `gwconns` SADDs can't create a ghost. Its retry clause needs a change (O6).

## O1 — implementation — high — BLOCKING

### Claim

The delete phase depends on state that no single Logic node has. Even if a node had it, the `cutoff_ms` rule has two defects:
1. It deletes live sessions whose Login was in flight when the pass started.
2. Its ghost bound is two cycles, not one.

### Evidence

> After the pass, Logic SSCANs `gwconns:gw:{epoch}`. For each field that points at this `gw|epoch`, whose conn_id was not in any chunk of the pass, and whose `login_ms < cutoff_ms`, it compare-and-deletes. The cutoff protects Logins that were in flight while the pass ran.

> Stateless. Every node serves every RPC

> Gateway→Logic: `dns:///` headless service + `round_robin`

> **cutoff_ms**: taken when the pass starts and used only for the delete phase.

> Ghost: valid entry, but the connection is gone | Logout dropped | ≤ 1 Reconcile cycle (~10 min + jitter)

### Why this matters

1. **The rule is not implementable.** A periodic pass sends ~200k entries over ~10 min, round-robined over every Logic pod and across `MaxConnectionAge` reconnects. No node sees "every chunk of the pass", and Logic keeps no state between chunks. Each implementer would invent something different: sticky routing, a per-pass Redis set, or skipping the phase.
2. **It deletes live sessions.** Connection C sends LoginReq at `recv_ms = cutoff_ms − 1ms`. The pass starts while C is still `AUTH_PENDING`, so C is not in the ONLINE set its loop's chunk is built from. The Login Lua then writes C's field and the follow-up SADD adds the uid to `gwconns`. At the end of the pass, C's conn_id is in no chunk and `login_ms < cutoff_ms`, so the delete phase removes the field of a **live, correctly logged-in** session. The cutoff protects Logins *received* after the pass starts, not Logins *in flight* when it starts. Under the R1-O8 ownership rule, C can't send messages until the next pass restores it (~10 min). The chance is small per pass, but storms are exactly when Logins take seconds and triggered passes run.
3. **The ghost bound is 2 cycles.** C is in a chunk at minute 1 of pass k and closes at minute 2; its Logout is dropped (Logic down for more than the 60s retry). Pass k skips C because C was in one of its chunks. Pass k+1 deletes C at its end, around minute 20. The contract says ≤ 1 cycle.

**Smallest fix (one RPC, keeps Logic stateless, removes `cutoff_ms` and chunk membership from the delete rule):** end each completed pass with `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`.
- `tracked_conn_ids` is every conn_id the gateway still tracks in **any** state, including closed connections whose fenced Logout has not been sent yet.
- `as_of_ms` is the gateway's clock when it collects the list.
- Whichever Logic node receives it SSCANs `gwconns` and compare-and-deletes each field that points at this `gw|epoch`, whose `conn_id ∉ tracked_conn_ids` and whose `login_ms < as_of_ms`.

Why it is safe: a conn_id leaves the tracked set only after its fence has held, so neither a Login nor a Reconcile can still write it. `login_ms` and `as_of_ms` come from the same gateway clock. Every Logout dropped before the pass ends is repaired at that pass's end, so the contract's ≤ 1 cycle becomes true.

Size: 8 bytes × 250k (cap) = 2MB, under gRPC's 4MB default. The 600k benchmark needs the list split or the limit raised. A cancelled pass sends no `ReconcileEnd`.

## O2 — risk — high — BLOCKING

### Claim

Guard condition 1 is true from the moment the **first** gateway renews after a systemic gateway→Logic outage. Every other healthy gateway has a stale score (> 60s) until its own renewal, up to 10s later, and the sweeper sweeps them in that window. Condition 2 doesn't help, because the sweeper's own Redis access was healthy throughout.

### Evidence

> sweep a stale epoch only if
> - at least one **other** epoch renewed within the last renewal interval (10s), and
> - the sweeper's own Redis access has been healthy for one full **lease TTL** (30s by default).

> it releases on its own once renewals resume.

> Race 12: a systemic outage (long Redis outage, Logic saturation) stops every renewal, so the guard holds and nothing is swept.

> Logic reports **Ready only once it holds a key set** from either source.

### Why this matters

Concrete scenarios: (a) a NetworkPolicy, DNS or Service mistake cuts gateway→Logic for 90s while Logic↔Redis is fine; (b) every Logic pod restarts, the account service is down, and the JWKS fetch times out, so pods reach Ready (and only then receive lease RPCs through the headless service) more than 30s after their sweeper goroutine started. In both cases the first renewal at t0 makes condition 1 true. Gateways 2–6 renew at random phases over t0…t0+10s, and any sweeper cycle in that window finds them stale with the guard released. Up to 5 healthy gateways (≈ 830k sessions) are swept.

The 90s Redis-pause test doesn't catch this: condition 2 holds the sweeper for 30s after Redis returns, and every gateway renews well within that. Recovery then relies on `missing`, which O3 shows can fail exactly here, because a renewal lands mid-sweep. This does not reopen R2-O3's choice of an "others are renewing" rule. It closes the rule's release edge.

**Smallest fix (one timestamp per sweeper, in memory):** condition 1 must have held **continuously for one lease TTL** before any sweep. Use a window slightly wider than the renewal interval (e.g. 1.5 × 10s) so that a lone surviving gateway's 10s + RPC-latency rhythm doesn't reset the clock. All healthy gateways renew within 10s < 30s of recovery. A truly dead gateway is swept 30s later than today, which is irrelevant because its entries are already invalid by the validity rule.

## O3 — risk — high — BLOCKING

### Claim

`missing` requires **both** an absent lease key **and** an absent `gwepochs` member. During a sweep (≈ 20–25s at 10k users/s for 200–250k uids), the member still exists, and Logic's renewal re-creates the lease key, so the reply is not `missing`. The sweeper doesn't notice the renewal: it goes on deleting fields that match the epoch, then `UNLINK`s `gwconns` and `ZREM`s the member. On the next renewal the lease key is present, so again the reply is not `missing`. The gateway is never told and doesn't Reconcile until its next periodic pass.

### Evidence

> The reply says whether the epoch was found **live** or **missing** (lease key absent and `gwepochs` member gone, i.e. swept). On `missing`, the gateway's lease call re-registers the epoch and the gateway starts an immediate triggered Reconcile pass.

> Claim each with `sweep:` SET NX, run `SSCAN gwconns`, and for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Then `UNLINK` the set ... and remove the ZSET member. Rate-limited to about 10k users/s.

> Race 12: If a single gateway's lease is wrongly starved while the others renew, it may be swept; its lease reply then reports `missing` and a triggered Reconcile restores its sessions within ~1 min.

> Contract: Live connection missing from Redis | ... wrongly swept | ≤ 1 cycle; ≤ ~1 min after a `missing` lease reply

### Why this matters

A starved lease recovers at some point. If the sweep started at 60s, a recovery anywhere in [60s, ~85s] hits this window. The gateway's whole population (up to 250k) then has no session entry for up to one periodic cycle (~10 min). Under the R1-O8 ownership rule, those connections can't send messages for that time, though their sockets are healthy. The race-12 claim and the R2-O8 contract line are false in this interleaving. The test "after a forced sweep of a live gateway, its sessions are restored within ~1 min" passes only if the sweep finishes before the gateway renews. O2's scenario makes this interleaving likely, not rare.

**Smallest fix (two local changes, no new key):**
1. Logic reports `missing` whenever the renewal found the **lease key absent** (the SET runs with `GET`, or an `EXISTS` first), whatever the state of the member. Expired-but-not-swept then costs one unnecessary triggered pass, which is rare by the TTL derivation.
2. Between rate-limit batches, the sweeper re-reads the epoch's `gwepochs` score. If it is newer than the score it saw at claim time, it stops without `UNLINK` or `ZREM`.

At most one in-flight batch can delete entries that the triggered pass already restored, and the next periodic pass repairs them. Add the test order: sweep running → gateway renews → triggered pass → sessions present.

## O4 — implementation — medium

### Claim

R2-O4 asked rule 3 to use "the login table's ordering", but rule 3 writes it out as "same device_id, higher conn_seq wins". The login table also has the R1-O5 regression row: a lower conn_seq received more than `login_deadline` after the stored login **wins**. Rule 3 leaves that row out.

### Evidence

> Rule 3: compare the two with the login table's ordering *(R2-O4)*: same device_id, higher `conn_seq` wins; different device_id, later `login_ms` wins.

> Login table: same device_id, new conn_seq ≤ stored, `recv_ms > stored login_ms + login_deadline` | counter regression: overwrite; kick the old connection with `REPLACED`

> Race 15: if the field rolls back to an older session while the newer connection is live, Reconcile rule 3 compares by the login table's ordering, writes the newer entry back and kicks the older connection.

### Why this matters

1. Device D held session S (conn_seq 50) on G1. D restores a backup (race 11), logs in as E (conn_seq 1) an hour later on G2, and E wins by the regression row. S is closed; its Logout no-ops, because the field holds E.
2. A Redis failover loses E's write, so the field rolls back to S. G1's lease is live.
3. E's next chunk on G2 hits rule 3: 50 > 1, so E goes to `not_owner` with `REPLACED`.
4. The legitimate client is closed with the one kick reason that doesn't auto-reconnect. G1's delete phase then removes S, and the user is offline until the app returns to the foreground.

This is rare (it needs both a regression and a failover), but race 15 is exactly the case rule 3 was changed for.

Separately, "later `login_ms` wins" has no rule for equal login_ms. If the implementer copies the login table's `≥` and both sessions are live (a Kick was lost), each gateway's Reconcile overwrites and kicks the other, and both devices end up with `KICKED_BY_OTHER_DEVICE`.

**Smallest fix (one sentence):** same device_id: if the two `login_ms` are within `login_deadline` of each other, the higher conn_seq wins; otherwise the later `login_ms` wins. Different device_id: on a tie the stored session wins. This matches the login table on every state the login Lua can reach, because the `EXPIRED` deadline makes "higher conn_seq but more than `login_deadline` earlier" unreachable at login. Add a Lua test for the regression case.

## O5 — specification quality — medium

### Claim

Rejections at accept (`at_capacity`) and at the handshake limit close the socket before any framing. The SDK treats this as an unexpected close: a new 0–20s jitter, then exponential backoff. Only `Busy` gets the SLO-compatible 1–3s retry. So R2-O2's test requirement, that clients rejected at the cap log in elsewhere within the SLO, can't be met.

### Evidence

> A connection closed at accept (`at_capacity`) is an unexpected close and follows the same rule; the new attempt uses a new source port

> `Busy{retry_after}` uses a short value, **1–3s with jitter**, so rejected clients still fit within the SLO *(R1-O7)*.

> rolling restart of all gateways at 1M total: no gateway exceeds the cap, and clients rejected at the cap log in elsewhere within the SLO *(R2-O2)*

> When either is exhausted the gateway does not start the handshake: it closes the connection (or sends `Busy{retry_after}` before any crypto, if the framing allows)

### Why this matters

Rolling restart in reverse ordinal order from 6 × 167k, with an even spread over the Ready gateways:

| Step | Load after the step |
|---|---|
| Drain gw5 | gw0–4 at 200k |
| Drain gw4 | gw0–3 at 240k, gw5 at 40k |
| Drain gw3 (240k) | gw0–2 would reach 288k, so they hit the 250k cap |

From step 3 on, about 3 of 5 hashed targets reject. Success per attempt is ~0.4, so ~36% of clients need a third attempt. With a 0–20s wait before each retry, a large share miss 30s. With 1–3s retries, 0.6^10 ≈ 0.6% are still failing after ~20s, so the test passes.

**Smallest fix (SDK rule only):** a connection closed by the server before the handshake completes counts as a `Busy` retry: wait 1–3s with jitter. After ~5 such retries, fall back to backoff.

## O6 — specification quality — medium

### Claim

Open point 6 has two loose ends.

1. **Retry timing.** "Retried by the next pass" means up to 10 min for a triggered pass. Triggered passes run after a sweep, when Logic may be slow (race 12's starvation cause, or 6 gateways × 5k/s), which is exactly when chunks breach the 5s deadline.
2. **Kick condition.** Rule 3 lists "write it ... and kick the stored session" without making the kick depend on the compare-and-set. If the compare-and-set is refused by the deadline but the kick still goes out, the stored session S is closed and its Logout deletes S's field. The newer connection E is then live but missing from Redis until the next pass.

### Evidence

> A refused entry counts as a definitive reply for fence (c) and is retried by the next pass.

> Live connection missing from Redis | ... | ≤ 1 cycle; ≤ ~1 min after a `missing` lease reply

> Chunk entry newer ... → compare-and-set write it, refresh the TTL, ensure the uid is in `gwconns`, and kick the stored session ... using the hardened Kick.

### Why this matters

Without these two clarifications, the ~1 min bound is a fair-weather number, and rule 3 can turn a refused write into an empty slot.

**Smallest fix (wording only):**
- Refused entries go back on the current pass's queue, rebuilt from live state when they are resent.
- Rule 3's kick is sent only if the compare-and-set wrote.

## O7 — specification quality — low

### Claim / Evidence

> Race integration tests (`deploy/compose`: Redis, Logic, **2 gateways** and a scripted client)

> kill 2 gateways at once: both epochs are swept while the others renew *(R2-O3)*

### Why this matters

With 2 gateways, killing both leaves no renewer, so the guard correctly holds forever and the test fails or gets rewritten ad hoc. The rig needs at least 3 gateways for this test; 4 also exercises O2's "lone renewer" rhythm.

## Explicitly not objecting to

- **Fence (c) and its deadline arithmetic:** the gateway release (`sent_ms + login_deadline + 1s`) and the Lua refusal (`TIME − sent_ms > login_deadline`) use the same stamp and the same 1s skew margin as fence (b). They are correct as written.
- **Lease TTL derivation (TTL > failover + 10s → failover < 20s):** a default Redis Cluster `cluster-node-timeout` of 15s plus election is tight but measured, as R2-O5 requires; the failover test catches a bad value.
- **Reconcile and storm arithmetic:** 200k/600s ≈ 333/s, 6 × 333 ≈ 2k/s, 6 × 5k = 30k/s, 200k/5k = 40s, 200k/20s = 10k/s, and the sweep time of 2 × TTL + 20s all check out.
- **Zone-loss capacity "just barely" (4 × 250k = 1M):** it holds only with the cap at its 250k default. R2-O3 explicitly made zone loss best effort with no SLO, so this is not a failure.
- **Hardened Kick vs contract:** retries are bounded at ~30s only while the lease is live; everything else falls to the ≤ 1 cycle backstop. Consistent.
- **Rule 4 restoring an older session over a newer one whose lease is momentarily expired:** it self-heals through rule 3 on the newer gateway's next pass. This is inherent to R1-O3's decided rule and within the contract bound.
- **Open point 2 (slot-scoped Revoke):** it remains open by the decision maker's choice, and this round was asked to rule only on 4–6.
- **Open points 1 and 3:** settled in earlier rounds and applied consistently.
- **`gwepochs` as a single ZSET on one shard:** six members renewed every 10s is negligible load, and losing it in a failover is repaired by the next renewal's ZADD.

## Verdict

**Not ready for plan.** O1 (the Reconcile delete phase can't be implemented on stateless Logic, and it deletes live sessions), O2 (the guard's release edge sweeps healthy gateways) and O3 (`missing` is not reported while a sweep is running) are all blocking. Each needs only a few lines in the dispositions file and no new key or component. Once they are decided, the spec is ready for plan; O4–O7 can be fixed during implementation.
