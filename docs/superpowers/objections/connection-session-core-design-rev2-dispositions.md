---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev2.md
objections: docs/superpowers/objections/connection-session-core-design-rev2.md
date: 2026-10-03
decision_maker: user
guiding_principle: new system; take the smallest fix that closes the failure, and avoid overdesign
---

# Disposition record — Connection & Session Core, revision 2

The user decides each objection. This file records the decision and the spec changes it implies. The spec itself has not been edited yet.

## O1 — risk — high — ACCEPTED

**Decision:** Build Reconcile chunks from live state, and add a Reconcile fence on Logout.

**Rationale:** Restoring from a snapshot taken at the start of a ~10-minute pass turns normal churn into a steady stream of ghosts. Building chunks from live state shrinks the window to one chunk round-trip. The fence closes that remaining window the same way the Logout fence handles Login (O9 of the first review). It needs no new RPC, key or message type.

**Spec changes to apply:**
- *Gateway → Reconcile:* the gateway reads each chunk from its per-loop connection tables when it sends it (via `loop.Execute`), not from a snapshot frozen at the start. `cutoff_ms` is still taken at the start of the pass and is still used only for the delete phase.
- *Gateway → Logout fence:* add condition **(c)**: if a connection closes while a Reconcile chunk containing it is in flight, its Logout waits until that chunk has a definitive reply. Implemented as a per-connection `in_reconcile_chunk` mark that is set when the chunk is built and cleared on release.
- *No definitive reply* (timeout, RPC error, Logic crash): reuse the Login deadline mechanism. Each chunk carries `sent_ms`. The Reconcile compare-and-set Lua refuses to write when `now − sent_ms > login_deadline`. The gateway releases the held Logout at `sent_ms + login_deadline + 1s`.
- *Races handled:* add "Connection closes during a Reconcile pass: its Logout is held until the chunk containing it is acknowledged, so the Logout always lands after any restore."
- *Tests:* race integration test: a connection closes after its chunk is sent but before Logic processes it (inject a Logic delay), and no ghost remains after the Logout.

## O2 — risk — high — ACCEPTED (minimal)

**Decision:** Add a hard per-gateway ONLINE cap. No active rebalancing.

**Rationale:** The cap bounds the damage from skew (at most ~250k on any gateway, and so the largest possible drain storm) using one counter and one check at accept. Skew below the cap is harmless. Rebalancing (drain pacing, `Reconnect` shedding) is deferred until metrics show it is needed.

**Spec changes to apply:**
- *Gateway → Admission control:* add a **connection cap** (configurable, default 250k; it must sit between the 200k operating point and the 600k ceiling). At the cap the gateway closes new connections at accept, before any crypto, and counts an `at_capacity` reject. The client's normal reconnect (new source port) lets the NLB flow hash pick another gateway.
- *Density:* note that with one gateway down, the remaining gateways can hold 5 × cap = 1.25M, which is at least 1M users.
- *Observability:* add `at_capacity` to the admission-reject reasons; add a connection-skew view (max/mean ONLINE per gateway).
- *Tests:* load test of a rolling restart of all gateways at 1M total, in which no gateway exceeds the cap and clients rejected at the cap log in elsewhere within the SLO.
- *Out of scope:* active rebalancing, deferred to sub-project 5.

## O3 — risk — high — ACCEPTED (minimal)

**Decision:** Spread gateways across zones and nodes; keep the SLO scoped to losing one gateway, with zone loss best effort; replace the mass-stale threshold with an "others are renewing" rule.

**Rationale:** Topology spread is a few lines of deploy config and makes a single node failure a true N−1 event. Sizing for a zone loss (7–9 gateways) is overdesign for a new system; with the O2 cap, 4 surviving gateways can still hold 1M, just barely. The old guard could not tell a systemic outage from a real multi-gateway death, and it never released. The new rule asks whether the system is demonstrably renewing leases, which answers that question directly and has a natural exit.

**Spec changes to apply:**
- *Deployment:* the gateway StatefulSet sets `topologySpreadConstraints` by `topology.kubernetes.io/zone` and `kubernetes.io/hostname`, with `maxSkew: 1`.
- *Success criteria / Storms:* the storm SLO covers the loss of **one gateway**. Losing several gateways at once (a zone) is **degraded, best effort, with no SLO**: clients may get `at_capacity` and retry until the lost pods are rescheduled.
- *Sweeper → Mass-stale guard:* replace the threshold (`>1 epoch / >20%`) with: **sweep a stale epoch only if at least one other epoch renewed within the last renewal interval (10s), and the sweeper's own Redis access has been healthy for one full lease period (30s).** Epochs that sent `EpochEnded` are still always swept. When the guard holds, it alerts as before.
- *Races handled, #12:* update to match. If a single gateway's lease is wrongly starved while the others renew, it may be swept; its lease reply reports `missing` and Reconcile restores it.
- *Tests:* kill 2 gateways at once, and both epochs are swept while the others renew; pause Redis for 90s, and nothing is swept (unchanged).

## O4 — risk — high — ACCEPTED

**Decision:** State the asynchronous-replication assumption; make Revoke durable with `WAIT`; have Reconcile rule 3 decide by the login table's ordering. This resolves open point 2.

**Rationale:** Losing `sess:` writes in a failover is already repaired by Reconcile, provided Reconcile picks the right winner. Losing `rev:{uid}` is a security regression that nothing repairs, and Revoke is rare enough that a synchronous `WAIT` costs nothing. Reusing the login table's ordering in rule 3 adds no new state and makes a rolled-back older session lose to the real, newer one.

**Spec changes to apply:**
- *Redis schema (new note):* Redis Cluster replicates asynchronously; a failover can lose ~1s of acknowledged writes. `sess:` and `gwconns` losses are repaired by Reconcile; lease losses are covered by the next renewal.
- *Logic → Revoke:* write the marker as `revoked_before_ms = max(stored, new)` (Lua, idempotent), then `WAIT 1 <timeout>`. If no replica confirms, return a retryable error; the account service retries `Revoke` until it succeeds. Field deletes and Kicks follow the confirmed write.
- *Logic → Reconcile handling, rule 3:* when the field holds a different session whose `gw|epoch` has a live lease, compare it with the snapshot entry using the login table's ordering (same device_id: higher `conn_seq` wins; different device_id: later `login_ms` wins).
  - Snapshot entry newer → compare-and-set write it, refresh the TTL, ensure it is in `gwconns`, and kick the stored session with the matching reason (hardened Kick).
  - Stored session newer → `not_owner` with `KICKED_BY_OTHER_DEVICE` / `REPLACED`, as before.
- *Open points:* remove #2 (resolved by the above).
- *Tests:* Lua/integration test that simulates a lost write (restore an older field by hand while the newer connection is live), after which Reconcile keeps the newer connection and kicks the older one; a Revoke whose `WAIT` fails returns an error and the retry is idempotent.

## O5 — risk — high — ACCEPTED (derived TTL; rest consciously carried)

**Decision:** Derive the lease TTL from the measured shard failover time, tie the sweeper threshold to it, and document the coupling for later sub-projects. Spreading lease keys across shards and adding an "unknown" lease state are not adopted.

**Rationale:** The coupling (presence validity depends on Logic and on the shard holding the lease key) is inherent to a lease design and is acceptable for a new system, as long as the TTL can outlast a normal failover and consumers know about it. The other remedies design sub-projects 2/3's read path too early.

**Spec changes to apply:**
- *Redis schema → `gwlease`:* the TTL is configurable (default 30s) with the requirement **TTL > measured shard failover time + one renewal interval**. Renewal stays at 10s.
- *Sweeper:* the staleness threshold becomes **2 × lease TTL** (60s by default), not a fixed value. The O3 guard's "Redis access healthy for one full lease period" uses the same TTL.
- *Interface constraints (new):* "If Logic, or the Redis shard holding a gateway's lease key, is unavailable for longer than the lease TTL, that gateway's sessions read as invalid although their connections are healthy. Sub-projects 2/3 must not trigger mass offline pushes on validity alone; the mitigation is theirs to design."
- *Tests:* fail over the Redis shard holding the lease keys and measure the time; the lease must survive it at the configured TTL.
- *Consciously carried:* a total Logic outage longer than the TTL makes all sessions read as invalid.

## O6 — risk — medium — ACCEPTED (minimal)

**Decision:** Always send the first Kick attempt, as O8 requires; retry only when the target's lease is live; use fail-fast calls; label the give-up metric. No batching.

**Rationale:** Because the first attempt is always sent, O8's concern (a lease that looks expired on a live gateway) is still covered, and Reconcile remains the backstop. Stopping retries to dead epochs removes ~400k pointless attempts per gateway death, and fail-fast avoids dial timeouts piling up on Logic. Batching per gateway would add a queue and a flush policy for little gain once retries are gone.

**Spec changes to apply:**
- *Logic → Hardened Kick:* the first attempt is always sent. Retries (up to 3 over ~30s) happen only while the target `gw|epoch` lease is live (Logic caches lease state for ~1s; there are only a handful of epochs). Kick RPCs are fail-fast (no `WaitForReady`).
- *Failure table, "Kick can't reach the gateway":* update to "retried while the target lease is live; otherwise one attempt, then the Reconcile backstop".
- *Observability:* Kick give-ups are labelled `target_lease=live|dead`; alerts use `live` only.
- *Tests:* SIGKILL a 200k gateway, and the number of Kick attempts to it is about one per relogin with no retries; a dropped Kick to a live gateway is still retried and then caught by Reconcile.

## O7 — risk — medium — ACCEPTED (with ConfigMap fallback)

**Decision:** Size Logic's `minReplicas` for the storm peak; gate Logic readiness on having a JWKS; ship JWKS as a ConfigMap that is used at cold start when the account service is unreachable.

**Rationale:** HPA reacts more slowly than the 30s SLO, so steady-state capacity must already cover the peak. The readiness gate keeps a pod with no key set out of the Login path. The ConfigMap removes the remaining double-failure case (all Logic pods restart while the account service is down) at the cost of one step in the key-rotation runbook.

**Spec changes to apply:**
- *Deployment / Logic:* `minReplicas` must handle the **20k logins/s peak without scaling**, as measured by the load test. HPA scales above that; the SLO does not depend on it.
- *Logic → JWKS:*
  - At startup, load the JWKS from the account service; if that fails, load it from a mounted ConfigMap (`jwks-fallback`) and keep retrying the account service on the normal refresh schedule.
  - Logic reports Ready only once it holds a key set from either source.
  - Alert while a pod runs on the fallback set.
- *Key rotation runbook:* a new signing key is published to the `jwks-fallback` ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap. (A stale ConfigMap only matters while the account service is unreachable.)
- *Tests:* load test of the storm with Logic held at `minReplicas`; Logic starts with the account service down, becomes Ready from the ConfigMap, and accepts tokens; a Logic pod with no key set from either source never becomes Ready.

## O8 — specification quality — medium — ACCEPTED

**Decision:** Add a Consistency contract section to the spec, and give Reconcile two speeds.

**Rationale:** The bounds already exist in the design but are spread across sections; consumers need one table to size caches and fallbacks. Two Reconcile speeds turn a ~10-minute restore after `missing` into ~40s by changing one setting.

**Spec changes to apply:**
- *New section "Consistency contract"* (after *Races handled*):

  | Divergence consumers may see | Cause | Bound | Repaired by |
  |---|---|---|---|
  | Ghost: valid entry, but the connection is gone | Logout dropped | ≤ 1 Reconcile cycle (~10 min + jitter) | Reconcile delete phase |
  | Old owner still connected after being replaced or revoked | Kick lost | ≤ 1 cycle | Reconcile `not_owner` / rule 1 |
  | Entry of a dead gateway | Gateway death | invalid after the lease TTL (30s); deleted after 2 × TTL plus sweep time | validity rule, then sweeper |
  | Live connection missing from Redis | Write lost in failover, or wrongly swept | ≤ 1 cycle; ≤ ~1 min after a `missing` lease reply | Reconcile restore |
  | Healthy session reads invalid | Logic or lease shard down longer than the TTL | the length of the outage | lease renewal resumes (O5) |

- *Gateway → Reconcile:* two paces, both configurable: the **periodic** pass is spread over the cycle (~330 entries/s at 200k); a pass **triggered by `missing`** runs at ~5k entries/s per gateway (200k in ~40s; ~30k Lua calls/s cluster-wide if all 6 gateways run one at once).
- *Tests:* after a forced sweep of a live gateway, its sessions are restored within ~1 min.

## O9 — implementation — low — ACCEPTED

**Decision:** The sweeper removes `gwconns` with `UNLINK`; document the per-gateway `SADD` concentration as acceptable.

**Rationale:** `UNLINK` frees the set in a background thread, so a 200k-member delete does not stall the shard. A storm's `SADD` load on one key (~10k/s) is well within a single Redis shard.

**Spec changes to apply:**
- *Sweeper:* "Then `UNLINK` the set and remove the ZSET member."
- *Redis schema → `gwconns`:* note: one key per epoch, so all of a gateway's `SADD`s land on one shard; acceptable at the storm rate.

## Summary of open points after this round

- Open point 2 of the spec is resolved by O4.
- Open points 1, 3 and 4 of the spec are unchanged by this round.
