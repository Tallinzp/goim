---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev2.md
date: 2026-10-03
mode: spec
focus: scalability, availability, reliability, eventual consistency
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: risk
    severity: high
    claim: "Reconcile restores sessions that closed after the snapshot was taken, so it creates ghosts on every cycle in proportion to churn."
    evidence: "\"the gateway sends its ONLINE set ... in rate-limited chunks. The snapshot carries a cutoff_ms taken when it started.\" / rule 4: \"Field is empty ... → write the entry (restore)\" / \"Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway\""
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: risk
    severity: high
    claim: "Rolling updates and post-failure recovery concentrate connections on the remaining gateways, far beyond the 200k operating point, because nothing rebalances connections or caps them per gateway."
    evidence: "\"Density: the operating point is at most 200k connections per gateway\" / \"Graceful drain: on SIGTERM ... send Reconnect{jitter} in batches over about 60s\" / admission control lists only handshake, login-queue and pending-memory caps"
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: risk
    severity: high
    claim: "The failure domain is sized as one pod, but a node or zone loss takes out several gateways at once: N−1 capacity does not hold, and the mass-stale guard then never sweeps the dead epochs."
    evidence: "\"1M users need at least 6 gateways (5 at 200k plus 1 for N−1)\" / \"If more than a configurable threshold (e.g. >1 epoch, or >20% of live epochs) are stale at once ... skip sweeping, raise an alert and metric, and re-check next cycle.\""
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: risk
    severity: high
    claim: "The spec treats acknowledged Redis writes as durable, but Redis Cluster replicates asynchronously, so a failover can undo a revocation or roll a slot back to an older session that Reconcile then defends against the newer one."
    evidence: "\"Redis Cluster; per-user keys hash-tagged {uid}\" / Reconcile rule 3: \"Field holds a different session whose gw|epoch has a live lease → return it in not_owner ... Never override it.\" / Open point 2 \"only possible after a lost write, e.g. a Redis failover\""
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: risk
    severity: high
    claim: "Session validity depends on one lease key per gateway, renewed through Logic, with a 30s fuse; a single Redis shard failover or a bad Logic rollout makes up to every session look offline."
    evidence: "\"A session is valid only if its field exists and EXISTS gwlease:{its gw}:{its epoch}\" / \"gwlease:gw:{epoch} | STRING | PX 30000, renewed every 10s\" / \"Layer rule: The gateway never touches Redis. Only Logic touches Redis.\""
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: risk
    severity: medium
    claim: "Every relogin after a gateway death triggers a Kick with retries towards the dead gateway, so Kick traffic and give-up metrics grow with storm size."
    evidence: "\"Hardened Kick (O8): Logic always sends the Kick ... up to 3 attempts over ~30s on transport errors, then give up and increment a metric\" / login table: \"same device_id, new conn_seq > stored | overwrite; kick the old connection with REPLACED\""
    disposition: pending
    disposition_rationale: null
  - id: O7
    category: risk
    severity: medium
    claim: "Logic's storm capacity depends on reactive HPA and on fetching JWKS from the account service at startup, so neither the 20k/s peak nor recovery after a Logic restart is guaranteed."
    evidence: "\"logic is a stateless Deployment with HPA\" / \"Scales out horizontally (HPA on CPU)\" / \"JWKS: load the Ed25519 key set from the account service at startup\""
    disposition: pending
    disposition_rationale: null
  - id: O8
    category: specification quality
    severity: medium
    claim: "The spec has several convergence bounds spread across sections, but no single staleness contract that sub-projects 2/3 can rely on, and the restore time after a missing epoch is unbounded."
    evidence: "\"a Kick lost after 3 retries is applied by the next Reconcile through not_owner (≤ ~10 min)\" / \"immediately on a missing lease reply ... in rate-limited chunks\" (no rate given) / \"Session-validity read path: deferred to sub-projects 2/3\""
    disposition: pending
    disposition_rationale: null
  - id: O9
    category: implementation
    severity: low
    claim: "gwconns is one SET of ~200k members per epoch in a single slot; deleting it with DEL blocks Redis, and all per-gateway SADD traffic lands on at most six slots."
    evidence: "\"gwconns:gw:{epoch} | SET | uids\" / Sweeper: \"Then delete the set and the ZSET member.\" / Login: \"SADD to the epoch index\""
    disposition: pending
    disposition_rationale: null
---

# Objection record: Connection & Session Core, revision 2

Review lens requested: **scalability, availability, reliability, eventual consistency**. Revision 2 already absorbed O1–O12 of the first review. The objections below are new, or show consequences the first round did not cover. Where one touches an earlier disposition, it says so instead of reopening that disposition.

## O1 — risk — high

### Claim

The Reconcile snapshot is taken once, at `cutoff_ms`, and then streamed in rate-limited chunks. Take a connection that is in the snapshot, closes during the pass, and has its Logout applied before Logic reaches its entry. Logic then finds the field empty and **restores** it under rule 4. The gateway already sent its one Logout, so nothing removes the restored entry until the *next* Reconcile pass. The result is a ghost with a live lease, so it counts as valid, for one to two cycles.

### Evidence

> the gateway sends its ONLINE set … in rate-limited chunks. The snapshot carries a `cutoff_ms` taken when it started.

> 4. Field is empty, or its `gw|epoch` has no live lease → write the entry (restore)

> Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway

If the chunk rate matches the cost line, one pass takes about the whole 10-minute cycle. The window is then the full cycle, not a few milliseconds.

### Why this matters

Mobile churn is constant: background kills, network switches, and closes from the heartbeat timeout. Every close that falls between the snapshot and the processing of its entry becomes a ghost. That is a steady population, likely thousands per gateway per cycle, not a rare race. Ghosts look online to sub-projects 2/3: pushes go to a gateway that says "unknown conn", and offline push (APNs/FCM) is delayed or skipped. `cutoff_ms` only protects the delete phase; nothing protects the restore phase. The existing test, "a dropped Logout is repaired within one cycle", does not cover this case, because here the Logout was *not* dropped.

Possible fixes: build each chunk from live state when it is sent rather than from the start-of-pass snapshot; have the gateway send a list of entries closed since the snapshot, which Logic compare-and-deletes after it restores; or re-check that each restored connection is still ONLINE at the end of the pass.

## O2 — risk — high

### Claim

Nothing in the design moves long-lived connections back once they land on a gateway, and nothing caps connections per gateway. A rolling update of the StatefulSet therefore piles connections onto the gateways drained last.

### Evidence

> **Density** *(O1)*: the operating point is at most 200k connections per gateway … about 167k each in steady state.

> **Graceful drain:** on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter}` in batches over about 60s

Admission control has a handshake limit, a Login-queue cap and a pending-memory cap. None of them limits ONLINE connections per gateway.

### Why this matters

Assume the NLB spreads new connections evenly over Ready targets, and the rollout drains in reverse ordinal order with 6 × 167k:

| step | drained | gw0 load after |
|---|---|---|
| drain gw5 | 167k → 5 others | 200k |
| drain gw4 | 200k → 5 others (incl. empty gw5) | 240k |
| drain gw3 | 240k | 288k |
| drain gw2 | 288k | ~346k |
| drain gw1 | ~346k | ~415k |

By the end of a routine deploy, gw0 holds about 2× the operating point. Its own drain is then a 415k-connection storm, about double what the SLO is sized for. The same skew follows every crash: the restarted pod comes back empty and only fills up through churn. The 200k operating point, which bounds blast radius, is therefore not enforced by anything in the design.

Possible fixes: a per-gateway ONLINE cap that answers `Busy` early, so the retry hashes to another target; drain pacing that waits for the replacement pod to be Ready; or occasional `Reconnect` shedding from gateways above the mean.

## O3 — risk — high

### Claim

Capacity planning and the mass-stale guard both assume gateways fail one at a time. On Kubernetes a node, node-pool or zone failure kills several gateways together, and the spec states no pod anti-affinity or topology spread.

### Evidence

> 1M users need at least 6 gateways (5 at 200k plus 1 for N−1)

> **Mass-stale guard** *(O2)*: … If more than a configurable threshold (e.g. >1 epoch, or >20% of live epochs) are stale at once, treat it as systemic … skip sweeping, raise an alert and metric, and re-check next cycle.

### Why this matters

- **Capacity:** losing one zone out of three takes out 2 of 6 gateways, which is 333k connections. The 4 survivors would need about 250k each, above the operating point, and the storm is about 17k logins/s, close to the full 20k/s headroom. If two gateways share a node, a single node loss already does this.
- **Guard never releases:** if two epochs genuinely die together, the guard trips on every cycle, because the epochs stay stale forever. The sweeper never cleans them up and the alert never clears, so it fires on a correctly diagnosed real event. Cleanup falls back to clients relogging in plus the 30-day TTL. The guard has no exit condition, such as "the stale epochs' pods no longer exist" or "other epochs from the same window renewed again", that would separate a real multi-gateway death from Redis or Logic trouble.

Spec changes to consider: require topology spread across zones and nodes; size N−k for k equal to the largest failure domain; and give the guard an exit condition, for example "other epochs are renewing normally, so this staleness is real".

## O4 — risk — high

### Claim

Redis Cluster acknowledges a write before it reaches the replica. A primary failover can lose roughly the last second of acknowledged writes. The spec reasons as if acknowledged Lua writes are durable, and two of its mechanisms turn a lost write into a wrong outcome rather than mere staleness.

### Evidence

> Redis Cluster; per-user keys hash-tagged `{uid}`

> 3. Field holds a **different** session whose `gw|epoch` has a **live** lease → return it in `not_owner` … Never override it.

> Open point 2: … even if the snapshot entry has a later `login_ms` (only possible after a lost write, e.g. a Redis failover).

> **Revoke** *(O10)*: … Logic sets `rev:{uid}` …

### Why this matters

1. **Lost revocation.** `Revoke` sets `rev:{uid}` and kicks. If the failover drops the marker, any token issued before the revocation is accepted again until it expires (up to 1h). Honest clients drop their token on the `REVOKED` kick; a stolen token does not. Reconcile cannot repair this, because the revocation marker is the very write that was lost.
2. **Rollback to an older session, enforced by Reconcile.** Device B logs in on gw-42 and the write replacing device A's entry (gw-17) is lost. A was kicked and is gone, but the field again points at A on gw-17, whose lease is live. When gw-42 reconciles, rule 3 returns B as `not_owner` with `KICKED_BY_OTHER_DEVICE`. The user now has **no** connection and is told another device took over. Whether this happens depends on which gateway reconciles first. Open point 2 calls this "deterministic"; it is deterministically wrong in this case.

Spec changes to consider: state the durability assumption explicitly; use `WAIT 1` (or a durable store) for `Revoke`, so the account service retries until the marker is replicated; and in rule 3, prefer the entry with the later `login_ms`/`conn_seq`, or check with the stored session's gateway via a Kick-style probe before marking the snapshot entry `not_owner`.

## O5 — risk — high

### Claim

The validity rule ties every session to one `gwlease` key per gateway. That key is renewed only through Logic, has a 30s TTL, and sits on a single Redis shard. Presence availability is therefore capped by that shard's failover time and by Logic availability, with a 30-second fuse.

### Evidence

> **A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

> `gwlease:gw:{epoch}` | STRING | `PX 30000`, renewed every 10s with a plain SET

> The gateway never touches Redis. Only Logic touches Redis.

### Why this matters

- **Shard failover:** Redis Cluster detects a failed primary after `cluster-node-timeout` (default 15s) plus an election, and managed offerings often take 30s or more. The last renewal before the failure may also be among the lost writes (see O4). If the failover takes longer than about 20s, every session on the gateways whose lease key lives on that shard becomes invalid, even though no gateway, connection or `sess:` key is affected. With 6 epochs, one shard can hold a large share of all leases.
- **Logic rollout:** a crash-looping Logic rollout stops lease renewals for every gateway. Within 30s, all 1M users look offline to sub-projects 2/3, which could set off a cluster-wide burst of offline pushes. The mass-stale guard only stops the *sweep*; it does not stop the *invalidity*.
- The spec doesn't say whether "after Redis becomes reachable again" (the sweeper's 30s wait) is judged per shard or for the cluster.

Spec changes to consider: state the lease TTL as a function of the measured shard failover time; put lease keys (and `gwepochs`) where one shard failover cannot hit all of them; give consumers a "lease unknown, treat as valid" state when Logic itself can't read leases; and add a test that fails over the shard holding the leases.

## O6 — risk — medium

### Claim

After a gateway dies, nearly every one of its 200k relogins finds its own old entry and triggers a `REPLACED` Kick at the dead `gw|epoch`. O8's "always send the Kick" rule, together with 3 retries over about 30s, makes Kick traffic grow with storm size, at exactly the moment Logic is busiest.

### Evidence

> **Hardened Kick** *(O8)*: Logic always sends the Kick … Bounded retry: up to 3 attempts over ~30s on transport errors, then give up and increment a metric.

> same device_id, new conn_seq > stored | overwrite; kick the old connection with `REPLACED`

### Why this matters

- After a node loss, the old pod object usually lingers for minutes (default tolerations are 300s). With `publishNotReadyAddresses`, its DNS name keeps resolving to a black-holed IP, so up to 600k Kick attempts sit in retry state on Logic during the storm.
- After a pod kill, StatefulSet brings back the *same name* quickly. The new gw-17 instance then receives about 200k Kicks with the wrong epoch while it absorbs its own storm.
- The `kick give-up` metric rises by about 200k on every node loss, so it can't serve as a signal for the case it was added for: a Kick that was lost to a live gateway.

This does not reopen O8's correctness argument, which is that a lease that merely looks expired is no reason to skip a Kick. It asks for Kicks to be coalesced per gateway (one batched RPC per `gw|epoch`), for retries to stop once the target channel has been down longer than one lease period (Reconcile is the backstop anyway), and for the give-up metric to be labelled by whether the target epoch's lease was live.

## O7 — risk — medium

### Claim

The storm SLO depends on Logic handling about 10k logins/s within seconds of a failure, but Logic scales reactively. A Logic pod also cannot serve logins until it has fetched JWKS from another service.

### Evidence

> logic is a stateless Deployment with HPA

> Stateless. … Scales out horizontally (HPA on CPU).

> **JWKS** *(O10)*: load the Ed25519 key set from the account service at startup … If a refresh fails, keep the last good set

### Why this matters

- The storm peaks within the 0–20s jitter window. HPA takes about 15s to sync, plus the metrics window, pod scheduling and startup, which is longer than the SLO. Steady-state Logic must therefore already be sized for the 20k/s peak (`minReplicas`), and the spec does not say so. If it isn't, the Login-queue cap returns `Busy`, clients retry 1–3s later, and the storm turns into retry amplification.
- "Keep the last good set" helps only a pod that has already loaded one. A new pod, or a pod restarted during an incident, has none. If the account service is degraded at that moment, which is likely when the incident is cluster-wide, Logic cannot verify any token and login recovery stalls.

Spec changes to consider: size `minReplicas` from the storm peak; ship JWKS through a ConfigMap or Secret (or a cache on disk) as the cold-start fallback; and add a storm load test with Logic at `minReplicas`.

## O8 — specification quality — medium

### Claim

Sub-projects 2/3 inherit a store that converges eventually, but the bounds are spread across sections and one of them is missing. They have no consolidated staleness contract to design against.

### Evidence

- Lost Kick: "≤ ~10 min" (Races 8).
- Dropped Logout: "repaired by the next Reconcile" (up to one cycle plus jitter; up to two cycles with O1).
- Dead gateway: the lease expires within 30s, the sweep starts after 60s, and with the guard tripped (O3) the sweep may never happen.
- Restore after a `missing` lease: "in rate-limited chunks", with no rate given. If the guard fails and every gateway is missing, 1M entries are restored at an unknown pace, and those users look offline until then.
- "Session-validity read path: deferred to sub-projects 2/3".

### Why this matters

Without a table of the form "a consumer may see state X for at most T after event Y", sub-project 2 cannot size its ownership-check cache, and sub-project 3 cannot decide when to fall back to offline push. Each will guess differently. Spec change to consider: a short **Consistency contract** section listing each divergence (ghost, lingering old owner, missing live session, invalid-but-alive) with its bound and the mechanism that enforces it, plus a stated Reconcile chunk rate and restore-time target.

## O9 — implementation — low

### Claim

`gwconns:gw:{epoch}` is a SET of about 200k members in one slot. The sweeper's "delete the set" with `DEL` frees the whole set synchronously on the Redis main thread, and every Login's `SADD` for a gateway goes to that one key.

### Evidence

> `gwconns:gw:{epoch}` | SET | uids; pruned by Reconcile

> Then delete the set and the ZSET member.

> SADD to the epoch index

### Why this matters

A synchronous free of a 200k-member set stalls the shard for tens of milliseconds, and that shard also holds unrelated `sess:{uid}` keys. Use `UNLINK`, or remove members in batches from the SSCAN loop. During a storm, the `SADD` load for each gateway lands on one shard; this is fine at 10k/s but worth stating. It also means a resharding of that one slot moves a big key.

## Explicitly not objecting to

- **Single-slot Lua for login/logout**: atomic compare-and-set on one `{uid}` slot is the right tool here, and the lost-write case is raised separately (O4).
- **Logout fence (O9 of the first review)**: the `recv_ms + login_deadline + 1s` bound depends on the Lua `EXPIRED` check, and the reasoning holds as long as gateway↔Redis clock skew stays under 1s.
- **150s heartbeat vs NAT/LB idle timeouts**: common cloud L4 idle timeouts are 350s (AWS NLB) or configurable, and the spec already measures reconnects after idle to catch shorter carrier NATs.
- **Storm arithmetic for a single gateway death**: 200k over 20s is about 10k/s and is correct; the objections are about multi-gateway (O3) and skewed (O2) cases, not this figure.
- **Open point 3 (slot-scoped Revoke vs Reconcile restore)**: real, but the spec already flags it, so repeating it would add no information.
- **Per-loop conn_id generation and gnet ownership**: correct as specified, and covered by a `-race` test.
- **Reconcile cost (~2k Lua calls/s cluster-wide)**: negligible for Redis Cluster; the issue with Reconcile is correctness (O1), not cost.
