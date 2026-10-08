---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev5.md
objections:
  - docs/superpowers/objections/connection-session-core-design-rev5-system-design.md
date: 2026-10-07
decision_maker: user
status: decided (2026-10-07) — every entry is decided by the user; accepted changes are applied to the rev6 spec (2026-10-03-connection-session-core-design-rev6.md)
guiding_principle: new system; take the smallest fix that closes the failure, and avoid overdesign
---

# Disposition record — Connection & Session Core, revision 5

Decisions on the system-design review of rev5. References use the prefix *R5S-O<n>* (objections O1–O9 of `rev5-system-design.md`). The user decides each objection. Accepted changes are listed under **Spec changes to apply** and are applied in the rev6 spec.

## Triage summary

| Ref | Topic | Severity | Decision | Change size |
|---|---|---|---|---|
| R5S-O1 | Login batch waits on the slowest Redis shard | high, blocking | **DECIDED: ACCEPT** (option A, per-entry results) | Login RPC shape + one Tier 2 test |
| R5S-O2 | Unbounded Kicks to a dead epoch on the storm path | high, blocking | **DECIDED: ACCEPT** (option A: async, 1s deadline, bounded queue) | three rules on the hardened Kick + tests |
| R5S-O3 | StatefulSet pod on a lost node is not rescheduled | medium | **DECIDED: ACCEPT** (option A: automated `out-of-service` taint) | one operational rule + recovery target + Tier 3 check |
| R5S-O4 | Rolling restart vs flow-hash LB hits the cap | medium | **DECIDED: ACCEPT** (option A: least-connections LB where available + separate pre-handshake retry counter) | one Deployment rule + one SDK rule + tests |
| R5S-O5 | No PodDisruptionBudget | medium | **DECIDED: ACCEPT** (option A: gateway + Logic PDBs, autoscaler annotation) | three manifest rules + one test |
| R5S-O6 | Correlated local triggers; whole-population reconnect | medium | **DECIDED: ACCEPT** (option A: new failure class + Tier 2 scenario + jitter windows in `LoginResp`) | one failure class, one `LoginResp` field group, one SDK rule, tests |
| R5S-O7 | Production node and LB limits unspecified | medium | **DECIDED: ACCEPT** (option A: requirements list + gateway startup self-check) | one Deployment subsection + one Readiness check + tests |
| R5S-O8 | `ReconcileEnd` delete phase unrated and uncounted | low | **DECIDED: ACCEPT** (option A: rate limit + cost line + duration metric) | one rate limit, cost line, one metric |
| R5S-O9 | Redis zone placement unstated | low | **DECIDED: ACCEPT** (option A: state placement + zone-loss row) | one Deployment rule + one failure row |

---

## R5S-O1 — a Login batch waits for its slowest Redis shard — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: the Login RPC returns each entry's result as soon as its own script finishes. Options B (Logic groups by Redis node and replies once per group) and C (accept the stall and document it) were rejected.

**Claim.** A batch of 200 random uids touches every shard, and go-redis's `ClusterPipeline.Exec` returns only when all node pipelines have finished. During one shard's failover, every batch stalls; past `login_deadline` every login in flight fails (`EXPIRED`, or a wait on fence (b)), not just the ~1/N on the failing shard.

**Rationale.** The gateway already resolves each Login on its own: fence (a) is per connection, and the Login-queue cap counts entries. Only the RPC's reply granularity has to change. Option B keeps a unary RPC but has Logic track cluster slot ownership, which changes during a failover, the one moment it matters. Option C accepts a cluster-wide login outage for every shard failover.

**Spec changes to apply:**
- *Internal RPC / Logic, RPCs:* `Login` becomes a batch request with **per-entry results streamed back** as each entry completes (server-streaming, or bidirectional with the batch as the request message). The batch still bounds how many entries go out together (200 / 5ms). It no longer bounds when they come back.
- *Logic, Login:* each entry's script runs and replies on its own. An entry on a slow shard never holds back an entry on a healthy one. Logic must not wait for the whole pipeline before replying.
- *Gateway, Login batching:* the RPC timeout (≥ `login_deadline + 1s` from `recv_ms`) applies per entry. Entries without a result when the stream ends or fails have no definitive reply and fall under fence (b), as today.
- *Failure table, Redis shard failover:* add "logins on the failing shard stall or come back `EXPIRED`; logins on other shards are unaffected (per-entry results)".
- *Tests:* a Logic unit test where one entry's script is delayed: the other entries' results arrive before it. A Tier 2 test: during a single-shard failover, logins for uids on the other shards keep their normal p99.
- *Open for the plan:* whether Reconcile chunks get the same per-entry streaming. A slow shard there only delays chunk resolution and causes re-queues, not an outage, so it isn't required.

---

## R5S-O2 — Kicks to a dead or unreachable epoch have no time limit — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: the Kick is sent after the Login reply and never delays it; every attempt has its own ~1s deadline; each target `gw|epoch` has a bounded Kick queue whose overflow is dropped and counted. Options B (skip the first attempt when the cached lease is dead) and C (per-attempt deadline only) were rejected.

**Claim.** On node loss the dead pod stays in Endpoints, so its pod DNS name resolves to an IP that never answers. Every relogin sends one first-attempt Kick there (R2-O6). grpc-go's minimum connect timeout is 20s, and a non-`WaitForReady` RPC still blocks while the channel is CONNECTING. The spec doesn't say whether the Login reply waits for the Kick: if it does, the 30s SLO fails; if not, Logic holds ~200k blocked RPCs during the storm.

**Rationale.** A keeps R2-O6's "first attempt always sent", which protects a starved but alive gateway, and bounds its cost in time (deadline) and memory (queue). B reverses R2-O6 and, without a deadline, still blocks for a node that died within the last lease TTL, while its lease still looks live. C leaves the pile-up unbounded and the reply ordering unstated. A dropped Kick is already covered: Reconcile `not_owner` closes the old connection within one cycle (race 8), and to a dead epoch the Kick has nothing to close.

**Spec changes to apply:**
- *Logic, Login:* the Kick follow-up is sent **after** the entry's Login result is returned, asynchronously; a Login result never waits for a Kick.
- *Logic, Hardened Kick:*
  - every Kick attempt (first and retries) has its own deadline (default ~1s, configurable), covering connect and RPC;
  - Kicks are queued **per target `gw|epoch`** in a bounded queue (size configurable; sized in the plan) drained by a bounded number of workers per target. When the queue is full, the attempt is dropped and counted (`kick_dropped{target_lease=live|dead}`); Reconcile is the backstop;
  - retries (up to 3 over ~30s while the target lease is live) go through the same queue and deadline.
- *Observability:* Kick queue depth per target, drops by `target_lease`, attempt latency.
- *Failure table, Node loss:* add "Kicks to the lost pod time out after ~1s each and never delay relogins; overflow is dropped".
- *Tests:*
  - Logic unit test: a Kick target that never answers (blackholed address): Login results return without waiting, each attempt ends within the deadline, and a full queue drops and counts instead of growing;
  - load test (b2): Login p99 during the storm is unaffected by Kicks to the dropped node, and Logic goroutine count stays bounded.

---

## R5S-O3 — a StatefulSet pod on a lost node is not rescheduled — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: automation applies the `node.kubernetes.io/out-of-service` taint when a gateway's node has been NotReady longer than X (default 2 min), with force-deleting the pod as the fallback. Options B (switch gateways to a Deployment) and C (alert plus a manual runbook) were rejected.

**Claim.** A StatefulSet guarantees at most one pod per name, so a pod on an unreachable node stays Terminating/Unknown until the kubelet confirms it is gone, someone force-deletes it, or the node is tainted `out-of-service`. Until then `gw-k` isn't recreated, the fleet stays at N−1 for an open-ended time, and a second loss is a no-SLO multi-gateway event. The stuck pod's Endpoint also keeps Kicks going to a dead IP (R5S-O2).

**Rationale.** The design doesn't rely on the at-most-one guarantee: identity is per epoch, so a zombie with the same name is harmless (race 4), and its Kicks are ignored on epoch mismatch. Ending the guarantee early is therefore safe, and it is the smallest change. B gives up stable pod DNS names, which Kick routing relies on, and would bring back the registry R1 dropped. C makes N−1 recovery depend on how fast a human responds.

**Spec changes to apply:**
- *Deployment:* add a **node-loss recovery** rule. A controller (a small operator, or the cloud's node auto-repair if it applies the same taint) applies `node.kubernetes.io/out-of-service=nodeshutdown:NoExecute` to a node that runs a gateway pod and has been NotReady for longer than X (default 2 min, configurable). Kubernetes then deletes the pod and its volume attachments, and the StatefulSet recreates `gw-k` on another node. If the taint can't be applied, the fallback is to force-delete the pod (`--grace-period=0 --force`). The taint is removed once the node is back or replaced.
- *Why it is safe:* add one sentence: a stale instance of `gw-k` that is still running on the partitioned node has a different epoch, so its lease, Kicks and Redis entries can't conflict with the new instance (race 4).
- *Success criteria / failure table, Node loss:* add a recovery target: a replacement gateway is Ready within X + gateway startup (≈ 2–3 min by default), after which the fleet is back at N.
- *Observability:* time from node NotReady to replacement gateway Ready; alert when it exceeds the target.
- *Tests:* Tier 3 `cloud`: stop a gateway's node (not just the pod); the taint is applied after X, `gw-k` is recreated on another node and becomes Ready within the target. On `TARGET=local` the controller logic is checked by stopping the kubelet on a k3s agent node; listed as such in `not_covered` if the local cluster can't simulate it.

---

## R5S-O4 — a rolling restart behind a flow-hash LB fills the remaining pods to the cap — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: require a least-connections LB algorithm where the cloud LB supports it, and give pre-handshake closes their own retry counter (limit 10, 0.5–1.5s waits) in the SDK, for any LB. Options B (SDK only), C (LB algorithm only) and D (slower rolls) were rejected.

**Claim.** The LB places connections without regard to load. Starting from 6 × 167k at 1M, drained clients spread evenly over the Ready pods push the pods not yet restarted to the 250k cap from the third step on (p3–p5 would reach 288k). With 3 of 5 targets full, each retry is rejected with probability 0.6, and 0.6⁵ ≈ 7.8% of the ~114k overflow clients (~9k) exhaust the 5 retryable rejects and fall into exponential backoff, past the SLO.

**Rationale.** The two halves fix different things. Least-connections removes the skew at its source and also fills restarted pods, which otherwise stay underfilled until later churn, but it isn't available on every LB (AWS NLB only flow-hashes). The SDK change makes the SLO hold on any LB: a pre-handshake close costs the gateway one accept and one close, no crypto, so more and faster retries are cheap for the server. D spreads reconnects in time but not in placement, so the cap is still hit.

The client can't tell `at_capacity` from the other pre-handshake closes (`handshake_rate`, `bad_version`, `noise_unknown_key`); all are silent closes. The new counter therefore covers every pre-handshake close. That is acceptable: `handshake_rate` is still shaped by the gateway's bucket (a fast retry is rejected as cheaply as the first), and `bad_version` / `noise_unknown_key` are persistent, so those clients reach exponential backoff after 10 tries instead of 5, a few seconds later.

**Spec changes to apply:**
- *Deployment:* the cloud LB uses a **least-connections** algorithm where it offers one (e.g. Alibaba CLB/NLB `wlc`, Tencent CLB `least_conn`). Where it only flow-hashes (e.g. AWS NLB), the SDK rule below is what keeps the SLO; state that skew then stays until churn evens it out (active rebalancing remains sub-project 5).
- *Client SDK rules, Retryable rejects:* split into two counters:
  - **pre-handshake closes** (the server closes the TCP connection before the Noise handshake completes): wait a uniform random **0.5–1.5s**, then reconnect; after **10 in a row**, exponential backoff (cap 5m);
  - **other retryable rejects** (`Busy{retry_after}`, `EXPIRED`, `KEY_UNKNOWN`, close after the handshake but before `LoginResp`/`Error`): unchanged, 1–3s (or `retry_after`), 5 in a row, then backoff.
  - Any outcome that isn't a pre-handshake close resets the pre-handshake counter, and vice versa. Both reset on a successful login. Stop conditions and precedence are unchanged.
  - The arithmetic, stated with the rule: with 3 of 5 targets full, 0.6¹⁰ ≈ 0.6% reach backoff; 10 retries take ~10s, which fits after the 5s drain jitter.
- *Storms, drain, failures* summary line and *Several gateways lost at once* row: replace "retry after 1–3s (up to 5 times, then backoff)" for pre-handshake closes with "0.5–1.5s, up to 10 times".
- *Tests:*
  - SDK unit tests: pre-handshake closes retry after 0.5–1.5s and fall back to backoff after 10 in a row; the two counters are independent and each resets the other; the 0.5–1.5s wait passes the uniformity bucket check;
  - Tier 3 rolling restart: assert that the share of clients reaching exponential backoff stays below 1%, and record per-pod connection counts at each step; on a least-connections LB (`cloud`, if available), the restarted pods are within 10% of the mean at the end of the roll.

---

## R5S-O5 — no PodDisruptionBudget — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: a gateway PDB with `maxUnavailable: 1`, the `safe-to-evict: "false"` annotation on gateway pods, and a Logic PDB with `minAvailable` = `minReplicas`. Options B (gateway PDB only) and C (node-pool upgrade settings instead of PDBs) were rejected.

**Claim.** With one gateway per node and no PDB, a managed node-pool upgrade or a cluster-autoscaler scale-down that drains nodes in parallel evicts several gateways at once: the no-SLO zone-loss behaviour, caused by routine operations.

**Rationale.** A PDB is the Kubernetes-native guard, it covers every eviction path (upgrades, autoscaler, manual `kubectl drain`), and it lives in the manifests. Graceful drain already makes a single eviction safe; the PDB makes sure there is only one at a time. B leaves the storm SLO's Logic floor (`minReplicas`, R2-O7) unprotected during a drain. C depends on the provider, misses the autoscaler and manual drains, and isn't visible in the manifests.

**Spec changes to apply:**
- *Deployment:*
  - gateway PodDisruptionBudget `maxUnavailable: 1`. Intended side effect, stated in the spec: if a gateway is already unavailable (e.g. after a node loss, R5S-O3), the PDB blocks further voluntary evictions until it is back, so an upgrade waits rather than turning a 1-gateway loss into a 2-gateway loss;
  - gateway pods carry `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`, so the autoscaler never removes a gateway node on its own; gateway capacity changes only through the StatefulSet;
  - Logic PodDisruptionBudget `minAvailable` = `minReplicas`, because the storm SLO assumes those replicas are Ready.
- *Graceful drain:* add that an eviction goes through the same SIGTERM drain, so `terminationGracePeriodSeconds` > drain window also bounds how long one eviction holds up a node upgrade.
- *Tests:* Tier 3 (`local` and `cloud`): `kubectl drain` two gateway nodes in parallel: the second eviction waits until the first gateway is Ready again; at no point are two gateways unavailable. A drain of Logic nodes never leaves fewer than `minReplicas` Ready.

---

## R5S-O6 — correlated local triggers; whole-population reconnect; jitter windows fixed in the app — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: add *whole-population reconnect* as a best-effort failure class with an expected recovery time, add a Tier 2 scenario for it, send the jitter windows in `LoginResp` (persisted and clamped by the SDK), and correct the "uncorrelated" wording. Options B (name and test only, no tunables) and C (widen the local trigger to 0–3s) were rejected.

**Claim.** The 0–1s local trigger skips the 0–20s storm wait on the assumption that network-change events are uncorrelated across users. A carrier or city network recovery fires them for millions of devices at once. A whole-population reconnect (LB failover, regional network blip: 1M in 20s ≈ 50k/s against 20k/s of headroom) is not a named failure class and has no stated recovery expectation. The jitter windows can only be changed by an app release.

**Rationale.** The gateways are already protected: the handshake bucket and the connection cap close the excess cheaply, and those closes are retryable rejects. What's missing is a stated expectation, a measurement, and an operational lever. Server-sent windows are that lever: ops can widen the storm window or the local window during an incident, or permanently after measurement, without waiting for an app release. C slows every legitimate network switch and still leaves the burst unbounded.

**Spec changes to apply:**
- *Success criteria / Storms, drain, failures:* add a failure class **whole-population reconnect** (LB failover or reset, regional or carrier network recovery): best effort, no SLO. Expected recovery ≈ users ÷ admitted login rate (1M ÷ 20k/s ≈ 50s) plus the backoff tail of clients that hit the reject limits. Admission control (handshake bucket, connection cap, Login-queue cap) shapes it; no component may fail or fall over under it.
- *Client SDK rules, Local trigger:* replace "These events are uncorrelated across users, so there is no storm to spread" with: such events are usually uncorrelated; when they are correlated (a carrier or city network recovering), the burst is shaped by gateway admission control and the retry rules, and ops can widen the local window through `LoginResp`.
- *Client protocol, `LoginResp`:* besides the heartbeat interval, it carries the **reconnect windows**: storm wait (default 0–20s), local trigger (default 0–1s), pre-handshake retry (default 0.5–1.5s, R5S-O4) and other retryable rejects (default 1–3s). Fields are optional; an absent field keeps the SDK's current value.
- *Client SDK rules:* the SDK **persists** the last windows it received (next to `device_id`/`conn_seq`) and uses them for every later wait, including before the next login. It **clamps** each value to a safe range built into the app (ranges fixed in the plan; e.g. storm 5–120s, local 0–10s, retries 0.25–10s), so a bad config can't produce a zero-jitter storm or an unbounded wait. Fresh installs use the built-in defaults.
- *Success criteria, Storm SLO:* note that the 30s SLO assumes the default storm window (0–20s); widening it is a deliberate trade of relogin time for load.
- *Tests:*
  - SDK unit tests: windows from `LoginResp` are used and persisted across restarts; out-of-range values are clamped; absent fields keep the current value;
  - Tier 2 scenario *whole-population reconnect*: drop every connection on all gateways at once (e.g. restart the local LB, or RST from loadgen's side in one instant); measure, with no SLO, time until 99% are logged in again, the share of clients reaching exponential backoff, and confirm that heartbeats of already-reconnected clients are answered on time and no component restarts;
  - the same scenario with the local-trigger cause (all clients raise a network-change event at once).

---

## R5S-O7 — production node and LB limits are unspecified — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: a *Production node and LB requirements* list under *Deployment*, plus a gateway startup self-check of the limits it can see (`RLIMIT_NOFILE`, `nf_conntrack_max`) that withholds Readiness when they are too low for the configured cap. Options B (list only) and C (an ops runbook outside the spec) were rejected.

**Claim.** `nofile`, conntrack and source-IP limits are set only for the Tier 2 test host (*Verification*). Production nodes and the cloud LB have the same limits: the `nf_conntrack_max` default (131k–262k depending on node memory) is at or below the 250k cap; the container's `nofile` must exceed the cap; the LB must hold ≥ 1.25M concurrent connections and the storm's new-connection rate; the LB idle timeout must exceed the 150s heartbeat (Azure's default is 240s). A miss shows up as unexplained drops at scale.

**Rationale.** These limits are tied to the cap, so they belong next to it in the spec, and the two the gateway can read cheaply should be enforced the same way the timing-profile ratios already are: a misconfigured node fails visibly at rollout, not under load. LB limits can't be read from the pod, so they stay a documented requirement checked by the Tier 3 `cloud` run. C lets the cap and the limits drift apart.

**Spec changes to apply:**
- *Deployment:* add **Production node and LB requirements**, each derived from the configured cap (`maxConnsPerGateway`):
  - container `nofile` (`RLIMIT_NOFILE`) ≥ cap + headroom (default headroom 10%, for gRPC, the pending set and closing sockets);
  - per gateway node, `nf_conntrack_max` ≥ 2 × cap, **or** conntrack disabled for the gateway port (`NOTRACK` / CNI equivalent), declared in the gateway config as `conntrack: notrack`;
  - `net.core.somaxconn` and the gnet listen backlog large enough for the per-gateway accept burst (≥ 4096; sized in load test (b1));
  - cloud LB: concurrent connections ≥ the fleet's total cap (6 × 250k = 1.5M by default; ≥ 1.25M for N−1), and new connections/s ≥ the whole-population reconnect rate it must shape (R5S-O6), not just the 20k/s storm peak;
  - LB idle timeout > heartbeat interval with margin (default 150s): a live client sends a frame at least every interval, so the LB never sees it idle for longer. Where the LB's timeout is fixed and lower, the heartbeat interval in `LoginResp` is lowered to fit.
- *Readiness:* add a **limits self-check** at startup: the gateway reads `RLIMIT_NOFILE` and, unless configured `conntrack: notrack`, `/proc/sys/net/netfilter/nf_conntrack_max`. If either is below the requirement for the configured cap, the gateway stays NotReady and logs which limit failed and the value it needs. (`nf_conntrack_max` is node-wide; an absent file means conntrack isn't loaded and passes.)
- *Observability:* expose the measured limits as gauges, and the node's `nf_conntrack_count` where readable, alerting at 80% of max.
- *Tests:*
  - Tier 1a unit test: the self-check fails Readiness for a `nofile` or `nf_conntrack_max` below the cap's requirement, passes with `conntrack: notrack`, and passes when the conntrack file is absent;
  - Tier 3 `cloud`: record the LB's configured idle timeout and connection limits in the JSON result, and fail the run if they violate the requirements; `TARGET=local` lists the LB limits as `not_covered`.

---

## R5S-O8 — the `ReconcileEnd` delete phase has no rate limit and its cost isn't counted — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: the delete phase runs under a rate limit like the sweeper's (default ~10k uids/s, configurable), its reads are added to the cost line, and its duration is measured. Options B (count only) and C (one Lua call per uid, as the fix) were rejected; C may still be used as an implementation optimisation.

**Claim.** Each pass's delete phase SSCANs `gwconns:gw:{epoch}` and reads `sess:{uid}` for every uid (~200k per pass per gateway), roughly doubling the stated ~330 Lua calls/s, and runs on one Logic node with no rate limit, so it can burst onto every shard once per pass.

**Rationale.** The sweeper already rate-limits the same kind of walk for the same reason; reusing that limit is the smallest fix and makes the cost line true. A rate-limited phase takes longer (~20s at 200k), which is harmless: ghosts are bounded by the Reconcile cycle (~10 min), not by seconds.

**Spec changes to apply:**
- *Logic, Reconcile handling, Delete phase:* runs under a configurable rate limit (default ~10k uids/s, the sweeper's rate), ~20s per pass at 200k. `ReconcileEnd` replies after the delete phase completes; the gateway's `ReconcileEnd` RPC deadline is set to cover it (≥ tracked uids ÷ rate + margin). If the Logic node dies mid-phase, the RPC fails and the gateway's existing retry (R3-OP4) resends the same payload; the compare-and-deletes are idempotent, so a partial first run is safe.
- *Cost line:* replace "200k entries every 10 min is ~330 Lua calls/s per gateway, ~2k/s cluster-wide" with: Reconcile writes ~330 Lua calls/s plus the delete phase's ~330 reads/s (SSCAN + `sess:` read, and a compare-and-delete only for ghosts) ≈ **~660 Redis ops/s per gateway, ~4k/s cluster-wide** at 6 gateways, periodic pace; the delete phase is capped at ~10k uids/s whatever the pace.
- *Observability:* delete-phase duration and uids scanned per pass, by pace.
- *Tests:* Logic unit test: a delete phase over N uids takes ≥ N ÷ rate and deletes the same fields as an unthrottled run; a `ReconcileEnd` interrupted mid-phase and retried leaves the same result as one uninterrupted run.

---

## R5S-O9 — Redis zone placement is unstated — DECIDED: ACCEPT (option A)

> **Decision (2026-10-07):** option A accepted: state the Redis Cluster placement (≥ 3 masters over ≥ 3 zones, each replica in a different zone from its master; for a managed service, its multi-zone setting) and add a Redis zone-loss row to the failure table. Options B (one sentence, placement left to ops) and C (A plus two replicas per master) were rejected.

**Claim.** Login availability equals Redis Cluster availability, and Revoke's `WAIT 1` needs a reachable replica on every shard, but the spec doesn't say how masters and replicas are placed across zones. A zone loss that takes a majority of masters stops cluster failover; a shard whose only replica is in the lost zone makes Revoke return retryable errors.

**Rationale.** The placement rule is one line and turns the zone-loss behaviour from implicit to stated. C doubles replica memory to keep Revoke error-free during a zone loss, although the account service already retries `Revoke` until it succeeds (R2-O4).

**Spec changes to apply:**
- *Deployment:* Redis Cluster has **≥ 3 masters spread over ≥ 3 zones, and each master's replica in a different zone from its master**, so one zone loss never removes a majority of masters and every master has a replica outside its own zone. For a managed Redis, the equivalent multi-zone setting is named in the deployment config.
- *Failure table:* add **Zone loss (Redis)**: masters in the lost zone fail over to their replicas in the surviving zones. Logins on those shards stall or come back `EXPIRED` for the failover time; logins on other shards are unaffected (R5S-O1). Lost writes are repaired as for a shard failover. Revoke may return retryable errors for shards whose replica was in the lost zone until a replica is rebuilt; the account service keeps retrying. The lease TTL rule (TTL > failover time + one renewal) already covers lease keys.
- *Tests:* Tier 2 Redis failover test: additionally fail over every master of one zone at once (the shape of a zone loss: one master with 3 masters, two with 6), together with the replicas in that zone, and record failover time and the share of logins affected; the lease must survive at the configured TTL.

---

## Follow-up (2026-10-08) — decisions from the Plan 1 interface review

These come from the user's review of the session store interfaces (Plan 1), not from a new review round. Refs *PI-<n>* are the review's decision points. Only PI-4 changes the spec; the others change code and are recorded in Plan 1's *Execution Notes*. **Not yet applied to the rev6 spec**: apply when the user asks (in place or in rev7).

### PI-4 — `Revoke` takes `before_ms` from the account service — DECIDED: ACCEPT (option A)

> **Decision (2026-10-08):** option A accepted: the account service supplies `before_ms` on the `Revoke` RPC. Option B (Logic stamps Redis `TIME`) was rejected.

**Claim.** The marker is compared with `tok_iat`, which the account service's clock stamps. A marker stamped by Logic or Redis is on a different clock: if it runs ahead, a token issued right after a password change is rejected as `REVOKED` (the user is sent back to the login screen until the skew passes); if it runs behind, tokens issued in the skew window survive. Each retry of a failed `Revoke` would also stamp a later time, and `max()` keeps it, so a token issued between the first attempt and the successful retry is revoked by mistake.

**Rationale.** Taking the time from the issuer makes the marker mean exactly "tokens this issuer issued before X", whatever the skew, and makes a retry resend the same value, so `Revoke` is truly idempotent.

**Spec changes to apply:**
- *Logic, RPCs and Revoke:* `Revoke(uid, scope = all | slot, before_ms, reason)`. `before_ms` is on the token issuer's clock, in the unit of `tok_iat` (ms); the account service sets it when it decides the revocation and resends the same value on every retry. The marker write is `revoked_before_ms = max(stored, before_ms)`.
- *Logic, Revoke validation:* reject `before_ms ≤ 0` or more than 5 minutes ahead of Redis `TIME` (`INVALID_ARGUMENT`, not retryable); this catches a seconds-vs-ms bug.
- *Tests:* a retried `Revoke` with the same `before_ms` leaves the marker unchanged; a token whose `iat` is ≥ `before_ms` logs in after the revoke; `before_ms` 10 minutes in the future is rejected.
