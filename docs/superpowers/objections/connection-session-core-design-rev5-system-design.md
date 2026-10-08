---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev5.md
date: 2026-10-07
mode: spec
focus: system-design review — scale, availability, blast radius, Kubernetes and cloud deployment behaviour; race logic deliberately not re-reviewed
reviewer_model: claude-opus-5-5
objections:
  - id: O1
    category: risk
    severity: high
    blocking: true
    claim: "A Login batch waits for its slowest Redis shard, so one shard's failover stalls every batch and turns a 1/N outage into a login outage for all users."
    evidence: "\"Login batching: up to 200 logins or 5ms per RPC\" / \"run the Lua login script (EVALSHA, pipelined)\" / failure table, Redis shard failover: only lost writes are listed"
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: risk
    severity: high
    blocking: true
    claim: "Kicks to a dead or unreachable epoch are on the storm path with no per-attempt deadline, no bound, and no statement that the Login reply doesn't wait for them; on node loss each can block ~20s in gRPC connect."
    evidence: "\"then do the follow-ups: SADD to the epoch index; if the result needs a kick, send it with the hardened Kick below\" / \"The first attempt is always sent, even when the target's lease looks expired\" / \"Kick RPCs are fail-fast (no WaitForReady)\""
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: risk
    severity: medium
    blocking: false
    claim: "A StatefulSet does not reschedule a pod from an unreachable node, so after a node loss the fleet stays at N−1 until someone force-deletes it; a second loss in that window is a no-SLO multi-gateway event."
    evidence: "\"Gateways run as a StatefulSet behind an L4 NLB ... so a single node failure is a true N−1 event\" / \"Losing several gateways at once ... is degraded, best effort, with no SLO\" / race 4: \"Zombie gateway with the same pod name: leases are keyed per epoch\""
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: risk
    severity: medium
    blocking: false
    claim: "A rolling restart behind a load-blind (flow-hash) LB drives the pods not yet restarted into the connection cap; with 3 of 5 targets full, ~7.8% of overflow clients exhaust 5 retryable rejects and drop into exponential backoff, past the SLO."
    evidence: "\"Each attempt uses a new source port, so the NLB flow hash can pick another gateway\" / \"Skew below the cap is harmless; active rebalancing is out of scope\" / \"After 5 retryable rejects in a row, fall back to exponential backoff (cap 5m)\" / Tier 3: \"rolling restart of all gateways at 1M total ... clients rejected at the cap log in elsewhere within the SLO\""
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: risk
    severity: medium
    blocking: false
    claim: "No PodDisruptionBudget is specified, so a routine node-pool upgrade or autoscaler drain can evict several gateways at once, which is the no-SLO zone-loss behaviour caused by normal operations."
    evidence: "Deployment row: StatefulSet with topologySpreadConstraints, no PDB / Graceful drain: \"terminationGracePeriodSeconds must be longer than the drain window\""
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: assumption
    severity: medium
    blocking: false
    claim: "Local network-change triggers are assumed uncorrelated, but a carrier or city network recovery fires them for millions of devices at once and bypasses the 0–20s storm jitter; a whole-population reconnect is not a named failure class, and the jitter windows can only be changed by an app release."
    evidence: "\"These events are uncorrelated across users, so there is no storm to spread\" / failure classes cover one gateway, and several gateways (zone) only"
    disposition: pending
    disposition_rationale: null
  - id: O7
    category: specification
    severity: medium
    blocking: false
    claim: "Kernel and LB limits are specified only for the Tier 2 test host; production nodes and the cloud LB have the same limits (conntrack, nofile, LB concurrent connections and CPS, LB idle timeout)."
    evidence: "\"Host prep provides ≥ 10 source IPs ... and sets nr_open/ulimit and conntrack (raised, or disabled on the test interface)\" — Verification section only"
    disposition: pending
    disposition_rationale: null
  - id: O8
    category: specification
    severity: low
    blocking: false
    claim: "The ReconcileEnd delete phase has no rate limit and its read cost (SSCAN plus a sess: read for every uid) is not in the cost line."
    evidence: "\"the Logic node that receives ReconcileEnd SSCANs gwconns:gw:{epoch}. For each uid it reads sess:{uid} ...\" / \"Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway\""
    disposition: pending
    disposition_rationale: null
  - id: O9
    category: specification
    severity: low
    blocking: false
    claim: "Login availability equals Redis Cluster availability, and Revoke needs a live replica, but the spec does not state how masters and replicas are placed across zones."
    evidence: "\"Redis down | New logins fail\" / Revoke: \"WAIT 1 <timeout>. If no replica confirms, return a retryable error\""
    disposition: pending
    disposition_rationale: null
---

# Objection record: Connection & Session Core, revision 5 (system-design review)

Scope: how rev5 behaves at scale and on real Kubernetes and cloud infrastructure: dependency blast radius, storm paths, scheduling, load balancing and host limits. Four earlier rounds covered the race logic (fences, Reconcile rules, sweeper guard), so this round does not re-review it. Suggested citation prefix for dispositions: *R5S-O<n>*.

## O1 — risk — high — BLOCKING

### Claim

A Login batch waits for its slowest Redis shard, so one shard's failover stalls every batch and turns a 1/N outage into a login outage for all users.

### Evidence

> **Login batching:** up to 200 logins or 5ms per RPC.

> run the Lua login script (EVALSHA, pipelined)

> Redis shard failover | Up to ~1s of acknowledged writes may be lost …

### Why this matters

200 random uids land on every shard. go-redis `ClusterPipeline.Exec` runs one pipeline per node and returns only when all have finished, so a batch takes as long as its slowest shard. During one shard's failover (seconds, up to `cluster-node-timeout`), every batch includes that shard and every batch stalls. Past `login_deadline` (5s), all logins in flight come back `EXPIRED` or wait out fence (b), not just the ~1/N whose uid is on the failing shard. A Reconcile chunk has the same shape, but there it only means re-queued entries.

### Suggested resolution

- Logic returns each Login entry as soon as its own script finishes. Either the Login RPC streams per-entry results, or Logic groups entries by node and answers each group separately. The gateway already handles each entry on its own (fence (a) is per connection).
- Tier 2 test: during a single-shard failover, logins for uids on the other shards keep their normal p99.

## O2 — risk — high — BLOCKING

### Claim

Kicks to a dead or unreachable epoch are on the storm path with no per-attempt deadline and no bound. The spec also doesn't say whether the Login reply waits for them. On node loss, each Kick can block ~20s in gRPC connect.

### Evidence

> **Login:** verify the JWT locally …, then run the Lua login script …, then do the follow-ups: SADD to the epoch index; if the result needs a kick, send it with the hardened Kick below.

> The **first attempt is always sent**, even when the target's lease looks expired

> Kick RPCs are **fail-fast** (no `WaitForReady`).

### Why this matters

In load test (b2), node loss, the dead pod stays in Endpoints until the node controller evicts it (see O3), so `gw-17.gateway.im.svc` still resolves to an IP that never answers. grpc-go's minimum connect timeout is 20s. A non-`WaitForReady` RPC fails fast only in TRANSIENT_FAILURE and still blocks while the channel is CONNECTING. That leaves ~200k Kick RPCs (one per relogin) blocked in Logic for up to ~20s each.

- If the Login reply waits for the Kick, the 30s SLO fails.
- If it doesn't, the cost is ~200k goroutines plus their buffers across the Logic pods during the storm.

### Suggested resolution

State three rules:
- The Kick is sent after the Login reply and never delays it.
- Every Kick attempt has its own deadline (~1s).
- Each target `gw|epoch` gets a bounded Kick queue. When it is full, the attempt is dropped and counted, with Reconcile as the backstop. A first attempt to an epoch whose cached lease is dead may also be dropped when the queue is full.

## O3 — risk — medium

### Claim

A StatefulSet does not reschedule a pod from an unreachable node. After a node loss the fleet stays at N−1 until someone force-deletes the pod. A second loss in that window is a no-SLO multi-gateway event.

### Evidence

> Gateways run as a StatefulSet … so a single node failure is a true N−1 event

> Losing several gateways at once (e.g. a zone) is **degraded, best effort, with no SLO**

> **Zombie gateway with the same pod name:** leases are keyed per epoch, so the instances don't conflict

### Why this matters

To guarantee at most one pod per name, the StatefulSet controller leaves a pod on an unreachable node in Terminating/Unknown. It waits until the kubelet confirms the pod is gone, an operator force-deletes it, or the node gets the `node.kubernetes.io/out-of-service` taint. Until then `gw-k` is not recreated, and the N−1 margin is used up for an open-ended time (hours if nobody is paged). The design doesn't need the at-most-one guarantee: race 4 already makes a zombie with the same name harmless. The stuck pod's Endpoint is also what keeps Kicks going to a dead IP (O2).

### Suggested resolution

Add an operational rule: when a gateway's node has been NotReady for longer than X (e.g. 2 min), automation applies the `out-of-service` taint or force-deletes the pod. Record time-to-replacement after node loss as a recovery target, and add it to the Tier 3 `cloud` scenario.

## O4 — risk — medium

### Claim

A rolling restart behind a load-blind (flow-hash) LB pushes the pods not yet restarted into the connection cap. Some rejected clients then run out of retryable rejects and drop into exponential backoff, past the SLO.

### Evidence

> Each attempt uses a new source port, so the NLB flow hash can pick another gateway

> Skew below the cap is harmless; active rebalancing is out of scope (sub-project 5).

> After **5 retryable rejects in a row**, fall back to exponential backoff (cap 5m).

### Why this matters

Starting from 6 × 167k at 1M, with drained clients spread evenly over the Ready pods:

| Step | p0 | p1 | p2 | p3–p5 |
|---|---|---|---|---|
| drain p0 | 0 (restarted) | 200k | 200k | 200k |
| drain p1 | 40k | 0 (restarted) | 240k | 240k |
| drain p2 | 88k | 48k | 0 (restarted) | 288k wanted → **capped at 250k** |

From step 3 on, 3 of the 5 targets are full, so each retry is rejected with probability 0.6. About 0.6⁵ ≈ 7.8% of the ~114k overflow clients (~9k) run through 5 rejects in a row and fall into exponential backoff. Rolling in reverse ordinal order is symmetric and changes nothing. The Tier 3 rolling-restart test would catch this, but the design gives no remedy.

### Suggested resolution

- Where the cloud LB supports least-connections (Alibaba CLB/NLB `wlc`, Tencent CLB `least_conn`), require it. That also fixes the empty pod after any restart.
- Where it only flow-hashes (AWS NLB), keep `at_capacity` closes out of the 5-in-a-row limit, or give them a higher limit, since they cost the gateway no crypto. Tighter retries with 0.5–1.5s waits are also an option.

## O5 — risk — medium

### Claim

No PodDisruptionBudget is specified, so a node-pool upgrade or a cluster-autoscaler drain can evict several gateways at once.

### Evidence

The Deployment row specifies a StatefulSet, topology spread, and `terminationGracePeriodSeconds` longer than the drain window. It specifies no PDB.

### Why this matters

With one gateway per node, a managed node-pool upgrade that drains several nodes in parallel produces the zone-loss behaviour (no SLO, survivors at the cap) from routine operations.

### Suggested resolution

- Gateway PDB `maxUnavailable: 1`, plus `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`.
- A Logic PDB that keeps at least `minReplicas` Ready, since the SLO assumes they are.

## O6 — assumption — medium

### Claim

The spec assumes local network-change triggers are uncorrelated. A carrier or city network recovery fires them for millions of devices at once and bypasses the 0–20s storm jitter. A whole-population reconnect is not a named failure class, and the jitter windows can only be changed by an app release.

### Evidence

> **Local trigger** *(R4H-O3)*: … attempt within a uniform random **0–1s**. These events are uncorrelated across users, so there is no storm to spread.

The failure classes cover one gateway, and several gateways (zone) only.

### Why this matters

Admission control still protects the gateways: the handshake bucket closes the excess, and each close is a retryable reject. The cost is a burst of rejects and a backoff tail of up to 5 min for clients that fail 5 times in a row. A full reconnect of all users (LB failover, a regional network blip) has the same shape: 1M in 20s is ~50k/s against 20k/s of headroom, with no stated recovery expectation.

### Suggested resolution

- Name *whole-population reconnect* as a best-effort class with an expected recovery time (≈ users ÷ admitted login rate, plus the backoff tail), and add a Tier 2 scenario.
- Send the jitter windows (storm 20s, local 0–1s, retryable 1–3s) in `LoginResp` with SDK defaults, so ops can widen them without an app release.

## O7 — specification — medium

### Claim

Kernel and LB limits are specified only for the Tier 2 test host. Production nodes and the cloud LB have the same limits.

### Evidence

> Host prep provides ≥ 10 source IPs for loadgen at 600k … and sets `nr_open`/ulimit and conntrack (raised, or disabled on the test interface).

This appears only in *Verification*.

### Why this matters

- `nf_conntrack_max` defaults (often 131k–262k, depending on node memory) sit at or below the 250k cap. Kube-proxy and the CNI track gateway traffic.
- The container's `nofile` must exceed the cap.
- The cloud LB must support ≥ 1.25M concurrent connections and the storm's new-connection rate. Some LB tiers are sized on exactly these figures.
- The LB idle timeout must exceed the 150s heartbeat (Azure's default is 240s).

### Suggested resolution

Add a *Production node and LB requirements* list under *Deployment*:
- nofile;
- `nf_conntrack_max` (or NOTRACK for the gateway port);
- somaxconn / accept backlog;
- LB concurrent connections and new connections/s;
- `heartbeat interval < LB idle timeout`.

## O8 — specification — low

### Claim / Evidence

> the Logic node that receives `ReconcileEnd` SSCANs `gwconns:gw:{epoch}`. For each uid it reads `sess:{uid}` …

> Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway

### Why this matters

Each pass also scans ~200k uids and reads their `sess:` hashes. That roughly doubles the stated cost. With no rate limit it runs as a burst on one Logic node and on every shard.

### Suggested resolution

Rate-limit the delete phase like the sweeper (about 10k users/s), and add its reads to the cost line.

## O9 — specification — low

### Claim / Evidence

> Redis down | New logins fail; existing connections stay up.

> `WAIT 1 <timeout>`. If no replica confirms, return a retryable error

### Why this matters

This is a reasonable choice, but zone placement decides how far it reaches. A zone loss must not take a majority of masters (cluster failover needs a master majority), and Revoke stays available only while every shard has a reachable replica.

### Suggested resolution

State the placement: masters spread over ≥ 3 zones, with each master's replica in a different zone. For a managed service, name the equivalent setting.

## Explicitly not objecting to

- **Fences (a)–(c), Reconcile rules 1–4, the delete-phase safety argument, the sweeper and its mass-stale guard:** covered by rounds 1–4. This round reviewed only their cost and dependencies (O1, O8).
- **Noise NK instead of TLS:** the reasons (gnet has no TLS, plaintext between NLB and pod, LB limits and pricing) hold.
- **Density arithmetic and the N−1 sizing:** 5 × 250k ≥ 1M and 200k / 20s = 10k/s check out. O4 is about uneven spread, not total capacity.
- **Clock contract and skew measurement:** sound, and measured on the right path.
- **gRPC `MaxConnectionAge` for DNS re-resolution after a Logic HPA scale-up:** correct and needed.
- **`gwconns` and `gwepochs` hot keys:** already measured in Tier 2 (R4M-9).

## Verdict

**Ready for plan 2 once O1 and O2 are decided.** Both change the Logic and Login RPC interfaces that plan 2 will be written against: per-entry Login results, and asynchronous bounded Kicks. O3–O7 are deployment and operations rules that can be added before plan 6 (Tier 2/3). O8 and O9 are one-line additions.
