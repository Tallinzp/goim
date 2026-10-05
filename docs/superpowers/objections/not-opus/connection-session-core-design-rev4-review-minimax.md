# Connection & Session Core Design — Rev4 Review

## Overall Assessment

This is a serious production-grade design, not a toy architecture.

The core session ownership model, gateway epoch model, delayed Logout handling, reconnect handling, and Reconcile design are strong.

I would **not freeze Rev4 yet**. There are **3 high-priority issues**, several medium-priority issues, and a few places where the specification's wording is stronger than what the implementation can actually guarantee.

### Overall Score

| Area | Assessment |
|---|---|
| Connection architecture | 🟢 Strong |
| Gateway ownership model | 🟢 Strong |
| Delayed Logout race | 🟢 Very strong |
| Reconnect race handling | 🟢 Strong |
| Reconcile design | 🟢 Strong but complex |
| Gateway epoch/lease model | 🟡 Good, some atomicity gaps |
| Revocation | 🔴 **Serious bug** |
| Time/order semantics | 🟡 Needs tightening |
| Storm recovery | 🟢 Plausible, must benchmark |
| Redis failure model | 🟡 Some assumptions need qualification |
| Operational complexity | 🟡 High |
| Test plan | 🟢 Excellent |

---

# 1. 🔴 Critical: `rev:{uid}` TTL Can Let a Revoked Connection Come Back

This is the biggest problem.

The current design says:

- `rev:{uid}` TTL = max token lifetime + margin
- Token expiry does not affect a connection that is already authenticated.

Those two decisions conflict.

### Failure scenario

```text
t=0       user logs in with token T
t=10m     account service revokes user
t=10m     revocation marker is created
t=10m     sess field is deleted
t=10m     Kick is lost
```

Reconcile correctly sees:

```text
tok_iat < revoked_before_ms
        ↓
      REVOKED
        ↓
do not restore
```

But later:

```text
t=2h     revocation marker expires
t=2h     gateway connection is still alive
t=2h     Reconcile runs
```

The original token was issued at `t=0`, and the design explicitly says an authenticated connection can remain valid after token expiry.

Now:

```text
revocation marker = gone
sess = gone

Reconcile sees live gateway connection
        ↓
RESTORE SESSION
```

The revoked connection has been resurrected.

This is a real security/correctness bug.

## Fix

Use one of these models.

### Option A — Long-lived revocation marker

Make:

```text
revocation marker TTL >= maximum possible authenticated connection lifetime
```

If connections can live indefinitely, the marker cannot have a finite TTL.

### Option B — Recommended: bounded authenticated connection lifetime

Define:

```text
access token lifetime = X
maximum authenticated connection lifetime = Y
revocation marker TTL >= Y + safety margin
```

For example:

```text
access token = 1h
authenticated connection max lifetime = 24h
revocation marker retention >= 24h + margin
```

The gateway must force reauthentication when the maximum connection lifetime expires.

This gives a clean security contract:

```text
token lifetime
      ↓
connection authentication lifetime
      ↓
revocation marker retention
```

### Recommendation

**Use Option B.**

The three lifetimes need to be explicitly connected in the specification.

---

# 2. 🔴 Reconcile vs Lease Is Not Atomic

The specification already identifies this as an open point.

The current sequence is approximately:

```text
Logic:
    check gwlease
          ↓
    observe lease = LIVE
          ↓
    CAS sess
```

But:

```text
T0: Logic checks lease → LIVE
T1: lease expires
T2: gateway is dead
T3: Reconcile CAS writes old gateway session
```

The CAS cannot atomically verify the gateway lease because:

```text
sess:{uid}
```

and

```text
gwlease:gw:{epoch}
```

may be in different Redis Cluster slots.

## Is this catastrophic?

No.

The existing design can remain eventually correct because:

```text
session restored
      ↓
lease absent
      ↓
session becomes invalid
      ↓
gateway renews → missing
      ↓
triggered Reconcile
```

The session ordering also prevents an old dead connection from defeating a newer login.

## Required specification change

Do not claim:

> Reconcile only writes if the gateway is alive.

That is not strictly guaranteed.

Use this wording instead:

> Reconcile may temporarily restore an entry based on a previously observed live lease. The lease remains the authoritative liveness predicate, and loss of the lease triggers reconciliation.

This accurately describes the system.

---

# 3. 🔴 `gwepochs` and `gwlease` Are Not Atomic

The design currently performs operations similar to:

```text
SET gwlease... PX ...
ZADD gwepochs ...
```

as separate operations.

Possible failure:

```text
SET gwlease succeeds
ZADD fails
```

Now the gateway is actually alive but:

```text
gwepochs
```

contains stale information.

The sweeper may incorrectly identify the gateway epoch as stale.

The recovery mechanism already exists:

```text
gateway renews
    ↓
gwepochs entry missing/stale
    ↓
trigger Reconcile
```

So this does not necessarily break correctness.

However, it can cause unnecessary cleanup work.

## Recommendation

Explicitly define:

> `gwepochs` is advisory only. A stale ZSET score never constitutes proof that a gateway epoch is dead.

The authoritative liveness check must be:

```text
EXISTS gwlease:gw:{epoch}
```

The sweeper should perform the authoritative lease check before acting.

---

# 4. 🟠 Login Ordering Needs a Precise Definition

The design uses something equivalent to:

```text
login_ms = gateway receive time
```

and uses that ordering to decide which login wins.

Consider clock skew:

```text
G17 clock = +8ms
G42 clock = -8ms
```

Actual event order:

```text
G42 receives login first
G17 receives login second
```

Possible timestamps:

```text
G42 = 10,000
G17 = 9,992
```

The system could conclude the earlier login is newer.

This matters because `login_ms` participates in correctness.

## Choose the intended semantics

### Semantics A

> The login that reaches Redis first wins.

This is easier to reason about in a distributed system.

### Semantics B

> The login that reaches the gateway first wins.

This requires gateway timestamps and bounded clock skew.

## Recommendation

Prefer **Semantics A**.

Define login ordering using the serialization point at Redis.

For example:

```text
Login request
      ↓
Redis Lua
      ↓
server-side ordering
      ↓
session ownership
```

If the actual product semantics require gateway receive time, keep `recv_ms`, but explicitly define the tolerated clock skew and monitor it.

---

# 5. 🟠 `recv_ms + login_deadline` Is Good, But Its Clock Assumption Must Be Explicit

The Login/Logout fence is one of the stronger parts of the design.

The basic idea is:

```text
gateway receives Login
        ↓
recv_ms
        ↓
Redis Lua rejects writes that are too late
        ↓
gateway Logout fence waits
        ↓
old Logout cannot race a possible Login write
```

This is good.

However, if Redis time and gateway wall-clock time are compared, the specification needs an explicit clock-skew contract.

Define something like:

```text
|gateway_clock - redis_clock| <= allowed_skew
```

and monitor violations.

A bad NTP condition should become an observable operational problem rather than an invisible correctness assumption.

---

# 6. 🟠 150-Second Heartbeat Is a Product/Operational Tradeoff

Current behavior is approximately:

```text
heartbeat = 150s
client dead detection ≈ 160s
gateway dead detection ≈ 310s
```

This is a long failure-detection interval for an IM system.

A dead network connection can remain logically connected for almost three minutes.

However, the reason for the long heartbeat is understandable:

```text
1M connections × heartbeat frequency
```

creates significant CPU/network/battery cost.

This is not a correctness problem.

It should be benchmarked as a configurable parameter:

```text
30s
60s
90s
150s
```

Measure:

- heartbeat CPU
- network bandwidth
- mobile battery impact
- NAT timeout behavior
- failure detection latency
- reconnect storms

The existing reconnect-after-idle metric is exactly the right metric to include.

---

# 7. 🟠 Graceful Drain Needs Explicit NLB Testing

The design has:

```text
SIGTERM
  ↓
NotReady
  ↓
stop accepting new traffic
  ↓
Reconnect with jitter
  ↓
close remaining connections
```

This is good.

But there are actually multiple traffic paths:

```text
Internet → NLB → Gateway
Logic → Gateway → Kick
Gateway → Logic
```

The NLB and internal service-discovery behavior need to be tested independently.

Acceptance test:

```text
Gateway pod becomes NotReady
        ↓
NLB stops assigning new traffic
        ↓
Logic can still resolve the draining gateway
        ↓
Kick still works
        ↓
existing clients receive Reconnect
```

Make this a dedicated integration test.

---

# 8. 🟠 The 250k Connection Cap Should Be Measured, Not Architectural

The current benchmark targets something like:

```text
600k = benchmark ceiling
200k = operating point
250k = hard cap
```

That is a reasonable initial configuration.

But the cap should ultimately be derived from measured SLO/resource limits.

A better definition is:

```text
connection_cap =
    min(
        memory_safe_limit,
        CPU_safe_limit,
        GC_safe_limit,
        reconnect_storm_safe_limit,
        latency_SLO_limit
    )
```

So:

```text
250k
```

should be the initial configured value, not an architectural constant.

---

# 9. 🟡 `gwconns` Has a Potential Single-Shard Hotspot

The design uses:

```text
gwconns:gw:{epoch}
```

as the per-epoch connection index.

That means all members for one gateway epoch are concentrated in one Redis key/shard.

For a reconnect storm this could produce something like:

```text
~10k SADD/s
```

for one gateway.

This may be completely fine.

But it needs an actual benchmark.

Test at:

```text
200k members
500k members
1M members
```

Measure:

- Redis CPU
- command latency
- `SADD` throughput
- `SSCAN` throughput
- deletion throughput
- Reconcile duration
- shard hotness

The important measurement is not only global Redis QPS; it is **single-shard CPU and latency**.

---

# 10. 🟡 The State Machine Is Becoming Complex

The design now has many interacting mechanisms:

```text
Login
Logout
Kick
Lease
Reconcile
ReconcileEnd
Sweeper
Revocation
CAS
conn_seq
login_ms
epoch
conn_id
device_id
deadline
tracked set
in_reconcile_chunk
```

This is not inherently bad.

The risk has shifted from architecture to implementation correctness.

## Recommendation: define a small set of formal invariants

For example:

```text
I1. conn_id is never reused.

I2. Only the gateway epoch that owns conn_id may act on it.

I3. Logout can only delete its exact session.

I4. Reconcile cannot overwrite a newer login.

I5. A revoked token can never be restored.

I6. A dead gateway epoch cannot become valid without a new lease.

I7. ReconcileEnd cannot delete a login received after as_of_ms.

I8. A newer ownership record always dominates an older ownership record.

I9. Physical gateway connection state is authoritative for actual TCP existence.

I10. Redis session state is authoritative for logical user ownership.
```

Then map every Lua script and test to one or more invariants.

This will make the implementation substantially easier to maintain.

---

# 11. 🟡 Clarify "Gateway Is Source of Truth" vs Redis

The specification currently uses language that can sound contradictory:

> Gateway is the source of truth for which connections exist.

and:

> `sess:{uid}` is the online mark/source of truth.

These should be separated into three concepts.

## Physical connection truth

```text
Gateway
```

answers:

> Does this TCP connection physically exist?

## Logical ownership truth

```text
sess:{uid}
```

answers:

> Which connection is currently authorized to represent this user slot?

## Gateway liveness truth

```text
gwlease:gw:{epoch}
```

answers:

> Is this gateway epoch currently considered alive?

That three-part model is much clearer.

---

# 12. 🟢 Dropping Generic Session Version Is Correct

I agree with the decision to remove a generic:

```text
session_version
```

You already have:

```text
gateway
epoch
conn_id
device_id
conn_seq
login ordering
```

The important thing is not having a field named `session_version`.

The important thing is having an orderable ownership identity.

Your model:

```text
gateway + epoch + conn_id
```

is actually more useful than:

```text
session_version = 42
```

because it identifies the concrete connection owner.

Keep this decision.

---

# 13. 🟢 Dropping the Distributed Login Lock Is Correct

You do not need a separate:

```text
distributed lock(uid)
```

if Login ownership is already serialized by the Redis Lua mutation of:

```text
sess:{uid}
```

Multiple logins can race:

```text
Login A
Login B
Login C
```

and Redis serializes the actual state transition.

This is preferable to:

```text
lock
 ↓
read
 ↓
modify
 ↓
unlock
```

The data mutation itself is the synchronization point.

Keep this architecture.

---

# 14. 🟢 Dropping the Reverse Mapping Is Correct

The old approach had:

```text
uid → gateway/worker/socket
gateway/worker/socket → uid
```

The new design avoids the reverse Redis mapping.

That is a good simplification.

The gateway already knows:

```text
conn_id → connection → uid
```

and:

```text
gwconns
```

provides the cleanup index.

This avoids maintaining two distributed ownership records that can disagree.

---

# 15. 🟢 Logout Fence Is Excellent

This is one of the strongest parts of the design.

The important race is:

```text
old connection dies
        ↓
Logout delayed
        ↓
new Login writes session
        ↓
old Logout deletes new session
```

Your combination of:

```text
exact-match deletion
+
Login ordering/deadline
+
bounded Logout fence
```

directly addresses this race.

The distinction between:

```text
definitive result
```

and:

```text
deadline expired / uncertain result
```

is especially good.

Keep this architecture.

---

# 16. 🟢 Epoch-Based Gateway Identity Is Correct

Using:

```text
gateway_id = pod name
epoch = random uint64 generated per boot
```

is much safer than assuming the pod name uniquely identifies a process.

Without an epoch:

```text
old g17
    ↓
dies

new g17
    ↓
starts
```

could create zombie identity collisions.

With:

```text
gateway + epoch + conn_id
```

an old process cannot impersonate the new process merely because Kubernetes reused the pod name.

Keep this.

---

# 17. 🟡 Define the Failure-Detection SLO Timestamp

If the SLO says:

```text
99% within 30s of gateway death
```

you need a reproducible definition of `gateway death`.

Possible timestamps include:

```text
kill issued
process exits
container exits
Kubernetes observes failure
NLB observes failure
TCP connections receive RST
```

Pick one.

For example:

```text
Tdeath =
    timestamp immediately before SIGKILL
```

for a controlled benchmark.

Then define:

```text
T_reconnect - Tdeath <= 30s
```

This prevents future disagreement over what the SLO actually measures.

---

# Recommended Revision Priority

## Must fix before implementation freeze

### P0

1. 🔴 Fix revocation-marker expiration and connection lifetime semantics.
2. 🔴 Define the relationship between token lifetime, authenticated connection lifetime, and revocation retention.
3. 🟠 Clarify that Reconcile vs lease is not atomic.
4. 🟠 Define `gwepochs` as advisory rather than authoritative.
5. 🟠 Decide the exact semantics of login ordering.

## Should fix before implementation

6. 🟠 Add explicit clock-skew monitoring/invariants.
7. 🟠 Make the 250k connection cap a measured safety limit.
8. 🟠 Add a dedicated NLB drain acceptance test.
9. 🟡 Benchmark `gwconns` single-shard behavior.
10. 🟡 Write the formal invariant list.

## Keep

11. 🟢 Epoch-based gateway identity.
12. 🟢 Exact-match Logout Lua.
13. 🟢 Logout fence.
14. 🟢 No distributed Login lock.
15. 🟢 No reverse mapping.
16. 🟢 No generic session version.
17. 🟢 Gateway-owned physical connection state.
18. 🟢 Redis logical ownership + reconciliation.
19. 🟢 Separate GatewayLease channel/worker pool.
20. 🟢 Extensive race/integration test plan.

---

# Final Verdict

**Rev4 is close, but I would not freeze it yet.**

I would **not redesign the architecture**.

The core model is good:

```text
                  ┌──────────────┐
clients ─────────►│   Gateway    │
                  │ physical     │
                  │ connection   │
                  └──────┬───────┘
                         │
                       gRPC
                         │
                  ┌──────▼───────┐
                  │    Logic     │
                  │ ownership    │
                  │ arbitration  │
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │    Redis     │
                  │ logical      │
                  │ ownership    │
                  └──────────────┘
```

The most important correction is **revocation**.

Once you fix that and tighten the lease/time/invariant definitions, Rev5 can become an **implementation-ready production specification** rather than another architecture draft.

## Bottom line

> **Do not throw away Rev4. Fix the proof around it.**

The architecture is fundamentally sound; the remaining work is mostly closing correctness gaps and making the distributed-system invariants explicit.
