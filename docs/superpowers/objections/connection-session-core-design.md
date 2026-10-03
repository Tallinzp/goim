---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design.md
date: 2026-10-03
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: premise
    severity: high
    claim: "The 600k-connections-per-gateway target is inherited from the earlier C++ system rather than derived from this deployment, and it sets both the hardest SLO and the most fragile mechanisms in the spec."
    evidence: "\"The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway).\" / \"One gateway holds 600k connections\" / \"When a full gateway (600k) dies, 99% of users are logged in again within 30s\" / \"Capacity: N−1 gateways must absorb one dead gateway's connections.\""
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: risk
    severity: critical
    claim: "Gateway liveness is inferred only from lease renewals that pass through Logic and Redis, so a Logic overload or a Redis outage longer than 60s makes the sweeper delete the sessions of healthy gateways, and nothing restores them."
    evidence: "\"Lease: call `GatewayLease` every 10s.\" / \"`gwlease:gw:{epoch}` ... `PX 30000`\" / \"Find `gwepochs` members whose last renewal is more than 60s old ... for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch.\" / \"Redis down | ... Lease SET resumes when Redis returns, and the entries become valid again.\""
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: alternatives
    severity: high
    claim: "The spec builds five separate partial cleanup mechanisms for stale Redis state, but never considers the one source that actually knows which connections exist, the gateway, reporting its state."
    evidence: "\"`gwconns:gw:{epoch}` | SET | uids | cleanup hint\" / \"Safety net: the 30-day TTL ... Gateways refresh the TTL for connections older than 15 days by sending a slow batch RPC\" / \"Logout goes into a bounded retry queue for 60s, then is dropped (the sweeper and lazy repair cover it)\" / \"the stale entry gets repaired lazily (in sub-project 3 ...)\""
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: implementation
    severity: high
    claim: "Across devices the login table resolves conflicts by Redis arrival order, so a delayed login from an abandoned device overrides the newer device's session and tells it not to reconnect, which breaks success criterion 3."
    evidence: "\"different device_id | overwrite (last one wins); kick the old one with `KICKED_BY_OTHER_DEVICE`\" / \"`KICKED_BY_OTHER_DEVICE` (shown to the user; the client must not reconnect automatically)\" / \"No stale event (late disconnect, late login, zombie gateway) can delete or override a newer session\""
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: risk
    severity: high
    claim: "A STALE decision ignores whether the stored entry is still live, so any regression of conn_seq locks a device out of its own slot until the 30-day TTL expires."
    evidence: "\"same device_id, new conn_seq ≤ stored | reject the new login as `STALE`; close it; no write\" / \"Whether the lease is live doesn't change the write decision.\" / \"`sess:{uid}` ... key TTL 30d, refreshed on login\""
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: specification quality
    severity: high
    claim: "The spec never says what a client does after a REPLACED kick, and it assumes one device_id means one client, so two clients sharing a device_id (browser tabs, a second process) can replace each other in a loop."
    evidence: "\"`REPLACED` (same device, newer connection; the user doesn't see it)\" / \"**device_id** (stable per install) and **conn_seq** (counter on the device, incremented on every connection attempt)\" / \"Web: WebSocket over wss ... designed now behind a transport interface\" / Race 7 covers only `KICKED_BY_OTHER_DEVICE`."
    disposition: pending
    disposition_rationale: null
  - id: O7
    category: specification quality
    severity: high
    claim: "The storm SLO has no defined clock, ignores how clients detect a gateway that dies silently, and the client's own reconnect jitter leaves no margin to meet it."
    evidence: "\"99% of users are logged in again within 30s\" / \"Client SDK: wait a random 0–30s before reconnecting after an unexpected close\" / \"Gateway crash | The lease expires within 30s; clients reconnect\" / \"(b) kill a full gateway and check that 99% of users are logged in again within 30s\""
    disposition: pending
    disposition_rationale: null
  - id: O8
    category: risk
    severity: high
    claim: "KICKED_BY_OTHER_DEVICE is presented to users as a security action, but the spec makes it best effort and leaves several paths where the displaced connection stays authenticated and ONLINE."
    evidence: "\"Kick can't reach the gateway | Ignored (best effort).\" / \"Logic checks the lease of the old gateway before sending a Kick\" / \"Kick acts only on connections in the `ONLINE` state.\" / \"Kick dialed by StatefulSet pod DNS\""
    disposition: pending
    disposition_rationale: null
  - id: O9
    category: implementation
    severity: medium
    claim: "Races 4–6 rely on Logout landing after Login, but neither a Login timeout nor a dropped Logout guarantees that, so live-looking ghost entries remain that sub-project 1 never repairs."
    evidence: "\"the gateway sends Logout after the Login reply arrives, so order is kept for each connection\" / \"Login RPC times out but Redis was written: the gateway closes and sends Logout, which is safe because compare-and-delete is idempotent\" / \"Logout ... dropped (the sweeper and lazy repair cover it)\""
    disposition: pending
    disposition_rationale: null
  - id: O10
    category: scope
    severity: medium
    claim: "The spec authenticates a connection once and then never again, yet connections are expected to last for weeks; it does not cover token expiry, revocation, or rotation of the signing key."
    evidence: "\"Logic verifies them locally (no database call on login)\" / \"One login attempt per connection\" / \"Gateways refresh the TTL for connections older than 15 days\" / Out of scope: \"account service\""
    disposition: pending
    disposition_rationale: null
  - id: O11
    category: risk
    severity: medium
    claim: "Both admission controls are aimed at the wrong thing during a storm: the per-IP bucket throttles carrier-NAT users, and the pending cap counts client round-trip time instead of server load."
    evidence: "\"A token bucket for new connections per IP.\" / \"cap the connections in `HANDSHAKE` + `AUTH_PENDING` per gateway (configurable, for example 5k); above the cap, reply `Busy{retry_after}`\" / \"cloud NLB\" / \"wss (TLS terminated at the cloud LB)\""
    disposition: pending
    disposition_rationale: null
  - id: O12
    category: specification quality
    severity: low
    claim: "The Logic RPC list is missing calls that the rest of the spec depends on, and the forced gRPC connection recycling has no grace period for in-flight batches."
    evidence: "\"RPCs: `Login` (batch), `Logout` (batch), `GatewayLease`, `EpochEnded`.\" / \"sending a slow batch RPC to Logic\" / \"**A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`\" / \"Logic server sets `MaxConnectionAge` ≈5m\""
    disposition: pending
    disposition_rationale: null
---

# Objection record — Connection & Session Core

Spec-time review. An independent review already exists at `docs/superpowers/specs/spec-review.md` and reaches several of the same findings. Where they overlap, this record says so. The objections below stand on their own so that each one can get its own disposition. Overlap is a reason to take a finding more seriously. It is not a reason to drop it.

## O1 — premise — high

### Claim

The figure of 600k connections per gateway comes from the user's earlier C++ system. Nothing in this spec derives it from the 1M-user target, the cloud environment, or the blast radius anyone is willing to accept. That one number then drives the hardest requirements in the spec: the size of the storm (600k logins in 30s), the gnet-specific loop-identity scheme, and the GOMEMLIMIT tuning work. The spec's own capacity rule also makes the headline failure scenario unlikely to happen.

### Evidence

> The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway).

> One gateway holds 600k connections (measure CPU / memory / GC).

> When a full gateway (600k) dies, 99% of users are logged in again within 30s

> Capacity: N−1 gateways must absorb one dead gateway's connections.

### Why this matters

For 1M users with a 600k ceiling, the N−1 rule needs at least 3 gateways, which puts each one at about 333k in steady state. A "full 600k gateway dies" event is then outside normal operation. Yet it sets the 20k/s login rate, the 50k/s headroom, and load test (b). If the target were something like 100–150k per pod, a lost pod would mean 4–6× fewer relogins, and the storm SLO might become easy to meet. The SLO is the hardest thing in the spec. The question is whether density is a goal in itself or a habit carried over from bare metal, where boxes were expensive and few. On Kubernetes, pods are cheap and blast radius is the cost that matters. If density is not a goal, much of the storm machinery is solving a problem the design created. If it is a goal, the spec should say why, because the answer decides how much of this design is necessary.

## O2 — risk — critical

### Claim

The spec decides whether a gateway is alive by one test: did its lease reach Redis recently? But the lease reaches Redis only through Logic (gateway → gRPC round_robin → Logic → Redis). So any condition that stops renewals while gateways are healthy is indistinguishable from mass gateway death. Two such conditions:

1. Redis is unavailable for more than 60s, or a failover takes that long.
2. Logic is saturated. The case to worry about is a storm: 20k logins/s on a CPU-bound Deployment whose HPA reacts in minutes, with `GatewayLease` queued behind the Login batches.

After 30s every affected session becomes "invalid". After 60s the sweeper deletes it. The gateway is never told and the connections stay up, but nothing writes them back into Redis. The second trigger is a cascade: the death of one gateway produces the storm, the storm starves the lease RPCs of the other gateways, and their sessions are swept too.

### Evidence

> Lease: call `GatewayLease` every 10s.

> `gwlease:gw:{epoch}` | STRING | `PX 30000`, renewed every 10s with a plain SET

> Find `gwepochs` members whose last renewal is more than 60s old ... for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Then delete the set and the ZSET member.

> Redis down | New logins fail; existing connections stay up. Lease SET resumes when Redis returns, and the entries become valid again.

The last row is true only if the outage is shorter than 60s. Past that, the first sweeper to run after Redis returns finds every score stale and deletes every session.

### Why this matters

This is the only failure mode in the spec that silently erases presence for the whole cluster. It is triggered by ordinary operational events, and the spec's own failure table says it cannot happen. The storm-cascade variant means the system is least safe at exactly the moment the SLO is measured. The spec also gives lease RPCs no priority over Login RPCs. (`spec-review.md` §2 raises the outage variant. The storm-starvation variant is not raised there.)

## O3 — alternatives — high

### Claim

Stale Redis state is handled by five partial mechanisms, each covering a different gap:

- the `gwconns` index,
- the sweeper,
- the 30-day TTL plus a 15-day refresh RPC,
- the 60s Logout retry queue,
- lazy repair deferred to sub-project 3.

The gateway is the only component that actually knows which connections exist. The spec never considers having it periodically report its live set to Logic, which would then reconcile Redis (anti-entropy, or full resync after an epoch is suspected dead). The spec doesn't list this option among its rejected ones, so it was likely never considered.

### Evidence

> `gwconns:gw:{epoch}` | SET | uids | cleanup hint (not atomic with sess, and not needed for correctness)

> Safety net: the 30-day TTL on `sess:{uid}`. Gateways refresh the TTL for connections older than 15 days by sending a slow batch RPC to Logic

> Logout goes into a bounded retry queue for 60s, then is dropped (the sweeper and lazy repair cover it).

> the stale entry gets repaired lazily (in sub-project 3, a push that gets "unknown conn" back leads Logic to compare-and-delete)

### Why this matters

Each mechanism covers one gap and leaves others: O2 (live sessions swept), O9 (ghost entries on live gateways), and lost writes after a Redis failover. Every gap gets its own patch, its own tests, and its own interaction with the other patches. Reconciling against the gateway's state would cover all of these classes with one tested path. It could also replace the TTL-refresh RPC and possibly `gwconns`. The spec's "Dropped" table shows the authors value cutting concepts, but this cut was never weighed.

## O4 — implementation — high

### Claim

Race 2 orders logins by `conn_seq`, and that only works on a single device. Across devices the Lua script has no ordering at all. Whichever login reaches Redis last wins.

Scenario:

1. A login from phone A gets stuck, for example in a 5ms batch behind a slow Logic pod or an RPC retry.
2. The user switches to phone B, and B logs in.
3. A's late login then overwrites B and sends B `KICKED_BY_OTHER_DEVICE`. By design, B does not reconnect.
4. A's gateway has already given up on that connection. Its Logout (race 4) removes A's entry.

The slot ends up empty. B shows "logged in on another device", and that device is not online.

### Evidence

> different device_id | overwrite (last one wins); kick the old one with `KICKED_BY_OTHER_DEVICE`

> `KICKED_BY_OTHER_DEVICE` (shown to the user; the client must not reconnect automatically).

> No stale event (late disconnect, late login, zombie gateway) can delete or override a newer session; covered by automated tests.

### Why this matters

This is a "late login" overriding a newer session, which is exactly what success criterion 3 rules out. It produces the most visible and confusing outcome the protocol has. The race tests ("races 1–5 above") would not catch it, because it isn't in the races list. The spec stores `login_ms` but never uses it to order logins. (`spec-review.md` §5 raises the same finding.)

## O5 — risk — high

### Claim

`STALE` is a permanent rejection with no escape. It compares only `conn_seq` and deliberately ignores whether the stored entry is still alive. If a device's counter ever goes backwards, every login from that device is rejected until the counter climbs past the stored value, or until the 30-day key TTL expires. Because every login refreshes that TTL, the stored value may never expire. Ways the counter can regress:

- the counter is persisted after the connect rather than before it,
- app data is cleared while the device_id survives (for example, the device_id in the iOS Keychain and the counter in UserDefaults),
- a restore from backup,
- an SDK bug.

The stored entry it loses to may even point at a dead epoch.

### Evidence

> same device_id, new conn_seq ≤ stored | reject the new login as `STALE`; close it; no write

> Whether the lease is live doesn't change the write decision.

> **device_id** (stable per install) and **conn_seq** (counter on the device, incremented on every connection attempt)

### Why this matters

A client-side persistence bug becomes a server-enforced lockout that support cannot easily see or clear. The spec doesn't say how durable `conn_seq` must be, how it relates to the lifetime of device_id, or how a client recovers from `STALE`. Recovery would need, for example, a defined error, a way to resync the counter, or a "stored entry is dead, so accept" rule. (`spec-review.md` §6 raises the same finding.)

## O6 — specification quality — high

### Claim

The spec states the client behaviour for `KICKED_BY_OTHER_DEVICE` (do not reconnect) but not for `REPLACED`. It also assumes, without saying so, that one device_id means one client instance at a time. Two clients that share a device_id and a counter, such as two browser tabs on a shared localStorage or two processes on Android, will keep sending higher `conn_seq` values. Each login replaces the other with `REPLACED`. If the replaced client reconnects, which seems to be the intent since "the user doesn't see it", the two loop forever. Race 7 solves this loop for different devices only.

### Evidence

> `REPLACED` (same device, newer connection; the user doesn't see it) and `KICKED_BY_OTHER_DEVICE` (shown to the user; the client must not reconnect automatically).

> **device_id** (stable per install)

> Web: WebSocket over wss ... designed now behind a transport interface, built as a late slice.

> 7. **Two devices kick each other back and forth:** the client must not reconnect automatically after `KICKED_BY_OTHER_DEVICE`.

### Why this matters

Two SDK authors (one for mobile, one for web) will reasonably make different choices here. One of those choices produces a reconnect loop that also generates a stream of Login, Kick, and Logout traffic to Redis. Multiple tabs are the normal case for the web transport, and the spec says this transport is "designed now". So the semantics of a device ID and of `REPLACED` on web need deciding now, not when the slice is built.

## O7 — specification quality — high

### Claim

The 30s storm SLO cannot be checked as written:

- **No clock.** The spec never says whether the 30s starts at gateway death, at the moment the client detects the loss, or at the reconnect.
- **Detection.** A process crash sends RST. Losing a node does not, and a cloud NLB generally does not reset client flows either. Clients then notice only through their own heartbeat (150s interval), and the spec doesn't define how a client decides the server is dead.
- **Jitter.** A uniform 0–30s reconnect jitter means about 1% of clients *start* reconnecting after 29.7s. Even if every login succeeds on the first try, the 99%-within-30s target has no margin. Any `Busy{retry_after}` or backoff makes it fail.
- **Test.** Load test (b) "kills" a gateway. That is presumably the process-crash case, which is the easy one.

### Evidence

> When a full gateway (600k) dies, 99% of users are logged in again within 30s

> Client SDK: wait a random 0–30s before reconnecting after an unexpected close; exponential backoff (cap 5m); obey `retry_after`.

> Gateway crash | The lease expires within 30s; clients reconnect; the sweeper cleans up after 60s.

### Why this matters

The SLO is listed among the success criteria, yet its own client-jitter rule makes it unreachable. Either the implementation will "fail" an SLO it could never meet, or someone will quietly reinterpret it. The spec needs to state which failure classes the SLO covers, where its clock starts, and a jitter window consistent with it. (`spec-review.md` §1 raises the same finding.)

## O8 — risk — high

### Claim

To a user, `KICKED_BY_OTHER_DEVICE` means "your other session has ended". That is the guarantee someone relies on after logging in on a new phone because the old one was lost. But the spec makes the kick best effort, and there are at least four ways the displaced connection stays authenticated and ONLINE:

1. The Kick is undeliverable and "ignored".
2. Logic checks the old gateway's lease, finds it expired (see O2 for how that happens on a live gateway), and skips the Kick.
3. The Kick arrives while the old connection is still `AUTH_PENDING`, and only `ONLINE` connections can be kicked.
4. The gateway is draining and NotReady, so its pod DNS name may not resolve.

In each case the old connection stays logged in. Redis no longer points to it, so it receives no pushes. Nothing in the spec stops it from *sending* as the user once messaging exists.

### Evidence

> Kick can't reach the gateway | Ignored (best effort).

> Redis Lua can't check the lease key (a different slot), so Logic checks the lease of the old gateway before sending a Kick.

> Kick acts only on connections in the `ONLINE` state.

> Logic→Gateway: Kick dialed by StatefulSet pod DNS (`gw-17.gateway.im.svc`)

### Why this matters

The spec does not say whether an un-kicked old connection may keep acting as the user. Sub-project 2 will inherit that unstated answer. The gateway never re-checks its connections against Redis, so a missed Kick lasts as long as the connection does, which can be weeks (see O10). (`spec-review.md` §3 and §7 raise mechanisms 3 and 4. The security consequence is not raised there.)

## O9 — implementation — medium

### Claim

Race 4 keeps per-connection order by waiting for the Login reply. Race 5 drops that guarantee: after a timeout, the gateway sends Logout while the Login may still be queued in Logic or in the Redis pipeline. Logout batches and Login batches go round_robin to different Logic pods, so the Logout can arrive first and match nothing, and then the Login writes. The result is an entry on a live, leased gateway that points at a closed connection, and it looks valid. The idempotence the spec appeals to doesn't help, because the problem is ordering. Race 6 and the Logic-down row produce the same ghost entry. That row also claims "the sweeper ... cover[s] it", but the sweeper only touches dead epochs.

### Evidence

> 4. ... the gateway sends Logout after the Login reply arrives, so order is kept for each connection.

> 5. **Login RPC times out but Redis was written:** the gateway closes and sends Logout, which is safe because compare-and-delete is idempotent.

> Logic down | ... Logout goes into a bounded retry queue for 60s, then is dropped (the sweeper and lazy repair cover it).

### Why this matters

Sub-project 1 claims these races are handled, and the race tests will check race 5 only in the order where it works. In practice, repair waits for sub-project 3. Until the client happens to reconnect, presence reports a dead connection as online. That breaks the "online mark (source of truth)" promise. (`spec-review.md` §4 raises the same finding.)

## O10 — scope — medium

### Claim

A connection is authenticated exactly once, by a JWT verified locally, and can then live for weeks. The spec explicitly plans for connections older than 15 days. Three questions are left unanswered, and each belongs to the connection layer, not the account service:

- What happens when the token expires while the connection is up?
- How does a ban, password change, or "log out all devices" end the connections that already exist? Local verification cannot see a revocation, and the only server-initiated close is the slot-replacement Kick.
- How does Logic get and rotate the account service's Ed25519 public keys?

### Evidence

> Separate account service issues Ed25519-signed JWTs; Logic verifies them locally (no database call on login).

> One login attempt per connection (a failure closes it).

> Gateways refresh the TTL for connections older than 15 days

> Out of scope ... account service.

### Why this matters

Putting the account service out of scope does not put out of scope the session layer's response to account events. If this isn't designed now, sub-project 1 ships with no way to force a connection closed except slot replacement. Adding one later means a new Logic→Gateway RPC and a new state transition, which this spec was meant to fix. (`spec-review.md` §10 raises revocation. It does not raise expiry on a live connection or key distribution.)

## O11 — risk — medium

### Claim

Both admission controls hit the wrong population during a storm:

- **The per-IP token bucket.** It throttles mobile users behind carrier-grade NAT, where thousands of legitimate users share one IP, and this happens exactly when they all reconnect at once. It also needs the real client IP. The spec never says how the gateway gets that IP behind an L4 NLB, or behind an LB that terminates TLS for wss (it needs proxy protocol or similar).
- **The cap on HANDSHAKE + AUTH_PENDING.** It counts connections waiting on *client* round trips. Slow clients fill the cap while the server sits idle, and the gateway answers `Busy`, which pushes users past the SLO in O7. The spec also has no timeout for the handshake or for auth, so a slow or malicious client can hold a slot forever.

### Evidence

> A token bucket for new connections per IP.

> cap the connections in `HANDSHAKE` + `AUTH_PENDING` per gateway (configurable, for example 5k); above the cap, reply `Busy{retry_after}` and close.

> Web: WebSocket over wss (TLS terminated at the cloud LB)

### Why this matters

The controls meant to protect the storm SLO are the most likely cause of missing it. Without the client IP, the per-IP bucket degrades into a per-LB-node bucket, which throttles all traffic arriving through that node. (`spec-review.md` §9 raises the same finding.)

## O12 — specification quality — low

### Claim

The Logic RPC list has four entries and is missing calls that the rest of the spec depends on:

- **The TTL-refresh "slow batch RPC".** The spec relies on it but doesn't list it.
- **A read path.** No RPC or library implements the rule that "a session is valid only if its field exists and its lease exists". That rule is cross-slot (sess plus one lease key per epoch), so its cost lands on sub-projects 2 and 3 without being accounted for.
- **A grace period for in-flight batches.** Forcing `MaxConnectionAge` ≈5m without a `MaxConnectionAgeGrace` can abort Login batches that are in flight. That turns into timeouts, which is the trigger for O9.

### Evidence

> RPCs: `Login` (batch), `Logout` (batch), `GatewayLease`, `EpochEnded`.

> Gateways refresh the TTL for connections older than 15 days by sending a slow batch RPC to Logic

> **A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

> Logic server sets `MaxConnectionAge` ≈5m so gateways pick up new pods.

### Why this matters

The `internal.proto` written from this spec will be incomplete on day one. The validity rule, which is central to correctness, has no owner in code, so each consumer will write its own version, possibly with a different meaning of "valid".

## Explicitly not objecting to

- **Noise NK over TLS for mobile**: pinning the server key and using AEAD per frame is a sound, cheap choice for a custom TCP protocol. The finer points (prologue binding, 0-RTT payload) are implementation details that `spec-review.md` §16–17 already records.
- **gnet with no goroutine per connection**: at the stated density, avoiding 600k goroutine stacks is justified. Whether that density is needed is O1, not a flaw in the choice of library.
- **The slot model (`userid + appid + platform`)**: it matches WeChat's product semantics, and it is what makes single-slot atomic Lua possible.
- **Dropping the distributed lock and the session version**: every change is one script on one hash-tagged slot, and the `gw|epoch|conn_id` triple does what a version would do.
- **Epoch-keyed leases and conn_ids that are never reused**: these handle zombie and same-gateway races correctly. The problem in O2 is who renews the lease and how the sweeper reads it, not the keying.
- **"The gateway never touches Redis"**: a clear failure boundary that's worth keeping. O3's alternative keeps it too.
- **The 150s heartbeat**: a reasonable default that the server can tune through `LoginResp`, and the spec adds a metric to detect NAT timeouts. Per-transport tuning is a parameter, not a design flaw.
- **The lazy loop-identity / `worker_id` scheme**: it is fiddly, but it is internally consistent and guarded by a `-race` test. A simpler global atomic is an optional simplification with no failure attached, so per the routing rule it belongs in the Cartographer's record.
- **Hash-tag notation and the `gwconns` set only ever growing**: real but minor, already in `spec-review.md` §12–13, and neither causes a correctness failure on its own.
