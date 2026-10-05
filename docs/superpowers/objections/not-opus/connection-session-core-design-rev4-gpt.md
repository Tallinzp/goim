---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev4.md
date: 2026-10-05
mode: spec
objections:
  - id: O1
    category: premise
    severity: high
    claim: "The slot model's correctness depends on the SDK preventing two simultaneous connections per device_id, but the spec treats this as a requirement the SDK will satisfy, not as a constraint the system must tolerate being violated."
    evidence: "device_id (R1-O6) identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard (e.g. a single connection service on Android)."
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: premise
    severity: high
    claim: "The 250k per-gateway connection cap is asserted as a default but never grounded in measurement on the chosen hardware, yet the entire N−1 storm absorption arithmetic depends on it."
    evidence: "a hard per-gateway connection cap (default 250k, see Admission control) bounds skew: with one gateway down the remaining five hold up to 5 × 250k = 1.25M ≥ 1M; with a whole zone down (4 survivors) they can still hold 1M, just barely."
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: alternatives
    severity: medium
    claim: "A simpler mobile transport — raw TCP plus TLS plus app-layer auth — would eliminate the entire Noise subsystem (handshake state machine, static-key pinning, key-ID rotation) at the cost of TLS termination at the gateway."
    evidence: "Client encryption | Custom app-layer encryption using the Noise Protocol Framework, NK pattern (flynn/noise): server static key pinned in the app, with a key ID for rotation; ChaCha20-Poly1305 per frame. ... Web relies on wss TLS and uses no Noise."
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: alternatives
    severity: high
    claim: "Reconcile could refresh TTLs and let stale entries expire, avoiding the compare-and-set race entirely and the 'two live sessions overwrite and close each other' tie-breaking it requires."
    evidence: "Field holds a different session whose gw|epoch has a live lease → compare the two with the login table's ordering, including its regression window (R2-O4, R3-O4): ... on a tie in either case, the stored session wins."
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: scope
    severity: medium
    claim: "The spec claims the web transport is 'designed now behind a transport interface, built as a late slice', but no transport interface is defined anywhere in the spec."
    evidence: "Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface, built as a late slice."
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: scope
    severity: high
    claim: "Noise key rotation is in scope (a 'key ID for rotation' is named) but no rotation runbook, no key-distribution mechanism, and no fallback during rotation are specified — unlike the JWKS rotation runbook which is fully designed."
    evidence: "server static key pinned in the app, with a key ID for rotation ... Key rotation runbook: a new signing key is published to the jwks-fallback ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap. (A stale ConfigMap only matters while the account service is unreachable.)"
    disposition: pending
    disposition_rationale: null
  - id: O7
    category: implementation
    severity: medium
    claim: "The gnet loop-ownership invariant is sourced from gnet's unreleased dev branch, so the spec's claim that OnOpen always runs on the owning loop's goroutine is only as durable as that branch's HEAD."
    evidence: "Checked against gnet's dev source: OnOpen always runs on the owning loop's goroutine (both accept paths). The loop index is unexported, but Conn.EventLoop() gives the loop's identity."
    disposition: pending
    disposition_rationale: null
  - id: O8
    category: implementation
    severity: high
    claim: "The refused-entry re-queue in Reconcile can amplify load under sustained overload — refused entries are re-issued up to three times per pass into a system that is already overloaded, so the retry traffic contends with whatever caused the first refusal."
    evidence: "Each entry is re-queued at most 3 times per pass, so the pass always ends; after that it waits for the next pass. ... A refused entry's conn_id goes back on the current pass's queue and is rebuilt from the live tables when its new chunk is sent"
    disposition: pending
    disposition_rationale: null
  - id: O9
    category: risk
    severity: high
    claim: "The Logout fence's 1-second skew budget is only enough for steady-state NTP; a clock-skew spike (e.g. NTP server failure or step adjustment) would invalidate fence (b) and let a Logout race a still-pending Login write."
    evidence: "(b) recv_ms + login_deadline + 1s has passed (the 1s absorbs gateway↔Redis clock skew). This applies to any Login without a definitive reply: timeout, RPC error, a gRPC connection reset by MaxConnectionAge, or a Logic crash."
    disposition: pending
    disposition_rationale: null
  - id: O10
    category: risk
    severity: high
    claim: "The mass-stale guard's 15-second window is calibrated against a 10-second renewal rhythm on the dedicated lease RPC pool, but nothing tests for that pool's latency; a stalled or paused renewal would be indistinguishable from a dead gateway and could trigger mass staleness."
    evidence: "at least one other epoch renewed within 1.5 × the renewal interval (15s), and this has held continuously for one lease TTL (30s). ... The 1.5× window keeps a lone surviving gateway's 10s rhythm (plus RPC latency) from resetting the clock"
    disposition: pending
    disposition_rationale: null
  - id: O11
    category: specification quality
    severity: medium
    claim: "The server-emitted Reconnect{jitter} command's payload semantics are undefined — clients cannot implement the drain behaviour without knowing whether jitter is a duration, a range, an opaque blob, or per-attempt guidance."
    evidence: "Commands: Login, LoginResp (includes the heartbeat interval), Ping/Pong, Logout, Kick{reason} (from the server), Reconnect{jitter} (from the server), Error, Busy{retry_after}."
    disposition: pending
    disposition_rationale: null
  - id: O12
    category: specification quality
    severity: high
    claim: "The 0–20s reconnect jitter window has no stated distribution, but the SLO arithmetic ('200k users spread over the 0–20s reconnect jitter is ~10k logins/s') implicitly assumes a uniform spread."
    evidence: "After losing an established connection unexpectedly, wait a random 0–20s before the first attempt ... Arithmetic: 200k users spread over the 0–20s reconnect jitter is ~10k logins/s, within the 20k/s peak headroom."
    disposition: pending
    disposition_rationale: null
---

# Objection record — rev4

## O1 — premise — high

### Claim
The slot model presumes the SDK will enforce "at most one connection per device_id" on the client. The spec treats this as a client-side requirement and only addresses the steady-state failure mode (race 10: "they replace each other once, and because `REPLACED` doesn't reconnect, the loop stops"). It does not address what happens when the SDK guard fails — and on Android/iOS, guards fail: virtual environments, parallel-space apps, dual-signing tools, jailbroken devices, debuggable builds, and process-level guards that don't survive a process restart. The architectural posture is "the SDK is the trust boundary for slot uniqueness," and the spec does not say what the server does when that boundary is breached.

### Evidence

> device_id (R1-O6) identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard (e.g. a single connection service on Android).

### Why this matters
If two simultaneous connections for the same slot land close enough in time, the spec's correctness criterion ("no stale event … can delete or override a newer session") is enforced by the Lua ordering rules, not by preventing the race from happening. This works for races 1–6 but it does *not* cover the case where both instances are simultaneously in `AUTH_PENDING` with overlapping `recv_ms` windows — both pass the revocation and expiry checks, the Lua compare-and-set produces one winner and one kick, and the kicked instance may have already moved a frame through `OnTraffic` into a downstream pipeline. The success criterion is silent about what downstream sub-project 2 sees.

---

## O2 — premise — high

### Claim
The 250k per-gateway connection cap is presented as a default but is not grounded in a measurement of achievable density on the chosen hardware (gnet, Noise, Redis reconciliation, timing wheel) under realistic memory and GC behaviour. The spec's storm arithmetic — "5 × 250k = 1.25M ≥ 1M" — depends on every gateway reliably holding its cap while absorbing a peer gateway's load. The 600k ceiling is explicitly a "benchmark, not the operating point," but the cap is set to 250k without showing that 250k is achievable either.

### Evidence

> a hard per-gateway connection cap (default 250k, see Admission control) bounds skew: with one gateway down the remaining five hold up to 5 × 250k = 1.25M ≥ 1M; with a whole zone down (4 survivors) they can still hold 1M, just barely.

### Why this matters
If a gateway actually tops out at, say, 180k under the chosen hardware, the arithmetic collapses: 5 × 180k = 900k < 1M, and one-gateway loss cannot be absorbed. The SLO depends on the cap being achievable. A spec that asserts the cap without measuring it is asserting an SLO whose precondition has not been verified.

---

## O3 — alternatives — medium

### Claim
Mobile transport is raw TCP plus Noise (NK pattern) plus app-layer auth, while web transport is wss plus app-layer auth. The two transports use different security stacks for no documented reason. A simpler architecture: mobile also uses TCP plus TLS (terminated at the gateway or at the cloud LB) plus app-layer auth. This would let mobile and web share the same encryption story, drop the entire Noise subsystem (handshake state machine, server static key pinning, key-ID rotation infrastructure), and rely on a battle-tested TLS stack for in-transit confidentiality. The cost is TLS termination at the gateway, which is a well-known commodity problem.

### Evidence

> Client encryption | Custom app-layer encryption using the Noise Protocol Framework, NK pattern (flynn/noise): server static key pinned in the app, with a key ID for rotation; ChaCha20-Poly1305 per frame. No resumption tickets (a handshake costs ~100µs, negligible in storms). Web relies on wss TLS and uses no Noise.

### Why this matters
The spec introduces a non-trivial cryptographic subsystem (Noise protocol implementation, NK handshake state machine, per-frame AEAD, key pinning, rotation lifecycle) when the standard solution (TLS) already exists and is used on the web path. The justification — defence-in-depth, handshake latency — is asserted without threat-model context. A reader of the spec cannot tell whether the design choice was deliberate or inherited from prior art.

---

## O4 — alternatives — high

### Claim
Reconcile's primary job, in this spec, is to write newer-over-older via compare-and-set Lua and to resolve ordering conflicts including a regression window. An alternative design uses Reconcile purely for TTL refresh: a chunk entry refreshes the `sess:{uid}` TTL (or, if the field is empty, writes the entry); the spec never overwrites a live, valid entry. Stale entries get cleaned by TTL expiry, and the "two live sessions overwrite and close each other" tie-breaking never happens because there is no overwrite.

### Evidence

> Field holds a different session whose gw|epoch has a live lease → compare the two with the login table's ordering, including its regression window (R2-O4, R3-O4): ... on a tie in either case, the stored session wins.

### Why this matters
The compare-and-set design carries meaningful complexity: a login-table ordering table inside the Lua script, a regression window for low `conn_seq`, a per-row decision tree for kicks vs rejections, and the tie-breaking rule ("stored session wins") which itself needs a test ("Equal login_ms on two devices: the stored session wins and no kick loop occurs"). The TTL-refresh-only design eliminates the entire ordering tree and replaces it with a single TTL extension. The price is slower cleanup of dead-epoch entries, but the sweeper already handles that path. The spec's choice of compare-and-set is presented as the only option, not as one of two.

---

## O5 — scope — medium

### Claim
The spec says the web transport is "designed now behind a transport interface, built as a late slice." Reading the spec, no transport interface is defined. The web path appears only in the architecture diagram, in the outer-frame description, in the rule "one connection per browser profile," and in the deferral "Web relies on wss TLS and uses no Noise." None of these constitute an interface — they are constraints on an interface that is not drawn.

### Evidence

> Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface, built as a late slice.

### Why this matters
"Designed now" is a claim about an artefact. If the artefact (the interface) does not exist in the spec, the claim is unsupported, and the implementation team will invent one — likely one that does not match what the spec assumes elsewhere (frame codec, outer `[u32 length][payload]` framing is TCP-shaped; wss is message-shaped). The risk is that the late slice arrives with a mismatched interface and forces a refactor of the mobile path that was assumed stable.

---

## O6 — scope — high

### Claim
Noise has "a key ID for rotation," but rotation is not specified anywhere: no runbook, no key-distribution mechanism, no behaviour during rotation, no behaviour when the server's static key is compromised. Compare to the JWKS section, which has a full runbook ("a new signing key is published to the jwks-fallback ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap"). The asymmetry is striking — JWKS gets a runbook, Noise gets a name.

### Evidence

> server static key pinned in the app, with a key ID for rotation ... Key rotation runbook: a new signing key is published to the jwks-fallback ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap.

### Why this matters
The Noise static key is pinned in the app binary. Rotation requires shipping a new app build. The spec does not say how long a rotation takes, how the new key is distributed, how clients with the old key behave after the rotation, or how a compromise of the pinned key is handled. A "key ID for rotation" without these details is a name, not a design — and the security-critical lifecycle is exactly where design matters most.

---

## O7 — implementation — medium

### Claim
The spec's loop-ownership invariant ("`OnOpen` always runs on the owning loop's goroutine") is sourced from gnet's `dev` branch — the unreleased development branch. The released version pin is "gnet ≥ v2.8.0," but the loop-ownership claim is verified against HEAD, not against any tagged release.

### Evidence

> Checked against gnet's dev source: OnOpen always runs on the owning loop's goroutine (both accept paths). The loop index is unexported, but Conn.EventLoop() gives the loop's identity.

### Why this matters
A spec that builds its concurrency story on unreleased upstream code is brittle: any rewrite of gnet's accept path, any change to how `EventLoop()` returns loop identity, or any change to the goroutine-affinity guarantee invalidates the architecture. The spec should pin to a specific tagged commit (or run the tests against a tagged release) and document which version of gnet was verified.

---

## O8 — implementation — high

### Claim
Reconcile re-queues refused entries up to three times per pass. The mechanism is correct on its own, but it interacts with the cause of the refusal: a refused entry was refused because the Lua deadline (`Redis TIME − sent_ms ≤ login_deadline`) had elapsed, which happens when Logic-to-Redis is slow or Redis is overloaded. Re-issuing the same entry into the same overloaded path produces more refused entries. The cap of three re-queues prevents infinite loops, but it does not prevent a transient overload from becoming a sustained one — every pass produces a fresh batch of re-queues.

### Evidence

> Each entry is re-queued at most 3 times per pass, so the pass always ends; after that it waits for the next pass. ... A refused entry's conn_id goes back on the current pass's queue and is rebuilt from the live tables when its new chunk is sent; it is skipped if the connection is no longer ONLINE.

### Why this matters
Without an explicit back-off between re-queues (or a circuit-breaker on the refusal rate), the design can amplify load during exactly the conditions where the system is most fragile. The cap of three is a correctness cap, not a load cap. A spec that handles "stuck at the cap" but does not handle "congestion-collapse from re-queue amplification" leaves an operational failure mode unaddressed.

---

## O9 — risk — high

### Claim
The Logout fence's `login_deadline + 1s` budget assumes gateway-to-Redis clock skew stays within ~1 second. On managed Kubernetes, NTP sync can drift further during upstream NTP-server failures, leap-second handling, or step adjustments. A skew spike of, say, 5 seconds would let a Logout land before a still-pending Lua write, creating a ghost.

### Evidence

> (b) `recv_ms + login_deadline + 1s` has passed (the 1s absorbs gateway↔Redis clock skew). This applies to any Login without a definitive reply: timeout, RPC error, a gRPC connection reset by `MaxConnectionAge`, or a Logic crash.

### Why this matters
The fence is the load-bearing guarantee for race 6 ("Login without a definitive reply, but Redis was written"). The 1-second skew budget is stated as a derived number, not as a measured or enforced property of the deployment. The spec's clock-skew assumption appears only as a parenthetical in the login Lua: "clock skew between gateways (NTP, a few ms) is far below the time a person takes to switch devices." This is true for steady-state NTP but not necessarily true during NTP incidents.

---

## O10 — risk — high

### Claim
The mass-stale guard uses a 15-second window (1.5× the 10-second renewal interval) calibrated against a "10s rhythm with jitter." The renewal RPC runs on a dedicated gRPC connection with its own worker pool — but nothing in the spec asserts that the pool is healthy, that the gRPC connection is live, or that the renewal latency stays below a few hundred milliseconds. If the dedicated pool stalls (pool exhaustion, GC pause, DNS resolution failure), renewals slow down silently and the guard's calibration no longer holds — a slow-but-alive gateway could be swept as dead.

### Evidence

> at least one other epoch renewed within 1.5 × the renewal interval (15s), and this has held continuously for one lease TTL (30s). Each sweeper keeps a `renewing_since` timestamp in memory ... The 1.5× window keeps a lone surviving gateway's 10s rhythm (plus RPC latency) from resetting the clock

### Why this matters
The guard is the safety net that prevents a systemic outage from being misdiagnosed as multi-gateway death (which would sweep all live sessions). If the guard misfires in the other direction — sweeping a healthy-but-slow gateway — sessions are restored by triggered Reconcile within ~1 minute, but only after the gateway's own renewals resume and trigger a `missing` reply. This depends on the gateway noticing its own lease is missing, which itself depends on the dedicated pool recovering. A circular failure mode.

---

## O11 — specification quality — medium

### Claim
The server-emitted `Reconnect{jitter}` command's payload is not defined. The spec lists it as a command alongside `Kick{reason}` (which has a clear reason enum) and `Busy{retry_after}` (which is a clear duration), but `Reconnect{jitter}` is not specified: is it a single duration in milliseconds, a range, a per-attempt guidance, or an opaque blob the SDK may ignore?

### Evidence

> Commands: `Login`, `LoginResp` (includes the heartbeat interval), `Ping/Pong`, `Logout`, `Kick{reason}` (from the server), `Reconnect{jitter}` (from the server), `Error`, `Busy{retry_after}`.

### Why this matters
Drain behaviour during graceful drain ("send `Reconnect{jitter}` in batches over about 60s") depends on the client interpreting the jitter value correctly. Without a spec, two reasonable implementers will produce different drain behaviours: one will treat it as a one-shot delay, another as a per-attempt jitter window, another as a back-off schedule. The graceful drain on SIGTERM is a visible user-facing event; getting it wrong is a visible regression.

---

## O12 — specification quality — high

### Claim
The 0–20s reconnect jitter window has no stated distribution. The spec's storm SLO arithmetic computes "200k users spread over the 0–20s reconnect jitter is ~10k logins/s," which implicitly assumes uniform distribution. If the distribution is exponential (clients prefer shorter waits), the peak rate at the head of the window is much higher than 10k/s; if it is uniform, the rate is exactly 10k/s; if it is something else, the rate is something else. The Logic `minReplicas` sizing ("must handle the 20k/s peak without scaling") depends on this number being right.

### Evidence

> After losing an established connection unexpectedly, wait a random 0–20s before the first attempt (leaving 10s for handshake, login and one Busy retry), then exponential backoff (cap 5m). Obey `retry_after`. ... Arithmetic: 200k users spread over the 0–20s reconnect jitter is ~10k logins/s, within the 20k/s peak headroom.

### Why this matters
The 20k/s peak headroom figure is the only thing standing between the SLO and a Logic overload that pushes relogin latency past 30 seconds. If the actual jitter distribution produces a peak above 20k/s at any moment during the 0–20s window, the headroom is gone. This number should be either measured (in a prior deployment) or specified (uniform vs. exponential vs. something else).

---

## Explicitly not objecting to

- **Race 6 (Login without definitive reply, but Redis was written).** The fence (b) and the Lua `EXPIRED` check together cover this correctly under the spec's stated clock-skew assumption; whether the assumption itself holds is O9.
- **Dropped concepts (per-user rate limit, distributed lock, reverse mapping, session version).** Each is justified in the spec with a clear reason; the justifications are reasonable on the facts cited.
- **The 5-second `HANDSHAKE` / `AUTH_PENDING` timeouts.** The spec does not say what fraction of legitimate mobile clients fail to complete a Noise handshake within 5 seconds, but the timeouts are well within industry norms for app-layer auth on healthy networks and are stated as configurable.
- **The two-tier Reconcile (periodic + triggered).** The triggered path is needed for the `missing` lease reply; combining the two into a single periodic-with-trigger design would force a more complex state machine. The complexity is justified.
- **The "stored session wins on tie" rule in Reconcile rule 3.** This rule is what prevents the two-devices-kick-each-other loop, and it is tested in the verification section ("Equal login_ms on two devices: the stored session wins and no kick loop occurs").
- **JWKS rotation runbook.** The asymmetric depth between the JWKS runbook (fully designed) and the Noise rotation plan (a name) is real — that asymmetry is captured in O6, not raised separately.
