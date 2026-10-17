---
spec: docs/superpowers/plans/2026-10-17-connection-session-core-06b3b5-refusal-hold.md
date: 2026-10-17
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: implementation
    severity: high
    claim: "The held count a least-connections LB needs is the open-connection gap between the refusing gateway and the others, not the refusal burst, and the plan's sizing of HeldCap (10,000) and of the expected gauge ('a few thousand at most') rests on neither."
    evidence: "Task 3 table: 'the gauge … a few thousand at most, under 10,000'; Architecture: 'the LB's count then carries the refusals (refusal rate × hold)'; 6b3b4 trial: 'open connections gw-1 / gw-2 177k / 156k at t=16s, 228k / 200k at 29s' (gaps of 21k–28k)."
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: risk
    severity: medium
    claim: "Draining refusals are held, so a rolling restart's clients sent back to a draining pod during the LB's deregistration delay each lose one more hold, but Review Focus 1 says the rolling-restart sizing is unaffected because only at_capacity matters there."
    evidence: "Review Focus 1: 'The spec's rolling-restart sizing (R5S-O4) is about at_capacity, which is not held, so that sizing stands.' Spec l.544: 'after NotReady plus the LB's deregistration delay, the draining pod accepts no new connections'; Global Constraints: 'Only pending_mem and draining refusals are held.'"
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: scope
    severity: medium
    claim: "The fix is verified only against loadgen's balancer, which counts a held socket as 1 and an instant close as 0, and the plan states that a real LB behaves the same way. A real L4 least-connections LB (LVS/IPVS wlc, which Alibaba CLB and Tier 3's local kube-proxy IPVS use) weighs active and inactive connections differently, and the plan names no check against one."
    evidence: "Architecture: 'So a held refusal counts there for the hold, as it would in a real LB.' Spec l.67: 'Alibaba CLB/NLB wlc, Tencent CLB least_conn'; spec l.482: Tier 3 local uses 'kube-proxy in IPVS mode'. After this plan names only 6b3c."
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: specification quality
    severity: medium
    claim: "Task 3's outcomes (a)/(b)/(c) hinge on 'even' and 'no frozen survivor', which the plan defines only loosely ('within about 2×', 'lower'), so two readers of the same report can file it under different outcomes."
    evidence: "Task 3 Step 2: 'the two within about 2× of each other'; 'no survivor lower at every sample'; '(a) Even, no frozen survivor'; '(c) Uneven with full 0'."
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: risk
    severity: low
    claim: "All of a tick's held sockets on a loop close in one wheel advance, so a loop at the cap's share does up to ~2,500 closes (10,000 over 4 loops) in one Execute, delaying that loop's handshakes and Pongs once a second, and nothing in Task 3 reads that delay."
    evidence: "Architecture: 'A loop's closes come in one batch per tick.' Review Focus 2 checks it through 'srv_est per survivor, refusals_held every 10s' and the storm profiles, neither of which shows per-tick latency."
    disposition: pending
    disposition_rationale: null
  - id: O6
    category: risk
    severity: low
    claim: "The nofile need rises by HeldCap for every deployment, so a gateway whose RLIMIT_NOFILE was set to exactly cap × 1.1 (the rev6 requirement) stays NotReady after the upgrade. The plan records this only for the Tier 2 scripts and the rev7 text, not for operators."
    evidence: "Global Constraints: 'nofile self-check: ceil(MaxConns × (1 + Headroom)) + HeldCap. Every Tier 2 scenario raises nofile to 1,048,576'; spec l.233: 'If either is below the requirement … the gateway stays NotReady'."
    disposition: pending
    disposition_rationale: null
---

## O1 — implementation — high

### Claim

The plan sizes the hold by the refusal burst ("refusal rate × hold"), and expects the `refusals_held` gauge to stay at "a few thousand at most". But least-connections keeps sending dials to the refusing gateway until its **total** count (admitted + held) catches up with the others'. So in steady state the held count has to cover the gap between the refusing gateway's admitted connections and the others' counts. That gap grows whenever the refusing gateway admits more slowly, and that is the situation that makes it refuse in the first place.

To keep a gap G covered, the gateway must keep G sockets held. With a mean hold of ~1s, that means G refusals per second, every one an accept, a close and a client redial. Above `HeldCap` the extra refusals close at once, and #131's loop comes back for the rest.

### Evidence

- Task 3's table: "the gauge … a few thousand at most, under 10,000".
- Architecture: "the LB's count then carries the refusals (refusal rate × hold)".
- The 6b3b4 trial's socket samples show gaps of 21k at 16s and 28k at 29s (177k / 156k, 228k / 200k), larger than `HeldCap`. With the hold the gap may never form, or may form smaller. The plan argues neither.

### Why this matters

If the gap is larger than 10,000 in the rerun, the result is outcome (b): uneven, with `full` above 0. The plan would then have shipped a cap with no stated basis, and the follow-up ("raise `-held-cap`") would be tuned by trial and error. The plan should:
- state the mechanism (held ≈ the count gap; refusals/s ≈ gap / mean hold);
- have Task 3 read the gap (`srv_est` differences between survivors) next to `refusals_held`;
- say why 10,000 is enough, or which reading decides it.

## O2 — risk — medium

### Claim

Review Focus 1 says that holding refusals does not touch the rolling-restart sizing, because that sizing is about `at_capacity`. But the plan also holds `draining` refusals, and a rolling restart is where draining refusals happen. Between NotReady and the end of the LB's deregistration delay, new dials still reach the draining pod and are refused as `draining`.

That includes the pod's own clients reconnecting after `Reconnect{jitter_ms}`. Under least-connections, the draining pod's falling count draws those dials back to it, which is #131's loop. Each of them now waits one hold (0.5–1.5s) before its pre-handshake retry.

### Evidence

- Review Focus 1: "The spec's rolling-restart sizing (R5S-O4) is about `at_capacity`, which is not held, so that sizing stands."
- Spec l.544: "after NotReady plus the LB's deregistration delay, the draining pod accepts no new connections".
- Global Constraints: "Only `pending_mem` and `draining` refusals are held."

### Why this matters

Tier 3's rolling-restart test has to show that rejected clients log in elsewhere within the SLO, and that fewer than 1% reach backoff. Under the hold, a client that a deregistering pod refuses several times loses ~1s more per attempt. Holding probably still helps more than it costs, because it breaks the loop. But the plan's claim is wrong as written, and the rev7 follow-up text repeats it ("The rolling-restart sizing above is about `at_capacity`, which is not held"). Plan 6d would inherit an unstated cost.

## O3 — scope — medium

### Claim

The only verification is loadgen's balancer, which counts a connection from dial to the client's close: an instant close counts 0, a held socket counts 1. The plan asserts that real LBs behave the same way.

The spec's named least-connections LBs include LVS-based ones (Alibaba CLB/NLB `wlc`), and Tier 3's local stand-in is kube-proxy in IPVS mode. IPVS `lc`/`wlc` weighs an active (ESTABLISHED) connection at 256 and an inactive one (FIN_WAIT, TIME_WAIT, CLOSE) at 1. So under IPVS:
- an instantly closed refusal still counts, though slightly;
- a held socket counts fully.

Both point the same way as loadgen's model, but by different amounts.

### Evidence

- Architecture: "So a held refusal counts there for the hold, as it would in a real LB."
- Spec l.67 (the LBs named); spec l.482 (Tier 3 `local`: "kube-proxy in IPVS mode").
- *After this plan* names only 6b3c.

### Why this matters

#131 was found, and will be confirmed, only in loadgen's model of the LB. The rev7 text will state a general claim about least-connections LBs. If no later plan is told to check it, the claim reaches Tier 3 untested. Plan 6d's rolling restart on IPVS is the natural check, and nothing records that it should be one.

## O4 — specification quality — medium

### Claim

The trial is recorded, not gated, which is the user's ruling. Its outcome still decides the next step: (a) done, (b) raise the cap, (c) debug. The conditions that pick the outcome are loose:
- "within about 2×";
- "no survivor lower at every sample";
- "both survivors above 0 while relogins remain";
- "lower" for recovery.

Run the first trial's numbers through them: 100,602 against 2,251,091 is far outside 2×. But 1.9× would be "even" under the plan, though it is still a strongly uneven split.

### Evidence

Task 3 Step 2's table, and the outcome list "(a) Even, no frozen survivor … (c) Uneven with `full` 0".

### Why this matters

The record goes into the roadmap and the rev7 text as "#131 fixed". A loose threshold makes that verdict depend on the reader. A precise definition would prevent that. For example:
- even: each survivor's `pending_mem` within 1.5× of the other's;
- frozen: `handshakes_ok` unchanged over two consecutive samples while `b_stress_back` is not yet complete.

## O5 — risk — low

### Claim

With the hold on the wheel, each loop closes all its due held sockets in one advance. That is up to ~2,500 per loop per tick (the 10,000 cap over 4 loops), each a gnet close and a `Closed` call. For that time the loop is not reading handshakes or answering Pings. The once-a-second LB-count dip that follows is a second effect.

### Evidence

- Architecture: "A loop's closes come in one batch per tick."
- Review Focus 2 says the batch is checked by "`srv_est` per survivor, `refusals_held` every 10s" and the storm profiles. None of these resolves a per-tick stall.

### Why this matters

The cost is probably tens of milliseconds per tick, which is acceptable. But it is unmeasured, and the plan's Review Focus claims a check it doesn't have. Either drop the claim, or name a reading that would show it, such as the storm's `reply_lag`, which `b_stress_storm_real` already records.

## O6 — risk — low

### Claim

The self-check's nofile requirement grows by `HeldCap` (10,000) for every gateway. A deployment that set `RLIMIT_NOFILE` to exactly what rev6 requires, cap × 1.1, goes NotReady after upgrading, with only the self-check's log line to explain why.

### Evidence

- Global Constraints: "nofile self-check: `ceil(MaxConns × (1 + Headroom)) + HeldCap`. Every Tier 2 scenario raises nofile to 1,048,576".
- Spec l.233: "If either is below the requirement … the gateway stays NotReady".

### Why this matters

No production deployment exists yet, and `deploy/k8s` comes in 6d. So the exposure today is the README and anything that quotes the rev6 formula. The rev7 text covers it (item 4). Whether the README's deployment notes need the new formula now is a one-line check that the plan doesn't make.

## Explicitly not objecting to

- **Holding on the 1s wheel instead of an exact random 0.5–1s:** the user ruled on it, the hold is uniform across arrival phase, and the SDK's own 0.5–1.5s retry spreads the redials after a batch close.
- **A `Conn` in `stateHeld` instead of a separate type:** it keeps one transport path, and the mutation table shows the state machine never sees it.
- **Holding sockets that flood clients open:** a slow-handshake or garbage flood that fills `PendingCap` can occupy all held places, but the cap bounds the fds. Over the cap the behaviour is exactly today's.
- **An accept-queue alternative (stop accepting while `pending` is at the cap, so the kernel's backlog holds the connection):** it would avoid the refusal's cost entirely, but gnet v2.10.0 has no way to pause its acceptor, and it changes #111's territory (making a client wait instead of refusing it), which the user has parked.
- **`at_capacity` not held:** the plan's reasoning holds. A gateway at the cap has the highest count, so least-connections does not single it out.
- **Test timing bounds (450ms–3s over TCP):** the wheel never fires early, so the lower bound cannot flake, and the upper bound has 1.5s of slack.
- **`closes_total{reason="held"}` as a new label:** no Tier 2 script sums `closes_total` across reasons (`floods.sh` and `capacity.sh` read only `heartbeat`).
