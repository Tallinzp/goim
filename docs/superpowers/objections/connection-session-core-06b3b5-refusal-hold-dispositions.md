# Dispositions: Plan 6b3b5 review (2026-10-17)

Objections: `connection-session-core-06b3b5-refusal-hold.md`. The user decided them with AskUserQuestion. O1 and O2 changed #131 itself. The discussion that followed **parked the plan**, so O3–O6 are deferred with it. #131 is amended (no gateway fix now) and **#132** is added to the roadmap.

## O1: accepted, widened by the user. The hold covers a count gap only up to the cap

**The user's point:** a newly started gateway with 0 connections among peers at 300k gets every dial from least-connections. It admits at its own handshake rate and refuses the rest, whether those refusals are held or not. A hold can cover a count gap only up to `-held-cap`. Covering 300k would take ~300k held sockets, refused at ~300k/s. Most of a fresh gateway's refusals at the default 2 slots are `no_slot`, which the plan doesn't hold anyway.

**Decision:** narrow the claim and add a new follow-up. The hold fixes only gateways with similar counts where one reaches the pending cap (held ≈ the count gap, up to the cap). The fresh-pod funnel is recorded as **#132**, for Plan 6d.

## O2: accepted. Draining refusals are not held

**Decision (the user):** draining refusals are not held. If a hold is ever built, it covers `pending_mem` only. This amends #131's "(and `draining`)".

## The plan: parked (the user, 2026-10-17)

After O1–O2 the user asked whether holding is worth it at all, and whether the LB, not the gateway, should solve the trial's problem. The reasoning the user accepted:
- **The hold helps one case only:** gateways with similar counts, one of which fires `pending_mem`. That happened only in the handshake-concurrency-4 trial, which is not a default setting.
  - At the default 2 slots the refusal is `no_slot`, an even share on every gateway: `pending_mem` was 0/0 in the 30j rerun, and 0 in floods since run 30c.
  - The spec's pending cap "must not trigger in a normal storm" (*Admission control*).
- **The hold does not help** a fresh or scaled-out gateway (#132), or `no_slot` refusals.
- **The hold costs** about 1s more per refused attempt, 10k fds in the nofile requirement, and a new socket state.
- **In the trial, `pending_mem` was backpressure from Logic.** 4 slots finished more handshakes than Logic could log in (606k `Busy`), so connections waiting in AUTH_PENDING filled the pending cap. Least-connections then made the split lopsided.
- **On "the LB should solve it":** the mismatch is in the LB's metric, which counts connections rather than admission rates. But L4 LBs offer few levers, and those vary by cloud. The spec must also work behind a flow-hash NLB. The one lever every LB has, readiness, reacts in 5–30s and can flap. So LB-side remedies belong where a real least-connections LB exists: Plan 6d (Tier 3, kube-proxy IPVS).

**Decision ("Record, LB in 6d"):**
- #131 is amended: no gateway-side fix now. The trial's split is recorded as a case where the pending cap fired at a non-default concurrency (Logic backpressure), amplified by least-connections.
- #132 and the LB-side remedies (readiness near the cap, LB slow start, checked on IPVS `lc`/`wlc`) go to Plan 6d.
- Plan 6b3b5 and `proto-6b3b5` (`d12b258`) are kept, parked with #111. If #111 is ever unparked, waiting handshakes stay pending, so `pending_mem` would bind at the default slots too. That is when this hold (pending_mem only, per O2) would be needed.

Next: Plan 6b3c.

## O3–O6: deferred with the plan

- **O3** (verified only against loadgen's LB model): moot now. Plan 6d's IPVS check under #132 covers the LB side.
- **O4** (loose outcome definitions): to apply if the plan is unparked.
- **O5** (batched closes per tick): to apply if the plan is unparked.
- **O6** (the nofile need rises by `HeldCap` on upgrade): to apply if the plan is unparked.
