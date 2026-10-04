---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev3.md
objections: docs/superpowers/objections/connection-session-core-design-rev3.md
date: 2026-10-04
decision_maker: user
guiding_principle: new system; take the smallest fix that closes the failure, and avoid overdesign
---

# Disposition record — Connection & Session Core, revision 3

The user decides each objection. This file records the decision and the spec changes it implies. The spec itself has not been edited yet.

## O1 — implementation — high — BLOCKING — ACCEPTED

**Decision:** End each completed Reconcile pass with a `ReconcileEnd` RPC that carries the gateway's tracked conn_ids. The delete phase is decided from that list, not from chunk membership.

**Rationale:** The current rule ("conn_id was not in any chunk of the pass") needs one Logic node to see every chunk, but Logic is stateless and chunks are round-robined. `cutoff_ms` also fails to protect Logins that are in flight when the pass starts, and it lets a ghost survive about two cycles. The gateway is the source of truth, so a list of the conn_ids it still tracks, stamped with its own clock, answers "is this connection gone?" directly. One RPC, no new key, and Logic stays stateless.

**Spec changes to apply:**
- *Logic → RPC list:* add `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`.
- *Gateway → Reconcile:* when a pass completes, the gateway collects every conn_id it still tracks, in **any** state, including closed connections whose fenced Logout has not been sent yet. It stamps `as_of_ms` (gateway wall clock, the same clock as `recv_ms`) and sends `ReconcileEnd`. A cancelled pass sends none. Remove `cutoff_ms`.
- *Logic → Reconcile handling, delete phase:* replace the post-pass paragraph with: the Logic node that receives `ReconcileEnd` SSCANs `gwconns:gw:{epoch}`, and compare-and-deletes each field that points at this `gw|epoch` whose `conn_id ∉ tracked_conn_ids` and whose `login_ms < as_of_ms`. It also removes uids with no remaining field from `gwconns`. Why this is safe: a conn_id leaves the tracked set only after its Logout fence has held, so no Login or Reconcile write for it can still land.
- *Size note:* 8 bytes × 250k (the cap) ≈ 2MB, under gRPC's 4MB default. At the 600k benchmark the list is split or the limit raised.
- *Consistency contract:* the ghost bound "≤ 1 Reconcile cycle" now holds, because a Logout dropped at any point before a pass ends is repaired at the end of that pass.
- *Tests:* a Login in flight when a pass starts (inject a Logic delay) is not deleted by that pass's delete phase; a Logout dropped after the connection's chunk was sent is repaired at the end of the same pass; a cancelled pass deletes nothing.

## O2 — risk — high — BLOCKING — ACCEPTED

**Decision:** The "others are renewing" condition must hold continuously for one lease TTL before any sweep, using a renewal window of 1.5 × the renewal interval.

**Rationale:** As written, the guard releases on the first renewal after a systemic outage that cuts gateways off from Logic while Logic still reaches Redis. Healthy gateways that haven't renewed yet (up to 10s later) are then swept. Waiting one TTL with renewals continuously present gives every healthy gateway three renewal chances. The wider window keeps a lone surviving gateway's 10s rhythm (plus RPC latency) from resetting the clock. It costs one in-memory timestamp per sweeper. A truly dead gateway is swept 30s later, which doesn't matter because its entries are already invalid by the validity rule. This closes the release edge of the R2-O3 rule; the rule itself is unchanged.

**Spec changes to apply:**
- *Sweeper → Mass-stale guard:* the first condition becomes: **at least one other epoch renewed within 1.5 × the renewal interval (15s), and this has held continuously for one lease TTL (30s)**. Each sweeper keeps a `renewing_since` timestamp in memory: it is reset when the condition is false and set when it first becomes true. A sweeper restart starts it over, which is conservative. The second condition (own Redis access healthy for one lease TTL) is unchanged.
- *Sweeper → Mass-stale guard, explanation:* "releases on its own once renewals resume" becomes "releases one lease TTL after renewals resume, by which time every healthy gateway has renewed".
- *Races handled, #12:* add: an outage between the gateways and Logic, with Logic↔Redis healthy, stops every renewal. When it ends, the guard waits one TTL of continuous renewals, so healthy gateways renew before any sweep.
- *Contract, "Entry of a dead gateway":* deleted after 2 × TTL **plus up to one TTL of guard wait** plus sweep time.
- *Tests:* block gateway→Logic for 90s with Logic↔Redis healthy, then restore: nothing is swept. Logic restart with renewals resuming more than 30s after the sweeper starts: nothing is swept. Existing test: kill 2 gateways while others renew; both epochs are still swept.

## O3 — risk — high — BLOCKING — ACCEPTED

**Decision:** Logic reports `missing` whenever a renewal finds the lease key absent. The sweeper aborts if the epoch renews while it is sweeping.

**Rationale:** `missing` currently requires both the lease key and the `gwepochs` member to be gone. A renewal that lands during the 20–25s sweep finds the member still present, so it is never reported, and the sweeper goes on deleting the gateway's sessions. Basing `missing` on the lease key alone tells the gateway that its sessions may have been touched. Re-checking the renewal score stops the sweeper from undoing the restore. Both are local changes with no new key. The residual risk is one in-flight sweeper batch deleting entries the triggered pass just restored, and the next periodic pass repairs those.

**Spec changes to apply:**
- *Gateway → Lease:* the reply reports **missing** when the renewal found the lease key absent (Logic renews with `SET ... PX <TTL> GET` and checks for a nil previous value), whatever the state of the `gwepochs` member. A lease that expired but was never swept costs one unneeded triggered pass, which is rare because of the TTL derivation (R2-O5).
- *Sweeper:* record the epoch's `gwepochs` score when claiming it. Between rate-limit batches, re-read the score. If it is newer than the claimed score, the epoch has renewed: stop at once, without `UNLINK`ing `gwconns` and without removing the ZSET member, and count a `sweep_aborted` metric.
- *Races handled, #12:* add: if the starved gateway renews while its epoch is being swept, the reply still reports `missing` (the lease key was absent), the sweeper stops at its next batch, and the triggered Reconcile restores whatever was deleted. Entries deleted by the last in-flight batch after the restore are repaired by the next periodic pass.
- *Observability:* add `sweep_aborted`.
- *Tests:* start a sweep of a live gateway's epoch (lease key removed), let the gateway renew mid-sweep: the reply is `missing`, the sweeper aborts without `UNLINK`/`ZREM`, and the sessions are restored within ~1 min. A renewal after a plain lease expiry with no sweep is `missing` and triggers one pass.

## O4 — implementation — medium — ACCEPTED

**Decision:** Rule 3 uses window-aware ordering for the same device_id and a "stored wins" tie-break.

**Rationale:** R2-O4 asked rule 3 to use the login table's ordering. "Higher conn_seq wins" leaves out the R1-O5 regression row, so after a failover rollback Reconcile can close the legitimate client with `REPLACED`, which doesn't reconnect automatically. The window-aware rule matches the login table on every state the login Lua can reach: a higher conn_seq more than `login_deadline` earlier is impossible at login, because of the `EXPIRED` deadline. The tie-break stops two live sessions from overwriting and closing each other.

**Spec changes to apply:**
- *Logic → Reconcile handling, rule 3:* replace the comparison with:
  - same device_id: if the two `login_ms` are within `login_deadline` of each other, the higher `conn_seq` wins; otherwise the later `login_ms` wins (the regression row);
  - different device_id: the later `login_ms` wins;
  - on a tie in either case, the stored session wins.
- *Races handled, #15:* add that the ordering includes the regression window, so a session that won through a counter regression keeps winning after a rollback.
- *Tests:* Lua/integration test: a regressed-counter session (low conn_seq, login more than `login_deadline` after the stored one) has its write rolled back by hand to the older high-conn_seq entry; Reconcile keeps the regressed session and kicks the older one. Equal `login_ms` on two devices: the stored session wins and no kick loop occurs.

## O5 — specification quality — medium — ACCEPTED

**Decision:** The SDK treats a server close before the handshake completes as a `Busy` retry (1–3s with jitter), for up to 5 consecutive times, then falls back to exponential backoff.

**Rationale:** `at_capacity` and handshake-limit rejects close the socket before any framing, so the client can't get `Busy{retry_after}`. Treating these closes as an unexpected close restarts the 0–20s wait, and the R2-O2 rolling-restart test can't meet the SLO that way (about 60% of attempts rejected once 3 of 5 targets are at the cap). A 1–3s retry fits the SLO. The limit of 5 keeps a real outage from causing a retry storm. The change is SDK-only, with no protocol change.

**Spec changes to apply:**
- *Client SDK rules → Reconnect:* replace "A connection closed at accept (`at_capacity`) is an unexpected close and follows the same rule" with: **a TCP connection that the server closes before the Noise handshake completes counts as a `Busy` retry: wait 1–3s with jitter.** This covers `at_capacity` and `handshake_rate` rejects. After 5 such closes in a row, fall back to exponential backoff (cap 5m). The count resets on a successful login. Each attempt uses a new source port, so the NLB flow hash can pick another gateway. The 0–20s wait still applies to the first attempt after losing an established connection.
- *Client protocol, `reconnect_reason`:* such retries are reported as `Busy retry`.
- *Tests:* rolling-restart load test unchanged (clients rejected at the cap log in elsewhere within the SLO); add an SDK unit test for the 5-retry limit and its reset.

## O6 — specification quality — medium — ACCEPTED

**Decision:** Entries refused by the deadline are re-queued in the current pass. Rule 3 kicks the stored session only if its compare-and-set wrote. Open point 6 is otherwise confirmed.

**Rationale:** Retrying only in the next pass turns the ~1 min restore after `missing` into ~10 min, exactly when Logic is slow. Kicking after a refused write closes the older session, and its Logout then empties the slot while the newer connection is live. Both fixes are wording only, with no new mechanism.

**Spec changes to apply:**
- *Open point 6:* confirmed. The deadline refuses only writes that create or replace a field (rules 3 and 4); late TTL refreshes and `gwconns` SADDs are not refused.
- *Logic → Reconcile handling:* Logic returns a per-entry result: `written`, `refused` (deadline), or `not_owner` (with reason). A compare-and-set that fails because the field changed is reported as a definitive result with no write, and the next pass handles the entry.
- *Logic → Reconcile handling, rule 3:* "kick the stored session" becomes "**if the compare-and-set wrote**, kick the stored session".
- *Gateway → Reconcile:* a `refused` entry is a definitive reply for fence (c). Its conn_id goes back on the current pass's queue and is rebuilt from the live tables when its chunk is sent; it is skipped if the connection is no longer ONLINE. Each entry is re-queued at most 3 times per pass, so the pass always ends; after that it waits for the next pass.
- *Tests:* inject a Logic delay longer than `login_deadline` on one chunk of a triggered pass: its entries are refused, re-queued and restored within the same pass. A refused rule-3 write sends no kick.

## O7 — specification quality — low — ACCEPTED

**Decision:** The compose rig runs 4 gateways.

**Rationale:** With 2 gateways, "kill 2 gateways while the others renew" leaves no renewer, so the guard correctly holds forever and the test can't pass. Four gateways leave two renewers after killing two, and also let the rig test the O2 single-renewer rhythm. The extra containers cost little.

**Spec changes to apply:**
- *Verification → Race integration tests:* `deploy/compose` runs Redis, Logic, **4 gateways** and a scripted client.
- *Tests:* kill 3 gateways while one keeps renewing at its 10s rhythm: the guard's `renewing_since` is not reset, and all three epochs are swept after the guard wait (O2).

## Summary after this round

- Blocking objections O1, O2 and O3 are accepted; the spec is ready for plan once the changes above are applied (revision 4).
- O4–O7 are accepted; their changes can be applied in the same revision.
- Open points 4 and 5 are confirmed as interpreted, with these clarifications to apply: the connection cap counts sockets (accept to close), not connection-table entries kept for a pending Logout; a cancelled Reconcile pass builds and sends no further chunks and sends no `ReconcileEnd`. Open point 6 is confirmed with the O6 changes.
- Open points 1 and 3 are unchanged. Open point 2 (slot-scoped Revoke) remains open.
