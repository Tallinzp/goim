---
spec: docs/superpowers/plans/2026-10-16-connection-session-core-06b3b2-run30-followups.md
date: 2026-10-17
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: premise
    severity: high
    claim: "The pinger fix treats run 30's 126s stall as dropped ticks, but the run 30 record leaves the cause open. Loadgen was not CPU-saturated, which a starved Ticker needs, and a Send blocked with no write deadline holds the whole shard's goroutine inside p.send, where catching up can't help. So run 30b may fail slow_heartbeats the same way, with only diagnostics added."
    evidence: "Task 1 test comment: \"run 30: dropped ticks stretched a 30s cycle to 126s\"; 6b3b Execution Notes, run 30 slow: \"The pinger visits slots on a time.Ticker, which drops ticks under load, and its Send has no write deadline, so which of the two stalled it is open\" and \"loadgen 3.02 + 0.89 + 2.60 cores of 8\"; Implementation choice 1: \"a write deadline on the pinger's Send ... not chosen. The diagnostics come first\"; clientconn/conn.go:102-104 Send holds wmu over an undeadlined WriteFrame."
    disposition: accepted
    disposition_rationale: "write deadline on the Ping Send besides the catch-up; ping_send_timeouts; comment says the cause is open (see dispositions file)"
  - id: O2
    category: scope
    severity: high
    claim: "Run 30 shows that the spec's first conntrack option (nf_conntrack_max >= 2 x cap) does not survive the spec's own handshake-flood test. A flood's TIME_WAIT entries (rate x 120s) fill a 2 x cap table long before the limiter acts, because conntrack counts a connection before the bucket or a slot sees it. The plan untracks the host and says the spec needs no change, so the finding goes unrecorded."
    evidence: "Spec l.76: \"nf_conntrack_max >= 2 x cap, or conntrack disabled for the gateway port (NOTRACK ...)\"; run 30: 61k/s flood, \"the 4,194,304-entry table\" full with garbage's TIME_WAIT entries (120s); After this plan: \"The spec: nothing here changes it\"; Global Constraints: the gateways are not passed -conntrack-notrack because \"nf_conntrack_max (4,194,304) passes the check anyway\"."
    disposition: accepted
    disposition_rationale: "spec follow-up #125 beside #97/#101; After this plan points to it (see dispositions file)"
  - id: O3
    category: specification quality
    severity: medium
    claim: "<phase>_conntrack gates only that the table did not near overflow (peak <= 80% of 4.19M), not that NOTRACK works. A rule shape that leaves the stack's flows tracked passes storm (200k connections, ~5% of max) and slow, so the check Review Focus 1 relies on is the ungated, hand-read 'peak far below the phase's connections'."
    evidence: "Implementation choice 3: \"passes if the peak is <= 80% of nf_conntrack_max\"; Review Focus 1: \"If the shape is wrong, the table fills again as in run 30. <phase>_conntrack ... catches that. Run 30b also reads its peak against the population\"; Task 5 Step 2: \"far below them (Review Focus 1)\"; notrack-check checks that the rules are present (iptables -C), not what they do."
    disposition: accepted
    disposition_rationale: "conntrack check also gates the peak against the phase's connections (see dispositions file)"
  - id: O4
    category: risk
    severity: medium
    claim: "The loadgen-bound threshold (85% of loadgen's CPUs) sits on run 30's measured 86%. Whether storm_rate passes ('loadgen-bound') or fails ('short') can therefore turn on about 1% of CPU noise, with identical gateway behaviour either way."
    evidence: "Review Focus 4: \"85% of loadgen's CPUs was run 30's 6.88 of 8 (86%), so storm sits on the edge\"; phase_storm: `\"loadgen-bound\" if d[\"u\"][\"loadgen\"][\"cores\"] >= 0.85 * d[\"cpus\"] else \"short\"`; the usage window runs from lg_start to the settled wait, past the ~12s ramp."
    disposition: accepted
    disposition_rationale: "loadgen-bound reads loadgen's CPU over the ramp window, 85% kept (see dispositions file)"
  - id: O5
    category: alternatives
    severity: medium
    claim: "The plan accepts that the spec's 20k/s storm on one gateway's limiter is never measured on Tier 2. It doesn't consider removing the bound in the storm phase itself: there only gw-0 runs, using 5.42 of the gateway set's 12 CPUs, so loadgen could take the idle half for that phase without changing any size."
    evidence: "lib.sh:27-29 T2_CPUS_GATEWAY 4-9,16-21 (12), T2_CPUS_LOADGEN 0-3,12-15 (8); run 30 storm: \"Loadgen 6.88 cores of its 8 CPUs, gw-0 5.42\"; phase_storm: STACK_GATEWAYS=1; Global Constraints: \"floods' phases, sizes and defaults are unchanged (#122)\"; #124.1: loadgen-bound \"is not a gateway failure\"."
    disposition: accepted
    disposition_rationale: "named as rejected: CPU sets unchanged, gw-0 would change (see dispositions file)"
  - id: O6
    category: specification quality
    severity: medium
    claim: "garbage_limited goes back to the gate 6b3b's O3 rejected (no_slot + no_token > 0) without citing it. When bound_by is 'slots', as run 30 expects on this host, the token bucket is never exercised, but unlike storm's loadgen bound nothing goes to not_covered. floods can pass while the bucket half of the spec's handshake-flood claim is untested."
    evidence: "6b3b objections O3 (accepted): \"garbage_limited passes on no_slot alone, which the semaphore's Erlang loss produces at almost any flood rate, so it cannot fail\"; this plan's header: \"on the Tier 2 host the slots bind first (~16k DH/s on 3, run 30)\"; phase_storm adds t2_not_covered for loadgen-bound, phase_garbage adds none."
    disposition: accepted
    disposition_rationale: "not_covered when every gateway's bound_by is slots; cite 6b3b O3 (see dispositions file)"
  - id: O7
    category: specification quality
    severity: medium
    claim: "Run 30b expects 30/30, but two of run 30's failures have no change meant to fix them: garbage_delivered (SYN loss, cause open, gate unchanged) and slow_held (slowhs max 7,782ms against 7.5s). Review Focus 5's decision tree also leaves out NOTRACK itself as a cure. Conntrack can drop SYNs without a full table (insert_failed, drop in /proc/net/stat/nf_conntrack), and that was never snapshotted, so a pass in 30b would leave run 30's SYN loss misattributed."
    evidence: "Task 5 Step 2: \"Expected: 30/30\"; run 30: \"Failed: storm_rate, garbage_delivered, garbage_limited, slow_held, slow_legit, slow_heartbeats\", \"slow_held failed on slowhs max_ms 7,782\", \"The SYN loss was not conntrack (no table full before 23:13:26)\"; Review Focus 5: \"If it fails again with softnet drops ... With neither, it is loadgen's in-flight cap ... The gate is not changed here\"."
    disposition: accepted
    disposition_rationale: "per-check expectations, NOTRACK branch, /proc/net/stat/nf_conntrack snapshots (see dispositions file)"
  - id: O8
    category: risk
    severity: low
    claim: "Review Focus 2's firewall check reads only `iptables -S INPUT`. A host ruleset in native nftables (or a firewalld zone) that accepts only established/related traffic, with no lo exemption, is invisible to it, and it would drop UNTRACKED loopback traffic."
    evidence: "Review Focus 2: \"`sudo iptables -S INPUT` must show lo accepted before any --ctstate rule ... or a policy of ACCEPT\"; Task 5 Step 1.1 repeats the same single command."
    disposition: accepted
    disposition_rationale: "nft list ruleset added to the firewall check (see dispositions file)"
  - id: O9
    category: implementation
    severity: low
    claim: "The pinger stamps every due client's lastPing and pingAt with one `now` taken before the burst. After a blocked Send, or across Review Focus 3's 50k-Ping burst, the clients later in the list are stamped before their Ping leaves. ping_gap_max_ms then understates the silence the gateway saw, and their Pong RTT includes loadgen's own send time, which pong_rtt's < 10s gate charges to the gateway."
    evidence: "light.go run loop: `now := time.Now().UnixNano()` once, then per client `lc.pingAt.Swap(now)`, `p.rec.PingGap(time.Duration(now - lc.lastPing.Swap(now)))`, then `p.send(lc)`; Review Focus 3: \"up to 50k Pings in one burst per shard\"; heartbeats(): `d[\"r\"][\"pong_rtt\"][\"max_ms\"] < 10000`."
    disposition: accepted
    disposition_rationale: "per-client timestamp just before each Send (see dispositions file)"
  - id: O10
    category: risk
    severity: low
    claim: "The guard now requires NOTRACK for every load scenario, so reruns of capacity, gwloss and slowlogic run under different kernel conditions from their README results (runs 18-29, conntrack tracking). The plan doesn't note that comparability break."
    evidence: "Task 3 Interfaces: \"t2_require_host_setup refuses (exit 2) when NOTRACK is missing ... Every load scenario already calls it\"; Task 5 Step 3 records only floods."
    disposition: accepted
    disposition_rationale: "roadmap comparability note (see dispositions file)"
---

# Plan 6b3b2: objections

## O1 — premise — high

### Claim

Task 1's catch-up fixes one of two possible causes of run 30's pinger stall, and the plan treats it as the known cause. Run 30's record names two candidates, ticks dropped by `time.Ticker` and a `Send` with no write deadline, and calls the question open. The figures lean towards the second. A Ticker drops ticks only when one loop iteration takes over 100ms. With 400k clients over 8 shards and 300 slots, an iteration sends about 167 Pings. The light instance used 3.02 cores, so it wasn't starved. Plain CPU starvation doesn't easily stretch a 30s cycle to 126s.

A `Send` blocked on a full socket buffer stops the shard's goroutine inside `p.send`. Catch-up only runs after `Send` returns. So for a blocked `Send` the plan adds diagnostics (`ping_send_max_ms`) and no fix. Meanwhile one blocked client still delays its whole shard, an eighth of the population.

### Evidence

> Task 1 test comment: "run 30: dropped ticks stretched a 30s cycle to 126s, and the gateways closed 177,743 clients for their heartbeat"

> 6b3b Execution Notes, run 30: "The pinger visits slots on a `time.Ticker`, which drops ticks under load, and its `Send` has no write deadline, so which of the two stalled it is open." … "loadgen 3.02 + 0.89 + 2.60 cores of 8."

> Implementation choice 1: "*Alternative:* a write deadline on the pinger's `Send` (needs a `clientconn` API), not chosen. The diagnostics come first."

`clientconn/conn.go:102-104`: `Send` takes `wmu` and calls `WriteFrame` with no deadline.

### Why this matters

If `Send` was the cause, and NOTRACK doesn't remove what blocked it, run 30b fails `slow_heartbeats` again and costs another host run before the real fix. The test comment would also lock in an unproven cause as the story the code tells. At the least, the comment should say "the cause is open". The plan should also say what run 30b's `ping_send_max_ms` leads to: probably a write deadline, or skipping a client whose previous Ping is still being written.

## O2 — scope — high

### Claim

Run 30 is evidence against one branch of a spec requirement, and the plan routes around it. The spec allows a gateway node either to keep `nf_conntrack_max ≥ 2 × cap` or to use NOTRACK. Conntrack creates an entry for every TCP connection the kernel sees, before the gateway's limiter refuses it, and keeps it 120s in TIME_WAIT. A node with a 600k cap has a 1.2M table under the first option. Garbage connections at a sustained ~10k/s fill that table with TIME_WAIT entries alone, and after that it drops new SYNs, legitimate logins included. The bucket and the slots can't prevent this, because they act after the connection exists. Run 30 hit exactly this at 4.19M.

### Evidence

> Spec l.76: "per gateway node, `nf_conntrack_max` ≥ 2 × cap, **or** conntrack disabled for the gateway port (`NOTRACK` / CNI equivalent)"

> Run 30: "the host's conntrack table full 23:13:26–23:14:30 with `garbage`'s TIME_WAIT entries"; the flood reached 61k/s.

> After this plan: "**The spec:** nothing here changes it."

> Global Constraints: the gateways are not passed `-conntrack-notrack`, because "`nf_conntrack_max` (4,194,304) passes the check anyway, and the flag would only skip a check that holds."

### Why this matters

The floods test exists to show that a gateway node survives a flood. With NOTRACK on the host, Tier 2 can no longer show that the other spec-sanctioned setup does not survive one. The self-check would report such a node Ready. The spec has a queue for follow-ups like this (#97, #101 wait for the next revision), so recording it costs one line. Options: size the table from the flood rate (rate × TIME_WAIT timeout plus 2 × cap), make NOTRACK the only option for gateway ports, or say that the 2 × cap option assumes no flood.

## O3 — specification quality — medium

### Claim

The new check proves the table did not overflow. It doesn't prove the rules work. Wrong rules leave every stack connection tracked: storm's 200k and slow's few hundred thousand stay under 80% of 4.19M and pass. Only garbage's churn would reach the limit. `notrack-check` confirms the rules exist (`iptables -C`), not that packets match them. "Far below the phase's connections" is the real test of NOTRACK, and it is a human reading in Task 5.

### Evidence

> Implementation choice 3: "The check passes if the peak is ≤ 80% of `nf_conntrack_max`"

> Review Focus 1: "`<phase>_conntrack` (peak under 80% of max) catches that. Run 30b also reads its `peak` against the population: with NOTRACK working, the peak should stay far below the phase's connections"

### Why this matters

6b3c and every later run inherit a check named `_conntrack` that passes while NOTRACK silently does nothing in phases without heavy churn. A second bound, such as a peak under some fraction of the phase's population or a fixed host baseline, would make the check prove what Review Focus 1 says it proves.

## O4 — risk — medium

### Claim

The verdict threshold sits on run 30's measured value, so run 30b's `storm_rate` outcome is close to a coin flip on loadgen's CPU average. At 86%, `storm_rate` passes as `loadgen-bound`. At 84%, with the same `new_per_s` and the same gateway, it fails as `short`. The average also covers the settle time after the ramp, which pulls it down and adds noise from things outside the storm.

### Evidence

> Review Focus 4: "85% of loadgen's CPUs was run 30's 6.88 of 8 (86%), so storm sits on the edge."

> `phase_storm`: `"met" if … else "loadgen-bound" if d["u"]["loadgen"]["cores"] >= 0.85 * d["cpus"] else "short"`

### Why this matters

A failing check that nobody can attribute to the gateway is what this plan is meant to remove. A threshold chosen to just include the last observation will misclassify the next one about half the time. Options: measure CPU over the ramp only, use a lower threshold with a stated margin, or record the verdict without gating on the boundary.

## O5 — alternatives — medium

### Claim

The storm phase runs one gateway on a 12-CPU set and left about 6.6 of those CPUs idle in run 30. Loadgen ran at its 8-CPU limit. Pinning loadgen to its own set plus the unused half of the gateway set, in this phase only, would probably reach the spec's 20k new clients/s. No size changes, so #122 stands. The plan doesn't consider this. It turns the bound into a pass with a `not_covered` entry, so the test the spec asks for is never run.

### Evidence

> `lib.sh:27-29`: `T2_CPUS_GATEWAY:=4-9,16-21` (12 CPUs), `T2_CPUS_LOADGEN:=0-3,12-15` (8).

> Run 30 storm: "Loadgen 6.88 cores of its 8 CPUs, gw-0 5.42 (12 loops, 6 slots)."

> `phase_storm`: `STACK_GATEWAYS=1`.

### Why this matters

This would be the third scenario recorded as loadgen-bound (b_stress #110, storm here). Each such entry means the Tier 2 host doesn't test the spec claim. Note that the gateway's `GOMAXPROCS` follows its CPU set, so shrinking gw-0 to 6 CPUs changes the gateway under test as well. That trade-off is the user's, but the plan should name it as rejected, not leave it out.

## O6 — specification quality — medium

### Claim

#124.2 brings back the gate the 6b3b review rejected as unable to fail (O3, accepted). The plan doesn't cite that disposition or explain what has changed. The new header says the slots bind first on this host, so under the expected outcome the bucket refuses nothing and is never exercised. The storm phase sends its untested half to `not_covered`. The garbage phase doesn't, although its half is equally untested.

### Evidence

> 6b3b O3 (accepted): "garbage_limited passes on no_slot alone, which the semaphore's Erlang loss produces at almost any flood rate, so it cannot fail"

> This plan's header: "on the Tier 2 host the slots bind first (~16k DH/s on 3, run 30), so which part bound and the DHs per second are recorded (#111's input)"

### Why this matters

The user ruled #124.2 knowingly, and this objection doesn't ask to reverse the ruling. But the README row Task 5 writes ("garbage at 80k/s …") will read as a full handshake-flood pass. A `not_covered` line when every gateway's `bound_by` is `slots`, plus a citation of 6b3b O3, would keep the record honest.

## O7 — specification quality — medium

### Claim

The expected result (30/30) assumes failures will pass that the plan doesn't address. `garbage_delivered` failed on SYN loss with the cause open and the gate unchanged. `slow_held` failed on slowhs `max_ms` 7,782 against its 7.5s bound. Neither is said to have been caused by the full table, and slowhs started before the light clients' trouble. The decision tree for SYN loss also leaves out NOTRACK itself. "No table full" doesn't mean conntrack dropped nothing: at 61k new flows/s, insert races and early drops show up in `/proc/net/stat/nf_conntrack` (`insert_failed`, `drop`), which run 30 didn't read.

### Evidence

> Task 5 Step 2: "Expected: 30/30, run 30's 27 plus …"

> Run 30: "`slow_held` failed on slowhs `max_ms` 7,782 (p99 6,153)"; "The SYN loss was not conntrack (no `table full` before 23:13:26). Its cause is open."

> Review Focus 5: "If it fails again with softnet drops on the gateway's CPUs, `netdev_max_backlog` is the next suspect. With neither, it is loadgen's in-flight cap (`skipped`)."

### Why this matters

If `garbage_delivered` passes in 30b, the tree has no branch for that, and the record will likely leave the SYN loss unexplained or credit the wrong cause. If `slow_held` fails again, the run fails on a check this plan never examined. Either state the expectation per check, or explain why run 30's failures should clear.

## O8 — risk — low

### Claim

The only firewall pre-check reads iptables' INPUT chain. On a kernel 6.8 host, a native nftables ruleset or a firewalld zone can hold a `ct state established,related accept` policy that `iptables -S` doesn't show. UNTRACKED packets don't match that policy and would be dropped.

### Evidence

> Review Focus 2 and Task 5 Step 1.1: "`sudo iptables -S INPUT`: `lo` is accepted before any `--ctstate` rule, or the policy is `ACCEPT`."

### Why this matters

The failure would look like run 30's SYN loss: dial timeouts from the first dial. That cost a run once already. Adding `sudo nft list ruleset` to the check covers it.

## O9 — implementation — low

### Claim

One timestamp, taken before the loop, stands for every Ping in a burst. The `ping_gap` gate and the Pong-RTT gate both read that timestamp. Review Focus 3 expects bursts of up to 50k Pings after a stall. In such a burst, or behind a blocked `Send`, the clients late in the list get a `lastPing` earlier than their real send time. That understates the gap the gateway saw. Their `pingAt` is early by the same amount, so loadgen's own send queue counts toward `pong_rtt`, and `pong_rtt.max_ms < 10000` charges it to the gateway.

### Evidence

> `light.go` (after Task 1): `now := time.Now().UnixNano()` once, then for each `lc`: `lc.pingAt.Swap(now)`, `p.rec.PingGap(time.Duration(now - lc.lastPing.Swap(now)))`, `s := time.Now(); p.send(lc)`.

### Why this matters

The diagnostics are meant to tell a loadgen problem from a gateway problem (Review Focus 3), and here a loadgen delay shows up in a gateway gate. Stamping each client just before its own `Send` costs nothing, since `time.Now()` is already called there.

## O10 — risk — low

### Claim

From now on, every load scenario runs with the stack's flows untracked, including scenarios whose README numbers were measured with tracking. Conntrack costs softirq CPU per packet and per new flow. Reruns of capacity, gwloss (188s) or slowlogic would be measured under different conditions, and the plan doesn't record that.

### Evidence

> Task 3 Interfaces: "`t2_require_host_setup` refuses (exit 2) when NOTRACK is missing or the base is outside the range. Every load scenario already calls it."

### Why this matters

A later regression or improvement in those scenarios could come from NOTRACK and not the code. One line in the roadmap's Tier 2 notes would avoid misreading it.

## Explicitly not objecting to

- **The gauge's inclusion (#124.4):** nothing in this plan reads it, but it implements a line the spec already has, and the user ruled on it explicitly.
- **The PREROUTING rules being redundant on loopback:** the conntrack entry from OUTPUT normally stays on the skb, so PREROUTING skips the packet. The extra rules cost nothing and cover kernels where that differs.
- **Pinging each slot once, at most one cycle's worth, after a stall:** that is the right contract. More would flood the gateway, and less would leave clients unpinged.
- **The hard-coded 17000–17199 range:** a test checks that goim-netdrop and lib.sh agree, and the guard refuses a base outside the range. load-probe (17200) and stack_test (17500) don't call the guard.
- **host-setup.sh now failing without iptables:** it runs under `set -euo pipefail`, and NOTRACK can't be applied without iptables, so failing beats the old warning.
- **No way to remove the NOTRACK rules:** a reboot clears them and the ports belong to the Tier 2 stack. An `un-notrack` command would be scope without a user.
- **A second Recorder mutex acquisition per Ping:** about 13k/s more acquisitions at 400k clients is negligible next to the Send syscall.
- **The `(HB + 5)` gap bound:** it sits well inside the gateway's `2 × HB + 10` close, and the plan explains why.
