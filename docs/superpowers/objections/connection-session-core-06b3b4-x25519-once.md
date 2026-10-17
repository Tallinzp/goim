---
spec: docs/superpowers/plans/2026-10-17-connection-session-core-06b3b4-x25519-once.md
date: 2026-10-17
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: risk
    severity: medium
    claim: "Each *wire.Initiator now keeps a per-handshake suite with a parsed *ecdh.PrivateKey until Finish: measured 614 → 1,014 bytes of live heap per Initiator (+65%). Loadgen's precomputer keeps one Initiator per logged-in SDK client (600,000 in gwloss's b_stress), about +240MB of live heap on a loadgen that already hit its memory limit there. The plan says loadgen changes nothing and verifies only floods, which runs light mode and doesn't precompute."
    evidence: "Architecture: \"Their signatures don't change, so the gateway (`internal/gateway/tcp`), the SDK, loadgen and `clientconn` change nothing\"; x25519.go `suite` holds `keys [2]*ecdh.PrivateKey` and `raw [2][]byte`; noise.go `type Initiator struct { hs *noise.HandshakeState ... }` (hs holds the suite); internal/loadgen/precompute.go `type prepared struct { in *wire.Initiator; frame []byte }`, sdkclient.go `pre atomic.Pointer[prepared]`, `held atomic.Pointer[handshake]`; runner.go `newPrecomputer(r, cfg.Clients, ...)`; roadmap plan 6 row: b_stress runs 27 and the 19GiB run, GC at loadgen's memory limit; measured on the dev machine: 20,000 Initiators, HeapAlloc after GC, 614 B each at 7424f17, 1,014 B each at proto-6b3b4 566e77d."
    disposition: accepted
    disposition_rationale: "record the per-Initiator cost (~240MB at 600k) in Review Focus 6 and a heap benchmark; no design change (see dispositions file)"
  - id: O2
    category: scope
    severity: medium
    claim: "The plan reconsiders only the floods README row, but other recorded results were set by the doubled X25519 too and are not marked as measured before the fix: the 600k SIGKILL row (179s, an upper bound set by loadgen at 12 of 14 cores with X25519 43–47% of loadgen), #110's loadgen-bound ruling, and #111's test that handshake concurrency 4 collapsed to 594s, which is why the default stays. Nothing schedules a gwloss rerun or records that these numbers are pre-#130."
    evidence: "Task 2 Step 2: \"The README's floods row quotes runs 30c–30h, all measured before this change. Whether run 30i replaces or joins those numbers is the user's call\"; README row \"A 600k gateway SIGKILLed\": \"These times are an upper bound: on one host the load generator's 600k SDK clients used 12 of its 14 cores in the storm's first 30 s\"; roadmap plan 6 row: b_stress \"loadgen CPU-saturated (13.6 of 14 cores, X25519 43%\", run 25 \"X25519 47% of loadgen\", \"handshake concurrency 4 (`d218483`) collapsed to 594s, so the default stays\"; *After this plan* names only 6b3c, #111's no_slot shares and fixed-base keygen."
    disposition: accepted
    disposition_rationale: "a full gwloss rerun in Task 2 after run 30i; records measured before #130 named (see dispositions file)"
  - id: O3
    category: risk
    severity: medium
    claim: "In run 30i's slow phase, slow-auth runs on the gateways' CPU set and completes a real handshake per connection, so its client-side crypto halves too. The gateway set's busy share falls for two reasons at once. The plan's 'What to read' compares busy per set with run 30e, so a valid 30i can't say how much of the gain is the gateways'. Run 30h showed that this shared set, not the gateways, was the edge."
    evidence: "Task 2 Step 1: \"slow-auth on the gateways' set (the default), as in run 30e\"; \"the gateway set's ramp busy share and the concurrency limit's refused share (51% in 30e) should both fall\"; What to read: \"`slow.window.ramp`/`.hold` busy per set\"; Architecture: \"An NK handshake costs 3 multiplications on each side (e, es, ee) instead of 6\"; roadmap #129 run 30h: \"gateway set 51.5% busy (30e: 88.1%) ... the 5,500–6,000/s edge was the shared CPU set, not the gateways\"; roadmap #126 records per-flood cores (\"light 4.42, slow-auth 2.68, slow-handshake 0.83 cores\")."
    disposition: accepted
    disposition_rationale: "What to read compares the gateway processes' and slow-auth's cores apart against run 30e (see dispositions file)"
  - id: O4
    category: specification quality
    severity: medium
    claim: "The only Tier 2 evidence that the fix works in the deployed binaries, on the host's CPU, is a profile, and the plan makes it optional. Without it, run 30i's numbers mix the halved crypto with run-to-run variance (runs 30c–30h gave the same scenario's refused share at 27–57%), and loadgen's benefit, which the storm reading relies on, isn't measured at all."
    evidence: "Task 2 Step 1: \"Optionally, if the gateways' CPU profile is wanted again, take a 30s profile from gw-0's admin port during the garbage phase\"; What to read: \"The profile, if taken: `x25519ScalarMult`'s share against 67.7%\"; Storm: \"`new_per_s` (15,962, loadgen-bound) may rise, since loadgen's own handshakes cost half\"; Size: the benchmarks are from \"the dev machine (AMD EPYC 7742, 2 cores)\"; roadmap #108: the Tier 2 host's cores are Zen 5 / Zen 5c."
    disposition: accepted
    disposition_rationale: "gw-0 garbage-phase and loadgen storm profiles both required, with the NewPrivateKey caller check (see dispositions file)"
  - id: O5
    category: specification quality
    severity: low
    claim: "Task 2 compares run 30i's slow phase with run 30e and says it is run 'as in run 30e', but run 30e was a slow-only probe (#129) and run 30i runs the slow phase after storm and garbage on the same stack. The comparison table's baseline differs in more than the code."
    evidence: "Task 2 Step 1: \"Run `GOIM_TEST_TIER=2 FLOODS_RATE=7500 make test-tier2 SCENARIO=floods`. That is all three phases, with the slow phase's light clients at 7,500/s and slow-auth on the gateways' set (the default), as in run 30e\"; roadmap #129: \"slow-only probe runs at higher light rates beside the same floods\"; #128: run 30d was `FLOODS_PHASES=slow`."
    disposition: accepted
    disposition_rationale: "Task 2 notes that run 30e was slow-only; difference accepted, no extra run (see dispositions file)"
---

# Objections: Plan 6b3b4 (compute each X25519 once)

Reviewed: `docs/superpowers/plans/2026-10-17-connection-session-core-06b3b4-x25519-once.md` at `7424f17`, the prototype `proto-6b3b4` (`566e77d`), and the sources it relies on: Go 1.27.1's `crypto/ecdh/x25519.go`, `golang.org/x/crypto@v0.54.0/curve25519`, `github.com/flynn/noise@v1.1.0`. The plan's mechanism checks out: `x25519` in x/crypto calls `ecdh.X25519().NewPrivateKey` (which runs `x25519ScalarMult` against the basepoint at once) and then `ECDH`, and `dh25519.GenerateKeypair` calls `X25519(priv, Basepoint)`, so 2 multiplications per DH and per key generation. Since Go 1.26, `ecdh`'s `GenerateKey` replaces a custom reader with the system's (`rand.CustomReader`, unless `GODEBUG=cryptocustomrand=1`), as Review Focus 5 says.

## O1 — risk — medium

### Claim

Before the change, a pending `Initiator` held flynn/noise's handshake state with the ephemeral as two 32-byte slices. After it, the state also holds a `*suite` with a parsed `*ecdh.PrivateKey` (its private slice, a `*PublicKey` and its slice) and the suite's own fields. I measured it on the dev machine: 20,000 Initiators kept alive, `HeapAlloc` after a GC, **614 bytes each at `7424f17`, 1,014 at `proto-6b3b4`**, so about +400 bytes (+65%).

On a phone that is nothing: one handshake at a time. In loadgen it is not. Since #106, the precomputer prepares each logged-in SDK client's next handshake and keeps it until the first mark, and #109 keeps an unanswered one too. In `gwloss`'s b_stress that is 600,000 Initiators, about +240MB of live heap.

### Evidence

> Architecture: "Their signatures don't change, so the gateway (`internal/gateway/tcp`), the SDK, loadgen and `clientconn` change nothing."

`internal/loadgen/precompute.go`: `type prepared struct { in *wire.Initiator; frame []byte }`. `sdkclient.go`: `pre atomic.Pointer[prepared]`, `held atomic.Pointer[handshake]`. The roadmap's plan 6 row: b_stress ran into GC at loadgen's memory limit (run 27, the diagnostic run, the 19GiB run) before 16GiB and the pinger timer fixed it. Run 30i, the plan's only Tier 2 check, runs `floods`, whose clients are light mode, and precompute needs sdk mode (`runner.go`: "precompute needs sdk mode").

### Why this matters

The plan says loadgen is unaffected and doesn't check the one scenario where this shows. 240MB is about 1.5% of loadgen's 16GiB, so it probably fits. But b_stress has already failed on loadgen memory once, and the next `gwloss` run would show a higher `loadgen_heap` with nothing in the record to explain it. Answers:
- **accept-as-stated**: write the measured per-Initiator cost and the 600k figure into Review Focus or the Execution Notes, so the next b_stress reader has it.
- **revise-spec**: have the precomputer keep less (for example only the frame and the ephemeral's bytes, building the Initiator at send time), or keep `Initiator` lean another way.
- **add-test**: a heap-per-Initiator benchmark next to the timing ones.
- **consciously-carry**: loadgen pays 240MB for half its handshake CPU, which b_stress needed more.

## O2 — scope — medium

### Claim

The plan only asks what happens to the floods README row. But the doubled X25519 also set other recorded results, which the plan neither reruns nor marks as measured before #130:
- the README's "A 600k gateway SIGKILLed" row (179s, an upper bound because loadgen used 12 of 14 cores, with X25519 at 43–47% of loadgen in the earlier b_stress profiles);
- #110's ruling that `b_stress_storm_real` is loadgen-bound;
- the handshake concurrency trial in the plan 6 row ("handshake concurrency 4 collapsed to 594s, so the default stays"), which is what the spec's default slot count rests on, and #111's parking.

### Evidence

> Task 2 Step 2: "The README's floods row quotes runs 30c–30h, all measured before this change. Whether run 30i replaces or joins those numbers is the user's call."

> README: "These times are an upper bound: on one host the load generator's 600k SDK clients used 12 of its 14 cores in the storm's first 30 s."

The roadmap's plan 6 row: "loadgen CPU-saturated (13.6 of 14 cores, X25519 43%", run 25 "X25519 47% of loadgen", and "handshake concurrency 4 (`d218483`) collapsed to 594s, so the default stays and 188s is b_stress's measured result". *After this plan* lists 6b3c, #111's `no_slot` shares and fixed-base key generation, nothing else.

### Why this matters

After this plan, the README and the roadmap mix numbers from two handshake costs without saying which is which. The b_stress number is the most affected: its bound was loadgen's handshake CPU. The slot count was checked once, at the old cost. With the slot hold halved, the reason given for keeping it may no longer hold. At minimum, *After this plan* should name `gwloss` (and the concurrency trial) as measured before #130 and say whether a rerun belongs before 6c or with Tier 3.

## O3 — risk — medium

### Claim

Slow-auth completes a real NK handshake per connection (initiate, the server's reply, finish) and then never authenticates. Since run 30e and by default it runs on the gateways' CPU set, and this plan halves its client-side crypto as well as the gateways'. Run 30i's gateway set will be less busy for two reasons at once. The plan's reading list compares busy share per set with run 30e. That can't tell the gateways' gain from the co-located flood's, which is exactly the confound run 30h exposed (the 5,500–6,000/s edge was the shared set, not the gateways).

### Evidence

> Task 2 Step 1: "slow-auth on the gateways' set (the default), as in run 30e" … "the gateway set's ramp busy share and the concurrency limit's refused share (51% in 30e) should both fall."

> What to read: "`slow.window.ramp`/`.hold` busy per set."

> Architecture: "An NK handshake costs 3 multiplications on each side (e, es, ee) instead of 6."

Roadmap #129, run 30h: "gateway set 51.5% busy (30e: 88.1%) … the 5,500–6,000/s edge was the shared CPU set, not the gateways." The harness already records cores per process (#126: "light 4.42, slow-auth 2.68, slow-handshake 0.83 cores").

### Why this matters

If run 30i comes back `valid`, the obvious reading is "the gateways got cheaper, so the edge moved". Part of it will be slow-auth getting cheaper on the same CPUs. The README's slow-phase edge sentence would then be rewritten on a mixed reading. The fix is small: name the gateway processes' cores and slow-auth's cores, each against run 30e, in *What to read*, and say which of them the comparison is about.

## O4 — specification quality — medium

### Claim

The plan's numbers for the gain come from the dev machine (EPYC 7742, Zen 2). On Tier 2 the only direct evidence that the deployed binaries do each X25519 once is a profile: `x25519ScalarMult`'s share, and the callers (`suite.DH` → `ECDH` with no `NewPrivateKey` under it). The plan makes that profile optional. The other readings are indirect and noisy. The concurrency limit's refused share moved from 27% to 57% across runs 30c–30h with the rate alone. The storm reading expects loadgen's handshakes to "cost half" with no measurement of loadgen.

### Evidence

> Task 2 Step 1: "Optionally, if the gateways' CPU profile is wanted again, take a 30s profile from gw-0's admin port during the garbage phase."

> What to read: "The profile, if taken: `x25519ScalarMult`'s share against 67.7%."

> Storm: "`new_per_s` (15,962, loadgen-bound) may rise, since loadgen's own handshakes cost half."

Size: benchmarks "on the dev machine (AMD EPYC 7742, 2 cores)"; roadmap #108: the host's cores are Zen 5 and Zen 5c.

### Why this matters

#130 was decided on a profile. If the run can be recorded without one, a disappointing or ambiguous run 30i has no way to tell "the fix didn't reach the binary or doesn't pay on Zen 5c" from "the gain is real but something else binds". The profile costs one `curl` during a phase the user is already watching. Make the gw-0 garbage-phase profile required, and add one from loadgen during the storm (loadgen already serves pprof, #106/#108).

## O5 — specification quality — low

### Claim

Task 2 says run 30i's slow phase is set up "as in run 30e" and compares it with run 30e. But run 30e was one of #129's slow-only probes, while run 30i runs the slow phase third, after storm and garbage, on the same stack (gateways that just refused 7.2M garbage handshakes, Logic and Redis after a 200k storm). The code isn't the only difference between the two runs.

### Evidence

> Task 2 Step 1: "That is all three phases, with the slow phase's light clients at 7,500/s and slow-auth on the gateways' set (the default), as in run 30e."

Roadmap #129: "slow-only probe runs at higher light rates beside the same floods"; #128: run 30d was `FLOODS_PHASES=slow`.

### Why this matters

This is minor, because the phases have their own boundaries (P6a2bI-3). But the plan should say that the baseline is a slow-only run, and either accept the difference or add a slow-only run 30i-b at 7,500/s. That gives a like-for-like comparison and costs about 4 minutes.

## Explicitly not objecting to

- **The premise (#130).** The cause is traced in the sources (x/crypto v0.54.0's `x25519` builds an `*ecdh.PrivateKey` per call, and Go 1.27.1's `NewPrivateKey` runs a basepoint multiplication at once), and the profile and the benchmarks agree. The user has decided it.
- **Wire compatibility.** The suite name, DH length and bytes are DH25519's. `TestHandshakeBytesMatchDH25519` seeds both suites the same way and crosses old and new sides, and nothing about the Config changes except `Random: nil`, which flynn/noise already maps to `crypto/rand`.
- **Concurrent use of the kept static key.** `ecdh.PrivateKey` is immutable, each `suite` belongs to one handshake, and the `-race` test exercises the gateway's sharing.
- **The low-order and length refusals.** `ecdh`'s `ECDH` refuses an all-zero result and `NewPublicKey` a non-32-byte key, and the direct `suite.DH` test pins it beside DH25519 (the plan already learned that the handshake-only test missed the mutation).
- **The `newPrivateKey` package variable swapped in tests.** No test in `internal/wire` calls `t.Parallel`, so the swap can't race a concurrent handshake test.
- **`Key` changing from an alias to a struct.** Outside `internal/wire`, nothing builds a `Key` literal, names `noise.DHKey`, or uses `wire.Suite` (grep). The Go clients and the gateway all go through `GenerateKey`/`ParsePrivate`/`LoadKeyDir`.
- **The per-handshake allocation of a `suite` on the gateway.** A few hundred bytes freed after `Respond` costs nothing next to the ~218µs of crypto it saves.
- **Leaving the gateway's limits alone.** Global Constraints keep slot count and bucket fixed and send any change to the user (#111, #116), which is the right separation. O2 only asks that the old trial's basis be recorded.
- **Not taking the fixed-base key generation now.** It is a further, separable gain. *After this plan* makes it conditional on a later profile, which O4 would supply.
