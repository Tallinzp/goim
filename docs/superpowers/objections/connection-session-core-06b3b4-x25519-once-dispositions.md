# Dispositions: Plan 6b3b4 review (2026-10-17)

Objections: `connection-session-core-06b3b4-x25519-once.md`. The user decided them with AskUserQuestion: O1–O4 first, then O5 together with where O2's rerun goes. All five are accepted. No new numbered choice is needed. #130 gets "reviewed: O1–O5 accepted", and Task 2 grows by a full `gwloss` run (O2). The changes below go into the plan. Task 1's code doesn't change, except for O1's benchmark, which is re-verified in `proto-6b3b4` and replayed into the plan byte for byte before execution.

## O1 — accepted: record the Initiator's memory, no design change

**Decision:** the design stays. The plan states the cost: a pending `*wire.Initiator` keeps its `suite` and the parsed ephemeral until `Finish`. On the dev machine that is 614 → 1,014 bytes of live heap each (20,000 Initiators, `HeapAlloc` after a GC). With loadgen's precompute (#106) holding one per SDK client, that is about +240MB at 600k. A benchmark reports bytes per Initiator before and after.

**Rationale:** 240MB is about 1.5% of loadgen's 16GiB limit in b_stress, and it buys half of loadgen's handshake CPU, which bound b_stress. b_stress has hit loadgen's memory limit before, though, so the number must be in the record, not found later in `loadgen_heap`.

**Plan changes to apply:**
- Review Focus: a sixth item, *Memory per pending handshake*, with the numbers above, why the suite must live until `Finish` (the `ee` DH uses the kept ephemeral), and that loadgen's precompute (`internal/loadgen/precompute.go`, `prepared.in`) and #109's `held` keep one per client.
- Architecture: "loadgen … change nothing" becomes "change no code; loadgen's precomputed handshakes each keep ~400 bytes more (Review Focus 6)".
- `bench_test.go`: a `BenchmarkInitiatorHeap` (or an equivalent heap-per-Initiator report via `b.ReportMetric`, exported API only, so it runs before and after like the others). Step 1 and Step 7 record its number. Settle the form in the prototype.
- Task 2: the `gwloss` run (O2) reads `loadgen_heap` (ramp and at_kill) against the last 16GiB b_stress runs.

## O2 — accepted: a full `gwloss` rerun in Task 2

**Decision:** after run 30i, Task 2 runs the full `gwloss` scenario (phases `b1 b2 b_stress`, the defaults) as run 30j (number at execution), at the same settings as the last full pass (`8981580`, 29/29, and the sampler rerun `271c022`). It is compared with those runs and the README's 600k SIGKILL row. *After this plan* and the roadmap mark which recorded results were measured before #130.

**Rationale:** b_stress's bound was loadgen's handshake CPU (X25519 43–47% of loadgen, 12–13.6 of 14 cores). The README's 179s is labelled an upper bound for that reason. #110's loadgen-bound ruling and the handshake-concurrency-4 trial (594s, why the default slot count stays) were also measured at the doubled cost. A rerun gives the README row a number at the current cost, and it is the only scenario that exercises O1's memory.

**Plan changes to apply:**
- Task 2 Step 1: a second user step after run 30i: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss`, the host checks as before (host-setup if rebooted, `dmesg -C`, desktop apps closed), with its expected duration taken from the last full run's record. Send back the `.report.txt`.
- What to read, gwloss: b1 as runs 21/28 (back times, `b1_slo`); b2 against run 28; b_stress `back_s` (179–188s), `storm_first30` / `loadgen_bound` and loadgen's cores in the storm, `b_stress_storm_real` (#110: does it still fail loadgen-bound?), survivors' refused handshakes (~3.9M), `loadgen_heap` (O1); a loadgen profile in the storm is already taken by `gwloss` (`profile_in_storm_s`), so read `x25519ScalarMult`'s share from it against 43–47%.
- Task 2 Step 2: record run 30j in the Execution Notes and roadmap (#130 line); the README's 600k row changes only if the user says so (both languages), as for the floods row.
- *After this plan*: name the handshake-concurrency-4 trial and #110 as measured before #130; whether to repeat the concurrency trial is the user's call after run 30j (input to #111/#116), not part of this plan.
- Plan header *Goal*/*Size*: Task 2 is two Tier 2 runs.

## O3 — accepted: the gateways' and slow-auth's cores apart

**Decision:** in run 30i's slow phase, *What to read* compares the gateway processes' cores (gw-0, gw-1) and slow-auth's cores separately against run 30e, as well as the set's busy share. The gain claimed for #130 on the gateway side is the gateways' cores at the same light rate. Slow-auth's drop is recorded as its own line.

**Rationale:** slow-auth completes a real NK handshake per connection and shares the gateway set, so its crypto halves as well. The set's busy share alone mixes the two, which is the confound run 30h exposed. The harness already records per-process cores (#126: light 4.42, slow-auth 2.68, slow-handshake 0.83).

**Plan changes to apply:**
- Task 2 Step 1, Slow bullet: "the gateway set's busy share falls for two reasons: the gateways' Respond and slow-auth's Initiate/Finish. The gateways' own cores are the measure."
- What to read, slow: gateway processes' cores and slow-auth's cores in the ramp and the hold, against run 30e; name where they come from in `result.json` (settle the field path in the prototype from a run 30e/30h result or the dry run).

## O4 — accepted: both profiles required

**Decision:** run 30i takes two profiles, both required: gw-0's CPU profile, 30s, during the garbage phase (as for run 30c), and loadgen's light instance during the storm. *What to read* reads `x25519ScalarMult`'s share in each (gw-0 against 67.7%), and that no `ecdh.(*x25519Curve).NewPrivateKey` sits under `wire.(*suite).DH` (only under `GenerateKeypair`). The gwloss run (O2) already profiles loadgen in its storm.

**Rationale:** the benchmarks are from the dev machine's Zen 2 cores. On the Tier 2 host, only a profile shows that the deployed binaries do each X25519 once. Run 30i's other readings are indirect and vary from run to run (the refused share was 27–57% across runs 30c–30h with the rate alone).

**Plan changes to apply:**
- Task 2 Step 1: replace "Optionally …" with the two required profiles, each with its exact command (the admin/control port, `seconds=30`, the timing within the phase) and `go tool pprof -top` plus `-peek 'x25519ScalarMult|NewPrivateKey'` output to send back. If floods.sh can take them itself (as gwloss does), prefer that, but that would be a code change to `floods.sh`: decide in the prototype, and keep it within Task 2 if done by hand.
- What to read: the profile line becomes required, for both profiles, with the caller check above.
- *After this plan*, fixed-base key generation: its trigger is these profiles.

## O5 — accepted: say that run 30e was slow-only

**Decision:** Task 2 says that run 30e, the slow phase's baseline, was a slow-only probe (`FLOODS_PHASES=slow`), while run 30i runs the slow phase after storm and garbage on the same stack. The difference is accepted, since phases have their own boundaries (P6a2bI-3). No extra run.

**Plan changes to apply:**
- Task 2 Step 1: "as in run 30e" becomes "with run 30e's settings (run 30e ran the slow phase alone; here it follows storm and garbage, which the phase boundaries separate)".
- What to read, slow: one line naming the same caveat beside the 30e comparison.
