# Race Integration Suite Implementation Plan — Plan 5b1 of 6 (part 1 of 3: faults)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Plan 5b is split into three files, each executed in its own session, in order: **5b1** `2026-10-08-connection-session-core-05b1-race-faults.md` (Tasks 1–3), **5b2** `2026-10-08-connection-session-core-05b2-race-harness.md` (Tasks 4–5), **5b3** `2026-10-08-connection-session-core-05b3-race-scenarios.md` (Tasks 6–12). Task numbers, choice numbers (#58–#64) and finding numbers are the same in all three.

**This part:** Tasks 1–3, the fault registry and the Logic and gateway fault hooks and flags. It ends with every binary accepting `-faults` and the profile flags; nothing runs the suite yet. Next: 5b2.

**Goal:** Build the Tier 1b race integration suite. Every race integration test in the spec runs against the real Logic, gateway and devtoken binaries, started as local processes on the test timing profile, with faults injected on request. Each scenario runs against its own Redis primary and replica, real `redis-server` processes, on the dev machine and on the Tier 2 host alike.

**Architecture:** Two kinds of change. The binaries get test-only hooks, all off unless started with `-faults`. A small registry, `internal/fault`, holds rules (op, key, delay, fail, before or after the real call, count, lifetime) and serves them under `/fault/` on the admin port. Logic wraps its `Store` so a Login, Logout or Reconcile chunk can be delayed or failed for one uid, holds or fails a gateway's ReconcileEnd at the RPC, and fails `ZADD gwepochs` through a go-redis hook. The gateway can drop Kicks, block its calls to Logic by method, and start a Reconcile pass on demand. New flags expose the intervals the profile scales, Kick addresses from a file instead of pod DNS, and a fixed clock offset (P4I-5). The suite itself is `internal/racetest`, all `_test.go` files, run only with `GOIM_RACE_SUITE=1`. `TestMain` builds the four binaries with `-race`. Each scenario starts a fresh stack: Redis, devtoken, Logic and four gateways. It drives scripted clients (`internal/clientconn`) or SDK clients, kills and pauses processes with signals, and checks Redis and the metrics. Redis is a `redis-server` 7.4.11 primary and replica, two more local processes per stack, so every scenario runs on real Redis on both tiers; `scripts/redis-build.sh` builds the pinned version from source, with no Docker. `scripts/tier2/race.sh` runs the same suite on the Tier 2 host.

**Tech Stack:** Go 1.27, the Plans 1–5a binaries and packages, Redis 7.4.11 (`redis-server`, built from the release tarball with its published SHA-256), `github.com/redis/go-redis/v9` hooks, `google.golang.org/grpc` interceptors, `net/http` for the fault API. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative). Sections: *Verification → Race integration tests* (all of it), *Test execution model* (Tier 1b, the test timing profile), *Races handled*, *Consistency contract*, *Storms, drain, failures*. Roadmap, choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. What this plan builds on: Plan 5a's *Execution Notes → For Plan 5b* and its interface review (P5I-1..P5I-6), and Plan 4's P4I-5 (the gateway clock flag).

## Global Constraints

- Module path `github.com/Tallinzp/goim`. No new module dependencies.
- Layer rule (Plan 3): no package under `internal/gateway/...` may depend on `internal/store` or `github.com/redis/...` (`TestGatewayDoesNotImportTheStore`). `internal/fault` imports neither.
- Production behaviour is unchanged unless a binary is started with `-faults`. A nil `*fault.Set` reports no fault, and `/fault/` is not served without the flag.
- Spec, test timing profile: "every interval and TTL is configurable. The profile scales the time-based ones down ~10× while keeping their ratios: lease TTL 3s, renewal every 1s, guard windows, periodic Reconcile, and the 90s outages become 9s. `login_deadline` and the 1s skew allowance are **not** scaled … Token lifetime is set per scenario through devtoken, and the `sess:`/`rev:` TTL stays at 30d. Gateway and Logic refuse to start if a profile breaks a required ratio (e.g. lease TTL ≤ failover time + one renewal interval)."
- Spec, race integration tests: "`deploy/compose`: Redis, Logic, **4 gateways** and a scripted client; Tier 1b, test timing profile". Choice #47 replaces compose with local processes; #62 makes Redis local processes too: a `redis-server` primary and replica per scenario, on the dev machine and at Tier 2 (`make test-tier2 SCENARIO=race`).
- Tier 1b runs "before every commit / merge" on the dev machine (2 cores / 2GB). Docker is never installed there (user decision 2026-10-07); `redis-server` built from source is (the user's decision at the Plan 5b review, 2026-10-08), for the race suite and for `make test-redis-local`.
- Tier 2 automation contract: one entry point per scenario, a host guard (`nproc`, memory), teardown always runs, machine-readable JSON with pass/fail and a matching exit code.
- Roadmap choices #1–#57 apply; this plan's choices are #58–#64 below.

## Review Focus

These are the five ways the suite could pass while proving nothing, or harm the host, that no spec test reaches. Most likely first. Each is pinned in Task 5.

1. **A process dies mid-scenario and the assertions still pass.** A gateway that panics after its client got the reply leaves a scenario green. Expected: the scenario fails and names the process with its log. Pinned: `startProc`'s cleanup check, `TestHarnessFailsWhatAssertionsMiss/unexpected-exit`.
2. **A fault that never fires.** A typo in a rule's key injects nothing, and the scenario passes for the wrong reason. Expected: every rule a scenario sets must have fired by the end (unless its process was killed). Pinned: `checkFaultsFired`, `TestHarnessFailsWhatAssertionsMiss/unfired-fault`.
3. **A data race inside a binary.** The binaries are built with `-race`, but the suite kills them, so the race exit code (66) never reaches anyone. Expected: a `WARNING: DATA RACE` in any process log fails the scenario. Pinned: `checkRaces`, `TestHarnessFailsWhatAssertionsMiss/race-report`, and the `data_race` mutation in *Execution Notes*.
4. **A Redis fault that is not the one the scenario caused.** A `WAIT` that fails because the replica has not linked yet makes a Revoke scenario pass its "WAIT fails" half for the wrong reason, and a pause that does not stop Redis proves nothing about the sweeper guard. Expected: each stack waits until the replica is linked (`master_link_status:up`, `connected_slaves:1`); with the replica stopped, `WAIT` runs out its timeout with 0, and acks again after it resumes; a paused primary answers nothing until it resumes. Pinned: `startRedis`, `TestRedisFaultsDoWhatTheyClaim`.
5. **Orphaned processes after a timeout or a panic.** `go test -timeout` kills the test binary without running cleanups, so four gateways and a Logic would keep their ports. Expected: every process dies with the test binary. Pinned: `Pdeathsig` in `startProc`, `TestProcessesDieWithTheSuite` (it needs no binaries and runs at Tier 1a too).

## Implementation choices this plan makes (decided 2026-10-08)

The spec leaves these open. Each one is restated in the task that implements it. They go into the roadmap as #58–#64. All were decided at the plan review on 2026-10-08: #58–#60 as proposed. #61, #62 and #64 are amended, because Redis is now a `redis-server` primary and replica per scenario on both tiers (the user's decision at the review, in place of the miniredis process this plan first proposed), and #63 because a run on that Redis exposed a timing margin (finding 12).

58. **Fault injection.**
    - A test-only registry, `internal/fault`. A rule has an op, an optional key (a uid, a gateway id, a conn_id or a method name), a delay, a fail flag, before or after the real call, a count and a lifetime. There is one rule per op, and a new `Put` replaces it. On the admin port, under `/fault/`, only with `-faults`: `PUT /fault/rule`, `GET` and `DELETE /fault/rule/{op}` (`GET` returns how often it fired), `GET /fault/info`, `POST /fault/action/{name}`.
    - Logic ops: `login` and `logout` per entry, `reconcile` (the whole chunk, at `ReadSlots`, its first Redis read), `reconcile_end` (the RPC, per gateway, before its delete phase scans `gwconns`), and `gwepochs` (a go-redis hook fails `ZADD gwepochs` after the lease `SET`, R4M-3).
    - Gateway ops: `kick` (the Kick RPC answers `UNAVAILABLE`, as a lost Kick looks to Logic) and `logic` (calls to Logic fail fast, per method). Action `reconcile` starts a triggered pass through the same `Trigger` a `missing` lease uses. `GET /fault/info` gives the gateway's epoch.
    - A delayed call is not cancelled with its RPC. It reaches Redis late, as a call queued behind a slow Redis does, so the Lua deadlines, not a cancelled context, keep a late write out (races 6 and 14, R3-O6).
    - Rejected: a proxy between the processes, which can't delay one uid's entry inside a batch or tell "written, reply lost" from "never written"; and iptables, which needs root and does not exist on the dev machine's unprivileged test runs.
59. **The test timing profile.**
    - Logic: lease TTL 3s, renewal 1s, failover 1.5s, lease cache 100ms, sweep every 500ms, Kick retries 1s apart, Kick targets idle after 6s, JWKS refresh 30s, retry while empty 500ms, unknown-kid refresh at most every 3s.
    - Gateway: lease every 1s with a 1s timeout and a 3s TTL, heartbeat 15s with the allowed range lowered to 3s–30s, Logout and ReconcileEnd retries for 6s (100ms doubling to 1s), Reconcile cycle 60s ± 6s, drain window 6s with a 500ms jitter and a 1s flush, drain timeout 30s.
    - SDK: built-in windows and backoff ÷10, heartbeat range 3s–30s.
    - Not scaled: `login_deadline` 5s, the 1s skew allowance and so the 6s Login timeout, the handshake and auth timeouts (5s; the spec's range is 3–30s), the 1s Kick attempt deadline and the 5s Logout RPC deadline (tied to RPC latency), `MaxConnectionAge` (load spreading, not a timing ratio), the 30d `sess:`/`rev:` TTL, and the SDK's client timeouts.
    - The profile is two flag lists in the suite. The binaries' own `Validate` refuses a broken ratio, pinned by `TestProfileBreakingARatioIsRefused` on the real binaries. New flags for intervals that were constants: Logic `-kick-retry-spacing`, `-kick-idle-timeout`, `-sweep-batch`, `-delete-batch`, `-jwks-refresh`, `-jwks-retry-empty`, `-jwks-unknown-kid`; gateway `-retry-backoff`, `-retry-max-delay`.
60. **Reconcile passes on demand.** Gateways run with `-reconcile-cycle 1h`, and scenarios start each pass through `POST /fault/action/reconcile`. Why: a pass the scenario did not ask for can repair the state before the scenario checks it, which hides the bug. At the profile's 60s cycle with 4 gateways, a random pass lands inside race 6's 7s window about 45% of the time. The periodic schedule is pinned by Plan 3's unit tests. Two scenarios keep periodic passes: `TestPeriodicPassRemovesAGhostWithinOneCycle` (6s cycle) and `TestCancelledPassSendsNoReconcileEnd` (3s cycle).
61. **The process harness** (`internal/racetest`).
    - Only `_test.go` files (plus a package doc), gated by `GOIM_RACE_SUITE=1` and run by `make test-race-suite`. `TestMain` builds `logic`, `gateway` and `devtoken` once per run with `-race`, and finds `redis-server` (`GOIM_REDIS_SERVER`, else `PATH`; 7.0 or later).
    - Each scenario gets a fresh stack: a Redis primary and replica, `devtoken serve`, one Logic and four gateways with one loop each. They start in under a second.
    - Ports come from a counter in 20000–32000, below Linux's ephemeral range. The probe binds without `SO_REUSEADDR`, as gnet does, so a port in TIME_WAIT from an earlier run is skipped.
    - Logic finds each gateway's Kick server through a JSON file read at every dial (`-kick-addr-file`), the local stand-in for pod DNS. A restart under the same pod name moves its address, as a rescheduled pod's DNS name does.
    - A scenario also fails on a race report in any log, a process that exited on its own, or a fault rule that never fired. Processes die with the test binary (`Pdeathsig`). Logs go to a temp dir, or to `GOIM_RACE_LOGS/<scenario>/`, and their tails are printed when a scenario fails.
62. **Redis: a `redis-server` primary and replica per scenario, on both tiers** (amended at the review: this plan first proposed a miniredis process with a simulated replica).
    - Each stack starts two `redis-server` processes on probed ports: a primary, and a replica (`--replicaof`). Nothing is persisted (`--save ""`, no AOF), the eviction policy is production's `noeviction`, and a diskless sync with no delay links the replica in well under a second. The stack waits for the link before the scenario starts.
    - A Redis pause is `SIGSTOP` on the primary; when it resumes, keys whose TTL ran out meanwhile expire (Redis's expiry times are absolute). A `WAIT` failure is `SIGSTOP` on the replica, which then acknowledges nothing.
    - The binary is the pinned Redis 7.4.11, the version the Tier 2 compose image runs, built from source by `scripts/redis-build.sh` (`make redis-server`; cc and make, no Docker). The suite takes `GOIM_REDIS_SERVER`, else `redis-server` on `PATH`, and refuses a Redis older than 7.0.
    - So every scenario runs on real Redis at Tier 1b and none skips, which amends #47 ("a miniredis server process at Tier 1b, scenarios that need real Redis skip"). The Tier 2 race run no longer uses the compose Redis: no `FLUSHALL` on a shared Redis, so the `GOIM_TEST_TIER=2` guard this plan first had is gone.
    - Rejected: the compose Redis at Tier 1b (Docker, the user's 2026-10-07 decision); a miniredis process (a simulated replica and ticker-driven TTLs that Tier 2 would have had to check).
63. **Clock skew** (R4M-5, P4I-5). `-clock-offset` shifts the gateway's clock by a fixed amount at start; it also drives the timing wheel, so a step at run time would fire timers early.
    - +2s fires the skew alert.
    - −5s makes the gateway's Logins `EXPIRED`, and the client logs in elsewhere.
    - The late-write ghost uses **+5s, not the spec's +2s** (amended at the review from the +3s first proposed). Fence (b) fired 6–8s after recv: the wheel rounds a deadline up to a whole second, and the 1s ticker that advanced it had its own phase (finding 12; the ticker now wakes on the second, so 6–7s). +5s puts the Lua deadline at 10s of real time, and the Login held 9s lands a second clear of both the latest fence and the deadline. With +2s the deadline is 7s, so a write after the fence fits only in some runs; with +3s and a 7.5s hold it failed once in three full runs. The repair path checked is the same at any offset.
64. **Tier 2 entry point.** `make test-tier2 SCENARIO=race` runs `scripts/tier2/race.sh` with four checks:
    - every scenario passes;
    - every listed scenario ran (no skips);
    - a mutation (fence (a) removed) is caught by race 5;
    - the suite ran on the pinned `redis-server` 7.4.11 (`TestMain` prints the version it found).

    *Amended at the review:* the script builds the pinned Redis (`make redis-server`) instead of starting the compose one, and the fourth check replaces "the Redis uses `noeviction`", which each stack now sets itself. Results go to `tier2-results/race-<ts>/`, with one `.report.txt` to send back.

## Execution Notes

- **Branch:** the branches keep stacking (the user's decision, 2026-10-08). Plan 5b branches from Plan 5a: `git checkout -b feat/plan5b-race feat/plan5a-sdk`. The plan and the roadmap edit were committed before Task 1 (ed43cfb; split into 5b1/5b2/5b3 in 47ac512), so the branch diff holds only Plan 5b's code.
- **Prototype:** every code block in this plan was compiled and run before the plan was written. The whole plan was built in a scratch clone of `22b1b95`, then the rendered plan was replayed into a second clone, task by task, one commit per task, by a script that applies only what this document says. `go vet ./...` passed after each task (it compiles every test file, so no task uses what a later one defines), and the replay's final tree matched the prototype byte for byte. Full runs on the dev machine:
  - Run 1 failed once on a port collision (finding 4).
  - Run 2 passed.
  - Run 3, with the self-checks on, failed two scenarios on scenario races (finding 11).
  - Run 4, after the fixes, passed all 55 tests in 4m45s.
  - Run 5 on the replayed tree passed too.
  
  The scenarios that changed after run 3 were then run 3–12 times each on their own, with no failure. On the replayed tree, `go test -race ./...` (Tier 1a) passed as well.
- **Redis switch at the review (2026-10-08):** the user decided that the harness runs `redis-server` processes instead of the miniredis process (#62 amended). Task 4 became the Redis build script, and Task 5's Redis code, `TestMain` and the self-checks changed; no scenario changed. Redis 7.4.11 was built on the dev machine from the release tarball, its SHA-256 checked (1m36s on two cores; `scripts/redis-build.sh` run again is a no-op). On the changed prototype:
  - Run 6 (`make test-race-suite`) passed all 56 tests (50 scenarios, 6 harness tests) in 4m39s.
  - Run 7 was `scripts/tier2/race.sh` itself, with only its host guard lowered: it built Redis into `.cache/redis`, and all four checks passed (56 of 56 ran and passed, the fence (a) mutant was caught, the suite ran on 7.4.11). The Tier 2 script has now run end to end once, which the compose version never could on the dev machine.
  - Run 8 failed `TestClockAheadAlertsAndItsLateWriteIsCleanedUp` once (finding 12, a timing margin the late wheel tick takes away; #63 amended to +5s / 9s). After the fix, the scenario passed 10 of 10 runs alone, and full runs 9 and 10 passed all 56 tests (4m38s each).
  - The rendered plan was replayed into a fresh clone as before; the tree matched the prototype byte for byte.
- **Mutation check:** each breakage below was made in a copy of the prototype and run against the named scenarios; each was caught. The fault check and the race check stayed on, so a mutant only counted when a scenario failed on its own assertion. That is how findings 3, 6, 7 and 9 below were found.
  - Fences:
    - fence (a) removed, the Logout sent while the Login is pending (`TestRace5CloseWhileLoginInFlight`);
    - fence (b) removed, the Logout sent on no definitive reply (`TestRace6LogicCrashLogoutWaitsForTheFence`);
    - fence (c) ignored (`TestRace14CloseDuringAChunk`, `TestRace18ClientLogoutDuringAChunk`).
  - Lua and Logic:
    - the login Lua's deadline removed (`TestRace2LoginPastTheDeadlineIsExpired`, `TestRace6WrittenWithoutReplyLogoutBeforeLogin`);
    - the delete phase ignoring the tracked set (`TestReconcileEndKeepsALoginInFlight`);
    - rule 1 removed from Logic and from the compare-and-set Lua (`TestRevokeWithADroppedKickIsCaughtByReconcile`, `TestRevocationOutlivesTheToken`, `TestSlotScopedRevoke`);
    - a refused rule-3 write that kicks anyway (`TestRefusedRule3WriteSendsNoKick`).
  - Sweeper:
    - the guard always allowing (`TestGatewaysCutOffFromLogicNothingIsSwept`, `TestLogicRestartWithLateRenewalsSweepsNothing`);
    - the lease-key check removed (`TestLeaseKeyIsAuthoritative`);
    - the mid-sweep abort removed (`TestSweepAbortsWhenTheGatewayRenews`, `TestSweepAbortsWhenALeaseKeyReappears`).
  - Gateway:
    - a cancelled pass that sends ReconcileEnd (`TestCancelledPassSendsNoReconcileEnd`);
    - the Kick epoch check removed (`TestRace4ZombieGatewayKickForTheOldEpochIsIgnored`);
    - kick-pending ignored (`TestKickDuringAuthPendingClosesWhenTheLoginReplyArrives`);
    - the Kick server stopped when the drain starts (`TestDrainingGatewayStillReceivesKicks`);
    - Task 9's check removed (`TestDrainSendsOneReconnectToALoginCompletingAsItStarts`).
  - The suite's own machinery:
    - the Redis faults, each caught by `TestRedisFaultsDoWhatTheyClaim`: a pause that does not stop the primary ("the paused primary answered"); a replica stop that does not stop it (also caught by `TestRevokeWhoseWAITFailsIsRetried` and `TestSlotRevokeWhoseWAITFailsSendsNoKick`); a replica that never links (every stack fails at start). The no-op pause is caught **only** by the self-check: `TestRedisPausedNothingIsSwept` checks an outcome, and with no pause nothing is swept either. That is why Review Focus 4 is pinned in the harness;
    - an unsynchronized write in the Kick handler that the lease loop reads, caught only by the race check (`TestDroppedKickIsRetriedThenCaughtByReconcile`, "the race detector reported a data race");
    - `-clock-offset` ignored (`TestClockAheadAlertsAndItsLateWriteIsCleanedUp`, `TestClockBehindLoginsExpireAndTheClientLogsInElsewhere`);
    - a fault delay that is cancelled with its RPC (`TestRace6WrittenWithoutReplyLogoutBeforeLogin`).
- **What prototyping changed** (each is in the code below):
  1. *Delayed calls outlive their RPC* (#58). The gateway cancels a Login stream at its 6s timeout. A cancellable delay would have aborted the call before Redis, so "Logout before Login" (race 6) would never have reached the Lua deadline it is meant to test. `TestFaultDelayOutlivesTheRPC` pins it, and the `fault_cancel` mutation shows race 6 depends on it.
  2. *A larger offset for the clock-skew ghost* (#63), measured: with +2s the ghost appeared in one run and not in the next. +3s came next and was amended to +5s after finding 12.
  3. *Fence (c) is checked directly.* The first race 14 survived the mutant without fence (c). The ghost it makes is real, but the same pass's delete phase removes it before any later check. Races 14 and 18 now check that the entry is still there while the chunk is held, which the mutant breaks.
  4. *Ports.* The first full run failed once because two processes got the same ephemeral port from `:0`. The counter below the ephemeral range fixed that. The mutation runs then hit gnet's `bind: address already in use` on ports left in TIME_WAIT by the previous run, and the strict probe fixed that.
  5. *Parallel starts.* Gateways started from goroutines raced on the Kick address file and called `t.Fatal` off the test goroutine. Now all four are launched first and then awaited.
  6. *The cancelled-pass scenario counts at Logic.* A first version checked one particular pass and was racy (a 2s cycle could run a pass before the scenario set its faults). A second compared Logic's ReconcileEnds per pace with the gateway's finished passes, and the `cancel_end` mutant survived it: a cancelled pass that wrongly ends also counts itself finished. The scenario now waits for a periodic chunk to be held in Logic, then counts every ReconcileEnd Logic receives from that moment: exactly one, the triggered pass's. The 10s cycle keeps the next periodic pass out of the window.
  7. *A vacuous scenario, found by the fault check.* With "a fault that never fired fails the scenario" switched on (Review Focus 2), `TestReconcileEndKeepsALoginInFlight` failed. Its ReconcileEnd hold sat at `DeleteUntracked`, and the pass's delete phase scanned `gwconns` before the delayed Login had added its uid, so the phase never looked at the field the scenario was about. The hold moved to the RPC, before the scan.
  8. *Fired counts survive expiry.* An expired rule was first deleted, so `Fired` lost its count and the fault check failed scenarios whose rule had a lifetime (`For`). Spent and expired rules now stay until cleared.
  9. *Rule 1 is checked twice.* Removing rule 1 from Logic's Go code alone is not caught: the compare-and-set Lua re-checks it (choice #4) and returns `REVOKED`. The `rule1` mutant removes both.
  10. *A gateway bug: two Reconnects.* The drain scenario failed about once in ten runs with a second `Reconnect` before the Kick. A Login completing as the drain starts was reconnected by `online()` and again by the drain's pass. Task 9 fixes it; the scenario expects exactly one.
  11. *Two scenario races found by the fault check and by repetition.* The stale-lease scenario's "starve gw-1" rule sometimes never fired: the overwrite could happen before gw-1's next renewal, so the starvation was not exercised. It now waits until the block has fired once before removing the lease key. The drain scenario read Logic's Kick metric before the RPC had returned (the client sees the Kick first); it now polls.
  12. *The wheel fires up to 2s late, not 1s* (found at the review, on `redis-server`). Full run 8 failed `TestClockAheadAlertsAndItsLateWriteIsCleanedUp` once: the connection was still tracked when the ghost appeared. Polling the gateway's `tracked` gauge over 15 runs showed fence (b) untracking at 6.92–7.00s after recv in 13 runs and at 7.92–7.99s in 2, together with Busy (both are 6s wheel timers). The cause: `wheel.schedule` rounds a deadline up to a whole second of the clock, and `advance` runs from a 1s `time.Ticker` with an unrelated phase, so a timer fires 0–2s late (`wheel.go` says up to one tick). Firing late is safe for every fence and timeout, and the SDK's 10s Login timeout is above the 8s worst case for Busy. The scenario moved from +3s / 7.5s to +5s / 9s (choice #63); its window had 0.5s of margin, and a late tick took all of it. With the wider window it passed 10 of 10 runs alone and every full run after. *Fixed after the review, before Task 1* (the user's request): `runTicker` now wakes just after each whole second of the gateway's clock (`untilNextTick`), so a timer fires at most one tick late, as Plan 3 meant; `TestRunningWheelFiresAtMostOneTickLate` failed 3 of 4 runs without it. The +5s / 9s window stays: it now has 2s of margin after the fence instead of 1s. The full suite passed on the prototype with the fix (56 tests). Tier 2 run 9 (`make test-tier2 SCENARIO=store`, 24 cores, Linux 7.0, Redis 7.4.11) passed all four checks on `142a6fe`, which has the fix and the reworked `TestTriggerCancelsAPeriodicPass`.
- **Spec test map** (*Verification → Race integration tests*): every bullet of the spec's list, in its order.
  - Races 1, 2, 3, 5, 10: `TestRace1DelayedDisconnect`, `TestRace2DelayedLoginSameDeviceIsStale`, `TestRace2LoginPastTheDeadlineIsExpired`, `TestRace3DelayedLoginOtherDeviceIsSuperseded`, `TestRace5CloseWhileLoginInFlight`, `TestRace10SharedDeviceIDReplacesOnce`. Race 4: `TestRace4ZombieGatewayKickForTheOldEpochIsIgnored`.
  - Race 6 in both orders, and with a Logic crash: `TestRace6WrittenWithoutReplyLoginBeforeLogout`, `TestRace6WrittenWithoutReplyLogoutBeforeLogin`, `TestRace6LogicCrashLogoutWaitsForTheFence`.
  - Race 14: `TestRace14CloseDuringAChunk`. Race 18: `TestRace18ClientLogoutDuringAChunk`. Race 16 and `ReconcileEnd`: `TestReconcileEndKeepsALoginInFlight`, `TestReconcileEndRepairsALogoutDroppedDuringThePass`, `TestCancelledPassSendsNoReconcileEnd`, `TestFailedReconcileEndIsRetried`.
  - Reconcile refusal: `TestRefusedChunkIsRequeuedAndRestoredInThePass`, `TestRefusedRule3WriteSendsNoKick`.
  - Gateway crash → sweep: `TestGatewayCrashIsSwept`. Two at once: `TestTwoGatewaysKilledAtOnceAreBothSwept`. Three while one renews: `TestThreeKilledWhileOneRenewsAreAllSwept`.
  - Redis paused, gateways cut off from Logic, Logic restart: `TestRedisPausedNothingIsSwept`, `TestGatewaysCutOffFromLogicNothingIsSwept`, `TestLogicRestartWithLateRenewalsSweepsNothing`.
  - Forced sweep restored: `TestForcedSweepOfALiveGatewayIsRestored`. Sweep abort, first lease, plain expiry: `TestSweepAbortsWhenTheGatewayRenews`, `TestFirstLeaseTriggersNoPassAndAPlainExpiryTriggersOne`.
  - Clock skew: `TestClockAheadAlertsAndItsLateWriteIsCleanedUp` (+2s alert; the ghost at +5s, choice #63), `TestClockBehindLoginsExpireAndTheClientLogsInElsewhere`.
  - Stale lease observation: `TestStaleLeaseObservationIsCorrectedByTheOwner`. Lease key authoritative: `TestLeaseKeyIsAuthoritative`, `TestSweepAbortsWhenALeaseKeyReappears`, `TestEndedEpochIsSweptAtOnce`.
  - Reconcile: `TestPeriodicPassRemovesAGhostWithinOneCycle`, `TestReconcileRestoresMissingAndDeletesGhosts`, `TestReconcileNeverOverridesANewerSession`, `TestDroppedLogoutIsRepairedByTheNextPass`.
  - Kick: `TestDroppedKickIsRetriedThenCaughtByReconcile`, `TestKickDuringAuthPendingClosesWhenTheLoginReplyArrives`, `TestDrainingGatewayStillReceivesKicks`.
  - Revoke: `TestRevokeClosesConnectionsOnTwoSlots`, `TestRevokeWithADroppedKickIsCaughtByReconcile`, `TestRevokeWhoseWAITFailsIsRetried`, `TestRevokeBeforeMs` (PI-4). Revocation outlives the token: `TestRevocationOutlivesTheToken`. Slot-scoped: `TestSlotScopedRevoke`, `TestSlotRevokeWhoseWAITFailsSendsNoKick`.
  - Key rotation: `TestKeyRotation`. JWKS: `TestLogicStartsFromTheJWKSFallback`, `TestLogicWithNoKeySetIsNeverReady`.
  - Consistency contract, "healthy session reads invalid during an outage": `TestGatewaysCutOffFromLogicNothingIsSwept` checks the lease keys are gone during the cut and back after it.
- **Timing:** the suite takes about 4¾ minutes on the dev machine (56 tests: 50 scenarios and 6 harness tests; binaries cached); the first build with `-race` adds about a minute, and the first `make redis-server` about a minute and a half. The longest scenarios are the 9s outages (15–18s each). Memory stays within Plan 5a's measured budget: one stack at a time, about 400MB; the two `redis-server` processes take a few MB each, less than the race-built miniredis process they replace.
- **Not verified at Tier 1b, and where each is closed:**
  1. **How Redis fails.** A pause is `SIGSTOP` (a hung Redis, connections open) and a lost replica is a stopped one. A Redis that restarts empty, a failover, or a replica that disconnects are not exercised here; failover is Plan 6's Tier 2 test. Redis is built with libc malloc instead of jemalloc, which the suite does not measure.
  2. **Redis Cluster.** Both tiers use one primary; Cluster stays Plan 1's open risk.
  3. **A real network.** Every process is on 127.0.0.1. Partitions are fault rules, not dropped packets. Silent node loss (b2) is Plan 6.
  4. **CPU contention.** On two cores, a 6s timeout scenario has about 0.5s of margin in a few places (refusal at 5.4s against the 6s chunk deadline). Two full runs passed after the fixes; a flake there is a timing margin, not a bug, and the scenario names it.
- **Tier 1a on real Redis (added at the review, Task 4):** `make test-redis-local` ran every package against a local primary and replica in 61s on the dev machine: all `ok`, 0 skips (the tests that were `TODO: Tier 2 integration` until now ran and passed). The Tier 2 `store` scenario's mutant (Revoke's `WAIT` on a pooled connection) was run that way too and caught by `TestRevokeFailsWhenReplicaDoesNotAck` ("err = <nil>, want ErrNotReplicated"). Plan 1's two open risks are unchanged: Redis Cluster, and replies lost after a write.
- **Executed 2026-10-09** (inline, plan code verbatim): Task 1 `77596b0`, Task 2 `4e6474a`, Task 3 `e5f6e32`; `go test -race ./...` green after each task. One ruling: the Interfaces line says every `*fault.Set` method is nil-safe, but the code guards only `Match` and `Fired`; kept as written, because only those two run without `-faults`. A fresh Opus final review found no Critical or Important issues. It noted four minors, each mattering only under `-faults`. At first they were deferred; the user then asked for them to be fixed, and they were, the same day (each fix has a test that failed first; full suite green):
  - M1: `FaultHook` also matches `EndEpoch`'s `ZADD gwepochs 0 <member>`, so a `gwepochs` rule live during that gateway's drain fails `EpochEnded` (Unavailable, lease cache not cleared) and uses up a `Count`. The only user (5b3, `For: 9s`) has no drain in its window. Fix if needed: skip score 0. Fixed: score 0 is skipped (`TestFaultHookLeavesEndEpochAlone`).
  - M2: `faultStore.LogoutEach` runs matched entries one after another in map order (N×delay, random write order), unlike `LoginEach`'s parallel goroutines. Every 5b3 use is one uid. Fixed: matched entries run in parallel (`TestFaultLogoutDelaysEntriesInParallel`).
  - M3: `Rule.After` is ignored by the gateway's `kick` and `logic` ops (it acts as a before-rule); not documented or refused. Fixed: `After` now means the Kick or call goes through and its reply is late or lost; on the Login stream it applies at the first read (`TestKickFaultAfterKicksThenFails`, `TestLogicFaultAfterCallsThenFails`, `TestLogicFaultAfterOnAStreamLosesTheReplies`; `TestLogicFaultDelayBeforeWaitsOnce` guards a delay-only rule against waiting twice). No 5b2/5b3 scenario sets `After` on a gateway op.
  - M4: the `internal/fault` package doc still says every method on a nil `*Set` reports no fault; only `Match`/`Fired` are nil-safe (see the ruling above). Fixed: the doc now says only `Match` and `Fired` are nil-safe.
- **For Plan 6:** reuse the harness's process and port code for the Tier 2/3 load scenarios. The fault API can slow Logic for the "Logic slowed" load test (`login` with a delay and no key). `gateway -clock-offset` and `logic -kick-addr-file` exist.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/fault/fault.go` | `Rule`, `Duration`, `Hit` (`Wait`), `ErrInjected`, `Set` (`New`, `Put`, `Clear`, `Match`, `Fired`, `Handler`) |
| `internal/logic/fault.go` | op names, `faultStore` (Login, Logout, ReadSlots), `faultUnary` (ReconcileEnd), `FaultHook` (`ZADD gwepochs`), `FileDial` |
| `internal/logic/server.go`, `cmd/logic/main.go` (modify) | `Deps.Faults`; `-faults`, `-kick-addr-file`, the profile's new interval flags |
| `internal/gateway/fault.go` | op names, `faultInfo`, the Logic client interceptors, the Kick check |
| `internal/gateway/gateway.go`, `server.go`, `kick.go`, `cmd/gateway/main.go` (modify) | `Deps.Faults`, `/fault/` with the `reconcile` action, `-faults`, `-clock-offset`, `-retry-backoff`, `-retry-max-delay` |
| `scripts/redis-build.sh`, `Makefile`, `.gitignore` (modify) | the pinned Redis 7.4.11 build (`make redis-server` into `.cache/redis`) |
| `scripts/redis-local.sh`, `internal/testredis/testredis.go` (modify) | a local primary and replica for Tier 1a on real Redis (`make test-redis-local`) |
| `internal/racetest/doc.go`, `main_test.go` | package doc; `TestMain` builds the binaries and finds `redis-server`; `suite` |
| `internal/racetest/proc_test.go` | `proc` (start, kill, pause, resume, term, tail), `freePort` |
| `internal/racetest/profile_test.go` | the timing profile's flag lists; `TestProfileBreakingARatioIsRefused` |
| `internal/racetest/redis_test.go` | `Redis`: the primary and replica processes, pause, replica down, entries, leases, scores, ghosts |
| `internal/racetest/stack_test.go` | `Options`, `Stack` (`New`, `base`), `Logic`, `Gateway`, restarts, the fault API, failure checks, `Metric`, `eventually`, `holds` |
| `internal/racetest/client_test.go` | scripted `Client`: `Dial`, tokens, `Req`, `Online`, `ExpectKick`, `Quiet` |
| `internal/racetest/sdk_test.go` | `sdkProfile`, `events`, `Stack.SDK` |
| `internal/racetest/metrics_test.go` | the series the scenarios read; `passDone`, `endsOK` |
| `internal/racetest/smoke_test.go`, `harness_test.go` | the stack comes up; the harness self-checks (Review Focus) |
| `internal/racetest/login_test.go` | races 1, 2, 3, 5, 6, 10 |
| `internal/racetest/reconcile_test.go` | races 14, 16, 18; ReconcileEnd; refusal; Reconcile repairs |
| `internal/racetest/sweep_test.go` | crashes, the guard under outages, forced sweep, sweep abort, lease key, stale lease observation |
| `internal/gateway/conn.go`, `login.go` (modify) | one Reconnect per connection (Task 9) |
| `internal/racetest/kick_test.go`, `revoke_test.go` | race 4, Kick tests; Revoke, slot Revoke, revocation outlives the token |
| `internal/racetest/auth_test.go`, `skew_test.go` | key rotation, JWKS fallback; clock skew |
| `scripts/tier2/race.sh`, `Makefile` (modify) | Tier 2 entry point; `make test-race-suite` |

---

### Task 1: The fault registry (`internal/fault`, choice #58)

**Files:**
- Create: `internal/fault/fault.go`
- Test: `internal/fault/fault_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `fault.Rule{Op, Key string; After bool; Delay Duration; Fail bool; Count int; For Duration}`; `fault.Duration` (JSON as a Go duration string); `fault.Hit{After bool; Delay time.Duration; Fail bool}` with `(Hit) Wait(ctx) error`; `fault.ErrInjected`; `fault.New() *Set`; `(*Set) Put(Rule) error`, `Clear(op string)`, `Match(op, key string) (Hit, bool)`, `Fired(op string) int`, `Handler(info func() any, actions map[string]func()) http.Handler`. Every method is safe on a nil `*Set`, which reports no fault (`Match`, `Fired`).

A rule matches its op and, if its key is set, that key. `Count` limits how many matches fire; a spent rule stays, so `Fired` still counts it. `For` limits how long it lives. A failing hit returns `ErrInjected` after its delay.

- [ ] **Step 1: Write the failing test**

Create `internal/fault/fault_test.go`:

```go
package fault

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNilSetHasNoFaults(t *testing.T) {
	var s *Set
	if _, ok := s.Match("login", "u1"); ok {
		t.Fatal("a nil Set matched")
	}
	if s.Fired("login") != 0 {
		t.Fatal("a nil Set fired")
	}
}

func TestMatchByOpAndKey(t *testing.T) {
	s := New()
	if err := s.Put(Rule{Op: "login", Key: "u1", After: true, Delay: Duration(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Match("login", "u2"); ok {
		t.Fatal("matched another key")
	}
	if _, ok := s.Match("logout", "u1"); ok {
		t.Fatal("matched another op")
	}
	h, ok := s.Match("login", "u1")
	if !ok || !h.After || h.Delay != 2*time.Second || h.Fail {
		t.Fatalf("Match = %+v, %v", h, ok)
	}
	if s.Fired("login") != 1 {
		t.Fatalf("Fired = %d, want 1", s.Fired("login"))
	}
	if err := s.Put(Rule{Op: "kick", Fail: true}); err != nil {
		t.Fatal(err)
	}
	if h, ok := s.Match("kick", "anything"); !ok || !h.Fail {
		t.Fatalf("an empty key must match every key: %+v, %v", h, ok)
	}
}

func TestCountSpendsTheRule(t *testing.T) {
	s := New()
	s.Put(Rule{Op: "reconcile_end", Fail: true, Count: 2})
	for i := 0; i < 2; i++ {
		if _, ok := s.Match("reconcile_end", "gw-1"); !ok {
			t.Fatalf("match %d missed", i)
		}
	}
	if _, ok := s.Match("reconcile_end", "gw-1"); ok {
		t.Fatal("matched after its count was spent")
	}
	if n := s.Fired("reconcile_end"); n != 2 {
		t.Fatalf("Fired = %d after the rule was spent, want 2", n)
	}
}

func TestForExpiresTheRule(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	s.Put(Rule{Op: "logic", Fail: true, For: Duration(9 * time.Second)})
	now = now.Add(8 * time.Second)
	if _, ok := s.Match("logic", ""); !ok {
		t.Fatal("expired early")
	}
	now = now.Add(time.Second)
	if _, ok := s.Match("logic", ""); ok {
		t.Fatal("matched at its expiry")
	}
	if n := s.Fired("logic"); n != 1 {
		t.Fatalf("Fired = %d after the rule expired, want 1", n)
	}
}

func TestPutReplacesAndClearRemoves(t *testing.T) {
	s := New()
	s.Put(Rule{Op: "login", Key: "u1"})
	s.Put(Rule{Op: "login", Key: "u2"})
	if _, ok := s.Match("login", "u1"); ok {
		t.Fatal("the replaced rule still matched")
	}
	s.Put(Rule{Op: "kick"})
	s.Clear("login")
	if _, ok := s.Match("login", "u2"); ok {
		t.Fatal("Clear left the rule")
	}
	s.Clear("")
	if _, ok := s.Match("kick", ""); ok {
		t.Fatal(`Clear("") left a rule`)
	}
	if err := s.Put(Rule{}); err == nil {
		t.Fatal("a rule without an op was accepted")
	}
}

func TestHitWait(t *testing.T) {
	if err := (Hit{Delay: time.Millisecond}).Wait(context.Background()); err != nil {
		t.Fatalf("delay only: %v", err)
	}
	if err := (Hit{Fail: true}).Wait(context.Background()); !errors.Is(err, ErrInjected) {
		t.Fatalf("fail: %v, want ErrInjected", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (Hit{Delay: time.Hour}).Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestHandler(t *testing.T) {
	s := New()
	ran := false
	srv := httptest.NewServer(s.Handler(func() any { return map[string]string{"gw": "gw-1"} },
		map[string]func(){"reconcile": func() { ran = true }}))
	defer srv.Close()
	do := func(method, path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if r := do("PUT", "/fault/rule", `{"op":"login","key":"u1","delay":"6s","after":true,"count":1}`); r.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT = %d", r.StatusCode)
	}
	if h, ok := s.Match("login", "u1"); !ok || h.Delay != 6*time.Second || !h.After {
		t.Fatalf("rule from JSON = %+v, %v", h, ok)
	}
	if r := do("PUT", "/fault/rule", `{"op":"login","delay":"soon"}`); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad duration: PUT = %d, want 400", r.StatusCode)
	}
	resp, err := http.Get(srv.URL + "/fault/rule/login")
	if err != nil {
		t.Fatal(err)
	}
	fired, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.TrimSpace(string(fired)) != `{"fired":1}` {
		t.Fatalf("GET rule = %s, want fired 1", fired)
	}
	s.Put(Rule{Op: "kick"})
	do("DELETE", "/fault/rule/kick", "")
	if _, ok := s.Match("kick", ""); ok {
		t.Fatal("DELETE left the rule")
	}
	if r := do("POST", "/fault/action/reconcile", ""); r.StatusCode != http.StatusNoContent || !ran {
		t.Fatalf("action: %d, ran %v", r.StatusCode, ran)
	}
	if r := do("POST", "/fault/action/nope", ""); r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown action = %d, want 404", r.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"gw":"gw-1"`) {
		t.Fatalf("info = %s", body)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/fault/`
Expected: FAIL to build, `undefined: New` (the package has no non-test file yet).

- [ ] **Step 3: Write the implementation**

Create `internal/fault/fault.go`:

```go
// Package fault is the test-only fault injection the race suite drives
// (Plan 5b, choice #58). A Logic or gateway process started with -faults
// serves /fault/ on its admin port; without the flag its *Set is nil, and
// every method on a nil *Set reports no fault, so production code paths are
// unchanged. Rules are keyed by op (one rule per op); the hooks that consult
// them live next to the code they break (internal/logic, internal/gateway).
package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Duration is a time.Duration written as a Go duration string ("6s") in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

// Rule is one injected fault.
type Rule struct {
	Op string `json:"op"` // the hook it breaks, e.g. "login", "kick"
	// Key narrows the hook to one uid, gateway id or method; "" matches all.
	Key string `json:"key,omitempty"`
	// After applies Delay and Fail after the real call instead of before
	// it: the write happened, but its reply is late or lost.
	After bool     `json:"after,omitempty"`
	Delay Duration `json:"delay,omitempty"`
	Fail  bool     `json:"fail,omitempty"`
	// Count is how many matches fire before the rule is spent; 0 = no limit.
	Count int `json:"count,omitempty"`
	// For is how long the rule lasts from Put; 0 = until cleared.
	For Duration `json:"for,omitempty"`
}

// Hit is a rule that matched one call.
type Hit struct {
	After bool
	Delay time.Duration
	Fail  bool
}

// ErrInjected is the error a failing rule makes a hook return.
var ErrInjected = errors.New("fault: injected failure")

// Wait sleeps for the hit's delay, or until ctx ends, then returns
// ErrInjected if the rule fails the call.
func (h Hit) Wait(ctx context.Context) error {
	if h.Delay > 0 {
		t := time.NewTimer(h.Delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	if h.Fail {
		return ErrInjected
	}
	return nil
}

type entry struct {
	Rule
	left    int // matches left; < 0 = no limit
	expires time.Time
	fired   int
}

// Set holds the active rules. A nil *Set has none.
type Set struct {
	mu    sync.Mutex
	rules map[string]*entry
	now   func() time.Time
}

// New returns an empty Set.
func New() *Set { return &Set{rules: map[string]*entry{}, now: time.Now} }

// Put installs r, replacing any rule for the same op.
func (s *Set) Put(r Rule) error {
	if r.Op == "" || r.Count < 0 || r.Delay < 0 || r.For < 0 {
		return fmt.Errorf("fault: bad rule %+v", r)
	}
	e := &entry{Rule: r, left: r.Count}
	if r.Count == 0 {
		e.left = -1
	}
	if r.For > 0 {
		e.expires = s.now().Add(time.Duration(r.For))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[r.Op] = e
	return nil
}

// Clear removes op's rule; "" removes every rule.
func (s *Set) Clear(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op == "" {
		s.rules = map[string]*entry{}
		return
	}
	delete(s.rules, op)
}

// Match reports whether op's rule applies to key, and uses up one count.
func (s *Set) Match(op, key string) (Hit, bool) {
	if s == nil {
		return Hit{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.rules[op]
	if !ok {
		return Hit{}, false
	}
	if e.Key != "" && e.Key != key {
		return Hit{}, false
	}
	// A spent or expired rule stays until cleared, so Fired still counts it.
	if e.left == 0 || (!e.expires.IsZero() && !s.now().Before(e.expires)) {
		return Hit{}, false
	}
	e.fired++
	if e.left > 0 {
		e.left--
	}
	return Hit{After: e.After, Delay: time.Duration(e.Delay), Fail: e.Fail}, true
}

// Fired returns how often op's rule has matched since it was Put, spent or
// not; 0 if there is none.
func (s *Set) Fired(op string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.rules[op]; ok {
		return e.fired
	}
	return 0
}

// Handler serves the fault API under /fault/:
//
//	PUT    /fault/rule          body: a Rule as JSON
//	GET    /fault/rule/{op}     {"fired": n}: how often op's rule matched
//	DELETE /fault/rule/{op}     clear op's rule ({op} empty: every rule)
//	GET    /fault/info          info() as JSON (ids, addresses, epoch); {} if info is nil
//	POST   /fault/action/{name} run a registered action, e.g. "reconcile"
func (s *Set) Handler(info func() any, actions map[string]func()) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /fault/rule", func(w http.ResponseWriter, r *http.Request) {
		var rule Rule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.Put(rule); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /fault/rule/{op}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Fired int `json:"fired"`
		}{s.Fired(r.PathValue("op"))})
	})
	mux.HandleFunc("DELETE /fault/rule/{op...}", func(w http.ResponseWriter, r *http.Request) {
		s.Clear(strings.Trim(r.PathValue("op"), "/"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /fault/info", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var v any = struct{}{}
		if info != nil {
			v = info()
		}
		json.NewEncoder(w).Encode(v)
	})
	mux.HandleFunc("POST /fault/action/{name}", func(w http.ResponseWriter, r *http.Request) {
		f, ok := actions[r.PathValue("name")]
		if !ok {
			http.Error(w, "unknown action", http.StatusNotFound)
			return
		}
		f()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/fault/ && go test -race -count=1 ./internal/fault/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/fault
git commit -m "feat(fault): test-only fault registry served under /fault/ (choice #58)"
```

---

### Task 2: Logic's fault hooks and flags (choices #58, #59, #61)

**Files:**
- Create: `internal/logic/fault.go`
- Modify: `internal/logic/server.go`, `cmd/logic/main.go`
- Test: `internal/logic/fault_test.go`, `cmd/logic/main_test.go`

**Interfaces:**
- Consumes: `fault.Set`, `fault.Rule`, `fault.Hit.Wait`, `fault.ErrInjected` (Task 1); `logic.Store` and `store.*Req/*Result` (Plans 1–2).
- Produces: `logic.FaultLogin = "login"`, `FaultLogout = "logout"`, `FaultReconcile = "reconcile"`, `FaultReconcileEnd = "reconcile_end"`, `FaultGWEpochs = "gwepochs"`; `logic.Deps.Faults *fault.Set`; `logic.FaultHook(*fault.Set) redis.Hook`; `logic.FileDial(path string) DialFunc`. Flags: `-faults`, `-kick-addr-file`, `-kick-retry-spacing`, `-kick-idle-timeout`, `-sweep-batch`, `-delete-batch`, `-jwks-refresh`, `-jwks-retry-empty`, `-jwks-unknown-kid`.

`faultStore` wraps the `Store` when `Deps.Faults` is set. A matched Login or Logout entry runs on its own, so the rest of its batch is not delayed with it. A matched call gets `context.WithoutCancel` (choice #58). The Reconcile hook sits at `ReadSlots`, the chunk's first Redis read: a delay there holds the whole chunk before Logic reads anything (races 14 and 18), and a delay past `login_deadline` makes its writes come back `refused` (R3-O6). The ReconcileEnd hook is a gRPC interceptor, added only with `Faults`: the delete phase scans `gwconns` first, so a store hook would only see the uids already there (*Execution Notes*, finding 7). `FaultHook` matches only `ZADD gwepochs <score> <gw>:<epoch>`, so the lease `SET` before it succeeds.

- [ ] **Step 1: Write the failing tests**

Create `internal/logic/fault_test.go`:

```go
package logic

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func faultEnv(t *testing.T) (faultStore, *store.Store, store.EpochRef) {
	t.Helper()
	st, _ := newStore(t, testConfig())
	return faultStore{st, fault.New()}, st, store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
}

func loginReq(t *testing.T, st Store, ref store.EpochRef, uid string, connID uint64) store.LoginReq {
	now := redisNow(t, st)
	return store.LoginReq{UID: uid, Field: field, Entry: session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch,
		ConnID: connID, DeviceID: "d-" + uid, ConnSeq: 1, LoginMs: now, TokIatMs: now - 1000}}
}

// A delayed entry does not hold up the rest of its batch.
func TestFaultLoginDelaysOnlyItsEntry(t *testing.T) {
	fs, st, ref := faultEnv(t)
	slow, fast := testredis.UID(), testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: slow, Delay: fault.Duration(300 * time.Millisecond)})
	reqs := []store.LoginReq{loginReq(t, st, ref, slow, 1), loginReq(t, st, ref, fast, 2)}
	var mu sync.Mutex
	var order []int
	start := time.Now()
	var slowAt time.Duration
	fs.LoginEach(ctx, reqs, func(i int, r store.LoginResult) {
		mu.Lock()
		defer mu.Unlock()
		if r.Err != nil || r.Code != store.LoginWritten {
			t.Errorf("entry %d: %+v", i, r)
		}
		order = append(order, i)
		if i == 0 {
			slowAt = time.Since(start)
		}
	})
	if len(order) != 2 || order[0] != 1 || slowAt < 300*time.Millisecond {
		t.Fatalf("order %v, slow entry after %v; want the fast one first and the slow one after 300ms", order, slowAt)
	}
}

// A delayed entry reaches Redis even after its RPC was cancelled, and the
// login Lua's deadline refuses it (race 6, Logout before Login).
func TestFaultDelayOutlivesTheRPC(t *testing.T) {
	cfg := testConfig()
	cfg.Store.LoginDeadline = 200 * time.Millisecond
	st, _ := newStore(t, cfg)
	fs := faultStore{st, fault.New()}
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
	uid := testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: uid, Delay: fault.Duration(400 * time.Millisecond)})
	rpc, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var got store.LoginResult
	fs.LoginEach(rpc, []store.LoginReq{loginReq(t, st, ref, uid, 1)}, func(_ int, r store.LoginResult) { got = r })
	if got.Err != nil || got.Code != store.LoginExpired {
		t.Fatalf("result %+v, want EXPIRED from the Lua", got)
	}
}

// After + Fail: Redis is written, but the reply says the outcome is unknown
// (race 6: no definitive reply, Redis written).
func TestFaultLoginAfterWritesThenFails(t *testing.T) {
	fs, st, ref := faultEnv(t)
	uid := testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: uid, After: true, Fail: true, Count: 1})
	var got store.LoginResult
	fs.LoginEach(ctx, []store.LoginReq{loginReq(t, st, ref, uid, 1)}, func(_ int, r store.LoginResult) { got = r })
	if !errors.Is(got.Err, fault.ErrInjected) {
		t.Fatalf("result %+v, want the injected error", got)
	}
	states := st.ReadSlots(ctx, []store.SlotRef{{UID: uid, Field: field}})
	if states[0].Stored == nil || states[0].Stored.ConnID != 1 {
		t.Fatalf("stored %+v, want the write to have happened", states[0])
	}
}

func TestFaultLogoutFailsOnlyItsEntry(t *testing.T) {
	fs, st, ref := faultEnv(t)
	a, b := testredis.UID(), testredis.UID()
	st.LoginEach(ctx, []store.LoginReq{loginReq(t, st, ref, a, 1), loginReq(t, st, ref, b, 2)}, func(int, store.LoginResult) {})
	fs.f.Put(fault.Rule{Op: FaultLogout, Key: a, Fail: true})
	res := make([]store.LogoutResult, 2)
	fs.LogoutEach(ctx, []store.LogoutReq{
		{UID: a, Field: field, Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: 1},
		{UID: b, Field: field, Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: 2},
	}, func(i int, r store.LogoutResult) { res[i] = r })
	if !errors.Is(res[0].Err, fault.ErrInjected) || res[1].Err != nil || !res[1].Deleted {
		t.Fatalf("results %+v", res)
	}
	if s := st.ReadSlots(ctx, []store.SlotRef{{UID: a, Field: field}}); s[0].Stored == nil {
		t.Fatal("the failed Logout deleted its entry")
	}
}

func TestFaultReconcileFailsTheWholeChunk(t *testing.T) {
	fs, _, _ := faultEnv(t)
	a, b := testredis.UID(), testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultReconcile, Key: b, Fail: true, Count: 1})
	states := fs.ReadSlots(ctx, []store.SlotRef{{UID: a, Field: field}, {UID: b, Field: field}})
	for i, s := range states {
		if !errors.Is(s.Err, fault.ErrInjected) {
			t.Fatalf("state %d: %+v, want the injected error", i, s)
		}
	}
	if states := fs.ReadSlots(ctx, []store.SlotRef{{UID: b, Field: field}}); states[0].Err != nil {
		t.Fatalf("Count 1 fired twice: %+v", states[0])
	}
}

// The ReconcileEnd hold sits before the handler, so the delete phase starts
// after it; a failing rule answers UNAVAILABLE, which the gateway retries.
func TestFaultReconcileEndFailsForItsGateway(t *testing.T) {
	f := fault.New()
	f.Put(fault.Rule{Op: FaultReconcileEnd, Key: "gw-a", Fail: true})
	calls := 0
	h := func(context.Context, any) (any, error) { calls++; return &pb.ReconcileEndReply{}, nil }
	end := func(gw string) error {
		_, err := faultUnary(f)(ctx, &pb.ReconcileEndRequest{Gw: &pb.GatewayRef{Gateway: gw, Epoch: 1}}, nil, h)
		return err
	}
	if err := end("gw-a"); status.Code(err) != codes.Unavailable || calls != 0 {
		t.Fatalf("gw-a: %v after %d handler calls; want UNAVAILABLE before the handler", err, calls)
	}
	if err := end("gw-b"); err != nil || calls != 1 {
		t.Fatalf("gw-b: %v, %d handler calls", err, calls)
	}
	if _, err := faultUnary(f)(ctx, &pb.LogoutBatch{}, nil, h); err != nil {
		t.Fatalf("another method: %v", err)
	}
}

// The lease SET succeeds and only the named gateway's ZADD gwepochs fails (R4M-3).
func TestFaultHookFailsOnlyTheNamedGatewaysZADD(t *testing.T) {
	rdb := testredis.New(t)
	f := fault.New()
	rdb.AddHook(FaultHook(f))
	st := store.New(rdb, testConfig().Store)
	a := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	b := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	f.Put(fault.Rule{Op: FaultGWEpochs, Key: a.Gateway})
	res, err := st.RenewLease(ctx, a)
	if err != nil || !errors.Is(res.IndexErr, fault.ErrInjected) {
		t.Fatalf("renewal of a: %+v %v; want IndexErr injected", res, err)
	}
	if live, _ := st.LeasesExist(ctx, []store.EpochRef{a}); !live[0] {
		t.Fatal("the lease SET did not happen")
	}
	if _, ok, _ := st.EpochScoreOf(ctx, a); ok {
		t.Fatal("a's gwepochs score was written")
	}
	if res, err := st.RenewLease(ctx, b); err != nil || res.IndexErr != nil {
		t.Fatalf("renewal of b: %+v %v", res, err)
	}
}

func TestFileDialReadsTheFileAtEachDial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kick.json")
	dial := FileDial(path)
	if _, err := dial("gw-1"); err == nil {
		t.Fatal("dial without a file succeeded")
	}
	os.WriteFile(path, []byte(`{"gw-1":"127.0.0.1:1"}`), 0o644)
	cc, err := dial("gw-1")
	if err != nil {
		t.Fatal(err)
	}
	cc.Close()
	if _, err := dial("gw-2"); err == nil {
		t.Fatal("dial of an unlisted gateway succeeded")
	}
	os.WriteFile(path, []byte(`{"gw-2":"127.0.0.1:2"}`), 0o644)
	if cc, err := dial("gw-2"); err != nil {
		t.Fatalf("after the file changed: %v", err)
	} else {
		cc.Close()
	}
}

// readyKeys is a key set that is always loaded.
type readyKeys struct{ staticKeys }

func (readyKeys) Ready() bool         { return true }
func (readyKeys) Run(context.Context) {}

// /fault/ is served only by a Logic built with Faults.
func TestFaultEndpointOnlyWithFaults(t *testing.T) {
	for _, withFaults := range []bool{false, true} {
		cfg := testConfig()
		cfg.SweepInterval = time.Hour
		st, _ := newStore(t, cfg)
		sc := DefaultServerConfig()
		sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
		reg := prometheus.NewRegistry()
		d := Deps{Store: st, Keys: readyKeys{}, Dial: staticDial(nil), Registry: reg, Metrics: NewMetrics(reg)}
		if withFaults {
			d.Faults = fault.New()
		}
		srv, err := NewServer(cfg, sc, d)
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		if err := srv.Start(runCtx); err != nil {
			t.Fatal(err)
		}
		_, _, admin := srv.Addrs()
		want := http.StatusNotFound
		if withFaults {
			want = http.StatusOK
		}
		if c := httpStatus(t, "http://"+admin+"/fault/info"); c != want {
			t.Errorf("faults %v: /fault/info = %d, want %d", withFaults, c, want)
		}
		cancel()
		srv.Stop()
	}
}
```

In `cmd/logic/main_test.go`, replace:

```go
		t.Fatal("run started with a grace below the Login timeout")
	}
}
```

with:

```go
		t.Fatal("run started with a grace below the Login timeout")
	}
}

// The race suite's flags (Plan 5b): Kick addresses from a file, faults on,
// and the intervals the timing profile scales that were not flags before.
func TestRaceSuiteFlags(t *testing.T) {
	cfg, _, o, err := parseFlags([]string{"-jwks-url", "x", "-kick-addr-file", "/tmp/kick.json", "-faults",
		"-kick-retry-spacing", "1s", "-kick-idle-timeout", "6s", "-sweep-batch", "50", "-delete-batch", "50",
		"-jwks-refresh", "30s", "-jwks-retry-empty", "500ms", "-jwks-unknown-kid", "3s"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.faults || o.kickAddrFile != "/tmp/kick.json" || cfg.Kick.RetrySpacing != time.Second ||
		cfg.Kick.IdleTimeout != 6*time.Second || cfg.SweepBatch != 50 || cfg.DeleteBatch != 50 {
		t.Fatalf("parsed %+v %+v", cfg, o)
	}
	if o.jwks.URL != "x" || o.jwks.Refresh != 30*time.Second || o.jwks.RetryEmpty != 500*time.Millisecond ||
		o.jwks.UnknownKid != 3*time.Second || o.jwks.FetchTimeout != 5*time.Second {
		t.Fatalf("JWKS config %+v", o.jwks)
	}
	if _, _, _, err := parseFlags([]string{"-jwks-url", "x", "-jwks-unknown-kid", "0s"}); err == nil {
		t.Fatal("a zero unknown-kid interval was accepted")
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/logic/ ./cmd/logic/`
Expected: FAIL to build, `undefined: faultStore`, `undefined: FaultLogin`, `o.faults undefined`.

- [ ] **Step 3: Write the implementation**

Create `internal/logic/fault.go`:

```go
package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Fault ops Logic serves (Plan 5b, choice #58). The race suite sets them on
// a Logic started with -faults; the key is a uid unless noted.
const (
	FaultLogin        = "login"         // LoginEach, per entry; After: Redis written, reply late or lost
	FaultLogout       = "logout"        // LogoutEach, per entry
	FaultReconcile    = "reconcile"     // ReadSlots: the whole chunk, if any entry matches
	FaultReconcileEnd = "reconcile_end" // the ReconcileEnd RPC, before its delete phase; key: gateway id
	FaultGWEpochs     = "gwepochs"      // ZADD gwepochs after a lease SET; key: gateway id (R4M-3)
)

// faultStore breaks Store calls on the race suite's request. A matched Login
// or Logout entry runs on its own, so the other entries of its batch are
// not delayed with it. A matched call is not cancelled with its RPC: it
// reaches Redis late, as a call queued behind a slow Redis does, so the Lua
// deadlines, not a cancelled context, are what keep a late write out
// (races 6 and 14, R3-O6).
type faultStore struct {
	Store
	f *fault.Set
}

func (s faultStore) LoginEach(ctx context.Context, reqs []store.LoginReq, done func(i int, r store.LoginResult)) {
	var wg sync.WaitGroup
	plain := make([]int, 0, len(reqs))
	for i, r := range reqs {
		h, ok := s.f.Match(FaultLogin, r.UID)
		if !ok {
			plain = append(plain, i)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.WithoutCancel(ctx)
			if !h.After {
				if err := h.Wait(ctx); err != nil {
					done(i, store.LoginResult{Err: err})
					return
				}
			}
			var res store.LoginResult
			s.Store.LoginEach(ctx, reqs[i:i+1], func(_ int, r store.LoginResult) { res = r })
			if h.After {
				if err := h.Wait(ctx); err != nil {
					res = store.LoginResult{Err: err}
				}
			}
			done(i, res)
		}()
	}
	if len(plain) > 0 {
		sub := make([]store.LoginReq, len(plain))
		for j, i := range plain {
			sub[j] = reqs[i]
		}
		s.Store.LoginEach(ctx, sub, func(j int, r store.LoginResult) { done(plain[j], r) })
	}
	wg.Wait()
}

func (s faultStore) LogoutEach(ctx context.Context, reqs []store.LogoutReq, done func(i int, r store.LogoutResult)) {
	plain := make([]int, 0, len(reqs))
	hits := map[int]fault.Hit{}
	for i, r := range reqs {
		if h, ok := s.f.Match(FaultLogout, r.UID); ok {
			hits[i] = h
		} else {
			plain = append(plain, i)
		}
	}
	if len(plain) > 0 {
		sub := make([]store.LogoutReq, len(plain))
		for j, i := range plain {
			sub[j] = reqs[i]
		}
		s.Store.LogoutEach(ctx, sub, func(j int, r store.LogoutResult) { done(plain[j], r) })
	}
	ctx = context.WithoutCancel(ctx)
	for i, h := range hits {
		if !h.After {
			if err := h.Wait(ctx); err != nil {
				done(i, store.LogoutResult{Err: err})
				continue
			}
		}
		var res store.LogoutResult
		s.Store.LogoutEach(ctx, reqs[i:i+1], func(_ int, r store.LogoutResult) { res = r })
		if h.After {
			if err := h.Wait(ctx); err != nil {
				res = store.LogoutResult{Err: err}
			}
		}
		done(i, res)
	}
}

// ReadSlots is the first Store call of a Reconcile chunk, so a delay here
// holds the whole chunk before Logic reads anything (races 14 and 18), and
// a delay past login_deadline makes its writes come back refused (R3-O6).
func (s faultStore) ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState {
	for _, r := range refs {
		if h, ok := s.f.Match(FaultReconcile, r.UID); ok {
			ctx = context.WithoutCancel(ctx)
			if err := h.Wait(ctx); err != nil {
				out := make([]store.SlotState, len(refs))
				for i := range out {
					out[i].Err = err
				}
				return out
			}
			break
		}
	}
	return s.Store.ReadSlots(ctx, refs)
}

// faultUnary holds or fails a ReconcileEnd RPC before its delete phase
// starts. It sits at the RPC, not at the store: the phase scans gwconns
// first, so a store hook would see only the uids that were there already.
func faultUnary(f *fault.Set) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if r, ok := req.(*pb.ReconcileEndRequest); ok {
			if hit, ok := f.Match(FaultReconcileEnd, r.GetGw().GetGateway()); ok {
				if err := hit.Wait(context.WithoutCancel(ctx)); err != nil {
					return nil, status.Errorf(codes.Unavailable, "fault: ReconcileEnd: %v", err)
				}
			}
		}
		return h(ctx, req)
	}
}

// FaultHook fails ZADD gwepochs for the gateway the gwepochs rule names,
// while the lease SET before it succeeds (R4M-3). Add it to Logic's Redis
// client only when -faults is set.
func FaultHook(f *fault.Set) redis.Hook { return gwepochsFault{f} }

type gwepochsFault struct{ f *fault.Set }

func (gwepochsFault) DialHook(next redis.DialHook) redis.DialHook { return next }

func (gwepochsFault) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h gwepochsFault) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// ZADD gwepochs <score> <gw>:<epoch>
		if a := cmd.Args(); len(a) == 4 && strings.EqualFold(fmt.Sprint(a[0]), "zadd") && a[1] == "gwepochs" {
			m := fmt.Sprint(a[3])
			if i := strings.LastIndexByte(m, ':'); i > 0 {
				if _, ok := h.f.Match(FaultGWEpochs, m[:i]); ok {
					cmd.SetErr(fault.ErrInjected)
					return fault.ErrInjected
				}
			}
		}
		return next(ctx, cmd)
	}
}

// FileDial dials gateways by the address a JSON file ({"gw-1":
// "127.0.0.1:9101", ...}) maps their id to, read again at every dial. The
// race suite runs gateways as local processes, which have no pod DNS; a new
// epoch of a pod name dials again, so a restart may move its address, as a
// rescheduled pod's DNS name does.
func FileDial(path string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var addrs map[string]string
		if err := json.Unmarshal(b, &addrs); err != nil {
			return nil, fmt.Errorf("kick address file %s: %w", path, err)
		}
		addr, ok := addrs[gw]
		if !ok {
			return nil, fmt.Errorf("kick address file %s: no gateway %q", path, gw)
		}
		return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}
```

In `internal/logic/server.go`, replace:

```go
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
```

with:

```go
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
```

In `internal/logic/server.go`, replace:

```go
	Dial     DialFunc
	Registry *prometheus.Registry // served on /metrics
	Metrics  *Metrics             // registered on Registry (NewMetrics)
}

// Server is one Logic pod: two gRPC servers, the admin HTTP server, the
```

with:

```go
	Dial     DialFunc
	Registry *prometheus.Registry // served on /metrics
	Metrics  *Metrics             // registered on Registry (NewMetrics)
	// Faults, if set, breaks Store calls on request and serves /fault/ on
	// the admin port (the race suite, -faults). Nil in production.
	Faults *fault.Set
}

// Server is one Logic pod: two gRPC servers, the admin HTTP server, the
```

In `internal/logic/server.go`, replace:

```go
		return nil, err
	}
	m := d.Metrics
	leases := NewLeaseCache(d.Store, cfg.LeaseCache, time.Now)
	kicker := NewKicker(cfg.Kick, leases, m, d.Dial)
	s := &Server{cfg: cfg, sc: sc, deps: d, m: m, kicker: kicker,
		svc:   NewService(cfg, d.Store, d.Keys, leases, kicker, m, sc.Workers),
		lease: NewLeaseService(d.Store, leases, m, sc.LeaseWorkers),
		sweep: NewSweeper(cfg, d.Store, m, time.Now)}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.unaryObserve),
		grpc.ChainStreamInterceptor(s.streamObserve),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
```

with:

```go
		return nil, err
	}
	m := d.Metrics
	if d.Faults != nil {
		d.Store = faultStore{d.Store, d.Faults}
	}
	leases := NewLeaseCache(d.Store, cfg.LeaseCache, time.Now)
	kicker := NewKicker(cfg.Kick, leases, m, d.Dial)
	s := &Server{cfg: cfg, sc: sc, deps: d, m: m, kicker: kicker,
		svc:   NewService(cfg, d.Store, d.Keys, leases, kicker, m, sc.Workers),
		lease: NewLeaseService(d.Store, leases, m, sc.LeaseWorkers),
		sweep: NewSweeper(cfg, d.Store, m, time.Now)}
	unary := []grpc.UnaryServerInterceptor{s.unaryObserve}
	if d.Faults != nil {
		unary = append(unary, faultUnary(d.Faults))
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(s.streamObserve),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
```

In `internal/logic/server.go`, replace:

```go
		}
		w.WriteHeader(http.StatusOK)
	})
	s.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}
```

with:

```go
		}
		w.WriteHeader(http.StatusOK)
	})
	if d.Faults != nil {
		mux.Handle("/fault/", d.Faults.Handler(nil, nil))
	}
	s.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}
```

In `cmd/logic/main.go`, replace:

```go
	"syscall"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/prometheus/client_golang/prometheus"
```

with:

```go
	"syscall"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/prometheus/client_golang/prometheus"
```

In `cmd/logic/main.go`, replace:

```go
	redisCluster bool
	jwksURL      string
	jwksFallback string
}

// parseFlags returns the validated configuration. Logic refuses to start on
// a configuration that breaks a required ratio (spec: Test execution model).
func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error) {
	cfg, sc, o := logic.DefaultConfig(), logic.DefaultServerConfig(), options{}
	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")

	fs.StringVar(&sc.GRPCAddr, "grpc-addr", sc.GRPCAddr, "Logic and Revocation gRPC listen address")
	fs.StringVar(&sc.LeaseAddr, "lease-addr", sc.LeaseAddr, "Lease gRPC listen address (dedicated, R1-O2)")
```

with:

```go
	redisCluster bool
	jwksURL      string
	jwksFallback string
	jwks         auth.ManagerConfig // its intervals; URL and fallback come from the two flags above
	kickAddrFile string
	faults       bool
}

// parseFlags returns the validated configuration. Logic refuses to start on
// a configuration that breaks a required ratio (spec: Test execution model).
func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error) {
	cfg, sc := logic.DefaultConfig(), logic.DefaultServerConfig()
	o := options{jwks: auth.DefaultManagerConfig("", "")}
	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")
	fs.DurationVar(&o.jwks.Refresh, "jwks-refresh", o.jwks.Refresh, "periodic JWKS refresh")
	fs.DurationVar(&o.jwks.RetryEmpty, "jwks-retry-empty", o.jwks.RetryEmpty, "JWKS retry interval while no key set is held")
	fs.DurationVar(&o.jwks.UnknownKid, "jwks-unknown-kid", o.jwks.UnknownKid, "at most one unknown-kid refresh per this long")
	fs.StringVar(&o.kickAddrFile, "kick-addr-file", "", "JSON file mapping gateway id to Kick address, read at each dial (tests; overrides -kick-addr-format)")
	fs.BoolVar(&o.faults, "faults", false, "serve the test-only fault API on the admin port (race suite only, never in production)")

	fs.StringVar(&sc.GRPCAddr, "grpc-addr", sc.GRPCAddr, "Logic and Revocation gRPC listen address")
	fs.StringVar(&sc.LeaseAddr, "lease-addr", sc.LeaseAddr, "Lease gRPC listen address (dedicated, R1-O2)")
```

In `cmd/logic/main.go`, replace:

```go
	fs.DurationVar(&cfg.LeaseCache, "lease-cache", cfg.LeaseCache, "lease observation cache")
	fs.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "sweeper cycle")
	fs.IntVar(&cfg.SweepRate, "sweep-rate", cfg.SweepRate, "sweeper uids/s")
	fs.IntVar(&cfg.DeleteRate, "delete-rate", cfg.DeleteRate, "ReconcileEnd delete phase uids/s")
	fs.DurationVar(&cfg.RevokeMaxAhead, "revoke-max-ahead", cfg.RevokeMaxAhead, "largest before_ms lead over Redis TIME")
	fs.IntVar(&cfg.Kick.Workers, "kick-workers", cfg.Kick.Workers, "Kick workers per target")
	fs.IntVar(&cfg.Kick.Queue, "kick-queue", cfg.Kick.Queue, "Kick queue per target")
	fs.DurationVar(&cfg.Kick.Deadline, "kick-deadline", cfg.Kick.Deadline, "Kick attempt deadline")
	fs.StringVar(&cfg.Kick.AddrFormat, "kick-addr-format", cfg.Kick.AddrFormat, "gateway pod DNS, %s = pod name")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, o, err
```

with:

```go
	fs.DurationVar(&cfg.LeaseCache, "lease-cache", cfg.LeaseCache, "lease observation cache")
	fs.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "sweeper cycle")
	fs.IntVar(&cfg.SweepRate, "sweep-rate", cfg.SweepRate, "sweeper uids/s")
	fs.IntVar(&cfg.SweepBatch, "sweep-batch", cfg.SweepBatch, "sweeper uids per page and abort check")
	fs.IntVar(&cfg.DeleteRate, "delete-rate", cfg.DeleteRate, "ReconcileEnd delete phase uids/s")
	fs.IntVar(&cfg.DeleteBatch, "delete-batch", cfg.DeleteBatch, "delete phase uids per page")
	fs.DurationVar(&cfg.RevokeMaxAhead, "revoke-max-ahead", cfg.RevokeMaxAhead, "largest before_ms lead over Redis TIME")
	fs.IntVar(&cfg.Kick.Workers, "kick-workers", cfg.Kick.Workers, "Kick workers per target")
	fs.IntVar(&cfg.Kick.Queue, "kick-queue", cfg.Kick.Queue, "Kick queue per target")
	fs.DurationVar(&cfg.Kick.Deadline, "kick-deadline", cfg.Kick.Deadline, "Kick attempt deadline")
	fs.DurationVar(&cfg.Kick.RetrySpacing, "kick-retry-spacing", cfg.Kick.RetrySpacing, "between Kick attempts")
	fs.DurationVar(&cfg.Kick.IdleTimeout, "kick-idle-timeout", cfg.Kick.IdleTimeout, "a Kick target with no work for this long is dropped")
	fs.StringVar(&cfg.Kick.AddrFormat, "kick-addr-format", cfg.Kick.AddrFormat, "gateway pod DNS, %s = pod name")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, o, err
```

In `cmd/logic/main.go`, replace:

```go
	if o.jwksURL == "" && o.jwksFallback == "" {
		return cfg, sc, o, errors.New("config: -jwks-url or -jwks-fallback is required")
	}
	return cfg, sc, o, nil
}

```

with:

```go
	if o.jwksURL == "" && o.jwksFallback == "" {
		return cfg, sc, o, errors.New("config: -jwks-url or -jwks-fallback is required")
	}
	o.jwks.URL, o.jwks.FallbackPath = o.jwksURL, o.jwksFallback
	if o.jwks.Refresh <= 0 || o.jwks.RetryEmpty <= 0 || o.jwks.UnknownKid <= 0 {
		return cfg, sc, o, errors.New("config: the JWKS intervals must be positive")
	}
	return cfg, sc, o, nil
}

```

In `cmd/logic/main.go`, replace:

```go
		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
	}
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
```

with:

```go
		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
	}
	defer rdb.Close()
	var faults *fault.Set
	if o.faults {
		faults = fault.New()
		rdb.AddHook(logic.FaultHook(faults))
		slog.Warn("logic: -faults is set: the test-only fault API is on the admin port")
	}
	dial := logic.PodDNSDial(cfg.Kick.AddrFormat)
	if o.kickAddrFile != "" {
		dial = logic.FileDial(o.kickAddrFile)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
```

In `cmd/logic/main.go`, replace:

```go
	defer stop()

	m := logic.NewMetrics(reg)
	keys := auth.NewManager(auth.DefaultManagerConfig(o.jwksURL, o.jwksFallback), m.AuthObserver())
	srv, err := logic.NewServer(cfg, sc, logic.Deps{Store: store.New(rdb, cfg.Store), Keys: keys,
		Dial: logic.PodDNSDial(cfg.Kick.AddrFormat), Registry: reg, Metrics: m})
	if err != nil {
		return err
	}
```

with:

```go
	defer stop()

	m := logic.NewMetrics(reg)
	keys := auth.NewManager(o.jwks, m.AuthObserver())
	srv, err := logic.NewServer(cfg, sc, logic.Deps{Store: store.New(rdb, cfg.Store), Keys: keys,
		Dial: dial, Registry: reg, Metrics: m, Faults: faults})
	if err != nil {
		return err
	}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./... && go test -race -count=1 ./internal/logic/ ./cmd/logic/`
Expected: `ok` for both. Then `go test -race -count=1 ./...`: no other package changes.

- [ ] **Step 5: Commit**

```bash
git add internal/logic/fault.go internal/logic/fault_test.go internal/logic/server.go cmd/logic
git commit -m "feat(logic): fault hooks, Kick addresses from a file, profile flags (Plan 5b)"
```

---

### Task 3: The gateway's fault hooks and flags (choices #58, #59, #60, #63; P4I-5)

**Files:**
- Create: `internal/gateway/fault.go`
- Modify: `internal/gateway/gateway.go`, `internal/gateway/server.go`, `internal/gateway/kick.go`, `cmd/gateway/main.go`
- Test: `internal/gateway/fault_test.go`, `cmd/gateway/main_test.go`

**Interfaces:**
- Consumes: `fault.Set` and friends (Task 1); the gateway core and its test harness (`startLive`, `fakeLogic`, `loginLive`, `kickClient`, `waitFor`; Plan 3).
- Produces: `gateway.FaultKick = "kick"` (key: conn_id in decimal), `gateway.FaultLogic = "logic"` (key: method name, e.g. `GatewayLease`); `gateway.Deps.Faults *fault.Set`; `GET /fault/info` → `{"gw", "epoch" (decimal string), "kick", "admin"}`; `POST /fault/action/reconcile`. Flags: `-faults`, `-clock-offset`, `-retry-backoff`, `-retry-max-delay`.

The reconciler is now built before the admin mux, so the `reconcile` action can name `g.recon.Trigger` with no race against `Start`. The Logic interceptors run after the observing ones, so a blocked call still shows in `rpc_seconds` with code `Unavailable`. `-clock-offset` sets `Deps.Clock` to `time.Now().Add(offset)`, fixed at start (choice #63).

- [ ] **Step 1: Write the failing tests**

Create `internal/gateway/fault_test.go`:

```go
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func liveWithFaults(t *testing.T) (*live, *fault.Set) {
	t.Helper()
	f := fault.New()
	l := startLive(t, nil, func(g *Gateway, _ *fakeLogic) { g.faults = f })
	return l, f
}

func TestKickFaultDropsKicksUntilCleared(t *testing.T) {
	l, f := liveWithFaults(t)
	kc := l.kickClient(t)
	k := l.loginLive(t, 1, "u1")
	f.Put(fault.Rule{Op: FaultKick, Key: strconv.FormatUint(k.c.id, 10), Fail: true})
	_, err := kc.Kick(context.Background(), &pb.KickRequest{Gw: &pb.GatewayRef{Gateway: "gw-1", Epoch: 0xE1},
		ConnId: k.c.id, Reason: pb.KickReason_KICK_REASON_REPLACED})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("dropped Kick: %v, want UNAVAILABLE (Logic retries those)", err)
	}
	time.Sleep(50 * time.Millisecond)
	if k.isClosed() {
		t.Fatal("a dropped Kick closed the connection")
	}
	f.Clear(FaultKick)
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "closed by the Kick after the fault cleared", k.isClosed)
}

func TestLogicFaultBlocksOneMethod(t *testing.T) {
	l, f := liveWithFaults(t)
	waitFor(t, "first lease", func() bool { return len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })) > 0 })
	f.Put(fault.Rule{Op: FaultLogic, Key: "GatewayLease", Fail: true})
	time.Sleep(150 * time.Millisecond) // a renewal already in flight may still land
	n := len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls }))
	l.loginLive(t, 1, "u1") // Login still reaches Logic
	time.Sleep(300 * time.Millisecond)
	if m := len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })); m != n {
		t.Fatalf("lease calls went from %d to %d while blocked", n, m)
	}
	f.Clear(FaultLogic)
	waitFor(t, "renewals resume", func() bool { return len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })) > n })
}

func TestFaultInfoAndReconcileAction(t *testing.T) {
	l, _ := liveWithFaults(t)
	l.loginLive(t, 1, "u1")
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	var info faultInfo
	json.NewDecoder(resp.Body).Decode(&info)
	resp.Body.Close()
	if info.Gateway != "gw-1" || info.Epoch != strconv.FormatUint(0xE1, 10) || info.Kick == "" {
		t.Fatalf("info %+v", info)
	}
	resp, err = http.Post("http://"+admin+"/fault/action/reconcile", "", strings.NewReader(""))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("action: %v %v", resp, err)
	}
	resp.Body.Close()
	waitFor(t, "the triggered pass's ReconcileEnd", func() bool {
		return len(get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends })) == 1
	})
	if e := get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends }); e[0].GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("pass pace %v, want triggered", e[0].GetPace())
	}
	if c := get(l.logic, func() []*pb.ReconcileChunk { return l.logic.chunks }); len(c) != 1 || len(c[0].GetEntries()) != 1 {
		t.Fatalf("chunks %v, want one chunk with u1", c)
	}
}

// Without Faults there is no /fault/ endpoint and nothing is broken.
func TestNoFaultEndpointInProduction(t *testing.T) {
	l := newLive(t, nil)
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/fault/info = %d, want 404", resp.StatusCode)
	}
}
```

In `cmd/gateway/main_test.go`, replace:

```go
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
```

with:

```go
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"os"
	"path/filepath"
```

In `cmd/gateway/main_test.go`, replace:

```go
	}
}

func TestRefusesBadConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-handshake-timeout", "2s"}, // spec: 3–30s
```

with:

```go
	}
}

// The race suite's flags (Plan 5b).
func TestRaceSuiteFlags(t *testing.T) {
	cfg, _, _, o, err := parseFlags([]string{"-id", "gw-0", "-faults", "-clock-offset", "-5s",
		"-retry-backoff", "100ms", "-retry-max-delay", "1s"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.faults || o.clockOffset != -5*time.Second || cfg.RetryBackoff != 100*time.Millisecond || cfg.RetryMaxDelay != time.Second {
		t.Fatalf("parsed %+v %+v", cfg, o)
	}
}

func TestRefusesBadConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-handshake-timeout", "2s"}, // spec: 3–30s
```

In `cmd/gateway/main_test.go`, replace:

```go
	logouts       atomic.Int32
	ended         atomic.Int32
	endedLoggedIn atomic.Int32 // EpochEnded calls made before any Logout
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
```

with:

```go
	logouts       atomic.Int32
	ended         atomic.Int32
	endedLoggedIn atomic.Int32 // EpochEnded calls made before any Logout
	leaseSkewMs   atomic.Int64 // gw_now_ms − local time of the last GatewayLease
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
```

In `cmd/gateway/main_test.go`, replace:

```go
	return r, nil
}

func (f *fakeLogic) GatewayLease(context.Context, *pb.LeaseRequest) (*pb.LeaseReply, error) {
	return &pb.LeaseReply{}, nil
}

```

with:

```go
	return r, nil
}

func (f *fakeLogic) GatewayLease(_ context.Context, r *pb.LeaseRequest) (*pb.LeaseReply, error) {
	f.leaseSkewMs.Store(r.GetGwNowMs() - time.Now().UnixMilli())
	return &pb.LeaseReply{}, nil
}

```

In `cmd/gateway/main_test.go`, replace:

```go
	return &pb.EpochEndedReply{}, nil
}

// The whole binary: serve, log a client in over TCP + Noise, then SIGTERM
// (ctx done): the client gets Reconnect, the socket closes, its fenced
// Logout reaches Logic before EpochEnded, and run returns.
```

with:

```go
	return &pb.EpochEndedReply{}, nil
}

// -clock-offset shifts the clock the gateway stamps on everything it sends
// Logic, here gw_now_ms (P4I-5).
func TestClockOffsetShiftsTheGatewayClock(t *testing.T) {
	logic := &fakeLogic{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterLogicServer(s, logic)
	pb.RegisterLeaseServer(s, logic)
	go s.Serve(l)
	defer s.Stop()
	keys := t.TempDir()
	if err := keygen([]string{"-key-id", "1", "-out", filepath.Join(keys, "1")}, io.Discard); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-id", "gw-0", "-listen", "tcp://127.0.0.1:0", "-noise-keys", keys,
			"-logic", l.Addr().String(), "-lease", l.Addr().String(), "-kick-addr", "127.0.0.1:0",
			"-admin-addr", "127.0.0.1:0", "-loops", "1", "-conntrack-notrack", "-clock-offset", "2s",
			"-drain-window", "100ms", "-drain-flush", "0s"}, func(a string) { started <- a })
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for logic.leaseSkewMs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if skew := logic.leaseSkewMs.Load(); skew < 1500 || skew > 2500 {
		t.Fatalf("gw_now_ms is %dms ahead, want about 2000", skew)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The whole binary: serve, log a client in over TCP + Noise, then SIGTERM
// (ctx done): the client gets Reconnect, the socket closes, its fenced
// Logout reaches Logic before EpochEnded, and run returns.
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/gateway/ ./cmd/gateway/`
Expected: FAIL to build, `g.faults undefined`, `undefined: FaultKick`, `o.faults undefined`.

- [ ] **Step 3: Write the implementation**

Create `internal/gateway/fault.go`:

```go
package gateway

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fault ops a gateway serves (Plan 5b, choice #58), set by the race suite on
// a gateway started with -faults.
const (
	// FaultKick fails Kick RPCs with UNAVAILABLE before they reach a loop,
	// as a lost Kick looks to Logic (which retries while the lease is live).
	// Key: the conn_id in decimal; "" drops every Kick.
	FaultKick = "kick"
	// FaultLogic fails this gateway's calls to Logic with UNAVAILABLE: a
	// gateway cut off from Logic. Key: the method name, e.g. "GatewayLease";
	// "" blocks every call.
	FaultLogic = "logic"
)

// faultInfo is what GET /fault/info returns: the ids the race suite needs to
// find this gateway's keys in Redis.
type faultInfo struct {
	Gateway string `json:"gw"`
	Epoch   string `json:"epoch"` // decimal, as in the Redis keys
	Kick    string `json:"kick"`
	Admin   string `json:"admin"`
}

// logicFault applies a FaultLogic rule to one call to method.
func (g *Gateway) logicFault(ctx context.Context, method string) error {
	h, ok := g.faults.Match(FaultLogic, method[strings.LastIndexByte(method, '/')+1:])
	if !ok {
		return nil
	}
	if err := h.Wait(ctx); err != nil {
		return status.Errorf(codes.Unavailable, "fault: Logic blocked: %v", err)
	}
	return nil
}

func (g *Gateway) faultUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := g.logicFault(ctx, method); err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func (g *Gateway) faultStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if err := g.logicFault(ctx, method); err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

// kickFault applies a FaultKick rule to a Kick for connID.
func (g *Gateway) kickFault(ctx context.Context, connID uint64) error {
	h, ok := g.faults.Match(FaultKick, strconv.FormatUint(connID, 10))
	if !ok {
		return nil
	}
	if err := h.Wait(ctx); err != nil {
		return status.Errorf(codes.Unavailable, "fault: Kick dropped: %v", err)
	}
	return nil
}
```

In `internal/gateway/gateway.go`, replace:

```go

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)
```

with:

```go

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)
```

In `internal/gateway/gateway.go`, replace:

```go
	// ReadyChecks are the transport's readiness conditions, e.g. the Noise
	// key set is loaded (Plan 4).
	ReadyChecks []func() error
}

// New builds the gateway core. Call Start to connect it to Logic.
```

with:

```go
	// ReadyChecks are the transport's readiness conditions, e.g. the Noise
	// key set is loaded (Plan 4).
	ReadyChecks []func() error
	// Faults, if set, drops Kicks and blocks calls to Logic on request and
	// serves /fault/ on the admin port (the race suite, -faults). Nil in
	// production.
	Faults *fault.Set
}

// New builds the gateway core. Call Start to connect it to Logic.
```

In `internal/gateway/gateway.go`, replace:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks, limits: hostLimits}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

with:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks, limits: hostLimits, faults: d.Faults}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

In `internal/gateway/server.go`, replace:

```go
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
```

with:

```go
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
```

In `internal/gateway/server.go`, replace:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	limits      limitSource
	limitsErr   error

```

with:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	faults      *fault.Set // nil in production
	limits      limitSource
	limitsErr   error

```

In `internal/gateway/server.go`, replace:

```go
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])
	g.leaseC = pb.NewLeaseClient(g.clients[1])

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
```

with:

```go
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])
	g.leaseC = pb.NewLeaseClient(g.clients[1])
	g.recon = newReconciler(g, logic)

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
```

In `internal/gateway/server.go`, replace:

```go
		}
		w.WriteHeader(http.StatusOK)
	})
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
```

with:

```go
		}
		w.WriteHeader(http.StatusOK)
	})
	if g.faults != nil {
		mux.Handle("/fault/", g.faults.Handler(func() any {
			kick, admin := g.Addrs()
			return faultInfo{Gateway: g.cfg.GatewayID, Epoch: strconv.FormatUint(g.cfg.Epoch, 10), Kick: kick, Admin: admin}
		}, map[string]func(){"reconcile": g.recon.Trigger}))
	}
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
```

In `internal/gateway/server.go`, replace:

```go
	g.goRun(func() { g.watchLogic(ctx, g.clients[0]) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	g.recon = newReconciler(g, logic)
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
```

with:

```go
	g.goRun(func() { g.watchLogic(ctx, g.clients[0]) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
```

In `internal/gateway/server.go`, replace:

```go
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
		grpc.WithChainUnaryInterceptor(g.observeUnary),
		grpc.WithChainStreamInterceptor(g.observeStream))
}

// rpcCtx bounds a call to Logic and tags it with a request ID (at most 64
```

with:

```go
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
		grpc.WithChainUnaryInterceptor(g.observeUnary, g.faultUnary),
		grpc.WithChainStreamInterceptor(g.observeStream, g.faultStream))
}

// rpcCtx bounds a call to Logic and tags it with a request ID (at most 64
```

In `internal/gateway/kick.go`, replace:

```go
	g *Gateway
}

func (k kickServer) Kick(_ context.Context, req *pb.KickRequest) (*pb.KickReply, error) {
	g := k.g
	// A Kick for another epoch of this pod name is ignored (race 4).
	if req.GetGw().GetGateway() != g.cfg.GatewayID || req.GetGw().GetEpoch() != g.cfg.Epoch {
		g.m.Kicks.WithLabelValues("ignored").Inc()
```

with:

```go
	g *Gateway
}

func (k kickServer) Kick(ctx context.Context, req *pb.KickRequest) (*pb.KickReply, error) {
	g := k.g
	if err := g.kickFault(ctx, req.GetConnId()); err != nil {
		return nil, err
	}
	// A Kick for another epoch of this pod name is ignored (race 4).
	if req.GetGw().GetGateway() != g.cfg.GatewayID || req.GetGw().GetEpoch() != g.cfg.Epoch {
		g.m.Kicks.WithLabelValues("ignored").Inc()
```

In `cmd/gateway/main.go`, replace:

```go
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/wire"
```

with:

```go
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/wire"
```

In `cmd/gateway/main.go`, replace:

```go
type options struct {
	keyDir       string
	drainTimeout time.Duration
}

// window is a flag holding a uniform wait "lo-hi", e.g. "1s-3s".
```

with:

```go
type options struct {
	keyDir       string
	drainTimeout time.Duration
	faults       bool
	// clockOffset shifts the gateway's wall clock for the race suite's
	// clock-skew scenario (R4M-5, P4I-5). It is fixed at start: the clock
	// also drives the timing wheel, and a step at run time would fire
	// timers early.
	clockOffset time.Duration
}

// window is a flag holding a uniform wait "lo-hi", e.g. "1s-3s".
```

In `cmd/gateway/main.go`, replace:

```go
	fs.StringVar(&tc.Addr, "listen", tc.Addr, "client TCP listen address (gnet)")
	fs.StringVar(&o.keyDir, "noise-keys", o.keyDir, "Noise key Secret directory: one file per key_id")
	fs.DurationVar(&o.drainTimeout, "drain-timeout", o.drainTimeout, "SIGTERM drain limit; keep below terminationGracePeriodSeconds")

	fs.StringVar(&sc.LogicTarget, "logic", sc.LogicTarget, "Logic gRPC target (Login/Logout/Reconcile)")
	fs.StringVar(&sc.LeaseTarget, "lease", sc.LeaseTarget, "Logic Lease gRPC target (dedicated channel)")
```

with:

```go
	fs.StringVar(&tc.Addr, "listen", tc.Addr, "client TCP listen address (gnet)")
	fs.StringVar(&o.keyDir, "noise-keys", o.keyDir, "Noise key Secret directory: one file per key_id")
	fs.DurationVar(&o.drainTimeout, "drain-timeout", o.drainTimeout, "SIGTERM drain limit; keep below terminationGracePeriodSeconds")
	fs.BoolVar(&o.faults, "faults", false, "serve the test-only fault API on the admin port (race suite only, never in production)")
	fs.DurationVar(&o.clockOffset, "clock-offset", 0, "shift the wall clock by this much (race suite clock skew only)")

	fs.StringVar(&sc.LogicTarget, "logic", sc.LogicTarget, "Logic gRPC target (Login/Logout/Reconcile)")
	fs.StringVar(&sc.LeaseTarget, "lease", sc.LeaseTarget, "Logic Lease gRPC target (dedicated channel)")
```

In `cmd/gateway/main.go`, replace:

```go
	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", cfg.LeaseTimeout, "GatewayLease call timeout")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "Logic's lease TTL (the trigger rule)")
	fs.DurationVar(&cfg.RetryFor, "retry-for", cfg.RetryFor, "Logout and ReconcileEnd retry budget")
	fs.DurationVar(&cfg.Reconcile.Cycle, "reconcile-cycle", cfg.Reconcile.Cycle, "periodic Reconcile cycle")
	fs.DurationVar(&cfg.Reconcile.Jitter, "reconcile-jitter", cfg.Reconcile.Jitter, "periodic Reconcile jitter")
	fs.IntVar(&cfg.Reconcile.TriggeredRate, "triggered-rate", cfg.Reconcile.TriggeredRate, "triggered pass entries/s")
```

with:

```go
	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", cfg.LeaseTimeout, "GatewayLease call timeout")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "Logic's lease TTL (the trigger rule)")
	fs.DurationVar(&cfg.RetryFor, "retry-for", cfg.RetryFor, "Logout and ReconcileEnd retry budget")
	fs.DurationVar(&cfg.RetryBackoff, "retry-backoff", cfg.RetryBackoff, "first Logout and ReconcileEnd retry delay, doubled per attempt")
	fs.DurationVar(&cfg.RetryMaxDelay, "retry-max-delay", cfg.RetryMaxDelay, "longest delay between retries")
	fs.DurationVar(&cfg.Reconcile.Cycle, "reconcile-cycle", cfg.Reconcile.Cycle, "periodic Reconcile cycle")
	fs.DurationVar(&cfg.Reconcile.Jitter, "reconcile-jitter", cfg.Reconcile.Jitter, "periodic Reconcile jitter")
	fs.IntVar(&cfg.Reconcile.TriggeredRate, "triggered-rate", cfg.Reconcile.TriggeredRate, "triggered pass entries/s")
```

In `cmd/gateway/main.go`, replace:

```go
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	var tr *tcp.Transport // set before Start; Ready reads it only after
	g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		return err
	}
```

with:

```go
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	var tr *tcp.Transport // set before Start; Ready reads it only after
	d := gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}}
	if o.faults {
		d.Faults = fault.New()
		slog.Warn("gateway: -faults is set: the test-only fault API is on the admin port")
	}
	if off := o.clockOffset; off != 0 {
		d.Clock = func() time.Time { return time.Now().Add(off) }
		slog.Warn("gateway: -clock-offset is set: the wall clock is shifted", "offset", off)
	}
	g, err := gateway.New(cfg, d)
	if err != nil {
		return err
	}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./... && go test -race -count=1 ./internal/gateway/... ./cmd/gateway/`
Expected: `ok`, including `TestGatewayDoesNotImportTheStore` (`internal/fault` has no Redis dependency). Then `go test -race -count=1 ./...`.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway cmd/gateway
git commit -m "feat(gateway): fault hooks, on-demand Reconcile, -clock-offset (Plan 5b, P4I-5)"
```

---

## After this part

Next is Plan 5b2 (`2026-10-08-connection-session-core-05b2-race-harness.md`, Tasks 4–5), in a fresh session on the same branch.
