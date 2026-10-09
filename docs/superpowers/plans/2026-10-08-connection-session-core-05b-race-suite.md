# Race Integration Suite Implementation Plan — Plan 5b of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

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
    - The late-write ghost uses **+5s, not the spec's +2s** (amended at the review from the +3s first proposed). Fence (b) fires 6–8s after recv: the wheel rounds a deadline up to a whole second, and the 1s ticker that advances it has its own phase (finding 12). +5s puts the Lua deadline at 10s of real time, and the Login held 9s lands a second clear of both the latest fence and the deadline. With +2s the deadline is 7s, so a write after the fence fits only in some runs; with +3s and a 7.5s hold it failed once in three full runs. The repair path checked is the same at any offset.
64. **Tier 2 entry point.** `make test-tier2 SCENARIO=race` runs `scripts/tier2/race.sh` with four checks:
    - every scenario passes;
    - every listed scenario ran (no skips);
    - a mutation (fence (a) removed) is caught by race 5;
    - the suite ran on the pinned `redis-server` 7.4.11 (`TestMain` prints the version it found).

    *Amended at the review:* the script builds the pinned Redis (`make redis-server`) instead of starting the compose one, and the fourth check replaces "the Redis uses `noeviction`", which each stack now sets itself. Results go to `tier2-results/race-<ts>/`, with one `.report.txt` to send back.

## Execution Notes

- **Branch:** the branches keep stacking (the user's decision, 2026-10-08). Plan 5b branches from Plan 5a: `git checkout -b feat/plan5b-race feat/plan5a-sdk`. Commit this plan and the roadmap edit before Task 1, so the branch diff holds only Plan 5b's code.
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
  12. *The wheel fires up to 2s late, not 1s* (found at the review, on `redis-server`). Full run 8 failed `TestClockAheadAlertsAndItsLateWriteIsCleanedUp` once: the connection was still tracked when the ghost appeared. Polling the gateway's `tracked` gauge over 15 runs showed fence (b) untracking at 6.92–7.00s after recv in 13 runs and at 7.92–7.99s in 2, together with Busy (both are 6s wheel timers). The cause: `wheel.schedule` rounds a deadline up to a whole second of the clock, and `advance` runs from a 1s `time.Ticker` with an unrelated phase, so a timer fires 0–2s late (`wheel.go` says up to one tick). Firing late is safe for every fence and timeout, and the SDK's 10s Login timeout is above the 8s worst case for Busy, so the gateway is not changed. The scenario moved from +3s / 7.5s to +5s / 9s (choice #63); its window had 0.5s of margin, and a late tick took all of it. With the wider window it passed 10 of 10 runs alone and every full run after. *For the user:* the `wheel.go` comment, and Plan 3's "fires at most one tick late", should say 0–2s; a fix would align the ticker to the second, which this plan does not do.
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

### Task 4: The Redis build and real Redis on the dev machine (choice #62)

**Files:**
- Create: `scripts/redis-build.sh`, `scripts/redis-local.sh`
- Modify: `Makefile`, `.gitignore`, `internal/testredis/testredis.go`

**Interfaces:**
- Consumes: the Redis 7.4.11 release tarball (`download.redis.io`) and its published SHA-256 (`github.com/redis/redis-hashes`); `testredis` and its `GOIM_TEST_REDIS`/`GOIM_TEST_REDIS_REPLICA` switch (Plan 1).
- Produces: `scripts/redis-build.sh [PREFIX]`, which leaves `PREFIX/bin/redis-server` and `redis-cli` (default `.cache/redis`); `make redis-server`; the Makefile variable `REDIS_SERVER` (`redis-server` on `PATH`, else the pinned build), which Task 5's `make test-race-suite` passes to the suite; `scripts/redis-local.sh REDIS_SERVER COMMAND...`; `make test-redis-local`.

The suite runs a real `redis-server` primary and replica per scenario (choice #62), and Docker is never installed on the dev machine. Redis builds from source with only cc and make, so the script pins the version the Tier 2 compose image runs, checks the tarball against its published hash, and installs into a git-ignored directory. It does nothing when the version is already there. A `redis-server` 7.0 or later on `PATH` is used in its place (Task 5's `TestMain` checks the version); on the dev machine the prototype used the same 7.4.11 build, installed into `~/.local`.

With `redis-server` on the dev machine, the Tier 1a tests that skip as `TODO: Tier 2 integration` (Plan 1's `testredis.RequireReal`: replication, `WAIT`, `DEBUG SLEEP` on the replica) can run there too (the user's decision at the review). `scripts/redis-local.sh` starts what the compose file starts, a primary and a replica that accepts `DEBUG` on ports 16379 and 16380, as two processes, runs a command against them and stops them. `make test-redis-local` runs `go test -race ./...` that way, the same tests `make test-redis` runs on the Tier 2 host. The Tier 2 `store` scenario stays; it runs them on the compose Redis.

- [ ] **Step 1: Write the scripts**

Create `scripts/redis-build.sh`:

```bash
#!/usr/bin/env bash
# Builds the Redis the race suite runs (Plan 5b, choice #62): redis-server
# 7.4.11, the version the Tier 2 compose image runs, from the release tarball
# checked against its published SHA-256 (github.com/redis/redis-hashes). It
# needs cc, make and curl, not Docker. It does nothing if PREFIX already holds
# this version.
#
# Usage: scripts/redis-build.sh [PREFIX]   (default .cache/redis; make redis-server)
set -euo pipefail
version=7.4.11
sha256=3c266ece0abd54ed3b1c912c6eb86b7508cf382cb690ee6649d3843f018f6357

prefix=${1:-.cache/redis}
mkdir -p "$prefix"
prefix=$(cd "$prefix" && pwd)
if "$prefix/bin/redis-server" --version 2>/dev/null | grep -q "v=$version "; then
  echo "redis-server $version is built: $prefix/bin/redis-server"
  exit 0
fi
src=$(mktemp -d)
trap 'rm -rf "$src"' EXIT
curl -fsSL -o "$src/redis.tar.gz" "https://download.redis.io/releases/redis-$version.tar.gz"
echo "$sha256  $src/redis.tar.gz" | sha256sum -c --quiet
tar -xzf "$src/redis.tar.gz" -C "$src"
# libc malloc: no jemalloc build, and the suite does not measure memory.
if ! make -C "$src/redis-$version" -j"$(getconf _NPROCESSORS_ONLN)" BUILD_TLS=no MALLOC=libc >"$src/build.log" 2>&1 ||
  ! make -C "$src/redis-$version" PREFIX="$prefix" install >>"$src/build.log" 2>&1; then
  tail -n 50 "$src/build.log" >&2
  exit 1
fi
"$prefix/bin/redis-server" --version
```

Create `scripts/redis-local.sh`:

```bash
#!/usr/bin/env bash
# Runs a command against a local Redis primary and replica: two redis-server
# processes, no Docker (Plan 5b). They take the Tier 2 compose Redis's place
# on the dev machine: the same ports (GOIM_REDIS_PORT, GOIM_REDIS_REPLICA_PORT;
# 16379 and 16380), nothing persisted, noeviction, and a replica that accepts
# DEBUG (DEBUG SLEEP is a replica that stops acknowledging). The command runs
# with GOIM_TEST_REDIS and GOIM_TEST_REDIS_REPLICA set; both processes are
# stopped when it ends, and its exit code is the script's.
#
# Usage: scripts/redis-local.sh REDIS_SERVER COMMAND...   (make test-redis-local)
set -uo pipefail
server=$1
shift
port=${GOIM_REDIS_PORT:-16379}
replica_port=${GOIM_REDIS_REPLICA_PORT:-16380}
cli="$(dirname "$server")/redis-cli"
for f in "$server" "$cli"; do
  [ -x "$f" ] || { echo "redis-local: $f not found (make redis-server)" >&2; exit 2; }
done
for p in "$port" "$replica_port"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "redis-local: port $p is in use; pick others with GOIM_REDIS_PORT / GOIM_REDIS_REPLICA_PORT" >&2
    exit 2
  fi
done

dir=$(mktemp -d)
pids=()
stop() {
  kill "${pids[@]}" 2>/dev/null
  wait "${pids[@]}" 2>/dev/null
  rm -rf "$dir"
}
trap stop EXIT
mkdir "$dir/primary" "$dir/replica"
common=(--bind 127.0.0.1 --save "" --appendonly no --maxmemory-policy noeviction
  --repl-diskless-sync yes --repl-diskless-sync-delay 0)
"$server" --port "$port" --dir "$dir/primary" "${common[@]}" >"$dir/primary.log" 2>&1 &
pids+=($!)
"$server" --port "$replica_port" --dir "$dir/replica" "${common[@]}" \
  --replicaof 127.0.0.1 "$port" --enable-debug-command yes >"$dir/replica.log" 2>&1 &
pids+=($!)

for _ in $(seq 100); do
  if "$cli" -p "$replica_port" info replication 2>/dev/null | grep -q master_link_status:up; then
    linked=1
    break
  fi
  sleep 0.1
done
if [ -z "${linked:-}" ]; then
  echo "redis-local: the replica did not link within 10s" >&2
  tail -n 20 "$dir/primary.log" "$dir/replica.log" >&2
  exit 2
fi
echo "redis-local: $("$server" --version | cut -d' ' -f3) primary 127.0.0.1:$port, replica 127.0.0.1:$replica_port"

GOIM_TEST_REDIS=127.0.0.1:$port GOIM_TEST_REDIS_REPLICA=127.0.0.1:$replica_port "$@"
```

In `Makefile`, replace:

```make
.PHONY: test proto redis-up redis-down test-redis test-tier2

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...
```

with:

```make
.PHONY: test redis-server test-redis-local proto redis-up redis-down test-redis test-tier2

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# The race suite's Redis: redis-server on PATH, else the pinned 7.4.11 that
# make redis-server builds into .cache/redis (cc and make, no Docker).
REDIS_SERVER ?= $(or $(shell command -v redis-server 2>/dev/null),$(CURDIR)/.cache/redis/bin/redis-server)
redis-server:
	scripts/redis-build.sh .cache/redis

# Tier 1a on real Redis, on the dev machine: every test, including the ones
# that skip as "TODO: Tier 2 integration", against a local redis-server
# primary and replica (no Docker). The tests make test-redis runs on the Tier 2 host.
test-redis-local:
	scripts/redis-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
```

In `.gitignore`, replace:

```
*.DS_Store
```

with:

```
*.DS_Store
.cache/
```

In `internal/testredis/testredis.go`, replace:

```go
// Tier 1a (dev machine, the default): every New starts a fresh in-process
// miniredis. Tier 2 (dedicated host): set GOIM_TEST_REDIS (and
// GOIM_TEST_REDIS_REPLICA) to a real Redis primary with one replica, e.g. the
// one `make redis-up` starts from deploy/compose/redis-test.yml. Tests that
// need real Redis behaviour (replication, WAIT, DEBUG) call RequireReal and
// are skipped at Tier 1a; Plan 1's Execution Notes list what that leaves unverified.
```

with:

```go
// Tier 1a (dev machine, the default): every New starts a fresh in-process
// miniredis. With GOIM_TEST_REDIS (and GOIM_TEST_REDIS_REPLICA) set, tests run
// against a real Redis primary with one replica: on the dev machine `make
// test-redis-local` starts two redis-server processes (no Docker); on the Tier
// 2 host `make redis-up` starts the compose one. Tests that need real Redis
// behaviour (replication, WAIT, DEBUG) call RequireReal and are skipped
// without it; Plan 1's Execution Notes list what that leaves unverified.
```

In `internal/testredis/testredis.go`, replace:

```go
		t.Skip("TODO: Tier 2 integration — needs a real Redis primary and replica " +
			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA); see Plan 1 Execution Notes")
```

with:

```go
		t.Skip("TODO: Tier 2 integration — needs a real Redis primary and replica " +
			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA; make test-redis-local); see Plan 1 Execution Notes")
```

In `internal/testredis/testredis.go`, replace:

```go
		t.Fatalf("test Redis not reachable at %s (on the Tier 2 host: `make redis-up`): %v", addr, err)
```

with:

```go
		t.Fatalf("test Redis not reachable at %s (make test-redis-local, or `make redis-up` on the Tier 2 host): %v", addr, err)
```

- [ ] **Step 2: Build Redis**

Run: `chmod +x scripts/redis-build.sh scripts/redis-local.sh && make redis-server && make redis-server`
Expected: the first run downloads, checks and builds (about 1½ minutes on the dev machine) and prints `Redis server v=7.4.11 …`; the second prints `redis-server 7.4.11 is built: …/.cache/redis/bin/redis-server`. `git status` shows no `.cache/`.

- [ ] **Step 3: Run Tier 1a on real Redis**

Run: `make test-redis-local 2>&1 | tee /tmp/real.log; grep -c 'TODO: Tier 2 integration' /tmp/real.log`
Expected: `redis-local: v=7.4.11 primary 127.0.0.1:16379, replica 127.0.0.1:16380`, every package `ok` (about a minute on the dev machine), and `0`: no test skipped. Afterwards nothing listens on 16379 or 16380.

- [ ] **Step 4: Commit**

```bash
git add scripts/redis-build.sh scripts/redis-local.sh Makefile .gitignore internal/testredis/testredis.go
git commit -m "build: pinned redis-server 7.4.11; Tier 1a on real Redis without Docker (choice #62)"
```

---

### Task 5: The process harness (choices #59–#62; Review Focus 1–5)

**Files:**
- Create: `internal/racetest/doc.go`, `main_test.go`, `proc_test.go`, `profile_test.go`, `redis_test.go`, `stack_test.go`, `client_test.go`, `sdk_test.go`, `metrics_test.go`
- Modify: `Makefile`
- Test: `internal/racetest/smoke_test.go`, `internal/racetest/harness_test.go`

**Interfaces:**
- Consumes: the four binaries; `clientconn` (Plan 5a: `Dial`, `Handshake`, `Send`, `Recv`, `ExpectClosed`, `IsClosed`); `sdk` (Plan 5a: `Config`, `Windows`, `Backoff`, `HeartbeatRange`, `MemStore`, `Event`); `auth.GenerateKey`, `MarshalJWKS`, `Sign`; `wire.GenerateKey`, `EncodeKey`; `session.Decode`; `redis-server` (Task 4); `logic.Fault*`, `gateway.Fault*` (Tasks 2–3).
- Produces, for Tasks 6–11:
  - stacks: `New(t, Options) *Stack`; `Options{Gateways int; Logic, Gateway []string; PeriodicPasses, NoJWKS, NoFallback bool}`; `Stack{Redis *Redis; Logic *Logic; GW []*Gateway}`; `Gateway{ID string; Epoch uint64; Listen, Kick, Admin string}`; `s.RestartGateway(i, flags...)`, `s.RestartLogic()`; `base(t, Options)`, `s.launchLogic()`; on any process `kill()`, `pause()`, `resume()`, `term(d)`, `exited()`.
  - faults: `s.LogicFault(fault.Rule)`, `s.Fault(i, rule)`, `s.ClearFault(i, op)`, `s.Reconcile(i)`, `s.LogicFired(op)`, `s.PublishKeys(keys...)`.
  - clients: `s.Dial(i) *Client`, `s.Token(uid)`, `s.TokenAt(uid, iatMs, ttl)`, `s.TokenWith(key, uid, platform, iatMs, ttl)`, `s.Req(uid, device, seq)`, `s.Online(i, uid, device, seq)`, `s.OnlineWith(i, req)`; on a `Client`: `Send`, `Next`, `Login`, `ExpectKick`, `Quiet`; `describe(frame)`, `errorCode(t, frame)`; `s.SDK(uid, store, gws...) (*sdk.Client, *events)` with `events.next`, `events.none`.
  - Redis: `s.Redis.PauseFor`, `ReplicaDownFor`, `Client`, `Entry`, `EntryOf`, `has`, `Put`, `Del`, `Ghost`, `LeaseExists`, `DelLease`, `Score`, `SetScore`, `Conns`, `Now`; `epochSuffix`; the constant `slot = "im:mobile"`.
  - checks: `s.Metric(admin, series)`, `s.gwMetric(i, series)`, `s.logicMetric(series)`, `eventually(t, what, d, cond)`, `holds(t, what, d, cond)`, `sprintf`, `dur`, `s.passDone(i, n, d)`, `s.endsOK(i, n, d)`, and the series constants in `metrics_test.go`.

How a scenario's processes are wired: Logic gets `-redis <addr> -redis-cluster=false`, `-kick-addr-file`, `-faults` and the profile. Each gateway `gw-<i>` gets one loop, caps of 1000, `-conntrack-notrack`, `-faults`, the profile, and `-reconcile-cycle 1h` unless `PeriodicPasses` is set. Tokens are signed in the test process with the key devtoken publishes (`auth.Sign`, the code `devtoken issue` runs); devtoken itself serves the JWKS file, which it re-reads at every request, so `PublishKeys` rotates keys.

Cleanup order matters, and `t.Cleanup` runs last-in first-out. `base` registers the log dump, then the race check. Each process, the two `redis-server` processes included, registers its own kill after that, and the fault check is registered at the first rule. So on the way out: the fault counts are read from live processes, the processes are killed and checked for unexpected exits, the logs are scanned for races, and the log tails are printed if anything failed.

- [ ] **Step 1: Write the failing tests**

Create `internal/racetest/smoke_test.go`:

```go
package racetest

import (
	"testing"
)

// The stack comes up: every gateway is Ready with its own epoch, renews its
// lease, and logs a client in; Logic writes the entry with the gateway's
// epoch.
func TestStackComesUp(t *testing.T) {
	s := New(t, Options{})
	for i, g := range s.GW {
		if !s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s: no lease key", g.ID)
		}
		uid := "smoke" + g.ID[3:]
		s.Online(i, uid, "d1", 1)
		e, ok := s.Redis.Entry(uid)
		if !ok || e.Gateway != g.ID || e.Epoch != g.Epoch {
			t.Fatalf("%s: entry %+v %v", uid, e, ok)
		}
	}
}
```

Create `internal/racetest/harness_test.go`:

```go
package racetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
)

// The harness fails a scenario for problems its assertions can't see
// (Review Focus 1–3). Each case runs TestHelperScenario, a deliberately
// broken scenario, in a child go test process and expects it to fail with
// the reason.
func TestHarnessFailsWhatAssertionsMiss(t *testing.T) {
	suite(t)
	for _, c := range []struct{ mode, want string }{
		{mode: "unexpected-exit", want: "exited on its own"},
		{mode: "unfired-fault", want: "never fired"},
		{mode: "race-report", want: "the race detector reported a data race"},
	} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperScenario$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), "GOIM_RACETEST_HELPER="+c.mode, "GOIM_RACE_BIN="+binDir, "GOIM_RACE_LOGS=",
			"GOIM_REDIS_SERVER="+redisServer)
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), c.want) {
			t.Errorf("%s: the helper scenario %s; want it to fail with %q:\n%s", c.mode,
				map[bool]string{true: "passed", false: "failed otherwise"}[err == nil], c.want, out)
		}
	}
}

// TestHelperScenario is the broken scenario TestHarnessFailsWhatAssertionsMiss
// runs in a child process. Run on its own, it does nothing.
func TestHelperScenario(t *testing.T) {
	mode := os.Getenv("GOIM_RACETEST_HELPER")
	if mode == "" {
		return
	}
	s := New(t, Options{Gateways: 1})
	switch mode {
	case "unexpected-exit": // a gateway dies, and nothing the scenario checks notices
		s.GW[0].signal(syscall.SIGKILL)
		<-s.GW[0].done
	case "unfired-fault": // a typo in the key: the scenario injects nothing
		s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: "nobody", Fail: true})
		s.Online(0, "somebody", "d1", 1)
	case "race-report": // as -race prints it, then keeps running
		f, _ := os.OpenFile(s.Logic.log, os.O_APPEND|os.O_WRONLY, 0)
		f.WriteString("==================\nWARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n")
		f.Close()
	}
}

// The two Redis faults the scenarios use do what they claim (Review Focus
// 4). The replica is linked when the stack starts, so WAIT gets its ack;
// while the replica is stopped WAIT runs out its timeout with 0, and after
// it resumes it acknowledges again. A paused primary answers nothing until
// it resumes, and a key whose TTL ran out meanwhile is gone after.
func TestRedisFaultsDoWhatTheyClaim(t *testing.T) {
	suite(t)
	r := startRedis(t, t.TempDir())
	conn := r.Client().Conn() // WAIT counts the writes made on its own connection
	defer conn.Close()
	acked := func(timeout time.Duration) int64 {
		t.Helper()
		if err := conn.Incr(ctx, "w").Err(); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Wait(ctx, 1, timeout).Result()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := acked(time.Second); n != 1 {
		t.Fatalf("WAIT with the replica up = %d, want 1", n)
	}
	r.ReplicaDownFor(time.Second)
	start := time.Now()
	if n := acked(300 * time.Millisecond); n != 0 {
		t.Fatalf("WAIT with the replica stopped = %d, want 0", n)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("WAIT returned after %v, before its 300ms timeout", d)
	}
	time.Sleep(time.Second)
	if n := acked(2 * time.Second); n != 1 {
		t.Fatalf("WAIT after the replica resumed = %d, want 1", n)
	}

	if err := conn.Set(ctx, "lease", "1", 300*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	r.PauseFor(time.Second)
	start = time.Now()
	if err := r.Client().Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 800*time.Millisecond {
		t.Fatalf("the paused primary answered after %v", d)
	}
	if r.Client().Exists(ctx, "lease").Val() != 0 {
		t.Fatal("a key whose 300ms TTL ran out during the pause still exists")
	}
}

// A process startProc started dies with the test binary (Pdeathsig): a go
// test timeout or a panic, which skip the cleanups, leaves no gateway
// holding its ports (Review Focus 5). It needs no binaries: it runs at
// Tier 1a too.
func TestProcessesDieWithTheSuite(t *testing.T) {
	if os.Getenv("GOIM_RACETEST_SPAWN") != "" { // the child: start a process, then die without cleanups
		sleep, err := exec.LookPath("sleep")
		if err != nil {
			t.Skip("no sleep binary")
		}
		binDir = filepath.Dir(sleep)
		p := startProc(t, t.TempDir(), "sleep", "sleep", "60")
		os.Stdout.WriteString("pid " + strconv.Itoa(p.cmd.Process.Pid) + "\n")
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessesDieWithTheSuite$", "-test.count=1")
	cmd.Env = append(os.Environ(), "GOIM_RACETEST_SPAWN=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	var pid int
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "pid "); ok {
			pid, _ = strconv.Atoi(v)
		}
	}
	if pid == 0 {
		t.Fatalf("no pid in %q", out)
	}
	eventually(t, "the grandchild died with its parent", 5*time.Second, func() bool {
		return syscall.Kill(pid, 0) != nil
	})
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go vet ./internal/racetest/`
Expected: FAIL, `undefined: New`, `undefined: suite`.

- [ ] **Step 3: Write the harness**

Create `internal/racetest/doc.go`:

```go
// Package racetest is the race integration suite (spec: Verification → Race
// integration tests; Tier 1b; Plan 5b). Each scenario starts the real
// binaries as local processes, built with -race (choice #47): a Redis,
// devtoken serving the JWKS, one Logic and four gateways, all on the test
// timing profile and with the test-only fault API on. The scenario drives
// scripted clients (internal/clientconn) or SDK clients, kills, pauses and
// restarts processes with signals, injects faults through each process's
// /fault/ endpoint, and checks the outcome in Redis and in the metrics.
//
// The suite runs only with GOIM_RACE_SUITE=1 (make test-race-suite); a
// plain `go test ./...` skips it. Redis is a redis-server primary and
// replica started for each scenario (GOIM_REDIS_SERVER or redis-server on
// PATH; make redis-server builds the pinned 7.4.11), on the dev machine and
// on the Tier 2 host (make test-tier2 SCENARIO=race) alike. Every file but
// this one is a _test.go file, so nothing here ships.
package racetest
```

Create `internal/racetest/main_test.go`:

```go
package racetest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const envSuite = "GOIM_RACE_SUITE"

// binDir holds the binaries TestMain built: logic, gateway, devtoken.
var binDir string

// redisServer is the redis-server binary every stack runs: GOIM_REDIS_SERVER,
// or redis-server on PATH (choice #62).
var redisServer string

func TestMain(m *testing.M) {
	if os.Getenv(envSuite) == "" {
		os.Exit(m.Run()) // every scenario skips
	}
	var err error
	if redisServer, err = findRedis(); err != nil {
		fmt.Fprintln(os.Stderr, "racetest:", err)
		os.Exit(2)
	}
	if dir := os.Getenv("GOIM_RACE_BIN"); dir != "" { // a helper run reuses its parent's build
		binDir = dir
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "goim-race-bin-")
	if err == nil {
		err = build(dir)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "racetest: build:", err)
		os.Exit(2)
	}
	binDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// build compiles the binaries with the race detector, as the suite runs them.
func build(dir string) error {
	root, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-race", "-o", dir+string(filepath.Separator),
		"./cmd/logic", "./cmd/gateway", "./cmd/devtoken")
	cmd.Dir = strings.TrimSpace(string(root))
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v\n%s", err, out)
	}
	return nil
}

var redisVersion = regexp.MustCompile(`v=(\d+)\.(\d+)\.(\d+)`)

// findRedis returns the redis-server to run and prints its version. It
// refuses one older than 7.0; the pinned build is 7.4.11, the Tier 2
// compose version (make redis-server).
func findRedis() (string, error) {
	path := os.Getenv("GOIM_REDIS_SERVER")
	if path == "" {
		var err error
		if path, err = exec.LookPath("redis-server"); err != nil {
			return "", fmt.Errorf("no redis-server on PATH: run make redis-server, or set GOIM_REDIS_SERVER")
		}
	}
	out, err := exec.Command(path, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("%s --version: %v", path, err)
	}
	v := redisVersion.FindStringSubmatch(string(out))
	if v == nil {
		return "", fmt.Errorf("%s --version: no version in %q", path, out)
	}
	if major, _ := strconv.Atoi(v[1]); major < 7 {
		return "", fmt.Errorf("%s is Redis %s.%s.%s; the suite needs 7.0 or later (make redis-server builds 7.4.11)", path, v[1], v[2], v[3])
	}
	fmt.Printf("racetest: %s: Redis %s.%s.%s\n", path, v[1], v[2], v[3])
	return path, nil
}

// suite skips t unless the race suite is on.
func suite(t *testing.T) {
	t.Helper()
	if binDir == "" {
		t.Skip("race suite: set " + envSuite + "=1 (make test-race-suite)")
	}
}
```

Create `internal/racetest/proc_test.go`:

```go
package racetest

import (
	"bufio"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// proc is one running binary. Its stdout and stderr go to <dir>/<name>.log.
type proc struct {
	name    string
	cmd     *exec.Cmd
	log     string
	done    chan struct{} // closed when the process has exited
	stopped atomic.Bool   // the scenario killed or terminated it
}

// startProc starts bin (in binDir unless the path is absolute) with args. t's cleanup fails the scenario if
// the process exited on its own (a crash the assertions may not see), then
// kills it. The process also dies with the test binary (Pdeathsig), so a
// go test timeout leaves no gateway holding its ports.
func startProc(t *testing.T, dir, name, bin string, args ...string) *proc {
	t.Helper()
	path := filepath.Join(dir, name+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(bin) {
		bin = filepath.Join(binDir, bin)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = f, f
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p := &proc{name: name, cmd: cmd, log: path, done: make(chan struct{})}
	go func() {
		cmd.Wait()
		f.Close()
		close(p.done)
	}()
	t.Cleanup(func() {
		if p.exited() && !p.stopped.Load() {
			t.Errorf("%s exited on its own (%v):\n%s", name, p.cmd.ProcessState, strings.Join(p.tail(30), "\n"))
		}
		p.kill()
	})
	return p
}

func (p *proc) signal(s syscall.Signal) { _ = p.cmd.Process.Signal(s) }

// kill sends SIGKILL (a crash) and waits for the exit.
func (p *proc) kill() {
	p.stopped.Store(true)
	p.signal(syscall.SIGKILL) // also ends a stopped process
	<-p.done
}

// pause and resume freeze the process (SIGSTOP/SIGCONT): a zombie gateway,
// a Redis outage, or a replica that stops acknowledging writes.
func (p *proc) pause()  { p.signal(syscall.SIGSTOP) }
func (p *proc) resume() { p.signal(syscall.SIGCONT) }

// term sends SIGTERM and waits up to d for the exit.
func (p *proc) term(d time.Duration) bool {
	p.stopped.Store(true)
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// tail returns the last n lines of the log.
func (p *proc) tail(n int) []string {
	f, err := os.Open(p.log)
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return lines
}

// Ports come from a counter below Linux's ephemeral range (32768 and up),
// so a port handed to one process is never handed out again in the run,
// and no outgoing socket (a client, a gRPC dial) can take it before the
// process binds it. A port something else holds is skipped, and so is one
// with connections in TIME_WAIT from an earlier run: the probe binds
// without SO_REUSEADDR, as gnet's listener does.
const (
	firstPort = 20000
	lastPort  = 32000
)

var nextPort atomic.Int32

var strictListen = net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
	var err error
	c.Control(func(fd uintptr) { err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0) })
	return err
}}

// freePort returns a localhost address for a process to listen on.
func freePort(t *testing.T) string {
	t.Helper()
	for range lastPort - firstPort {
		p := firstPort + int(nextPort.Add(1))%(lastPort-firstPort)
		addr := "127.0.0.1:" + strconv.Itoa(p)
		if l, err := strictListen.Listen(context.Background(), "tcp", addr); err == nil {
			l.Close()
			return addr
		}
	}
	t.Fatal("no free port")
	return ""
}
```

Create `internal/racetest/profile_test.go`:

```go
package racetest

import (
	"strings"
	"testing"
	"time"
)

// The test timing profile (spec: Test execution model; choice #59): every
// time-based setting scaled ~10× down with its ratios kept. Lease TTL 3s,
// renewal every 1s, the sweeper guard's windows (derived from those two),
// Kick retries 1s apart, the Logout and ReconcileEnd retry budget 6s, the
// drain window 6s, heartbeat 15s, and the spec's 90s outages become 9s in
// the scenarios. login_deadline (5s) and the 1s skew allowance are not
// scaled: they are tied to real RPC latency. The sess:/rev: TTL stays 30d,
// and token lifetimes are set per scenario.
//
// The binaries check the ratios themselves (TestProfileBreakingARatioIsRefused).

// logicProfile is appended to every Logic's flags.
var logicProfile = []string{
	"-lease-ttl", "3s", "-renew-interval", "1s", "-redis-failover", "1500ms", "-lease-cache", "100ms",
	"-sweep-interval", "500ms",
	"-kick-retry-spacing", "1s", "-kick-idle-timeout", "6s",
	"-jwks-refresh", "30s", "-jwks-retry-empty", "500ms", "-jwks-unknown-kid", "3s",
}

// gatewayProfile is appended to every gateway's flags. The periodic
// Reconcile cycle scales to 60s ± 6s; scenarios run passes on demand
// instead (Options.PeriodicPasses, choice #60).
var gatewayProfile = []string{
	"-lease-interval", "1s", "-lease-timeout", "1s", "-lease-ttl", "3s",
	"-heartbeat", "15s", "-heartbeat-min", "3s", "-heartbeat-max", "30s",
	"-retry-for", "6s", "-retry-backoff", "100ms", "-retry-max-delay", "1s",
	"-reconcile-cycle", "60s", "-reconcile-jitter", "6s",
	"-drain-window", "6s", "-drain-jitter", "500ms", "-drain-flush", "1s", "-drain-timeout", "30s",
}

// The binaries refuse a profile that breaks a required ratio (spec: Test
// execution model): Logic's lease TTL must exceed the failover time plus a
// renewal interval (R2-O5), and the gateway's lease TTL its renewal interval.
func TestProfileBreakingARatioIsRefused(t *testing.T) {
	suite(t)
	dir := t.TempDir()
	logicArgs := append(append([]string{"-jwks-url", "http://127.0.0.1:1/"}, logicProfile...), "-redis-failover", "2s")
	gwArgs := append(append([]string{"-id", "gw-0", "-noise-keys", dir}, gatewayProfile...), "-lease-ttl", "1s")
	for _, c := range []struct {
		bin  string
		args []string
		want string
	}{{"logic", logicArgs, "R2-O5"}, {"gateway", gwArgs, "LeaseTTL"}} {
		p := startProc(t, dir, c.bin, c.bin, c.args...)
		select {
		case <-p.done:
			p.stopped.Store(true) // the exit is what this test wants
		case <-time.After(10 * time.Second):
			t.Fatalf("%s started with a broken ratio", c.bin)
		}
		if p.cmd.ProcessState.ExitCode() == 0 || !strings.Contains(strings.Join(p.tail(5), "\n"), c.want) {
			t.Fatalf("%s: exit %d, log %q; want a refusal naming %s", c.bin, p.cmd.ProcessState.ExitCode(), p.tail(5), c.want)
		}
	}
}
```

Create `internal/racetest/redis_test.go`:

```go
package racetest

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Redis is the stack's Redis: a redis-server primary and a replica, two
// processes started fresh for each scenario, on the dev machine and on the
// Tier 2 host alike (choice #62). Revoke's WAIT needs the replica.
type Redis struct {
	t                *testing.T
	Addr             string
	rdb              *redis.Client
	primary, replica *proc
}

// startRedis starts the primary and its replica and waits until the replica
// is linked, so a WAIT in the scenario fails only when the scenario stops it.
func startRedis(t *testing.T, dir string) *Redis {
	t.Helper()
	r := &Redis{t: t, Addr: freePort(t)}
	replicaAddr := freePort(t)
	host, port, _ := net.SplitHostPort(r.Addr)
	r.primary = startRedisServer(t, dir, "redis", r.Addr)
	r.replica = startRedisServer(t, dir, "redis-replica", replicaAddr, "--replicaof", host, port)
	r.rdb = redis.NewClient(&redis.Options{Addr: r.Addr})
	replica := redis.NewClient(&redis.Options{Addr: replicaAddr})
	t.Cleanup(func() { r.rdb.Close(); replica.Close() })
	eventually(t, "the replica is linked to the primary", 10*time.Second, func() bool {
		return strings.Contains(replica.Info(ctx, "replication").Val(), "master_link_status:up") &&
			strings.Contains(r.rdb.Info(ctx, "replication").Val(), "connected_slaves:1")
	})
	return r
}

// startRedisServer runs redis-server on addr with nothing persisted and the
// eviction policy production uses (noeviction). A diskless sync with no
// delay links a replica in well under a second.
func startRedisServer(t *testing.T, dir, name, addr string, extra ...string) *proc {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	data := filepath.Join(dir, name)
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	args := []string{"--bind", host, "--port", port, "--dir", data, "--save", "", "--appendonly", "no",
		"--maxmemory-policy", "noeviction", "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0"}
	return startProc(t, dir, name, redisServer, append(args, extra...)...)
}

// PauseFor makes Redis stop answering for d and returns at once: every
// client's commands block until it resumes (spec: Redis paused for 90s).
// The primary is SIGSTOPped; when it resumes, keys whose TTL ran out
// meanwhile expire, as Redis's expiry times are absolute.
func (r *Redis) PauseFor(d time.Duration) {
	r.primary.pause()
	time.AfterFunc(d, r.primary.resume)
}

// ReplicaDownFor stops the replica for d, so it acknowledges no write and a
// Revoke's WAIT times out with 0 replicas.
func (r *Redis) ReplicaDownFor(d time.Duration) {
	r.replica.pause()
	time.AfterFunc(d, r.replica.resume)
}

// Client is a plain client for checks and for writing state by hand.
func (r *Redis) Client() *redis.Client { return r.rdb }

const slot = "im:mobile" // every scenario logs in as appid im, platform mobile

// Entry returns the stored owner of uid's im:mobile slot.
func (r *Redis) Entry(uid string) (session.Entry, bool) { return r.EntryOf(uid, slot) }

// EntryOf returns the stored owner of uid's field.
func (r *Redis) EntryOf(uid, field string) (session.Entry, bool) {
	r.t.Helper()
	v, err := r.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		r.t.Fatal(err)
	}
	e, err := session.Decode(v)
	if err != nil {
		r.t.Fatalf("sess:{%s} %s = %q: %v", uid, field, v, err)
	}
	return e, true
}

func epochSuffix(gw string, epoch uint64) string {
	return gw + ":{" + strconv.FormatUint(epoch, 10) + "}"
}

// LeaseExists reports whether the epoch's lease key exists.
func (r *Redis) LeaseExists(gw string, epoch uint64) bool {
	return r.rdb.Exists(ctx, "gwlease:"+epochSuffix(gw, epoch)).Val() == 1
}

// DelLease deletes the epoch's lease key: the epoch reads as dead, and its
// next renewal reports missing.
func (r *Redis) DelLease(gw string, epoch uint64) {
	if err := r.rdb.Del(ctx, "gwlease:"+epochSuffix(gw, epoch)).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Score returns the epoch's gwepochs score; ok is false once it was swept.
func (r *Redis) Score(gw string, epoch uint64) (ms float64, ok bool) {
	v, err := r.rdb.ZScore(ctx, "gwepochs", gw+":"+strconv.FormatUint(epoch, 10)).Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		r.t.Fatal(err)
	}
	return v, true
}

// SetScore sets the epoch's gwepochs score, e.g. to make it look stale.
func (r *Redis) SetScore(gw string, epoch uint64, ms int64) {
	if err := r.rdb.ZAdd(ctx, "gwepochs", redis.Z{Score: float64(ms), Member: gw + ":" + strconv.FormatUint(epoch, 10)}).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Conns returns the epoch's gwconns members.
func (r *Redis) Conns(gw string, epoch uint64) []string {
	return r.rdb.SMembers(ctx, "gwconns:"+epochSuffix(gw, epoch)).Val()
}

// Now returns Redis TIME in ms.
func (r *Redis) Now() int64 {
	r.t.Helper()
	tm, err := r.rdb.Time(ctx).Result()
	if err != nil {
		r.t.Fatal(err)
	}
	return tm.UnixMilli()
}

// Put writes e as uid's im:mobile entry by hand and adds uid to the epoch's
// gwconns set, as a Login would: a ghost, or an entry a lost write left.
func (r *Redis) Put(uid string, e session.Entry) {
	r.t.Helper()
	if _, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, "sess:{"+uid+"}", slot, e.Encode())
		p.SAdd(ctx, "gwconns:"+epochSuffix(e.Gateway, e.Epoch), uid)
		return nil
	}); err != nil {
		r.t.Fatal(err)
	}
}

// Del deletes uid's im:mobile entry by hand, as a lost write or a sweep would.
func (r *Redis) Del(uid string) {
	if err := r.rdb.HDel(ctx, "sess:{"+uid+"}", slot).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Ghost writes an entry for gateway g's current epoch whose connection g
// never had (conn_id from the top of the range), logged in a minute ago.
func (r *Redis) Ghost(uid string, g *Gateway, connID uint64) session.Entry {
	r.t.Helper()
	now := r.Now()
	e := session.Entry{Gateway: g.ID, Epoch: g.Epoch, ConnID: connID, DeviceID: "ghost-" + uid, ConnSeq: 1,
		LoginMs: now - 60_000, TokIatMs: now - 61_000}
	r.Put(uid, e)
	return e
}

func (r *Redis) has(uid string) bool { _, ok := r.Entry(uid); return ok }
```

Create `internal/racetest/stack_test.go`:

```go
package racetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/wire"
)

// Options change a stack from the default: four gateways on the profile,
// Reconcile passes only on demand, and an account service that is up.
type Options struct {
	Gateways int      // 0 means 4 (spec: Redis, Logic, 4 gateways)
	Logic    []string // extra Logic flags, after the profile's (last wins)
	Gateway  []string // extra flags for every gateway
	// PeriodicPasses keeps the profile's 60s ± 6s Reconcile cycle. By
	// default each gateway runs passes only when a scenario asks
	// (Gateway.Reconcile), so no pass the scenario did not ask for can
	// repair what it is about to check (choice #60).
	PeriodicPasses bool
	// NoJWKS starts Logic with the account service down: only the
	// jwks-fallback file can give it a key set.
	NoJWKS bool
	// NoFallback starts Logic without a jwks-fallback file.
	NoFallback bool
}

// Stack is one scenario's processes.
type Stack struct {
	t   *testing.T
	dir string
	opt Options

	Redis *Redis
	Logic *Logic
	GW    []*Gateway

	jwtKey   auth.PrivateKey // signs tokens
	jwksPath string          // what devtoken serves, re-read per request
	jwksURL  string
	fallback string
	noise    wire.Key
	noiseDir string
	kickFile string
	kicks    map[string]string // gateway id → Kick address (kickFile)
	rules    []*setRule        // every fault rule the scenario set
}

// setRule is a fault rule the scenario set, checked at the end: a rule that
// never fired means the scenario did not exercise what it claims.
type setRule struct {
	p       *proc
	admin   string
	op      string
	fired   int // read when the rule was cleared
	cleared bool
}

// Logic is the stack's Logic process.
type Logic struct {
	*proc
	GRPC, Lease, Admin string
	args               []string
}

// Gateway is one gateway process. A restart keeps ID and gets a new Epoch.
type Gateway struct {
	*proc
	Index               int
	ID                  string
	Epoch               uint64
	Listen, Kick, Admin string
	extra               []string
}

// New starts a stack and waits until Logic and every gateway are Ready.
func New(t *testing.T, opt Options) *Stack {
	t.Helper()
	s := base(t, opt)
	s.Logic = s.launchLogic()
	s.waitReady(s.Logic.proc, s.Logic.Admin)
	s.GW = make([]*Gateway, s.opt.Gateways)
	for i := range s.GW {
		s.GW[i] = s.launchGateway(i, nil)
	}
	for _, g := range s.GW {
		s.awaitGateway(g)
	}
	return s
}

// base starts what Logic and the gateways need: Redis, the account service
// (devtoken) and its fallback file, the Noise key and the Kick address file.
func base(t *testing.T, opt Options) *Stack {
	t.Helper()
	suite(t)
	if opt.Gateways == 0 {
		opt.Gateways = 4
	}
	dir := t.TempDir()
	if d := os.Getenv("GOIM_RACE_LOGS"); d != "" {
		dir = filepath.Join(d, strings.ReplaceAll(t.Name(), "/", "_"))
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := &Stack{t: t, dir: dir, opt: opt, kicks: map[string]string{}, kickFile: filepath.Join(dir, "kick.json")}
	t.Cleanup(s.dumpOnFailure) // cleanups run last-in first-out: after checkRaces
	t.Cleanup(s.checkRaces)
	s.Redis = startRedis(t, dir)
	s.startJWKS()
	s.startNoise()
	s.setKick("", "")
	return s
}

// checkRaces fails the scenario if any process's race detector reported a
// data race. The processes are killed, not stopped, so their exit code
// can't carry it; the report is in the log.
func (s *Stack) checkRaces() {
	ents, _ := os.ReadDir(s.dir)
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		b, _ := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if i := strings.Index(string(b), "WARNING: DATA RACE"); i >= 0 {
			s.t.Errorf("%s: the race detector reported a data race:\n%s", e.Name(), string(b[i:min(len(b), i+4000)]))
		}
	}
}

// dumpOnFailure logs the end of every process log when the scenario failed.
func (s *Stack) dumpOnFailure() {
	if !s.t.Failed() {
		return
	}
	ents, _ := os.ReadDir(s.dir)
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		p := &proc{log: filepath.Join(s.dir, e.Name())}
		s.t.Logf("==== %s (last 40 lines) ====\n%s", e.Name(), strings.Join(p.tail(40), "\n"))
	}
}

func (s *Stack) startJWKS() {
	t := s.t
	var err error
	if s.jwtKey, err = auth.GenerateKey("k1"); err != nil {
		t.Fatal(err)
	}
	s.jwksPath = filepath.Join(s.dir, "jwks.json")
	s.PublishKeys(s.jwtKey)
	if !s.opt.NoFallback {
		s.fallback = filepath.Join(s.dir, "jwks-fallback.json")
		doc, _ := auth.MarshalJWKS(s.jwtKey)
		if err := os.WriteFile(s.fallback, doc, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	addr := freePort(t)
	s.jwksURL = "http://" + addr + "/"
	if s.opt.NoJWKS {
		return // nothing listens on addr: the account service is down
	}
	startProc(t, s.dir, "devtoken", "devtoken", "serve", "-addr", addr, "-jwks", s.jwksPath)
	eventually(t, "devtoken serves the JWKS", 10*time.Second, func() bool {
		resp, err := http.Get(s.jwksURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

// PublishKeys makes the account service serve exactly keys (devtoken
// re-reads the file at every request).
func (s *Stack) PublishKeys(keys ...auth.PrivateKey) {
	doc, err := auth.MarshalJWKS(keys...)
	if err != nil {
		s.t.Fatal(err)
	}
	tmp := s.jwksPath + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		s.t.Fatal(err)
	}
	if err := os.Rename(tmp, s.jwksPath); err != nil {
		s.t.Fatal(err)
	}
}

func (s *Stack) startNoise() {
	var err error
	if s.noise, err = wire.GenerateKey(); err != nil {
		s.t.Fatal(err)
	}
	s.noiseDir = filepath.Join(s.dir, "noise")
	priv, _ := wire.EncodeKey(s.noise)
	if err := os.MkdirAll(s.noiseDir, 0o700); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.noiseDir, "1"), []byte(priv+"\n"), 0o600); err != nil {
		s.t.Fatal(err)
	}
}

// setKick records gateway id's Kick address and rewrites the file Logic
// dials from (logic.FileDial), the local stand-in for pod DNS.
func (s *Stack) setKick(id, addr string) {
	if id != "" {
		s.kicks[id] = addr
	}
	doc, _ := json.Marshal(s.kicks)
	tmp := s.kickFile + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		s.t.Fatal(err)
	}
	if err := os.Rename(tmp, s.kickFile); err != nil {
		s.t.Fatal(err)
	}
}

// launchLogic starts Logic without waiting for Ready.
func (s *Stack) launchLogic() *Logic {
	t := s.t
	l := &Logic{GRPC: freePort(t), Lease: freePort(t), Admin: freePort(t)}
	l.args = []string{"-redis", s.Redis.Addr, "-redis-cluster=false",
		"-grpc-addr", l.GRPC, "-lease-addr", l.Lease, "-admin-addr", l.Admin,
		"-jwks-url", s.jwksURL, "-jwks-fallback", s.fallback, "-kick-addr-file", s.kickFile, "-faults"}
	l.args = append(append(l.args, logicProfile...), s.opt.Logic...)
	l.proc = startProc(t, s.dir, "logic", "logic", l.args...)
	return l
}

// RestartLogic kills Logic (a crash), starts it again on the same ports and
// waits until it is Ready.
func (s *Stack) RestartLogic() {
	s.t.Helper()
	l := s.Logic
	l.kill()
	l.proc = startProc(s.t, s.dir, "logic", "logic", l.args...)
	s.waitReady(l.proc, l.Admin)
}

// launchGateway starts gateway i (id gw-i) with extra flags after the
// stack's; awaitGateway waits until it is Ready.
func (s *Stack) launchGateway(i int, extra []string) *Gateway {
	t := s.t
	g := &Gateway{Index: i, ID: fmt.Sprintf("gw-%d", i), Listen: freePort(t), Kick: freePort(t), Admin: freePort(t), extra: extra}
	args := []string{"-id", g.ID, "-listen", "tcp://" + g.Listen, "-noise-keys", s.noiseDir,
		"-logic", s.Logic.GRPC, "-lease", s.Logic.Lease, "-kick-addr", g.Kick, "-admin-addr", g.Admin,
		"-loops", "1", "-max-conns", "1000", "-pending-cap", "1000", "-logout-queue", "1000",
		"-conntrack-notrack", "-faults"}
	args = append(args, gatewayProfile...)
	if !s.opt.PeriodicPasses {
		args = append(args, "-reconcile-cycle", "1h", "-reconcile-jitter", "0s")
	}
	args = append(append(args, s.opt.Gateway...), extra...)
	s.setKick(g.ID, g.Kick)
	g.proc = startProc(t, s.dir, g.ID, "gateway", args...)
	return g
}

// awaitGateway waits until g is Ready and reads its epoch.
func (s *Stack) awaitGateway(g *Gateway) {
	s.t.Helper()
	s.waitReady(g.proc, g.Admin)
	var info struct{ Epoch string }
	if err := getJSON("http://"+g.Admin+"/fault/info", &info); err != nil {
		s.t.Fatalf("%s: /fault/info: %v", g.ID, err)
	}
	g.Epoch, _ = strconv.ParseUint(info.Epoch, 10, 64)
}

// RestartGateway starts gateway i again under the same id (a new epoch)
// with extra flags, after its old process was killed, paused or ended.
func (s *Stack) RestartGateway(i int, extra ...string) *Gateway {
	s.t.Helper()
	g := s.launchGateway(i, extra)
	s.awaitGateway(g)
	s.GW[i] = g
	return g
}

// waitReady waits for /readyz to answer 200; it fails t if the process
// exits or 60s pass (race builds start slowly on two cores).
func (s *Stack) waitReady(p *proc, admin string) {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if p.exited() {
			s.t.Fatalf("%s exited before Ready:\n%s", p.name, strings.Join(p.tail(30), "\n"))
		}
		if resp, err := http.Get("http://" + admin + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatalf("%s not Ready after 60s:\n%s", p.name, strings.Join(p.tail(30), "\n"))
}

// ---- the fault API (choice #58) ----

func faultPut(t *testing.T, admin string, r fault.Rule) {
	t.Helper()
	body, _ := json.Marshal(r)
	req, _ := http.NewRequest(http.MethodPut, "http://"+admin+"/fault/rule", bytes.NewReader(body))
	httpDo(t, req)
}

func faultClear(t *testing.T, admin, op string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, "http://"+admin+"/fault/rule/"+op, nil)
	httpDo(t, req)
}

func httpDo(t *testing.T, req *http.Request) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s %s: %s %s", req.Method, req.URL, resp.Status, b)
	}
}

func getJSON(url string, v any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

// LogicFault sets a rule on Logic (ops: logic.FaultLogin, ...).
func (s *Stack) LogicFault(r fault.Rule) { s.setFault(s.Logic.proc, s.Logic.Admin, r) }

// Fault sets a rule on gateway i (ops: gateway.FaultKick, gateway.FaultLogic).
func (s *Stack) Fault(i int, r fault.Rule) { s.setFault(s.GW[i].proc, s.GW[i].Admin, r) }

// ClearFault removes gateway i's rule for op.
func (s *Stack) ClearFault(i int, op string) { s.clearFault(s.GW[i].Admin, op) }

func (s *Stack) setFault(p *proc, admin string, r fault.Rule) {
	s.t.Helper()
	faultPut(s.t, admin, r)
	if s.rules == nil {
		// Registered after the processes, so it runs before they are killed.
		s.t.Cleanup(s.checkFaultsFired)
	}
	s.rules = append(s.rules, &setRule{p: p, admin: admin, op: r.Op})
}

func (s *Stack) clearFault(admin, op string) {
	s.t.Helper()
	for _, r := range s.rules {
		if r.admin == admin && r.op == op && !r.cleared {
			r.fired, r.cleared = faultFired(s.t, admin, op), true
		}
	}
	faultClear(s.t, admin, op)
}

// checkFaultsFired fails the scenario for every rule that never matched,
// unless its process was killed (its counts died with it).
func (s *Stack) checkFaultsFired() {
	for _, r := range s.rules {
		n := r.fired
		if !r.cleared {
			if r.p.exited() {
				continue
			}
			var v struct{ Fired int }
			if err := getJSON("http://"+r.admin+"/fault/rule/"+r.op, &v); err != nil {
				s.t.Errorf("fault %q on %s: %v", r.op, r.p.name, err)
				continue
			}
			n = v.Fired
		}
		if n == 0 {
			s.t.Errorf("fault %q on %s never fired: the scenario did not exercise what it claims", r.op, r.p.name)
		}
	}
}

// Reconcile starts a triggered Reconcile pass on gateway i.
func (s *Stack) Reconcile(i int) {
	s.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+s.GW[i].Admin+"/fault/action/reconcile", nil)
	httpDo(s.t, req)
}

// ---- checks ----

// Metric returns the value of one series from admin's /metrics, written as
// the exposition prints it, e.g. `goim_gateway_kicks_total{result="ignored"}`;
// 0 if absent.
func (s *Stack) Metric(admin, series string) float64 {
	s.t.Helper()
	resp, err := http.Get("http://" + admin + "/metrics")
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				s.t.Fatalf("%s: %q", series, line)
			}
			return f
		}
	}
	return 0
}

// eventually polls cond every 50ms until it holds, or fails t after d.
func eventually(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// holds checks that cond stays true for d (polling every 50ms).
func holds(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("no longer true: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func faultFired(t *testing.T, admin, op string) int {
	t.Helper()
	var v struct{ Fired int }
	if err := getJSON("http://"+admin+"/fault/rule/"+op, &v); err != nil {
		t.Fatal(err)
	}
	return v.Fired
}

// LogicFired returns how often Logic's rule for op has matched.
func (s *Stack) LogicFired(op string) int { return faultFired(s.t, s.Logic.Admin, op) }

// Fired returns how often gateway i's rule for op has matched.
func (s *Stack) Fired(i int, op string) int { return faultFired(s.t, s.GW[i].Admin, op) }

// gwMetric reads one of gateway i's series.
func (s *Stack) gwMetric(i int, series string) float64 { return s.Metric(s.GW[i].Admin, series) }

// logicMetric reads one of Logic's series.
func (s *Stack) logicMetric(series string) float64 { return s.Metric(s.Logic.Admin, series) }
```

Create `internal/racetest/client_test.go`:

```go
package racetest

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/clientconn"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// Client is one scripted connection (internal/clientconn) to gateway GW.
type Client struct {
	*clientconn.Conn
	t   *testing.T
	GW  int
	seq uint32 // req_id
}

// Dial connects to gateway i and runs the Noise handshake.
func (s *Stack) Dial(i int) *Client {
	s.t.Helper()
	c, err := clientconn.Dial(s.GW[i].Listen, 5*time.Second)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, s.noise.Public, time.Now().Add(10*time.Second)); err != nil {
		s.t.Fatalf("handshake with %s: %v", s.GW[i].ID, err)
	}
	return &Client{Conn: c, t: s.t, GW: i}
}

// TokenAt signs a token for uid issued at iatMs and valid for ttl.
func (s *Stack) TokenAt(uid string, iatMs int64, ttl time.Duration) string {
	return s.TokenWith(s.jwtKey, uid, "mobile", iatMs, ttl)
}

// TokenWith signs a token for uid's platform with key.
func (s *Stack) TokenWith(key auth.PrivateKey, uid, platform string, iatMs int64, ttl time.Duration) string {
	s.t.Helper()
	tok, err := auth.Sign(key, auth.Claims{UID: uid, AppID: "im", Platform: platform,
		IatMs: iatMs, ExpMs: iatMs + ttl.Milliseconds()})
	if err != nil {
		s.t.Fatal(err)
	}
	return tok
}

// Token signs a token for uid issued now, valid for an hour.
func (s *Stack) Token(uid string) string { return s.TokenAt(uid, time.Now().UnixMilli(), time.Hour) }

// Req is a LoginReq for uid's im:mobile slot from device with conn_seq seq.
func (s *Stack) Req(uid, device string, seq uint64) *clientpb.LoginReq {
	return &clientpb.LoginReq{Token: s.Token(uid), Appid: "im", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: device, ConnSeq: seq, ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// Send sends one frame.
func (c *Client) Send(cmd clientpb.Cmd, msg proto.Message) {
	c.t.Helper()
	c.seq++
	if err := c.Conn.Send(cmd, c.seq, msg); err != nil {
		c.t.Fatalf("send %v: %v", cmd, err)
	}
}

// Next reads the next frame within d.
func (c *Client) Next(d time.Duration) clientconn.Frame {
	c.t.Helper()
	f, err := c.Recv(time.Now().Add(d))
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return f
}

// Login sends req and returns the gateway's answer (LoginResp, Error, Busy
// or Kick) within 10s.
func (c *Client) Login(req *clientpb.LoginReq) clientconn.Frame {
	c.t.Helper()
	c.Send(clientpb.Cmd_CMD_LOGIN, req)
	return c.Next(10 * time.Second)
}

// Online dials gateway i and logs uid in from device with conn_seq seq; it
// fails t unless the answer is LoginResp.
func (s *Stack) Online(i int, uid, device string, seq uint64) *Client {
	s.t.Helper()
	return s.OnlineWith(i, s.Req(uid, device, seq))
}

// OnlineWith dials gateway i and logs in with req; it fails t unless the
// answer is LoginResp.
func (s *Stack) OnlineWith(i int, req *clientpb.LoginReq) *Client {
	s.t.Helper()
	c := s.Dial(i)
	if f := c.Login(req); f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		s.t.Fatalf("%s on %s: got %v %s, want LoginResp", tokenUID(req), s.GW[i].ID, f.Cmd, describe(f))
	}
	return c
}

// tokenUID reads the uid from a LoginReq's token, for messages.
func tokenUID(req *clientpb.LoginReq) string {
	parts := strings.Split(req.GetToken(), ".")
	if len(parts) != 3 {
		return "?"
	}
	b, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct{ Sub string }
	json.Unmarshal(b, &c)
	return c.Sub
}

// ExpectKick waits up to d for a Kick with reason, then the close.
func (c *Client) ExpectKick(reason clientpb.KickReason, d time.Duration) {
	c.t.Helper()
	f := c.Next(d)
	var k clientpb.Kick
	if f.Cmd != clientpb.Cmd_CMD_KICK || f.Decode(&k) != nil || k.Reason != reason {
		c.t.Fatalf("got %v %s, want Kick %v", f.Cmd, describe(f), reason)
	}
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err != nil {
		c.t.Fatal(err)
	}
}

// Quiet checks that nothing arrives and the connection stays open for d.
func (c *Client) Quiet(d time.Duration) {
	c.t.Helper()
	if f, err := c.Recv(time.Now().Add(d)); err == nil {
		c.t.Fatalf("got %v %s, want nothing", f.Cmd, describe(f))
	} else if clientconn.IsClosed(err) {
		c.t.Fatal("connection closed, want it open")
	}
}

// describe decodes a frame's body for failure messages.
func describe(f clientconn.Frame) string {
	var m proto.Message
	switch f.Cmd {
	case clientpb.Cmd_CMD_ERROR:
		m = &clientpb.Error{}
	case clientpb.Cmd_CMD_KICK:
		m = &clientpb.Kick{}
	case clientpb.Cmd_CMD_BUSY:
		m = &clientpb.Busy{}
	case clientpb.Cmd_CMD_RECONNECT:
		m = &clientpb.Reconnect{}
	case clientpb.Cmd_CMD_LOGIN_RESP:
		m = &clientpb.LoginResp{}
	default:
		return ""
	}
	if f.Decode(m) != nil {
		return "(undecodable)"
	}
	return "{" + prototext.MarshalOptions{}.Format(m) + "}"
}

// errorCode returns the code of an Error frame, or fails t.
func errorCode(t *testing.T, f clientconn.Frame) clientpb.ErrorCode {
	t.Helper()
	var e clientpb.Error
	if f.Cmd != clientpb.Cmd_CMD_ERROR || f.Decode(&e) != nil {
		t.Fatalf("got %v %s, want Error", f.Cmd, describe(f))
	}
	return e.Code
}
```

Create `internal/racetest/sdk_test.go`:

```go
package racetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
)

// sdkProfile is the SDK's side of the test timing profile (Plan 5a,
// Execution Notes → For Plan 5b): the built-in windows and the backoff
// scaled ÷10 like the servers, and the heartbeat range lowered to the
// gateways' 3s–30s. The client timeouts (token, connect, Login, Pong) are
// not scaled: like login_deadline, they are tied to network latency.
func sdkProfile(cfg *sdk.Config) {
	cfg.Windows = sdk.Windows{
		Storm:        sdk.Window{Lo: 0, Hi: 2 * time.Second},
		Local:        sdk.Window{Lo: 0, Hi: 100 * time.Millisecond},
		PreHandshake: sdk.Window{Lo: 50 * time.Millisecond, Hi: 150 * time.Millisecond},
		Retryable:    sdk.Window{Lo: 100 * time.Millisecond, Hi: 300 * time.Millisecond},
	}
	cfg.Backoff = sdk.Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second}
	cfg.Heartbeat = sdk.HeartbeatRange{Min: 3 * time.Second, Max: 30 * time.Second}
}

// events records an SDK client's events.
type events struct {
	mu  sync.Mutex
	evs []sdk.Event
	got int
}

func (r *events) add(e sdk.Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// next waits up to d for an event of kind, skipping others, and returns it.
func (r *events) next(t *testing.T, kind sdk.EventKind, d time.Duration) sdk.Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for r.got < len(r.evs) {
			e := r.evs[r.got]
			r.got++
			if e.Kind == kind {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %v event within %v", kind, d)
	return sdk.Event{}
}

// none fails t if an event of kind arrives within d.
func (r *events) none(t *testing.T, kind sdk.EventKind, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.evs[r.got:] {
		if e.Kind == kind {
			t.Fatalf("unexpected %v event: %+v", kind, e)
		}
	}
	r.got = len(r.evs)
}

// SDK returns an idle SDK client for uid that picks one of the given
// gateways at random per attempt (all of them if none is given).
func (s *Stack) SDK(uid string, store sdk.Store, gws ...int) (*sdk.Client, *events) {
	s.t.Helper()
	if len(gws) == 0 {
		for i := range s.GW {
			gws = append(gws, i)
		}
	}
	r := &events{}
	cfg := sdk.DefaultConfig()
	sdkProfile(&cfg)
	for _, i := range gws {
		cfg.Addrs = append(cfg.Addrs, s.GW[i].Listen)
	}
	cfg.KeyID, cfg.ServerKey = 1, s.noise.Public
	cfg.AppID, cfg.Platform = "im", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return s.Token(uid), nil }
	cfg.Store, cfg.OnEvent = store, r.add
	c, err := sdk.New(cfg)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(c.Close)
	return c, r
}
```

Create `internal/racetest/metrics_test.go`:

```go
package racetest

import (
	"fmt"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
)

// The series the scenarios read, as the exposition prints them; %s are
// label values.
const (
	logicLogins       = `goim_logic_logins_total{result="%s"}`
	logicLogouts      = `goim_logic_logouts_total{result="%s"}`
	logicSweeps       = `goim_logic_sweeps_total{result="%s"}`
	logicGuardHolds   = `goim_logic_sweep_guard_holds_total`
	logicRenewing     = `goim_logic_sweep_guard_renewing_seconds`
	logicKickAttempts = `goim_logic_kick_attempts_total{attempt="%s",result="%s"}`
	logicKickGiveUps  = `goim_logic_kick_giveups_total{target_lease="live"}`

	gwPasses     = `goim_gateway_reconcile_passes_total{pace="%s",result="%s"}`
	gwEnds       = `goim_gateway_reconcile_end_total{result="%s"}`
	gwRequeued   = `goim_gateway_reconcile_requeued_total`
	gwKicks      = `goim_gateway_kicks_total{result="%s"}`
	gwLogouts    = `goim_gateway_logouts_total{result="%s"}`
	gwLeaseCalls = `goim_gateway_lease_calls_total{result="%s"}`
	triggered    = "PACE_TRIGGERED"
	periodic     = "PACE_PERIODIC"
)

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }

// dur writes a fault rule's duration.
func dur(d time.Duration) fault.Duration { return fault.Duration(d) }

// passDone waits until gateway i has finished n triggered passes.
func (s *Stack) passDone(i, n int, d time.Duration) {
	s.t.Helper()
	eventually(s.t, sprintf("%s finished %d triggered passes", s.GW[i].ID, n), d, func() bool {
		return s.gwMetric(i, sprintf(gwPasses, triggered, "done")) >= float64(n)
	})
}

// endsOK waits until gateway i's ReconcileEnds have succeeded n times
// (each returns after Logic's delete phase).
func (s *Stack) endsOK(i, n int, d time.Duration) {
	s.t.Helper()
	eventually(s.t, sprintf("%s: %d ReconcileEnds ok", s.GW[i].ID, n), d, func() bool {
		return s.gwMetric(i, sprintf(gwEnds, "ok")) >= float64(n)
	})
}
```

In `Makefile`, replace:

```make
.PHONY: test redis-server test-redis-local proto redis-up redis-down test-redis test-tier2

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# The race suite's Redis: redis-server on PATH, else the pinned 7.4.11 that
# make redis-server builds into .cache/redis (cc and make, no Docker).
REDIS_SERVER ?= $(or $(shell command -v redis-server 2>/dev/null),$(CURDIR)/.cache/redis/bin/redis-server)
redis-server:
	scripts/redis-build.sh .cache/redis

# Tier 1a on real Redis, on the dev machine: every test, including the ones
# that skip as "TODO: Tier 2 integration", against a local redis-server
# primary and replica (no Docker). The tests make test-redis runs on the Tier 2 host.
test-redis-local:
	scripts/redis-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
```

with:

```make
.PHONY: test test-race-suite redis-server test-redis-local proto redis-up redis-down test-redis test-tier2

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# Tier 1b (dev machine, before every commit): the race integration suite. It
# builds the binaries with -race and runs them as local processes, with a
# redis-server primary and replica per scenario (Plan 5b). GOIM_RACE_LOGS=dir
# keeps every scenario's logs.
test-race-suite:
	GOIM_RACE_SUITE=1 GOIM_REDIS_SERVER=$(REDIS_SERVER) go test -count=1 -timeout 30m ./internal/racetest/

# The race suite's Redis: redis-server on PATH, else the pinned 7.4.11 that
# make redis-server builds into .cache/redis (cc and make, no Docker).
REDIS_SERVER ?= $(or $(shell command -v redis-server 2>/dev/null),$(CURDIR)/.cache/redis/bin/redis-server)
redis-server:
	scripts/redis-build.sh .cache/redis

# Tier 1a on real Redis, on the dev machine: every test, including the ones
# that skip as "TODO: Tier 2 integration", against a local redis-server
# primary and replica (no Docker). The tests make test-redis runs on the Tier 2 host.
test-redis-local:
	scripts/redis-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
```

- [ ] **Step 4: Run the harness tests**

Run: `go vet ./... && go test -count=1 ./internal/racetest/`
Expected: `ok`. Without `GOIM_RACE_SUITE` every scenario skips; `TestProcessesDieWithTheSuite` runs, since it needs no binaries.

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestStackComesUp|TestHarnessFailsWhatAssertionsMiss|TestRedisFaultsDoWhatTheyClaim|TestProfileBreakingARatioIsRefused|TestProcessesDieWithTheSuite' ./internal/racetest/`
Expected: the line `racetest: <path>: Redis 7.4.11`, then five `PASS` lines. The first run builds the binaries with `-race`, about a minute on the dev machine. With no `redis-server` on `PATH`, set `GOIM_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server` (`make test-race-suite` does that).

- [ ] **Step 5: Commit**

```bash
git add internal/racetest Makefile
git commit -m "test(racetest): process harness, timing profile and self-checks (choices #59-#62)"
```

---

### Task 6: Login and Logout races (races 1, 2, 3, 5, 6, 10)

**Files:**
- Test: `internal/racetest/login_test.go`

**Interfaces:**
- Consumes: the harness (Task 5); `logic.FaultLogin`, `logic.FaultLogout` (Task 2); `sdk.MemStore`, `sdk.State`, `sdk.EventOnline`, `EventIdle`, `EventConnecting`, `EventWaiting` (Plan 5a).
- Produces: nothing for later tasks.

These scenarios pin behaviour Plans 1–5a built, so they pass at once. Step 3 breaks fence (a) by hand to see one of them fail. Timing at the profile: a Login held 2s sits well inside `login_deadline` (5s); 5.5s is past it but inside the gateway's 6s Login timeout; 7s and 8s are past both.

- [ ] **Step 1: Write the scenarios**

Create `internal/racetest/login_test.go`:

```go
package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/sdk"
)

// Race 1, delayed disconnect: the old connection's Logout reaches Logic
// after the device logged in again on another gateway. The compare-and-delete
// misses, and the new session stays.
func TestRace1DelayedDisconnect(t *testing.T) {
	s := New(t, Options{})
	const uid = "r1"
	old := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	old.Close() // gw-0 sends the Logout; Logic holds it for 2s
	time.Sleep(500 * time.Millisecond)
	s.Online(1, uid, "d1", 2)
	eventually(t, "the delayed Logout ran and missed", 5*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogouts, "not_owner")) == 1
	})
	if e, ok := s.Redis.Entry(uid); !ok || e.Gateway != "gw-1" || e.ConnSeq != 2 {
		t.Fatalf("entry %+v %v, want the new session on gw-1", e, ok)
	}
}

// Race 2, delayed login from an abandoned connection of the same device:
// S1's Login reaches Redis after S2's, inside the deadline, and is STALE
// with S2's counter; S2 is not kicked.
func TestRace2DelayedLoginSameDeviceIsStale(t *testing.T) {
	s := New(t, Options{})
	const uid = "r2"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	s1 := s.Dial(0)
	s1.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond) // S1's Login is held in Logic
	s2 := s.Online(1, uid, "d1", 2)
	f := s1.Next(5 * time.Second)
	var e clientpb.Error
	if f.Cmd != clientpb.Cmd_CMD_ERROR || f.Decode(&e) != nil || e.Code != clientpb.ErrorCode_ERROR_CODE_STALE || e.StoredConnSeq != 2 {
		t.Fatalf("S1 got %v %s, want STALE with stored_conn_seq 2", f.Cmd, describe(f))
	}
	s2.Quiet(time.Second)
	if en, _ := s.Redis.Entry(uid); en.Gateway != "gw-1" || en.ConnSeq != 2 {
		t.Fatalf("entry %+v, want S2", en)
	}
}

// Race 2, a login later than the deadline is EXPIRED and writes nothing.
func TestRace2LoginPastTheDeadlineIsExpired(t *testing.T) {
	s := New(t, Options{})
	const uid = "r2x"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(5500 * time.Millisecond), Count: 1})
	c := s.Dial(0)
	if code := errorCode(t, c.Login(s.Req(uid, "d1", 1))); code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("got %v, want EXPIRED", code)
	}
	if _, ok := s.Redis.Entry(uid); ok {
		t.Fatal("an EXPIRED login wrote an entry")
	}
}

// Race 3, delayed login from another device: its recv_ms is older than the
// stored login_ms, so it is SUPERSEDED and the newer device is not kicked.
func TestRace3DelayedLoginOtherDeviceIsSuperseded(t *testing.T) {
	s := New(t, Options{})
	const uid = "r3"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	d1 := s.Dial(0)
	d1.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond)
	d2 := s.Online(1, uid, "d2", 1)
	if code := errorCode(t, d1.Next(5*time.Second)); code != clientpb.ErrorCode_ERROR_CODE_SUPERSEDED {
		t.Fatalf("d1 got %v, want SUPERSEDED", code)
	}
	d2.Quiet(time.Second)
	if en, _ := s.Redis.Entry(uid); en.DeviceID != "d2" {
		t.Fatalf("entry %+v, want d2", en)
	}
}

// Race 5, the connection closes while its Login is in Logic: fence (a)
// holds the Logout until the Login reply, so the Logout lands after the
// write and no ghost remains.
func TestRace5CloseWhileLoginInFlight(t *testing.T) {
	s := New(t, Options{})
	const uid = "r5"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	c := s.Dial(0)
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond)
	c.Close()
	eventually(t, "Logic wrote the login", 5*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogins, "ok")) == 1
	})
	eventually(t, "the fenced Logout deleted it", 3*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
	holds(t, "no ghost", time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
}

// Race 6, Login before Logout: Redis is written but the reply comes after
// the gateway's Login timeout. The client gets Busy, and fence (b) sends the
// Logout at recv_ms + login_deadline + 1s, after the write: no ghost.
func TestRace6WrittenWithoutReplyLoginBeforeLogout(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6a"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(8 * time.Second), Count: 1})
	c := s.Dial(0)
	if f := c.Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy (no definitive reply)", f.Cmd, describe(f))
	}
	eventually(t, "the fenced Logout deleted the written entry", 3*time.Second, func() bool {
		_, ok := s.Redis.Entry(uid)
		return !ok && s.Metric(s.Logic.Admin, sprintf(logicLogouts, "deleted")) == 1
	})
}

// Race 6, Logout before Login: the Login reaches Redis only after the fence
// released the Logout. The login Lua's deadline refuses it (EXPIRED), so the
// early Logout leaves nothing behind.
func TestRace6WrittenWithoutReplyLogoutBeforeLogin(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6b"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(7 * time.Second), Count: 1})
	c := s.Dial(0)
	if f := c.Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	eventually(t, "the Logout ran first and found nothing", 3*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogouts, "not_owner")) == 1
	})
	eventually(t, "the late Login was refused", 3*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogins, "expired")) == 1
	})
	if _, ok := s.Redis.Entry(uid); ok {
		t.Fatal("ghost: the late Login wrote after the Logout")
	}
}

// Race 6, Logic crashes with the Login written but not answered: the client
// gets Busy, and the Logout still waits for fence (b) although Logic is back
// within a second.
func TestRace6LogicCrashLogoutWaitsForTheFence(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6c"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(time.Minute), Count: 1})
	c := s.Dial(0)
	sent := time.Now()
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "Logic wrote the login", 3*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return ok })
	s.RestartLogic()
	if f := c.Next(10 * time.Second); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	// The fence releases at recv_ms + 6s; Logic has been back since well before.
	holds(t, "the Logout waits for the fence", time.Until(sent.Add(5*time.Second)), func() bool {
		_, ok := s.Redis.Entry(uid)
		return ok
	})
	eventually(t, "the fenced Logout deleted it", 5*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
}

// Race 10, two client instances share a device_id, on different gateways:
// they replace each other once, the replaced one goes idle (REPLACED does
// not reconnect), and the loop stops.
func TestRace10SharedDeviceIDReplacesOnce(t *testing.T) {
	s := New(t, Options{})
	const uid = "r10"
	st1 := &sdk.MemStore{}
	c1, r1 := s.SDK(uid, st1, 0)
	c1.Connect()
	r1.next(t, sdk.EventOnline, 10*time.Second)
	shared, _ := st1.Load()
	st2 := &sdk.MemStore{}
	st2.Save(sdk.State{DeviceID: shared.DeviceID, ConnSeq: 100})
	c2, r2 := s.SDK(uid, st2, 1)
	c2.Connect()
	r2.next(t, sdk.EventOnline, 10*time.Second)
	if e := r1.next(t, sdk.EventIdle, 10*time.Second); e.Kick != clientpb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("first instance idle with %v, want REPLACED", e.Kick)
	}
	r1.none(t, sdk.EventConnecting, 3*time.Second)
	r2.none(t, sdk.EventWaiting, 0)
	if en, _ := s.Redis.Entry(uid); en.Gateway != "gw-1" || en.ConnSeq != 101 {
		t.Fatalf("entry %+v, want the second instance on gw-1", en)
	}
}
```

- [ ] **Step 2: Run them**

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestRace(1|2|3|5|6|10)' ./internal/racetest/`
Expected: 9 `PASS` lines, about 50s.

- [ ] **Step 3: See one fail**

In `internal/gateway/gateway.go`, in `settle`, change `if c.outcome == loginOK || c.fenceB {` to `if c.outcome == loginOK || c.outcome == loginPending || c.fenceB {` (no fence (a)). Run `GOIM_RACE_SUITE=1 go test -count=1 -run TestRace5 ./internal/racetest/`.
Expected: FAIL, `the fenced Logout deleted it`. Revert the change (`git checkout internal/gateway/gateway.go`), and run it again to see it pass.

- [ ] **Step 4: Commit**

```bash
git add internal/racetest/login_test.go
git commit -m "test(racetest): races 1, 2, 3, 5, 6 (both orders, Logic crash) and 10"
```

---

### Task 7: Reconcile scenarios (races 7, 14, 16, 18; ReconcileEnd; refusal)

**Files:**
- Test: `internal/racetest/reconcile_test.go`

**Interfaces:**
- Consumes: the harness (Task 5); `logic.FaultReconcile`, `FaultReconcileEnd`, `FaultLogin`, `FaultLogout` (Task 2); `gateway.FaultKick` (Task 3); `session.Entry`.
- Produces: nothing for later tasks.

A chunk held 5.4s in Logic comes back `refused`: the compare-and-set runs after `sent_ms + login_deadline` (5s), and the reply still arrives before the gateway resolves the chunk at its deadline (6s). That 0.6s is the suite's tightest margin on two cores (*Execution Notes*, risk 4). The dropped-Logout-in-the-same-pass scenario keeps its pass running with an entry that is refused three times (3 × 5.4s). Its Logout is dropped 6s after the first chunk resolves, so the pass ends after the drop.

- [ ] **Step 1: Write the scenarios**

Create `internal/racetest/reconcile_test.go`:

```go
package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
)

// Race 14: the connection closes after its chunk is sent and before Logic
// processes it. Fence (c) holds the Logout until the chunk is resolved, so
// the chunk's refresh (or restore) lands first and the Logout deletes it.
// The check is that the Logout waits: a Logout that went first would let
// the chunk restore a ghost, which this pass's own delete phase would then
// remove before any later check could see it.
func TestRace14CloseDuringAChunk(t *testing.T) {
	s := New(t, Options{})
	const uid = "r14"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(3 * time.Second), Count: 1})
	s.Reconcile(0)
	eventually(t, "the chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	c.Close()
	holds(t, "fence (c) holds the Logout while the chunk is in Logic", 2*time.Second, func() bool { return s.Redis.has(uid) })
	s.endsOK(0, 1, 10*time.Second)
	eventually(t, "the fenced Logout deleted the entry", 5*time.Second, func() bool { return !s.Redis.has(uid) })
	holds(t, "no ghost", time.Second, func() bool { return !s.Redis.has(uid) })
}

// Race 18: a client Logout while its chunk is held in Logic gets LogoutResp
// at once; the Logout RPC waits for fence (c), so no ghost remains.
func TestRace18ClientLogoutDuringAChunk(t *testing.T) {
	s := New(t, Options{})
	const uid = "r18"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(3 * time.Second), Count: 1})
	s.Reconcile(0)
	eventually(t, "the chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	start := time.Now()
	c.Send(clientpb.Cmd_CMD_LOGOUT, nil)
	if f := c.Next(time.Second); f.Cmd != clientpb.Cmd_CMD_LOGOUT_RESP {
		t.Fatalf("got %v, want LogoutResp", f.Cmd)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("LogoutResp after %v: it must not wait for the chunk", d)
	}
	holds(t, "fence (c) holds the Logout RPC while the chunk is in Logic", 1500*time.Millisecond, func() bool { return s.Redis.has(uid) })
	s.endsOK(0, 1, 10*time.Second)
	eventually(t, "the Logout deleted the entry after the chunk", 5*time.Second, func() bool { return !s.Redis.has(uid) })
	holds(t, "no ghost", time.Second, func() bool { return !s.Redis.has(uid) })
}

// Race 16 / ReconcileEnd: a Login in flight when the pass starts is tracked,
// so the pass's delete phase leaves its field alone even though the field
// was written before the phase ran.
func TestReconcileEndKeepsALoginInFlight(t *testing.T) {
	s := New(t, Options{})
	const uid = "r16a"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID, Delay: dur(3 * time.Second), Count: 1})
	c := s.Dial(0)
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "the Login is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultLogin) == 1 })
	s.Reconcile(0) // no ONLINE connection: the pass ends at once with the Login's conn_id tracked
	if f := c.Next(5 * time.Second); f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("got %v %s, want LoginResp", f.Cmd, describe(f))
	}
	s.endsOK(0, 1, 10*time.Second) // the delete phase ran after the write
	if !s.Redis.has(uid) {
		t.Fatal("the delete phase removed a Login that was in flight when its pass started")
	}
}

// Race 16 / ReconcileEnd: a Logout dropped after the connection's chunk was
// sent is repaired at the end of the same pass. The pass is kept running by
// another entry whose writes are refused and re-queued (R3-O6).
func TestReconcileEndRepairsALogoutDroppedDuringThePass(t *testing.T) {
	s := New(t, Options{})
	const dropped, slow = "r16b", "r16c"
	a := s.Online(0, dropped, "d1", 1)
	s.Online(0, slow, "d2", 1)
	s.Redis.Del(slow) // slow needs a rule-4 restore, which the deadline can refuse
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: dropped, Fail: true})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: slow, Delay: dur(5400 * time.Millisecond), Count: 3})
	s.Reconcile(0)
	eventually(t, "the first chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	a.Close() // its Logout waits for the chunk, then fails until the 6s budget drops it
	eventually(t, "the Logout was dropped", 15*time.Second, func() bool { return s.gwMetric(0, sprintf(gwLogouts, "dropped")) == 1 })
	if s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 0 {
		t.Fatal("the pass ended before the Logout was dropped; the scenario needs a longer pass")
	}
	if !s.Redis.has(dropped) {
		t.Fatal("no ghost to repair")
	}
	s.endsOK(0, 1, 15*time.Second)
	if s.Redis.has(dropped) {
		t.Fatal("the pass's delete phase left the dropped Logout's ghost")
	}
	if !s.Redis.has(slow) || s.gwMetric(0, gwRequeued) != 3 {
		t.Fatalf("slow restored %v after %v re-queues, want restored after 3", s.Redis.has(slow), s.gwMetric(0, gwRequeued))
	}
}

// ReconcileEnd: a periodic pass cancelled by a trigger sends no
// ReconcileEnd, so it deletes nothing (R3-O1). From the moment its chunk is
// held in Logic, Logic counts ReconcileEnds: only the triggered pass that
// follows may send one. The cycle is 10s, so the next periodic pass comes
// well after the check; one gateway, so the count is its own.
func TestCancelledPassSendsNoReconcileEnd(t *testing.T) {
	s := New(t, Options{Gateways: 1, PeriodicPasses: true, Gateway: []string{"-reconcile-cycle", "10s", "-reconcile-jitter", "0s"}})
	const uid = "r16d"
	s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	eventually(t, "a periodic pass's chunk is held in Logic", 12*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID}) // counts, changes nothing
	s.Reconcile(0)                                                         // cancels the periodic pass
	eventually(t, "the periodic pass was cancelled", 5*time.Second, func() bool {
		return s.gwMetric(0, sprintf(gwPasses, periodic, "cancelled")) == 1
	})
	s.passDone(0, 1, 5*time.Second)
	s.endsOK(0, 1, 5*time.Second)
	time.Sleep(500 * time.Millisecond)
	if n := s.LogicFired(logic.FaultReconcileEnd); n != 1 {
		t.Fatalf("Logic got %d ReconcileEnds since the periodic pass started, want 1 (the triggered pass)", n)
	}
}

// ReconcileEnd: one that fails is retried, and its deletes apply.
func TestFailedReconcileEndIsRetried(t *testing.T) {
	s := New(t, Options{})
	const ghost = "r16f"
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID, Fail: true, Count: 1})
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if s.gwMetric(0, sprintf(gwEnds, "retried")) != 1 {
		t.Fatalf("retried %v, want 1", s.gwMetric(0, sprintf(gwEnds, "retried")))
	}
	if s.Redis.has(ghost) {
		t.Fatal("the retried ReconcileEnd's delete did not apply")
	}
}

// Reconcile refusal (R3-O6): one chunk held past login_deadline comes back
// refused; its entry is re-queued and restored within the same pass.
func TestRefusedChunkIsRequeuedAndRestoredInThePass(t *testing.T) {
	s := New(t, Options{})
	const uid = "rr1"
	s.Online(0, uid, "d1", 1)
	s.Redis.Del(uid)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(5400 * time.Millisecond), Count: 1})
	s.Reconcile(0)
	s.passDone(0, 1, 15*time.Second)
	if !s.Redis.has(uid) || s.gwMetric(0, gwRequeued) != 1 || s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 1 {
		t.Fatalf("restored %v, re-queued %v, passes %v; want restored by the one pass after one re-queue",
			s.Redis.has(uid), s.gwMetric(0, gwRequeued), s.gwMetric(0, sprintf(gwPasses, triggered, "done")))
	}
}

// Reconcile refusal: a refused rule-3 write sends no Kick. The stored entry
// is an older session of the same device on live gw-1 (a lost write put it
// back); only the write that lands, after the re-queue, kicks it.
func TestRefusedRule3WriteSendsNoKick(t *testing.T) {
	s := New(t, Options{})
	const uid = "rr2"
	s.Online(0, uid, "d1", 2)
	now := s.Redis.Now()
	s.Redis.Put(uid, session.Entry{Gateway: s.GW[1].ID, Epoch: s.GW[1].Epoch, ConnID: 1<<48 - 1, DeviceID: "d1",
		ConnSeq: 1, LoginMs: now - 60_000, TokIatMs: now - 61_000})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(5400 * time.Millisecond), Count: 1})
	s.Reconcile(0)
	s.passDone(0, 1, 15*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != s.GW[0].ID || s.gwMetric(0, gwRequeued) != 1 {
		t.Fatalf("entry %+v after %v re-queues, want gw-0's session written back after one", e, s.gwMetric(0, gwRequeued))
	}
	eventually(t, "the written rule-3 entry's Kick reached gw-1", 5*time.Second, func() bool {
		return s.gwMetric(1, sprintf(gwKicks, "ignored")) >= 1
	})
	time.Sleep(500 * time.Millisecond)
	if n := s.gwMetric(1, sprintf(gwKicks, "ignored")); n != 1 {
		t.Fatalf("gw-1 got %v Kicks, want 1: the refused write must not kick", n)
	}
}

// Reconcile: a missing entry is restored (rule 4), and a ghost of this
// epoch is deleted by the delete phase.
func TestReconcileRestoresMissingAndDeletesGhosts(t *testing.T) {
	s := New(t, Options{})
	const live, ghost = "rc1", "rc2"
	s.Online(0, live, "d1", 1)
	s.Redis.Del(live)
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if !s.Redis.has(live) || s.Redis.has(ghost) {
		t.Fatalf("live restored %v, ghost left %v", s.Redis.has(live), s.Redis.has(ghost))
	}
}

// Reconcile: a ghost is removed within one cycle by a periodic pass, with
// no trigger. The cycle here is 6s (the profile's 60s would cost a minute).
func TestPeriodicPassRemovesAGhostWithinOneCycle(t *testing.T) {
	s := New(t, Options{PeriodicPasses: true, Gateway: []string{"-reconcile-cycle", "6s", "-reconcile-jitter", "600ms"}})
	const ghost = "rc3"
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	eventually(t, "a periodic pass removed the ghost", 8*time.Second, func() bool { return !s.Redis.has(ghost) })
	if s.gwMetric(0, sprintf(gwPasses, periodic, "done")) < 1 {
		t.Fatal("no periodic pass ran")
	}
}

// Reconcile: a newer session on another gateway is never overridden. The
// older connection, whose Kick was dropped, is closed as not_owner.
func TestReconcileNeverOverridesANewerSession(t *testing.T) {
	s := New(t, Options{})
	const uid = "rc4"
	old := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d2", 1)
	eventually(t, "Logic gave up on the dropped Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	s.ClearFault(0, gateway.FaultKick)
	s.Reconcile(0)
	old.ExpectKick(clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, 10*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != "gw-1" || e.DeviceID != "d2" {
		t.Fatalf("entry %+v, want d2 on gw-1", e)
	}
}

// Reconcile: a Logout dropped while the gateway is alive leaves a ghost
// that the next pass deletes (race 7).
func TestDroppedLogoutIsRepairedByTheNextPass(t *testing.T) {
	s := New(t, Options{})
	const uid = "rc5"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: uid, Fail: true})
	c.Close()
	eventually(t, "the Logout was dropped", 15*time.Second, func() bool { return s.gwMetric(0, sprintf(gwLogouts, "dropped")) == 1 })
	if !s.Redis.has(uid) {
		t.Fatal("no ghost")
	}
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the next pass left the ghost")
	}
}
```

- [ ] **Step 2: Run them**

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestRace1[48]|TestReconcile|TestCancelled|TestFailedReconcileEnd|TestRefused|TestPeriodic|TestDropped' ./internal/racetest/`
Expected: 12 `PASS` lines, about 65s.

- [ ] **Step 3: See one fail**

In `internal/gateway/gateway.go`, in `settle`, change `if c.inChunk {` to `if false && c.inChunk {` (no fence (c)). Run `GOIM_RACE_SUITE=1 go test -count=1 -run 'TestRace1[48]' ./internal/racetest/`.
Expected: both FAIL, `fence (c) holds the Logout … while the chunk is in Logic`. Revert.

- [ ] **Step 4: Commit**

```bash
git add internal/racetest/reconcile_test.go
git commit -m "test(racetest): Reconcile races 14, 16, 18, ReconcileEnd, refusal and repairs"
```

---

### Task 8: Sweeper and lease scenarios (race 12, R2-O3, R3-O2, R3-O3, R4M-2, R4M-3)

**Files:**
- Test: `internal/racetest/sweep_test.go`

**Interfaces:**
- Consumes: the harness (Task 5); `gateway.FaultLogic`, `gateway.FaultKick` (Task 3); `logic.FaultGWEpochs` (Task 2); `session.Entry`.
- Produces: nothing for later tasks.

The bound for a dead gateway's entries at the profile: 2 × lease TTL (6s) before its score is stale, up to one TTL (3s) of guard wait, and the sweep (`sweepBound` = 11s with a second of slack). "Forced sweep" makes a live epoch look dead: its renewals are blocked, its lease key deleted, its score aged by 10s. The two abort scenarios slow the sweeper to 100 uids/s in batches of 10 and give it 300 uids, about 3s of work.

- [ ] **Step 1: Write the scenarios**

Create `internal/racetest/sweep_test.go`:

```go
package racetest

import (
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
)

// sweepBound is how long after a gateway's last renewal its entries must be
// gone at the profile: 2 × lease TTL (6s), up to one TTL of guard wait (3s),
// and the sweep itself, with a second of slack for -race on two cores.
const sweepBound = 11 * time.Second

// swept reports whether gw's epoch is gone from gwepochs (FinishSweep).
func (s *Stack) swept(g *Gateway) bool { _, ok := s.Redis.Score(g.ID, g.Epoch); return !ok }

// onlineOnEach logs one client in on every gateway and returns their uids.
func (s *Stack) onlineOnEach(prefix string) []string {
	s.t.Helper()
	uids := make([]string, len(s.GW))
	for i := range s.GW {
		uids[i] = prefix + strconv.Itoa(i)
		s.Online(i, uids[i], "d1", 1)
	}
	return uids
}

// noSweepFor checks that Logic sweeps nothing for d.
func (s *Stack) noSweepFor(d time.Duration) {
	s.t.Helper()
	holds(s.t, "nothing is swept", d, func() bool { return s.logicMetric(sprintf(logicSweeps, "done")) == 0 })
}

// Gateway crash: its lease expires, and the sweeper deletes its entries and
// its gwepochs member while the other gateways renew.
func TestGatewayCrashIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw1-")
	dead := s.GW[0]
	dead.kill()
	eventually(t, "the lease expired", 4*time.Second, func() bool { return !s.Redis.LeaseExists(dead.ID, dead.Epoch) })
	eventually(t, "the dead epoch was swept", sweepBound, func() bool { return s.swept(dead) })
	if s.Redis.has(uids[0]) || len(s.Redis.Conns(dead.ID, dead.Epoch)) != 0 {
		t.Fatal("the sweep left the dead gateway's entry or gwconns")
	}
	for _, uid := range uids[1:] {
		if !s.Redis.has(uid) {
			t.Fatalf("%s on a live gateway was deleted", uid)
		}
	}
}

// Two gateways killed at once: both epochs are swept while the others renew (R2-O3).
func TestTwoGatewaysKilledAtOnceAreBothSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw2-")
	s.GW[0].kill()
	s.GW[1].kill()
	eventually(t, "both epochs swept", sweepBound, func() bool { return s.swept(s.GW[0]) && s.swept(s.GW[1]) })
	if s.Redis.has(uids[0]) || s.Redis.has(uids[1]) || !s.Redis.has(uids[2]) || !s.Redis.has(uids[3]) {
		t.Fatal("wrong entries swept")
	}
}

// Three gateways killed while one keeps renewing at its 1s rhythm: the
// guard's renewing_since is never reset, and all three epochs are swept
// after the guard wait (R3-O7, R3-O2).
func TestThreeKilledWhileOneRenewsAreAllSwept(t *testing.T) {
	s := New(t, Options{})
	s.onlineOnEach("sw3-")
	for _, g := range s.GW[:3] {
		g.kill()
	}
	last := s.logicMetric(logicRenewing)
	eventually(t, "all three epochs swept", sweepBound, func() bool {
		if r := s.logicMetric(logicRenewing); r < last {
			t.Fatalf("renewing_since was reset: %vs after %vs", r, last)
		} else {
			last = r
		}
		return s.swept(s.GW[0]) && s.swept(s.GW[1]) && s.swept(s.GW[2])
	})
	if s.swept(s.GW[3]) {
		t.Fatal("the renewing gateway was swept")
	}
}

// Redis paused for 9s (the spec's 90s): nothing is swept, and the sessions
// are still there afterwards (each gateway's lease reply reports missing,
// and its triggered pass refreshes them).
func TestRedisPausedNothingIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw4-")
	s.Redis.PauseFor(9 * time.Second)
	time.Sleep(9 * time.Second)
	s.noSweepFor(8 * time.Second)
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone after the Redis pause", uid)
		}
	}
}

// Every gateway cut off from Logic for 9s, Logic↔Redis healthy: no epoch
// renews, so the guard holds the stale epochs back; when the gateways
// return, they renew before the guard releases (R3-O2). Meanwhile their
// sessions read as invalid (no lease key), and valid again after: the
// consistency contract's "healthy session reads invalid" row.
func TestGatewaysCutOffFromLogicNothingIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw5-")
	for i := range s.GW {
		s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Fail: true, For: dur(9 * time.Second)})
	}
	time.Sleep(5 * time.Second)
	for _, g := range s.GW {
		if s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s's lease outlived 5s without renewals", g.ID)
		}
	}
	time.Sleep(4 * time.Second)
	s.noSweepFor(6 * time.Second)
	for _, g := range s.GW {
		if !s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s's lease is not back", g.ID)
		}
	}
	if s.logicMetric(logicGuardHolds) == 0 {
		t.Fatal("the guard never held: the epochs never looked stale, so the scenario proved nothing")
	}
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone", uid)
		}
	}
}

// Logic restarts and renewals resume more than one lease TTL (3s) after its
// sweeper started: its fresh guard holds, and nothing is swept (R3-O2).
func TestLogicRestartWithLateRenewalsSweepsNothing(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw6-")
	for i := range s.GW {
		s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true, For: dur(9 * time.Second)})
	}
	s.RestartLogic()
	time.Sleep(9 * time.Second)
	s.noSweepFor(6 * time.Second)
	if s.logicMetric(logicGuardHolds) == 0 {
		t.Fatal("the guard never held")
	}
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone", uid)
		}
	}
}

// forceSweep makes gateway i's live epoch look dead to the sweeper: its
// renewals are blocked, its lease key deleted and its score aged.
func (s *Stack) forceSweep(i int) {
	g := s.GW[i]
	s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true})
	s.Redis.DelLease(g.ID, g.Epoch)
	s.Redis.SetScore(g.ID, g.Epoch, s.Redis.Now()-10_000)
}

// A forced sweep of a live gateway: its sessions are deleted, then restored
// by the pass its next renewal (missing) triggers, within ~6s (R2-O8).
func TestForcedSweepOfALiveGatewayIsRestored(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw7"
	s.Online(0, uid, "d1", 1)
	s.forceSweep(0)
	eventually(t, "the live epoch was swept", 5*time.Second, func() bool { return s.swept(s.GW[0]) && !s.Redis.has(uid) })
	s.ClearFault(0, gateway.FaultLogic)
	eventually(t, "the triggered pass restored the session", 6*time.Second, func() bool { return s.Redis.has(uid) })
	if s.gwMetric(0, sprintf(gwLeaseCalls, "missing")) < 1 {
		t.Fatal("the renewal after the sweep did not report missing")
	}
}

// fill writes n fake entries for epoch (gw, epoch), so a slow sweep of it
// takes a while.
func (s *Stack) fill(prefix, gw string, epoch uint64, n int) {
	now := s.Redis.Now()
	for i := 0; i < n; i++ {
		s.Redis.Put(prefix+strconv.Itoa(i), session.Entry{Gateway: gw, Epoch: epoch, ConnID: uint64(i + 1),
			DeviceID: "fake", ConnSeq: 1, LoginMs: now - 60_000, TokIatMs: now - 61_000})
	}
}

func (s *Stack) claimed(gw string, epoch uint64) bool {
	return s.Redis.Client().Exists(ctx, "sweep:"+epochSuffix(gw, epoch)).Val() == 1
}

// slowSweep is a sweeper that takes ~3s for 300 uids.
var slowSweep = []string{"-sweep-rate", "100", "-sweep-batch", "10"}

// Sweep abort (R3-O3): a live gateway renews while its epoch is being
// swept. The reply is missing, the sweeper aborts at its next batch without
// UNLINK or ZREM and deletes its claim (R3-OP6), and the triggered pass
// restores the session within ~6s.
func TestSweepAbortsWhenTheGatewayRenews(t *testing.T) {
	s := New(t, Options{Logic: slowSweep})
	const uid = "sw8"
	g := s.GW[0]
	s.Online(0, uid, "d1", 1)
	s.fill("sw8f", g.ID, g.Epoch, 300)
	s.forceSweep(0)
	eventually(t, "the sweep started", 5*time.Second, func() bool { return s.claimed(g.ID, g.Epoch) })
	s.ClearFault(0, gateway.FaultLogic)
	eventually(t, "the sweep aborted", 5*time.Second, func() bool { return s.logicMetric(sprintf(logicSweeps, "aborted")) == 1 })
	if s.claimed(g.ID, g.Epoch) || s.swept(g) || len(s.Redis.Conns(g.ID, g.Epoch)) == 0 {
		t.Fatalf("after the abort: claim %v, member gone %v, gwconns %d; want no claim, member and gwconns kept",
			s.claimed(g.ID, g.Epoch), s.swept(g), len(s.Redis.Conns(g.ID, g.Epoch)))
	}
	eventually(t, "the triggered pass restored the session", 6*time.Second, func() bool { return s.Redis.has(uid) })
	if s.logicMetric(sprintf(logicSweeps, "done")) != 0 {
		t.Fatal("a sweep finished")
	}
}

// The first lease call after boot finds a new epoch's key absent and
// triggers no pass (R3-OP5); a renewal after a plain expiry, with no sweep,
// is missing and triggers exactly one pass.
func TestFirstLeaseTriggersNoPassAndAPlainExpiryTriggersOne(t *testing.T) {
	s := New(t, Options{})
	time.Sleep(2 * time.Second)
	if s.gwMetric(0, sprintf(gwLeaseCalls, "first")) != 1 || s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 0 {
		t.Fatalf("first %v, passes %v; want 1 first call and no pass",
			s.gwMetric(0, sprintf(gwLeaseCalls, "first")), s.gwMetric(0, sprintf(gwPasses, triggered, "done")))
	}
	s.Fault(0, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true, For: dur(4 * time.Second)})
	eventually(t, "the lease expired", 5*time.Second, func() bool { return !s.Redis.LeaseExists(s.GW[0].ID, s.GW[0].Epoch) })
	s.passDone(0, 1, 6*time.Second)
	time.Sleep(2 * time.Second)
	if m, p := s.gwMetric(0, sprintf(gwLeaseCalls, "missing")), s.gwMetric(0, sprintf(gwPasses, triggered, "done")); m != 1 || p != 1 {
		t.Fatalf("missing %v, passes %v; want one of each", m, p)
	}
}

// The lease key is authoritative (R4M-3): with gw-0's ZADD gwepochs failing
// for 9s while its lease SET succeeds, its score goes stale, the sweeper
// claims it, finds the lease key and skips it.
func TestLeaseKeyIsAuthoritative(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw9"
	s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultGWEpochs, Key: s.GW[0].ID, For: dur(9 * time.Second)})
	eventually(t, "the sweeper skipped the live epoch", 10*time.Second, func() bool {
		return s.logicMetric(sprintf(logicSweeps, "lease_live")) >= 1
	})
	time.Sleep(3 * time.Second)
	if s.logicMetric(sprintf(logicSweeps, "done")) != 0 || !s.Redis.has(uid) {
		t.Fatal("the live epoch was swept")
	}
}

// The lease key is authoritative mid-sweep: a lease key that reappears
// while a sweep runs, with the score unchanged, aborts it.
func TestSweepAbortsWhenALeaseKeyReappears(t *testing.T) {
	s := New(t, Options{Logic: slowSweep})
	const gw, epoch = "gw-9", 77
	s.fill("sw10f", gw, epoch, 300)
	s.Redis.SetScore(gw, epoch, s.Redis.Now()-10_000)
	eventually(t, "the sweep started", 5*time.Second, func() bool { return s.claimed(gw, epoch) })
	s.Redis.Client().Set(ctx, "gwlease:"+epochSuffix(gw, epoch), "x", time.Minute)
	eventually(t, "the sweep aborted", 5*time.Second, func() bool { return s.logicMetric(sprintf(logicSweeps, "aborted")) == 1 })
	if _, ok := s.Redis.Score(gw, epoch); !ok {
		t.Fatal("the aborted sweep removed the gwepochs member")
	}
}

// An epoch that sent EpochEnded is swept at once, not after 2 × TTL.
func TestEndedEpochIsSweptAtOnce(t *testing.T) {
	s := New(t, Options{})
	g := s.GW[0]
	go g.term(30 * time.Second)
	eventually(t, "EpochEnded set the score to 0", 15*time.Second, func() bool {
		ms, ok := s.Redis.Score(g.ID, g.Epoch)
		return ok && ms == 0
	})
	start := time.Now()
	eventually(t, "the ended epoch was swept", 3*time.Second, func() bool { return s.swept(g) })
	t.Logf("swept %v after EpochEnded", time.Since(start))
}

// Stale lease observation (R4M-2): gw-1's lease is starved (key removed)
// while its newer session S2 is stored; gw-0's older S1, whose Kick was
// dropped, overwrites S2 by rule 4. gw-1's next renewal reports missing, and
// its triggered pass writes S2 back by rule 3 and kicks S1, within ~6s.
func TestStaleLeaseObservationIsCorrectedByTheOwner(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw11"
	s1 := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d1", 2)
	eventually(t, "Logic gave up on S1's Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	s.Fault(1, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true})
	eventually(t, "gw-1's renewals are blocked", 3*time.Second, func() bool { return s.Fired(1, gateway.FaultLogic) >= 1 })
	s.Redis.DelLease(s.GW[1].ID, s.GW[1].Epoch)
	s.Reconcile(0)
	eventually(t, "rule 4 overwrote S2 with S1", 5*time.Second, func() bool {
		e, _ := s.Redis.Entry(uid)
		return e.Gateway == s.GW[0].ID
	})
	s.ClearFault(0, gateway.FaultKick)
	s.ClearFault(1, gateway.FaultLogic)
	start := time.Now()
	s1.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 8*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != s.GW[1].ID || e.ConnSeq != 2 {
		t.Fatalf("entry %+v, want S2 written back", e)
	}
	t.Logf("corrected %v after gw-1 renewed", time.Since(start))
}
```

- [ ] **Step 2: Run them**

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestGatewayCrash|TestTwoGateways|TestThreeKilled|TestRedisPaused|TestGatewaysCutOff|TestLogicRestart|TestForcedSweep|TestSweepAborts|TestFirstLease|TestLeaseKey|TestEndedEpoch|TestStaleLease' ./internal/racetest/`
Expected: 13 `PASS` lines, about two minutes.

- [ ] **Step 3: See one fail**

In `internal/logic/guard.go`, make `Allows` return `true` at its first line. Run `GOIM_RACE_SUITE=1 go test -count=1 -run 'TestGatewaysCutOff|TestLogicRestart' ./internal/racetest/`.
Expected: both FAIL, `no longer true: nothing is swept`. Revert. (`TestRedisPausedNothingIsSwept` passes even without the guard: when Redis resumes, the gateways' queued renewals land before the sweeper's next read. It checks the outcome the spec asks for, not the guard.)

- [ ] **Step 4: Commit**

```bash
git add internal/racetest/sweep_test.go
git commit -m "test(racetest): sweeper guard, sweep abort, lease key and stale lease scenarios"
```

---

### Task 9: One Reconnect per connection during the drain (found by the suite)

**Files:**
- Modify: `internal/gateway/conn.go`, `internal/gateway/login.go`
- Test: `internal/gateway/drain_test.go`

**Interfaces:**
- Consumes: the gateway core's drain (Plan 3) and its fake-loop harness (`newCore`, `authed`, `login`, `reply`, `fakeLink.sent`).
- Produces: `Conn.reconnected`; `reconnect` sends `Reconnect{jitter_ms}` at most once per connection.

The prototype of Task 10's `TestDrainingGatewayStillReceivesKicks` failed about once in ten runs: the client got two `Reconnect` frames. A temporary log in `reconnect` showed both callers on the same connection, `online()` and the drain's pass over ONLINE connections. A Login whose reply is applied after `Drain` sets `draining` but before the drain's pass runs on that loop is reconnected by `online()` (which serves connections that come ONLINE during the drain, choice #35), and then again by the pass, which finds it ONLINE. The second frame is harmless to the SDK (it has closed already), but the spec's drain sends one `Reconnect` per connection, and a scripted or native client may count them. The fix marks the connection.

- [ ] **Step 1: Write the failing test**

In `internal/gateway/drain_test.go`, replace:

```go
		t.Fatalf("%d GatewayLease calls after EpochEnded", m-n)
	}
}
```

with:

```go
		t.Fatalf("%d GatewayLease calls after EpochEnded", m-n)
	}
}

// A Login that completes as the drain starts gets one Reconnect, not one
// from online() and another from the drain's pass over ONLINE connections
// (found by Plan 5b's TestDrainingGatewayStillReceivesKicks).
func TestDrainSendsOneReconnectToALoginCompletingAsItStarts(t *testing.T) {
	h := newCore(t, func(c *Config) { c.Drain.Window, c.Drain.Flush = time.Second, 0 })
	k := h.authed(0)
	it := h.login(k, "u1")
	it.state.Store(itemSent)
	h.g.draining.Store(true) // the drain's first step; its pass over the loops comes after
	h.reply(it, loginReply{outcome: loginOK, label: "ok", uid: "u1", tokIatMs: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	h.g.Drain(ctx)
	n := 0
	for _, f := range k.sent() {
		if f.cmd == clientpb.Cmd_CMD_RECONNECT {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d Reconnect frames, want 1", n)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -count=1 -run TestDrainSendsOneReconnect ./internal/gateway/`
Expected: FAIL, `2 Reconnect frames, want 1`.

- [ ] **Step 3: Write the fix**

In `internal/gateway/conn.go`, replace:

```go

	inChunk      bool // in a Reconcile chunk not yet resolved (fence c)
	logoutQueued bool
}

// ID returns the conn_id.
```

with:

```go

	inChunk      bool // in a Reconcile chunk not yet resolved (fence c)
	logoutQueued bool
	reconnected  bool // Reconnect sent (drain): once per connection
}

// ID returns the conn_id.
```

In `internal/gateway/login.go`, replace:

```go
// reconnect sends Reconnect{jitter_ms} (spec: Graceful drain); the client
// closes and reconnects elsewhere.
func (g *Gateway) reconnect(c *Conn) {
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}

```

with:

```go
// reconnect sends Reconnect{jitter_ms} (spec: Graceful drain); the client
// closes and reconnects elsewhere.
func (g *Gateway) reconnect(c *Conn) {
	if c.reconnected {
		return // a Login completing as the drain starts is seen by both online() and the drain's pass
	}
	c.reconnected = true
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}

```

- [ ] **Step 4: Run the gateway tests**

Run: `go vet ./internal/gateway/ && go test -race -count=1 ./internal/gateway/...`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/conn.go internal/gateway/login.go internal/gateway/drain_test.go
git commit -m "fix(gateway): one Reconnect per connection when a Login completes as the drain starts"
```

---

### Task 10: Kick and Revoke scenarios (races 4, 8, 17; R2-O4, R4M-1, PI-4)

**Files:**
- Test: `internal/racetest/kick_test.go`, `internal/racetest/revoke_test.go`

**Interfaces:**
- Consumes: the harness (Task 5); `gateway.FaultKick`, `gateway.FaultLogic` (Task 3); `logic.FaultLogin` (Task 2); `internalpb.RevocationClient` (Plan 2); one Reconnect per connection (Task 9).
- Produces: nothing for later tasks.

The zombie (race 4) is `SIGSTOP`ped, and a new epoch starts under the same name. Its first connection gets the same conn_id as the zombie's: one loop, a counter from 1, which the scenario checks. The Kick file now points at the new instance, as pod DNS would. "Revocation outlives the token" uses a 2s token, shorter than the 9s cut.

- [ ] **Step 1: Write the scenarios**

Create `internal/racetest/kick_test.go`:

```go
package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
)

// Race 4, zombie gateway with the same pod name: gw-0 freezes and is
// replaced by a new epoch under the same name, whose first connection gets
// the same conn_id. The relogin's Kick for the old epoch reaches the new
// instance (pod DNS follows the name) and is ignored for its epoch.
func TestRace4ZombieGatewayKickForTheOldEpochIsIgnored(t *testing.T) {
	s := New(t, Options{})
	const uid = "r4"
	s.Online(0, uid, "d1", 1)
	first, _ := s.Redis.Entry(uid)
	zombie := s.GW[0]
	zombie.pause()
	defer zombie.kill()
	s.RestartGateway(0)
	c := s.Online(0, uid, "d1", 2)
	e, _ := s.Redis.Entry(uid)
	if e.Epoch == first.Epoch || e.ConnID != first.ConnID {
		t.Fatalf("new entry %+v, old %+v: the scenario needs a new epoch with the same conn_id", e, first)
	}
	eventually(t, "the new instance ignored the Kick for the old epoch", 5*time.Second, func() bool {
		return s.gwMetric(0, sprintf(gwKicks, "ignored")) == 1
	})
	c.Quiet(time.Second)
}

// A Kick that arrives while the connection's Login is in flight (AUTH_PENDING)
// is held, and applied when the Login reply arrives: the client gets the
// Kick instead of LoginResp (race 8).
func TestKickDuringAuthPendingClosesWhenTheLoginReplyArrives(t *testing.T) {
	s := New(t, Options{})
	const uid = "k1"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(2 * time.Second), Count: 1})
	a := s.Dial(0)
	a.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "a's Login was written", 3*time.Second, func() bool { return s.Redis.has(uid) })
	s.Online(1, uid, "d2", 1) // kicks a while its Login reply is held
	eventually(t, "the Kick is pending on gw-0", 3*time.Second, func() bool { return s.gwMetric(0, sprintf(gwKicks, "pending")) == 1 })
	a.ExpectKick(clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, 5*time.Second)
}

// A Kick to a live gateway that is lost is retried (3 times, 1s apart at the
// profile) while the target's lease is live, then given up; the next
// Reconcile pass closes the connection (race 8, R2-O6).
func TestDroppedKickIsRetriedThenCaughtByReconcile(t *testing.T) {
	s := New(t, Options{})
	const uid = "k2"
	old := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d1", 2)
	eventually(t, "Logic gave up after its retries", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	if n := s.logicMetric(sprintf(logicKickAttempts, "retry", "error")); n != 3 {
		t.Fatalf("%v retries, want 3", n)
	}
	old.Quiet(500 * time.Millisecond)
	s.ClearFault(0, gateway.FaultKick)
	s.Reconcile(0)
	old.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 10*time.Second)
}

// A draining gateway still receives Kicks: its Kick server stays up until
// the end of the drain (publishNotReadyAddresses).
func TestDrainingGatewayStillReceivesKicks(t *testing.T) {
	s := New(t, Options{})
	const uid = "k3"
	a := s.Online(0, uid, "d1", 1)
	go s.GW[0].term(30 * time.Second)
	if f := a.Next(5 * time.Second); f.Cmd != clientpb.Cmd_CMD_RECONNECT {
		t.Fatalf("got %v, want Reconnect from the draining gateway", f.Cmd)
	}
	// The client ignores Reconnect; another instance of the device logs in,
	// and Logic kicks this one on the draining gateway. Nothing comes in
	// between: one Reconnect per connection (Task 9).
	s.Online(1, uid, "d1", 2)
	a.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 5*time.Second)
	// The client sees the Kick before Logic's RPC returns and counts it.
	eventually(t, "Logic's Kick attempt succeeded", 2*time.Second, func() bool {
		return s.logicMetric(sprintf(logicKickAttempts, "first", "ok")) == 1
	})
}
```

Create `internal/racetest/revoke_test.go`:

```go
package racetest

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Revoke calls Logic's Revocation service, as the account service does.
func (s *Stack) Revoke(req *pb.RevokeRequest) (*pb.RevokeReply, error) {
	s.t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+s.Logic.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	defer cc.Close()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return pb.NewRevocationClient(cc).Revoke(cctx, req)
}

// revokeAll revokes every slot of uid issued before now; it fails t on error.
func (s *Stack) revokeAll(uid string) int64 {
	s.t.Helper()
	before := s.Redis.Now()
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		s.t.Fatal(err)
	}
	return before
}

// pcReq is a LoginReq for uid's im:pc slot.
func (s *Stack) pcReq(uid, device string) *clientpb.LoginReq {
	req := s.Req(uid, device, 1)
	req.Platform = clientpb.Platform_PLATFORM_PC
	req.Token = s.TokenWith(s.jwtKey, uid, "pc", time.Now().UnixMilli(), time.Hour)
	return req
}

func (s *Stack) marker(uid, field string) int64 {
	s.t.Helper()
	v, err := s.Redis.Client().HGet(ctx, "rev:{"+uid+"}", field).Result()
	if err != nil {
		s.t.Fatalf("rev:{%s} %s: %v", uid, field, err)
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// Revoke closes the user's connections on two slots and deletes both fields.
func TestRevokeClosesConnectionsOnTwoSlots(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv1"
	m := s.Online(0, uid, "d1", 1)
	pc := s.OnlineWith(1, s.pcReq(uid, "d2"))
	s.revokeAll(uid)
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	pc.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if _, ok := s.Redis.EntryOf(uid, "im:pc"); ok || s.Redis.has(uid) {
		t.Fatal("a revoked field is still stored")
	}
}

// A Revoke whose Kick is lost: the next Reconcile pass closes the connection
// through rule 1 (not_owner REVOKED) and does not restore the session.
func TestRevokeWithADroppedKickIsCaughtByReconcile(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv2"
	c := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.revokeAll(uid)
	eventually(t, "Logic gave up on the Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	c.Quiet(500 * time.Millisecond)
	s.Reconcile(0)
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 10*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("Reconcile restored a revoked session")
	}
}

// A Revoke whose WAIT fails returns a retryable error and changes nothing a
// client sees; the retry with the same before_ms succeeds (R2-O4).
func TestRevokeWhoseWAITFailsIsRetried(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv3"
	c := s.Online(0, uid, "d1", 1)
	before := s.Redis.Now()
	s.Redis.ReplicaDownFor(2 * time.Second)
	_, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Revoke with no replica ack: %v, want UNAVAILABLE", err)
	}
	c.Quiet(500 * time.Millisecond)
	if !s.Redis.has(uid) {
		t.Fatal("an unconfirmed Revoke deleted the session")
	}
	time.Sleep(2 * time.Second)
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if m := s.marker(uid, "*"); m != before {
		t.Fatalf("marker %d, want %d", m, before)
	}
}

// PI-4: a retried Revoke with the same before_ms leaves the marker as it
// is; a token issued at or after before_ms logs in; a before_ms 10 minutes
// ahead of Redis TIME is rejected.
func TestRevokeBeforeMs(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv4"
	s.Online(0, uid, "d1", 1)
	before := s.revokeAll(uid)
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		t.Fatal(err)
	}
	if m := s.marker(uid, "*"); m != before {
		t.Fatalf("marker %d after the retry, want %d", m, before)
	}
	req := s.Req(uid, "d1", 2)
	req.Token = s.TokenAt(uid, before, time.Hour)
	s.OnlineWith(1, req)
	_, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: s.Redis.Now() + 10*60_000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("before_ms 10m ahead: %v, want INVALID_ARGUMENT", err)
	}
}

// Revocation outlives the token (R4M-1): the token lives 2s, shorter than
// the 9s outage. gw-0 is cut off from Logic; its connection is revoked with
// the Kick dropped. When gw-0 is back, its triggered pass closes the
// connection with REVOKED and does not restore it.
func TestRevocationOutlivesTheToken(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv5"
	req := s.Req(uid, "d1", 1)
	req.Token = s.TokenAt(uid, time.Now().UnixMilli(), 2*time.Second)
	c := s.OnlineWith(0, req)
	s.Fault(0, fault.Rule{Op: gateway.FaultLogic, Fail: true, For: dur(9 * time.Second)})
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.revokeAll(uid)
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 20*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the pass restored the revoked session")
	}
}

// Slot-scoped Revoke (R3-OP2): with its Kick dropped, the next Reconcile
// closes the connection through not_owner (REVOKED) and does not restore
// it; the user's other slot is unaffected.
func TestSlotScopedRevoke(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv6"
	m := s.Online(0, uid, "d1", 1)
	pc := s.OnlineWith(1, s.pcReq(uid, "d2"))
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, BeforeMs: s.Redis.Now()}); err != nil {
		t.Fatal(err)
	}
	if s.Redis.has(uid) {
		t.Fatal("the mobile field is still stored")
	}
	s.Reconcile(0)
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 10*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("Reconcile restored the revoked slot")
	}
	pc.Quiet(500 * time.Millisecond)
	if _, ok := s.Redis.EntryOf(uid, "im:pc"); !ok {
		t.Fatal("the pc slot was touched")
	}
}

// A slot Revoke whose WAIT fails returns an error and sends no Kick; the
// retry is idempotent.
func TestSlotRevokeWhoseWAITFailsSendsNoKick(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv7"
	m := s.Online(0, uid, "d1", 1)
	req := &pb.RevokeRequest{Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, BeforeMs: s.Redis.Now()}
	s.Redis.ReplicaDownFor(2 * time.Second)
	if _, err := s.Revoke(req); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want UNAVAILABLE", err)
	}
	m.Quiet(500 * time.Millisecond)
	if s.logicMetric(`goim_logic_revocation_kicks_total`) != 0 || !s.Redis.has(uid) {
		t.Fatal("the failed Revoke kicked or deleted")
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < 2; i++ {
		if _, err := s.Revoke(req); err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
	}
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if m := s.marker(uid, "im:mobile"); m != req.BeforeMs {
		t.Fatalf("slot marker %d, want %d", m, req.BeforeMs)
	}
}
```

- [ ] **Step 2: Run them**

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestRace4|TestKick|TestDropped|TestDraining|TestRevo|TestSlot' ./internal/racetest/`
Expected: 12 `PASS` lines (`TestDroppedLogoutIsRepairedByTheNextPass` from Task 7 matches too), about 40s. `TestDrainingGatewayStillReceivesKicks` expects the Kick right after one Reconnect, which Task 9 guarantees.

- [ ] **Step 3: See one fail**

In `internal/gateway/kick.go`, drop the epoch from the check: `if req.GetGw().GetGateway() != g.cfg.GatewayID {`. Run `GOIM_RACE_SUITE=1 go test -count=1 -run TestRace4 ./internal/racetest/`.
Expected: FAIL: the new instance closes its own connection. Revert.

- [ ] **Step 4: Commit**

```bash
git add internal/racetest/kick_test.go internal/racetest/revoke_test.go
git commit -m "test(racetest): race 4, Kick, Revoke, slot Revoke and revocation outlives the token"
```

---

### Task 11: Key rotation, JWKS fallback and clock skew (R4H-O10, R2-O7, R4M-5)

**Files:**
- Test: `internal/racetest/auth_test.go`, `internal/racetest/skew_test.go`

**Interfaces:**
- Consumes: the harness (Task 5), including `base` and `launchLogic` for a Logic that never becomes Ready; `gateway -clock-offset` (Task 3); `logic.FaultLogin` (Task 2).
- Produces: nothing.

- [ ] **Step 1: Write the scenarios**

Create `internal/racetest/auth_test.go`:

```go
package racetest

import (
	"net/http"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
)

// Key rotation (R4H-O10): a key that signs before Logic has fetched it gives
// KEY_UNKNOWN, then a login succeeds after the background unknown-kid
// refresh; during the overlap both kids are accepted.
func TestKeyRotation(t *testing.T) {
	s := New(t, Options{})
	const uid = "a1"
	k2, err := auth.GenerateKey("k2")
	if err != nil {
		t.Fatal(err)
	}
	s.PublishKeys(s.jwtKey, k2)
	req := s.Req(uid, "d1", 1)
	req.Token = s.TokenWith(k2, uid, "mobile", time.Now().UnixMilli(), time.Hour)
	if code := errorCode(t, s.Dial(0).Login(req)); code != clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN {
		t.Fatalf("got %v, want KEY_UNKNOWN", code)
	}
	eventually(t, "a k2 token logs in after the refresh", 5*time.Second, func() bool {
		req.ConnSeq++
		return s.Dial(0).Login(req).Cmd == clientpb.Cmd_CMD_LOGIN_RESP
	})
	s.Online(1, uid, "d1", req.ConnSeq+1) // k1 still accepted
	if n := s.logicMetric(`goim_logic_jwks_unknown_kid_refreshes_total`); n != 1 {
		t.Fatalf("%v unknown-kid refreshes, want 1", n)
	}
}

// JWKS (R2-O7): Logic starts with the account service down, becomes Ready
// from the jwks-fallback file and accepts tokens.
func TestLogicStartsFromTheJWKSFallback(t *testing.T) {
	s := New(t, Options{NoJWKS: true})
	s.Online(0, "a2", "d1", 1)
	if s.logicMetric(`goim_logic_jwks_fallback`) != 1 {
		t.Fatal("jwks_fallback is not 1")
	}
}

// JWKS: a Logic with no key set from either source never becomes Ready.
func TestLogicWithNoKeySetIsNeverReady(t *testing.T) {
	s := base(t, Options{NoJWKS: true, NoFallback: true})
	l := s.launchLogic()
	holds(t, "Logic stays NotReady and running", 3*time.Second, func() bool {
		if l.exited() {
			return false
		}
		resp, err := http.Get("http://" + l.Admin + "/readyz")
		if err != nil {
			return true // not listening yet
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	})
}
```

Create `internal/racetest/skew_test.go`:

```go
package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
)

// Clock skew ahead (R4M-5): +2s fires the skew alert. A gateway ahead by
// more than the 1s skew allowance can also see a Login written after its
// fence (b) released the Logout: the Lua's deadline runs on the gateway's
// recv_ms, the fence on its wheel. That ghost is deleted by the next pass's
// delete phase. The ghost uses +5s: the fence fires 6–8s after recv (the
// wheel rounds the deadline up to a whole second, and its 1s ticker has its
// own phase), and +5s puts the Lua deadline at 10s of real time, so a write
// at 9s lands a second clear of both.
func TestClockAheadAlertsAndItsLateWriteIsCleanedUp(t *testing.T) {
	s := New(t, Options{})
	s.GW[0].kill()
	s.RestartGateway(0, "-clock-offset", "2s")
	s.GW[1].kill()
	s.RestartGateway(1, "-clock-offset", "5s")
	eventually(t, "the skew alert fired for both", 5*time.Second, func() bool {
		return s.logicMetric(`goim_logic_clock_skew_alert{gw="gw-0"}`) == 1 &&
			s.logicMetric(`goim_logic_clock_skew_alert{gw="gw-1"}`) == 1
	})
	const uid = "sk1"
	// The Login reaches Redis 9s after recv: after the fence (at most 8s)
	// and inside the Lua deadline, 10s of real time with recv_ms 5s ahead.
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(9 * time.Second), Count: 1})
	if f := s.Dial(1).Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	eventually(t, "the late write left a ghost", 5*time.Second, func() bool { return s.Redis.has(uid) })
	if s.gwMetric(1, `goim_gateway_tracked`) != 0 {
		t.Fatal("the connection is still tracked: its Logout has not gone yet")
	}
	s.Reconcile(1)
	s.endsOK(1, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the pass's delete phase left the ghost")
	}
}

// Clock skew −5s: the gateway's recv_ms is already past the deadline when
// the Lua runs, so its Logins are EXPIRED; the client logs in elsewhere.
func TestClockBehindLoginsExpireAndTheClientLogsInElsewhere(t *testing.T) {
	s := New(t, Options{})
	s.GW[0].kill()
	s.RestartGateway(0, "-clock-offset", "-5s")
	const uid = "sk2"
	if code := errorCode(t, s.Dial(0).Login(s.Req(uid, "d1", 1))); code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("got %v, want EXPIRED", code)
	}
	s.Online(1, uid, "d1", 2)
}
```

- [ ] **Step 2: Run them**

Run: `GOIM_RACE_SUITE=1 go test -count=1 -v -run 'TestKeyRotation|TestLogicStarts|TestLogicWithNo|TestClock' ./internal/racetest/`
Expected: 5 `PASS` lines, about 15s.

- [ ] **Step 3: See one fail**

In `cmd/gateway/main.go`, replace `d.Clock = func() time.Time { return time.Now().Add(off) }` with `_ = off`. Run `GOIM_RACE_SUITE=1 go test -count=1 -run TestClock ./internal/racetest/`.
Expected: both FAIL (no skew alert; the −5s gateway answers `LoginResp`). Revert.

- [ ] **Step 4: Run the whole suite**

Run: `make test-race-suite`
Expected: `ok github.com/Tallinzp/goim/internal/racetest` in about 4½ minutes. Then `go vet ./... && make test`: Tier 1a still passes, and the suite skips there.

- [ ] **Step 5: Commit**

```bash
git add internal/racetest/auth_test.go internal/racetest/skew_test.go
git commit -m "test(racetest): key rotation, JWKS fallback and clock skew"
```

---

### Task 12: The Tier 2 entry point (choice #64)

**Files:**
- Create: `scripts/tier2/race.sh`

**Interfaces:**
- Consumes: `make redis-server` (Task 4); the suite (Tasks 5–11).
- Produces: `make test-tier2 SCENARIO=race`; `tier2-results/race-<ts>/result.json`, `tests.log`, `logs/<scenario>/*.log`, and `tier2-results/race-<ts>.report.txt`.

The script follows `scripts/tier2/store.sh`: host guard, a mutation in a throwaway worktree, JSON result, teardown that always writes the report. It needs no Docker: it builds the pinned Redis (`make redis-server`, a no-op once built) and each scenario starts its own. The host guard keeps it off the dev machine; Step 2 checks its syntax and the mutation's pattern there. (In the prototype it also ran end to end on the dev machine with only the guard lowered, *Execution Notes*.)

- [ ] **Step 1: Write the script**

Create `scripts/tier2/race.sh`:

```bash
#!/usr/bin/env bash
# Tier 2: run the race integration suite (Plan 5b) on the Tier 2 host. The
# binaries run as local processes (choice #47), with a redis-server primary
# and replica per scenario, the pinned 7.4.11 that make redis-server builds
# (choice #62): the same suite as the dev machine, on a host with CPU to spare.
#
# Usage (on the Tier 2 host, from the repo root): make test-tier2 SCENARIO=race
#   The host guard refuses smaller hosts.
#   Results: tier2-results/race-<timestamp>/ (result.json, tests.log, the
#   process logs of every scenario), plus ONE file to send back if anything
#   fails: tier2-results/race-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. tests:    every scenario passes (GOIM_RACE_SUITE=1, binaries built with -race)
#   2. all_ran:  every scenario ran (passed, not skipped): `go test -list` count = PASS count
#   3. mutation: with fence (a) removed (a closed connection's Logout is sent
#                before its Login reply), TestRace5CloseWhileLoginInFlight fails
#   4. redis:    the suite ran on the pinned redis-server 7.4.11
set -uo pipefail

cd "$(git rev-parse --show-toplevel)"
export CGO_ENABLED=1 GOIM_RACE_SUITE=1
export GOIM_REDIS_SERVER="$PWD/.cache/redis/bin/redis-server"
redis_version=7.4.11

# Host guard (spec: Tier 2 host is 12 cores / 26GB; never the dev machine).
cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null)
if [ -r /proc/meminfo ]; then
  mem_gb=$(awk '/MemTotal/ {printf "%d", $2 / 1024 / 1024}' /proc/meminfo)
else
  mem_bytes=$(sysctl -n hw.memsize 2>/dev/null)
  mem_gb=$(( ${mem_bytes:-0} / 1024 / 1024 / 1024 ))
fi
for v in "$cores" "$mem_gb"; do
  case "$v" in
    ''|*[!0-9]*|0) echo "refused: cannot determine cores/RAM (cores='$cores', mem_gb='$mem_gb')" >&2; exit 2 ;;
  esac
done
if [ "$cores" -lt 12 ] || [ "$mem_gb" -lt 24 ]; then
  echo "refused: Tier 2 needs >= 12 cores and >= 24GB RAM (have $cores cores, ${mem_gb}GB)" >&2
  exit 2
fi
for tool in go gcc make git curl; do
  command -v "$tool" >/dev/null || { echo "refused: $tool not installed" >&2; exit 2; }
done

out="$PWD/tier2-results/race-$(date +%Y%m%d-%H%M%S)"
report="$out.report.txt"
mkdir -p "$out/logs"
export GOIM_RACE_LOGS="$out/logs"
mutdir=""

section() { printf '\n===== %s =====\n' "$1"; }
write_report() {
  {
    echo "goim Tier 2 report: scenario race"
    section "environment"
    echo "date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit:  $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
    echo "dirty:   $(git status --porcelain | wc -l) changed files"
    echo "host:    $cores cores, ${mem_gb}GB RAM, $(uname -sr)"
    echo "go:      $(go version)"
    echo "gcc:     $(gcc --version | head -1)"
    echo "redis:   $("$GOIM_REDIS_SERVER" --version 2>&1)"
    echo "ports:   suite 20000-32000; ephemeral $(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
    section "result.json"
    cat "$out/result.json" 2>/dev/null || echo "(not written: the run stopped early)"
    section "redis-build.log"
    tail -n 50 "$out/redis-build.log" 2>/dev/null
    section "tests.log (RUN/PASS lines removed; failing scenarios include their process logs)"
    if [ -f "$out/tests.log" ]; then
      grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 4000
    fi
    section "mutation.log"
    tail -n 200 "$out/mutation.log" 2>/dev/null
  } >"$report" 2>&1
}
teardown() {
  write_report
  [ -n "$mutdir" ] && git worktree remove --force "$mutdir" >/dev/null 2>&1
  echo "report: $report"
}
trap teardown EXIT

if ! make redis-server >"$out/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $out/redis-build.log" >&2
  exit 1
fi

# 1 + 2: the suite. Each scenario starts its own Redis primary and replica.
listed=$(go test -list '.*' ./internal/racetest/ | grep -c '^Test')
go test -count=1 -v -timeout 45m ./internal/racetest/ >"$out/tests.log" 2>&1
tests_rc=$?
passed=$(grep -cE '^--- PASS' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)

# 3: mutation check in a throwaway worktree. The mutant sends a closed
# connection's Logout as soon as it closes while its Login is in flight
# (fence (a) removed): the Logout lands before the write and a ghost stays.
mutdir=$(mktemp -d)
git worktree add --detach "$mutdir" HEAD >/dev/null 2>&1
sed -i.orig 's/if c.outcome == loginOK || c.fenceB { \/\/ fence (a) or (b)/if c.outcome == loginOK || c.outcome == loginPending || c.fenceB { \/\/ MUTANT: no fence (a)/' "$mutdir/internal/gateway/gateway.go"
if ! grep -q 'MUTANT: no fence (a)' "$mutdir/internal/gateway/gateway.go"; then
  mutation="error: could not apply the mutation"
elif ! (cd "$mutdir" && go vet ./internal/gateway/) >"$out/mutation.log" 2>&1; then
  mutation="error: the mutant does not compile"
elif (cd "$mutdir" && GOIM_RACE_LOGS= go test -count=1 -v -run '^TestRace5CloseWhileLoginInFlight$' ./internal/racetest/) >>"$out/mutation.log" 2>&1; then
  mutation="fail: race 5 still passes without fence (a)"
elif grep -q -- '--- FAIL: TestRace5CloseWhileLoginInFlight' "$out/mutation.log" && grep -q 'no ghost\|fenced Logout' "$out/mutation.log"; then
  mutation="pass"
else
  mutation="error: the test failed for another reason, see mutation.log"
fi

# 4: the Redis the suite reported it ran (TestMain prints it).
ran_on=$(grep -m1 '^racetest: .*: Redis ' "$out/tests.log" | sed 's/.*: Redis //')

ok=true
[ "$tests_rc" -eq 0 ] || ok=false
[ "$passed" -eq "$listed" ] || ok=false
[ "$mutation" = "pass" ] || ok=false
[ "$ran_on" = "$redis_version" ] || ok=false

cat >"$out/result.json" <<JSON
{
  "scenario": "race",
  "tier": 2,
  "commit": "$(git rev-parse HEAD)",
  "host": {"cores": $cores, "mem_gb": $mem_gb},
  "pass": $ok,
  "checks": {
    "tests": {"pass": $([ "$tests_rc" -eq 0 ] && echo true || echo false), "failed": "$failed"},
    "all_ran": {"pass": $([ "$passed" -eq "$listed" ] && echo true || echo false), "listed": $listed, "passed": $passed},
    "mutation": {"pass": $([ "$mutation" = "pass" ] && echo true || echo false), "detail": "$mutation"},
    "redis": {"pass": $([ "$ran_on" = "$redis_version" ] && echo true || echo false), "want": "$redis_version", "ran_on": "$ran_on"}
  },
  "not_covered": ["Redis Cluster (one primary + one replica here)", "a real network between the processes (all on 127.0.0.1)"]
}
JSON

cat "$out/result.json"
$ok && exit 0 || exit 1
```

- [ ] **Step 2: Check it**

Run: `chmod +x scripts/tier2/race.sh && bash -n scripts/tier2/race.sh && grep -c 'if c.outcome == loginOK || c.fenceB { // fence (a) or (b)' internal/gateway/gateway.go`
Expected: no syntax error, and `1`: the mutation's `sed` pattern matches the code exactly once. `scripts/tier2/race.sh` on the dev machine exits 2 with `refused: Tier 2 needs >= 12 cores`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/race.sh
git commit -m "test(tier2): make test-tier2 SCENARIO=race (choice #64)"
```

---

## After this plan

Every race integration test in the spec runs on the dev machine before each commit (`make test-race-suite`), against the real binaries and a real Redis primary and replica on the test timing profile, and again on the Tier 2 host (`make test-tier2 SCENARIO=race`). The suite found one gateway bug, fixed in Task 9. The roadmap records #58–#64 and this plan's test names in the invariant index.

Plan 6 builds `cmd/loadgen` and the Tier 2/3 load scenarios. It can reuse the harness's process and port handling and the fault API (a slowed Logic), and it decides the client per load test as P5I-2 records.

The open risks are listed in *Execution Notes*.
