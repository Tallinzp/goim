# Race Integration Suite Implementation Plan — Plan 5b3 of 6 (part 3 of 3: scenarios and Tier 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Plan 5b is split into three files, each executed in its own session, in order: **5b1** `2026-10-08-connection-session-core-05b1-race-faults.md` (Tasks 1–3), **5b2** `2026-10-08-connection-session-core-05b2-race-harness.md` (Tasks 4–5), **5b3** `2026-10-08-connection-session-core-05b3-race-scenarios.md` (Tasks 6–12). Task numbers, choice numbers (#58–#64) and finding numbers are the same in all three.

**Tech Stack:** Go 1.27, the Plans 1–5a binaries and packages, Redis 7.4.11 (`redis-server`, built from the release tarball with its published SHA-256), `github.com/redis/go-redis/v9` hooks, `google.golang.org/grpc` interceptors, `net/http` for the fault API. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative). Sections: *Verification → Race integration tests* (all of it), *Test execution model* (Tier 1b, the test timing profile), *Races handled*, *Consistency contract*, *Storms, drain, failures*. Roadmap, choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. What this plan builds on: Plan 5a's *Execution Notes → For Plan 5b* and its interface review (P5I-1..P5I-6), and Plan 4's P4I-5 (the gateway clock flag).

**Read in 5b1 when cited:** the full Goal and Architecture, *Implementation choices* (#58–#64) and *Execution Notes* (prototype runs, findings 1–12, the mutation list, the spec test map, open risks) are in `2026-10-08-connection-session-core-05b1-race-faults.md`. A task that cites a choice or a finding by number means that section there.

**This part:** Tasks 6–12, the race scenarios on 5b2's harness, the gateway fix the suite found (Task 9), and the Tier 2 entry point. **Branch:** continue on `feat/plan5b-race` after 5b2's last commit.

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
// delete phase. The ghost uses +5s: the fence fires 6–7s after recv (the
// wheel rounds the deadline up to a whole second), and +5s puts the Lua
// deadline at 10s of real time, so a write at 9s lands well after the fence
// and a second before the deadline.
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
	// The Login reaches Redis 9s after recv: after the fence (at most 7s)
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
