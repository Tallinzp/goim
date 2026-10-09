# Race Integration Suite Implementation Plan — Plan 5b2 of 6 (part 2 of 3: Redis build and harness)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Plan 5b is split into three files, each executed in its own session, in order: **5b1** `2026-10-08-connection-session-core-05b1-race-faults.md` (Tasks 1–3), **5b2** `2026-10-08-connection-session-core-05b2-race-harness.md` (Tasks 4–5), **5b3** `2026-10-08-connection-session-core-05b3-race-scenarios.md` (Tasks 6–12). Task numbers, choice numbers (#58–#64) and finding numbers are the same in all three.

**Tech Stack:** Go 1.27, the Plans 1–5a binaries and packages, Redis 7.4.11 (`redis-server`, built from the release tarball with its published SHA-256), `github.com/redis/go-redis/v9` hooks, `google.golang.org/grpc` interceptors, `net/http` for the fault API. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative). Sections: *Verification → Race integration tests* (all of it), *Test execution model* (Tier 1b, the test timing profile), *Races handled*, *Consistency contract*, *Storms, drain, failures*. Roadmap, choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. What this plan builds on: Plan 5a's *Execution Notes → For Plan 5b* and its interface review (P5I-1..P5I-6), and Plan 4's P4I-5 (the gateway clock flag).

**Read in 5b1 when cited:** the full Goal and Architecture, *Implementation choices* (#58–#64) and *Execution Notes* (prototype runs, findings 1–12, the mutation list, the spec test map, open risks) are in `2026-10-08-connection-session-core-05b1-race-faults.md`. A task that cites a choice or a finding by number means that section there.

**This part:** Tasks 4–5, the pinned Redis build (`make redis-server`, `make test-redis-local`) and the process harness `internal/racetest` with its self-checks (Review Focus 1–5). It needs 5b1's binaries with `-faults`. **Branch:** continue on `feat/plan5b-race` after 5b1's last commit. Next: 5b3.

## Global Constraints

- Module path `github.com/Tallinzp/goim`. No new module dependencies.
- Layer rule (Plan 3): no package under `internal/gateway/...` may depend on `internal/store` or `github.com/redis/...` (`TestGatewayDoesNotImportTheStore`). `internal/fault` imports neither.
- Production behaviour is unchanged unless a binary is started with `-faults`. A nil `*fault.Set` reports no fault, and `/fault/` is not served without the flag.
- Spec, test timing profile: "every interval and TTL is configurable. The profile scales the time-based ones down ~10× while keeping their ratios: lease TTL 3s, renewal every 1s, guard windows, periodic Reconcile, and the 90s outages become 9s. `login_deadline` and the 1s skew allowance are **not** scaled … Token lifetime is set per scenario through devtoken, and the `sess:`/`rev:` TTL stays at 30d. Gateway and Logic refuse to start if a profile breaks a required ratio (e.g. lease TTL ≤ failover time + one renewal interval)."
- Spec, race integration tests: "`deploy/compose`: Redis, Logic, **4 gateways** and a scripted client; Tier 1b, test timing profile". Choice #47 replaces compose with local processes; #62 makes Redis local processes too: a `redis-server` primary and replica per scenario, on the dev machine and at Tier 2 (`make test-tier2 SCENARIO=race`).
- Tier 1b runs "before every commit / merge" on the dev machine (2 cores / 2GB). Docker is never installed there (user decision 2026-10-07); `redis-server` built from source is (the user's decision at the Plan 5b review, 2026-10-08), for the race suite and for `make test-redis-local`.
- Tier 2 automation contract: one entry point per scenario, a host guard (`nproc`, memory), teardown always runs, machine-readable JSON with pass/fail and a matching exit code.
- Roadmap choices #1–#57 apply; Plan 5b's choices are #58–#64, in 5b1's *Implementation choices*.

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

## After this part

Next is Plan 5b3 (`2026-10-08-connection-session-core-05b3-race-scenarios.md`, Tasks 6–12), in a fresh session on the same branch.

## Execution Notes (5b2)

Executed inline 2026-10-09 on `feat/plan5b-race`: Task 4 `6e69b5f`, Task 5 `057e63c`, plan code verbatim. Step 3's `grep -c 'TODO: Tier 2 integration'` counts 0 without `-v` whatever happens (go test prints skip reasons only with `-v`); re-run with `-v`: 0 such skips, 8 skips that are miniredis-only by design (plus the race suite's own opt-in skips).

Fresh Opus final review, fixed in `ab1653d`, each with a test that failed first:
- **C-1** `make test-redis-local` had lost its recipe line (an execution splice error in Task 5's Makefile edit, not a plan defect) and did nothing with exit 0.
- **I-1** `TestProcessesDieWithTheSuite`'s child inherited `GOIM_RACE_SUITE`, built the race binaries into a `goim-race-bin-*` temp dir and exited without removing it (~85MB per suite run), and leaked its `t.TempDir`. The child now runs with the suite switch empty, in a directory the parent owns; the test checks nothing is left in the temp dir.
- **I-2** (Review Focus 1) `kill()`/`term()` marked a process stopped even if it had already exited on its own, hiding a crash in the window a scenario then "crashes" it. They now record it; new helper case `crash-then-kill`.
- **I-3** (Review Focus 2) the fired check was one cleanup registered at the first rule, so rules on processes started later (restarts) were skipped as killed. Now one check per rule, registered after its process started; new helper case `unfired-fault-after-restart`.

Deferred minors: M-1 a re-`Put` of the same op on the same process resets `Fired`, so an earlier unfired rule passes if the later one fired; M-2 the helper child test binaries have no `Pdeathsig`/`-test.timeout`; M-3 `Pdeathsig` follows the forking OS thread (Go #27505), harmless without `LockOSThread` — worth a comment; M-4 `race-report` proves only the log scan, so re-run the `data_race` mutation after 5b3; M-5 `TestProfileBreakingARatioIsRefused`'s processes are not race-scanned.
