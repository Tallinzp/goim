# Session Store (Redis + Lua) Implementation Plan — Plan 1 of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the Go module and build `internal/store`, Logic's only path to Redis: every key and Lua script in the spec's Redis schema. Tests run on in-process miniredis at Tier 1a; tests that need real Redis are deferred to the Tier 2 host.

**Architecture:** `internal/session` owns the slot record (`gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`), its validation and the Reconcile ordering rule. `internal/store` wraps a `redis.UniversalClient` (Cluster in production, one node plus a replica in tests). It embeds one Lua file per atomic operation and runs batches as pipelined `EVALSHA`. If a script is missing from the cache (`NOSCRIPT`), it falls back to `EVAL`. The policy decisions (which kick to send, lease liveness caching, the sweeper's guard, Reconcile rules 1–4) belong to Logic in Plan 2. This plan provides the atomic primitives they rely on.

**Tech Stack:** Go 1.27, `github.com/redis/go-redis/v9 v9.23.0`, `github.com/alicebob/miniredis/v2 v2.39.0` (in-process Redis for Tier 1a), Redis 7.4 in Docker Compose (Tier 2 host only), Lua 5.1 (Redis scripting).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev5.md`. Roadmap and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`.

## Global Constraints

- Module path `github.com/Tallinzp/goim`; one Go module containing `cmd/{gateway,logic,devtoken,loadgen}`, `internal/...`, `api/{client,internal}.proto`, `deploy/{compose,k8s}`.
- Layer rule: "The gateway never touches Redis. Only Logic touches Redis." `internal/store` is imported only by Logic code.
- Keys (spec *Redis schema*): `sess:{uid}` HASH, field `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`, key TTL 30d refreshed on login and by Reconcile; `rev:{uid}` HASH, field `*` or `appid:platform` → `revoked_before_ms`, key TTL = the `sess:{uid}` TTL (30d; not separately configurable), refreshed on every marker write; `gwlease:gw:{epoch}` STRING, `PX <lease TTL>` (default 30s), renewed with `SET ... PX <TTL> GET`; `gwepochs` ZSET, member `gw:epoch`, score = last renewal in ms, advisory; `gwconns:gw:{epoch}` SET of uids; `sweep:gw:{epoch}` STRING `SET NX PX 300000`.
- `login_deadline` default 5s. Login Lua order: revocation first, then deadline, then the login table (R4H-OP1).
- Effective revocation marker for a slot: `max(rev[*], rev[appid:platform])`; a missing field counts as 0.
- Revoke: the marker is written with `max(stored, new)`, then `WAIT 1 <timeout>`; it must be confirmed before any field is deleted and before `Revoke` returns success.
- Reconcile compare-and-set writes only if the field is still what Logic observed **and** Redis `TIME − sent_ms ≤ login_deadline`. The deadline refuses only writes that create or replace a field. Late TTL refreshes and `gwconns` SADDs are never refused.
- Tier 1a runs on the dev machine (2 cores / 2GB) on every change: unit tests with `-race`. **User decision (2026-10-07), overriding the spec's "Lua tests against a real Redis in Docker (Tier 1a)":** Docker is not installed on the dev machine. Lua tests run on in-process miniredis there. Tests that need real Redis call `testredis.RequireReal` and skip with `TODO: Tier 2 integration`; they run on the Tier 2 host with `make test-redis`. See *Execution Notes*.
- Implementation choices 1–10 in the roadmap ("Implementation choices the spec leaves open") apply to this plan.

## Review Focus

1. **`conn_id` and `epoch` above 2^53.** Epochs are random uint64s, so almost every epoch is above 2^53. `conn_id = (worker_id << 48) | counter` goes above it on any loop with `worker_id` ≥ 32. A Lua `tonumber` would make different ids compare equal. Expected: Lua compares them only as strings, so a Logout for `2^63` never deletes `2^63+1`. Pinned in Task 4.
2. **Field-value injection.** A `device_id` or gateway id containing `|`, or a `conn_seq` above 2^53, would corrupt the stored record or the Lua comparisons. Expected: rejected in Go before any Redis call, without failing the other requests in the batch. Pinned in Tasks 2 and 3.
3. **Script cache flushed by a failover.** A new master has no cached scripts, so every pipelined `EVALSHA` gets `NOSCRIPT`. Expected: the batch falls back to `EVAL` and every login still gets its real result. Pinned in Task 3.
4. **`WAIT` on the wrong connection.** go-redis pools connections, and a `WAIT` on a connection that made no writes returns at once with every replica counted. Expected: `Revoke` reports `ErrNotReplicated` and deletes nothing when the replica has not acknowledged the marker. Pinned in Task 5 by a **Tier 2** test (skipped locally; see *Execution Notes*).
5. **A fresh login racing a user-wide revoke.** A user changes their password and logs in again with a new token between the marker write and the field delete. Expected: the new session survives, because only fields whose token predates their effective marker are deleted. Pinned in Task 5.

## Execution Notes

- **Execution mode:** Native: tasks are implemented in one session. One final whole-branch review runs on the most capable model, and it must list every risk not yet verified on real Redis.
- **Baseline:** the plan documents are committed before Task 1, so the branch diff contains only this plan's code.
- **Approved choices (2026-10-07):** roadmap choices #2, #3, #4 and #5.
- **Tier 1a runs on miniredis; nothing has been verified on real Redis yet.** The user decided not to install Docker on the dev machine (2 cores / 2GB). These risks stay open until `make test-redis` passes on the Tier 2 host:
  1. **Revoke `WAIT`** (Review Focus 4). `TestRevokeEndToEnd` and `TestRevokeFailsWhenReplicaDoesNotAck` skip locally (`TODO: Tier 2 integration`). Locally, miniredis always answers `WAIT` with 0, which only proves that an unconfirmed marker deletes nothing (`TestRevokeWithoutReplicaAckDeletesNothing`). It does not prove that `WAIT` runs on the marker's connection, or that a real replica's acknowledgement lets `Revoke` succeed.
  2. **Replica set-up.** `TestReplicaIsAttached` skips locally. It checks the compose replica is linked and accepts `DEBUG`.
  3. **Lua engine.** miniredis runs scripts on gopher-lua, not Redis's Lua 5.1. `string.gmatch`, `tonumber` on large strings, and `false` for nil replies may differ. The `split` helper and the 2^53 string comparisons depend on them.
  4. **Script cache.** `TestLoginSurvivesScriptFlush` exercises miniredis's `SCRIPT FLUSH` and `NOSCRIPT`. The exact error text a real Redis returns in a pipeline is untested locally.
  5. **`TIME` inside scripts.** miniredis serves `TIME` from the host clock. Real Redis 7 allows `TIME` before a write under effects replication; this needs a real-Redis run.
  6. **TTL and SET semantics.** `PEXPIRE`/`PTTL`, `SET ... PX ... GET` returning nil for a missing key, `SSCAN` paging, and `UNLINK` are emulated.
  7. **Redis Cluster.** Neither tier runs a Cluster in this plan. `MasterForKey` (the Revoke and lease paths) and cross-slot pipelines are untested until Plan 5's compose cluster. `TestKeysShareSlotsWhereScriptsNeedIt` only checks the hash-tag rule offline.
  8. **Lua global protection.** Real Redis rejects a script that creates a global variable; gopher-lua may not. The current scripts are clean, but a future script could pass locally and fail in production.
  9. **NOSCRIPT across Cluster nodes.** The fallback is tested on one node only, not in a ClusterClient pipeline spread over several nodes where only one node lost its script cache.
  10. **Revoke after a shard failover.** (a) The new master has no replica until one re-attaches, so every Revoke on that shard returns `ErrNotReplicated` until then (an availability cost; the account service retries). (b) Revoke's sticky `Conn` from `MasterForKey` gets `MOVED` back as a plain error. go-redis reloads the slot map only when cluster-routed traffic hits `MOVED`, or after `ClusterStateReloadInterval` (60s), so Revoke can fail for up to 60s on a quiet cluster. Logic's continuous cluster-routed traffic normally triggers the reload at once.
  11. **Client retries re-running scripts.** go-redis re-sends a command or pipeline whose reply was lost after the write (read timeout, EOF). Fixed for login, compare-and-set and lease (see *Final review fixes*). Not fixed for `rev_delete.lua`: a re-run deletes nothing and returns an empty list, so those Revoke kicks are lost. Reconcile rule 1 closes the connections within one cycle (the contract's "Kick lost" row). No test injects a lost reply on real Redis; that needs a fault-injecting TCP proxy on the Tier 2 host.
  12. **Eviction policy.** Every key has a TTL, so any `volatile-*` or `allkeys-*` `maxmemory-policy` can silently evict `rev:{uid}` (a lost revocation that `WAIT` cannot prevent), `gwlease` (a false `missing`) or `sess`. Production Redis must run with `noeviction`. The spec doesn't say so; Plan 6's deploy checks must assert `CONFIG GET maxmemory-policy`.
  13. **SSCAN paging.** On real Redis a set of up to 128 members is listpack-encoded and comes back in one `SSCAN` call, so `TestScanConnsPagesThroughTheSet` (50 members) does not exercise paging there.
  14. **Hash-tag slots.** `TestKeysShareSlotsWhereScriptsNeedIt` checks a hand-written rule, not Redis's CRC16. A Cluster test should assert `CLUSTER KEYSLOT` equality for `sess:{u}`/`rev:{u}` and the three per-epoch keys.
- **Tier 2 command** (Tier 2 host, Docker installed): `GOIM_TEST_TIER=2 make redis-up test-redis`. Expected: every package `ok`, and no test skipped with `TODO: Tier 2 integration`.

- **Final review fixes (2026-10-07).** These are the interface changes Plan 2 must use; the task code blocks above show the pre-review version.
  - **`LoginAlready` (`"ALREADY"`):** the login Lua returns it when the slot already holds this exact entry, i.e. a re-run of a request whose first reply was lost. Treat it as success with no kick.
  - **Compare-and-set re-run:** `reconcile_cas.lua` returns `WRITTEN` when the field already holds the chunk entry, so Logic still kicks the replaced entry.
  - **`RenewLease`:** now one script (`lua/lease.lua`) that reads the old value, sets `<nonce>|<missing>` with `PX <TTL>`, and returns `TIME`. It runs through the cluster-routed client, and a re-run with the same nonce returns the first run's `Missing`. The lease value is no longer `1`; only its existence matters (validity rule).
  - **Plan 3 note:** a renewal RPC whose outcome is unknown (timeout, transport error) must be treated as `missing`, with a triggered pass, because the renewal may have recreated an absent key.

---

## File Structure

| File | Responsibility |
|---|---|
| `go.mod`, `go.sum` | Module `github.com/Tallinzp/goim`, go-redis v9.23.0 |
| `Makefile` | `test` (Tier 1a: miniredis, `-race`); `redis-up`, `redis-down`, `test-redis` (Tier 2 host only, guarded by `GOIM_TEST_TIER=2`) |
| `deploy/compose/redis-test.yml` | Tier 2 Redis: primary on 6379, replica on 6380 with DEBUG enabled |
| `internal/testredis/testredis.go` | Test helper: in-process miniredis by default, real Redis when `GOIM_TEST_REDIS` is set; `RequireReal` skips real-Redis tests at Tier 1a; unique uids and gateway ids |
| `internal/session/session.go` | `Entry` encode/decode/validate, slot field, uid/gateway validation, `ChunkWins` ordering |
| `internal/store/store.go` | `Config`, `Store`, embedded scripts, pipelined `EVALSHA` with `NOSCRIPT` fallback, `nodeFor`, `RedisNowMs` |
| `internal/store/keys.go` | Key builders and `EpochRef` |
| `internal/store/login.go` + `lua/login.lua` | Login Lua, `AddConns` |
| `internal/store/logout.go` + `lua/logout.lua` | Logout compare-and-delete |
| `internal/store/revoke.go` + `lua/rev_mark.lua`, `lua/rev_delete.lua` | Marker write + `WAIT` on one connection, then delete revoked fields |
| `internal/store/reconcile.go` + `lua/reconcile_cas.lua` | `ReadSlots`, `CompareAndSet`, `RefreshSlots` |
| `internal/store/reconcile_end.go` + `lua/reconcile_end.lua` | `ScanConns`, `DeleteUntracked` (ReconcileEnd delete phase) |
| `internal/store/lease.go` | `RenewLease`, `EndEpoch`, `LeasesExist`, `ListEpochs` |
| `internal/store/sweep.go` + `lua/sweep_uid.lua` | `EpochScoreOf`, `ClaimSweep`, `ReleaseSweep`, `SweepUsers`, `FinishSweep` |

Each `*.go` file in `internal/store` has a matching `*_test.go`; shared test helpers are in `internal/store/helpers_test.go`.

---

### Task 1: Toolchain, module and test Redis helper

**Files:**
- Create: `go.mod`, `Makefile`, `deploy/compose/redis-test.yml`, `internal/testredis/testredis.go`
- Test: `internal/testredis/testredis_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `testredis.New(t testing.TB) *redis.Client` (fresh miniredis per call, or the real primary when `GOIM_TEST_REDIS` is set), `testredis.NewReplica(t testing.TB) *redis.Client` (real replica; skips at Tier 1a), `testredis.Real() bool`, `testredis.RequireReal(t testing.TB)`, `testredis.UID() string`, `testredis.Gateway() string`; `make test` (Tier 1a), `make redis-up`, `make redis-down`, `make test-redis` (Tier 2).

- [ ] **Step 1: Install the C toolchain the race detector needs**

The dev machine has Go 1.27.1 but no C compiler (`-race` needs cgo) and no `make`. Do **not** install Docker here: real Redis runs only on the Tier 2 host. This step needs sudo, so ask your human partner to run it if you can't:

```bash
sudo apt-get update && sudo apt-get install -y gcc libc6-dev make
```

Verify: `gcc --version | head -1 && make --version | head -1`
Expected: a gcc version and GNU Make.

- [ ] **Step 2: Create the module**

The directory is already a git repository with the plan documents committed as the baseline.

```bash
cd /home/hermann/claude/goim
go mod init github.com/Tallinzp/goim
go get github.com/redis/go-redis/v9@v9.23.0 github.com/alicebob/miniredis/v2@v2.39.0
```

Expected: `go.mod` contains `module github.com/Tallinzp/goim` and requires both modules.

- [ ] **Step 3: Write the tests**

```go
package testredis

import (
	"context"
	"strings"
	"testing"
)

func TestNewGivesAWorkingRedis(t *testing.T) {
	if err := New(t).Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

// Tier 2: the real Redis must have a linked replica that accepts DEBUG;
// Revoke's WAIT test needs both.
func TestReplicaIsAttached(t *testing.T) {
	ctx := context.Background()
	New(t)
	replica := NewReplica(t)
	info, err := replica.Info(ctx, "replication").Result()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info, "role:slave") || !strings.Contains(info, "master_link_status:up") {
		t.Fatalf("replica is not linked to the primary:\n%s", info)
	}
	if err := replica.Do(ctx, "DEBUG", "SLEEP", "0").Err(); err != nil {
		t.Fatalf("replica rejects DEBUG (start it with --enable-debug-command yes): %v", err)
	}
}
```

- [ ] **Step 4: Run them to verify they fail**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/testredis/`
Expected: FAIL — build errors `undefined: New` and `undefined: NewReplica`.

- [ ] **Step 5: Write the helper**

```go
// Package testredis gives tests a Redis.
//
// Tier 1a (dev machine, the default): every New starts a fresh in-process
// miniredis. Tier 2 (dedicated host): set GOIM_TEST_REDIS (and
// GOIM_TEST_REDIS_REPLICA) to a real Redis primary with one replica, e.g. the
// one `make redis-up` starts from deploy/compose/redis-test.yml. Tests that
// need real Redis behaviour (replication, WAIT, DEBUG) call RequireReal and
// are skipped at Tier 1a; Plan 1's Execution Notes list what that leaves unverified.
package testredis

import (
	"context"
	"encoding/hex"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Real reports whether tests run against a real Redis (GOIM_TEST_REDIS is set).
func Real() bool { return os.Getenv("GOIM_TEST_REDIS") != "" }

// RequireReal skips t at Tier 1a.
func RequireReal(t testing.TB) {
	t.Helper()
	if !Real() {
		t.Skip("TODO: Tier 2 integration — needs a real Redis primary and replica " +
			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA); see Plan 1 Execution Notes")
	}
}

// New returns a client for the test Redis, closed when the test ends.
func New(t testing.TB) *redis.Client {
	t.Helper()
	if Real() {
		return dial(t, os.Getenv("GOIM_TEST_REDIS"))
	}
	m := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// NewReplica returns a client for the real replica. It skips t at Tier 1a.
func NewReplica(t testing.TB) *redis.Client {
	t.Helper()
	RequireReal(t)
	addr := os.Getenv("GOIM_TEST_REDIS_REPLICA")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}
	return dial(t, addr)
}

func dial(t testing.TB, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("test Redis not reachable at %s (on the Tier 2 host: `make redis-up`): %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// UID returns a uid no other test uses, so tests never need FLUSHALL.
func UID() string { return "u" + randHex() }

// Gateway returns a gateway id no other test uses.
func Gateway() string { return "gw-" + randHex() }

func randHex() string {
	var b [6]byte
	for i := range b {
		b[i] = byte(rand.Uint32())
	}
	return hex.EncodeToString(b[:])
}
```

- [ ] **Step 6: Add the Tier 2 compose file and the Makefile**

`deploy/compose/redis-test.yml` is used only on the Tier 2 host:

```yaml
# Real Redis for the tests that skip at Tier 1a: one primary and one replica (Revoke WAIT needs a replica).
# Tier 2 host only (not the dev machine): `make redis-up`, then `make test-redis`.
services:
  redis-primary:
    image: redis:7.4
    command: ["redis-server", "--port", "6379", "--save", "", "--appendonly", "no"]
    ports: ["127.0.0.1:6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "6379", "ping"]
      interval: 1s
      retries: 30
  redis-replica:
    image: redis:7.4
    command: ["redis-server", "--port", "6380", "--save", "", "--appendonly", "no",
              "--replicaof", "redis-primary", "6379", "--enable-debug-command", "yes"]
    ports: ["127.0.0.1:6380:6380"]
    depends_on:
      redis-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "redis-cli -p 6380 info replication | grep -q master_link_status:up"]
      interval: 1s
      retries: 30
```

`Makefile` (recipe lines start with a tab):

```make
export CGO_ENABLED := 1

COMPOSE_REDIS := docker compose -f deploy/compose/redis-test.yml
TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM_TEST_TIER=2"; exit 1; }

.PHONY: test redis-up redis-down test-redis

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# Tier 2 host only: real Redis primary + replica for the tests that skip at Tier 1a.
redis-up:
	@$(TIER2_GUARD)
	$(COMPOSE_REDIS) up -d --wait

redis-down:
	$(COMPOSE_REDIS) down

test-redis:
	@$(TIER2_GUARD)
	GOIM_TEST_REDIS=127.0.0.1:6379 GOIM_TEST_REDIS_REPLICA=127.0.0.1:6380 go test -race -count=1 ./...
```

- [ ] **Step 7: Run the tests**

Run: `make test` then `CGO_ENABLED=1 go test -count=1 -v ./internal/testredis/ | grep -E '^(--- |ok)'`
Expected: `ok`. `TestNewGivesAWorkingRedis` passes; `TestReplicaIsAttached` is `SKIP` with `TODO: Tier 2 integration`. `make redis-up` without `GOIM_TEST_TIER=2` prints `Tier 2 host only` and fails.

- [ ] **Step 8: Commit**

```bash
go mod tidy
git add go.mod go.sum Makefile deploy/compose/redis-test.yml internal/testredis
git commit -m "build: add Go module and test Redis helper (miniredis at Tier 1a, real Redis at Tier 2)"
```

---

### Task 2: Session entry, validation and ordering

**Files:**
- Create: `internal/session/session.go`
- Test: `internal/session/session_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (package `session`):
  - `type Platform string` with `Mobile`, `Pad`, `Web`, `PC`; `const MaxConnSeq = 1<<53 - 1`
  - `func ValidUID(uid string) error`, `func ValidGateway(gw string) error`, `func SlotField(appid string, p Platform) (string, error)`, `func ValidField(f string) error`
  - `type Entry struct { Gateway string; Epoch, ConnID uint64; DeviceID string; ConnSeq uint64; LoginMs, TokIatMs int64 }`
  - `func (e Entry) Validate() error`, `func (e Entry) Encode() string`, `func (e Entry) OwnerPrefix() string` (`"gw|epoch|conn_id|"`), `func EpochPrefix(gw string, epoch uint64) string` (`"gw|epoch|"`), `func Decode(s string) (Entry, error)`
  - `func ChunkWins(chunk, stored Entry, deadlineMs int64) bool` — Reconcile rule 3 ordering; Plan 2's rule-3 handler calls it.

The ordering in `ChunkWins` comes from spec *Reconcile handling* rule 3. With the same device_id and `login_ms` values within `login_deadline` of each other, the higher `conn_seq` wins. Otherwise the later `login_ms` wins. A tie goes to the stored entry. "Within" is inclusive, matching the login table's `recv_ms ≤ stored login_ms + login_deadline`.

- [ ] **Step 1: Write the failing tests**

```go
package session

import (
	"strings"
	"testing"
)

func validEntry() Entry {
	return Entry{
		Gateway:  "gw-17",
		Epoch:    1<<63 + 5, // above 2^53: must survive as a string
		ConnID:   3<<48 | 42,
		DeviceID: "dev-a",
		ConnSeq:  7,
		LoginMs:  1_760_000_000_123,
		TokIatMs: 1_759_999_000_000,
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	e := validEntry()
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(e.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("round trip: got %+v want %+v", got, e)
	}
	want := "gw-17|9223372036854775813|844424930132010|dev-a|7|1760000000123|1759999000000"
	if e.Encode() != want {
		t.Fatalf("Encode = %q, want %q", e.Encode(), want)
	}
	if !strings.HasPrefix(e.Encode(), e.OwnerPrefix()) || !strings.HasPrefix(e.Encode(), EpochPrefix(e.Gateway, e.Epoch)) {
		t.Fatal("prefixes must match the encoded value")
	}
}

func TestValidateRejectsUnencodable(t *testing.T) {
	cases := map[string]func(*Entry){
		"device_id with pipe": func(e *Entry) { e.DeviceID = "a|b" },
		"empty device_id":     func(e *Entry) { e.DeviceID = "" },
		"long device_id":      func(e *Entry) { e.DeviceID = strings.Repeat("x", 129) },
		"gateway with pipe":   func(e *Entry) { e.Gateway = "gw|1" },
		"gateway with colon":  func(e *Entry) { e.Gateway = "gw:1" },
		"conn_seq above 2^53": func(e *Entry) { e.ConnSeq = MaxConnSeq + 1 },
		"zero login_ms":       func(e *Entry) { e.LoginMs = 0 },
	}
	for name, mut := range cases {
		e := validEntry()
		mut(&e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: Validate accepted %+v", name, e)
		}
	}
}

func TestSlotFieldAndUID(t *testing.T) {
	f, err := SlotField("im", Mobile)
	if err != nil || f != "im:mobile" {
		t.Fatalf("SlotField = %q, %v", f, err)
	}
	if ValidField(f) != nil {
		t.Fatal("ValidField rejected a SlotField result")
	}
	for _, bad := range []string{"*", "im", "im:tv", "i|m:mobile", ":mobile"} {
		if ValidField(bad) == nil {
			t.Errorf("ValidField(%q) accepted", bad)
		}
	}
	if _, err := SlotField("im", "tv"); err == nil {
		t.Fatal("unknown platform accepted")
	}
	for _, bad := range []string{"", "a{b", "a}b", "a b", strings.Repeat("u", 65)} {
		if ValidUID(bad) == nil {
			t.Errorf("ValidUID(%q) accepted", bad)
		}
	}
	if ValidUID("10086") != nil {
		t.Fatal("ValidUID rejected a numeric uid")
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "a|b", "gw|x|1|d|1|1|1", "gw|1|1|d|1|1|1|extra"} {
		if _, err := Decode(bad); err == nil {
			t.Errorf("Decode(%q) accepted", bad)
		}
	}
}

func TestChunkWins(t *testing.T) {
	const dl = 5000
	base := validEntry()
	with := func(dev string, seq uint64, login int64) Entry {
		e := base
		e.DeviceID, e.ConnSeq, e.LoginMs = dev, seq, login
		return e
	}
	cases := []struct {
		name          string
		chunk, stored Entry
		want          bool
	}{
		{"same device, higher seq, inside window", with("d", 8, 1000), with("d", 7, 900), true},
		{"same device, lower seq, inside window", with("d", 6, 1000), with("d", 7, 900), false},
		{"same device, equal seq: stored wins", with("d", 7, 1000), with("d", 7, 900), false},
		{"same device, window edge is inside", with("d", 6, 6000), with("d", 7, 1000), false},
		{"regressed counter, later beyond window, wins", with("d", 1, 6001), with("d", 7, 1000), true},
		{"higher seq but earlier beyond window loses", with("d", 9, 1000), with("d", 7, 6001), false},
		{"other device, later wins", with("x", 1, 1001), with("d", 7, 1000), true},
		{"other device, earlier loses", with("x", 9, 999), with("d", 7, 1000), false},
		{"other device, equal login_ms: stored wins", with("x", 9, 1000), with("d", 7, 1000), false},
	}
	for _, c := range cases {
		if got := ChunkWins(c.chunk, c.stored, dl); got != c.want {
			t.Errorf("%s: ChunkWins = %v, want %v", c.name, got, c.want)
		}
		if c.want && ChunkWins(c.stored, c.chunk, dl) {
			t.Errorf("%s: both entries win against each other", c.name)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(validEntry().Encode())
	f.Add("gw|1|2|d|3|4|5")
	f.Fuzz(func(t *testing.T, s string) {
		e, err := Decode(s)
		if err != nil || e.Validate() != nil {
			return
		}
		if got, err := Decode(e.Encode()); err != nil || got != e {
			t.Fatalf("re-decode of %q: %+v, %v", s, got, err)
		}
	})
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/session/`
Expected: FAIL — build errors such as `undefined: Entry` for the names this task adds.

- [ ] **Step 3: Implement**

```go
// Package session defines the slot ownership record stored in sess:{uid}
// and the ordering rule shared by the login Lua and Reconcile rule 3.
package session

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Platform is the device class part of a slot (userid + appid + platform).
type Platform string

const (
	Mobile Platform = "mobile" // Android and iOS share one slot
	Pad    Platform = "pad"
	Web    Platform = "web"
	PC     Platform = "pc"
)

// MaxConnSeq is the largest conn_seq the Lua scripts compare exactly:
// Lua numbers are doubles, so integers above 2^53 lose precision.
const MaxConnSeq = 1<<53 - 1

const maxDeviceID = 128

var (
	uidRe   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	appidRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	// Pod names are DNS-1123 labels; ':' and '|' can never appear.
	gatewayRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$`)
)

// ValidUID reports whether uid is safe to embed in a hash-tagged key.
func ValidUID(uid string) error {
	if !uidRe.MatchString(uid) {
		return fmt.Errorf("session: invalid uid %q", uid)
	}
	return nil
}

// ValidGateway reports whether gw is a valid gateway_id (pod name).
func ValidGateway(gw string) error {
	if !gatewayRe.MatchString(gw) {
		return fmt.Errorf("session: invalid gateway id %q", gw)
	}
	return nil
}

// SlotField returns the sess:{uid} field name "appid:platform".
func SlotField(appid string, p Platform) (string, error) {
	if !appidRe.MatchString(appid) {
		return "", fmt.Errorf("session: invalid appid %q", appid)
	}
	switch p {
	case Mobile, Pad, Web, PC:
	default:
		return "", fmt.Errorf("session: invalid platform %q", p)
	}
	return appid + ":" + string(p), nil
}

// ValidField reports whether f has the form SlotField produces.
func ValidField(f string) error {
	appid, p, ok := strings.Cut(f, ":")
	if !ok {
		return fmt.Errorf("session: invalid slot field %q", f)
	}
	_, err := SlotField(appid, Platform(p))
	return err
}

// Entry is one slot's owner: gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat.
type Entry struct {
	Gateway  string
	Epoch    uint64
	ConnID   uint64
	DeviceID string
	ConnSeq  uint64
	LoginMs  int64 // the gateway's recv_ms for the Login (R1-O4)
	TokIatMs int64 // the token's iat, in milliseconds
}

// Validate rejects values the '|'-separated encoding or the Lua number
// comparisons cannot represent.
func (e Entry) Validate() error {
	if err := ValidGateway(e.Gateway); err != nil {
		return err
	}
	if e.DeviceID == "" || len(e.DeviceID) > maxDeviceID || strings.Contains(e.DeviceID, "|") {
		return fmt.Errorf("session: invalid device_id %q", e.DeviceID)
	}
	if e.ConnSeq > MaxConnSeq {
		return fmt.Errorf("session: conn_seq %d above %d", e.ConnSeq, uint64(MaxConnSeq))
	}
	if e.LoginMs <= 0 || e.TokIatMs < 0 {
		return errors.New("session: login_ms must be positive and tok_iat non-negative")
	}
	return nil
}

// Encode returns the stored field value. Call Validate first.
func (e Entry) Encode() string {
	return e.OwnerPrefix() + e.DeviceID + "|" +
		strconv.FormatUint(e.ConnSeq, 10) + "|" +
		strconv.FormatInt(e.LoginMs, 10) + "|" +
		strconv.FormatInt(e.TokIatMs, 10)
}

// OwnerPrefix is "gw|epoch|conn_id|": a stored value starting with it
// belongs to exactly this connection (conn_ids are never reused).
func (e Entry) OwnerPrefix() string {
	return EpochPrefix(e.Gateway, e.Epoch) + strconv.FormatUint(e.ConnID, 10) + "|"
}

// EpochPrefix is "gw|epoch|": every value written for that gateway epoch starts with it.
func EpochPrefix(gw string, epoch uint64) string {
	return gw + "|" + strconv.FormatUint(epoch, 10) + "|"
}

// Decode parses a stored field value.
func Decode(s string) (Entry, error) {
	p := strings.Split(s, "|")
	if len(p) != 7 {
		return Entry{}, fmt.Errorf("session: malformed entry %q", s)
	}
	var e Entry
	var err error
	e.Gateway, e.DeviceID = p[0], p[3]
	if e.Epoch, err = strconv.ParseUint(p[1], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: epoch: %w", s, err)
	}
	if e.ConnID, err = strconv.ParseUint(p[2], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: conn_id: %w", s, err)
	}
	if e.ConnSeq, err = strconv.ParseUint(p[4], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: conn_seq: %w", s, err)
	}
	if e.LoginMs, err = strconv.ParseInt(p[5], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: login_ms: %w", s, err)
	}
	if e.TokIatMs, err = strconv.ParseInt(p[6], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: tok_iat: %w", s, err)
	}
	return e, nil
}

// ChunkWins reports whether a Reconcile chunk entry beats the stored entry
// under the login table's ordering (Reconcile rule 3, R2-O4, R3-O4):
//   - same device_id, login_ms within deadlineMs of each other: higher conn_seq wins;
//   - same device_id otherwise, or different device_id: later login_ms wins;
//   - ties go to the stored entry, so two live sessions never close each other.
func ChunkWins(chunk, stored Entry, deadlineMs int64) bool {
	if chunk.DeviceID == stored.DeviceID {
		d := chunk.LoginMs - stored.LoginMs
		if d >= -deadlineMs && d <= deadlineMs {
			return chunk.ConnSeq > stored.ConnSeq
		}
	}
	return chunk.LoginMs > stored.LoginMs
}
```

- [ ] **Step 4: Run the tests and a short fuzz**

Run: `CGO_ENABLED=1 go test -race ./internal/session/ && go test -run='^$' -fuzz=FuzzDecode -fuzztime=10s ./internal/session/`
Expected: `ok`, then the fuzzer reports `PASS` with no failing input.

- [ ] **Step 5: Commit**

```bash
git add internal/session
git commit -m "feat(session): add slot entry format, validation and rule-3 ordering"
```

---

### Task 3: Store core and the login Lua

**Files:**
- Create: `internal/store/store.go`, `internal/store/keys.go`, `internal/store/login.go`, `internal/store/lua/login.lua`
- Test: `internal/store/keys_test.go`, `internal/store/helpers_test.go`, `internal/store/login_test.go`

**Interfaces:**
- Consumes: `session.Entry`, `session.ValidUID`, `session.ValidField`, `session.ValidGateway`, `session.EpochPrefix`, `session.Decode` (Task 2); `testredis.New`, `testredis.UID`, `testredis.Gateway` (Task 1).
- Produces (package `store`):
  - `type Config struct { LoginDeadline, SessTTL, LeaseTTL, WaitTimeout time.Duration }`, `func DefaultConfig() Config` (5s, 30d, 30s, 1s)
  - `type Store`, `func New(rdb redis.UniversalClient, cfg Config) *Store`, `func (s *Store) RedisNowMs(ctx) (int64, error)`
  - `type EpochRef struct { Gateway string; Epoch uint64 }`
  - `type LoginCode string` with `LoginWritten`, `LoginReplaced`, `LoginRegressed`, `LoginKicked`, `LoginStale`, `LoginSuperseded`, `LoginExpired`, `LoginRevoked`
  - `type LoginReq struct { UID, Field string; Entry session.Entry }` (`Entry.LoginMs` = the gateway's `recv_ms`)
  - `type LoginResult struct { Code LoginCode; Prev *session.Entry; Err error }`
  - `func (s *Store) Login(ctx, []LoginReq) []LoginResult` — results in request order
  - `func (s *Store) AddConns(ctx, ref EpochRef, uids []string) error` — the Login follow-up SADD
  - unexported, used by later tasks: `scriptCall`, `runBatch`, `nodeFor`, `mustScript`, `ms`, `sessKey`, `revKey`, `epochsKey`, `sweepClaimTTL`, `EpochRef.{leaseKey,connsKey,sweepKey,member,prefix,validate}`

How Plan 2 uses the codes: `REPLACED` and `REGRESSED` kick `Prev` with `REPLACED` (`REGRESSED` also increments `conn_seq_regressed`); `KICKED` kicks `Prev` with `KICKED_BY_OTHER_DEVICE`; `STALE` returns `Prev.ConnSeq` to the client. `Prev` is the stored entry, which also feeds the dead-epoch cross-check metric.

Keys: per-user keys are tagged `{uid}`; per-epoch keys are `gwlease:<gw>:{<epoch>}`, `gwconns:<gw>:{<epoch>}`, `sweep:<gw>:{<epoch>}` (roadmap choice 1).

- [ ] **Step 1: Write the failing tests**

`internal/store/keys_test.go`:

```go
package store

import (
	"strings"
	"testing"
)

// hashTag mirrors Redis Cluster's rule: the slot is computed from the text
// between the first '{' and the next '}', if that text is non-empty.
func hashTag(key string) string {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return key
	}
	j := strings.IndexByte(key[i+1:], '}')
	if j <= 0 {
		return key
	}
	return key[i+1 : i+1+j]
}

func TestKeysShareSlotsWhereScriptsNeedIt(t *testing.T) {
	if hashTag(sessKey("10086")) != hashTag(revKey("10086")) {
		t.Fatal("sess:{uid} and rev:{uid} must share a slot: the login and Reconcile Lua read both")
	}
	r := EpochRef{Gateway: "gw-17", Epoch: 1<<63 + 1}
	if hashTag(r.leaseKey()) != hashTag(r.connsKey()) || hashTag(r.leaseKey()) != hashTag(r.sweepKey()) {
		t.Fatal("per-epoch keys must share a slot")
	}
	if got, want := r.leaseKey(), "gwlease:gw-17:{9223372036854775809}"; got != want {
		t.Fatalf("leaseKey = %q, want %q", got, want)
	}
	if got, want := r.member(), "gw-17:9223372036854775809"; got != want {
		t.Fatalf("member = %q, want %q", got, want)
	}
}
```

`internal/store/helpers_test.go` (shared by every store test):

```go
package store

import (
	"context"
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const field = "im:mobile"

func newStore(t *testing.T) (*Store, *redis.Client) {
	t.Helper()
	rdb := testredis.New(t)
	return New(rdb, DefaultConfig()), rdb
}

func nowMs(t *testing.T, s *Store) int64 {
	t.Helper()
	n, err := s.RedisNowMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// entry builds a valid entry on gateway gw-1, epoch 1<<60 (above 2^53).
func entry(conn uint64, dev string, seq uint64, loginMs int64) session.Entry {
	return session.Entry{Gateway: "gw-1", Epoch: 1 << 60, ConnID: conn, DeviceID: dev,
		ConnSeq: seq, LoginMs: loginMs, TokIatMs: 1000}
}

func login1(t *testing.T, s *Store, uid string, e session.Entry) LoginResult {
	t.Helper()
	res := s.Login(ctx, []LoginReq{{UID: uid, Field: field, Entry: e}})[0]
	if res.Err != nil {
		t.Fatalf("login: %v", res.Err)
	}
	return res
}

func stored(t *testing.T, rdb *redis.Client, uid string) string {
	t.Helper()
	v, err := rdb.HGet(ctx, sessKey(uid), field).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return v
}
```

`internal/store/login_test.go`:

```go
package store

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
)

// One test per row of the login table, plus the two pre-checks.
func TestLoginTable(t *testing.T) {
	s, rdb := newStore(t)
	dl := ms(DefaultConfig().LoginDeadline)

	type step struct {
		conn    uint64
		dev     string
		seq     uint64
		loginAt int64 // offset from now, ms; the Login's recv_ms
	}
	cases := []struct {
		name       string
		first      *step
		second     step
		want       LoginCode
		wantStored uint64 // conn_id stored afterwards
	}{
		{"empty slot: write", nil, step{2, "d", 1, 0}, LoginWritten, 2},
		{"same device, higher conn_seq: replace", &step{1, "d", 1, -100}, step{2, "d", 2, 0}, LoginReplaced, 2},
		{"same device, lower conn_seq inside window: STALE", &step{1, "d", 5, -100}, step{2, "d", 4, 0}, LoginStale, 1},
		{"same device, equal conn_seq inside window: STALE", &step{1, "d", 5, -100}, step{2, "d", 5, 0}, LoginStale, 1},
		{"same device, lower conn_seq outside window: regression", &step{1, "d", 5, -dl - 2000}, step{2, "d", 1, 0}, LoginRegressed, 2},
		{"other device, later: kick", &step{1, "d", 5, -100}, step{2, "x", 1, 0}, LoginKicked, 2},
		{"other device, earlier: SUPERSEDED", &step{1, "d", 5, 0}, step{2, "x", 9, -100}, LoginSuperseded, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uid := testredis.UID()
			now := nowMs(t, s)
			if c.first != nil {
				f := c.first
				// Write the first entry directly: its login_ms may be older than the deadline allows.
				e := entry(f.conn, f.dev, f.seq, now+f.loginAt)
				if err := rdb.HSet(ctx, sessKey(uid), field, e.Encode()).Err(); err != nil {
					t.Fatal(err)
				}
			}
			n := c.second
			res := login1(t, s, uid, entry(n.conn, n.dev, n.seq, now+n.loginAt))
			if res.Code != c.want {
				t.Fatalf("code = %s, want %s", res.Code, c.want)
			}
			if c.first != nil && (res.Prev == nil || res.Prev.ConnID != c.first.conn) {
				t.Fatalf("Prev = %+v, want conn %d", res.Prev, c.first.conn)
			}
			if c.want == LoginStale && res.Prev.ConnSeq != c.first.seq {
				t.Fatalf("STALE must return the stored conn_seq %d, got %d", c.first.seq, res.Prev.ConnSeq)
			}
			got, err := decodeStored(stored(t, rdb, uid))
			if err != nil || got != c.wantStored {
				t.Fatalf("stored conn = %d (%v), want %d", got, err, c.wantStored)
			}
		})
	}
}

func decodeStored(v string) (uint64, error) {
	if v == "" {
		return 0, nil
	}
	e, err := session.Decode(v)
	return e.ConnID, err
}

func TestLoginExpired(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	late := nowMs(t, s) - ms(DefaultConfig().LoginDeadline) - 500
	if res := login1(t, s, uid, entry(1, "d", 1, late)); res.Code != LoginExpired {
		t.Fatalf("code = %s, want EXPIRED", res.Code)
	}
	if stored(t, rdb, uid) != "" {
		t.Fatal("EXPIRED must not write")
	}
}

func TestLoginRevoked(t *testing.T) {
	s, rdb := newStore(t)
	now := nowMs(t, s)

	t.Run("user-wide marker", func(t *testing.T) {
		uid := testredis.UID()
		rdb.HSet(ctx, revKey(uid), "*", 2000)
		e := entry(1, "d", 1, now) // tok_iat 1000 < 2000
		if res := login1(t, s, uid, e); res.Code != LoginRevoked {
			t.Fatalf("code = %s, want REVOKED", res.Code)
		}
		e.TokIatMs = 2000 // not before the marker
		if res := login1(t, s, uid, e); res.Code != LoginWritten {
			t.Fatalf("code = %s, want WRITTEN", res.Code)
		}
	})
	t.Run("slot marker blocks only its slot", func(t *testing.T) {
		uid := testredis.UID()
		rdb.HSet(ctx, revKey(uid), field, 2000)
		if res := login1(t, s, uid, entry(1, "d", 1, now)); res.Code != LoginRevoked {
			t.Fatalf("code = %s, want REVOKED", res.Code)
		}
		other := s.Login(ctx, []LoginReq{{UID: uid, Field: "im:pc", Entry: entry(2, "d", 1, now)}})[0]
		if other.Err != nil || other.Code != LoginWritten {
			t.Fatalf("other slot: %+v", other)
		}
	})
	t.Run("effective marker is the max of both fields", func(t *testing.T) {
		uid := testredis.UID()
		rdb.HSet(ctx, revKey(uid), "*", 500, field, 2000)
		if res := login1(t, s, uid, entry(1, "d", 1, now)); res.Code != LoginRevoked {
			t.Fatalf("code = %s, want REVOKED", res.Code)
		}
	})
	t.Run("revoked and late reports REVOKED (R4H-OP1)", func(t *testing.T) {
		uid := testredis.UID()
		rdb.HSet(ctx, revKey(uid), "*", 2000)
		late := now - ms(DefaultConfig().LoginDeadline) - 500
		if res := login1(t, s, uid, entry(1, "d", 1, late)); res.Code != LoginRevoked {
			t.Fatalf("code = %s, want REVOKED", res.Code)
		}
	})
}

func TestLoginRefreshesTTLAndRejectsInvalid(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	login1(t, s, uid, entry(1, "d", 1, nowMs(t, s)))
	ttl := rdb.PTTL(ctx, sessKey(uid)).Val()
	if ttl < DefaultConfig().SessTTL-time.Minute {
		t.Fatalf("sess TTL = %v, want ~30d", ttl)
	}
	bad := entry(2, "a|b", 1, nowMs(t, s))
	res := s.Login(ctx, []LoginReq{{UID: uid, Field: field, Entry: bad}, {UID: uid, Field: field, Entry: entry(3, "d", 2, nowMs(t, s))}})
	if res[0].Err == nil {
		t.Fatal("device_id with '|' must be rejected before Redis")
	}
	if res[1].Err != nil || res[1].Code != LoginReplaced {
		t.Fatalf("valid request in the same batch: %+v", res[1])
	}
}

// Review focus: a new master after failover has an empty script cache.
func TestLoginSurvivesScriptFlush(t *testing.T) {
	s, rdb := newStore(t)
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	uids := []string{testredis.UID(), testredis.UID()}
	now := nowMs(t, s)
	res := s.Login(ctx, []LoginReq{
		{UID: uids[0], Field: field, Entry: entry(1, "d", 1, now)},
		{UID: uids[1], Field: field, Entry: entry(2, "d", 1, now)},
	})
	for i, r := range res {
		if r.Err != nil || r.Code != LoginWritten {
			t.Fatalf("login %d after SCRIPT FLUSH: %+v", i, r)
		}
	}
}

func TestAddConns(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
	if err := s.AddConns(ctx, ref, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if n := rdb.SCard(ctx, ref.connsKey()).Val(); n != 2 {
		t.Fatalf("gwconns size = %d, want 2", n)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/`
Expected: FAIL — build errors such as `undefined: sessKey` and `undefined: Store` for the names this task adds.

- [ ] **Step 3: Write the store core**

`internal/store/store.go`:

```go
// Package store is Logic's only access path to Redis: the session schema
// and its Lua scripts. Gateways must never import it (layer rule).
package store

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds the values the scripts depend on.
type Config struct {
	LoginDeadline time.Duration // login_deadline (default 5s)
	SessTTL       time.Duration // sess:{uid} TTL, also used for rev:{uid} (R4M-1); default 30d
	LeaseTTL      time.Duration // gwlease TTL (default 30s)
	WaitTimeout   time.Duration // Revoke's WAIT 1 <timeout> (default 1s)
}

// DefaultConfig returns the spec defaults.
func DefaultConfig() Config {
	return Config{
		LoginDeadline: 5 * time.Second,
		SessTTL:       30 * 24 * time.Hour,
		LeaseTTL:      30 * time.Second,
		WaitTimeout:   time.Second,
	}
}

// sweepClaimTTL is the sweep:gw:{epoch} claim's PX (spec: SET NX PX 300000).
const sweepClaimTTL = 300 * time.Second

// Store runs every session-store operation against a Redis Cluster or,
// in tests, a single Redis node.
type Store struct {
	rdb redis.UniversalClient
	cfg Config
}

// New returns a Store. rdb must be a *redis.ClusterClient or a *redis.Client.
func New(rdb redis.UniversalClient, cfg Config) *Store {
	return &Store{rdb: rdb, cfg: cfg}
}

//go:embed lua/*.lua
var luaFS embed.FS

func mustScript(name string) *redis.Script {
	src, err := luaFS.ReadFile("lua/" + name)
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(src))
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// scriptCall is one script invocation inside a pipelined batch.
type scriptCall struct {
	keys []string
	args []any
}

// runBatch pipelines EVALSHA for every call. Calls that fail with NOSCRIPT
// (a new master after failover has an empty script cache) did not run, so
// they are re-run one by one with Run, which falls back to EVAL.
func (s *Store) runBatch(ctx context.Context, sc *redis.Script, calls []scriptCall) []*redis.Cmd {
	cmds := make([]*redis.Cmd, len(calls))
	if len(calls) == 0 {
		return cmds
	}
	// The pipeline's own error repeats the first failed command; each cmd is inspected below.
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, c := range calls {
			cmds[i] = sc.EvalSha(ctx, p, c.keys, c.args...)
		}
		return nil
	})
	for i, cmd := range cmds {
		if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
			cmds[i] = sc.Run(ctx, s.rdb, calls[i].keys, calls[i].args...)
		}
	}
	return cmds
}

// nodeFor returns the client for the master that owns key, so commands that
// must share one connection (a write followed by WAIT) can use Conn().
func (s *Store) nodeFor(ctx context.Context, key string) (*redis.Client, error) {
	switch c := s.rdb.(type) {
	case *redis.ClusterClient:
		return c.MasterForKey(ctx, key)
	case *redis.Client:
		return c, nil
	default:
		return nil, fmt.Errorf("store: unsupported redis client %T", s.rdb)
	}
}

// RedisNowMs returns Redis TIME in milliseconds.
func (s *Store) RedisNowMs(ctx context.Context) (int64, error) {
	t, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}
```

`internal/store/keys.go`:

```go
package store

import (
	"strconv"

	"github.com/Tallinzp/goim/internal/session"
)

// Redis keys (spec: Redis schema). Per-user keys are hash-tagged {uid};
// per-epoch keys are hash-tagged {epoch}, so one gateway epoch's lease and
// gwconns set live on one shard.

const epochsKey = "gwepochs"

func sessKey(uid string) string { return "sess:{" + uid + "}" }
func revKey(uid string) string  { return "rev:{" + uid + "}" }

// EpochRef names one gateway instance: gateway_id (pod name) plus its boot epoch.
type EpochRef struct {
	Gateway string
	Epoch   uint64
}

func (r EpochRef) validate() error { return session.ValidGateway(r.Gateway) }

func (r EpochRef) suffix() string { return r.Gateway + ":{" + strconv.FormatUint(r.Epoch, 10) + "}" }

func (r EpochRef) leaseKey() string { return "gwlease:" + r.suffix() }
func (r EpochRef) connsKey() string { return "gwconns:" + r.suffix() }
func (r EpochRef) sweepKey() string { return "sweep:" + r.suffix() }

// member is the gwepochs ZSET member "gw:epoch".
func (r EpochRef) member() string { return r.Gateway + ":" + strconv.FormatUint(r.Epoch, 10) }

// prefix is "gw|epoch|", the start of every sess value this epoch wrote.
func (r EpochRef) prefix() string { return session.EpochPrefix(r.Gateway, r.Epoch) }
```

- [ ] **Step 4: Write the login Lua and its wrapper**

`internal/store/lua/login.lua`. Lua 5.1's `gmatch` yields extra empty matches with `[^|]*`, so `split` appends a `|` and matches `([^|]*)|`:

```lua
-- Login Lua (spec: Redis schema / Login Lua). One slot: sess:{uid} and rev:{uid}.
-- KEYS[1] sess:{uid}        KEYS[2] rev:{uid}
-- ARGV[1] field             ARGV[2] new entry value
-- ARGV[3] device_id         ARGV[4] conn_seq
-- ARGV[5] recv_ms           ARGV[6] tok_iat_ms
-- ARGV[7] login_deadline_ms ARGV[8] sess TTL ms
-- Returns {code, stored value or ''}.
-- epoch and conn_id are uint64 and are never converted to Lua numbers (doubles).
local function split(s)
  local out = {}
  for part in string.gmatch(s .. '|', '([^|]*)|') do out[#out + 1] = part end
  return out
end

local field = ARGV[1]

-- 1. Revocation first (R4H-OP1): the effective marker is max(rev[*], rev[field]).
local marks = redis.call('HMGET', KEYS[2], '*', field)
local marker = 0
for i = 1, 2 do
  if marks[i] then
    local m = tonumber(marks[i])
    if m > marker then marker = m end
  end
end
if tonumber(ARGV[6]) < marker then return {'REVOKED', ''} end

-- 2. Deadline (R1-O4): no write later than recv_ms + login_deadline.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local recv = tonumber(ARGV[5])
local deadline = tonumber(ARGV[7])
if now - recv > deadline then return {'EXPIRED', ''} end

-- 3. The login table.
local code = 'WRITTEN'
local stored = redis.call('HGET', KEYS[1], field)
if stored then
  local s = split(stored)
  local s_seq, s_login = tonumber(s[5]), tonumber(s[6])
  if s[4] == ARGV[3] then
    if tonumber(ARGV[4]) > s_seq then
      code = 'REPLACED'
    elseif recv <= s_login + deadline then
      return {'STALE', stored}
    else
      code = 'REGRESSED'
    end
  elseif recv >= s_login then
    code = 'KICKED'
  else
    return {'SUPERSEDED', stored}
  end
end
redis.call('HSET', KEYS[1], field, ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return {code, stored or ''}
```

`internal/store/login.go`:

```go
package store

import (
	"context"
	"fmt"

	"github.com/Tallinzp/goim/internal/session"
)

// LoginCode is the login Lua's decision.
type LoginCode string

const (
	LoginWritten    LoginCode = "WRITTEN"    // slot was empty; no kick
	LoginReplaced   LoginCode = "REPLACED"   // same device, higher conn_seq: kick Prev with REPLACED
	LoginRegressed  LoginCode = "REGRESSED"  // same device, counter regression: kick Prev with REPLACED, count conn_seq_regressed
	LoginKicked     LoginCode = "KICKED"     // other device, not earlier: kick Prev with KICKED_BY_OTHER_DEVICE
	LoginStale      LoginCode = "STALE"      // rejected; Prev.ConnSeq goes back to the client
	LoginSuperseded LoginCode = "SUPERSEDED" // rejected; no write, no kick
	LoginExpired    LoginCode = "EXPIRED"    // rejected; reached Redis after recv_ms + login_deadline
	LoginRevoked    LoginCode = "REVOKED"    // rejected; token iat before the effective marker
)

// LoginReq is one Login. Entry.LoginMs is the gateway's recv_ms.
type LoginReq struct {
	UID   string
	Field string
	Entry session.Entry
}

// LoginResult is the outcome of one LoginReq. Prev is the stored entry the
// script saw (nil if the slot was empty or the result is EXPIRED/REVOKED).
type LoginResult struct {
	Code LoginCode
	Prev *session.Entry
	Err  error
}

var loginScript = mustScript("login.lua")

func (r LoginReq) validate() error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	if err := session.ValidField(r.Field); err != nil {
		return err
	}
	return r.Entry.Validate()
}

// Login runs the login Lua for a batch, pipelined. Results are in request order;
// an invalid request gets Err and is not sent.
func (s *Store) Login(ctx context.Context, reqs []LoginReq) []LoginResult {
	out := make([]LoginResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := r.validate(); err != nil {
			out[i].Err = err
			continue
		}
		e := r.Entry
		calls = append(calls, scriptCall{
			keys: []string{sessKey(r.UID), revKey(r.UID)},
			args: []any{r.Field, e.Encode(), e.DeviceID, e.ConnSeq, e.LoginMs, e.TokIatMs,
				ms(s.cfg.LoginDeadline), ms(s.cfg.SessTTL)},
		})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, loginScript, calls) {
		out[idx[j]] = parseLogin(cmd.Slice())
	}
	return out
}

func parseLogin(vals []any, err error) LoginResult {
	if err != nil {
		return LoginResult{Err: err}
	}
	if len(vals) != 2 {
		return LoginResult{Err: fmt.Errorf("store: login script returned %v", vals)}
	}
	code, _ := vals[0].(string)
	prev, _ := vals[1].(string)
	res := LoginResult{Code: LoginCode(code)}
	if prev != "" {
		e, err := session.Decode(prev)
		if err != nil {
			res.Err = err
			return res
		}
		res.Prev = &e
	}
	return res
}

// AddConns is the Login follow-up SADD of uids to gwconns:gw:{epoch}.
func (s *Store) AddConns(ctx context.Context, ref EpochRef, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if err := ref.validate(); err != nil {
		return err
	}
	members := make([]any, len(uids))
	for i, u := range uids {
		members[i] = u
	}
	return s.rdb.SAdd(ctx, ref.connsKey(), members...).Err()
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/`
Expected: `ok`. `TestLoginTable` has 7 subtests, one per row of the login table. `TestLoginRevoked` has 4. `TestLoginSurvivesScriptFlush` passes after `SCRIPT FLUSH`.

- [ ] **Step 6: Commit**

```bash
git add internal/store
git commit -m "feat(store): add Redis store core and login Lua"
```

---

### Task 4: Logout compare-and-delete

**Files:**
- Create: `internal/store/logout.go`, `internal/store/lua/logout.lua`
- Test: `internal/store/logout_test.go`

**Interfaces:**
- Consumes: `Store`, `runBatch`, `mustScript`, `sessKey`, `scriptCall` (Task 3); `session.Entry.OwnerPrefix`, `session.ValidUID`, `session.ValidField`, `session.ValidGateway` (Task 2); test helpers `newStore`, `nowMs`, `entry`, `login1`, `stored`, `field`, `ctx` (Task 3).
- Produces: `type LogoutReq struct { UID, Field, Gateway string; Epoch, ConnID uint64 }`, `type LogoutResult struct { Deleted bool; Err error }`, `func (s *Store) Logout(ctx, []LogoutReq) []LogoutResult`.

The script matches the stored value against the `gw|epoch|conn_id|` prefix as a string. The trailing `|` stops conn_id `12` from matching a stored `123`.

- [ ] **Step 1: Write the failing test**

```go
package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

func TestLogoutCompareAndDelete(t *testing.T) {
	s, rdb := newStore(t)
	now := nowMs(t, s)

	t.Run("own entry is deleted", func(t *testing.T) {
		uid := testredis.UID()
		e := entry(1<<62, "d", 1, now)
		login1(t, s, uid, e)
		r := s.Logout(ctx, []LogoutReq{{UID: uid, Field: field, Gateway: e.Gateway, Epoch: e.Epoch, ConnID: e.ConnID}})[0]
		if r.Err != nil || !r.Deleted || stored(t, rdb, uid) != "" {
			t.Fatalf("logout of own entry: %+v, stored %q", r, stored(t, rdb, uid))
		}
	})
	t.Run("race 1: delayed logout of an older connection is a no-op", func(t *testing.T) {
		uid := testredis.UID()
		newer := entry(2, "d", 2, now)
		newer.Gateway = "gw-42"
		login1(t, s, uid, newer)
		r := s.Logout(ctx, []LogoutReq{{UID: uid, Field: field, Gateway: "gw-17", Epoch: 1 << 60, ConnID: 1}})[0]
		if r.Err != nil || r.Deleted || stored(t, rdb, uid) == "" {
			t.Fatalf("stale logout deleted the newer session: %+v", r)
		}
	})
	// Review focus: conn_ids differ only below 2^53 precision; a numeric compare would match both.
	t.Run("conn_ids above 2^53 are compared exactly", func(t *testing.T) {
		uid := testredis.UID()
		e := entry(1<<63+1, "d", 1, now)
		login1(t, s, uid, e)
		r := s.Logout(ctx, []LogoutReq{{UID: uid, Field: field, Gateway: e.Gateway, Epoch: e.Epoch, ConnID: 1 << 63}})[0]
		if r.Err != nil || r.Deleted {
			t.Fatalf("logout of conn 2^63 deleted conn 2^63+1: %+v", r)
		}
		// A conn_id that is a string prefix of the stored one must not match either.
		r = s.Logout(ctx, []LogoutReq{{UID: uid, Field: field, Gateway: e.Gateway, Epoch: e.Epoch, ConnID: (1<<63 + 1) / 10}})[0]
		if r.Deleted {
			t.Fatal("prefix conn_id matched")
		}
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run TestLogout`
Expected: FAIL — build error `undefined: LogoutReq`.

- [ ] **Step 3: Implement**

`internal/store/lua/logout.lua`:

```lua
-- Logout Lua: compare-and-delete (spec: Logout Lua).
-- KEYS[1] sess:{uid}; ARGV[1] field; ARGV[2] owner prefix 'gw|epoch|conn_id|'.
-- Returns 1 if the field belonged to this connection and was deleted, else 0.
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur and string.sub(cur, 1, #ARGV[2]) == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return 1
end
return 0
```

`internal/store/logout.go`:

```go
package store

import (
	"context"

	"github.com/Tallinzp/goim/internal/session"
)

// LogoutReq deletes UID's Field only if it still belongs to Gateway|Epoch|ConnID.
type LogoutReq struct {
	UID     string
	Field   string
	Gateway string
	Epoch   uint64
	ConnID  uint64
}

// LogoutResult reports whether the field was deleted (false: a newer session owns it).
type LogoutResult struct {
	Deleted bool
	Err     error
}

var logoutScript = mustScript("logout.lua")

// Logout runs the compare-and-delete for a batch, pipelined, in request order.
func (s *Store) Logout(ctx context.Context, reqs []LogoutReq) []LogoutResult {
	out := make([]LogoutResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := validLogout(r); err != nil {
			out[i].Err = err
			continue
		}
		owner := session.Entry{Gateway: r.Gateway, Epoch: r.Epoch, ConnID: r.ConnID}.OwnerPrefix()
		calls = append(calls, scriptCall{keys: []string{sessKey(r.UID)}, args: []any{r.Field, owner}})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, logoutScript, calls) {
		n, err := cmd.Int()
		out[idx[j]] = LogoutResult{Deleted: n == 1, Err: err}
	}
	return out
}

func validLogout(r LogoutReq) error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	if err := session.ValidField(r.Field); err != nil {
		return err
	}
	return session.ValidGateway(r.Gateway)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add logout compare-and-delete"
```

---

### Task 5: Revoke — confirmed marker, then delete

**Files:**
- Create: `internal/store/revoke.go`, `internal/store/lua/rev_mark.lua`, `internal/store/lua/rev_delete.lua`
- Test: `internal/store/revoke_test.go`

**Interfaces:**
- Consumes: `Store`, `nodeFor`, `mustScript`, `sessKey`, `revKey`, `ms` (Task 3); `session.Decode`, `session.ValidUID`, `session.ValidField` (Task 2); `testredis.New`, `testredis.NewReplica`, `testredis.Real`, `testredis.RequireReal` (Task 1).
- Produces: `const ScopeAll = "*"`, `var ErrNotReplicated error`, `type Revoked struct { Field string; Entry session.Entry }`, `func (s *Store) Revoke(ctx, uid, scope string, beforeMs int64) ([]Revoked, error)`. Plan 2's `Revoke` RPC calls it, maps `ErrNotReplicated` to a retryable error, and kicks every returned entry with `REVOKED`.

Three rules shape this code:

- **Order.** The marker must be confirmed by a replica before any field is deleted, so `Revoke` is one method, not two that a caller could call in the wrong order.
- **One connection.** `WAIT` counts only the writes made on its own connection. `markRevoked` takes the master for `rev:{uid}` (`nodeFor`) and runs both the marker script and `WAIT` on one `Conn()`.
- **Fresh tokens survive.** The delete script removes a field only if its `tok_iat` is below the field's effective marker, which it reads in the same script (roadmap choice 5).

Step 2 lives in the unexported `deleteRevoked`, so the Tier 1a tests can test both Lua scripts on miniredis without `WAIT`. Two tests need a real replica and skip locally: `TestRevokeEndToEnd` and `TestRevokeFailsWhenReplicaDoesNotAck` (`TODO: Tier 2 integration`, *Execution Notes* risk 1).

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"errors"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/testredis"
)

// markScript runs the marker script alone. Revoke's WAIT needs a real replica
// (Tier 2), so the Lua is tested here without it.
func markScript(t *testing.T, s *Store, uid, scope string, beforeMs int64) {
	t.Helper()
	err := revMarkScript.Run(ctx, s.rdb, []string{revKey(uid)}, scope, beforeMs, ms(s.cfg.SessTTL)).Err()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRevokeMarkerScriptIsMaxIdempotentAndRefreshesTTL(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	markScript(t, s, uid, ScopeAll, 5000)
	if ttl := rdb.PTTL(ctx, revKey(uid)).Val(); ttl < DefaultConfig().SessTTL-time.Minute {
		t.Fatalf("rev TTL = %v, want the sess TTL (R4M-1)", ttl)
	}
	rdb.PExpire(ctx, revKey(uid), time.Minute)
	markScript(t, s, uid, ScopeAll, 3000) // older marker: kept at 5000
	if v := rdb.HGet(ctx, revKey(uid), "*").Val(); v != "5000" {
		t.Fatalf("marker = %s, want max(5000, 3000)", v)
	}
	if ttl := rdb.PTTL(ctx, revKey(uid)).Val(); ttl < DefaultConfig().SessTTL-time.Minute {
		t.Fatalf("a rewrite must refresh the rev TTL, got %v", ttl)
	}
	markScript(t, s, uid, ScopeAll, 5000) // same call again: idempotent
	if v := rdb.HGet(ctx, revKey(uid), "*").Val(); v != "5000" {
		t.Fatalf("marker = %s after repeat", v)
	}
}

func TestRevokeDeletesOnlyAffectedFields(t *testing.T) {
	s, rdb := newStore(t)
	now := nowMs(t, s)
	write := func(uid, f string, conn uint64, iat int64) {
		e := entry(conn, "d", 1, now)
		e.TokIatMs = iat
		if err := rdb.HSet(ctx, sessKey(uid), f, e.Encode()).Err(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("scope all deletes every slot with an older token", func(t *testing.T) {
		uid := testredis.UID()
		write(uid, "im:mobile", 1, 1000)
		write(uid, "im:pc", 2, 1000)
		markScript(t, s, uid, ScopeAll, 2000)
		got, err := s.deleteRevoked(ctx, uid, ScopeAll)
		if err != nil || len(got) != 2 {
			t.Fatalf("deleteRevoked = %+v, %v; want 2 deleted", got, err)
		}
		if n := rdb.HLen(ctx, sessKey(uid)).Val(); n != 0 {
			t.Fatalf("%d fields left", n)
		}
	})
	t.Run("scope slot leaves other slots alone", func(t *testing.T) {
		uid := testredis.UID()
		write(uid, "im:mobile", 1, 1000)
		write(uid, "im:pc", 2, 1000)
		markScript(t, s, uid, "im:pc", 2000)
		got, err := s.deleteRevoked(ctx, uid, "im:pc")
		if err != nil || len(got) != 1 || got[0].Field != "im:pc" || got[0].Entry.ConnID != 2 {
			t.Fatalf("deleteRevoked = %+v, %v", got, err)
		}
		if !rdb.HExists(ctx, sessKey(uid), "im:mobile").Val() {
			t.Fatal("slot revoke deleted another slot")
		}
	})
	// Review focus: a login with a fresh token between marker write and delete must survive.
	t.Run("a session with a token newer than the marker is kept", func(t *testing.T) {
		uid := testredis.UID()
		write(uid, "im:mobile", 1, 1000)
		write(uid, "im:pc", 2, 3000)
		markScript(t, s, uid, ScopeAll, 2000)
		got, err := s.deleteRevoked(ctx, uid, ScopeAll)
		if err != nil || len(got) != 1 || got[0].Entry.ConnID != 1 {
			t.Fatalf("deleteRevoked = %+v, %v; want only conn 1", got, err)
		}
		if !rdb.HExists(ctx, sessKey(uid), "im:pc").Val() {
			t.Fatal("fresh session deleted")
		}
	})
	t.Run("invalid scope is rejected before Redis", func(t *testing.T) {
		if _, err := s.Revoke(ctx, testredis.UID(), "im", 1); err == nil {
			t.Fatal("scope without platform accepted")
		}
	})
}

// With no replica to acknowledge (miniredis at Tier 1a always answers WAIT
// with 0), Revoke must fail and delete nothing.
func TestRevokeWithoutReplicaAckDeletesNothing(t *testing.T) {
	if testredis.Real() {
		t.Skip("covered by TestRevokeFailsWhenReplicaDoesNotAck against a real replica")
	}
	s, rdb := newStore(t)
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), field, entry(1, "d", 1, nowMs(t, s)).Encode())
	if _, err := s.Revoke(ctx, uid, ScopeAll, 2000); !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("err = %v, want ErrNotReplicated", err)
	}
	if !rdb.HExists(ctx, sessKey(uid), field).Val() {
		t.Fatal("nothing may be deleted before the marker is confirmed")
	}
}

// Tier 2: the full Revoke path with a real replica confirming the marker.
func TestRevokeEndToEnd(t *testing.T) {
	testredis.RequireReal(t)
	s, rdb := newStore(t)
	uid := testredis.UID()
	e := entry(1, "d", 1, nowMs(t, s))
	rdb.HSet(ctx, sessKey(uid), field, e.Encode())
	got, err := s.Revoke(ctx, uid, ScopeAll, 2000)
	if err != nil || len(got) != 1 || got[0].Entry != e {
		t.Fatalf("Revoke = %+v, %v", got, err)
	}
	if ttl := rdb.PTTL(ctx, revKey(uid)).Val(); ttl < DefaultConfig().SessTTL-time.Minute {
		t.Fatalf("rev TTL = %v", ttl)
	}
	if _, err := s.Revoke(ctx, uid, ScopeAll, 2000); err != nil { // idempotent retry
		t.Fatal(err)
	}
}

// Tier 2. Review focus: WAIT must run on the connection that wrote the marker.
// A WAIT on a fresh pooled connection returns at once with every replica
// counted, so it would report success while the replica has not seen the write.
func TestRevokeFailsWhenReplicaDoesNotAck(t *testing.T) {
	testredis.RequireReal(t)
	rdb := testredis.New(t)
	replica := testredis.NewReplica(t)
	cfg := DefaultConfig()
	cfg.WaitTimeout = 200 * time.Millisecond
	s := New(rdb, cfg)
	uid := testredis.UID()
	e := entry(1, "d", 1, nowMs(t, s))
	if err := rdb.HSet(ctx, sessKey(uid), field, e.Encode()).Err(); err != nil {
		t.Fatal(err)
	}

	slept := make(chan error, 1)
	go func() { slept <- replica.Do(ctx, "DEBUG", "SLEEP", "1").Err() }()
	time.Sleep(100 * time.Millisecond) // the replica is now blocked and cannot ack

	if _, err := s.Revoke(ctx, uid, ScopeAll, 2000); !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("Revoke with a sleeping replica: err = %v, want ErrNotReplicated", err)
	}
	if !rdb.HExists(ctx, sessKey(uid), field).Val() {
		t.Fatal("nothing may be deleted before the marker is confirmed")
	}
	if err := <-slept; err != nil {
		t.Fatalf("DEBUG SLEEP on the replica: %v (replica needs --enable-debug-command yes)", err)
	}
	got, err := s.Revoke(ctx, uid, ScopeAll, 2000) // the account service's retry
	if err != nil || len(got) != 1 {
		t.Fatalf("retry: %+v, %v", got, err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run TestRevoke`
Expected: FAIL — build errors such as `undefined: revMarkScript`, `undefined: ScopeAll` and `s.deleteRevoked undefined`.

- [ ] **Step 3: Implement**

`internal/store/lua/rev_mark.lua`:

```lua
-- Revocation marker write (spec: Revoke step 1). Idempotent: max(stored, new).
-- KEYS[1] rev:{uid}; ARGV[1] '*' or 'appid:platform'; ARGV[2] revoked_before_ms; ARGV[3] TTL ms.
-- The TTL (= sess TTL, R4M-1) is refreshed on every write, even when the stored value is kept.
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if not cur or tonumber(cur) < tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
```

`internal/store/lua/rev_delete.lua`:

```lua
-- Revoke step 2: delete the sess fields in scope whose token predates their effective marker.
-- A field written by a login with a newer token (after the marker) is kept.
-- KEYS[1] sess:{uid}; KEYS[2] rev:{uid}; ARGV[1] '*' (every field) or one 'appid:platform'.
-- Returns {field, value, field, value, ...} of the deleted entries, for the kicks.
local function split(s)
  local out = {}
  for part in string.gmatch(s .. '|', '([^|]*)|') do out[#out + 1] = part end
  return out
end
local function marker(f)
  local m = redis.call('HMGET', KEYS[2], '*', f)
  local best = 0
  for i = 1, 2 do
    if m[i] then
      local v = tonumber(m[i])
      if v > best then best = v end
    end
  end
  return best
end

local fields
if ARGV[1] == '*' then
  fields = redis.call('HKEYS', KEYS[1])
else
  fields = {ARGV[1]}
end
local out = {}
for _, f in ipairs(fields) do
  local v = redis.call('HGET', KEYS[1], f)
  if v and tonumber(split(v)[7]) < marker(f) then
    redis.call('HDEL', KEYS[1], f)
    out[#out + 1] = f
    out[#out + 1] = v
  end
end
return out
```

`internal/store/revoke.go`:

```go
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tallinzp/goim/internal/session"
)

// ScopeAll is the rev:{uid} field for a user-wide revocation.
const ScopeAll = "*"

// ErrNotReplicated means no replica confirmed the marker within WaitTimeout.
// Nothing was deleted; the caller (the account service) retries Revoke.
var ErrNotReplicated = errors.New("store: revocation marker not confirmed by a replica")

// Revoked is one sess field deleted by Revoke; its connection must be kicked with REVOKED.
type Revoked struct {
	Field string
	Entry session.Entry
}

var (
	revMarkScript   = mustScript("rev_mark.lua")
	revDeleteScript = mustScript("rev_delete.lua")
)

// Revoke writes revoked_before_ms = max(stored, beforeMs) for scope (ScopeAll or
// a slot field), waits until one replica confirms it, and only then deletes the
// affected sess fields (spec: Revoke). It is idempotent.
func (s *Store) Revoke(ctx context.Context, uid, scope string, beforeMs int64) ([]Revoked, error) {
	if err := session.ValidUID(uid); err != nil {
		return nil, err
	}
	if scope != ScopeAll {
		if err := session.ValidField(scope); err != nil {
			return nil, err
		}
	}
	if err := s.markRevoked(ctx, uid, scope, beforeMs); err != nil {
		return nil, err
	}
	return s.deleteRevoked(ctx, uid, scope)
}

// deleteRevoked is Revoke step 2: delete the fields in scope whose token
// predates their effective marker. Only Revoke may call it, after the marker
// is confirmed.
func (s *Store) deleteRevoked(ctx context.Context, uid, scope string) ([]Revoked, error) {
	vals, err := revDeleteScript.Run(ctx, s.rdb, []string{sessKey(uid), revKey(uid)}, scope).StringSlice()
	if err != nil {
		return nil, err
	}
	out := make([]Revoked, 0, len(vals)/2)
	for i := 0; i+1 < len(vals); i += 2 {
		e, err := session.Decode(vals[i+1])
		if err != nil {
			return out, err
		}
		out = append(out, Revoked{Field: vals[i], Entry: e})
	}
	return out, nil
}

// markRevoked runs the marker write and WAIT on one connection: WAIT only
// counts the writes made earlier on the same connection, so a pooled WAIT
// on another connection would confirm nothing.
func (s *Store) markRevoked(ctx context.Context, uid, scope string, beforeMs int64) error {
	key := revKey(uid)
	node, err := s.nodeFor(ctx, key)
	if err != nil {
		return err
	}
	conn := node.Conn()
	defer conn.Close()
	if err := revMarkScript.Run(ctx, conn, []string{key}, scope, beforeMs, ms(s.cfg.SessTTL)).Err(); err != nil {
		return fmt.Errorf("store: revocation marker: %w", err)
	}
	n, err := conn.Wait(ctx, 1, s.cfg.WaitTimeout).Result()
	if err != nil {
		return fmt.Errorf("store: WAIT: %w", err)
	}
	if n < 1 {
		return ErrNotReplicated
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/ -run TestRevoke -v`
Expected: `TestRevokeMarkerScriptIsMaxIdempotentAndRefreshesTTL`, `TestRevokeDeletesOnlyAffectedFields` (4 subtests) and `TestRevokeWithoutReplicaAckDeletesNothing` pass. `TestRevokeEndToEnd` and `TestRevokeFailsWhenReplicaDoesNotAck` are `SKIP` with `TODO: Tier 2 integration`.

- [ ] **Step 5: (Tier 2 host only, not part of local execution) Prove the WAIT test catches the wrong connection**

On the Tier 2 host, temporarily replace `conn.Wait(ctx, 1, s.cfg.WaitTimeout)` in `markRevoked` with `s.rdb.Wait(ctx, 1, s.cfg.WaitTimeout)`, then run `GOIM_TEST_TIER=2 GOIM_TEST_REDIS=127.0.0.1:6379 GOIM_TEST_REDIS_REPLICA=127.0.0.1:6380 CGO_ENABLED=1 go test -race -count=1 ./internal/store/ -run TestRevokeFailsWhenReplicaDoesNotAck`.
Expected: FAIL (`want ErrNotReplicated`). Restore `conn.Wait(...)`, rerun, and expect PASS. On the dev machine, skip this step and leave *Execution Notes* risk 1 open.

- [ ] **Step 6: Commit**

```bash
git add internal/store
git commit -m "feat(store): add revoke with replica-confirmed marker"
```

---

### Task 6: Reconcile read, compare-and-set and refresh

**Files:**
- Create: `internal/store/reconcile.go`, `internal/store/lua/reconcile_cas.lua`
- Test: `internal/store/reconcile_test.go`

**Interfaces:**
- Consumes: `Store`, `runBatch`, `mustScript`, `sessKey`, `revKey`, `ms`, `EpochRef`, `ScopeAll` (Tasks 3, 5); `session.Entry`, `session.Decode`, `session.ValidUID`, `session.ValidField` (Task 2).
- Produces:
  - `type SlotRef struct { UID, Field string }`
  - `type SlotState struct { Raw string; Stored *session.Entry; MarkerMs int64; Err error }`
  - `func (s *Store) ReadSlots(ctx, []SlotRef) []SlotState`
  - `type CASCode string` with `CASWritten`, `CASUnchanged`, `CASRefused`, `CASRevoked`
  - `type CASReq struct { UID, Field, ExpectRaw string; Entry session.Entry; SentMs int64 }`, `type CASResult struct { Code CASCode; Err error }`
  - `func (s *Store) CompareAndSet(ctx, []CASReq) []CASResult`
  - `func (s *Store) RefreshSlots(ctx, ref EpochRef, uids []string) error`

How Plan 2's Reconcile handler uses these, per chunk:

1. `ReadSlots` for every entry.
2. Rule 1: `tok_iat < MarkerMs` → `not_owner(REVOKED)`.
3. Rule 2: `Stored` is this exact `gw|epoch|conn_id` → `RefreshSlots`, result `written`.
4. Rule 3: `Stored` is another session whose lease is live → `session.ChunkWins` decides. If the chunk entry wins, `CompareAndSet` with `ExpectRaw = Raw`; otherwise `not_owner`.
5. Rule 4: the field is empty or the stored lease is dead → `CompareAndSet` with `ExpectRaw = Raw`.

After every `CASWritten`, the handler calls `RefreshSlots` to keep `gwconns` up to date.

The compare-and-set checks, in this order:

1. `REVOKED`: the marker check from rule 1, repeated atomically inside the write (roadmap choice 4).
2. `REFUSED`: the deadline.
3. `UNCHANGED`: the field no longer holds the value Logic read.
4. `WRITTEN`.

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/testredis"
)

func TestReadSlots(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	e := entry(9, "d", 1, nowMs(t, s))
	rdb.HSet(ctx, sessKey(uid), field, e.Encode())
	rdb.HSet(ctx, revKey(uid), "*", 300, field, 700)
	empty := testredis.UID()

	got := s.ReadSlots(ctx, []SlotRef{{uid, field}, {empty, field}, {"bad uid", field}})
	if got[0].Err != nil || got[0].Stored == nil || *got[0].Stored != e || got[0].Raw != e.Encode() || got[0].MarkerMs != 700 {
		t.Fatalf("slot 0: %+v", got[0])
	}
	if got[1].Err != nil || got[1].Stored != nil || got[1].Raw != "" || got[1].MarkerMs != 0 {
		t.Fatalf("empty slot: %+v", got[1])
	}
	if got[2].Err == nil {
		t.Fatal("invalid uid accepted")
	}
}

func TestCompareAndSet(t *testing.T) {
	s, rdb := newStore(t)
	cas1 := func(r CASReq) CASCode {
		t.Helper()
		res := s.CompareAndSet(ctx, []CASReq{r})[0]
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		return res.Code
	}

	t.Run("restore into an empty field (rule 4)", func(t *testing.T) {
		uid := testredis.UID()
		now := nowMs(t, s)
		e := entry(1, "d", 1, now)
		if c := cas1(CASReq{UID: uid, Field: field, Entry: e, SentMs: now}); c != CASWritten {
			t.Fatalf("code = %s", c)
		}
		if stored(t, rdb, uid) != e.Encode() {
			t.Fatal("not written")
		}
		if rdb.PTTL(ctx, sessKey(uid)).Val() < DefaultConfig().SessTTL-time.Minute {
			t.Fatal("TTL not refreshed")
		}
	})
	t.Run("overwrite the observed value (rule 3/4)", func(t *testing.T) {
		uid := testredis.UID()
		now := nowMs(t, s)
		old := entry(1, "d", 1, now-100)
		rdb.HSet(ctx, sessKey(uid), field, old.Encode())
		e := entry(2, "d", 2, now)
		if c := cas1(CASReq{UID: uid, Field: field, ExpectRaw: old.Encode(), Entry: e, SentMs: now}); c != CASWritten {
			t.Fatalf("code = %s", c)
		}
	})
	t.Run("field changed since the read: UNCHANGED, no write", func(t *testing.T) {
		uid := testredis.UID()
		now := nowMs(t, s)
		other := entry(3, "x", 1, now)
		rdb.HSet(ctx, sessKey(uid), field, other.Encode())
		if c := cas1(CASReq{UID: uid, Field: field, ExpectRaw: "", Entry: entry(1, "d", 1, now), SentMs: now}); c != CASUnchanged {
			t.Fatalf("code = %s", c)
		}
		if stored(t, rdb, uid) != other.Encode() {
			t.Fatal("UNCHANGED must not write")
		}
	})
	t.Run("past sent_ms + login_deadline: REFUSED, no write", func(t *testing.T) {
		uid := testredis.UID()
		now := nowMs(t, s)
		sent := now - ms(DefaultConfig().LoginDeadline) - 500
		if c := cas1(CASReq{UID: uid, Field: field, Entry: entry(1, "d", 1, now), SentMs: sent}); c != CASRefused {
			t.Fatalf("code = %s", c)
		}
		if stored(t, rdb, uid) != "" {
			t.Fatal("REFUSED must not write")
		}
	})
	// The marker is checked inside the script, so a Revoke that lands after
	// Logic's read cannot be undone by a restore.
	t.Run("token older than the marker: REVOKED, no write", func(t *testing.T) {
		uid := testredis.UID()
		now := nowMs(t, s)
		rdb.HSet(ctx, revKey(uid), field, 5000) // entry's tok_iat is 1000
		if c := cas1(CASReq{UID: uid, Field: field, Entry: entry(1, "d", 1, now), SentMs: now}); c != CASRevoked {
			t.Fatalf("code = %s", c)
		}
		if stored(t, rdb, uid) != "" {
			t.Fatal("REVOKED must not write")
		}
	})
}

func TestRefreshSlots(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), field, entry(1, "d", 1, nowMs(t, s)).Encode())
	rdb.PExpire(ctx, sessKey(uid), time.Minute)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	if err := s.RefreshSlots(ctx, ref, []string{uid}); err != nil {
		t.Fatal(err)
	}
	if rdb.PTTL(ctx, sessKey(uid)).Val() < DefaultConfig().SessTTL-time.Minute {
		t.Fatal("TTL not refreshed")
	}
	if !rdb.SIsMember(ctx, ref.connsKey(), uid).Val() {
		t.Fatal("uid not added to gwconns")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run 'TestReadSlots|TestCompareAndSet|TestRefreshSlots'`
Expected: FAIL — build error `undefined: SlotRef`.

- [ ] **Step 3: Implement**

`internal/store/lua/reconcile_cas.lua`:

```lua
-- Reconcile compare-and-set (spec: Reconcile handling, rules 3 and 4).
-- KEYS[1] sess:{uid}; KEYS[2] rev:{uid}
-- ARGV[1] field; ARGV[2] value Logic observed ('' = absent); ARGV[3] chunk entry value
-- ARGV[4] tok_iat_ms; ARGV[5] sent_ms; ARGV[6] login_deadline_ms; ARGV[7] sess TTL ms
-- Returns REVOKED | REFUSED | UNCHANGED | WRITTEN.
-- Rule 1 is re-checked here: a Revoke that lands between Logic's read and this
-- write (marker confirmed, field deleted) must not be undone by a restore.
local marks = redis.call('HMGET', KEYS[2], '*', ARGV[1])
local marker = 0
for i = 1, 2 do
  if marks[i] then
    local m = tonumber(marks[i])
    if m > marker then marker = m end
  end
end
if tonumber(ARGV[4]) < marker then return 'REVOKED' end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if now - tonumber(ARGV[5]) > tonumber(ARGV[6]) then return 'REFUSED' end

local cur = redis.call('HGET', KEYS[1], ARGV[1]) or ''
if cur ~= ARGV[2] then return 'UNCHANGED' end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[7])
return 'WRITTEN'
```

`internal/store/reconcile.go`:

```go
package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

// SlotRef names one slot of one user.
type SlotRef struct {
	UID   string
	Field string
}

// SlotState is what Logic observes before applying Reconcile rules 1–4.
type SlotState struct {
	Raw      string         // stored value; "" if the field is absent
	Stored   *session.Entry // decoded Raw; nil if absent
	MarkerMs int64          // effective revocation marker: max(rev[*], rev[field]), 0 if none
	Err      error
}

func (r SlotRef) validate() error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	return session.ValidField(r.Field)
}

// ReadSlots reads each slot's field and effective revocation marker, pipelined.
func (s *Store) ReadSlots(ctx context.Context, refs []SlotRef) []SlotState {
	out := make([]SlotState, len(refs))
	gets := make([]*redis.StringCmd, len(refs))
	marks := make([]*redis.SliceCmd, len(refs))
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, r := range refs {
			if out[i].Err = r.validate(); out[i].Err != nil {
				continue
			}
			gets[i] = p.HGet(ctx, sessKey(r.UID), r.Field)
			marks[i] = p.HMGet(ctx, revKey(r.UID), ScopeAll, r.Field)
		}
		return nil
	})
	for i := range refs {
		if out[i].Err != nil {
			continue
		}
		out[i] = readSlot(gets[i], marks[i])
	}
	return out
}

func readSlot(get *redis.StringCmd, mark *redis.SliceCmd) SlotState {
	var st SlotState
	raw, err := get.Result()
	switch {
	case err == redis.Nil:
	case err != nil:
		return SlotState{Err: err}
	default:
		e, err := session.Decode(raw)
		if err != nil {
			return SlotState{Err: err}
		}
		st.Raw, st.Stored = raw, &e
	}
	vals, err := mark.Result()
	if err != nil {
		return SlotState{Err: err}
	}
	for _, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue // field absent counts as 0
		}
		m, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return SlotState{Err: fmt.Errorf("store: revocation marker %q: %w", str, err)}
		}
		st.MarkerMs = max(st.MarkerMs, m)
	}
	return st
}

// CASCode is the compare-and-set script's result.
type CASCode string

const (
	CASWritten   CASCode = "WRITTEN"   // field was still ExpectRaw; Entry written
	CASUnchanged CASCode = "UNCHANGED" // field changed since Logic read it; no write, not re-queued
	CASRefused   CASCode = "REFUSED"   // Redis TIME − sent_ms > login_deadline; no write, re-queued
	CASRevoked   CASCode = "REVOKED"   // Entry's token predates the marker; no write, not_owner(REVOKED)
)

// CASReq writes Entry into UID's Field only if it still holds ExpectRaw ("" = absent).
type CASReq struct {
	UID       string
	Field     string
	ExpectRaw string
	Entry     session.Entry
	SentMs    int64 // the chunk's sent_ms (gateway wall clock)
}

// CASResult is the outcome of one CASReq.
type CASResult struct {
	Code CASCode
	Err  error
}

var casScript = mustScript("reconcile_cas.lua")

// CompareAndSet runs the Reconcile compare-and-set for a batch, pipelined, in request order.
func (s *Store) CompareAndSet(ctx context.Context, reqs []CASReq) []CASResult {
	out := make([]CASResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := (SlotRef{r.UID, r.Field}).validate(); err != nil {
			out[i].Err = err
			continue
		}
		if err := r.Entry.Validate(); err != nil {
			out[i].Err = err
			continue
		}
		calls = append(calls, scriptCall{
			keys: []string{sessKey(r.UID), revKey(r.UID)},
			args: []any{r.Field, r.ExpectRaw, r.Entry.Encode(), r.Entry.TokIatMs, r.SentMs,
				ms(s.cfg.LoginDeadline), ms(s.cfg.SessTTL)},
		})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, casScript, calls) {
		code, err := cmd.Text()
		out[idx[j]] = CASResult{Code: CASCode(code), Err: err}
	}
	return out
}

// RefreshSlots refreshes the sess:{uid} TTL of each uid and ensures each is in
// gwconns:gw:{epoch} (rules 2–4). It is never refused by the deadline (R3-O6).
func (s *Store) RefreshSlots(ctx context.Context, ref EpochRef, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if err := ref.validate(); err != nil {
		return err
	}
	members := make([]any, len(uids))
	for i, u := range uids {
		if err := session.ValidUID(u); err != nil {
			return err
		}
		members[i] = u
	}
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, u := range uids {
			p.PExpire(ctx, sessKey(u), s.cfg.SessTTL)
		}
		p.SAdd(ctx, ref.connsKey(), members...)
		return nil
	})
	return err
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add Reconcile read, compare-and-set and refresh"
```

---

### Task 7: ReconcileEnd delete phase

**Files:**
- Create: `internal/store/reconcile_end.go`, `internal/store/lua/reconcile_end.lua`
- Test: `internal/store/reconcile_end_test.go`

**Interfaces:**
- Consumes: `Store`, `runBatch`, `mustScript`, `sessKey`, `EpochRef.{connsKey,prefix,validate}` (Task 3); `session.Decode`, `session.Entry` (Task 2).
- Produces:
  - `func (s *Store) ScanConns(ctx, ref EpochRef, cursor uint64, count int64) ([]string, uint64, error)` — one SSCAN page; next cursor 0 ends the scan. Plan 2's sweeper uses it too.
  - `func (s *Store) DeleteUntracked(ctx, ref EpochRef, asOfMs int64, uids []string, tracked map[uint64]struct{}) (int, error)` — returns the number of fields deleted.

Plan 2's `ReconcileEnd` handler builds `tracked` once from `tracked_conn_ids`, then pages through `ScanConns` and calls `DeleteUntracked` for each page. It deletes a field only if three things hold: it points at `ref`, its `conn_id` is not tracked, and its `login_ms < as_of_ms`. Go selects the candidates from `HGETALL`. The Lua deletes each candidate only if its value is unchanged, then counts the fields this epoch still holds. Uids with none left are removed from `gwconns`; the code comment explains why that `SREM` need not be atomic.

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestDeleteUntracked(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 9}
	const asOf = 10_000
	mk := func(conn uint64, login int64) session.Entry {
		return session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: conn, DeviceID: "d",
			ConnSeq: 1, LoginMs: login, TokIatMs: 1}
	}
	put := func(uid, f string, e session.Entry) {
		if err := rdb.HSet(ctx, sessKey(uid), f, e.Encode()).Err(); err != nil {
			t.Fatal(err)
		}
		rdb.SAdd(ctx, ref.connsKey(), uid)
	}

	ghost := testredis.UID()   // conn 1: not tracked, logged in before as_of → deleted
	live := testredis.UID()    // conn 2: tracked → kept
	late := testredis.UID()    // conn 3: not tracked, login_ms >= as_of → kept
	otherGw := testredis.UID() // field of another gateway → kept, uid leaves gwconns
	mixed := testredis.UID()   // one ghost slot, one tracked slot of this epoch
	put(ghost, field, mk(1, asOf-1))
	put(live, field, mk(2, asOf-1))
	put(late, field, mk(3, asOf))
	other := mk(4, 1)
	other.Gateway = "gw-other"
	put(otherGw, field, other)
	put(mixed, "im:mobile", mk(5, 1))
	put(mixed, "im:pc", mk(6, 1))

	tracked := map[uint64]struct{}{2: {}, 6: {}}
	n, err := s.DeleteUntracked(ctx, ref, asOf, []string{ghost, live, late, otherGw, mixed}, tracked)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("deleted %d fields, want 2 (ghost, mixed:mobile)", n)
	}
	for uid, want := range map[string]bool{ghost: false, live: true, late: true, otherGw: true} {
		if got := stored(t, rdb, uid) != ""; got != want {
			t.Errorf("uid %s: field present = %v, want %v", uid, got, want)
		}
	}
	if rdb.HExists(ctx, sessKey(mixed), "im:mobile").Val() || !rdb.HExists(ctx, sessKey(mixed), "im:pc").Val() {
		t.Error("mixed: wrong field deleted")
	}
	members := rdb.SMembers(ctx, ref.connsKey()).Val()
	want := map[string]bool{live: true, late: true, mixed: true}
	if len(members) != len(want) {
		t.Fatalf("gwconns = %v, want %v", members, want)
	}
	for _, m := range members {
		if !want[m] {
			t.Fatalf("gwconns kept %s", m)
		}
	}
}

func TestScanConnsPagesThroughTheSet(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 3}
	for i := 0; i < 50; i++ {
		rdb.SAdd(ctx, ref.connsKey(), testredis.UID())
	}
	seen := map[string]bool{}
	var cursor uint64
	for {
		uids, next, err := s.ScanConns(ctx, ref, cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range uids {
			seen[u] = true
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if len(seen) != 50 {
		t.Fatalf("scanned %d uids, want 50", len(seen))
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run 'TestDeleteUntracked|TestScanConns'`
Expected: FAIL — build error `s.DeleteUntracked undefined (type *Store has no field or method DeleteUntracked)`.

- [ ] **Step 3: Implement**

`internal/store/lua/reconcile_end.lua`:

```lua
-- ReconcileEnd delete phase, one uid (spec: Reconcile handling / Delete phase).
-- KEYS[1] sess:{uid}; ARGV[1] epoch prefix 'gw|epoch|'
-- ARGV[2], ARGV[3], ...: field, value pairs Logic selected; each is deleted only if unchanged.
-- Returns {deleted, fields still pointing at this epoch}.
local deleted = 0
for i = 2, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
    redis.call('HDEL', KEYS[1], ARGV[i])
    deleted = deleted + 1
  end
end
local remaining = 0
for _, v in ipairs(redis.call('HVALS', KEYS[1])) do
  if string.sub(v, 1, #ARGV[1]) == ARGV[1] then remaining = remaining + 1 end
end
return {deleted, remaining}
```

`internal/store/reconcile_end.go`:

```go
package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var reconcileEndScript = mustScript("reconcile_end.lua")

// ScanConns returns one SSCAN page of gwconns:gw:{epoch}. A next cursor of 0 ends the scan.
func (s *Store) ScanConns(ctx context.Context, ref EpochRef, cursor uint64, count int64) ([]string, uint64, error) {
	if err := ref.validate(); err != nil {
		return nil, 0, err
	}
	return s.rdb.SScan(ctx, ref.connsKey(), cursor, "", count).Result()
}

// DeleteUntracked is the ReconcileEnd delete phase for one page of uids: it
// compare-and-deletes every field that points at ref whose conn_id is not in
// tracked and whose login_ms < asOfMs, then removes from gwconns the uids with
// no field left for ref. It returns the number of fields deleted.
//
// The SREM is not atomic with the sess check: a Login that writes and SADDs in
// between can lose its gwconns membership. gwconns is only a cleanup hint; rule 2
// of the next pass re-adds the uid.
func (s *Store) DeleteUntracked(ctx context.Context, ref EpochRef, asOfMs int64, uids []string, tracked map[uint64]struct{}) (int, error) {
	if err := ref.validate(); err != nil {
		return 0, err
	}
	prefix := ref.prefix()
	alls := make([]*redis.MapStringStringCmd, len(uids))
	if _, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, u := range uids {
			alls[i] = p.HGetAll(ctx, sessKey(u))
		}
		return nil
	}); err != nil {
		return 0, err
	}

	var calls []scriptCall
	var callUIDs []string
	var gone []any // uids with no field left for this epoch
	for i, u := range uids {
		fields := alls[i].Val()
		args := []any{prefix}
		ours := 0
		for f, v := range fields {
			if !strings.HasPrefix(v, prefix) {
				continue
			}
			ours++
			e, err := session.Decode(v)
			if err != nil {
				return 0, err
			}
			if _, ok := tracked[e.ConnID]; ok || e.LoginMs >= asOfMs {
				continue
			}
			args = append(args, f, v)
		}
		switch {
		case len(args) > 1:
			calls = append(calls, scriptCall{keys: []string{sessKey(u)}, args: args})
			callUIDs = append(callUIDs, u)
		case ours == 0:
			gone = append(gone, u)
		}
	}

	deleted := 0
	for j, cmd := range s.runBatch(ctx, reconcileEndScript, calls) {
		vals, err := cmd.Int64Slice()
		if err != nil {
			return deleted, err
		}
		if len(vals) != 2 {
			return deleted, fmt.Errorf("store: reconcile_end returned %v", vals)
		}
		deleted += int(vals[0])
		if vals[1] == 0 {
			gone = append(gone, callUIDs[j])
		}
	}
	if len(gone) > 0 {
		if err := s.rdb.SRem(ctx, ref.connsKey(), gone...).Err(); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add ReconcileEnd delete phase"
```

---

### Task 8: Gateway lease and epoch index

**Files:**
- Create: `internal/store/lease.go`
- Test: `internal/store/lease_test.go`

**Interfaces:**
- Consumes: `Store`, `nodeFor`, `epochsKey`, `EpochRef.{leaseKey,member,validate}` (Task 3).
- Produces:
  - `type LeaseResult struct { Missing bool; RedisNowMs int64; IndexErr error }`
  - `func (s *Store) RenewLease(ctx, ref EpochRef) (LeaseResult, error)` — Plan 2's `GatewayLease` replies `missing` when `Missing` is true. It exports `clock_skew_ms = RedisNowMs − gw_now_ms`, and it logs and counts `IndexErr` without failing the RPC.
  - `func (s *Store) EndEpoch(ctx, ref EpochRef) error` — `EpochEnded`: DEL the lease key, set the score to 0 (roadmap choice 2).
  - `func (s *Store) LeasesExist(ctx, refs []EpochRef) ([]bool, error)` — backs Logic's ~1s lease cache (rule 3/4, hardened Kick retries) and the sweeper's lease-key check.
  - `type EpochScore struct { EpochRef; ScoreMs int64 }`, `func (s *Store) ListEpochs(ctx) ([]EpochScore, error)`

`SET ... GET` and `TIME` go in one pipeline on the lease key's master, so `RedisNowMs` comes from that shard's clock (R4M-5). The `gwepochs` score is set to that same time. The `ZADD` is a separate command on another slot. If it fails, the lease is still renewed, so the error is returned in `IndexErr` instead of as the call's error (R4M-3).

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

func TestRenewLease(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 3}

	first, err := s.RenewLease(ctx, ref)
	if err != nil || !first.Missing || first.IndexErr != nil {
		t.Fatalf("first renewal of a new epoch: %+v, %v; want missing", first, err)
	}
	second, err := s.RenewLease(ctx, ref)
	if err != nil || second.Missing {
		t.Fatalf("second renewal: %+v, %v; want live", second, err)
	}
	if ttl := rdb.PTTL(ctx, ref.leaseKey()).Val(); ttl <= 0 || ttl > DefaultConfig().LeaseTTL {
		t.Fatalf("lease PTTL = %v, want (0, 30s]", ttl)
	}
	if d := second.RedisNowMs - nowMs(t, s); d > 1000 || d < -1000 {
		t.Fatalf("RedisNowMs off by %dms", d)
	}
	score, err := rdb.ZScore(ctx, epochsKey, ref.member()).Result()
	if err != nil || int64(score) != second.RedisNowMs {
		t.Fatalf("gwepochs score = %v (%v), want %d", score, err, second.RedisNowMs)
	}

	rdb.Del(ctx, ref.leaseKey()) // expired or swept
	third, err := s.RenewLease(ctx, ref)
	if err != nil || !third.Missing {
		t.Fatalf("renewal after the key vanished: %+v, %v; want missing", third, err)
	}
}

func TestEndEpochAndLeasesExist(t *testing.T) {
	s, rdb := newStore(t)
	live := EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	ended := EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	for _, r := range []EpochRef{live, ended} {
		if _, err := s.RenewLease(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EndEpoch(ctx, ended); err != nil {
		t.Fatal(err)
	}
	got, err := s.LeasesExist(ctx, []EpochRef{live, ended})
	if err != nil || !got[0] || got[1] {
		t.Fatalf("LeasesExist = %v, %v; want [true false]", got, err)
	}
	if sc := rdb.ZScore(ctx, epochsKey, ended.member()).Val(); sc != 0 {
		t.Fatalf("ended epoch score = %v, want 0", sc)
	}

	all, err := s.ListEpochs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := map[EpochRef]int64{}
	for _, e := range all {
		found[e.EpochRef] = e.ScoreMs
	}
	if sc, ok := found[ended]; !ok || sc != 0 {
		t.Fatalf("ListEpochs: ended epoch %v, %v", sc, ok)
	}
	if sc, ok := found[live]; !ok || sc <= 0 {
		t.Fatalf("ListEpochs: live epoch %v, %v", sc, ok)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run 'TestRenewLease|TestEndEpoch'`
Expected: FAIL — build error `s.RenewLease undefined`.

- [ ] **Step 3: Implement**

```go
package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// LeaseResult is one GatewayLease renewal.
type LeaseResult struct {
	Missing    bool  // the lease key was absent before this renewal (R3-O3)
	RedisNowMs int64 // TIME on the lease key's shard, for clock_skew_ms (R4M-5)
	// IndexErr is a failed ZADD gwepochs. The lease itself was renewed, and
	// the lease key, not the advisory score, decides liveness (R4M-3).
	IndexErr error
}

// RenewLease runs SET gwlease:gw:{epoch} 1 PX <LeaseTTL> GET plus TIME on the
// lease key's shard, then updates the gwepochs score to that TIME.
func (s *Store) RenewLease(ctx context.Context, ref EpochRef) (LeaseResult, error) {
	if err := ref.validate(); err != nil {
		return LeaseResult{}, err
	}
	node, err := s.nodeFor(ctx, ref.leaseKey())
	if err != nil {
		return LeaseResult{}, err
	}
	var set *redis.StatusCmd
	var now *redis.TimeCmd
	_, _ = node.Pipelined(ctx, func(p redis.Pipeliner) error {
		set = p.SetArgs(ctx, ref.leaseKey(), "1", redis.SetArgs{TTL: s.cfg.LeaseTTL, Get: true})
		now = p.Time(ctx)
		return nil
	})
	var res LeaseResult
	switch err := set.Err(); {
	case err == redis.Nil:
		res.Missing = true
	case err != nil:
		return LeaseResult{}, fmt.Errorf("store: lease SET: %w", err)
	}
	t, err := now.Result()
	if err != nil {
		return LeaseResult{}, fmt.Errorf("store: lease TIME: %w", err)
	}
	res.RedisNowMs = t.UnixMilli()
	res.IndexErr = s.rdb.ZAdd(ctx, epochsKey, redis.Z{Score: float64(res.RedisNowMs), Member: ref.member()}).Err()
	return res, nil
}

// EndEpoch handles EpochEnded: DEL the lease key, so the epoch is invalid at
// once, and set its gwepochs score to 0, which tells the sweeper the epoch
// ended (exempt from the mass-stale guard and older than any threshold).
func (s *Store) EndEpoch(ctx context.Context, ref EpochRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := s.rdb.Del(ctx, ref.leaseKey()).Err(); err != nil {
		return err
	}
	return s.rdb.ZAdd(ctx, epochsKey, redis.Z{Score: 0, Member: ref.member()}).Err()
}

// LeasesExist reports, per ref, whether its lease key exists (the authoritative
// liveness predicate). Logic caches the answers for ~1s.
func (s *Store) LeasesExist(ctx context.Context, refs []EpochRef) ([]bool, error) {
	cmds := make([]*redis.IntCmd, len(refs))
	for _, r := range refs {
		if err := r.validate(); err != nil {
			return nil, err
		}
	}
	if _, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, r := range refs {
			cmds[i] = p.Exists(ctx, r.leaseKey())
		}
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]bool, len(refs))
	for i, c := range cmds {
		out[i] = c.Val() == 1
	}
	return out, nil
}

// EpochScore is one gwepochs member. ScoreMs 0 means the epoch sent EpochEnded.
type EpochScore struct {
	EpochRef
	ScoreMs int64
}

// ListEpochs returns every gwepochs member (a handful: one per gateway epoch).
func (s *Store) ListEpochs(ctx context.Context) ([]EpochScore, error) {
	zs, err := s.rdb.ZRangeWithScores(ctx, epochsKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]EpochScore, 0, len(zs))
	for _, z := range zs {
		m, _ := z.Member.(string)
		i := strings.LastIndexByte(m, ':')
		if i <= 0 {
			return nil, fmt.Errorf("store: malformed gwepochs member %q", m)
		}
		epoch, err := strconv.ParseUint(m[i+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("store: malformed gwepochs member %q: %w", m, err)
		}
		out = append(out, EpochScore{EpochRef: EpochRef{Gateway: m[:i], Epoch: epoch}, ScoreMs: int64(z.Score)})
	}
	return out, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CGO_ENABLED=1 go test -race -count=1 ./internal/store/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add gateway lease renewal and epoch index"
```

---

### Task 9: Sweeper primitives

**Files:**
- Create: `internal/store/sweep.go`, `internal/store/lua/sweep_uid.lua`
- Test: `internal/store/sweep_test.go`

**Interfaces:**
- Consumes: `Store`, `runBatch`, `mustScript`, `sessKey`, `epochsKey`, `sweepClaimTTL`, `EpochRef` (Task 3); `RenewLease` (Task 8); `session.ValidUID`, `session.Entry` (Task 2).
- Produces: `func (s *Store) EpochScoreOf(ctx, ref) (scoreMs int64, ok bool, err error)`, `func (s *Store) ClaimSweep(ctx, ref) (bool, error)`, `func (s *Store) ReleaseSweep(ctx, ref) error`, `func (s *Store) SweepUsers(ctx, ref, uids []string) (int, error)`, `func (s *Store) FinishSweep(ctx, ref) error`.

Plan 2's sweeper loop drives these. For each candidate epoch from `ListEpochs` that passes the mass-stale guard:

1. `ClaimSweep`, and record the claimed score.
2. `LeasesExist`: if the lease key exists, `ReleaseSweep` and skip the epoch.
3. Page through `ScanConns` and call `SweepUsers` on each page.
4. Between pages, check `EpochScoreOf` and `LeasesExist`. If the score is newer than the claimed one or the lease key exists, the epoch renewed: `ReleaseSweep` and stop. Never call `FinishSweep` after an abort.
5. When the scan completes, `FinishSweep`.

- [ ] **Step 1: Write the failing tests**

```go
package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestSweepDeletesOnlyDeadEpochFields(t *testing.T) {
	s, rdb := newStore(t)
	dead := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 1}
	// Same pod name, new epoch (race 4): must survive the sweep of the old epoch.
	alive := EpochRef{Gateway: dead.Gateway, Epoch: 1 << 63}
	mk := func(r EpochRef, conn uint64) string {
		return session.Entry{Gateway: r.Gateway, Epoch: r.Epoch, ConnID: conn, DeviceID: "d",
			ConnSeq: 1, LoginMs: 1, TokIatMs: 1}.Encode()
	}
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), "im:mobile", mk(dead, 1), "im:pc", mk(alive, 2), "im:pad", mk(dead, 3))
	rdb.SAdd(ctx, dead.connsKey(), uid)
	if _, err := s.RenewLease(ctx, dead); err != nil {
		t.Fatal(err)
	}

	ok, err := s.ClaimSweep(ctx, dead)
	if err != nil || !ok {
		t.Fatalf("first claim: %v, %v", ok, err)
	}
	if ok, _ := s.ClaimSweep(ctx, dead); ok {
		t.Fatal("second claim must fail while the first is held")
	}
	n, err := s.SweepUsers(ctx, dead, []string{uid})
	if err != nil || n != 2 {
		t.Fatalf("SweepUsers = %d, %v; want 2", n, err)
	}
	if f := rdb.HKeys(ctx, sessKey(uid)).Val(); len(f) != 1 || f[0] != "im:pc" {
		t.Fatalf("fields left = %v, want [im:pc]", f)
	}
	if err := s.FinishSweep(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, dead.connsKey()).Val() != 0 {
		t.Fatal("gwconns not unlinked")
	}
	if _, ok, _ := s.EpochScoreOf(ctx, dead); ok {
		t.Fatal("gwepochs member not removed")
	}
}

func TestReleaseSweepFreesTheClaim(t *testing.T) {
	s, _ := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 5}
	if ok, _ := s.ClaimSweep(ctx, ref); !ok {
		t.Fatal("claim failed")
	}
	if err := s.ReleaseSweep(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimSweep(ctx, ref); !ok {
		t.Fatal("claim after release failed")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CGO_ENABLED=1 go test -race ./internal/store/ -run 'TestSweep|TestReleaseSweep'`
Expected: FAIL — build error `s.ClaimSweep undefined`.

- [ ] **Step 3: Implement**

`internal/store/lua/sweep_uid.lua`:

```lua
-- Sweeper, one uid: delete every field written by the dead epoch.
-- KEYS[1] sess:{uid}; ARGV[1] epoch prefix 'gw|epoch|'. Returns the number deleted.
local n = 0
local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
  if string.sub(all[i + 1], 1, #ARGV[1]) == ARGV[1] then
    redis.call('HDEL', KEYS[1], all[i])
    n = n + 1
  end
end
return n
```

`internal/store/sweep.go`:

```go
package store

import (
	"context"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var sweepScript = mustScript("sweep_uid.lua")

// EpochScoreOf returns ref's gwepochs score; ok is false if the member is gone.
func (s *Store) EpochScoreOf(ctx context.Context, ref EpochRef) (scoreMs int64, ok bool, err error) {
	sc, err := s.rdb.ZScore(ctx, epochsKey, ref.member()).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return int64(sc), true, nil
}

// ClaimSweep takes sweep:gw:{epoch} with SET NX PX 300000.
func (s *Store) ClaimSweep(ctx context.Context, ref EpochRef) (bool, error) {
	if err := ref.validate(); err != nil {
		return false, err
	}
	return s.rdb.SetNX(ctx, ref.sweepKey(), "1", sweepClaimTTL).Result()
}

// ReleaseSweep deletes the claim, so it cannot block a later legitimate sweep (R3-OP6).
func (s *Store) ReleaseSweep(ctx context.Context, ref EpochRef) error {
	return s.rdb.Del(ctx, ref.sweepKey()).Err()
}

// SweepUsers deletes, for each uid, every sess field that ref wrote. It returns
// the number of fields deleted.
func (s *Store) SweepUsers(ctx context.Context, ref EpochRef, uids []string) (int, error) {
	if err := ref.validate(); err != nil {
		return 0, err
	}
	calls := make([]scriptCall, 0, len(uids))
	for _, u := range uids {
		if err := session.ValidUID(u); err != nil {
			return 0, err
		}
		calls = append(calls, scriptCall{keys: []string{sessKey(u)}, args: []any{ref.prefix()}})
	}
	n := 0
	for _, cmd := range s.runBatch(ctx, sweepScript, calls) {
		d, err := cmd.Int()
		if err != nil {
			return n, err
		}
		n += d
	}
	return n, nil
}

// FinishSweep UNLINKs gwconns:gw:{epoch} (freed off the main thread) and removes
// the gwepochs member. Call it only after a sweep that was not aborted.
func (s *Store) FinishSweep(ctx context.Context, ref EpochRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := s.rdb.Unlink(ctx, ref.connsKey()).Err(); err != nil {
		return err
	}
	return s.rdb.ZRem(ctx, epochsKey, ref.member()).Err()
}
```

- [ ] **Step 4: Run the whole Tier 1a suite**

Run: `make test && gofmt -l . && go vet ./... && CGO_ENABLED=1 go test -count=1 -v ./... | grep -c 'TODO: Tier 2 integration'`
Expected: every package `ok`; `gofmt -l` and `go vet` print nothing; the count is `3` (`TestReplicaIsAttached`, `TestRevokeEndToEnd`, `TestRevokeFailsWhenReplicaDoesNotAck`).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "feat(store): add sweeper primitives"
```

---

## After this plan

`internal/store` is complete and has no callers yet. It is verified on miniredis only; the open risks are in *Execution Notes*, and `make test-redis` on the Tier 2 host closes them. Plan 2 (Logic service) is written next, against these interfaces. Spec tests that need Logic's rule engine are in Plan 2 (Tier 1a, real Redis): "Reconcile rule 3 after a simulated lost write" and "Reconcile rule 3 with a regressed counter". They build on `ReadSlots`, `ChunkWins` and `CompareAndSet` from this plan. The same goes for "the sweeper skipping an epoch whose lease key exists although its score is stale" (R4M-3): Plan 2's sweeper loop makes that decision with `LeasesExist`.
