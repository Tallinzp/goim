// Package testredis gives tests a Redis.
//
// Tier 1a (dev machine, the default): every New starts a fresh in-process
// miniredis. With GOIM_TEST_REDIS (and GOIM_TEST_REDIS_REPLICA) set, tests run
// against a real Redis primary with one replica: on the dev machine `make
// test-redis-local` starts two redis-server processes (no Docker); on the Tier
// 2 host `make redis-up` starts the compose one. Tests that need real Redis
// behaviour (replication, WAIT, DEBUG) call RequireReal and are skipped
// without it; Plan 1's Execution Notes list what that leaves unverified.
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
			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA; make test-redis-local); see Plan 1 Execution Notes")
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
		addr = "127.0.0.1:16380"
	}
	return dial(t, addr)
}

func dial(t testing.TB, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("test Redis not reachable at %s (make test-redis-local, or `make redis-up` on the Tier 2 host): %v", addr, err)
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
