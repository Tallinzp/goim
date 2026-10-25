// Package testredis gives tests a Redis.
//
// Tier 1a (dev machine, the default): every New starts a fresh in-process
// miniredis. With GOIM_TEST_REDIS (and GOIM_TEST_REDIS_REPLICA) set, tests run
// against a real Redis primary with one replica: on the dev machine `make
// test-redis-local` starts two redis-server processes (no Docker); on the Tier
// 2 host `make redis-up` starts the compose one. With GOIM_TEST_REDIS_CLUSTER
// set (the nodes' addresses, comma-separated) they run against a Redis
// Cluster instead (P5bI-4, Plan 6c1): `make test-redis-cluster-local` starts
// 3 masters and 3 replicas. Tests that need real Redis behaviour
// (replication, WAIT, DEBUG) call RequireReal and are skipped without it;
// Plan 1's Execution Notes list what that leaves unverified.
package testredis

import (
	"context"
	"encoding/hex"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Real reports whether tests run against a real Redis (GOIM_TEST_REDIS or
// GOIM_TEST_REDIS_CLUSTER is set).
func Real() bool { return os.Getenv("GOIM_TEST_REDIS") != "" || Cluster() }

// Cluster reports whether tests run against a Redis Cluster (GOIM_TEST_REDIS_CLUSTER is set).
func Cluster() bool { return os.Getenv("GOIM_TEST_REDIS_CLUSTER") != "" }

// RequireReal skips t at Tier 1a.
func RequireReal(t testing.TB) {
	t.Helper()
	if !Real() {
		t.Skip("TODO: Tier 2 integration — needs a real Redis primary and replica " +
			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA; make test-redis-local), or a Cluster " +
			"(GOIM_TEST_REDIS_CLUSTER; make test-redis-cluster-local); see Plan 1 Execution Notes")
	}
}

// New returns a client for the test Redis, closed when the test ends: a
// *redis.ClusterClient on a Cluster, else a *redis.Client.
func New(t testing.TB) redis.UniversalClient {
	t.Helper()
	if Cluster() {
		return dialCluster(t, strings.Split(os.Getenv("GOIM_TEST_REDIS_CLUSTER"), ","))
	}
	if Real() {
		return dial(t, os.Getenv("GOIM_TEST_REDIS"))
	}
	m := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// MasterOf returns a client for the master that owns key: rdb itself on a
// single Redis, the owning node's client on a Cluster (not closed by the
// test; the cluster client owns it).
func MasterOf(t testing.TB, rdb redis.UniversalClient, key string) *redis.Client {
	t.Helper()
	switch c := rdb.(type) {
	case *redis.Client:
		return c
	case *redis.ClusterClient:
		m, err := c.MasterForKey(context.Background(), key)
		if err != nil {
			t.Fatalf("master for %s: %v", key, err)
		}
		return m
	default:
		t.Fatalf("testredis: unsupported client %T", rdb)
		return nil
	}
}

// ReplicaOf returns a client for the replica of key's master: the one
// replica (GOIM_TEST_REDIS_REPLICA) on a single Redis, on a Cluster the
// first replica CLUSTER REPLICAS names. It skips t at Tier 1a.
func ReplicaOf(t testing.TB, rdb redis.UniversalClient, key string) *redis.Client {
	t.Helper()
	RequireReal(t)
	if !Cluster() {
		addr := os.Getenv("GOIM_TEST_REDIS_REPLICA")
		if addr == "" {
			addr = "127.0.0.1:16380"
		}
		return dial(t, addr)
	}
	m := MasterOf(t, rdb, key)
	ctx := context.Background()
	id, err := m.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatalf("CLUSTER MYID on %s: %v", m.Options().Addr, err)
	}
	lines, err := m.Do(ctx, "CLUSTER", "REPLICAS", id).StringSlice()
	if err != nil || len(lines) == 0 {
		t.Fatalf("CLUSTER REPLICAS of %s: %v %v (want at least one replica)", m.Options().Addr, lines, err)
	}
	// "<id> <ip:port@cport> slave <master-id> ...": the address before the '@'.
	f := strings.Fields(lines[0])
	if len(f) < 2 {
		t.Fatalf("CLUSTER REPLICAS line %q", lines[0])
	}
	addr, _, _ := strings.Cut(f[1], "@")
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

func dialCluster(t testing.TB, addrs []string) *redis.ClusterClient {
	t.Helper()
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.ForEachShard(ctx, func(ctx context.Context, n *redis.Client) error { return n.Ping(ctx).Err() }); err != nil {
		t.Fatalf("test Redis Cluster not reachable at %v (make test-redis-cluster-local): %v", addrs, err)
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
