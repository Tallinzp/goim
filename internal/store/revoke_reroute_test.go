package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// failedOverMaster stands in for a master that lost its slots in a failover:
// every command gets reply (e.g. "MOVED ..."). With reply "" it is a node
// that refuses connections.
func failedOverMaster(t *testing.T, reply string) *redis.Client {
	t.Helper()
	addr := "127.0.0.1:1" // nothing listens on port 1: the dial is refused
	if reply != "" {
		m := miniredis.RunT(t)
		m.SetError(reply)
		addr = m.Addr()
	}
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	t.Cleanup(func() { c.Close() })
	return c
}

// rerouteStore resolves every key to old until the slot map has been reloaded
// mapAfter times, and to the key's master on the store's own Redis after that
// (mapAfter < 0: never). It returns the store, its own client and the reload
// counter.
func rerouteStore(t *testing.T, old *redis.Client, mapAfter int32) (*Store, redis.UniversalClient, *atomic.Int32) {
	t.Helper()
	s, rdb := newStore(t)
	var reloads atomic.Int32
	s.reloadSlots = func(context.Context) { reloads.Add(1) }
	s.masterFor = func(_ context.Context, key string) (*redis.Client, error) {
		if mapAfter >= 0 && reloads.Load() >= mapAfter {
			return testredis.MasterOf(t, rdb, key), nil
		}
		return old, nil
	}
	return s, rdb, &reloads
}

// Revoke's final outcome on the store's own node: success against a real
// primary with a replica (Tier 2), ErrNotReplicated on miniredis (Tier 1a).
func wantRevokeOutcome(t *testing.T, err error) {
	t.Helper()
	if testredis.Real() {
		if err != nil {
			t.Fatalf("err = %v, want success on the new master", err)
		}
	} else if !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("err = %v, want ErrNotReplicated from the new master (miniredis WAIT is 0)", err)
	}
}

// PI-6: a MOVED reply from the cached master makes Revoke reload the slot map
// and write and confirm the marker on the new master.
func TestRevokeReroutesAfterMoved(t *testing.T) {
	old := failedOverMaster(t, "MOVED 1234 10.0.0.9:6379")
	s, rdb, reloads := rerouteStore(t, old, 2) // the async reload lands on the 2nd poll
	uid := testredis.UID()

	_, err := s.Revoke(ctx, uid, ScopeAll, 2000)
	wantRevokeOutcome(t, err)
	if v := rdb.HGet(ctx, revKey(uid), ScopeAll).Val(); v != "2000" {
		t.Fatalf("marker on the new master = %q, want 2000", v)
	}
	if n := reloads.Load(); n != 2 {
		t.Fatalf("slot map reloaded %d times, want 2 (poll until it changes)", n)
	}
}

// PI-6: the old master is gone (connection refused): same re-route.
func TestRevokeReroutesAfterConnectionError(t *testing.T) {
	s, rdb, _ := rerouteStore(t, failedOverMaster(t, ""), 1)
	uid := testredis.UID()
	_, err := s.Revoke(ctx, uid, ScopeAll, 2000)
	wantRevokeOutcome(t, err)
	if v := rdb.HGet(ctx, revKey(uid), ScopeAll).Val(); v != "2000" {
		t.Fatalf("marker on the new master = %q, want 2000", v)
	}
}

// PI-6: the slot map never changes within rerouteWait: Revoke returns the
// original MOVED error (retryable by the account service) and deletes nothing.
func TestRevokeGivesUpWhenTheSlotMapDoesNotChange(t *testing.T) {
	s, rdb, reloads := rerouteStore(t, failedOverMaster(t, "MOVED 1234 10.0.0.9:6379"), -1)
	s.rerouteWait = 300 * time.Millisecond
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), field, entry(1, "d", 1, nowMs(t, s)).Encode())

	start := time.Now()
	_, err := s.Revoke(ctx, uid, ScopeAll, 2000)
	if _, moved := redis.IsMovedError(err); !moved {
		t.Fatalf("err = %v, want the MOVED error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("gave up after %v, want about rerouteWait", d)
	}
	if reloads.Load() < 2 {
		t.Fatalf("slot map reloaded %d times, want polling", reloads.Load())
	}
	if !rdb.HExists(ctx, sessKey(uid), field).Val() {
		t.Fatal("nothing may be deleted when the marker was not confirmed")
	}
}

// PI-6: only a cluster client has a slot map, and ErrNotReplicated or a
// script error is final: neither triggers a re-route.
func TestRevokeDoesNotRerouteFinalErrors(t *testing.T) {
	s, _ := newStore(t)
	if got := s.reloadSlots != nil; got != testredis.Cluster() {
		t.Fatalf("slot-map reload set = %v, want %v: only a cluster client has one", got, testredis.Cluster())
	}
	for _, err := range []error{ErrNotReplicated, context.DeadlineExceeded,
		redis.Nil /* a Redis reply */, nil} {
		if staleMaster(err) {
			t.Errorf("staleMaster(%v) = true, want false", err)
		}
	}
	s2, _, reloads := rerouteStore(t, failedOverMaster(t, "ERR Error running script"), 1)
	if _, err := s2.Revoke(ctx, testredis.UID(), ScopeAll, 2000); err == nil || errors.Is(err, ErrNotReplicated) {
		t.Fatalf("err = %v, want the script error", err)
	}
	if reloads.Load() != 0 {
		t.Fatal("a script error must not trigger a re-route")
	}
}
