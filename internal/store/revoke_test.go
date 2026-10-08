package store

import (
	"errors"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
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
		got, err := s.deleteRevoked(ctx, uid, ScopeAll, 2000)
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
		got, err := s.deleteRevoked(ctx, uid, "im:pc", 2000)
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
		got, err := s.deleteRevoked(ctx, uid, ScopeAll, 2000)
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
	t.Run("before_ms must be positive", func(t *testing.T) {
		if _, err := s.Revoke(ctx, testredis.UID(), ScopeAll, 0); err == nil {
			t.Fatal("before_ms 0 accepted")
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
	again, err := s.Revoke(ctx, uid, ScopeAll, 2000) // the account service's retry
	if err != nil || len(again) != 1 || again[0].Entry != e {
		t.Fatalf("retry = %+v, %v; want the first attempt's deletion again (PI-2)", again, err)
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

	// Every write so far is acknowledged; only the marker write will not be.
	waitReplicaAcked(t, rdb)

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

// PI-2: a re-run of the delete step (go-redis re-sends after a lost reply, or
// the account service retries Revoke with the same before_ms) returns every
// entry the request deleted, so its kicks are not lost.
func TestRevokeReRunReturnsTheSameDeletions(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	a, b := entry(1, "d", 1, nowMs(t, s)), entry(2, "d", 1, nowMs(t, s))
	rdb.HSet(ctx, sessKey(uid), "im:mobile", a.Encode(), "im:pc", b.Encode())
	markScript(t, s, uid, ScopeAll, 2000)

	first, err := s.deleteRevoked(ctx, uid, ScopeAll, 2000)
	if err != nil || len(first) != 2 {
		t.Fatalf("first run = %+v, %v", first, err)
	}
	again, err := s.deleteRevoked(ctx, uid, ScopeAll, 2000)
	if err != nil || len(again) != 2 || again[0] != first[0] || again[1] != first[1] {
		t.Fatalf("re-run = %+v, %v; want %+v", again, err, first)
	}
	if again[0].Field != "im:mobile" || again[0].Entry != a || again[1].Entry != b {
		t.Fatalf("re-run = %+v; want sorted by field with the deleted entries", again)
	}
	ttl := rdb.PTTL(ctx, revDeletedKey(uid, ScopeAll, 2000)).Val()
	if ttl <= 0 || ttl > revDeletedTTL {
		t.Fatalf("revdel TTL = %v, want (0, %v]", ttl, revDeletedTTL)
	}

	// A different request (another before_ms) does not inherit those kicks.
	markScript(t, s, uid, ScopeAll, 3000)
	if other, err := s.deleteRevoked(ctx, uid, ScopeAll, 3000); err != nil || len(other) != 0 {
		t.Fatalf("another request = %+v, %v; want nothing", other, err)
	}
	// A request that deleted nothing leaves no key behind.
	if rdb.Exists(ctx, revDeletedKey(uid, ScopeAll, 3000)).Val() != 0 {
		t.Fatal("a request that deleted nothing must not write a revdel key")
	}
}

// PI-3: a stored value that Go cannot decode is still deleted and reported,
// with EntryErr set and Err nil; the other fields are unaffected. A value whose
// tok_iat is not a number cannot be a valid session and is deleted too.
func TestRevokeReportsUndecodableEntries(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	good := entry(1, "d", 1, nowMs(t, s))
	rdb.HSet(ctx, sessKey(uid),
		"im:mobile", good.Encode(),
		"im:pc", "gw-1|not-a-number|1|d|1|1|1000", // Lua reads tok_iat; Go rejects the epoch
		"im:pad", "garbage") // no tok_iat at all
	markScript(t, s, uid, ScopeAll, 2000)

	got, err := s.deleteRevoked(ctx, uid, ScopeAll, 2000)
	if err != nil || len(got) != 3 {
		t.Fatalf("deleteRevoked = %+v, %v; want 3 deleted and no error", got, err)
	}
	byField := map[string]Revoked{}
	for _, r := range got {
		byField[r.Field] = r
	}
	if r := byField["im:mobile"]; r.EntryErr != nil || r.Entry != good {
		t.Fatalf("im:mobile = %+v", r)
	}
	for _, f := range []string{"im:pc", "im:pad"} {
		if r := byField[f]; r.EntryErr == nil || r.Entry != (session.Entry{}) {
			t.Fatalf("%s = %+v; want EntryErr set and a zero Entry", f, r)
		}
	}
	if n := rdb.HLen(ctx, sessKey(uid)).Val(); n != 0 {
		t.Fatalf("%d fields left", n)
	}
}
