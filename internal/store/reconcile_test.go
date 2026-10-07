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
