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
