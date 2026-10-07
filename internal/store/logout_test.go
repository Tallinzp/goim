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
