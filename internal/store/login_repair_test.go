package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

// A stored value the login Lua itself cannot read must not block the slot:
// the login is written over it with no kick (REPAIRED), Err stays nil and
// PrevErr reports the unreadable value.
func TestLoginRepairsUnreadableStoredValue(t *testing.T) {
	for name, bad := range map[string]string{
		"not an entry":       "garbage",
		"too few fields":     "gw-1|1|1|d|1|1",
		"conn_seq not a num": "gw-1|1|1|d|x|1|1000",
		"login_ms not a num": "gw-1|1|1|d|1|x|1000",
	} {
		t.Run(name, func(t *testing.T) {
			s, rdb := newStore(t)
			uid := testredis.UID()
			rdb.HSet(ctx, sessKey(uid), field, bad)
			e := entry(7, "d", 1, nowMs(t, s))

			r := login1(t, s, uid, e)
			if r.Code != LoginRepaired || r.Prev != nil || r.PrevErr == nil {
				t.Fatalf("result = %+v; want REPAIRED with PrevErr set", r)
			}
			if got := rdb.HGet(ctx, sessKey(uid), field).Val(); got != e.Encode() {
				t.Fatalf("stored = %q, want the new entry", got)
			}
		})
	}
}
