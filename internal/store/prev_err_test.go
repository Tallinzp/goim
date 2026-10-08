package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

// undecodable is a stored value the Lua scripts can still read (device_id,
// conn_seq, login_ms and tok_iat are fine) but session.Decode rejects (the
// epoch is not a number): written by an older version, or by hand.
const undecodable = "gw-1|not-a-number|1|other-dev|1|1|1000"

// PI-3: the login went through, so Err is nil and Code stands; only the
// replaced entry is unusable, and that is reported in PrevErr.
func TestLoginReportsUndecodablePrevSeparately(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), field, undecodable)
	e := entry(7, "d", 1, nowMs(t, s))

	r := login1(t, s, uid, e)
	if r.Err != nil || r.Code != LoginKicked {
		t.Fatalf("result = %+v; want KICKED with Err nil", r)
	}
	if r.Prev != nil || r.PrevErr == nil {
		t.Fatalf("Prev = %v, PrevErr = %v; want nil and the decode error", r.Prev, r.PrevErr)
	}
	if got := rdb.HGet(ctx, sessKey(uid), field).Val(); got != e.Encode() {
		t.Fatalf("stored = %q, want the new entry", got)
	}
}

// PI-3: ReadSlots keeps Raw (usable as ExpectRaw), reports the decode error in
// StoredErr and still reads the marker; Err stays nil.
func TestReadSlotsReportsUndecodableStoredSeparately(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), field, undecodable)
	markScript(t, s, uid, ScopeAll, 500)

	st := s.ReadSlots(ctx, []SlotRef{{UID: uid, Field: field}})[0]
	if st.Err != nil || st.Raw != undecodable || st.Stored != nil || st.StoredErr == nil || st.MarkerMs != 500 {
		t.Fatalf("state = %+v", st)
	}
	e := entry(7, "d", 1, nowMs(t, s))
	cas := s.CompareAndSet(ctx, []CASReq{{UID: uid, Field: field, ExpectRaw: st.Raw, Entry: e, SentMs: nowMs(t, s)}})[0]
	if cas.Err != nil || cas.Code != CASWritten {
		t.Fatalf("CompareAndSet over the undecodable value = %+v; want WRITTEN", cas)
	}
}
