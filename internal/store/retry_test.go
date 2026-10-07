package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

// go-redis re-sends a pipeline whose reply was lost after the write (read
// timeout, EOF). A re-run must report the first run's outcome, not a reject.

func TestLoginRerunAfterLostReplyIsIdempotent(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	now := nowMs(t, s)
	login1(t, s, uid, entry(4, "x", 1, now-100))
	req := entry(5, "d", 1, now)
	if res := login1(t, s, uid, req); res.Code != LoginKicked {
		t.Fatalf("first run: %s, want KICKED", res.Code)
	}
	res := login1(t, s, uid, req) // the same request, re-sent
	if res.Code != LoginAlready {
		t.Fatalf("re-run: %s, want ALREADY", res.Code)
	}
	if stored(t, rdb, uid) != req.Encode() {
		t.Fatal("re-run changed the stored entry")
	}
}

func TestCompareAndSetRerunReportsWritten(t *testing.T) {
	s, rdb := newStore(t)
	uid := testredis.UID()
	now := nowMs(t, s)
	old := entry(1, "d", 1, now-100)
	rdb.HSet(ctx, sessKey(uid), field, old.Encode())
	req := CASReq{UID: uid, Field: field, ExpectRaw: old.Encode(), Entry: entry(2, "d", 2, now), SentMs: now}
	for i, want := range []CASCode{CASWritten, CASWritten} {
		res := s.CompareAndSet(ctx, []CASReq{req})[0]
		if res.Err != nil || res.Code != want {
			t.Fatalf("run %d: %+v, want %s", i, res, want)
		}
	}
}

func TestRenewLeaseRerunKeepsMissing(t *testing.T) {
	s, _ := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 11}
	for i := 0; i < 2; i++ { // the same call (same nonce), re-sent
		res, err := s.renewLease(ctx, ref, 42)
		if err != nil || !res.Missing {
			t.Fatalf("run %d: %+v, %v; want missing both times", i, res, err)
		}
	}
	res, err := s.renewLease(ctx, ref, 43) // the next renewal
	if err != nil || res.Missing {
		t.Fatalf("next renewal: %+v, %v; want live", res, err)
	}
}
