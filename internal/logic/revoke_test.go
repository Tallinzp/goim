package logic

import (
	"context"
	"errors"
	"testing"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRevokeRejectsBadArguments(t *testing.T) {
	e := newEnv(t)
	now := redisNow(t, e.real)
	uid := testredis.UID()
	for name, req := range map[string]*pb.RevokeRequest{
		"before_ms zero":          {Uid: uid, All: true, BeforeMs: 0},
		"before_ms 10 min ahead":  {Uid: uid, All: true, BeforeMs: now + 10*60_000},
		"before_ms in seconds×1k": {Uid: uid, All: true, BeforeMs: now * 1000},
		"bad uid":                 {Uid: "a|b", All: true, BeforeMs: now},
		"slot without platform":   {Uid: uid, Appid: "im", BeforeMs: now},
	} {
		if _, err := e.svc.Revoke(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: err %v, want InvalidArgument", name, err)
		}
	}
	if v, _ := e.rdb.Exists(ctx, "rev:{"+uid+"}").Result(); v != 0 {
		t.Fatal("a rejected Revoke wrote a marker")
	}
}

// miniredis answers WAIT with 0 replicas, so the real store reports
// ErrNotReplicated: the RPC is retryable and nothing is deleted or kicked.
func TestRevokeUnconfirmedMarkerIsRetryable(t *testing.T) {
	if testredis.Real() {
		t.Skip("needs a Redis whose WAIT reports no replica (miniredis)")
	}
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d", 1))
	_, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: uid, All: true, BeforeMs: redisNow(t, e.real)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
	if e.get(uid) == "" || len(e.kicks.all()) != 0 {
		t.Fatal("an unconfirmed Revoke deleted or kicked")
	}
	if counter(e.m.Revokes.WithLabelValues("all", "not_replicated")) != 1 {
		t.Fatal("not_replicated not counted")
	}
}

// confirmedRevoke stands in for a store whose replica confirms the marker.
type confirmedRevoke struct {
	*store.Store
	out []store.Revoked
	got []string
}

func (c *confirmedRevoke) Revoke(_ context.Context, uid, scope string, beforeMs int64) ([]store.Revoked, error) {
	c.got = append(c.got, scope)
	return c.out, nil
}

func TestRevokeKicksEveryDeletedConnection(t *testing.T) {
	gw := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1 << 62}
	ent := func(conn uint64) session.Entry {
		return session.Entry{Gateway: gw.Gateway, Epoch: gw.Epoch, ConnID: conn, DeviceID: "d", ConnSeq: 1, LoginMs: 1}
	}
	fake := &confirmedRevoke{out: []store.Revoked{
		{Field: "im:mobile", Entry: ent(1)},
		{Field: "im:pad", Entry: ent(2)},
		{Field: "im:web", EntryErr: errors.New("garbage")},
	}}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { fake.Store = s; return fake })
	now := redisNow(t, e.real)

	rep, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: testredis.UID(), All: true, BeforeMs: now, Reason: "ban"})
	if err != nil || rep.Kicked != 2 {
		t.Fatalf("reply %v err %v", rep, err)
	}
	want := []recordedKick{{gw, 1, pb.KickReason_KICK_REASON_REVOKED}, {gw, 2, pb.KickReason_KICK_REASON_REVOKED}}
	if k := e.kicks.all(); len(k) != 2 || k[0] != want[0] || k[1] != want[1] {
		t.Fatalf("kicks %+v", k)
	}
	if counter(e.m.Undecodable.WithLabelValues("revoke")) != 1 {
		t.Fatal("undecodable deleted entry not counted")
	}

	// Slot scope passes the slot field as the marker scope.
	if _, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: testredis.UID(), Appid: "im",
		Platform: pb.Platform_PLATFORM_PAD, BeforeMs: now}); err != nil {
		t.Fatal(err)
	}
	if fake.got[1] != "im:pad" {
		t.Fatalf("scope %q, want im:pad", fake.got[1])
	}
}

// Tier 2: the full path on a real primary with a replica. Spec (PI-4): a retry
// with the same before_ms leaves the marker unchanged and returns the same
// kicks; a token whose iat ≥ before_ms logs in after the revoke.
func TestRevokeEndToEndOnRealRedis(t *testing.T) {
	testredis.RequireReal(t)
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d", 1))
	before := redisNow(t, e.real)
	req := &pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}
	for i := range 2 {
		rep, err := e.svc.Revoke(ctx, req)
		if err != nil || rep.Kicked != 1 {
			t.Fatalf("attempt %d: reply %v err %v", i, rep, err)
		}
	}
	if e.get(uid) != "" {
		t.Fatal("field not deleted")
	}
	if v, _ := e.rdb.HGet(ctx, "rev:{"+uid+"}", "*").Int64(); v != before {
		t.Fatalf("marker %d, want %d", v, before)
	}
	fresh := e.loginEntry(uid, 2, "d", 2)
	fresh.Token = e.token(uid, before) // iat == before_ms: not revoked
	if r := e.login(c, fresh)[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("fresh token after revoke: %+v", r)
	}
}
