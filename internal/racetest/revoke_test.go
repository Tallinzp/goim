package racetest

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Revoke calls Logic's Revocation service, as the account service does.
func (s *Stack) Revoke(req *pb.RevokeRequest) (*pb.RevokeReply, error) {
	s.t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+s.Logic.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	defer cc.Close()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return pb.NewRevocationClient(cc).Revoke(cctx, req)
}

// revokeAll revokes every slot of uid issued before now; it fails t on error.
func (s *Stack) revokeAll(uid string) int64 {
	s.t.Helper()
	before := s.Redis.Now()
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		s.t.Fatal(err)
	}
	return before
}

// pcReq is a LoginReq for uid's im:pc slot.
func (s *Stack) pcReq(uid, device string) *clientpb.LoginReq {
	req := s.Req(uid, device, 1)
	req.Platform = clientpb.Platform_PLATFORM_PC
	req.Token = s.TokenWith(s.jwtKey, uid, "pc", time.Now().UnixMilli(), time.Hour)
	return req
}

func (s *Stack) marker(uid, field string) int64 {
	s.t.Helper()
	v, err := s.Redis.Client().HGet(ctx, "rev:{"+uid+"}", field).Result()
	if err != nil {
		s.t.Fatalf("rev:{%s} %s: %v", uid, field, err)
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// Revoke closes the user's connections on two slots and deletes both fields.
func TestRevokeClosesConnectionsOnTwoSlots(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv1"
	m := s.Online(0, uid, "d1", 1)
	pc := s.OnlineWith(1, s.pcReq(uid, "d2"))
	s.revokeAll(uid)
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	pc.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if _, ok := s.Redis.EntryOf(uid, "im:pc"); ok || s.Redis.has(uid) {
		t.Fatal("a revoked field is still stored")
	}
}

// A Revoke whose Kick is lost: the next Reconcile pass closes the connection
// through rule 1 (not_owner REVOKED) and does not restore the session.
func TestRevokeWithADroppedKickIsCaughtByReconcile(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv2"
	c := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.revokeAll(uid)
	eventually(t, "Logic gave up on the Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	c.Quiet(500 * time.Millisecond)
	s.Reconcile(0)
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 10*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("Reconcile restored a revoked session")
	}
}

// A Revoke whose WAIT fails returns a retryable error and changes nothing a
// client sees; the retry with the same before_ms succeeds (R2-O4).
func TestRevokeWhoseWAITFailsIsRetried(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv3"
	c := s.Online(0, uid, "d1", 1)
	before := s.Redis.Now()
	s.Redis.ReplicaDownFor(2 * time.Second)
	_, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Revoke with no replica ack: %v, want UNAVAILABLE", err)
	}
	c.Quiet(500 * time.Millisecond)
	if !s.Redis.has(uid) {
		t.Fatal("an unconfirmed Revoke deleted the session")
	}
	time.Sleep(2 * time.Second)
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if m := s.marker(uid, "*"); m != before {
		t.Fatalf("marker %d, want %d", m, before)
	}
}

// PI-4: a retried Revoke with the same before_ms leaves the marker as it
// is; a token issued at or after before_ms logs in; a before_ms 10 minutes
// ahead of Redis TIME is rejected.
func TestRevokeBeforeMs(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv4"
	s.Online(0, uid, "d1", 1)
	before := s.revokeAll(uid)
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}); err != nil {
		t.Fatal(err)
	}
	if m := s.marker(uid, "*"); m != before {
		t.Fatalf("marker %d after the retry, want %d", m, before)
	}
	req := s.Req(uid, "d1", 2)
	req.Token = s.TokenAt(uid, before, time.Hour)
	s.OnlineWith(1, req)
	_, err := s.Revoke(&pb.RevokeRequest{Uid: uid, All: true, BeforeMs: s.Redis.Now() + 10*60_000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("before_ms 10m ahead: %v, want INVALID_ARGUMENT", err)
	}
}

// Revocation outlives the token (R4M-1): the token lives 2s, shorter than
// the 9s outage. gw-0 is cut off from Logic; its connection is revoked with
// the Kick dropped. When gw-0 is back, its triggered pass closes the
// connection with REVOKED and does not restore it.
func TestRevocationOutlivesTheToken(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv5"
	req := s.Req(uid, "d1", 1)
	req.Token = s.TokenAt(uid, time.Now().UnixMilli(), 2*time.Second)
	c := s.OnlineWith(0, req)
	s.Fault(0, fault.Rule{Op: gateway.FaultLogic, Fail: true, For: dur(9 * time.Second)})
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.revokeAll(uid)
	c.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 20*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the pass restored the revoked session")
	}
}

// Slot-scoped Revoke (R3-OP2): with its Kick dropped, the next Reconcile
// closes the connection through not_owner (REVOKED) and does not restore
// it; the user's other slot is unaffected.
func TestSlotScopedRevoke(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv6"
	m := s.Online(0, uid, "d1", 1)
	pc := s.OnlineWith(1, s.pcReq(uid, "d2"))
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	if _, err := s.Revoke(&pb.RevokeRequest{Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, BeforeMs: s.Redis.Now()}); err != nil {
		t.Fatal(err)
	}
	if s.Redis.has(uid) {
		t.Fatal("the mobile field is still stored")
	}
	s.Reconcile(0)
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 10*time.Second)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("Reconcile restored the revoked slot")
	}
	pc.Quiet(500 * time.Millisecond)
	if _, ok := s.Redis.EntryOf(uid, "im:pc"); !ok {
		t.Fatal("the pc slot was touched")
	}
}

// A slot Revoke whose WAIT fails returns an error and sends no Kick; the
// retry is idempotent.
func TestSlotRevokeWhoseWAITFailsSendsNoKick(t *testing.T) {
	s := New(t, Options{})
	const uid = "rv7"
	m := s.Online(0, uid, "d1", 1)
	req := &pb.RevokeRequest{Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, BeforeMs: s.Redis.Now()}
	s.Redis.ReplicaDownFor(2 * time.Second)
	if _, err := s.Revoke(req); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want UNAVAILABLE", err)
	}
	m.Quiet(500 * time.Millisecond)
	if s.logicMetric(`goim_logic_revocation_kicks_total`) != 0 || !s.Redis.has(uid) {
		t.Fatal("the failed Revoke kicked or deleted")
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < 2; i++ {
		if _, err := s.Revoke(req); err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
	}
	m.ExpectKick(clientpb.KickReason_KICK_REASON_REVOKED, 5*time.Second)
	if m := s.marker(uid, "im:mobile"); m != req.BeforeMs {
		t.Fatalf("slot marker %d, want %d", m, req.BeforeMs)
	}
}
