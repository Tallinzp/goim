package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rEntry is a chunk entry for uid on e.gw.
func (e *env) rEntry(uid string, connID uint64, dev string, seq uint64, loginMs int64) *pb.ReconcileEntry {
	return &pb.ReconcileEntry{ConnId: connID, Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE,
		DeviceId: dev, ConnSeq: seq, LoginMs: loginMs, TokIatMs: loginMs - 1000}
}

// asEntry is the session.Entry a chunk entry on e.gw stands for.
func (e *env) asEntry(r *pb.ReconcileEntry) session.Entry {
	return session.Entry{Gateway: e.gw.Gateway, Epoch: e.gw.Epoch, ConnID: r.ConnId, DeviceID: r.DeviceId,
		ConnSeq: r.ConnSeq, LoginMs: r.LoginMs, TokIatMs: r.TokIatMs}
}

// put writes a sess field by hand (a lost write, a rollback, or another gateway's login).
func (e *env) put(uid string, ent session.Entry) {
	e.t.Helper()
	if err := e.rdb.HSet(ctx, "sess:{"+uid+"}", field, ent.Encode()).Err(); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get(uid string) string {
	v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result()
	return v
}

func (e *env) reconcile(entries ...*pb.ReconcileEntry) []*pb.ReconcileResult {
	e.t.Helper()
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: redisNow(e.t, e.real), Entries: entries})
	if err != nil {
		e.t.Fatal(err)
	}
	return rep.Results
}

// liveGateway is another gateway epoch with a live lease.
func (e *env) liveGateway() store.EpochRef {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 77}
	if _, err := e.real.RenewLease(ctx, ref); err != nil {
		e.t.Fatal(err)
	}
	return ref
}

func onGateway(ref store.EpochRef, conn uint64, dev string, seq uint64, loginMs int64) session.Entry {
	return session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: conn, DeviceID: dev,
		ConnSeq: seq, LoginMs: loginMs, TokIatMs: loginMs - 1000}
}

func code(r *pb.ReconcileResult) pb.ReconcileCode { return r.Code }

func TestReconcileRule1Revoked(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", field, now).Err(); err != nil { // slot marker
		t.Fatal(err)
	}
	r := e.reconcile(e.rEntry(uid, 1, "d", 1, now-10))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_REVOKED {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != "" {
		t.Fatal("rule 1 wrote")
	}
}

func TestReconcileRule2RefreshesOwnEntry(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	re := e.rEntry(uid, 1, "d", 1, now)
	e.put(uid, e.asEntry(re))
	if err := e.rdb.PExpire(ctx, "sess:{"+uid+"}", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if r := e.reconcile(re)[0]; code(r) != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if ttl := e.rdb.PTTL(ctx, "sess:{"+uid+"}").Val(); ttl < time.Hour {
		t.Fatalf("TTL not refreshed: %v", ttl)
	}
	if uids, _, _ := e.real.ScanConns(ctx, e.gw, 0, 10); len(uids) != 1 || uids[0] != uid {
		t.Fatalf("gwconns %v", uids)
	}
}

func TestReconcileRule3StoredNewerIsNotOwner(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	now := redisNow(t, e.real)
	for _, c := range []struct {
		dev    string
		reason pb.KickReason
	}{{"d1", pb.KickReason_KICK_REASON_REPLACED}, {"d2", pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}} {
		uid := testredis.UID()
		stored := onGateway(other, 9, c.dev, 5, now) // newer: same login_ms window, higher seq / later
		e.put(uid, stored)
		r := e.reconcile(e.rEntry(uid, 1, "d1", 1, now-100))[0]
		if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != c.reason {
			t.Fatalf("dev %s: result %+v", c.dev, r)
		}
		if e.get(uid) != stored.Encode() {
			t.Fatal("stored session overwritten")
		}
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("not_owner must not kick the stored session")
	}
}

// Spec: Reconcile rule 3 after a simulated lost write (R2-O4): restore an
// older field by hand while the newer connection is live; Reconcile keeps the
// newer connection and kicks the older one.
func TestReconcileRule3AfterLostWrite(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	older := onGateway(other, 9, "d2", 1, now-2000) // another device, earlier login
	e.put(uid, older)                               // the rollback
	newer := e.rEntry(uid, 1, "d1", 1, now)
	if r := e.reconcile(newer)[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != e.asEntry(newer).Encode() {
		t.Fatal("newer session not written back")
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Spec: Reconcile rule 3 with a regressed counter (R3-O4): a session that won
// through a counter regression keeps winning after its write is rolled back.
func TestReconcileRule3RegressedCounter(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	deadline := e.cfg.Store.LoginDeadline.Milliseconds()
	highSeqOld := onGateway(other, 9, "d1", 9, now-deadline-1000) // before the reinstall
	e.put(uid, highSeqOld)
	regressed := e.rEntry(uid, 1, "d1", 1, now) // low conn_seq, outside the window
	if r := e.reconcile(regressed)[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != e.asEntry(regressed).Encode() {
		t.Fatal("regressed session not kept")
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_REPLACED}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Spec: equal login_ms on two devices: the stored session wins and no kick
// loop occurs.
func TestReconcileRule3TieKeepsStored(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	stored := onGateway(other, 9, "d2", 1, now)
	e.put(uid, stored)
	r := e.reconcile(e.rEntry(uid, 1, "d1", 1, now))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != stored.Encode() || len(e.kicks.all()) != 0 {
		t.Fatal("tie must keep the stored session and kick nobody")
	}
}

func TestReconcileRule4Restores(t *testing.T) {
	e := newEnv(t)
	now := redisNow(t, e.real)
	empty, deadOwner, unreadable := testredis.UID(), testredis.UID(), testredis.UID()
	dead := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 3} // never renewed
	e.put(deadOwner, onGateway(dead, 9, "d2", 1, now+1000))        // even a "newer" one: its lease is gone
	if err := e.rdb.HSet(ctx, "sess:{"+unreadable+"}", field, "garbage").Err(); err != nil {
		t.Fatal(err)
	}
	entries := []*pb.ReconcileEntry{e.rEntry(empty, 1, "d", 1, now), e.rEntry(deadOwner, 2, "d", 1, now), e.rEntry(unreadable, 3, "d", 1, now)}
	for i, r := range e.reconcile(entries...) {
		if r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
			t.Fatalf("entry %d: %+v", i, r)
		}
		if got := e.get(entries[i].Uid); got != e.asEntry(entries[i]).Encode() {
			t.Fatalf("entry %d not restored: %q", i, got)
		}
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("rule 4 kicks nobody")
	}
	if counter(e.m.Undecodable.WithLabelValues("reconcile")) != 1 {
		t.Fatal("unreadable entry not counted")
	}
}

func TestReconcileRefusedAfterDeadline(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw),
		SentMs:  now - e.cfg.Store.LoginDeadline.Milliseconds() - 1000,
		Entries: []*pb.ReconcileEntry{e.rEntry(uid, 1, "d", 1, now)}})
	if err != nil {
		t.Fatal(err)
	}
	if code(rep.Results[0]) != pb.ReconcileCode_RECONCILE_CODE_REFUSED || e.get(uid) != "" {
		t.Fatalf("result %+v, stored %q", rep.Results[0], e.get(uid))
	}
}

// racingRead changes the slot right after Logic reads it.
type racingRead struct {
	*store.Store
	after func()
}

func (r racingRead) ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState {
	out := r.Store.ReadSlots(ctx, refs)
	r.after()
	return out
}

func TestReconcileConcurrentChange(t *testing.T) {
	uid := testredis.UID()
	var e *env
	var mutate func()
	e = newEnvWith(t, testConfig(), func(s *store.Store) Store { return racingRead{s, func() { mutate() }} })
	now := redisNow(t, e.real)

	// A login lands between the read and the compare-and-set: UNCHANGED, no kick.
	mutate = func() { e.put(uid, onGateway(e.liveGateway(), 9, "d2", 1, now)) }
	if r := e.reconcile(e.rEntry(uid, 1, "d", 1, now))[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_UNCHANGED {
		t.Fatalf("result %+v", r)
	}

	// A Revoke lands between the read and the compare-and-set: NOT_OWNER(REVOKED).
	uid2 := testredis.UID()
	mutate = func() { e.rdb.HSet(ctx, "rev:{"+uid2+"}", "*", now) }
	r := e.reconcile(e.rEntry(uid2, 2, "d", 1, now))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_REVOKED || e.get(uid2) != "" {
		t.Fatalf("result %+v stored %q", r, e.get(uid2))
	}

	// Rule 3 (chunk newer than a live stored session) whose compare-and-set
	// loses to a concurrent change: UNCHANGED, and the stored session is not kicked.
	uid3, other := testredis.UID(), e.liveGateway()
	e.put(uid3, onGateway(other, 9, "d2", 1, now-2000))
	mutate = func() { e.put(uid3, onGateway(other, 10, "d3", 1, now-1000)) }
	if r := e.reconcile(e.rEntry(uid3, 3, "d1", 1, now))[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_UNCHANGED {
		t.Fatalf("rule 3 race: result %+v", r)
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("no kick when the compare-and-set did not write")
	}
}

// Spec (R3-O6): a refused rule-3 write sends no kick.
func TestReconcileRefusedRule3SendsNoKick(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	e.put(uid, onGateway(other, 9, "d2", 1, now-2000))
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw),
		SentMs:  now - e.cfg.Store.LoginDeadline.Milliseconds() - 1000,
		Entries: []*pb.ReconcileEntry{e.rEntry(uid, 1, "d1", 1, now)}})
	if err != nil {
		t.Fatal(err)
	}
	if code(rep.Results[0]) != pb.ReconcileCode_RECONCILE_CODE_REFUSED || len(e.kicks.all()) != 0 {
		t.Fatalf("result %+v kicks %v", rep.Results[0], e.kicks.all())
	}
}

// failingCAS reports every compare-and-set's outcome as unknown.
type failingCAS struct{ *store.Store }

func (f failingCAS) CompareAndSet(_ context.Context, reqs []store.CASReq) []store.CASResult {
	out := make([]store.CASResult, len(reqs))
	for i := range out {
		out[i].Err = errors.New("i/o timeout")
	}
	return out
}

func TestReconcileUnknownOutcomeFailsTheChunk(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingCAS{s} })
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: redisNow(t, e.real),
		Entries: []*pb.ReconcileEntry{e.rEntry(testredis.UID(), 1, "d", 1, redisNow(t, e.real))}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
}

// secondCASFails reports the second compare-and-set's outcome as unknown.
type secondCASFails struct{ *store.Store }

func (f secondCASFails) CompareAndSet(ctx context.Context, reqs []store.CASReq) []store.CASResult {
	out := f.Store.CompareAndSet(ctx, reqs[:1])
	for range reqs[1:] {
		out = append(out, store.CASResult{Err: errors.New("i/o timeout")})
	}
	return out
}

// A rule-3 override that wrote is kicked even when another entry of the chunk
// fails the RPC: the stored session it replaced must not wait a whole cycle.
func TestReconcileKicksWrittenOverrideWhenChunkFails(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return secondCASFails{s} })
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	e.put(uid, onGateway(other, 9, "d2", 1, now-2000))
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: now, Entries: []*pb.ReconcileEntry{
		e.rEntry(uid, 1, "d1", 1, now), e.rEntry(testredis.UID(), 2, "d", 1, now)}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

func TestReconcileRejectsInvalidChunk(t *testing.T) {
	e := newEnv(t)
	bad := e.rEntry("bad uid", 1, "d", 1, redisNow(t, e.real))
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: 1, Entries: []*pb.ReconcileEntry{bad}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err %v", err)
	}
}

// Review Focus 4: an empty chunk gets an empty reply.
func TestReconcileEmptyChunk(t *testing.T) {
	e := newEnv(t)
	if rs := e.reconcile(); len(rs) != 0 {
		t.Fatalf("results %v", rs)
	}
}
