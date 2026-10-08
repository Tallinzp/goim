package logic

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

// phaseFixture stores, for n uids on e.gw: a ghost (untracked, before as_of)
// for even i, a tracked connection for odd i, and every 5th uid also holds a
// login received at as_of. It returns the uids and the request.
func phaseFixture(t *testing.T, e *env, uids []string) *pb.ReconcileEndRequest {
	t.Helper()
	asOf := redisNow(t, e.real)
	req := &pb.ReconcileEndRequest{Gw: pbRef(e.gw), AsOfMs: asOf, Pace: pb.Pace_PACE_PERIODIC}
	for i, uid := range uids {
		ent := session.Entry{Gateway: e.gw.Gateway, Epoch: e.gw.Epoch, ConnID: uint64(1000 + i),
			DeviceID: "d", ConnSeq: 1, LoginMs: asOf - 10, TokIatMs: 1}
		if i%5 == 0 {
			ent.LoginMs = asOf // in flight at as_of: kept
		}
		if i%2 == 1 {
			req.TrackedConnIds = append(req.TrackedConnIds, ent.ConnID)
		}
		e.put(uid, ent)
	}
	if err := e.real.AddConns(ctx, e.gw, uids); err != nil {
		t.Fatal(err)
	}
	return req
}

// dump returns, per uid, whether its field is still there and whether the
// uid is still in gwconns: the delete phase's outcome, comparable across envs.
func dump(t *testing.T, e *env, uids []string) []string {
	t.Helper()
	out := make([]string, len(uids))
	for i, uid := range uids {
		present := e.get(uid) != ""
		in, err := e.rdb.SIsMember(ctx, "gwconns:"+e.gw.Gateway+fmt.Sprintf(":{%d}", e.gw.Epoch), uid).Result()
		if err != nil {
			t.Fatal(err)
		}
		out[i] = fmt.Sprintf("present=%v in=%v", present, in)
	}
	return out
}

func uidList(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = testredis.UID()
	}
	return out
}

// stableScan pages through a snapshot of gwconns taken at cursor 0. On
// miniredis an SSCAN cursor is an index into the sorted members, so the SREMs
// the delete phase makes mid-scan shift it and skip members. Real Redis
// guarantees that a full scan returns every member present throughout, which
// is what the handler relies on; this wrapper restores that guarantee.
type stableScan struct {
	Store
	snap []string
}

func (s *stableScan) ScanConns(ctx context.Context, ref store.EpochRef, cursor uint64, count int64) ([]string, uint64, error) {
	if cursor == 0 {
		s.snap = nil
		for c := uint64(0); ; {
			page, next, err := s.Store.ScanConns(ctx, ref, c, 1000)
			if err != nil {
				return nil, 0, err
			}
			s.snap = append(s.snap, page...)
			if c = next; c == 0 {
				break
			}
		}
	}
	end := min(int(cursor)+int(count), len(s.snap))
	next := uint64(end)
	if end == len(s.snap) {
		next = 0
	}
	return s.snap[cursor:end], next, nil
}

func TestReconcileEndDeletesOnlyUntrackedGhosts(t *testing.T) {
	e := newEnv(t)
	uids := uidList(10)
	other := testredis.UID() // a field of another epoch on a uid in this set
	otherGw := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	req := phaseFixture(t, e, uids)
	if err := e.rdb.HSet(ctx, "sess:{"+other+"}", field,
		session.Entry{Gateway: otherGw.Gateway, Epoch: 2, ConnID: 5, DeviceID: "d", ConnSeq: 1, LoginMs: 1, TokIatMs: 1}.Encode()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := e.real.AddConns(ctx, e.gw, []string{other}); err != nil {
		t.Fatal(err)
	}
	rep, err := e.svc.ReconcileEnd(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// Ghosts: even i not divisible by 5 → i = 2, 4, 6, 8.
	if rep.Deleted != 4 {
		t.Fatalf("deleted %d, want 4", rep.Deleted)
	}
	for i, uid := range uids {
		gone := e.get(uid) == ""
		if want := i%2 == 0 && i%5 != 0; gone != want {
			t.Errorf("uid %d: deleted %v, want %v", i, gone, want)
		}
	}
	if e.get(other) == "" {
		t.Fatal("another epoch's field was deleted")
	}
	// A uid with no field of this epoch left leaves gwconns.
	in, _ := e.rdb.SIsMember(ctx, fmt.Sprintf("gwconns:%s:{%d}", e.gw.Gateway, e.gw.Epoch), uids[2]).Result()
	if in {
		t.Fatal("emptied uid still in gwconns")
	}
}

// Spec (R5S-O8): a delete phase over N uids takes ≥ N ÷ rate (less the
// limiter's initial burst of one batch) and deletes the same fields as an
// unthrottled run.
func TestReconcileEndIsRateLimited(t *testing.T) {
	throttled := testConfig()
	throttled.DeleteRate, throttled.DeleteBatch = 1000, 100
	fast := testConfig()
	fast.DeleteRate, fast.DeleteBatch = 1_000_000, 100

	uids := uidList(600)
	run := func(cfg Config) ([]string, time.Duration) {
		e := newEnvWith(t, cfg, func(s *store.Store) Store { return &stableScan{Store: s} })
		req := phaseFixture(t, e, uids)
		start := time.Now()
		if _, err := e.svc.ReconcileEnd(ctx, req); err != nil {
			t.Fatal(err)
		}
		return dump(t, e, uids), time.Since(start)
	}
	slowState, slow := run(throttled)
	fastState, _ := run(fast)
	if min := time.Duration(600-100) * time.Second / 1000; slow < min {
		t.Fatalf("600 uids at 1000/s took %v, want ≥ %v", slow, min)
	}
	if !reflect.DeepEqual(slowState, fastState) {
		t.Fatal("throttled and unthrottled runs left different state")
	}
}

// interrupted fails DeleteUntracked after the first page, like a Logic node
// dying mid-phase.
type interrupted struct {
	stableScan
	calls int
}

func (s *interrupted) DeleteUntracked(ctx context.Context, ref store.EpochRef, asOf int64, uids []string, tracked map[uint64]struct{}) (int, error) {
	s.calls++
	if s.calls == 2 {
		return 0, errors.New("node died")
	}
	return s.stableScan.DeleteUntracked(ctx, ref, asOf, uids, tracked)
}

// Spec (R5S-O8): a ReconcileEnd interrupted mid-phase and retried leaves the
// same result as one uninterrupted run.
func TestReconcileEndRetryAfterInterruption(t *testing.T) {
	cfg := testConfig()
	cfg.DeleteBatch = 10
	uids := uidList(50)

	cut := &interrupted{}
	e := newEnvWith(t, cfg, func(s *store.Store) Store { cut.stableScan.Store = s; return cut })
	req := phaseFixture(t, e, uids)
	if _, err := e.svc.ReconcileEnd(ctx, req); err == nil {
		t.Fatal("interrupted phase reported success")
	}
	if _, err := e.svc.ReconcileEnd(ctx, req); err != nil { // the gateway's retry, same payload
		t.Fatal(err)
	}

	whole := newEnvWith(t, cfg, func(s *store.Store) Store { return &stableScan{Store: s} })
	req2 := phaseFixture(t, whole, uids)
	if _, err := whole.svc.ReconcileEnd(ctx, req2); err != nil {
		t.Fatal(err)
	}
	if a, b := dump(t, e, uids), dump(t, whole, uids); !reflect.DeepEqual(a, b) {
		t.Fatalf("retried run differs from an uninterrupted one:\n%v\n%v", a, b)
	}
}

func TestReconcileEndRejectsBadRequest(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.ReconcileEnd(ctx, &pb.ReconcileEndRequest{Gw: pbRef(e.gw)}); err == nil {
		t.Fatal("as_of_ms 0 accepted")
	}
}

// Review Focus 5: the first pass of a fresh epoch has no gwconns key yet.
func TestReconcileEndWithoutConnsKey(t *testing.T) {
	e := newEnv(t)
	rep, err := e.svc.ReconcileEnd(ctx, &pb.ReconcileEndRequest{Gw: pbRef(e.gw), AsOfMs: redisNow(t, e.real)})
	if err != nil || rep.Deleted != 0 {
		t.Fatalf("reply %v err %v", rep, err)
	}
}
