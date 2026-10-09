package logic

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func faultEnv(t *testing.T) (faultStore, *store.Store, store.EpochRef) {
	t.Helper()
	st, _ := newStore(t, testConfig())
	return faultStore{st, fault.New()}, st, store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
}

func loginReq(t *testing.T, st Store, ref store.EpochRef, uid string, connID uint64) store.LoginReq {
	now := redisNow(t, st)
	return store.LoginReq{UID: uid, Field: field, Entry: session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch,
		ConnID: connID, DeviceID: "d-" + uid, ConnSeq: 1, LoginMs: now, TokIatMs: now - 1000}}
}

// A delayed entry does not hold up the rest of its batch.
func TestFaultLoginDelaysOnlyItsEntry(t *testing.T) {
	fs, st, ref := faultEnv(t)
	slow, fast := testredis.UID(), testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: slow, Delay: fault.Duration(300 * time.Millisecond)})
	reqs := []store.LoginReq{loginReq(t, st, ref, slow, 1), loginReq(t, st, ref, fast, 2)}
	var mu sync.Mutex
	var order []int
	start := time.Now()
	var slowAt time.Duration
	fs.LoginEach(ctx, reqs, func(i int, r store.LoginResult) {
		mu.Lock()
		defer mu.Unlock()
		if r.Err != nil || r.Code != store.LoginWritten {
			t.Errorf("entry %d: %+v", i, r)
		}
		order = append(order, i)
		if i == 0 {
			slowAt = time.Since(start)
		}
	})
	if len(order) != 2 || order[0] != 1 || slowAt < 300*time.Millisecond {
		t.Fatalf("order %v, slow entry after %v; want the fast one first and the slow one after 300ms", order, slowAt)
	}
}

// A delayed entry reaches Redis even after its RPC was cancelled, and the
// login Lua's deadline refuses it (race 6, Logout before Login).
func TestFaultDelayOutlivesTheRPC(t *testing.T) {
	cfg := testConfig()
	cfg.Store.LoginDeadline = 200 * time.Millisecond
	st, _ := newStore(t, cfg)
	fs := faultStore{st, fault.New()}
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
	uid := testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: uid, Delay: fault.Duration(400 * time.Millisecond)})
	rpc, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var got store.LoginResult
	fs.LoginEach(rpc, []store.LoginReq{loginReq(t, st, ref, uid, 1)}, func(_ int, r store.LoginResult) { got = r })
	if got.Err != nil || got.Code != store.LoginExpired {
		t.Fatalf("result %+v, want EXPIRED from the Lua", got)
	}
}

// After + Fail: Redis is written, but the reply says the outcome is unknown
// (race 6: no definitive reply, Redis written).
func TestFaultLoginAfterWritesThenFails(t *testing.T) {
	fs, st, ref := faultEnv(t)
	uid := testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultLogin, Key: uid, After: true, Fail: true, Count: 1})
	var got store.LoginResult
	fs.LoginEach(ctx, []store.LoginReq{loginReq(t, st, ref, uid, 1)}, func(_ int, r store.LoginResult) { got = r })
	if !errors.Is(got.Err, fault.ErrInjected) {
		t.Fatalf("result %+v, want the injected error", got)
	}
	states := st.ReadSlots(ctx, []store.SlotRef{{UID: uid, Field: field}})
	if states[0].Stored == nil || states[0].Stored.ConnID != 1 {
		t.Fatalf("stored %+v, want the write to have happened", states[0])
	}
}

func TestFaultLogoutFailsOnlyItsEntry(t *testing.T) {
	fs, st, ref := faultEnv(t)
	a, b := testredis.UID(), testredis.UID()
	st.LoginEach(ctx, []store.LoginReq{loginReq(t, st, ref, a, 1), loginReq(t, st, ref, b, 2)}, func(int, store.LoginResult) {})
	fs.f.Put(fault.Rule{Op: FaultLogout, Key: a, Fail: true})
	res := make([]store.LogoutResult, 2)
	fs.LogoutEach(ctx, []store.LogoutReq{
		{UID: a, Field: field, Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: 1},
		{UID: b, Field: field, Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: 2},
	}, func(i int, r store.LogoutResult) { res[i] = r })
	if !errors.Is(res[0].Err, fault.ErrInjected) || res[1].Err != nil || !res[1].Deleted {
		t.Fatalf("results %+v", res)
	}
	if s := st.ReadSlots(ctx, []store.SlotRef{{UID: a, Field: field}}); s[0].Stored == nil {
		t.Fatal("the failed Logout deleted its entry")
	}
}

func TestFaultReconcileFailsTheWholeChunk(t *testing.T) {
	fs, _, _ := faultEnv(t)
	a, b := testredis.UID(), testredis.UID()
	fs.f.Put(fault.Rule{Op: FaultReconcile, Key: b, Fail: true, Count: 1})
	states := fs.ReadSlots(ctx, []store.SlotRef{{UID: a, Field: field}, {UID: b, Field: field}})
	for i, s := range states {
		if !errors.Is(s.Err, fault.ErrInjected) {
			t.Fatalf("state %d: %+v, want the injected error", i, s)
		}
	}
	if states := fs.ReadSlots(ctx, []store.SlotRef{{UID: b, Field: field}}); states[0].Err != nil {
		t.Fatalf("Count 1 fired twice: %+v", states[0])
	}
}

// The ReconcileEnd hold sits before the handler, so the delete phase starts
// after it; a failing rule answers UNAVAILABLE, which the gateway retries.
func TestFaultReconcileEndFailsForItsGateway(t *testing.T) {
	f := fault.New()
	f.Put(fault.Rule{Op: FaultReconcileEnd, Key: "gw-a", Fail: true})
	calls := 0
	h := func(context.Context, any) (any, error) { calls++; return &pb.ReconcileEndReply{}, nil }
	end := func(gw string) error {
		_, err := faultUnary(f)(ctx, &pb.ReconcileEndRequest{Gw: &pb.GatewayRef{Gateway: gw, Epoch: 1}}, nil, h)
		return err
	}
	if err := end("gw-a"); status.Code(err) != codes.Unavailable || calls != 0 {
		t.Fatalf("gw-a: %v after %d handler calls; want UNAVAILABLE before the handler", err, calls)
	}
	if err := end("gw-b"); err != nil || calls != 1 {
		t.Fatalf("gw-b: %v, %d handler calls", err, calls)
	}
	if _, err := faultUnary(f)(ctx, &pb.LogoutBatch{}, nil, h); err != nil {
		t.Fatalf("another method: %v", err)
	}
}

// The lease SET succeeds and only the named gateway's ZADD gwepochs fails (R4M-3).
func TestFaultHookFailsOnlyTheNamedGatewaysZADD(t *testing.T) {
	rdb := testredis.New(t)
	f := fault.New()
	rdb.AddHook(FaultHook(f))
	st := store.New(rdb, testConfig().Store)
	a := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	b := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	f.Put(fault.Rule{Op: FaultGWEpochs, Key: a.Gateway})
	res, err := st.RenewLease(ctx, a)
	if err != nil || !errors.Is(res.IndexErr, fault.ErrInjected) {
		t.Fatalf("renewal of a: %+v %v; want IndexErr injected", res, err)
	}
	if live, _ := st.LeasesExist(ctx, []store.EpochRef{a}); !live[0] {
		t.Fatal("the lease SET did not happen")
	}
	if _, ok, _ := st.EpochScoreOf(ctx, a); ok {
		t.Fatal("a's gwepochs score was written")
	}
	if res, err := st.RenewLease(ctx, b); err != nil || res.IndexErr != nil {
		t.Fatalf("renewal of b: %+v %v", res, err)
	}
}

func TestFileDialReadsTheFileAtEachDial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kick.json")
	dial := FileDial(path)
	if _, err := dial("gw-1"); err == nil {
		t.Fatal("dial without a file succeeded")
	}
	os.WriteFile(path, []byte(`{"gw-1":"127.0.0.1:1"}`), 0o644)
	cc, err := dial("gw-1")
	if err != nil {
		t.Fatal(err)
	}
	cc.Close()
	if _, err := dial("gw-2"); err == nil {
		t.Fatal("dial of an unlisted gateway succeeded")
	}
	os.WriteFile(path, []byte(`{"gw-2":"127.0.0.1:2"}`), 0o644)
	if cc, err := dial("gw-2"); err != nil {
		t.Fatalf("after the file changed: %v", err)
	} else {
		cc.Close()
	}
}

// readyKeys is a key set that is always loaded.
type readyKeys struct{ staticKeys }

func (readyKeys) Ready() bool         { return true }
func (readyKeys) Run(context.Context) {}

// /fault/ is served only by a Logic built with Faults.
func TestFaultEndpointOnlyWithFaults(t *testing.T) {
	for _, withFaults := range []bool{false, true} {
		cfg := testConfig()
		cfg.SweepInterval = time.Hour
		st, _ := newStore(t, cfg)
		sc := DefaultServerConfig()
		sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
		reg := prometheus.NewRegistry()
		d := Deps{Store: st, Keys: readyKeys{}, Dial: staticDial(nil), Registry: reg, Metrics: NewMetrics(reg)}
		if withFaults {
			d.Faults = fault.New()
		}
		srv, err := NewServer(cfg, sc, d)
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		if err := srv.Start(runCtx); err != nil {
			t.Fatal(err)
		}
		_, _, admin := srv.Addrs()
		want := http.StatusNotFound
		if withFaults {
			want = http.StatusOK
		}
		if c := httpStatus(t, "http://"+admin+"/fault/info"); c != want {
			t.Errorf("faults %v: /fault/info = %d, want %d", withFaults, c, want)
		}
		cancel()
		srv.Stop()
	}
}
