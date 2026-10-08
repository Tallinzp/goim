package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

// fakeLogic serves the Logic and Lease services on two localhost listeners.
// Each handler can be replaced; the defaults accept everything.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer

	mu           sync.Mutex
	login        func(*pb.LoginBatch, grpc.ServerStreamingServer[pb.LoginResult]) error
	logout       func(*pb.LogoutBatch) (*pb.LogoutReply, error)
	reconcile    func(*pb.ReconcileChunk) (*pb.ReconcileReply, error)
	reconcileEnd func(*pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error)
	lease        func(*pb.LeaseRequest) (*pb.LeaseReply, error)

	logins     []*pb.LoginBatch
	logouts    []*pb.LogoutBatch
	chunks     []*pb.ReconcileChunk
	ends       []*pb.ReconcileEndRequest
	leaseCalls []*pb.LeaseRequest
	epochEnded int

	logicAddr, leaseAddr string
}

func startFakeLogic(t *testing.T) *fakeLogic {
	t.Helper()
	f := &fakeLogic{}
	for i, reg := range []func(*grpc.Server){
		func(s *grpc.Server) { pb.RegisterLogicServer(s, f) },
		func(s *grpc.Server) { pb.RegisterLeaseServer(s, f) },
	} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		reg(s)
		go s.Serve(l)
		t.Cleanup(s.Stop)
		if i == 0 {
			f.logicAddr = l.Addr().String()
		} else {
			f.leaseAddr = l.Addr().String()
		}
	}
	return f
}

func (f *fakeLogic) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	f.mu.Lock()
	f.logins = append(f.logins, b)
	h := f.login
	f.mu.Unlock()
	if h != nil {
		return h(b, s)
	}
	for i, e := range b.GetEntries() {
		if err := s.Send(okResult(i, e)); err != nil {
			return err
		}
	}
	return nil
}

func okResult(i int, e *pb.LoginEntry) *pb.LoginResult {
	return &pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: tokenSubject(e.GetToken()), TokIatMs: 1}
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.mu.Lock()
	f.logouts = append(f.logouts, b)
	h := f.logout
	f.mu.Unlock()
	if h != nil {
		return h(b)
	}
	return allDone(b), nil
}

func allDone(b *pb.LogoutBatch) *pb.LogoutReply {
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r
}

func (f *fakeLogic) Reconcile(_ context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	f.mu.Lock()
	f.chunks = append(f.chunks, c)
	h := f.reconcile
	f.mu.Unlock()
	if h != nil {
		return h(c)
	}
	return replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN), nil
}

func replyAll(c *pb.ReconcileChunk, code pb.ReconcileCode) *pb.ReconcileReply {
	r := &pb.ReconcileReply{}
	for range c.GetEntries() {
		r.Results = append(r.Results, &pb.ReconcileResult{Code: code})
	}
	return r
}

func (f *fakeLogic) ReconcileEnd(_ context.Context, r *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	f.mu.Lock()
	f.ends = append(f.ends, r)
	h := f.reconcileEnd
	f.mu.Unlock()
	if h != nil {
		return h(r)
	}
	return &pb.ReconcileEndReply{}, nil
}

func (f *fakeLogic) GatewayLease(_ context.Context, r *pb.LeaseRequest) (*pb.LeaseReply, error) {
	f.mu.Lock()
	f.leaseCalls = append(f.leaseCalls, r)
	h, first := f.lease, len(f.leaseCalls) == 1
	f.mu.Unlock()
	if h != nil {
		return h(r)
	}
	return &pb.LeaseReply{Missing: first}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.mu.Lock()
	f.epochEnded++
	f.mu.Unlock()
	return &pb.EpochEndedReply{}, nil
}

// get reads recorded calls under the lock.
func get[T any](f *fakeLogic, fn func() T) T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fn()
}

// ---- started harness ----

// live is a started Gateway on fake loops, talking to a fakeLogic.
type live struct {
	*core
	logic *fakeLogic
}

// fastConfig shrinks the background intervals so tests run quickly.
func fastConfig(c *Config) {
	c.LeaseInterval, c.LeaseTimeout, c.LeaseTTL = 100*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond
	c.Reconcile.Cycle, c.Reconcile.Jitter = time.Hour, 0 // passes only when a test triggers one
	c.RetryBackoff, c.RetryMaxDelay = 20*time.Millisecond, 50*time.Millisecond
}

func newLive(t *testing.T, mod func(*Config)) *live { t.Helper(); return startLive(t, mod, nil) }

// startLive is newLive with pre run on the Gateway before Start.
func startLive(t *testing.T, mod func(*Config), pre func(*Gateway, *fakeLogic)) *live {
	t.Helper()
	f := startFakeLogic(t)
	h := newCore(t, func(c *Config) {
		fastConfig(c)
		if mod != nil {
			mod(c)
		}
	})
	if pre != nil {
		pre(h.g, f)
	}
	sc := ServerConfig{LogicTarget: f.logicAddr, LeaseTarget: f.leaseAddr}
	if err := h.g.Start(context.Background(), sc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.g.Stop)
	return &live{core: h, logic: f}
}

// loginLive sends a LoginReq and waits for the connection to be ONLINE.
func (l *live) loginLive(t *testing.T, li int, uid string) *fakeLink {
	t.Helper()
	k := l.authed(li)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 7, loginReq(uid, "dev-"+uid, 1))
	waitFor(t, uid+" ONLINE", func() bool { return onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) })
	return k
}
