package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// fakeLogic accepts every Login (uid = the token), Logout and Reconcile, and
// renews every lease. It records Login entries and Logouts.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer

	mu         sync.Mutex
	entries    []*pb.LoginEntry
	logouts    []*pb.LogoutEntry
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

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	f.mu.Lock()
	f.entries = append(f.entries, b.GetEntries()...)
	f.mu.Unlock()
	for i, e := range b.GetEntries() {
		if err := s.Send(&pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: e.GetToken(), TokIatMs: 1}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.mu.Lock()
	f.logouts = append(f.logouts, b.GetEntries()...)
	f.mu.Unlock()
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r, nil
}

func (f *fakeLogic) Reconcile(_ context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	r := &pb.ReconcileReply{}
	for range c.GetEntries() {
		r.Results = append(r.Results, &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_WRITTEN})
	}
	return r, nil
}

func (f *fakeLogic) ReconcileEnd(context.Context, *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	return &pb.ReconcileEndReply{}, nil
}

func (f *fakeLogic) GatewayLease(context.Context, *pb.LeaseRequest) (*pb.LeaseReply, error) {
	return &pb.LeaseReply{}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.mu.Lock()
	f.epochEnded++
	f.mu.Unlock()
	return &pb.EpochEndedReply{}, nil
}

// connID returns the conn_id Logic saw for uid's Login.
func (f *fakeLogic) connID(uid string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if e.GetToken() == uid {
			return e.GetConnId(), true
		}
	}
	return 0, false
}

func (f *fakeLogic) loggedOut(uid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.logouts {
		if e.GetUid() == uid {
			return true
		}
	}
	return false
}

// server is a started gateway core with this transport on 127.0.0.1.
type server struct {
	t     *testing.T
	g     *gateway.Gateway
	m     *gateway.Metrics
	tr    *Transport
	logic *fakeLogic
	keys  wire.KeySet
	ref   *pb.GatewayRef
}

func testConfig() gateway.Config {
	cfg := gateway.DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-0", gateway.NewEpoch()
	cfg.NumLoops = 2
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 500, 500, 500, 500
	cfg.Limits.Notrack = true
	return cfg
}

// newServer starts a gateway with this transport against a fake Logic. mod
// changes the configuration; each set runs on the transport before it
// starts.
func newServer(t *testing.T, mod func(*gateway.Config, *Config), set ...func(*Transport)) *server {
	t.Helper()
	logic := startFakeLogic(t)
	s := startServer(t, mod, logic.logicAddr, logic.leaseAddr, set...)
	s.logic = logic
	return s
}

// startServer starts a gateway with this transport against the Logic at
// logicAddr and leaseAddr.
func startServer(t *testing.T, mod func(*gateway.Config, *Config), logicAddr, leaseAddr string, set ...func(*Transport)) *server {
	t.Helper()
	cfg := testConfig()
	tcfg := DefaultConfig(cfg.NumLoops)
	tcfg.Addr = "tcp://127.0.0.1:0"
	if mod != nil {
		mod(&cfg, &tcfg)
	}
	k1, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, _ := wire.GenerateKey()
	keys := wire.KeySet{1: k1, 2: k2}
	reg := prometheus.NewRegistry()
	m := gateway.NewMetrics(reg)
	var tr *Transport
	g, err := gateway.New(cfg, gateway.Deps{Metrics: m, Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		t.Fatal(err)
	}
	if tr, err = New(g, keys, tcfg, reg); err != nil {
		t.Fatal(err)
	}
	for _, f := range set {
		f(tr)
	}
	if err := g.Start(context.Background(), gateway.ServerConfig{LogicTarget: logicAddr,
		LeaseTarget: leaseAddr, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr.Stop(ctx)
		g.Stop()
	})
	return &server{t: t, g: g, m: m, tr: tr, keys: keys,
		ref: &pb.GatewayRef{Gateway: cfg.GatewayID, Epoch: cfg.Epoch}}
}

func (s *server) rejects(reason string) int {
	return int(testutil.ToFloat64(s.m.Rejects.WithLabelValues(reason)))
}

func (s *server) closes(reason string) int {
	return int(testutil.ToFloat64(s.m.Closes.WithLabelValues(reason)))
}

// client is a test client on one TCP connection.
type client struct {
	t    *testing.T
	nc   net.Conn
	sess *wire.Session
}

func (s *server) dial() *client {
	s.t.Helper()
	nc, err := net.Dial("tcp", s.tr.Addr())
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { nc.Close() })
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	return &client{t: s.t, nc: nc}
}

// hello dials and completes the handshake with key 1.
func (s *server) hello() *client {
	s.t.Helper()
	c := s.dial()
	sess, err := wire.ClientHandshake(c.nc, 1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		s.t.Fatal(err)
	}
	c.sess = sess
	return c
}

// online dials, handshakes and logs in as uid; the fake Logic takes the
// token as the uid.
func (s *server) online(uid string) *client { return s.login(uid, "d-"+uid) }

// login dials, handshakes and logs in with token from device.
func (s *server) login(token, device string) *client {
	s.t.Helper()
	c := s.hello()
	c.send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: token, Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: device, ConnSeq: 1})
	var resp clientpb.LoginResp
	if cmd, id := c.recv(&resp); cmd != clientpb.Cmd_CMD_LOGIN_RESP || id != 1 || resp.HeartbeatS != 150 {
		s.t.Fatalf("login reply %v %d %v", cmd, id, &resp)
	}
	return c
}

func (c *client) send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	c.t.Helper()
	c.nc.Write(c.seal(cmd, reqID, msg))
}

func (c *client) seal(cmd clientpb.Cmd, reqID uint32, msg proto.Message) []byte {
	c.t.Helper()
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			c.t.Fatal(err)
		}
	}
	f, err := c.sess.Seal(nil, uint16(cmd), reqID, body)
	if err != nil {
		c.t.Fatal(err)
	}
	return f
}

// recv reads one frame, decoding its body into msg if not nil.
func (c *client) recv(msg proto.Message) (clientpb.Cmd, uint32) {
	c.t.Helper()
	cmd, id, body, err := c.sess.ReadFrame(c.nc)
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	if msg != nil {
		if err := proto.Unmarshal(body, msg); err != nil {
			c.t.Fatal(err)
		}
	}
	return clientpb.Cmd(cmd), id
}

// closedByServer reports whether the server closed the connection with no
// more bytes sent.
func (c *client) closedByServer() bool {
	c.t.Helper()
	var b [1]byte
	n, err := c.nc.Read(b[:])
	return n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || isReset(err))
}

func isReset(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && !op.Timeout()
}

// waitFor polls cond for up to 5s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
