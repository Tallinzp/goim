package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDefaultFlagsAreValid(t *testing.T) {
	cfg, sc, tc, o, err := parseFlags([]string{"-id", "gw-0"})
	if err != nil {
		t.Fatal(err)
	}
	n := runtime.GOMAXPROCS(0)
	if cfg.NumLoops != n || tc.HandshakeConcurrency != max(1, (n+1)/2) || cfg.Epoch == 0 {
		t.Fatalf("loops %d, handshake concurrency %d, epoch %x", cfg.NumLoops, tc.HandshakeConcurrency, cfg.Epoch)
	}
	if tc.MaxOutbound != wire.HeaderLen+wire.MaxLength {
		t.Fatalf("max outbound %d, want one max frame", tc.MaxOutbound)
	}
	if tc.Addr != "tcp://:8000" || sc.KickAddr != ":9100" || o.keyDir != "/etc/goim/noise" || o.drainTimeout != 85*time.Second {
		t.Fatalf("defaults: %+v %+v %+v", tc, sc, o)
	}
	cfg, _, tc, _, err = parseFlags([]string{"-id", "gw-1", "-loops", "16", "-storm-wait", "5s-60s", "-max-outbound", "131072"})
	if err != nil {
		t.Fatal(err)
	}
	if tc.MaxOutbound != 131072 {
		t.Fatalf("max outbound %d", tc.MaxOutbound)
	}
	if tc.HandshakeConcurrency != 8 || cfg.Windows.StormWait.Lo != 5*time.Second || cfg.Windows.StormWait.Hi != time.Minute {
		t.Fatalf("concurrency %d, storm wait %+v", tc.HandshakeConcurrency, cfg.Windows.StormWait)
	}
}

// The Logout queue follows the connection cap unless set (#89): a gateway
// whose connections all close at once (a loadgen stop, a whole-population
// reconnect) queues one Logout per connection, and a queue below the cap
// drops the excess, leaving sessions for the sweeper.
func TestLogoutQueueFollowsTheCap(t *testing.T) {
	for _, c := range []struct {
		args []string
		want int
	}{
		{nil, 250_000},
		{[]string{"-max-conns", "650000"}, 650_000},
		{[]string{"-max-conns", "650000", "-logout-queue", "1000"}, 1000},
		{[]string{"-logout-queue", "1000", "-max-conns", "650000"}, 1000},
	} {
		cfg, _, _, _, err := parseFlags(append([]string{"-id", "gw-0"}, c.args...))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.LogoutQueue != c.want {
			t.Errorf("%v: logout queue %d, want %d", c.args, cfg.LogoutQueue, c.want)
		}
	}
}

// The race suite's flags (Plan 5b).
func TestRaceSuiteFlags(t *testing.T) {
	cfg, _, _, o, err := parseFlags([]string{"-id", "gw-0", "-faults", "-clock-offset", "-5s",
		"-retry-backoff", "100ms", "-retry-max-delay", "1s"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.faults || o.clockOffset != -5*time.Second || cfg.RetryBackoff != 100*time.Millisecond || cfg.RetryMaxDelay != time.Second {
		t.Fatalf("parsed %+v %+v", cfg, o)
	}
}

func TestRefusesBadConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-handshake-timeout", "2s"}, // spec: 3–30s
		{"-auth-timeout", "31s"},
		{"-loops", "300"}, // gnet's limit is 256
		{"-busy-retry", "3s-1s"},
		{"-max-client-frame", "10"},
		{"-max-outbound", "4096"}, // below one frame
		{"-id", "Not_A_Label"},
	} {
		if _, _, _, _, err := parseFlags(append([]string{"-id", "gw-0"}, args...)); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

func TestNoiseKeygen(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	if err := keygen([]string{"-key-id", "3", "-out", filepath.Join(dir, "3")}, &out); err != nil {
		t.Fatal(err)
	}
	ks, err := wire.LoadKeyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, pub := wire.EncodeKey(ks[3])
	if got := strings.TrimSpace(out.String()); got != "key_id 3 public "+pub {
		t.Fatalf("printed %q, want the public key %s", got, pub)
	}
	if fi, _ := os.Stat(filepath.Join(dir, "3")); fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v", fi.Mode())
	}
	// A second run must not replace a key whose public half may be pinned.
	before, _ := os.ReadFile(filepath.Join(dir, "3"))
	if err := keygen([]string{"-key-id", "3", "-out", filepath.Join(dir, "3")}, &out); err == nil {
		t.Fatal("an existing key file was overwritten")
	}
	if after, _ := os.ReadFile(filepath.Join(dir, "3")); !bytes.Equal(before, after) {
		t.Fatal("the existing key file changed")
	}
	if err := keygen([]string{"-key-id", "70000", "-out", filepath.Join(dir, "x")}, &out); err == nil {
		t.Fatal("key_id above 65535 accepted")
	}
}

// fakeLogic accepts every Login (uid = token) and Logout, renews every
// lease, and counts Logouts and EpochEnded.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer
	logouts       atomic.Int32
	ended         atomic.Int32
	endedLoggedIn atomic.Int32 // EpochEnded calls made before any Logout
	leaseSkewMs   atomic.Int64 // gw_now_ms − local time of the last GatewayLease
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	for i, e := range b.GetEntries() {
		s.Send(&pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: e.GetToken(), TokIatMs: 1})
	}
	return nil
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.logouts.Add(int32(len(b.GetEntries())))
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r, nil
}

func (f *fakeLogic) GatewayLease(_ context.Context, r *pb.LeaseRequest) (*pb.LeaseReply, error) {
	f.leaseSkewMs.Store(r.GetGwNowMs() - time.Now().UnixMilli())
	return &pb.LeaseReply{}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.ended.Add(1)
	if f.logouts.Load() == 0 {
		f.endedLoggedIn.Add(1)
	}
	return &pb.EpochEndedReply{}, nil
}

// -clock-offset shifts the clock the gateway stamps on everything it sends
// Logic, here gw_now_ms (P4I-5).
func TestClockOffsetShiftsTheGatewayClock(t *testing.T) {
	logic := &fakeLogic{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterLogicServer(s, logic)
	pb.RegisterLeaseServer(s, logic)
	go s.Serve(l)
	defer s.Stop()
	keys := t.TempDir()
	if err := keygen([]string{"-key-id", "1", "-out", filepath.Join(keys, "1")}, io.Discard); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-id", "gw-0", "-listen", "tcp://127.0.0.1:0", "-noise-keys", keys,
			"-logic", l.Addr().String(), "-lease", l.Addr().String(), "-kick-addr", "127.0.0.1:0",
			"-admin-addr", "127.0.0.1:0", "-loops", "1", "-conntrack-notrack", "-clock-offset", "2s",
			"-drain-window", "100ms", "-drain-flush", "0s"}, func(a string) { started <- a })
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for logic.leaseSkewMs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if skew := logic.leaseSkewMs.Load(); skew < 1500 || skew > 2500 {
		t.Fatalf("gw_now_ms is %dms ahead, want about 2000", skew)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The whole binary: serve, log a client in over TCP + Noise, then SIGTERM
// (ctx done): the client gets Reconnect, the socket closes, its fenced
// Logout reaches Logic before EpochEnded, and run returns.
func TestRunServesThenDrainsOnSIGTERM(t *testing.T) {
	logic := &fakeLogic{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterLogicServer(s, logic)
	pb.RegisterLeaseServer(s, logic)
	go s.Serve(l)
	defer s.Stop()

	keys := t.TempDir()
	var out bytes.Buffer
	if err := keygen([]string{"-key-id", "1", "-out", filepath.Join(keys, "1")}, &out); err != nil {
		t.Fatal(err)
	}
	pub, _ := base64.StdEncoding.DecodeString(strings.Fields(out.String())[3])

	ctx, cancel := context.WithCancel(context.Background())
	addr := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-id", "gw-0", "-listen", "tcp://127.0.0.1:0", "-noise-keys", keys,
			"-logic", l.Addr().String(), "-lease", l.Addr().String(), "-kick-addr", "127.0.0.1:0",
			"-admin-addr", "127.0.0.1:0", "-loops", "2", "-max-conns", "100", "-pending-cap", "100",
			"-logout-queue", "100", "-conntrack-notrack", "-drain-window", "200ms", "-drain-flush", "0s"},
			func(a string) { addr <- a })
	}()
	var gw string
	select {
	case gw = <-addr:
	case err := <-done:
		t.Fatal(err)
	}

	nc, err := net.Dial("tcp", gw)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	sess, err := wire.ClientHandshake(nc, 1, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(&clientpb.LoginReq{Token: "u1", Appid: "app", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: "d1", ConnSeq: 1})
	sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_LOGIN), 1, body)
	if cmd, _, _, err := sess.ReadFrame(nc); err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("login: %v %v", cmd, err)
	}

	cancel() // SIGTERM
	cmd, _, body, err := sess.ReadFrame(nc)
	var rc clientpb.Reconnect
	if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_RECONNECT || proto.Unmarshal(body, &rc) != nil || rc.JitterMs != 5000 {
		t.Fatalf("drain: %v %v %v", cmd, &rc, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after the drain")
	}
	if logic.ended.Load() != 1 || logic.logouts.Load() != 1 || logic.endedLoggedIn.Load() != 0 {
		t.Fatalf("EpochEnded %d times (%d before any Logout), Logouts %d; want 1, 0, 1",
			logic.ended.Load(), logic.endedLoggedIn.Load(), logic.logouts.Load())
	}
}

// The Login RPC window is settable (the slowlogic diagnostic, #115):
// LoginRPCs × LoginBatch Logins sit inside Logic at once, ~1s of work for a
// Logic slowed to 25% of a CPU.
func TestLoginWindowFlags(t *testing.T) {
	cfg, _, _, _, err := parseFlags([]string{"-id", "gw-0", "-login-rpcs", "4", "-login-batch", "50"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LoginRPCs != 4 || cfg.LoginBatch != 50 {
		t.Fatalf("login rpcs %d, batch %d, want 4, 50", cfg.LoginRPCs, cfg.LoginBatch)
	}
	for _, args := range [][]string{{"-login-rpcs", "0"}, {"-login-batch", "0"}} {
		if _, _, _, _, err := parseFlags(append([]string{"-id", "gw-0"}, args...)); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}
