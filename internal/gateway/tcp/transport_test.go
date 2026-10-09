package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLoginPingLogoutOverTCP(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u1")
	c.send(clientpb.Cmd_CMD_PING, 5, nil)
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != 5 {
		t.Fatalf("got %v %d, want Pong 5", cmd, id)
	}
	c.send(clientpb.Cmd_CMD_LOGOUT, 6, nil)
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_LOGOUT_RESP || id != 6 {
		t.Fatalf("got %v %d, want LogoutResp 6", cmd, id)
	}
	if !c.closedByServer() {
		t.Fatal("not closed after LogoutResp")
	}
	waitFor(t, "fenced Logout reaches Logic", func() bool { return s.logic.loggedOut("u1") })
	if n := testutil.ToFloat64(s.tr.hs.WithLabelValues("ok")); n != 1 {
		t.Fatalf("handshakes_total{ok} = %v", n)
	}
}

func TestPartialAndCoalescedFrames(t *testing.T) {
	s := newServer(t, nil)
	// The handshake frame one byte at a time.
	c := s.dial()
	in, f1, _ := wire.Initiate(2, s.keys[2].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	for _, b := range f1 {
		c.nc.Write([]byte{b})
		time.Sleep(time.Millisecond)
	}
	reply := make([]byte, wire.HeaderLen+wire.HandshakeMsgLen)
	if _, err := ioReadFull(c, reply); err != nil {
		t.Fatal(err)
	}
	sess, err := in.Finish(reply)
	if err != nil {
		t.Fatal(err)
	}
	c.sess = sess
	c.send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "u2", Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: "d", ConnSeq: 1})
	if cmd, _ := c.recv(nil); cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("got %v", cmd)
	}
	// Three Pings in one write, then one Ping split into pieces.
	var burst []byte
	for id := uint32(10); id < 13; id++ {
		burst = append(burst, c.seal(clientpb.Cmd_CMD_PING, id, nil)...)
	}
	c.nc.Write(burst)
	for want := uint32(10); want < 13; want++ {
		if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != want {
			t.Fatalf("got %v %d, want Pong %d", cmd, id, want)
		}
	}
	f := c.seal(clientpb.Cmd_CMD_PING, 20, nil)
	for _, piece := range [][]byte{f[:3], f[3:9], f[9:]} {
		c.nc.Write(piece)
		time.Sleep(5 * time.Millisecond)
	}
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != 20 {
		t.Fatalf("got %v %d, want Pong 20", cmd, id)
	}
}

func ioReadFull(c *client, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := c.nc.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// handshakeFrame is a client handshake frame for key 1, changed by mod.
func handshakeFrame(t *testing.T, s *server, mod func([]byte)) []byte {
	t.Helper()
	_, f, err := wire.Initiate(1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		t.Fatal(err)
	}
	if mod != nil {
		mod(f)
	}
	return f
}

func TestPreHandshakeRejectsCloseBeforeAnyReply(t *testing.T) {
	s := newServer(t, nil)
	for _, tc := range []struct {
		name, reject, close string
		mod                 func([]byte)
	}{
		{"version 2", gateway.RejectBadVersion, "rejected", func(f []byte) { f[0] = 2 }},
		{"unknown key_id", gateway.RejectNoiseUnknownKey, "rejected", func(f []byte) { binary.BigEndian.PutUint16(f[6:], 9) }},
		{"flag bit set", "", closeBadFrame, func(f []byte) { f[3] = 0x01 }},
		{"Login before the handshake", "", closeBadFrame, func(f []byte) { f[2] = byte(clientpb.Cmd_CMD_LOGIN) }},
		{"wrong handshake size", "", closeBadFrame, func(f []byte) { f[5]++ }},
	} {
		before := s.closes(tc.close)
		c := s.dial()
		c.nc.Write(handshakeFrame(t, s, tc.mod))
		if !c.closedByServer() {
			t.Fatalf("%s: not closed, or bytes were sent", tc.name)
		}
		waitFor(t, tc.name+" counted", func() bool { return s.closes(tc.close) == before+1 })
		if tc.reject != "" && s.rejects(tc.reject) != 1 {
			t.Fatalf("%s: %s rejects = %d", tc.name, tc.reject, s.rejects(tc.reject))
		}
	}
	// A key_id changed in flight to another known key fails the handshake.
	c := s.dial()
	c.nc.Write(handshakeFrame(t, s, func(f []byte) { binary.BigEndian.PutUint16(f[6:], 2) }))
	if !c.closedByServer() {
		t.Fatal("a handshake for the wrong key was answered")
	}
	waitFor(t, "failed handshake counted", func() bool { return testutil.ToFloat64(s.tr.hs.WithLabelValues("failed")) == 1 })
}

func TestHandshakeRateRejects(t *testing.T) {
	s := newServer(t, func(_ *gateway.Config, tc *Config) { tc.HandshakeRate, tc.HandshakeBurst = 0.001, 1 })
	s.hello() // the one token
	c := s.dial()
	c.nc.Write(handshakeFrame(t, s, nil))
	if !c.closedByServer() {
		t.Fatal("second handshake answered with an empty bucket")
	}
	waitFor(t, "handshake_rate counted", func() bool { return s.rejects(gateway.RejectHandshakeRate) == 1 })
	if n := testutil.ToFloat64(s.tr.hs.WithLabelValues(noToken)); n != 1 {
		t.Fatalf("handshakes_total{no_token} = %v, want 1", n)
	}
}

// A socket over the cap is closed at accept; its close is not reported to
// the core, so the cap's count stays right.
func TestAtCapacityClosesAtAccept(t *testing.T) {
	s := newServer(t, func(c *gateway.Config, _ *Config) { c.MaxConns = 1 })
	a := s.online("ua")
	b := s.dial()
	if !b.closedByServer() || s.rejects(gateway.RejectAtCapacity) != 1 {
		t.Fatalf("second socket: at_capacity rejects = %d", s.rejects(gateway.RejectAtCapacity))
	}
	a.send(clientpb.Cmd_CMD_PING, 2, nil)
	if cmd, _ := a.recv(nil); cmd != clientpb.Cmd_CMD_PONG {
		t.Fatal("first connection broken by the reject")
	}
	a.nc.Close()
	waitFor(t, "a slot after the close", func() bool {
		c := s.dial()
		c.nc.Write(handshakeFrame(t, s, nil))
		reply := make([]byte, wire.HeaderLen+wire.HandshakeMsgLen)
		_, err := ioReadFull(c, reply)
		return err == nil
	})
}

func TestOversizedFrameIsClosedFromItsHeader(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u3")
	var h [wire.HeaderLen]byte
	wire.Header{Version: wire.Version, Cmd: uint16(clientpb.Cmd_CMD_PING), Length: 8193}.Put(h[:])
	c.nc.Write(h[:]) // the payload never comes
	if !c.closedByServer() {
		t.Fatal("not closed on an oversized header")
	}
	waitFor(t, "bad_frame counted", func() bool { return s.closes(closeBadFrame) == 1 })
}

func TestTamperedHeaderIsClosedAsDecryptFailure(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u4")
	f := c.seal(clientpb.Cmd_CMD_PING, 3, nil)
	f[2] = byte(clientpb.Cmd_CMD_LOGOUT) // Cmd changed in flight
	c.nc.Write(f)
	if !c.closedByServer() {
		t.Fatal("tampered frame accepted")
	}
	waitFor(t, "decrypt counted", func() bool { return s.closes(closeDecrypt) == 1 })
}

// A Kick arrives by gRPC and reaches the connection through Execute.
func TestKickReachesTheClient(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u5")
	id, ok := s.logic.connID("u5")
	if !ok {
		t.Fatal("no conn_id")
	}
	kickAddr, _ := s.g.Addrs()
	cc, err := grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	if _, err := pb.NewGatewayClient(cc).Kick(context.Background(), &pb.KickRequest{Gw: s.ref, ConnId: id,
		Reason: pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}); err != nil {
		t.Fatal(err)
	}
	var k clientpb.Kick
	if cmd, _ := c.recv(&k); cmd != clientpb.Cmd_CMD_KICK || k.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("got %v %v", cmd, &k)
	}
	if !c.closedByServer() {
		t.Fatal("not closed after the Kick")
	}
}

// gnet closes a socket inside Write when the write fails; the core must hear
// of it only after its current call returns.
func TestCloseInsideACoreCallIsReportedAfterIt(t *testing.T) {
	s := newServer(t, nil)
	l := &loop{g: s.g}
	tc := &conn{t: s.tr, l: l}
	var reason string
	l.run(func() { tc.c, reason = s.g.Open(l, tc) })
	if reason != "" {
		t.Fatal(reason)
	}
	handshaking := func() float64 { return testutil.ToFloat64(s.m.Conns.WithLabelValues("handshake")) }
	l.run(func() {
		l.closed(tc) // as if a Send inside this call failed
		if handshaking() != 1 {
			t.Error("Closed reached the core inside its call")
		}
		tc.Send(clientpb.Cmd_CMD_PONG, 1, nil) // a no-op now
		tc.Close()
	})
	if handshaking() != 0 {
		t.Fatal("Closed not reported after the call")
	}
}

// A logged-in client that pipelines Pings and never reads must not grow the
// gateway's outbound buffer without bound: past MaxOutbound the connection
// is closed as slow_consumer.
func TestSlowConsumerIsClosedAtTheOutboundCap(t *testing.T) {
	s := newServer(t, func(_ *gateway.Config, tc *Config) { tc.MaxOutbound = wire.HeaderLen + wire.MaxLength })
	c := s.online("u1")
	deadline := time.Now().Add(8 * time.Second)
	for id := uint32(10); s.closes(closeSlowConsumer) == 0; {
		if time.Now().After(deadline) {
			t.Fatal("never closed as slow_consumer")
		}
		var burst []byte
		for range 1000 {
			burst = append(burst, c.seal(clientpb.Cmd_CMD_PING, id, nil)...)
			id++
		}
		if _, err := c.nc.Write(burst); err != nil {
			break // the server closed it
		}
	}
	waitFor(t, "closes_total{slow_consumer}", func() bool { return s.closes(closeSlowConsumer) == 1 })
	waitFor(t, "fenced Logout reaches Logic", func() bool { return s.logic.loggedOut("u1") })
}

// P4I-1 on real gnet: a socket closed at accept is a pre-handshake close
// for the client, whether its read of the reply sees EOF or a reset. (Its
// one write can't fail: a reset only answers data it already sent.)
func TestRejectAtAcceptIsAPreHandshakeCloseForTheClient(t *testing.T) {
	s := newServer(t, func(c *gateway.Config, _ *Config) { c.MaxConns = 1 })
	s.online("ua")
	for i := range 50 {
		nc, err := net.Dial("tcp", s.tr.Addr())
		if err != nil {
			t.Fatal(err)
		}
		nc.SetDeadline(time.Now().Add(5 * time.Second))
		if i%2 == 1 {
			time.Sleep(5 * time.Millisecond) // the close lands before the write
		}
		_, err = wire.ClientHandshake(nc, 1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		nc.Close()
		if !errors.Is(err, wire.ErrClosedBeforeReply) {
			t.Fatalf("attempt %d: err %v, want ErrClosedBeforeReply", i, err)
		}
	}
}
