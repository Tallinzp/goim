package racetest

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/clientconn"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// Client is one scripted connection (internal/clientconn) to gateway GW.
type Client struct {
	*clientconn.Conn
	t   *testing.T
	GW  int
	seq uint32 // req_id
}

// Dial connects to gateway i and runs the Noise handshake.
func (s *Stack) Dial(i int) *Client {
	s.t.Helper()
	c, err := clientconn.Dial(s.GW[i].Listen, 5*time.Second)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, s.noise.Public, time.Now().Add(10*time.Second)); err != nil {
		s.t.Fatalf("handshake with %s: %v", s.GW[i].ID, err)
	}
	return &Client{Conn: c, t: s.t, GW: i}
}

// TokenAt signs a token for uid issued at iatMs and valid for ttl.
func (s *Stack) TokenAt(uid string, iatMs int64, ttl time.Duration) string {
	return s.TokenWith(s.jwtKey, uid, "mobile", iatMs, ttl)
}

// TokenWith signs a token for uid's platform with key.
func (s *Stack) TokenWith(key auth.PrivateKey, uid, platform string, iatMs int64, ttl time.Duration) string {
	s.t.Helper()
	tok, err := auth.Sign(key, auth.Claims{UID: uid, AppID: "im", Platform: platform,
		IatMs: iatMs, ExpMs: iatMs + ttl.Milliseconds()})
	if err != nil {
		s.t.Fatal(err)
	}
	return tok
}

// Token signs a token for uid issued now, valid for an hour.
func (s *Stack) Token(uid string) string { return s.TokenAt(uid, time.Now().UnixMilli(), time.Hour) }

// Req is a LoginReq for uid's im:mobile slot from device with conn_seq seq.
func (s *Stack) Req(uid, device string, seq uint64) *clientpb.LoginReq {
	return &clientpb.LoginReq{Token: s.Token(uid), Appid: "im", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: device, ConnSeq: seq, ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// Send sends one frame.
func (c *Client) Send(cmd clientpb.Cmd, msg proto.Message) {
	c.t.Helper()
	c.seq++
	if err := c.Conn.Send(cmd, c.seq, msg); err != nil {
		c.t.Fatalf("send %v: %v", cmd, err)
	}
}

// Next reads the next frame within d.
func (c *Client) Next(d time.Duration) clientconn.Frame {
	c.t.Helper()
	f, err := c.Recv(time.Now().Add(d))
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return f
}

// Login sends req and returns the gateway's answer (LoginResp, Error, Busy
// or Kick) within 10s.
func (c *Client) Login(req *clientpb.LoginReq) clientconn.Frame {
	c.t.Helper()
	c.Send(clientpb.Cmd_CMD_LOGIN, req)
	return c.Next(10 * time.Second)
}

// Online dials gateway i and logs uid in from device with conn_seq seq; it
// fails t unless the answer is LoginResp.
func (s *Stack) Online(i int, uid, device string, seq uint64) *Client {
	s.t.Helper()
	return s.OnlineWith(i, s.Req(uid, device, seq))
}

// OnlineWith dials gateway i and logs in with req; it fails t unless the
// answer is LoginResp.
func (s *Stack) OnlineWith(i int, req *clientpb.LoginReq) *Client {
	s.t.Helper()
	c := s.Dial(i)
	if f := c.Login(req); f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		s.t.Fatalf("%s on %s: got %v %s, want LoginResp", tokenUID(req), s.GW[i].ID, f.Cmd, describe(f))
	}
	return c
}

// tokenUID reads the uid from a LoginReq's token, for messages.
func tokenUID(req *clientpb.LoginReq) string {
	parts := strings.Split(req.GetToken(), ".")
	if len(parts) != 3 {
		return "?"
	}
	b, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct{ Sub string }
	json.Unmarshal(b, &c)
	return c.Sub
}

// ExpectKick waits up to d for a Kick with reason, then the close.
func (c *Client) ExpectKick(reason clientpb.KickReason, d time.Duration) {
	c.t.Helper()
	f := c.Next(d)
	var k clientpb.Kick
	if f.Cmd != clientpb.Cmd_CMD_KICK || f.Decode(&k) != nil || k.Reason != reason {
		c.t.Fatalf("got %v %s, want Kick %v", f.Cmd, describe(f), reason)
	}
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err != nil {
		c.t.Fatal(err)
	}
}

// Quiet checks that nothing arrives and the connection stays open for d.
func (c *Client) Quiet(d time.Duration) {
	c.t.Helper()
	if f, err := c.Recv(time.Now().Add(d)); err == nil {
		c.t.Fatalf("got %v %s, want nothing", f.Cmd, describe(f))
	} else if clientconn.IsClosed(err) {
		c.t.Fatal("connection closed, want it open")
	}
}

// describe decodes a frame's body for failure messages.
func describe(f clientconn.Frame) string {
	var m proto.Message
	switch f.Cmd {
	case clientpb.Cmd_CMD_ERROR:
		m = &clientpb.Error{}
	case clientpb.Cmd_CMD_KICK:
		m = &clientpb.Kick{}
	case clientpb.Cmd_CMD_BUSY:
		m = &clientpb.Busy{}
	case clientpb.Cmd_CMD_RECONNECT:
		m = &clientpb.Reconnect{}
	case clientpb.Cmd_CMD_LOGIN_RESP:
		m = &clientpb.LoginResp{}
	default:
		return ""
	}
	if f.Decode(m) != nil {
		return "(undecodable)"
	}
	return "{" + prototext.MarshalOptions{}.Format(m) + "}"
}

// errorCode returns the code of an Error frame, or fails t.
func errorCode(t *testing.T, f clientconn.Frame) clientpb.ErrorCode {
	t.Helper()
	var e clientpb.Error
	if f.Cmd != clientpb.Cmd_CMD_ERROR || f.Decode(&e) != nil {
		t.Fatalf("got %v %s, want Error", f.Cmd, describe(f))
	}
	return e.Code
}
