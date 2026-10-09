package sdk

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/protobuf/proto"
)

// fakeGW is a scripted gateway on net.Pipe, so a whole client runs inside a
// synctest bubble on fake time. serve runs once per connection (n counts
// from 1) on the connection's own goroutine.
type fakeGW struct {
	t     *testing.T
	key   wire.Key
	serve func(n int, c *fakeConn)

	mu    sync.Mutex
	n     int
	addrs []string
}

func newFake(t *testing.T, serve func(n int, c *fakeConn)) *fakeGW {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return &fakeGW{t: t, key: key, serve: serve}
}

func (f *fakeGW) dial(_ context.Context, addr string) (net.Conn, error) {
	client, server := net.Pipe()
	f.mu.Lock()
	f.n++
	n := f.n
	f.addrs = append(f.addrs, addr)
	f.mu.Unlock()
	go func() {
		defer server.Close()
		f.serve(n, &fakeConn{t: f.t, nc: server, key: f.key})
	}()
	return client, nil
}

// dials is the number of connections the client opened.
func (f *fakeGW) dials() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// fakeConn is the server side of one connection.
type fakeConn struct {
	t    *testing.T
	nc   net.Conn
	key  wire.Key
	sess *wire.Session
}

// hello reads frame 1 without answering it.
func (c *fakeConn) hello() bool {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	_, err := io.ReadFull(c.nc, f)
	return err == nil
}

// login answers the handshake and returns the LoginReq, or nil.
func (c *fakeConn) login() *clientpb.LoginReq {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	if _, err := io.ReadFull(c.nc, f); err != nil {
		return nil
	}
	sess, reply, err := wire.Respond(c.key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		c.t.Error(err)
		return nil
	}
	c.sess = sess
	if _, err := c.nc.Write(reply); err != nil {
		return nil
	}
	cmd, _, body, err := sess.ReadFrame(c.nc)
	if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN {
		return nil
	}
	var req clientpb.LoginReq
	if err := proto.Unmarshal(body, &req); err != nil {
		c.t.Error(err)
		return nil
	}
	return &req
}

// online logs the client in with resp (nil: heartbeat 150s, no windows).
func (c *fakeConn) online(resp *clientpb.LoginResp) *clientpb.LoginReq {
	req := c.login()
	if req == nil {
		return nil
	}
	if resp == nil {
		resp = &clientpb.LoginResp{HeartbeatS: 150}
	}
	c.send(clientpb.Cmd_CMD_LOGIN_RESP, 1, resp)
	return req
}

func (c *fakeConn) send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	var body []byte
	if msg != nil {
		body, _ = proto.Marshal(msg)
	}
	c.sess.WriteFrame(c.nc, uint16(cmd), reqID, body)
}

// recv reads one client frame.
func (c *fakeConn) recv() (clientpb.Cmd, error) {
	cmd, _, _, err := c.sess.ReadFrame(c.nc)
	return clientpb.Cmd(cmd), err
}

// pong answers every Ping until the client closes.
func (c *fakeConn) pong() {
	for {
		cmd, id, _, err := c.sess.ReadFrame(c.nc)
		if err != nil {
			return
		}
		if clientpb.Cmd(cmd) == clientpb.Cmd_CMD_PING {
			c.sess.WriteFrame(c.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	}
}

// drain reads and drops client frames until the client closes.
func (c *fakeConn) drain() { io.Copy(io.Discard, c.nc) }

// rec records events.
type rec struct {
	mu  sync.Mutex
	evs []Event
	got int // events already returned by take
}

func (r *rec) add(e Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// take waits until the bubble is idle, then returns the events since the
// last take.
func (r *rec) take() []Event {
	synctest.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := r.evs[r.got:]
	r.got = len(r.evs)
	return evs
}

// testClient starts a client against f inside the bubble; it is closed when
// the bubble's function returns.
func testClient(t *testing.T, f *fakeGW, store *MemStore, mod func(*Config)) (*Client, *rec) {
	t.Helper()
	r := &rec{}
	cfg := DefaultConfig()
	cfg.Addrs, cfg.KeyID, cfg.ServerKey = []string{"gw-a:8000"}, 1, f.key.Public
	cfg.AppID, cfg.Platform = "app", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return "tok", nil }
	cfg.Store, cfg.Dial, cfg.Rand, cfg.OnEvent = store, f.dial, seeded(), r.add
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, r
}

// kinds lists the events' kinds, for failure messages and comparisons.
func kinds(evs []Event) []EventKind {
	out := make([]EventKind, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

// expect fails t unless evs have exactly these kinds, and returns them.
func expect(t *testing.T, evs []Event, want ...EventKind) []Event {
	t.Helper()
	got := kinds(evs)
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	return evs
}
