package clientconn

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
)

// peer is the server side of one test connection: a raw socket, and the
// Noise session once serve has answered the handshake.
type peer struct {
	nc   net.Conn
	sess *wire.Session
}

// listen starts a server that hands every accepted socket to serve on its
// own goroutine, and returns its address and key.
func listen(t *testing.T, serve func(*peer, wire.Key)) (string, wire.Key) {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { l.Close(); wg.Wait() })
	wg.Go(func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer nc.Close()
				nc.SetDeadline(time.Now().Add(10 * time.Second))
				serve(&peer{nc: nc}, key)
			})
		}
	})
	return l.Addr().String(), key
}

// respond reads frame 1 and answers the handshake.
func (p *peer) respond(key wire.Key) error {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	if _, err := io.ReadFull(p.nc, f); err != nil {
		return err
	}
	sess, reply, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return err
	}
	p.sess = sess
	_, err = p.nc.Write(reply)
	return err
}

func dial(t *testing.T, addr string, key wire.Key) *Conn {
	t.Helper()
	c, err := Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, key.Public, time.Now().Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSendAndRecvAfterTheHandshake(t *testing.T) {
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			cmd, id, body, err := p.sess.ReadFrame(p.nc)
			if err != nil {
				return
			}
			// Echo: a Ping gets a Pong, a LoginReq comes back as LoginResp
			// with the body unchanged (the test only checks it decodes).
			reply := clientpb.Cmd_CMD_PONG
			if clientpb.Cmd(cmd) == clientpb.Cmd_CMD_LOGIN {
				reply, body = clientpb.Cmd_CMD_LOGIN_RESP, nil
			}
			p.sess.WriteFrame(p.nc, uint16(reply), id, body)
		}
	})
	c := dial(t, addr, key)
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "t", DeviceId: "d", ConnSeq: 3}); err != nil {
		t.Fatal(err)
	}
	f, err := c.Recv(time.Now().Add(5 * time.Second))
	var resp clientpb.LoginResp
	if err != nil || f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP || f.ReqID != 1 || f.Decode(&resp) != nil {
		t.Fatalf("got %+v, %v; want LoginResp 1", f, err)
	}
	if err := c.Send(clientpb.Cmd_CMD_PING, 7, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Recv(time.Now().Add(5 * time.Second)); err != nil || f.Cmd != clientpb.Cmd_CMD_PONG || f.ReqID != 7 {
		t.Fatalf("got %+v, %v; want Pong 7", f, err)
	}
}

// A pre-handshake close (spec: Client SDK rules) is told apart from a
// handshake that fails after the server answered.
func TestHandshakeReportsAPreHandshakeClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve func(*peer, wire.Key)
		want  error
	}{
		{"FIN", func(p *peer, _ wire.Key) {}, ErrClosedBeforeReply},
		{"RST", func(p *peer, _ wire.Key) {
			io.ReadFull(p.nc, make([]byte, wire.HeaderLen+wire.HandshakeLen)) // the reset must answer data
			p.nc.(*net.TCPConn).SetLinger(0)
		}, ErrClosedBeforeReply},
		{"garbage reply", func(p *peer, _ wire.Key) {
			io.ReadFull(p.nc, make([]byte, wire.HeaderLen+wire.HandshakeLen))
			p.nc.Write(make([]byte, wire.HeaderLen+wire.HandshakeMsgLen))
		}, wire.ErrHandshake},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, key := listen(t, tc.serve)
			c, err := Dial(addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.Handshake(1, key.Public, time.Now().Add(5*time.Second)); !errors.Is(err, tc.want) {
				t.Fatalf("Handshake = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestHandshakeHonoursItsDeadline(t *testing.T) {
	addr, key := listen(t, func(p *peer, _ wire.Key) { io.Copy(io.Discard, p.nc) }) // never answers
	c, err := Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Handshake(1, key.Public, time.Now().Add(100*time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Handshake = %v, want a deadline error", err)
	}
}

// The server tells a client's FIN from its reset; the race suite uses both.
func TestCloseSendsFINAndResetSendsRST(t *testing.T) {
	got := make(chan error, 2)
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		_, err := p.nc.Read(make([]byte, 1))
		got <- err
	})
	dial(t, addr, key).Close()
	if err := <-got; !errors.Is(err, io.EOF) {
		t.Fatalf("after Close the server read %v, want EOF", err)
	}
	dial(t, addr, key).Reset()
	if err := <-got; !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("after Reset the server read %v, want ECONNRESET", err)
	}
}

func TestExpectClosed(t *testing.T) {
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			cmd, id, _, err := p.sess.ReadFrame(p.nc)
			if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_PING {
				return // a Logout: close at once
			}
			p.sess.WriteFrame(p.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	})
	c := dial(t, addr, key)
	c.Send(clientpb.Cmd_CMD_LOGOUT, 1, nil)
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("ExpectClosed after a server close = %v", err)
	}
	c = dial(t, addr, key)
	c.Send(clientpb.Cmd_CMD_PING, 1, nil)
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err == nil {
		t.Fatal("ExpectClosed passed although a Pong arrived")
	}
	c = dial(t, addr, key)
	if err := c.ExpectClosed(time.Now().Add(50 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ExpectClosed on an open connection = %v, want a timeout", err)
	}
}

// Send is serialized, and may run while another goroutine waits in Recv
// (run under -race).
func TestSendFromSeveralGoroutinesWhileReceiving(t *testing.T) {
	const n = 200
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			_, id, _, err := p.sess.ReadFrame(p.nc)
			if err != nil {
				return
			}
			p.sess.WriteFrame(p.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	})
	c := dial(t, addr, key)
	done := make(chan map[uint32]bool)
	go func() {
		seen := map[uint32]bool{}
		for len(seen) < n {
			f, err := c.Recv(time.Now().Add(5 * time.Second))
			if err != nil {
				break
			}
			seen[f.ReqID] = true
		}
		done <- seen
	}()
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range n / 4 {
				c.Send(clientpb.Cmd_CMD_PING, uint32(g*n/4+i), nil)
			}
		})
	}
	wg.Wait()
	if seen := <-done; len(seen) != n {
		t.Fatalf("got %d distinct Pongs, want %d (a frame was interleaved or lost)", len(seen), n)
	}
}

// Send and Recv before Handshake return ErrNoHandshake, not a panic, so a
// mistake in a race script fails the test (P5I-5). SendRaw needs no session.
func TestSendAndRecvBeforeTheHandshakeFail(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := New(client)
	defer c.Close()
	if err := c.Send(clientpb.Cmd_CMD_PING, 1, nil); !errors.Is(err, ErrNoHandshake) {
		t.Fatalf("Send: %v, want ErrNoHandshake", err)
	}
	if _, err := c.Recv(time.Now().Add(time.Second)); !errors.Is(err, ErrNoHandshake) {
		t.Fatalf("Recv: %v, want ErrNoHandshake", err)
	}
	go io.Copy(io.Discard, server)
	if err := c.SendRaw([]byte{1, 2, 3}); err != nil {
		t.Fatalf("SendRaw: %v", err)
	}
}

// A Send after the server reset the connection fails with EPIPE once the
// reset has been read, and IsClosed reports it (P5I-6).
func TestSendAfterAResetIsClosed(t *testing.T) {
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		p.nc.(*net.TCPConn).SetLinger(0)
	})
	c := dial(t, addr, key)
	if _, err := c.Recv(time.Now().Add(5 * time.Second)); !IsClosed(err) {
		t.Fatalf("Recv: %v, want a close", err)
	}
	err := c.Send(clientpb.Cmd_CMD_PING, 1, nil)
	if err == nil || !IsClosed(err) {
		t.Fatalf("Send after the reset: %v, want an error IsClosed accepts", err)
	}
}

func TestIsClosedAcceptsEveryPeerClose(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED} {
		err := &net.OpError{Op: "write", Err: os.NewSyscallError("write", errno)}
		if !IsClosed(err) {
			t.Errorf("IsClosed(%v) = false", err)
		}
	}
}
