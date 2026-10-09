// Package clientconn is one blocking client connection to a gateway: dial,
// the Noise NK handshake, and encrypted frames carrying protobuf bodies
// (spec: Client protocol). It is the scripted test client (any frame, at any
// time, then a FIN or a reset), and the transport under the Go SDK
// (internal/sdk) and Plan 6's loadgen. It adds no policy: no retries, no
// timers, no Ping.
package clientconn

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/protobuf/proto"
)

// ErrClosedBeforeReply is wire.ErrClosedBeforeReply: the server closed or
// reset the connection before any byte of its handshake reply, the SDK's
// pre-handshake close (spec: Client SDK rules).
var ErrClosedBeforeReply = wire.ErrClosedBeforeReply

// ErrNoHandshake is Send or Recv on a Conn whose Handshake has not
// succeeded (P5I-5).
var ErrNoHandshake = errors.New("clientconn: no handshake")

// Frame is one decrypted server frame.
type Frame struct {
	Cmd   clientpb.Cmd
	ReqID uint32
	Body  []byte
}

// Decode unmarshals the body into msg.
func (f Frame) Decode(msg proto.Message) error { return proto.Unmarshal(f.Body, msg) }

// Conn is one connection. Send may be called from one goroutine while Recv
// runs on another (wire.Session's directions share no state, P4I-4); Send
// itself is serialized, so several goroutines may send.
type Conn struct {
	nc   net.Conn
	sess *wire.Session
	wmu  sync.Mutex
}

// New wraps a connected net.Conn (a TCP socket, or net.Pipe in tests). Call
// Handshake before any other method.
func New(nc net.Conn) *Conn { return &Conn{nc: nc} }

// Dial connects to addr with a new socket, so each attempt gets a new source
// port and the NLB's flow hash can pick another gateway (R2-O2).
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return New(nc), nil
}

// Handshake runs Noise NK against the server key pinned under keyID. It
// must finish by deadline (zero: none). A close or reset before any reply
// byte is reported as errors.Is(err, ErrClosedBeforeReply).
func (c *Conn) Handshake(keyID uint16, serverPub []byte, deadline time.Time) error {
	if err := c.nc.SetDeadline(deadline); err != nil {
		return err
	}
	sess, err := wire.ClientHandshake(c.nc, keyID, serverPub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return err
	}
	c.sess = sess
	return c.nc.SetDeadline(time.Time{})
}

// Send seals and writes one frame; msg nil sends an empty body.
func (c *Conn) Send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) error {
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			return err
		}
	}
	if c.sess == nil {
		return ErrNoHandshake
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.sess.WriteFrame(c.nc, uint16(cmd), reqID, body)
}

// SendRaw writes bytes as they are, e.g. a tampered or partial frame.
func (c *Conn) SendRaw(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.nc.Write(b)
	return err
}

// Recv reads one frame. A read past deadline (zero: none) fails with an
// error matching os.ErrDeadlineExceeded.
func (c *Conn) Recv(deadline time.Time) (Frame, error) {
	if c.sess == nil {
		return Frame{}, ErrNoHandshake
	}
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return Frame{}, err
	}
	cmd, reqID, body, err := c.sess.ReadFrame(c.nc)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Cmd: clientpb.Cmd(cmd), ReqID: reqID, Body: body}, nil
}

// SetReadDeadline changes the deadline of a Recv in progress, e.g. to arm
// the Pong timeout while another goroutine waits in Recv.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.nc.SetReadDeadline(t) }

// Close closes the socket with a FIN.
func (c *Conn) Close() error { return c.nc.Close() }

// Reset closes the socket with a RST (SO_LINGER 0), as a crashed client or a
// dropped NAT mapping looks to the server. On a non-TCP conn it is Close.
func (c *Conn) Reset() error {
	if tc, ok := c.nc.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	return c.nc.Close()
}

// LocalAddr is the socket's local address (its source port).
func (c *Conn) LocalAddr() net.Addr { return c.nc.LocalAddr() }

// ExpectClosed waits until deadline for the server to close the
// connection. It fails if a frame or any byte arrives first, or on timeout.
func (c *Conn) ExpectClosed(deadline time.Time) error {
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return err
	}
	var b [1]byte
	n, err := c.nc.Read(b[:])
	switch {
	case n > 0:
		return errors.New("clientconn: got bytes, want a close")
	case IsClosed(err):
		return nil
	default:
		return fmt.Errorf("clientconn: want a close: %w", err)
	}
}

// IsClosed reports whether err means the peer closed (FIN) or reset the
// connection, or it was closed locally. A write after the reset was read
// fails with EPIPE (P5I-6).
func IsClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe)
}
