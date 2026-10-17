package tcp

import (
	"errors"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/panjf2000/gnet/v2"
)

// Close reasons this transport adds to closes_total.
const (
	closeBadFrame  = "bad_frame" // header, size or command not allowed here
	closeDecrypt   = "decrypt"   // AEAD failure: tampered, replayed or garbage
	closeHandshake = "handshake" // the Noise handshake failed
	// The client doesn't read: its unsent frames passed MaxOutbound.
	closeSlowConsumer = "slow_consumer"
)

var errBadFrame = errors.New("tcp: bad frame")

// conn is one admitted socket: the core's Link, and the frame and Noise
// state. Touched only on its loop.
type conn struct {
	t       *Transport
	l       *loop
	gc      gnet.Conn // nil once gnet closed the socket
	c       *gateway.Conn
	sess    *wire.Session // nil until the handshake completes
	closing bool          // Close requested
	held    bool          // a refusal the core holds open (#131): nothing is read
}

func (c *conn) hook(site string) {
	if c.t.onLoop != nil {
		c.t.onLoop(c, site)
	}
}

// Send seals one frame and writes it (gateway.Link).
func (c *conn) Send(cmd clientpb.Cmd, reqID uint32, body []byte) {
	if c.gc == nil || c.sess == nil {
		return
	}
	c.hook("send")
	f, err := c.sess.Seal(c.l.out[:0], uint16(cmd), reqID, body)
	if err != nil {
		c.t.log.Error("seal", "cmd", cmd, "err", err)
		return
	}
	c.l.out = f[:0]
	if c.gc.OutboundBuffered()+len(f) > c.t.cfg.MaxOutbound {
		c.t.g.Close(c.c, closeSlowConsumer)
		return
	}
	c.gc.Write(f) // gnet copies what the socket doesn't take at once
}

// Close closes the socket (gateway.Link); OnClose follows.
func (c *conn) Close() {
	if c.gc == nil || c.closing {
		return
	}
	c.closing = true
	c.hook("closereq")
	c.gc.Close()
}

// read handles every whole frame buffered, in order, until the buffer runs
// out or the connection is closing.
func (c *conn) read() {
	for c.gc != nil && !c.closing {
		_, f, err := wire.Cut(c.gc, c.check)
		if err != nil {
			if errors.Is(err, wire.ErrVersion) && c.sess == nil {
				c.t.g.Reject(c.c, gateway.RejectBadVersion)
			} else {
				c.t.g.Close(c.c, closeBadFrame)
			}
			return
		}
		if f == nil {
			return
		}
		if c.sess == nil {
			c.handshake(f)
		} else {
			c.frame(f)
		}
		if c.gc == nil {
			return
		}
		c.gc.Discard(len(f))
	}
}

// check vets a header before its payload is buffered: before the handshake
// only the handshake frame, of its exact size; after it, frames of at least
// the AEAD overhead and at most MaxClientFrame.
func (c *conn) check(h wire.Header) error {
	if err := h.Check(); err != nil {
		return err
	}
	if c.sess == nil {
		if h.Cmd != uint16(clientpb.Cmd_CMD_HANDSHAKE) || h.Length != wire.HandshakeLen {
			return errBadFrame
		}
		return nil
	}
	if int(h.Length) < wire.Overhead || int(h.Length) > c.t.cfg.MaxClientFrame {
		return errBadFrame
	}
	return nil
}

// handshake answers the client's handshake frame (spec: Noise key runbook,
// Admission control): an unknown key_id or an exhausted limiter closes the
// connection before any crypto.
func (c *conn) handshake(f []byte) {
	key, ok := c.t.keys[wire.KeyID(f)]
	if !ok {
		c.t.g.Reject(c.c, gateway.RejectNoiseUnknownKey)
		return
	}
	if why := c.t.lim.acquire(); why != "" {
		c.t.hs.WithLabelValues(why).Inc()
		c.t.g.Reject(c.c, gateway.RejectHandshakeRate)
		return
	}
	sess, reply, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	c.t.lim.release()
	if err != nil {
		c.t.hs.WithLabelValues("failed").Inc()
		c.t.g.Close(c.c, closeHandshake)
		return
	}
	c.t.hs.WithLabelValues("ok").Inc()
	c.gc.Write(reply)
	if c.gc == nil {
		return
	}
	c.sess = sess
	c.t.g.HandshakeDone(c.c)
}

// frame opens one frame and hands it to the core.
func (c *conn) frame(f []byte) {
	cmd, reqID, body, out, err := c.sess.Open(c.l.in[:0], f)
	if err != nil {
		c.t.g.Close(c.c, closeDecrypt)
		return
	}
	c.t.g.Frame(c.c, clientpb.Cmd(cmd), reqID, body) // the core copies what it keeps
	c.l.in = out[:0]
}
