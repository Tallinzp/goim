package tcp

import (
	"context"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/panjf2000/gnet/v2"
)

// loop is the transport's share of one gnet event loop: the gateway.Loop the
// core binds to, and scratch buffers for sealing and opening frames. Every
// field is touched only on the loop's goroutine.
//
// It also guards against re-entry. gnet closes a socket synchronously when a
// write fails (conn.write calls loop.close, which calls OnClose), so a
// Link.Send from inside the core could otherwise call Gateway.Closed while
// the core is still in the middle of a call for that connection. run counts
// how deep the loop is inside the core; a close inside it is queued and
// reported once the outermost call returns.
type loop struct {
	el     gnet.EventLoop
	g      *gateway.Gateway
	depth  int
	queued []*conn // closed by gnet inside a core call, not reported yet
	in     []byte  // opened frames: req_id || body
	out    []byte  // sealed frames
}

// Execute runs fn on the loop (gateway.Loop). gnet does not keep the order
// of two Execute calls; the core never relies on it.
func (l *loop) Execute(fn func()) error {
	return l.el.Execute(context.Background(), gnet.RunnableFunc(func(context.Context) error {
		l.run(fn)
		return nil
	}))
}

// run calls into the core, then reports the closes queued meanwhile.
func (l *loop) run(fn func()) {
	l.depth++
	fn()
	l.depth--
	if l.depth > 0 {
		return
	}
	for len(l.queued) > 0 {
		batch := l.queued
		l.queued = nil
		l.depth++
		for _, c := range batch {
			l.g.Closed(c.c)
		}
		l.depth--
	}
}

// closed is gnet's OnClose for c: from now on c's Link is a no-op, and the
// core hears of the close once it is not inside a call.
func (l *loop) closed(c *conn) {
	c.gc = nil
	l.queued = append(l.queued, c)
	if l.depth == 0 {
		l.run(func() {})
	}
}
