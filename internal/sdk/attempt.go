package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
)

// endLogoutResp ends a connection the client logged out of; the policy
// never sees it.
const endLogoutResp EndKind = 100

// attempt is one connection attempt and, once it logs in, the connection.
// Its goroutine reports to the client's run loop; cancel aborts it.
type attempt struct {
	cancel     context.CancelFunc
	refresh    bool             // Token's refresh argument
	cc         *clientconn.Conn // set before the attempt reports it is online
	loggingOut atomic.Bool      // the client sent Logout: LogoutResp ends the connection
	triggered  bool             // run loop: a local trigger came before the Login was answered
}

// attemptMsg is what an attempt's goroutine reports: online (resp set),
// then an end, or only an end; note, at any time, is an anomaly to log.
type attemptMsg struct {
	a    *attempt
	resp *clientpb.LoginResp
	end  end
	note error
}

// run dials addr, logs in with req and then serves the connection until it
// ends, reporting to out.
func (a *attempt) run(ctx context.Context, cfg *Config, addr string, req *clientpb.LoginReq, out chan<- attemptMsg) {
	defer a.cancel()
	// A cancelled attempt reports nothing: the run loop has dropped it, or
	// has stopped.
	report := func(m attemptMsg) bool {
		select {
		case out <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}
	resp, e := a.login(ctx, cfg, addr, req)
	if resp == nil {
		report(attemptMsg{a: a, end: e})
		return
	}
	if report(attemptMsg{a: a, resp: resp}) {
		report(attemptMsg{a: a, end: a.serve(ctx, cfg, resp, report)})
	}
}

// login runs the attempt up to the Login reply and classifies every way it
// can end (spec: Retryable rejects).
func (a *attempt) login(ctx context.Context, cfg *Config, addr string, req *clientpb.LoginReq) (*clientpb.LoginResp, end) {
	tok, err := token(ctx, cfg, a.refresh)
	if err != nil {
		return nil, end{kind: EndFailed, err: err}
	}
	req.Token = tok
	connectBy := time.Now().Add(cfg.ConnectTimeout) // dial and handshake together
	dctx, cancel := context.WithDeadline(ctx, connectBy)
	defer cancel()
	nc, err := cfg.Dial(dctx, addr)
	if err != nil {
		return nil, end{kind: EndFailed, err: err}
	}
	context.AfterFunc(ctx, func() { nc.Close() }) // cancel aborts any read or write
	a.cc = clientconn.New(nc)
	in, frame, err := cfg.Initiate(cfg.KeyID, cfg.ServerKey)
	if err != nil {
		nc.Close()
		return nil, end{kind: EndFailed, err: err}
	}
	switch err := a.cc.HandshakeWith(in, frame, connectBy); {
	case errors.Is(err, clientconn.ErrClosedBeforeReply):
		nc.Close()
		return nil, end{kind: EndPreHandshake, err: err}
	case err != nil:
		nc.Close()
		return nil, end{kind: EndFailed, err: err}
	}
	if err := a.cc.Send(clientpb.Cmd_CMD_LOGIN, 1, req); err != nil {
		nc.Close()
		return nil, end{kind: EndNoReply, err: err}
	}
	deadline := time.Now().Add(cfg.LoginTimeout)
	for {
		f, err := a.cc.Recv(deadline)
		if err != nil { // closed, reset, timed out or undecryptable: no reply
			nc.Close()
			return nil, end{kind: EndNoReply, err: err}
		}
		if resp, e, done := loginReply(f); done {
			if resp == nil {
				nc.Close()
			}
			return resp, e
		}
	}
}

// token calls cfg.Token and stops waiting after cfg.TokenTimeout: a token
// server that hangs on a dead network must not hold the attempt, which no
// local trigger can cut short.
func token(ctx context.Context, cfg *Config, refresh bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.TokenTimeout)
	defer cancel()
	type result struct {
		tok string
		err error
	}
	got := make(chan result, 1)
	go func() {
		tok, err := cfg.Token(ctx, refresh)
		got <- result{tok, err}
	}()
	select {
	case r := <-got:
		return r.tok, r.err
	case <-ctx.Done():
		return "", fmt.Errorf("sdk: token: %w", ctx.Err())
	}
}

// loginReply classifies a frame received while waiting for the Login reply;
// done false means keep waiting.
func loginReply(f clientconn.Frame) (resp *clientpb.LoginResp, e end, done bool) {
	switch f.Cmd {
	case clientpb.Cmd_CMD_LOGIN_RESP:
		resp = &clientpb.LoginResp{}
		if err := f.Decode(resp); err != nil {
			return nil, end{kind: EndNoReply, err: err}, true
		}
		return resp, end{}, true
	case clientpb.Cmd_CMD_ERROR: // undecodable: code 0, an unknown code
		var m clientpb.Error
		err := f.Decode(&m)
		return nil, end{kind: EndRejected, code: m.GetCode(), storedSeq: m.GetStoredConnSeq(), err: err}, true
	case clientpb.Cmd_CMD_BUSY:
		var m clientpb.Busy
		err := f.Decode(&m)
		return nil, end{kind: EndBusy, retryAfter: time.Duration(m.GetRetryAfterMs()) * time.Millisecond, err: err}, true
	case clientpb.Cmd_CMD_KICK: // kick-pending: the Kick replaces the Login reply
		var m clientpb.Kick
		err := f.Decode(&m)
		return nil, end{kind: EndKicked, kick: m.GetReason(), err: err}, true
	}
	return nil, end{}, false
}

// serve runs a logged-in connection until it ends: one Ping per heartbeat
// interval from a pinger, and a close when no frame arrives within
// PongTimeout of a Ping (spec: Dead-server detection). Kick and Reconnect
// end it; the client closes on Reconnect (R4H-O3). A LogoutResp ends it
// only after the client sent Logout; any other one is reported as a note and
// ignored.
func (a *attempt) serve(ctx context.Context, cfg *Config, resp *clientpb.LoginResp, report func(attemptMsg) bool) end {
	hb, _ := cfg.Heartbeat.Of(resp)
	start := time.Now()
	var lastRecv atomic.Int64 // ns since start of the last frame received
	var pongTimedOut atomic.Bool
	p := startPinger(hb, cfg.PongTimeout, start, &lastRecv,
		func(reqID uint32) error { return a.cc.Send(clientpb.Cmd_CMD_PING, reqID, nil) },
		func() { pongTimedOut.Store(true); a.cc.Close() })
	defer p.stop()
	defer a.cc.Close() // before stop: a firing still in Send fails at once
	for {
		f, err := a.cc.Recv(time.Time{})
		if err != nil {
			if pongTimedOut.Load() {
				return end{kind: EndPongTimeout, err: err}
			}
			return end{kind: EndLost, err: err}
		}
		lastRecv.Store(int64(time.Since(start)))
		switch f.Cmd {
		case clientpb.Cmd_CMD_KICK:
			var m clientpb.Kick
			err := f.Decode(&m)
			return end{kind: EndKicked, kick: m.GetReason(), err: err}
		case clientpb.Cmd_CMD_RECONNECT:
			var m clientpb.Reconnect
			err := f.Decode(&m)
			return end{kind: EndReconnect, jitter: time.Duration(m.GetJitterMs()) * time.Millisecond, err: err}
		case clientpb.Cmd_CMD_LOGOUT_RESP:
			if a.loggingOut.Load() {
				return end{kind: endLogoutResp}
			}
			report(attemptMsg{a: a, note: errors.New("sdk: LogoutResp without a Logout")})
		}
	}
}

// tcpDial is the default Config.Dial: a new TCP socket per attempt, so each
// attempt gets a new source port (R2-O2).
func tcpDial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
