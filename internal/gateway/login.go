package gateway

import (
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/protobuf/proto"
)

// loginReply is what a Login worker learned about one entry.
type loginReply struct {
	outcome       outcome // loginOK, loginRejected or loginUnknown
	code          clientpb.ErrorCode
	label         string // logins_total result
	uid           string
	tokIatMs      int64
	storedConnSeq uint64
}

// unknownReply is a Login with no definitive reply: fence (b) applies.
var unknownReply = loginReply{outcome: loginUnknown, label: "error"}

// loginFrame admits a LoginReq (spec: Login-queue cap, recv_ms) and queues it
// for the batcher.
func (g *Gateway) loginFrame(c *Conn, reqID uint32, body []byte) {
	var req clientpb.LoginReq
	if err := proto.Unmarshal(body, &req); err != nil {
		g.closeConn(c, "protocol")
		return
	}
	c.ls.wheel.stop(&c.life) // AUTH_PENDING ends at LoginReq; login_deadline governs from here (R4H-O7)
	c.loginReqID = reqID
	if g.loginQueued.Add(1) > int64(g.cfg.LoginQueueCap) {
		g.loginQueued.Add(-1)
		g.reject(RejectLoginQueue)
		g.m.Logins.WithLabelValues("busy").Inc()
		g.busy(c, reqID)
		return
	}
	g.m.LoginQueue.Inc()
	now := g.now()
	c.recvMs = now.UnixMilli()
	c.appid, c.platform = req.GetAppid(), pb.Platform(req.GetPlatform())
	c.deviceID, c.connSeq = req.GetDeviceId(), req.GetConnSeq()
	c.reason, c.offlineMs = req.GetReconnectReason(), req.GetOfflineMs()
	it := &loginItem{ls: c.ls, connID: c.id, entry: &pb.LoginEntry{
		ConnId: c.id, Token: req.GetToken(), Appid: c.appid, Platform: c.platform,
		DeviceId: c.deviceID, ConnSeq: c.connSeq, RecvMs: c.recvMs}}
	c.login, c.outcome = it, loginPending
	c.ls.wheel.schedule(&c.fence, now.Add(g.cfg.FenceAfter()), c.onFence)
	select {
	case g.loginCh <- it:
	default: // unreachable: the channel holds LoginQueueCap items
		it.state.Store(itemCancelled)
		c.login, c.outcome = nil, loginNone
		g.loginFinished()
		g.busy(c, reqID)
	}
}

// loginFinished releases one Login-queue slot.
func (g *Gateway) loginFinished() {
	g.loginQueued.Add(-1)
	g.m.LoginQueue.Dec()
}

// applyLogin runs on the loop with a worker's reply for it.
func (g *Gateway) applyLogin(it *loginItem, r loginReply) {
	c := it.ls.conns[it.connID]
	if c == nil || c.login != it || c.outcome != loginPending {
		return
	}
	g.m.Logins.WithLabelValues(r.label).Inc()
	open := c.state == stateAuthPending && !c.closing
	switch r.outcome {
	case loginOK: // fence (a)
		c.outcome, c.login, c.uid, c.tokIatMs = loginOK, nil, r.uid, r.tokIatMs
		c.ls.wheel.stop(&c.fence)
		if open {
			if c.kickPending != clientpb.KickReason_KICK_REASON_UNSPECIFIED {
				g.kickConn(c, c.kickPending, "kick")
			} else {
				g.online(c)
			}
		}
	case loginRejected: // fence (a); nothing was written
		c.outcome, c.login = loginRejected, nil
		c.ls.wheel.stop(&c.fence)
		if open {
			g.send(c, clientpb.Cmd_CMD_ERROR, c.loginReqID, &clientpb.Error{Code: r.code, StoredConnSeq: r.storedConnSeq})
			g.closeConn(c, "login_rejected")
		}
	default: // no definitive reply: the Logout waits for fence (b)
		c.outcome = loginUnknown
		if open {
			g.busy(c, c.loginReqID)
		}
	}
	g.settle(c)
}

// online completes a Login: LoginResp, then ONLINE with the heartbeat armed.
func (g *Gateway) online(c *Conn) {
	now := g.now()
	g.setState(c, stateOnline)
	c.lastActive = now.UnixMilli()
	g.send(c, clientpb.Cmd_CMD_LOGIN_RESP, c.loginReqID, g.loginResp())
	c.ls.wheel.schedule(&c.life, now.Add(g.cfg.DeadAfter()), c.onLife)
	took := float64(now.UnixMilli()-c.recvMs) / 1000
	g.m.LoginSeconds.Observe(took)
	g.m.ReloginSeconds.WithLabelValues(c.reason.String()).Observe(float64(c.offlineMs)/1000 + took)
	if g.draining.Load() {
		g.reconnect(c)
	}
}

func (g *Gateway) loginResp() *clientpb.LoginResp {
	w := g.cfg.Windows
	return &clientpb.LoginResp{HeartbeatS: uint32(g.cfg.Heartbeat / time.Second),
		StormWait: w.StormWait.pb(), LocalTrigger: w.LocalTrigger.pb(),
		PreHandshakeRetry: w.PreHandshakeRetry.pb(), Retryable: w.Retryable.pb()}
}

func (w Window) pb() *clientpb.Window {
	if w == (Window{}) {
		return nil // absent: the SDK keeps its current value
	}
	return &clientpb.Window{LoMs: uint32(w.Lo.Milliseconds()), HiMs: uint32(w.Hi.Milliseconds())}
}

// reconnect sends Reconnect{jitter_ms} (spec: Graceful drain); the client
// closes and reconnects elsewhere.
func (g *Gateway) reconnect(c *Conn) {
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}
