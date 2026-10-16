package gateway

import (
	"context"
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
			// A pending Kick proves Logic wrote this session and replaced it;
			// a Busy retry would evict the device that replaced it.
			if c.kickPending != clientpb.KickReason_KICK_REASON_UNSPECIFIED {
				g.kickConn(c, c.kickPending, "kick")
			} else {
				g.busy(c, c.loginReqID)
			}
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
	if c.reconnected {
		return // a Login completing as the drain starts is seen by both online() and the drain's pass
	}
	c.reconnected = true
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}

// ---- batching and the Login RPC ----

// batch collects items from in into slices of up to size, sending each when
// it is full or linger after its first item (spec: Login batching).
func batch[T any](ctx context.Context, in <-chan T, out chan<- []T, size int, linger time.Duration) {
	var b []T
	t := time.NewTimer(linger)
	t.Stop()
	flush := func() bool {
		t.Stop()
		select {
		case out <- b:
			b = nil
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case it := <-in:
			b = append(b, it)
			if len(b) == 1 {
				t.Reset(linger)
			}
			if len(b) >= size && !flush() {
				return
			}
		case <-t.C:
			if len(b) > 0 && !flush() {
				return
			}
		}
	}
}

// startLogin runs the Login batcher and LoginRPCs workers.
func (g *Gateway) startLogin(ctx context.Context, logic pb.LogicClient) {
	work := make(chan []*loginItem)
	g.goRun(func() { batch(ctx, g.loginCh, work, g.cfg.LoginBatch, g.cfg.LoginLinger) })
	for range g.cfg.LoginRPCs {
		g.goRun(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b := <-work:
					g.sendLogins(ctx, logic, b)
				}
			}
		})
	}
}

// sendLogins sends one batch and hands each entry's reply to its loop as it
// arrives (R5S-O1). An entry with no result by recv_ms + LoginTimeout, or when
// the stream ends, has no definitive reply (spec: Login batching).
func (g *Gateway) sendLogins(ctx context.Context, logic pb.LogicClient, items []*loginItem) {
	nowMs := g.now().UnixMilli()
	live := make([]*loginItem, 0, len(items))
	entries := make([]*pb.LoginEntry, 0, len(items))
	var latest int64
	for _, it := range items {
		if nowMs-it.entry.RecvMs > g.cfg.LoginDeadline.Milliseconds() {
			// Dropped before it was sent: it never reaches Logic, so it needs no Logout.
			if it.state.CompareAndSwap(itemQueued, itemDropped) {
				g.loginFinished()
				g.post(it, loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_EXPIRED, label: "expired_queued"})
			}
			continue
		}
		if !it.state.CompareAndSwap(itemQueued, itemSent) {
			continue // cancelled: the loop released the slot
		}
		live = append(live, it)
		entries = append(entries, it.entry)
		latest = max(latest, it.entry.RecvMs)
	}
	if len(live) == 0 {
		return
	}
	for _, it := range live {
		g.m.LoginQueueSeconds.Observe(float64(nowMs-it.entry.RecvMs) / 1000)
	}
	var sentMs int64
	resolved := make([]bool, len(live))
	left := len(live)
	resolve := func(i int, r loginReply) {
		if !resolved[i] {
			resolved[i] = true
			left--
			if r.outcome != loginUnknown {
				g.m.LoginLogicSeconds.Observe(float64(g.now().UnixMilli()-sentMs) / 1000)
			}
			g.loginFinished()
			g.post(live[i], r)
		}
	}
	defer func() {
		for i := range live {
			resolve(i, unknownReply)
		}
	}()

	until := func(recvMs int64) time.Duration {
		return time.Duration(recvMs+g.cfg.LoginTimeout.Milliseconds()-g.now().UnixMilli()) * time.Millisecond
	}
	cctx, cancel := g.rpcCtx(ctx, until(latest))
	defer cancel()
	sentMs = g.now().UnixMilli()
	stream, err := logic.Login(cctx, &pb.LoginBatch{Gw: g.ref, Entries: entries})
	if err != nil {
		return
	}
	results := make(chan *pb.LoginResult)
	go func() {
		defer close(results)
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case results <- r:
			case <-cctx.Done():
				return
			}
		}
	}()
	t := time.NewTimer(0)
	defer t.Stop()
	for left > 0 {
		next := time.Duration(-1)
		for i, it := range live {
			if resolved[i] {
				continue
			}
			if d := until(it.entry.RecvMs); d <= 0 {
				resolve(i, unknownReply) // per-entry timeout (R5S-O1)
			} else if next < 0 || d < next {
				next = d
			}
		}
		if left == 0 {
			return
		}
		t.Reset(next)
		select {
		case r, ok := <-results:
			if !ok {
				return // the stream ended: the rest have no definitive reply
			}
			if i := int(r.GetIndex()); i < len(live) {
				resolve(i, replyFromPB(r))
			}
		case <-t.C:
		}
	}
}

func (g *Gateway) post(it *loginItem, r loginReply) {
	_ = it.ls.loop().Execute(func() { g.applyLogin(it, r) }) // fails only at shutdown
}

var rejectCodes = map[pb.LoginCode]struct {
	code  clientpb.ErrorCode
	label string
}{
	pb.LoginCode_LOGIN_CODE_STALE:         {clientpb.ErrorCode_ERROR_CODE_STALE, "stale"},
	pb.LoginCode_LOGIN_CODE_SUPERSEDED:    {clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, "superseded"},
	pb.LoginCode_LOGIN_CODE_EXPIRED:       {clientpb.ErrorCode_ERROR_CODE_EXPIRED, "expired"},
	pb.LoginCode_LOGIN_CODE_REVOKED:       {clientpb.ErrorCode_ERROR_CODE_REVOKED, "revoked"},
	pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN:   {clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN, "key_unknown"},
	pb.LoginCode_LOGIN_CODE_TOKEN_INVALID: {clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID, "token_invalid"},
	pb.LoginCode_LOGIN_CODE_BAD_REQUEST:   {clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST, "bad_request"},
}

// replyFromPB maps Logic's result. OUTCOME_UNKNOWN, and any code this gateway
// doesn't know, is not a definitive reply.
func replyFromPB(r *pb.LoginResult) loginReply {
	if r.GetCode() == pb.LoginCode_LOGIN_CODE_OK && r.GetUid() != "" {
		return loginReply{outcome: loginOK, label: "ok", uid: r.GetUid(), tokIatMs: r.GetTokIatMs()}
	}
	if rc, ok := rejectCodes[r.GetCode()]; ok {
		return loginReply{outcome: loginRejected, code: rc.code, label: rc.label, storedConnSeq: r.GetStoredConnSeq()}
	}
	return unknownReply
}
