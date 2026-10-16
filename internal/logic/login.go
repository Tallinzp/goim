package logic

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc"
)

// Login verifies each entry's token, runs the login Lua and streams each
// entry's result as soon as it is known (R5S-O1). Follow-ups come after the
// result: the Kick of a replaced connection (R5S-O2), then, once the batch is
// done, the gwconns SADD and the dead-epoch cross-check.
func (s *Service) Login(b *pb.LoginBatch, stream grpc.ServerStreamingServer[pb.LoginResult]) error {
	ctx := stream.Context()
	ref, err := epochRef(b.GetGw())
	if err != nil {
		return invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	var mu sync.Mutex // Send is not safe for concurrent use; LoginEach's done is concurrent
	var sendErr error
	send := func(r *pb.LoginResult) {
		if err := stream.Send(r); err != nil && sendErr == nil {
			sendErr = err
		}
	}

	entries := b.GetEntries()
	prepStart := time.Now()
	reqs := make([]store.LoginReq, 0, len(entries))
	idx := make([]int, 0, len(entries))
	for i, e := range entries {
		req, code := s.prepareLogin(ref, e)
		if code != pb.LoginCode_LOGIN_CODE_OK {
			s.m.Logins.WithLabelValues(loginLabel[code]).Inc()
			mu.Lock()
			send(&pb.LoginResult{Index: uint32(i), Code: code})
			mu.Unlock()
			continue
		}
		reqs = append(reqs, req)
		idx = append(idx, i)
	}

	// Where a Login's time goes before Redis (the slowlogic diagnostic,
	// #115 on hold): the batch's token checks, then each entry's age from
	// the gateway's recv_ms as the batch goes to the store.
	s.m.LoginPrepare.Observe(time.Since(prepStart).Seconds())
	nowMs := s.now().UnixMilli()
	for _, r := range reqs {
		s.m.LoginAge.Observe(float64(nowMs-r.Entry.LoginMs) / 1000)
	}

	var uids []string
	var prevs []replaced
	start := time.Now()
	s.st.LoginEach(ctx, reqs, func(j int, r store.LoginResult) {
		i := idx[j]
		res, kickReason := s.loginResult(ctx, r)
		res.Index = uint32(i)
		if res.Code == pb.LoginCode_LOGIN_CODE_OK {
			res.Uid, res.TokIatMs = reqs[j].UID, reqs[j].Entry.TokIatMs
		}
		mu.Lock()
		send(res)
		if res.Code == pb.LoginCode_LOGIN_CODE_OK {
			uids = append(uids, reqs[j].UID)
			if r.Prev != nil {
				prevs = append(prevs, replaced{prev: *r.Prev, loginMs: reqs[j].Entry.LoginMs})
			}
		}
		mu.Unlock()
		// The Kick is queued after the result is sent and never delays it (R5S-O2).
		if kickReason != pb.KickReason_KICK_REASON_UNSPECIFIED && r.Prev != nil {
			s.kick.Kick(store.EpochRef{Gateway: r.Prev.Gateway, Epoch: r.Prev.Epoch}, r.Prev.ConnID, kickReason)
		}
	})
	s.m.StoreSeconds.WithLabelValues("login").Observe(time.Since(start).Seconds())

	// Follow-ups that don't change any result. They outlive the stream: a
	// gateway that cancels once it has every result must not cancel them. A
	// lost SADD is repaired by Reconcile rule 2 (gwconns is only a cleanup hint).
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), followUpTimeout)
	defer cancel()
	if len(uids) > 0 {
		if err := s.st.AddConns(fctx, ref, uids); err != nil {
			s.m.ConnsAddFailures.Inc()
			logger(ctx).Warn("login: gwconns SADD failed", "gw", ref.Gateway, "epoch", ref.Epoch, "uids", len(uids), "err", err)
		}
	}
	s.crossCheck(fctx, ref, prevs)
	return sendErr
}

// followUpTimeout bounds Login's follow-ups after the results are sent.
const followUpTimeout = time.Second

// prepareLogin validates one entry and verifies its token. It returns the
// store request and LOGIN_CODE_OK, or the rejection code.
func (s *Service) prepareLogin(ref store.EpochRef, e *pb.LoginEntry) (store.LoginReq, pb.LoginCode) {
	field, err := slotField(e.GetAppid(), e.GetPlatform())
	if err != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_BAD_REQUEST
	}
	entry := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId(),
		DeviceID: e.GetDeviceId(), ConnSeq: e.GetConnSeq(), LoginMs: e.GetRecvMs()}
	if err := entry.Validate(); err != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_BAD_REQUEST
	}
	c, err := s.keys.Verify(e.GetToken())
	switch {
	case errors.Is(err, auth.ErrKeyUnknown):
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN
	case err != nil:
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_TOKEN_INVALID
	}
	// The slot must be the one the token was issued for (spec: Presence).
	if c.AppID != e.GetAppid() || c.Platform != string(platforms[e.GetPlatform()]) || session.ValidUID(c.UID) != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_TOKEN_INVALID
	}
	entry.TokIatMs = c.IatMs
	return store.LoginReq{UID: c.UID, Field: field, Entry: entry}, pb.LoginCode_LOGIN_CODE_OK
}

var loginLabel = map[pb.LoginCode]string{
	pb.LoginCode_LOGIN_CODE_OK:              "ok",
	pb.LoginCode_LOGIN_CODE_STALE:           "stale",
	pb.LoginCode_LOGIN_CODE_SUPERSEDED:      "superseded",
	pb.LoginCode_LOGIN_CODE_EXPIRED:         "expired",
	pb.LoginCode_LOGIN_CODE_REVOKED:         "revoked",
	pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN:     "key_unknown",
	pb.LoginCode_LOGIN_CODE_TOKEN_INVALID:   "token_invalid",
	pb.LoginCode_LOGIN_CODE_BAD_REQUEST:     "bad_request",
	pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN: "error",
}

// loginResult maps the store's result to the RPC result and the reason to
// kick the replaced connection with (UNSPECIFIED: no kick).
func (s *Service) loginResult(ctx context.Context, r store.LoginResult) (*pb.LoginResult, pb.KickReason) {
	if r.Err != nil {
		s.m.Logins.WithLabelValues("error").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN}, pb.KickReason_KICK_REASON_UNSPECIFIED
	}
	if r.PrevErr != nil {
		// The stored entry did not decode: follow Code, skip the kick;
		// Reconcile closes the old connection (PI-3).
		s.m.Undecodable.WithLabelValues("login").Inc()
	}
	ok := &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OK}
	none := pb.KickReason_KICK_REASON_UNSPECIFIED
	switch r.Code {
	case store.LoginWritten, store.LoginAlready, store.LoginRepaired:
		s.m.Logins.WithLabelValues("ok").Inc()
		return ok, none
	case store.LoginReplaced:
		s.m.Logins.WithLabelValues("replaced").Inc()
		return ok, pb.KickReason_KICK_REASON_REPLACED
	case store.LoginRegressed:
		s.m.Logins.WithLabelValues("replaced").Inc()
		s.m.ConnSeqRegressed.Inc()
		return ok, pb.KickReason_KICK_REASON_REPLACED
	case store.LoginKicked:
		s.m.Logins.WithLabelValues("kicked").Inc()
		return ok, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE
	case store.LoginStale:
		s.m.Logins.WithLabelValues("stale").Inc()
		res := &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_STALE}
		if r.Prev != nil {
			res.StoredConnSeq = r.Prev.ConnSeq
		}
		return res, none
	case store.LoginSuperseded:
		s.m.Logins.WithLabelValues("superseded").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_SUPERSEDED}, none
	case store.LoginExpired:
		s.m.Logins.WithLabelValues("expired").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_EXPIRED}, none
	case store.LoginRevoked:
		s.m.Logins.WithLabelValues("revoked").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_REVOKED}, none
	default:
		logger(ctx).Error("login: unknown script result", "code", r.Code)
		s.m.Logins.WithLabelValues("error").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN}, none
	}
}

// replaced is a stored entry a login replaced, with the new login's login_ms.
type replaced struct {
	prev    session.Entry
	loginMs int64
}

// crossCheck records, for each relogin that replaced an entry of another
// epoch whose lease is gone, login_ms − that epoch's last renewal: time from
// gateway death to relogin, with no client data (R1-O7). Ended epochs
// (score 0) and swept ones (no score) are skipped.
func (s *Service) crossCheck(ctx context.Context, ref store.EpochRef, rs []replaced) {
	var refs []store.EpochRef
	for _, r := range rs {
		if pr := (store.EpochRef{Gateway: r.prev.Gateway, Epoch: r.prev.Epoch}); pr != ref {
			refs = append(refs, pr)
		}
	}
	if len(refs) == 0 {
		return
	}
	live, err := s.leases.Live(ctx, refs)
	if err != nil {
		return
	}
	for _, r := range rs {
		pr := store.EpochRef{Gateway: r.prev.Gateway, Epoch: r.prev.Epoch}
		if pr == ref || live[pr] {
			continue
		}
		renewed, ok, err := s.leases.RenewedMs(ctx, pr)
		if err != nil || !ok || renewed == 0 {
			continue
		}
		s.m.DeadEpochRelogin.Observe(float64(r.loginMs-renewed) / 1000)
	}
}
