package logic

import (
	"context"
	"errors"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Revoke writes the revocation marker, confirmed by a replica, then deletes
// the affected fields and kicks their connections with REVOKED (spec: Revoke,
// R2-O4, R3-OP2, PI-4). UNAVAILABLE means the marker was not confirmed and
// nothing was deleted: the account service retries with the same before_ms.
func (s *Service) Revoke(ctx context.Context, r *pb.RevokeRequest) (*pb.RevokeReply, error) {
	scope, label := store.ScopeAll, "all"
	if !r.GetAll() {
		f, err := slotField(r.GetAppid(), r.GetPlatform())
		if err != nil {
			s.m.Revokes.WithLabelValues("slot", "invalid").Inc()
			return nil, invalid("%v", err)
		}
		scope, label = f, "slot"
	}
	if err := session.ValidUID(r.GetUid()); err != nil {
		s.m.Revokes.WithLabelValues(label, "invalid").Inc()
		return nil, invalid("%v", err)
	}
	now, err := s.st.RedisNowMs(ctx)
	if err != nil {
		s.m.Revokes.WithLabelValues(label, "error").Inc()
		return nil, unavailable("redis time: %v", err)
	}
	// Catches a seconds-vs-ms bug in the caller; not retryable (PI-4).
	if b := r.GetBeforeMs(); b <= 0 || b > now+s.cfg.RevokeMaxAhead.Milliseconds() {
		s.m.Revokes.WithLabelValues(label, "invalid").Inc()
		return nil, invalid("before_ms %d: must be positive and at most %v past Redis TIME %d", b, s.cfg.RevokeMaxAhead, now)
	}
	revoked, err := s.st.Revoke(ctx, r.GetUid(), scope, r.GetBeforeMs())
	switch {
	case errors.Is(err, store.ErrNotReplicated):
		s.m.Revokes.WithLabelValues(label, "not_replicated").Inc()
		return nil, unavailable("revocation marker not confirmed by a replica; retry")
	case err != nil:
		s.m.Revokes.WithLabelValues(label, "error").Inc()
		return nil, unavailable("revoke: %v", err)
	}
	kicked := 0
	for _, rv := range revoked {
		if rv.EntryErr != nil {
			// No connection to kick; Reconcile rule 1 closes it (PI-3).
			s.m.Undecodable.WithLabelValues("revoke").Inc()
			continue
		}
		s.kick.Kick(store.EpochRef{Gateway: rv.Entry.Gateway, Epoch: rv.Entry.Epoch}, rv.Entry.ConnID,
			pb.KickReason_KICK_REASON_REVOKED)
		kicked++
	}
	s.m.RevocationKicks.Add(float64(kicked))
	s.m.Revokes.WithLabelValues(label, "ok").Inc()
	logger(ctx).Info("revoked", "uid", r.GetUid(), "scope", scope, "before_ms", r.GetBeforeMs(),
		"reason", r.GetReason(), "kicked", kicked)
	return &pb.RevokeReply{Kicked: uint32(kicked)}, nil
}
