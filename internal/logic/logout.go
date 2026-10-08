package logic

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Logout compare-and-deletes each entry's field (spec: Logout Lua). done[i]
// is false only when entry i's outcome is unknown; the gateway retries those.
// The gateway's Logout fence, not Logic, orders a Logout after its Login.
func (s *Service) Logout(ctx context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	ref, err := epochRef(b.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	entries := b.GetEntries()
	done := make([]bool, len(entries))
	reqs := make([]store.LogoutReq, 0, len(entries))
	idx := make([]int, 0, len(entries))
	for i, e := range entries {
		field, err := slotField(e.GetAppid(), e.GetPlatform())
		if err == nil {
			err = session.ValidUID(e.GetUid())
		}
		if err != nil {
			// A retry can't fix it; the gateway drops it and Reconcile's
			// delete phase removes any field it left.
			done[i] = true
			s.m.Logouts.WithLabelValues("invalid").Inc()
			logger(ctx).Warn("logout: invalid entry", "uid", e.GetUid(), "conn_id", e.GetConnId(), "err", err)
			continue
		}
		reqs = append(reqs, store.LogoutReq{UID: e.GetUid(), Field: field,
			Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId()})
		idx = append(idx, i)
	}
	start := time.Now()
	// done is called concurrently, but each call writes its own element.
	s.st.LogoutEach(ctx, reqs, func(j int, r store.LogoutResult) {
		switch {
		case r.Err != nil:
			s.m.Logouts.WithLabelValues("unknown").Inc()
		case r.Deleted:
			done[idx[j]] = true
			s.m.Logouts.WithLabelValues("deleted").Inc()
		default:
			done[idx[j]] = true
			s.m.Logouts.WithLabelValues("not_owner").Inc()
		}
	})
	s.m.StoreSeconds.WithLabelValues("logout").Observe(time.Since(start).Seconds())
	return &pb.LogoutReply{Done: done}, nil
}
