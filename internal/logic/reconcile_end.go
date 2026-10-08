package logic

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/status"
)

var paceLabel = map[pb.Pace]string{
	pb.Pace_PACE_UNSPECIFIED: "unspecified",
	pb.Pace_PACE_PERIODIC:    "periodic",
	pb.Pace_PACE_TRIGGERED:   "triggered",
}

// ReconcileEnd runs the delete phase for one finished pass (R3-O1, R5S-O8):
// SSCAN gwconns:gw:{epoch} and compare-and-delete every field of this epoch
// whose conn_id is not tracked and whose login_ms < as_of_ms, at most
// DeleteRate uids/s. It replies when the phase completes. A retry after a
// failure resends the same payload; the deletes are idempotent.
func (s *Service) ReconcileEnd(ctx context.Context, r *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	ref, err := epochRef(r.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	if r.GetAsOfMs() <= 0 {
		return nil, invalid("as_of_ms %d", r.GetAsOfMs())
	}
	ids := r.GetTrackedConnIds()
	tracked := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		tracked[id] = struct{}{}
	}
	pace := paceLabel[r.GetPace()]
	s.m.ReconcileEndTracked.Observe(float64(len(ids)))

	batch := s.cfg.DeleteBatch
	lim := rate.NewLimiter(rate.Limit(s.cfg.DeleteRate), batch)
	start := time.Now()
	deleted, scanned := 0, 0
	var cursor uint64
	for {
		uids, next, err := s.st.ScanConns(ctx, ref, cursor, int64(batch))
		if err != nil {
			return nil, unavailable("scan gwconns: %v", err)
		}
		// SSCAN's COUNT is a hint: a page can be larger than the batch.
		for off := 0; off < len(uids); off += batch {
			part := uids[off:min(off+batch, len(uids))]
			if err := lim.WaitN(ctx, len(part)); err != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			n, err := s.st.DeleteUntracked(ctx, ref, r.GetAsOfMs(), part, tracked)
			deleted += n
			s.m.ReconcileEndDeleted.Add(float64(n))
			if err != nil {
				return nil, unavailable("delete phase: %v", err)
			}
			scanned += len(part)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	s.m.ReconcileEndSeconds.WithLabelValues(pace).Observe(time.Since(start).Seconds())
	s.m.ReconcileEndScanned.WithLabelValues(pace).Observe(float64(scanned))
	return &pb.ReconcileEndReply{Deleted: uint32(deleted)}, nil
}
