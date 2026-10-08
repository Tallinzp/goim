package logic

import (
	"context"
	"strings"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Reconcile applies rules 1–4 to each entry of one chunk (spec: Reconcile
// handling). The chunk is resolved as a whole (roadmap choice #14): if any
// entry's outcome is unknown, the RPC fails, and the gateway resolves the
// chunk at its deadline, after which the compare-and-set can no longer write.
func (s *Service) Reconcile(ctx context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	ref, err := epochRef(c.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	entries := c.GetEntries()
	slots := make([]store.SlotRef, len(entries))
	chunk := make([]session.Entry, len(entries))
	for i, e := range entries {
		field, err := slotField(e.GetAppid(), e.GetPlatform())
		if err == nil {
			err = session.ValidUID(e.GetUid())
		}
		ent := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId(),
			DeviceID: e.GetDeviceId(), ConnSeq: e.GetConnSeq(), LoginMs: e.GetLoginMs(), TokIatMs: e.GetTokIatMs()}
		if err == nil {
			err = ent.Validate()
		}
		if err != nil {
			// Entries come from Logic's own Login results, so this is a gateway bug.
			return nil, invalid("entry %d: %v", i, err)
		}
		slots[i], chunk[i] = store.SlotRef{UID: e.GetUid(), Field: field}, ent
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	start := time.Now()
	defer func() { s.m.StoreSeconds.WithLabelValues("reconcile").Observe(time.Since(start).Seconds()) }()
	states := s.st.ReadSlots(ctx, slots)
	var others []store.EpochRef
	for i, st := range states {
		if st.Err != nil {
			return nil, unavailable("read slot %d: %v", i, st.Err)
		}
		if st.StoredErr != nil {
			s.m.Undecodable.WithLabelValues("reconcile").Inc()
		}
		if st.Stored != nil {
			others = append(others, storedRef(st.Stored))
		}
	}
	live, err := s.leases.Live(ctx, others)
	if err != nil {
		return nil, unavailable("lease check: %v", err)
	}

	results := make([]*pb.ReconcileResult, len(entries))
	written := &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_WRITTEN}
	var refresh []string // uids whose TTL and gwconns membership to refresh
	var cas []store.CASReq
	var casIdx []int
	var casKick []pb.KickReason // UNSPECIFIED: rule 4, no kick
	deadlineMs := s.cfg.Store.LoginDeadline.Milliseconds()
	for i, e := range chunk {
		st := states[i]
		switch {
		case e.TokIatMs < st.MarkerMs: // rule 1: revoked, write nothing
			results[i] = s.notOwner(pb.KickReason_KICK_REASON_REVOKED)
		case st.Raw != "" && strings.HasPrefix(st.Raw, e.OwnerPrefix()): // rule 2: ours
			results[i] = written
			refresh = append(refresh, slots[i].UID)
			s.m.ReconcileEntries.WithLabelValues("refreshed", "").Inc()
		case st.Stored != nil && live[storedRef(st.Stored)]: // rule 3: another live session
			reason := kickReason(e.DeviceID == st.Stored.DeviceID)
			if !session.ChunkWins(e, *st.Stored, deadlineMs) {
				results[i] = s.notOwner(reason)
				continue
			}
			// Only after a lost write (e.g. a failover): write ours back.
			cas = append(cas, store.CASReq{UID: slots[i].UID, Field: slots[i].Field,
				ExpectRaw: st.Raw, Entry: e, SentMs: c.GetSentMs()})
			casIdx, casKick = append(casIdx, i), append(casKick, reason)
		default: // rule 4: empty, dead lease, or unreadable: restore
			cas = append(cas, store.CASReq{UID: slots[i].UID, Field: slots[i].Field,
				ExpectRaw: st.Raw, Entry: e, SentMs: c.GetSentMs()})
			casIdx, casKick = append(casIdx, i), append(casKick, pb.KickReason_KICK_REASON_UNSPECIFIED)
		}
	}

	var kicks []func()
	var unknown error // first entry whose outcome is unknown
	for j, r := range s.st.CompareAndSet(ctx, cas) {
		i := casIdx[j]
		if r.Err != nil {
			if unknown == nil {
				unknown = unavailable("compare-and-set %d: %v", i, r.Err)
			}
			continue
		}
		switch r.Code {
		case store.CASWritten:
			results[i] = written
			refresh = append(refresh, slots[i].UID)
			if reason := casKick[j]; reason != pb.KickReason_KICK_REASON_UNSPECIFIED {
				// Kick the replaced session only if the compare-and-set wrote (R3-O6).
				prev := *states[i].Stored
				kicks = append(kicks, func() { s.kick.Kick(storedRef(&prev), prev.ConnID, reason) })
				s.m.ReconcileEntries.WithLabelValues("override", "").Inc()
			} else {
				s.m.ReconcileEntries.WithLabelValues("restored", "").Inc()
			}
		case store.CASUnchanged:
			results[i] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_UNCHANGED}
			s.m.ReconcileEntries.WithLabelValues("unchanged", "").Inc()
		case store.CASRefused:
			results[i] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_REFUSED}
			s.m.ReconcileEntries.WithLabelValues("refused", "").Inc()
		case store.CASRevoked: // a Revoke landed after the read (roadmap choice #4)
			results[i] = s.notOwner(pb.KickReason_KICK_REASON_REVOKED)
		default:
			logger(ctx).Error("reconcile: unknown compare-and-set result", "code", r.Code)
			if unknown == nil {
				unknown = unavailable("compare-and-set %d: result %q", i, r.Code)
			}
		}
	}
	// A compare-and-set that wrote has replaced the stored session, whatever
	// happened to the rest of the chunk: kick it even if the RPC fails below.
	for _, k := range kicks {
		k()
	}
	if unknown != nil {
		return nil, unknown
	}
	// TTL refresh and gwconns SADD are never refused (R3-O6). If they fail,
	// the chunk has no definitive reply; nothing it wrote is wrong.
	if err := s.st.RefreshSlots(ctx, ref, refresh); err != nil {
		return nil, unavailable("refresh: %v", err)
	}
	return &pb.ReconcileReply{Results: results}, nil
}

var kickLabel = map[pb.KickReason]string{
	pb.KickReason_KICK_REASON_REPLACED:               "replaced",
	pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE: "kicked_by_other_device",
	pb.KickReason_KICK_REASON_REVOKED:                "revoked",
}

func (s *Service) notOwner(reason pb.KickReason) *pb.ReconcileResult {
	s.m.ReconcileEntries.WithLabelValues("not_owner", kickLabel[reason]).Inc()
	return &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER, Reason: reason}
}

func storedRef(e *session.Entry) store.EpochRef {
	return store.EpochRef{Gateway: e.Gateway, Epoch: e.Epoch}
}
