package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

// SlotRef names one slot of one user.
type SlotRef struct {
	UID   string
	Field string
}

// SlotState is what Logic observes before applying Reconcile rules 1–4.
type SlotState struct {
	Raw      string         // stored value; "" if the field is absent
	Stored   *session.Entry // decoded Raw; nil if absent
	MarkerMs int64          // effective revocation marker: max(rev[*], rev[field]), 0 if none
	Err      error
}

func (r SlotRef) validate() error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	return session.ValidField(r.Field)
}

// ReadSlots reads each slot's field and effective revocation marker, pipelined.
func (s *Store) ReadSlots(ctx context.Context, refs []SlotRef) []SlotState {
	out := make([]SlotState, len(refs))
	gets := make([]*redis.StringCmd, len(refs))
	marks := make([]*redis.SliceCmd, len(refs))
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, r := range refs {
			if out[i].Err = r.validate(); out[i].Err != nil {
				continue
			}
			gets[i] = p.HGet(ctx, sessKey(r.UID), r.Field)
			marks[i] = p.HMGet(ctx, revKey(r.UID), ScopeAll, r.Field)
		}
		return nil
	})
	for i := range refs {
		if out[i].Err != nil {
			continue
		}
		out[i] = readSlot(gets[i], marks[i])
	}
	return out
}

func readSlot(get *redis.StringCmd, mark *redis.SliceCmd) SlotState {
	var st SlotState
	raw, err := get.Result()
	switch {
	case err == redis.Nil:
	case err != nil:
		return SlotState{Err: err}
	default:
		e, err := session.Decode(raw)
		if err != nil {
			return SlotState{Err: err}
		}
		st.Raw, st.Stored = raw, &e
	}
	vals, err := mark.Result()
	if err != nil {
		return SlotState{Err: err}
	}
	for _, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue // field absent counts as 0
		}
		m, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return SlotState{Err: fmt.Errorf("store: revocation marker %q: %w", str, err)}
		}
		st.MarkerMs = max(st.MarkerMs, m)
	}
	return st
}

// CASCode is the compare-and-set script's result.
type CASCode string

const (
	CASWritten   CASCode = "WRITTEN"   // field was still ExpectRaw; Entry written
	CASUnchanged CASCode = "UNCHANGED" // field changed since Logic read it; no write, not re-queued
	CASRefused   CASCode = "REFUSED"   // Redis TIME − sent_ms > login_deadline; no write, re-queued
	CASRevoked   CASCode = "REVOKED"   // Entry's token predates the marker; no write, not_owner(REVOKED)
)

// CASReq writes Entry into UID's Field only if it still holds ExpectRaw ("" = absent).
type CASReq struct {
	UID       string
	Field     string
	ExpectRaw string
	Entry     session.Entry
	SentMs    int64 // the chunk's sent_ms (gateway wall clock)
}

// CASResult is the outcome of one CASReq.
type CASResult struct {
	Code CASCode
	Err  error
}

var casScript = mustScript("reconcile_cas.lua")

// CompareAndSet runs the Reconcile compare-and-set for a batch, pipelined, in request order.
func (s *Store) CompareAndSet(ctx context.Context, reqs []CASReq) []CASResult {
	out := make([]CASResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := (SlotRef{r.UID, r.Field}).validate(); err != nil {
			out[i].Err = err
			continue
		}
		if err := r.Entry.Validate(); err != nil {
			out[i].Err = err
			continue
		}
		calls = append(calls, scriptCall{
			keys: []string{sessKey(r.UID), revKey(r.UID)},
			args: []any{r.Field, r.ExpectRaw, r.Entry.Encode(), r.Entry.TokIatMs, r.SentMs,
				ms(s.cfg.LoginDeadline), ms(s.cfg.SessTTL)},
		})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, casScript, calls) {
		code, err := cmd.Text()
		out[idx[j]] = CASResult{Code: CASCode(code), Err: err}
	}
	return out
}

// RefreshSlots refreshes the sess:{uid} TTL of each uid and ensures each is in
// gwconns:gw:{epoch} (rules 2–4). It is never refused by the deadline (R3-O6).
func (s *Store) RefreshSlots(ctx context.Context, ref EpochRef, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if err := ref.validate(); err != nil {
		return err
	}
	members := make([]any, len(uids))
	for i, u := range uids {
		if err := session.ValidUID(u); err != nil {
			return err
		}
		members[i] = u
	}
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, u := range uids {
			p.PExpire(ctx, sessKey(u), s.cfg.SessTTL)
		}
		p.SAdd(ctx, ref.connsKey(), members...)
		return nil
	})
	return err
}
