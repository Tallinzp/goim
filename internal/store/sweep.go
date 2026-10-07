package store

import (
	"context"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var sweepScript = mustScript("sweep_uid.lua")

// EpochScoreOf returns ref's gwepochs score; ok is false if the member is gone.
func (s *Store) EpochScoreOf(ctx context.Context, ref EpochRef) (scoreMs int64, ok bool, err error) {
	sc, err := s.rdb.ZScore(ctx, epochsKey, ref.member()).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return int64(sc), true, nil
}

// ClaimSweep takes sweep:gw:{epoch} with SET NX PX 300000.
func (s *Store) ClaimSweep(ctx context.Context, ref EpochRef) (bool, error) {
	if err := ref.validate(); err != nil {
		return false, err
	}
	return s.rdb.SetNX(ctx, ref.sweepKey(), "1", sweepClaimTTL).Result()
}

// ReleaseSweep deletes the claim, so it cannot block a later legitimate sweep (R3-OP6).
func (s *Store) ReleaseSweep(ctx context.Context, ref EpochRef) error {
	return s.rdb.Del(ctx, ref.sweepKey()).Err()
}

// SweepUsers deletes, for each uid, every sess field that ref wrote. It returns
// the number of fields deleted.
func (s *Store) SweepUsers(ctx context.Context, ref EpochRef, uids []string) (int, error) {
	if err := ref.validate(); err != nil {
		return 0, err
	}
	calls := make([]scriptCall, 0, len(uids))
	for _, u := range uids {
		if err := session.ValidUID(u); err != nil {
			return 0, err
		}
		calls = append(calls, scriptCall{keys: []string{sessKey(u)}, args: []any{ref.prefix()}})
	}
	n := 0
	for _, cmd := range s.runBatch(ctx, sweepScript, calls) {
		d, err := cmd.Int()
		if err != nil {
			return n, err
		}
		n += d
	}
	return n, nil
}

// FinishSweep UNLINKs gwconns:gw:{epoch} (freed off the main thread) and removes
// the gwepochs member. Call it only after a sweep that was not aborted.
func (s *Store) FinishSweep(ctx context.Context, ref EpochRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := s.rdb.Unlink(ctx, ref.connsKey()).Err(); err != nil {
		return err
	}
	return s.rdb.ZRem(ctx, epochsKey, ref.member()).Err()
}
