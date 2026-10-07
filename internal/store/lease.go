package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// LeaseResult is one GatewayLease renewal.
type LeaseResult struct {
	Missing    bool  // the lease key was absent before this renewal (R3-O3)
	RedisNowMs int64 // TIME on the lease key's shard, for clock_skew_ms (R4M-5)
	// IndexErr is a failed ZADD gwepochs. The lease itself was renewed, and
	// the lease key, not the advisory score, decides liveness (R4M-3).
	IndexErr error
}

var leaseScript = mustScript("lease.lua")

// RenewLease renews gwlease:gw:{epoch} with one script that reads the old value,
// sets the new one with PX <LeaseTTL>, and reads TIME on the lease key's shard,
// then updates the gwepochs score to that TIME. The script goes through the
// cluster-routed client, so a MOVED after a failover reloads the slot map.
func (s *Store) RenewLease(ctx context.Context, ref EpochRef) (LeaseResult, error) {
	return s.renewLease(ctx, ref, rand.Uint64())
}

// renewLease is RenewLease with an explicit nonce; a re-run with the same nonce
// returns the first run's Missing.
func (s *Store) renewLease(ctx context.Context, ref EpochRef, nonce uint64) (LeaseResult, error) {
	if err := ref.validate(); err != nil {
		return LeaseResult{}, err
	}
	vals, err := leaseScript.Run(ctx, s.rdb, []string{ref.leaseKey()}, nonce, ms(s.cfg.LeaseTTL)).Int64Slice()
	if err != nil {
		return LeaseResult{}, fmt.Errorf("store: lease renewal: %w", err)
	}
	if len(vals) != 2 {
		return LeaseResult{}, fmt.Errorf("store: lease script returned %v", vals)
	}
	res := LeaseResult{Missing: vals[0] == 1, RedisNowMs: vals[1]}
	res.IndexErr = s.rdb.ZAdd(ctx, epochsKey, redis.Z{Score: float64(res.RedisNowMs), Member: ref.member()}).Err()
	return res, nil
}

// EndEpoch handles EpochEnded: DEL the lease key, so the epoch is invalid at
// once, and set its gwepochs score to 0, which tells the sweeper the epoch
// ended (exempt from the mass-stale guard and older than any threshold).
func (s *Store) EndEpoch(ctx context.Context, ref EpochRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := s.rdb.Del(ctx, ref.leaseKey()).Err(); err != nil {
		return err
	}
	return s.rdb.ZAdd(ctx, epochsKey, redis.Z{Score: 0, Member: ref.member()}).Err()
}

// LeasesExist reports, per ref, whether its lease key exists (the authoritative
// liveness predicate). Logic caches the answers for ~1s.
func (s *Store) LeasesExist(ctx context.Context, refs []EpochRef) ([]bool, error) {
	cmds := make([]*redis.IntCmd, len(refs))
	for _, r := range refs {
		if err := r.validate(); err != nil {
			return nil, err
		}
	}
	if _, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, r := range refs {
			cmds[i] = p.Exists(ctx, r.leaseKey())
		}
		return nil
	}); err != nil {
		return nil, err
	}
	out := make([]bool, len(refs))
	for i, c := range cmds {
		out[i] = c.Val() == 1
	}
	return out, nil
}

// EpochScore is one gwepochs member. ScoreMs 0 means the epoch sent EpochEnded.
type EpochScore struct {
	EpochRef
	ScoreMs int64
}

// ListEpochs returns every gwepochs member (a handful: one per gateway epoch).
func (s *Store) ListEpochs(ctx context.Context) ([]EpochScore, error) {
	zs, err := s.rdb.ZRangeWithScores(ctx, epochsKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]EpochScore, 0, len(zs))
	for _, z := range zs {
		m, _ := z.Member.(string)
		i := strings.LastIndexByte(m, ':')
		if i <= 0 {
			return nil, fmt.Errorf("store: malformed gwepochs member %q", m)
		}
		epoch, err := strconv.ParseUint(m[i+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("store: malformed gwepochs member %q: %w", m, err)
		}
		out = append(out, EpochScore{EpochRef: EpochRef{Gateway: m[:i], Epoch: epoch}, ScoreMs: int64(z.Score)})
	}
	return out, nil
}
