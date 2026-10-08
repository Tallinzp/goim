package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var reconcileEndScript = mustScript("reconcile_end.lua")

// ScanConns returns one SSCAN page of gwconns:gw:{epoch}. A next cursor of 0 ends the scan.
func (s *Store) ScanConns(ctx context.Context, ref EpochRef, cursor uint64, count int64) ([]string, uint64, error) {
	if err := ref.validate(); err != nil {
		return nil, 0, err
	}
	return s.rdb.SScan(ctx, ref.connsKey(), cursor, "", count).Result()
}

// DeleteUntracked is the ReconcileEnd delete phase for one page of uids: it
// compare-and-deletes every field that points at ref whose conn_id is not in
// tracked and whose login_ms < asOfMs, then removes from gwconns the uids with
// no field left for ref. A field with ref's prefix that does not decode is
// deleted too. It returns the number of fields deleted.
//
// The SREM is not atomic with the sess check: a Login that writes and SADDs in
// between can lose its gwconns membership. gwconns is only a cleanup hint; rule 2
// of the next pass re-adds the uid.
func (s *Store) DeleteUntracked(ctx context.Context, ref EpochRef, asOfMs int64, uids []string, tracked map[uint64]struct{}) (int, error) {
	if err := ref.validate(); err != nil {
		return 0, err
	}
	prefix := ref.prefix()
	alls := make([]*redis.MapStringStringCmd, len(uids))
	if _, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, u := range uids {
			alls[i] = p.HGetAll(ctx, sessKey(u))
		}
		return nil
	}); err != nil {
		return 0, err
	}

	var calls []scriptCall
	var callUIDs []string
	var gone []any // uids with no field left for this epoch
	for i, u := range uids {
		fields := alls[i].Val()
		args := []any{prefix}
		ours := 0
		for f, v := range fields {
			if !strings.HasPrefix(v, prefix) {
				continue
			}
			ours++
			// A value that does not decode can't name a tracked connection or
			// a login in flight: it is a ghost like any other (PI-3).
			if e, err := session.Decode(v); err == nil {
				if _, ok := tracked[e.ConnID]; ok || e.LoginMs >= asOfMs {
					continue
				}
			}
			args = append(args, f, v)
		}
		switch {
		case len(args) > 1:
			calls = append(calls, scriptCall{keys: []string{sessKey(u)}, args: args})
			callUIDs = append(callUIDs, u)
		case ours == 0:
			gone = append(gone, u)
		}
	}

	deleted := 0
	for j, cmd := range s.runBatch(ctx, reconcileEndScript, calls) {
		vals, err := cmd.Int64Slice()
		if err != nil {
			return deleted, err
		}
		if len(vals) != 2 {
			return deleted, fmt.Errorf("store: reconcile_end returned %v", vals)
		}
		deleted += int(vals[0])
		if vals[1] == 0 {
			gone = append(gone, callUIDs[j])
		}
	}
	if len(gone) > 0 {
		if err := s.rdb.SRem(ctx, ref.connsKey(), gone...).Err(); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}
