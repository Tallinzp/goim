package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

// ScopeAll is the rev:{uid} field for a user-wide revocation.
const ScopeAll = "*"

// ErrNotReplicated means no replica confirmed the marker within WaitTimeout.
// Nothing was deleted; the caller (the account service) retries Revoke.
var ErrNotReplicated = errors.New("store: revocation marker not confirmed by a replica")

// revDeletedTTL is how long a Revoke request's deleted entries stay readable
// for a re-run (PI-2). After it, a re-run returns nothing and Reconcile rule 1
// closes those connections within one cycle.
const revDeletedTTL = 5 * time.Minute

// Revoked is one sess field deleted by Revoke; its connection must be kicked
// with REVOKED. If the deleted value did not decode, Entry is zero and EntryErr
// says why: no kick is possible, and Reconcile rule 1 closes the connection (PI-3).
type Revoked struct {
	Field    string
	Entry    session.Entry
	EntryErr error
}

var (
	revMarkScript   = mustScript("rev_mark.lua")
	revDeleteScript = mustScript("rev_delete.lua")
)

// Revoke writes revoked_before_ms = max(stored, beforeMs) for scope (ScopeAll or
// a slot field), waits until one replica confirms it, and only then deletes the
// affected sess fields (spec: Revoke). beforeMs comes from the account service
// and is the same on every retry (PI-4): it names the request, so a retry, or a
// re-run after a lost reply, returns every entry the request has deleted (PI-2).
func (s *Store) Revoke(ctx context.Context, uid, scope string, beforeMs int64) ([]Revoked, error) {
	if err := session.ValidUID(uid); err != nil {
		return nil, err
	}
	if scope != ScopeAll {
		if err := session.ValidField(scope); err != nil {
			return nil, err
		}
	}
	if beforeMs <= 0 {
		return nil, fmt.Errorf("store: revoked_before_ms %d is not positive", beforeMs)
	}
	if err := s.markRevoked(ctx, uid, scope, beforeMs); err != nil {
		return nil, err
	}
	return s.deleteRevoked(ctx, uid, scope, beforeMs)
}

// deleteRevoked is Revoke step 2: delete the fields in scope whose token
// predates their effective marker. Only Revoke may call it, after the marker
// is confirmed. The result is sorted by field.
func (s *Store) deleteRevoked(ctx context.Context, uid, scope string, beforeMs int64) ([]Revoked, error) {
	keys := []string{sessKey(uid), revKey(uid), revDeletedKey(uid, scope, beforeMs)}
	vals, err := revDeleteScript.Run(ctx, s.rdb, keys, scope, ms(revDeletedTTL)).StringSlice()
	if err != nil {
		return nil, err
	}
	out := make([]Revoked, 0, len(vals)/2)
	for i := 0; i+1 < len(vals); i += 2 {
		r := Revoked{Field: vals[i]}
		if e, err := session.Decode(vals[i+1]); err != nil {
			r.EntryErr = err
		} else {
			r.Entry = e
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	return out, nil
}

// markRevoked writes and confirms the marker on the key's master. If that
// master turns out to be stale (a Cluster failover: MOVED, READONLY or a
// connection error), it waits for the slot map to name a new master and tries
// once more there (PI-6). The retry is safe: the marker write is max().
func (s *Store) markRevoked(ctx context.Context, uid, scope string, beforeMs int64) error {
	key := revKey(uid)
	node, err := s.masterFor(ctx, key)
	if err != nil {
		return err
	}
	err = s.markOn(ctx, node, key, scope, beforeMs)
	if !staleMaster(err) {
		return err
	}
	next, rerr := s.rerouteMaster(ctx, key, node.Options().Addr)
	if rerr != nil {
		return fmt.Errorf("%w (re-route: %v)", err, rerr)
	}
	return s.markOn(ctx, next, key, scope, beforeMs)
}

// markOn runs the marker write and WAIT on one connection: WAIT only counts
// the writes made earlier on the same connection, so a pooled WAIT on another
// connection would confirm nothing.
func (s *Store) markOn(ctx context.Context, node *redis.Client, key, scope string, beforeMs int64) error {
	conn := node.Conn()
	defer conn.Close()
	if err := revMarkScript.Run(ctx, conn, []string{key}, scope, beforeMs, ms(s.cfg.SessTTL)).Err(); err != nil {
		return fmt.Errorf("store: revocation marker: %w", err)
	}
	n, err := conn.Wait(ctx, 1, s.cfg.WaitTimeout).Result()
	if err != nil {
		return fmt.Errorf("store: WAIT: %w", err)
	}
	if n < 1 {
		return ErrNotReplicated
	}
	return nil
}

// staleMaster reports whether err means the node may no longer own the key:
// a MOVED or READONLY reply, or a failure that is not a Redis reply at all
// (dial, EOF, reset). ErrNotReplicated, the caller's own deadline and any
// other Redis reply are final.
func staleMaster(err error) bool {
	if err == nil || errors.Is(err, ErrNotReplicated) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if _, ok := redis.IsMovedError(err); ok {
		return true
	}
	if redis.IsReadOnlyError(err) {
		return true
	}
	var reply redis.Error
	return !errors.As(err, &reply)
}

// rerouteMaster reloads the slot map until it names a master for key other
// than oldAddr, for at most rerouteWait. ReloadState is asynchronous, so it
// polls. A single-node client has no slot map and never re-routes.
func (s *Store) rerouteMaster(ctx context.Context, key, oldAddr string) (*redis.Client, error) {
	if s.reloadSlots == nil {
		return nil, errors.New("not a cluster client")
	}
	ctx, cancel := context.WithTimeout(ctx, s.rerouteWait)
	defer cancel()
	tick := time.NewTicker(reroutePoll)
	defer tick.Stop()
	for {
		s.reloadSlots(ctx)
		if node, err := s.masterFor(ctx, key); err == nil && node.Options().Addr != oldAddr {
			return node, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("slot map still names %s: %w", oldAddr, ctx.Err())
		case <-tick.C:
		}
	}
}
