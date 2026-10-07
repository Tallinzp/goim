package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tallinzp/goim/internal/session"
)

// ScopeAll is the rev:{uid} field for a user-wide revocation.
const ScopeAll = "*"

// ErrNotReplicated means no replica confirmed the marker within WaitTimeout.
// Nothing was deleted; the caller (the account service) retries Revoke.
var ErrNotReplicated = errors.New("store: revocation marker not confirmed by a replica")

// Revoked is one sess field deleted by Revoke; its connection must be kicked with REVOKED.
type Revoked struct {
	Field string
	Entry session.Entry
}

var (
	revMarkScript   = mustScript("rev_mark.lua")
	revDeleteScript = mustScript("rev_delete.lua")
)

// Revoke writes revoked_before_ms = max(stored, beforeMs) for scope (ScopeAll or
// a slot field), waits until one replica confirms it, and only then deletes the
// affected sess fields (spec: Revoke). It is idempotent.
func (s *Store) Revoke(ctx context.Context, uid, scope string, beforeMs int64) ([]Revoked, error) {
	if err := session.ValidUID(uid); err != nil {
		return nil, err
	}
	if scope != ScopeAll {
		if err := session.ValidField(scope); err != nil {
			return nil, err
		}
	}
	if err := s.markRevoked(ctx, uid, scope, beforeMs); err != nil {
		return nil, err
	}
	return s.deleteRevoked(ctx, uid, scope)
}

// deleteRevoked is Revoke step 2: delete the fields in scope whose token
// predates their effective marker. Only Revoke may call it, after the marker
// is confirmed.
func (s *Store) deleteRevoked(ctx context.Context, uid, scope string) ([]Revoked, error) {
	vals, err := revDeleteScript.Run(ctx, s.rdb, []string{sessKey(uid), revKey(uid)}, scope).StringSlice()
	if err != nil {
		return nil, err
	}
	out := make([]Revoked, 0, len(vals)/2)
	for i := 0; i+1 < len(vals); i += 2 {
		e, err := session.Decode(vals[i+1])
		if err != nil {
			return out, err
		}
		out = append(out, Revoked{Field: vals[i], Entry: e})
	}
	return out, nil
}

// markRevoked runs the marker write and WAIT on one connection: WAIT only
// counts the writes made earlier on the same connection, so a pooled WAIT
// on another connection would confirm nothing.
func (s *Store) markRevoked(ctx context.Context, uid, scope string, beforeMs int64) error {
	key := revKey(uid)
	node, err := s.nodeFor(ctx, key)
	if err != nil {
		return err
	}
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
