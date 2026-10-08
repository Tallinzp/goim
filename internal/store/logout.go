package store

import (
	"context"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

// LogoutReq deletes UID's Field only if it still belongs to Gateway|Epoch|ConnID.
type LogoutReq struct {
	UID     string
	Field   string
	Gateway string
	Epoch   uint64
	ConnID  uint64
}

// LogoutResult reports whether the field was deleted (false: a newer session owns it).
type LogoutResult struct {
	Deleted bool
	Err     error
}

var logoutScript = mustScript("logout.lua")

// LogoutEach runs the compare-and-delete for a batch, pipelined per owning
// master, and calls done once per request as soon as its group returns.
func (s *Store) LogoutEach(ctx context.Context, reqs []LogoutReq, done func(i int, r LogoutResult)) {
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := validLogout(r); err != nil {
			done(i, LogoutResult{Err: err})
			continue
		}
		owner := session.Entry{Gateway: r.Gateway, Epoch: r.Epoch, ConnID: r.ConnID}.OwnerPrefix()
		calls = append(calls, scriptCall{keys: []string{sessKey(r.UID)}, args: []any{r.Field, owner}})
		idx = append(idx, i)
	}
	s.runEach(ctx, logoutScript, calls, func(j int, cmd *redis.Cmd) {
		n, err := cmd.Int()
		done(idx[j], LogoutResult{Deleted: n == 1, Err: err})
	})
}

// Logout is LogoutEach collected in request order.
func (s *Store) Logout(ctx context.Context, reqs []LogoutReq) []LogoutResult {
	out := make([]LogoutResult, len(reqs))
	s.LogoutEach(ctx, reqs, func(i int, r LogoutResult) { out[i] = r })
	return out
}

func validLogout(r LogoutReq) error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	if err := session.ValidField(r.Field); err != nil {
		return err
	}
	return session.ValidGateway(r.Gateway)
}
