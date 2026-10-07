package store

import (
	"context"

	"github.com/Tallinzp/goim/internal/session"
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

// Logout runs the compare-and-delete for a batch, pipelined, in request order.
func (s *Store) Logout(ctx context.Context, reqs []LogoutReq) []LogoutResult {
	out := make([]LogoutResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := validLogout(r); err != nil {
			out[i].Err = err
			continue
		}
		owner := session.Entry{Gateway: r.Gateway, Epoch: r.Epoch, ConnID: r.ConnID}.OwnerPrefix()
		calls = append(calls, scriptCall{keys: []string{sessKey(r.UID)}, args: []any{r.Field, owner}})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, logoutScript, calls) {
		n, err := cmd.Int()
		out[idx[j]] = LogoutResult{Deleted: n == 1, Err: err}
	}
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
