package store

import (
	"context"
	"fmt"

	"github.com/Tallinzp/goim/internal/session"
)

// LoginCode is the login Lua's decision.
type LoginCode string

const (
	LoginWritten    LoginCode = "WRITTEN"    // slot was empty; no kick
	LoginReplaced   LoginCode = "REPLACED"   // same device, higher conn_seq: kick Prev with REPLACED
	LoginRegressed  LoginCode = "REGRESSED"  // same device, counter regression: kick Prev with REPLACED, count conn_seq_regressed
	LoginKicked     LoginCode = "KICKED"     // other device, not earlier: kick Prev with KICKED_BY_OTHER_DEVICE
	LoginStale      LoginCode = "STALE"      // rejected; Prev.ConnSeq goes back to the client
	LoginSuperseded LoginCode = "SUPERSEDED" // rejected; no write, no kick
	LoginExpired    LoginCode = "EXPIRED"    // rejected; reached Redis after recv_ms + login_deadline
	LoginRevoked    LoginCode = "REVOKED"    // rejected; token iat before the effective marker
)

// LoginReq is one Login. Entry.LoginMs is the gateway's recv_ms.
type LoginReq struct {
	UID   string
	Field string
	Entry session.Entry
}

// LoginResult is the outcome of one LoginReq. Prev is the stored entry the
// script saw (nil if the slot was empty or the result is EXPIRED/REVOKED).
type LoginResult struct {
	Code LoginCode
	Prev *session.Entry
	Err  error
}

var loginScript = mustScript("login.lua")

func (r LoginReq) validate() error {
	if err := session.ValidUID(r.UID); err != nil {
		return err
	}
	if err := session.ValidField(r.Field); err != nil {
		return err
	}
	return r.Entry.Validate()
}

// Login runs the login Lua for a batch, pipelined. Results are in request order;
// an invalid request gets Err and is not sent.
func (s *Store) Login(ctx context.Context, reqs []LoginReq) []LoginResult {
	out := make([]LoginResult, len(reqs))
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := r.validate(); err != nil {
			out[i].Err = err
			continue
		}
		e := r.Entry
		calls = append(calls, scriptCall{
			keys: []string{sessKey(r.UID), revKey(r.UID)},
			args: []any{r.Field, e.Encode(), e.DeviceID, e.ConnSeq, e.LoginMs, e.TokIatMs,
				ms(s.cfg.LoginDeadline), ms(s.cfg.SessTTL)},
		})
		idx = append(idx, i)
	}
	for j, cmd := range s.runBatch(ctx, loginScript, calls) {
		out[idx[j]] = parseLogin(cmd.Slice())
	}
	return out
}

func parseLogin(vals []any, err error) LoginResult {
	if err != nil {
		return LoginResult{Err: err}
	}
	if len(vals) != 2 {
		return LoginResult{Err: fmt.Errorf("store: login script returned %v", vals)}
	}
	code, _ := vals[0].(string)
	prev, _ := vals[1].(string)
	res := LoginResult{Code: LoginCode(code)}
	if prev != "" {
		e, err := session.Decode(prev)
		if err != nil {
			res.Err = err
			return res
		}
		res.Prev = &e
	}
	return res
}

// AddConns is the Login follow-up SADD of uids to gwconns:gw:{epoch}.
func (s *Store) AddConns(ctx context.Context, ref EpochRef, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if err := ref.validate(); err != nil {
		return err
	}
	members := make([]any, len(uids))
	for i, u := range uids {
		members[i] = u
	}
	return s.rdb.SAdd(ctx, ref.connsKey(), members...).Err()
}
