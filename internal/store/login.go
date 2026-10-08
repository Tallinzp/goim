package store

import (
	"context"
	"fmt"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
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
	// LoginAlready: this exact entry is already stored, written by an earlier run
	// of the same request whose reply was lost (go-redis re-sent it). It is a
	// success; the kick that run reported is lost, and Reconcile's not_owner
	// closes the old connection within one cycle.
	LoginAlready LoginCode = "ALREADY"
	// LoginRepaired: the stored value was unreadable even to the Lua (not 7
	// fields, or conn_seq / login_ms not numbers), so it cannot be a valid
	// owner. The login was written over it with no kick; PrevErr is set.
	// Logic answers it like WRITTEN and counts it.
	LoginRepaired LoginCode = "REPAIRED"
)

// LoginReq is one Login. Entry.LoginMs is the gateway's recv_ms.
type LoginReq struct {
	UID   string
	Field string
	Entry session.Entry
}

// LoginResult is the outcome of one LoginReq. Err is set only when the outcome
// is unknown (transport failure, invalid request, malformed reply); if Err is
// nil, Code is what the script did. Prev is the stored entry the script saw
// (nil if it returned none). If that entry did not decode, Prev is nil and
// PrevErr says why: follow Code, skip the kick (Reconcile closes the old
// connection) and count it (PI-3).
type LoginResult struct {
	Code    LoginCode
	Prev    *session.Entry
	PrevErr error
	Err     error
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

// LoginEach runs the login Lua for a batch, pipelined per owning master, and
// calls done once per request as soon as its group returns (R5S-O1). An
// invalid request is reported at once and not sent. done may be called from
// several goroutines at the same time.
func (s *Store) LoginEach(ctx context.Context, reqs []LoginReq, done func(i int, r LoginResult)) {
	calls := make([]scriptCall, 0, len(reqs))
	idx := make([]int, 0, len(reqs))
	for i, r := range reqs {
		if err := r.validate(); err != nil {
			done(i, LoginResult{Err: err})
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
	s.runEach(ctx, loginScript, calls, func(j int, cmd *redis.Cmd) {
		done(idx[j], parseLogin(cmd.Slice()))
	})
}

// Login is LoginEach collected in request order.
func (s *Store) Login(ctx context.Context, reqs []LoginReq) []LoginResult {
	out := make([]LoginResult, len(reqs))
	s.LoginEach(ctx, reqs, func(i int, r LoginResult) { out[i] = r })
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
			res.PrevErr = err
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
