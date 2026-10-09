package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Fault ops Logic serves (Plan 5b, choice #58). The race suite sets them on
// a Logic started with -faults; the key is a uid unless noted.
const (
	FaultLogin        = "login"         // LoginEach, per entry; After: Redis written, reply late or lost
	FaultLogout       = "logout"        // LogoutEach, per entry
	FaultReconcile    = "reconcile"     // ReadSlots: the whole chunk, if any entry matches
	FaultReconcileEnd = "reconcile_end" // the ReconcileEnd RPC, before its delete phase; key: gateway id
	FaultGWEpochs     = "gwepochs"      // ZADD gwepochs after a lease SET; key: gateway id (R4M-3)
)

// faultStore breaks Store calls on the race suite's request. A matched Login
// or Logout entry runs on its own, so the other entries of its batch are
// not delayed with it. A matched call is not cancelled with its RPC: it
// reaches Redis late, as a call queued behind a slow Redis does, so the Lua
// deadlines, not a cancelled context, are what keep a late write out
// (races 6 and 14, R3-O6).
type faultStore struct {
	Store
	f *fault.Set
}

func (s faultStore) LoginEach(ctx context.Context, reqs []store.LoginReq, done func(i int, r store.LoginResult)) {
	var wg sync.WaitGroup
	plain := make([]int, 0, len(reqs))
	for i, r := range reqs {
		h, ok := s.f.Match(FaultLogin, r.UID)
		if !ok {
			plain = append(plain, i)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.WithoutCancel(ctx)
			if !h.After {
				if err := h.Wait(ctx); err != nil {
					done(i, store.LoginResult{Err: err})
					return
				}
			}
			var res store.LoginResult
			s.Store.LoginEach(ctx, reqs[i:i+1], func(_ int, r store.LoginResult) { res = r })
			if h.After {
				if err := h.Wait(ctx); err != nil {
					res = store.LoginResult{Err: err}
				}
			}
			done(i, res)
		}()
	}
	if len(plain) > 0 {
		sub := make([]store.LoginReq, len(plain))
		for j, i := range plain {
			sub[j] = reqs[i]
		}
		s.Store.LoginEach(ctx, sub, func(j int, r store.LoginResult) { done(plain[j], r) })
	}
	wg.Wait()
}

func (s faultStore) LogoutEach(ctx context.Context, reqs []store.LogoutReq, done func(i int, r store.LogoutResult)) {
	plain := make([]int, 0, len(reqs))
	hits := map[int]fault.Hit{}
	for i, r := range reqs {
		if h, ok := s.f.Match(FaultLogout, r.UID); ok {
			hits[i] = h
		} else {
			plain = append(plain, i)
		}
	}
	if len(plain) > 0 {
		sub := make([]store.LogoutReq, len(plain))
		for j, i := range plain {
			sub[j] = reqs[i]
		}
		s.Store.LogoutEach(ctx, sub, func(j int, r store.LogoutResult) { done(plain[j], r) })
	}
	ctx = context.WithoutCancel(ctx)
	for i, h := range hits {
		if !h.After {
			if err := h.Wait(ctx); err != nil {
				done(i, store.LogoutResult{Err: err})
				continue
			}
		}
		var res store.LogoutResult
		s.Store.LogoutEach(ctx, reqs[i:i+1], func(_ int, r store.LogoutResult) { res = r })
		if h.After {
			if err := h.Wait(ctx); err != nil {
				res = store.LogoutResult{Err: err}
			}
		}
		done(i, res)
	}
}

// ReadSlots is the first Store call of a Reconcile chunk, so a delay here
// holds the whole chunk before Logic reads anything (races 14 and 18), and
// a delay past login_deadline makes its writes come back refused (R3-O6).
func (s faultStore) ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState {
	for _, r := range refs {
		if h, ok := s.f.Match(FaultReconcile, r.UID); ok {
			ctx = context.WithoutCancel(ctx)
			if err := h.Wait(ctx); err != nil {
				out := make([]store.SlotState, len(refs))
				for i := range out {
					out[i].Err = err
				}
				return out
			}
			break
		}
	}
	return s.Store.ReadSlots(ctx, refs)
}

// faultUnary holds or fails a ReconcileEnd RPC before its delete phase
// starts. It sits at the RPC, not at the store: the phase scans gwconns
// first, so a store hook would see only the uids that were there already.
func faultUnary(f *fault.Set) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if r, ok := req.(*pb.ReconcileEndRequest); ok {
			if hit, ok := f.Match(FaultReconcileEnd, r.GetGw().GetGateway()); ok {
				if err := hit.Wait(context.WithoutCancel(ctx)); err != nil {
					return nil, status.Errorf(codes.Unavailable, "fault: ReconcileEnd: %v", err)
				}
			}
		}
		return h(ctx, req)
	}
}

// FaultHook fails ZADD gwepochs for the gateway the gwepochs rule names,
// while the lease SET before it succeeds (R4M-3). Add it to Logic's Redis
// client only when -faults is set.
func FaultHook(f *fault.Set) redis.Hook { return gwepochsFault{f} }

type gwepochsFault struct{ f *fault.Set }

func (gwepochsFault) DialHook(next redis.DialHook) redis.DialHook { return next }

func (gwepochsFault) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h gwepochsFault) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// ZADD gwepochs <score> <gw>:<epoch>
		if a := cmd.Args(); len(a) == 4 && strings.EqualFold(fmt.Sprint(a[0]), "zadd") && a[1] == "gwepochs" {
			m := fmt.Sprint(a[3])
			if i := strings.LastIndexByte(m, ':'); i > 0 {
				if _, ok := h.f.Match(FaultGWEpochs, m[:i]); ok {
					cmd.SetErr(fault.ErrInjected)
					return fault.ErrInjected
				}
			}
		}
		return next(ctx, cmd)
	}
}

// FileDial dials gateways by the address a JSON file ({"gw-1":
// "127.0.0.1:9101", ...}) maps their id to, read again at every dial. The
// race suite runs gateways as local processes, which have no pod DNS; a new
// epoch of a pod name dials again, so a restart may move its address, as a
// rescheduled pod's DNS name does.
func FileDial(path string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var addrs map[string]string
		if err := json.Unmarshal(b, &addrs); err != nil {
			return nil, fmt.Errorf("kick address file %s: %w", path, err)
		}
		addr, ok := addrs[gw]
		if !ok {
			return nil, fmt.Errorf("kick address file %s: no gateway %q", path, gw)
		}
		return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}
