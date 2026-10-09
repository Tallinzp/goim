package racetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
)

// sdkProfile is the SDK's side of the test timing profile (Plan 5a,
// Execution Notes → For Plan 5b): the built-in windows and the backoff
// scaled ÷10 like the servers, and the heartbeat range lowered to the
// gateways' 3s–30s. The client timeouts (token, connect, Login, Pong) are
// not scaled: like login_deadline, they are tied to network latency.
func sdkProfile(cfg *sdk.Config) {
	cfg.Windows = sdk.Windows{
		Storm:        sdk.Window{Lo: 0, Hi: 2 * time.Second},
		Local:        sdk.Window{Lo: 0, Hi: 100 * time.Millisecond},
		PreHandshake: sdk.Window{Lo: 50 * time.Millisecond, Hi: 150 * time.Millisecond},
		Retryable:    sdk.Window{Lo: 100 * time.Millisecond, Hi: 300 * time.Millisecond},
	}
	cfg.Backoff = sdk.Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second}
	cfg.Heartbeat = sdk.HeartbeatRange{Min: 3 * time.Second, Max: 30 * time.Second}
}

// events records an SDK client's events.
type events struct {
	mu  sync.Mutex
	evs []sdk.Event
	got int
}

func (r *events) add(e sdk.Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// next waits up to d for an event of kind, skipping others, and returns it.
func (r *events) next(t *testing.T, kind sdk.EventKind, d time.Duration) sdk.Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for r.got < len(r.evs) {
			e := r.evs[r.got]
			r.got++
			if e.Kind == kind {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %v event within %v", kind, d)
	return sdk.Event{}
}

// none fails t if an event of kind arrives within d.
func (r *events) none(t *testing.T, kind sdk.EventKind, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.evs[r.got:] {
		if e.Kind == kind {
			t.Fatalf("unexpected %v event: %+v", kind, e)
		}
	}
	r.got = len(r.evs)
}

// SDK returns an idle SDK client for uid that picks one of the given
// gateways at random per attempt (all of them if none is given).
func (s *Stack) SDK(uid string, store sdk.Store, gws ...int) (*sdk.Client, *events) {
	s.t.Helper()
	if len(gws) == 0 {
		for i := range s.GW {
			gws = append(gws, i)
		}
	}
	r := &events{}
	cfg := sdk.DefaultConfig()
	sdkProfile(&cfg)
	for _, i := range gws {
		cfg.Addrs = append(cfg.Addrs, s.GW[i].Listen)
	}
	cfg.KeyID, cfg.ServerKey = 1, s.noise.Public
	cfg.AppID, cfg.Platform = "im", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return s.Token(uid), nil }
	cfg.Store, cfg.OnEvent = store, r.add
	c, err := sdk.New(cfg)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(c.Close)
	return c, r
}
