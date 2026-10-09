// Package fault is the test-only fault injection the race suite drives
// (Plan 5b, choice #58). A Logic or gateway process started with -faults
// serves /fault/ on its admin port; without the flag its *Set is nil, and
// every method on a nil *Set reports no fault, so production code paths are
// unchanged. Rules are keyed by op (one rule per op); the hooks that consult
// them live next to the code they break (internal/logic, internal/gateway).
package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Duration is a time.Duration written as a Go duration string ("6s") in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

// Rule is one injected fault.
type Rule struct {
	Op string `json:"op"` // the hook it breaks, e.g. "login", "kick"
	// Key narrows the hook to one uid, gateway id or method; "" matches all.
	Key string `json:"key,omitempty"`
	// After applies Delay and Fail after the real call instead of before
	// it: the write happened, but its reply is late or lost.
	After bool     `json:"after,omitempty"`
	Delay Duration `json:"delay,omitempty"`
	Fail  bool     `json:"fail,omitempty"`
	// Count is how many matches fire before the rule is spent; 0 = no limit.
	Count int `json:"count,omitempty"`
	// For is how long the rule lasts from Put; 0 = until cleared.
	For Duration `json:"for,omitempty"`
}

// Hit is a rule that matched one call.
type Hit struct {
	After bool
	Delay time.Duration
	Fail  bool
}

// ErrInjected is the error a failing rule makes a hook return.
var ErrInjected = errors.New("fault: injected failure")

// Wait sleeps for the hit's delay, or until ctx ends, then returns
// ErrInjected if the rule fails the call.
func (h Hit) Wait(ctx context.Context) error {
	if h.Delay > 0 {
		t := time.NewTimer(h.Delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	if h.Fail {
		return ErrInjected
	}
	return nil
}

type entry struct {
	Rule
	left    int // matches left; < 0 = no limit
	expires time.Time
	fired   int
}

// Set holds the active rules. A nil *Set has none.
type Set struct {
	mu    sync.Mutex
	rules map[string]*entry
	now   func() time.Time
}

// New returns an empty Set.
func New() *Set { return &Set{rules: map[string]*entry{}, now: time.Now} }

// Put installs r, replacing any rule for the same op.
func (s *Set) Put(r Rule) error {
	if r.Op == "" || r.Count < 0 || r.Delay < 0 || r.For < 0 {
		return fmt.Errorf("fault: bad rule %+v", r)
	}
	e := &entry{Rule: r, left: r.Count}
	if r.Count == 0 {
		e.left = -1
	}
	if r.For > 0 {
		e.expires = s.now().Add(time.Duration(r.For))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[r.Op] = e
	return nil
}

// Clear removes op's rule; "" removes every rule.
func (s *Set) Clear(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op == "" {
		s.rules = map[string]*entry{}
		return
	}
	delete(s.rules, op)
}

// Match reports whether op's rule applies to key, and uses up one count.
func (s *Set) Match(op, key string) (Hit, bool) {
	if s == nil {
		return Hit{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.rules[op]
	if !ok {
		return Hit{}, false
	}
	if e.Key != "" && e.Key != key {
		return Hit{}, false
	}
	// A spent or expired rule stays until cleared, so Fired still counts it.
	if e.left == 0 || (!e.expires.IsZero() && !s.now().Before(e.expires)) {
		return Hit{}, false
	}
	e.fired++
	if e.left > 0 {
		e.left--
	}
	return Hit{After: e.After, Delay: time.Duration(e.Delay), Fail: e.Fail}, true
}

// Fired returns how often op's rule has matched since it was Put, spent or
// not; 0 if there is none.
func (s *Set) Fired(op string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.rules[op]; ok {
		return e.fired
	}
	return 0
}

// Handler serves the fault API under /fault/:
//
//	PUT    /fault/rule          body: a Rule as JSON
//	GET    /fault/rule/{op}     {"fired": n}: how often op's rule matched
//	DELETE /fault/rule/{op}     clear op's rule ({op} empty: every rule)
//	GET    /fault/info          info() as JSON (ids, addresses, epoch); {} if info is nil
//	POST   /fault/action/{name} run a registered action, e.g. "reconcile"
func (s *Set) Handler(info func() any, actions map[string]func()) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /fault/rule", func(w http.ResponseWriter, r *http.Request) {
		var rule Rule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.Put(rule); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /fault/rule/{op}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Fired int `json:"fired"`
		}{s.Fired(r.PathValue("op"))})
	})
	mux.HandleFunc("DELETE /fault/rule/{op...}", func(w http.ResponseWriter, r *http.Request) {
		s.Clear(strings.Trim(r.PathValue("op"), "/"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /fault/info", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var v any = struct{}{}
		if info != nil {
			v = info()
		}
		json.NewEncoder(w).Encode(v)
	})
	mux.HandleFunc("POST /fault/action/{name}", func(w http.ResponseWriter, r *http.Request) {
		f, ok := actions[r.PathValue("name")]
		if !ok {
			http.Error(w, "unknown action", http.StatusNotFound)
			return
		}
		f()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}
