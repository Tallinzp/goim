package fault

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNilSetHasNoFaults(t *testing.T) {
	var s *Set
	if _, ok := s.Match("login", "u1"); ok {
		t.Fatal("a nil Set matched")
	}
	if s.Fired("login") != 0 {
		t.Fatal("a nil Set fired")
	}
}

func TestMatchByOpAndKey(t *testing.T) {
	s := New()
	if err := s.Put(Rule{Op: "login", Key: "u1", After: true, Delay: Duration(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Match("login", "u2"); ok {
		t.Fatal("matched another key")
	}
	if _, ok := s.Match("logout", "u1"); ok {
		t.Fatal("matched another op")
	}
	h, ok := s.Match("login", "u1")
	if !ok || !h.After || h.Delay != 2*time.Second || h.Fail {
		t.Fatalf("Match = %+v, %v", h, ok)
	}
	if s.Fired("login") != 1 {
		t.Fatalf("Fired = %d, want 1", s.Fired("login"))
	}
	if err := s.Put(Rule{Op: "kick", Fail: true}); err != nil {
		t.Fatal(err)
	}
	if h, ok := s.Match("kick", "anything"); !ok || !h.Fail {
		t.Fatalf("an empty key must match every key: %+v, %v", h, ok)
	}
}

func TestCountSpendsTheRule(t *testing.T) {
	s := New()
	s.Put(Rule{Op: "reconcile_end", Fail: true, Count: 2})
	for i := 0; i < 2; i++ {
		if _, ok := s.Match("reconcile_end", "gw-1"); !ok {
			t.Fatalf("match %d missed", i)
		}
	}
	if _, ok := s.Match("reconcile_end", "gw-1"); ok {
		t.Fatal("matched after its count was spent")
	}
	if n := s.Fired("reconcile_end"); n != 2 {
		t.Fatalf("Fired = %d after the rule was spent, want 2", n)
	}
}

func TestForExpiresTheRule(t *testing.T) {
	s := New()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }
	s.Put(Rule{Op: "logic", Fail: true, For: Duration(9 * time.Second)})
	now = now.Add(8 * time.Second)
	if _, ok := s.Match("logic", ""); !ok {
		t.Fatal("expired early")
	}
	now = now.Add(time.Second)
	if _, ok := s.Match("logic", ""); ok {
		t.Fatal("matched at its expiry")
	}
	if n := s.Fired("logic"); n != 1 {
		t.Fatalf("Fired = %d after the rule expired, want 1", n)
	}
}

func TestPutReplacesAndClearRemoves(t *testing.T) {
	s := New()
	s.Put(Rule{Op: "login", Key: "u1"})
	s.Put(Rule{Op: "login", Key: "u2"})
	if _, ok := s.Match("login", "u1"); ok {
		t.Fatal("the replaced rule still matched")
	}
	s.Put(Rule{Op: "kick"})
	s.Clear("login")
	if _, ok := s.Match("login", "u2"); ok {
		t.Fatal("Clear left the rule")
	}
	s.Clear("")
	if _, ok := s.Match("kick", ""); ok {
		t.Fatal(`Clear("") left a rule`)
	}
	if err := s.Put(Rule{}); err == nil {
		t.Fatal("a rule without an op was accepted")
	}
}

func TestHitWait(t *testing.T) {
	if err := (Hit{Delay: time.Millisecond}).Wait(context.Background()); err != nil {
		t.Fatalf("delay only: %v", err)
	}
	if err := (Hit{Fail: true}).Wait(context.Background()); !errors.Is(err, ErrInjected) {
		t.Fatalf("fail: %v, want ErrInjected", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (Hit{Delay: time.Hour}).Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestHandler(t *testing.T) {
	s := New()
	ran := false
	srv := httptest.NewServer(s.Handler(func() any { return map[string]string{"gw": "gw-1"} },
		map[string]func(){"reconcile": func() { ran = true }}))
	defer srv.Close()
	do := func(method, path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if r := do("PUT", "/fault/rule", `{"op":"login","key":"u1","delay":"6s","after":true,"count":1}`); r.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT = %d", r.StatusCode)
	}
	if h, ok := s.Match("login", "u1"); !ok || h.Delay != 6*time.Second || !h.After {
		t.Fatalf("rule from JSON = %+v, %v", h, ok)
	}
	if r := do("PUT", "/fault/rule", `{"op":"login","delay":"soon"}`); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad duration: PUT = %d, want 400", r.StatusCode)
	}
	resp, err := http.Get(srv.URL + "/fault/rule/login")
	if err != nil {
		t.Fatal(err)
	}
	fired, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.TrimSpace(string(fired)) != `{"fired":1}` {
		t.Fatalf("GET rule = %s, want fired 1", fired)
	}
	s.Put(Rule{Op: "kick"})
	do("DELETE", "/fault/rule/kick", "")
	if _, ok := s.Match("kick", ""); ok {
		t.Fatal("DELETE left the rule")
	}
	if r := do("POST", "/fault/action/reconcile", ""); r.StatusCode != http.StatusNoContent || !ran {
		t.Fatalf("action: %d, ran %v", r.StatusCode, ran)
	}
	if r := do("POST", "/fault/action/nope", ""); r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown action = %d, want 404", r.StatusCode)
	}
	resp, err = http.Get(srv.URL + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"gw":"gw-1"`) {
		t.Fatalf("info = %s", body)
	}
}
