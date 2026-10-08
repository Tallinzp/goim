package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// source is a JWKS endpoint whose document and availability tests change.
type source struct {
	mu    sync.Mutex
	doc   []byte
	down  bool
	calls atomic.Int32
}

func (s *source) fetch(context.Context) ([]byte, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return nil, errors.New("account service down")
	}
	return s.doc, nil
}

func (s *source) publish(t *testing.T, keys ...PrivateKey) {
	t.Helper()
	doc, err := MarshalJWKS(keys...)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.doc, s.down = doc, false
	s.mu.Unlock()
}

func noFallback() ([]byte, error) { return nil, errors.New("no fallback") }

func testManager(src *source, fallback func() ([]byte, error), clk *fakeClock) *Manager {
	cfg := DefaultManagerConfig("unused", "unused")
	return newManager(cfg, Observer{}, src.fetch, fallback, clk.Now)
}

func TestManagerFallsBackToConfigMap(t *testing.T) {
	k := testKey(t, "k1")
	doc, err := MarshalJWKS(k)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultManagerConfig("http://127.0.0.1:1/jwks", path) // nothing listens on port 1
	cfg.FetchTimeout = 500 * time.Millisecond
	var fallbackSeen bool
	m := NewManager(cfg, Observer{Source: func(fb bool) { fallbackSeen = fb }})
	if m.Ready() {
		t.Fatal("ready before Load")
	}
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !m.Ready() || !m.OnFallback() || !fallbackSeen {
		t.Fatalf("ready %v fallback %v seen %v", m.Ready(), m.OnFallback(), fallbackSeen)
	}
	if _, err := m.Verify(sign(t, k, claims(time.Now().UnixMilli()))); err != nil {
		t.Fatalf("token from the fallback key: %v", err)
	}
}

func TestManagerNotReadyWithoutAnySource(t *testing.T) {
	src := &source{down: true}
	m := testManager(src, noFallback, &fakeClock{t: time.Unix(5, 0)})
	if err := m.Load(context.Background()); err == nil {
		t.Fatal("Load must fail with no source")
	}
	if m.Ready() {
		t.Fatal("ready with no key set")
	}
	if _, err := m.Verify("a.b.c"); !errors.Is(err, ErrKeyUnknown) {
		t.Fatalf("verify with no key set: %v", err)
	}
}

func TestManagerFetchesOverHTTP(t *testing.T) {
	k := testKey(t, "k1")
	doc, _ := MarshalJWKS(k)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write(doc) }))
	defer srv.Close()
	m := NewManager(DefaultManagerConfig(srv.URL, ""), Observer{})
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.OnFallback() {
		t.Fatal("set came from the URL, not the fallback")
	}
}

func TestUnknownKidFloodCausesOneFetchPer30s(t *testing.T) {
	k1, k2 := testKey(t, "k1"), testKey(t, "k2")
	src := &source{}
	src.publish(t, k1)
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	m := testManager(src, noFallback, clk)
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.calls.Store(0)

	// k2 starts signing before Logic has it (a broken runbook): every request
	// is rejected at once, and the flood shares one background fetch.
	src.publish(t, k1, k2)
	tok := sign(t, k2, claims(clk.Now().UnixMilli()))
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Verify(tok); !errors.Is(err, ErrKeyUnknown) && err != nil {
				t.Errorf("verify: %v", err)
			}
		}()
	}
	wg.Wait()
	m.bg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}
	// The refresh landed: the client's retry after 1–3s succeeds.
	if _, err := m.Verify(tok); err != nil {
		t.Fatalf("after refresh: %v", err)
	}

	// A kid still unknown is rejected from memory until the window ends.
	tok9 := sign(t, testKey(t, "k9"), claims(clk.Now().UnixMilli()))
	clk.Add(29 * time.Second)
	m.Verify(tok9)
	m.bg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("fetches inside the window = %d, want 1", n)
	}
	clk.Add(time.Second)
	m.Verify(tok9)
	m.bg.Wait()
	if n := src.calls.Load(); n != 2 {
		t.Fatalf("fetches after 30s = %d, want 2", n)
	}
}

func TestFailedRefreshKeepsLastGoodSet(t *testing.T) {
	k := testKey(t, "k1")
	src := &source{}
	src.publish(t, k)
	var failures atomic.Int32
	m := newManager(DefaultManagerConfig("u", ""), Observer{RefreshFailed: func(error) { failures.Add(1) }},
		src.fetch, noFallback, (&fakeClock{t: time.Unix(1_000_000, 0)}).Now)
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	src.down = true
	src.mu.Unlock()
	if err := m.refreshURL(context.Background()); err == nil {
		t.Fatal("refresh against a down source succeeded")
	}
	if !m.Ready() || failures.Load() != 1 {
		t.Fatalf("ready %v failures %d", m.Ready(), failures.Load())
	}
	if _, err := m.Verify(sign(t, k, claims(1_000_000_000))); err != nil {
		t.Fatalf("last good set: %v", err)
	}
}
