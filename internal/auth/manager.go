package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ManagerConfig configures the key set source (spec: JWKS, R2-O7, R4H-O10).
type ManagerConfig struct {
	URL          string        // account service JWKS endpoint
	FallbackPath string        // mounted jwks-fallback ConfigMap file; "" for none
	Refresh      time.Duration // periodic refresh (5m)
	RetryEmpty   time.Duration // retry interval while no key set is held (5s)
	UnknownKid   time.Duration // at most one unknown-kid refresh per this long (30s)
	FetchTimeout time.Duration // per fetch (5s)
	Leeway       time.Duration // allowed exp overrun for issuer↔Logic clock skew (30s)
}

// DefaultManagerConfig returns the spec defaults for url and fallback.
func DefaultManagerConfig(url, fallback string) ManagerConfig {
	return ManagerConfig{URL: url, FallbackPath: fallback, Refresh: 5 * time.Minute,
		RetryEmpty: 5 * time.Second, UnknownKid: 30 * time.Second,
		FetchTimeout: 5 * time.Second, Leeway: 30 * time.Second}
}

// Observer receives the Manager's events, for metrics and alerts. Nil fields are skipped.
type Observer struct {
	RefreshFailed     func(err error)     // a fetch from the account service failed
	UnknownKidRefresh func()              // an unknown kid started a refresh
	Source            func(fallback bool) // the key set now in use came from the fallback file (true) or the URL
}

// Manager holds the current key set and keeps it fresh.
type Manager struct {
	cfg      ManagerConfig
	fetch    func(ctx context.Context) ([]byte, error)
	fallback func() ([]byte, error)
	now      func() time.Time
	obs      Observer

	keys       atomic.Pointer[KeySet]
	onFallback atomic.Bool

	mu          sync.Mutex
	nextUnknown time.Time       // unknown-kid refreshes are allowed from this time on
	ctx         context.Context // Run's context, for background refreshes
	bg          sync.WaitGroup  // background refreshes (tests wait on it)
	stopped     bool            // Run has ended: no new background refreshes
}

// NewManager builds a Manager that fetches cfg.URL over HTTP and reads
// cfg.FallbackPath from disk.
func NewManager(cfg ManagerConfig, obs Observer) *Manager {
	client := &http.Client{Timeout: cfg.FetchTimeout}
	fetch := func(ctx context.Context) ([]byte, error) {
		if cfg.URL == "" {
			return nil, errors.New("auth: no JWKS URL configured")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("auth: JWKS fetch: %s", resp.Status)
		}
		return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	}
	fallback := func() ([]byte, error) {
		if cfg.FallbackPath == "" {
			return nil, errors.New("auth: no fallback configured")
		}
		return os.ReadFile(cfg.FallbackPath)
	}
	return newManager(cfg, obs, fetch, fallback, time.Now)
}

func newManager(cfg ManagerConfig, obs Observer, fetch func(context.Context) ([]byte, error),
	fallback func() ([]byte, error), now func() time.Time) *Manager {
	return &Manager{cfg: cfg, fetch: fetch, fallback: fallback, now: now, obs: obs, ctx: context.Background()}
}

// Ready reports whether a key set is held from either source (spec: Logic is
// Ready only once it holds a key set).
func (m *Manager) Ready() bool { return m.keys.Load() != nil }

// OnFallback reports whether the key set in use came from the fallback file.
func (m *Manager) OnFallback() bool { return m.onFallback.Load() }

// Load does the startup load: the account service first, then the fallback
// file. It returns an error only if neither source gave a key set.
func (m *Manager) Load(ctx context.Context) error {
	if err := m.refreshURL(ctx); err == nil {
		return nil
	}
	data, err := m.fallback()
	if err != nil {
		return fmt.Errorf("auth: no key set: fallback: %w", err)
	}
	ks, err := ParseJWKS(data)
	if err != nil {
		return fmt.Errorf("auth: no key set: fallback: %w", err)
	}
	m.set(ks, true)
	return nil
}

// Run refreshes until ctx ends: every Refresh while a key set is held, every
// RetryEmpty (trying the fallback too) while none is. Call Load first.
func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	m.mu.Unlock()
	for {
		wait := m.cfg.Refresh
		if !m.Ready() {
			wait = m.cfg.RetryEmpty
		}
		select {
		case <-ctx.Done():
			// Under mu, so every bg.Add happens before this Wait or not at all.
			m.mu.Lock()
			m.stopped = true
			m.mu.Unlock()
			m.bg.Wait()
			return
		case <-time.After(wait):
		}
		if m.Ready() {
			_ = m.refreshURL(ctx) // failure: keep the last good set (counted by the observer)
		} else {
			_ = m.Load(ctx)
		}
	}
}

// refreshURL fetches the account service's set and installs it on success.
func (m *Manager) refreshURL(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.FetchTimeout)
	defer cancel()
	data, err := m.fetch(ctx)
	if err == nil {
		var ks KeySet
		if ks, err = ParseJWKS(data); err == nil {
			m.set(ks, false)
			return nil
		}
	}
	if m.obs.RefreshFailed != nil {
		m.obs.RefreshFailed(err)
	}
	return err
}

func (m *Manager) set(ks KeySet, fallback bool) {
	m.keys.Store(&ks)
	m.onFallback.Store(fallback)
	if m.obs.Source != nil {
		m.obs.Source(fallback)
	}
}

// Verify checks token against the current key set. An unknown kid is
// rejected at once with ErrKeyUnknown and may start a background refresh.
func (m *Manager) Verify(token string) (Claims, error) {
	ksp := m.keys.Load()
	if ksp == nil {
		return Claims{}, fmt.Errorf("%w: no key set loaded", ErrKeyUnknown)
	}
	c, err := Verify(token, *ksp, m.now().UnixMilli(), m.cfg.Leeway.Milliseconds())
	if errors.Is(err, ErrKeyUnknown) {
		m.unknownKid()
	}
	return c, err
}

// unknownKid starts a background refresh unless one started less than
// UnknownKid ago. This one gate gives the spec's three rules: the rate limit
// (one refresh per 30s per pod, across kids), single-flight (requests inside
// the window share the refresh already started) and the negative cache (a kid
// still unknown after that refresh is rejected from memory until the window
// ends, without a fetch).
func (m *Manager) unknownKid() {
	m.mu.Lock()
	now := m.now()
	if m.stopped || now.Before(m.nextUnknown) {
		m.mu.Unlock()
		return
	}
	m.nextUnknown = now.Add(m.cfg.UnknownKid)
	ctx := m.ctx
	m.bg.Add(1)
	m.mu.Unlock()
	if m.obs.UnknownKidRefresh != nil {
		m.obs.UnknownKidRefresh()
	}
	go func() {
		defer m.bg.Done()
		_ = m.refreshURL(ctx)
	}()
}
