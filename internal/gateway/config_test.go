package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// testConfig is a small valid configuration: 4 loops, caps of 1000.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-1", 0xE1
	cfg.NumLoops = 4
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 1000, 1000, 1000, 1000
	return cfg
}

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-0", NewEpoch()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.DeadAfter() != 310*time.Second || cfg.FenceAfter() != 6*time.Second {
		t.Fatalf("dead after %v, fence after %v; want 310s and 6s", cfg.DeadAfter(), cfg.FenceAfter())
	}
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutRangeIsEnforced(t *testing.T) {
	for _, d := range []time.Duration{2 * time.Second, 31 * time.Second} {
		cfg := testConfig()
		cfg.HandshakeTimeout = d
		if cfg.Validate() == nil {
			t.Errorf("HandshakeTimeout %v accepted", d)
		}
		cfg = testConfig()
		cfg.AuthTimeout = d
		if cfg.Validate() == nil {
			t.Errorf("AuthTimeout %v accepted", d)
		}
	}
	for _, d := range []time.Duration{3 * time.Second, 30 * time.Second} {
		cfg := testConfig()
		cfg.HandshakeTimeout, cfg.AuthTimeout = d, d
		if err := cfg.Validate(); err != nil {
			t.Errorf("%v refused: %v", d, err)
		}
	}
}

func TestValidateRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*Config)
		want string
	}{
		{"bad gateway id", func(c *Config) { c.GatewayID = "GW_1" }, "gateway id"},
		{"zero epoch", func(c *Config) { c.Epoch = 0 }, "epoch"},
		{"no loops", func(c *Config) { c.NumLoops = 0 }, "NumLoops"},
		{"too many loops", func(c *Config) { c.NumLoops = 1<<16 + 1 }, "NumLoops"},
		{"zero cap", func(c *Config) { c.MaxConns = 0 }, "MaxConns"},
		{"Login timeout below the fence", func(c *Config) { c.LoginTimeout = 5900 * time.Millisecond }, "LoginTimeout"},
		{"fractional heartbeat", func(c *Config) { c.Heartbeat = 1500 * time.Millisecond }, "Heartbeat"},
		{"heartbeat below its range", func(c *Config) { c.Heartbeat = 10 * time.Second }, "outside [30s, 5m0s]"},
		{"heartbeat above its range", func(c *Config) { c.Heartbeat = 600 * time.Second }, "outside [30s, 5m0s]"},
		{"inverted heartbeat range", func(c *Config) { c.HeartbeatMin = 10 * time.Minute }, "HeartbeatMin"},
		{"lease timeout above the interval", func(c *Config) { c.LeaseTimeout = 11 * time.Second }, "LeaseTimeout"},
		{"lease TTL not above the interval", func(c *Config) { c.LeaseTTL = 10 * time.Second }, "LeaseTTL"},
		{"jitter of half a cycle", func(c *Config) { c.Reconcile.Jitter = 5 * time.Minute }, "Jitter"},
		{"empty Busy window", func(c *Config) { c.BusyRetry = Window{3 * time.Second, time.Second} }, "BusyRetry"},
		{"inverted LoginResp window", func(c *Config) { c.Windows.Retryable = Window{2 * time.Second, time.Second} }, "Retryable"},
		// P6b1I-1: a ReconcileEnd lists every tracked conn_id, open or
		// awaiting its Logout, and must fit Logic's message limit.
		{"ReconcileEnd above Logic's limit", func(c *Config) { c.MaxConns, c.LogoutQueue = 1_100_000, 1_100_000 }, "ReconcileEnd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.mod(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want an error about %s", err, tc.want)
			}
		})
	}
}

func TestNewEpochIsNonZeroAndRandom(t *testing.T) {
	a, b := NewEpoch(), NewEpoch()
	if a == 0 || b == 0 || a == b {
		t.Fatalf("epochs %x, %x", a, b)
	}
}

func TestNewMetricsRegistersEverything(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.Conns.WithLabelValues("online").Inc()
	m.Rejects.WithLabelValues(RejectAtCapacity).Inc()
	families, err := reg.Gather()
	if err != nil || len(families) < 2 {
		t.Fatalf("gathered %d families, err %v", len(families), err)
	}
}

// P6b2a-b1I-2: every reject reason is exported at 0 before its first reject,
// so a scrape reads "no reject" as 0, not as a missing series.
func TestRejectReasonsExportedAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	n, err := testutil.GatherAndCount(reg, "goim_gateway_admission_rejects_total")
	if err != nil || n != 9 {
		t.Fatalf("%d admission_rejects_total series before any reject, want 9 (err %v)", n, err)
	}
}

func TestRefusalHoldIsValidated(t *testing.T) {
	for name, mod := range map[string]func(*Config){
		"negative hold":          func(c *Config) { c.RefuseHold = -time.Millisecond },
		"hold ≥ handshake limit": func(c *Config) { c.RefuseHold = c.HandshakeTimeout },
		"negative cap":           func(c *Config) { c.HeldCap = -1 },
	} {
		cfg := testConfig()
		mod(&cfg)
		if cfg.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
