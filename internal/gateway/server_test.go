package gateway

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestLimitsSelfCheck(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	cfg.MaxConns, cfg.HeldCap = 1000, 100 // needs nofile ≥ 1100 + 100, nf_conntrack_max ≥ 2000
	src := func(nofile uint64, conntrack string, err error) limitSource {
		return limitSource{
			nofile:   func() (uint64, error) { return nofile, nil },
			readFile: func(string) ([]byte, error) { return []byte(conntrack), err },
		}
	}
	for _, tc := range []struct {
		name    string
		src     limitSource
		notrack bool
		fail    string
	}{
		{"enough", src(1200, "2000\n", nil), false, ""},
		{"nofile too low", src(1199, "2000\n", nil), false, "RLIMIT_NOFILE"},
		{"conntrack too low", src(1200, "1999\n", nil), false, "nf_conntrack_max"},
		{"conntrack too low but notrack", src(1200, "10\n", nil), true, ""},
		{"conntrack not loaded", src(1200, "", fs.ErrNotExist), false, ""},
		{"conntrack unreadable", src(1200, "", errors.New("permission denied")), false, "nf_conntrack_max"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			c.Limits.Notrack = tc.notrack
			err := checkLimits(c, tc.src, m)
			if (err == nil) != (tc.fail == "") || (err != nil && !strings.Contains(err.Error(), tc.fail)) {
				t.Fatalf("err %v, want failure mentioning %q", err, tc.fail)
			}
		})
	}
}

func readyz(t *testing.T, g *Gateway) int {
	t.Helper()
	_, admin := g.Addrs()
	resp, err := http.Get("http://" + admin + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestReadyOnlyAfterLogicAndFirstLease(t *testing.T) {
	gate := make(chan struct{})
	var keysLoaded atomic.Bool
	l := startLive(t, nil, func(g *Gateway, f *fakeLogic) {
		g.readyChecks = []func() error{func() error {
			if !keysLoaded.Load() {
				return errors.New("Noise key set not loaded")
			}
			return nil
		}}
		f.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) { <-gate; return &pb.LeaseReply{Missing: true}, nil }
	})
	waitFor(t, "Logic channel up", l.g.logicUp.Load)
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("Ready before the first GatewayLease succeeded")
	}
	close(gate)
	waitFor(t, "first lease", l.g.leaseOK.Load)
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("Ready before the transport's key set is loaded")
	}
	keysLoaded.Store(true)
	if readyz(t, l.g) != http.StatusOK {
		t.Fatalf("not Ready: %v", l.g.Ready())
	}
}

// The node's nf_conntrack_count is a gauge read at each scrape, where the
// file is readable at startup, and absent where it is not (spec:
// Observability, limits; run 30: the table filled unseen).
func TestConntrackCountGauge(t *testing.T) {
	n := "120\n"
	c := conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return []byte(n), nil }})
	if c == nil {
		t.Fatal("a readable count has no gauge")
	}
	if v := testutil.ToFloat64(c); v != 120 {
		t.Fatalf("gauge %v, want 120", v)
	}
	n = "121\n"
	if v := testutil.ToFloat64(c); v != 121 {
		t.Fatalf("gauge %v after the count changed, want 121", v)
	}
	for _, err := range []error{fs.ErrNotExist, errors.New("permission denied")} {
		if conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return nil, err }}) != nil {
			t.Fatalf("%v: a gauge, want none", err)
		}
	}
	if conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return []byte("x\n"), nil }}) != nil {
		t.Fatal("an unparsable count: a gauge, want none")
	}
}

// Start registers the gauge, so /metrics serves it.
func TestMetricsServeConntrackCount(t *testing.T) {
	l := startLive(t, nil, nil) // okLimits: every limit file reads 4000000
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "goim_gateway_nf_conntrack_count 4e+06") {
		t.Fatal("/metrics has no goim_gateway_nf_conntrack_count 4e+06")
	}
}

func TestFailedLimitsKeepTheGatewayNotReady(t *testing.T) {
	l := startLive(t, nil, func(g *Gateway, _ *fakeLogic) {
		g.limits = limitSource{nofile: func() (uint64, error) { return 10, nil }, readFile: okLimits.readFile}
	})
	waitFor(t, "first lease", l.g.leaseOK.Load)
	if err := l.g.Ready(); err == nil || !strings.Contains(err.Error(), "RLIMIT_NOFILE") {
		t.Fatalf("Ready() = %v, want the nofile failure", err)
	}
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("/readyz is OK with a failed limits check")
	}
}

// The admin port serves pprof, so load runs can save profiles (choice #85).
func TestAdminServesPprof(t *testing.T) {
	l := startLive(t, nil, nil)
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine profile:") {
		t.Fatalf("pprof: %d %.80q", resp.StatusCode, body)
	}
}
