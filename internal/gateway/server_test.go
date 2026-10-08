package gateway

import (
	"errors"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
)

func TestLimitsSelfCheck(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	cfg.MaxConns = 1000 // needs nofile ≥ 1100, nf_conntrack_max ≥ 2000
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
		{"enough", src(1100, "2000\n", nil), false, ""},
		{"nofile too low", src(1099, "2000\n", nil), false, "RLIMIT_NOFILE"},
		{"conntrack too low", src(1100, "1999\n", nil), false, "nf_conntrack_max"},
		{"conntrack too low but notrack", src(1100, "10\n", nil), true, ""},
		{"conntrack not loaded", src(1100, "", fs.ErrNotExist), false, ""},
		{"conntrack unreadable", src(1100, "", errors.New("permission denied")), false, "nf_conntrack_max"},
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
