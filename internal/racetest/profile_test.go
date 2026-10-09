package racetest

import (
	"strings"
	"testing"
	"time"
)

// The test timing profile (spec: Test execution model; choice #59): every
// time-based setting scaled ~10× down with its ratios kept. Lease TTL 3s,
// renewal every 1s, the sweeper guard's windows (derived from those two),
// Kick retries 1s apart, the Logout and ReconcileEnd retry budget 6s, the
// drain window 6s, heartbeat 15s, and the spec's 90s outages become 9s in
// the scenarios. login_deadline (5s) and the 1s skew allowance are not
// scaled: they are tied to real RPC latency. The sess:/rev: TTL stays 30d,
// and token lifetimes are set per scenario.
//
// The binaries check the ratios themselves (TestProfileBreakingARatioIsRefused).

// logicProfile is appended to every Logic's flags.
var logicProfile = []string{
	"-lease-ttl", "3s", "-renew-interval", "1s", "-redis-failover", "1500ms", "-lease-cache", "100ms",
	"-sweep-interval", "500ms",
	"-kick-retry-spacing", "1s", "-kick-idle-timeout", "6s",
	"-jwks-refresh", "30s", "-jwks-retry-empty", "500ms", "-jwks-unknown-kid", "3s",
}

// gatewayProfile is appended to every gateway's flags. The periodic
// Reconcile cycle scales to 60s ± 6s; scenarios run passes on demand
// instead (Options.PeriodicPasses, choice #60).
var gatewayProfile = []string{
	"-lease-interval", "1s", "-lease-timeout", "1s", "-lease-ttl", "3s",
	"-heartbeat", "15s", "-heartbeat-min", "3s", "-heartbeat-max", "30s",
	"-retry-for", "6s", "-retry-backoff", "100ms", "-retry-max-delay", "1s",
	"-reconcile-cycle", "60s", "-reconcile-jitter", "6s",
	"-drain-window", "6s", "-drain-jitter", "500ms", "-drain-flush", "1s", "-drain-timeout", "30s",
}

// The binaries refuse a profile that breaks a required ratio (spec: Test
// execution model): Logic's lease TTL must exceed the failover time plus a
// renewal interval (R2-O5), and the gateway's lease TTL its renewal interval.
func TestProfileBreakingARatioIsRefused(t *testing.T) {
	suite(t)
	dir := t.TempDir()
	logicArgs := append(append([]string{"-jwks-url", "http://127.0.0.1:1/"}, logicProfile...), "-redis-failover", "2s")
	gwArgs := append(append([]string{"-id", "gw-0", "-noise-keys", dir}, gatewayProfile...), "-lease-ttl", "1s")
	for _, c := range []struct {
		bin  string
		args []string
		want string
	}{{"logic", logicArgs, "R2-O5"}, {"gateway", gwArgs, "LeaseTTL"}} {
		p := startProc(t, dir, c.bin, c.bin, c.args...)
		select {
		case <-p.done:
			p.stopped.Store(true) // the exit is what this test wants
		case <-time.After(10 * time.Second):
			t.Fatalf("%s started with a broken ratio", c.bin)
		}
		if p.cmd.ProcessState.ExitCode() == 0 || !strings.Contains(strings.Join(p.tail(5), "\n"), c.want) {
			t.Fatalf("%s: exit %d, log %q; want a refusal naming %s", c.bin, p.cmd.ProcessState.ExitCode(), p.tail(5), c.want)
		}
	}
}
