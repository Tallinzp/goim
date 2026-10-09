package racetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/wire"
)

// Options change a stack from the default: four gateways on the profile,
// Reconcile passes only on demand, and an account service that is up.
type Options struct {
	Gateways int      // 0 means 4 (spec: Redis, Logic, 4 gateways)
	Logic    []string // extra Logic flags, after the profile's (last wins)
	Gateway  []string // extra flags for every gateway
	// PeriodicPasses keeps the profile's 60s ± 6s Reconcile cycle. By
	// default each gateway runs passes only when a scenario asks
	// (Gateway.Reconcile), so no pass the scenario did not ask for can
	// repair what it is about to check (choice #60).
	PeriodicPasses bool
	// NoJWKS starts Logic with the account service down: only the
	// jwks-fallback file can give it a key set.
	NoJWKS bool
	// NoFallback starts Logic without a jwks-fallback file.
	NoFallback bool
}

// Stack is one scenario's processes.
type Stack struct {
	t   *testing.T
	dir string
	opt Options

	Redis *Redis
	Logic *Logic
	GW    []*Gateway

	jwtKey   auth.PrivateKey // signs tokens
	jwksPath string          // what devtoken serves, re-read per request
	jwksURL  string
	fallback string
	noise    wire.Key
	noiseDir string
	kickFile string
	kicks    map[string]string // gateway id → Kick address (kickFile)
	rules    []*setRule        // every fault rule the scenario set
}

// setRule is a fault rule the scenario set, checked at the end: a rule that
// never fired means the scenario did not exercise what it claims.
type setRule struct {
	p       *proc
	admin   string
	op      string
	fired   int // read when the rule was cleared
	cleared bool
}

// Logic is the stack's Logic process.
type Logic struct {
	*proc
	GRPC, Lease, Admin string
	args               []string
}

// Gateway is one gateway process. A restart keeps ID and gets a new Epoch.
type Gateway struct {
	*proc
	Index               int
	ID                  string
	Epoch               uint64
	Listen, Kick, Admin string
	extra               []string
}

// New starts a stack and waits until Logic and every gateway are Ready.
func New(t *testing.T, opt Options) *Stack {
	t.Helper()
	s := base(t, opt)
	s.Logic = s.launchLogic()
	s.waitReady(s.Logic.proc, s.Logic.Admin)
	s.GW = make([]*Gateway, s.opt.Gateways)
	for i := range s.GW {
		s.GW[i] = s.launchGateway(i, nil)
	}
	for _, g := range s.GW {
		s.awaitGateway(g)
	}
	return s
}

// base starts what Logic and the gateways need: Redis, the account service
// (devtoken) and its fallback file, the Noise key and the Kick address file.
func base(t *testing.T, opt Options) *Stack {
	t.Helper()
	suite(t)
	if opt.Gateways == 0 {
		opt.Gateways = 4
	}
	dir := t.TempDir()
	if d := os.Getenv("GOIM_RACE_LOGS"); d != "" {
		dir = filepath.Join(d, strings.ReplaceAll(t.Name(), "/", "_"))
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s := &Stack{t: t, dir: dir, opt: opt, kicks: map[string]string{}, kickFile: filepath.Join(dir, "kick.json")}
	t.Cleanup(s.dumpOnFailure) // cleanups run last-in first-out: after every process's checks
	s.Redis = startRedis(t, dir)
	s.startJWKS()
	s.startNoise()
	s.setKick("", "")
	return s
}

// dumpOnFailure logs the end of every process log when the scenario failed.
func (s *Stack) dumpOnFailure() {
	if !s.t.Failed() {
		return
	}
	ents, _ := os.ReadDir(s.dir)
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		p := &proc{log: filepath.Join(s.dir, e.Name())}
		s.t.Logf("==== %s (last 40 lines) ====\n%s", e.Name(), strings.Join(p.tail(40), "\n"))
	}
}

func (s *Stack) startJWKS() {
	t := s.t
	var err error
	if s.jwtKey, err = auth.GenerateKey("k1"); err != nil {
		t.Fatal(err)
	}
	s.jwksPath = filepath.Join(s.dir, "jwks.json")
	s.PublishKeys(s.jwtKey)
	if !s.opt.NoFallback {
		s.fallback = filepath.Join(s.dir, "jwks-fallback.json")
		doc, _ := auth.MarshalJWKS(s.jwtKey)
		if err := os.WriteFile(s.fallback, doc, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	addr := freePort(t)
	s.jwksURL = "http://" + addr + "/"
	if s.opt.NoJWKS {
		return // nothing listens on addr: the account service is down
	}
	startProc(t, s.dir, "devtoken", "devtoken", "serve", "-addr", addr, "-jwks", s.jwksPath)
	eventually(t, "devtoken serves the JWKS", 10*time.Second, func() bool {
		resp, err := http.Get(s.jwksURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

// PublishKeys makes the account service serve exactly keys (devtoken
// re-reads the file at every request).
func (s *Stack) PublishKeys(keys ...auth.PrivateKey) {
	doc, err := auth.MarshalJWKS(keys...)
	if err != nil {
		s.t.Fatal(err)
	}
	tmp := s.jwksPath + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		s.t.Fatal(err)
	}
	if err := os.Rename(tmp, s.jwksPath); err != nil {
		s.t.Fatal(err)
	}
}

func (s *Stack) startNoise() {
	var err error
	if s.noise, err = wire.GenerateKey(); err != nil {
		s.t.Fatal(err)
	}
	s.noiseDir = filepath.Join(s.dir, "noise")
	priv, _ := wire.EncodeKey(s.noise)
	if err := os.MkdirAll(s.noiseDir, 0o700); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.noiseDir, "1"), []byte(priv+"\n"), 0o600); err != nil {
		s.t.Fatal(err)
	}
}

// setKick records gateway id's Kick address and rewrites the file Logic
// dials from (logic.FileDial), the local stand-in for pod DNS.
func (s *Stack) setKick(id, addr string) {
	if id != "" {
		s.kicks[id] = addr
	}
	doc, _ := json.Marshal(s.kicks)
	tmp := s.kickFile + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		s.t.Fatal(err)
	}
	if err := os.Rename(tmp, s.kickFile); err != nil {
		s.t.Fatal(err)
	}
}

// launchLogic starts Logic without waiting for Ready.
func (s *Stack) launchLogic() *Logic {
	t := s.t
	l := &Logic{GRPC: freePort(t), Lease: freePort(t), Admin: freePort(t)}
	l.args = []string{"-redis", s.Redis.Addr, "-redis-cluster=false",
		"-grpc-addr", l.GRPC, "-lease-addr", l.Lease, "-admin-addr", l.Admin,
		"-jwks-url", s.jwksURL, "-jwks-fallback", s.fallback, "-kick-addr-file", s.kickFile, "-faults"}
	l.args = append(append(l.args, logicProfile...), s.opt.Logic...)
	l.proc = startProc(t, s.dir, "logic", "logic", l.args...)
	return l
}

// RestartLogic kills Logic (a crash), starts it again on the same ports and
// waits until it is Ready.
func (s *Stack) RestartLogic() {
	s.t.Helper()
	l := s.Logic
	l.kill()
	l.proc = startProc(s.t, s.dir, "logic", "logic", l.args...)
	s.waitReady(l.proc, l.Admin)
}

// launchGateway starts gateway i (id gw-i) with extra flags after the
// stack's; awaitGateway waits until it is Ready.
func (s *Stack) launchGateway(i int, extra []string) *Gateway {
	t := s.t
	g := &Gateway{Index: i, ID: fmt.Sprintf("gw-%d", i), Listen: freePort(t), Kick: freePort(t), Admin: freePort(t), extra: extra}
	args := []string{"-id", g.ID, "-listen", "tcp://" + g.Listen, "-noise-keys", s.noiseDir,
		"-logic", s.Logic.GRPC, "-lease", s.Logic.Lease, "-kick-addr", g.Kick, "-admin-addr", g.Admin,
		"-loops", "1", "-max-conns", "1000", "-pending-cap", "1000", "-logout-queue", "1000",
		"-conntrack-notrack", "-faults"}
	args = append(args, gatewayProfile...)
	if !s.opt.PeriodicPasses {
		args = append(args, "-reconcile-cycle", "1h", "-reconcile-jitter", "0s")
	}
	args = append(append(args, s.opt.Gateway...), extra...)
	s.setKick(g.ID, g.Kick)
	g.proc = startProc(t, s.dir, g.ID, "gateway", args...)
	return g
}

// awaitGateway waits until g is Ready and reads its epoch.
func (s *Stack) awaitGateway(g *Gateway) {
	s.t.Helper()
	s.waitReady(g.proc, g.Admin)
	var info struct{ Epoch string }
	if err := getJSON("http://"+g.Admin+"/fault/info", &info); err != nil {
		s.t.Fatalf("%s: /fault/info: %v", g.ID, err)
	}
	g.Epoch, _ = strconv.ParseUint(info.Epoch, 10, 64)
}

// RestartGateway starts gateway i again under the same id (a new epoch)
// with extra flags, after its old process was killed, paused or ended.
func (s *Stack) RestartGateway(i int, extra ...string) *Gateway {
	s.t.Helper()
	g := s.launchGateway(i, extra)
	s.awaitGateway(g)
	s.GW[i] = g
	return g
}

// waitReady waits for /readyz to answer 200; it fails t if the process
// exits or 60s pass (race builds start slowly on two cores).
func (s *Stack) waitReady(p *proc, admin string) {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if p.exited() {
			s.t.Fatalf("%s exited before Ready:\n%s", p.name, strings.Join(p.tail(30), "\n"))
		}
		if resp, err := http.Get("http://" + admin + "/readyz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatalf("%s not Ready after 60s:\n%s", p.name, strings.Join(p.tail(30), "\n"))
}

// ---- the fault API (choice #58) ----

func faultPut(t *testing.T, admin string, r fault.Rule) {
	t.Helper()
	body, _ := json.Marshal(r)
	req, _ := http.NewRequest(http.MethodPut, "http://"+admin+"/fault/rule", bytes.NewReader(body))
	httpDo(t, req)
}

func faultClear(t *testing.T, admin, op string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, "http://"+admin+"/fault/rule/"+op, nil)
	httpDo(t, req)
}

func httpDo(t *testing.T, req *http.Request) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s %s: %s %s", req.Method, req.URL, resp.Status, b)
	}
}

func getJSON(url string, v any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

// LogicFault sets a rule on Logic (ops: logic.FaultLogin, ...).
func (s *Stack) LogicFault(r fault.Rule) { s.setFault(s.Logic.proc, s.Logic.Admin, r) }

// Fault sets a rule on gateway i (ops: gateway.FaultKick, gateway.FaultLogic).
func (s *Stack) Fault(i int, r fault.Rule) { s.setFault(s.GW[i].proc, s.GW[i].Admin, r) }

// ClearFault removes gateway i's rule for op.
func (s *Stack) ClearFault(i int, op string) { s.clearFault(s.GW[i].Admin, op) }

func (s *Stack) setFault(p *proc, admin string, r fault.Rule) {
	s.t.Helper()
	s.retire(admin, r.Op) // Put replaces the rule and its count: keep what the old one fired
	faultPut(s.t, admin, r)
	sr := &setRule{p: p, admin: admin, op: r.Op}
	s.rules = append(s.rules, sr)
	// Registered after its process started, so it runs before that process
	// is killed, a process restarted after the scenario's first rule included.
	s.t.Cleanup(func() { s.checkFired(sr) })
}

func (s *Stack) clearFault(admin, op string) {
	s.t.Helper()
	s.retire(admin, op)
	faultClear(s.t, admin, op)
}

// retire records the fired count of the live rule for op on admin, before
// the scenario clears or replaces it.
func (s *Stack) retire(admin, op string) {
	s.t.Helper()
	for _, r := range s.rules {
		if r.admin == admin && r.op == op && !r.cleared && !r.p.exited() {
			r.fired, r.cleared = faultFired(s.t, admin, op), true
		}
	}
}

// checkFired fails the scenario if rule r never matched, unless the
// scenario killed its process (its counts died with it).
func (s *Stack) checkFired(r *setRule) {
	n := r.fired
	if !r.cleared {
		if r.p.exited() {
			return
		}
		var v struct{ Fired int }
		if err := getJSON("http://"+r.admin+"/fault/rule/"+r.op, &v); err != nil {
			s.t.Errorf("fault %q on %s: %v", r.op, r.p.name, err)
			return
		}
		n = v.Fired
	}
	if n == 0 {
		s.t.Errorf("fault %q on %s never fired: the scenario did not exercise what it claims", r.op, r.p.name)
	}
}

// Reconcile starts a triggered Reconcile pass on gateway i.
func (s *Stack) Reconcile(i int) {
	s.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+s.GW[i].Admin+"/fault/action/reconcile", nil)
	httpDo(s.t, req)
}

// ---- checks ----

// Metric returns the value of one series from admin's /metrics, written as
// the exposition prints it, e.g. `goim_gateway_kicks_total{result="ignored"}`;
// 0 if absent.
func (s *Stack) Metric(admin, series string) float64 {
	s.t.Helper()
	resp, err := http.Get("http://" + admin + "/metrics")
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				s.t.Fatalf("%s: %q", series, line)
			}
			return f
		}
	}
	return 0
}

// eventually polls cond every 50ms until it holds, or fails t after d.
func eventually(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// holds checks that cond stays true for d (polling every 50ms).
func holds(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("no longer true: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func faultFired(t *testing.T, admin, op string) int {
	t.Helper()
	var v struct{ Fired int }
	if err := getJSON("http://"+admin+"/fault/rule/"+op, &v); err != nil {
		t.Fatal(err)
	}
	return v.Fired
}

// LogicFired returns how often Logic's rule for op has matched.
func (s *Stack) LogicFired(op string) int { return faultFired(s.t, s.Logic.Admin, op) }

// Fired returns how often gateway i's rule for op has matched.
func (s *Stack) Fired(i int, op string) int { return faultFired(s.t, s.GW[i].Admin, op) }

// gwMetric reads one of gateway i's series.
func (s *Stack) gwMetric(i int, series string) float64 { return s.Metric(s.GW[i].Admin, series) }

// logicMetric reads one of Logic's series.
func (s *Stack) logicMetric(series string) float64 { return s.Metric(s.Logic.Admin, series) }
