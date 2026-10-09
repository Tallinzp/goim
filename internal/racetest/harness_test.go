package racetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
)

// The harness fails a scenario for problems its assertions can't see
// (Review Focus 1–3). Each case runs TestHelperScenario, a deliberately
// broken scenario, in a child go test process and expects it to fail with
// the reason.
func TestHarnessFailsWhatAssertionsMiss(t *testing.T) {
	suite(t)
	for _, c := range []struct{ mode, want string }{
		{mode: "unexpected-exit", want: "exited on its own"},
		{mode: "unfired-fault", want: "never fired"},
		{mode: "race-report", want: "the race detector reported a data race"},
		{mode: "crash-then-kill", want: "exited on its own"},
		{mode: "unfired-fault-after-restart", want: "never fired"},
		{mode: "unfired-fault-replaced", want: "never fired"},
		{mode: "race-report-bare", want: "the race detector reported a data race"},
	} {
		cmd := childTest("TestHelperScenario", "-test.v")
		cmd.Env = append(os.Environ(), "GOIM_RACETEST_HELPER="+c.mode, "GOIM_RACE_BIN="+binDir, "GOIM_RACE_LOGS=",
			"GOIM_REDIS_SERVER="+redisServer)
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), c.want) {
			t.Errorf("%s: the helper scenario %s; want it to fail with %q:\n%s", c.mode,
				map[bool]string{true: "passed", false: "failed otherwise"}[err == nil], c.want, out)
		}
	}
}

// childTest runs test name of this test binary in a child process. Like
// every process the suite starts, it dies with its parent (Pdeathsig), and a
// timeout ends it if it hangs, taking its own stack down with it.
func childTest(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^" + name + "$", "-test.count=1", "-test.timeout=3m"}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	return cmd
}

// TestHelperScenario is the broken scenario TestHarnessFailsWhatAssertionsMiss
// runs in a child process. Run on its own, it does nothing.
func TestHelperScenario(t *testing.T) {
	mode := os.Getenv("GOIM_RACETEST_HELPER")
	if mode == "" {
		return
	}
	if mode == "race-report-bare" { // a process started outside a Stack, as the profile test does
		sleep, _ := exec.LookPath("sleep")
		p := startProc(t, t.TempDir(), "sleep", sleep, "60")
		f, _ := os.OpenFile(p.log, os.O_APPEND|os.O_WRONLY, 0)
		f.WriteString("==================\nWARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n")
		f.Close()
		return
	}
	s := New(t, Options{Gateways: 1})
	switch mode {
	case "unexpected-exit": // a gateway dies, and nothing the scenario checks notices
		s.GW[0].signal(syscall.SIGKILL)
		<-s.GW[0].done
	case "unfired-fault": // a typo in the key: the scenario injects nothing
		s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: "nobody", Fail: true})
		s.Online(0, "somebody", "d1", 1)
	case "race-report": // as -race prints it, then keeps running
		f, _ := os.OpenFile(s.Logic.log, os.O_APPEND|os.O_WRONLY, 0)
		f.WriteString("==================\nWARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n")
		f.Close()
	case "crash-then-kill": // a gateway dies, then the scenario "crashes" it as planned
		s.GW[0].signal(syscall.SIGKILL)
		<-s.GW[0].done
		s.GW[0].kill()
	case "unfired-fault-after-restart": // a typo in a rule set on a gateway started after the first rule
		s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: "somebody", Delay: fault.Duration(time.Millisecond)})
		s.Online(0, "somebody", "d1", 1)
		s.GW[0].kill()
		s.RestartGateway(0)
		s.Fault(0, fault.Rule{Op: gateway.FaultKick, Key: "nobody", Fail: true})
	case "unfired-fault-replaced": // a typo in a rule the scenario then replaces with one that fires
		s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: "nobody", Fail: true})
		s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: "somebody", Delay: fault.Duration(time.Millisecond)})
		s.Online(0, "somebody", "d1", 1)
	}
}

// The two Redis faults the scenarios use do what they claim (Review Focus
// 4). The replica is linked when the stack starts, so WAIT gets its ack;
// while the replica is stopped WAIT runs out its timeout with 0, and after
// it resumes it acknowledges again. A paused primary answers nothing until
// it resumes, and a key whose TTL ran out meanwhile is gone after.
func TestRedisFaultsDoWhatTheyClaim(t *testing.T) {
	suite(t)
	r := startRedis(t, t.TempDir())
	conn := r.Client().Conn() // WAIT counts the writes made on its own connection
	defer conn.Close()
	acked := func(timeout time.Duration) int64 {
		t.Helper()
		if err := conn.Incr(ctx, "w").Err(); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Wait(ctx, 1, timeout).Result()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := acked(time.Second); n != 1 {
		t.Fatalf("WAIT with the replica up = %d, want 1", n)
	}
	r.ReplicaDownFor(time.Second)
	start := time.Now()
	if n := acked(300 * time.Millisecond); n != 0 {
		t.Fatalf("WAIT with the replica stopped = %d, want 0", n)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("WAIT returned after %v, before its 300ms timeout", d)
	}
	time.Sleep(time.Second)
	if n := acked(2 * time.Second); n != 1 {
		t.Fatalf("WAIT after the replica resumed = %d, want 1", n)
	}

	if err := conn.Set(ctx, "lease", "1", 300*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	r.PauseFor(time.Second)
	start = time.Now()
	if err := r.Client().Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 800*time.Millisecond {
		t.Fatalf("the paused primary answered after %v", d)
	}
	if r.Client().Exists(ctx, "lease").Val() != 0 {
		t.Fatal("a key whose 300ms TTL ran out during the pause still exists")
	}
}

// A process startProc started dies with the test binary (Pdeathsig): a go
// test timeout or a panic, which skip the cleanups, leaves no gateway
// holding its ports (Review Focus 5). It needs no binaries: it runs at
// Tier 1a too.
func TestProcessesDieWithTheSuite(t *testing.T) {
	if os.Getenv("GOIM_RACETEST_SPAWN") != "" { // the child: start a process, then die without cleanups
		sleep, err := exec.LookPath("sleep")
		if err != nil {
			t.Skip("no sleep binary")
		}
		binDir = filepath.Dir(sleep)
		p := startProc(t, os.Getenv("GOIM_RACETEST_SPAWN"), "sleep", "sleep", "60")
		os.Stdout.WriteString("pid " + strconv.Itoa(p.cmd.Process.Pid) + "\n")
		os.Exit(0)
	}
	leftovers := func() []string {
		a, _ := filepath.Glob(filepath.Join(os.TempDir(), "goim-race-bin-*"))
		b, _ := filepath.Glob(filepath.Join(os.TempDir(), "TestProcessesDieWithTheSuite*"))
		return append(a, b...)
	}
	spawnDir := t.TempDir()
	before := leftovers()
	cmd := childTest("TestProcessesDieWithTheSuite")
	// The child exits without cleanups: it gets a directory this test removes,
	// and no suite switch, so its TestMain builds no binaries it would leak.
	cmd.Env = append(os.Environ(), "GOIM_RACETEST_SPAWN="+spawnDir, envSuite+"=")
	out, err := cmd.Output()
	if after := leftovers(); len(after) > len(before) {
		t.Errorf("the child left files behind in %s: %d before, %d after", os.TempDir(), len(before), len(after))
	}
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	var pid int
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "pid "); ok {
			pid, _ = strconv.Atoi(v)
		}
	}
	if pid == 0 {
		t.Fatalf("no pid in %q", out)
	}
	eventually(t, "the grandchild died with its parent", 5*time.Second, func() bool {
		return syscall.Kill(pid, 0) != nil
	})
}
