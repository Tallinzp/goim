package racetest

import (
	"bufio"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// proc is one running binary. Its stdout and stderr go to <dir>/<name>.log.
type proc struct {
	name    string
	cmd     *exec.Cmd
	log     string
	logFrom int64         // the log's size at start: a restart appends to its predecessor's log
	done    chan struct{} // closed when the process has exited
	stopped atomic.Bool   // the scenario killed or terminated it
	crashed atomic.Bool   // it had exited on its own when the scenario stopped it
}

// startProc starts bin (in binDir unless the path is absolute) with args. t's cleanup fails the scenario if
// the process exited on its own (a crash the assertions may not see), then
// kills it and fails the scenario if its race detector reported a data race
// (the kill means the race exit code reaches no one; the report is in the
// log). The process also dies with the test binary (Pdeathsig), so a go
// test timeout leaves no gateway holding its ports. Pdeathsig fires when the
// OS thread that started the process exits (Go issue #27505), not the test
// binary: nothing here may call runtime.LockOSThread, or a process could be
// killed under a live scenario.
func startProc(t *testing.T, dir, name, bin string, args ...string) *proc {
	t.Helper()
	path := filepath.Join(dir, name+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(bin) {
		bin = filepath.Join(binDir, bin)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = f, f
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	var from int64
	if fi, err := f.Stat(); err == nil {
		from = fi.Size()
	}
	if err := cmd.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p := &proc{name: name, cmd: cmd, log: path, logFrom: from, done: make(chan struct{})}
	go func() {
		cmd.Wait()
		f.Close()
		close(p.done)
	}()
	t.Cleanup(func() {
		if p.exited() && !p.stopped.Load() || p.crashed.Load() {
			t.Errorf("%s exited on its own (%v):\n%s", name, p.cmd.ProcessState, strings.Join(p.tail(30), "\n"))
		}
		p.kill()
		p.checkRace(t)
		p.checkPanic(t)
	})
	return p
}

// checkRace fails t if this run of the process logged a data race.
func (p *proc) checkRace(t *testing.T) {
	b, _ := os.ReadFile(p.log)
	b = b[min(p.logFrom, int64(len(b))):]
	if i := strings.Index(string(b), "WARNING: DATA RACE"); i >= 0 {
		t.Errorf("%s: the race detector reported a data race:\n%s", p.name, string(b[i:min(len(b), i+4000)]))
	}
}

// checkPanic fails t if this run of the process panicked or hit a fatal
// error. It catches a crash after the scenario's own term (a panic in the
// drain), which the exit check above ignores: the scenario stopped it.
func (p *proc) checkPanic(t *testing.T) {
	b, _ := os.ReadFile(p.log)
	b = b[min(p.logFrom, int64(len(b))):]
	if loc := panicLine.FindIndex(b); loc != nil {
		t.Errorf("%s panicked:\n%s", p.name, string(b[loc[0]:min(len(b), loc[0]+4000)]))
	}
}

// panicLine is how the Go runtime starts a crash report.
var panicLine = regexp.MustCompile(`(?m)^(panic: |fatal error: )`)

func (p *proc) signal(s syscall.Signal) { _ = p.cmd.Process.Signal(s) }

// kill sends SIGKILL (a crash) and waits for the exit.
func (p *proc) kill() {
	p.stop()
	p.signal(syscall.SIGKILL) // also ends a stopped process
	<-p.done
}

// pause and resume freeze the process (SIGSTOP/SIGCONT): a zombie gateway,
// a Redis outage, or a replica that stops acknowledging writes.
func (p *proc) pause()  { p.signal(syscall.SIGSTOP) }
func (p *proc) resume() { p.signal(syscall.SIGCONT) }

// term sends SIGTERM and waits up to d for the exit.
func (p *proc) term(d time.Duration) bool {
	p.stop()
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// stop marks the process stopped by the scenario. One that had already
// exited on its own is still reported: a crash in the window under test
// must not hide behind the scenario's own kill.
func (p *proc) stop() {
	if p.exited() && !p.stopped.Load() {
		p.crashed.Store(true)
	}
	p.stopped.Store(true)
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// tail returns the last n lines of the log.
func (p *proc) tail(n int) []string {
	f, err := os.Open(p.log)
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return lines
}

// Ports come from a counter below Linux's ephemeral range (32768 and up),
// so a port handed to one process is never handed out again in the run,
// and no outgoing socket (a client, a gRPC dial) can take it before the
// process binds it. A port something else holds is skipped, and so is one
// with connections in TIME_WAIT from an earlier run: the probe binds
// without SO_REUSEADDR, as gnet's listener does.
const (
	firstPort = 20000
	lastPort  = 32000
)

var nextPort atomic.Int32

var strictListen = net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
	var err error
	c.Control(func(fd uintptr) { err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0) })
	return err
}}

// freePort returns a localhost address for a process to listen on.
func freePort(t *testing.T) string {
	t.Helper()
	for range lastPort - firstPort {
		p := firstPort + int(nextPort.Add(1))%(lastPort-firstPort)
		addr := "127.0.0.1:" + strconv.Itoa(p)
		if l, err := strictListen.Listen(context.Background(), "tcp", addr); err == nil {
			l.Close()
			return addr
		}
	}
	t.Fatal("no free port")
	return ""
}
