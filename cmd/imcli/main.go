// Command imcli is a minimal client for a goim gateway: dial, Noise NK
// handshake, Login with an access token, then print every frame the server
// sends and keep the session alive with Pings. It has no reconnect policy:
// it is a probe, not an app.
//
// After `make deploy`, with the default gateway (127.0.0.1:18100), key id 1
// and pinned key in $GOIM_HOME/keys/noise-public:
//
//	go run ./cmd/imcli -token "$(make -s deploy-token USER_ID=alice)"
//
// On another machine, pass the address and key that `make deploy-status`
// prints: -addr 192.168.0.109:18100 -key <base64 public key>.
//
// Commands: ping, logout, quit. Ctrl-C logs out.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
)

const (
	// The gateway closes a silent ONLINE connection after 2 × heartbeat + 10s;
	// Pinging at the LoginResp heartbeat is plenty.
	logoutTimeout = 2 * time.Second // Logout → LogoutResp, as the SDK's
	writeTimeout  = 5 * time.Second
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "imcli:", err)
		os.Exit(1)
	}
}

// cli is one logged-in session. Only the run loop touches its fields.
type cli struct {
	c        *clientconn.Conn
	out      io.Writer
	lines    <-chan string
	stop     <-chan struct{}
	duration <-chan time.Time
	frames   <-chan readResult
	done     chan struct{}

	reqID  atomic.Uint32
	sent   map[uint32]time.Time
	ticker *time.Ticker
	pings  <-chan time.Time

	loggingOut  bool
	logoutTimer *time.Timer
	logoutC     <-chan time.Time
}

type readResult struct {
	f   clientconn.Frame
	err error
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("imcli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: imcli [flags]")
		fmt.Fprintln(fs.Output(), "\nA minimal goim gateway client. It logs in, answers heartbeats and")
		fmt.Fprintln(fs.Output(), "prints every server frame. Commands: ping, logout, quit.")
		fmt.Fprintln(fs.Output(), "\nFlags:")
		fs.PrintDefaults()
	}
	var (
		addr      = fs.String("addr", envOr("GOIM_ADDR", "127.0.0.1:18100"), "gateway address (or GOIM_ADDR)")
		keyID     = fs.Uint("key-id", envUint("GOIM_KEY_ID", 1), "pinned Noise key id (or GOIM_KEY_ID)")
		keyText   = fs.String("key", os.Getenv("GOIM_NOISE_PUBLIC"), "pinned Noise public key, base64 (default: $GOIM_HOME/keys/noise-public)")
		token     = fs.String("token", os.Getenv("GOIM_TOKEN"), "access token from `make deploy-token USER_ID=...` (or GOIM_TOKEN)")
		appid     = fs.String("appid", envOr("GOIM_APPID", "im"), "Login appid (or GOIM_APPID)")
		platform  = fs.String("platform", envOr("GOIM_PLATFORM", "mobile"), "device class: mobile, pad, web or pc (or GOIM_PLATFORM)")
		deviceID  = fs.String("device-id", "", "device_id (default: random 32 hex chars); reuse one to test a kick")
		timeout   = fs.Duration("timeout", 10*time.Second, "connect and Login deadline")
		pingEvery = fs.Duration("ping-every", 0, "Ping interval (0: the LoginResp heartbeat; <0: no automatic Ping)")
		duration  = fs.Duration("duration", 0, "log out and exit after this long (0: until Ctrl-C or quit)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if *pingEvery > 0 && *pingEvery < 10*time.Millisecond {
		return errors.New("-ping-every is too small (minimum 10ms)")
	}
	if *keyID > 65535 {
		return errors.New("-key-id must be in 0–65535")
	}
	if strings.TrimSpace(*token) == "" {
		return errors.New("-token or GOIM_TOKEN is required")
	}
	plat, err := parsePlatform(*platform)
	if err != nil {
		return err
	}
	key, err := loadServerKey(*keyText)
	if err != nil {
		return err
	}
	dev := strings.TrimSpace(*deviceID)
	if dev == "" {
		dev = newDeviceID()
	}

	logf := func(format string, args ...any) {
		line := append([]any{stamp()}, args...)
		fmt.Fprintf(stdout, "%s "+format+"\n", line...)
	}

	req := &clientpb.LoginReq{
		Token:           strings.TrimSpace(*token),
		Appid:           *appid,
		Platform:        plat,
		DeviceId:        dev,
		ConnSeq:         1,
		ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT,
	}
	deadline := time.Now().Add(*timeout)

	logf("connecting %s (key id %d, device %s)", *addr, *keyID, dev)
	c, err := clientconn.Dial(*addr, time.Until(deadline))
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer c.Close()

	if err := c.Handshake(uint16(*keyID), key, deadline); err != nil {
		return fmt.Errorf("noise handshake: %w", err)
	}
	logf("handshake ok")
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, req); err != nil {
		return fmt.Errorf("send login: %w", err)
	}
	resp, err := waitLogin(c, deadline)
	if err != nil {
		return err
	}
	logf("login ok: %s", loginSummary(resp))

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lines := make(chan string)
	go readLines(stdin, lines, stopCtx.Done())

	var durationC <-chan time.Time
	if *duration > 0 {
		t := time.NewTimer(*duration)
		defer t.Stop()
		durationC = t.C
	}

	u := &cli{
		c:        c,
		out:      stdout,
		lines:    lines,
		stop:     stopCtx.Done(),
		duration: durationC,
		done:     make(chan struct{}),
		sent:     make(map[uint32]time.Time),
	}
	u.reqID.Store(1)
	u.frames = u.readLoop()

	if interval := pingInterval(resp, *pingEvery); interval > 0 {
		u.ticker = time.NewTicker(interval)
		u.pings = u.ticker.C
		logf("heartbeat every %s", interval)
	} else {
		logf("heartbeat disabled")
	}
	logf("commands: ping, logout, quit (Ctrl-C logs out)")

	return u.loop()
}

// waitLogin reads until Login is answered. The gateway may answer with the
// LoginResp, a rejected Login, Busy, or a Kick left over from an old session.
func waitLogin(c *clientconn.Conn, deadline time.Time) (*clientpb.LoginResp, error) {
	for {
		f, err := c.Recv(deadline)
		if err != nil {
			return nil, fmt.Errorf("wait for login reply: %w", err)
		}
		switch f.Cmd {
		case clientpb.Cmd_CMD_LOGIN_RESP:
			var resp clientpb.LoginResp
			if err := f.Decode(&resp); err != nil {
				return nil, fmt.Errorf("decode login resp: %w", err)
			}
			return &resp, nil
		case clientpb.Cmd_CMD_ERROR:
			var m clientpb.Error
			if err := f.Decode(&m); err != nil {
				return nil, fmt.Errorf("decode login error: %w", err)
			}
			if m.GetStoredConnSeq() != 0 {
				return nil, fmt.Errorf("login rejected: %s (stored conn_seq %d)", m.GetCode(), m.GetStoredConnSeq())
			}
			return nil, fmt.Errorf("login rejected: %s", m.GetCode())
		case clientpb.Cmd_CMD_BUSY:
			var m clientpb.Busy
			if err := f.Decode(&m); err != nil {
				return nil, fmt.Errorf("decode busy: %w", err)
			}
			return nil, fmt.Errorf("gateway busy: retry after %dms", m.GetRetryAfterMs())
		case clientpb.Cmd_CMD_KICK:
			var m clientpb.Kick
			if err := f.Decode(&m); err != nil {
				return nil, fmt.Errorf("decode kick: %w", err)
			}
			return nil, fmt.Errorf("kicked during login: %s", m.GetReason())
		default:
			return nil, fmt.Errorf("unexpected %s before login reply", cmdName(f.Cmd))
		}
	}
}

func pingInterval(resp *clientpb.LoginResp, every time.Duration) time.Duration {
	if every != 0 {
		return every
	}
	if resp.GetHeartbeatS() == 0 {
		return 0
	}
	return time.Duration(resp.GetHeartbeatS()) * time.Second
}

func (u *cli) readLoop() <-chan readResult {
	frames := make(chan readResult, 16)
	go func() {
		for {
			f, err := u.c.Recv(time.Time{})
			select {
			case frames <- readResult{f: f, err: err}:
			case <-u.done:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return frames
}

func (u *cli) loop() error {
	defer close(u.done)
	defer u.stopTimers()
	for {
		select {
		case <-u.stop:
			u.logf("signal: logging out")
			u.stop = nil
			u.startLogout()
		case <-u.duration:
			u.logf("duration reached: logging out")
			u.duration = nil
			u.startLogout()
		case line, ok := <-u.lines:
			if !ok {
				u.lines = nil
				continue
			}
			switch strings.ToLower(strings.TrimSpace(line)) {
			case "":
			case "ping":
				if err := u.sendPing(); err != nil {
					return fmt.Errorf("ping: %w", err)
				}
			case "logout":
				u.startLogout()
			case "quit", "exit":
				u.logf("closing")
				return nil
			case "help", "?":
				u.logf("commands: ping, logout, quit")
			default:
				u.logf("unknown command %q (try: ping, logout, quit)", strings.TrimSpace(line))
			}
		case <-u.pings:
			if u.loggingOut {
				continue
			}
			if err := u.sendPing(); err != nil {
				return fmt.Errorf("heartbeat ping: %w", err)
			}
		case <-u.logoutC:
			return errors.New("logout: no LogoutResp within " + logoutTimeout.String())
		case r := <-u.frames:
			if r.err != nil {
				if u.loggingOut && clientconn.IsClosed(r.err) {
					return errors.New("connection closed before LogoutResp")
				}
				return fmt.Errorf("connection: %w", r.err)
			}
			done, err := u.frame(r.f)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

func (u *cli) frame(f clientconn.Frame) (done bool, err error) {
	switch f.Cmd {
	case clientpb.Cmd_CMD_PONG:
		at, ok := u.sent[f.ReqID]
		if !ok {
			u.logf("pong req=%d", f.ReqID)
			break
		}
		delete(u.sent, f.ReqID)
		u.logf("pong req=%d rtt=%s", f.ReqID, time.Since(at).Round(time.Microsecond))
	case clientpb.Cmd_CMD_KICK:
		var m clientpb.Kick
		if err := f.Decode(&m); err != nil {
			return true, fmt.Errorf("decode kick: %w", err)
		}
		u.logf("kicked: %s", m.GetReason())
		return true, nil
	case clientpb.Cmd_CMD_RECONNECT:
		var m clientpb.Reconnect
		if err := f.Decode(&m); err != nil {
			return true, fmt.Errorf("decode reconnect: %w", err)
		}
		u.logf("server asks to reconnect: jitter=%dms", m.GetJitterMs())
		return true, nil
	case clientpb.Cmd_CMD_ERROR:
		var m clientpb.Error
		if err := f.Decode(&m); err != nil {
			return true, fmt.Errorf("decode error: %w", err)
		}
		u.logf("server error: %s", m.GetCode())
		return true, nil
	case clientpb.Cmd_CMD_BUSY:
		var m clientpb.Busy
		if err := f.Decode(&m); err != nil {
			return true, fmt.Errorf("decode busy: %w", err)
		}
		u.logf("server busy: retry after %dms", m.GetRetryAfterMs())
		return true, nil
	case clientpb.Cmd_CMD_LOGOUT_RESP:
		if u.loggingOut {
			u.logf("logout ok")
			return true, nil
		}
		u.logf("unexpected LogoutResp")
	case clientpb.Cmd_CMD_LOGIN_RESP:
		u.logf("unexpected second LoginResp")
	default:
		u.logf("frame %s req=%d body=%dB", cmdName(f.Cmd), f.ReqID, len(f.Body))
	}
	return false, nil
}

func (u *cli) sendPing() error {
	id := u.reqID.Add(1)
	u.sent[id] = time.Now()
	if err := u.c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		delete(u.sent, id)
		return err
	}
	if err := u.c.Send(clientpb.Cmd_CMD_PING, id, nil); err != nil {
		delete(u.sent, id)
		return err
	}
	return nil
}

func (u *cli) startLogout() {
	if u.loggingOut {
		return
	}
	u.loggingOut = true
	if u.ticker != nil {
		u.ticker.Stop()
		u.pings = nil
	}
	id := u.reqID.Add(1)
	u.logf("logout")
	if err := u.c.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		u.logf("logout send: %v", err)
	} else if err := u.c.Send(clientpb.Cmd_CMD_LOGOUT, id, nil); err != nil {
		u.logf("logout send: %v", err)
	}
	u.logoutTimer = time.NewTimer(logoutTimeout)
	u.logoutC = u.logoutTimer.C
}

func (u *cli) stopTimers() {
	if u.ticker != nil {
		u.ticker.Stop()
	}
	if u.logoutTimer != nil {
		u.logoutTimer.Stop()
	}
}

func (u *cli) logf(format string, args ...any) {
	line := append([]any{stamp()}, args...)
	fmt.Fprintf(u.out, "%s "+format+"\n", line...)
}

func readLines(r io.Reader, out chan<- string, stop <-chan struct{}) {
	defer close(out)
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		select {
		case out <- sc.Text():
		case <-stop:
			return
		}
	}
}

func loadServerKey(text string) ([]byte, error) {
	s := strings.TrimSpace(text)
	if s == "" {
		home := os.Getenv("GOIM_HOME")
		if home == "" {
			h, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("find home directory: %w", err)
			}
			home = filepath.Join(h, ".goim")
		}
		b, err := os.ReadFile(filepath.Join(home, "keys", "noise-public"))
		if err != nil {
			return nil, fmt.Errorf("read Noise public key: %w (pass -key)", err)
		}
		s = strings.TrimSpace(string(b))
	}
	pub, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode Noise public key: %w", err)
	}
	if len(pub) != 32 {
		return nil, fmt.Errorf("Noise public key is %d bytes, want 32", len(pub))
	}
	return pub, nil
}

func parsePlatform(s string) (clientpb.Platform, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "mobile":
		return clientpb.Platform_PLATFORM_MOBILE, nil
	case "pad":
		return clientpb.Platform_PLATFORM_PAD, nil
	case "web":
		return clientpb.Platform_PLATFORM_WEB, nil
	case "pc":
		return clientpb.Platform_PLATFORM_PC, nil
	default:
		return clientpb.Platform_PLATFORM_UNSPECIFIED,
			fmt.Errorf("unknown platform %q (mobile, pad, web, pc)", s)
	}
}

func loginSummary(resp *clientpb.LoginResp) string {
	s := fmt.Sprintf("heartbeat=%s", seconds(resp.GetHeartbeatS()))
	if w := resp.GetStormWait(); w != nil {
		s += " storm=" + window(w)
	}
	if w := resp.GetLocalTrigger(); w != nil {
		s += " local=" + window(w)
	}
	if w := resp.GetPreHandshakeRetry(); w != nil {
		s += " pre-handshake=" + window(w)
	}
	if w := resp.GetRetryable(); w != nil {
		s += " retryable=" + window(w)
	}
	return s
}

func window(w *clientpb.Window) string {
	return fmt.Sprintf("%d-%dms", w.GetLoMs(), w.GetHiMs())
}

func seconds(n uint32) string {
	if n == 0 {
		return "none"
	}
	return (time.Duration(n) * time.Second).String()
}

func cmdName(c clientpb.Cmd) string { return strings.TrimPrefix(c.String(), "CMD_") }

func newDeviceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("imcli-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func stamp() string { return time.Now().Format("15:04:05.000") }

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envUint(name string, def uint) uint {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint(n)
		}
	}
	return def
}
