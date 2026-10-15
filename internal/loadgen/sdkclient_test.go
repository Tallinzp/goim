package loadgen

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
	"github.com/Tallinzp/goim/internal/wire"
)

// An SDK client's backoff and failure waits count as backoff; the quick
// retries before them do not (P6a2bI-2).
func TestSDKClientCountsBackoffWaits(t *testing.T) {
	r, err := New(Config{Mode: "sdk", Clients: 2, Rate: 1, UIDPrefix: "x"}, Env{GatewayAddrs: []string{"127.0.0.1:1"}, GatewayIDs: []string{"gw-0"}})
	if err != nil {
		t.Fatal(err)
	}
	sc := &sdkClient{r: r, i: 1}
	for _, k := range []sdk.WaitKind{sdk.WaitPreHandshake, sdk.WaitRetryable, sdk.WaitStorm, sdk.WaitBackoff, sdk.WaitFailure} {
		sc.event(sdk.Event{Kind: sdk.EventWaiting, Wait: sdk.Wait{Kind: k}, End: sdk.EndFailed})
	}
	c := r.Stats().Counts
	if c.BackoffClients != 1 || c.BackoffWaits["backoff"] != 1 || c.BackoffWaits["failure"] != 1 || len(c.BackoffWaits) != 2 {
		t.Fatalf("backoff: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
	}
}

// #107: an attempt with no wait before it (the first Connect) records no
// attempt lag; one after a wait records one, and only once.
func TestAttemptLagOnlyAfterAWait(t *testing.T) {
	r := &Runner{rec: NewRecorder(1, []string{"gw-0"})}
	sc := &sdkClient{r: r}
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	if n := r.rec.Counts().StormSinceMark.AttemptLag.N; n != 0 {
		t.Fatalf("first attempt: %d lags, want 0", n)
	}
	sc.event(sdk.Event{Kind: sdk.EventWaiting, Wait: sdk.Wait{Kind: sdk.WaitRetryable, D: time.Millisecond}})
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	if n := r.rec.Counts().StormSinceMark.AttemptLag.N; n != 1 {
		t.Fatalf("after one wait: %d lags, want 1", n)
	}
}

// handshakeClient is an sdkClient with a server key, for #109's tests.
func handshakeClient(t *testing.T) (*sdkClient, wire.Key) {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{env: Env{NoiseKeyID: 1, NoisePublic: key.Public}, rec: NewRecorder(1, []string{"gw-0"})}
	return &sdkClient{r: r}, key
}

// readFrame reads a client's handshake frame on the server's side of a pipe.
func readFrame(s net.Conn) <-chan []byte {
	got := make(chan []byte, 1)
	go func() {
		f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
		if _, err := io.ReadFull(s, f); err != nil {
			f = nil
		}
		got <- f
	}()
	return got
}

// #109: a handshake no reply answered is sent again by the next attempt,
// and completes there; once answered, the next attempt makes a new one.
func TestUnansweredHandshakeIsSentAgain(t *testing.T) {
	sc, key := handshakeClient(t)
	cmd := uint16(clientpb.Cmd_CMD_HANDSHAKE)

	// Attempt 1: the gateway reads the frame and closes (its handshake limit).
	c1, s1 := net.Pipe()
	in1, f1, err := sc.initiate(1, key.Public)
	if err != nil {
		t.Fatal(err)
	}
	got := readFrame(s1)
	go func() { <-got; s1.Close() }()
	rc1 := sc.track(c1)
	if _, err := wire.ClientHandshakeWith(rc1, in1, f1); err == nil {
		t.Fatal("attempt 1: a closed gateway completed the handshake")
	}
	rc1.Close() // as the SDK's login does when a handshake fails

	// Attempt 2: the same frame, answered this time.
	c2, s2 := net.Pipe()
	defer c2.Close()
	defer s2.Close()
	in2, f2, err := sc.initiate(1, key.Public)
	if err != nil {
		t.Fatal(err)
	}
	if in2 != in1 || !bytes.Equal(f2, f1) {
		t.Fatal("attempt 2: a new handshake, want attempt 1's")
	}
	srv := make(chan *wire.Session, 1)
	go func() {
		f := <-readFrame(s2)
		sess, reply, err := wire.Respond(key, f, cmd)
		if err != nil || !bytes.Equal(f, f1) {
			srv <- nil
			return
		}
		s2.Write(reply)
		srv <- sess
	}()
	cs, err := wire.ClientHandshakeWith(sc.track(c2), in2, f2)
	if err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	ss := <-srv
	if ss == nil {
		t.Fatal("attempt 2: the gateway did not get attempt 1's frame")
	}
	sealed, err := cs.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := ss.Open(nil, sealed); err != nil || string(body) != "hi" {
		t.Fatalf("open: %q %v", body, err)
	}

	// Attempt 3: answered, so a new one; the client holds no Initiator.
	if sc.held.Load() != nil {
		t.Fatal("an answered handshake is still held")
	}
	_, f3, err := sc.initiate(1, key.Public)
	if err != nil || bytes.Equal(f3, f1) {
		t.Fatalf("attempt 3: %v, same frame %v", err, bytes.Equal(f3, f1))
	}
	s := sc.r.rec.Counts().StormSinceMark
	if s.HandshakesInline != 2 || s.HandshakesReused != 1 || s.SendLag.N != 2 {
		t.Fatalf("storm %+v: want 2 inline, 1 reused, 2 send lags", s)
	}
}

// byteConn is a connection whose every Read returns one byte: a reply that
// is already there.
type byteConn struct{ net.Conn }

func (byteConn) Read(b []byte) (int, error)  { b[0] = 1; return 1, nil }
func (byteConn) Write(b []byte) (int, error) { return len(b), nil }
func (byteConn) Close() error                { return nil }

// #109: an attempt the SDK cancels can still be running when the next one
// starts. A handshake is sent again only once its sender's connection is
// closed, which then can neither take it nor read a reply for it; one
// answered is never sent again.
func TestAHeldHandshakeIsFinishedOnce(t *testing.T) {
	t.Run("not while its sender is open", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		if _, err := sc.track(byteConn{}).Write(f); err != nil {
			t.Fatal(err)
		}
		if _, f2, _ := sc.initiate(1, key.Public); bytes.Equal(f2, f) {
			t.Fatal("sent again while its sender may still read the reply")
		}
	})
	t.Run("its closed sender reads no reply", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc1 := sc.track(byteConn{})
		if _, err := rc1.Write(f); err != nil {
			t.Fatal(err)
		}
		rc1.Close()
		_, f2, _ := sc.initiate(1, key.Public)
		if !bytes.Equal(f2, f) {
			t.Fatal("not sent again after its sender closed")
		}
		if _, err := rc1.Read(make([]byte, 1)); !errors.Is(err, errHandshakeTaken) {
			t.Fatalf("the closed sender read a reply: %v", err)
		}
		rc2 := sc.track(byteConn{})
		if _, err := rc2.Write(f2); err != nil {
			t.Fatal(err)
		}
		if n, err := rc2.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the new sender: %d %v", n, err)
		}
	})
	// Review O3: A took the held handshake, was cancelled before writing
	// (its connection closed), and B took it too; A's late write must not
	// take it back from B, whose reply would then be refused.
	t.Run("a closed sender's late write", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc0 := sc.track(byteConn{})
		if _, err := rc0.Write(f); err != nil {
			t.Fatal(err)
		}
		rc0.Close() // refused: the handshake is reusable
		_, fa, _ := sc.initiate(1, key.Public)
		rcA := sc.track(byteConn{})
		rcA.Close() // cancelled before its write
		_, fb, _ := sc.initiate(1, key.Public)
		if !bytes.Equal(fa, f) || !bytes.Equal(fb, f) {
			t.Fatal("not sent again")
		}
		rcB := sc.track(byteConn{})
		if _, err := rcB.Write(fb); err != nil {
			t.Fatal(err)
		}
		if _, err := rcA.Write(fa); !errors.Is(err, errHandshakeTaken) {
			t.Fatalf("the cancelled attempt's late write: %v", err)
		}
		if n, err := rcB.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the live sender: %d %v", n, err)
		}
	})
	t.Run("answered before the resend", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc1 := sc.track(byteConn{})
		if _, err := rc1.Write(f); err != nil {
			t.Fatal(err)
		}
		if n, err := rc1.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the sender: %d %v", n, err)
		}
		rc1.Close()
		if _, f2, _ := sc.initiate(1, key.Public); bytes.Equal(f2, f) {
			t.Fatal("an answered handshake was sent again")
		}
	})
}
