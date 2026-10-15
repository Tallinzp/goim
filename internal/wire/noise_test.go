package wire

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

const cmdHandshake = 1

func key(t *testing.T) Key {
	t.Helper()
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// handshake runs both sides in memory; the server picks the key by key_id.
func handshake(t *testing.T, ks KeySet, keyID uint16, pinned []byte, tamper func([]byte)) (cli, srv *Session, err error) {
	t.Helper()
	in, f1, err := Initiate(keyID, pinned, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if tamper != nil {
		tamper(f1)
	}
	k, ok := ks[KeyID(f1)]
	if !ok {
		t.Fatalf("key_id %d not in the set", KeyID(f1))
	}
	srv, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		return nil, nil, err
	}
	cli, err = in.Finish(f2)
	return cli, srv, err
}

func TestHandshakeFrameSizes(t *testing.T) {
	k := key(t)
	in, f1, err := Initiate(7, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if len(f1) != HeaderLen+HandshakeLen || ParseHeader(f1) != (Header{Version: Version, Cmd: cmdHandshake, Length: HandshakeLen}) || KeyID(f1) != 7 {
		t.Fatalf("client frame % x", f1)
	}
	_, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if len(f2) != HeaderLen+HandshakeMsgLen || ParseHeader(f2).Length != HandshakeMsgLen {
		t.Fatalf("server frame % x", f2)
	}
	if _, err := in.Finish(f2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Respond(k, f1[:len(f1)-1], cmdHandshake); !errors.Is(err, ErrHandshake) {
		t.Fatalf("short frame: %v", err)
	}
}

func TestHandshakeWithEitherOfTwoKeyIDs(t *testing.T) {
	ks := KeySet{1: key(t), 2: key(t)}
	for id, k := range ks {
		cli, srv, err := handshake(t, ks, id, k.Public, nil)
		if err != nil {
			t.Fatalf("key_id %d: %v", id, err)
		}
		f, err := cli.Seal(nil, 2, 9, []byte("login"))
		if err != nil {
			t.Fatal(err)
		}
		cmd, reqID, body, _, err := srv.Open(nil, f)
		if err != nil || cmd != 2 || reqID != 9 || string(body) != "login" {
			t.Fatalf("key_id %d: %d %d %q %v", id, cmd, reqID, body, err)
		}
	}
}

// The prologue authenticates key_id: with one key filed under two IDs, a
// key_id changed in flight still selects the right key, so only the
// prologue can make the handshake fail.
func TestTamperedKeyIDFailsThroughThePrologue(t *testing.T) {
	k := key(t)
	ks := KeySet{1: k, 2: k}
	if _, _, err := handshake(t, ks, 1, k.Public, nil); err != nil {
		t.Fatalf("untampered: %v", err)
	}
	_, _, err := handshake(t, ks, 1, k.Public, func(f []byte) { f[HeaderLen+1] = 2 })
	if !errors.Is(err, ErrHandshake) {
		t.Fatalf("tampered key_id: %v", err)
	}
}

func TestWrongPinnedKeyFails(t *testing.T) {
	ks := KeySet{1: key(t)}
	if _, _, err := handshake(t, ks, 1, key(t).Public, nil); !errors.Is(err, ErrHandshake) {
		t.Fatalf("client pinned another key: %v", err)
	}
}

func TestTamperedHeaderFailsTheAEAD(t *testing.T) {
	k := key(t)
	for i := range HeaderLen {
		cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
		if err != nil {
			t.Fatal(err)
		}
		f, _ := cli.Seal(nil, 4, 1, []byte("ping"))
		bad := bytes.Clone(f)
		if i == 4 || i == 5 { // Length: a frame of another size; Open checks it first
			bad[i] ^= 0x01
		} else {
			bad[i] ^= 0x80
		}
		if _, _, _, _, err := srv.Open(nil, bad); !errors.Is(err, ErrFrame) {
			t.Fatalf("header byte %d tampered: %v", i, err)
		}
		// The same frame untampered opens, so the header alone failed it.
		if _, _, _, _, err := srv.Open(nil, f); err != nil {
			t.Fatalf("header byte %d: the untampered frame fails too: %v", i, err)
		}
	}
}

func TestEachDirectionHasItsOwnNonceCounter(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	f1, _ := cli.Seal(nil, 4, 1, nil)
	f2, _ := cli.Seal(nil, 4, 2, nil)
	if _, _, _, _, err := srv.Open(nil, f2); err == nil {
		t.Fatal("frame 2 opened before frame 1")
	}
	// A failed Open leaves the counter where it was.
	if _, id, _, _, err := srv.Open(nil, f1); err != nil || id != 1 {
		t.Fatal(id, err)
	}
	if _, _, _, _, err := srv.Open(nil, f1); err == nil {
		t.Fatal("replayed frame opened")
	}
	r, _ := srv.Seal(nil, 5, 1, nil) // the server's first frame: nonce 0 the other way
	if _, id, _, _, err := cli.Open(nil, r); err != nil || id != 1 {
		t.Fatal(id, err)
	}
}

func TestLargestBodyAndOversizedBody(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte{0xab}, MaxBody)
	f, err := cli.Seal(make([]byte, 3, 8), 2, 1, body) // appends after existing bytes
	if err != nil || len(f) != 3+HeaderLen+MaxLength || ParseHeader(f[3:]).Length != MaxLength {
		t.Fatalf("len %d, err %v", len(f), err)
	}
	if _, _, got, _, err := srv.Open(nil, f[3:]); err != nil || !bytes.Equal(got, body) {
		t.Fatal(err)
	}
	if _, err := cli.Seal(nil, 2, 1, make([]byte, MaxBody+1)); !errors.Is(err, ErrFrame) {
		t.Fatalf("oversized body: %v", err)
	}
}

func TestClientHelpersOverAPipe(t *testing.T) {
	k := key(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan error, 1)
	go func() {
		f := make([]byte, HeaderLen+HandshakeLen)
		if _, err := io.ReadFull(b, f); err != nil {
			done <- err
			return
		}
		srv, reply, err := Respond(k, f, cmdHandshake)
		if err != nil {
			done <- err
			return
		}
		b.Write(reply)
		cmd, id, body, err := srv.ReadFrame(b)
		if err == nil {
			err = srv.WriteFrame(b, cmd+1, id, body)
		}
		done <- err
	}()
	cli, err := ClientHandshake(a, 3, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteFrame(a, 4, 77, []byte("x")); err != nil {
		t.Fatal(err)
	}
	cmd, id, body, err := cli.ReadFrame(a)
	if err != nil || cmd != 5 || id != 77 || string(body) != "x" {
		t.Fatal(cmd, id, body, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLoadKeyDir(t *testing.T) {
	dir := t.TempDir()
	k1, k7 := key(t), key(t)
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p1, pub1 := EncodeKey(k1)
	p7, _ := EncodeKey(k7)
	write("1", p1+"\n")
	write("7", p7)
	write("README", "not a key")
	os.Mkdir(filepath.Join(dir, "..data"), 0o700) // a Secret volume's own entries
	ks, err := LoadKeyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 2 || !bytes.Equal(ks[1].Public, k1.Public) || !bytes.Equal(ks[7].Private, k7.Private) {
		t.Fatalf("loaded %v", ks)
	}
	if _, pub := EncodeKey(ks[1]); pub != pub1 {
		t.Fatal("public key not derived from the private key")
	}
	write("8", "!!!")
	if _, err := LoadKeyDir(dir); err == nil {
		t.Fatal("a bad key file loaded")
	}
	if _, err := LoadKeyDir(t.TempDir()); err == nil {
		t.Fatal("an empty key set loaded")
	}
}

// P4I-1: a server that closes before any reply byte (at_capacity,
// handshake_rate, bad_version, noise_unknown_key, draining) is reported as
// ErrClosedBeforeReply, which the SDK counts as a pre-handshake close; a
// close after part of the reply is not.
func TestClientHandshakeReportsAPreHandshakeClose(t *testing.T) {
	k := key(t)
	for name, reply := range map[string][]byte{"no reply": nil, "part of a reply": {1, 0}} {
		cli, srv := net.Pipe()
		go func() {
			io.ReadFull(srv, make([]byte, HeaderLen+HandshakeLen))
			srv.Write(reply)
			srv.Close()
		}()
		_, err := ClientHandshake(cli, 1, k.Public, cmdHandshake)
		if got, want := errors.Is(err, ErrClosedBeforeReply), reply == nil; got != want || err == nil {
			t.Errorf("%s: err %v, ErrClosedBeforeReply %v, want %v", name, err, got, want)
		}
		cli.Close()
	}
	// A reset (the transport's close of a rejected socket can be one).
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() {
		c, err := lis.Accept()
		if err != nil {
			return
		}
		io.ReadFull(c, make([]byte, HeaderLen+HandshakeLen))
		c.(*net.TCPConn).SetLinger(0)
		c.Close()
	}()
	c, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := ClientHandshake(c, 1, k.Public, cmdHandshake); !errors.Is(err, ErrClosedBeforeReply) {
		t.Fatalf("reset: err %v, want ErrClosedBeforeReply", err)
	}
}

// P4I-2: ReadFrame allocates for the frame it reads, not for the largest
// possible one (Plan 6's loadgen reads millions of frames through it).
func TestReadFrameAllocatesTheFrameNotTheMax(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	var stream bytes.Buffer
	for i := range n {
		if err := srv.WriteFrame(&stream, 7, uint32(i), []byte("pong")); err != nil {
			t.Fatal(err)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range n {
		if _, id, body, err := cli.ReadFrame(&stream); err != nil || id != uint32(i) || string(body) != "pong" {
			t.Fatalf("frame %d: id %d body %q err %v", i, id, body, err)
		}
	}
	runtime.ReadMemStats(&after)
	// 64KB per frame would be 13MB; small frames stay far below 1MB even
	// with the rest of the process allocating alongside.
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("reading %d small frames allocated %d bytes", n, got)
	}
}

// P4I-3: two key files naming the same key_id ("1" and "01") are an error,
// not a silent choice by directory order.
func TestLoadKeyDirRefusesADuplicateKeyID(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"1", "01"} {
		p, _ := EncodeKey(key(t))
		if err := os.WriteFile(filepath.Join(dir, name), []byte(p), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := LoadKeyDir(dir); err == nil {
		t.Fatal("files 1 and 01 both loaded as key_id 1")
	}
}

// P4I-4: the SDK reads on one goroutine and writes on another; the two
// directions share no state, so -race stays quiet.
func TestSessionSendAndReceiveOnTwoGoroutines(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	up, down := make(chan []byte, 100), make(chan []byte, 100)
	for i := range 100 {
		f, _ := srv.Seal(nil, 7, uint32(i), []byte("down"))
		down <- f
	}
	close(down)
	var wg sync.WaitGroup
	wg.Go(func() { // the SDK's writer
		for i := range 100 {
			f, err := cli.Seal(nil, 7, uint32(i), []byte("up"))
			if err != nil {
				t.Error(err)
				return
			}
			up <- f
		}
		close(up)
	})
	wg.Go(func() { // the SDK's reader
		for f := range down {
			if _, _, _, _, err := cli.Open(nil, f); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()
	for f := range up {
		if _, _, _, _, err := srv.Open(nil, f); err != nil {
			t.Fatal(err)
		}
	}
}

// #106: a first message made ahead of time completes a handshake later, and
// the server receives exactly its bytes.
func TestClientHandshakeWithAPrecomputedInitiator(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	in, frame, err := Initiate(7, key.Public, 1)
	if err != nil {
		t.Fatal(err)
	}
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()
	srv := make(chan *Session, 1)
	go func() {
		got := make([]byte, HeaderLen+HandshakeLen)
		if _, err := io.ReadFull(s, got); err != nil || !bytes.Equal(got, frame) {
			srv <- nil
			return
		}
		sess, reply, err := Respond(key, got, 1)
		if err != nil {
			srv <- nil
			return
		}
		if _, err := s.Write(reply); err != nil {
			srv <- nil
			return
		}
		srv <- sess
	}()
	cs, err := ClientHandshakeWith(c, in, frame)
	if err != nil {
		t.Fatal(err)
	}
	ss := <-srv
	if ss == nil {
		t.Fatal("the server did not receive the precomputed frame, or could not answer it")
	}
	sealed, err := cs.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := ss.Open(nil, sealed); err != nil || string(body) != "hi" {
		t.Fatalf("open: %q %v", body, err)
	}
}
