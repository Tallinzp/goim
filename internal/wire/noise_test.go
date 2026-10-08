package wire

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
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
