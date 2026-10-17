package wire

import (
	"bytes"
	"crypto/ecdh"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/flynn/noise"
)

// dh25519 is flynn/noise's own suite, the one suite replaces (#130).
var dh25519 = noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)

func seeded(seed byte) *rand.ChaCha8 { return rand.NewChaCha8([32]byte{seed}) }

// countParses counts newPrivateKey's calls until the test ends.
func countParses(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	orig := newPrivateKey
	newPrivateKey = func(b []byte) (*ecdh.PrivateKey, error) {
		n.Add(1)
		return orig(b)
	}
	t.Cleanup(func() { newPrivateKey = orig })
	return &n
}

func TestSuiteNameIsDH25519s(t *testing.T) {
	if got, want := newSuite(Key{}).Name(), dh25519.Name(); !bytes.Equal(got, want) {
		t.Fatalf("name %q, want %q", got, want)
	}
}

// The same randomness gives the same bytes on the wire as DH25519, and each
// side completes a handshake with the other's old implementation.
func TestHandshakeBytesMatchDH25519(t *testing.T) {
	k := key(t)
	_, f1, err := initiate(newSuite(Key{}), 3, k.Public, cmdHandshake, seeded(1))
	if err != nil {
		t.Fatal(err)
	}
	refIn, ref1, err := initiate(dh25519, 3, k.Public, cmdHandshake, seeded(1))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f1, ref1) {
		t.Fatal("the client's handshake frame differs from DH25519's")
	}
	srv, f2, err := respond(newSuite(k), k, f1, cmdHandshake, seeded(2))
	if err != nil {
		t.Fatal(err)
	}
	refSrv, ref2, err := respond(dh25519, k, f1, cmdHandshake, seeded(2))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f2, ref2) {
		t.Fatal("the server's handshake frame differs from DH25519's")
	}
	cli, err := refIn.Finish(f2) // an old client finishes against the new server
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := cli.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*Session{"new server": srv, "old server": refSrv} {
		if _, _, body, _, err := s.Open(nil, sealed); err != nil || string(body) != "hi" {
			t.Fatalf("%s: open %q %v", name, body, err)
		}
	}
	if sealed, err = srv.Seal(nil, 9, 2, []byte("ho")); err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := cli.Open(nil, sealed); err != nil || string(body) != "ho" {
		t.Fatalf("old client: open %q %v", body, err)
	}
}

// A handshake parses only the ephemeral it generates: the static key was
// parsed once, and each DH reuses a kept key. So the server does 3 scalar
// multiplications (e, es, ee) and the client 3 (e, es, ee), not 6.
func TestAHandshakeParsesOnlyItsEphemeral(t *testing.T) {
	k := key(t)
	n := countParses(t)
	in, f1, err := Initiate(1, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Swap(0); got != 1 {
		t.Fatalf("Initiate parsed %d keys, want 1", got)
	}
	_, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Swap(0); got != 1 {
		t.Fatalf("Respond parsed %d keys, want 1", got)
	}
	if _, err := in.Finish(f2); err != nil {
		t.Fatal(err)
	}
	if got := n.Load(); got != 0 {
		t.Fatalf("Finish parsed %d keys, want 0", got)
	}
}

// A Key built from its bytes alone still answers, parsing the static key per
// handshake.
func TestAKeyFromItsBytesAloneStillWorks(t *testing.T) {
	k := key(t)
	ks := KeySet{1: {Private: k.Private, Public: k.Public}}
	if _, _, err := handshake(t, ks, 1, k.Public, nil); err != nil {
		t.Fatal(err)
	}
}

// DH fails where curve25519.X25519 does: on an all-zero result (a
// low-order public key such as all zeros) and on a public key that isn't 32
// bytes. A handshake with such an ephemeral fails.
func TestDHRefusesWhatX25519Refuses(t *testing.T) {
	k := key(t)
	s := newSuite(k)
	for _, pub := range [][]byte{make([]byte, 32), make([]byte, 31)} {
		if _, err := s.DH(k.Private, pub); err == nil {
			t.Fatalf("DH with a %d-byte public key %x: no error", len(pub), pub)
		}
		if _, err := dh25519.DH(k.Private, pub); err == nil {
			t.Fatalf("DH25519 accepted %x", pub)
		}
	}
	f := make([]byte, HeaderLen+HandshakeLen) // ephemeral all zeros
	Header{Version: Version, Cmd: cmdHandshake, Length: HandshakeLen}.Put(f)
	if _, _, err := Respond(k, f, cmdHandshake); !errors.Is(err, ErrHandshake) {
		t.Fatalf("err %v, want ErrHandshake", err)
	}
}

// The gateway's event loops answer on one Key at once (run with -race).
func TestConcurrentRespondsOnOneKey(t *testing.T) {
	k := key(t)
	ks := KeySet{1: k}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 10 {
				// Not handshake: its t.Fatal can't stop the test from a
				// goroutine (final review M2).
				in, f1, err := Initiate(1, k.Public, cmdHandshake)
				if err == nil {
					var f2 []byte
					if _, f2, err = Respond(ks[KeyID(f1)], f1, cmdHandshake); err == nil {
						_, err = in.Finish(f2)
					}
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// A finished handshake forgets its keys: each Session keeps the suite in its
// CipherStates for the whole connection, and needs only its cipher (final
// review I1: +400 B and the ephemeral per live connection otherwise).
func TestASessionKeepsNoHandshakeKeys(t *testing.T) {
	k := key(t)
	cli, srv := newSuite(Key{}), newSuite(k)
	in, f1, err := initiate(cli, 1, k.Public, cmdHandshake, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, f2, err := respond(srv, k, f1, cmdHandshake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := in.Finish(f2); err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*suite{"client": cli, "server": srv} {
		if s.keys != [2]*ecdh.PrivateKey{} || s.raw[0] != nil || s.raw[1] != nil {
			t.Errorf("%s suite still keeps its keys", name)
		}
	}
}

var suiteSink *suite // so newSuite's result escapes, as in respond

// A server suite reuses the Key's private bytes rather than copying them: one
// allocation, the suite itself, per Respond (final review M1).
func TestNewSuiteAllocatesOnlyItself(t *testing.T) {
	k := key(t)
	if n := testing.AllocsPerRun(100, func() { suiteSink = newSuite(k) }); n != 1 {
		t.Fatalf("newSuite allocates %v times, want 1", n)
	}
}
