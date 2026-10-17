package wire

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/flynn/noise"
)

// newPrivateKey is ecdh's X25519 NewPrivateKey, which computes the public key
// at once: one scalar multiplication. A variable so tests can count calls.
var newPrivateKey = ecdh.X25519().NewPrivateKey

// suiteName is Noise_NK_25519_ChaChaPoly_SHA256's cipher suite name, the
// one flynn/noise's own DH25519 suite has.
var suiteName = []byte("25519_ChaChaPoly_SHA256")

// suite is the cipher suite of one handshake (spec: Client encryption).
// SHA-256 rather than BLAKE2s because the apps' platform crypto (iOS
// CryptoKit, Android) has it natively.
//
// Its DH function is flynn/noise's DH25519 in bytes, but it keeps the
// *ecdh.PrivateKey of the static key and of the ephemeral it generates, so
// each DH is one scalar multiplication. DH25519 calls x/crypto's
// curve25519.X25519, which builds an *ecdh.PrivateKey from the bytes each
// time and so computes a public key nobody reads: two multiplications per DH
// and two per key generation (#130).
type suite struct {
	noise.CipherFunc
	noise.HashFunc
	keys [2]*ecdh.PrivateKey // the static key, then the ephemeral; nil if absent
	raw  [2][]byte           // their private bytes, as flynn/noise passes them back
}

// newSuite returns the suite for one handshake with the static key k (nil
// for none, the client's side of NK). A suite serves one handshake: it
// remembers the one ephemeral it generates.
func newSuite(k *ecdh.PrivateKey) *suite {
	s := &suite{CipherFunc: noise.CipherChaChaPoly, HashFunc: noise.HashSHA256}
	if k != nil {
		s.keys[0], s.raw[0] = k, k.Bytes()
	}
	return s
}

func (s *suite) Name() []byte   { return suiteName }
func (s *suite) DHLen() int     { return 32 }
func (s *suite) DHName() string { return "25519" }

// GenerateKeypair reads the private key from rng as DH25519 does, so a given
// rng gives the same key pair, and keeps it for DH.
func (s *suite) GenerateKeypair(rng io.Reader) (noise.DHKey, error) {
	if rng == nil {
		rng = rand.Reader
	}
	b := make([]byte, 32)
	if _, err := io.ReadFull(rng, b); err != nil {
		return noise.DHKey{}, err
	}
	k, err := newPrivateKey(b)
	if err != nil {
		return noise.DHKey{}, err
	}
	s.keys[1], s.raw[1] = k, b
	return noise.DHKey{Private: b, Public: k.PublicKey().Bytes()}, nil
}

// forget drops the kept keys once the handshake's DHs are done. The Session's
// CipherStates keep the suite for the connection's life and use only its
// cipher (final review I1).
func (s *suite) forget() { s.keys, s.raw = [2]*ecdh.PrivateKey{}, [2][]byte{} }

// DH uses the kept key whose bytes are priv, else parses priv. Like
// curve25519.X25519 it fails on a public key that isn't 32 bytes and on an
// all-zero result (a low-order point).
func (s *suite) DH(priv, pub []byte) ([]byte, error) {
	var k *ecdh.PrivateKey
	for i, r := range s.raw {
		if s.keys[i] != nil && subtle.ConstantTimeCompare(r, priv) == 1 {
			k = s.keys[i]
			break
		}
	}
	if k == nil {
		var err error
		if k, err = newPrivateKey(priv); err != nil {
			return nil, err
		}
	}
	p, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return k.ECDH(p)
}
