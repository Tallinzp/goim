package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
)

// jwk is one JSON Web Key; only OKP / Ed25519 keys are used.
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	D   string `json:"d,omitempty"` // private part; devtoken key files only
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// ParseJWKS reads a JWKS document. Keys that are not OKP/Ed25519 are skipped
// (the issuer may publish other kinds); a document with no usable key is an
// error, so a broken fetch never replaces a good set.
func ParseJWKS(data []byte) (KeySet, error) {
	var s jwkSet
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("auth: jwks: %w", err)
	}
	ks := KeySet{}
	for _, k := range s.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Kid == "" {
			continue
		}
		x, err := b64.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("auth: jwks: key %q: bad x", k.Kid)
		}
		ks[k.Kid] = ed25519.PublicKey(x)
	}
	if len(ks) == 0 {
		return nil, errors.New("auth: jwks: no Ed25519 key")
	}
	return ks, nil
}

// MarshalJWKS writes the public JWKS for keys.
func MarshalJWKS(keys ...PrivateKey) ([]byte, error) {
	s := jwkSet{Keys: make([]jwk, 0, len(keys))}
	for _, k := range keys {
		pub := k.Key.Public().(ed25519.PublicKey)
		s.Keys = append(s.Keys, jwk{Kty: "OKP", Crv: "Ed25519", Kid: k.Kid, X: b64.EncodeToString(pub)})
	}
	return json.MarshalIndent(s, "", "  ")
}

// GenerateKey returns a new signing key.
func GenerateKey(kid string) (PrivateKey, error) {
	if kid == "" {
		return PrivateKey{}, errors.New("auth: empty kid")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return PrivateKey{}, err
	}
	return PrivateKey{Kid: kid, Key: priv}, nil
}

// MarshalPrivateJWK writes k as a private OKP JWK (devtoken key file).
func MarshalPrivateJWK(k PrivateKey) ([]byte, error) {
	pub := k.Key.Public().(ed25519.PublicKey)
	return json.MarshalIndent(jwk{Kty: "OKP", Crv: "Ed25519", Kid: k.Kid,
		X: b64.EncodeToString(pub), D: b64.EncodeToString(k.Key.Seed())}, "", "  ")
}

// ParsePrivateJWK reads a key file written by MarshalPrivateJWK.
func ParsePrivateJWK(data []byte) (PrivateKey, error) {
	var k jwk
	if err := json.Unmarshal(data, &k); err != nil {
		return PrivateKey{}, fmt.Errorf("auth: private jwk: %w", err)
	}
	seed, err := b64.DecodeString(k.D)
	if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Kid == "" || err != nil || len(seed) != ed25519.SeedSize {
		return PrivateKey{}, errors.New("auth: private jwk: not an Ed25519 key with kid and d")
	}
	return PrivateKey{Kid: k.Kid, Key: ed25519.NewKeyFromSeed(seed)}, nil
}
