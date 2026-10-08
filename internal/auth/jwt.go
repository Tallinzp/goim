// Package auth verifies the account service's Ed25519 access tokens (spec:
// Auth, JWKS). It is a minimal JWS compact / EdDSA implementation: only what
// Logic accepts is parsed, and iat is read with millisecond precision.
package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	// ErrKeyUnknown: the token's kid is not in the key set (R4H-O10).
	ErrKeyUnknown = errors.New("auth: unknown kid")
	// ErrInvalid: malformed, bad signature, wrong alg, missing claim or expired.
	ErrInvalid = errors.New("auth: invalid token")
)

// MaxTokenLen bounds the work spent on one token.
const MaxTokenLen = 4096

// Claims is what Logic reads from an access token.
type Claims struct {
	UID      string // sub
	AppID    string // appid
	Platform string // platform: mobile | pad | web | pc
	IatMs    int64  // iat, fractional seconds read exactly to ms (roadmap choice #3)
	ExpMs    int64  // exp
}

// KeySet maps kid to an Ed25519 public key.
type KeySet map[string]ed25519.PublicKey

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ,omitempty"`
}

type payload struct {
	Sub      string      `json:"sub"`
	AppID    string      `json:"appid"`
	Platform string      `json:"platform"`
	Iat      json.Number `json:"iat"`
	Exp      json.Number `json:"exp"`
}

// b64 rejects non-canonical encodings (non-zero padding bits), so one token
// has exactly one string form.
var b64 = base64.RawURLEncoding.Strict()

// Verify checks token against keys at nowMs. A token is expired once
// nowMs ≥ exp + leewayMs. An unknown kid returns an error that matches
// ErrKeyUnknown; every other failure matches ErrInvalid.
func Verify(token string, keys KeySet, nowMs, leewayMs int64) (Claims, error) {
	if len(token) > MaxTokenLen {
		return Claims{}, fmt.Errorf("%w: %d bytes", ErrInvalid, len(token))
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: not a JWS compact token", ErrInvalid)
	}
	var h header
	if err := decodeJSON(parts[0], &h); err != nil {
		return Claims{}, fmt.Errorf("%w: header: %v", ErrInvalid, err)
	}
	if h.Alg != "EdDSA" || h.Kid == "" {
		return Claims{}, fmt.Errorf("%w: alg %q kid %q", ErrInvalid, h.Alg, h.Kid)
	}
	pub, ok := keys[h.Kid]
	if !ok {
		return Claims{}, fmt.Errorf("%w: %q", ErrKeyUnknown, h.Kid)
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		return Claims{}, fmt.Errorf("%w: bad signature", ErrInvalid)
	}
	var p payload
	if err := decodeJSON(parts[1], &p); err != nil {
		return Claims{}, fmt.Errorf("%w: claims: %v", ErrInvalid, err)
	}
	c := Claims{UID: p.Sub, AppID: p.AppID, Platform: p.Platform}
	if c.UID == "" || c.AppID == "" || c.Platform == "" {
		return Claims{}, fmt.Errorf("%w: sub, appid and platform are required", ErrInvalid)
	}
	if c.IatMs, err = parseMs(string(p.Iat)); err != nil {
		return Claims{}, fmt.Errorf("%w: iat: %v", ErrInvalid, err)
	}
	if c.ExpMs, err = parseMs(string(p.Exp)); err != nil {
		return Claims{}, fmt.Errorf("%w: exp: %v", ErrInvalid, err)
	}
	if nowMs >= c.ExpMs+leewayMs {
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalid)
	}
	return c, nil
}

func decodeJSON(seg string, v any) error {
	raw, err := b64.DecodeString(seg)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(v)
}

// numericDate is a JSON NumericDate as devtoken writes it: whole seconds with
// up to 9 fraction digits, no sign or exponent.
var numericDate = regexp.MustCompile(`^([0-9]{1,12})(?:\.([0-9]{1,9}))?$`)

// parseMs reads a NumericDate exactly to milliseconds (extra digits are
// truncated). Parsing it as a float64 could round 1759900000.123 down to
// …122, and a token issued in the same ms as a revocation would read as revoked.
func parseMs(s string) (int64, error) {
	m := numericDate.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("NumericDate %q", s)
	}
	sec, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, err
	}
	frac := (m[2] + "000")[:3]
	msPart, _ := strconv.ParseInt(frac, 10, 64)
	return sec*1000 + msPart, nil
}

// formatMs writes ms as a NumericDate with exactly three fraction digits.
func formatMs(ms int64) json.Number {
	return json.Number(fmt.Sprintf("%d.%03d", ms/1000, ms%1000))
}

// PrivateKey is a signing key with its kid (devtoken and tests only; the
// account service signs production tokens).
type PrivateKey struct {
	Kid string
	Key ed25519.PrivateKey
}

// Sign issues a token for c. IatMs and ExpMs must be non-negative.
func Sign(k PrivateKey, c Claims) (string, error) {
	if c.IatMs < 0 || c.ExpMs < 0 {
		return "", errors.New("auth: iat and exp must be non-negative")
	}
	h, err := json.Marshal(header{Alg: "EdDSA", Kid: k.Kid, Typ: "JWT"})
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(payload{Sub: c.UID, AppID: c.AppID, Platform: c.Platform,
		Iat: formatMs(c.IatMs), Exp: formatMs(c.ExpMs)})
	if err != nil {
		return "", err
	}
	signing := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
	return signing + "." + b64.EncodeToString(ed25519.Sign(k.Key, []byte(signing))), nil
}
