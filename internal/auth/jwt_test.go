package auth

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testKey(t *testing.T, kid string) PrivateKey {
	t.Helper()
	k, err := GenerateKey(kid)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func keySet(t *testing.T, keys ...PrivateKey) KeySet {
	t.Helper()
	doc, err := MarshalJWKS(keys...)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := ParseJWKS(doc)
	if err != nil {
		t.Fatal(err)
	}
	return ks
}

func claims(iatMs int64) Claims {
	return Claims{UID: "u1", AppID: "im", Platform: "mobile", IatMs: iatMs, ExpMs: iatMs + 3600_000}
}

func sign(t *testing.T, k PrivateKey, c Claims) string {
	t.Helper()
	tok, err := Sign(k, c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestVerifyRoundTripKeepsMilliseconds(t *testing.T) {
	k := testKey(t, "k1")
	// 1759900000.123 is not exact as a float64; the ms must survive anyway.
	for _, iat := range []int64{1759900000123, 1759900000001, 1759900000999, 1000} {
		c := claims(iat)
		got, err := Verify(sign(t, k, c), keySet(t, k), iat, 0)
		if err != nil {
			t.Fatalf("iat %d: %v", iat, err)
		}
		if got != c {
			t.Fatalf("got %+v, want %+v", got, c)
		}
	}
}

func TestVerifyAcceptsEitherKeyDuringRotation(t *testing.T) {
	k1, k2 := testKey(t, "k1"), testKey(t, "k2")
	ks := keySet(t, k1, k2)
	for _, k := range []PrivateKey{k1, k2} {
		if _, err := Verify(sign(t, k, claims(5000)), ks, 5000, 0); err != nil {
			t.Fatalf("kid %s: %v", k.Kid, err)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	k, other := testKey(t, "k1"), testKey(t, "k1") // same kid, different key
	ks := keySet(t, k)
	good := sign(t, k, claims(5000))
	parts := strings.Split(good, ".")
	hdr := func(h string) string { return b64.EncodeToString([]byte(h)) }
	body := func(p string) string { return b64.EncodeToString([]byte(p)) }

	cases := []struct {
		name  string
		token string
		now   int64
		want  error
	}{
		{"unknown kid", sign(t, testKey(t, "k9"), claims(5000)), 5000, ErrKeyUnknown},
		{"wrong key for kid", sign(t, other, claims(5000)), 5000, ErrInvalid},
		{"tampered claims", parts[0] + "." + body(`{"sub":"u2","appid":"im","platform":"mobile","iat":5,"exp":9999}`) + "." + parts[2], 5000, ErrInvalid},
		{"alg none", hdr(`{"alg":"none","kid":"k1"}`) + "." + parts[1] + ".", 5000, ErrInvalid},
		{"alg HS256", hdr(`{"alg":"HS256","kid":"k1"}`) + "." + parts[1] + "." + parts[2], 5000, ErrInvalid},
		{"two segments", parts[0] + "." + parts[1], 5000, ErrInvalid},
		{"expired", good, 5000 + 3600_000, ErrInvalid},
		{"too long", strings.Repeat("a", MaxTokenLen+1), 5000, ErrInvalid},
	}
	for _, c := range cases {
		if _, err := Verify(c.token, ks, c.now, 0); !errors.Is(err, c.want) {
			t.Errorf("%s: err %v, want %v", c.name, err, c.want)
		}
	}
}

func TestVerifyRejectsNonCanonicalSignature(t *testing.T) {
	k := testKey(t, "k1")
	tok := sign(t, k, claims(5000))
	// A 64-byte signature is 86 base64 characters; the last one carries 2 data
	// bits and 4 padding bits. Setting a padding bit keeps the bytes the same.
	alpha := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alpha, tok[len(tok)-1])
	alt := tok[:len(tok)-1] + string(alpha[last|1])
	if alt == tok {
		alt = tok[:len(tok)-1] + string(alpha[last|2])
	}
	if _, err := Verify(alt, keySet(t, k), 5000, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-canonical signature: %v", err)
	}
}

func TestVerifyLeeway(t *testing.T) {
	k := testKey(t, "k1")
	tok := sign(t, k, claims(5000)) // exp 3605000
	if _, err := Verify(tok, keySet(t, k), 3605000+29_999, 30_000); err != nil {
		t.Fatalf("inside leeway: %v", err)
	}
	if _, err := Verify(tok, keySet(t, k), 3605000+30_000, 30_000); !errors.Is(err, ErrInvalid) {
		t.Fatalf("past leeway: %v", err)
	}
}

func TestVerifyRejectsBadClaims(t *testing.T) {
	k := testKey(t, "k1")
	ks := keySet(t, k)
	for _, p := range []string{
		`{"appid":"im","platform":"mobile","iat":5,"exp":99999999}`,               // no sub
		`{"sub":"u","appid":"im","platform":"mobile","iat":1.7e9,"exp":99999999}`, // exponent
		`{"sub":"u","appid":"im","platform":"mobile","iat":-5,"exp":99999999}`,    // negative
		`{"sub":"u","appid":"im","platform":"mobile","iat":5}`,                    // no exp
	} {
		tok := rawSign(t, k, p)
		if _, err := Verify(tok, ks, 1, 0); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err %v, want ErrInvalid", p, err)
		}
	}
}

// rawSign signs an arbitrary claims document.
func rawSign(t *testing.T, k PrivateKey, claimsJSON string) string {
	t.Helper()
	h, err := json.Marshal(header{Alg: "EdDSA", Kid: k.Kid})
	if err != nil {
		t.Fatal(err)
	}
	signing := b64.EncodeToString(h) + "." + b64.EncodeToString([]byte(claimsJSON))
	return signing + "." + b64.EncodeToString(ed25519.Sign(k.Key, []byte(signing)))
}

func TestParseMs(t *testing.T) {
	for in, want := range map[string]int64{
		"0": 0, "5": 5000, "5.1": 5100, "5.12": 5120, "5.123": 5123, "5.1239": 5123, "1759900000.123": 1759900000123,
	} {
		got, err := parseMs(in)
		if err != nil || got != want {
			t.Errorf("parseMs(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "-1", "1e3", "1.", ".5", "1.0000000001", "0x10"} {
		if _, err := parseMs(in); err == nil {
			t.Errorf("parseMs(%q) accepted", in)
		}
	}
}

func TestPrivateJWKRoundTrip(t *testing.T) {
	k := testKey(t, "k1")
	doc, err := MarshalPrivateJWK(k)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePrivateJWK(doc)
	if err != nil || got.Kid != "k1" || !got.Key.Equal(k.Key) {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	if _, err := ParseJWKS([]byte(`{"keys":[{"kty":"RSA","kid":"r"}]}`)); err == nil {
		t.Fatal("a JWKS with no Ed25519 key must be an error")
	}
}
