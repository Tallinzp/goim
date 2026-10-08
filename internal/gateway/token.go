package gateway

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// maxToken matches Logic's limit on a JWS compact token (roadmap choice #16).
const maxToken = 4096

// tokenSubject returns the token's sub claim without verifying anything, or
// "" if it can't be read. Only a Logout after a Login with no definitive
// reply uses it, because only Logic's reply carries the verified uid: a
// forged sub makes that compare-and-delete miss, and a token that can't be
// read was rejected by Logic, so it wrote nothing.
func tokenSubject(tok string) string {
	if len(tok) > maxToken {
		return ""
	}
	_, rest, ok := strings.Cut(tok, ".")
	if !ok {
		return ""
	}
	payload, _, ok := strings.Cut(rest, ".")
	if !ok {
		return ""
	}
	b, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(b, &claims) != nil {
		return ""
	}
	return claims.Sub
}
