package racetest

import (
	"net/http"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
)

// Key rotation (R4H-O10): a key that signs before Logic has fetched it gives
// KEY_UNKNOWN, then a login succeeds after the background unknown-kid
// refresh; during the overlap both kids are accepted.
func TestKeyRotation(t *testing.T) {
	s := New(t, Options{})
	const uid = "a1"
	k2, err := auth.GenerateKey("k2")
	if err != nil {
		t.Fatal(err)
	}
	s.PublishKeys(s.jwtKey, k2)
	req := s.Req(uid, "d1", 1)
	req.Token = s.TokenWith(k2, uid, "mobile", time.Now().UnixMilli(), time.Hour)
	if code := errorCode(t, s.Dial(0).Login(req)); code != clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN {
		t.Fatalf("got %v, want KEY_UNKNOWN", code)
	}
	eventually(t, "a k2 token logs in after the refresh", 5*time.Second, func() bool {
		req.ConnSeq++
		return s.Dial(0).Login(req).Cmd == clientpb.Cmd_CMD_LOGIN_RESP
	})
	s.Online(1, uid, "d1", req.ConnSeq+1) // k1 still accepted
	if n := s.logicMetric(`goim_logic_jwks_unknown_kid_refreshes_total`); n != 1 {
		t.Fatalf("%v unknown-kid refreshes, want 1", n)
	}
}

// JWKS (R2-O7): Logic starts with the account service down, becomes Ready
// from the jwks-fallback file and accepts tokens.
func TestLogicStartsFromTheJWKSFallback(t *testing.T) {
	s := New(t, Options{NoJWKS: true})
	s.Online(0, "a2", "d1", 1)
	if s.logicMetric(`goim_logic_jwks_fallback`) != 1 {
		t.Fatal("jwks_fallback is not 1")
	}
}

// JWKS: a Logic with no key set from either source never becomes Ready.
func TestLogicWithNoKeySetIsNeverReady(t *testing.T) {
	s := base(t, Options{NoJWKS: true, NoFallback: true})
	l := s.launchLogic()
	holds(t, "Logic stays NotReady and running", 3*time.Second, func() bool {
		if l.exited() {
			return false
		}
		resp, err := http.Get("http://" + l.Admin + "/readyz")
		if err != nil {
			return true // not listening yet
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	})
}
