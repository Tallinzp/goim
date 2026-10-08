package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
)

func TestKeygenJWKSIssueServe(t *testing.T) {
	dir := t.TempDir()
	k1, k2, set := filepath.Join(dir, "k1.json"), filepath.Join(dir, "k2.json"), filepath.Join(dir, "jwks.json")
	for _, args := range [][]string{
		{"keygen", "-kid", "k1", "-out", k1},
		{"keygen", "-kid", "k2", "-out", k2},
		{"jwks", "-out", set, k1, k2},
	} {
		if err := run(args, &bytes.Buffer{}); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}

	// iat is issued with ms precision (roadmap choice #3).
	var out bytes.Buffer
	fixed := func() time.Time { return time.UnixMilli(1759900000123) }
	if err := issue([]string{"-key", k2, "-sub", "u1", "-appid", "im", "-platform", "pad", "-ttl", "2s"}, &out, fixed); err != nil {
		t.Fatal(err)
	}
	tok := strings.TrimSpace(out.String())

	srv := httptest.NewServer(jwksHandler(set))
	defer srv.Close()
	m := auth.NewManager(auth.DefaultManagerConfig(srv.URL, ""), auth.Observer{})
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	ks, err := auth.ParseJWKS(mustRead(t, set))
	if err != nil {
		t.Fatal(err)
	}
	c, err := auth.Verify(tok, ks, 1759900000123, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := auth.Claims{UID: "u1", AppID: "im", Platform: "pad", IatMs: 1759900000123, ExpMs: 1759900002123}
	if c != want {
		t.Fatalf("claims %+v, want %+v", c, want)
	}
	if !m.Ready() {
		t.Fatal("manager did not load the served JWKS")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
