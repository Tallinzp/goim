package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/loadgen"
	"github.com/Tallinzp/goim/internal/wire"
)

func TestRunRefusesBadFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "-env is required"},
		{[]string{"-env", "/nonexistent"}, "no such file"},
		{[]string{"-nope"}, "flag provided but not defined"},
		{[]string{"-env", "ENV", "-down", "gw-9"}, "down"},
		{[]string{"-env", "ENV", "-precompute"}, "sdk mode"},
		{[]string{"-env", "ENV", "-mode", "slow-auth", "-timeout", "0s"}, "Timeout"},
		{[]string{"-env", "ENV", "-mode", "garbage", "-source-net", "256"}, "source net"},
	} {
		for i, a := range tc.args {
			if a == "ENV" {
				tc.args[i] = writeEnv(t, "127.0.0.1:1")
			}
		}
		err := run(context.Background(), tc.args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err %v, want %q", tc.args, err, tc.want)
		}
	}
}

// writeEnv writes a stack.env for one gateway gw-0 at addr and returns its path.
func writeEnv(t *testing.T, addr string) string {
	t.Helper()
	dir := t.TempDir()
	k, _ := auth.GenerateKey("k1")
	doc, _ := auth.MarshalPrivateJWK(k)
	noise, _ := wire.GenerateKey() // a zero key fails the handshake before any byte is sent
	os.WriteFile(filepath.Join(dir, "jwt.jwk"), doc, 0o600)
	env := "GATEWAY_ADDRS=" + addr + "\nGATEWAY_IDS=gw-0\nNOISE_KEY_ID=1\nNOISE_PUBLIC=" +
		base64.StdEncoding.EncodeToString(noise.Public) + "\nJWT_KEY=" + filepath.Join(dir, "jwt.jwk") + "\n"
	os.WriteFile(filepath.Join(dir, "stack.env"), []byte(env), 0o644)
	return filepath.Join(dir, "stack.env")
}

// A run against a "gateway" that closes every connection stops at
// -duration and writes a result counting the failed attempts.
func TestRunWritesTheResultAtTheEnd(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Read(make([]byte, 1)) // the handshake has arrived: a close now is before the reply
			c.Close()
		}
	}()
	envPath := writeEnv(t, l.Addr().String())
	out := filepath.Join(filepath.Dir(envPath), "result.json")
	err = run(context.Background(), []string{"-env", envPath, "-clients", "3", "-rate", "100",
		"-duration", "1s", "-control", "127.0.0.1:0", "-out", out}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var res loadgen.Result
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatalf("result: %v\n%s", err, b)
	}
	if res.Mode != "light" || res.Clients != 3 || res.Started != 3 || res.Online != 0 || res.Failed["pre-handshake"] < 3 {
		t.Fatalf("result %+v", res)
	}
}
