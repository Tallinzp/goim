package loadgen

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tallinzp/goim/internal/auth"
)

// writeEnv writes a stack.env with a real token key; mod edits the lines.
func writeEnv(t *testing.T, mod func(map[string]string)) string {
	t.Helper()
	dir := t.TempDir()
	k, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalPrivateJWK(k)
	key := filepath.Join(dir, "jwt.jwk")
	if err := os.WriteFile(key, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	kv := map[string]string{"GATEWAY_ADDRS": "127.0.0.1:1,127.0.0.1:2", "GATEWAY_IDS": "gw-0,gw-1",
		"NOISE_KEY_ID": "1", "NOISE_PUBLIC": base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"JWT_KEY": key, "REDIS_ADDR": "127.0.0.1:3"}
	if mod != nil {
		mod(kv)
	}
	var b strings.Builder
	b.WriteString("# stack.env\n\n")
	for k, v := range kv {
		b.WriteString(k + "=" + v + "\n")
	}
	path := filepath.Join(dir, "stack.env")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadEnv(t *testing.T) {
	e, err := LoadEnv(writeEnv(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.GatewayAddrs) != 2 || e.GatewayAddrs[1] != "127.0.0.1:2" || e.GatewayIDs[1] != "gw-1" ||
		e.NoiseKeyID != 1 || len(e.NoisePublic) != 32 || e.JWTKey.Kid != "k1" {
		t.Fatalf("env: %+v", e)
	}
	if e.Gateway("gw-1") != 1 || e.Gateway("gw-9") != -1 {
		t.Fatal("Gateway lookup")
	}
}

func TestLoadEnvRefusesBrokenFiles(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mod        func(map[string]string)
	}{
		{"missing key", "NOISE_PUBLIC is missing", func(kv map[string]string) { delete(kv, "NOISE_PUBLIC") }},
		{"ids and addrs differ", "2 GATEWAY_ADDRS but 1 GATEWAY_IDS", func(kv map[string]string) { kv["GATEWAY_IDS"] = "gw-0" }},
		{"short noise key", "32-byte", func(kv map[string]string) { kv["NOISE_PUBLIC"] = "AAAA" }},
		{"bad key id", "NOISE_KEY_ID", func(kv map[string]string) { kv["NOISE_KEY_ID"] = "70000" }},
		{"no token key file", "JWT_KEY", func(kv map[string]string) { kv["JWT_KEY"] = "/nonexistent" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadEnv(writeEnv(t, tc.mod))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want %q", err, tc.want)
			}
		})
	}
}
