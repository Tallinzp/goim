// Package loadgen is the load generator behind cmd/loadgen (Plan 6a2): many
// client connections from one process, spread over the stack's gateways by
// a least-connections balancer, with per-client times that give the storm
// SLO distribution directly (spec: Verification → Load tests). Two modes
// (choice #80): light holds connections with clientconn and a shared
// pinger; sdk runs one internal/sdk client per connection, with its
// reconnect rules.
package loadgen

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Tallinzp/goim/internal/auth"
)

// Env is what loadgen reads from scripts/stack.sh's stack.env (P6a1I-4).
type Env struct {
	GatewayAddrs []string // client listeners, in gateway order
	GatewayIDs   []string // gw-0, gw-1, … in the same order
	NoiseKeyID   uint16
	NoisePublic  []byte // the pinned Noise static public key
	JWTKey       auth.PrivateKey
}

// LoadEnv reads a stack.env file: KEY=VALUE lines, blank lines and #
// comments skipped. GATEWAY_ADDRS, GATEWAY_IDS, NOISE_KEY_ID, NOISE_PUBLIC
// and JWT_KEY (the path of devtoken's private key file) are required.
func LoadEnv(path string) (Env, error) {
	f, err := os.Open(path)
	if err != nil {
		return Env{}, err
	}
	defer f.Close()
	kv := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return Env{}, fmt.Errorf("loadgen: %s: not KEY=VALUE: %q", path, line)
		}
		kv[k] = v
	}
	if err := sc.Err(); err != nil {
		return Env{}, err
	}
	for _, k := range []string{"GATEWAY_ADDRS", "GATEWAY_IDS", "NOISE_KEY_ID", "NOISE_PUBLIC", "JWT_KEY"} {
		if kv[k] == "" {
			return Env{}, fmt.Errorf("loadgen: %s: %s is missing", path, k)
		}
	}
	e := Env{GatewayAddrs: strings.Split(kv["GATEWAY_ADDRS"], ","), GatewayIDs: strings.Split(kv["GATEWAY_IDS"], ",")}
	if len(e.GatewayAddrs) != len(e.GatewayIDs) {
		return Env{}, fmt.Errorf("loadgen: %s: %d GATEWAY_ADDRS but %d GATEWAY_IDS", path, len(e.GatewayAddrs), len(e.GatewayIDs))
	}
	id, err := strconv.ParseUint(kv["NOISE_KEY_ID"], 10, 16)
	if err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: NOISE_KEY_ID: %w", path, err)
	}
	e.NoiseKeyID = uint16(id)
	if e.NoisePublic, err = base64.StdEncoding.DecodeString(kv["NOISE_PUBLIC"]); err != nil || len(e.NoisePublic) != 32 {
		return Env{}, fmt.Errorf("loadgen: %s: NOISE_PUBLIC is not a base64 32-byte key", path)
	}
	doc, err := os.ReadFile(kv["JWT_KEY"])
	if err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: JWT_KEY: %w", path, err)
	}
	if e.JWTKey, err = auth.ParsePrivateJWK(doc); err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: JWT_KEY: %w", path, err)
	}
	return e, nil
}

// Gateway is the index of gateway id, or -1.
func (e Env) Gateway(id string) int {
	for i, g := range e.GatewayIDs {
		if g == id {
			return i
		}
	}
	return -1
}
