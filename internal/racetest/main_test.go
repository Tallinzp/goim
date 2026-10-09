package racetest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const envSuite = "GOIM_RACE_SUITE"

// binDir holds the binaries TestMain built: logic, gateway, devtoken.
var binDir string

// redisServer is the redis-server binary every stack runs: GOIM_REDIS_SERVER,
// or redis-server on PATH (choice #62).
var redisServer string

func TestMain(m *testing.M) {
	if os.Getenv(envSuite) == "" {
		os.Exit(m.Run()) // every scenario skips
	}
	var err error
	if redisServer, err = findRedis(); err != nil {
		fmt.Fprintln(os.Stderr, "racetest:", err)
		os.Exit(2)
	}
	if dir := os.Getenv("GOIM_RACE_BIN"); dir != "" { // a helper run reuses its parent's build
		binDir = dir
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "goim-race-bin-")
	if err == nil {
		err = build(dir)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "racetest: build:", err)
		os.Exit(2)
	}
	binDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// build compiles the binaries with the race detector, as the suite runs them.
func build(dir string) error {
	root, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-race", "-o", dir+string(filepath.Separator),
		"./cmd/logic", "./cmd/gateway", "./cmd/devtoken")
	cmd.Dir = strings.TrimSpace(string(root))
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v\n%s", err, out)
	}
	return nil
}

var redisVersion = regexp.MustCompile(`v=(\d+)\.(\d+)\.(\d+)`)

// findRedis returns the redis-server to run and prints its version. It
// refuses one older than 7.0; the pinned build is 7.4.11, the Tier 2
// compose version (make redis-server).
func findRedis() (string, error) {
	path := os.Getenv("GOIM_REDIS_SERVER")
	if path == "" {
		var err error
		if path, err = exec.LookPath("redis-server"); err != nil {
			return "", fmt.Errorf("no redis-server on PATH: run make redis-server, or set GOIM_REDIS_SERVER")
		}
	}
	out, err := exec.Command(path, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("%s --version: %v", path, err)
	}
	v := redisVersion.FindStringSubmatch(string(out))
	if v == nil {
		return "", fmt.Errorf("%s --version: no version in %q", path, out)
	}
	if major, _ := strconv.Atoi(v[1]); major < 7 {
		return "", fmt.Errorf("%s is Redis %s.%s.%s; the suite needs 7.0 or later (make redis-server builds 7.4.11)", path, v[1], v[2], v[3])
	}
	fmt.Printf("racetest: %s: Redis %s.%s.%s\n", path, v[1], v[2], v[3])
	return path, nil
}

// suite skips t unless the race suite is on.
func suite(t *testing.T) {
	t.Helper()
	if binDir == "" {
		t.Skip("race suite: set " + envSuite + "=1 (make test-race-suite)")
	}
}
