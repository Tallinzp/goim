package main

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultFlagsAreValid(t *testing.T) {
	cfg, sc, o, err := parseFlags([]string{"-jwks-url", "http://account/jwks"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster {
		t.Fatalf("defaults: %+v %+v %+v", cfg, sc, o)
	}
}

// The test timing profile (Plan 5) scales intervals ~10× and keeps the ratios.
func TestScaledProfileFlags(t *testing.T) {
	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s",
		"-redis-failover", "1500ms", "-lease-cache", "100ms", "-sweep-interval", "500ms"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRefusesBrokenRatio(t *testing.T) {
	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s", "-redis-failover", "2s"})
	if err == nil || !strings.Contains(err.Error(), "R2-O5") {
		t.Fatalf("lease TTL 3s with failover 2s + renew 1s: %v", err)
	}
	if err := run([]string{"-jwks-url", "x", "-max-conn-age-grace", "5s"}); err == nil {
		t.Fatal("run started with a grace below the Login timeout")
	}
}

// The race suite's flags (Plan 5b): Kick addresses from a file, faults on,
// and the intervals the timing profile scales that were not flags before.
func TestRaceSuiteFlags(t *testing.T) {
	cfg, _, o, err := parseFlags([]string{"-jwks-url", "x", "-kick-addr-file", "/tmp/kick.json", "-faults",
		"-kick-retry-spacing", "1s", "-kick-idle-timeout", "6s", "-sweep-batch", "50", "-delete-batch", "50",
		"-jwks-refresh", "30s", "-jwks-retry-empty", "500ms", "-jwks-unknown-kid", "3s"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.faults || o.kickAddrFile != "/tmp/kick.json" || cfg.Kick.RetrySpacing != time.Second ||
		cfg.Kick.IdleTimeout != 6*time.Second || cfg.SweepBatch != 50 || cfg.DeleteBatch != 50 {
		t.Fatalf("parsed %+v %+v", cfg, o)
	}
	if o.jwks.URL != "x" || o.jwks.Refresh != 30*time.Second || o.jwks.RetryEmpty != 500*time.Millisecond ||
		o.jwks.UnknownKid != 3*time.Second || o.jwks.FetchTimeout != 5*time.Second {
		t.Fatalf("JWKS config %+v", o.jwks)
	}
	if _, _, _, err := parseFlags([]string{"-jwks-url", "x", "-jwks-unknown-kid", "0s"}); err == nil {
		t.Fatal("a zero unknown-kid interval was accepted")
	}
}
