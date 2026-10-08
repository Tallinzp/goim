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
