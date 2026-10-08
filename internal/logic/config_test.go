package logic

import (
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	c := DefaultConfig()
	if c.StaleAfter() != 60*time.Second || c.RecentRenewal() != 15*time.Second {
		t.Fatalf("stale %v recent %v", c.StaleAfter(), c.RecentRenewal())
	}
}

// The test timing profile scales time ~10× but keeps login_deadline (spec).
func TestScaledProfileIsValid(t *testing.T) {
	c := DefaultConfig()
	c.Store.LeaseTTL = 3 * time.Second
	c.RenewInterval = time.Second
	c.RedisFailover = 1500 * time.Millisecond
	c.LeaseCache = 100 * time.Millisecond
	c.SweepInterval = 500 * time.Millisecond
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefusesBrokenRatios(t *testing.T) {
	cases := map[string]func(*Config){
		"lease TTL":    func(c *Config) { c.Store.LeaseTTL = c.RedisFailover + c.RenewInterval },
		"lease cache":  func(c *Config) { c.LeaseCache = c.RenewInterval },
		"sweep batch":  func(c *Config) { c.SweepBatch = c.SweepRate + 1 },
		"delete rate":  func(c *Config) { c.DeleteRate = 0 },
		"kick workers": func(c *Config) { c.Kick.Workers = 0 },
		"kick addr":    func(c *Config) { c.Kick.AddrFormat = "" },
		"deadline": func(c *Config) {
			c.Store = store.Config{SessTTL: time.Hour, LeaseTTL: time.Hour, WaitTimeout: time.Second}
		},
		"kick deadline": func(c *Config) { c.Kick.Deadline = 0 },
	}
	for name, mutate := range cases {
		c := DefaultConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	c := DefaultConfig()
	c.Store.LeaseTTL = 20 * time.Second
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "R2-O5") {
		t.Fatalf("lease TTL 20s with failover 15s + renew 10s: %v", err)
	}
}
