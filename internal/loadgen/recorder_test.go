package loadgen

import (
	"testing"
	"time"
)

var t0 = time.UnixMilli(1_000_000)

func at(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func TestRecorderCountsPerGateway(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(5))
	r.Online(1, 1, at(0), at(7))
	r.Online(2, 1, at(0), at(9))
	r.Lost(2, "lost", at(10))
	r.Lost(2, "lost", at(11)) // not logged in: ignored
	r.Failed("dial")
	c := r.Counts()
	if c.Online != 2 || c.OnlineByGateway["gw-0"] != 1 || c.OnlineByGateway["gw-1"] != 1 || c.Logins != 3 ||
		c.Lost["lost"] != 1 || c.Failed["dial"] != 1 || c.LoginLatency.Max != 9 || c.LoginLatency.N != 3 {
		t.Fatalf("counts %+v", c)
	}
}

// The population is the clients logged in on the marked gateway at the
// mark; times run from the mark (b1) and from each client's loss (b2).
func TestRecorderSLOFromMarkAndDetection(t *testing.T) {
	r := NewRecorder(4, []string{"gw-0", "gw-1"})
	for i := range 3 {
		r.Online(i, 1, at(0), at(1))
	}
	r.Online(3, 0, at(0), at(1)) // on another gateway: not in the population
	if n, err := r.Mark("death", 1, at(1000)); err != nil || n != 3 {
		t.Fatalf("mark: %d %v", n, err)
	}
	if _, err := r.Mark("death", 1, at(1000)); err == nil {
		t.Fatal("duplicate mark accepted")
	}
	r.Lost(0, "lost", at(1010))
	r.Lost(1, "lost", at(1020))
	r.Lost(2, "pong-timeout", at(13_000))
	r.Online(0, 0, at(5000), at(6000))
	r.Online(1, 0, at(20000), at(21_020))
	r.Online(2, 0, at(30000), at(33_000))
	r.Lost(3, "lost", at(40_000)) // not a member: no effect
	s, err := r.SLO("death", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway != "gw-1" || s.Population != 3 || s.Lost != 3 || s.Back != 3 {
		t.Fatalf("slo %+v", s)
	}
	// From the mark: 5000, 20020, 32000 → p99 32000 > 30000.
	if s.FromMark.P50 != 20_020 || s.FromMark.P99 != 32_000 || s.PassFromMark {
		t.Fatalf("from mark %+v pass %v", s.FromMark, s.PassFromMark)
	}
	// From detection: 4990, 20000, 20000 → p99 20000.
	if s.FromDetection.P99 != 20_000 || !s.PassFromDetection {
		t.Fatalf("from detection %+v pass %v", s.FromDetection, s.PassFromDetection)
	}
	if s.Detection.P99 != 12_000 {
		t.Fatalf("detection %+v", s.Detection)
	}
}

// A member that never logs in again fails the SLO, however fast the rest.
func TestRecorderSLOMissingMemberFails(t *testing.T) {
	r := NewRecorder(2, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(1))
	r.Online(1, 0, at(0), at(1))
	r.Mark("death", 0, at(100))
	r.Lost(0, "lost", at(101))
	r.Lost(1, "lost", at(101))
	r.Online(0, 0, at(200), at(300))
	s, _ := r.SLO("death", 30*time.Second)
	if s.Back != 1 || s.FromMark.Missing != 1 || s.PassFromMark || s.PassFromDetection {
		t.Fatalf("slo %+v", s)
	}
	if _, err := r.SLO("nope", time.Second); err == nil {
		t.Fatal("unknown mark accepted")
	}
	// A mark with nobody in it does not pass either.
	r.Mark("empty", 1, at(400))
	if s, _ := r.SLO("empty", time.Second); s.Population != 0 || s.PassFromMark {
		t.Fatalf("empty %+v", s)
	}
}

// A mark on every gateway (-1) takes everyone logged in.
func TestRecorderMarkAll(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(1))
	r.Online(1, 1, at(0), at(1))
	if n, _ := r.Mark("all", -1, at(5)); n != 2 {
		t.Fatalf("population %d, want 2", n)
	}
	if s, _ := r.SLO("all", time.Second); s.Gateway != "all" {
		t.Fatalf("gateway %q", s.Gateway)
	}
	if got := r.Marks(); len(got) != 1 || got[0] != "all" {
		t.Fatalf("marks %v", got)
	}
}

// A member counts as back only after it lost its connection: a second
// Online with no loss in between (not a relogin) is not.
func TestRecorderBackNeedsALoss(t *testing.T) {
	r := NewRecorder(1, []string{"gw-0"})
	r.Online(0, 0, at(0), at(1))
	r.Mark("m", 0, at(10))
	r.Online(0, 0, at(20), at(30))
	if s, _ := r.SLO("m", time.Second); s.Back != 0 {
		t.Fatalf("back %d without a loss", s.Back)
	}
	r.Lost(0, "lost", at(40))
	r.Online(0, 0, at(50), at(60))
	if s, _ := r.SLO("m", time.Second); s.Back != 1 || s.FromMark.P50 != 50 {
		t.Fatalf("slo %+v", s)
	}
}

// A later mark takes over members of an earlier one that were not back yet;
// the earlier SLO counts them as missing and reports how many it lost so
// (6a2a final review). Members already back are not counted.
func TestRecorderSLOReportsReassignedMembers(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0", "gw-1"})
	for i := range 3 {
		r.Online(i, 0, at(0), at(1))
	}
	r.Mark("a", 0, at(10))
	r.Lost(0, "lost", at(20))
	r.Online(0, 1, at(30), at(40)) // back before the next mark
	r.Mark("b", -1, at(50))        // takes 1 and 2, not back yet, and 0
	s, _ := r.SLO("a", time.Second)
	if s.Reassigned != 2 || s.Back != 1 || s.FromMark.Missing != 2 {
		t.Fatalf("slo a %+v", s)
	}
	if s, _ := r.SLO("b", time.Second); s.Population != 3 || s.Reassigned != 0 {
		t.Fatalf("slo b %+v", s)
	}
}

// Clients that reach backoff (P6a2bI-2): each client once, every wait by kind.
func TestRecorderCountsBackoffClients(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0"})
	r.Backoff(0, "backoff")
	r.Backoff(0, "backoff")
	r.Backoff(1, "failure")
	c := r.Counts()
	if c.BackoffClients != 2 || c.BackoffWaits["backoff"] != 2 || c.BackoffWaits["failure"] != 1 || len(c.BackoffWaits) != 2 {
		t.Fatalf("backoff: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
	}
}

// The backoff since the latest mark (P6b2a-stressI-1): b_stress's ramp can
// put clients in backoff before the kill, and the run's own count would mix
// them into the storm's. Each mark restarts it: a client counts once per mark.
func TestRecorderCountsBackoffSinceMark(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0"})
	r.Backoff(0, "backoff") // the ramp
	if c := r.Counts(); c.BackoffSinceMark.Mark != "" || c.BackoffSinceMark.Clients != 1 {
		t.Fatalf("before a mark: %+v", c.BackoffSinceMark)
	}
	if _, err := r.Mark("death", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c := r.Counts(); c.BackoffSinceMark.Clients != 0 || len(c.BackoffSinceMark.Waits) != 0 {
		t.Fatalf("at the mark: %+v", c.BackoffSinceMark)
	}
	r.Backoff(0, "failure") // backed off in the ramp, and again in the storm
	r.Backoff(0, "backoff")
	r.Backoff(1, "backoff")
	c := r.Counts()
	s := c.BackoffSinceMark
	if s.Mark != "death" || s.Clients != 2 || s.Waits["backoff"] != 2 || s.Waits["failure"] != 1 || len(s.Waits) != 2 {
		t.Fatalf("since the mark: %+v", s)
	}
	if c.BackoffClients != 2 || c.BackoffWaits["backoff"] != 3 {
		t.Fatalf("the run: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
	}
}

// #107, #109: the storm's lags and handshake counts restart at each mark; a
// negative lag (a wall-clock step) counts as 0.
func TestRecorderCountsTheStormSinceMark(t *testing.T) {
	r := NewRecorder(2, []string{"gw-0"})
	r.AttemptLag(5 * time.Second) // the ramp's: before any mark
	r.ConnectLag(5 * time.Second)
	r.SendLag(5 * time.Second)
	r.ReplyLag(5 * time.Second)
	r.Handshake(HandshakeInline)
	if _, err := r.Mark("death", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.AttemptLag(120 * time.Millisecond)
	r.AttemptLag(-time.Millisecond)
	r.ConnectLag(9 * time.Millisecond)
	r.ConnectLag(-time.Millisecond)
	r.SendLag(7 * time.Millisecond)
	r.ReplyLag(3 * time.Millisecond)
	r.Handshake(HandshakePrecomputed)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeReused)
	s := r.Counts().StormSinceMark
	if s.Mark != "death" || s.AttemptLag.N != 2 || s.AttemptLag.Max != 120 ||
		s.ConnectLag.N != 2 || s.ConnectLag.Max != 9 || s.SendLag.N != 1 || s.SendLag.Max != 7 ||
		s.ReplyLag.N != 1 || s.ReplyLag.Max != 3 ||
		s.HandshakesPrecomputed != 1 || s.HandshakesInline != 2 || s.HandshakesReused != 1 {
		t.Fatalf("storm since mark %+v", s)
	}
}
