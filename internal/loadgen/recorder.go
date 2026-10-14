package loadgen

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Recorder keeps per-client times and the run's counters (choice #83). Every
// method takes one mutex. Logins arrive at most at the login rate (20k/s)
// and Pongs at clients ÷ PingEvery (60k/s at 600k every 10s), both well
// below what a mutex serves; callers pass the times in, so a wait for the
// lock never shows in a sample (P6a2aI-4).
type Recorder struct {
	mu             sync.Mutex
	ids            []string
	clients        []clientState
	online         []int
	logins         int64
	login          *hist
	lost           map[string]int64
	failed         map[string]int64
	backoff        map[string]int64 // waits by kind
	backoffClients int64
	since          BackoffCount // since the latest mark (P6b2a-stressI-1)
	pongs          int64
	missing        int64
	gapMax         time.Duration
	pongRTT        *hist
	marks          []*mark // by index
	byName         map[string]*mark
}

// clientState: gw is the gateway it is logged in on (-1: none); mark and
// pos place it in its latest mark (-1: none); backoff: it has reached backoff;
// backoffMarks: the mark count + 1 when it was last counted in
// Recorder.since (0: never).
type clientState struct {
	gw, mark, pos int32
	backoffMarks  int32
	backoff       bool
}

// mark is a named instant and the clients logged in on its gateway then
// (the population of an SLO), with when each lost its connection and
// logged in again after it (0: not yet). reassigned counts members a later
// mark took over before they were back.
type mark struct {
	gw         int
	at         int64
	members    []int32
	lost, back []int64
	reassigned int
}

// NewRecorder records clients 0..n-1 over gateways ids.
func NewRecorder(n int, ids []string) *Recorder {
	r := &Recorder{ids: ids, clients: make([]clientState, n), online: make([]int, len(ids)),
		login: newHist(), pongRTT: newHist(), lost: map[string]int64{}, failed: map[string]int64{}, backoff: map[string]int64{},
		since: BackoffCount{Waits: map[string]int64{}}, byName: map[string]*mark{}}
	for i := range r.clients {
		r.clients[i] = clientState{gw: -1, mark: -1}
	}
	return r
}

func (r *Recorder) markOf(c clientState) *mark {
	if c.mark < 0 {
		return nil
	}
	return r.marks[c.mark]
}

// Online: client i logged in on gateway gw; its attempt started at start.
func (r *Recorder) Online(i, gw int, start, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &r.clients[i]
	if c.gw >= 0 {
		r.online[c.gw]--
	}
	c.gw = int32(gw)
	r.online[gw]++
	r.logins++
	r.login.add(now.Sub(start).Milliseconds())
	if m := r.markOf(*c); m != nil && m.back[c.pos] == 0 && m.lost[c.pos] != 0 {
		m.back[c.pos] = now.UnixMilli()
	}
}

// Lost: client i's logged-in connection ended, for why (an sdk.EndKind name,
// or what the light client saw).
func (r *Recorder) Lost(i int, why string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &r.clients[i]
	if c.gw < 0 {
		return
	}
	r.online[c.gw]--
	c.gw = -1
	r.lost[why]++
	if m := r.markOf(*c); m != nil && m.lost[c.pos] == 0 {
		m.lost[c.pos] = now.UnixMilli()
	}
}

// Failed: an attempt ended before logging in, for why.
func (r *Recorder) Failed(why string) {
	r.mu.Lock()
	r.failed[why]++
	r.mu.Unlock()
}

// Backoff: client i's next attempt waits by exponential backoff, of kind
// (an sdk.WaitKind name: backoff or failure; P6a2bI-2).
func (r *Recorder) Backoff(i int, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backoff[kind]++
	r.since.Waits[kind]++
	c := &r.clients[i]
	if !c.backoff {
		c.backoff = true
		r.backoffClients++
	}
	if e := int32(len(r.marks)) + 1; c.backoffMarks != e {
		c.backoffMarks = e
		r.since.Clients++
	}
}

// Pong: a Ping was answered after rtt.
func (r *Recorder) Pong(rtt time.Duration) {
	r.mu.Lock()
	r.pongs++
	r.pongRTT.add(rtt.Milliseconds())
	r.mu.Unlock()
}

// PongMissing: a Ping was due while the previous one had no Pong yet.
func (r *Recorder) PongMissing() {
	r.mu.Lock()
	r.missing++
	r.mu.Unlock()
}

// PingGap: a client's Ping went out gap after its previous one (or after
// it was added); the longest is kept.
func (r *Recorder) PingGap(gap time.Duration) {
	r.mu.Lock()
	r.gapMax = max(r.gapMax, gap)
	r.mu.Unlock()
}

// Mark records instant now as name, with the clients logged in on gateway
// gw (-1: on any gateway) as its population, and returns its size. A client
// belongs to its latest mark only. Names are unique. gw -1 is only for an
// event that ends every client's connection: a member that never loses it
// is never back and fails the SLO (P6a2aI-2).
func (r *Recorder) Mark(name string, gw int, now time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		return 0, fmt.Errorf("loadgen: mark %q exists", name)
	}
	idx, m := int32(len(r.marks)), &mark{gw: gw, at: now.UnixMilli()}
	for i := range r.clients {
		c := &r.clients[i]
		if c.gw >= 0 && (gw < 0 || int(c.gw) == gw) {
			if old := r.markOf(*c); old != nil && old.back[c.pos] == 0 {
				old.reassigned++
			}
			c.mark, c.pos = idx, int32(len(m.members))
			m.members = append(m.members, int32(i))
		}
	}
	m.lost, m.back = make([]int64, len(m.members)), make([]int64, len(m.members))
	r.marks = append(r.marks, m)
	r.byName[name] = m
	r.since = BackoffCount{Mark: name, Waits: map[string]int64{}}
	return len(m.members), nil
}

// SLO is the storm distribution after a mark (spec: Load tests; loadgen
// computes it directly). From the mark: b1's "logged in again within 30s of
// T_death". From detection: b2's "within 30s of the client detecting the
// loss". A member that never logged in again fails both, and so does one a
// later mark took over before it was back (Reassigned: the later mark's
// SLO has it).
type SLO struct {
	Mark              string `json:"mark"`
	Gateway           string `json:"gateway"`
	AtMs              int64  `json:"at_ms"`
	WithinMs          int64  `json:"within_ms"`
	Population        int    `json:"population"`
	Lost              int    `json:"lost"`
	Back              int    `json:"back"`
	Reassigned        int    `json:"reassigned"`
	FromMark          Dist   `json:"from_mark"`
	Detection         Dist   `json:"detection"`
	FromDetection     Dist   `json:"from_detection"`
	PassFromMark      bool   `json:"pass_from_mark"`
	PassFromDetection bool   `json:"pass_from_detection"`
}

// SLO computes mark name's distributions; pass means p99 ≤ within.
func (r *Recorder) SLO(name string, within time.Duration) (SLO, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.byName[name]
	if !ok {
		return SLO{}, fmt.Errorf("loadgen: no mark %q", name)
	}
	s := SLO{Mark: name, Gateway: "all", AtMs: m.at, WithinMs: within.Milliseconds(), Population: len(m.members), Reassigned: m.reassigned}
	if m.gw >= 0 {
		s.Gateway = r.ids[m.gw]
	}
	fromMark, detect, fromDetect := newHist(), newHist(), newHist()
	for k := range m.members {
		if m.lost[k] != 0 {
			s.Lost++
			detect.add(m.lost[k] - m.at)
		}
		if m.back[k] != 0 {
			s.Back++
			fromMark.add(m.back[k] - m.at)
			fromDetect.add(m.back[k] - m.lost[k])
		}
	}
	n := int64(len(m.members))
	s.FromMark, s.Detection, s.FromDetection = fromMark.dist(n), detect.dist(n), fromDetect.dist(n)
	s.PassFromMark = n > 0 && s.FromMark.Within(s.WithinMs)
	s.PassFromDetection = n > 0 && s.FromDetection.Within(s.WithinMs)
	return s, nil
}

// Marks are the names of every mark.
func (r *Recorder) Marks() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Counts are the run's counters now.
type Counts struct {
	Online           int              `json:"online"`
	OnlineByGateway  map[string]int   `json:"online_by_gateway"`
	Logins           int64            `json:"logins"`
	Lost             map[string]int64 `json:"lost"`
	Failed           map[string]int64 `json:"failed"`
	BackoffClients   int64            `json:"backoff_clients"` // clients that reached backoff, each once
	BackoffWaits     map[string]int64 `json:"backoff_waits"`   // backoff waits by kind
	BackoffSinceMark BackoffCount     `json:"backoff_since_mark"`
	LoginLatency     Dist             `json:"login_latency"`
	Pongs            int64            `json:"pongs"`
	PongMissing      int64            `json:"pong_missing"`
	PingGapMaxMs     int64            `json:"ping_gap_max_ms"`
	PongRTT          Dist             `json:"pong_rtt"`
}

// BackoffCount is the backoff since mark Mark ("": the run's start, before
// any mark): each client once, every wait by kind. A storm's own count, apart
// from the clients that backed off in the ramp before it (P6b2a-stressI-1).
type BackoffCount struct {
	Mark    string           `json:"mark"`
	Clients int64            `json:"clients"`
	Waits   map[string]int64 `json:"waits"`
}

// Counts returns a snapshot of the counters.
func (r *Recorder) Counts() Counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := Counts{OnlineByGateway: map[string]int{}, Logins: r.logins, Lost: map[string]int64{}, Failed: map[string]int64{},
		BackoffClients: r.backoffClients, BackoffWaits: map[string]int64{},
		BackoffSinceMark: BackoffCount{Mark: r.since.Mark, Clients: r.since.Clients, Waits: map[string]int64{}},
		LoginLatency:     r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(), PongRTT: r.pongRTT.dist(r.pongRTT.n)}
	for i, n := range r.online {
		c.Online += n
		c.OnlineByGateway[r.ids[i]] = n
	}
	for k, v := range r.lost {
		c.Lost[k] = v
	}
	for k, v := range r.failed {
		c.Failed[k] = v
	}
	for k, v := range r.backoff {
		c.BackoffWaits[k] = v
	}
	for k, v := range r.since.Waits {
		c.BackoffSinceMark.Waits[k] = v
	}
	return c
}
