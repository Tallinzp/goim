package racetest

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
)

// The series the scenarios read, as the exposition prints them; %s are
// label values.
const (
	logicLogins       = `goim_logic_logins_total{result="%s"}`
	logicLogouts      = `goim_logic_logouts_total{result="%s"}`
	logicSweeps       = `goim_logic_sweeps_total{result="%s"}`
	logicGuardHolds   = `goim_logic_sweep_guard_holds_total`
	logicSweepDeleted = `goim_logic_sweep_deleted_total`
	logicRenewing     = `goim_logic_sweep_guard_renewing_seconds`
	logicKickAttempts = `goim_logic_kick_attempts_total{attempt="%s",result="%s"}`
	logicKickGiveUps  = `goim_logic_kick_giveups_total{target_lease="live"}`

	gwPasses     = `goim_gateway_reconcile_passes_total{pace="%s",result="%s"}`
	gwEnds       = `goim_gateway_reconcile_end_total{result="%s"}`
	gwRequeued   = `goim_gateway_reconcile_requeued_total`
	gwKicks      = `goim_gateway_kicks_total{result="%s"}`
	gwLogouts    = `goim_gateway_logouts_total{result="%s"}`
	gwLeaseCalls = `goim_gateway_lease_calls_total{result="%s"}`
	triggered    = "PACE_TRIGGERED"
	periodic     = "PACE_PERIODIC"
)

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }

// dur writes a fault rule's duration.
func dur(d time.Duration) fault.Duration { return fault.Duration(d) }

// passDone waits until gateway i has finished n triggered passes.
func (s *Stack) passDone(i, n int, d time.Duration) {
	s.t.Helper()
	eventually(s.t, sprintf("%s finished %d triggered passes", s.GW[i].ID, n), d, func() bool {
		return s.gwMetric(i, sprintf(gwPasses, triggered, "done")) >= float64(n)
	})
}

// endsOK waits until gateway i's ReconcileEnds have succeeded n times
// (each returns after Logic's delete phase).
func (s *Stack) endsOK(i, n int, d time.Duration) {
	s.t.Helper()
	eventually(s.t, sprintf("%s: %d ReconcileEnds ok", s.GW[i].ID, n), d, func() bool {
		return s.gwMetric(i, sprintf(gwEnds, "ok")) >= float64(n)
	})
}

// A series a scenario reads must name a metric Logic or the gateway
// registers, with its label names: a typo would read 0 and pass every
// "== 0" check. It needs no binaries: it runs at Tier 1a too.
func TestSeriesCheckRefusesUnregisteredSeries(t *testing.T) {
	for _, series := range []string{
		logicLogins, logicLogouts, logicSweeps, logicGuardHolds, logicSweepDeleted, logicRenewing,
		logicKickAttempts, logicKickGiveUps, gwPasses, gwEnds, gwRequeued, gwKicks, gwLogouts, gwLeaseCalls,
	} {
		if err := checkSeries(series); err != nil {
			t.Error(err)
		}
	}
	for _, series := range []string{
		`goim_gateway_kick_total{result="ignored"}`,         // a typo in the name
		`goim_gateway_kicks_total{reason="ignored"}`,        // a typo in a label name
		`goim_logic_sweep_guard_holds_total{result="done"}`, // labels on a metric that has none
		`goim_gateway_kicks_total`,                          // a labelled metric without its labels
	} {
		if checkSeries(series) == nil {
			t.Errorf("%s: accepted", series)
		}
	}
}

// checkSeries reports a series that names no metric Logic or the gateway
// registers, or names one with other label names. Label values can't be
// checked: an unused one is absent from /metrics as a misspelled one is.
func checkSeries(series string) error {
	name, labels, _ := strings.Cut(series, "{")
	var keys []string
	for _, pair := range strings.Split(strings.TrimSuffix(labels, "}"), ",") {
		if k, _, ok := strings.Cut(pair, "="); ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	want, ok := registered()[name]
	if !ok {
		return fmt.Errorf("%s: no such metric in Logic or the gateway", series)
	}
	if !slices.Equal(keys, want) {
		return fmt.Errorf("%s: labels %v, the metric has %v", series, keys, want)
	}
	return nil
}

// registered maps every metric Logic and the gateway register to its sorted
// label names.
var registered = sync.OnceValue(func() map[string][]string {
	r := descRecorder{}
	logic.NewMetrics(r)
	gateway.NewMetrics(r)
	return r
})

// descRecorder is a prometheus.Registerer that records each collector's
// metric names and label names.
type descRecorder map[string][]string

var descFields = regexp.MustCompile(`^Desc\{fqName: "([^"]+)".*variableLabels: \{([^}]*)\}\}$`)

func (r descRecorder) Register(c prometheus.Collector) error {
	ch := make(chan *prometheus.Desc)
	go func() { c.Describe(ch); close(ch) }()
	for d := range ch {
		m := descFields.FindStringSubmatch(d.String())
		if m == nil {
			panic("racetest: cannot parse " + d.String())
		}
		var labels []string
		if m[2] != "" {
			labels = strings.Split(m[2], ",")
		}
		slices.Sort(labels)
		r[m[1]] = labels
	}
	return nil
}

func (r descRecorder) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		r.Register(c)
	}
}

func (r descRecorder) Unregister(prometheus.Collector) bool { return false }
