package racetest

import (
	"fmt"
	"time"

	"github.com/Tallinzp/goim/internal/fault"
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
