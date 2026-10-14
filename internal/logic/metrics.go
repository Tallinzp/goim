package logic

import (
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/prometheus/client_golang/prometheus"
)

// SkewAlertMs is the clock_skew_ms alert threshold: half the 1s skew budget (R4M-5).
const SkewAlertMs = 500

// Metrics are Logic's Prometheus metrics (spec: Observability, Logic side).
type Metrics struct {
	Logins           *prometheus.CounterVec // result
	ConnSeqRegressed prometheus.Counter
	Undecodable      *prometheus.CounterVec // op: stored entries that did not decode (PI-3)
	DeadEpochRelogin prometheus.Histogram   // login_ms (or a refused Kick's Kick() call) − dead epoch's last renewal (R1-O7)
	StoreSeconds     *prometheus.HistogramVec
	RPCSeconds       *prometheus.HistogramVec // method, code
	Logouts          *prometheus.CounterVec   // result
	ConnsAddFailures prometheus.Counter       // Login follow-up SADD gwconns failed

	LeaseRenewals      *prometheus.CounterVec // result: live | missing | error
	LeaseIndexFailures prometheus.Counter     // ZADD gwepochs failed after a renewal
	ClockSkewMs        *prometheus.GaugeVec   // gw
	ClockSkewAlert     *prometheus.GaugeVec   // gw: 1 while |skew| > SkewAlertMs

	KickAttempts       *prometheus.CounterVec // attempt: first | retry; result: ok | refused | timeout | error (#97)
	KickGiveUps        *prometheus.CounterVec // target_lease
	KickDropped        *prometheus.CounterVec // target_lease (R5S-O2)
	KickQueueDepth     *prometheus.GaugeVec   // target gw:epoch
	KickAttemptSeconds prometheus.Histogram

	ReconcileEntries    *prometheus.CounterVec // result, reason
	ReconcileEndDeleted prometheus.Counter
	ReconcileEndTracked prometheus.Histogram
	ReconcileEndSeconds *prometheus.HistogramVec // pace (R5S-O8)
	ReconcileEndScanned *prometheus.HistogramVec // pace

	Revokes         *prometheus.CounterVec // scope, result
	RevocationKicks prometheus.Counter

	Sweeps               *prometheus.CounterVec // result
	SweepUIDs            prometheus.Counter
	SweepDeleted         prometheus.Counter
	GuardHolding         prometheus.Gauge // 1 while the mass-stale guard holds back a sweep (alert)
	GuardHolds           prometheus.Counter
	GuardRenewingSeconds prometheus.Gauge // time since renewing_since; 0 while not renewing

	JWKSRefreshFailures     prometheus.Counter
	JWKSUnknownKidRefreshes prometheus.Counter
	JWKSFallback            prometheus.Gauge // 1 while on jwks-fallback (alert)
}

// NewMetrics registers every metric on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "goim_logic"
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
		reg.MustRegister(v)
		return v
	}
	c := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	gv := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help}, labels)
		reg.MustRegister(v)
		return v
	}
	g := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	hv := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets}, labels)
		reg.MustRegister(v)
		return v
	}
	h := func(name, help string, buckets []float64) prometheus.Histogram {
		v := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets})
		reg.MustRegister(v)
		return v
	}
	lat := prometheus.ExponentialBuckets(0.0005, 2, 16) // 0.5ms … ~16s
	return &Metrics{
		Logins:           cv("logins_total", "Logins by result.", "result"),
		ConnSeqRegressed: c("conn_seq_regressed_total", "Logins accepted as a device counter regression."),
		Undecodable:      cv("undecodable_entries_total", "Stored sess values that did not decode, by operation.", "op"),
		DeadEpochRelogin: h("dead_epoch_relogin_seconds", "Relogin login_ms, or the Kick() call of its first Kick refused while the lease was live, minus the dead epoch's last lease renewal.",
			prometheus.LinearBuckets(0, 5, 24)),
		StoreSeconds:     hv("store_seconds", "Session store call latency.", lat, "op"),
		RPCSeconds:       hv("rpc_seconds", "gRPC handler latency.", lat, "method", "code"),
		Logouts:          cv("logouts_total", "Logout entries by result.", "result"),
		ConnsAddFailures: c("gwconns_add_failures_total", "Login follow-up SADD to gwconns failed."),

		LeaseRenewals:      cv("lease_renewals_total", "GatewayLease calls by result.", "result"),
		LeaseIndexFailures: c("lease_index_failures_total", "ZADD gwepochs failed after a lease renewal."),
		ClockSkewMs:        gv("clock_skew_ms", "Redis TIME minus the gateway's clock at its last lease call.", "gw"),
		ClockSkewAlert:     gv("clock_skew_alert", "1 while |clock_skew_ms| exceeds 500.", "gw"),

		KickAttempts:       cv("kick_attempts_total", "Kick attempts.", "attempt", "result"),
		KickGiveUps:        cv("kick_giveups_total", "Kicks given up after their attempts.", "target_lease"),
		KickDropped:        cv("kick_dropped_total", "Kick attempts dropped by a full per-target queue.", "target_lease"),
		KickQueueDepth:     gv("kick_queue_depth", "Queued Kick attempts per target.", "target"),
		KickAttemptSeconds: h("kick_attempt_seconds", "Kick attempt latency, connect included.", lat),

		ReconcileEntries:    cv("reconcile_entries_total", "Reconcile entries by result.", "result", "reason"),
		ReconcileEndDeleted: c("reconcile_end_deleted_total", "Fields deleted by ReconcileEnd delete phases."),
		ReconcileEndTracked: h("reconcile_end_tracked", "tracked_conn_ids per ReconcileEnd.",
			prometheus.ExponentialBuckets(1000, 2, 10)),
		ReconcileEndSeconds: hv("reconcile_end_seconds", "ReconcileEnd delete phase duration.",
			prometheus.ExponentialBuckets(0.01, 2, 14), "pace"),
		ReconcileEndScanned: hv("reconcile_end_scanned_uids", "uids scanned per delete phase.",
			prometheus.ExponentialBuckets(1000, 2, 10), "pace"),

		Revokes:         cv("revokes_total", "Revoke calls.", "scope", "result"),
		RevocationKicks: c("revocation_kicks_total", "REVOKED Kicks queued by Revoke."),

		Sweeps:               cv("sweeps_total", "Sweep attempts by result.", "result"),
		SweepUIDs:            c("sweep_uids_total", "uids visited by the sweeper."),
		SweepDeleted:         c("sweep_deleted_total", "Fields deleted by the sweeper."),
		GuardHolding:         g("sweep_guard_holding", "1 while the mass-stale guard holds back a stale epoch."),
		GuardHolds:           c("sweep_guard_holds_total", "Sweeps held back by the mass-stale guard."),
		GuardRenewingSeconds: g("sweep_guard_renewing_seconds", "Seconds since renewing_since; 0 while not renewing."),

		JWKSRefreshFailures:     c("jwks_refresh_failures_total", "Failed JWKS fetches from the account service."),
		JWKSUnknownKidRefreshes: c("jwks_unknown_kid_refreshes_total", "JWKS refreshes started by an unknown kid."),
		JWKSFallback:            g("jwks_fallback", "1 while the key set in use came from jwks-fallback."),
	}
}

// AuthObserver wires the key manager's events to the metrics.
func (m *Metrics) AuthObserver() auth.Observer {
	return auth.Observer{
		RefreshFailed:     func(error) { m.JWKSRefreshFailures.Inc() },
		UnknownKidRefresh: func() { m.JWKSUnknownKidRefreshes.Inc() },
		Source: func(fallback bool) {
			if fallback {
				m.JWKSFallback.Set(1)
			} else {
				m.JWKSFallback.Set(0)
			}
		},
	}
}
