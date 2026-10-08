package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Admission reject reasons (spec: Observability). The last three are the
// TCP transport's, passed to Reject before the Noise handshake.
const (
	RejectAtCapacity       = "at_capacity"
	RejectPendingMem       = "pending_mem"
	RejectDraining         = "draining"
	RejectLoginQueue       = "login_queue"
	RejectHandshakeTimeout = "handshake_timeout"
	RejectAuthTimeout      = "auth_timeout"
	RejectHandshakeRate    = "handshake_rate"
	RejectBadVersion       = "bad_version"
	RejectNoiseUnknownKey  = "noise_unknown_key"
)

// Metrics are the gateway core's Prometheus metrics (spec: Observability).
// Connection skew (max / mean ONLINE per gateway) is computed in Prometheus
// from Conns{state="online"}.
type Metrics struct {
	Conns          *prometheus.GaugeVec   // state: handshake | auth_pending | online
	AwaitingLogout prometheus.Gauge       // closed connections still tracked
	Tracked        prometheus.Gauge       // every table entry
	Rejects        *prometheus.CounterVec // reason
	Closes         *prometheus.CounterVec // reason: client | heartbeat | kick | not_owner | logout | protocol | drain

	Logins         *prometheus.CounterVec // result
	LoginQueue     prometheus.Gauge       // Logins waiting for or inside a Login RPC
	LoginSeconds   prometheus.Histogram   // LoginResp sent − recv_ms
	ReloginSeconds *prometheus.HistogramVec

	Logouts *prometheus.CounterVec // result: done | retried | dropped
	Kicks   *prometheus.CounterVec // result: closed | pending | ignored

	LeaseCalls *prometheus.CounterVec // result: live | missing | error

	Passes            *prometheus.CounterVec // pace, result: done | cancelled | incomplete
	PassSeconds       *prometheus.HistogramVec
	ReconcileEntries  *prometheus.CounterVec // result: written | unchanged | refused | not_owner | no_reply
	ReconcileNotOwner *prometheus.CounterVec // reason
	Requeued          prometheus.Counter
	RequeueLimit      prometheus.Counter
	ReconcileEnds     *prometheus.CounterVec // result: ok | retried | dropped
	EndDeleted        prometheus.Counter
	EndTracked        prometheus.Gauge // tracked_conn_ids of the last ReconcileEnd

	Limits  *prometheus.GaugeVec // limit: nofile | nf_conntrack_max
	Ready   prometheus.Gauge
	RPCSecs *prometheus.HistogramVec // method, code
}

// NewMetrics registers every metric on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "goim_gateway"
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
		Conns:          gv("connections", "Open connections by state.", "state"),
		AwaitingLogout: g("awaiting_logout", "Closed connections still tracked: their fenced Logout is pending."),
		Tracked:        g("tracked", "Connection table entries (the tracked set)."),
		Rejects:        cv("admission_rejects_total", "Connections rejected or closed by admission control.", "reason"),
		Closes:         cv("closes_total", "Connections the gateway closed, by reason.", "reason"),

		Logins:       cv("logins_total", "Logins by result, as the client sees it.", "result"),
		LoginQueue:   g("login_queue", "Logins waiting for or inside a Login RPC."),
		LoginSeconds: h("login_seconds", "LoginResp sent minus recv_ms.", lat),
		ReloginSeconds: hv("relogin_seconds", "offline_ms + (login OK − recv_ms).",
			prometheus.LinearBuckets(0, 2.5, 25), "reconnect_reason"),

		Logouts: cv("logouts_total", "Fenced Logout entries by result.", "result"),
		Kicks:   cv("kicks_total", "Kick RPCs received, by what they did.", "result"),

		LeaseCalls: cv("lease_calls_total", "GatewayLease calls by result.", "result"),

		Passes: cv("reconcile_passes_total", "Reconcile passes by pace and result.", "pace", "result"),
		PassSeconds: hv("reconcile_pass_seconds", "Reconcile pass duration (cycle), by pace.",
			prometheus.ExponentialBuckets(1, 2, 12), "pace"),
		ReconcileEntries:  cv("reconcile_entries_total", "Reconcile entries by result.", "result"),
		ReconcileNotOwner: cv("reconcile_not_owner_total", "not_owner entries by reason.", "reason"),
		Requeued:          c("reconcile_requeued_total", "refused entries put back on the pass queue."),
		RequeueLimit:      c("reconcile_requeue_limit_total", "refused entries dropped at the re-queue limit."),
		ReconcileEnds:     cv("reconcile_end_total", "ReconcileEnd attempts by result.", "result"),
		EndDeleted:        c("reconcile_end_deleted_total", "Fields deleted by this gateway's ReconcileEnds."),
		EndTracked:        g("reconcile_end_tracked", "tracked_conn_ids in the last ReconcileEnd."),

		Limits:  gv("limit", "Limits read by the startup self-check.", "limit"),
		Ready:   g("ready", "1 while the gateway reports Ready."),
		RPCSecs: hv("rpc_seconds", "Gateway → Logic RPC latency.", lat, "method", "code"),
	}
}
