package loadgen

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Tallinzp/goim/internal/pprofhttp"
)

// Result is what cmd/loadgen writes at exit: the final stats and the SLO of
// every mark.
type Result struct {
	Stats
	SLOs map[string]SLO `json:"slos"`
}

// Result is the final result, SLOs judged against within.
func (r *Runner) Result(within time.Duration) Result {
	res := Result{Stats: r.Stats(), SLOs: map[string]SLO{}}
	for _, name := range r.rec.Marks() {
		res.SLOs[name], _ = r.rec.SLO(name, within)
	}
	return res
}

// Handler is the control API (choice #84), for the scenario scripts:
//
//	GET  /stats                               Stats
//	POST /kill?gateway=gw-1&pid=N&mark=death  SIGKILL and mark T_death  {"mark", "population"}
//	                                          409: pid gone, mark taken or a flood mode, nothing changed; 500: ErrKillAfterMark
//	POST /mark?gateway=gw-1|all&mark=drop     a mark; &down=1 also takes the gateway out of the balancer
//	POST /up?gateway=gw-1                     the gateway is back in the balancer  {"gateway"}
//	                                          also after /kill: send it once the gateway listens again
//	POST /reset?mark=reset                    sdk: mark every gateway, then RST every connection  {"mark", "population", "close_ms"}
//	                                          409: not sdk mode or the mark taken, nothing changed
//	POST /network-change                      sdk: NetworkChanged on every client  {"clients", "ms"}; 409: not sdk mode
//	POST /ghost?mark=ghost                    sdk: mark every gateway, then every connection lost to its client
//	                                          and kept open, unread, until its gateway closes it  {"mark", "population", "ms"}
//	                                          409: not sdk mode or the mark taken, nothing changed; 500: ErrGhostKeeper
//	GET  /clients                             {"uid_prefix", "gateways", "gw": [client i's gateway index, -1: offline]}
//	GET  /slo?mark=death&within=30s           SLO
//	GET  /runtime                             Runtime: loadgen's GC, memory and scheduling counters
//	/debug/pprof/                             loadgen's own profiles
func (r *Runner) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, r.Stats()) })
	mux.HandleFunc("GET /runtime", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, ReadRuntime()) })
	mux.HandleFunc("POST /kill", func(w http.ResponseWriter, q *http.Request) {
		gw := r.env.Gateway(q.FormValue("gateway"))
		pid, err := strconv.Atoi(q.FormValue("pid"))
		if gw < 0 || err != nil || q.FormValue("mark") == "" {
			http.Error(w, "want gateway=<id>&pid=<n>&mark=<name>", http.StatusBadRequest)
			return
		}
		n, err := r.Kill(gw, pid, q.FormValue("mark"))
		if errors.Is(err, ErrKillAfterMark) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n})
	})
	mux.HandleFunc("POST /mark", func(w http.ResponseWriter, q *http.Request) {
		gw := -1
		if id := q.FormValue("gateway"); id != "all" {
			gw = r.env.Gateway(id)
		}
		if (gw < 0 && q.FormValue("gateway") != "all") || q.FormValue("mark") == "" {
			http.Error(w, "want gateway=<id>|all&mark=<name>", http.StatusBadRequest)
			return
		}
		n, err := r.Mark(gw, q.FormValue("mark"), q.FormValue("down") == "1")
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n})
	})
	mux.HandleFunc("POST /up", func(w http.ResponseWriter, q *http.Request) {
		gw := r.env.Gateway(q.FormValue("gateway"))
		if gw < 0 {
			http.Error(w, "want gateway=<id>", http.StatusBadRequest)
			return
		}
		r.Up(gw)
		writeJSON(w, map[string]any{"gateway": q.FormValue("gateway")})
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, q *http.Request) {
		if q.FormValue("mark") == "" {
			http.Error(w, "want mark=<name>", http.StatusBadRequest)
			return
		}
		n, took, err := r.Reset(q.FormValue("mark"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n, "close_ms": took.Milliseconds()})
	})
	mux.HandleFunc("POST /ghost", func(w http.ResponseWriter, q *http.Request) {
		if q.FormValue("mark") == "" {
			http.Error(w, "want mark=<name>", http.StatusBadRequest)
			return
		}
		n, took, err := r.Ghost(q.FormValue("mark"))
		if errors.Is(err, ErrGhostKeeper) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n, "ms": took.Milliseconds()})
	})
	mux.HandleFunc("GET /clients", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"uid_prefix": r.cfg.UIDPrefix, "gateways": r.env.GatewayIDs, "gw": r.rec.Gateways()})
	})
	mux.HandleFunc("POST /network-change", func(w http.ResponseWriter, _ *http.Request) {
		n, took, err := r.NetworkChange()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]any{"clients": n, "ms": took.Milliseconds()})
	})
	mux.HandleFunc("GET /slo", func(w http.ResponseWriter, q *http.Request) {
		within, err := time.ParseDuration(q.FormValue("within"))
		if err != nil || within <= 0 {
			http.Error(w, "want mark=<name>&within=<duration>", http.StatusBadRequest)
			return
		}
		s, err := r.rec.SLO(q.FormValue("mark"), within)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, s)
	})
	pprofhttp.Register(mux)
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
