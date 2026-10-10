// Package pprofhttp serves net/http/pprof on an admin mux (Plan 6a2, choice
// #85), so load runs can save CPU, heap and goroutine profiles as artifacts
// (spec: Automation contract). The admin port is pod-internal: never behind
// the NLB.
package pprofhttp

import (
	"net/http"
	"net/http/pprof"
)

// Register adds /debug/pprof/ and its handlers to mux.
func Register(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
