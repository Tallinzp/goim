package pprofhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRegisterServesProfiles(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, body := get(t, srv.URL+"/debug/pprof/goroutine?debug=1"); code != http.StatusOK || !strings.Contains(body, "goroutine profile:") {
		t.Fatalf("goroutine: %d %.80q", code, body)
	}
	if code, body := get(t, srv.URL+"/debug/pprof/heap"); code != http.StatusOK || len(body) == 0 {
		t.Fatalf("heap: %d, %d bytes", code, len(body))
	}
	if code, body := get(t, srv.URL+"/debug/pprof/profile?seconds=1"); code != http.StatusOK || len(body) == 0 {
		t.Fatalf("cpu profile: %d, %d bytes", code, len(body))
	}
}
