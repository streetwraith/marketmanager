package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// healthProbe is what the container HEALTHCHECK runs: the distroless image has
// no shell and no curl, so a wrong exit code silently breaks orchestration.
func TestHealthProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}

	t.Run("healthy server exits 0", func(t *testing.T) {
		t.Setenv("HTTP_ADDR", ":"+u.Port())
		if got := healthProbe(); got != 0 {
			t.Errorf("healthProbe() = %d, want 0", got)
		}
	})

	t.Run("nothing listening exits 1", func(t *testing.T) {
		// Port 1 is reserved and never served by this test.
		t.Setenv("HTTP_ADDR", ":1")
		if got := healthProbe(); got != 1 {
			t.Errorf("healthProbe() = %d, want 1", got)
		}
	})
}

// HTTP_ADDR is operator input. SplitHostPort accepts a named service, so the
// port is validated before it reaches a URL.
func TestHealthProbeRejectsBadAddresses(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{"named service rather than a number", ":notaport"},
		{"no port at all", "localhost"},
		{"port out of range", ":70000"},
		{"negative port", ":-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HTTP_ADDR", tc.addr)
			if got := healthProbe(); got != 1 {
				t.Errorf("healthProbe() with HTTP_ADDR=%q = %d, want 1", tc.addr, got)
			}
		})
	}
}

// A server that answers but is unhealthy must fail the probe, or a broken
// instance keeps receiving traffic.
func TestHealthProbeFailsOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	t.Setenv("HTTP_ADDR", ":"+u.Port())
	if got := healthProbe(); got != 1 {
		t.Errorf("healthProbe() = %d against a 503, want 1", got)
	}
}
