package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"marketmanager/internal/sentrytest"
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

// capturePanic has two halves: report the panic, and let it carry on killing the
// process. Swallowing it would leave a dead goroutine behind a /healthz that
// still passes, because the check only pings Postgres.
func TestCapturePanicReportsAndRepanics(t *testing.T) {
	sink := sentrytest.Capture(t)

	var escaped any
	func() {
		defer func() { escaped = recover() }()
		defer capturePanic("history")
		panic("history importer exploded")
	}()

	if escaped == nil {
		t.Fatal("capturePanic swallowed the panic; the process must still die")
	}
	events := sink.Captured()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].Tags["component"] != "history" {
		t.Errorf("event tags = %v, want component=history", events[0].Tags)
	}
}

// A goroutine that returns normally must report nothing.
func TestCapturePanicIsQuietWithoutAPanic(t *testing.T) {
	sink := sentrytest.Capture(t)
	func() { defer capturePanic("scheduler") }()
	if got := len(sink.Captured()); got != 0 {
		t.Fatalf("events = %d, want 0", got)
	}
}

// A release that never changes looks valid and silently makes every regression
// undetectable, which is worse than sending none at all.
func TestSentryRelease(t *testing.T) {
	tests := []struct {
		name   string
		commit string
		want   string
	}{
		{"a git-built container carries the real SHA", "c6f6815", "c6f6815"},
		{"a Docker-image app carries the literal HEAD", "HEAD", ""},
		{"an unset variable leaves the release empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SOURCE_COMMIT", tc.commit)
			if got := sentryRelease(); got != tc.want {
				t.Errorf("sentryRelease() = %q, want %q", got, tc.want)
			}
		})
	}
}

// A DSN is a write credential for the error store, and sentry quotes the whole
// DSN back inside its own parse error. Logging that error would put the
// credential in stdout, where anyone with container log access can read it and
// then forge or flood events to bury a real incident.
func TestSentryInitNeverLogsTheDsn(t *testing.T) {
	const marker = "UNIQUE_DSN_MARKER_d41d8cd9"
	// The control character makes url.Parse reject the DSN, so Init returns the
	// error that quotes it. Init binds no client when it fails, so the global hub
	// is left alone.
	t.Setenv("BUGSINK_DSN", "https://"+marker+"@errors.example.invalid/\x7f1")

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	initSentry()

	// Without this the test would still pass if the failure branch stopped
	// logging, or stopped being reached at all.
	if !strings.Contains(logs.String(), "sentry init failed") {
		t.Fatalf("the init failure was not logged; got %q", logs.String())
	}
	if strings.Contains(logs.String(), marker) {
		t.Errorf("the DSN reached the log: %s", logs.String())
	}
}
