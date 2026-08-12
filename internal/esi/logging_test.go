package esi

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Logs must never carry an ESI response body. Order payloads are ~240 KB per
// page and 1,516 pages a cycle; leaking even a fragment would bury the signal
// and balloon log storage.
func TestNoResponseBodyInLogs(t *testing.T) {
	// A recognisable marker that only ever appears in the response payload.
	const marker = "UNIQUE_BODY_MARKER_d41d8cd9"

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"success", http.StatusOK, `[{"order_id":1,"range":"` + marker + `"}]`},
		{"rate limited", http.StatusTooManyRequests, `{"error":"` + marker + `"}`},
		{"error limited", StatusErrorLimited, `{"error":"` + marker + `"}`},
		{"server error", http.StatusInternalServerError, `{"error":"` + marker + `"}`},
		{"malformed json", http.StatusOK, `{{{` + marker},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			// Debug is the most verbose the service can be configured to run at.
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Ratelimit-Remaining", "11998")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			c := New(Options{BaseURL: srv.URL, UserAgent: "t", CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
				RPS: 1000, Concurrency: 4})
			_, err := c.OrdersPage(context.Background(), 10000002, 1)

			// The error itself is logged by callers, so it must not carry the body.
			if err != nil && strings.Contains(err.Error(), marker) {
				t.Errorf("error text leaks the response body: %v", err)
			}
			if got := logs.String(); strings.Contains(got, marker) {
				t.Errorf("logs leak the response body:\n%s", got)
			}
		})
	}
}

// A leak could also arrive through struct formatting rather than a log call.
func TestHTTPErrorDoesNotCarryBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`SENSITIVE_PAYLOAD`))
	}))
	t.Cleanup(srv.Close)

	c := New(Options{BaseURL: srv.URL, UserAgent: "t", CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS: 1000, Concurrency: 4})
	_, err := c.OrdersPage(context.Background(), 10000002, 1)
	if err == nil {
		t.Fatal("expected an error")
	}
	// %+v is what a careless logger or a panic dump would produce.
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if s := fmt.Sprintf(format, err); strings.Contains(s, "SENSITIVE_PAYLOAD") {
			t.Errorf("%s of the error leaks the body: %s", format, s)
		}
	}
	// The fields the service does need must still be there.
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error lost the status: %v", err)
	}
}
