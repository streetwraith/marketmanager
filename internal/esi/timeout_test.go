package esi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A connection that will not open is dead, and must fail on the connect budget
// rather than waiting out the far more generous request budget.
func TestConnectTimeoutIsIndependentOfRequestTimeout(t *testing.T) {
	c := New(Options{
		// 203.0.113.0/24 is TEST-NET-3: reserved, routable nowhere, so the dial
		// hangs rather than being refused.
		BaseURL:        "http://203.0.113.1:81",
		UserAgent:      "t",
		ConnectTimeout: 300 * time.Millisecond,
		RequestTimeout: 30 * time.Second,
		RPS:            1000,
		Concurrency:    4,
	})

	start := time.Now()
	_, err := c.OrdersPage(context.Background(), 10000002, 1)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a dial failure")
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v; the connect timeout did not apply and it waited on the request timeout", elapsed)
	}
}

// A slow response is not a dead one. It must be bounded by the request timeout,
// not the much shorter connect budget.
func TestRequestTimeoutCoversASlowBody(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Pages", "1")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // headers sent, body stalls
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	c := New(Options{
		BaseURL: srv.URL, UserAgent: "t",
		ConnectTimeout: 5 * time.Second,
		RequestTimeout: 400 * time.Millisecond,
		RPS:            1000, Concurrency: 4,
	})

	start := time.Now()
	_, err := c.OrdersPage(context.Background(), 10000002, 1)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout on a stalled body")
	}
	if elapsed < 300*time.Millisecond {
		t.Errorf("failed after %v; the connect timeout cut short a healthy connection", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v; the request timeout did not apply", elapsed)
	}
	// A timeout must never leak the response body into the error text.
	if strings.Contains(err.Error(), "UNIQUE") {
		t.Error("error leaks body content")
	}
}
