package esi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, h http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return New(Options{
		BaseURL:           srv.URL,
		UserAgent:         "marketmanager-test/1.0 (test@example.com)",
		CompatibilityDate: "2026-08-04", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS:         1000, // effectively unlimited for tests
		Concurrency: 8,
	})
}

func TestOrdersPageParsesHeaders(t *testing.T) {
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Pages", "413")
		w.Header().Set("X-Ratelimit-Remaining", "11998")
		w.Header().Set("Expires", "Mon, 10 Aug 2026 12:55:23 GMT")
		w.Header().Set("Last-Modified", "Mon, 10 Aug 2026 12:50:23 GMT")
		_, _ = w.Write([]byte(`[{"order_id":1,"type_id":34,"location_id":60003760,"system_id":30000142,
			"is_buy_order":false,"price":5.5,"volume_remain":10,"volume_total":20,"min_volume":1,
			"duration":90,"range":"region","issued":"2026-06-12T14:58:08Z"}]`))
	}))

	p, err := c.OrdersPage(context.Background(), 10000002, 1)
	if err != nil {
		t.Fatalf("OrdersPage: %v", err)
	}
	if p.Pages != 413 {
		t.Errorf("Pages = %d, want 413", p.Pages)
	}
	if p.Expires.Sub(p.LastModified) != 5*time.Minute {
		t.Errorf("Expires - LastModified = %v, want 5m", p.Expires.Sub(p.LastModified))
	}
	if len(p.Orders) != 1 {
		t.Fatalf("got %d orders, want 1", len(p.Orders))
	}
	o := p.Orders[0]
	if o.OrderID != 1 || o.TypeID != 34 || o.Range != "region" {
		t.Errorf("order decoded wrong: %+v", o)
	}
	// Price is carried as raw JSON text so it reaches numeric(20,2) undamaged.
	if o.Price.String() != "5.5" {
		t.Errorf("Price = %q, want the literal 5.5 preserved", o.Price.String())
	}
	if o.Issued.UTC().Format(time.RFC3339) != "2026-06-12T14:58:08Z" {
		t.Errorf("Issued = %v", o.Issued)
	}
	// The budget must learn from every response.
	if got, ok := c.Budget.Remaining(); !ok || got != 11998 {
		t.Errorf("Budget.Remaining() = %d, %v; want 11998, true", got, ok)
	}
}

func TestSendsRequiredHeaders(t *testing.T) {
	var gotUA, gotCompat, gotAcceptEncoding string
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		gotCompat = r.Header.Get("X-Compatibility-Date")
		gotAcceptEncoding = r.Header.Get("Accept-Encoding")
		_, _ = w.Write([]byte(`[]`))
	}))
	if _, err := c.OrdersPage(context.Background(), 10000002, 1); err != nil {
		t.Fatalf("OrdersPage: %v", err)
	}
	if gotUA != "marketmanager-test/1.0 (test@example.com)" {
		t.Errorf("User-Agent = %q", gotUA)
	}
	if gotCompat != "2026-08-04" {
		t.Errorf("X-Compatibility-Date = %q", gotCompat)
	}
	// Left to the transport, so responses are decompressed transparently.
	if gotAcceptEncoding != "gzip" {
		t.Errorf("Accept-Encoding = %q, want gzip from the transport", gotAcceptEncoding)
	}
}

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		headers       map[string]string
		wantRateLimit bool
		wantErrLimit  bool
		wantServerErr bool
		wantRetry     time.Duration
	}{
		{"429 carries Retry-After", http.StatusTooManyRequests,
			map[string]string{"Retry-After": "17"}, true, false, false, 17 * time.Second},
		{"420 is the legacy error limit", StatusErrorLimited,
			map[string]string{"X-ESI-Error-Limit-Reset": "43"}, false, true, false, 0},
		{"503 is a server error", http.StatusServiceUnavailable, nil, false, false, true, 0},
		{"404 is none of them", http.StatusNotFound, nil, false, false, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
			}))
			_, err := c.OrdersPage(context.Background(), 10000002, 1)
			if err == nil {
				t.Fatal("expected an error")
			}
			if got := IsRateLimited(err); got != tc.wantRateLimit {
				t.Errorf("IsRateLimited = %v, want %v", got, tc.wantRateLimit)
			}
			if got := IsErrorLimited(err); got != tc.wantErrLimit {
				t.Errorf("IsErrorLimited = %v, want %v", got, tc.wantErrLimit)
			}
			if got := IsServerError(err); got != tc.wantServerErr {
				t.Errorf("IsServerError = %v, want %v", got, tc.wantServerErr)
			}
			var he *HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("error is not an *HTTPError: %v", err)
			}
			if he.RetryAfter != tc.wantRetry {
				t.Errorf("RetryAfter = %v, want %v", he.RetryAfter, tc.wantRetry)
			}
		})
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	var inFlight, peak int32
	c := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&peak)
			if n <= old || atomic.CompareAndSwapInt32(&peak, old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		_, _ = w.Write([]byte(`[]`))
	}))

	done := make(chan struct{})
	for i := range 40 {
		go func() {
			defer func() { done <- struct{}{} }()
			_, _ = c.OrdersPage(context.Background(), 10000002, i+1)
		}()
	}
	for range 40 {
		<-done
	}
	if p := atomic.LoadInt32(&peak); p > 8 {
		t.Errorf("peak in-flight = %d, want at most the configured 8", p)
	}
}

func TestRateLimitIsEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)
	c := New(Options{BaseURL: srv.URL, UserAgent: "t", CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS: 20, Concurrency: 8})

	start := time.Now()
	for i := range 10 {
		if _, err := c.OrdersPage(context.Background(), 10000002, i+1); err != nil {
			t.Fatalf("OrdersPage: %v", err)
		}
	}
	// 10 requests at 20/s cannot finish faster than ~450ms with a burst of 1.
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Errorf("10 requests at 20 rps took %v, want at least 400ms", elapsed)
	}
}

func TestTokenCost(t *testing.T) {
	tests := []struct {
		status int
		want   int
	}{
		{200, 2}, {204, 2}, {304, 1}, {301, 1}, {404, 5}, {429, 5}, {420, 5},
		{500, 0}, {503, 0},
	}
	for _, tc := range tests {
		if got := TokenCost(tc.status); got != tc.want {
			t.Errorf("TokenCost(%d) = %d, want %d", tc.status, got, tc.want)
		}
	}
}
