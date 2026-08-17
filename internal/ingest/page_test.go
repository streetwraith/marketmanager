package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// flakyESI fails a specific page a set number of times, then serves it.
type flakyESI struct {
	pages     int
	failPage  int
	failTimes int
	status    int

	mu       sync.Mutex
	attempts map[int]int
}

func (f *flakyESI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}
	f.mu.Lock()
	if f.attempts == nil {
		f.attempts = map[int]int{}
	}
	f.attempts[page]++
	n := f.attempts[page]
	f.mu.Unlock()

	if page == f.failPage && n <= f.failTimes {
		w.Header().Set("X-Ratelimit-Remaining", "11000")
		w.WriteHeader(f.status)
		return
	}
	w.Header().Set("X-Pages", strconv.Itoa(f.pages))
	w.Header().Set("X-Ratelimit-Remaining", "11000")
	w.Header().Set("Last-Modified", "Mon, 10 Aug 2026 12:50:23 GMT")
	w.Header().Set("Expires", "Mon, 10 Aug 2026 12:55:23 GMT")
	_, _ = w.Write([]byte(`[{"order_id":1,"type_id":34,"location_id":1,"system_id":1,
		"is_buy_order":false,"price":1.5,"volume_remain":1,"volume_total":1,"min_volume":1,
		"duration":90,"range":"region","issued":"2026-06-12T14:58:08Z"}]`))
}

func flakyFetcher(t *testing.T, f *flakyESI) *Fetcher {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := esi.New(esi.Options{BaseURL: srv.URL, UserAgent: "t", CompatibilityDate: "d",
		ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS: 5000, Concurrency: 8})
	return NewFetcher(c, FetcherConfig{
		Reserve: 600, BudgetFloor: 300, PageAttempts: 3,
		ErrorLimitFloor: 30, PageBackoffUnit: time.Millisecond,
	})
}

func runSweep(t *testing.T, f *Fetcher) (*SweepMeta, error) {
	t.Helper()
	out := make(chan []esi.Order, 64)
	done := make(chan struct{})
	go func() {
		for range out { //nolint:revive // draining
		}
		close(done)
	}()
	meta, err := f.Sweep(context.Background(), region.Region{ID: 10000002, Priority: 2}, out)
	<-done
	return meta, err
}

// The point of the change: one transient page failure must not discard the whole
// sweep. Before, a single 503 on page 7 of 413 cost the entire region.
//
// The page fails exactly once. A second 5xx in a row would arm the outage gate,
// which suspends retries by design, and whether it lands in a row depends on how
// the other pages interleave.
func TestSweepRetriesASinglePageInsteadOfFailing(t *testing.T) {
	fake := &flakyESI{pages: 10, failPage: 7, failTimes: 1, status: http.StatusServiceUnavailable}
	meta, err := runSweep(t, flakyFetcher(t, fake))
	if err != nil {
		t.Fatalf("sweep failed despite a retryable page: %v", err)
	}
	if meta.Pages != 10 {
		t.Errorf("Pages = %d, want 10", meta.Pages)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.attempts[7]; got != 2 {
		t.Errorf("page 7 attempted %d times, want 2 (one failure then success)", got)
	}
	// Only the failing page is refetched; the other nine are fetched once.
	for p := 1; p <= 10; p++ {
		if p == 7 {
			continue
		}
		if got := fake.attempts[p]; got != 1 {
			t.Errorf("page %d attempted %d times, want 1", p, got)
		}
	}
	// 5xx costs 0 tokens, so the failure is free; 10 successes at 2 each.
	if meta.TokensSpent != 20 {
		t.Errorf("TokensSpent = %d, want 20", meta.TokensSpent)
	}
}

// Retries are bounded: a page that never recovers still fails the sweep.
//
// The page answers 429 rather than 5xx to keep this about the retry bound alone.
// A repeated 5xx arms the outage gate, which stops retrying for its own reason.
func TestSweepGivesUpAfterPageAttempts(t *testing.T) {
	fake := &flakyESI{pages: 6, failPage: 3, failTimes: 99, status: http.StatusTooManyRequests}
	_, err := runSweep(t, flakyFetcher(t, fake))
	if err == nil {
		t.Fatal("expected the sweep to fail when a page never recovers")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.attempts[3]; got != 3 {
		t.Errorf("page 3 attempted %d times, want exactly PageAttempts=3", got)
	}
}

// A 4xx will fail identically next time, so retrying it only wastes 5 tokens a go.
func TestSweepDoesNotRetryClientErrors(t *testing.T) {
	fake := &flakyESI{pages: 6, failPage: 4, failTimes: 99, status: http.StatusNotFound}
	_, err := runSweep(t, flakyFetcher(t, fake))
	if err == nil {
		t.Fatal("expected the sweep to fail on a 404")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.attempts[4]; got != 1 {
		t.Errorf("page 4 attempted %d times, want 1; a 404 is not retryable", got)
	}
}

func TestRetryablePage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"503", &esi.HTTPError{Status: 503}, true},
		{"429", &esi.HTTPError{Status: 429}, true},
		{"420 blocks the whole IP", &esi.HTTPError{Status: esi.StatusErrorLimited}, false},
		{"404", &esi.HTTPError{Status: 404}, false},
		{"400", &esi.HTTPError{Status: 400}, false},
		{"wrapped 503", errors.New("x"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryablePage(tc.err); got != tc.want {
				t.Errorf("retryablePage(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// While the outage gate is armed, a page must not be retried: those retries strike
// the shared error limit that the pause exists to protect.
//
// Page 1 fails, which ends the sweep before any other page is requested, so no
// success can clear the gate and the attempt count is deterministic.
func TestSweepDoesNotRetryWhileTheOutageGateIsArmed(t *testing.T) {
	fake := &flakyESI{pages: 6, failPage: 1, failTimes: 99, status: http.StatusServiceUnavailable}
	f := flakyFetcher(t, fake)
	f.client.Outage.Observe(503, 0)
	f.client.Outage.Observe(503, 0)

	if _, err := runSweep(t, f); err == nil {
		t.Fatal("expected the sweep to fail while the upstream is down")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.attempts[1]; got != 1 {
		t.Errorf("page 1 attempted %d times with the gate armed, want 1", got)
	}
}
