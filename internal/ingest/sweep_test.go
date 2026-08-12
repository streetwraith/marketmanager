package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// fakeESI serves a multi-page region. lastModifiedFor lets a test make one page
// disagree with the rest, which is the case the consistency check exists for.
type fakeESI struct {
	pages           int
	ordersPerPage   int
	remaining       int
	lastModifiedFor func(page int) string
	pagesFor        func(page int) int
	statusFor       func(page int) int

	mu       sync.Mutex
	requests []int
}

func (f *fakeESI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}
	f.mu.Lock()
	f.requests = append(f.requests, page)
	f.mu.Unlock()

	if f.statusFor != nil {
		if s := f.statusFor(page); s != 0 && s != http.StatusOK {
			w.Header().Set("X-Ratelimit-Remaining", strconv.Itoa(f.remaining))
			w.WriteHeader(s)
			return
		}
	}

	pages := f.pages
	if f.pagesFor != nil {
		pages = f.pagesFor(page)
	}
	lm := "Mon, 10 Aug 2026 12:50:23 GMT"
	if f.lastModifiedFor != nil {
		lm = f.lastModifiedFor(page)
	}
	w.Header().Set("X-Pages", strconv.Itoa(pages))
	w.Header().Set("X-Ratelimit-Remaining", strconv.Itoa(f.remaining))
	w.Header().Set("Last-Modified", lm)
	w.Header().Set("Expires", "Mon, 10 Aug 2026 12:55:23 GMT")

	body := "["
	for i := range f.ordersPerPage {
		if i > 0 {
			body += ","
		}
		id := (page-1)*f.ordersPerPage + i + 1
		body += fmt.Sprintf(`{"order_id":%d,"type_id":34,"location_id":60003760,"system_id":30000142,
			"is_buy_order":false,"price":1.5,"volume_remain":1,"volume_total":1,"min_volume":1,
			"duration":90,"range":"region","issued":"2026-06-12T14:58:08Z"}`, id)
	}
	body += "]"
	_, _ = w.Write([]byte(body))
}

func newFetcher(t *testing.T, f *fakeESI) *Fetcher {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := esi.New(esi.Options{
		BaseURL: srv.URL, UserAgent: "t", CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS: 5000, Concurrency: 8,
	})
	// The production defaults.
	return NewFetcher(c, FetcherConfig{
		Reserve: 600, BudgetFloor: 300, PageAttempts: 3,
		ErrorLimitFloor: 30, PageBackoffUnit: time.Millisecond,
	})
}

// drain consumes what a sweep emits, standing in for the writer a real caller
// attaches. Without it the sweep would block on its output channel.
func drain(out chan []esi.Order) {
	go func() {
		for range out { //nolint:revive // draining
		}
	}()
}

func TestSweepCollectsEveryPage(t *testing.T) {
	f := &fakeESI{pages: 5, ordersPerPage: 10, remaining: 11998}
	fetcher := newFetcher(t, f)

	out := make(chan []esi.Order, 16)
	var got int
	done := make(chan struct{})
	go func() {
		for b := range out {
			got += len(b)
		}
		close(done)
	}()

	meta, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002, Priority: 2}, out)
	<-done
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if meta.Pages != 5 {
		t.Errorf("Pages = %d, want 5", meta.Pages)
	}
	if got != 50 {
		t.Errorf("collected %d orders, want 50", got)
	}
	if meta.OrderCount != 50 {
		t.Errorf("meta.OrderCount = %d, want 50", meta.OrderCount)
	}
	// 5 pages at 2 tokens each.
	if meta.TokensSpent != 10 {
		t.Errorf("TokensSpent = %d, want 10", meta.TokensSpent)
	}
}

// ESI shifts orders between pages when the snapshot rolls mid-fetch. A mixed page
// set must never be published.
func TestSweepRejectsLastModifiedMismatch(t *testing.T) {
	f := &fakeESI{pages: 4, ordersPerPage: 3, remaining: 11998,
		lastModifiedFor: func(page int) string {
			if page == 3 {
				return "Mon, 10 Aug 2026 12:55:23 GMT" // a later snapshot
			}
			return "Mon, 10 Aug 2026 12:50:23 GMT"
		}}
	fetcher := newFetcher(t, f)

	out := make(chan []esi.Order, 16)
	drain(out)
	_, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002}, out)
	if !errors.Is(err, ErrInconsistentPageSet) {
		t.Fatalf("err = %v, want ErrInconsistentPageSet", err)
	}
}

func TestSweepRejectsPageCountChange(t *testing.T) {
	f := &fakeESI{pages: 4, ordersPerPage: 3, remaining: 11998,
		pagesFor: func(page int) int {
			if page == 2 {
				return 5 // the book grew mid-sweep
			}
			return 4
		}}
	fetcher := newFetcher(t, f)

	out := make(chan []esi.Order, 16)
	drain(out)
	_, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002}, out)
	if !errors.Is(err, ErrInconsistentPageSet) {
		t.Fatalf("err = %v, want ErrInconsistentPageSet", err)
	}
}

func TestSweepDefersWhenPageSetWillNotFit(t *testing.T) {
	// 400 pages need 798 more tokens after page 1, but only 700 remain.
	f := &fakeESI{pages: 400, ordersPerPage: 1, remaining: 700}
	fetcher := newFetcher(t, f)

	out := make(chan []esi.Order, 16)
	drain(out)
	_, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002}, out)

	var d *ErrDeferred
	if !errors.As(err, &d) {
		t.Fatalf("err = %v, want ErrDeferred", err)
	}
	// Only page 1 may have been fetched.
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Errorf("made %d requests while deferring, want 1", len(f.requests))
	}
}

func TestSweepStopsBelowFloor(t *testing.T) {
	f := &fakeESI{pages: 2, ordersPerPage: 1, remaining: 250}
	fetcher := newFetcher(t, f)

	// Teach the budget the low value, as a previous response would have.
	fetcher.client.Budget.Observe(250)

	out := make(chan []esi.Order, 4)
	drain(out)
	_, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002}, out)

	var d *ErrDeferred
	if !errors.As(err, &d) {
		t.Fatalf("err = %v, want ErrDeferred", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 0 {
		t.Errorf("made %d requests below the floor, want 0", len(f.requests))
	}
}

func TestSweepPropagatesPageFailure(t *testing.T) {
	f := &fakeESI{pages: 4, ordersPerPage: 2, remaining: 11998,
		statusFor: func(page int) int {
			if page == 3 {
				return http.StatusServiceUnavailable
			}
			return http.StatusOK
		}}
	fetcher := newFetcher(t, f)

	out := make(chan []esi.Order, 16)
	drain(out)
	_, err := fetcher.Sweep(context.Background(), region.Region{ID: 10000002}, out)
	if err == nil {
		t.Fatal("expected an error when a page fails")
	}
	if !esi.IsServerError(err) {
		t.Errorf("err = %v, want it to classify as a server error", err)
	}
}

func TestSweepClosesOutputEvenOnFailure(t *testing.T) {
	f := &fakeESI{pages: 1, ordersPerPage: 1, remaining: 250}
	fetcher := newFetcher(t, f)
	fetcher.client.Budget.Observe(250)

	out := make(chan []esi.Order, 4)
	_, _ = fetcher.Sweep(context.Background(), region.Region{ID: 1}, out)

	select {
	case _, open := <-out:
		if open {
			t.Error("channel delivered a value; expected it closed and empty")
		}
	case <-time.After(time.Second):
		t.Error("output channel was not closed; a writer would leak")
	}
}
