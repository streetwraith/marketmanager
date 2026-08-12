package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeCycle records what the scheduler asked for and returns scripted results.
type fakeCycle struct {
	mu    sync.Mutex
	calls []int64
	// result is consulted per call; attempt counts from 1.
	result func(r region.Region, attempt int) (Result, error)

	attempts map[int64]int
}

func (f *fakeCycle) Run(_ context.Context, r region.Region) (Result, error) {
	f.mu.Lock()
	if f.attempts == nil {
		f.attempts = map[int64]int{}
	}
	f.attempts[r.ID]++
	n := f.attempts[r.ID]
	f.calls = append(f.calls, r.ID)
	f.mu.Unlock()

	if f.result != nil {
		return f.result(r, n)
	}
	return Result{Region: r, Outcome: OutcomeOK, Meta: SweepMeta{Expires: time.Now().Add(5 * time.Minute)}}, nil
}

func (f *fakeCycle) callsFor(id int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[id]
}

func (f *fakeCycle) order() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.calls...)
}

func testScheduler(t *testing.T, fc *fakeCycle, regions []region.Region) *Scheduler {
	t.Helper()
	client := esi.New(esi.Options{BaseURL: "http://127.0.0.1:1", UserAgent: "t",
		CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second, RPS: 1000, Concurrency: 4})
	return NewScheduler(fc, client, regions, SchedulerConfig{
		MaxJitter:       0,
		CanaryInterval:  time.Second,
		MaxAttempts:     3,
		ErrorLimitFloor: 30,
		BudgetFloor:     300,
	}, discardLogger())
}

var (
	rDomain = region.Region{ID: region.Domain, Name: "Domain", Priority: 1}
	rForge  = region.Region{ID: region.TheForge, Name: "The Forge", Priority: 2}
	rSinq   = region.Region{ID: region.SinqLaison, Name: "Sinq Laison", Priority: 5}
	rKhanid = region.Region{ID: 10000049, Name: "Khanid", Priority: region.Rest}
)

// The owner's rule: a retry costs tokens, so only priorities 1-4 get them.
func TestRetriesOnlyForHighPriority(t *testing.T) {
	tests := []struct {
		name         string
		r            region.Region
		wantAttempts int
	}{
		{"Domain retries to the limit", rDomain, 3},
		{"The Forge retries to the limit", rForge, 3},
		{"Sinq Laison does not retry", rSinq, 1},
		{"a non-hub region does not retry", rKhanid, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
					return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("boom")
				}}
				s := testScheduler(t, fc, []region.Region{tc.r})
				s.runRegion(context.Background(), tc.r)
				if got := fc.callsFor(tc.r.ID); got != tc.wantAttempts {
					t.Errorf("attempts = %d, want %d", got, tc.wantAttempts)
				}
			})
		})
	}
}

// A deferral is the governor working, not a fault. Retrying would spend exactly
// the tokens it just protected.
func TestDeferredIsNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeDeferred, Err: &ErrDeferred{Reason: "budget"}}, nil
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.runRegion(context.Background(), rDomain)
		if got := fc.callsFor(rDomain.ID); got != 1 {
			t.Errorf("attempts = %d, want 1; a deferral must not be retried", got)
		}
	})
}

// A retry that eventually succeeds must stop retrying and schedule the next tick.
func TestRetryStopsOnSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, attempt int) (Result, error) {
			if attempt < 2 {
				return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("transient")
			}
			return Result{Region: r, Outcome: OutcomeOK,
				Meta: SweepMeta{Expires: time.Now().Add(5 * time.Minute)}}, nil
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.runRegion(context.Background(), rDomain)

		if got := fc.callsFor(rDomain.ID); got != 2 {
			t.Errorf("attempts = %d, want 2", got)
		}
		due, ok := s.due[rDomain.ID]
		if !ok {
			t.Fatal("no next due time recorded after success")
		}
		if wait := time.Until(due); wait < 4*time.Minute {
			t.Errorf("next due in %v, want about 5 minutes after Expires", wait)
		}
	})
}

// Priority decides the order within the freshness band, so the most important
// region is never queued behind the least important.
func TestPassRunsInPriorityOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{}
		// Deliberately supplied out of order.
		regions := []region.Region{rKhanid, rSinq, rForge, rDomain}
		s := testScheduler(t, fc, regions)
		for _, r := range regions {
			s.due[r.ID] = time.Now().Add(-time.Second) // all due
		}
		s.pass(context.Background())

		want := []int64{rDomain.ID, rForge.ID, rSinq.ID, rKhanid.ID}
		got := fc.order()
		if len(got) != len(want) {
			t.Fatalf("ran %d regions, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("position %d = %d, want %d (full order %v)", i, got[i], want[i], got)
			}
		}
	})
}

// A region that is not due yet must not be fetched.
func TestPassSkipsRegionsNotDue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{}
		s := testScheduler(t, fc, []region.Region{rDomain, rForge})
		s.due[rDomain.ID] = time.Now().Add(-time.Second)   // due
		s.due[rForge.ID] = time.Now().Add(2 * time.Minute) // not yet
		s.pass(context.Background())

		if got := fc.callsFor(rDomain.ID); got != 1 {
			t.Errorf("Domain ran %d times, want 1", got)
		}
		if got := fc.callsFor(rForge.ID); got != 0 {
			t.Errorf("The Forge ran %d times, want 0; it is not due", got)
		}
	})
}

// Tripping the legacy error limit blocks every application sharing the source IP,
// so nothing may be fetched until it resets.
func TestErrorLimitPausesEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.due[rDomain.ID] = time.Now().Add(-time.Second)
		s.client.ErrorLimit.Block(45 * time.Second)

		s.pass(context.Background())
		if got := fc.callsFor(rDomain.ID); got != 0 {
			t.Errorf("ran %d cycles while error limited, want 0", got)
		}
	})
}

// Below the floor, fetching stops entirely rather than draining the last tokens.
func TestBudgetFloorPausesFetching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.due[rDomain.ID] = time.Now().Add(-time.Second)
		s.client.Budget.Observe(120) // below the 300 floor

		s.pass(context.Background())
		if got := fc.callsFor(rDomain.ID); got != 0 {
			t.Errorf("ran %d cycles below the budget floor, want 0", got)
		}
	})
}

func TestNextDueNeverSpins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := testScheduler(t, &fakeCycle{}, nil)
		// An Expires already in the past must still push the next fetch forward.
		got := s.nextDue(time.Now().Add(-10 * time.Minute))
		if wait := time.Until(got); wait < time.Second {
			t.Errorf("next due in %v; a stale Expires would spin the loop", wait)
		}
	})
}

// primeESI serves page 1 for any region with a fixed Expires, and counts calls.
type primeESI struct {
	mu       sync.Mutex
	requests []string
	expires  string
}

func (p *primeESI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.requests = append(p.requests, r.URL.Path+"?"+r.URL.RawQuery)
	p.mu.Unlock()
	w.Header().Set("X-Pages", "413")
	w.Header().Set("X-Ratelimit-Remaining", "11998")
	w.Header().Set("Last-Modified", "Mon, 10 Aug 2026 12:50:23 GMT")
	w.Header().Set("Expires", p.expires)
	_, _ = w.Write([]byte(`[]`))
}

// prime is the cold-start guard. Without it a restart fits four full sweeps into
// the first 15-minute window instead of three and exhausts the token bucket, and
// it would do that on every deploy.
func TestPrimeCostsOnePageAndDefersTheFirstSweep(t *testing.T) {
	fake := &primeESI{expires: "Mon, 10 Aug 2026 12:55:23 GMT"}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	client := esi.New(esi.Options{BaseURL: srv.URL, UserAgent: "t",
		CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second, RPS: 1000, Concurrency: 4})
	fc := &fakeCycle{}
	regions := []region.Region{rDomain, rForge, rSinq}
	s := NewScheduler(fc, client, regions, SchedulerConfig{
		MaxJitter: 0, CanaryInterval: time.Second, MaxAttempts: 3,
		ErrorLimitFloor: 30, BudgetFloor: 300,
	}, discardLogger())

	if err := s.prime(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Exactly one request per region, and only page 1: priming must not sweep.
	fake.mu.Lock()
	got := append([]string(nil), fake.requests...)
	fake.mu.Unlock()
	if len(got) != len(regions) {
		t.Errorf("made %d requests to prime %d regions, want one each: %v", len(got), len(regions), got)
	}
	for _, req := range got {
		if !strings.HasSuffix(req, "page=1") {
			t.Errorf("primed with %q; only page 1 may be fetched", req)
		}
	}

	// No region may be fetched before its own Expires.
	for _, r := range regions {
		due, ok := s.due[r.ID]
		if !ok {
			t.Errorf("%s has no due time after priming", r.Name)
			continue
		}
		if !due.After(time.Now()) {
			t.Errorf("%s is due immediately (%v); the first sweep was not deferred", r.Name, due)
		}
	}
	// And nothing was swept during priming.
	if n := len(fc.order()); n != 0 {
		t.Errorf("ran %d cycles during priming, want 0", n)
	}
}

// One unreachable region must not stop the service from starting.
func TestPrimeSurvivesAFailingRegion(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Pages", "1")
		w.Header().Set("Expires", "Mon, 10 Aug 2026 12:55:23 GMT")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	client := esi.New(esi.Options{BaseURL: srv.URL, UserAgent: "t",
		CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second, RPS: 1000, Concurrency: 4})
	regions := []region.Region{rDomain, rForge}
	s := NewScheduler(&fakeCycle{}, client, regions, SchedulerConfig{
		MaxJitter: 0, CanaryInterval: time.Second, MaxAttempts: 3,
		ErrorLimitFloor: 30, BudgetFloor: 300,
	}, discardLogger())

	if err := s.prime(context.Background()); err != nil {
		t.Fatalf("prime returned an error for one bad region: %v", err)
	}
	// Both regions still get a due time; the failed one is simply retried sooner.
	for _, r := range regions {
		if _, ok := s.due[r.ID]; !ok {
			t.Errorf("%s has no due time after a partial prime failure", r.Name)
		}
	}
}

// A deferral must wait for the region's own Expires. Retrying sooner re-pays for
// page 1 to re-learn that the budget is short, which keeps it short.
func TestDeferredWaitsForTheNextSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expires := time.Now().Add(4 * time.Minute)
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{
				Region:  r,
				Outcome: OutcomeDeferred,
				Meta:    SweepMeta{Expires: expires},
				Err:     &ErrDeferred{Reason: "budget"},
			}, nil
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.runRegion(context.Background(), rDomain)

		due, ok := s.due[rDomain.ID]
		if !ok {
			t.Fatal("no due time recorded after a deferral")
		}
		if wait := time.Until(due); wait < 3*time.Minute {
			t.Errorf("next attempt in %v; a deferral must wait for the next snapshot, "+
				"not re-pay for page 1 immediately", wait)
		}
	})
}
