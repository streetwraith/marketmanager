package ingest

import (
	"context"
	"errors"
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
	"marketmanager/internal/sentrytest"
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

// An upstream maintenance answers 5xx on every route. Fetching on must stop for
// every region, not only for the one that met the failure.
func TestOutagePausesEveryRegion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{}
		s := testScheduler(t, fc, []region.Region{rDomain, rForge})
		for _, r := range []region.Region{rDomain, rForge} {
			s.due[r.ID] = time.Now().Add(-time.Second)
		}
		s.client.Outage.Observe(503, 0)
		s.client.Outage.Observe(503, 0)

		s.pass(context.Background())

		if got := len(fc.order()); got != 0 {
			t.Errorf("ran %d cycles during an upstream outage, want 0", got)
		}
	})
}

// While paused, one probe per interval replaces one strike per region per cycle.
// That ratio is what keeps a maintenance from tripping the shared 420.
func TestOutageProbesOnceAndResumesOnRecovery(t *testing.T) {
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		w.Header().Set("X-Pages", "1")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)

	client := esi.New(esi.Options{BaseURL: srv.URL, UserAgent: "t",
		CompatibilityDate: "d", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second, RPS: 1000, Concurrency: 4})
	fc := &fakeCycle{}
	regions := []region.Region{rDomain, rForge}
	s := NewScheduler(fc, client, regions, SchedulerConfig{
		MaxJitter: 0, CanaryInterval: time.Second, MaxAttempts: 3,
		ErrorLimitFloor: 30, BudgetFloor: 300,
	}, discardLogger())
	for _, r := range regions {
		s.due[r.ID] = time.Now().Add(-time.Second)
	}
	client.Outage.Observe(503, 0)
	client.Outage.Observe(503, 0)

	// The paused pass spends exactly one request, and no region cycle.
	s.pass(context.Background())
	if got := probes.Load(); got != 1 {
		t.Errorf("made %d requests while paused, want exactly 1 probe", got)
	}
	if got := len(fc.order()); got != 0 {
		t.Errorf("ran %d cycles while paused, want 0", got)
	}

	// The probe succeeded, so the pause is over and the next pass fetches at once
	// rather than sitting out the rest of the minute.
	if d := client.Outage.PausedFor(); d != 0 {
		t.Fatalf("still paused for %v after a successful probe", d)
	}
	s.pass(context.Background())
	if got := len(fc.order()); got != len(regions) {
		t.Errorf("ran %d cycles after recovery, want %d", got, len(regions))
	}
}

// A region already in flight when the outage arms must not keep retrying: those
// retries strike the same shared error limit the pause exists to protect.
func TestOutageSuspendsRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("esi: http 503")
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.client.Outage.Observe(503, 0)
		s.client.Outage.Observe(503, 0)

		s.runRegion(context.Background(), rDomain)

		// rDomain is priority 1 and would normally take all three attempts.
		if got := fc.callsFor(rDomain.ID); got != 1 {
			t.Errorf("attempts = %d during an outage, want 1", got)
		}
	})
}

// The gate can arm part way through a pass. Every region falls due in the same
// band, so the remaining ones must not each spend a strike before the next tick.
func TestOutageStopsThePassPartWayThrough(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		regions := []region.Region{rDomain, rForge, rSinq}
		fc := &fakeCycle{}
		s := testScheduler(t, fc, regions)
		fc.result = func(r region.Region, _ int) (Result, error) {
			s.client.Outage.Observe(503, 0)
			s.client.Outage.Observe(503, 0)
			return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("esi: http 503")
		}
		for _, r := range regions {
			s.due[r.ID] = time.Now().Add(-time.Second)
		}

		s.pass(context.Background())

		if got := len(fc.order()); got != 1 {
			t.Errorf("ran %d cycles, want 1: the pass must stop when the gate arms", got)
		}
	})
}

// One upstream problem hits every region inside the same due band, and the loop
// ticks each second. Without the streak that is hundreds of events per outage.
func TestSchedulerReportsOnceForARegionOutage(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("esi unreachable")
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})

		s.runRegion(context.Background(), rDomain)
		s.runRegion(context.Background(), rDomain)
		if got := len(sink.Captured()); got != 1 {
			t.Fatalf("events after two failing cycles = %d, want 1", got)
		}

		// One success ends the run, so the next outage is a new fault.
		fc.result = func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeOK,
				Meta: SweepMeta{Expires: time.Now().Add(5 * time.Minute)}}, nil
		}
		s.runRegion(context.Background(), rDomain)
		fc.result = func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("esi unreachable")
		}
		s.runRegion(context.Background(), rDomain)

		events := sink.Captured()
		if len(events) != 2 {
			t.Fatalf("events after a second outage = %d, want 2", len(events))
		}
		for i, e := range events {
			if e.Tags["component"] != "scheduler" || e.Tags["region"] != "Domain" {
				t.Errorf("event %d tags = %v, want component=scheduler region=Domain", i, e.Tags)
			}
		}

		// The context is what an operator reads to decide whether to act.
		cycle := events[0].Contexts["cycle"]
		if cycle["region_id"] != rDomain.ID {
			t.Errorf("region_id = %v, want %d", cycle["region_id"], rDomain.ID)
		}
		if cycle["priority"] != rDomain.Priority {
			t.Errorf("priority = %v, want %d", cycle["priority"], rDomain.Priority)
		}
		if cycle["outcome"] != OutcomeFailed {
			t.Errorf("outcome = %v, want %s", cycle["outcome"], OutcomeFailed)
		}
		if cycle["attempts"] != 3 {
			t.Errorf("attempts = %v, want 3 (Domain retries to the limit)", cycle["attempts"])
		}
	})
}

// Each region reports for itself: a fault confined to one region must not be
// masked by another region's open run.
func TestSchedulerTracksEachRegionSeparately(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, fmt.Errorf("esi unreachable")
		}}
		s := testScheduler(t, fc, []region.Region{rDomain, rForge})

		s.runRegion(context.Background(), rDomain)
		s.runRegion(context.Background(), rForge)

		if got := len(sink.Captured()); got != 2 {
			t.Fatalf("events = %d, want one per region", got)
		}
	})
}

// The governor refusing a region is the design working. So is a shutdown, which
// is a cancelled parent context rather than an error that mentions one.
func TestSchedulerReportsNeitherDeferralNorShutdown(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeDeferred, Err: &ErrDeferred{Reason: "budget"}}, nil
		}}
		s := testScheduler(t, fc, []region.Region{rDomain, rForge})
		s.runRegion(context.Background(), rDomain)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fc.result = func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, errors.New("db down")
		}
		s.runRegion(ctx, rForge)

		if got := len(sink.Captured()); got != 0 {
			t.Fatalf("events = %d, want 0", got)
		}
	})
}

// A cancelled inner context is not a shutdown. A database failure during the
// copy aborts the in-flight sweep, so the cause wraps context.Canceled while the
// service itself runs on. That fault is exactly the kind a person must see, so
// the capture path must never filter on the error alone.
func TestSchedulerReportsACauseThatWrapsCancellation(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		cause := errors.Join(
			fmt.Errorf("region 10000002 page 7: %w", context.Canceled),
			errors.New("copy orders: connection lost"),
		)
		fc := &fakeCycle{result: func(r region.Region, _ int) (Result, error) {
			return Result{Region: r, Outcome: OutcomeFailed}, cause
		}}
		s := testScheduler(t, fc, []region.Region{rDomain})
		s.runRegion(context.Background(), rDomain)

		if got := len(sink.Captured()); got != 1 {
			t.Fatalf("events = %d, want 1", got)
		}
	})
}

// The legacy limit is counted per source IP, so tripping it returns 420 to every
// other application on this host. It reports once per trip, not once per tick.
func TestSchedulerReportsTheErrorLimitOncePerTrip(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := testScheduler(t, &fakeCycle{}, []region.Region{rDomain})

		s.client.ErrorLimit.Block(45 * time.Second)
		s.pass(ctx)                        // trips, reports, then sleeps out the block
		s.noteErrorLimit(40 * time.Second) // a re-entry inside the same trip stays quiet
		events := sink.Captured()
		if len(events) != 1 {
			t.Fatalf("events = %d, want 1", len(events))
		}
		if events[0].Tags["component"] != "scheduler" {
			t.Errorf("tags = %v, want component=scheduler", events[0].Tags)
		}

		s.pass(ctx) // the block has expired, so the guard clears
		s.client.ErrorLimit.Block(30 * time.Second)
		s.pass(ctx) // a new trip is a new fault
		if got := len(sink.Captured()); got != 2 {
			t.Fatalf("events after a second trip = %d, want 2", got)
		}
	})
}

// A budget pause and an upstream 5xx are this service backing off from its own
// upstream. Both clear in minutes and neither needs a person.
func TestSchedulerReportsNeitherBudgetFloorNorOutage(t *testing.T) {
	sink := sentrytest.Capture(t)
	synctest.Test(t, func(t *testing.T) {
		s := testScheduler(t, &fakeCycle{}, []region.Region{rDomain})
		s.due[rDomain.ID] = time.Now().Add(-time.Second)

		s.client.Budget.Observe(120) // below the 300 floor
		s.pass(context.Background())

		s.client.Outage.Observe(503, 0)
		s.client.Outage.Observe(503, 0)
		s.pass(context.Background())

		if got := len(sink.Captured()); got != 0 {
			t.Fatalf("events = %d, want 0", got)
		}
	})
}
