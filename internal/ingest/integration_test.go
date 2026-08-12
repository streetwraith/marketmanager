//go:build integration

// These tests hit real ESI and a real database. Run with:
//
//	MM_TEST_DSN="postgres://marketmanager:marketmanager@127.0.0.1:5432/eve?sslmode=disable" \
//	MM_TEST_USER_AGENT="marketmanager-test/1.0 (you@example.com)" \
//	  go test -tags integration -timeout 15m ./internal/ingest/
package ingest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
	"marketmanager/internal/store"
)

// liveEnv opens the shared setup every integration test in this package needs:
// a real database and a user agent for the real services.
func liveEnv(t *testing.T) (*store.Store, string) {
	t.Helper()
	dsn, ua := os.Getenv("MM_TEST_DSN"), os.Getenv("MM_TEST_USER_AGENT")
	if dsn == "" || ua == "" {
		t.Skip("MM_TEST_DSN or MM_TEST_USER_AGENT not set")
	}
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(st.Close)
	return st, ua
}

func liveCycle(t *testing.T) (*Cycle, *store.Store, *esi.Client) {
	t.Helper()
	st, ua := liveEnv(t)

	c := esi.New(esi.Options{
		BaseURL: "https://esi.evetech.net", UserAgent: ua,
		CompatibilityDate: "2026-08-04", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second, RPS: 70, Concurrency: 64,
	})
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return NewCycle(NewFetcher(c, FetcherConfig{
		Reserve: 600, BudgetFloor: 300, PageAttempts: 3,
		ErrorLimitFloor: 30, PageBackoffUnit: 250 * time.Millisecond,
	}), st, c, "64MB", log), st, c
}

// The Forge is the stress case: ~413 pages and ~412,000 orders.
var theForge = region.Region{ID: region.TheForge, Name: "The Forge", Priority: 2}

func TestLiveForgeCycle(t *testing.T) {
	cyc, st, client := liveCycle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// The test creates what it needs rather than assuming the service has run.
	if err := st.EnsureRegionObjects(ctx, []region.Region{theForge}); err != nil {
		t.Fatalf("EnsureRegionObjects: %v", err)
	}
	// Start from nothing, so the first cycle exercises the resync path.
	if _, err := st.Pool().Exec(ctx, "TRUNCATE market.orders_10000002"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	first, err := cyc.Run(ctx, theForge)
	if err != nil {
		t.Fatalf("first cycle: %v", err)
	}
	if first.Outcome != OutcomeResync {
		t.Fatalf("first outcome = %q, want %q (cause: %v)", first.Outcome, OutcomeResync, first.Err)
	}
	rem, _ := client.Budget.Remaining()
	t.Logf("cycle 1 (resync): pages=%d orders=%d tokens=%d fetch=%s loaded=%d remaining=%d",
		first.Meta.Pages, first.Meta.OrderCount, first.Meta.TokensSpent,
		first.Meta.Duration.Round(time.Millisecond), first.Delta.Inserted, rem)

	if first.Meta.Pages < 300 {
		t.Errorf("pages = %d, suspiciously low for The Forge", first.Meta.Pages)
	}
	// 2 tokens per 200 response.
	if want := first.Meta.Pages * 2; first.Meta.TokensSpent != want {
		t.Errorf("tokens = %d, want %d", first.Meta.TokensSpent, want)
	}

	if testing.Short() {
		t.Skip("skipping the cross-tick delta in short mode")
	}

	// Wait for the next snapshot, so the second cycle sees genuine churn.
	wait := time.Until(first.Meta.Expires.Add(3 * time.Second))
	if wait > 0 {
		t.Logf("waiting %s for the next Expires tick", wait.Round(time.Second))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			t.Fatal("timed out waiting for the next tick")
		}
	}

	second, err := cyc.Run(ctx, theForge)
	if err != nil {
		t.Fatalf("second cycle: %v", err)
	}
	if second.Outcome != OutcomeOK {
		t.Fatalf("second outcome = %q, want %q (a resync here means the checksum drifted; cause: %v)",
			second.Outcome, OutcomeOK, second.Err)
	}
	touched := second.Delta.Inserted + second.Delta.Updated + second.Delta.Deleted
	pct := float64(touched) / float64(second.Staged) * 100
	rem, _ = client.Budget.Remaining()
	t.Logf("cycle 2 (delta): orders=%d  +%d ~%d -%d  duplicates=%d  touched=%.2f%%  remaining=%d",
		second.Staged, second.Delta.Inserted, second.Delta.Updated, second.Delta.Deleted,
		second.Delta.Duplicates, pct, rem)

	// The whole design rests on churn being a small fraction of the table. If this
	// ever fails, the delta approach needs revisiting.
	if pct > 10 {
		t.Errorf("delta touched %.2f%% of the table; the design assumes well under 10%%", pct)
	}
	if touched == 0 {
		t.Error("delta touched nothing across an Expires boundary; the snapshot did not advance")
	}
}

// The repair path: when the drift guard disagrees, the cycle must rebuild the
// region rather than leave wrong data in place.
//
// Correct delta SQL is self-correcting, so real drift cannot be induced from
// outside. The guard is forced instead, which is what the verify seam exists for.
func TestLiveCycleResyncsOnDrift(t *testing.T) {
	cyc, st, _ := liveCycle(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// The global PLEX market is one page, so this costs 2 tokens per cycle.
	plex := region.Region{ID: region.GlobalPLEX, Name: "GPMR-01", Priority: region.Rest}
	if err := st.EnsureRegionObjects(ctx, []region.Region{plex}); err != nil {
		t.Fatalf("EnsureRegionObjects: %v", err)
	}

	// A normal cycle first, so the region holds data and the next cycle takes the
	// delta path rather than the first-load path.
	if _, err := cyc.Run(ctx, plex); err != nil {
		t.Fatalf("first cycle: %v", err)
	}
	if _, err := cyc.Run(ctx, plex); err != nil {
		t.Fatalf("second cycle: %v", err)
	}

	// Now force the guard to disagree.
	real := cyc.verify
	cyc.verify = func(ctx context.Context, regionID int64) (store.Checksum, store.Checksum, error) {
		live, staged, err := real(ctx, regionID)
		staged.Hash += "0" // no longer equal to live
		return live, staged, err
	}
	res, err := cyc.Run(ctx, plex)
	if err != nil {
		t.Fatalf("cycle with forced drift: %v", err)
	}
	if res.Outcome != OutcomeResync {
		t.Errorf("outcome = %q, want %q; the drift guard did not trigger a repair",
			res.Outcome, OutcomeResync)
	}

	// After the repair the region must still be correct.
	cyc.verify = real
	live, staged, err := st.VerifyRegion(ctx, plex.ID)
	if err != nil {
		t.Fatalf("VerifyRegion: %v", err)
	}
	if live.Rows != staged.Rows || live.Hash != staged.Hash {
		t.Errorf("region is wrong after the repair: live=%+v staged=%+v", live, staged)
	}
	if live.Rows == 0 {
		t.Error("region is empty after the repair")
	}
}
