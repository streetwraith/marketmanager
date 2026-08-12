//go:build integration

package ingest

import (
	"context"
	"os"
	"testing"
	"time"

	"marketmanager/internal/store"
)

// Statistics on a partitioned parent are only ever refreshed by an explicit
// ANALYZE: autovacuum processes the leaves and skips the parent. This used to run
// on the 24h prune ticker, which left the planner with no statistics at all for a
// day after every restart.
func TestMaintenanceAnalyzesAtStartup(t *testing.T) {
	dsn := os.Getenv("MM_TEST_DSN")
	if dsn == "" {
		t.Skip("MM_TEST_DSN not set")
	}
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(st.Close)
	ctx := context.Background()

	lastAnalyze := func() *time.Time {
		var ts *time.Time
		if err := st.Pool().QueryRow(ctx, `
			SELECT last_analyze FROM pg_stat_user_tables
			WHERE schemaname='market' AND relname='orders'`).Scan(&ts); err != nil {
			t.Fatalf("read last_analyze: %v", err)
		}
		return ts
	}
	before := lastAnalyze()

	// A prune interval far in the future, so a passing test can only be the
	// start-up analyze and never the prune ticker.
	m := NewMaintenance(st, 24*time.Hour, 24*time.Hour, 30, discardLogger())
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(runCtx) }()

	deadline := time.After(25 * time.Second)
	for {
		after := lastAnalyze()
		if after != nil && (before == nil || after.After(*before)) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("parents were not analyzed at start-up (last_analyze still %v)", before)
		case <-time.After(250 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v, want nil on cancellation", err)
	}
}
