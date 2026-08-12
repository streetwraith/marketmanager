//go:build integration

package ingest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"marketmanager/internal/everef"
	"marketmanager/internal/store"
)

// A deliberately shallow backfill. The point is to prove the path works, not to
// load two years from a third-party service inside a test.
const testBackfillDays = 4

func liveHistory(t *testing.T) (*HistoryImporter, *store.Store) {
	t.Helper()
	st, ua := liveEnv(t)

	regions, err := st.Regions(context.Background())
	if err != nil {
		t.Fatalf("Regions: %v", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	imp := NewHistoryImporter(
		everef.New("https://data.everef.net/market-history", ua), st, regions,
		HistoryConfig{PollInterval: time.Hour, BackfillDays: testBackfillDays,
			RecentDays: 10, Spacing: 500 * time.Millisecond}, log)
	return imp, st
}

func TestLiveHistoryImport(t *testing.T) {
	imp, st := liveHistory(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	from := time.Now().UTC().AddDate(0, 0, -testBackfillDays)
	if err := st.EnsureHistoryPartitions(ctx, from, time.Now().UTC()); err != nil {
		t.Fatalf("EnsureHistoryPartitions: %v", err)
	}
	// Start clean so the counts below mean what they say.
	for _, q := range []string{
		"DELETE FROM market.history WHERE date >= $1",
		"DELETE FROM market.everef_day WHERE day >= $1",
	} {
		if _, err := st.Pool().Exec(ctx, q, from); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}

	imp.once(ctx)

	rows, minDay, maxDay, err := st.HistoryCoverage(ctx)
	if err != nil {
		t.Fatalf("HistoryCoverage: %v", err)
	}
	if rows == 0 {
		t.Fatal("imported nothing")
	}
	t.Logf("after first import: rows=%d from=%s to=%s",
		rows, minDay.Format(everef.DateFormat), maxDay.Format(everef.DateFormat))

	// Only tracked regions may be stored. EVE Ref publishes ~77.
	var untracked int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM market.history h
		WHERE NOT EXISTS (SELECT 1 FROM market.region_status rs WHERE rs.region_id = h.region_id)
	`).Scan(&untracked); err != nil {
		t.Fatalf("untracked check: %v", err)
	}
	if untracked != 0 {
		t.Errorf("stored %d rows for untracked regions", untracked)
	}

	// Rows must land in the yearly partition matching their own date.
	var misfiled int
	if err := st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM market.history h
		WHERE substring(h.tableoid::regclass::text from 'history_([0-9]{4})')::int
		   <> date_part('year', h.date)::int
	`).Scan(&misfiled); err != nil {
		t.Fatalf("partition check: %v", err)
	}
	if misfiled != 0 {
		t.Errorf("%d rows landed in the wrong yearly partition", misfiled)
	}

	// The whole point of the watermark: a second pass over unchanged files must
	// upsert nothing, rather than rewriting every row it already holds.
	before := rows
	imp.once(ctx)
	after, _, _, err := st.HistoryCoverage(ctx)
	if err != nil {
		t.Fatalf("HistoryCoverage: %v", err)
	}
	if after != before {
		t.Errorf("row count changed on a no-op re-import: %d -> %d", before, after)
	}

	var reimported int64
	if err := st.Pool().QueryRow(ctx, `
		SELECT COALESCE(sum(rows_upserted), 0) FROM market.everef_day WHERE day >= $1`,
		from).Scan(&reimported); err != nil {
		t.Fatalf("rows_upserted: %v", err)
	}
	t.Logf("cumulative rows upserted across both passes: %d (stored: %d)", reimported, after)
	// Without the watermark this would be roughly double the stored count.
	if reimported > after*3/2 {
		t.Errorf("upserted %d rows to store %d; the watermark filter is not working",
			reimported, after)
	}
}
