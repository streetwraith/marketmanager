package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"marketmanager/internal/everef"
)

// historyStaging is the scratch table day files land in. Like the order staging
// tables it is created once and TRUNCATEd, so importing adds no catalog churn.
const historyStaging = "stg_history"

var historyCols = []string{
	"region_id", "type_id", "date", "average", "highest", "lowest",
	"volume", "order_count", "http_last_modified",
}

// EnsureHistoryPartitions creates a yearly partition for every year the range
// touches, plus the staging table.
//
// Partitioning by year makes the retention depth cheap to change in both
// directions: going deeper is importing older day files, and going shallower is
// DROP PARTITION rather than a mass DELETE that would bloat the table.
func (s *Store) EnsureHistoryPartitions(ctx context.Context, from, to time.Time) error {
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		`CREATE UNLOGGED TABLE IF NOT EXISTS %s.%s (LIKE %s.history)`,
		Schema, historyStaging, Schema,
	)); err != nil {
		return fmt.Errorf("create history staging: %w", err)
	}
	for y := from.Year(); y <= to.Year(); y++ {
		if err := s.ensureHistoryYear(ctx, y); err != nil {
			return err
		}
	}
	return nil
}

// ensureHistoryYear creates one yearly partition if it is missing.
//
// This runs on every import, not only at start-up. Creating partitions once at
// start-up leaves no home for the first row of a new year, and the insert fails
// with "no partition of relation found for row" every poll until someone
// restarts the service. There is deliberately no DEFAULT partition, because a
// row landing in one would be silently misfiled rather than loudly rejected.
func (s *Store) ensureHistoryYear(ctx context.Context, year int) error {
	// The bounds derive from an int, so they cannot inject.
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s.history_%d PARTITION OF %s.history
		 FOR VALUES FROM ('%d-01-01') TO ('%d-01-01')`,
		Schema, year, Schema, year, year+1,
	)); err != nil {
		return fmt.Errorf("create history partition %d: %w", year, err)
	}
	return nil
}

// EverefDay is the bookkeeping for one day file.
type EverefDay struct {
	Day          time.Time
	TotalsCount  int64
	RowsUpserted int64
	Watermark    time.Time
	ImportedAt   time.Time
}

// EverefDays loads the bookkeeping, keyed by the day in EVE Ref's format.
func (s *Store) EverefDays(ctx context.Context) (map[string]EverefDay, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT day, totals_count, rows_upserted, watermark, imported_at FROM `+Schema+`.everef_day`)
	if err != nil {
		return nil, fmt.Errorf("load everef_day: %w", err)
	}
	defer rows.Close()

	out := make(map[string]EverefDay)
	for rows.Next() {
		var d EverefDay
		var wm *time.Time
		if err := rows.Scan(&d.Day, &d.TotalsCount, &d.RowsUpserted, &wm, &d.ImportedAt); err != nil {
			return nil, fmt.Errorf("scan everef_day: %w", err)
		}
		if wm != nil {
			d.Watermark = *wm
		}
		out[d.Day.Format(everef.DateFormat)] = d
	}
	return out, rows.Err()
}

// historyCopySource feeds pgx.CopyFrom from parsed rows.
type historyCopySource struct {
	rows []everef.Row
	i    int
}

func (h *historyCopySource) Next() bool { h.i++; return h.i <= len(h.rows) }

func (h *historyCopySource) Values() ([]any, error) {
	r := h.rows[h.i-1]
	return []any{
		r.RegionID, r.TypeID, r.Date, r.Average, r.Highest, r.Lowest,
		r.Volume, r.OrderCount, r.HTTPLastModified,
	}, nil
}

func (h *historyCopySource) Err() error { return nil }

// ImportHistory upserts one day's rows and records the bookkeeping.
//
// The upsert is idempotent on (region_id, type_id, date), so a day may be
// imported any number of times as EVE Ref fills it in.
func (s *Store) ImportHistory(ctx context.Context, day time.Time, rows []everef.Row,
	totalsCount int64, watermark time.Time, now time.Time) (int64, error) {

	// The day may be the first of a new year, which start-up could not have known.
	if err := s.ensureHistoryYear(ctx, day.Year()); err != nil {
		return 0, err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire history conn: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var upserted int64
	if len(rows) > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE %s.%s`, Schema, historyStaging)); err != nil {
			return 0, fmt.Errorf("truncate history staging: %w", err)
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{Schema, historyStaging}, historyCols,
			&historyCopySource{rows: rows}); err != nil {
			return 0, fmt.Errorf("copy history staging: %w", err)
		}
		// A day file can carry the same (region, type, date) twice across waves, so
		// deduplicate to the most recently scraped copy before upserting.
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %[1]s.history (region_id, type_id, date, average, highest, lowest,
			                           volume, order_count, http_last_modified)
			SELECT DISTINCT ON (region_id, type_id, date)
			       region_id, type_id, date, average, highest, lowest,
			       volume, order_count, http_last_modified
			FROM %[1]s.%[2]s
			ORDER BY region_id, type_id, date, http_last_modified DESC
			ON CONFLICT (region_id, type_id, date) DO UPDATE SET
			    average = EXCLUDED.average, highest = EXCLUDED.highest, lowest = EXCLUDED.lowest,
			    volume = EXCLUDED.volume, order_count = EXCLUDED.order_count,
			    http_last_modified = EXCLUDED.http_last_modified`,
			Schema, historyStaging))
		if err != nil {
			return 0, fmt.Errorf("upsert history for %s: %w", day.Format(everef.DateFormat), err)
		}
		upserted = tag.RowsAffected()
	}

	var wm any
	if !watermark.IsZero() {
		wm = watermark
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO `+Schema+`.everef_day (day, totals_count, rows_upserted, watermark, imported_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (day) DO UPDATE SET
		    totals_count = EXCLUDED.totals_count,
		    rows_upserted = `+Schema+`.everef_day.rows_upserted + EXCLUDED.rows_upserted,
		    watermark = EXCLUDED.watermark,
		    imported_at = EXCLUDED.imported_at`,
		day, totalsCount, upserted, wm, now); err != nil {
		return 0, fmt.Errorf("record everef_day: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit history import: %w", err)
	}
	return upserted, nil
}

// HistoryCoverage reports what is stored, for logging and for verification.
func (s *Store) HistoryCoverage(ctx context.Context) (rows int64, minDay, maxDay time.Time, err error) {
	var mn, mx *time.Time
	// Aggregates always return exactly one row, so there is no no-rows case.
	err = s.pool.QueryRow(ctx,
		`SELECT count(*), min(date), max(date) FROM `+Schema+`.history`).Scan(&rows, &mn, &mx)
	if err != nil {
		return 0, minDay, maxDay, fmt.Errorf("history coverage: %w", err)
	}
	if mn != nil {
		minDay = *mn
	}
	if mx != nil {
		maxDay = *mx
	}
	return rows, minDay, maxDay, nil
}
