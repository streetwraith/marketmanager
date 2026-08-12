package store

import (
	"context"
	"fmt"
	"time"
)

// RegionStatus is the contract with the consumer. It polls refreshed_at to know
// when to run its own post-refresh work.
type RegionStatus struct {
	RegionID     int64
	RefreshedAt  time.Time
	Expires      time.Time
	LastModified time.Time
	Pages        int
	OrderCount   int64
}

// MarkRefreshed records a successful cycle and clears the error counter.
func (s *Store) MarkRefreshed(ctx context.Context, st RegionStatus) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE `+Schema+`.region_status
		SET refreshed_at = $2, expires = $3, last_modified = $4, pages = $5,
		    order_count = $6, consecutive_errors = 0, last_error = NULL, last_error_at = NULL
		WHERE region_id = $1`,
		st.RegionID, st.RefreshedAt, st.Expires, st.LastModified, st.Pages, st.OrderCount)
	if err != nil {
		return fmt.Errorf("mark refreshed %d: %w", st.RegionID, err)
	}
	return nil
}

// MarkFailed records a failed cycle. refreshed_at is deliberately left alone: the
// previous snapshot is still live and still correct, so the consumer must not be
// told it changed.
//
// at comes from the caller rather than the database, so every timestamp in the
// consumer contract has one owner. The consumer compares refreshed_at against its
// own bookkeeping, and mixing the application clock with the database clock would
// make that comparison wrong by the skew between two hosts.
func (s *Store) MarkFailed(ctx context.Context, regionID int64, cause string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE `+Schema+`.region_status
		SET consecutive_errors = consecutive_errors + 1, last_error = $2, last_error_at = $3
		WHERE region_id = $1`, regionID, cause, at)
	if err != nil {
		return fmt.Errorf("mark failed %d: %w", regionID, err)
	}
	return nil
}

// IngestLogEntry is one row of operational history.
type IngestLogEntry struct {
	RegionID       int64
	CycleStartedAt time.Time
	Pages          int
	TokensSpent    int
	RemainingAfter int
	FetchMS        int
	CopyMS         int
	VerifyMS       int
	ApplyMS        int
	RowsInserted   int64
	RowsUpdated    int64
	RowsDeleted    int64
	DuplicateRows  int64
	Outcome        string
	DeferredReason string
}

func (s *Store) LogIngest(ctx context.Context, e IngestLogEntry) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO `+Schema+`.ingest_log (
			region_id, cycle_started_at, pages, tokens_spent, remaining_after,
			fetch_ms, copy_ms, apply_ms, verify_ms,
			rows_inserted, rows_updated, rows_deleted, duplicate_rows, outcome, deferred_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NULLIF($15,''))`,
		e.RegionID, e.CycleStartedAt, e.Pages, e.TokensSpent, e.RemainingAfter,
		e.FetchMS, e.CopyMS, e.ApplyMS, e.VerifyMS,
		e.RowsInserted, e.RowsUpdated, e.RowsDeleted, e.DuplicateRows, e.Outcome, e.DeferredReason)
	if err != nil {
		return fmt.Errorf("log ingest %d: %w", e.RegionID, err)
	}
	return nil
}

// PruneIngestLog drops rows older than the retention window.
func (s *Store) PruneIngestLog(ctx context.Context, days int) (int64, error) {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s.ingest_log WHERE cycle_started_at < now() - make_interval(days => $1)`, Schema), days)
	if err != nil {
		return 0, fmt.Errorf("prune ingest_log: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RegionRowCount reports how many orders a region currently holds. Zero means the
// region has never loaded, so the cycle must take the resync path.
func (s *Store) RegionRowCount(ctx context.Context, regionID int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s.%s`, Schema, PartitionName(regionID))).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count region %d: %w", regionID, err)
	}
	return n, nil
}

// AnalyzeParents refreshes statistics on the partitioned parents.
//
// Autovacuum does not do this. It processes leaf partitions but never the
// parent, so without an explicit ANALYZE any consumer query that aggregates
// across regions plans against empty statistics. ANALYZE takes only
// SHARE UPDATE EXCLUSIVE, so it does not block readers, but it does conflict
// with TRUNCATE, which is why it runs on the maintenance ticker rather than
// inside a cycle.
func (s *Store) AnalyzeParents(ctx context.Context) error {
	for _, t := range []string{"orders", "history"} {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf("ANALYZE %s.%s", Schema, t)); err != nil {
			return fmt.Errorf("analyze %s: %w", t, err)
		}
	}
	return nil
}
