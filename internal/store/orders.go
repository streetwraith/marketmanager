package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"marketmanager/internal/esi"
)

// orderCols is the column list, in the order CopyOrders supplies values.
var orderCols = []string{
	"region_id", "order_id", "type_id", "location_id", "system_id", "is_buy_order",
	"price", "volume_remain", "volume_total", "min_volume", "duration", "range", "issued",
}

// compareCols are the columns that decide whether a surviving order changed.
// region_id and order_id are the key, so they are excluded.
var compareCols = []string{
	"type_id", "location_id", "system_id", "is_buy_order", "price",
	"volume_remain", "volume_total", "min_volume", "duration", "range", "issued",
}

// PartitionName is the per-region partition of market.orders.
func PartitionName(id int64) string { return fmt.Sprintf("orders_%d", id) }

// StagingName is the per-region scratch table the fetched snapshot lands in.
//
// These tables are created once and TRUNCATEd each cycle rather than created and
// dropped, so a 5-minute ingest cycle adds no system catalog churn. They are
// UNLOGGED because they are rebuilt every cycle: skipping WAL for them is most of
// why a cycle writes ~1-3 MB of WAL instead of ~95 MB.
func StagingName(id int64) string { return fmt.Sprintf("stg_%d", id) }

// orderCopySource feeds pgx.CopyFrom from the sweep's output channel, so pages
// stream into Postgres as they arrive instead of buffering a whole region.
type orderCopySource struct {
	in       <-chan []esi.Order
	regionID int64

	batch []esi.Order
	idx   int
	cur   esi.Order
}

func (s *orderCopySource) Next() bool {
	for s.idx >= len(s.batch) {
		batch, ok := <-s.in
		if !ok {
			return false
		}
		s.batch, s.idx = batch, 0
	}
	s.cur = s.batch[s.idx]
	s.idx++
	return true
}

func (s *orderCopySource) Values() ([]any, error) {
	o := s.cur
	return []any{
		s.regionID, o.OrderID, o.TypeID, o.LocationID, o.SystemID, o.IsBuyOrder,
		o.Price.String(), o.VolumeRemain, o.VolumeTotal, o.MinVolume, o.Duration,
		o.Range, o.Issued,
	}, nil
}

func (s *orderCopySource) Err() error { return nil }

// drainOrders consumes the remainder of a sweep's output so an early return
// cannot block the producer.
//
// It drains in the background deliberately. Waiting synchronously would hold the
// caller until the whole sweep finished, delaying the cancellation that exists to
// stop fetching pages nobody will store.
func drainOrders(in <-chan []esi.Order) {
	go func() {
		for range in { //nolint:revive // draining
		}
	}()
}

// CopyOrders truncates the region's staging table and streams the channel into
// it. It returns when the channel closes.
func (s *Store) CopyOrders(ctx context.Context, regionID int64, in <-chan []esi.Order) (int64, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire copy conn: %w", err)
	}
	defer conn.Release()

	staging := StagingName(regionID)
	if _, err := conn.Exec(ctx, fmt.Sprintf(`TRUNCATE %s.%s`, Schema, staging)); err != nil {
		drainOrders(in)
		return 0, fmt.Errorf("truncate staging %s: %w", staging, err)
	}

	src := &orderCopySource{in: in, regionID: regionID}
	n, err := conn.CopyFrom(ctx, pgx.Identifier{Schema, staging}, orderCols, src)
	if err != nil {
		drainOrders(in)
		return n, fmt.Errorf("copy into %s: %w", staging, err)
	}
	return n, nil
}

// DeltaResult reports what one apply changed.
type DeltaResult struct {
	Inserted   int64
	Updated    int64
	Deleted    int64
	Duplicates int64
}

// applySQL computes the delta and applies it in a single statement.
//
// Measured against a real Forge snapshot, only ~0.52% of rows change per cycle,
// so this touches roughly 2,000 rows instead of rewriting 406,000. It takes only
// ROW EXCLUSIVE, which never conflicts with the ACCESS SHARE readers take, so
// consumers are not blocked at all. Being one statement, it is atomic: a reader
// sees either the whole old snapshot or the whole new one.
//
// The DISTINCT ON is mandatory. ESI can return the same order on two pages when
// the page set shifts mid-fetch; left in place, a duplicated new order violates
// the primary key and aborts the entire cycle.
func applySQL(regionID int64) string {
	part := fmt.Sprintf("%s.%s", Schema, PartitionName(regionID))
	staging := fmt.Sprintf("%s.%s", Schema, StagingName(regionID))

	setClause := make([]string, 0, len(compareCols))
	insCols := make([]string, 0, len(orderCols))
	insVals := make([]string, 0, len(orderCols))
	distinct := make([]string, 0, len(orderCols))
	for _, c := range compareCols {
		setClause = append(setClause, fmt.Sprintf("%q = d.%s", c, quoteAlias(c)))
	}
	for _, c := range orderCols {
		insCols = append(insCols, fmt.Sprintf("%q", c))
		if c == "order_id" {
			insVals = append(insVals, "d.oid")
		} else {
			insVals = append(insVals, "d."+quoteAlias(c))
		}
	}
	for _, c := range orderCols {
		if c == "order_id" {
			continue
		}
		distinct = append(distinct, fmt.Sprintf("s.%q AS %s", c, quoteAlias(c)))
	}

	sCols := make([]string, 0, len(compareCols))
	lCols := make([]string, 0, len(compareCols))
	for _, c := range compareCols {
		sCols = append(sCols, "s."+quoteAlias(c))
		lCols = append(lCols, fmt.Sprintf("l.%q", c))
	}

	return fmt.Sprintf(`
WITH s AS (
    SELECT DISTINCT ON (order_id) order_id AS oid, %[1]s
    FROM %[2]s s ORDER BY order_id
),
d AS MATERIALIZED (
    SELECT CASE WHEN l.order_id IS NULL THEN 'I'
                WHEN s.oid IS NULL      THEN 'D'
                ELSE 'U' END AS op,
           COALESCE(s.oid, l.order_id) AS oid, %[3]s
    FROM s FULL OUTER JOIN %[4]s l ON l.order_id = s.oid
    WHERE l.order_id IS NULL OR s.oid IS NULL
       OR (%[5]s) IS DISTINCT FROM (%[6]s)
),
del AS (
    DELETE FROM %[4]s l USING d WHERE d.op = 'D' AND l.order_id = d.oid RETURNING 1
),
upd AS (
    UPDATE %[4]s l SET %[7]s FROM d
    WHERE d.op = 'U' AND l.region_id = d.region_id AND l.order_id = d.oid RETURNING 1
),
ins AS (
    INSERT INTO %[4]s (%[8]s) SELECT %[9]s FROM d WHERE d.op = 'I' RETURNING 1
)
SELECT (SELECT count(*) FROM ins), (SELECT count(*) FROM upd), (SELECT count(*) FROM del)`,
		strings.Join(distinct, ", "),  // 1: staging projection
		staging,                       // 2
		selectAliases(),               // 3: delta projection
		part,                          // 4
		strings.Join(sCols, ", "),     // 5
		strings.Join(lCols, ", "),     // 6
		strings.Join(setClause, ", "), // 7
		strings.Join(insCols, ", "),   // 8
		strings.Join(insVals, ", "),   // 9
	)
}

// quoteAlias keeps "range" from colliding with the type name when used as an alias.
func quoteAlias(c string) string {
	if c == "range" {
		return `"range"`
	}
	return c
}

func selectAliases() string {
	var out []string
	for _, c := range orderCols {
		if c == "order_id" {
			continue
		}
		out = append(out, "s."+quoteAlias(c))
	}
	return strings.Join(out, ", ")
}

// ApplyDelta computes and applies the delta for one region.
func (s *Store) ApplyDelta(ctx context.Context, regionID int64, workMem string) (DeltaResult, error) {
	var res DeltaResult

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return res, fmt.Errorf("acquire apply conn: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 64MB keeps the delta's hash join to a single batch. 4MB spills to four and
	// costs ~50% more; 256MB buys nothing. Set locally so other apps are unaffected.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL work_mem = %s", quoteLiteral(workMem))); err != nil {
		return res, fmt.Errorf("set work_mem: %w", err)
	}

	staging := fmt.Sprintf("%s.%s", Schema, StagingName(regionID))
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) - count(DISTINCT order_id) FROM %s`, staging),
	).Scan(&res.Duplicates); err != nil {
		return res, fmt.Errorf("count duplicates: %w", err)
	}

	if err := tx.QueryRow(ctx, applySQL(regionID)).Scan(&res.Inserted, &res.Updated, &res.Deleted); err != nil {
		return res, fmt.Errorf("apply delta for region %d: %w", regionID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit delta: %w", err)
	}
	return res, nil
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Checksum is the drift guard. Both the live partition and the staging table can
// compute it, and they must agree after an apply.
//
// Hash is text, not an integer: sum() over bigint yields numeric, and the real
// values run past what int64 can hold.
type Checksum struct {
	Rows int64
	Hash string
}

// checksumOf hashes whatever `from` selects. It takes a FROM clause rather than a
// table name so the staging side can be deduplicated first.
func (s *Store) checksumOf(ctx context.Context, from string) (Checksum, error) {
	var c Checksum
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT count(*), COALESCE(sum(hashtextextended(
			region_id::text||order_id::text||price::text||volume_remain::text||issued::text, 0)), 0)::text
		FROM %s`, from)).Scan(&c.Rows, &c.Hash)
	return c, err
}

// VerifyRegion compares the live partition against the staging snapshot it was
// built from. A mismatch means the delta drifted and the region needs a resync.
//
// The staging side is deduplicated, because the apply deduplicates too. Comparing
// against raw staging would report drift on any cycle where ESI returned the same
// order twice, which it does when the page set shifts mid-fetch.
func (s *Store) VerifyRegion(ctx context.Context, regionID int64) (live, staged Checksum, err error) {
	live, err = s.checksumOf(ctx, fmt.Sprintf("%s.%s", Schema, PartitionName(regionID)))
	if err != nil {
		return live, staged, fmt.Errorf("checksum live: %w", err)
	}
	staged, err = s.checksumOf(ctx, fmt.Sprintf(
		`(SELECT DISTINCT ON (order_id) * FROM %s.%s ORDER BY order_id) dedup`,
		Schema, StagingName(regionID)))
	if err != nil {
		return live, staged, fmt.Errorf("checksum staging: %w", err)
	}
	return live, staged, nil
}

// ResyncRegion rebuilds a region from its staging table. This is the fallback
// when the checksum disagrees, and the path used for a first load. It costs no
// ESI tokens, because the snapshot is already staged.
//
// TRUNCATE takes ACCESS EXCLUSIVE on the partition, so this does block readers of
// that one region for the duration. lock_timeout keeps a waiting TRUNCATE from
// making readers queue behind it indefinitely.
func (s *Store) ResyncRegion(ctx context.Context, regionID int64) (int64, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '5s'"); err != nil {
		return 0, err
	}
	part := fmt.Sprintf("%s.%s", Schema, PartitionName(regionID))
	staging := fmt.Sprintf("%s.%s", Schema, StagingName(regionID))
	if _, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE %s`, part)); err != nil {
		return 0, fmt.Errorf("truncate %s: %w", part, err)
	}
	cols := strings.Join(quoteAll(orderCols), ", ")
	tag, err := tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (%s) SELECT DISTINCT ON (order_id) %s FROM %s ORDER BY order_id`,
		part, cols, cols, staging))
	if err != nil {
		return 0, fmt.Errorf("resync insert %s: %w", part, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func quoteAll(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = fmt.Sprintf("%q", c)
	}
	return out
}
