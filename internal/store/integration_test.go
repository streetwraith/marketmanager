//go:build integration

// Integration tests need a real Postgres with the market schema present. Run with:
//
//	MM_TEST_DSN="postgres://marketmanager:marketmanager@127.0.0.1:5432/eve?sslmode=disable" \
//	  go test -tags integration ./internal/store/
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/everef"
	"marketmanager/internal/region"
)

// testRegion is not a real EVE region, so these tests can never disturb ingested data.
const testRegion int64 = 99999999

// testStore takes testing.TB so tests and benchmarks share one opener.
func testStore(tb testing.TB) *Store {
	tb.Helper()
	dsn := os.Getenv("MM_TEST_DSN")
	if dsn == "" {
		tb.Skip("MM_TEST_DSN not set")
	}
	st, err := Open(context.Background(), dsn)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(st.Close)
	return st
}

// setupTestRegion gives the test its own partition and staging table, and removes
// them afterwards.
func setupTestRegion(t *testing.T, st *Store) {
	t.Helper()
	ctx := context.Background()
	regions := []region.Region{{ID: testRegion, Name: "TEST", Priority: region.Rest}}

	drop := func() {
		for _, q := range []string{
			fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, Schema, PartitionName(testRegion)),
			fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, Schema, StagingName(testRegion)),
		} {
			if _, err := st.pool.Exec(ctx, q); err != nil {
				t.Fatalf("cleanup %q: %v", q, err)
			}
		}
		_, _ = st.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.region_status WHERE region_id=$1`, Schema), testRegion)
	}
	drop()
	if err := st.EnsureRegionObjects(ctx, regions); err != nil {
		t.Fatalf("EnsureRegionObjects: %v", err)
	}
	t.Cleanup(drop)
}

func order(id int64, price string, vol int64) esi.Order {
	return esi.Order{
		OrderID: id, TypeID: 34, LocationID: 60003760, SystemID: 30000142,
		IsBuyOrder: false, Price: json.Number(price), VolumeRemain: vol, VolumeTotal: 100,
		MinVolume: 1, Duration: 90, Range: "region",
		Issued: time.Date(2026, 6, 12, 14, 58, 8, 0, time.UTC),
	}
}

// stage streams orders into the region's staging table, as a sweep would.
func stage(t *testing.T, st *Store, orders []esi.Order) {
	t.Helper()
	ch := make(chan []esi.Order, 1)
	ch <- orders
	close(ch)
	if _, err := st.CopyOrders(context.Background(), testRegion, ch); err != nil {
		t.Fatalf("CopyOrders: %v", err)
	}
}

// assertLiveMatchesStaging is the real assertion: the live partition must be row
// for row identical to the snapshot it was built from.
func assertLiveMatchesStaging(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	part := fmt.Sprintf("%s.%s", Schema, PartitionName(testRegion))
	staging := fmt.Sprintf("%s.%s", Schema, StagingName(testRegion))
	cols := `region_id, order_id, type_id, location_id, system_id, is_buy_order, price,
	         volume_remain, volume_total, min_volume, duration, "range", issued`

	// Each branch is parenthesised, or the ORDER BY would bind to the set operation.
	var liveOnly, stageOnly int
	q := fmt.Sprintf(`
		SELECT (SELECT count(*) FROM (
		            (SELECT %[1]s FROM %[2]s)
		            EXCEPT ALL
		            (SELECT DISTINCT ON (order_id) %[1]s FROM %[3]s ORDER BY order_id)) a),
		       (SELECT count(*) FROM (
		            (SELECT DISTINCT ON (order_id) %[1]s FROM %[3]s ORDER BY order_id)
		            EXCEPT ALL
		            (SELECT %[1]s FROM %[2]s)) b)`, cols, part, staging)
	if err := st.pool.QueryRow(ctx, q).Scan(&liveOnly, &stageOnly); err != nil {
		t.Fatalf("compare: %v", err)
	}
	if liveOnly != 0 || stageOnly != 0 {
		t.Errorf("live and staging disagree: %d rows only in live, %d only in staging", liveOnly, stageOnly)
	}

	live, staged, err := st.VerifyRegion(ctx, testRegion)
	if err != nil {
		t.Fatalf("VerifyRegion: %v", err)
	}
	if live.Rows != staged.Rows || live.Hash != staged.Hash {
		t.Errorf("checksum drift: live=%+v staged=%+v", live, staged)
	}
}

func TestDeltaAppliesChurn(t *testing.T) {
	st := testStore(t)
	setupTestRegion(t, st)
	ctx := context.Background()

	// First load takes the resync path, as a cold start would.
	initial := []esi.Order{
		order(1, "100.00", 10), order(2, "200.00", 20),
		order(3, "300.00", 30), order(4, "400.00", 40),
	}
	stage(t, st, initial)
	n, err := st.ResyncRegion(ctx, testRegion)
	if err != nil {
		t.Fatalf("ResyncRegion: %v", err)
	}
	if n != 4 {
		t.Fatalf("resync inserted %d rows, want 4", n)
	}
	assertLiveMatchesStaging(ctx, t, st)

	// Next cycle: one removed, one added, one repriced, one untouched.
	next := []esi.Order{
		order(1, "100.00", 10), // unchanged
		order(2, "222.50", 20), // price changed
		order(4, "400.00", 44), // volume changed
		order(5, "500.00", 50), // added
		// order 3 is gone
	}
	stage(t, st, next)

	res, err := st.ApplyDelta(ctx, testRegion, "64MB")
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if res.Inserted != 1 || res.Updated != 2 || res.Deleted != 1 {
		t.Errorf("delta = +%d ~%d -%d, want +1 ~2 -1", res.Inserted, res.Updated, res.Deleted)
	}
	if res.Duplicates != 0 {
		t.Errorf("Duplicates = %d, want 0", res.Duplicates)
	}
	assertLiveMatchesStaging(ctx, t, st)
}

// An unchanged snapshot must produce no writes at all. This is what makes the
// whole approach cheap: ~99.5% of rows are untouched every cycle.
func TestDeltaOnUnchangedSnapshotWritesNothing(t *testing.T) {
	st := testStore(t)
	setupTestRegion(t, st)
	ctx := context.Background()

	orders := []esi.Order{order(1, "100.00", 10), order(2, "200.00", 20), order(3, "300.00", 30)}
	stage(t, st, orders)
	if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
		t.Fatalf("ResyncRegion: %v", err)
	}

	stage(t, st, orders)
	res, err := st.ApplyDelta(ctx, testRegion, "64MB")
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if res.Inserted != 0 || res.Updated != 0 || res.Deleted != 0 {
		t.Errorf("delta on an identical snapshot = +%d ~%d -%d, want all zero",
			res.Inserted, res.Updated, res.Deleted)
	}
	assertLiveMatchesStaging(ctx, t, st)
}

// ESI can return the same order on two pages when the page set shifts mid-fetch.
// Without deduplication a duplicated new order violates the primary key and
// aborts the whole cycle.
func TestDeltaSurvivesDuplicateOrderIDs(t *testing.T) {
	st := testStore(t)
	setupTestRegion(t, st)
	ctx := context.Background()

	stage(t, st, []esi.Order{order(1, "100.00", 10)})
	if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
		t.Fatalf("ResyncRegion: %v", err)
	}

	// Order 2 is new and duplicated; order 1 exists and is duplicated with a
	// changed price. Both cases would break an unguarded apply.
	stage(t, st, []esi.Order{
		order(1, "111.00", 10), order(1, "111.00", 10),
		order(2, "200.00", 20), order(2, "200.00", 20),
	})

	res, err := st.ApplyDelta(ctx, testRegion, "64MB")
	if err != nil {
		t.Fatalf("ApplyDelta with duplicates: %v", err)
	}
	if res.Duplicates != 2 {
		t.Errorf("Duplicates = %d, want 2", res.Duplicates)
	}
	if res.Inserted != 1 || res.Updated != 1 {
		t.Errorf("delta = +%d ~%d, want +1 ~1", res.Inserted, res.Updated)
	}
	assertLiveMatchesStaging(ctx, t, st)
}

// Price must survive the trip to numeric(20,2) exactly.
func TestPricePrecision(t *testing.T) {
	st := testStore(t)
	setupTestRegion(t, st)
	ctx := context.Background()

	stage(t, st, []esi.Order{
		order(1, "5500000.00", 1), order(2, "0.01", 1),
		order(3, "9999999999999.99", 1), order(4, "1234.56", 1),
	})
	if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
		t.Fatalf("ResyncRegion: %v", err)
	}

	rows, err := st.pool.Query(ctx, fmt.Sprintf(
		`SELECT order_id, price::text FROM %s.%s ORDER BY order_id`,
		Schema, PartitionName(testRegion)))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	want := map[int64]string{1: "5500000.00", 2: "0.01", 3: "9999999999999.99", 4: "1234.56"}
	for rows.Next() {
		var id int64
		var price string
		if err := rows.Scan(&id, &price); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if price != want[id] {
			t.Errorf("order %d price = %s, want %s", id, price, want[id])
		}
	}
}

// The drift guard must actually notice when the live partition and the snapshot
// it was built from disagree. It is the only thing standing between a delta bug
// and silently wrong data.
func TestVerifyRegionDetectsDrift(t *testing.T) {
	st := testStore(t)
	setupTestRegion(t, st)
	ctx := context.Background()

	stage(t, st, []esi.Order{order(1, "100.00", 10), order(2, "200.00", 20)})
	if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
		t.Fatalf("ResyncRegion: %v", err)
	}
	live, staged, err := st.VerifyRegion(ctx, testRegion)
	if err != nil {
		t.Fatalf("VerifyRegion: %v", err)
	}
	if live.Rows != staged.Rows || live.Hash != staged.Hash {
		t.Fatalf("reported drift on a clean load: live=%+v staged=%+v", live, staged)
	}

	tests := []struct {
		name    string
		corrupt string
	}{
		{"a changed value", `UPDATE market.orders_99999999 SET price = 999.99 WHERE order_id = 1`},
		{"a missing row", `DELETE FROM market.orders_99999999 WHERE order_id = 1`},
		{"an extra row", `INSERT INTO market.orders_99999999
			SELECT region_id, 999, type_id, location_id, system_id, is_buy_order, price,
			       volume_remain, volume_total, min_volume, duration, "range", issued
			FROM market.orders_99999999 WHERE order_id = 2`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Rebuild a clean copy, then damage it in one specific way.
			if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
				t.Fatalf("ResyncRegion: %v", err)
			}
			if _, err := st.pool.Exec(ctx, tc.corrupt); err != nil {
				t.Fatalf("corrupt: %v", err)
			}
			live, staged, err := st.VerifyRegion(ctx, testRegion)
			if err != nil {
				t.Fatalf("VerifyRegion: %v", err)
			}
			if live.Rows == staged.Rows && live.Hash == staged.Hash {
				t.Errorf("drift not detected: live=%+v staged=%+v", live, staged)
			}
			// And the resync must repair it.
			if _, err := st.ResyncRegion(ctx, testRegion); err != nil {
				t.Fatalf("repair ResyncRegion: %v", err)
			}
			assertLiveMatchesStaging(ctx, t, st)
		})
	}
}

// Partitions used to be created once at start-up, so the first row of a new year
// had no home and every import failed until someone restarted the service.
func TestImportHistoryCreatesTheYearPartition(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// A year far enough out that no start-up call would have covered it.
	future := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	part := fmt.Sprintf("%s.history_%d", Schema, future.Year())
	drop := func() { _, _ = st.pool.Exec(ctx, "DROP TABLE IF EXISTS "+part) }
	drop()
	t.Cleanup(drop)

	rows := []everef.Row{{
		RegionID: testRegion, TypeID: 34, Date: future,
		Average: "1.00", Highest: "2.00", Lowest: "0.50",
		Volume: 10, OrderCount: 2, HTTPLastModified: future,
	}}
	n, err := st.ImportHistory(ctx, future, rows, 1, future, time.Now())
	if err != nil {
		t.Fatalf("ImportHistory into an uncreated year: %v", err)
	}
	if n != 1 {
		t.Errorf("upserted %d rows, want 1", n)
	}

	var got int
	if err := st.pool.QueryRow(ctx,
		"SELECT count(*) FROM "+part+" WHERE region_id=$1", testRegion).Scan(&got); err != nil {
		t.Fatalf("the year partition was not created: %v", err)
	}
	if got != 1 {
		t.Errorf("partition holds %d rows, want 1", got)
	}
	_, _ = st.pool.Exec(ctx, "DELETE FROM market.history WHERE region_id=$1", testRegion)
}
