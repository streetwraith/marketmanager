//go:build integration

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// BenchmarkCopyOrders measures staging throughput, which sets how fast a fetched
// snapshot can reach the database. Run with:
//
//	MM_TEST_DSN=... go test -tags integration -bench CopyOrders -benchtime 1x ./internal/store/
func BenchmarkCopyOrders(b *testing.B) {
	st := testStore(b)
	ctx := context.Background()
	regions := []region.Region{{ID: testRegion, Name: "BENCH", Priority: region.Rest}}
	if err := st.EnsureRegionObjects(ctx, regions); err != nil {
		b.Fatalf("EnsureRegionObjects: %v", err)
	}
	b.Cleanup(func() {
		_, _ = st.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, Schema, PartitionName(testRegion)))
		_, _ = st.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, Schema, StagingName(testRegion)))
		_, _ = st.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.region_status WHERE region_id=$1`, Schema), testRegion)
	})

	const n = 400_000
	orders := make([]esi.Order, n)
	for i := range orders {
		orders[i] = esi.Order{
			OrderID: int64(i + 1), TypeID: 34, LocationID: 60003760, SystemID: 30000142,
			IsBuyOrder: i%2 == 0, Price: json.Number(fmt.Sprintf("%d.%02d", i%100000, i%100)),
			VolumeRemain: int64(i % 5000), VolumeTotal: 5000, MinVolume: 1, Duration: 90,
			Range: "region", Issued: time.Date(2026, 6, 12, 14, 58, 8, 0, time.UTC),
		}
	}

	for b.Loop() {
		// Feed in page-sized batches, exactly as a sweep does.
		ch := make(chan []esi.Order, 8)
		go func() {
			for i := 0; i < n; i += 1000 {
				ch <- orders[i:min(i+1000, n)]
			}
			close(ch)
		}()
		start := time.Now()
		copied, err := st.CopyOrders(ctx, testRegion, ch)
		if err != nil {
			b.Fatalf("CopyOrders: %v", err)
		}
		elapsed := time.Since(start)
		b.ReportMetric(float64(copied)/elapsed.Seconds(), "rows/s")
		b.ReportMetric(elapsed.Seconds(), "secs")
	}
}
