package store

import (
	"context"
	"fmt"

	"marketmanager/internal/region"
)

// empireRegionsSQL derives the ingestible region set from the SDE rather than
// hardcoding it, because new empire regions do appear (Exordium did).
//
// This also returns GPMR-01 (19000001), the global PLEX market, whose single
// system has security 1.0. That is intentional: it is a real market we want, it
// costs 1 page, and its orders are disjoint from every normal region feed.
const empireRegionsSQL = `
SELECT r._key, r.name_en
FROM sde.map_regions r
JOIN sde.map_solar_systems s ON s.region_id = r._key
GROUP BY r._key, r.name_en
HAVING max(s.security_status) >= 0.05
ORDER BY r._key`

// Regions resolves the region set from the SDE and assigns fetch priorities.
func (s *Store) Regions(ctx context.Context) ([]region.Region, error) {
	rows, err := s.pool.Query(ctx, empireRegionsSQL)
	if err != nil {
		return nil, fmt.Errorf("query sde regions: %w", err)
	}
	defer rows.Close()

	var out []region.Region
	for rows.Next() {
		var r region.Region
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			return nil, fmt.Errorf("scan region: %w", err)
		}
		r.Priority = region.Priority(r.ID)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate regions: %w", err)
	}
	if len(out) == 0 {
		// An empty set means the sde schema is missing or unreadable. Ingesting
		// nothing silently would look like a healthy service doing no work.
		return nil, fmt.Errorf("sde returned no regions; is the sde schema present and readable?")
	}
	return out, nil
}

// EnsureRegionObjects creates everything a region needs: its partition of
// market.orders, its staging table, and its region_status row.
//
// These are deliberately one call. A region whose partition exists but whose
// staging table does not fails only at its first cycle, which is a slow and
// confusing way to learn about a missing setup step.
func (s *Store) EnsureRegionObjects(ctx context.Context, regions []region.Region) error {
	for _, r := range regions {
		// Both names derive from an int64, so they cannot inject.
		part := PartitionName(r.ID)
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s.orders FOR VALUES IN (%d)`,
			Schema, part, Schema, r.ID,
		)); err != nil {
			return fmt.Errorf("create partition %s: %w", part, err)
		}
		if _, err := s.pool.Exec(ctx, fmt.Sprintf(
			`CREATE UNLOGGED TABLE IF NOT EXISTS %s.%s (LIKE %s.orders)`,
			Schema, StagingName(r.ID), Schema,
		)); err != nil {
			return fmt.Errorf("create staging for %d: %w", r.ID, err)
		}
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO `+Schema+`.region_status (region_id, region_name, priority)
			VALUES ($1, $2, $3)
			ON CONFLICT (region_id) DO UPDATE
			SET region_name = EXCLUDED.region_name, priority = EXCLUDED.priority`,
			r.ID, r.Name, r.Priority,
		); err != nil {
			return fmt.Errorf("seed region_status %d: %w", r.ID, err)
		}
	}
	return nil
}
