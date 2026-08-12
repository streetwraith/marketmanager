package store

import (
	"testing"

	"marketmanager/internal/region"
)

// The partition and staging names are derived from an int64 rather than
// interpolated from anything caller-supplied, which is what makes it safe to
// build the DDL with fmt.Sprintf.
func TestObjectNames(t *testing.T) {
	if got, want := PartitionName(region.TheForge), "orders_10000002"; got != want {
		t.Errorf("PartitionName = %q, want %q", got, want)
	}
	if got, want := StagingName(region.TheForge), "stg_10000002"; got != want {
		t.Errorf("StagingName = %q, want %q", got, want)
	}
}
