package everef

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const header = "average,date,highest,lowest,order_count,volume,http_last_modified,region_id,type_id\n"

// All fixtures describe the same trading day; only the scrape time varies.
const fixtureDay = "2026-08-05"

func row(lm string, region, typeID int64, avg string) string {
	return fmt.Sprintf("%s,%s,9.9,1.1,10,1000,%s,%d,%d\n", avg, fixtureDay, lm, region, typeID)
}

func mustParse(t *testing.T, s string, f Filter) ([]Row, time.Time, int) {
	t.Helper()
	rows, wm, total, err := Parse(strings.NewReader(s), f)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return rows, wm, total
}

func TestParseKeepsOnlyTrackedRegions(t *testing.T) {
	// EVE Ref publishes ~77 regions; only ours are wanted, about 70% of a file.
	in := header +
		row("2026-08-06T11:04:33Z", 10000002, 34, "3.91") +
		row("2026-08-06T11:04:33Z", 10000060, 34, "4.00") + // Delve, untracked
		row("2026-08-06T11:04:33Z", 19000001, 44992, "5500000.00")

	rows, _, total := mustParse(t, in, Filter{Regions: map[int64]bool{10000002: true, 19000001: true}})
	if total != 3 {
		t.Errorf("scanned %d records, want 3", total)
	}
	if len(rows) != 2 {
		t.Fatalf("kept %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		if r.RegionID == 10000060 {
			t.Error("kept an untracked region")
		}
	}
}

// This filter is what stops a re-import rewriting rows already stored. A day file
// fills in waves over about four days.
func TestParseWatermarkKeepsOnlyNewWaves(t *testing.T) {
	in := header +
		row("2026-08-06T11:04:33Z", 10000002, 34, "1.00") +
		row("2026-08-07T11:02:41Z", 10000002, 35, "2.00") +
		row("2026-08-08T11:02:44Z", 10000002, 36, "3.00")

	all := map[int64]bool{10000002: true}
	rows, wm, _ := mustParse(t, in, Filter{Regions: all})
	if len(rows) != 3 {
		t.Errorf("with no watermark kept %d rows, want 3", len(rows))
	}
	want := time.Date(2026, 8, 8, 11, 2, 44, 0, time.UTC)
	if !wm.Equal(want) {
		t.Errorf("watermark = %v, want %v", wm, want)
	}

	// Re-import after the first two waves: only the third is new.
	after := time.Date(2026, 8, 7, 11, 2, 41, 0, time.UTC)
	rows, wm2, _ := mustParse(t, in, Filter{Regions: all, After: after})
	if len(rows) != 1 {
		t.Fatalf("kept %d rows after the watermark, want 1", len(rows))
	}
	if rows[0].TypeID != 36 {
		t.Errorf("kept type %d, want 36", rows[0].TypeID)
	}
	// The watermark must still advance from every row, not just the kept ones.
	if !wm2.Equal(want) {
		t.Errorf("watermark = %v, want %v", wm2, want)
	}
}

// A file whose new rows are all for untracked regions must still advance the
// watermark, or it would be re-read forever.
func TestParseWatermarkAdvancesOnUntrackedRows(t *testing.T) {
	in := header +
		row("2026-08-09T11:00:00Z", 10000060, 34, "1.00") // untracked only

	rows, wm, _ := mustParse(t, in, Filter{Regions: map[int64]bool{10000002: true}})
	if len(rows) != 0 {
		t.Errorf("kept %d rows, want 0", len(rows))
	}
	if wm.IsZero() {
		t.Error("watermark did not advance; the file would be re-read forever")
	}
}

// ISK must reach numeric(20,2) as written, never through a float.
func TestParseKeepsPriceText(t *testing.T) {
	in := header + row("2026-08-06T11:04:33Z", 10000002, 44992, "5500000.55")
	rows, _, _ := mustParse(t, in, Filter{Regions: map[int64]bool{10000002: true}})
	if len(rows) != 1 {
		t.Fatalf("kept %d rows, want 1", len(rows))
	}
	if rows[0].Average != "5500000.55" {
		t.Errorf("Average = %q, want the literal preserved", rows[0].Average)
	}
}

func TestParseRejectsMissingColumn(t *testing.T) {
	in := "average,date,highest\n1,2026-08-05,2\n"
	if _, _, _, err := Parse(strings.NewReader(in), Filter{}); err == nil {
		t.Fatal("expected an error for a truncated header")
	}
}

func TestParseRejectsBadRecord(t *testing.T) {
	in := header + "1.0,not-a-date,9.9,1.1,10,1000,2026-08-06T11:04:33Z,10000002,34\n"
	_, _, _, err := Parse(strings.NewReader(in), Filter{Regions: map[int64]bool{10000002: true}})
	if err == nil {
		t.Fatal("expected an error for an unparsable date")
	}
	if !strings.Contains(err.Error(), "date") {
		t.Errorf("error = %q, want it to name the bad column", err)
	}
}

func TestDaysAreSortedOldestFirst(t *testing.T) {
	got := Days(map[string]int64{"2026-08-09": 1, "2026-08-05": 2, "2026-08-07": 3})
	want := []string{"2026-08-05", "2026-08-07", "2026-08-09"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Days() = %v, want %v", got, want)
		}
	}
}

// The ISK columns are third-party input reaching a numeric(20,2) column. A bad
// value must fail the record that is wrong, not the whole day at the COPY.
func TestParseRejectsBadISK(t *testing.T) {
	tests := []struct {
		name string
		avg  string
	}{
		{"empty", ""},
		{"not a number", "abc"},
		{"scientific notation", "1e999"},
		{"infinity", "Inf"},
		{"not a number literal", "NaN"},
		{"hex", "0x10"},
		{"too many digits for numeric(20,2)", "1234567890123456789012.00"},
		{"injection-shaped", "1.0); DROP TABLE market.history --"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := header + row("2026-08-06T11:04:33Z", 10000002, 34, tc.avg)
			_, _, _, err := Parse(strings.NewReader(in), Filter{Regions: map[int64]bool{10000002: true}})
			if err == nil {
				t.Fatalf("Parse accepted average=%q", tc.avg)
			}
			if !strings.Contains(err.Error(), "average") {
				t.Errorf("error = %q, want it to name the offending column", err)
			}
		})
	}
}

func TestParseAcceptsRealISK(t *testing.T) {
	for _, v := range []string{"0.01", "3.91", "5500000.00", "999999999999999999.99", "-1.50"} {
		in := header + row("2026-08-06T11:04:33Z", 10000002, 34, v)
		rows, _, _, err := Parse(strings.NewReader(in), Filter{Regions: map[int64]bool{10000002: true}})
		if err != nil {
			t.Errorf("Parse rejected a valid price %q: %v", v, err)
			continue
		}
		// The text must survive unchanged, or precision is lost before the database.
		if rows[0].Average != v {
			t.Errorf("Average = %q, want %q unchanged", rows[0].Average, v)
		}
	}
}
