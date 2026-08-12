package everef

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Row is one (region, type, day) history record.
//
// The ISK columns stay as raw text so they reach numeric(20,2) undamaged, the
// same reason order prices are not decoded to float64.
type Row struct {
	RegionID         int64
	TypeID           int64
	Date             time.Time
	Average          string
	Highest          string
	Lowest           string
	Volume           int64
	OrderCount       int64
	HTTPLastModified time.Time
}

// wantCols are the columns the parser needs. The file also carries no others
// today, but they are looked up by name so a new column cannot shift the parse.
var wantCols = []string{
	"average", "date", "highest", "lowest", "order_count", "volume",
	"http_last_modified", "region_id", "type_id",
}

// Filter decides which rows to keep.
type Filter struct {
	// Regions limits the import to the regions this service tracks. EVE Ref
	// publishes all ~77 regions, of which ours are about 70% of each file.
	Regions map[int64]bool
	// After keeps only rows scraped since the last import. This is what makes a
	// re-import cheap: a day file grows in waves, and without this filter each
	// re-import rewrites every row it already holds.
	After time.Time
}

// Parse reads a day file and returns the rows that pass the filter, plus the
// highest http_last_modified seen across the whole file.
//
// The watermark comes from every row, not just the kept ones, so a file whose new
// rows are all for untracked regions still advances the watermark and is not
// re-read next time.
func Parse(r io.Reader, f Filter) (rows []Row, watermark time.Time, total int, err error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true

	header, err := cr.Read()
	if err != nil {
		return nil, watermark, 0, fmt.Errorf("read header: %w", err)
	}
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[name] = i
	}
	for _, c := range wantCols {
		if _, ok := idx[c]; !ok {
			return nil, watermark, 0, fmt.Errorf("missing column %q in header %v", c, header)
		}
	}

	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, watermark, total, fmt.Errorf("read record %d: %w", total, err)
		}
		total++

		lm, err := time.Parse(time.RFC3339, rec[idx["http_last_modified"]])
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad http_last_modified: %w", total, err)
		}
		if lm.After(watermark) {
			watermark = lm
		}

		regionID, err := strconv.ParseInt(rec[idx["region_id"]], 10, 64)
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad region_id: %w", total, err)
		}
		if f.Regions != nil && !f.Regions[regionID] {
			continue
		}
		if !f.After.IsZero() && !lm.After(f.After) {
			continue
		}

		date, err := time.Parse(DateFormat, rec[idx["date"]])
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad date: %w", total, err)
		}
		typeID, err := strconv.ParseInt(rec[idx["type_id"]], 10, 64)
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad type_id: %w", total, err)
		}
		volume, err := strconv.ParseInt(rec[idx["volume"]], 10, 64)
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad volume: %w", total, err)
		}
		orderCount, err := strconv.ParseInt(rec[idx["order_count"]], 10, 64)
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad order_count: %w", total, err)
		}
		// The ISK columns are carried as text so they reach numeric(20,2) exactly,
		// but they are still third-party input. Validating here fails the record
		// that is actually wrong, rather than failing the whole day at the COPY
		// with an error that names neither the row nor the column.
		avg, err := iskColumn(rec[idx["average"]])
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad average: %w", total, err)
		}
		high, err := iskColumn(rec[idx["highest"]])
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad highest: %w", total, err)
		}
		low, err := iskColumn(rec[idx["lowest"]])
		if err != nil {
			return nil, watermark, total, fmt.Errorf("record %d: bad lowest: %w", total, err)
		}

		rows = append(rows, Row{
			RegionID: regionID, TypeID: typeID, Date: date,
			Average: avg, Highest: high, Lowest: low,
			Volume: volume, OrderCount: orderCount, HTTPLastModified: lm,
		})
	}
	return rows, watermark, total, nil
}

// iskColumn checks that s is a decimal the destination column can hold, and
// returns it unchanged so no precision is lost on the way to the database.
func iskColumn(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty")
	}
	// ParseFloat accepts exactly the syntax numeric does, minus the exponent and
	// special forms that would surprise a price column.
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return "", fmt.Errorf("%q is not a decimal", s)
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' && r != '-' && r != '+' {
			return "", fmt.Errorf("%q contains %q", s, r)
		}
	}
	// numeric(20,2) holds 18 digits before the point.
	if i := strings.IndexByte(s, '.'); i >= 0 {
		if len(strings.TrimLeft(s[:i], "+-")) > 18 {
			return "", fmt.Errorf("%q exceeds numeric(20,2)", s)
		}
	} else if len(strings.TrimLeft(s, "+-")) > 18 {
		return "", fmt.Errorf("%q exceeds numeric(20,2)", s)
	}
	return s, nil
}

// Days returns the day keys of totals, sorted oldest first.
func Days(totals map[string]int64) []string {
	days := make([]string, 0, len(totals))
	for d := range totals {
		days = append(days, d)
	}
	slices.Sort(days)
	return days
}
