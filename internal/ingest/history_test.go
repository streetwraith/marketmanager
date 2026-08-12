package ingest

import (
	"slices"
	"testing"
	"time"

	"marketmanager/internal/everef"
	"marketmanager/internal/store"
)

func testImporter(backfill, recent int) *HistoryImporter {
	return &HistoryImporter{
		cfg: HistoryConfig{BackfillDays: backfill, RecentDays: recent},
		log: discardLogger(),
	}
}

func day(offset int) string {
	return time.Now().UTC().AddDate(0, 0, offset).Format(everef.DateFormat)
}

func TestSelectDays(t *testing.T) {
	yesterday, twoDays, old, ancient := day(-1), day(-2), day(-30), day(-900)

	tests := []struct {
		name   string
		totals map[string]int64
		stored map[string]store.EverefDay
		want   []string
	}{
		{
			name:   "a day never imported is selected",
			totals: map[string]int64{yesterday: 22369},
			stored: nil,
			want:   []string{yesterday},
		},
		{
			// The plan's original rule was a fixed threshold near 40,000. Real files
			// sit at ~19.5k for days, so that rule would never import them.
			name:   "a recent day well below any completeness threshold is still selected",
			totals: map[string]int64{yesterday: 19423},
			stored: nil,
			want:   []string{yesterday},
		},
		{
			name:   "a recent day that grew is re-selected",
			totals: map[string]int64{twoDays: 47587},
			stored: map[string]store.EverefDay{twoDays: {TotalsCount: 22369}},
			want:   []string{twoDays},
		},
		{
			name:   "a recent day that did not grow is skipped",
			totals: map[string]int64{twoDays: 22369},
			stored: map[string]store.EverefDay{twoDays: {TotalsCount: 22369}},
			want:   nil,
		},
		{
			// Beyond the recent window a day is final: files stop growing after
			// about four days, so re-checking forever would waste bandwidth.
			name:   "an old day that grew is not re-selected",
			totals: map[string]int64{old: 99999},
			stored: map[string]store.EverefDay{old: {TotalsCount: 47000}},
			want:   nil,
		},
		{
			name:   "an old day never imported is still backfilled",
			totals: map[string]int64{old: 47000},
			stored: nil,
			want:   []string{old},
		},
		{
			name:   "a day older than the backfill depth is ignored",
			totals: map[string]int64{ancient: 47000},
			stored: nil,
			want:   nil,
		},
		{
			name:   "an announced but empty day is skipped",
			totals: map[string]int64{yesterday: 0},
			stored: nil,
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := testImporter(730, 10)
			got := h.selectDays(tc.totals, tc.stored)
			if !slices.Equal(got, tc.want) {
				t.Errorf("selectDays = %v, want %v", got, tc.want)
			}
		})
	}
}

// Oldest first, so a backfill fills history forward in time and an interrupted
// run leaves a contiguous range rather than holes.
func TestSelectDaysIsOrderedOldestFirst(t *testing.T) {
	h := testImporter(730, 10)
	totals := map[string]int64{day(-1): 100, day(-5): 100, day(-3): 100}
	got := h.selectDays(totals, nil)
	want := []string{day(-5), day(-3), day(-1)}
	if !slices.Equal(got, want) {
		t.Errorf("selectDays = %v, want %v", got, want)
	}
}
