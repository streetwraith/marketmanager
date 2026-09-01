package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/getsentry/sentry-go"

	"marketmanager/internal/everef"
	"marketmanager/internal/region"
	"marketmanager/internal/store"
)

// HistoryConfig tunes the daily history import.
type HistoryConfig struct {
	// PollInterval is how often to check the index. EVE Ref publishes the new day
	// around 11:05 UTC and then keeps filling it for several days.
	PollInterval time.Duration
	// BackfillDays is how deep to go on first run.
	BackfillDays int
	// RecentDays is how far back to keep re-checking. A day file grows for about
	// four days; beyond this window a day is treated as final.
	RecentDays int
	// Spacing is the pause between day files, to stay polite to a best-effort
	// third-party service.
	Spacing time.Duration
}

// HistoryImporter keeps market.history in step with the EVE Ref dataset.
type HistoryImporter struct {
	client  *everef.Client
	store   *store.Store
	regions map[int64]bool
	cfg     HistoryConfig
	log     *slog.Logger

	// reports keeps one outage to one event per site; see streak.
	reports streak
}

// The history failure sites, used in the event tag and as the streak key.
const (
	jobTotals      = "totals"
	jobBookkeeping = "bookkeeping"
	jobImportDay   = "import-day"
)

func NewHistoryImporter(c *everef.Client, st *store.Store, regions []region.Region,
	cfg HistoryConfig, log *slog.Logger) *HistoryImporter {

	set := make(map[int64]bool, len(regions))
	for _, r := range regions {
		set[r.ID] = true
	}
	return &HistoryImporter{client: c, store: st, regions: set, cfg: cfg, log: log}
}

func (h *HistoryImporter) Run(ctx context.Context) error {
	// Run once at start so a fresh deployment does not wait for the first tick.
	h.once(ctx)

	t := time.NewTicker(h.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			h.log.Info("history importer stopped")
			return nil
		case <-t.C:
			h.once(ctx)
		}
	}
}

// once polls the index and imports whatever has changed.
//
// Errors are logged, never returned: a third-party outage must not tear down
// order ingestion, which is the service's primary job.
func (h *HistoryImporter) once(ctx context.Context) {
	start := time.Now()
	totals, err := h.client.Totals(ctx)
	if err != nil {
		if ctx.Err() == nil {
			h.log.Error("poll everef totals", "err", err)
			h.note(jobTotals, "", err)
		}
		return
	}
	h.reports.ends(jobTotals)
	stored, err := h.store.EverefDays(ctx)
	if err != nil {
		if ctx.Err() == nil {
			h.log.Error("load everef bookkeeping", "err", err)
			h.note(jobBookkeeping, "", err)
		}
		return
	}
	h.reports.ends(jobBookkeeping)

	due := h.selectDays(totals, stored)
	h.log.Debug("history poll", "days_in_index", len(totals), "days_stored", len(stored),
		"days_due", len(due), "poll_ms", time.Since(start).Milliseconds())
	if len(due) == 0 {
		return
	}
	h.log.Info("history import starting", "days", len(due))

	var imported, files int
	for _, day := range due {
		if ctx.Err() != nil {
			return
		}
		n, err := h.importDay(ctx, day, totals[day], stored[day].Watermark)
		if err != nil {
			if errors.Is(err, everef.ErrDayNotFound) {
				// Not published yet. It will appear on a later poll.
				continue
			}
			if ctx.Err() == nil {
				h.log.Error("import day", "day", day, "err", err)
				h.note(jobImportDay, day, err)
			}
			continue
		}
		h.reports.ends(jobImportDay)
		imported += int(n)
		files++
		if !sleep(ctx, h.cfg.Spacing) {
			return
		}
	}
	if files > 0 {
		rows, minDay, maxDay, err := h.store.HistoryCoverage(ctx)
		if err != nil {
			h.log.Warn("history coverage", "err", err)
		}
		h.log.Info("history import done",
			"files", files, "rows_upserted", imported,
			"total_rows", rows, "from", dayStr(minDay), "to", dayStr(maxDay),
			"total_ms", time.Since(start).Milliseconds())
	}
}

// note reports a failed history site, once per run of failures. EVE Ref is a
// best-effort third party polled every 15 minutes, so a day-long outage there
// would otherwise send 96 events that all say the same thing.
// day names the day file a failure concerns, and is empty for a site that does
// not have one.
func (h *HistoryImporter) note(job, day string, err error) {
	if !h.reports.first(job) {
		return
	}
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("component", "history")
		scope.SetTag("job", job)
		if day != "" {
			scope.SetContext("history", sentry.Context{"day": day})
		}
		sentry.CaptureException(err)
	})
}

// selectDays decides which days to fetch, oldest first.
//
// A day is fetched when the index reports more records than were last imported.
// There is deliberately no absolute completeness threshold: real day files sit
// well below a "complete" count for days, so any fixed number would either skip
// them forever or re-fetch settled days for no reason.
func (h *HistoryImporter) selectDays(totals map[string]int64, stored map[string]store.EverefDay) []string {
	// Truncated to midnight: day keys are dates, so carrying a time of day here
	// would silently clip the oldest day of the window.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	oldest := today.AddDate(0, 0, -h.cfg.BackfillDays)
	recent := today.AddDate(0, 0, -h.cfg.RecentDays)

	var due []string
	for _, day := range everef.Days(totals) {
		d, err := time.Parse(everef.DateFormat, day)
		if err != nil || d.Before(oldest) {
			continue
		}
		if totals[day] == 0 {
			continue // announced but empty so far
		}
		st, seen := stored[day]
		switch {
		case !seen:
			due = append(due, day) // never imported
		case d.After(recent) && totals[day] > st.TotalsCount:
			due = append(due, day) // still filling, and it grew
		}
	}
	return due
}

// importDay fetches one day and upserts the rows newer than the watermark.
func (h *HistoryImporter) importDay(ctx context.Context, day string, total int64, watermark time.Time) (int64, error) {
	d, err := time.Parse(everef.DateFormat, day)
	if err != nil {
		return 0, fmt.Errorf("bad day %q: %w", day, err)
	}

	fetchStart := time.Now()
	rc, err := h.client.OpenDay(ctx, d)
	if err != nil {
		return 0, err
	}
	//nolint:errcheck // the stream is fully read; a close error is not actionable
	defer rc.Close()

	rows, newWatermark, scanned, err := everef.Parse(rc, everef.Filter{
		Regions: h.regions,
		After:   watermark,
	})
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", day, err)
	}
	parseMS := time.Since(fetchStart).Milliseconds()

	writeStart := time.Now()
	n, err := h.store.ImportHistory(ctx, d, rows, total, newWatermark, time.Now())
	if err != nil {
		return 0, err
	}
	h.log.Info("imported day", "day", day,
		"scanned", scanned, "kept", len(rows), "upserted", n,
		"watermark", dayTimeStr(newWatermark),
		"fetch_parse_ms", parseMS, "write_ms", time.Since(writeStart).Milliseconds())
	return n, nil
}

func dayStr(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(everef.DateFormat)
}

func dayTimeStr(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
