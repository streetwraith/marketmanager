package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
	"marketmanager/internal/store"
)

// Outcomes recorded in market.ingest_log.
const (
	OutcomeOK           = "ok"
	OutcomeResync       = "resync"
	OutcomeDeferred     = "deferred"
	OutcomeInconsistent = "inconsistent"
	OutcomeFailed       = "failed"
)

// Timings are the per-phase durations of one cycle, in milliseconds. FetchMS and
// CopyMS overlap almost entirely: pages stream into Postgres while later pages
// are still in flight, so they are two views of the same window rather than
// stages to add together.
type Timings struct {
	FetchMS  int
	CopyMS   int
	ApplyMS  int
	VerifyMS int
	TotalMS  int
}

// Result summarises one region cycle.
type Result struct {
	Region  region.Region
	Outcome string
	Meta    SweepMeta
	Delta   store.DeltaResult
	Staged  int64
	Timings Timings
	// Err is set when the cycle did not publish. It is reported rather than
	// returned, because a failed cycle is normal: the previous snapshot stays
	// live and the next Expires tick is minutes away, so it must not stop the
	// scheduler. Callers are expected to log it.
	Err error
}

// Cycle runs one region's refresh: fetch, stage, apply, verify.
type Cycle struct {
	fetcher *Fetcher
	store   *store.Store
	client  *esi.Client
	workMem string
	log     *slog.Logger

	// verify is the drift guard, injectable so the repair path can be tested.
	// Correct delta SQL is self-correcting, so genuine drift cannot be induced
	// from outside; without this seam the branch that repairs it could only ever
	// be checked by reading it.
	verify func(ctx context.Context, regionID int64) (live, staged store.Checksum, err error)
}

func NewCycle(f *Fetcher, st *store.Store, c *esi.Client, workMem string, log *slog.Logger) *Cycle {
	return &Cycle{
		fetcher: f, store: st, client: c, workMem: workMem, log: log,
		verify: st.VerifyRegion,
	}
}

// Run refreshes one region.
//
// Nothing becomes visible until the apply commits, so a failure at any earlier
// step leaves the previous snapshot live and costs nothing. The cycle is always
// recorded in ingest_log, whatever the outcome.
func (c *Cycle) Run(ctx context.Context, r region.Region) (Result, error) {
	started := time.Now()
	res := Result{Region: r, Outcome: OutcomeFailed}
	entry := store.IngestLogEntry{RegionID: r.ID, CycleStartedAt: started}

	// The sweep streams pages while CopyOrders consumes them, so a region is never
	// buffered whole in memory. Its context is cancellable so that a database
	// failure stops the fetch instead of spending tokens on pages nobody will store.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	defer cancelSweep()

	ch := make(chan []esi.Order, 8)
	type sweepOut struct {
		meta *SweepMeta
		err  error
	}
	sweepDone := make(chan sweepOut, 1)
	go func() {
		meta, err := c.fetcher.Sweep(sweepCtx, r, ch)
		sweepDone <- sweepOut{meta, err}
	}()

	copyStart := time.Now()
	staged, copyErr := c.store.CopyOrders(ctx, r.ID, ch)
	copyElapsed := time.Since(copyStart)
	if copyErr != nil {
		cancelSweep()
	}
	sw := <-sweepDone

	res.Meta = *sw.meta
	res.Staged = staged
	entry.Pages = sw.meta.Pages
	entry.TokensSpent = sw.meta.TokensSpent
	res.Timings.FetchMS = int(sw.meta.Duration.Milliseconds())
	res.Timings.CopyMS = int(copyElapsed.Milliseconds())
	entry.FetchMS = res.Timings.FetchMS
	entry.CopyMS = res.Timings.CopyMS
	c.log.Debug("staged region", "region", r.Name,
		"pages", sw.meta.Pages, "orders", sw.meta.OrderCount, "staged", staged,
		"fetch_ms", res.Timings.FetchMS, "copy_ms", res.Timings.CopyMS)
	if rem, ok := c.client.Budget.Remaining(); ok {
		entry.RemainingAfter = rem
	}

	if err := errors.Join(sw.err, copyErr); err != nil {
		res.Timings.TotalMS = int(time.Since(started).Milliseconds())
		c.recordFailure(ctx, r, err, &res, &entry)
		return res, nil
	}

	// Everything fetched must have reached staging. Without this check a row lost
	// in between would be invisible: the delta would faithfully delete the
	// "missing" orders from the live table, and the checksum guard would still
	// agree, because it compares live against staging rather than against what
	// ESI actually sent.
	if staged != int64(sw.meta.OrderCount) {
		err := fmt.Errorf("staged %d rows but fetched %d orders", staged, sw.meta.OrderCount)
		c.recordFailure(ctx, r, err, &res, &entry)
		return res, err
	}

	// A region with no rows has never loaded, so there is no delta to compute.
	count, err := c.store.RegionRowCount(ctx, r.ID)
	if err != nil {
		return c.fail(ctx, r, &res, &entry, err)
	}

	applyStart := time.Now()
	if count == 0 {
		n, err := c.store.ResyncRegion(ctx, r.ID)
		if err != nil {
			return c.fail(ctx, r, &res, &entry, err)
		}
		res.Delta.Inserted = n
		res.Outcome = OutcomeResync
	} else {
		d, err := c.store.ApplyDelta(ctx, r.ID, c.workMem)
		if err != nil {
			return c.fail(ctx, r, &res, &entry, err)
		}
		res.Delta = d
		res.Outcome = OutcomeOK
	}
	res.Timings.ApplyMS = int(time.Since(applyStart).Milliseconds())
	entry.ApplyMS = res.Timings.ApplyMS
	c.log.Debug("applied region", "region", r.Name, "outcome", res.Outcome,
		"ins", res.Delta.Inserted, "upd", res.Delta.Updated, "del", res.Delta.Deleted,
		"apply_ms", res.Timings.ApplyMS)

	// The drift guard. On disagreement, rebuild this one region from the snapshot
	// already staged, which costs no ESI tokens.
	verifyStart := time.Now()
	live, stagedSum, err := c.verify(ctx, r.ID)
	res.Timings.VerifyMS = int(time.Since(verifyStart).Milliseconds())
	entry.VerifyMS = res.Timings.VerifyMS
	if err != nil {
		return c.fail(ctx, r, &res, &entry, err)
	}
	if live.Rows != stagedSum.Rows || live.Hash != stagedSum.Hash {
		c.log.Warn("checksum drift, resyncing region",
			"region", r.Name, "live_rows", live.Rows, "staged_rows", stagedSum.Rows)
		if _, err := c.store.ResyncRegion(ctx, r.ID); err != nil {
			return c.fail(ctx, r, &res, &entry, err)
		}
		res.Outcome = OutcomeResync
	}

	entry.Outcome = res.Outcome
	entry.RowsInserted = res.Delta.Inserted
	entry.RowsUpdated = res.Delta.Updated
	entry.RowsDeleted = res.Delta.Deleted
	entry.DuplicateRows = res.Delta.Duplicates

	if err := c.store.MarkRefreshed(ctx, store.RegionStatus{
		RegionID:     r.ID,
		RefreshedAt:  time.Now(),
		Expires:      sw.meta.Expires,
		LastModified: sw.meta.LastModified,
		Pages:        sw.meta.Pages,
		OrderCount:   staged,
	}); err != nil {
		return res, err
	}
	if err := c.store.LogIngest(ctx, entry); err != nil {
		c.log.Warn("log ingest", "region", r.Name, "err", err)
	}
	res.Timings.TotalMS = int(time.Since(started).Milliseconds())
	if res.Delta.Duplicates > 0 {
		// Duplicates mean the page set was inconsistent in a way the Last-Modified
		// check did not catch, so it is worth surfacing rather than swallowing.
		c.log.Warn("duplicate order ids in page set",
			"region", r.Name, "duplicates", res.Delta.Duplicates)
	}
	return res, nil
}

// recordFailure classifies a failure, records it, and marks the region.
//
// A failed cycle is not fatal: nothing was published, so the previous snapshot is
// still live and still correct, and the next Expires tick is at most 5 minutes
// away. Both the fetch path and the database path funnel through here so the
// outcome, the error counter, and the log row can never disagree.
func (c *Cycle) recordFailure(ctx context.Context, r region.Region, err error,
	res *Result, entry *store.IngestLogEntry) {

	// A shutdown is not a fault. Recording one would leave a spurious error on the
	// region the consumer watches, and the write would fail anyway because it
	// shares the cancelled context.
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		res.Outcome = OutcomeFailed
		c.log.Debug("cycle interrupted by shutdown", "region", r.Name)
		return
	}

	var d *ErrDeferred
	switch {
	case errors.As(err, &d):
		res.Outcome, entry.DeferredReason = OutcomeDeferred, d.Reason
	case errors.Is(err, ErrInconsistentPageSet):
		res.Outcome = OutcomeInconsistent
	default:
		res.Outcome = OutcomeFailed
	}
	entry.Outcome = res.Outcome
	res.Err = err

	// A deferral is a deliberate choice, not a fault, so it must not inflate the
	// consecutive error count the consumer watches.
	if res.Outcome != OutcomeDeferred {
		if merr := c.store.MarkFailed(ctx, r.ID, err.Error(), time.Now()); merr != nil {
			c.log.Warn("mark failed", "region", r.Name, "err", merr)
		}
	}
	if lerr := c.store.LogIngest(ctx, *entry); lerr != nil {
		c.log.Warn("log ingest", "region", r.Name, "err", lerr)
	}
}

// fail records a database-side failure and returns it. Unlike a fetch failure it
// is returned as well as recorded, because it means the local database is not
// behaving and the caller should see that.
func (c *Cycle) fail(ctx context.Context, r region.Region, res *Result,
	entry *store.IngestLogEntry, cause error) (Result, error) {

	c.recordFailure(ctx, r, cause, res, entry)
	res.Outcome, entry.Outcome = OutcomeFailed, OutcomeFailed
	return *res, fmt.Errorf("region %s: %w", r.Name, cause)
}
