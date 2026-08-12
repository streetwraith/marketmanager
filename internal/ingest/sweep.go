// Package ingest turns ESI responses into database state.
package ingest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// ErrInconsistentPageSet means the pages did not all come from one snapshot, so
// the result must be discarded. A partial or mixed page set would look to a
// consumer like orders vanished.
var ErrInconsistentPageSet = fmt.Errorf("ingest: page set is inconsistent")

// ErrDeferred means the budget governor refused the region this cycle.
type ErrDeferred struct {
	Reason string
}

func (e *ErrDeferred) Error() string { return "ingest: deferred: " + e.Reason }

// SweepMeta describes a completed page set.
type SweepMeta struct {
	Pages        int
	Expires      time.Time
	LastModified time.Time
	OrderCount   int
	TokensSpent  int
	Duration     time.Duration
}

// Fetcher sweeps a region's whole order book.
type Fetcher struct {
	client *esi.Client

	reserve         int
	floor           int
	pageAttempts    int
	errorLimitFloor int
	pageBackoffUnit time.Duration
}

// FetcherConfig groups the budget and retry settings, which have grown past what
// positional parameters read well for.
type FetcherConfig struct {
	Reserve         int
	BudgetFloor     int
	PageAttempts    int
	ErrorLimitFloor int
	PageBackoffUnit time.Duration
}

func NewFetcher(c *esi.Client, cfg FetcherConfig) *Fetcher {
	return &Fetcher{
		client: c, reserve: cfg.Reserve, floor: cfg.BudgetFloor,
		pageAttempts: cfg.PageAttempts, errorLimitFloor: cfg.ErrorLimitFloor,
		pageBackoffUnit: cfg.PageBackoffUnit,
	}
}

// Sweep fetches every page of a region and streams each page's orders to out.
// It closes out before returning.
//
// The caller may write those orders somewhere immediately: nothing is published
// until the delta applies, so a sweep that fails partway is discarded safely.
// Consistency is only knowable once every page has arrived, so it is checked at
// the end and reported as ErrInconsistentPageSet.
func (f *Fetcher) Sweep(ctx context.Context, r region.Region, out chan<- []esi.Order) (*SweepMeta, error) {
	defer close(out)
	start := time.Now()
	meta := &SweepMeta{}

	if f.client.Budget.BelowFloor(f.floor) {
		rem, _ := f.client.Budget.Remaining()
		return meta, &ErrDeferred{Reason: fmt.Sprintf("budget below floor (%d)", rem)}
	}

	first, firstTokens, err := f.fetchPage(ctx, r, 1)
	meta.TokensSpent += firstTokens
	if err != nil {
		return meta, fmt.Errorf("region %d page 1: %w", r.ID, err)
	}

	meta.Pages = first.Pages
	meta.Expires = first.Expires
	meta.LastModified = first.LastModified

	// Page counts drift, so the cost is recomputed from this cycle's X-Pages and
	// never cached. Page 1 is already spent, hence Pages-1.
	remainingCost := (first.Pages - 1) * esi.TokenCost(200)
	if !f.client.Budget.Fits(remainingCost, f.reserve) {
		rem, _ := f.client.Budget.Remaining()
		return meta, &ErrDeferred{
			Reason: fmt.Sprintf("%d pages cost %d tokens, only %d remaining", first.Pages, remainingCost, rem),
		}
	}

	select {
	case out <- first.Orders:
		meta.OrderCount += len(first.Orders)
	case <-ctx.Done():
		return meta, ctx.Err()
	}

	if first.Pages == 1 {
		meta.Duration = time.Since(start)
		return meta, nil
	}

	var (
		mu        sync.Mutex
		mismatch  bool
		pagesGrew bool
		tokens    int
		count     int
	)

	g, gctx := errgroup.WithContext(ctx)
	for page := 2; page <= first.Pages; page++ {
		g.Go(func() error {
			p, spent, err := f.fetchPage(gctx, r, page)
			mu.Lock()
			tokens += spent
			mu.Unlock()
			if err != nil {
				return fmt.Errorf("region %d page %d: %w", r.ID, page, err)
			}
			// Every page must come from the same server-side snapshot.
			if !p.LastModified.Equal(first.LastModified) {
				mu.Lock()
				mismatch = true
				mu.Unlock()
			}
			if p.Pages != first.Pages {
				mu.Lock()
				pagesGrew = true
				mu.Unlock()
			}
			mu.Lock()
			count += len(p.Orders)
			mu.Unlock()
			select {
			case out <- p.Orders:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		})
	}
	err = g.Wait()

	meta.TokensSpent += tokens
	meta.OrderCount += count
	meta.Duration = time.Since(start)

	if err != nil {
		return meta, err
	}
	if mismatch || pagesGrew {
		return meta, fmt.Errorf("%w: region %d (last-modified mismatch=%v, x-pages changed=%v)",
			ErrInconsistentPageSet, r.ID, mismatch, pagesGrew)
	}
	return meta, nil
}
