package ingest

import (
	"context"
	"errors"
	"net"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// retryablePage reports whether a single page is worth fetching again.
//
// Timeouts and connection errors are the common case during peak hours, when
// per-request latency rises from under a second to roughly fifteen. A 5xx is
// retryable too: it costs no tokens, though it does strike the legacy error
// limit, which is why the caller gates on that budget as well.
//
// A 4xx is not retryable: it will fail identically. A 420 is not retryable by
// anyone, because the whole IP is blocked until the error window resets.
func retryablePage(err error) bool {
	if err == nil || esi.IsErrorLimited(err) {
		return false
	}
	if esi.IsRateLimited(err) || esi.IsServerError(err) {
		return true
	}
	var he *esi.HTTPError
	if errors.As(err, &he) {
		return false // any other HTTP status will not change on a retry
	}
	// Transport-level: timeout, reset, refused, DNS.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// retriesBlocked reports why no retry may run right now, or an empty string.
//
// Retrying into a low error budget is how a bad spell becomes a 420 that blocks
// every application sharing this IP. A retry into an upstream that is already
// down spends the same strikes for nothing.
func retriesBlocked(c *esi.Client, errorLimitFloor int) string {
	if c.Outage.PausedFor() > 0 {
		return "upstream outage"
	}
	if !c.ErrorLimit.Healthy(errorLimitFloor) {
		return "error budget low"
	}
	return ""
}

// pageBackoff is deliberately short. A sweep has to finish inside the 300s
// snapshot window or the Last-Modified check will reject it, so a page retry
// competes with the deadline that makes the whole sweep worth doing.
func (f *Fetcher) pageBackoff(attempt int, err error) time.Duration {
	var he *esi.HTTPError
	if errors.As(err, &he) && he.RetryAfter > 0 {
		return he.RetryAfter
	}
	return time.Duration(attempt) * f.pageBackoffUnit
}

// fetchPage fetches one page, retrying it in place rather than failing the sweep.
//
// This is the difference between spending 2 tokens and spending a whole sweep.
// Before, one slow page out of 413 aborted the region, and a priority 1-4 region
// then retried the entire page set up to three times: 2,478 tokens for The Forge,
// spent during exactly the peak hours when ESI is least able to serve them.
//
// It returns the tokens spent across every attempt, so a retry is never invisible
// in the budget accounting.
func (f *Fetcher) fetchPage(ctx context.Context, r region.Region, page int) (*esi.OrderPage, int, error) {
	var (
		tokens  int
		lastErr error
	)
	for attempt := 1; attempt <= f.pageAttempts; attempt++ {
		p, err := f.client.OrdersPage(ctx, r.ID, page)
		if p != nil {
			tokens += esi.TokenCost(p.Status)
		}
		if err == nil {
			return p, tokens, nil
		}
		lastErr = err

		if !retryablePage(err) || attempt == f.pageAttempts || ctx.Err() != nil {
			break
		}
		if retriesBlocked(f.client, f.errorLimitFloor) != "" {
			break
		}
		if !sleep(ctx, f.pageBackoff(attempt, err)) {
			break
		}
	}
	return nil, tokens, lastErr
}
