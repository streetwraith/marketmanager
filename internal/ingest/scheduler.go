package ingest

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"

	"marketmanager/internal/esi"
	"marketmanager/internal/region"
)

// SchedulerConfig tunes the refresh loop.
type SchedulerConfig struct {
	// MaxJitter spreads the fetch a little past each region's Expires, so the
	// service does not hit ESI at the exact instant the snapshot rolls.
	MaxJitter time.Duration
	// CanaryInterval is how often to probe for budget recovery while stopped.
	CanaryInterval time.Duration
	// MaxAttempts includes the first try. Only priorities 1-4 use it.
	MaxAttempts int
	// ErrorLimitFloor suspends retries while the legacy error budget is this low.
	ErrorLimitFloor int
	BudgetFloor     int
}

// cycleRunner is what the scheduler needs from a Cycle. It is an interface so the
// scheduling decisions can be tested without ESI or a database.
type cycleRunner interface {
	Run(ctx context.Context, r region.Region) (Result, error)
}

// Scheduler drives the refresh loop.
//
// Every region turns fresh inside the same ~60 second band, because the upstream
// regenerates all snapshots in one batch. Start offsets therefore cannot be
// staggered. Priority instead decides the order within that band: regions are
// processed one at a time in priority order, so the most important region is
// never queued behind the least important. Running them sequentially costs no
// extra wall time, because the global request rate is the binding constraint.
type Scheduler struct {
	cycle   cycleRunner
	client  *esi.Client
	regions []region.Region
	cfg     SchedulerConfig
	log     *slog.Logger

	due map[int64]time.Time
}

func NewScheduler(c cycleRunner, client *esi.Client,
	regions []region.Region, cfg SchedulerConfig, log *slog.Logger) *Scheduler {

	ordered := slices.Clone(regions)
	slices.SortFunc(ordered, func(a, b region.Region) int {
		if a.Priority != b.Priority {
			return a.Priority - b.Priority
		}
		return int(a.ID - b.ID)
	})
	return &Scheduler{
		cycle: c, client: client, regions: ordered,
		cfg: cfg, log: log, due: make(map[int64]time.Time, len(regions)),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.prime(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return nil
		case <-tick.C:
			s.pass(ctx)
		}
	}
}

// prime learns each region's Expires without sweeping, then waits for the next
// tick before the first full fetch.
//
// A naive start fits four full cycles into the first 15-minute window instead of
// three, because the first sweep catches data already partway through its TTL and
// the second therefore falls ~3 minutes later rather than 5. That alone exhausts
// the token bucket, and it would repeat on every restart. Page 1 of 25 regions
// costs 50 tokens and removes the problem entirely.
func (s *Scheduler) prime(ctx context.Context) error {
	start := time.Now()
	for _, r := range s.regions {
		p, err := s.client.OrdersPage(ctx, r.ID, 1)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// One unreachable region must not stop the service. Try it shortly.
			s.log.Warn("prime failed, will retry", "region", r.Name, "err", err)
			s.due[r.ID] = time.Now().Add(30 * time.Second)
			continue
		}
		s.due[r.ID] = s.nextDue(p.Expires)
	}
	rem, _ := s.client.Budget.Remaining()
	s.log.Info("primed", "regions", len(s.regions),
		"took_ms", time.Since(start).Milliseconds(), "remaining", rem)
	return nil
}

// nextDue is when to fetch a region whose snapshot expires at exp.
func (s *Scheduler) nextDue(exp time.Time) time.Time {
	if exp.IsZero() {
		return time.Now().Add(5 * time.Minute)
	}
	// Jitter spreads requests; it is not a security decision.
	jitter := time.Duration(rand.Int64N(int64(s.cfg.MaxJitter) + 1)) //nolint:gosec // not cryptographic
	due := exp.Add(time.Second + jitter)
	// A stale Expires must not spin the loop.
	if min := time.Now().Add(5 * time.Second); due.Before(min) {
		return min
	}
	return due
}

// pass runs every region that is due, in priority order.
func (s *Scheduler) pass(ctx context.Context) {
	if d := s.client.ErrorLimit.BlockedFor(); d > 0 {
		s.log.Error("error limit tripped, all requests paused",
			"resumes_in_s", int(d.Seconds()))
		sleep(ctx, d)
		return
	}
	if s.client.Budget.BelowFloor(s.cfg.BudgetFloor) {
		s.canary(ctx)
		return
	}

	for _, r := range s.regions {
		if ctx.Err() != nil {
			return
		}
		due, ok := s.due[r.ID]
		if !ok || time.Now().Before(due) {
			continue
		}
		s.log.Debug("region due", "region", r.Name, "priority", r.Priority,
			"late_ms", time.Since(due).Milliseconds())
		s.runRegion(ctx, r)
	}
}

// runRegion runs one region's cycle, retrying only where the owner's rule allows.
func (s *Scheduler) runRegion(ctx context.Context, r region.Region) {
	attempts := 1
	if r.CanRetry() {
		attempts = s.cfg.MaxAttempts
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		res, err := s.cycle.Run(ctx, r)
		if ctx.Err() != nil {
			return
		}
		if err == nil && res.Err == nil {
			s.due[r.ID] = s.nextDue(res.Meta.Expires)
			rem, _ := s.client.Budget.Remaining()
			s.log.Info("region refreshed",
				"region", r.Name, "priority", r.Priority, "outcome", res.Outcome,
				"pages", res.Meta.Pages, "orders", res.Staged,
				"ins", res.Delta.Inserted, "upd", res.Delta.Updated, "del", res.Delta.Deleted,
				"dup", res.Delta.Duplicates,
				"fetch_ms", res.Timings.FetchMS, "copy_ms", res.Timings.CopyMS,
				"apply_ms", res.Timings.ApplyMS, "verify_ms", res.Timings.VerifyMS,
				"total_ms", res.Timings.TotalMS,
				"tokens", res.Meta.TokensSpent, "remaining", rem)
			return
		}

		cause := err
		if cause == nil {
			cause = res.Err
		}

		// A deferral is the governor working as intended, not a fault. Retrying
		// would spend the very tokens it just protected.
		//
		// The next attempt waits for the region's own Expires rather than a short
		// fixed delay. Discovering that the budget is still short costs another
		// page 1, and nothing can change until the snapshot does: retrying every
		// 30s across the low-priority regions would burn ~1,200 tokens per window
		// purely to re-learn that there is no budget, which keeps the budget short.
		if res.Outcome == OutcomeDeferred {
			s.due[r.ID] = s.nextDue(res.Meta.Expires)
			s.log.Info("region deferred", "region", r.Name, "reason", cause,
				"next_attempt", s.due[r.ID].UTC().Format(time.RFC3339),
				"tokens", res.Meta.TokensSpent)
			return
		}
		if esi.IsErrorLimited(cause) {
			s.log.Error("error limit tripped", "region", r.Name)
			return
		}

		last := attempt == attempts
		if last || !s.client.ErrorLimit.Healthy(s.cfg.ErrorLimitFloor) {
			if !last {
				// Retrying now risks a 420, which would block every application
				// sharing this IP, not just this one.
				s.log.Warn("retries suspended, error budget low", "region", r.Name)
			}
			if ctx.Err() != nil {
				return // shutting down; not a failure
			}
			s.log.Error("region cycle failed",
				"region", r.Name, "priority", r.Priority, "attempts", attempt,
				"outcome", res.Outcome, "err", cause)
			// The previous snapshot is still live and still correct, so simply
			// wait for the next tick rather than hammering.
			s.due[r.ID] = time.Now().Add(30 * time.Second)
			return
		}

		backoff := time.Duration(attempt) * 2 * time.Second
		if ra := retryAfter(cause); ra > 0 {
			backoff = ra
		}
		lvl := slog.LevelWarn
		if esi.IsRateLimited(cause) {
			lvl = slog.LevelError
		}
		s.log.Log(ctx, lvl, "region cycle failed, retrying",
			"region", r.Name, "attempt", attempt, "of", attempts,
			"backoff_ms", backoff.Milliseconds(), "err", cause)
		if !sleep(ctx, backoff) {
			return
		}
	}
}

// canary spends 2 tokens on the cheapest real region to learn whether the budget
// has recovered. The global PLEX market is one page, and its data is wanted anyway.
func (s *Scheduler) canary(ctx context.Context) {
	rem, _ := s.client.Budget.Remaining()
	s.log.Error("rate limit reached, fetching paused",
		"remaining", rem, "floor", s.cfg.BudgetFloor,
		"probe_interval_s", int(s.cfg.CanaryInterval.Seconds()))
	if _, err := s.client.OrdersPage(ctx, region.GlobalPLEX, 1); err != nil && ctx.Err() == nil {
		s.log.Warn("canary failed", "err", err)
	}
	sleep(ctx, s.cfg.CanaryInterval)
}

func retryAfter(err error) time.Duration {
	var he *esi.HTTPError
	if errors.As(err, &he) {
		return he.RetryAfter
	}
	return 0
}

// sleep waits or returns false if the context ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
