package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"

	"marketmanager/internal/store"
)

// Maintenance runs the two housekeeping jobs this service needs.
//
// They are on separate tickers because their natural frequencies differ by about
// 24x. Pruning trims ~7,200 ingest_log rows a day and wants to run daily.
// Statistics on the partitioned parents need refreshing far sooner: autovacuum
// never analyses a parent at all, so hanging that off the prune ticker left the
// planner with no statistics for a whole day after every restart.
type Maintenance struct {
	store           *store.Store
	pruneInterval   time.Duration
	analyzeInterval time.Duration
	retentionDays   int
	log             *slog.Logger

	pruneMu   sync.Mutex
	analyzeMu sync.Mutex

	// reports keeps one broken database to one event per job; see streak.
	reports streak
}

// The job names used in the log, in the event tag, and as the streak key.
const (
	jobPrune   = "prune"
	jobAnalyze = "analyze"
)

func NewMaintenance(st *store.Store, pruneInterval, analyzeInterval time.Duration,
	retentionDays int, log *slog.Logger) *Maintenance {

	return &Maintenance{
		store: st, pruneInterval: pruneInterval, analyzeInterval: analyzeInterval,
		retentionDays: retentionDays, log: log,
	}
}

func (m *Maintenance) Run(ctx context.Context) error {
	// Analyse once at start. Waiting for the first tick would leave the parents
	// without statistics for an hour on every restart, and on a database that
	// already holds data there is nothing to gain by waiting.
	m.analyze(ctx)

	pruneTick := time.NewTicker(m.pruneInterval)
	defer pruneTick.Stop()
	analyzeTick := time.NewTicker(m.analyzeInterval)
	defer analyzeTick.Stop()

	for {
		select {
		case <-ctx.Done():
			m.log.Info("maintenance stopped")
			return nil
		case <-pruneTick.C:
			m.prune(ctx)
		case <-analyzeTick.C:
			m.analyze(ctx)
		}
	}
}

// prune trims ingest_log to the retention window. Errors are logged, never
// returned: failed housekeeping must not tear down ingestion.
func (m *Maintenance) prune(ctx context.Context) {
	if !m.pruneMu.TryLock() {
		m.log.Info("prune already running, skipping")
		return
	}
	defer m.pruneMu.Unlock()

	start := time.Now()
	n, err := m.store.PruneIngestLog(ctx, m.retentionDays)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("prune ingest_log", "err", err)
			m.note(jobPrune, err)
		}
		return
	}
	m.reports.ends(jobPrune)
	m.log.Info("pruned ingest_log", "rows", n, "retention_days", m.retentionDays,
		"ms", time.Since(start).Milliseconds())
}

// analyze refreshes statistics on the partitioned parents.
//
// Autovacuum processes leaf partitions but never the parent, so without this any
// consumer query that aggregates across regions plans against empty statistics.
// ANALYZE takes only SHARE UPDATE EXCLUSIVE and does not block readers, but it
// does conflict with the TRUNCATE a region resync uses, so a collision simply
// retries on the next tick rather than forcing anything.
func (m *Maintenance) analyze(ctx context.Context) {
	if !m.analyzeMu.TryLock() {
		m.log.Info("analyze already running, skipping")
		return
	}
	defer m.analyzeMu.Unlock()

	start := time.Now()
	if err := m.store.AnalyzeParents(ctx); err != nil {
		if ctx.Err() == nil {
			m.log.Error("analyze parents", "err", err)
			m.note(jobAnalyze, err)
		}
		return
	}
	m.reports.ends(jobAnalyze)
	m.log.Info("analyzed partitioned parents", "ms", time.Since(start).Milliseconds())
}

// note reports a failed housekeeping job, once per run of failures. Both jobs
// only ever fail because the database does, and a broken database fails every
// tick until somebody fixes it.
func (m *Maintenance) note(job string, err error) {
	if !m.reports.first(job) {
		return
	}
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("component", "maintenance")
		scope.SetTag("job", job)
		sentry.CaptureException(err)
	})
}
