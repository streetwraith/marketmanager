// Command marketmanager ingests EVE Online market data into the market schema.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"

	"marketmanager/internal/config"
	"marketmanager/internal/esi"
	"marketmanager/internal/everef"
	"marketmanager/internal/ingest"
	"marketmanager/internal/region"
	"marketmanager/internal/store"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe the local /healthz and exit 0 (healthy) or 1; for the container HEALTHCHECK")
	flag.Parse()

	// Handled before config load, so the probe needs no DATABASE_URL.
	if *healthcheck {
		os.Exit(healthProbe())
	}

	// The SDK starts before config.Load, so a missing DATABASE_URL still reports.
	// BUGSINK_DSN therefore bypasses internal/config on purpose.
	loadEnvFile()
	initSentry()
	defer capturePanic("main")

	if err := run(context.Background()); err != nil {
		slog.Error("fatal", "err", err)
		// A cancelled context here is a SIGTERM during startup, not a fault.
		if !errors.Is(err, context.Canceled) {
			sentry.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("component", "fatal")
				sentry.CaptureException(err)
			})
		}
		// os.Exit skips deferred calls, so flush here rather than in a defer.
		sentry.Flush(sentryFlushTimeout)
		os.Exit(1)
	}
	sentry.Flush(sentryFlushTimeout)
	slog.Info("shutdown complete")
}

// sentryFlushTimeout bounds the wait for buffered events on the way out.
const sentryFlushTimeout = 2 * time.Second

// initSentry starts error reporting when BUGSINK_DSN is set. An empty DSN keeps
// local development and CI silent: every sentry call is then a no-op.
func initSentry() {
	dsn := os.Getenv("BUGSINK_DSN")
	if dsn == "" {
		return
	}
	err := sentry.Init(sentry.ClientOptions{
		Dsn: dsn,
		// Our errors come from fmt.Errorf and carry no stack of their own.
		AttachStacktrace: true,
		Environment:      "production",
		Release:          sentryRelease(),
	})
	if err != nil {
		// A DSN that url.Parse rejects is quoted back inside the error, and a DSN
		// is a write credential. Never let it reach the log.
		slog.Error("sentry init failed; check BUGSINK_DSN")
		return
	}
	slog.Info("error reporting enabled")
}

// sentryRelease is the release stamped on an error event. Coolify injects the
// real commit SHA into a git-built container, and the literal string "HEAD" into
// a Docker-image one. A constant release is worse than none, because every deploy
// then looks like the same one and Bugsink can never detect a regression.
func sentryRelease() string {
	if release := os.Getenv("SOURCE_COMMIT"); release != "HEAD" {
		return release
	}
	return ""
}

// capturePanic reports a panic from one of the service's goroutines, then
// re-panics so the process still dies and Coolify restarts it. sentry.Recover
// swallows the panic instead, which would leave a dead goroutine behind a
// /healthz that still passes, because the check only pings Postgres.
func capturePanic(component string) {
	r := recover()
	if r == nil {
		return
	}
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("component", component)
		sentry.CurrentHub().Recover(r)
	})
	sentry.Flush(sentryFlushTimeout)
	panic(r)
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		// Configuration is rejected before the level is known, so use the default.
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
		return err
	}
	level, err := config.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	slog.Info("starting",
		"log_level", cfg.LogLevel, "fetch_rps", cfg.FetchRPS,
		"fetch_concurrency", cfg.FetchConcurrency, "budget_reserve", cfg.BudgetReserve,
		"budget_floor", cfg.BudgetFloor, "history_backfill_days", cfg.HistoryBackfillDays,
		"analyze_interval", cfg.AnalyzeInterval.String(),
		"compatibility_date", cfg.CompatibilityDate,
		"esi_connect_timeout", cfg.ESIConnectTimeout.String(),
		"esi_request_timeout", cfg.ESIRequestTimeout.String(),
		"page_attempts", cfg.PageAttempts)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		return err
	}

	regions, err := st.Regions(ctx)
	if err != nil {
		return err
	}
	if err := st.EnsureRegionObjects(ctx, regions); err != nil {
		return err
	}
	// GPMR-01 arriving through a security filter is surprising, so log the
	// resolved set rather than leaving it implicit.
	slog.Info("regions resolved", "count", len(regions), "names", regionNames(regions))

	client := esi.New(esi.Options{
		BaseURL:           cfg.ESIBaseURL,
		UserAgent:         cfg.UserAgent,
		CompatibilityDate: cfg.CompatibilityDate,
		ConnectTimeout:    cfg.ESIConnectTimeout,
		RequestTimeout:    cfg.ESIRequestTimeout,
		RPS:               cfg.FetchRPS,
		Concurrency:       cfg.FetchConcurrency,
	})
	cycle := ingest.NewCycle(
		ingest.NewFetcher(client, ingest.FetcherConfig{
			Reserve:         cfg.BudgetReserve,
			BudgetFloor:     cfg.BudgetFloor,
			PageAttempts:    cfg.PageAttempts,
			ErrorLimitFloor: cfg.ErrorLimitFloor,
			PageBackoffUnit: cfg.PageBackoffUnit,
		}),
		st, client, cfg.DeltaWorkMem, slog.Default())
	scheduler := ingest.NewScheduler(cycle, client, regions, ingest.SchedulerConfig{
		MaxJitter:       3 * time.Second,
		CanaryInterval:  25 * time.Second,
		MaxAttempts:     3,
		ErrorLimitFloor: cfg.ErrorLimitFloor,
		BudgetFloor:     cfg.BudgetFloor,
	}, slog.Default())
	maint := ingest.NewMaintenance(st, cfg.PruneInterval, cfg.AnalyzeInterval,
		cfg.IngestLogRetentionDays, slog.Default())

	historyFrom := time.Now().UTC().AddDate(0, 0, -cfg.HistoryBackfillDays)
	if err := st.EnsureHistoryPartitions(ctx, historyFrom, time.Now().UTC()); err != nil {
		return err
	}
	history := ingest.NewHistoryImporter(
		everef.New(cfg.EverefBaseURL, cfg.UserAgent), st, regions,
		ingest.HistoryConfig{
			PollInterval: cfg.HistoryPollInterval,
			BackfillDays: cfg.HistoryBackfillDays,
			RecentDays:   cfg.HistoryRecentDays,
			Spacing:      cfg.HistorySpacing,
		}, slog.Default())

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { defer capturePanic("scheduler"); return scheduler.Run(ctx) })
	g.Go(func() error { defer capturePanic("maintenance"); return maint.Run(ctx) })
	g.Go(func() error { defer capturePanic("history"); return history.Run(ctx) })

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      newMux(st),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	g.Go(func() error {
		slog.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// A fresh context is the point: the parent is already cancelled, and the
		// shutdown still needs a grace period to drain.
		return srv.Shutdown(shutdownCtx) //nolint:contextcheck // deliberate
	})

	return g.Wait()
}

func regionNames(regions []region.Region) []string {
	names := make([]string, 0, len(regions))
	for _, r := range regions {
		names = append(names, fmt.Sprintf("%s(p%d)", r.Name, r.Priority))
	}
	return names
}

func newMux(st *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	return mux
}

// loadEnvFile reads .env when present. Prod injects real environment variables
// and ships no .env, so this is a no-op there.
func loadEnvFile() {
	if _, err := os.Stat(".env"); err != nil {
		return
	}
	if err := godotenv.Load(); err != nil {
		slog.Warn("load .env", "err", err)
	}
}

// healthProbe dials the service's own /healthz. The distroless image has no
// shell and no curl, so the binary is its own health check client.
func healthProbe() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad HTTP_ADDR:", err)
		return 1
	}
	// HTTP_ADDR is operator input, so the port is validated before it is spliced
	// into a URL rather than trusted. SplitHostPort accepts a named service too.
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		fmt.Fprintln(os.Stderr, "bad port in HTTP_ADDR:", portStr)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// The host is a hardcoded loopback address and the port is a validated
	// integer, so the URL cannot be steered anywhere. gosec's taint analysis
	// still reports it, because it does not recognise the check above.
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // loopback, validated port
	if err != nil {
		return 1
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req) //nolint:gosec // loopback, validated port
	if err != nil {
		return 1
	}
	//nolint:errcheck // the body is not read; a close error is not actionable
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
