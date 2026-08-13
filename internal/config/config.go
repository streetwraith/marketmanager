// Package config loads and validates the service configuration from the environment.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every tunable the service reads. All values come from the
// environment; there is no config file.
type Config struct {
	DatabaseURL string // required
	UserAgent   string // required, ESI etiquette: app name plus a contact email

	ESIBaseURL        string
	CompatibilityDate string

	// ESIConnectTimeout bounds the TCP dial and TLS handshake; ESIRequestTimeout
	// bounds the whole request. They are separate because a connection that will
	// not open is dead, while a slow response is merely slow: ESI's per-request
	// latency has been measured swinging from ~0.8s off-peak to ~15s at peak.
	ESIConnectTimeout time.Duration
	ESIRequestTimeout time.Duration

	// FetchRPS is the politeness contract with ESI and binds where latency is low.
	// FetchConcurrency binds where it is high, since throughput is roughly
	// concurrency/latency. Whichever binds first, binds.
	//
	// ESI grants each source IP a throughput allowance and queues everything
	// offered above it, so in-flight requests above the allowance only inflate
	// per-request latency (measured: p50 187ms at 8 in flight, 1875ms at 64, same
	// req/s). The default of 16 saturates the allowance in every measured regime
	// while keeping latency far from ESIRequestTimeout. See PROJECT.md.
	FetchRPS         float64
	FetchConcurrency int

	// BudgetReserve is how many tokens to leave unspent when deciding whether a
	// region's page set fits. BudgetFloor is the level below which the fetcher
	// stops entirely and probes for recovery.
	BudgetReserve int
	BudgetFloor   int

	// PageAttempts includes the first try. A page retry costs 2 tokens; failing
	// the sweep instead discards every token already spent on the other pages, so
	// this applies to all regions rather than only the retry-eligible priorities.
	PageAttempts int
	// ErrorLimitFloor suspends retries while the legacy per-IP error budget is
	// low. Tripping it returns 420 for every route and blocks other applications.
	ErrorLimitFloor int
	PageBackoffUnit time.Duration

	DeltaWorkMem string

	// LogLevel is debug, info, warn or error. Prod runs at info; debug adds
	// per-request and per-phase detail that is far too chatty for a live service.
	LogLevel string

	HTTPAddr               string
	IngestLogRetentionDays int
	PruneInterval          time.Duration
	// AnalyzeInterval is separate from PruneInterval because the two jobs have
	// natural frequencies 24x apart: pruning a 7,200-row/day log wants daily,
	// while the partitioned parents need statistics far sooner than that.
	AnalyzeInterval time.Duration

	// EVE Ref daily history.
	EverefBaseURL       string
	HistoryPollInterval time.Duration
	HistoryBackfillDays int
	// HistoryRecentDays is how far back to keep re-checking. A day file keeps
	// growing for about four days, so anything older is treated as final.
	HistoryRecentDays int
	HistorySpacing    time.Duration
}

// Load reads the environment and fails loudly on anything missing or nonsensical.
func Load() (Config, error) {
	c := Config{
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		UserAgent:         os.Getenv("USER_AGENT"),
		ESIBaseURL:        env("ESI_BASE_URL", "https://esi.evetech.net"),
		CompatibilityDate: env("X_COMPATIBILITY_DATE", "2026-08-04"),
		DeltaWorkMem:      env("DELTA_WORK_MEM", "64MB"),
		LogLevel:          env("LOG_LEVEL", "info"),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		EverefBaseURL:     env("EVEREF_BASE_URL", "https://data.everef.net/market-history"),
	}

	var err error
	if c.ESIConnectTimeout, err = envDuration("ESI_CONNECT_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if c.ESIRequestTimeout, err = envDuration("ESI_REQUEST_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.FetchRPS, err = envFloat("FETCH_RPS", 70); err != nil {
		return Config{}, err
	}
	if c.FetchConcurrency, err = envInt("FETCH_CONCURRENCY", 16); err != nil {
		return Config{}, err
	}
	if c.BudgetReserve, err = envInt("BUDGET_RESERVE", 600); err != nil {
		return Config{}, err
	}
	if c.BudgetFloor, err = envInt("BUDGET_FLOOR", 300); err != nil {
		return Config{}, err
	}
	if c.PageAttempts, err = envInt("PAGE_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if c.ErrorLimitFloor, err = envInt("ERROR_LIMIT_FLOOR", 30); err != nil {
		return Config{}, err
	}
	if c.PageBackoffUnit, err = envDuration("PAGE_BACKOFF_UNIT", 250*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.IngestLogRetentionDays, err = envInt("INGEST_LOG_RETENTION_DAYS", 30); err != nil {
		return Config{}, err
	}
	if c.PruneInterval, err = envDuration("PRUNE_INTERVAL", 24*time.Hour); err != nil {
		return Config{}, err
	}
	if c.AnalyzeInterval, err = envDuration("ANALYZE_INTERVAL", time.Hour); err != nil {
		return Config{}, err
	}
	if c.HistoryPollInterval, err = envDuration("HISTORY_POLL_INTERVAL", 15*time.Minute); err != nil {
		return Config{}, err
	}
	if c.HistoryBackfillDays, err = envInt("HISTORY_BACKFILL_DAYS", 730); err != nil {
		return Config{}, err
	}
	if c.HistoryRecentDays, err = envInt("HISTORY_RECENT_DAYS", 10); err != nil {
		return Config{}, err
	}
	if c.HistorySpacing, err = envDuration("HISTORY_SPACING", 2*time.Second); err != nil {
		return Config{}, err
	}

	switch {
	case c.DatabaseURL == "":
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	case c.UserAgent == "":
		return Config{}, fmt.Errorf("USER_AGENT is required (ESI etiquette: app name and contact email)")
	case c.ESIConnectTimeout <= 0:
		return Config{}, fmt.Errorf("ESI_CONNECT_TIMEOUT must be positive, got %v", c.ESIConnectTimeout)
	case c.ESIRequestTimeout <= 0:
		return Config{}, fmt.Errorf("ESI_REQUEST_TIMEOUT must be positive, got %v", c.ESIRequestTimeout)
	case c.ESIRequestTimeout < c.ESIConnectTimeout:
		return Config{}, fmt.Errorf("ESI_REQUEST_TIMEOUT (%v) must be at least ESI_CONNECT_TIMEOUT (%v)",
			c.ESIRequestTimeout, c.ESIConnectTimeout)
	case c.FetchRPS <= 0:
		return Config{}, fmt.Errorf("FETCH_RPS must be positive, got %v", c.FetchRPS)
	case c.FetchConcurrency < 1:
		return Config{}, fmt.Errorf("FETCH_CONCURRENCY must be at least 1, got %d", c.FetchConcurrency)
	case c.BudgetFloor < 0:
		return Config{}, fmt.Errorf("BUDGET_FLOOR must not be negative, got %d", c.BudgetFloor)
	case c.BudgetReserve < c.BudgetFloor:
		// Below the floor the fetcher stops outright, so a reserve under it could
		// never trigger a deferral.
		return Config{}, fmt.Errorf("BUDGET_RESERVE (%d) must be at least BUDGET_FLOOR (%d)", c.BudgetReserve, c.BudgetFloor)
	case c.PageAttempts < 1:
		return Config{}, fmt.Errorf("PAGE_ATTEMPTS must be at least 1, got %d", c.PageAttempts)
	case c.ErrorLimitFloor < 0:
		return Config{}, fmt.Errorf("ERROR_LIMIT_FLOOR must not be negative, got %d", c.ErrorLimitFloor)
	case c.PageBackoffUnit < 0:
		return Config{}, fmt.Errorf("PAGE_BACKOFF_UNIT must not be negative, got %v", c.PageBackoffUnit)
	case c.IngestLogRetentionDays < 1:
		return Config{}, fmt.Errorf("INGEST_LOG_RETENTION_DAYS must be at least 1, got %d", c.IngestLogRetentionDays)
	case c.PruneInterval <= 0:
		return Config{}, fmt.Errorf("PRUNE_INTERVAL must be positive, got %v", c.PruneInterval)
	case c.AnalyzeInterval <= 0:
		return Config{}, fmt.Errorf("ANALYZE_INTERVAL must be positive, got %v", c.AnalyzeInterval)
	case c.HistoryPollInterval <= 0:
		return Config{}, fmt.Errorf("HISTORY_POLL_INTERVAL must be positive, got %v", c.HistoryPollInterval)
	case c.HistoryBackfillDays < 1:
		return Config{}, fmt.Errorf("HISTORY_BACKFILL_DAYS must be at least 1, got %d", c.HistoryBackfillDays)
	case c.HistoryRecentDays < 1:
		return Config{}, fmt.Errorf("HISTORY_RECENT_DAYS must be at least 1, got %d", c.HistoryRecentDays)
	case c.HistorySpacing < 0:
		return Config{}, fmt.Errorf("HISTORY_SPACING must not be negative, got %v", c.HistorySpacing)
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ParseLogLevel maps the configured name onto a slog level.
func ParseLogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL must be debug, info, warn or error, got %q", name)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
