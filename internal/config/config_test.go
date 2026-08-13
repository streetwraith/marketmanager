package config

import (
	"strings"
	"testing"
	"time"
)

// setEnv applies vars for one test and lets t.Setenv restore them afterwards.
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	// Clear everything Load reads, so the host environment cannot leak in.
	for _, k := range []string{
		"DATABASE_URL", "USER_AGENT", "ESI_BASE_URL", "X_COMPATIBILITY_DATE",
		"DELTA_WORK_MEM", "HTTP_ADDR", "FETCH_RPS", "FETCH_CONCURRENCY",
		"BUDGET_RESERVE", "BUDGET_FLOOR",
		"INGEST_LOG_RETENTION_DAYS", "PRUNE_INTERVAL",
	} {
		t.Setenv(k, "")
	}
	for k, v := range vars {
		t.Setenv(k, v)
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgres://u@h/db",
		"USER_AGENT":   "marketmanager/1.0 (ops@example.com)",
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, validEnv())
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.FetchRPS != 70 {
		t.Errorf("FetchRPS = %v, want 70", c.FetchRPS)
	}
	if c.FetchConcurrency != 16 {
		t.Errorf("FetchConcurrency = %d, want 16", c.FetchConcurrency)
	}
	if c.CompatibilityDate != "2026-08-04" {
		t.Errorf("CompatibilityDate = %q, want 2026-08-04", c.CompatibilityDate)
	}
	if c.IngestLogRetentionDays != 30 {
		t.Errorf("IngestLogRetentionDays = %d, want 30", c.IngestLogRetentionDays)
	}
	if c.PruneInterval != 24*time.Hour {
		t.Errorf("PruneInterval = %v, want 24h", c.PruneInterval)
	}
	if c.DeltaWorkMem != "64MB" {
		t.Errorf("DeltaWorkMem = %q, want 64MB", c.DeltaWorkMem)
	}
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name     string
		override map[string]string
		wantErr  string
	}{
		{"missing dsn", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"missing user agent", map[string]string{"USER_AGENT": ""}, "USER_AGENT"},
		{"zero rps", map[string]string{"FETCH_RPS": "0"}, "FETCH_RPS"},
		{"negative rps", map[string]string{"FETCH_RPS": "-1"}, "FETCH_RPS"},
		{"zero concurrency", map[string]string{"FETCH_CONCURRENCY": "0"}, "FETCH_CONCURRENCY"},
		{"negative floor", map[string]string{"BUDGET_FLOOR": "-1"}, "BUDGET_FLOOR"},
		{"reserve below floor", map[string]string{"BUDGET_RESERVE": "100", "BUDGET_FLOOR": "300"}, "BUDGET_RESERVE"},
		{"zero retention", map[string]string{"INGEST_LOG_RETENTION_DAYS": "0"}, "INGEST_LOG_RETENTION_DAYS"},
		{"zero prune interval", map[string]string{"PRUNE_INTERVAL": "0s"}, "PRUNE_INTERVAL"},
		{"unparsable rps", map[string]string{"FETCH_RPS": "fast"}, "FETCH_RPS"},
		{"unparsable concurrency", map[string]string{"FETCH_CONCURRENCY": "many"}, "FETCH_CONCURRENCY"},
		{"unparsable duration", map[string]string{"PRUNE_INTERVAL": "soon"}, "PRUNE_INTERVAL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vars := validEnv()
			for k, v := range tc.override {
				vars[k] = v
			}
			setEnv(t, vars)
			_, err := Load()
			if err == nil {
				t.Fatalf("Load succeeded, want error mentioning %s", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to mention %s", err, tc.wantErr)
			}
		})
	}
}

func TestLoadOverrides(t *testing.T) {
	vars := validEnv()
	vars["FETCH_RPS"] = "175"
	vars["FETCH_CONCURRENCY"] = "32"
	vars["PRUNE_INTERVAL"] = "1h"
	setEnv(t, vars)

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.FetchRPS != 175 {
		t.Errorf("FetchRPS = %v, want 175", c.FetchRPS)
	}
	if c.FetchConcurrency != 32 {
		t.Errorf("FetchConcurrency = %d, want 32", c.FetchConcurrency)
	}
	if c.PruneInterval != time.Hour {
		t.Errorf("PruneInterval = %v, want 1h", c.PruneInterval)
	}
}
