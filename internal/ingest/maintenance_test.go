package ingest

import (
	"errors"
	"testing"

	"marketmanager/internal/sentrytest"
)

// Both housekeeping jobs only ever fail because the database does, and a broken
// database fails every tick. Each job reports its first failure and then stays
// quiet, and each job tracks its own run.
func TestMaintenanceReportsOncePerJobRun(t *testing.T) {
	sink := sentrytest.Capture(t)
	m := &Maintenance{}
	boom := errors.New("db down")

	m.note(jobPrune, boom)
	m.note(jobPrune, boom)
	m.note(jobAnalyze, boom)
	if got := len(sink.Captured()); got != 2 {
		t.Fatalf("events = %d, want one per job", got)
	}

	m.reports.ends(jobPrune)
	m.note(jobPrune, boom)

	events := sink.Captured()
	if len(events) != 3 {
		t.Fatalf("events after a success ended the run = %d, want 3", len(events))
	}
	if events[0].Tags["component"] != "maintenance" || events[0].Tags["job"] != jobPrune {
		t.Errorf("tags = %v, want component=maintenance job=%s", events[0].Tags, jobPrune)
	}
	if events[1].Tags["job"] != jobAnalyze {
		t.Errorf("tags = %v, want job=%s", events[1].Tags, jobAnalyze)
	}
}
