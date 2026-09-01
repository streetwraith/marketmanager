package ingest

import "testing"

// A ticker is the retry here, so a job that keeps failing is one fault, not one
// fault per tick. Every capture site reports the first failure and stays quiet
// until the job succeeds again.
func TestStreakReportsTheFirstFailureOnly(t *testing.T) {
	var s streak

	if !s.first("a") {
		t.Fatal("the first failure must report")
	}
	if s.first("a") {
		t.Error("a second failure in the same run must stay quiet")
	}
	if !s.first("b") {
		t.Error("a different key must report on its own")
	}

	s.ends("a")
	if !s.first("a") {
		t.Error("a failure after a success must report again")
	}
	if s.first("b") {
		t.Error("ending one key must not end another")
	}
}
