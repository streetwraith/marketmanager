package esi

import "testing"

func TestBudgetUnknownAllowsFirstRequest(t *testing.T) {
	var b Budget
	if _, ok := b.Remaining(); ok {
		t.Error("Remaining() reported a known value before any response")
	}
	// The first request is what establishes the budget, so it must not be gated.
	if !b.Fits(826, 600) {
		t.Error("Fits() denied the first request; nothing is known yet")
	}
	if b.BelowFloor(300) {
		t.Error("BelowFloor() was true before any response")
	}
}

func TestBudgetFits(t *testing.T) {
	tests := []struct {
		name      string
		remaining int
		cost      int
		reserve   int
		want      bool
	}{
		{"comfortably fits", 11998, 826, 600, true},
		{"exactly at the reserve still fits", 1426, 826, 600, true},
		{"one token short", 1425, 826, 600, false},
		{"a Forge sweep does not fit near the floor", 900, 826, 600, false},
		{"a single page still fits when a sweep does not", 900, 2, 600, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b Budget
			b.Observe(tc.remaining)
			if got := b.Fits(tc.cost, tc.reserve); got != tc.want {
				t.Errorf("Fits(%d, %d) with %d remaining = %v, want %v",
					tc.cost, tc.reserve, tc.remaining, got, tc.want)
			}
		})
	}
}

func TestBudgetBelowFloor(t *testing.T) {
	var b Budget
	b.Observe(301)
	if b.BelowFloor(300) {
		t.Error("301 should not be below a floor of 300")
	}
	b.Observe(299)
	if !b.BelowFloor(300) {
		t.Error("299 should be below a floor of 300")
	}
	// Recovery must be observable, so the fetcher can resume.
	b.Observe(2909)
	if b.BelowFloor(300) {
		t.Error("budget did not recover after a higher observation")
	}
}
