package esi

import (
	"testing"
	"time"
)

func TestErrorLimitUnknownIsHealthy(t *testing.T) {
	var e ErrorLimit
	// Before any error response there is nothing to be cautious about, so retries
	// must not be suspended.
	if !e.Healthy(30) {
		t.Error("Healthy() was false before any error was seen")
	}
	if d := e.BlockedFor(); d != 0 {
		t.Errorf("BlockedFor() = %v before any error, want 0", d)
	}
}

func TestErrorLimitHealthy(t *testing.T) {
	tests := []struct {
		name   string
		remain int
		floor  int
		want   bool
	}{
		{"plenty of budget", 95, 30, true},
		{"one above the floor", 31, 30, true},
		{"exactly at the floor is not healthy", 30, 30, false},
		{"below the floor", 5, 30, false},
		{"exhausted", 0, 30, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e ErrorLimit
			e.Observe(tc.remain, 0)
			if got := e.Healthy(tc.floor); got != tc.want {
				t.Errorf("Healthy(%d) with %d remaining = %v, want %v",
					tc.floor, tc.remain, got, tc.want)
			}
		})
	}
}

func TestErrorLimitBlocks(t *testing.T) {
	var e ErrorLimit
	e.Block(45 * time.Second)
	d := e.BlockedFor()
	if d <= 40*time.Second || d > 45*time.Second {
		t.Errorf("BlockedFor() = %v, want just under 45s", d)
	}
}

// A 420 without a reset header still has to stop everything. Falling through to
// zero would let the service keep hammering a limit it has already tripped.
func TestErrorLimitBlockWithoutHeaderUsesTheDocumentedWindow(t *testing.T) {
	var e ErrorLimit
	e.Block(0)
	if d := e.BlockedFor(); d <= 0 {
		t.Fatalf("BlockedFor() = %v after a 420 with no reset header, want a real delay", d)
	}
	if d := e.BlockedFor(); d > time.Minute {
		t.Errorf("BlockedFor() = %v, want at most the documented one-minute window", d)
	}
}

// The block may only ever be extended. A later, shorter reset must not shorten an
// existing block, or a burst of 420s would let work resume early and re-trip it.
func TestErrorLimitNeverShortensAnExistingBlock(t *testing.T) {
	var e ErrorLimit
	e.Block(60 * time.Second)
	long := e.BlockedFor()

	e.Block(2 * time.Second)
	if got := e.BlockedFor(); got < long-time.Second {
		t.Errorf("a 2s block shortened a 60s block to %v", got)
	}

	// Observe carries a reset too, and must obey the same rule.
	e.Observe(10, 1*time.Second)
	if got := e.BlockedFor(); got < long-2*time.Second {
		t.Errorf("Observe with a 1s reset shortened the block to %v", got)
	}
}

// Observing a healthy budget must not resurrect an expired block.
func TestErrorLimitRecovers(t *testing.T) {
	var e ErrorLimit
	e.Block(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if d := e.BlockedFor(); d != 0 {
		t.Errorf("BlockedFor() = %v after the window passed, want 0", d)
	}
	e.Observe(95, 0)
	if !e.Healthy(30) {
		t.Error("Healthy() was false after the budget recovered")
	}
}
