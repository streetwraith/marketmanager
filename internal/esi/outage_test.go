package esi

import (
	"testing"
	"testing/synctest"
	"time"
)

// One 5xx is a single bad node. Pausing every region for it would cost more
// freshness than the strikes it saves.
func TestOutageIgnoresASingle5xx(t *testing.T) {
	var o Outage
	o.Observe(503, 0)
	if d := o.PausedFor(); d != 0 {
		t.Errorf("PausedFor() = %v after one 503, want 0", d)
	}
}

func TestOutageArmsOnTheSecond5xx(t *testing.T) {
	tests := []struct {
		name       string
		statuses   []int
		retryAfter time.Duration
		// wantPause is the exact pause the gate must arm, or zero for no pause.
		// The check allows a second of slack for the clock.
		wantPause time.Duration
	}{
		{"two 503s pause for the default minute", []int{503, 503}, 0, time.Minute},
		{"a 502 and a 504 are both upstream failures", []int{502, 504}, 0, time.Minute},
		{"Retry-After wins over the default", []int{503, 503}, 20 * time.Second, 20 * time.Second},
		{"a 200 between them breaks the run", []int{503, 200, 503}, 0, 0},
		{"a 404 is not an outage", []int{404, 404}, 0, 0},
		{"a 420 is the error limit, not an outage", []int{420, 420}, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var o Outage
			for _, s := range tc.statuses {
				o.Observe(s, tc.retryAfter)
			}
			d := o.PausedFor()
			if tc.wantPause == 0 {
				if d != 0 {
					t.Errorf("PausedFor() = %v, want 0", d)
				}
				return
			}
			if d > tc.wantPause || d < tc.wantPause-time.Second {
				t.Errorf("PausedFor() = %v, want %v", d, tc.wantPause)
			}
		})
	}
}

// The probe that ends the pause is a normal request. Its success must resume
// every region at once, which is the whole point of probing frequently.
func TestOutageSuccessClearsThePause(t *testing.T) {
	var o Outage
	o.Observe(503, 0)
	o.Observe(503, 0)
	if o.PausedFor() == 0 {
		t.Fatal("no pause after two 503s")
	}

	o.Observe(200, 0)
	if d := o.PausedFor(); d != 0 {
		t.Errorf("PausedFor() = %v after the upstream answered, want 0", d)
	}
}

// A short Retry-After arriving late must not release 25 regions into an upstream
// that an earlier response said would be down for longer.
func TestOutageNeverShortensAnActivePause(t *testing.T) {
	var o Outage
	o.Observe(503, 5*time.Minute)
	o.Observe(503, 5*time.Minute)
	long := o.PausedFor()

	o.Observe(503, time.Second)
	if got := o.PausedFor(); got < long-time.Second {
		t.Errorf("a 1s Retry-After cut a %v pause down to %v", long, got)
	}
}

func TestOutageRecoversWhenThePauseExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var o Outage
		o.Observe(503, 0)
		o.Observe(503, 0)
		time.Sleep(defaultOutagePause + time.Second)
		if d := o.PausedFor(); d != 0 {
			t.Errorf("PausedFor() = %v after the pause expired, want 0", d)
		}
	})
}
