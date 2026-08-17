package esi

import (
	"sync"
	"time"
)

// Outage pauses every caller while ESI answers 5xx.
//
// A 5xx costs no tokens, so the budget never notices one. The legacy error limit
// does: an upstream maintenance answers 5xx on every route, and each region cycle
// then spends strikes against a limit that is counted per IP and shared with
// every other application on this host. Measured on 2026-08-17, a 50-minute
// maintenance produced about 40 failed cycles per minute against a limit of 100
// errors per minute.
//
// One 5xx is a single bad node and must not stop the service. The second in a row
// means the upstream is down.
type Outage struct {
	mu          sync.Mutex
	consecutive int
	pausedUntil time.Time
}

const (
	outageThreshold = 2
	// defaultOutagePause applies when the response carries no Retry-After.
	defaultOutagePause = time.Minute
)

// Observe records the status of one response. retryAfter is the Retry-After
// header of that response, or zero when it carries none.
func (o *Outage) Observe(status int, retryAfter time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if status < 500 {
		// The upstream answers again. Resume at once rather than sit out the rest
		// of a pause the upstream has already ended.
		o.consecutive = 0
		o.pausedUntil = time.Time{}
		return
	}

	o.consecutive++
	if o.consecutive < outageThreshold {
		return
	}
	d := retryAfter
	if d <= 0 {
		d = defaultOutagePause
	}
	// Only ever extend: a later, shorter Retry-After must not cut a longer pause
	// short and let 25 regions resume into an upstream that is still down.
	if until := time.Now().Add(d); until.After(o.pausedUntil) {
		o.pausedUntil = until
	}
}

// PausedFor reports how long to wait before fetching again, or zero.
//
// The probe that tests for recovery must ignore this: something has to call ESI
// for the pause to ever clear.
func (o *Outage) PausedFor() time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	if d := time.Until(o.pausedUntil); d > 0 {
		return d
	}
	return 0
}
