package esi

import (
	"sync"
	"time"
)

// ErrorLimit tracks the legacy per-IP error limit: 100 non-2xx/3xx responses per
// minute, after which ESI answers 420 on every route.
//
// It is entirely separate from the token bucket, and it matters more than the
// token cost suggests. A 5xx costs zero tokens but still strikes this limit, and
// tripping it blocks every other application sharing the source IP, including
// authenticated calls this service knows nothing about.
type ErrorLimit struct {
	mu           sync.Mutex
	remain       int
	known        bool
	blockedUntil time.Time
}

// Observe records the headers carried by an error response.
func (e *ErrorLimit) Observe(remain int, reset time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.remain, e.known = remain, true
	if reset > 0 {
		e.resetAt(reset)
	}
}

// Block stops all work until the error window resets. Called on a 420.
func (e *ErrorLimit) Block(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if d <= 0 {
		d = time.Minute // the documented window, when no header says otherwise
	}
	e.resetAt(d)
}

// resetAt extends the block. The caller must hold the lock.
func (e *ErrorLimit) resetAt(d time.Duration) {
	until := time.Now().Add(d)
	if until.After(e.blockedUntil) {
		e.blockedUntil = until
	}
}

// BlockedFor reports how long to wait before issuing any request, or zero.
func (e *ErrorLimit) BlockedFor() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if d := time.Until(e.blockedUntil); d > 0 {
		return d
	}
	return 0
}

// Healthy reports whether there is enough error budget left to risk a retry.
// Before any error has been seen there is nothing to worry about.
func (e *ErrorLimit) Healthy(floor int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.known {
		return true
	}
	return e.remain > floor
}
