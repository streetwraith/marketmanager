package esi

import "sync"

// Budget tracks the market-order token bucket.
//
// The bucket is a 15-minute sliding window: each spent token returns exactly 900
// seconds after it was spent, and ESI publishes no reset header. Rather than
// model that locally, the governor simply believes X-Ratelimit-Remaining, which
// arrives on every response. That is simpler, and strictly better in one way: the
// header reflects every consumer sharing the source IP, not just this service.
type Budget struct {
	mu        sync.Mutex
	remaining int
	known     bool
}

// Observe records the value carried by a response.
func (b *Budget) Observe(remaining int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.remaining, b.known = remaining, true
}

// Remaining reports the last seen value, and whether anything has been seen yet.
func (b *Budget) Remaining() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining, b.known
}

// Fits reports whether spending cost tokens would still leave the reserve intact.
// Before the first response there is no information, so it allows the request:
// that first request is what establishes the budget.
func (b *Budget) Fits(cost, reserve int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.known {
		return true
	}
	return b.remaining-cost >= reserve
}

// BelowFloor reports whether the fetcher should stop entirely and probe for
// recovery instead.
func (b *Budget) BelowFloor(floor int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.known && b.remaining < floor
}
