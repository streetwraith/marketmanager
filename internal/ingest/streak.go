package ingest

import "sync"

// streak tracks which keys are inside a run of failures, so a ticker-driven job
// reports its first failure and then stays quiet until it succeeds again. One
// fault must produce one event; PROJECT.md records what that saves here.
//
// The zero value is ready to use.
type streak struct {
	mu     sync.Mutex
	firing map[string]bool
}

// first reports whether a failure at key starts a new run. It answers false
// while a run is already in progress.
//
// It never inspects the error. Every caller already gates on a live context, and
// a real fault can still wrap context.Canceled; filtering here would drop it.
func (s *streak) first(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firing[key] {
		return false
	}
	if s.firing == nil {
		s.firing = make(map[string]bool)
	}
	s.firing[key] = true
	return true
}

// ends clears the run at key after a success, so the next failure reports again.
func (s *streak) ends(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.firing, key)
}
