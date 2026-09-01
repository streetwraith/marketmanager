// Package sentrytest records the error events a test's code sends, instead of
// delivering them to a server.
package sentrytest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// Sink is a sentry Transport that records events instead of sending them.
type Sink struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (s *Sink) Configure(sentry.ClientOptions)        {}
func (s *Sink) Flush(time.Duration) bool              { return true }
func (s *Sink) FlushWithContext(context.Context) bool { return true }
func (s *Sink) Close()                                {}

func (s *Sink) SendEvent(e *sentry.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

// Captured returns the events recorded so far.
func (s *Sink) Captured() []*sentry.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*sentry.Event(nil), s.events...)
}

// Capture points the global sentry hub at a Sink for one test, then restores it.
// Call it outside a synctest bubble: the sentry client starts background
// goroutines that outlive the bubble, and a bubble only ends once every goroutine
// inside it has exited.
func Capture(t *testing.T) *Sink {
	t.Helper()
	sink := &Sink{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:       "https://public@example.invalid/1",
		Transport: sink,
	})
	if err != nil {
		t.Fatalf("sentry client: %v", err)
	}
	hub := sentry.CurrentHub()
	prev := hub.Client()
	hub.BindClient(client)
	t.Cleanup(func() {
		hub.BindClient(prev)
		client.Close()
	})
	return sink
}
