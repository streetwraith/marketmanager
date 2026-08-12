//go:build integration

// Integration tests hit the real ESI. They are deliberately cheap: the global
// PLEX market is a single page, so a full run costs 2 tokens of the 12,000 the
// market-order bucket allows per 15 minutes. Run with:
//
//	MM_TEST_USER_AGENT="marketmanager-test/1.0 (you@example.com)" \
//	  go test -tags integration ./internal/esi/
package esi

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"
)

func liveClient(t *testing.T) *Client {
	t.Helper()
	ua := os.Getenv("MM_TEST_USER_AGENT")
	if ua == "" {
		t.Skip("MM_TEST_USER_AGENT not set")
	}
	return New(Options{
		BaseURL:           "https://esi.evetech.net",
		UserAgent:         ua,
		CompatibilityDate: "2026-08-04", ConnectTimeout: 5 * time.Second, RequestTimeout: 30 * time.Second,
		RPS:         10,
		Concurrency: 4,
	})
}

// The global PLEX market is region 19000001 (GPMR-01 in the SDE). It holds only
// PLEX (type 44992), always fits one page, and is disjoint from every normal
// region feed.
func TestLiveGlobalPLEXMarket(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	p, err := c.OrdersPage(ctx, 19000001, 1)
	if err != nil {
		t.Fatalf("OrdersPage: %v", err)
	}
	if p.Pages != 1 {
		t.Errorf("Pages = %d, want 1; the global PLEX market outgrew one page", p.Pages)
	}
	if len(p.Orders) == 0 {
		t.Fatal("no orders returned")
	}
	for _, o := range p.Orders {
		if o.TypeID != 44992 {
			t.Fatalf("found type %d; the global market should hold only PLEX (44992)", o.TypeID)
			break
		}
	}
	// The scheduler depends on both of these, and on their 300s spacing.
	if p.Expires.IsZero() || p.LastModified.IsZero() {
		t.Errorf("missing cache headers: Expires=%v Last-Modified=%v", p.Expires, p.LastModified)
	}
	if d := p.Expires.Sub(p.LastModified); d != 5*time.Minute {
		t.Errorf("Expires - Last-Modified = %v, want 5m (server cache TTL)", d)
	}
	// The budget governor is useless without this header.
	if rem, ok := c.Budget.Remaining(); !ok {
		t.Error("no X-Ratelimit-Remaining seen; the budget governor cannot work")
	} else if rem <= 0 || rem > 12000 {
		t.Errorf("Remaining = %d, outside the documented 12000 bucket", rem)
	}
	t.Logf("orders=%d pages=%d remaining=%d expires=%s",
		len(p.Orders), p.Pages, mustRemaining(c), p.Expires.Format(time.RFC3339))
}

func mustRemaining(c *Client) int {
	n, _ := c.Budget.Remaining()
	return n
}

// Setting Transport.DialContext disables automatic HTTP/2 unless ForceAttemptHTTP2
// is set. A silent fallback to HTTP/1.1 would serialise every sweep behind
// MaxIdleConnsPerHost connections instead of multiplexing streams, which is a
// large performance regression that nothing else in the suite would catch.
func TestLiveStillNegotiatesHTTP2(t *testing.T) {
	c := liveClient(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		c.baseURL+"/markets/19000001/orders?page=1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-Compatibility-Date", c.compatDate)

	resp, err := c.hc.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Proto != "HTTP/2.0" {
		t.Errorf("negotiated %s, want HTTP/2.0 — the sweep will serialise", resp.Proto)
	}
	t.Logf("proto=%s status=%d", resp.Proto, resp.StatusCode)
}
