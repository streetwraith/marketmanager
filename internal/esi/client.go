// Package esi talks to the EVE Swagger Interface, politely.
package esi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// maxPageBytes caps one order page. Observed pages are ~240 KB decompressed;
// this is generous headroom against a malformed response.
const maxPageBytes = 32 << 20

// Client is a rate-limited ESI client. It is safe for concurrent use.
type Client struct {
	baseURL    string
	userAgent  string
	compatDate string

	hc      *http.Client
	limiter *rate.Limiter
	sem     chan struct{}

	Budget     *Budget
	ErrorLimit *ErrorLimit
}

type Options struct {
	BaseURL           string
	UserAgent         string
	CompatibilityDate string
	// ConnectTimeout bounds establishing a connection: TCP dial and TLS
	// handshake. A connection that cannot be made in a few seconds is dead, and
	// failing fast on it is free.
	ConnectTimeout time.Duration
	// RequestTimeout bounds a whole request including the body read. It is
	// deliberately far more generous than ConnectTimeout: a slow response is not
	// a dead one, and ESI has been measured at a ~15s median during peak hours.
	// Timing out early does not cancel the server's work, so the token is spent
	// either way and an impatient retry simply spends it twice.
	RequestTimeout time.Duration
	// RPS is the politeness contract and binds where latency is low.
	RPS float64
	// Concurrency binds where latency is high, since throughput is roughly
	// concurrency divided by latency.
	Concurrency int
}

func New(o Options) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// The default of 2 would serialise the sweep if the connection ever falls
	// back to HTTP/1.1. ESI speaks HTTP/2, where these become streams.
	tr.MaxIdleConnsPerHost = o.Concurrency
	tr.MaxConnsPerHost = o.Concurrency
	// Setting DialContext below disables automatic HTTP/2 negotiation unless this
	// is set. Without h2 the sweep would fall back to one request per connection.
	tr.ForceAttemptHTTP2 = true
	tr.DialContext = (&net.Dialer{
		Timeout:   o.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	// The TLS handshake is the other half of connecting, so it gets the same
	// budget rather than the 10s default.
	tr.TLSHandshakeTimeout = o.ConnectTimeout

	return &Client{
		baseURL:    o.BaseURL,
		userAgent:  o.UserAgent,
		compatDate: o.CompatibilityDate,
		hc:         &http.Client{Timeout: o.RequestTimeout, Transport: tr},
		// Burst of 1 keeps the emitted rate smooth rather than spiky.
		limiter:    rate.NewLimiter(rate.Limit(o.RPS), 1),
		sem:        make(chan struct{}, o.Concurrency),
		Budget:     &Budget{},
		ErrorLimit: &ErrorLimit{},
	}
}

// response carries the parsed body plus the headers the pipeline reasons about.
type response struct {
	Status       int
	Body         []byte
	Pages        int
	Expires      time.Time
	LastModified time.Time
	Remaining    int
}

// get issues one rate-limited, concurrency-bounded GET.
func (c *Client) get(ctx context.Context, path string) (*response, error) {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-Compatibility-Date", c.compatDate)
	// Accept-Encoding is deliberately unset: the transport then adds gzip and
	// decompresses transparently.

	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck // the body is fully read below; a close error is not actionable
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPageBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	r := &response{Status: resp.StatusCode, Body: body, Pages: 1}
	if v := resp.Header.Get("X-Ratelimit-Remaining"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			r.Remaining = n
			// Every response updates the governor, including error responses.
			c.Budget.Observe(n)
		}
	}
	if v := resp.Header.Get("X-Pages"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			r.Pages = n
		}
	}
	r.Expires = parseHTTPTime(resp.Header.Get("Expires"))
	r.LastModified = parseHTTPTime(resp.Header.Get("Last-Modified"))

	// Path and status only: never the body.
	slog.Debug("esi request",
		"path", path, "status", resp.StatusCode,
		"ms", time.Since(start).Milliseconds(), "bytes", len(body),
		"remaining", r.Remaining)

	if resp.StatusCode != http.StatusOK {
		he := newHTTPError(resp)
		// Every non-2xx strikes the legacy error limit, whatever it cost in tokens.
		if he.HasErrorLimit {
			c.ErrorLimit.Observe(he.ErrorLimitRemain, he.ErrorLimitReset)
		}
		if he.Status == StatusErrorLimited {
			c.ErrorLimit.Block(he.ErrorLimitReset)
		}
		return r, he
	}
	return r, nil
}

func (c *Client) getJSON(ctx context.Context, path string, v any) (*response, error) {
	r, err := c.get(ctx, path)
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(r.Body, v); err != nil {
		return r, fmt.Errorf("decode %s: %w", path, err)
	}
	return r, nil
}

func parseHTTPTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := http.ParseTime(s)
	if err != nil {
		return time.Time{}
	}
	return t
}
