package everef

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// bzip2Fixture compresses s with the bzip2 binary. Go has a bzip2 reader but no
// writer, and the point of these tests is to exercise the real decompression
// path rather than a stand-in.
func bzip2Fixture(t *testing.T, s string) []byte {
	t.Helper()
	if _, err := exec.LookPath("bzip2"); err != nil {
		t.Skip("bzip2 not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bzip2", "-c")
	cmd.Stdin = strings.NewReader(s)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("bzip2: %v", err)
	}
	return out
}

func TestOpenDayDecompresses(t *testing.T) {
	const csv = "average,date\n1.5,2026-08-05\n"
	body := bzip2Fixture(t, csv)

	var gotPath, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotUA = r.URL.Path, r.Header.Get("User-Agent")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "marketmanager-test/1.0 (test@example.com)")
	rc, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("OpenDay: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != csv {
		t.Errorf("decompressed %q, want %q", got, csv)
	}
	// The year directory and the filename both derive from the date.
	if want := "/2026/market-history-2026-08-05.csv.bz2"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if gotUA == "" {
		t.Error("no User-Agent sent; EVE Ref asks callers to identify themselves")
	}
}

// Close must reach the HTTP response, not just the decompressor, or every day
// file leaks a connection.
func TestOpenDayCloseReleasesTheResponse(t *testing.T) {
	body := bzip2Fixture(t, "average,date\n1.5,2026-08-05\n")
	var closed atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "t")
	c.hc.Transport = &closeTrackingTransport{rt: http.DefaultTransport, closed: &closed}

	rc, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("OpenDay: %v", err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("read: %v", err)
	}
	if closed.Load() {
		t.Fatal("response body closed before the caller closed the stream")
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed.Load() {
		t.Error("closing the decompressed stream did not close the HTTP response")
	}
}

type closeTrackingTransport struct {
	rt     http.RoundTripper
	closed *atomic.Bool
}

func (c *closeTrackingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.rt.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	resp.Body = &trackingBody{ReadCloser: resp.Body, closed: c.closed}
	return resp, nil
}

type trackingBody struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (t *trackingBody) Close() error {
	t.closed.Store(true)
	return t.ReadCloser.Close()
}

// A day that is not published yet is an expected, recoverable condition: the
// importer skips it and tries again on a later poll.
func TestOpenDayNotFoundIsASentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "t")
	_, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrDayNotFound) {
		t.Fatalf("err = %v, want ErrDayNotFound", err)
	}
}

func TestOpenDayOtherStatusIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "t")
	_, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected an error on a 500")
	}
	if errors.Is(err, ErrDayNotFound) {
		t.Error("a 500 was reported as a missing day; it would be silently skipped forever")
	}
}

func TestTotals(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/totals.json" {
			t.Errorf("path = %q, want /totals.json", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"2026-08-05":46987,"2026-08-09":19768}`))
	}))
	t.Cleanup(srv.Close)

	got, err := New(srv.URL, "t").Totals(context.Background())
	if err != nil {
		t.Fatalf("Totals: %v", err)
	}
	if got["2026-08-05"] != 46987 || got["2026-08-09"] != 19768 {
		t.Errorf("Totals = %v", got)
	}
}

// EVE Ref is a third-party service, and bzip2 reaches ~1000:1 on crafted input.
// An unbounded stream here could exhaust memory and take down order ingestion.
func TestOpenDayCapsDecompressedSize(t *testing.T) {
	const payload = 100_000
	body := bzip2Fixture(t, strings.Repeat("a", payload))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "t")
	c.maxDecompressed = 1024 // far below what the stream would expand to

	rc, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("OpenDay: %v", err)
	}
	defer func() { _ = rc.Close() }()

	n, err := io.Copy(io.Discard, rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != c.maxDecompressed {
		t.Errorf("read %d bytes from a %d-byte payload; the cap did not truncate at %d",
			n, payload, c.maxDecompressed)
	}
}

// The compressed side is capped too, so a slow endless response cannot be fed in
// forever even if it never expands.
func TestOpenDayCapsCompressedSize(t *testing.T) {
	body := bzip2Fixture(t, strings.Repeat("a", 100_000))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "t")
	c.maxCompressed = 10 // truncates mid-stream, so decompression must fail

	rc, err := c.OpenDay(context.Background(), time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("OpenDay: %v", err)
	}
	defer func() { _ = rc.Close() }()

	if _, err := io.Copy(io.Discard, rc); err == nil {
		t.Error("a truncated bzip2 stream decoded without error")
	}
}
