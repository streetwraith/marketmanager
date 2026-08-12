// Package everef reads the EVE Ref daily market history dataset.
//
// EVE Ref is a third-party, best-effort service. Be polite: poll the small index,
// never the day files, and send an identifying User-Agent.
package everef

import (
	"compress/bzip2"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DateFormat is how EVE Ref names days, in both totals.json and the file paths.
const DateFormat = "2006-01-02"

// maxTotalsBytes caps the index. It is ~30 KB today, one entry per day since 2022.
const maxTotalsBytes = 8 << 20

// maxDayBytes caps a compressed day file, and maxDayDecompressedBytes caps what
// it may expand to.
//
// Both matter: EVE Ref is a third-party, best-effort service, and bzip2 reaches
// roughly 1000:1 on crafted input. An unbounded stream here could exhaust memory
// and take down order ingestion, which is this service's actual job. A complete
// day is ~550 KB compressed and ~7 MB decompressed, so these leave wide headroom.
const (
	maxDayBytes             = 64 << 20
	maxDayDecompressedBytes = 512 << 20
)

type Client struct {
	baseURL   string
	userAgent string
	hc        *http.Client

	// Size caps, overridable so tests can prove truncation without materialising
	// half a gigabyte.
	maxCompressed   int64
	maxDecompressed int64
}

func New(baseURL, userAgent string) *Client {
	return &Client{
		baseURL:   baseURL,
		userAgent: userAgent,
		// Day files are a few hundred KB compressed, but a backfill fetches many.
		hc:              &http.Client{Timeout: 5 * time.Minute},
		maxCompressed:   maxDayBytes,
		maxDecompressed: maxDayDecompressedBytes,
	}
}

// Totals returns the record count EVE Ref reports for each day.
//
// A day's count keeps growing for about four days after the date, because EVE Ref
// works through (region, type) pairs over successive scrapes. The count is
// therefore a change signal, not a completeness signal.
func (c *Client) Totals(ctx context.Context) (map[string]int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/totals.json", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch totals: %w", err)
	}
	//nolint:errcheck // the body is fully read; a close error is not actionable
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch totals: http %d", resp.StatusCode)
	}

	var totals map[string]int64
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxTotalsBytes)).Decode(&totals); err != nil {
		return nil, fmt.Errorf("decode totals: %w", err)
	}
	return totals, nil
}

// ErrDayNotFound means EVE Ref has not published that day yet.
var ErrDayNotFound = fmt.Errorf("everef: day file not found")

// OpenDay streams and decompresses one day file. The caller closes the reader.
//
// The file is decompressed in flight rather than staged on disk: a day is a few
// hundred KB compressed and the rows are filtered as they are read, so there is
// nothing to leave behind afterwards.
func (c *Client) OpenDay(ctx context.Context, day time.Time) (io.ReadCloser, error) {
	d := day.Format(DateFormat)
	url := fmt.Sprintf("%s/%d/market-history-%s.csv.bz2", c.baseURL, day.Year(), d)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch day %s: %w", d, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s", ErrDayNotFound, d)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("fetch day %s: http %d", d, resp.StatusCode)
	}
	compressed := io.LimitReader(resp.Body, c.maxCompressed)
	return &bzipReader{
		Reader: io.LimitReader(bzip2.NewReader(compressed), c.maxDecompressed),
		closer: resp.Body,
	}, nil
}

// bzipReader closes the underlying response when the decompressed stream is closed.
type bzipReader struct {
	io.Reader
	closer io.Closer
}

func (b *bzipReader) Close() error { return b.closer.Close() }
