package esi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// StatusErrorLimited is the legacy per-IP error limit. Once tripped, ESI returns
// it for every route, so it must stop all work, not just the failing region.
const StatusErrorLimited = 420

// HTTPError is any non-200 response, carrying the headers needed to react.
type HTTPError struct {
	Status int
	// RetryAfter is set on 429.
	RetryAfter time.Duration
	// ErrorLimitRemain and ErrorLimitReset track the legacy 100-errors-per-minute
	// limit, which is global to the source IP and shared with other apps.
	ErrorLimitRemain int
	ErrorLimitReset  time.Duration
	HasErrorLimit    bool
}

// Note: the response body is deliberately not captured. Nothing needs it, and
// keeping it would put ESI payload bytes one %+v away from the logs.

func (e *HTTPError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("esi: http %d (retry after %s)", e.Status, e.RetryAfter)
	}
	return fmt.Sprintf("esi: http %d", e.Status)
}

func newHTTPError(resp *http.Response) *HTTPError {
	e := &HTTPError{Status: resp.StatusCode}
	if v := resp.Header.Get("Retry-After"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			e.RetryAfter = time.Duration(n) * time.Second
		}
	}
	if v := resp.Header.Get("X-ESI-Error-Limit-Remain"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			e.ErrorLimitRemain, e.HasErrorLimit = n, true
		}
	}
	if v := resp.Header.Get("X-ESI-Error-Limit-Reset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			e.ErrorLimitReset = time.Duration(n) * time.Second
		}
	}
	return e
}

// IsRateLimited reports a 429, which costs 5 tokens and one error-limit strike.
func IsRateLimited(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status == http.StatusTooManyRequests
}

// IsErrorLimited reports a 420. Everything must stop until the window resets.
func IsErrorLimited(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status == StatusErrorLimited
}

// IsServerError reports a 5xx. These cost zero tokens but still strike the
// legacy error limit, so they are cheap to retry but not free.
func IsServerError(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status >= 500
}

// TokenCost is what a response costs against the market-order bucket.
// Source: the ESI rate-limiting documentation.
func TokenCost(status int) int {
	switch {
	case status == 304:
		return 1
	case status >= 200 && status < 300:
		return 2
	case status >= 300 && status < 400:
		return 1
	case status >= 400 && status < 500:
		return 5
	default: // 5xx
		return 0
	}
}
