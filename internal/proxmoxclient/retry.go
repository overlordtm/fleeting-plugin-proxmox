package proxmoxclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"syscall"
	"time"
)

// APIError represents a non-2xx response from the Proxmox API. 404 responses are
// surfaced as ErrNotFound instead; every other status >= 400 becomes an APIError so
// callers (and IsRetryable) can inspect the status code.
type APIError struct {
	StatusCode int
	Details    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("proxmox api error: %s", e.Details)
}

// RetryPolicy bounds how long an idempotent operation is retried when it fails with a
// transient error. A non-positive MaxElapsed disables retries entirely.
type RetryPolicy struct {
	MaxElapsed time.Duration // total time budget across attempts
	BaseDelay  time.Duration // initial backoff before the first retry
	MaxDelay   time.Duration // per-attempt backoff cap
}

const (
	retryBaseDelay = 250 * time.Millisecond
	retryMaxDelay  = 5 * time.Second
)

// NewRetryPolicy returns a policy with the standard backoff shape and the given total
// budget. maxElapsed <= 0 yields a policy that performs no retries.
func NewRetryPolicy(maxElapsed time.Duration) RetryPolicy {
	return RetryPolicy{
		MaxElapsed: maxElapsed,
		BaseDelay:  retryBaseDelay,
		MaxDelay:   retryMaxDelay,
	}
}

// IsRetryable reports whether err is a transient failure worth retrying on an idempotent
// operation: network/DNS blips, connection resets, truncated responses, and 5xx/429
// server responses. Context cancellation, ErrNotFound, and 4xx responses are terminal.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrNotFound) {
		return false
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode >= 500 || apiErr.StatusCode == http.StatusTooManyRequests
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	// net.Error covers *url.Error, *net.OpError and *net.DNSError, which all satisfy the
	// interface (they expose Timeout/Temporary), so DNS resolution failures and dial/read
	// timeouts are caught here.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}

// Retry runs fn until it succeeds, returns a non-retryable error, or the policy budget /
// ctx is exhausted, sleeping with exponentially increasing, fully-jittered backoff between
// attempts. The last error is returned when the budget is exhausted.
func Retry(ctx context.Context, policy RetryPolicy, fn func(ctx context.Context) error) error {
	err := fn(ctx)
	if err == nil || !IsRetryable(err) || policy.MaxElapsed <= 0 {
		return err
	}

	deadline := time.Now().Add(policy.MaxElapsed)
	base := policy.BaseDelay
	if base <= 0 {
		base = retryBaseDelay
	}
	next := base

	for {
		if ctx.Err() != nil {
			return err
		}

		attemptDelay := next
		if policy.MaxDelay > 0 && attemptDelay > policy.MaxDelay {
			attemptDelay = policy.MaxDelay
		}
		sleep := time.Duration(rand.Int63n(int64(attemptDelay) + 1))
		if remaining := time.Until(deadline); remaining <= 0 {
			return err
		} else if sleep > remaining {
			sleep = remaining
		}

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}

		err = fn(ctx)
		if err == nil || !IsRetryable(err) {
			return err
		}

		next *= 2
		if policy.MaxDelay > 0 && next > policy.MaxDelay {
			next = policy.MaxDelay
		}
	}
}
