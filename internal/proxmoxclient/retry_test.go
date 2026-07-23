package proxmoxclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"not found", fmt.Errorf("%w: missing", ErrNotFound), false},
		{"api 500", &APIError{StatusCode: 500}, true},
		{"api 502 wrapped", fmt.Errorf("call: %w", &APIError{StatusCode: 502}), true},
		{"api 429", &APIError{StatusCode: http.StatusTooManyRequests}, true},
		{"api 400", &APIError{StatusCode: 400}, false},
		{"api 403", &APIError{StatusCode: 403}, false},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"connection reset", syscall.ECONNRESET, true},
		{"dns timeout", &net.DNSError{Err: "i/o timeout", Name: "pve2.example.test", IsTimeout: true}, true},
		{"url wrapped op error", &url.Error{Op: "Get", URL: "https://pve2", Err: &net.OpError{Op: "dial", Err: errors.New("boom")}}, true},
		{"opaque error", errors.New("boom"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsRetryable(tc.err))
		})
	}
}

func fastPolicy() RetryPolicy {
	return RetryPolicy{MaxElapsed: time.Second, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func TestRetrySucceedsFirstAttempt(t *testing.T) {
	var calls int
	err := Retry(context.Background(), fastPolicy(), func(context.Context) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestRetryRecoversAfterTransientErrors(t *testing.T) {
	var calls int
	err := Retry(context.Background(), fastPolicy(), func(context.Context) error {
		calls++
		if calls < 3 {
			return &APIError{StatusCode: 503}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestRetryStopsOnNonRetryableError(t *testing.T) {
	var calls int
	err := Retry(context.Background(), fastPolicy(), func(context.Context) error {
		calls++
		return &APIError{StatusCode: 400}
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestRetryDisabledWhenBudgetZero(t *testing.T) {
	var calls int
	err := Retry(context.Background(), NewRetryPolicy(0), func(context.Context) error {
		calls++
		return &APIError{StatusCode: 503}
	})
	require.Error(t, err)
	require.Equal(t, 1, calls, "no retries when MaxElapsed is zero")
}

func TestRetryStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	err := Retry(ctx, RetryPolicy{MaxElapsed: time.Minute, BaseDelay: 20 * time.Millisecond, MaxDelay: 20 * time.Millisecond}, func(context.Context) error {
		calls++
		cancel()
		return &APIError{StatusCode: 503}
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

// jsonServer returns an httptest server whose handler is driven by fn, plus a pointer to the
// request counter.
func jsonServer(t *testing.T, fn func(count int64, w http.ResponseWriter, r *http.Request)) (*httptest.Server, *int64) {
	t.Helper()
	var count int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&count, 1)
		w.Header().Set("Content-Type", "application/json")
		fn(n, w, r)
	}))
	t.Cleanup(server.Close)
	return server, &count
}

func newRetryingClient(t *testing.T, baseURL string, maxElapsed time.Duration) *Client {
	t.Helper()
	client, err := New(Config{
		BaseURL:         baseURL,
		TokenID:         "user@pam!token",
		TokenSecret:     "secret",
		RetryMaxElapsed: maxElapsed,
	})
	require.NoError(t, err)
	return client
}

func TestGetRetriesTransientServerError(t *testing.T) {
	server, count := jsonServer(t, func(n int64, w http.ResponseWriter, _ *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"data":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"release":"8.2"}}`))
	})

	client := newRetryingClient(t, server.URL, 2*time.Second)
	version, err := client.GetVersion(context.Background())
	require.NoError(t, err)
	require.Equal(t, "8.2", version.Release)
	require.Equal(t, int64(2), atomic.LoadInt64(count), "should retry once after 503")
}

func TestGetDoesNotRetryClientError(t *testing.T) {
	server, count := jsonServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"data":null}`))
	})

	client := newRetryingClient(t, server.URL, 2*time.Second)
	_, err := client.GetVersion(context.Background())
	require.Error(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(count), "4xx must not be retried")
}

func TestGetPreservesNotFound(t *testing.T) {
	server, count := jsonServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"data":null}`))
	})

	client := newRetryingClient(t, server.URL, 2*time.Second)
	_, err := client.GetVMConfig(context.Background(), "node1", 1000)
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, int64(1), atomic.LoadInt64(count), "404 must not be retried")
}

func TestWaitForTaskRidesOutTransientPollErrors(t *testing.T) {
	server, count := jsonServer(t, func(n int64, w http.ResponseWriter, _ *http.Request) {
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"data":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
	})

	// GET-layer retry disabled so this exercises WaitForTask's own tolerance loop.
	client := newRetryingClient(t, server.URL, 0)
	err := client.WaitForTask(context.Background(), "node1", "UPID:clone", time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, atomic.LoadInt64(count), int64(3))
}

func TestWaitForTaskReturnsTaskFailure(t *testing.T) {
	server, _ := jsonServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"stopped","exitstatus":"clone failed"}}`))
	})

	client := newRetryingClient(t, server.URL, 0)
	err := client.WaitForTask(context.Background(), "node1", "UPID:clone", time.Millisecond)
	require.ErrorContains(t, err, "clone failed")
}

func TestWaitForTaskStopsOnTerminalError(t *testing.T) {
	server, count := jsonServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"data":null}`))
	})

	client := newRetryingClient(t, server.URL, 0)
	err := client.WaitForTask(context.Background(), "node1", "UPID:clone", time.Millisecond)
	require.Error(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(count), "a 4xx poll response is terminal")
}
