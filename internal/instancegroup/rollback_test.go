package instancegroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
)

// rollbackTestServer serves the calls safeDestroyProvisioningVM makes for VM node1/1000,
// failing the first DELETE with a transient 503 to model a network blip during cleanup.
func rollbackTestServer(t *testing.T) (*httptest.Server, *int64) {
	t.Helper()
	var deletes int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/node1/qemu/1000/config":
			_, _ = w.Write([]byte(`{"data":{"name":"runner-1000","pool":"gitlab-runners","template":0}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/node1/qemu/1000/status/current":
			_, _ = w.Write([]byte(`{"data":{"status":"stopped"}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/api2/json/nodes/node1/qemu/1000":
			if atomic.AddInt64(&deletes, 1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"data":null}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":"UPID:delete"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api2/json/nodes/node1/tasks/UPID:delete/status":
			_, _ = w.Write([]byte(`{"data":{"status":"stopped","exitstatus":"OK"}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return server, &deletes
}

func rollbackTestGroup(t *testing.T, baseURL string) *Group {
	t.Helper()
	// GET-layer retry disabled so the transient DELETE failure is only recovered by the
	// rollback retry wrapper under test, not masked by lower-level retries.
	client, err := proxmoxclient.New(proxmoxclient.Config{
		BaseURL:         baseURL,
		TokenID:         "user@pam!token",
		TokenSecret:     "secret",
		RetryMaxElapsed: 0,
	})
	require.NoError(t, err)
	return &Group{
		client: client,
		cfg: Config{
			NamePrefix:       "runner",
			Pool:             "gitlab-runners",
			VMIDMin:          1000,
			VMIDMax:          2000,
			TaskPollInterval: time.Millisecond,
			ShutdownTimeout:  time.Second,
			CloneTimeout:     2 * time.Second,
		},
	}
}

func TestRollbackRetryDeletesVMDespiteTransientFailure(t *testing.T) {
	server, deletes := rollbackTestServer(t)
	group := rollbackTestGroup(t, server.URL)

	policy := proxmoxclient.RetryPolicy{MaxElapsed: 2 * time.Second, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	err := proxmoxclient.Retry(context.Background(), policy, func(ctx context.Context) error {
		return group.safeDestroyProvisioningVM(ctx, "node1", 1000, "runner-1000")
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), atomic.LoadInt64(deletes), "delete should be retried after the transient 503")
}

func TestRollbackWithoutRetryStrandsVMOnTransientFailure(t *testing.T) {
	server, deletes := rollbackTestServer(t)
	group := rollbackTestGroup(t, server.URL)

	// A single attempt (no retry) fails on the transient DELETE — the pre-fix behaviour that
	// left the VM stranded.
	err := group.safeDestroyProvisioningVM(context.Background(), "node1", 1000, "runner-1000")

	require.Error(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(deletes))
}
