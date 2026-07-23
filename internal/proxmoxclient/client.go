package proxmoxclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrNotFound = errors.New("resource not found")

type Config struct {
	BaseURL            string
	TokenID            string
	TokenSecret        string
	TLSCAFile          string
	InsecureSkipVerify bool
	AllowedServerNames []string
	// RetryMaxElapsed bounds how long an idempotent read is retried when it fails with a
	// transient network/DNS/5xx error. Zero disables retries.
	RetryMaxElapsed time.Duration
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	authHeader string
	retry      RetryPolicy
}

type envelope[T any] struct {
	Data T `json:"data"`
}

type Version struct {
	Release string `json:"release"`
}

type ClusterResource struct {
	ID         string  `json:"id"`
	Type       string  `json:"type"`
	PluginType string  `json:"plugintype"`
	Pool       string  `json:"pool"`
	Node       string  `json:"node"`
	Name       string  `json:"name"`
	Storage    string  `json:"storage"`
	Tags       string  `json:"tags"`
	Status     string  `json:"status"`
	Template   int     `json:"template"`
	Shared     int     `json:"shared"`
	VMID       int     `json:"vmid"`
	Disk       int64   `json:"disk"`
	MaxMem     int64   `json:"maxmem"`
	MaxDisk    int64   `json:"maxdisk"`
	MaxCPU     float64 `json:"maxcpu"`
	CPU        float64 `json:"cpu"`
}

func (r *ClusterResource) UnmarshalJSON(data []byte) error {
	var raw struct {
		ID         string     `json:"id"`
		Type       string     `json:"type"`
		PluginType string     `json:"plugintype"`
		Pool       string     `json:"pool"`
		Node       string     `json:"node"`
		Name       string     `json:"name"`
		Storage    string     `json:"storage"`
		Tags       string     `json:"tags"`
		Status     string     `json:"status"`
		Template   laxInt     `json:"template"`
		Shared     laxInt     `json:"shared"`
		VMID       laxInt     `json:"vmid"`
		Disk       laxInt64   `json:"disk"`
		MaxMem     laxInt64   `json:"maxmem"`
		MaxDisk    laxInt64   `json:"maxdisk"`
		MaxCPU     laxFloat64 `json:"maxcpu"`
		CPU        laxFloat64 `json:"cpu"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*r = ClusterResource{
		ID:         raw.ID,
		Type:       raw.Type,
		PluginType: raw.PluginType,
		Pool:       raw.Pool,
		Node:       raw.Node,
		Name:       raw.Name,
		Storage:    raw.Storage,
		Tags:       raw.Tags,
		Status:     raw.Status,
		Template:   int(raw.Template),
		Shared:     int(raw.Shared),
		VMID:       int(raw.VMID),
		Disk:       int64(raw.Disk),
		MaxMem:     int64(raw.MaxMem),
		MaxDisk:    int64(raw.MaxDisk),
		MaxCPU:     float64(raw.MaxCPU),
		CPU:        float64(raw.CPU),
	}
	return nil
}

type laxInt int
type laxInt64 int64
type laxFloat64 float64

func (v *laxInt) UnmarshalJSON(data []byte) error {
	n, err := parseLaxInt64(data)
	if err != nil {
		return err
	}
	*v = laxInt(n)
	return nil
}

func (v *laxInt64) UnmarshalJSON(data []byte) error {
	n, err := parseLaxInt64(data)
	if err != nil {
		return err
	}
	*v = laxInt64(n)
	return nil
}

func (v *laxFloat64) UnmarshalJSON(data []byte) error {
	text, err := laxNumberText(data)
	if err != nil {
		return err
	}
	if text == "" {
		*v = 0
		return nil
	}
	n, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return err
	}
	*v = laxFloat64(n)
	return nil
}

func parseLaxInt64(data []byte) (int64, error) {
	text, err := laxNumberText(data)
	if err != nil {
		return 0, err
	}
	if text == "" {
		return 0, nil
	}
	if strings.ContainsAny(text, ".eE") {
		n, err := strconv.ParseFloat(text, 64)
		return int64(n), err
	}
	return strconv.ParseInt(text, 10, 64)
}

func laxNumberText(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" {
		return "", nil
	}
	if strings.HasPrefix(text, `"`) {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return "", err
		}
		return strings.TrimSpace(value), nil
	}
	return text, nil
}

type NodeStatus struct {
	CPU     float64 `json:"cpu"`
	CPUInfo struct {
		CPUs int `json:"cpus"`
	} `json:"cpuinfo"`
	Memory struct {
		Used  int64 `json:"used"`
		Total int64 `json:"total"`
	} `json:"memory"`
	RootFS struct {
		Used  int64 `json:"used"`
		Total int64 `json:"total"`
	} `json:"rootfs"`
}

type VMConfig struct {
	Name         string            `json:"name"`
	Pool         string            `json:"pool"`
	Tags         string            `json:"tags"`
	Description  string            `json:"description"`
	IPConfig0    string            `json:"ipconfig0"`
	BootDisk     string            `json:"bootdisk"`
	SCSI0        string            `json:"scsi0"`
	VirtIO0      string            `json:"virtio0"`
	SATA0        string            `json:"sata0"`
	IDE0         string            `json:"ide0"`
	DiskDevices  map[string]string `json:"-"`
	SSHKeys      string            `json:"sshkeys"`
	CIUser       string            `json:"ciuser"`
	NameServer   string            `json:"nameserver"`
	SearchDomain string            `json:"searchdomain"`
	Agent        string            `json:"agent"`
	Template     int               `json:"template"`
}

func (c *VMConfig) UnmarshalJSON(data []byte) error {
	type rawVMConfig VMConfig
	var raw rawVMConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*c = VMConfig(raw)

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	c.DiskDevices = make(map[string]string)
	for name, value := range fields {
		if !isQEMUDiskDevice(name) {
			continue
		}
		var disk string
		if err := json.Unmarshal(value, &disk); err != nil {
			return fmt.Errorf("decode disk device %s: %w", name, err)
		}
		if disk != "" {
			c.DiskDevices[name] = disk
		}
	}
	return nil
}

func (c VMConfig) DiskValue(device string) string {
	if value := c.DiskDevices[device]; value != "" {
		return value
	}
	switch device {
	case "scsi0":
		return c.SCSI0
	case "virtio0":
		return c.VirtIO0
	case "sata0":
		return c.SATA0
	case "ide0":
		return c.IDE0
	default:
		return ""
	}
}

func (c VMConfig) DiskDeviceNames() []string {
	names := make([]string, 0, len(c.DiskDevices)+4)
	seen := map[string]struct{}{}
	add := func(name, value string) {
		if value == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	for name, value := range c.DiskDevices {
		add(name, value)
	}
	add("scsi0", c.SCSI0)
	add("virtio0", c.VirtIO0)
	add("sata0", c.SATA0)
	add("ide0", c.IDE0)

	sort.Slice(names, func(i, j int) bool {
		leftBus, leftIndex, _ := splitQEMUDiskDevice(names[i])
		rightBus, rightIndex, _ := splitQEMUDiskDevice(names[j])
		if leftBus != rightBus {
			return diskBusOrder(leftBus) < diskBusOrder(rightBus)
		}
		if leftIndex != rightIndex {
			return leftIndex < rightIndex
		}
		return names[i] < names[j]
	})

	return names
}

func isQEMUDiskDevice(device string) bool {
	_, _, ok := splitQEMUDiskDevice(device)
	return ok
}

func splitQEMUDiskDevice(device string) (string, int, bool) {
	for _, bus := range []string{"scsi", "virtio", "sata", "ide"} {
		if !strings.HasPrefix(device, bus) {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(device, bus))
		if err != nil {
			return "", 0, false
		}
		return bus, index, true
	}
	return "", 0, false
}

func diskBusOrder(bus string) int {
	switch bus {
	case "scsi":
		return 0
	case "virtio":
		return 1
	case "sata":
		return 2
	case "ide":
		return 3
	default:
		return 4
	}
}

type VMStatus struct {
	Status    string `json:"status"`
	QMPStatus string `json:"qmpstatus"`
}

type TaskStatus struct {
	Status     string `json:"status"`
	ExitStatus string `json:"exitstatus"`
}

type PoolInfo struct {
	PoolID string `json:"poolid"`
}

type AgentInterface struct {
	Name        string           `json:"name"`
	IPAddresses []AgentIPAddress `json:"ip-addresses"`
}

type AgentIPAddress struct {
	IPAddress string `json:"ip-address"`
	IPType    string `json:"ip-address-type"`
}

type CloneRequest struct {
	NewID         int
	Name          string
	TargetNode    string
	Pool          string
	TargetStorage string
	Full          bool
	Snapshot      string
}

type MigrateRequest struct {
	TargetNode    string
	TargetStorage string
}

type SetConfigRequest struct {
	CloudInitInterface string
	Tags               []string
	Description        string
	IPConfig           string
	MemoryMB           int64
	CPUCores           int
	CIUser             string
	CIPassword         string
	SSHKeys            []string
	NameServer         string
	SearchDomain       string
	AgentEnabled       bool
	DisableCIUpgrade   bool
}

func New(cfg Config) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse api_url: %w", err)
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.TLSCAFile != "" {
		pemData, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls_ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("load tls_ca_file: no certificates found")
		}
		tlsConfig.RootCAs = pool
	}
	configureTLSVerification(baseURL, cfg, tlsConfig)

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
		authHeader: fmt.Sprintf("PVEAPIToken=%s=%s", cfg.TokenID, cfg.TokenSecret),
		retry:      NewRetryPolicy(cfg.RetryMaxElapsed),
	}, nil
}

func configureTLSVerification(baseURL *url.URL, cfg Config, tlsConfig *tls.Config) {
	if cfg.InsecureSkipVerify || len(cfg.AllowedServerNames) == 0 {
		return
	}

	allowedNames := tlsAllowedServerNames(baseURL.Hostname(), cfg.AllowedServerNames)
	if len(allowedNames) == 0 {
		return
	}

	roots := tlsConfig.RootCAs
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		return verifyTLSConnection(state, roots, allowedNames)
	}
}

func tlsAllowedServerNames(apiHost string, nodeNames []string) []string {
	seen := make(map[string]struct{}, len(nodeNames)+1)
	names := make([]string, 0, len(nodeNames)+1)
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	add(apiHost)
	for _, nodeName := range nodeNames {
		add(nodeName)
	}
	return names
}

func verifyTLSConnection(state tls.ConnectionState, roots *x509.CertPool, allowedNames []string) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("tls: server did not provide a certificate")
	}

	leaf := state.PeerCertificates[0]
	intermediates := x509.NewCertPool()
	for _, cert := range state.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}

	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return err
	}

	for _, name := range allowedNames {
		if err := leaf.VerifyHostname(name); err == nil {
			return nil
		}
	}
	return fmt.Errorf("tls: certificate is not valid for api_url host or configured nodes: allowed names %s", strings.Join(allowedNames, ", "))
}

func (c *Client) GetVersion(ctx context.Context) (Version, error) {
	var version Version
	err := c.get(ctx, "/version", &version)
	return version, err
}

func (c *Client) GetPool(ctx context.Context, pool string) (PoolInfo, error) {
	var out PoolInfo
	err := c.get(ctx, path.Join("/pools", pool), &out)
	return out, err
}

func (c *Client) ListClusterResources(ctx context.Context, resourceType string) ([]ClusterResource, error) {
	query := url.Values{}
	if resourceType != "" {
		query.Set("type", resourceType)
	}
	var resources []ClusterResource
	err := c.getWithQuery(ctx, "/cluster/resources", query, &resources)
	return resources, err
}

func (c *Client) GetNodeStatus(ctx context.Context, node string) (NodeStatus, error) {
	var out NodeStatus
	err := c.get(ctx, path.Join("/nodes", node, "status"), &out)
	return out, err
}

func (c *Client) GetVMConfig(ctx context.Context, node string, vmid int) (VMConfig, error) {
	var out VMConfig
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "config"), &out)
	return out, err
}

func (c *Client) GetVMStatus(ctx context.Context, node string, vmid int) (VMStatus, error) {
	var out VMStatus
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "current"), &out)
	return out, err
}

func (c *Client) CloneVM(ctx context.Context, sourceNode string, templateVMID int, req CloneRequest) (string, error) {
	form := url.Values{}
	form.Set("newid", fmt.Sprintf("%d", req.NewID))
	form.Set("name", req.Name)
	form.Set("target", req.TargetNode)
	form.Set("pool", req.Pool)
	if req.TargetStorage != "" {
		form.Set("storage", req.TargetStorage)
	}
	if req.Full {
		form.Set("full", "1")
	} else {
		form.Set("full", "0")
	}
	if req.Snapshot != "" {
		form.Set("snapname", req.Snapshot)
	}
	return c.postString(ctx, path.Join("/nodes", sourceNode, "qemu", fmt.Sprintf("%d", templateVMID), "clone"), form)
}

func (c *Client) MigrateVM(ctx context.Context, sourceNode string, vmid int, req MigrateRequest) (string, error) {
	form := url.Values{}
	form.Set("target", req.TargetNode)
	form.Set("online", "0")
	if req.TargetStorage != "" {
		form.Set("targetstorage", req.TargetStorage)
	}
	return c.postString(ctx, path.Join("/nodes", sourceNode, "qemu", fmt.Sprintf("%d", vmid), "migrate"), form)
}

func (c *Client) SetVMConfig(ctx context.Context, node string, vmid int, req SetConfigRequest) (string, error) {
	form := url.Values{}
	form.Set("tags", strings.Join(req.Tags, ";"))
	form.Set("description", req.Description)
	if req.MemoryMB > 0 {
		form.Set("memory", fmt.Sprintf("%d", req.MemoryMB))
	}
	if req.CPUCores > 0 {
		form.Set("cores", fmt.Sprintf("%d", req.CPUCores))
	}
	if req.IPConfig != "" {
		if req.CloudInitInterface == "" {
			req.CloudInitInterface = "ipconfig0"
		}
		form.Set(req.CloudInitInterface, req.IPConfig)
	}
	if req.CIUser != "" {
		form.Set("ciuser", req.CIUser)
	}
	if req.CIPassword != "" {
		form.Set("cipassword", req.CIPassword)
	}
	if len(req.SSHKeys) > 0 {
		keys := make([]string, 0, len(req.SSHKeys))
		for _, key := range req.SSHKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				keys = append(keys, key)
			}
		}
		if len(keys) > 0 {
			encoded := url.QueryEscape(strings.Join(keys, "\n"))
			encoded = strings.ReplaceAll(encoded, "+", "%20")
			form.Set("sshkeys", encoded)
		}
	}
	if req.NameServer != "" {
		form.Set("nameserver", req.NameServer)
	}
	if req.SearchDomain != "" {
		form.Set("searchdomain", req.SearchDomain)
	}
	if req.DisableCIUpgrade {
		form.Set("ciupgrade", "0")
	}
	if req.AgentEnabled {
		form.Set("agent", "1")
	}
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "config"), form)
}

func (c *Client) ResizeVMDisk(ctx context.Context, node string, vmid int, disk string, sizeMB int64) (string, error) {
	form := url.Values{}
	form.Set("disk", disk)
	form.Set("size", fmt.Sprintf("%dM", sizeMB))
	return c.writeString(ctx, http.MethodPut, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "resize"), form)
}

func (c *Client) StartVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "start"), nil)
}

func (c *Client) StopVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "stop"), nil)
}

func (c *Client) SuspendVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "suspend"), nil)
}

func (c *Client) ResumeVM(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "status", "resume"), nil)
}

func (c *Client) ConvertVMToTemplate(ctx context.Context, node string, vmid int) (string, error) {
	return c.postString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "template"), nil)
}

func (c *Client) DeleteVM(ctx context.Context, node string, vmid int) (string, error) {
	form := url.Values{}
	form.Set("purge", "1")
	form.Set("destroy-unreferenced-disks", "1")
	return c.deleteString(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid)), form)
}

func (c *Client) GetTaskStatus(ctx context.Context, node, upid string) (TaskStatus, error) {
	var out TaskStatus
	err := c.get(ctx, path.Join("/nodes", node, "tasks", upid, "status"), &out)
	return out, err
}

func (c *Client) WaitForTask(ctx context.Context, node, upid string, pollInterval time.Duration) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		status, err := c.GetTaskStatus(ctx, node, upid)
		if err != nil {
			// The Proxmox task keeps running server-side, so a transient error while
			// polling must not be mistaken for task failure. Keep polling until the task
			// reports completion or the context (phase timeout) is exhausted; bail only on
			// a terminal error.
			if !IsRetryable(err) {
				return err
			}
		} else if status.Status == "stopped" {
			if status.ExitStatus != "" && status.ExitStatus != "OK" {
				return fmt.Errorf("task %s failed: %s", upid, status.ExitStatus)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) GetAgentInterfaces(ctx context.Context, node string, vmid int) ([]AgentInterface, error) {
	var out struct {
		Result []AgentInterface `json:"result"`
	}
	err := c.get(ctx, path.Join("/nodes", node, "qemu", fmt.Sprintf("%d", vmid), "agent", "network-get-interfaces"), &out)
	return out.Result, err
}

func (c *Client) get(ctx context.Context, p string, out any) error {
	return c.getWithQuery(ctx, p, nil, out)
}

func (c *Client) getWithQuery(ctx context.Context, p string, query url.Values, out any) error {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, "/api2/json", p)
	u.RawQuery = query.Encode()

	// GETs are idempotent, so transient network/DNS/5xx failures are retried within the
	// configured budget. The request is rebuilt each attempt.
	return Retry(ctx, c.retry, func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", c.authHeader)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		return decodeResponse(resp, out)
	})
}

func (c *Client) postString(ctx context.Context, p string, form url.Values) (string, error) {
	return c.writeString(ctx, http.MethodPost, p, form)
}

func (c *Client) deleteString(ctx context.Context, p string, form url.Values) (string, error) {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, "/api2/json", p)
	if form != nil {
		u.RawQuery = form.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", c.authHeader)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out string
	if err := decodeResponse(resp, &out); err != nil {
		return "", err
	}
	return out, nil
}

func (c *Client) writeString(ctx context.Context, method, p string, form url.Values) (string, error) {
	u := *c.baseURL
	u.Path = path.Join(c.baseURL.Path, "/api2/json", p)

	body := bytes.NewBufferString("")
	if form != nil {
		body = bytes.NewBufferString(form.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", c.authHeader)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out string
	if err := decodeResponse(resp, &out); err != nil {
		return "", err
	}
	return out, nil
}

func decodeResponse(resp *http.Response, out any) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %s", ErrNotFound, proxmoxAPIErrorDetails(resp, body))
		}
		return &APIError{StatusCode: resp.StatusCode, Details: proxmoxAPIErrorDetails(resp, body)}
	}

	var raw envelope[json.RawMessage]
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw.Data, out); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}

func proxmoxAPIErrorDetails(resp *http.Response, body []byte) string {
	parts := []string{fmt.Sprintf("status=%d", resp.StatusCode)}
	if message := proxmoxStatusMessage(resp); message != "" {
		parts = append(parts, fmt.Sprintf("status_message=%q", message))
	}
	if bodyText := strings.TrimSpace(string(body)); bodyText != "" {
		parts = append(parts, fmt.Sprintf("body=%s", bodyText))
	}
	return strings.Join(parts, " ")
}

func proxmoxStatusMessage(resp *http.Response) string {
	message := strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)))
	if message == "" || message == http.StatusText(resp.StatusCode) {
		return ""
	}
	return message
}
