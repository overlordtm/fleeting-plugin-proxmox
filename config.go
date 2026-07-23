package proxmox

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
)

const (
	defaultCloneMode          = "auto"
	defaultNetworkMode        = "static"
	defaultTemplateStageMode  = "auto"
	defaultTaskPollInterval   = 2 * time.Second
	defaultCloneTimeout       = 10 * time.Minute
	defaultStartTimeout       = 5 * time.Minute
	defaultShutdownTimeout    = 2 * time.Minute
	defaultAgentTimeout       = 3 * time.Minute
	defaultAPIRetryMaxElapsed = 30 * time.Second
	defaultIPReuseCooldown    = 0 * time.Second
	defaultMetricsInterval    = 15 * time.Second
	defaultStateDir           = "/var/lib/fleeting-plugin-proxmox"
	defaultStateFileBasename  = "state.json"
	managedByTag              = "managed-by-fleeting-plugin-proxmox"
)

type vmidRange struct {
	Min int
	Max int
}

type pluginConfig struct {
	APIURL                string        `json:"api_url"`
	TokenID               string        `json:"token_id"`
	TokenSecret           string        `json:"token_secret"`
	TLSCAFile             string        `json:"tls_ca_file"`
	TLSInsecureSkipVerify bool          `json:"tls_insecure_skip_verify"`
	ClusterName           string        `json:"cluster_name"`
	Pool                  string        `json:"pool"`
	TemplateVMIDs         []int         `json:"template_vmids"`
	TemplateStageMode     string        `json:"template_stage_mode"`
	TemplateVMIDRange     string        `json:"template_vmid_range"`
	TemplateNamePrefix    string        `json:"template_name_prefix"`
	NamePrefix            string        `json:"name_prefix"`
	VMIDRange             string        `json:"vmid_range"`
	Nodes                 LaxStringList `json:"nodes"`

	CloneMode      string        `json:"clone_mode"`
	TargetStorages LaxStringList `json:"target_storages"`
	CloneSnapshot  string        `json:"clone_snapshot"`
	VMMemoryMB     int64         `json:"vm_memory_mb"`
	VMCPUCores     int           `json:"vm_cpu_cores"`
	VMDiskMB       int64         `json:"vm_disk_mb"`
	VMDiskDevice   string        `json:"vm_disk_device"`

	NodeReserveMemoryMB              int64              `json:"node_reserve_memory_mb"`
	NodeReserveMemoryPercent         int                `json:"node_reserve_memory_percent"`
	NodeReserveCPUCores              int                `json:"node_reserve_cpu_cores"`
	NodeReserveCPUPercent            int                `json:"node_reserve_cpu_percent"`
	NodeReserveDiskGB                int64              `json:"node_reserve_disk_gb"`
	NodeReserveDiskPercent           int                `json:"node_reserve_disk_percent"`
	NodeMemoryAllocationLimitPercent int                `json:"node_memory_allocation_limit_percent"`
	NodeCPUAllocationLimitPercent    int                `json:"node_cpu_allocation_limit_percent"`
	NodePolicies                     []nodePolicyConfig `json:"node_policies"`
	Scheduler                        string             `json:"scheduler"`

	MaxParallelClones  int `json:"max_parallel_clones"`
	MaxParallelStarts  int `json:"max_parallel_starts"`
	MaxParallelDeletes int `json:"max_parallel_deletes"`

	TaskPollInterval   string `json:"task_poll_interval"`
	CloneTimeout       string `json:"clone_timeout"`
	StartTimeout       string `json:"start_timeout"`
	ShutdownTimeout    string `json:"shutdown_timeout"`
	APIRetryMaxElapsed string `json:"api_retry_max_elapsed"`
	MetricsSocket      string `json:"metrics_socket"`
	MetricsInterval    string `json:"metrics_interval"`

	NetworkMode  string        `json:"network_mode"`
	CIUser       string        `json:"ci_user"`
	CISSHKeys    LaxStringList `json:"ci_ssh_keys"`
	NameServers  LaxStringList `json:"nameserver"`
	SearchDomain string        `json:"searchdomain"`

	IPPoolNetwork       string        `json:"ip_pool_network"`
	IPPoolGateway       string        `json:"ip_pool_gateway"`
	IPPoolRanges        LaxStringList `json:"ip_pool_ranges"`
	IPPoolExclude       LaxStringList `json:"ip_pool_exclude"`
	IPPoolReuseCooldown string        `json:"ip_pool_reuse_cooldown"`
	StateFile           string        `json:"state_file"`

	AgentRequired bool   `json:"agent_required"`
	AgentTimeout  string `json:"agent_timeout"`
	PreferIPv6    bool   `json:"prefer_ipv6"`

	Tags                LaxStringList `json:"tags"`
	DescriptionTemplate string        `json:"description_template"`

	parsedVMIDRange          vmidRange
	parsedTemplateVMIDRange  vmidRange
	parsedTaskPoll           time.Duration
	parsedCloneTimeout       time.Duration
	parsedStartTimeout       time.Duration
	parsedShutdownTimeout    time.Duration
	parsedAgentTimeout       time.Duration
	parsedAPIRetryMaxElapsed time.Duration
	parsedIPReuseCooldown    time.Duration
	parsedMetricsInterval    time.Duration
	parsedPoolPrefix         netip.Prefix
	parsedGateway            netip.Addr
	agentRequiredSet         bool
}

type nodePolicyConfig struct {
	Nodes LaxStringList `json:"nodes"`

	ReserveMemoryMB      *int64 `json:"reserve_memory_mb"`
	ReserveMemoryPercent *int   `json:"reserve_memory_percent"`
	ReserveCPUCores      *int   `json:"reserve_cpu_cores"`
	ReserveCPUPercent    *int   `json:"reserve_cpu_percent"`
	ReserveDiskGB        *int64 `json:"reserve_disk_gb"`
	ReserveDiskPercent   *int   `json:"reserve_disk_percent"`

	MemoryAllocationLimitPercent *int `json:"memory_allocation_limit_percent"`
	CPUAllocationLimitPercent    *int `json:"cpu_allocation_limit_percent"`
}

func (g *InstanceGroup) config() *pluginConfig {
	return &g.pluginConfig
}

func (c *pluginConfig) UnmarshalJSON(data []byte) error {
	type rawPluginConfig pluginConfig

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	decoded := rawPluginConfig(*c)
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	*c = pluginConfig(decoded)
	if value, ok := raw["agent_required"]; ok {
		c.agentRequiredSet = true
		if err := json.Unmarshal(value, &c.AgentRequired); err != nil {
			return err
		}
	}

	return nil
}

func (c *pluginConfig) applyDefaults(settings provider.Settings) {
	if c.ClusterName == "" {
		c.ClusterName = "default"
	}
	if c.CloneMode == "" {
		c.CloneMode = defaultCloneMode
	}
	if c.TemplateStageMode == "" {
		c.TemplateStageMode = defaultTemplateStageMode
	}
	if c.NetworkMode == "" {
		c.NetworkMode = defaultNetworkMode
	}
	if c.TemplateNamePrefix == "" && c.NamePrefix != "" {
		c.TemplateNamePrefix = c.NamePrefix + "-template"
	}
	if c.MaxParallelClones <= 0 {
		c.MaxParallelClones = 2
	}
	if c.MaxParallelStarts <= 0 {
		c.MaxParallelStarts = 4
	}
	if c.MaxParallelDeletes <= 0 {
		c.MaxParallelDeletes = 2
	}
	if c.TaskPollInterval == "" {
		c.TaskPollInterval = defaultTaskPollInterval.String()
	}
	if c.Scheduler == "" {
		c.Scheduler = "balanced"
	}
	if c.CloneTimeout == "" {
		c.CloneTimeout = defaultCloneTimeout.String()
	}
	if c.StartTimeout == "" {
		c.StartTimeout = defaultStartTimeout.String()
	}
	if c.ShutdownTimeout == "" {
		c.ShutdownTimeout = defaultShutdownTimeout.String()
	}
	if c.APIRetryMaxElapsed == "" {
		c.APIRetryMaxElapsed = defaultAPIRetryMaxElapsed.String()
	}
	if c.MetricsSocket != "" && c.MetricsInterval == "" {
		c.MetricsInterval = defaultMetricsInterval.String()
	}
	if c.IPPoolReuseCooldown == "" {
		c.IPPoolReuseCooldown = defaultIPReuseCooldown.String()
	}
	if c.NetworkMode == "static" && c.StateFile == "" {
		c.StateFile = filepath.Join(defaultStateDir, defaultStateFileName(c.ClusterName, c.Pool, c.NamePrefix))
	}
	if !c.agentRequiredSet {
		c.AgentRequired = true
	}
	if c.AgentTimeout == "" {
		c.AgentTimeout = defaultAgentTimeout.String()
	}
	if settings.Protocol == "" {
		settings.Protocol = provider.ProtocolSSH
	}
}

func (c *pluginConfig) validate(settings provider.Settings) error {
	c.applyEnv()
	c.applyDefaults(settings)

	var errs []error

	if c.APIURL == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: api_url"))
	}
	if c.TokenID == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: token_id"))
	}
	if c.TokenSecret == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: token_secret"))
	}
	if c.Pool == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: pool"))
	}
	if len(c.TemplateVMIDs) == 0 {
		errs = append(errs, fmt.Errorf("missing required plugin config: template_vmids"))
	}
	if c.NamePrefix == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: name_prefix"))
	}
	if len(c.Nodes) == 0 {
		errs = append(errs, fmt.Errorf("missing required plugin config: nodes"))
	}
	if c.VMIDRange == "" {
		errs = append(errs, fmt.Errorf("missing required plugin config: vmid_range"))
	}
	if settings.Protocol != "" {
		if err := settings.Protocol.Valid(); err != nil {
			errs = append(errs, fmt.Errorf("unsupported connector protocol: %s", settings.Protocol))
		}
	}
	validOS := map[string]bool{"linux": true, "windows": true, "darwin": true}
	if settings.OS != "" && !validOS[settings.OS] {
		errs = append(errs, fmt.Errorf("unsupported connector OS: %s (supported: linux, windows, darwin)", settings.OS))
	}
	if c.CloneMode != "auto" && c.CloneMode != "linked" && c.CloneMode != "full" {
		errs = append(errs, fmt.Errorf("invalid clone_mode: %s", c.CloneMode))
	}
	if c.TemplateStageMode != "off" && c.TemplateStageMode != "auto" && c.TemplateStageMode != "required" {
		errs = append(errs, fmt.Errorf("invalid template_stage_mode: %s", c.TemplateStageMode))
	}
	if c.VMMemoryMB < 0 {
		errs = append(errs, fmt.Errorf("vm_memory_mb must be >= 0"))
	}
	if c.VMCPUCores < 0 {
		errs = append(errs, fmt.Errorf("vm_cpu_cores must be >= 0"))
	}
	if c.VMDiskMB < 0 {
		errs = append(errs, fmt.Errorf("vm_disk_mb must be >= 0"))
	}
	if c.NodeReserveMemoryMB < 0 {
		errs = append(errs, fmt.Errorf("node_reserve_memory_mb must be >= 0"))
	}
	if c.NodeReserveCPUCores < 0 {
		errs = append(errs, fmt.Errorf("node_reserve_cpu_cores must be >= 0"))
	}
	if c.NodeReserveDiskGB < 0 {
		errs = append(errs, fmt.Errorf("node_reserve_disk_gb must be >= 0"))
	}
	if c.NodeMemoryAllocationLimitPercent < 0 {
		errs = append(errs, fmt.Errorf("node_memory_allocation_limit_percent must be >= 0"))
	}
	if c.NodeCPUAllocationLimitPercent < 0 {
		errs = append(errs, fmt.Errorf("node_cpu_allocation_limit_percent must be >= 0"))
	}
	if c.NodeReserveMemoryPercent < 0 || c.NodeReserveMemoryPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_memory_percent must be between 0 and 100"))
	}
	if c.NodeReserveCPUPercent < 0 || c.NodeReserveCPUPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_cpu_percent must be between 0 and 100"))
	}
	if c.NodeReserveDiskPercent < 0 || c.NodeReserveDiskPercent > 100 {
		errs = append(errs, fmt.Errorf("node_reserve_disk_percent must be between 0 and 100"))
	}
	if c.NetworkMode != "static" && c.NetworkMode != "dhcp" {
		errs = append(errs, fmt.Errorf("invalid network_mode: %s", c.NetworkMode))
	}
	if c.Scheduler != "balanced" && c.Scheduler != "most_free_ram" && c.Scheduler != "most_free_cpu" && c.Scheduler != "round_robin" {
		errs = append(errs, fmt.Errorf("invalid scheduler: %s", c.Scheduler))
	}
	if c.NetworkMode == "dhcp" && !c.AgentRequired {
		errs = append(errs, fmt.Errorf("network_mode=dhcp requires agent_required=true"))
	}
	if len(c.TargetStorages) == 0 && (c.NodeReserveDiskGB > 0 || c.NodeReserveDiskPercent > 0) {
		errs = append(errs, fmt.Errorf("node_reserve_disk requires target_storages"))
	}
	if len(c.TargetStorages) == 0 && c.hasPerNodeDiskReserve() {
		errs = append(errs, fmt.Errorf("node_policies reserve_disk requires target_storages"))
	}
	if c.PreferIPv6 {
		errs = append(errs, fmt.Errorf("prefer_ipv6 is unsupported in v1"))
	}

	c.validateNodePolicies(&errs)

	c.parsedVMIDRange = parseVMIDRange(c.VMIDRange, &errs)
	if c.TemplateVMIDRange != "" {
		c.parsedTemplateVMIDRange = parseVMIDRange(c.TemplateVMIDRange, &errs)
		if c.parsedTemplateVMIDRange.Min <= c.parsedVMIDRange.Max && c.parsedTemplateVMIDRange.Max >= c.parsedVMIDRange.Min {
			errs = append(errs, fmt.Errorf("template_vmid_range must not overlap vmid_range"))
		}
	} else if c.TemplateStageMode == "required" {
		errs = append(errs, fmt.Errorf("missing required plugin config when template_stage_mode=required: template_vmid_range"))
	}

	seenTemplateVMIDs := map[int]struct{}{}
	for _, vmid := range c.TemplateVMIDs {
		if vmid <= 0 {
			errs = append(errs, fmt.Errorf("template_vmids contains invalid VMID %d", vmid))
			continue
		}
		if _, exists := seenTemplateVMIDs[vmid]; exists {
			errs = append(errs, fmt.Errorf("template_vmids contains duplicate VMID %d", vmid))
			continue
		}
		seenTemplateVMIDs[vmid] = struct{}{}
	}

	c.parsedTaskPoll = parsePositiveDurationField("task_poll_interval", c.TaskPollInterval, &errs)
	c.parsedCloneTimeout = parsePositiveDurationField("clone_timeout", c.CloneTimeout, &errs)
	c.parsedStartTimeout = parsePositiveDurationField("start_timeout", c.StartTimeout, &errs)
	c.parsedShutdownTimeout = parsePositiveDurationField("shutdown_timeout", c.ShutdownTimeout, &errs)
	c.parsedAgentTimeout = parsePositiveDurationField("agent_timeout", c.AgentTimeout, &errs)
	c.parsedAPIRetryMaxElapsed = parseNonNegativeDurationField("api_retry_max_elapsed", c.APIRetryMaxElapsed, &errs)
	c.parsedIPReuseCooldown = parseNonNegativeDurationField("ip_pool_reuse_cooldown", c.IPPoolReuseCooldown, &errs)
	if c.MetricsSocket != "" {
		c.parsedMetricsInterval = parsePositiveDurationField("metrics_interval", c.MetricsInterval, &errs)
	}

	if c.NetworkMode == "static" {
		if c.IPPoolNetwork == "" {
			errs = append(errs, fmt.Errorf("missing required plugin config for static network_mode: ip_pool_network"))
		}
		if c.IPPoolGateway == "" {
			errs = append(errs, fmt.Errorf("missing required plugin config for static network_mode: ip_pool_gateway"))
		}

		prefix, err := netip.ParsePrefix(c.IPPoolNetwork)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid ip_pool_network: %w", err))
		} else if !prefix.Addr().Is4() {
			errs = append(errs, fmt.Errorf("ip_pool_network must be IPv4"))
		} else {
			c.parsedPoolPrefix = prefix.Masked()
		}

		gateway, err := netip.ParseAddr(c.IPPoolGateway)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid ip_pool_gateway: %w", err))
		} else if !gateway.Is4() {
			errs = append(errs, fmt.Errorf("ip_pool_gateway must be IPv4"))
		} else {
			c.parsedGateway = gateway
		}

		for _, value := range append(append(LaxStringList{}, c.IPPoolExclude...), c.flattenRanges()...) {
			if strings.Contains(value, "-") {
				if _, _, err := parseAddrRange(value); err != nil {
					errs = append(errs, fmt.Errorf("invalid range %q: %w", value, err))
				}
				continue
			}

			addr, err := netip.ParseAddr(value)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid IPv4 address %q: %w", value, err))
				continue
			}
			if c.parsedPoolPrefix.IsValid() && !c.parsedPoolPrefix.Contains(addr) {
				errs = append(errs, fmt.Errorf("address %q is outside ip_pool_network", value))
			}
		}
	}

	if c.NetworkMode == "static" {
		if err := os.MkdirAll(filepath.Dir(c.StateFile), 0o755); err != nil {
			errs = append(errs, fmt.Errorf("prepare state_file directory: %w", err))
		}
	}

	return errors.Join(errs...)
}

func (c *pluginConfig) defaultNodePolicy() scheduler.NodePolicy {
	return scheduler.NodePolicy{
		Reserve: scheduler.Reserve{
			MemoryMB:      c.NodeReserveMemoryMB,
			MemoryPercent: c.NodeReserveMemoryPercent,
			DiskGB:        c.NodeReserveDiskGB,
			DiskPercent:   c.NodeReserveDiskPercent,
			CPUCores:      c.NodeReserveCPUCores,
			CPUPercent:    c.NodeReserveCPUPercent,
		},
		MemoryAllocationLimitPercent: c.NodeMemoryAllocationLimitPercent,
		CPUAllocationLimitPercent:    c.NodeCPUAllocationLimitPercent,
	}
}

func (c *pluginConfig) resolveNodePolicies() map[string]scheduler.NodePolicy {
	policies := make(map[string]scheduler.NodePolicy, len(c.Nodes))
	defaultPolicy := c.defaultNodePolicy()
	for _, node := range c.Nodes {
		policies[node] = defaultPolicy
	}
	for _, override := range c.NodePolicies {
		for _, node := range override.Nodes {
			policy, ok := policies[node]
			if !ok {
				continue
			}
			applyNodePolicyOverride(&policy, override)
			policies[node] = policy
		}
	}
	return policies
}

func applyNodePolicyOverride(policy *scheduler.NodePolicy, override nodePolicyConfig) {
	if override.ReserveMemoryMB != nil {
		policy.Reserve.MemoryMB = *override.ReserveMemoryMB
	}
	if override.ReserveMemoryPercent != nil {
		policy.Reserve.MemoryPercent = *override.ReserveMemoryPercent
	}
	if override.ReserveCPUCores != nil {
		policy.Reserve.CPUCores = *override.ReserveCPUCores
	}
	if override.ReserveCPUPercent != nil {
		policy.Reserve.CPUPercent = *override.ReserveCPUPercent
	}
	if override.ReserveDiskGB != nil {
		policy.Reserve.DiskGB = *override.ReserveDiskGB
	}
	if override.ReserveDiskPercent != nil {
		policy.Reserve.DiskPercent = *override.ReserveDiskPercent
	}
	if override.MemoryAllocationLimitPercent != nil {
		policy.MemoryAllocationLimitPercent = *override.MemoryAllocationLimitPercent
	}
	if override.CPUAllocationLimitPercent != nil {
		policy.CPUAllocationLimitPercent = *override.CPUAllocationLimitPercent
	}
}

func (c *pluginConfig) validateNodePolicies(errs *[]error) {
	knownNodes := make(map[string]struct{}, len(c.Nodes))
	for _, node := range c.Nodes {
		knownNodes[node] = struct{}{}
	}

	assigned := map[string]struct{}{}
	for i, policy := range c.NodePolicies {
		prefix := fmt.Sprintf("node_policies[%d]", i)
		if len(policy.Nodes) == 0 {
			*errs = append(*errs, fmt.Errorf("%s.nodes must not be empty", prefix))
		}
		for _, node := range policy.Nodes {
			if _, ok := knownNodes[node]; !ok {
				*errs = append(*errs, fmt.Errorf("%s.nodes contains unknown node %q", prefix, node))
				continue
			}
			if _, exists := assigned[node]; exists {
				*errs = append(*errs, fmt.Errorf("node %q is listed by more than one node_policies entry", node))
				continue
			}
			assigned[node] = struct{}{}
		}

		validateOptionalInt64(prefix+".reserve_memory_mb", policy.ReserveMemoryMB, 0, errs)
		validateOptionalInt64(prefix+".reserve_disk_gb", policy.ReserveDiskGB, 0, errs)
		validateOptionalInt(prefix+".reserve_cpu_cores", policy.ReserveCPUCores, 0, errs)
		validateOptionalPercent(prefix+".reserve_memory_percent", policy.ReserveMemoryPercent, errs)
		validateOptionalPercent(prefix+".reserve_cpu_percent", policy.ReserveCPUPercent, errs)
		validateOptionalPercent(prefix+".reserve_disk_percent", policy.ReserveDiskPercent, errs)
		validateOptionalInt(prefix+".memory_allocation_limit_percent", policy.MemoryAllocationLimitPercent, 0, errs)
		validateOptionalInt(prefix+".cpu_allocation_limit_percent", policy.CPUAllocationLimitPercent, 0, errs)
	}
}

func validateOptionalInt(name string, value *int, min int, errs *[]error) {
	if value == nil {
		return
	}
	if *value < min {
		*errs = append(*errs, fmt.Errorf("%s must be >= %d", name, min))
	}
}

func validateOptionalInt64(name string, value *int64, min int64, errs *[]error) {
	if value == nil {
		return
	}
	if *value < min {
		*errs = append(*errs, fmt.Errorf("%s must be >= %d", name, min))
	}
}

func validateOptionalPercent(name string, value *int, errs *[]error) {
	if value == nil {
		return
	}
	if *value < 0 || *value > 100 {
		*errs = append(*errs, fmt.Errorf("%s must be between 0 and 100", name))
	}
}

func (c *pluginConfig) hasPerNodeDiskReserve() bool {
	for _, policy := range c.NodePolicies {
		if policy.ReserveDiskGB != nil && *policy.ReserveDiskGB > 0 {
			return true
		}
		if policy.ReserveDiskPercent != nil && *policy.ReserveDiskPercent > 0 {
			return true
		}
	}
	return false
}

func (c *pluginConfig) applyEnv() {
	overrideEnv(&c.APIURL, "PROXMOX_API_URL")
	overrideEnv(&c.TokenID, "PROXMOX_TOKEN_ID")
	overrideEnv(&c.TokenSecret, "PROXMOX_TOKEN_SECRET")
	overrideEnv(&c.TLSCAFile, "PROXMOX_TLS_CA_FILE")
	overrideEnv(&c.StateFile, "PROXMOX_STATE_FILE")
	overrideEnv(&c.MetricsSocket, "PROXMOX_METRICS_SOCKET")
	overrideEnv(&c.MetricsInterval, "PROXMOX_METRICS_INTERVAL")
}

func (c *pluginConfig) mandatoryTags() []string {
	tagGroup := sanitizeTag("fleeting-group-" + c.NamePrefix)
	return append([]string{managedByTag, tagGroup}, c.Tags...)
}

func (c *pluginConfig) managedTemplateTags() []string {
	return []string{managedByTag, "managed-role-template-stage"}
}

func (c *pluginConfig) flattenRanges() []string {
	return slices.Clone(c.IPPoolRanges)
}

func parseVMIDRange(value string, errs *[]error) vmidRange {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		*errs = append(*errs, fmt.Errorf("invalid vmid_range: %s", value))
		return vmidRange{}
	}

	var r vmidRange
	if _, err := fmt.Sscanf(value, "%d-%d", &r.Min, &r.Max); err != nil || r.Min <= 0 || r.Max < r.Min {
		*errs = append(*errs, fmt.Errorf("invalid vmid_range: %s", value))
	}

	return r
}

func parseDurationField(name, value string, errs *[]error) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("invalid %s: %w", name, err))
	}
	return d
}

func parsePositiveDurationField(name, value string, errs *[]error) time.Duration {
	d := parseDurationField(name, value, errs)
	if d <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be > 0", name))
	}
	return d
}

func parseNonNegativeDurationField(name, value string, errs *[]error) time.Duration {
	d := parseDurationField(name, value, errs)
	if d < 0 {
		*errs = append(*errs, fmt.Errorf("%s must be >= 0", name))
	}
	return d
}

func defaultStateFileName(clusterName, pool, namePrefix string) string {
	parts := []string{
		sanitizeTag(clusterName),
		sanitizeTag(pool),
		sanitizeTag(namePrefix),
	}
	filtered := parts[:0]
	for _, part := range parts {
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return defaultStateFileBasename
	}
	return strings.Join(append(filtered, defaultStateFileBasename), "-")
}

func overrideEnv(dst *string, key string) {
	if value := os.Getenv(key); value != "" {
		*dst = value
	}
}

func sanitizeTag(v string) string {
	v = strings.ToLower(v)
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	return strings.Trim(b.String(), "-")
}

func parseAddrRange(value string) (netip.Addr, netip.Addr, error) {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("range must be start-end")
	}

	start, err := netip.ParseAddr(parts[0])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	end, err := netip.ParseAddr(parts[1])
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	if !start.Is4() || !end.Is4() {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("only IPv4 ranges are supported")
	}
	if start.Compare(end) > 0 {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("range start must be <= end")
	}
	return start, end, nil
}
