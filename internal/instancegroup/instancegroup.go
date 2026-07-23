package instancegroup

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/ippool"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/limiter"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
)

type Config struct {
	ClusterName                  string
	Pool                         string
	TemplateVMIDs                []int
	TemplateStageMode            string
	TemplateVMIDMin              int
	TemplateVMIDMax              int
	TemplateNamePrefix           string
	VMIDMin                      int
	VMIDMax                      int
	NamePrefix                   string
	Nodes                        []string
	CloneMode                    string
	TargetStorages               []string
	CloneSnapshot                string
	VMMemoryMB                   int64
	VMCPUCores                   int
	VMDiskMB                     int64
	VMDiskDevice                 string
	MandatoryTags                []string
	ManagedTemplateTags          []string
	DescriptionTemplate          string
	CloudInitInterface           string
	NetworkMode                  string
	CIUser                       string
	NameServers                  []string
	SearchDomain                 string
	TaskPollInterval             time.Duration
	CloneTimeout                 time.Duration
	StartTimeout                 time.Duration
	ShutdownTimeout              time.Duration
	AgentTimeout                 time.Duration
	AgentRequired                bool
	GeneratedSSHPublicKey        string
	GeneratedPassword            string
	StaticSSHPublicKeys          []string
	OS                           string
	Scheduler                    *scheduler.Scheduler
	MemoryAllocationLimitPercent int
	CPUAllocationLimitPercent    int
	Reserve                      scheduler.Reserve
	NodePolicies                 map[string]scheduler.NodePolicy
}

type ManagedInstance struct {
	ID    string
	Node  string
	VMID  int
	Name  string
	State provider.State
	IP    netip.Addr
}

type Group struct {
	client        *proxmoxclient.Client
	log           hclog.Logger
	cfg           Config
	pool          *ippool.Pool
	cloneLimiter  *limiter.Limiter
	startLimiter  *limiter.Limiter
	deleteLimiter *limiter.Limiter
	problems      metrics.ProblemReporter
	runCtx        context.Context
	cancelRun     context.CancelFunc
	provisionWg   sync.WaitGroup
	operationSeq  atomic.Uint64

	mu sync.Mutex
	// transient overrides smooth out state transitions while long-running Proxmox tasks complete.
	transient        map[string]provider.State
	accepted         map[string]acceptedProvision
	pendingByNode    map[string]pendingReservation
	pendingVMIDs     map[int]struct{}
	cloneQuarantine  map[clonePlacementKey]time.Time
	unavailableNodes map[string]bool
	templatesByNode  map[string]templateChoice
	storageNodes     map[string]map[string]struct{}
	storageShared    map[string]bool
	storagePlugins   map[string]string
	templateSizing   proxmoxclient.ClusterResource
	templateDisk     string
}

var diskSizePattern = regexp.MustCompile(`(?:^|,)size=([0-9]+(?:\.[0-9]+)?)([KMGT])(?:,|$)`)
var linkedClonePluginTypes = map[string]struct{}{
	"dir":      {},
	"nfs":      {},
	"lvmthin":  {},
	"zfspool":  {},
	"rbd":      {},
	"sheepdog": {},
	"nexenta":  {},
}

const (
	managedByTag                = "managed-by-fleeting-plugin-proxmox"
	cloneCollisionQuarantineTTL = 30 * time.Minute
)

type clonePlacementKey struct {
	Node    string
	Storage string
	VMID    int
}

type provisionPlan struct {
	OperationID     string
	TemplateNode    string
	TemplateVMID    int
	TemplateStorage string
	Node            string
	TargetStorage   string
	VMID            int
	Requirement     scheduler.Requirement
}

type pendingReservation struct {
	MemoryMB  float64
	CPUCores  float64
	StorageGB map[string]float64
}

type acceptedProvision struct {
	Node             string
	VMID             int
	State            provider.State
	ReportedCreating bool
}

type nodePlanState struct {
	Name                    string
	TemplateNode            string
	TemplateVMID            int
	TemplateStorage         string
	TotalMemoryMB           float64
	FreeMemoryMB            float64
	AllocatedMemoryMB       float64
	MemoryAllocationLimitMB float64
	TotalCPUCores           float64
	FreeCPUCores            float64
	AllocatedCPUCores       float64
	CPUAllocationLimitCores float64
	Reserve                 scheduler.Reserve
	StorageTotalGB          map[string]float64
	StorageFreeGB           map[string]float64
}

type templateChoice struct {
	Resource      proxmoxclient.ClusterResource
	Storage       string
	StorageNodes  map[string]struct{}
	StorageShared bool
}

type ManagedTemplate struct {
	ID         string
	Node       string
	VMID       int
	Name       string
	SourceVMID int
	Storage    string
}

type managedTemplateStageMethod string

const (
	managedTemplateStageDirectClone  managedTemplateStageMethod = "direct_clone"
	managedTemplateStageCloneMigrate managedTemplateStageMethod = "clone_migrate"
)

const (
	sourceTemplateVersionKey = "template-version"
	stagedTemplateVersionKey = "source-template-version"
)

func New(client *proxmoxclient.Client, log hclog.Logger, cfg Config, pool *ippool.Pool, cloneLimiter, startLimiter, deleteLimiter *limiter.Limiter, problems metrics.ProblemReporter) *Group {
	runCtx, cancelRun := context.WithCancel(context.Background())
	return &Group{
		client:           client,
		log:              log,
		cfg:              cfg,
		pool:             pool,
		cloneLimiter:     cloneLimiter,
		startLimiter:     startLimiter,
		deleteLimiter:    deleteLimiter,
		problems:         problems,
		runCtx:           runCtx,
		cancelRun:        cancelRun,
		transient:        map[string]provider.State{},
		accepted:         map[string]acceptedProvision{},
		pendingByNode:    map[string]pendingReservation{},
		pendingVMIDs:     map[int]struct{}{},
		cloneQuarantine:  map[clonePlacementKey]time.Time{},
		unavailableNodes: map[string]bool{},
	}
}

func (g *Group) Init(ctx context.Context) error {
	if _, err := g.client.GetPool(ctx, g.cfg.Pool); err != nil {
		return fmt.Errorf("verify pool: %w", err)
	}
	if _, err := g.client.GetVersion(ctx); err != nil {
		return fmt.Errorf("verify api connectivity: %w", err)
	}

	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return err
	}
	templateResources, err := g.findTemplates(resources)
	if err != nil {
		return err
	}

	storageResources, err := g.client.ListClusterResources(ctx, "storage")
	if err != nil {
		return err
	}
	storageNodes := indexStorageNodes(storageResources)
	storageShared := indexSharedStorages(storageResources)

	for _, node := range g.cfg.Nodes {
		if _, err := g.client.GetNodeStatus(ctx, node); err != nil {
			return fmt.Errorf("verify node %s: %w", node, err)
		}
	}

	templateStorageNodes, err := g.loadTemplateStorageNodes(ctx, templateResources, storageResources)
	if err != nil {
		return err
	}

	if g.cfg.TemplateStageMode != "off" {
		templateResources, err = g.stageTemplates(ctx, templateResources, templateStorageNodes, storageShared)
		if err != nil {
			return err
		}
		templateStorageNodes, err = g.loadTemplateStorageNodes(ctx, templateResources, storageResources)
		if err != nil {
			return err
		}
	}

	templatesByNode, templateSizing, templateDisk, err := g.resolveNodeTemplates(ctx, templateResources, templateStorageNodes, storageShared)
	if err != nil {
		return err
	}
	g.storageNodes = storageNodes
	g.storageShared = storageShared
	g.storagePlugins = indexStoragePlugins(storageResources)
	g.templatesByNode = templatesByNode
	g.templateSizing = templateSizing
	g.templateDisk = templateDisk
	g.logEffectiveTopology()

	if err := g.cleanupPreexistingStopped(ctx, resources); err != nil {
		return err
	}

	managed, err := g.List(ctx)
	if err != nil {
		return err
	}

	managed, err = g.cleanupManagedOutsideIPPool(ctx, managed)
	if err != nil {
		return err
	}

	active := make(map[string]netip.Addr, len(managed))
	for _, instance := range managed {
		if instance.IP.IsValid() {
			active[instance.ID] = instance.IP
		}
	}

	if g.pool == nil {
		return nil
	}

	return g.pool.Reconcile(ctx, active)
}

func (g *Group) cleanupPreexistingStopped(ctx context.Context, resources []proxmoxclient.ClusterResource) error {
	for _, resource := range resources {
		if !g.isManaged(resource) {
			continue
		}
		if resource.Status != "stopped" && resource.Status != "paused" {
			continue
		}

		instance := ManagedInstance{
			ID:    instanceID(resource.Node, resource.VMID),
			Node:  resource.Node,
			VMID:  resource.VMID,
			Name:  resource.Name,
			State: provider.StateDeleting,
		}

		g.log.Warn("deleting preexisting stopped instance", "phase", "cleanup", "instance", instance.ID, "status", resource.Status)
		if err := g.safeDestroyManagedVM(ctx, instance); err != nil {
			return fmt.Errorf("delete preexisting stopped instance %s: %w", instance.ID, err)
		}
		if g.pool != nil {
			if err := g.pool.Forget(ctx, instance.ID); err != nil {
				return fmt.Errorf("forget lease for %s: %w", instance.ID, err)
			}
		}
	}

	return nil
}

func (g *Group) cleanupManagedOutsideIPPool(ctx context.Context, instances []ManagedInstance) ([]ManagedInstance, error) {
	if g.pool == nil || g.cfg.NetworkMode != "static" {
		return instances, nil
	}

	remaining := instances[:0]
	for _, instance := range instances {
		if !instance.IP.IsValid() || g.pool.Allows(instance.IP) {
			remaining = append(remaining, instance)
			continue
		}

		g.log.Warn("deleting managed instance outside static IP pool", "phase", "cleanup", "instance", instance.ID, "ip", instance.IP.String())
		if err := g.ensureVMStopped(ctx, instance.Node, instance.VMID); err != nil {
			return nil, fmt.Errorf("stop managed instance %s outside static IP pool: %w", instance.ID, err)
		}
		if err := g.safeDestroyManagedVM(ctx, instance); err != nil {
			return nil, fmt.Errorf("delete managed instance %s outside static IP pool: %w", instance.ID, err)
		}
		if err := g.pool.Forget(ctx, instance.ID); err != nil {
			return nil, fmt.Errorf("forget lease for %s outside static IP pool: %w", instance.ID, err)
		}
	}

	return remaining, nil
}

type managedTemplateArtifact struct {
	ManagedTemplate
	Resource proxmoxclient.ClusterResource
	Complete bool
}

func (g *Group) findManagedTemplateArtifacts(resources []proxmoxclient.ClusterResource) []managedTemplateArtifact {
	out := make([]managedTemplateArtifact, 0)
	for _, resource := range resources {
		if !g.isPotentialManagedTemplateArtifact(resource) {
			continue
		}
		sourceVMID, ok := parseManagedTemplateSourceVMID(resource.Name)
		if !ok {
			continue
		}
		out = append(out, managedTemplateArtifact{
			ManagedTemplate: ManagedTemplate{
				ID:         instanceID(resource.Node, resource.VMID),
				Node:       resource.Node,
				VMID:       resource.VMID,
				Name:       resource.Name,
				SourceVMID: sourceVMID,
				Storage:    resource.Storage,
			},
			Resource: resource,
			Complete: g.isManagedTemplate(resource),
		})
	}
	return out
}

func (g *Group) stageTemplates(ctx context.Context, templates []proxmoxclient.ClusterResource, storageNodes map[int]map[string]struct{}, storageShared map[string]bool) ([]proxmoxclient.ClusterResource, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return nil, err
	}
	localTemplateByNode := map[string]templateChoice{}
	byVMID := make(map[int]proxmoxclient.ClusterResource, len(templates))
	sourceVersions := map[int]string{}
	for _, template := range templates {
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, fmt.Errorf("read template config %d: %w", template.VMID, err)
		}
		diskDevice, err := g.resolveDiskDevice(config)
		if err != nil {
			return nil, fmt.Errorf("resolve template disk device for %d: %w", template.VMID, err)
		}
		storageName, err := extractStorageName(config.DiskValue(diskDevice))
		if err != nil {
			return nil, fmt.Errorf("resolve template storage for %d: %w", template.VMID, err)
		}
		localTemplateByNode[template.Node] = templateChoice{
			Resource:      template,
			Storage:       storageName,
			StorageNodes:  storageNodes[template.VMID],
			StorageShared: storageShared[storageName],
		}
		byVMID[template.VMID] = template
		sourceVersions[template.VMID] = descriptionValue(config.Description, sourceTemplateVersionKey)
	}

	existingTemplates := make(map[string]existingManagedTemplate)
	existingOrder := make([]existingManagedTemplate, 0)
	existingArtifacts := make(map[string]existingManagedTemplate)
	for _, artifact := range g.findManagedTemplateArtifacts(resources) {
		template := artifact.ManagedTemplate
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, fmt.Errorf("read managed template config %s: %w", template.ID, err)
		}
		state := existingManagedTemplate{
			ManagedTemplate:    template,
			Resource:           artifact.Resource,
			Version:            descriptionValue(config.Description, stagedTemplateVersionKey),
			AllowPartialDelete: !artifact.Complete,
		}
		existingArtifacts[template.ID] = state
		if artifact.Complete {
			existingTemplates[managedTemplateKey(template.Node, template.SourceVMID)] = state
		}
		existingOrder = append(existingOrder, state)
	}
	keepTemplates := map[string]ManagedTemplate{}
	type stageRequest struct {
		node   string
		source templateChoice
		method managedTemplateStageMethod
	}
	type stagePlan struct {
		ManagedTemplate
		method managedTemplateStageMethod
	}
	stageRequests := make([]stageRequest, 0)

	for _, node := range g.cfg.Nodes {
		if _, ok := localTemplateByNode[node]; ok {
			continue
		}

		sourceChoice, stageMethod, found := selectStageSource(templates, localTemplateByNode, node)
		if !found {
			if g.cfg.TemplateStageMode == "required" {
				return nil, fmt.Errorf("node %s has no stageable template source; cross-node staging requires shared template storage or matching target storage", node)
			}
			continue
		}
		if g.cfg.TemplateVMIDMin == 0 || g.cfg.TemplateVMIDMax == 0 {
			return nil, fmt.Errorf("template staging is required for at least one node, but template_vmid_range is not configured")
		}
		sourceVersion := sourceVersions[sourceChoice.Resource.VMID]
		if existing, ok := existingTemplates[managedTemplateKey(node, sourceChoice.Resource.VMID)]; ok && shouldReuseManagedTemplate(sourceVersion, existing.Version) {
			templates = append(templates, existing.Resource)
			keepTemplates[existing.ID] = existing.ManagedTemplate
			continue
		}

		stageRequests = append(stageRequests, stageRequest{
			node:   node,
			source: sourceChoice,
			method: stageMethod,
		})
	}

	usedVMIDs := map[int]struct{}{}
	for _, resource := range resources {
		if resource.VMID <= 0 {
			continue
		}
		// Obsolete managed template artifacts are deleted before new staging, so their VMIDs are reusable.
		if _, ok := existingArtifacts[instanceID(resource.Node, resource.VMID)]; ok {
			if _, keep := keepTemplates[instanceID(resource.Node, resource.VMID)]; !keep {
				continue
			}
		}
		usedVMIDs[resource.VMID] = struct{}{}
	}

	plans := make([]stagePlan, 0, len(stageRequests))
	for _, request := range stageRequests {
		vmid, err := g.allocateManagedTemplateVMID(usedVMIDs)
		if err != nil {
			return nil, err
		}
		usedVMIDs[vmid] = struct{}{}
		plans = append(plans, stagePlan{
			ManagedTemplate: ManagedTemplate{
				ID:         instanceID(request.node, vmid),
				Node:       request.node,
				VMID:       vmid,
				Name:       fmt.Sprintf("%s-%s-%d", g.cfg.TemplateNamePrefix, request.node, request.source.Resource.VMID),
				SourceVMID: request.source.Resource.VMID,
				Storage:    request.source.Storage,
			},
			method: request.method,
		})
	}

	for _, existing := range existingOrder {
		if _, keep := keepTemplates[existing.ID]; keep {
			continue
		}
		g.log.Warn("deleting preexisting managed template", "phase", "template-stage", "template", existing.ID)
		if err := g.destroyManagedTemplate(ctx, existing.ManagedTemplate, existing.AllowPartialDelete); err != nil {
			return nil, fmt.Errorf("delete managed template %s: %w", existing.ID, err)
		}
	}

	for _, stagedPlan := range plans {
		staged := stagedPlan.ManagedTemplate
		source := byVMID[staged.SourceVMID]
		if err := g.createManagedTemplate(ctx, source, staged, stagedPlan.method, sourceVersions[staged.SourceVMID]); err != nil {
			g.cleanupFailedManagedTemplate(staged, source.Node)
			return nil, err
		}
		resource, err := g.waitManagedTemplateResource(ctx, staged)
		if err != nil {
			g.cleanupFailedManagedTemplate(staged, source.Node)
			return nil, err
		}
		templates = append(templates, resource)
	}

	return templates, nil
}

func selectStageSource(templates []proxmoxclient.ClusterResource, choicesByNode map[string]templateChoice, targetNode string) (templateChoice, managedTemplateStageMethod, bool) {
	for _, template := range templates {
		choice := choicesByNode[template.Node]
		if choice.Resource.Node == targetNode {
			continue
		}
		if !choice.StorageShared {
			continue
		}
		if !storageAllowsNode(choice.StorageNodes, targetNode) {
			continue
		}
		return choice, managedTemplateStageDirectClone, true
	}

	for _, template := range templates {
		choice := choicesByNode[template.Node]
		if choice.Resource.Node == targetNode {
			continue
		}
		if choice.StorageShared {
			continue
		}
		if len(choice.StorageNodes) == 0 {
			continue
		}
		if !storageAllowsNode(choice.StorageNodes, targetNode) {
			continue
		}
		return choice, managedTemplateStageCloneMigrate, true
	}

	return templateChoice{}, "", false
}

func (g *Group) createManagedTemplate(ctx context.Context, source proxmoxclient.ClusterResource, staged ManagedTemplate, method managedTemplateStageMethod, sourceVersion string) error {
	if method == managedTemplateStageCloneMigrate {
		return g.createManagedTemplateViaMigration(ctx, source, staged, sourceVersion)
	}

	mode := "full"
	targetStorage := staged.Storage
	if g.supportsLinkedClone(staged.Storage) {
		mode = "linked"
		targetStorage = ""
	}

	g.log.Info("staging managed template", "phase", "template-stage", "template", staged.ID, "source_vmid", source.VMID, "source_node", source.Node, "storage", staged.Storage, "clone_mode", mode)

	cloneCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	upid, err := g.client.CloneVM(cloneCtx, source.Node, source.VMID, proxmoxclient.CloneRequest{
		NewID:         staged.VMID,
		Name:          staged.Name,
		TargetNode:    staged.Node,
		Pool:          g.cfg.Pool,
		TargetStorage: targetStorage,
		Full:          mode == "full",
		Snapshot:      g.cfg.CloneSnapshot,
	})
	if err != nil {
		return fmt.Errorf("clone staged template from %d to %s: %w", source.VMID, staged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, source.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait staged template clone %s: %w", staged.ID, err)
	}

	description := stagedTemplateDescription(staged.Node, staged.SourceVMID, sourceVersion)
	upid, err = g.client.SetVMConfig(cloneCtx, staged.Node, staged.VMID, proxmoxclient.SetConfigRequest{
		Tags:        g.cfg.ManagedTemplateTags,
		Description: description,
	})
	if err != nil {
		return fmt.Errorf("tag staged template %s: %w", staged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, staged.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait staged template config %s: %w", staged.ID, err)
	}

	status, err := g.client.GetVMStatus(cloneCtx, staged.Node, staged.VMID)
	if err == nil && status.Status == "running" {
		if err := g.ensureVMStopped(cloneCtx, staged.Node, staged.VMID); err != nil {
			return fmt.Errorf("stop staged template %s: %w", staged.ID, err)
		}
	}

	upid, err = g.client.ConvertVMToTemplate(cloneCtx, staged.Node, staged.VMID)
	if err != nil {
		return fmt.Errorf("convert staged template %s: %w", staged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, staged.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait convert staged template %s: %w", staged.ID, err)
	}

	return nil
}

func (g *Group) createManagedTemplateViaMigration(ctx context.Context, source proxmoxclient.ClusterResource, staged ManagedTemplate, sourceVersion string) error {
	g.log.Info("staging managed template via migration", "phase", "template-stage", "template", staged.ID, "source_vmid", source.VMID, "source_node", source.Node, "storage", staged.Storage, "clone_mode", "full")

	cloneCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	sourceStaged := staged
	sourceStaged.Node = source.Node
	sourceStaged.ID = instanceID(source.Node, staged.VMID)

	upid, err := g.client.CloneVM(cloneCtx, source.Node, source.VMID, proxmoxclient.CloneRequest{
		NewID:         staged.VMID,
		Name:          staged.Name,
		TargetNode:    source.Node,
		Pool:          g.cfg.Pool,
		TargetStorage: staged.Storage,
		Full:          true,
		Snapshot:      g.cfg.CloneSnapshot,
	})
	if err != nil {
		return fmt.Errorf("clone staged template locally from %d to %s: %w", source.VMID, sourceStaged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, source.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait local staged template clone %s: %w", sourceStaged.ID, err)
	}

	description := stagedTemplateDescription(staged.Node, staged.SourceVMID, sourceVersion)
	upid, err = g.client.SetVMConfig(cloneCtx, source.Node, staged.VMID, proxmoxclient.SetConfigRequest{
		Tags:        g.cfg.ManagedTemplateTags,
		Description: description,
	})
	if err != nil {
		return fmt.Errorf("tag local staged template %s: %w", sourceStaged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, source.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait local staged template config %s: %w", sourceStaged.ID, err)
	}

	status, err := g.client.GetVMStatus(cloneCtx, source.Node, staged.VMID)
	if err == nil && status.Status == "running" {
		if err := g.ensureVMStopped(cloneCtx, source.Node, staged.VMID); err != nil {
			return fmt.Errorf("stop local staged template %s: %w", sourceStaged.ID, err)
		}
	}

	upid, err = g.client.MigrateVM(cloneCtx, source.Node, staged.VMID, proxmoxclient.MigrateRequest{
		TargetNode:    staged.Node,
		TargetStorage: staged.Storage,
	})
	if err != nil {
		return fmt.Errorf("migrate staged template %s to %s: %w", sourceStaged.ID, staged.Node, err)
	}
	if err := g.client.WaitForTask(cloneCtx, source.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait staged template migration %s to %s: %w", sourceStaged.ID, staged.Node, err)
	}

	upid, err = g.client.SetVMConfig(cloneCtx, staged.Node, staged.VMID, proxmoxclient.SetConfigRequest{
		Tags:        g.cfg.ManagedTemplateTags,
		Description: description,
	})
	if err != nil {
		return fmt.Errorf("tag migrated staged template %s: %w", staged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, staged.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait migrated staged template config %s: %w", staged.ID, err)
	}

	upid, err = g.client.ConvertVMToTemplate(cloneCtx, staged.Node, staged.VMID)
	if err != nil {
		return fmt.Errorf("convert staged template %s: %w", staged.ID, err)
	}
	if err := g.client.WaitForTask(cloneCtx, staged.Node, upid, g.cfg.TaskPollInterval); err != nil {
		return fmt.Errorf("wait convert staged template %s: %w", staged.ID, err)
	}

	return nil
}

func (g *Group) cleanupFailedManagedTemplate(template ManagedTemplate, extraNodes ...string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), g.cfg.ShutdownTimeout)
	defer cancel()

	nodes := append([]string{template.Node}, extraNodes...)
	seen := map[string]struct{}{}
	for _, node := range nodes {
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}

		nodeTemplate := template
		nodeTemplate.Node = node
		nodeTemplate.ID = instanceID(node, template.VMID)
		if err := g.destroyManagedTemplate(cleanupCtx, nodeTemplate, true); err != nil {
			g.log.Warn("failed to clean up staged template after error", "phase", "template-stage", "template", nodeTemplate.ID, "error", err)
		}
	}
}

type existingManagedTemplate struct {
	ManagedTemplate
	Resource           proxmoxclient.ClusterResource
	Version            string
	AllowPartialDelete bool
}

func (g *Group) lookupManagedTemplateResource(ctx context.Context, node string, vmid int) (proxmoxclient.ClusterResource, bool, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return proxmoxclient.ClusterResource{}, false, err
	}
	for _, resource := range resources {
		if resource.Node != node || resource.VMID != vmid {
			continue
		}
		if !g.isManagedTemplate(resource) {
			return proxmoxclient.ClusterResource{}, false, nil
		}
		return resource, true, nil
	}
	return proxmoxclient.ClusterResource{}, false, nil
}

func (g *Group) lookupTemplateResource(ctx context.Context, template ManagedTemplate) (proxmoxclient.ClusterResource, error) {
	resource, found, err := g.lookupManagedTemplateResource(ctx, template.Node, template.VMID)
	if err != nil {
		return proxmoxclient.ClusterResource{}, err
	}
	if !found {
		return proxmoxclient.ClusterResource{}, fmt.Errorf("staged template %s/%d is not found as managed template", template.Node, template.VMID)
	}
	return resource, nil
}

func (g *Group) waitManagedTemplateResource(ctx context.Context, template ManagedTemplate) (proxmoxclient.ClusterResource, error) {
	waitCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	ticker := time.NewTicker(g.cfg.TaskPollInterval)
	defer ticker.Stop()

	for {
		resource, err := g.lookupTemplateResource(waitCtx, template)
		if err == nil {
			return resource, nil
		}

		select {
		case <-waitCtx.Done():
			return proxmoxclient.ClusterResource{}, err
		case <-ticker.C:
		}
	}
}

func (g *Group) allocateManagedTemplateVMID(used map[int]struct{}) (int, error) {
	for vmid := g.cfg.TemplateVMIDMin; vmid <= g.cfg.TemplateVMIDMax; vmid++ {
		if _, exists := used[vmid]; exists {
			continue
		}
		return vmid, nil
	}
	return 0, fmt.Errorf("no free VMID in configured template_vmid_range")
}

func (g *Group) destroyManagedTemplate(ctx context.Context, template ManagedTemplate, allowPartial bool) error {
	config, found, err := g.lookupVMConfig(ctx, template.Node, template.VMID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if allowPartial {
		if template.VMID < g.cfg.TemplateVMIDMin || template.VMID > g.cfg.TemplateVMIDMax {
			return fmt.Errorf("refusing to delete failed staged template %s/%d outside configured template_vmid_range", template.Node, template.VMID)
		}
		if config.Name != template.Name {
			return fmt.Errorf("refusing to delete failed staged template %s/%d with unexpected name %q", template.Node, template.VMID, config.Name)
		}
		if config.Pool != "" && config.Pool != g.cfg.Pool {
			return fmt.Errorf("refusing to delete failed staged template %s/%d from pool %q", template.Node, template.VMID, config.Pool)
		}
	} else if !g.isManagedTemplateConfig(template.VMID, config) {
		return fmt.Errorf("refusing to delete non-managed template %s/%d", template.Node, template.VMID)
	}

	upid, err := g.client.DeleteVM(ctx, template.Node, template.VMID)
	if err != nil {
		return err
	}
	if err := g.client.WaitForTask(ctx, template.Node, upid, g.cfg.TaskPollInterval); err != nil {
		if isMissingManagedTemplateAfterDelete(err) {
			return nil
		}
		return err
	}
	return nil
}

func (g *Group) List(ctx context.Context) ([]ManagedInstance, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return nil, err
	}

	instances := make([]ManagedInstance, 0)
	seen := map[string]struct{}{}
	for _, resource := range resources {
		if !g.isManaged(resource) {
			continue
		}

		state := mapState(resource.Status)
		id := instanceID(resource.Node, resource.VMID)
		seen[id] = struct{}{}

		g.mu.Lock()
		delete(g.accepted, id)
		if transient, ok := g.transient[id]; ok {
			state = transient
		}
		g.mu.Unlock()

		instance := ManagedInstance{
			ID:    id,
			Node:  resource.Node,
			VMID:  resource.VMID,
			Name:  resource.Name,
			State: state,
		}

		config, err := g.client.GetVMConfig(ctx, resource.Node, resource.VMID)
		if err == nil && g.cfg.NetworkMode == "static" {
			if ip, ok := parseStaticIPv4(config.IPConfig0); ok {
				instance.IP = ip
			}
		}

		instances = append(instances, instance)
	}

	g.mu.Lock()
	for id, accepted := range g.accepted {
		if _, ok := seen[id]; ok {
			continue
		}

		state := accepted.State
		if state == provider.StateCreating {
			accepted.ReportedCreating = true
			g.accepted[id] = accepted
		} else if !accepted.ReportedCreating {
			state = provider.StateCreating
			accepted.ReportedCreating = true
			g.accepted[id] = accepted
		} else if state == provider.StateDeleted || state == provider.StateTimeout {
			delete(g.accepted, id)
		}

		instances = append(instances, ManagedInstance{
			ID:    id,
			Node:  accepted.Node,
			VMID:  accepted.VMID,
			Name:  fmt.Sprintf("%s-%d", g.cfg.NamePrefix, accepted.VMID),
			State: state,
		})
	}
	g.mu.Unlock()

	return instances, nil
}

func (g *Group) MetricsSnapshot(ctx context.Context) (metrics.Snapshot, error) {
	vmResources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return metrics.Snapshot{}, err
	}

	var storageResources []proxmoxclient.ClusterResource
	storageNodes := map[string]map[string]struct{}{}
	if len(g.cfg.TargetStorages) > 0 {
		storageResources, err = g.client.ListClusterResources(ctx, "storage")
		if err != nil {
			return metrics.Snapshot{}, err
		}
		storageNodes = indexStorageNodes(storageResources)
	}

	states, _, err := g.collectNodePlanStates(ctx, storageResources, storageNodes)
	if err != nil {
		return metrics.Snapshot{}, err
	}

	g.mu.Lock()
	g.applyAllocatedResources(states, vmResources)
	g.applyPendingReservations(states)
	pendingInstances := len(g.pendingVMIDs)
	instances := g.instanceStateCountsLocked(vmResources)
	g.mu.Unlock()

	snapshot := metrics.Snapshot{
		Version:           1,
		Identity:          metrics.Identity{Cluster: g.cfg.ClusterName, Pool: g.cfg.Pool, Group: g.cfg.NamePrefix},
		Up:                true,
		LastScrapeSuccess: true,
		Instances:         instances,
		PendingInstances:  pendingInstances,
		Nodes:             make([]metrics.NodeSnapshot, 0, len(states)),
	}

	for _, state := range states {
		resourceTotals := state.resourceTotals()
		memoryReserveBytes := state.Reserve.MemoryMBFor(resourceTotals) * 1024.0 * 1024.0
		cpuReserveCores := state.Reserve.CPUCoresFor(resourceTotals)
		snapshot.Nodes = append(snapshot.Nodes, metrics.NodeSnapshot{
			Node:                              state.Name,
			TotalCPUCores:                     state.TotalCPUCores,
			RuntimeFreeCPUCores:               state.FreeCPUCores,
			ReservedCPUCores:                  cpuReserveCores,
			AllocatedCPUCores:                 state.AllocatedCPUCores,
			CPUAllocationLimitCores:           state.CPUAllocationLimitCores,
			PhysicalAllocationFreeCPUCores:    physicalAllocationFree(state.TotalCPUCores, state.AllocatedCPUCores),
			TotalMemoryBytes:                  state.TotalMemoryMB * 1024.0 * 1024.0,
			RuntimeFreeMemoryBytes:            state.FreeMemoryMB * 1024.0 * 1024.0,
			ReservedMemoryBytes:               memoryReserveBytes,
			AllocatedMemoryBytes:              state.AllocatedMemoryMB * 1024.0 * 1024.0,
			MemoryAllocationLimitBytes:        state.MemoryAllocationLimitMB * 1024.0 * 1024.0,
			PhysicalAllocationFreeMemoryBytes: physicalAllocationFree(state.TotalMemoryMB, state.AllocatedMemoryMB) * 1024.0 * 1024.0,
		})

		for storage, freeGB := range state.StorageFreeGB {
			schedulerNode := state.schedulerNode(storage)
			snapshot.Storages = append(snapshot.Storages, metrics.StorageSnapshot{
				Node:          state.Name,
				Storage:       storage,
				TotalBytes:    state.StorageTotalGB[storage] * 1024.0 * 1024.0 * 1024.0,
				FreeBytes:     freeGB * 1024.0 * 1024.0 * 1024.0,
				ReservedBytes: state.Reserve.DiskGBFor(schedulerNode) * 1024.0 * 1024.0 * 1024.0,
			})
		}
	}

	return snapshot, nil
}

func (g *Group) instanceStateCountsLocked(resources []proxmoxclient.ClusterResource) map[string]int {
	counts := map[string]int{}
	seen := map[string]struct{}{}
	for _, resource := range resources {
		if !g.isManaged(resource) {
			continue
		}

		id := instanceID(resource.Node, resource.VMID)
		seen[id] = struct{}{}
		state := mapState(resource.Status)
		if transient, ok := g.transient[id]; ok {
			state = transient
		}
		counts[string(state)]++
	}

	for id, accepted := range g.accepted {
		if _, ok := seen[id]; ok {
			continue
		}
		counts[string(accepted.State)]++
	}
	return counts
}

func (g *Group) Increase(ctx context.Context, delta int) ([]string, error) {
	if delta <= 0 {
		return nil, nil
	}

	operationID := g.nextOperationID("inc")
	log := g.log.With("operation_id", operationID, "operation", "increase")
	started := time.Now()
	log.Info("increase started", "requested", delta)

	plans, planErr := g.planIncrease(ctx, delta, operationID)
	if len(plans) == 0 {
		if planErr != nil {
			log.Error("increase failed", "duration", humanDuration(started), "error", planErr)
			var placementErr *scheduler.PlacementError
			if !errors.As(planErr, &placementErr) && !errors.Is(planErr, context.Canceled) {
				g.reportProblem(metrics.ProblemEvent{
					Code:        "increase_failed",
					State:       metrics.ProblemRecent,
					Severity:    "error",
					Phase:       "placement",
					OperationID: operationID,
					Message:     planErr.Error(),
				})
			}
		}
		return nil, planErr
	}

	for _, plan := range plans {
		id := instanceID(plan.Node, plan.VMID)
		g.acceptProvision(plan)
		g.setTransient(id, provider.StateCreating)
		g.provisionWg.Add(1)
		go func(plan provisionPlan, id string) {
			defer g.provisionWg.Done()
			defer g.releasePendingPlan(plan)

			if _, err := g.provisionOne(g.runCtx, plan); err != nil {
				g.failAcceptedProvision(plan)
				g.log.Error(
					"provisioning failed",
					"operation_id", plan.OperationID,
					"operation", "increase",
					"phase", "provision",
					"instance", id,
					"error", err,
				)
			}
		}(plan, id)
	}

	if planErr != nil {
		log.Warn("provisioning partially requested", "requested", delta, "accepted", len(plans), "error", planErr)
	}
	log.Info("increase accepted", "duration", humanDuration(started), "requested", delta, "accepted", len(plans))

	return plannedIDs(plans), nil
}

func (g *Group) Decrease(ctx context.Context, ids []string) ([]string, error) {
	type result struct {
		id  string
		err error
	}

	operationID := g.nextOperationID("dec")
	log := g.log.With("operation_id", operationID, "operation", "decrease")
	started := time.Now()
	log.Info("decrease started", "requested", len(ids))

	workerCount := g.deleteLimiter.Capacity()
	if workerCount <= 0 {
		workerCount = 1
	}

	jobs := make(chan string)
	results := make(chan result, len(ids))
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				err := g.deleteLimiter.Do(ctx, func(ctx context.Context) error {
					return g.deleteOne(ctx, id, operationID)
				})
				results <- result{id: id, err: err}
			}
		}()
	}

	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	close(results)

	var deleted []string
	var errs []error
	for res := range results {
		if res.err != nil {
			log.Error("deletion failed", "phase", "delete", "instance", res.id, "error", res.err)
			if !errors.Is(res.err, context.Canceled) {
				node, _, _ := parseInstanceID(res.id)
				g.reportProblem(metrics.ProblemEvent{
					Code:        "delete_failed",
					State:       metrics.ProblemRecent,
					Severity:    "error",
					Phase:       "delete",
					Node:        node,
					Instance:    res.id,
					OperationID: operationID,
					Message:     res.err.Error(),
				})
			}
			errs = append(errs, res.err)
			continue
		}
		deleted = append(deleted, res.id)
	}

	log.Info("decrease completed", "duration", humanDuration(started), "requested", len(ids), "deleted", len(deleted), "failed", len(errs))
	return deleted, errors.Join(errs...)
}

func (g *Group) Suspend(ctx context.Context, ids []string) ([]string, error) {
	var succeeded []string
	var errs []error

	for _, id := range ids {
		node, vmid, err := parseInstanceID(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		g.setTransient(id, provider.StateSuspending)

		upid, err := g.client.SuspendVM(ctx, node, vmid)
		if err != nil {
			g.clearTransient(id)
			errs = append(errs, fmt.Errorf("suspend %s: %w", id, err))
			continue
		}

		if err := g.client.WaitForTask(ctx, node, upid, g.cfg.TaskPollInterval); err != nil {
			g.clearTransient(id)
			errs = append(errs, fmt.Errorf("suspend %s: %w", id, err))
			continue
		}

		g.clearTransient(id)
		succeeded = append(succeeded, id)
	}

	return succeeded, errors.Join(errs...)
}

func (g *Group) Resume(ctx context.Context, ids []string) ([]string, error) {
	var succeeded []string
	var errs []error

	for _, id := range ids {
		node, vmid, err := parseInstanceID(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		g.setTransient(id, provider.StateResuming)

		upid, err := g.client.ResumeVM(ctx, node, vmid)
		if err != nil {
			g.clearTransient(id)
			errs = append(errs, fmt.Errorf("resume %s: %w", id, err))
			continue
		}

		if err := g.client.WaitForTask(ctx, node, upid, g.cfg.TaskPollInterval); err != nil {
			g.clearTransient(id)
			errs = append(errs, fmt.Errorf("resume %s: %w", id, err))
			continue
		}

		g.clearTransient(id)
		succeeded = append(succeeded, id)
	}

	return succeeded, errors.Join(errs...)
}

func (g *Group) Shutdown(ctx context.Context) error {
	if g.cancelRun != nil {
		g.cancelRun()
	}

	done := make(chan struct{})
	go func() {
		g.provisionWg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (g *Group) ConnectInfo(ctx context.Context, id string, settings provider.Settings) (provider.ConnectInfo, error) {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return provider.ConnectInfo{}, err
	}

	if g.cfg.AgentRequired {
		ip, err := g.discoverIPAddress(ctx, instance.Node, instance.VMID, instance.IP, g.cfg.AgentTimeout)
		if err != nil {
			return provider.ConnectInfo{}, err
		}
		instance.IP = ip
	}

	info := provider.ConnectInfo{ConnectorConfig: settings.ConnectorConfig}
	info.ID = id
	if info.OS == "" {
		info.OS = "linux"
	}
	if info.Arch == "" {
		info.Arch = "amd64"
	}
	if info.Protocol == "" {
		info.Protocol = provider.ProtocolSSH
	}
	if info.ProtocolPort == 0 {
		info.ProtocolPort = provider.DefaultProtocolPorts[info.Protocol]
	}
	if info.Username == "" {
		if g.cfg.CIUser != "" {
			info.Username = g.cfg.CIUser
		} else {
			info.Username = "root"
		}
	}
	info.InternalAddr = instance.IP.String()
	info.ExternalAddr = instance.IP.String()
	return info, nil
}

func (g *Group) Heartbeat(ctx context.Context, id string) error {
	instance, err := g.Get(ctx, id)
	if err != nil {
		return provider.ErrInstanceUnhealthy
	}
	if instance.State != provider.StateRunning {
		return provider.ErrInstanceUnhealthy
	}
	if !g.cfg.AgentRequired {
		return nil
	}
	if _, err := g.discoverIPAddress(ctx, instance.Node, instance.VMID, instance.IP, g.cfg.AgentTimeout); err != nil {
		return provider.ErrInstanceUnhealthy
	}
	return nil
}

func (g *Group) Get(ctx context.Context, id string) (ManagedInstance, error) {
	node, vmid, err := parseInstanceID(id)
	if err != nil {
		return ManagedInstance{}, err
	}

	resource, found, err := g.lookupManagedResource(ctx, node, vmid)
	if err != nil {
		return ManagedInstance{}, err
	}
	if !found {
		return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
	}

	config, err := g.client.GetVMConfig(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return ManagedInstance{}, fmt.Errorf("managed instance %s not found", id)
		}
		return ManagedInstance{}, err
	}

	status, err := g.client.GetVMStatus(ctx, node, vmid)
	if err != nil {
		return ManagedInstance{}, err
	}

	instance := ManagedInstance{
		ID:    id,
		Node:  node,
		VMID:  vmid,
		Name:  resource.Name,
		State: mapState(status.Status),
	}
	if g.cfg.NetworkMode == "static" {
		if ip, ok := parseStaticIPv4(config.IPConfig0); ok {
			instance.IP = ip
		}
	}

	return instance, nil
}

func (g *Group) planIncrease(ctx context.Context, delta int, operationID string) ([]provisionPlan, error) {
	vmResources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return nil, err
	}

	var storageResources []proxmoxclient.ClusterResource
	storageNodes := map[string]map[string]struct{}{}
	if len(g.cfg.TargetStorages) > 0 {
		storageResources, err = g.client.ListClusterResources(ctx, "storage")
		if err != nil {
			return nil, err
		}
		storageNodes = indexStorageNodes(storageResources)
	}

	states, skippedReasons, err := g.collectNodePlanStates(ctx, storageResources, storageNodes)
	if err != nil {
		return nil, err
	}

	req := scheduler.Requirement{
		MemoryMB: g.requiredMemoryMB(),
		DiskGB:   g.requiredDiskGB(),
		CPUCores: g.requiredCPUCores(),
	}
	if len(g.cfg.TargetStorages) == 0 {
		req.DiskGB = 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	g.pruneCloneQuarantineLocked(now)
	vmids := g.availableVMIDs(vmResources, delta+len(g.cloneQuarantine))
	if len(vmids) == 0 {
		return nil, fmt.Errorf("no free VMID in configured range")
	}
	g.applyAllocatedResources(states, vmResources)
	g.applyPendingReservations(states)
	plans := make([]provisionPlan, 0, delta)
	var errs []error
	if len(vmids) < delta {
		errs = append(errs, fmt.Errorf("only %d free VMIDs available in configured range", len(vmids)))
	}

	var lastPlacementErr error
	for _, vmid := range vmids {
		if len(plans) == delta {
			break
		}

		nodeInfos, dynamicSkipped, quarantineSkipped := g.buildCandidateNodesForVMID(states, vmid, now)
		if len(nodeInfos) == 0 {
			reasons := append(append([]string{}, skippedReasons...), dynamicSkipped...)
			if len(reasons) == 0 {
				reasons = scheduler.Diagnose(g.diagnosticNodes(states), g.cfg.Reserve, req)
			}
			lastPlacementErr = &scheduler.PlacementError{Reasons: reasons}
			if quarantineSkipped {
				continue
			}
			break
		}

		targetNode, err := g.cfg.Scheduler.Select(nodeInfos, g.cfg.Reserve, req)
		if err != nil {
			var placementErr *scheduler.PlacementError
			if errors.As(err, &placementErr) {
				placementErr.Reasons = append(append([]string{}, skippedReasons...), dynamicSkipped...)
				if len(placementErr.Reasons) == 0 {
					placementErr.Reasons = scheduler.Diagnose(g.diagnosticNodes(states), g.cfg.Reserve, req)
				}
			}
			lastPlacementErr = err
			if quarantineSkipped {
				continue
			}
			break
		}

		plans = append(plans, provisionPlan{
			OperationID:     operationID,
			TemplateNode:    targetNode.TemplateNode,
			TemplateVMID:    targetNode.TemplateVMID,
			TemplateStorage: stateTemplateStorage(states, targetNode.Name),
			Node:            targetNode.Name,
			TargetStorage:   targetNode.TargetStorage,
			VMID:            vmid,
			Requirement:     req,
		})
		g.reservePlannedResources(states, targetNode, req)
	}

	if len(plans) < delta && lastPlacementErr != nil {
		var placementErr *scheduler.PlacementError
		if errors.As(lastPlacementErr, &placementErr) && len(placementErr.Reasons) > 0 {
			g.log.Warn(
				"placement rejected",
				"operation_id", operationID,
				"operation", "increase",
				"phase", "placement",
				"reasons", strings.Join(placementErr.Reasons, "; "),
			)
		}
		errs = append(errs, lastPlacementErr)
	}

	g.registerPendingPlans(plans)

	return plans, errors.Join(errs...)
}

func (g *Group) provisionOne(ctx context.Context, plan provisionPlan) (string, error) {
	id := instanceID(plan.Node, plan.VMID)
	var lease ippool.Lease
	var err error
	log := g.log.With(
		"operation_id", plan.OperationID,
		"operation", "increase",
		"instance", id,
		"node", plan.Node,
		"vmid", plan.VMID,
		"storage", plan.TargetStorage,
	)
	if g.cfg.NetworkMode == "static" {
		if g.pool == nil {
			return "", fmt.Errorf("static network mode requires ip pool")
		}
		lease, err = g.pool.Acquire(ctx, id)
		if err != nil {
			g.reportPlanProblem(plan, "ip_allocation_failed", metrics.ProblemRecent, "network", err, time.Time{})
			return "", err
		}
	}

	g.setTransient(id, provider.StateCreating)
	defer func() {
		g.clearTransient(id)
	}()

	rollback := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), g.cfg.CloneTimeout)
		defer cancel()
		// safeDestroyProvisioningVM is idempotent, so retry it through transient
		// network/DNS blips: the outage that caused the failure often also blocks the
		// cleanup that would otherwise strand the VM.
		expectedName := fmt.Sprintf("%s-%d", g.cfg.NamePrefix, plan.VMID)
		cleanupErr := proxmoxclient.Retry(cleanupCtx, proxmoxclient.NewRetryPolicy(g.cfg.CloneTimeout), func(ctx context.Context) error {
			return g.safeDestroyProvisioningVM(ctx, plan.Node, plan.VMID, expectedName)
		})
		if cleanupErr != nil {
			log.Error("provisioning cleanup failed", "phase", "cleanup", "error", cleanupErr)
			g.reportPlanProblem(plan, "cleanup_failed", metrics.ProblemRecent, "cleanup", cleanupErr, time.Time{})
		}
		if g.pool != nil {
			// Failed provisioning must not leave leases behind or put them into reuse cooldown.
			if err := g.pool.Forget(cleanupCtx, id); err != nil {
				log.Error("lease cleanup failed", "phase", "cleanup", "error", err)
				g.reportPlanProblem(plan, "cleanup_failed", metrics.ProblemRecent, "cleanup", err, time.Time{})
			}
		}
	}

	cloneCtx, cancel := context.WithTimeout(ctx, g.cfg.CloneTimeout)
	defer cancel()

	log.Info("provisioning started", "phase", "provision")
	cloneStart := time.Now()
	err = g.cloneLimiter.Do(cloneCtx, func(ctx context.Context) error {
		log.Info("clone started", "phase", "clone")
		effectiveMode := g.effectiveCloneMode(plan)
		targetStorage := plan.TargetStorage
		fullClone := effectiveMode == "full"
		if effectiveMode == "linked" {
			targetStorage = ""
		}
		upid, err := g.client.CloneVM(ctx, plan.TemplateNode, plan.TemplateVMID, proxmoxclient.CloneRequest{
			NewID:         plan.VMID,
			Name:          fmt.Sprintf("%s-%d", g.cfg.NamePrefix, plan.VMID),
			TargetNode:    plan.Node,
			Pool:          g.cfg.Pool,
			TargetStorage: targetStorage,
			Full:          fullClone,
			Snapshot:      g.cfg.CloneSnapshot,
		})
		if err != nil {
			return err
		}
		return g.client.WaitForTask(ctx, plan.TemplateNode, upid, g.cfg.TaskPollInterval)
	})
	if err != nil {
		log.Error("clone failed", "phase", "clone", "duration", humanDuration(cloneStart), "error", err)
		if isCloneVolumeCollision(err, plan.VMID) {
			g.reportPlanProblem(plan, "clone_volume_collision", metrics.ProblemActive, "clone", err, time.Now().Add(cloneCollisionQuarantineTTL))
		} else {
			g.reportPlanProblem(plan, "clone_failed", metrics.ProblemRecent, "clone", err, time.Time{})
		}
		g.quarantineClonePlacement(plan, err)
		rollback()
		return "", err
	}
	g.clearClonePlacementQuarantine(plan)
	log.Info("clone completed", "phase", "clone", "duration", humanDuration(cloneStart))

	description, err := g.renderDescription(plan.Node, plan.VMID, lease.IP)
	if err != nil {
		g.reportPlanProblem(plan, "config_failed", metrics.ProblemRecent, "config", err, time.Time{})
		rollback()
		return "", err
	}

	sshKeys := append([]string{}, g.cfg.StaticSSHPublicKeys...)
	if g.cfg.GeneratedSSHPublicKey != "" {
		sshKeys = append(sshKeys, g.cfg.GeneratedSSHPublicKey)
	}

	ipConfig := "ip=dhcp"
	if g.cfg.NetworkMode == "static" {
		ipConfig = fmt.Sprintf("ip=%s/%d,gw=%s", lease.IP, lease.Prefix.Bits(), lease.Gateway)
	}

	configStart := time.Now()
	log.Info("config started", "phase", "config")
	upid, err := g.client.SetVMConfig(cloneCtx, plan.Node, plan.VMID, proxmoxclient.SetConfigRequest{
		CloudInitInterface: g.cfg.CloudInitInterface,
		Tags:               g.cfg.MandatoryTags,
		Description:        description,
		IPConfig:           ipConfig,
		MemoryMB:           g.cfg.VMMemoryMB,
		CPUCores:           g.cfg.VMCPUCores,
		CIUser:             g.cfg.CIUser,
		CIPassword:         g.cfg.GeneratedPassword,
		SSHKeys:            sshKeys,
		NameServer:         strings.Join(g.cfg.NameServers, " "),
		SearchDomain:       g.cfg.SearchDomain,
		AgentEnabled:       g.cfg.AgentRequired,
		DisableCIUpgrade:   true,
	})
	if err != nil {
		log.Error("config failed", "phase", "config", "duration", humanDuration(configStart), "error", err)
		g.reportPlanProblem(plan, "config_failed", metrics.ProblemRecent, "config", err, time.Time{})
		rollback()
		return "", err
	}
	if err := g.client.WaitForTask(cloneCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
		log.Error("config wait failed", "phase", "config", "duration", humanDuration(configStart), "error", err)
		g.reportPlanProblem(plan, "config_failed", metrics.ProblemRecent, "config", err, time.Time{})
		rollback()
		return "", err
	}
	log.Info("config completed", "phase", "config", "duration", humanDuration(configStart))

	if g.cfg.VMDiskMB > 0 {
		resizeStart := time.Now()
		log.Info("resize started", "phase", "resize", "disk_device", g.templateDisk, "disk_mb", g.cfg.VMDiskMB)
		upid, err = g.client.ResizeVMDisk(cloneCtx, plan.Node, plan.VMID, g.templateDisk, g.cfg.VMDiskMB)
		if err != nil {
			log.Error("resize failed", "phase", "resize", "duration", humanDuration(resizeStart), "error", err)
			g.reportPlanProblem(plan, "resize_failed", metrics.ProblemRecent, "resize", err, time.Time{})
			rollback()
			return "", err
		}
		if err := g.client.WaitForTask(cloneCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
			log.Error("resize wait failed", "phase", "resize", "duration", humanDuration(resizeStart), "error", err)
			g.reportPlanProblem(plan, "resize_failed", metrics.ProblemRecent, "resize", err, time.Time{})
			rollback()
			return "", err
		}
		log.Info("resize completed", "phase", "resize", "duration", humanDuration(resizeStart))
	}

	startPhase := time.Now()
	err = g.startLimiter.Do(ctx, func(ctx context.Context) error {
		startCtx, cancel := context.WithTimeout(ctx, g.cfg.StartTimeout)
		defer cancel()

		log.Info("start started", "phase", "start")
		upid, err := g.client.StartVM(startCtx, plan.Node, plan.VMID)
		if err != nil {
			return fmt.Errorf("StartVM API call: %w", err)
		}
		log.Info("start VM requested, waiting for task")
		if err := g.client.WaitForTask(startCtx, plan.Node, upid, g.cfg.TaskPollInterval); err != nil {
			return fmt.Errorf("start task: %w", err)
		}
		log.Info("start task completed")
		if g.cfg.AgentRequired {
			log.Info("waiting for guest agent")
			if _, err := g.discoverIPAddress(startCtx, plan.Node, plan.VMID, lease.IP, g.cfg.AgentTimeout); err != nil {
				return fmt.Errorf("guest agent discovery: %w", err)
			}
			log.Info("guest agent ready")
		}
		return nil
	})
	if err != nil {
		log.Error("start failed", "phase", "start", "duration", humanDuration(startPhase), "error", err)
		g.reportPlanProblem(plan, "start_failed", metrics.ProblemRecent, "start", err, time.Time{})
		rollback()
		return "", err
	}
	log.Info("start completed", "phase", "start", "duration", humanDuration(startPhase))
	log.Info("provisioning completed", "phase", "provision")

	return id, nil
}

func (g *Group) effectiveCloneMode(plan provisionPlan) string {
	switch g.cfg.CloneMode {
	case "linked":
		return "linked"
	case "full":
		return "full"
	default:
		if plan.TemplateNode == plan.Node {
			if plan.TargetStorage == "" || plan.TargetStorage == plan.TemplateStorage {
				if g.supportsLinkedClone(plan.TemplateStorage) {
					return "linked"
				}
			}
		}
		return "full"
	}
}

func (g *Group) supportsLinkedClone(storage string) bool {
	pluginType := g.storagePlugins[storage]
	_, ok := linkedClonePluginTypes[pluginType]
	return ok
}

func (g *Group) deleteOne(ctx context.Context, id, operationID string) error {
	started := time.Now()
	log := g.log.With("operation_id", operationID, "operation", "decrease", "phase", "delete", "instance", id)
	log.Info("deletion started")

	instance, err := g.Get(ctx, id)
	if err != nil {
		return err
	}

	g.setTransient(id, provider.StateDeleting)
	defer g.clearTransient(id)

	if err := g.ensureVMStopped(ctx, instance.Node, instance.VMID); err != nil {
		return err
	}

	if err := g.safeDestroyManagedVM(ctx, instance); err != nil {
		return err
	}

	if g.pool != nil {
		if err := g.pool.Release(ctx, id); err != nil {
			return err
		}
	}
	log.Info("deletion completed", "duration", humanDuration(started))
	return nil
}

func (g *Group) safeDestroyManagedVM(ctx context.Context, instance ManagedInstance) error {
	_, found, err := g.lookupManagedResource(ctx, instance.Node, instance.VMID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	upid, err := g.client.DeleteVM(ctx, instance.Node, instance.VMID)
	if err != nil {
		return err
	}
	return g.client.WaitForTask(ctx, instance.Node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) safeDestroyProvisioningVM(ctx context.Context, node string, vmid int, expectedName string) error {
	config, found, err := g.lookupVMConfig(ctx, node, vmid)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Rollback happens before the instance is fully tagged as managed, so we accept only
	// the narrow identity established by this provisioning attempt.
	if config.Template == 1 || config.Name != expectedName {
		return fmt.Errorf("refusing to delete unexpected VM %s/%d during rollback", node, vmid)
	}
	if config.Pool != "" && config.Pool != g.cfg.Pool {
		return fmt.Errorf("refusing to delete VM %s/%d with unexpected pool %q during rollback", node, vmid, config.Pool)
	}
	if vmid < g.cfg.VMIDMin || vmid > g.cfg.VMIDMax {
		return fmt.Errorf("refusing to delete VM %s/%d outside configured VMID range during rollback", node, vmid)
	}
	if err := g.ensureVMStopped(ctx, node, vmid); err != nil {
		return err
	}

	upid, err := g.client.DeleteVM(ctx, node, vmid)
	if err != nil {
		return err
	}
	return g.client.WaitForTask(ctx, node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) ensureVMStopped(ctx context.Context, node string, vmid int) error {
	status, err := g.client.GetVMStatus(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return nil
		}
		return err
	}
	if status.Status != "running" {
		return nil
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, g.cfg.ShutdownTimeout)
	defer stopCancel()
	upid, stopErr := g.client.StopVM(stopCtx, node, vmid)
	if stopErr != nil {
		return stopErr
	}
	return g.client.WaitForTask(stopCtx, node, upid, g.cfg.TaskPollInterval)
}

func (g *Group) lookupVMConfig(ctx context.Context, node string, vmid int) (proxmoxclient.VMConfig, bool, error) {
	config, err := g.client.GetVMConfig(ctx, node, vmid)
	if err != nil {
		if errors.Is(err, proxmoxclient.ErrNotFound) {
			return proxmoxclient.VMConfig{}, false, nil
		}
		return proxmoxclient.VMConfig{}, false, err
	}

	return config, true, nil
}

func (g *Group) lookupManagedResource(ctx context.Context, node string, vmid int) (proxmoxclient.ClusterResource, bool, error) {
	resources, err := g.client.ListClusterResources(ctx, "vm")
	if err != nil {
		return proxmoxclient.ClusterResource{}, false, err
	}
	for _, resource := range resources {
		if resource.Node != node || resource.VMID != vmid {
			continue
		}
		if !g.isManaged(resource) {
			return proxmoxclient.ClusterResource{}, false, nil
		}
		return resource, true, nil
	}
	return proxmoxclient.ClusterResource{}, false, nil
}

func (g *Group) discoverIPAddress(ctx context.Context, node string, vmid int, expected netip.Addr, timeout time.Duration) (netip.Addr, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(g.cfg.TaskPollInterval)
	defer ticker.Stop()

	for {
		interfaces, err := g.client.GetAgentInterfaces(waitCtx, node, vmid)
		if err == nil {
			for _, iface := range interfaces {
				for _, ip := range iface.IPAddresses {
					if ip.IPType != "ipv4" {
						continue
					}
					// The guest agent can report loopback or link-local addresses before the
					// real workload address appears. Only return a globally useful IPv4.
					addr, parseErr := netip.ParseAddr(ip.IPAddress)
					if parseErr != nil || !addr.Is4() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
						continue
					}
					if expected.IsValid() {
						if addr == expected {
							return addr, nil
						}
						continue
					}
					return addr, nil
				}
			}
		}

		select {
		case <-waitCtx.Done():
			if err != nil {
				return netip.Addr{}, fmt.Errorf("wait for guest agent ip %s: %w", expected, err)
			}
			if expected.IsValid() {
				return netip.Addr{}, fmt.Errorf("guest agent did not report expected IP %s", expected)
			}
			return netip.Addr{}, fmt.Errorf("guest agent did not report any usable IPv4 address")
		case <-ticker.C:
		}
	}
}

func (g *Group) collectNodePlanStates(ctx context.Context, storageResources []proxmoxclient.ClusterResource, storageNodes map[string]map[string]struct{}) ([]nodePlanState, []string, error) {
	nodes := make([]nodePlanState, 0, len(g.cfg.Nodes))
	skipped := make([]string, 0)
	for _, node := range g.cfg.Nodes {
		templateChoice, ok := g.templatesByNode[node]
		if !ok {
			skipped = append(skipped, fmt.Sprintf("%s: no usable template", node))
			continue
		}

		status, err := g.client.GetNodeStatus(ctx, node)
		if err != nil {
			g.log.Warn("skipping unavailable node", "phase", "placement", "node", node, "error", err)
			g.markNodeUnavailable(node, err)
			skipped = append(skipped, fmt.Sprintf("%s: unavailable", node))
			continue
		}
		g.clearNodeUnavailable(node)

		storageFree := map[string]float64{}
		storageTotal := map[string]float64{}
		if len(g.cfg.TargetStorages) > 0 {
			for _, resource := range storageResources {
				if resource.Type != "storage" {
					continue
				}
				if resource.Node != "" && resource.Node != node {
					continue
				}
				if !containsString(g.cfg.TargetStorages, resource.Storage) {
					continue
				}
				nodes, ok := storageNodes[resource.Storage]
				if !ok || !storageAllowsNode(nodes, node) {
					continue
				}
				if templateChoice.Resource.Node != node {
					if !g.storageShared[resource.Storage] {
						continue
					}
					if !storageAllowsNode(nodes, templateChoice.Resource.Node) {
						continue
					}
				}
				free := float64(resource.MaxDisk-resource.Disk) / 1024.0 / 1024.0 / 1024.0
				total := float64(resource.MaxDisk) / 1024.0 / 1024.0 / 1024.0
				if current, exists := storageFree[resource.Storage]; !exists || free > current {
					storageFree[resource.Storage] = free
				}
				if current, exists := storageTotal[resource.Storage]; !exists || total > current {
					storageTotal[resource.Storage] = total
				}
			}
			if len(storageFree) == 0 {
				skipped = append(skipped, fmt.Sprintf("%s: no eligible target storage from allowlist %s", node, strings.Join(g.cfg.TargetStorages, ",")))
				continue
			}
		}

		totalCPU := float64(status.CPUInfo.CPUs)
		totalMemoryMB := float64(status.Memory.Total) / 1024.0 / 1024.0
		policy := g.nodePolicy(node)
		nodes = append(nodes, nodePlanState{
			Name:                    node,
			TemplateNode:            templateChoice.Resource.Node,
			TemplateVMID:            templateChoice.Resource.VMID,
			TemplateStorage:         templateChoice.Storage,
			TotalMemoryMB:           totalMemoryMB,
			FreeMemoryMB:            float64(status.Memory.Total-status.Memory.Used) / 1024.0 / 1024.0,
			MemoryAllocationLimitMB: allocationLimit(totalMemoryMB, policy.MemoryAllocationLimitPercent),
			TotalCPUCores:           totalCPU,
			FreeCPUCores:            totalCPU - (status.CPU * totalCPU),
			CPUAllocationLimitCores: allocationLimit(totalCPU, policy.CPUAllocationLimitPercent),
			Reserve:                 policy.Reserve,
			StorageTotalGB:          storageTotal,
			StorageFreeGB:           storageFree,
		})
	}
	return nodes, skipped, nil
}

func (g *Group) buildCandidateNodes(states []nodePlanState) ([]scheduler.Node, []string) {
	nodes, skipped, _ := g.buildCandidateNodesForVMID(states, 0, time.Now())
	return nodes, skipped
}

func (g *Group) buildCandidateNodesForVMID(states []nodePlanState, vmid int, now time.Time) ([]scheduler.Node, []string, bool) {
	nodes := make([]scheduler.Node, 0, len(states))
	skipped := make([]string, 0)
	quarantineSkipped := false
	for _, state := range states {
		targetStorage := ""
		freeDiskGB := 0.0
		if len(g.cfg.TargetStorages) > 0 {
			bestFree := -1.0
			for storage, free := range state.StorageFreeGB {
				key := g.clonePlacementKey(provisionPlan{
					TemplateNode:    state.TemplateNode,
					TemplateStorage: state.TemplateStorage,
					Node:            state.Name,
					TargetStorage:   storage,
					VMID:            vmid,
				})
				if until, quarantined := g.clonePlacementQuarantinedLocked(key, now); quarantined {
					quarantineSkipped = true
					skipped = append(skipped, fmt.Sprintf("%s: VMID %d on storage %s quarantined until %s", state.Name, vmid, key.Storage, until.Format(time.RFC3339)))
					continue
				}
				if free > bestFree {
					bestFree = free
					targetStorage = storage
					freeDiskGB = free
				}
			}
			if targetStorage == "" {
				skipped = append(skipped, fmt.Sprintf("%s: no eligible target storage from allowlist %s", state.Name, strings.Join(g.cfg.TargetStorages, ",")))
				continue
			}
		} else {
			key := g.clonePlacementKey(provisionPlan{
				TemplateNode:    state.TemplateNode,
				TemplateStorage: state.TemplateStorage,
				Node:            state.Name,
				VMID:            vmid,
			})
			if until, quarantined := g.clonePlacementQuarantinedLocked(key, now); quarantined {
				quarantineSkipped = true
				skipped = append(skipped, fmt.Sprintf("%s: VMID %d on default storage quarantined until %s", state.Name, vmid, until.Format(time.RFC3339)))
				continue
			}
		}

		node := state.schedulerNode(targetStorage)
		node.FreeDiskGB = freeDiskGB
		nodes = append(nodes, node)
	}
	return nodes, skipped, quarantineSkipped
}

func (g *Group) clonePlacementQuarantinedLocked(key clonePlacementKey, now time.Time) (time.Time, bool) {
	until, ok := g.cloneQuarantine[key]
	if !ok {
		return time.Time{}, false
	}
	if !now.Before(until) {
		delete(g.cloneQuarantine, key)
		return time.Time{}, false
	}
	return until, true
}

func (g *Group) pruneCloneQuarantineLocked(now time.Time) {
	for key, until := range g.cloneQuarantine {
		if !now.Before(until) {
			delete(g.cloneQuarantine, key)
		}
	}
}

func (g *Group) clonePlacementKey(plan provisionPlan) clonePlacementKey {
	storage := plan.TargetStorage
	if g.effectiveCloneMode(plan) == "linked" {
		storage = plan.TemplateStorage
	}
	return clonePlacementKey{Node: plan.Node, Storage: storage, VMID: plan.VMID}
}

func (g *Group) quarantineClonePlacement(plan provisionPlan, err error) {
	if !isCloneVolumeCollision(err, plan.VMID) {
		return
	}

	key := g.clonePlacementKey(plan)
	until := time.Now().Add(cloneCollisionQuarantineTTL)

	g.mu.Lock()
	if g.cloneQuarantine == nil {
		g.cloneQuarantine = map[clonePlacementKey]time.Time{}
	}
	g.cloneQuarantine[key] = until
	g.mu.Unlock()

	g.log.Warn(
		"quarantining clone placement after storage collision",
		"operation_id", plan.OperationID,
		"operation", "increase",
		"phase", "clone",
		"node", plan.Node,
		"storage", key.Storage,
		"vmid", plan.VMID,
		"until", until,
	)
}

func (g *Group) clearClonePlacementQuarantine(plan provisionPlan) {
	key := g.clonePlacementKey(plan)
	g.mu.Lock()
	delete(g.cloneQuarantine, key)
	g.mu.Unlock()
}

func isCloneVolumeCollision(err error, vmid int) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "already exists") {
		return false
	}
	if !strings.Contains(message, "disk image") && !strings.Contains(message, "volume") {
		return false
	}
	return strings.Contains(message, fmt.Sprintf("vm-%d-", vmid))
}

func stateTemplateStorage(states []nodePlanState, node string) string {
	for _, state := range states {
		if state.Name == node {
			return state.TemplateStorage
		}
	}
	return ""
}

func allocationLimit(total float64, percent int) float64 {
	if percent <= 0 || total <= 0 {
		return 0
	}
	return total * float64(percent) / 100.0
}

func (g *Group) nodePolicy(node string) scheduler.NodePolicy {
	if policy, ok := g.cfg.NodePolicies[node]; ok {
		return policy
	}
	return scheduler.NodePolicy{
		Reserve:                      g.cfg.Reserve,
		MemoryAllocationLimitPercent: g.cfg.MemoryAllocationLimitPercent,
		CPUAllocationLimitPercent:    g.cfg.CPUAllocationLimitPercent,
	}
}

func (s nodePlanState) schedulerNode(targetStorage string) scheduler.Node {
	return scheduler.Node{
		Name:                    s.Name,
		TemplateNode:            s.TemplateNode,
		TemplateVMID:            s.TemplateVMID,
		TargetStorage:           targetStorage,
		TotalMemoryMB:           s.TotalMemoryMB,
		FreeMemoryMB:            s.FreeMemoryMB,
		AllocatedMemoryMB:       s.AllocatedMemoryMB,
		MemoryAllocationLimitMB: s.MemoryAllocationLimitMB,
		TotalDiskGB:             s.StorageTotalGB[targetStorage],
		FreeDiskGB:              s.StorageFreeGB[targetStorage],
		TotalCPUCores:           s.TotalCPUCores,
		FreeCPUCores:            s.FreeCPUCores,
		AllocatedCPUCores:       s.AllocatedCPUCores,
		CPUAllocationLimitCores: s.CPUAllocationLimitCores,
		Reserve:                 s.Reserve,
		ReserveSet:              true,
	}
}

func (s nodePlanState) resourceTotals() scheduler.Node {
	return scheduler.Node{
		Name:          s.Name,
		TotalMemoryMB: s.TotalMemoryMB,
		TotalCPUCores: s.TotalCPUCores,
	}
}

func physicalAllocationFree(total, allocated float64) float64 {
	free := total - allocated
	if free < 0 {
		return 0
	}
	return free
}

func (g *Group) diagnosticNodes(states []nodePlanState) []scheduler.Node {
	nodes := make([]scheduler.Node, 0, len(states))
	for _, state := range states {
		targetStorage := ""
		freeDiskGB := 0.0
		for storage, free := range state.StorageFreeGB {
			if targetStorage == "" || free > freeDiskGB {
				targetStorage = storage
				freeDiskGB = free
			}
		}
		node := state.schedulerNode(targetStorage)
		node.FreeDiskGB = freeDiskGB
		nodes = append(nodes, node)
	}
	return nodes
}

func (g *Group) reservePlannedResources(states []nodePlanState, node scheduler.Node, req scheduler.Requirement) {
	for i := range states {
		if states[i].Name != node.Name {
			continue
		}
		states[i].FreeMemoryMB -= req.MemoryMB
		states[i].AllocatedMemoryMB += req.MemoryMB
		states[i].FreeCPUCores -= req.CPUCores
		states[i].AllocatedCPUCores += req.CPUCores
		if node.TargetStorage != "" {
			states[i].StorageFreeGB[node.TargetStorage] -= req.DiskGB
		}
		return
	}
}

func (g *Group) applyAllocatedResources(states []nodePlanState, resources []proxmoxclient.ClusterResource) {
	stateByNode := make(map[string]*nodePlanState, len(states))
	for i := range states {
		stateByNode[states[i].Name] = &states[i]
	}

	for _, resource := range resources {
		if !shouldCountAllocatedResource(resource) {
			continue
		}
		if _, pending := g.pendingVMIDs[resource.VMID]; pending {
			continue
		}
		state, ok := stateByNode[resource.Node]
		if !ok {
			continue
		}
		state.AllocatedMemoryMB += float64(resource.MaxMem) / 1024.0 / 1024.0
		state.AllocatedCPUCores += resource.MaxCPU
	}
}

func shouldCountAllocatedResource(resource proxmoxclient.ClusterResource) bool {
	if resource.Type != "qemu" || resource.Template == 1 {
		return false
	}
	if resource.Status == "running" {
		return true
	}
	_, ok := parseTags(resource.Tags)[managedByTag]
	return ok
}

func (g *Group) applyPendingReservations(states []nodePlanState) {
	for i := range states {
		pending, ok := g.pendingByNode[states[i].Name]
		if !ok {
			continue
		}
		states[i].FreeMemoryMB -= pending.MemoryMB
		states[i].AllocatedMemoryMB += pending.MemoryMB
		states[i].FreeCPUCores -= pending.CPUCores
		states[i].AllocatedCPUCores += pending.CPUCores
		for storage, reserved := range pending.StorageGB {
			states[i].StorageFreeGB[storage] -= reserved
		}
	}
}

func (g *Group) registerPendingPlans(plans []provisionPlan) {
	for _, plan := range plans {
		pending := g.pendingByNode[plan.Node]
		pending.MemoryMB += plan.Requirement.MemoryMB
		pending.CPUCores += plan.Requirement.CPUCores
		if plan.TargetStorage != "" && plan.Requirement.DiskGB > 0 {
			if pending.StorageGB == nil {
				pending.StorageGB = map[string]float64{}
			}
			pending.StorageGB[plan.TargetStorage] += plan.Requirement.DiskGB
		}
		g.pendingByNode[plan.Node] = pending
		g.pendingVMIDs[plan.VMID] = struct{}{}
	}
}

func (g *Group) releasePendingPlan(plan provisionPlan) {
	g.mu.Lock()
	defer g.mu.Unlock()

	pending, ok := g.pendingByNode[plan.Node]
	if ok {
		pending.MemoryMB -= plan.Requirement.MemoryMB
		if pending.MemoryMB < 0 {
			pending.MemoryMB = 0
		}
		pending.CPUCores -= plan.Requirement.CPUCores
		if pending.CPUCores < 0 {
			pending.CPUCores = 0
		}
		if plan.TargetStorage != "" && pending.StorageGB != nil {
			pending.StorageGB[plan.TargetStorage] -= plan.Requirement.DiskGB
			if pending.StorageGB[plan.TargetStorage] <= 0 {
				delete(pending.StorageGB, plan.TargetStorage)
			}
		}
		if pending.MemoryMB == 0 && pending.CPUCores == 0 && len(pending.StorageGB) == 0 {
			delete(g.pendingByNode, plan.Node)
		} else {
			g.pendingByNode[plan.Node] = pending
		}
	}

	delete(g.pendingVMIDs, plan.VMID)
}

func (g *Group) acceptProvision(plan provisionPlan) {
	g.mu.Lock()
	defer g.mu.Unlock()

	id := instanceID(plan.Node, plan.VMID)
	g.accepted[id] = acceptedProvision{
		Node:  plan.Node,
		VMID:  plan.VMID,
		State: provider.StateCreating,
	}
}

func (g *Group) failAcceptedProvision(plan provisionPlan) {
	g.mu.Lock()
	defer g.mu.Unlock()

	id := instanceID(plan.Node, plan.VMID)
	accepted, ok := g.accepted[id]
	if !ok {
		accepted = acceptedProvision{
			Node:             plan.Node,
			VMID:             plan.VMID,
			ReportedCreating: true,
		}
	}
	accepted.State = provider.StateDeleted
	g.accepted[id] = accepted
}

func storageAllowsNode(nodes map[string]struct{}, node string) bool {
	if len(nodes) == 0 {
		return true
	}
	_, ok := nodes[node]
	return ok
}

func indexStorageNodes(resources []proxmoxclient.ClusterResource) map[string]map[string]struct{} {
	nodesByStorage := make(map[string]map[string]struct{})
	for _, resource := range resources {
		if resource.Type != "storage" || resource.Storage == "" || resource.Node == "" {
			continue
		}
		nodes := nodesByStorage[resource.Storage]
		if nodes == nil {
			nodes = map[string]struct{}{}
			nodesByStorage[resource.Storage] = nodes
		}
		nodes[resource.Node] = struct{}{}
	}
	return nodesByStorage
}

func indexSharedStorages(resources []proxmoxclient.ClusterResource) map[string]bool {
	sharedByStorage := make(map[string]bool)
	for _, resource := range resources {
		if resource.Type != "storage" || resource.Storage == "" {
			continue
		}
		if resource.Shared != 0 {
			sharedByStorage[resource.Storage] = true
			continue
		}
		if _, exists := sharedByStorage[resource.Storage]; !exists {
			sharedByStorage[resource.Storage] = false
		}
	}
	return sharedByStorage
}

func indexStoragePlugins(resources []proxmoxclient.ClusterResource) map[string]string {
	pluginsByStorage := make(map[string]string)
	for _, resource := range resources {
		if resource.Type != "storage" || resource.Storage == "" || resource.PluginType == "" {
			continue
		}
		if _, exists := pluginsByStorage[resource.Storage]; exists {
			continue
		}
		pluginsByStorage[resource.Storage] = resource.PluginType
	}
	return pluginsByStorage
}

func (g *Group) loadTemplateStorageNodes(ctx context.Context, templates []proxmoxclient.ClusterResource, storageResources []proxmoxclient.ClusterResource) (map[int]map[string]struct{}, error) {
	configs := make(map[int]map[string]struct{}, len(templates))
	nodesByStorage := indexStorageNodes(storageResources)
	for _, template := range templates {
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, fmt.Errorf("read template config %d: %w", template.VMID, err)
		}
		diskDevice, err := g.resolveDiskDevice(config)
		if err != nil {
			return nil, fmt.Errorf("resolve template disk device for %d: %w", template.VMID, err)
		}
		storageName, err := extractStorageName(config.DiskValue(diskDevice))
		if err != nil {
			return nil, fmt.Errorf("resolve template storage for %d: %w", template.VMID, err)
		}
		configs[template.VMID] = nodesByStorage[storageName]
	}
	return configs, nil
}

func (g *Group) resolveNodeTemplates(ctx context.Context, templates []proxmoxclient.ClusterResource, storageNodes map[int]map[string]struct{}, storageShared map[string]bool) (map[string]templateChoice, proxmoxclient.ClusterResource, string, error) {
	choices := make(map[string]templateChoice, len(g.cfg.Nodes))
	localTemplateByNode := map[string]templateChoice{}
	var sizingTemplate proxmoxclient.ClusterResource
	var diskDevice string
	sizingSet := false

	for _, template := range templates {
		config, err := g.client.GetVMConfig(ctx, template.Node, template.VMID)
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("read template config %d: %w", template.VMID, err)
		}
		resolvedDisk, err := g.resolveDiskDevice(config)
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("resolve template disk device for %d: %w", template.VMID, err)
		}
		if !sizingSet {
			sizingTemplate = template
			diskDevice = resolvedDisk
			sizingSet = true
		} else if template.MaxMem != sizingTemplate.MaxMem || template.MaxDisk != sizingTemplate.MaxDisk || template.MaxCPU != sizingTemplate.MaxCPU {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("all template_vmids must have identical memory/cpu/disk sizing")
		} else if resolvedDisk != diskDevice {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("all template_vmids must use the same boot disk device")
		}

		currentDiskMB, ok := diskSizeMB(config.DiskValue(resolvedDisk))
		if g.cfg.VMDiskMB > 0 && ok && g.cfg.VMDiskMB < currentDiskMB {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("vm_disk_mb=%d is smaller than template disk %s size %dMB", g.cfg.VMDiskMB, resolvedDisk, currentDiskMB)
		}

		storageName, err := extractStorageName(config.DiskValue(resolvedDisk))
		if err != nil {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("resolve template storage for %d: %w", template.VMID, err)
		}
		choice := templateChoice{
			Resource:      template,
			Storage:       storageName,
			StorageNodes:  storageNodes[template.VMID],
			StorageShared: storageShared[storageName],
		}

		if _, exists := localTemplateByNode[template.Node]; exists {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("multiple template_vmids resolve to node %s", template.Node)
		}
		localTemplateByNode[template.Node] = choice
	}

	for _, node := range g.cfg.Nodes {
		if localChoice, ok := localTemplateByNode[node]; ok {
			choices[node] = localChoice
			continue
		}

		found := false
		for _, template := range templates {
			choice := localTemplateByNode[template.Node]
			if !containsString(g.cfg.TargetStorages, choice.Storage) {
				continue
			}
			if !choice.StorageShared {
				continue
			}
			if !storageAllowsNode(choice.StorageNodes, node) {
				continue
			}
			choices[node] = choice
			found = true
			break
		}
		if !found {
			return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("node %s has no usable template: no local template and no shared template path via configured target_storages", node)
		}
	}

	if g.cfg.CloneMode == "linked" {
		for _, node := range g.cfg.Nodes {
			choice := choices[node]
			if choice.Resource.Node != node {
				return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("clone_mode=linked requires a local template on every configured node; node %s resolves to template on %s", node, choice.Resource.Node)
			}
			if len(g.cfg.TargetStorages) > 0 && !containsString(g.cfg.TargetStorages, choice.Storage) {
				return nil, proxmoxclient.ClusterResource{}, "", fmt.Errorf("clone_mode=linked requires template storage %s for node %s to be included in target_storages", choice.Storage, node)
			}
		}
	}

	return choices, sizingTemplate, diskDevice, nil
}

func extractStorageName(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("disk value is empty")
	}
	storage, _, ok := strings.Cut(value, ":")
	if !ok || storage == "" {
		return "", fmt.Errorf("unable to parse storage from %q", value)
	}
	return storage, nil
}

func (g *Group) logEffectiveTopology() {
	nodes := append([]string(nil), g.cfg.Nodes...)
	for _, node := range nodes {
		choice, ok := g.templatesByNode[node]
		if !ok {
			continue
		}

		cloneMode := g.cfg.CloneMode
		targetStorage := "<proxmox-default>"
		if len(g.cfg.TargetStorages) > 0 {
			eligible := g.eligibleTargetStorages(node, choice)
			if len(eligible) > 0 {
				targetStorage = strings.Join(eligible, ",")
			} else {
				targetStorage = "<none>"
			}
		} else if g.cfg.CloneMode == "auto" && choice.Resource.Node == node {
			targetStorage = choice.Storage
		}

		g.log.Info(
			"validated node topology",
			"phase", "init",
			"node", node,
			"template_vmid", choice.Resource.VMID,
			"template_node", choice.Resource.Node,
			"template_storage", choice.Storage,
			"clone_mode_effective", cloneMode,
			"target_storage_effective", targetStorage,
		)
	}
}

func (g *Group) eligibleTargetStorages(node string, choice templateChoice) []string {
	eligible := make([]string, 0, len(g.cfg.TargetStorages))
	for _, storage := range g.cfg.TargetStorages {
		nodes := g.storageNodes[storage]
		if !storageAllowsNode(nodes, node) {
			continue
		}
		if choice.Resource.Node != node {
			if !g.storageShared[storage] {
				continue
			}
			if !storageAllowsNode(nodes, choice.Resource.Node) {
				continue
			}
		}
		eligible = append(eligible, storage)
	}
	return eligible
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func (g *Group) provisionWorkerCount() int {
	workers := g.cloneLimiter.Capacity()
	if startWorkers := g.startLimiter.Capacity(); startWorkers > workers {
		workers = startWorkers
	}
	if workers <= 0 {
		return 1
	}
	return workers
}

func (g *Group) requiredMemoryMB() float64 {
	if g.cfg.VMMemoryMB > 0 {
		return float64(g.cfg.VMMemoryMB)
	}
	return float64(g.templateSizing.MaxMem) / 1024.0 / 1024.0
}

func (g *Group) requiredDiskGB() float64 {
	if g.cfg.VMDiskMB > 0 {
		return float64(g.cfg.VMDiskMB) / 1024.0
	}
	return float64(g.templateSizing.MaxDisk) / 1024.0 / 1024.0 / 1024.0
}

func (g *Group) requiredCPUCores() float64 {
	if g.cfg.VMCPUCores > 0 {
		return float64(g.cfg.VMCPUCores)
	}
	return g.templateSizing.MaxCPU
}

func (g *Group) resolveDiskDevice(config proxmoxclient.VMConfig) (string, error) {
	if g.cfg.VMDiskDevice != "" {
		value := config.DiskValue(g.cfg.VMDiskDevice)
		if value == "" {
			return "", fmt.Errorf("vm_disk_device %q not present in template config", g.cfg.VMDiskDevice)
		}
		if !isResizableDiskValue(value) {
			return "", fmt.Errorf("vm_disk_device %q is not a resizable disk device: %s", g.cfg.VMDiskDevice, value)
		}
		return g.cfg.VMDiskDevice, nil
	}
	if config.BootDisk != "" && isResizableDiskValue(config.DiskValue(config.BootDisk)) {
		return config.BootDisk, nil
	}
	for _, device := range config.DiskDeviceNames() {
		if isResizableDiskValue(config.DiskValue(device)) {
			return device, nil
		}
	}
	return "", fmt.Errorf("unable to determine template disk device; set vm_disk_device explicitly")
}

func isResizableDiskValue(value string) bool {
	value = strings.ToLower(value)
	if strings.TrimSpace(value) == "" {
		return false
	}
	if strings.Contains(value, "media=cdrom") {
		return false
	}
	if strings.Contains(value, "cloudinit") {
		return false
	}
	return true
}

func diskSizeMB(value string) (int64, bool) {
	match := diskSizePattern.FindStringSubmatch(value)
	if len(match) != 3 {
		return 0, false
	}

	size, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}

	switch match[2] {
	case "K":
		return int64(size / 1024.0), true
	case "M":
		return int64(size), true
	case "G":
		return int64(size * 1024.0), true
	case "T":
		return int64(size * 1024.0 * 1024.0), true
	default:
		return 0, false
	}
}

func parseInstanceID(value string) (string, int, error) {
	node, vmidValue, ok := strings.Cut(value, "/")
	if !ok || node == "" || vmidValue == "" {
		return "", 0, fmt.Errorf("invalid instance ID %q", value)
	}

	vmid, err := strconv.Atoi(vmidValue)
	if err != nil {
		return "", 0, fmt.Errorf("invalid instance ID %q", value)
	}

	return node, vmid, nil
}

func (g *Group) findTemplates(resources []proxmoxclient.ClusterResource) ([]proxmoxclient.ClusterResource, error) {
	byVMID := make(map[int]proxmoxclient.ClusterResource, len(resources))
	for _, resource := range resources {
		if resource.Type != "qemu" {
			continue
		}
		byVMID[resource.VMID] = resource
	}

	templates := make([]proxmoxclient.ClusterResource, 0, len(g.cfg.TemplateVMIDs))
	for _, vmid := range g.cfg.TemplateVMIDs {
		resource, ok := byVMID[vmid]
		if !ok {
			return nil, fmt.Errorf("template_vmids contains VMID %d which was not found", vmid)
		}
		if resource.Template != 1 {
			return nil, fmt.Errorf("template_vmid %d is not a template", vmid)
		}
		templates = append(templates, resource)
	}
	return templates, nil
}

func (g *Group) availableVMIDs(resources []proxmoxclient.ClusterResource, limit int) []int {
	used := map[int]struct{}{}
	for _, resource := range resources {
		if resource.VMID > 0 {
			used[resource.VMID] = struct{}{}
		}
	}
	for vmid := range g.pendingVMIDs {
		used[vmid] = struct{}{}
	}
	vmids := make([]int, 0, limit)
	for vmid := g.cfg.VMIDMin; vmid <= g.cfg.VMIDMax; vmid++ {
		if _, exists := used[vmid]; exists {
			continue
		}
		vmids = append(vmids, vmid)
		if len(vmids) == limit {
			break
		}
	}
	return vmids
}

func (g *Group) isManaged(resource proxmoxclient.ClusterResource) bool {
	if resource.Type != "qemu" || resource.Template == 1 {
		return false
	}
	if resource.Pool != g.cfg.Pool {
		return false
	}
	if resource.VMID < g.cfg.VMIDMin || resource.VMID > g.cfg.VMIDMax {
		return false
	}

	tagSet := parseTags(resource.Tags)
	for _, tag := range g.cfg.MandatoryTags {
		if _, ok := tagSet[tag]; !ok {
			return false
		}
	}

	return true
}

func (g *Group) isManagedTemplate(resource proxmoxclient.ClusterResource) bool {
	if resource.Type != "qemu" {
		return false
	}
	if resource.Pool != g.cfg.Pool {
		return false
	}
	return resource.Template == 1 && g.isManagedTemplateIdentity(resource.VMID, resource.Name, resource.Tags)
}

func (g *Group) isPotentialManagedTemplateArtifact(resource proxmoxclient.ClusterResource) bool {
	if resource.Type != "qemu" {
		return false
	}
	if resource.Pool != g.cfg.Pool {
		return false
	}
	if !g.hasManagedTemplateVMIDAndName(resource.VMID, resource.Name) {
		return false
	}
	_, ok := parseManagedTemplateSourceVMID(resource.Name)
	return ok
}

func (g *Group) isManagedTemplateConfig(vmid int, config proxmoxclient.VMConfig) bool {
	return g.isManagedTemplateIdentity(vmid, config.Name, config.Tags)
}

func isMissingManagedTemplateAfterDelete(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "Configuration file") && strings.Contains(message, "does not exist")
}

func (g *Group) isManagedTemplateIdentity(vmid int, name, tags string) bool {
	if !g.hasManagedTemplateVMIDAndName(vmid, name) {
		return false
	}

	tagSet := parseTags(tags)
	for _, tag := range g.cfg.ManagedTemplateTags {
		if _, ok := tagSet[tag]; !ok {
			return false
		}
	}

	return true
}

func (g *Group) hasManagedTemplateVMIDAndName(vmid int, name string) bool {
	if vmid < g.cfg.TemplateVMIDMin || vmid > g.cfg.TemplateVMIDMax {
		return false
	}
	return strings.HasPrefix(name, g.cfg.TemplateNamePrefix+"-")
}

func managedTemplateKey(node string, sourceVMID int) string {
	return fmt.Sprintf("%s/%d", node, sourceVMID)
}

func parseManagedTemplateSourceVMID(name string) (int, bool) {
	if !strings.Contains(name, "-") {
		return 0, false
	}
	last := name[strings.LastIndex(name, "-")+1:]
	vmid, err := strconv.Atoi(last)
	if err != nil {
		return 0, false
	}
	return vmid, true
}

func descriptionValue(description, key string) string {
	prefix := key + "="
	for _, line := range strings.Split(description, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

func stagedTemplateDescription(node string, sourceVMID int, sourceVersion string) string {
	description := fmt.Sprintf("Managed staged template for node %s from source template %d", node, sourceVMID)
	if sourceVersion == "" {
		return description
	}
	return description + "\n" + stagedTemplateVersionKey + "=" + sourceVersion
}

func shouldReuseManagedTemplate(sourceVersion, stagedVersion string) bool {
	return sourceVersion == "" || sourceVersion == stagedVersion
}

func (g *Group) renderDescription(node string, vmid int, ip netip.Addr) (string, error) {
	ipValue := "dhcp"
	if ip.IsValid() {
		ipValue = ip.String()
	}

	if g.cfg.DescriptionTemplate == "" {
		return fmt.Sprintf("Managed by %s; vmid=%d; node=%s; ip=%s", "fleeting-plugin-proxmox", vmid, node, ipValue), nil
	}

	tmpl, err := template.New("description").Parse(g.cfg.DescriptionTemplate)
	if err != nil {
		return "", err
	}

	data := map[string]any{
		"Node": node,
		"VMID": vmid,
		"IP":   ipValue,
		"Pool": g.cfg.Pool,
	}

	var builder strings.Builder
	if err := tmpl.Execute(&builder, data); err != nil {
		return "", err
	}
	return builder.String(), nil
}

func (g *Group) setTransient(id string, state provider.State) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.transient[id] = state
}

func (g *Group) clearTransient(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.transient, id)
}

func instanceID(node string, vmid int) string {
	return fmt.Sprintf("%s/%d", node, vmid)
}

func plannedIDs(plans []provisionPlan) []string {
	ids := make([]string, 0, len(plans))
	for _, plan := range plans {
		ids = append(ids, instanceID(plan.Node, plan.VMID))
	}
	return ids
}

func (g *Group) nextOperationID(prefix string) string {
	return fmt.Sprintf("%s-%x-%x", prefix, time.Now().UnixMilli(), g.operationSeq.Add(1))
}

func humanDuration(started time.Time) string {
	return formatDuration(time.Since(started))
}

func formatDuration(duration time.Duration) string {
	return duration.Round(time.Millisecond).String()
}

func (g *Group) reportPlanProblem(plan provisionPlan, code string, state metrics.ProblemState, phase string, err error, activeUntil time.Time) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	event := metrics.ProblemEvent{
		Code:        code,
		State:       state,
		Severity:    "error",
		Phase:       phase,
		Node:        plan.Node,
		Storage:     plan.TargetStorage,
		Instance:    instanceID(plan.Node, plan.VMID),
		OperationID: plan.OperationID,
		Message:     err.Error(),
	}
	if phase == "clone" {
		event.Storage = g.clonePlacementKey(plan).Storage
	}
	if !activeUntil.IsZero() {
		event.ActiveUntilUnix = activeUntil.Unix()
	}
	g.reportProblem(event)
}

func (g *Group) reportProblem(event metrics.ProblemEvent) {
	if g.problems == nil {
		return
	}
	g.problems.ReportProblem(event)
}

func (g *Group) markNodeUnavailable(node string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	g.mu.Lock()
	if g.unavailableNodes == nil {
		g.unavailableNodes = map[string]bool{}
	}
	g.unavailableNodes[node] = true
	g.mu.Unlock()
	g.reportProblem(metrics.ProblemEvent{
		Code:     "node_unavailable",
		State:    metrics.ProblemActive,
		Severity: "error",
		Phase:    "placement",
		Node:     node,
		Message:  err.Error(),
	})
}

func (g *Group) clearNodeUnavailable(node string) {
	g.mu.Lock()
	wasUnavailable := g.unavailableNodes[node]
	delete(g.unavailableNodes, node)
	g.mu.Unlock()
	if !wasUnavailable {
		return
	}
	g.reportProblem(metrics.ProblemEvent{
		Code:  "node_unavailable",
		State: metrics.ProblemResolved,
		Phase: "placement",
		Node:  node,
	})
}

func mapState(status string) provider.State {
	switch status {
	case "running":
		return provider.StateRunning
	case "paused":
		return provider.StateSuspended
	case "stopped":
		return provider.StateCreating
	default:
		return provider.StateCreating
	}
}

func parseStaticIPv4(value string) (netip.Addr, bool) {
	parts := strings.Split(value, ",")
	for _, part := range parts {
		if !strings.HasPrefix(part, "ip=") {
			continue
		}
		value := strings.TrimPrefix(part, "ip=")
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return netip.Addr{}, false
		}
		if prefix.Addr().Is4() {
			return prefix.Addr(), true
		}
	}
	return netip.Addr{}, false
}

func parseTags(value string) map[string]struct{} {
	out := map[string]struct{}{}
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ';' || r == ',' || r == ' '
	})
	for _, field := range fields {
		if field == "" {
			continue
		}
		out[field] = struct{}{}
	}
	return out
}
