package proxmox

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"golang.org/x/crypto/ssh"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/instancegroup"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/ippool"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/limiter"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/proxmoxclient"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/scheduler"
	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/state"
)

var _ provider.InstanceGroup = (*InstanceGroup)(nil)

type InstanceGroup struct {
	pluginConfig

	log      hclog.Logger
	settings provider.Settings

	client          *proxmoxclient.Client
	group           *instancegroup.Group
	metricsReporter *metrics.Reporter
	sshPub          string
}

func (g *InstanceGroup) Init(ctx context.Context, log hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	groupID := path.Join("proxmox", g.ClusterName, g.Pool, g.NamePrefix)
	g.log = log.With("group", groupID, "cluster", g.ClusterName, "pool", g.Pool)
	g.settings = settings

	if err := g.config().validate(g.settings); err != nil {
		g.configureMetricsReporter(metrics.DefaultReporterInterval)
		return g.failInit(err)
	}
	g.configureMetricsReporter(g.parsedMetricsInterval)

	if g.settings.Protocol == "" {
		g.settings.Protocol = provider.ProtocolSSH
	}
	if g.settings.Username == "" && g.CIUser != "" {
		g.settings.Username = g.CIUser
	}

	var publicKey string
	var ciPassword string
	var err error

	if isWinRM(g.settings.Protocol) {
		ciPassword, err = g.prepareWinRMCredentials()
		if err != nil {
			return g.failInit(err)
		}
	} else {
		publicKey, err = g.prepareSSHCredentials()
		if err != nil {
			return provider.ProviderInfo{}, err
		}
		if g.settings.OS == "windows" {
			ciPassword, err = generatePassword()
			if err != nil {
				return g.failInit(err)
			}
		}
	}

	g.client, err = proxmoxclient.New(proxmoxclient.Config{
		BaseURL:            g.APIURL,
		TokenID:            g.TokenID,
		TokenSecret:        g.TokenSecret,
		TLSCAFile:          g.TLSCAFile,
		InsecureSkipVerify: g.TLSInsecureSkipVerify,
		AllowedServerNames: []string(g.Nodes),
	})
	if err != nil {
		return g.failInit(err)
	}

	var pool *ippool.Pool
	if g.NetworkMode == "static" {
		store := state.NewFileStore(g.StateFile)
		pool, err = ippool.New(ippool.Config{
			Prefix:        g.parsedPoolPrefix,
			Gateway:       g.parsedGateway,
			Ranges:        g.IPPoolRanges,
			Exclude:       g.IPPoolExclude,
			ReuseCooldown: g.parsedIPReuseCooldown,
		}, store)
		if err != nil {
			return g.failInit(err)
		}
	}

	var problemReporter metrics.ProblemReporter
	if g.metricsReporter != nil {
		problemReporter = g.metricsReporter
	}

	g.group = instancegroup.New(
		g.client,
		g.log,
		instancegroup.Config{
			ClusterName:                  g.ClusterName,
			Pool:                         g.Pool,
			TemplateVMIDs:                g.TemplateVMIDs,
			TemplateStageMode:            g.TemplateStageMode,
			TemplateVMIDMin:              g.parsedTemplateVMIDRange.Min,
			TemplateVMIDMax:              g.parsedTemplateVMIDRange.Max,
			TemplateNamePrefix:           g.TemplateNamePrefix,
			VMIDMin:                      g.parsedVMIDRange.Min,
			VMIDMax:                      g.parsedVMIDRange.Max,
			NamePrefix:                   g.NamePrefix,
			Nodes:                        g.Nodes,
			CloneMode:                    g.CloneMode,
			TargetStorages:               g.TargetStorages,
			CloneSnapshot:                g.CloneSnapshot,
			VMMemoryMB:                   g.VMMemoryMB,
			VMCPUCores:                   g.VMCPUCores,
			VMDiskMB:                     g.VMDiskMB,
			VMDiskDevice:                 g.VMDiskDevice,
			MandatoryTags:                g.mandatoryTags(),
			ManagedTemplateTags:          g.managedTemplateTags(),
			DescriptionTemplate:          g.DescriptionTemplate,
			CloudInitInterface:           "ipconfig0",
			NetworkMode:                  g.NetworkMode,
			CIUser:                       g.CIUser,
			NameServers:                  g.NameServers,
			SearchDomain:                 g.SearchDomain,
			TaskPollInterval:             g.parsedTaskPoll,
			CloneTimeout:                 g.parsedCloneTimeout,
			StartTimeout:                 g.parsedStartTimeout,
			ShutdownTimeout:              g.parsedShutdownTimeout,
			AgentTimeout:                 g.parsedAgentTimeout,
			AgentRequired:                g.AgentRequired,
			GeneratedSSHPublicKey:        publicKey,
			GeneratedPassword:            ciPassword,
			StaticSSHPublicKeys:          g.CISSHKeys,
			OS:                           g.settings.OS,
			Scheduler:                    scheduler.New(g.Scheduler),
			MemoryAllocationLimitPercent: g.NodeMemoryAllocationLimitPercent,
			CPUAllocationLimitPercent:    g.NodeCPUAllocationLimitPercent,
			NodePolicies:                 g.resolveNodePolicies(),
			Reserve: scheduler.Reserve{
				MemoryMB:      g.NodeReserveMemoryMB,
				MemoryPercent: g.NodeReserveMemoryPercent,
				DiskGB:        g.NodeReserveDiskGB,
				DiskPercent:   g.NodeReserveDiskPercent,
				CPUCores:      g.NodeReserveCPUCores,
				CPUPercent:    g.NodeReserveCPUPercent,
			},
		},
		pool,
		limiter.New(g.MaxParallelClones),
		limiter.New(g.MaxParallelStarts),
		limiter.New(g.MaxParallelDeletes),
		problemReporter,
	)

	if err := g.group.Init(ctx); err != nil {
		return g.failInit(err)
	}
	if g.metricsReporter != nil {
		g.metricsReporter.ReportProblem(metrics.ProblemEvent{
			Code:  "init_failed",
			State: metrics.ProblemResolved,
			Phase: "init",
		})
		g.metricsReporter.Start()
	}

	return provider.ProviderInfo{
		ID:           groupID,
		MaxSize:      g.parsedVMIDRange.Max - g.parsedVMIDRange.Min + 1,
		Version:      Version.String(),
		BuildInfo:    Version.BuildInfo(),
		Capabilities: []provider.Capability{provider.CapabilitySuspendResume},
	}, nil
}

func (g *InstanceGroup) configureMetricsReporter(interval time.Duration) {
	if g.MetricsSocket == "" {
		return
	}
	g.metricsReporter = metrics.NewReporter(metrics.ReporterConfig{
		SocketPath: g.MetricsSocket,
		Interval:   interval,
		Identity: metrics.Identity{
			Cluster: g.ClusterName,
			Pool:    g.Pool,
			Group:   g.NamePrefix,
		},
		Collect: func(ctx context.Context) (metrics.Snapshot, error) {
			return g.group.MetricsSnapshot(ctx)
		},
		Info: g.log.Info,
		Warn: g.log.Warn,
	})
}

func (g *InstanceGroup) failInit(err error) (provider.ProviderInfo, error) {
	if g.metricsReporter != nil {
		reporter := g.metricsReporter
		reporter.ReportProblem(metrics.ProblemEvent{
			Code:     "init_failed",
			State:    metrics.ProblemActive,
			Severity: "error",
			Phase:    "init",
			Message:  err.Error(),
		})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		reporter.FlushProblems(ctx, false)
		cancel()
		g.metricsReporter = nil
	}
	return provider.ProviderInfo{}, err
}

func (g *InstanceGroup) reportRuntimeProblem(code, phase, instance string, err error) {
	if g.metricsReporter == nil || err == nil || errors.Is(err, context.Canceled) {
		return
	}
	g.metricsReporter.ReportProblem(metrics.ProblemEvent{
		Code:     code,
		State:    metrics.ProblemRecent,
		Severity: "error",
		Phase:    phase,
		Instance: instance,
		Message:  err.Error(),
	})
}

func (g *InstanceGroup) Update(ctx context.Context, update func(instance string, state provider.State)) error {
	instances, err := g.group.List(ctx)
	if err != nil {
		g.reportRuntimeProblem("reconcile_failed", "reconcile", "", err)
		return err
	}
	for _, instance := range instances {
		update(instance.ID, instance.State)
	}
	return nil
}

func (g *InstanceGroup) Increase(ctx context.Context, delta int) (int, error) {
	if delta <= 0 {
		return 0, nil
	}
	created, err := g.group.Increase(ctx, delta)
	return len(created), err
}

func (g *InstanceGroup) Decrease(ctx context.Context, instances []string) ([]string, error) {
	return g.group.Decrease(ctx, instances)
}

func (g *InstanceGroup) ConnectInfo(ctx context.Context, instance string) (provider.ConnectInfo, error) {
	info, err := g.group.ConnectInfo(ctx, instance, g.settings)
	if err != nil {
		g.reportRuntimeProblem("connect_info_failed", "connect", instance, err)
		return provider.ConnectInfo{}, err
	}
	info.Key = g.settings.Key
	info.Password = g.settings.Password
	return info, nil
}

func (g *InstanceGroup) Heartbeat(ctx context.Context, instance string) error {
	err := g.group.Heartbeat(ctx, instance)
	if err != nil {
		g.reportRuntimeProblem("heartbeat_failed", "heartbeat", instance, err)
	}
	return err
}

func (g *InstanceGroup) Suspend(ctx context.Context, instances []string) ([]string, error) {
	return g.group.Suspend(ctx, instances)
}

func (g *InstanceGroup) Resume(ctx context.Context, instances []string) ([]string, error) {
	return g.group.Resume(ctx, instances)
}

func (g *InstanceGroup) Shutdown(ctx context.Context) error {
	var groupErr error
	if g.group != nil {
		groupErr = g.group.Shutdown(ctx)
	}
	if g.metricsReporter != nil {
		g.metricsReporter.Shutdown(ctx)
		g.metricsReporter = nil
	}
	return groupErr
}

func (g *InstanceGroup) prepareSSHCredentials() (string, error) {
	if g.settings.UseStaticCredentials && len(g.settings.Key) > 0 {
		pub, err := publicKeyFromPrivate(g.settings.Key)
		if err != nil {
			return "", err
		}
		g.sshPub = pub
		return pub, nil
	}

	if len(g.settings.Key) > 0 {
		pub, err := publicKeyFromPrivate(g.settings.Key)
		if err != nil {
			return "", err
		}
		g.sshPub = pub
		return pub, nil
	}

	pub, priv, err := generateSSHKeyPair()
	if err != nil {
		return "", err
	}
	g.settings.Key = priv
	g.sshPub = pub
	return pub, nil
}

func generateSSHKeyPair() (string, []byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", nil, err
	}

	sshPub, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return "", nil, err
	}

	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return "", nil, err
	}

	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	return ensureSSHKeyComment(string(ssh.MarshalAuthorizedKey(sshPub))), privatePEM, nil
}

func publicKeyFromPrivate(privateKey []byte) (string, error) {
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return "", fmt.Errorf("parse private key: %w", err)
	}
	return ensureSSHKeyComment(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))), nil
}

func isWinRM(p provider.Protocol) bool {
	return p == provider.ProtocolWinRM || p == provider.ProtocolWinRMHttps
}

func (g *InstanceGroup) prepareWinRMCredentials() (string, error) {
	password, err := generatePassword()
	if err != nil {
		return "", err
	}
	if g.settings.Password == "" {
		g.settings.Password = password
	}
	return password, nil
}

func generatePassword() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// 6 bytes → 8 base64 chars (mixed case + digits).
	// Suffix guarantees Windows password complexity (special, digit, lower, upper).
	pass := base64.RawStdEncoding.EncodeToString(b) + ".0aZ"
	return pass, nil
}

func ensureSSHKeyComment(publicKey string) string {
	publicKey = strings.TrimSpace(publicKey)
	if publicKey == "" {
		return publicKey
	}

	fields := strings.Fields(publicKey)
	if len(fields) < 2 {
		return publicKey
	}
	if len(fields) >= 3 {
		return publicKey + "\n"
	}
	return publicKey + " gitlab-runner@fleeting-proxmox\n"
}
