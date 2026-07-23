# Fleeting Plugin Proxmox

A GitLab Fleeting plugin for Proxmox VE.

## Overview

This plugin provisions ephemeral Proxmox VMs for GitLab Runner Fleeting and has been exercised with both `instance` and `docker-autoscaler`.

- QEMU only
- Linux guests only
- SSH connector only
- Cloud-Init networking in either `static` or `dhcp` mode
- dedicated Proxmox pool, dedicated VMID range, and mandatory management tags

Tested against Proxmox VE 8 and Proxmox VE 9.

### Template Placement

The plugin supports multi-node runner layouts where each node must clone runner VMs onto its own local storage.

When `template_stage_mode` is enabled, the plugin prepares managed per-node staged templates during `Init()` and clones runner VMs from them.

This works around the Proxmox VE limitation that, even with a template on shared storage, runner VMs still cannot be provisioned directly onto each node's own local storage in a multi-node layout.

## Build and Install

Use the repository `Makefile`:

```bash
make test
make build
```

This produces:

```text
dist/fleeting-plugin-proxmox
```

To remove build artifacts:

```bash
make clean
```

### Installing

1. Build the binary with `make build`.
2. Copy `dist/fleeting-plugin-proxmox` to the GitLab Runner host under the name `fleeting-plugin-proxmox`.
3. Make sure the binary is executable and discoverable via `$PATH`.
4. In the `[runners.autoscaler]` section of `config.toml`, set `plugin = "fleeting-plugin-proxmox"`.
5. Restart `gitlab-runner`.

## Configuration

Unless stated otherwise, the options below belong to `[runners.autoscaler.plugin_config]` in `config.toml`.

### Plugin configuration reference

#### API and identity

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `api_url` | string | yes |  | Proxmox API base URL, for example `https://pve.example.com:8006`. |
| `token_id` | string | yes |  | Proxmox API token ID. |
| `token_secret` | string | yes |  | Proxmox API token secret. |
| `tls_ca_file` | string | no |  | Optional CA bundle used to verify the Proxmox TLS certificate. |
| `tls_insecure_skip_verify` | bool | no | `false` | Development only. |
| `cluster_name` | string | no | `default` | Logical cluster identifier used in Fleeting provider IDs and default allocator state path. |
| `pool` | string | yes |  | Dedicated Proxmox pool for managed VMs. |
| `name_prefix` | string | yes |  | Prefix used for created VM names and mandatory management tags. |
| `vmid_range` | string | yes |  | Dedicated VMID range in `start-end` form. |
| `nodes` | string or list | yes |  | Placement allowlist. |

#### Templates and storage

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `template_vmids` | int list | yes |  | Source QEMU template VMIDs. The plugin prefers a template already local to the selected node. |
| `template_stage_mode` | enum | no | `auto` | One of `off`, `auto`, or `required`. The default enables managed staged templates automatically when needed. |
| `template_vmid_range` | string | required for `required`, or for `auto` if staging is needed |  | Dedicated VMID range in `start-end` form for managed staged templates. Must not overlap `vmid_range`. |
| `template_name_prefix` | string | no | `<name_prefix>-template` | Prefix used for managed staged template names. |
| `clone_mode` | enum | no | `auto` | One of `auto`, `linked`, or `full`. |
| `target_storages` | string or list | no |  | Datastore allowlist. |
| `clone_snapshot` | string | no |  | Optional snapshot name used when cloning from the template. |
| `vm_memory_mb` | int64 | no | template value | Optional memory override in MiB. |
| `vm_cpu_cores` | int | no | template value | Optional vCPU override. |
| `vm_disk_mb` | int64 | no | template value | Optional absolute primary disk size in MiB. Must not be smaller than the template disk. |
| `vm_disk_device` | string | no | autodetect | Optional explicit disk device for `vm_disk_mb`, for example `scsi0`. |

With `template_stage_mode` enabled, the plugin works around the Proxmox limitation that a configured source template cannot always be cloned directly for every allowed node. It does that by cloning the source template into a managed temporary template on the affected node during `Init()`, then using that staged template for subsequent VM provisioning on that node.

#### Placement and concurrency

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `scheduler` | enum | no | `balanced` | One of `balanced`, `most_free_ram`, `most_free_cpu`, or `round_robin`. |
| `node_reserve_memory_mb` | int64 | no | `0` | Reserved memory headroom on the node. |
| `node_reserve_memory_percent` | int | no | `0` | Reserved memory headroom on the node, as a percentage of total memory. Overrides `node_reserve_memory_mb` when set. |
| `node_reserve_cpu_cores` | int | no | `0` | Reserved CPU headroom on the node. |
| `node_reserve_cpu_percent` | int | no | `0` | Reserved CPU headroom on the node, as a percentage of total CPU capacity. Overrides `node_reserve_cpu_cores` when set. |
| `node_reserve_disk_gb` | int64 | no | `0` | Reserved free space on the target datastore. |
| `node_reserve_disk_percent` | int | no | `0` | Reserved free space on the target datastore, as a percentage of total capacity. Overrides `node_reserve_disk_gb` when set. |
| `node_memory_allocation_limit_percent` | int | no | `0` | Maximum committed VM memory on a node as a percentage of node memory. `0` disables the allocation limit. |
| `node_cpu_allocation_limit_percent` | int | no | `0` | Maximum committed VM vCPU on a node as a percentage of node CPU capacity. `0` disables the allocation limit. |
| `node_policies` | array | no |  | Per-node overrides for reserve and committed allocation settings. Each entry applies to one or more configured `nodes`. |
| `max_parallel_clones` | int | no | `2` | Maximum concurrent clone operations. |
| `max_parallel_starts` | int | no | `4` | Maximum concurrent start operations. |
| `max_parallel_deletes` | int | no | `2` | Maximum concurrent delete operations. |
| `task_poll_interval` | duration string | no | `2s` | Poll interval for Proxmox async task completion. |
| `clone_timeout` | duration string | no | `10m` | Timeout for clone operations. |
| `start_timeout` | duration string | no | `5m` | Timeout for VM start and readiness wait. |
| `shutdown_timeout` | duration string | no | `2m` | Timeout for stop-and-delete task completion during instance removal. |
| `api_retry_max_elapsed` | duration string | no | `30s` | Time budget for retrying an idempotent Proxmox API read (task polling, config/status/list) through transient network/DNS/5xx failures. `0` disables retries. Prevents a brief blip from being mistaken for a clone failure and from stranding a VM during rollback. |
| `metrics_socket` | path string | no |  | Unix socket used to publish metrics snapshots to a local `metrics-exporter` process. Empty disables metrics. |
| `metrics_interval` | duration string | no | `15s` when `metrics_socket` is set | Interval for publishing metrics snapshots. |

#### Cloud-Init and networking

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `network_mode` | enum | yes | `static` | One of `static` or `dhcp`. |
| `ci_user` | string | no |  | Optional Cloud-Init login user. Also becomes the default SSH username returned by the plugin. |
| `ci_ssh_keys` | string or list | no |  | Optional public SSH keys. |
| `nameserver` | string or list | no |  | Optional DNS servers. |
| `searchdomain` | string | no |  | Optional DNS search domain. |

#### Static IP allocator

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `ip_pool_network` | CIDR string | for `static` |  | IPv4 subnet used for static allocation. |
| `ip_pool_gateway` | IPv4 string | for `static` |  | IPv4 gateway used for static allocation. |
| `ip_pool_ranges` | range string or list | no | full subnet minus reserved addresses | Optional address ranges within `ip_pool_network`, for example `10.10.20.100-10.10.20.199`. |
| `ip_pool_exclude` | IPv4 string or list | no |  | Optional excluded addresses. |
| `ip_pool_reuse_cooldown` | duration string | no | `0s` | Cooldown before a released static IP can be reused. |
| `state_file` | path string | no | `/var/lib/fleeting-plugin-proxmox/<cluster>-<pool>-<name_prefix>-state.json` | Persistent allocator state file. |

When `network_mode = "static"`, the configured pool is treated as desired state for managed VMs. On startup, managed VMs with static addresses outside the current `ip_pool_network`/`ip_pool_ranges` set, after `ip_pool_exclude`, are stopped, deleted, and replaced through normal scaling.

#### Guest agent and metadata

| Option | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `agent_required` | bool | no | `true` | Mandatory for `network_mode = "dhcp"`. |
| `agent_timeout` | duration string | no | `3m` | Timeout for guest-agent IP discovery. |
| `prefer_ipv6` | bool | no | `false` | Not supported by the current implementation. |
| `tags` | string or list | no |  | Optional extra management tags. |
| `description_template` | string | no |  | Optional Go `text/template` using `.Node`, `.VMID`, `.IP`, `.Pool`. |

## Safety

The plugin only manages VMs that satisfy all of the following:

- QEMU VM
- member of the configured Proxmox pool
- within the configured VMID range
- carrying the mandatory plugin tags

This prevents accidental cleanup of unrelated workloads on the same hypervisor.
You still need to grant only the minimum required Proxmox privileges. The plugin is conservative about managed VM identity, but it cannot compensate for an over-privileged API token.

When `template_stage_mode` is enabled, the plugin also manages temporary staged templates. They are identified separately from runner VMs by:

- the configured Proxmox pool
- the dedicated `template_vmid_range`
- the configured `template_name_prefix`
- fixed internal tags for template staging

Managed staged templates are reused across restarts by default.

If you want the fleet to rebuild staged templates after changing a gold template, add or update a `template-version=<value>` line in that gold template's `description`. For example:

```text
# bump when replacing the template disk
template-version=2
```

When `template-version` is absent, existing staged templates are reused as-is. When it changes, the plugin rebuilds the affected staged templates from the updated gold template.

### Required RBAC groups

One practical way to split Proxmox privileges for this plugin is into three roles.

#### `GitlabFleetingNodes`

- assign on `/nodes/<allowed-node>`
- privileges:
  - `Sys.Audit`

#### `GitlabFleetingPool`

- assign on `/pool/<managed-pool>`
- privileges:
  - `Pool.Audit`
  - `Sys.Audit`
  - `Datastore.Audit`
  - `Datastore.AllocateSpace`
  - `VM.Audit`
  - `VM.Clone`
  - `VM.Migrate`
  - `VM.Allocate`
  - `VM.Config.CPU`
  - `VM.Config.Memory`
  - `VM.Config.Disk`
  - `VM.Config.Options`
  - `VM.Config.Cloudinit`
  - `VM.PowerMgmt`
  - `VM.Monitor`

#### `GitlabFleetingSDN`

- assign on `/sdn/zones/localnetwork/<bridge>`
- privileges:
  - `SDN.Use`

Notes:

- the practical model above assumes target storages and template VMs are members of the managed pool
- `VM.Monitor` is required on Proxmox VE 8 for guest-agent IP discovery
- `VM.GuestAgent.Audit` is required on Proxmox VE 9 for guest-agent IP discovery
- `Sys.Audit` is retained in the pool role for practical compatibility across PVE 8 and 9 deployments
- `SDN.Use` is required on the `localnetwork/<bridge>` path used by the template NIC
- the plugin does not configure the VM bridge; bridge and SDN attachment come from the template NIC
- if `template_stage_mode` is enabled, the same pool role also needs access to create, convert, and delete the managed staged templates inside the configured `template_vmid_range`

### Node reserve semantics

`node_reserve_memory_mb` / `node_reserve_memory_percent`, `node_reserve_cpu_cores` / `node_reserve_cpu_percent`, and `node_reserve_disk_gb` / `node_reserve_disk_percent` are admission filters based on the current free resources reported by Proxmox for a node.

They are evaluated as:

- current free memory minus the template VM memory must stay above `node_reserve_memory_mb`
- current free CPU headroom minus the template VM vCPU count must stay above `node_reserve_cpu_cores`
- current free disk minus the template VM disk size must stay above `node_reserve_disk_gb`
- when the corresponding `*_percent` field is set, the reserve is derived from total node or datastore capacity instead of the absolute field

For CPU specifically, the plugin does not use load average. It converts the current Proxmox CPU utilization fraction into free cores:

- `free_cpu_cores = total_cpus - (cpu_utilization * total_cpus)`

They do not create reservations in Proxmox and they do not use a separate reservation accounting model.

`node_memory_allocation_limit_percent` and `node_cpu_allocation_limit_percent` add a second admission filter based on committed VM sizing, not current utilization. When set, the plugin sums running non-template QEMU VMs visible to the API token on each configured node plus in-flight provisioning reservations, then rejects placements that would exceed the configured percentage of node memory or CPU capacity.

For example, on a 64-core node:

- `node_cpu_allocation_limit_percent = 100` allows up to 64 committed vCPU
- `node_cpu_allocation_limit_percent = 150` allows up to 96 committed vCPU
- `node_cpu_allocation_limit_percent = 0` disables the committed CPU filter

Per-node policies override any subset of these global placement settings for one or more nodes:

```toml
[[runners.autoscaler.plugin_config.node_policies]]
nodes = ["pve01", "pve02"]
reserve_memory_mb = 32768
reserve_memory_percent = 0
reserve_cpu_cores = 8
reserve_disk_gb = 100
memory_allocation_limit_percent = 100
cpu_allocation_limit_percent = 200
```

Unset fields inherit the global value. Explicit `0` disables that field for the listed nodes, which is useful when a global percentage reserve is set and a node policy should switch back to an absolute reserve.

The committed allocation limit is only an admission guard. The `most_free_ram`, `most_free_cpu`, and `balanced` scheduler strategies rank eligible nodes using current free resources capped by physical committed headroom, so a node with `cpu_allocation_limit_percent = 200` is allowed to accept more committed vCPU but is not treated as having more physical CPU than it actually has.

### Prometheus metrics

Multiple GitLab Runner autoscaler entries can start separate plugin processes on the same host. To avoid per-process port conflicts, the plugin publishes metrics snapshots to a local Unix socket and a single exporter process exposes one Prometheus endpoint.

Start the exporter with the same binary:

```shell
/usr/local/bin/fleeting-plugin-proxmox metrics-exporter \
  --listen 0.0.0.0:9252 \
  --socket /run/fleeting-plugin-proxmox/metrics.sock
```

Use `--listen 127.0.0.1:9252` when Prometheus or an agent scrapes locally.

Then enable reporting in each plugin config:

```toml
metrics_socket = "/run/fleeting-plugin-proxmox/metrics.sock"
metrics_interval = "15s"
```

The reporter does not fail plugin initialization when the socket is missing. It retries until the exporter appears, and reconnects on later exporter restarts.

An example systemd service is provided at [`examples/fleeting-plugin-proxmox-metrics-exporter.service`](/workspace/fleeting-plugin-proxmox/examples/fleeting-plugin-proxmox-metrics-exporter.service).

The exporter also aggregates operational problems from every plugin process using the same socket. A human-readable summary is written atomically to:

```text
/run/fleeting-plugin-proxmox/exceptions
```

Check it directly:

```shell
cat /run/fleeting-plugin-proxmox/exceptions
curl http://127.0.0.1:9252/problems
curl http://127.0.0.1:9252/problems.json
```

The summary status is:

- `UNKNOWN` until the exporter receives its first fleet snapshot
- `OK` when reporters are current and no retained problems exist
- `DEGRADED` when recent or resolved errors remain within the retention window
- `ERROR` when an active problem exists or a reporter is stale/offline

Repeated failures are grouped by fleet, code, phase, node, and storage. VMID, operation ID, and the latest error remain diagnostic fields and do not create a separate problem for every failed VM. Recent and resolved problems are retained for 24 hours by default. Exporter flags `--problem-retention`, `--problem-limit`, and `--exceptions-file` control this behavior.

Prometheus can scrape the exporter directly:

```yaml
scrape_configs:
  - job_name: fleeting-proxmox
    static_configs:
      - targets:
          - gitlab-fleeting1:9252
```

All plugin metrics include `cluster`, `pool`, and `group` labels. Per-node metrics also include `node`; storage metrics include `storage`.

Problem metrics use stable `code`, `phase`, `node`, and `storage` labels:

```text
fleeting_proxmox_problem_active
fleeting_proxmox_problem_recent
fleeting_proxmox_problem_occurrences
fleeting_proxmox_problem_last_seen_timestamp_seconds
```

Error text is deliberately excluded from Prometheus labels.

### Log correlation

Plugin log records use the same full group ID as taskscaler:

```text
group=proxmox/<cluster>/<pool>/<name_prefix>
```

Filter one fleet:

```shell
journalctl -u gitlab-runner --grep='group=proxmox/cluster-pve01/gitlab-runners1/amd64-runner1'
```

Each `Increase` and `Decrease` batch has an `operation_id`. VM operations also include `instance`, `node`, `storage`, `vmid`, and `phase`. Durations are rendered as human-readable values such as `45ms`, `2.042s`, or `3m1.2s`.

## Runner Configuration

See [`examples/config.toml`](/workspace/fleeting-plugin-proxmox/examples/config.toml) for a complete example.

Minimal shape:

```toml
[[runners]]
  name = "proxmox-fleeting"
  url = "https://gitlab.example.com"
  token = "RUNNER_TOKEN"
  executor = "instance"

  [runners.autoscaler]
    plugin = "fleeting-plugin-proxmox"
    capacity_per_instance = 1
    max_use_count = 1
    max_instances = 20
    instance_ready_command = "cloud-init status --wait || test $? -eq 2"

  [runners.autoscaler.plugin_config]
    api_url = "https://pve.example.com:8006"
    token_id = "gitlab-runner@pve!fleeting"
    token_secret = "REDACTED"
    cluster_name = "prod-pve"
    pool = "gitlab-ci"
    template_vmids = [9000]
    template_stage_mode = "auto"
    template_vmid_range = "510000-510099"
    name_prefix = "glr"
    vmid_range = "500000-500999"
    nodes = ["pve01", "pve02"]
    target_storages = ["ceph-vm", "local-lvm"]
    network_mode = "static"
    ip_pool_network = "10.10.20.0/24"
    ip_pool_gateway = "10.10.20.1"
    ip_pool_ranges = ["10.10.20.100-10.10.20.199"]
    state_file = "/var/lib/fleeting-plugin-proxmox/prod-pve-gitlab-ci-glr-state.json"
    ci_user = "ubuntu"
    ci_ssh_keys = ["ssh-ed25519 AAAA... runner@example"]

  [runners.autoscaler.connector_config]
    username = "ubuntu"
    use_external_addr = false
    protocol = "ssh"
```

See [`examples/docker-autoscaler.config.toml`](/workspace/fleeting-plugin-proxmox/examples/docker-autoscaler.config.toml) for a `docker-autoscaler` example.

## Notes

- The template VM should already have Cloud-Init and QEMU guest agent enabled.
- Use a dedicated Proxmox pool and a dedicated VMID range.
- If `template_stage_mode` is enabled, also reserve a separate `template_vmid_range` for managed staged templates.
- For non-shared template storage, managed staging can clone locally and then migrate the staged VM only when the target node has storage with the same storage ID.
- Use a subnet dedicated to ephemeral runner VMs. Do not share it with manually managed VMs.
- Changing the static IP pool is disruptive: managed VMs outside the new pool are deleted during plugin startup.
- `network_mode = "dhcp"` skips the local IP allocator and requires the guest agent to report the acquired address.
- For `docker-autoscaler`, the template VM must already contain a working Docker Engine configuration suitable for GitLab Runner.
