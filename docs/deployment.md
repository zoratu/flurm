# FLURM Deployment Guide

This document covers deploying FLURM in various configurations, from single-node development setups to production multi-controller clusters.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Single Node Setup](#single-node-setup)
3. [Multi-Controller Cluster](#multi-controller-cluster)
4. [Configuration Reference](#configuration-reference)
5. [Monitoring and Metrics](#monitoring-and-metrics)
6. [Backup and Recovery](#backup-and-recovery)
7. [Upgrades](#upgrades)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 2 GB | 8+ GB |
| Disk | 10 GB | 50+ GB SSD |
| Network | 100 Mbps | 1 Gbps+ |

### Software Requirements

- Erlang/OTP 26.0 or later
- Linux kernel 4.15+ (for cgroups v2)
- systemd (for service management)
- Munge (for authentication)

### Installation

```bash
# Install Erlang (Debian/Ubuntu)
apt-get install erlang

# Install Erlang (RHEL/CentOS)
yum install erlang

# Install Munge
apt-get install munge libmunge-dev
# or
yum install munge munge-devel

# Build FLURM
git clone https://github.com/your-org/flurm.git
cd flurm
rebar3 release
```

## Single Node Setup

### Quick Start

```bash
# Create configuration directory
sudo mkdir -p /etc/flurm
sudo mkdir -p /var/lib/flurm
sudo mkdir -p /var/log/flurm

# Copy default configuration
sudo cp config/flurm.config.example /etc/flurm/flurm.config

# Start FLURM
_build/default/rel/flurm/bin/flurm foreground
```

### Single Node Configuration

```erlang
%% /etc/flurm/flurm.config
[
  {flurm, [
    %% Cluster identity
    {cluster_name, "dev-cluster"},
    {controller_name, "controller1"},

    %% Network
    {slurmctld_host, "localhost"},
    {slurmctld_port, 6817},
    {slurmd_port, 6818},

    %% Single controller mode
    {controllers, [
      {"localhost", 6817}
    ]},

    %% Data directories
    {state_dir, "/var/lib/flurm"},
    {log_dir, "/var/log/flurm"},
    {spool_dir, "/var/spool/flurm"},

    %% Scheduler
    {scheduler_plugin, flurm_sched_backfill},
    {schedule_interval, 1000},

    %% Checkpointing
    {checkpoint_interval, 60000},
    {checkpoint_dir, "/var/lib/flurm/checkpoint"},

    %% Authentication
    {auth_type, munge},

    %% Logging
    {log_level, info}
  ]},

  {mnesia, [
    {dir, "/var/lib/flurm/mnesia"}
  ]},

  {kernel, [
    {logger_level, info},
    {logger, [
      {handler, default, logger_std_h, #{
        config => #{
          file => "/var/log/flurm/flurm.log",
          max_no_bytes => 10485760,
          max_no_files => 10
        }
      }}
    ]}
  ]}
].
```

### Systemd Service

```ini
# /etc/systemd/system/flurm.service
[Unit]
Description=FLURM Workload Manager
After=network.target munge.service
Requires=munge.service

[Service]
Type=simple
User=flurm
Group=flurm
Environment=HOME=/var/lib/flurm
ExecStart=/opt/flurm/bin/flurm foreground
ExecStop=/opt/flurm/bin/flurm stop
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable flurm
sudo systemctl start flurm
sudo systemctl status flurm
```

## Multi-Controller Cluster

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FLURM HA Cluster                                  │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │ Controller 1 │◄──►│ Controller 2 │◄──►│ Controller 3 │              │
│  │   (Leader)   │    │  (Follower)  │    │  (Follower)  │              │
│  │ 10.0.0.1     │    │  10.0.0.2    │    │  10.0.0.3    │              │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘              │
│         │                   │                   │                       │
│         └───────────────────┼───────────────────┘                       │
│                             │                                           │
│  ┌──────────────────────────┼──────────────────────────┐               │
│  │                          │                          │               │
│  │      ┌─────────┐    ┌─────────┐    ┌─────────┐     │               │
│  │      │ Node 1  │    │ Node 2  │    │ Node 3  │ ... │ Compute Nodes │
│  │      └─────────┘    └─────────┘    └─────────┘     │               │
│  │                                                     │               │
│  └─────────────────────────────────────────────────────┘               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Controller Configuration

Each controller needs identical configuration with its own identity:

```erlang
%% /etc/flurm/flurm.config (Controller 1)
[
  {flurm, [
    {cluster_name, "prod-cluster"},
    {controller_name, "controller1"},

    %% This controller's address
    {slurmctld_host, "10.0.0.1"},
    {slurmctld_port, 6817},

    %% All controllers in the cluster
    {controllers, [
      {"10.0.0.1", 6817},
      {"10.0.0.2", 6817},
      {"10.0.0.3", 6817}
    ]},

    %% Raft consensus settings
    {raft_election_timeout, {150, 300}},  % min, max ms
    {raft_heartbeat_interval, 50},        % ms

    %% Distributed Erlang
    {distributed, [
      {flurm, [
        {controller1, 'flurm@controller1.example.com'},
        {controller2, 'flurm@controller2.example.com'},
        {controller3, 'flurm@controller3.example.com'}
      ]}
    ]},

    %% State directories
    {state_dir, "/var/lib/flurm"},
    {checkpoint_dir, "/var/lib/flurm/checkpoint"},

    %% Scheduler
    {scheduler_plugin, flurm_sched_backfill},

    %% Node management
    {heartbeat_interval, 30000},
    {heartbeat_timeout, 90000},
    {node_down_threshold, 3}
  ]},

  {mnesia, [
    {dir, "/var/lib/flurm/mnesia"},
    {extra_db_nodes, [
      'flurm@controller2.example.com',
      'flurm@controller3.example.com'
    ]}
  ]}
].
```

### Network Configuration

```bash
# Ensure all controllers can communicate
# Required ports:
# - 6817: SLURM protocol (client connections)
# - 6818: slurmd protocol (compute nodes)
# - 4369: Erlang EPMD
# - 9100-9200: Erlang distribution

# Firewall rules (iptables)
iptables -A INPUT -p tcp --dport 6817 -j ACCEPT
iptables -A INPUT -p tcp --dport 6818 -j ACCEPT
iptables -A INPUT -p tcp --dport 4369 -j ACCEPT
iptables -A INPUT -p tcp --dport 9100:9200 -j ACCEPT
```

### Cluster Bootstrap

```bash
# On controller1 (first node)
/opt/flurm/bin/flurm start

# Wait for it to become leader
/opt/flurm/bin/flurm eval 'flurm_consensus:status().'

# On controller2
/opt/flurm/bin/flurm start

# On controller3
/opt/flurm/bin/flurm start

# Verify cluster status
/opt/flurm/bin/flurm eval 'flurm_consensus:cluster_status().'
```

### Compute Node Configuration

```erlang
%% /etc/flurm/slurmd.config
[
  {flurm_slurmd, [
    {node_name, "node001"},

    %% Controller addresses
    {controllers, [
      {"10.0.0.1", 6817},
      {"10.0.0.2", 6817},
      {"10.0.0.3", 6817}
    ]},

    %% Local daemon settings
    {slurmd_port, 6818},
    {spool_dir, "/var/spool/flurmd"},

    %% Resource declaration
    {cpus, 64},
    {memory, 256000},  % MB
    {tmp_disk, 500000}, % MB
    {features, ["gpu", "nvme", "ib"]},

    %% Generic resources (GPUs, etc.)
    {gres, [
      {gpu, [{type, "a100"}, {count, 4}]}
    ]},

    %% Partitions this node belongs to
    {partitions, ["batch", "gpu"]},

    %% Heartbeat settings
    {heartbeat_interval, 30000}
  ]}
].
```

## Configuration Reference

### Core Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cluster_name` | string | "cluster" | Unique cluster identifier |
| `controller_name` | string | hostname | This controller's name |
| `slurmctld_host` | string | "localhost" | Controller bind address |
| `slurmctld_port` | integer | 6817 | Controller port |
| `slurmd_port` | integer | 6818 | Compute daemon port |
| `controllers` | list | [] | List of {host, port} tuples |

### Scheduler Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scheduler_plugin` | atom | `flurm_sched_backfill` | Scheduler to use |
| `schedule_interval` | integer | 1000 | Scheduling cycle (ms) |
| `max_job_count` | integer | 100000 | Maximum pending jobs |
| `default_time_limit` | integer | 60 | Default job time (min) |
| `max_time_limit` | integer | 43200 | Maximum job time (min) |

### Consensus Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `raft_election_timeout` | tuple | {150, 300} | Election timeout range (ms) |
| `raft_heartbeat_interval` | integer | 50 | Leader heartbeat (ms) |
| `raft_snapshot_interval` | integer | 10000 | Entries between snapshots |

### Partition Definition

```erlang
{partitions, [
  #{
    name => <<"batch">>,
    nodes => ["node[001-100]"],
    max_time => 1440,       % 24 hours
    default => true,
    state => up,
    priority_tier => 1,
    max_nodes_per_job => 50,
    preempt_mode => requeue
  },
  #{
    name => <<"gpu">>,
    nodes => ["gpu[01-10]"],
    max_time => 2880,       % 48 hours
    default => false,
    state => up,
    priority_tier => 2,
    features_required => [<<"gpu">>]
  },
  #{
    name => <<"debug">>,
    nodes => ["node001"],
    max_time => 30,
    default => false,
    state => up,
    priority_tier => 3,
    max_jobs_per_user => 2
  }
]}
```

## Monitoring and Metrics

### Prometheus Metrics

FLURM exposes Prometheus metrics on port 9090:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'flurm'
    static_configs:
      - targets:
        - 'controller1:9090'
        - 'controller2:9090'
        - 'controller3:9090'
    metrics_path: /metrics
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `flurm_jobs_total` | counter | Total jobs submitted |
| `flurm_jobs_running` | gauge | Currently running jobs |
| `flurm_jobs_pending` | gauge | Jobs waiting to run |
| `flurm_jobs_completed` | counter | Successfully completed jobs |
| `flurm_jobs_failed` | counter | Failed jobs |
| `flurm_nodes_total` | gauge | Total compute nodes |
| `flurm_nodes_idle` | gauge | Idle nodes |
| `flurm_nodes_allocated` | gauge | Nodes with jobs |
| `flurm_nodes_down` | gauge | Unreachable nodes |
| `flurm_scheduler_cycle_ms` | histogram | Scheduling cycle duration |
| `flurm_protocol_requests` | counter | Protocol requests by type |
| `flurm_consensus_term` | gauge | Current Raft term |
| `flurm_consensus_commits` | counter | Committed log entries |

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "FLURM Cluster",
    "panels": [
      {
        "title": "Job Status",
        "type": "stat",
        "targets": [
          {"expr": "flurm_jobs_running"},
          {"expr": "flurm_jobs_pending"}
        ]
      },
      {
        "title": "Node Status",
        "type": "piechart",
        "targets": [
          {"expr": "flurm_nodes_idle", "legendFormat": "Idle"},
          {"expr": "flurm_nodes_allocated", "legendFormat": "Allocated"},
          {"expr": "flurm_nodes_down", "legendFormat": "Down"}
        ]
      },
      {
        "title": "Scheduler Performance",
        "type": "graph",
        "targets": [
          {"expr": "histogram_quantile(0.99, flurm_scheduler_cycle_ms_bucket)"}
        ]
      }
    ]
  }
}
```

### Health Checks

```bash
# CLI health check
/opt/flurm/bin/flurm eval 'flurm_health:check().'

# HTTP health endpoint
curl http://controller1:9090/health

# Expected response:
{
  "status": "healthy",
  "consensus": "leader",
  "nodes": {"total": 100, "up": 98, "down": 2},
  "jobs": {"running": 450, "pending": 120}
}
```

### Alerting Rules

```yaml
# alerts.yml
groups:
  - name: flurm
    rules:
      - alert: FlurmNoLeader
        expr: sum(flurm_consensus_is_leader) == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "No FLURM leader elected"

      - alert: FlurmNodeDown
        expr: flurm_nodes_down > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} FLURM nodes are down"

      - alert: FlurmJobBacklog
        expr: flurm_jobs_pending > 10000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "High job backlog: {{ $value }} pending jobs"
```

## Backup and Recovery

### State Backup

```bash
# Create backup
/opt/flurm/bin/flurm eval 'flurm_backup:create("/backup/flurm-$(date +%Y%m%d).tar.gz").'

# Automated backup script
#!/bin/bash
BACKUP_DIR=/backup/flurm
DATE=$(date +%Y%m%d-%H%M%S)

# Stop accepting new jobs
/opt/flurm/bin/flurm eval 'flurm:drain().'

# Create backup
/opt/flurm/bin/flurm eval "flurm_backup:create(\"${BACKUP_DIR}/flurm-${DATE}.tar.gz\")."

# Resume operations
/opt/flurm/bin/flurm eval 'flurm:resume().'

# Keep last 7 days
find $BACKUP_DIR -name "flurm-*.tar.gz" -mtime +7 -delete
```

### Recovery Procedure

```bash
# Stop all controllers
systemctl stop flurm

# Restore from backup (on each controller)
/opt/flurm/bin/flurm eval 'flurm_backup:restore("/backup/flurm-20240115.tar.gz").'

# Start controllers
systemctl start flurm

# Verify state
/opt/flurm/bin/flurm eval 'flurm:status().'
```

## Upgrades

### Hot Code Upgrade

FLURM supports hot code upgrades without stopping the cluster:

```bash
# Build new release
cd /opt/flurm-src
git pull
rebar3 release
rebar3 appup generate

# Deploy upgrade
/opt/flurm/bin/flurm upgrade "2.0.0"

# Verify
/opt/flurm/bin/flurm versions
```

### Rolling Upgrade

For major versions, perform rolling upgrades:

```bash
# Upgrade followers first
for controller in controller2 controller3; do
    ssh $controller 'systemctl stop flurm'
    ssh $controller 'cp -r /opt/flurm-new /opt/flurm'
    ssh $controller 'systemctl start flurm'
    sleep 30  # Wait for rejoin
done

# Transfer leadership
/opt/flurm/bin/flurm eval 'flurm_consensus:transfer_leadership(controller2).'

# Upgrade old leader
systemctl stop flurm
cp -r /opt/flurm-new /opt/flurm
systemctl start flurm
```

## Troubleshooting

### Common Issues

#### Controller Won't Start

```bash
# Check logs
journalctl -u flurm -f

# Common causes:
# 1. Port already in use
netstat -tlnp | grep 6817

# 2. Mnesia corruption
rm -rf /var/lib/flurm/mnesia/*
/opt/flurm/bin/flurm start

# 3. Permission issues
chown -R flurm:flurm /var/lib/flurm
```

#### Nodes Not Registering

```bash
# Check network connectivity
ping controller1
telnet controller1 6817

# Check Munge
munge -n | unmunge

# Check slurmd logs
journalctl -u flurmd -f

# Manual registration test
/opt/flurm/bin/flurm eval 'flurm_node_manager:register_test("node001").'
```

#### Split Brain

```bash
# Check consensus state on all controllers
for c in controller1 controller2 controller3; do
    echo "=== $c ==="
    ssh $c '/opt/flurm/bin/flurm eval "flurm_consensus:status()."'
done

# Force leader election
/opt/flurm/bin/flurm eval 'flurm_consensus:force_election().'
```

#### Scheduler Not Running Jobs

```bash
# Check scheduler status
/opt/flurm/bin/flurm eval 'flurm_scheduler:status().'

# Check available resources
/opt/flurm/bin/flurm eval 'flurm_node_manager:available_resources().'

# Manual schedule cycle
/opt/flurm/bin/flurm eval 'flurm_scheduler:run_cycle().'

# Check job requirements vs available
/opt/flurm/bin/flurm eval 'flurm_scheduler:debug_job(12345).'
```

### Debug Shell

```bash
# Attach to running system
/opt/flurm/bin/flurm remote_console

# Useful debug commands
> flurm:status().
> flurm_consensus:status().
> flurm_job_manager:list_jobs().
> flurm_node_manager:list_nodes().
> observer:start().  % GUI process inspector
> recon:proc_count(memory, 10).  % Top 10 by memory
```

### Log Levels

```erlang
%% Increase logging temporarily
logger:set_primary_config(level, debug).

%% Per-module logging
logger:set_module_level(flurm_protocol, debug).
logger:set_module_level(flurm_consensus, debug).

%% Reset
logger:set_primary_config(level, info).
```

---

See also:
- [Architecture](architecture.md) for system design
- [Testing Guide](testing.md) for validation procedures
