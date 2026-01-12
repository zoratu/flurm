# FLURM Deployment Guide

This guide covers deploying FLURM in production environments.

## Prerequisites

- Erlang/OTP 26 or later
- rebar3 build tool
- Network connectivity between all nodes on ports 6817-6819

## Single-Node Deployment

For testing and development:

```bash
# Build the release
rebar3 release

# Start the controller
_build/default/rel/flurmctld/bin/flurmctld foreground
```

## Multi-Controller Cluster

For high availability, deploy at least 3 controllers:

### Step 1: Configure Controllers

Create `/etc/flurm/sys.config` on each controller:

```erlang
[
  {flurm_controller, [
    {cluster_name, <<"production-cluster">>},
    {node_name, 'flurm@controller1.example.com'},
    {peer_nodes, [
      'flurm@controller2.example.com',
      'flurm@controller3.example.com'
    ]},
    {listen_port, 6817},
    {ra_data_dir, "/var/lib/flurm/ra"}
  ]},
  {lager, [
    {handlers, [
      {lager_file_backend, [{file, "/var/log/flurm/controller.log"}, {level, info}]}
    ]}
  ]}
].
```

### Step 2: Configure VM Arguments

Create `/etc/flurm/vm.args`:

```
-name flurm@controller1.example.com
-setcookie FLURM_SECRET_COOKIE
+K true
+P 1000000
+Q 1000000
-env ERL_MAX_PORTS 100000
```

### Step 3: Start Controllers

On each controller node:

```bash
_build/prod/rel/flurmctld/bin/flurmctld start
```

### Step 4: Verify Cluster

```bash
# Check cluster status
_build/prod/rel/flurmctld/bin/flurmctld remote_console

> flurm_cluster:status().
#{leader => 'flurm@controller1.example.com',
  members => ['flurm@controller1.example.com',
              'flurm@controller2.example.com',
              'flurm@controller3.example.com'],
  state => ready}
```

## Compute Node Deployment

### Step 1: Configure Node Daemon

Create `/etc/flurm/node.config`:

```erlang
[
  {flurm_node_daemon, [
    {node_name, <<"compute-001">>},
    {controllers, [
      {"controller1.example.com", 6817},
      {"controller2.example.com", 6817},
      {"controller3.example.com", 6817}
    ]},
    {cpus, auto},           % Auto-detect or specify count
    {memory_mb, auto},      % Auto-detect or specify MB
    {features, [<<"avx2">>, <<"gpu">>]},
    {partitions, [<<"batch">>, <<"gpu">>]}
  ]}
].
```

### Step 2: Start Node Daemon

```bash
_build/prod/rel/flurmnd/bin/flurmnd start
```

## Accounting Daemon (slurmdbd)

Deploy the accounting daemon for job accounting:

```erlang
[
  {flurm_dbd, [
    {listen_port, 6819},
    {storage_backend, mnesia},  % or 'ets' for in-memory only
    {data_dir, "/var/lib/flurm/dbd"}
  ]}
].
```

```bash
_build/prod/rel/flurmbd/bin/flurmbd start
```

## SLURM Configuration Compatibility

FLURM can parse standard slurm.conf files:

```bash
# Example slurm.conf location
ControlMachine=controller1
ControlAddr=controller1.example.com
BackupController=controller2
BackupAddr=controller2.example.com

ClusterName=production-cluster
SlurmctldPort=6817
SlurmdPort=6818

# Partitions
PartitionName=batch Nodes=compute-[001-100] Default=YES MaxTime=24:00:00 State=UP
PartitionName=gpu Nodes=gpu-[001-010] MaxTime=48:00:00 State=UP

# Nodes
NodeName=compute-[001-100] CPUs=32 RealMemory=128000 State=UNKNOWN
NodeName=gpu-[001-010] CPUs=64 RealMemory=256000 Gres=gpu:4 State=UNKNOWN
```

## Hot Configuration Reload

FLURM supports live configuration reload without restart:

```bash
# From SLURM client
scontrol reconfigure

# Or from Erlang console
> flurm_config:reload().
```

## Monitoring

### Prometheus Metrics

FLURM exposes metrics at `http://<controller>:9090/metrics`:

- `flurm_jobs_pending` - Number of pending jobs
- `flurm_jobs_running` - Number of running jobs
- `flurm_nodes_up` - Number of available nodes
- `flurm_scheduler_decisions_total` - Scheduler decisions count
- `flurm_consensus_term` - Current Raft term

### Health Checks

```bash
# HTTP health endpoint
curl http://controller1:6817/health

# Response
{"status": "healthy", "leader": true, "term": 42}
```

## Troubleshooting

### Controller Won't Start

1. Check Erlang is installed: `erl -version`
2. Verify network connectivity between controllers
3. Check cookie matches on all nodes
4. Review logs: `/var/log/flurm/controller.log`

### Node Registration Fails

1. Verify controller addresses are correct
2. Check firewall allows port 6817
3. Ensure node name is unique
4. Check node daemon logs

### Split-Brain Recovery

If cluster experiences network partition:

1. Stop all controllers
2. Clear Ra state: `rm -rf /var/lib/flurm/ra/*`
3. Start controllers one at a time
4. Verify cluster with `flurm_cluster:status()`

## Upgrading

### Hot Code Upgrade

FLURM supports zero-downtime upgrades:

```bash
# Build new release
rebar3 release

# Apply upgrade
_build/prod/rel/flurmctld/bin/flurmctld upgrade "0.2.0"
```

### Rolling Restart

For major upgrades:

1. Upgrade followers first
2. Trigger leader failover: `flurm_cluster:transfer_leadership()`
3. Upgrade former leader
4. Verify cluster health
