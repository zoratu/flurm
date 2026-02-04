# SLURM to FLURM Migration Guide

This guide provides step-by-step instructions for migrating from SLURM to FLURM. FLURM is a drop-in replacement for the SLURM daemons (slurmctld, slurmd) while maintaining compatibility with existing SLURM client tools (sbatch, squeue, sinfo, scancel, etc.).

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Migration Checklist](#migration-checklist)
4. [Single Controller Migration](#single-controller-migration)
5. [High-Availability Migration](#high-availability-migration)
6. [Rollback Procedure](#rollback-procedure)
7. [Verification Tests](#verification-tests)
8. [Troubleshooting](#troubleshooting)

## Overview

### What FLURM Replaces

| SLURM Component | FLURM Replacement |
|-----------------|-------------------|
| `slurmctld` | `flurmctld` (Erlang-based controller) |
| `slurmd` | `flurmnd` (Erlang-based node daemon) |
| `slurmdbd` | `flurmbd` (Erlang-based accounting daemon) |

### What Stays the Same

- All SLURM client tools (`sbatch`, `squeue`, `sinfo`, `scancel`, `srun`, etc.)
- MUNGE authentication
- Job scripts (no changes needed)
- Basic `slurm.conf` settings (partitions, nodes)

### Key Benefits After Migration

- **Zero-downtime upgrades** via Erlang hot code loading
- **Active-active controllers** instead of primary/backup
- **No global lock** in scheduler (actor-based)
- **Automatic failover** with Ra consensus
- **Dynamic node scaling** without restart

## Prerequisites

### Hardware Requirements

- **Controller nodes**: 4+ cores, 8GB+ RAM, SSD storage
- **Compute nodes**: Same as SLURM requirements
- **Network**: All nodes must be able to reach controllers on ports 6817 (SLURM protocol) and 6818 (internal)

### Software Requirements

```bash
# Install Erlang/OTP 25+ (Rocky Linux/RHEL)
dnf install -y epel-release
dnf install -y erlang

# Verify version
erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().'
# Should show "25" or higher

# MUNGE must be installed and running
dnf install -y munge munge-libs
systemctl enable --now munge
```

### Pre-Migration Checks

```bash
# 1. Verify MUNGE is working
munge -n | unmunge

# 2. Check current SLURM version
slurmctld -V

# 3. Backup current state
cp -r /var/spool/slurm/ctld /var/spool/slurm/ctld.backup
cp /etc/slurm/slurm.conf /etc/slurm/slurm.conf.backup

# 4. List running jobs (drain before migration)
squeue
```

## Migration Checklist

- [ ] All running jobs completed or checkpointed
- [ ] SLURM state backed up
- [ ] MUNGE working on all nodes
- [ ] Erlang installed on controller and compute nodes
- [ ] FLURM binaries built and deployed
- [ ] Firewall ports opened (6817, 6818, 4369, 9100-9110)
- [ ] DNS/hostnames correct for all nodes
- [ ] Rollback plan documented

## Hot Migration (Zero-Downtime)

For production clusters where draining all jobs is impractical, FLURM supports a hot migration path that preserves running jobs.

### Overview

The hot migration process:
1. Start FLURM in "shadow mode" alongside SLURM
2. FLURM imports current state from SLURM
3. Switch client traffic to FLURM (via load balancer or DNS)
4. FLURM takes over job management
5. Stop SLURM (running jobs continue under FLURM)

### Step 1: Build and Deploy FLURM

```bash
# Build FLURM
git clone https://github.com/your-org/flurm.git
cd flurm && rebar3 release

# Deploy to controller nodes (use different ports initially)
scp -r _build/default/rel/flurm controller:/opt/flurm
```

### Step 2: Start FLURM in Shadow Mode

Shadow mode allows FLURM to run alongside SLURM, importing state without serving clients.

```bash
# Create shadow mode config
cat > /etc/flurm/shadow.config << 'EOF'
[
  {flurm, [
    {shadow_mode, true},
    {slurmctld_port, 16817},     % Different port from SLURM
    {slurm_state_dir, "/var/spool/slurm/ctld"},
    {import_slurm_state, true},
    {sync_interval, 5000}        % Sync every 5 seconds
  ]}
].
EOF

# Start FLURM in shadow mode
/opt/flurm/bin/flurm foreground -config /etc/flurm/shadow.config
```

### Step 3: Verify State Import

```bash
# Compare job counts
SLURM_JOBS=$(squeue -h | wc -l)
FLURM_JOBS=$(curl -s http://localhost:16817/api/jobs | jq length)
echo "SLURM: $SLURM_JOBS jobs, FLURM: $FLURM_JOBS jobs"

# Compare node states
sinfo > /tmp/slurm_nodes.txt
curl -s http://localhost:16817/api/nodes > /tmp/flurm_nodes.json
```

### Step 4: Switch Client Traffic

Option A: Load Balancer (Recommended)

```bash
# Update HAProxy/nginx to route new connections to FLURM
# Old config:
#   backend slurm_controllers
#     server slurmctld 192.168.1.10:6817 check
#
# New config:
backend slurm_controllers
  server flurmctld 192.168.1.10:16817 check weight 100
  server slurmctld 192.168.1.10:6817 check weight 0 backup
```

Option B: DNS Cutover

```bash
# Update DNS to point to FLURM
# Or update /etc/slurm/slurm.conf on clients:
SlurmctldHost=controller(192.168.1.10) SlurmctldPort=16817
```

### Step 5: Disable Shadow Mode

Once traffic is flowing to FLURM:

```bash
# Reconfigure FLURM to take over completely
cat > /etc/flurm/production.config << 'EOF'
[
  {flurm, [
    {shadow_mode, false},
    {slurmctld_port, 6817},      % Standard SLURM port
    {import_slurm_state, false}  % No more imports
  ]}
].
EOF

# Restart FLURM with production config
/opt/flurm/bin/flurm stop
/opt/flurm/bin/flurm foreground -config /etc/flurm/production.config
```

### Step 6: Stop SLURM

```bash
# Stop SLURM controller (jobs continue under FLURM)
systemctl stop slurmctld

# Update compute nodes to use FLURM (rolling restart)
pdsh -a 'systemctl stop slurmd && systemctl start flurmnd'
```

### Verification During Hot Migration

```bash
#!/bin/bash
# hot_migration_check.sh - Run continuously during migration

while true; do
    echo "=== $(date) ==="

    # Job counts
    SLURM_RUNNING=$(squeue -h -t RUNNING 2>/dev/null | wc -l)
    FLURM_RUNNING=$(curl -s http://localhost:16817/api/jobs?state=running 2>/dev/null | jq length)
    echo "Running jobs - SLURM: $SLURM_RUNNING, FLURM: $FLURM_RUNNING"

    # New submissions going to FLURM?
    echo "Recent submissions:"
    curl -s http://localhost:16817/api/jobs?limit=5 | jq -r '.[] | "\(.id) \(.state) \(.submit_time)"'

    sleep 10
done
```

### Rollback During Hot Migration

If issues occur during hot migration:

```bash
# Revert load balancer to SLURM
# In HAProxy:
backend slurm_controllers
  server slurmctld 192.168.1.10:6817 check weight 100
  server flurmctld 192.168.1.10:16817 check weight 0 backup

# Stop FLURM shadow
/opt/flurm/bin/flurm stop

# SLURM continues serving all requests
```

---

## Single Controller Migration

### Step 1: Build FLURM

```bash
# Clone repository
git clone https://github.com/your-org/flurm.git
cd flurm

# Build
rebar3 compile
rebar3 release
```

### Step 2: Stop SLURM Daemons

```bash
# On controller
systemctl stop slurmctld

# On all compute nodes
pdsh -a 'systemctl stop slurmd'
```

### Step 3: Start FLURM Controller

```bash
# Create data directory
mkdir -p /var/lib/flurm/ra
chown flurm:flurm /var/lib/flurm

# Start controller (single node mode)
cd /opt/flurm
./start_controller.erl

# Or as a systemd service
cat > /etc/systemd/system/flurmctld.service << 'EOF'
[Unit]
Description=FLURM Controller Daemon
After=network.target munge.service
Requires=munge.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/flurm
ExecStart=/opt/flurm/start_controller.erl
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now flurmctld
```

### Step 4: Start FLURM Node Daemons

```bash
# On each compute node
cat > /etc/systemd/system/flurmnd.service << 'EOF'
[Unit]
Description=FLURM Node Daemon
After=network.target

[Service]
Type=simple
User=root
Environment=FLURM_CONTROLLER_HOST=controller-hostname
Environment=FLURM_CONTROLLER_PORT=6818
WorkingDirectory=/opt/flurm
ExecStart=/opt/flurm/start_node.erl
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now flurmnd
```

### Step 5: Verify Migration

```bash
# Check nodes are registered
sinfo

# Submit a test job
sbatch --wrap="hostname && sleep 10 && echo done"

# Check job status
squeue

# Verify job completed
sacct -j <job_id>
```

## High-Availability Migration

For production environments, deploy 3 or 5 controllers with Ra consensus.

### Step 1: Prepare Controller Nodes

On each controller node (ctrl1, ctrl2, ctrl3):

```bash
# Install Erlang and FLURM (same as single controller)

# Configure for HA
cat > /etc/flurm/flurm.conf << 'EOF'
# FLURM HA Configuration
FLURM_NODE_NAME=flurm@ctrl1.example.com
FLURM_COOKIE=your_secret_cookie_here
FLURM_CLUSTER_NODES=flurm@ctrl1.example.com,flurm@ctrl2.example.com,flurm@ctrl3.example.com
FLURM_RA_DATA_DIR=/var/lib/flurm/ra
EOF

# Source config in systemd service
# Add: EnvironmentFile=/etc/flurm/flurm.conf
```

### Step 2: Start Controllers in Order

```bash
# On ctrl1 (first)
systemctl start flurmctld

# Wait for startup
sleep 10

# On ctrl2
systemctl start flurmctld

# On ctrl3
systemctl start flurmctld
```

### Step 3: Verify Cluster Formation

```bash
# Check cluster status (from any controller)
curl -s http://ctrl1:8080/api/cluster/status | jq .

# Expected output:
# {
#   "leader": "flurm@ctrl1.example.com",
#   "members": ["flurm@ctrl1.example.com", "flurm@ctrl2.example.com", "flurm@ctrl3.example.com"],
#   "healthy": true
# }
```

### Step 4: Configure Client Failover

Update client configurations to use multiple controllers:

```bash
# /etc/slurm/slurm.conf on client nodes
SlurmctldHost=ctrl1(192.168.1.10)
SlurmctldHost=ctrl2(192.168.1.11)
SlurmctldHost=ctrl3(192.168.1.12)
```

Or use a load balancer in front of the controllers.

### Step 5: Test Failover

```bash
# Submit a long-running job
sbatch --wrap="sleep 300"

# Kill the leader controller
ssh ctrl1 'systemctl stop flurmctld'

# Verify job is still tracked
squeue  # Should still show the job

# Verify new leader elected
curl -s http://ctrl2:8080/api/cluster/status | jq .leader

# Restart the stopped controller
ssh ctrl1 'systemctl start flurmctld'

# Verify it rejoins
curl -s http://ctrl1:8080/api/cluster/status | jq .members
```

## Rollback Procedure

If issues occur, rollback to SLURM:

### Immediate Rollback (Within Minutes)

```bash
# Stop FLURM
systemctl stop flurmctld
pdsh -a 'systemctl stop flurmnd'

# Start SLURM
systemctl start slurmctld
pdsh -a 'systemctl start slurmd'

# Verify
sinfo
squeue
```

### Rollback After Running Jobs

If jobs were submitted to FLURM:

```bash
# 1. Let running jobs complete (or cancel them)
scancel --state=RUNNING

# 2. Wait for completion
while squeue | grep -q R; do sleep 10; done

# 3. Stop FLURM
systemctl stop flurmctld
pdsh -a 'systemctl stop flurmnd'

# 4. Restore SLURM state backup
cp -r /var/spool/slurm/ctld.backup/* /var/spool/slurm/ctld/

# 5. Start SLURM
systemctl start slurmctld
pdsh -a 'systemctl start slurmd'
```

## Verification Tests

Run these tests after migration to verify functionality:

### Test 1: Basic Job Submission

```bash
#!/bin/bash
# test_basic_submit.sh

echo "Test 1: Basic job submission"
JOB_ID=$(sbatch --wrap="echo hello" --parsable)
echo "Submitted job $JOB_ID"

sleep 5
STATE=$(squeue -j $JOB_ID -h -o %T 2>/dev/null || echo "COMPLETED")
if [ "$STATE" = "COMPLETED" ] || [ "$STATE" = "" ]; then
    echo "PASS: Job completed successfully"
else
    echo "FAIL: Job state is $STATE"
    exit 1
fi
```

### Test 2: Node Status

```bash
#!/bin/bash
# test_node_status.sh

echo "Test 2: Node status"
NODE_COUNT=$(sinfo -h -o %D | awk '{sum+=$1} END {print sum}')
if [ "$NODE_COUNT" -gt 0 ]; then
    echo "PASS: $NODE_COUNT nodes registered"
else
    echo "FAIL: No nodes registered"
    exit 1
fi
```

### Test 3: Job Cancellation

```bash
#!/bin/bash
# test_cancel.sh

echo "Test 3: Job cancellation"
JOB_ID=$(sbatch --wrap="sleep 300" --parsable)
echo "Submitted job $JOB_ID"

sleep 2
scancel $JOB_ID

sleep 2
STATE=$(squeue -j $JOB_ID -h -o %T 2>/dev/null)
if [ -z "$STATE" ]; then
    echo "PASS: Job cancelled successfully"
else
    echo "FAIL: Job still exists with state $STATE"
    exit 1
fi
```

### Test 4: Controller Failover (HA Only)

```bash
#!/bin/bash
# test_failover.sh

echo "Test 4: Controller failover"

# Submit job
JOB_ID=$(sbatch --wrap="sleep 60" --parsable)
echo "Submitted job $JOB_ID"

# Get current leader
LEADER=$(curl -s http://ctrl1:8080/api/cluster/status | jq -r .leader)
echo "Current leader: $LEADER"

# Kill leader
ssh ${LEADER%%@*} 'systemctl stop flurmctld'
sleep 10

# Check job still exists
STATE=$(squeue -j $JOB_ID -h -o %T)
if [ -n "$STATE" ]; then
    echo "PASS: Job survived failover (state: $STATE)"
else
    echo "FAIL: Job lost during failover"
    exit 1
fi

# Restart killed controller
ssh ${LEADER%%@*} 'systemctl start flurmctld'
```

### Run All Tests

```bash
#!/bin/bash
# run_all_tests.sh

FAILED=0

for test in test_basic_submit.sh test_node_status.sh test_cancel.sh; do
    echo "========================================"
    ./$test || FAILED=$((FAILED + 1))
    echo ""
done

echo "========================================"
if [ "$FAILED" -eq 0 ]; then
    echo "All tests passed!"
else
    echo "$FAILED test(s) failed"
    exit 1
fi
```

## Troubleshooting

### Problem: Nodes Not Registering

```bash
# Check node daemon logs
journalctl -u flurmnd -f

# Verify network connectivity
nc -zv controller-hostname 6818

# Check firewall
firewall-cmd --list-ports
```

### Problem: Jobs Not Scheduling

```bash
# Check scheduler logs
journalctl -u flurmctld -f | grep -i schedul

# Verify node resources
sinfo -N -l

# Check job requirements vs available resources
squeue -o "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"
```

### Problem: MUNGE Authentication Failures

```bash
# Test MUNGE
munge -n | ssh compute-node unmunge

# Check MUNGE key is identical on all nodes
md5sum /etc/munge/munge.key

# Restart MUNGE
systemctl restart munge
```

### Problem: Ra Cluster Not Forming

```bash
# Check Erlang distribution
epmd -names

# Verify cookie matches on all controllers
# Check /etc/flurm/flurm.conf

# Check network between controllers
ping ctrl2 && ping ctrl3

# Verify DNS resolution
host ctrl1.example.com
```

### Problem: Performance Issues

```bash
# Check controller memory
erl_call -sname flurm -c your_cookie -a 'erlang memory []'

# Check Ra status
erl_call -sname flurm -c your_cookie -a 'flurm_db_cluster status []'

# Enable debug logging temporarily
# In start_controller.erl, add:
# lager:set_loglevel(lager_console_backend, debug)
```

## End-to-End Migration Testing

Before performing a production migration, it is strongly recommended to run the end-to-end migration test suite. This validates the complete migration path in a controlled Docker environment.

### Prerequisites

- Docker and docker-compose installed
- At least 8GB RAM available for test containers
- 20GB disk space for container images

### Quick Start

```bash
# Run all migration tests
./scripts/run-migration-e2e-tests.sh

# Run with verbose output
./scripts/run-migration-e2e-tests.sh --verbose

# Run quick smoke tests only
./scripts/run-migration-e2e-tests.sh --quick

# Run specific migration stage tests
./scripts/run-migration-e2e-tests.sh --stage shadow
./scripts/run-migration-e2e-tests.sh --stage active
./scripts/run-migration-e2e-tests.sh --stage primary
./scripts/run-migration-e2e-tests.sh --stage standalone

# Run rollback tests
./scripts/run-migration-e2e-tests.sh --stage rollback

# Keep containers running after tests (for debugging)
./scripts/run-migration-e2e-tests.sh --keep-running
```

### Test Environment

The test environment includes:

| Service | Description | IP Address |
|---------|-------------|------------|
| mysql | MySQL for slurmdbd | 172.31.0.5 |
| slurm-dbd | SLURM accounting daemon | 172.31.0.6 |
| slurm-controller | SLURM controller (slurmctld) | 172.31.0.10 |
| slurm-node-1 | SLURM compute node | 172.31.0.11 |
| slurm-node-2 | SLURM compute node | 172.31.0.12 |
| flurm-controller | FLURM controller | 172.31.0.20 |
| flurm-node-1 | FLURM compute node | 172.31.0.21 |
| test-orchestrator | Test runner | 172.31.0.100 |
| load-generator | Simulates job workload | 172.31.0.101 |

### Test Categories

#### Shadow Mode Tests
- `shadow_mode_observation_test`: FLURM observes SLURM without interference
- `shadow_mode_no_job_interference_test`: FLURM doesn't handle jobs in shadow mode
- `shadow_mode_state_sync_test`: FLURM syncs state from SLURM

#### Active Mode Tests
- `active_mode_forwarding_test`: Jobs submitted to FLURM are forwarded to SLURM
- `active_mode_local_execution_test`: Local jobs execute on FLURM
- `active_mode_mixed_workload_test`: Mixed local and forwarded workload

#### Primary Mode Tests
- `primary_mode_drain_test`: SLURM drains while FLURM handles new jobs
- `primary_mode_new_jobs_local_test`: New jobs execute locally on FLURM
- `primary_mode_forwarded_jobs_complete_test`: Previously forwarded jobs complete

#### Standalone Mode Tests
- `standalone_cutover_test`: Full cutover to FLURM-only
- `standalone_no_slurm_deps_test`: No SLURM dependencies in standalone
- `standalone_all_local_test`: All jobs execute locally

#### Rollback Tests
- `rollback_from_active_test`: Rollback from ACTIVE to SHADOW mode
- `rollback_from_primary_test`: Rollback from PRIMARY to ACTIVE mode
- `rollback_preserves_state_test`: State is preserved after rollback

#### Job Continuity Tests
- `job_continuity_test`: Jobs survive mode transitions
- `job_survives_shadow_to_active_test`: Jobs survive shadow->active
- `job_survives_active_to_primary_test`: Jobs survive active->primary
- `job_survives_primary_to_standalone_test`: Jobs survive primary->standalone

#### Accounting Sync Tests
- `accounting_sync_test`: Accounting data syncs between systems
- `accounting_no_double_count_test`: No double-counting of jobs
- `accounting_sync_during_transition_test`: Sync works during transitions

### Running Erlang Common Test Suite Directly

```bash
# From the project root
cd apps/flurm_controller/integration_test

# Run all migration e2e tests
ct_run -dir . -suite flurm_migration_e2e_SUITE

# Run specific group
ct_run -dir . -suite flurm_migration_e2e_SUITE -group shadow_mode

# Run with verbose logging
ct_run -dir . -suite flurm_migration_e2e_SUITE -verbosity 100
```

### Property-Based Testing

The migration state machine is also tested using PropEr for property-based testing:

```bash
# Run all migration property tests
rebar3 proper -m flurm_migration_prop_tests -n 100

# Run specific property
rebar3 proper -m flurm_migration_prop_tests -p prop_valid_mode_transitions -n 500
rebar3 proper -m flurm_migration_prop_tests -p prop_no_job_loss -n 500
rebar3 proper -m flurm_migration_prop_tests -p prop_rollback_idempotent -n 500
```

Properties tested:
- `prop_valid_mode_transitions`: Only valid transitions succeed
- `prop_no_job_loss`: Jobs are never lost during transitions
- `prop_rollback_idempotent`: Multiple rollbacks are safe
- `prop_rollback_preserves_state`: State is preserved after rollback
- `prop_cluster_registration_idempotent`: Cluster registration is idempotent

### Interpreting Test Results

Test results are stored in `docker/test-results/`:

- `e2e-results-YYYYMMDD-HHMMSS.log`: Main test output
- `container-logs.txt`: All container logs
- `flurm-controller.log`: FLURM controller specific logs
- `slurm-controller.log`: SLURM controller specific logs

### Troubleshooting Test Failures

```bash
# Check container health
docker-compose -f docker/docker-compose.migration-e2e.yml ps

# View FLURM logs
docker-compose -f docker/docker-compose.migration-e2e.yml logs flurm-controller

# View SLURM logs
docker-compose -f docker/docker-compose.migration-e2e.yml logs slurm-controller

# Enter test orchestrator for manual testing
docker-compose -f docker/docker-compose.migration-e2e.yml exec test-orchestrator bash

# Check FLURM bridge status
curl http://localhost:8080/api/v1/migration/status | jq .

# Manually set mode for debugging
curl -X PUT http://localhost:8080/api/v1/migration/mode \
  -H "Content-Type: application/json" \
  -d '{"mode": "active"}'
```

### Cleanup

```bash
# Stop and remove all test containers and volumes
docker-compose -f docker/docker-compose.migration-e2e.yml down -v

# Remove test images
docker rmi $(docker images -q "migration-*")
```

## Support

- GitHub Issues: https://github.com/your-org/flurm/issues
- Documentation: https://flurm.readthedocs.io
- Community Slack: #flurm on hpc-social.slack.com

## Appendix: Feature Comparison

| Feature | SLURM | FLURM |
|---------|-------|-------|
| Job submission (sbatch) | ✓ | ✓ |
| Job status (squeue) | ✓ | ✓ |
| Node info (sinfo) | ✓ | ✓ |
| Job cancel (scancel) | ✓ | ✓ |
| Interactive jobs (srun) | ✓ | ✓ |
| Job arrays | ✓ | ✓ |
| Job dependencies | ✓ | ✓ |
| Accounting (sacct/sacctmgr) | ✓ | ✓ |
| Accounting daemon (slurmdbd) | ✓ | ✓ |
| Backfill scheduling | ✓ | ✓ |
| Fair-share scheduling | ✓ | ✓ |
| Preemption | ✓ | ✓ |
| Reservations | ✓ | ✓ |
| GRES/GPU scheduling | ✓ | ✓ |
| Burst buffers | ✓ | ✓ |
| License management | ✓ | ✓ |
| Node drain/resume | ✓ | ✓ |
| Hot config reload | Partial | ✓ |
| Active-active HA | ✗ | ✓ |
| Zero-downtime upgrades | ✗ | ✓ |
| Dynamic node scaling | Restart | ✓ |
| Sub-second failover | ✗ | ✓ |
