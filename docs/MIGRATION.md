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
| `slurmdbd` | Coming soon (currently use existing) |

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
| Interactive jobs (srun) | ✓ | Planned |
| Job arrays | ✓ | Planned |
| Job dependencies | ✓ | Planned |
| Accounting (sacct) | ✓ | Planned |
| Backfill scheduling | ✓ | Planned |
| Fair-share scheduling | ✓ | Planned |
| Hot config reload | Partial | ✓ |
| Active-active HA | ✗ | ✓ |
| Zero-downtime upgrades | ✗ | ✓ |
| Dynamic node scaling | Restart | ✓ |
