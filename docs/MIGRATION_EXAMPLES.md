# SLURM Migration Examples

Ready-to-use scripts and configurations for common migration scenarios.

## Scenario 1: Small Cluster (1-10 nodes)

### Pre-Migration Script
```bash
#!/bin/bash
# small_cluster_premigration.sh

set -e

echo "=== SLURM to FLURM Pre-Migration Check ==="

# 1. Check current state
echo "Current job status:"
squeue -l
echo ""

echo "Current node status:"
sinfo -N -l
echo ""

# 2. Backup SLURM state
BACKUP_DIR="/var/backup/slurm_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -r /var/spool/slurmctld "$BACKUP_DIR/"
cp /etc/slurm/slurm.conf "$BACKUP_DIR/"
echo "Backup created: $BACKUP_DIR"

# 3. Check MUNGE
if munge -n | unmunge > /dev/null 2>&1; then
    echo "MUNGE: OK"
else
    echo "MUNGE: FAILED - fix before proceeding"
    exit 1
fi

# 4. Check Erlang
if erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' 2>/dev/null; then
    echo "Erlang: OK"
else
    echo "Erlang: NOT INSTALLED"
    exit 1
fi

echo ""
echo "Pre-migration checks passed. Ready to proceed."
```

### Migration Script
```bash
#!/bin/bash
# small_cluster_migrate.sh

set -e
FLURM_DIR="/opt/flurm"

echo "=== Step 1: Drain jobs ==="
scontrol update partition=all state=drain reason="FLURM migration"

# Wait for jobs to complete (max 30 minutes)
TIMEOUT=1800
ELAPSED=0
while [ "$(squeue -h -t running | wc -l)" -gt 0 ] && [ $ELAPSED -lt $TIMEOUT ]; do
    echo "Waiting for $(squeue -h -t running | wc -l) running jobs..."
    sleep 30
    ELAPSED=$((ELAPSED + 30))
done

if [ "$(squeue -h -t running | wc -l)" -gt 0 ]; then
    echo "WARNING: Jobs still running after timeout"
    squeue -l
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
fi

echo "=== Step 2: Stop SLURM ==="
systemctl stop slurmctld
systemctl stop slurmd

echo "=== Step 3: Configure FLURM ==="
cat > /etc/flurm/sys.config << 'EOF'
[
  {flurm_controller, [
    {cluster_name, <<"small-cluster">>},
    {listen_port, 6817},
    {ra_data_dir, "/var/lib/flurm/ra"}
  ]},
  {lager, [
    {handlers, [
      {lager_file_backend, [{file, "/var/log/flurm/controller.log"}, {level, info}]}
    ]}
  ]}
].
EOF

cat > /etc/flurm/vm.args << EOF
-name flurm@$(hostname -f)
-setcookie $(cat /etc/munge/munge.key | md5sum | cut -d' ' -f1)
+K true
+P 1000000
EOF

echo "=== Step 4: Start FLURM ==="
systemctl start flurmctld
sleep 10

# Verify controller
if scontrol ping > /dev/null 2>&1; then
    echo "Controller: OK"
else
    echo "Controller: FAILED"
    journalctl -u flurmctld -n 50
    exit 1
fi

echo "=== Step 5: Start compute nodes ==="
systemctl start flurmnd

echo "=== Step 6: Resume partitions ==="
scontrol update partition=all state=up

echo ""
echo "=== Migration Complete ==="
sinfo
squeue
```

---

## Scenario 2: Medium Cluster with HA (10-100 nodes)

### Controller Configuration
```bash
#!/bin/bash
# ha_controller_setup.sh <controller_number>

CONTROLLER_NUM=$1  # 1, 2, or 3
CONTROLLER_IPS=("192.168.1.10" "192.168.1.11" "192.168.1.12")
CONTROLLER_NAMES=("ctrl1.example.com" "ctrl2.example.com" "ctrl3.example.com")

cat > /etc/flurm/sys.config << EOF
[
  {flurm_controller, [
    {cluster_name, <<"prod-cluster">>},
    {node_name, 'flurm@${CONTROLLER_NAMES[$CONTROLLER_NUM-1]}'},
    {peer_nodes, [
      'flurm@${CONTROLLER_NAMES[0]}',
      'flurm@${CONTROLLER_NAMES[1]}',
      'flurm@${CONTROLLER_NAMES[2]}'
    ]},
    {listen_port, 6817},
    {ra_data_dir, "/var/lib/flurm/ra"},
    {http_port, 6820}
  ]},
  {lager, [
    {handlers, [
      {lager_file_backend, [{file, "/var/log/flurm/controller.log"}, {level, info}]},
      {lager_file_backend, [{file, "/var/log/flurm/error.log"}, {level, error}]}
    ]}
  ]}
].
EOF

cat > /etc/flurm/vm.args << EOF
-name flurm@${CONTROLLER_NAMES[$CONTROLLER_NUM-1]}
-setcookie FLURM_PROD_COOKIE_CHANGE_ME
+K true
+P 1000000
+Q 1000000
-kernel inet_dist_listen_min 9100
-kernel inet_dist_listen_max 9110
EOF

mkdir -p /var/lib/flurm/ra /var/log/flurm
chown -R flurm:flurm /var/lib/flurm /var/log/flurm
```

### Rolling Migration Script
```bash
#!/bin/bash
# ha_rolling_migrate.sh

set -e

CONTROLLERS=("ctrl1.example.com" "ctrl2.example.com" "ctrl3.example.com")
NODES_FILE="/etc/flurm/nodes.txt"  # One node hostname per line

echo "=== HA Rolling Migration ==="

# Phase 1: Start FLURM controllers in shadow mode
echo "Phase 1: Starting FLURM controllers..."
for ctrl in "${CONTROLLERS[@]}"; do
    echo "  Starting $ctrl..."
    ssh "$ctrl" "
        cat > /etc/flurm/shadow.config << 'INNER'
[{flurm_controller, [{shadow_mode, true}, {slurmctld_port, 16817}]}].
INNER
        systemctl start flurmctld-shadow
    "
done

# Wait for cluster formation
sleep 30

# Verify cluster
echo "Verifying FLURM cluster..."
curl -s http://${CONTROLLERS[0]}:16820/api/v1/cluster/status | jq .

# Phase 2: Import state from SLURM
echo "Phase 2: Importing SLURM state..."
curl -X POST http://${CONTROLLERS[0]}:16820/api/v1/import/slurm \
    -d '{"slurm_state_dir": "/var/spool/slurmctld"}'

# Verify import
SLURM_JOBS=$(squeue -h | wc -l)
FLURM_JOBS=$(curl -s http://${CONTROLLERS[0]}:16820/api/v1/jobs | jq length)
echo "Jobs imported: SLURM=$SLURM_JOBS, FLURM=$FLURM_JOBS"

# Phase 3: Switch traffic
echo "Phase 3: Switching traffic to FLURM..."
read -p "Update load balancer/DNS to point to FLURM port 16817, then press Enter"

# Phase 4: Disable shadow mode, take over port 6817
echo "Phase 4: Switching FLURM to production mode..."
for ctrl in "${CONTROLLERS[@]}"; do
    echo "  Switching $ctrl..."
    ssh "$ctrl" "
        systemctl stop slurmctld
        systemctl stop flurmctld-shadow
        systemctl start flurmctld
    "
    sleep 10
done

# Phase 5: Migrate compute nodes
echo "Phase 5: Migrating compute nodes..."
while read node; do
    echo "  Migrating $node..."
    ssh "$node" "
        systemctl stop slurmd
        systemctl start flurmnd
    " &
    # Don't overwhelm the network - migrate in batches of 10
    if [ $((++count % 10)) -eq 0 ]; then
        wait
        sleep 5
    fi
done < "$NODES_FILE"
wait

echo "=== Migration Complete ==="
sinfo -N
```

---

## Scenario 3: Live Migration (Zero-Downtime)

### Bridge Mode Configuration
```bash
#!/bin/bash
# live_migration_setup.sh

# Configure FLURM in bridge mode - forwards jobs to SLURM while building state
cat > /etc/flurm/bridge.config << 'EOF'
[
  {flurm_controller, [
    {cluster_name, <<"live-migration">>},
    {listen_port, 16817},
    {bridge_mode, active},
    {slurm_controller_host, "slurmctld.example.com"},
    {slurm_controller_port, 6817},
    %% Forward all jobs to SLURM during migration
    {forward_jobs, true},
    %% Mirror job state from SLURM
    {sync_jobs, true},
    {sync_interval, 1000}
  ]}
].
EOF
```

### Traffic Switching Script
```bash
#!/bin/bash
# live_traffic_switch.sh

set -e

FLURM_HOST="flurm.example.com"
SLURM_HOST="slurmctld.example.com"
LB_CONFIG="/etc/haproxy/haproxy.cfg"

# Current state
echo "Current routing:"
grep -A5 "backend slurm" "$LB_CONFIG"

# Phase 1: 10% traffic to FLURM
echo "Phase 1: Routing 10% traffic to FLURM..."
cat > /tmp/haproxy_phase1.cfg << EOF
backend slurm_controllers
    balance roundrobin
    server flurm $FLURM_HOST:16817 weight 10 check
    server slurm $SLURM_HOST:6817 weight 90 check
EOF
sudo cp /tmp/haproxy_phase1.cfg "$LB_CONFIG"
sudo systemctl reload haproxy

echo "Monitoring for 5 minutes..."
for i in {1..30}; do
    echo "$(date): Jobs via FLURM: $(curl -s http://$FLURM_HOST:16820/metrics | grep flurm_jobs_submitted_total)"
    sleep 10
done

read -p "Errors? (y to rollback, n to continue) " -n 1 -r
[[ $REPLY =~ ^[Yy]$ ]] && { echo "Rolling back..."; exit 1; }

# Phase 2: 50% traffic
echo "Phase 2: Routing 50% traffic to FLURM..."
cat > /tmp/haproxy_phase2.cfg << EOF
backend slurm_controllers
    balance roundrobin
    server flurm $FLURM_HOST:16817 weight 50 check
    server slurm $SLURM_HOST:6817 weight 50 check
EOF
sudo cp /tmp/haproxy_phase2.cfg "$LB_CONFIG"
sudo systemctl reload haproxy

echo "Monitoring for 10 minutes..."
sleep 600

# Phase 3: 100% traffic to FLURM
echo "Phase 3: Routing 100% traffic to FLURM..."
cat > /tmp/haproxy_phase3.cfg << EOF
backend slurm_controllers
    balance roundrobin
    server flurm $FLURM_HOST:16817 weight 100 check
    server slurm $SLURM_HOST:6817 weight 0 check backup
EOF
sudo cp /tmp/haproxy_phase3.cfg "$LB_CONFIG"
sudo systemctl reload haproxy

echo "Traffic switch complete. SLURM is now backup only."
```

---

## Scenario 4: Accounting Migration (slurmdbd to flurmbd)

### Export SLURM Accounting
```bash
#!/bin/bash
# export_slurm_accounting.sh

OUTPUT_DIR="/var/backup/slurm_accounting_$(date +%Y%m%d)"
mkdir -p "$OUTPUT_DIR"

# Export job history
echo "Exporting job history..."
sacct -S 2020-01-01 -E now -P --format=ALL > "$OUTPUT_DIR/jobs.csv"

# Export users
echo "Exporting users..."
sacctmgr show user -P > "$OUTPUT_DIR/users.csv"

# Export accounts
echo "Exporting accounts..."
sacctmgr show account -P > "$OUTPUT_DIR/accounts.csv"

# Export associations
echo "Exporting associations..."
sacctmgr show association -P > "$OUTPUT_DIR/associations.csv"

# Export QOS
echo "Exporting QOS..."
sacctmgr show qos -P > "$OUTPUT_DIR/qos.csv"

echo "Accounting data exported to: $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"
```

### Import to FLURM
```bash
#!/bin/bash
# import_flurm_accounting.sh

INPUT_DIR="$1"
FLURM_DBD_HOST="flurmbd.example.com"
FLURM_DBD_PORT="6819"

if [ -z "$INPUT_DIR" ]; then
    echo "Usage: $0 <export_directory>"
    exit 1
fi

# Import accounts first (parents before children)
echo "Importing accounts..."
curl -X POST "http://$FLURM_DBD_HOST:$FLURM_DBD_PORT/api/v1/import/accounts" \
    -F "file=@$INPUT_DIR/accounts.csv"

# Import users
echo "Importing users..."
curl -X POST "http://$FLURM_DBD_HOST:$FLURM_DBD_PORT/api/v1/import/users" \
    -F "file=@$INPUT_DIR/users.csv"

# Import QOS
echo "Importing QOS..."
curl -X POST "http://$FLURM_DBD_HOST:$FLURM_DBD_PORT/api/v1/import/qos" \
    -F "file=@$INPUT_DIR/qos.csv"

# Import associations
echo "Importing associations..."
curl -X POST "http://$FLURM_DBD_HOST:$FLURM_DBD_PORT/api/v1/import/associations" \
    -F "file=@$INPUT_DIR/associations.csv"

# Import job history (may take a while for large clusters)
echo "Importing job history (this may take a while)..."
curl -X POST "http://$FLURM_DBD_HOST:$FLURM_DBD_PORT/api/v1/import/jobs" \
    -F "file=@$INPUT_DIR/jobs.csv" \
    --max-time 3600

echo "Import complete. Verify with: sacct"
```

---

## Verification Scripts

### Post-Migration Validation
```bash
#!/bin/bash
# post_migration_validate.sh

ERRORS=0

echo "=== FLURM Post-Migration Validation ==="

# 1. Controller health
echo -n "Controller health: "
if curl -sf http://localhost:6820/health > /dev/null; then
    echo "PASS"
else
    echo "FAIL"
    ((ERRORS++))
fi

# 2. scontrol ping
echo -n "scontrol ping: "
if scontrol ping > /dev/null 2>&1; then
    echo "PASS"
else
    echo "FAIL"
    ((ERRORS++))
fi

# 3. Nodes online
echo -n "Nodes online: "
TOTAL=$(sinfo -h -N | wc -l)
UP=$(sinfo -h -N -t idle,alloc,mix | wc -l)
if [ "$UP" -eq "$TOTAL" ]; then
    echo "PASS ($UP/$TOTAL)"
else
    echo "WARN ($UP/$TOTAL up)"
fi

# 4. Submit test job
echo -n "Test job submission: "
JOBID=$(sbatch --wrap="hostname; sleep 5" -o /tmp/test_job_%j.out 2>&1 | awk '{print $4}')
if [ -n "$JOBID" ]; then
    echo "PASS (JobID: $JOBID)"
    # Wait and check completion
    sleep 30
    STATE=$(sacct -j "$JOBID" -n -o state | head -1 | tr -d ' ')
    echo -n "Test job completion: "
    if [ "$STATE" = "COMPLETED" ]; then
        echo "PASS"
    else
        echo "FAIL (State: $STATE)"
        ((ERRORS++))
    fi
else
    echo "FAIL"
    ((ERRORS++))
fi

# 5. Accounting
echo -n "Accounting (sacct): "
if sacct > /dev/null 2>&1; then
    echo "PASS"
else
    echo "FAIL"
    ((ERRORS++))
fi

# Summary
echo ""
echo "=== Validation Complete ==="
if [ "$ERRORS" -eq 0 ]; then
    echo "All checks passed!"
    exit 0
else
    echo "ERRORS: $ERRORS"
    exit 1
fi
```

---

## Rollback Scripts

### Quick Rollback
```bash
#!/bin/bash
# quick_rollback.sh

echo "=== Emergency Rollback to SLURM ==="

# Stop FLURM
echo "Stopping FLURM..."
systemctl stop flurmctld
systemctl stop flurmnd

# Start SLURM
echo "Starting SLURM..."
systemctl start slurmctld
sleep 10
systemctl start slurmd

# Verify
echo "Verifying SLURM..."
scontrol ping
sinfo

echo "Rollback complete."
```

See [MIGRATION.md](MIGRATION.md) for the full migration guide.
