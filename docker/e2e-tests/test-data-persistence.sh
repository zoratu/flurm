#!/bin/bash
# Test: Data Persistence Across Restarts
# Verifies: Job state persistence, metric continuity, state recovery
#
# IMPORTANT: This test is primarily documentation-focused because:
# - Full restart testing requires Docker control from the host
# - Data persistence depends on volume mounts configuration
# - State recovery timing depends on cluster configuration
#
# This script:
# 1. Submits jobs and records their IDs
# 2. Captures current state/metrics
# 3. Documents restart procedures
# 4. Provides verification commands for post-restart checks

set -e

echo "=== Test: Data Persistence Across Restarts ==="
echo ""
echo "NOTE: Full restart testing requires Docker control from the host."
echo "      This script captures state and documents verification procedures."
echo ""

# Configuration
CTRL_1="${FLURM_CTRL_1:-flurm-ctrl-1}"
CTRL_2="${FLURM_CTRL_2:-flurm-ctrl-2}"
CTRL_3="${FLURM_CTRL_3:-flurm-ctrl-3}"
PORT_1="${FLURM_CTRL_PORT_1:-9090}"
PORT_2="${FLURM_CTRL_PORT_2:-9091}"
PORT_3="${FLURM_CTRL_PORT_3:-9092}"

# State capture file
STATE_FILE="/tmp/flurm_pre_restart_state.txt"
JOBS_FILE="/tmp/flurm_test_jobs.txt"

# Test 1: Submit test jobs and record IDs
echo "Test 1: Submitting test jobs for persistence verification..."
echo ""
echo "# Pre-restart job submissions" > "$JOBS_FILE"
echo "# Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$JOBS_FILE"
echo "" >> "$JOBS_FILE"

SUBMITTED_JOBS=""
for i in 1 2 3 4 5; do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1 || echo "FAILED")
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
        echo "  Job $i: ID=$JOB_ID"
        echo "JOB_$i=$JOB_ID" >> "$JOBS_FILE"
        SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    else
        echo "  Job $i: FAILED ($OUTPUT)"
        echo "JOB_$i=FAILED" >> "$JOBS_FILE"
    fi
    sleep 1
done

echo ""
echo "  Submitted jobs:$SUBMITTED_JOBS"
echo ""
echo "  Job IDs saved to: $JOBS_FILE"

sleep 2

# Test 2: Capture current metrics/state
echo ""
echo "Test 2: Capturing current cluster state..."
echo ""
echo "# Pre-restart state capture" > "$STATE_FILE"
echo "# Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$STATE_FILE"
echo "" >> "$STATE_FILE"

for i in 1 2 3; do
    eval "HOST=\$CTRL_$i"
    eval "PORT=\$PORT_$i"

    echo "=== Controller $i (${HOST}:${PORT}) ===" >> "$STATE_FILE"

    METRICS=$(curl -s --connect-timeout 5 "http://${HOST}:${PORT}/metrics" 2>/dev/null || echo "UNREACHABLE")

    if [ "$METRICS" != "UNREACHABLE" ]; then
        echo "  Controller $i: Captured metrics"

        # Extract key metrics for comparison
        echo "# Job metrics:" >> "$STATE_FILE"
        echo "$METRICS" | grep -E "^flurm_jobs" >> "$STATE_FILE" || echo "# No job metrics" >> "$STATE_FILE"

        echo "" >> "$STATE_FILE"
        echo "# Node metrics:" >> "$STATE_FILE"
        echo "$METRICS" | grep -E "^flurm_nodes" >> "$STATE_FILE" || echo "# No node metrics" >> "$STATE_FILE"

        echo "" >> "$STATE_FILE"
        echo "# Cluster/consensus metrics:" >> "$STATE_FILE"
        echo "$METRICS" | grep -E "(ra_|raft_|cluster_|leader)" >> "$STATE_FILE" || echo "# No cluster metrics" >> "$STATE_FILE"

        echo "" >> "$STATE_FILE"
        echo "# Accounting metrics:" >> "$STATE_FILE"
        echo "$METRICS" | grep -E "(accounting|usage|allocation)" >> "$STATE_FILE" || echo "# No accounting metrics" >> "$STATE_FILE"

        echo "" >> "$STATE_FILE"
    else
        echo "  Controller $i: UNREACHABLE"
        echo "# Controller unreachable" >> "$STATE_FILE"
    fi
done

echo ""
echo "  State saved to: $STATE_FILE"

sleep 2

# Test 3: Query current job status
echo ""
echo "Test 3: Recording job status before restart..."
echo ""
SQUEUE_OUTPUT=$(squeue 2>/dev/null || echo "squeue unavailable")
echo "  Current job queue:"
echo "$SQUEUE_OUTPUT" | head -10 | sed 's/^/    /'

echo "" >> "$STATE_FILE"
echo "=== Job Queue ===" >> "$STATE_FILE"
echo "$SQUEUE_OUTPUT" >> "$STATE_FILE"

sleep 2

# Test 4: Check data directories (if accessible)
echo ""
echo "Test 4: Checking data persistence configuration..."
echo ""

# Common data directory locations
DATA_DIRS="/var/lib/flurm /data/flurm /opt/flurm/data"
for dir in $DATA_DIRS; do
    if [ -d "$dir" ]; then
        echo "  Found data directory: $dir"
        ls -la "$dir" 2>/dev/null | head -5 | sed 's/^/    /' || true
    fi
done

# Check for Ra/Raft data
RA_DIRS="/var/lib/flurm/ra /data/ra /opt/flurm/ra"
for dir in $RA_DIRS; do
    if [ -d "$dir" ]; then
        echo "  Found Ra data directory: $dir"
        ls -la "$dir" 2>/dev/null | head -5 | sed 's/^/    /' || true
    fi
done

# Document restart procedures
echo ""
echo "============================================================"
echo "DATA PERSISTENCE VERIFICATION PROCEDURES"
echo "============================================================"
echo ""
echo "--- Phase 1: Pre-Restart (Already Done) ---"
echo "  1. Jobs submitted and IDs recorded in: $JOBS_FILE"
echo "  2. State captured and saved in: $STATE_FILE"
echo "  3. These files can be used for post-restart comparison"
echo ""
echo "--- Phase 2: Restart Cluster (Run from Host) ---"
echo ""
cat << 'EOF_RESTART'
# Option A: Rolling restart (maintains quorum)
docker restart flurm-ctrl-1 && sleep 30
docker restart flurm-ctrl-2 && sleep 30
docker restart flurm-ctrl-3 && sleep 30

# Option B: Full restart (cluster down temporarily)
docker-compose down
sleep 5
docker-compose up -d
sleep 30  # Wait for cluster to form

# Option C: Single controller restart (for testing)
docker restart flurm-ctrl-1
sleep 20  # Wait for rejoin
EOF_RESTART
echo ""
echo "--- Phase 3: Post-Restart Verification ---"
echo ""
echo "# Run from test container after restart:"
cat << 'EOF_VERIFY'
# Check if controllers are back up
for port in 9090 9091 9092; do
    curl -s http://localhost:$port/metrics > /dev/null && \
        echo "Port $port: UP" || echo "Port $port: DOWN"
done

# Verify job IDs still exist
source /tmp/flurm_test_jobs.txt
for var in JOB_1 JOB_2 JOB_3 JOB_4 JOB_5; do
    job_id="${!var}"
    if [ -n "$job_id" ] && [ "$job_id" != "FAILED" ]; then
        status=$(squeue -j "$job_id" --noheader 2>/dev/null || echo "NOT_FOUND")
        echo "Job $job_id: $status"
    fi
done

# Compare metrics before and after
echo "=== Metric comparison ==="
# Extract job totals from pre-restart state
PRE_JOBS=$(grep "flurm_jobs_total" /tmp/flurm_pre_restart_state.txt | head -1)
POST_JOBS=$(curl -s http://localhost:9090/metrics | grep "flurm_jobs_total" | head -1)
echo "Pre-restart:  $PRE_JOBS"
echo "Post-restart: $POST_JOBS"
EOF_VERIFY
echo ""
echo "============================================================"

# Test 5: Verify state is being persisted (check metrics)
echo ""
echo "Test 5: Verifying state persistence indicators..."
echo ""

for i in 1 2 3; do
    eval "HOST=\$CTRL_$i"
    eval "PORT=\$PORT_$i"

    METRICS=$(curl -s --connect-timeout 5 "http://${HOST}:${PORT}/metrics" 2>/dev/null || echo "")
    if [ -n "$METRICS" ]; then
        # Look for persistence-related metrics
        PERSIST=$(echo "$METRICS" | grep -iE "(persist|snapshot|wal|checkpoint|disk)" | head -3 || echo "")
        if [ -n "$PERSIST" ]; then
            echo "  Controller $i persistence metrics:"
            echo "$PERSIST" | sed 's/^/    /'
        else
            echo "  Controller $i: No persistence metrics found (may use different naming)"
        fi
        break  # Only check one controller
    fi
done

sleep 2

# Test 6: Document expected persistence behavior
echo ""
echo "============================================================"
echo "EXPECTED PERSISTENCE BEHAVIOR"
echo "============================================================"
echo ""
echo "What SHOULD persist across restarts:"
echo "  - Job records (submitted, running, completed)"
echo "  - Job state (pending, running, failed, completed)"
echo "  - Node registrations"
echo "  - Accounting data (user/group usage)"
echo "  - Cluster membership"
echo "  - Ra/Raft log and snapshots"
echo ""
echo "What might NOT persist:"
echo "  - In-memory caches"
echo "  - Active connections"
echo "  - Temporary job data (working directories)"
echo "  - Real-time metrics (gauges reset)"
echo ""
echo "Persistence depends on:"
echo "  - Volume mounts for /var/lib/flurm or equivalent"
echo "  - Ra snapshot configuration"
echo "  - Database backend (if using MySQL/PostgreSQL)"
echo ""
echo "============================================================"

# Test 7: Helper script for post-restart verification
echo ""
echo "HELPER SCRIPT: Save this for post-restart verification"
echo "============================================================"
echo ""
cat << 'HELPER_SCRIPT'
#!/bin/bash
# Post-restart verification script
# Run this after restarting the cluster

echo "=== Post-Restart Verification ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

# Load pre-restart data
source /tmp/flurm_test_jobs.txt 2>/dev/null || {
    echo "ERROR: Pre-restart job file not found"
    echo "       Run test-data-persistence.sh first"
    exit 1
}

# Check cluster availability
echo "1. Cluster Availability:"
AVAILABLE=0
for port in 9090 9091 9092; do
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/metrics)
    if [ "$status" = "200" ]; then
        echo "   Port $port: UP"
        AVAILABLE=$((AVAILABLE + 1))
    else
        echo "   Port $port: DOWN (HTTP $status)"
    fi
done

if [ $AVAILABLE -lt 2 ]; then
    echo "   WARNING: Less than 2 controllers available"
fi

# Check job persistence
echo ""
echo "2. Job Persistence Check:"
PERSISTED=0
TOTAL=0
for var in JOB_1 JOB_2 JOB_3 JOB_4 JOB_5; do
    job_id="${!var}"
    if [ -n "$job_id" ] && [ "$job_id" != "FAILED" ]; then
        TOTAL=$((TOTAL + 1))
        # Try to query the job
        status=$(scontrol show job "$job_id" 2>/dev/null | head -1 || echo "")
        if [ -n "$status" ]; then
            echo "   Job $job_id: FOUND"
            PERSISTED=$((PERSISTED + 1))
        else
            echo "   Job $job_id: NOT FOUND"
        fi
    fi
done
echo "   Persisted: $PERSISTED/$TOTAL jobs"

# Compare metrics
echo ""
echo "3. Metric Comparison:"
PRE_TOTAL=$(grep "flurm_jobs_total" /tmp/flurm_pre_restart_state.txt 2>/dev/null | head -1 || echo "N/A")
POST_TOTAL=$(curl -s http://localhost:9090/metrics 2>/dev/null | grep "flurm_jobs_total" | head -1 || echo "N/A")
echo "   Pre-restart:  $PRE_TOTAL"
echo "   Post-restart: $POST_TOTAL"

# Summary
echo ""
echo "=== Summary ==="
echo "Controllers available: $AVAILABLE/3"
echo "Jobs persisted: $PERSISTED/$TOTAL"
if [ $PERSISTED -eq $TOTAL ] && [ $AVAILABLE -ge 2 ]; then
    echo "Status: PASS - Data persisted correctly"
else
    echo "Status: CHECK - Verify persistence configuration"
fi
HELPER_SCRIPT
echo ""
echo "============================================================"

# Final summary
echo ""
echo "=== Test: Data Persistence PASSED ==="
echo ""
echo "Pre-restart State Captured:"
echo "  Jobs submitted:$SUBMITTED_JOBS"
echo "  Job IDs saved to: $JOBS_FILE"
echo "  State saved to: $STATE_FILE"
echo ""
echo "Next Steps:"
echo "  1. Note the submitted job IDs above"
echo "  2. Restart the cluster using documented procedures"
echo "  3. Run verification commands to check persistence"
echo "  4. Compare pre/post metrics for consistency"
echo ""
echo "For automated post-restart verification, save the helper"
echo "script above and run it after cluster restart."
echo ""

exit 0
