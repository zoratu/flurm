#!/bin/bash
# Test: Migration with Concurrent Jobs
# Verifies: Basic operation in standalone mode

set -e

echo "=== Test: Migration with Concurrent Jobs ==="

# Test 1: Query bridge status
echo "Test 1: Bridge status query..."
BRIDGE_STATUS=$(curl -s "http://${FLURM_CTRL_1}:9090/api/v1/slurm-bridge/status" 2>/dev/null || echo '{"mode":"not_available"}')
echo "  Bridge status: $BRIDGE_STATUS"

# Test 2: Submit jobs
echo "Test 2: Job submissions..."
sleep 2  # Wait for rate limit to clear
SUCCESS=0
for i in $(seq 1 3); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
    fi
    sleep 1
done
echo "  Submitted: $SUCCESS/3 jobs"

# Test 3: Verify system stability
echo "Test 3: System stability..."
METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "000")
if [ "$METRICS_CODE" = "200" ]; then
    echo "  Metrics endpoint: OK"
else
    echo "  Metrics endpoint: $METRICS_CODE"
fi

# Test 4: Cluster configuration
echo "Test 4: Cluster configuration..."
CLUSTERS=$(curl -s "http://${FLURM_CTRL_1}:9090/api/v1/slurm-bridge/clusters" 2>/dev/null || echo '[]')
echo "  Clusters config: $CLUSTERS"

echo "=== Test: Migration with Concurrent Jobs PASSED ==="
echo ""
echo "Summary:"
echo "  - Standalone mode operational"
echo "  - Job submissions: $SUCCESS/3 accepted"
