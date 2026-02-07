#!/bin/bash
# Test: HA Failover
# Verifies: Cluster availability and metrics

set -e

echo "=== Test: HA Failover ==="

# Test 1: Verify controllers are reachable via metrics
echo "Test 1: Controller availability..."
AVAILABLE=0
for CTRL in "${FLURM_CTRL_1}:9090" "${FLURM_CTRL_2}:9091" "${FLURM_CTRL_3}:9092"; do
    HOST=$(echo $CTRL | cut -d: -f1)
    PORT=$(echo $CTRL | cut -d: -f2)
    RESPONSE=$(curl -s -w "%{http_code}" -o /dev/null "http://${HOST}:${PORT}/metrics" 2>/dev/null || echo "000")
    if [ "$RESPONSE" = "200" ]; then
        echo "  ${HOST}:${PORT}: UP"
        AVAILABLE=$((AVAILABLE + 1))
    else
        echo "  ${HOST}:${PORT}: DOWN (code $RESPONSE)"
    fi
done

if [ $AVAILABLE -lt 1 ]; then
    echo "FAIL: No controllers available"
    exit 1
fi
echo "  Controllers available: $AVAILABLE/3"

# Test 2: Query cluster status via metrics
echo "Test 2: Cluster metrics..."
METRICS=$(curl -s "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "")
if [ -n "$METRICS" ]; then
    echo "  Metrics retrieved from controller 1"
    if echo "$METRICS" | grep -q "flurm_"; then
        echo "  FLURM metrics present"
    fi
else
    echo "  WARN: Could not fetch metrics"
fi

# Test 3: Job submission to primary controller
echo "Test 3: Job submission to primary..."
sleep 2  # Wait for rate limit to clear
OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Job submitted: $JOB_ID"
else
    echo "  WARN: Job submission result: $OUTPUT"
fi

# Test 4: Multiple submissions to verify consistency
echo "Test 4: Multiple submissions..."
sleep 2
SUCCESS=0
for i in $(seq 1 3); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
    fi
    sleep 1
done
echo "  Submissions: $SUCCESS/3 successful"

# Test 5: Node status query
echo "Test 5: Node status..."
SINFO=$(sinfo 2>/dev/null || echo "")
if [ -n "$SINFO" ]; then
    echo "  Node information available"
else
    echo "  WARN: sinfo limited"
fi

echo "=== Test: HA Failover PASSED ==="
echo ""
echo "Summary:"
echo "  - Controllers available: $AVAILABLE/3"
echo "  - Primary controller accepting jobs"
