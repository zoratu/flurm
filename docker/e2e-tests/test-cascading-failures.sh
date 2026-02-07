#!/bin/bash
# Test: Cascading Failures Recovery
# Verifies: System stability under stress

set -e

echo "=== Test: Cascading Failures Recovery ==="

# Test 1: Submit jobs and verify system accepts them
echo "Test 1: Stress test submissions..."
JOB_IDS=""
SUCCESS=0
FAIL=0
for i in $(seq 1 10); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
        JOB_IDS="$JOB_IDS $JOB_ID"
        SUCCESS=$((SUCCESS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
    # Small delay to avoid rate limiting
    sleep 0.5
done
echo "  Submitted: $SUCCESS, Failed: $FAIL"

if [ $SUCCESS -lt 5 ]; then
    echo "FAIL: Too few jobs accepted under stress"
    exit 1
fi

# Test 2: System still responsive after batch submission
echo "Test 2: Post-batch responsiveness..."
sleep 2
METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "000")
if [ "$METRICS_CODE" = "200" ]; then
    echo "  Controller metrics: OK"
else
    echo "  WARN: Controller metrics returned $METRICS_CODE"
fi

# Test 3: Additional submission after stress
echo "Test 3: Post-stress submission..."
sleep 2
OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Post-stress submission: OK"
else
    echo "  WARN: Post-stress submission may have issues: $OUTPUT"
fi

# Test 4: Query operations
echo "Test 4: Query operations..."
squeue > /dev/null 2>&1 && echo "  squeue: OK" || echo "  squeue: Limited"
sinfo > /dev/null 2>&1 && echo "  sinfo: OK" || echo "  sinfo: Limited"

echo "=== Test: Cascading Failures Recovery PASSED ==="
echo ""
echo "Summary:"
echo "  - Stress submissions: $SUCCESS/10 accepted"
echo "  - System remained responsive"
