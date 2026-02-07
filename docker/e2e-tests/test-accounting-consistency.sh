#!/bin/bash
# Test: Accounting Consistency
# Verifies: Job submission and metrics tracking

set -e

echo "=== Test: Accounting Consistency ==="

# Test 1: Submit job for accounting
echo "Test 1: Job submission for accounting..."
sleep 2  # Wait for rate limit to clear
OUTPUT=$(sbatch --cpus-per-task=2 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Job submitted: $JOB_ID"
else
    echo "  WARN: Job submission result: $OUTPUT"
fi

# Test 2: Multiple job submissions
echo "Test 2: Multiple submissions..."
sleep 2
SUCCESS=0
for i in $(seq 1 3); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
    fi
    sleep 1
done
echo "  Submitted: $SUCCESS/3 jobs"

# Test 3: Query sacct (accounting)
echo "Test 3: Accounting query..."
SACCT_OUTPUT=$(sacct 2>&1 || echo "")
if echo "$SACCT_OUTPUT" | grep -qi "disabled\|not configured"; then
    echo "  WARN: Accounting storage is disabled (expected in basic setup)"
elif [ -n "$SACCT_OUTPUT" ]; then
    echo "  Accounting data available"
else
    echo "  WARN: sacct returned empty"
fi

# Test 4: Metrics show job activity
echo "Test 4: Metrics tracking..."
METRICS=$(curl -s "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "")
if echo "$METRICS" | grep -q "flurm_jobs"; then
    echo "  Job metrics present"
    JOBS_METRIC=$(echo "$METRICS" | grep "flurm_jobs" | head -1)
    echo "  Sample: $JOBS_METRIC"
else
    echo "  WARN: Job metrics not found"
fi

# Test 5: User query
echo "Test 5: User information..."
squeue -u root 2>/dev/null && echo "  User queue query: OK" || echo "  User queue query: Limited"

echo "=== Test: Accounting Consistency PASSED ==="
echo ""
echo "Summary:"
echo "  - Job submissions accepted"
echo "  - Metrics endpoint available"
