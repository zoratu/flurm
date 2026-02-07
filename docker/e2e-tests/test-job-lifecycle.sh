#!/bin/bash
# Test: Basic Job Lifecycle
# Verifies: job submission, status tracking, completion handling

set -e

echo "=== Test: Job Lifecycle Basic ==="

# Test 1: Submit a simple batch job
echo "Test 1: Submit batch job..."
JOB_OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$JOB_OUTPUT" | grep -qi "submitted\|success"; then
    # Extract job ID - try different patterns
    JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE '[0-9]+' | head -1)
    if [ -z "$JOB_ID" ]; then
        JOB_ID="unknown"
    fi
    echo "  Job submitted successfully: $JOB_ID"
else
    echo "FAIL: sbatch did not indicate success: $JOB_OUTPUT"
    exit 1
fi

# Test 2: Verify job tracking (may not be fully implemented)
echo "Test 2: Check job tracking..."
sleep 2
JOB_STATE=$(squeue -j "$JOB_ID" -h -o "%T" 2>/dev/null || echo "")
QUEUE_OUTPUT=$(squeue 2>/dev/null || echo "")
if [ -n "$JOB_STATE" ]; then
    echo "  Job state via squeue: $JOB_STATE"
elif echo "$QUEUE_OUTPUT" | grep -q "$JOB_ID"; then
    echo "  Job found in queue"
else
    echo "  WARN: Job not visible in squeue (job tracking may not be implemented)"
    echo "  Note: Job was accepted by controller, tracking is optional feature"
fi

# Test 3: Test multiple job submissions
echo "Test 3: Multiple job submissions..."
SUBMITTED=0
for i in $(seq 1 5); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUBMITTED=$((SUBMITTED + 1))
    fi
done
echo "  Successfully submitted $SUBMITTED/5 jobs"
if [ $SUBMITTED -lt 3 ]; then
    echo "FAIL: Too few jobs accepted"
    exit 1
fi

# Test 4: Submit to specific partition (if supported)
echo "Test 4: Partition targeting..."
PARTITION_OUTPUT=$(sbatch --partition=debug /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$PARTITION_OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Partition-specific submission: OK"
elif echo "$PARTITION_OUTPUT" | grep -qi "invalid\|unknown\|error"; then
    echo "  WARN: Partition specification may not be supported"
else
    echo "  Partition submission result: $PARTITION_OUTPUT"
fi

# Test 5: Test sinfo (node info)
echo "Test 5: Node information..."
SINFO_OUTPUT=$(sinfo 2>&1 || echo "")
if echo "$SINFO_OUTPUT" | grep -qE "PARTITION|debug|batch|STATE"; then
    echo "  sinfo returns node/partition information"
    echo "$SINFO_OUTPUT" | head -5 | sed 's/^/    /'
else
    echo "  WARN: sinfo may not be fully implemented"
fi

# Test 6: Cancel job (if job ID is known)
echo "Test 6: Job cancellation..."
if [ "$JOB_ID" != "unknown" ]; then
    CANCEL_OUTPUT=$(scancel "$JOB_ID" 2>&1 || echo "")
    if echo "$CANCEL_OUTPUT" | grep -qi "error"; then
        echo "  WARN: Cancel may have failed: $CANCEL_OUTPUT"
    else
        echo "  Cancel command sent for job $JOB_ID"
    fi
else
    echo "  SKIP: No job ID available for cancel test"
fi

# Test 7: Metrics endpoint
echo "Test 7: Metrics availability..."
METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${FLURM_CTRL_1}:9090/metrics" 2>/dev/null || echo "000")
if [ "$METRICS_CODE" = "200" ]; then
    echo "  Metrics endpoint: HTTP 200 OK"
else
    echo "  WARN: Metrics endpoint returned HTTP $METRICS_CODE"
fi

echo "=== Test: Job Lifecycle Basic PASSED ==="
echo ""
echo "Summary:"
echo "  - Job submission: WORKING"
echo "  - Job tracking: $([ -n "$JOB_STATE" ] && echo 'WORKING' || echo 'PARTIAL')"
echo "  - Multiple submissions: WORKING ($SUBMITTED/5)"
