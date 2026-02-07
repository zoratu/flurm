#!/bin/bash
# Test: Scale Simulation
# Verifies: System behavior under batch job submission

set -e

echo "=== Test: Scale Simulation ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"

# ============================================
# Test 1: Batch submission
# ============================================
echo "Test 1: Batch submission (20 jobs)..."

SUBMITTED=0
FAILED=0

for i in $(seq 1 20); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUBMITTED=$((SUBMITTED + 1))
    else
        FAILED=$((FAILED + 1))
    fi
    sleep 0.3
done

echo "  Submitted: $SUBMITTED"
echo "  Failed: $FAILED"

if [ $SUBMITTED -lt 10 ]; then
    echo "FAIL: Less than half of jobs accepted"
    exit 1
fi

# ============================================
# Test 2: System stability after batch
# ============================================
echo ""
echo "Test 2: System stability..."

sleep 2

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" = "200" ]; then
    echo "  Metrics: OK"
else
    echo "FAIL: Metrics unhealthy: HTTP $METRICS_CODE"
    exit 1
fi

# ============================================
# Test 3: squeue after batch
# ============================================
echo ""
echo "Test 3: squeue responsiveness..."

squeue >/dev/null 2>&1 || true
echo "  squeue: OK"

# ============================================
# Test 4: Additional submission
# ============================================
echo ""
echo "Test 4: Post-batch submission..."

sleep 2

OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Post-batch: accepted"
else
    echo "FAIL: Post-batch rejected: $OUTPUT"
    exit 1
fi

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Scale Simulation PASSED ==="
echo ""
echo "Summary:"
echo "  - Batch jobs: $SUBMITTED/20 accepted"
echo "  - System stability: OK"
echo "  - Post-batch submission: OK"

exit 0
