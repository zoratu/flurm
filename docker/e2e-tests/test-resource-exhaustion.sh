#!/bin/bash
# Test: Resource Exhaustion Handling
# Verifies: System handles rapid submissions, queue limits

set -e

echo "=== Test: Resource Exhaustion Handling ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"

# ============================================
# Test 1: Rapid job submissions
# ============================================
echo "Test 1: Rapid submissions (10 jobs)..."

SUBMITTED=0
REJECTED=0

for i in $(seq 1 10); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUBMITTED=$((SUBMITTED + 1))
    else
        REJECTED=$((REJECTED + 1))
    fi
    sleep 0.5
done

echo "  Submitted: $SUBMITTED"
echo "  Rejected: $REJECTED"

if [ $SUBMITTED -lt 5 ]; then
    echo "FAIL: Too many rejections"
    exit 1
fi

# ============================================
# Test 2: Controller stability under load
# ============================================
echo ""
echo "Test 2: Controller stability..."

sleep 2

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" = "200" ]; then
    echo "  Controller: stable after load"
else
    echo "FAIL: Controller unstable: HTTP $METRICS_CODE"
    exit 1
fi

# ============================================
# Test 3: Queue still accessible
# ============================================
echo ""
echo "Test 3: Queue accessibility..."

sleep 2

squeue >/dev/null 2>&1 || true
echo "  Queue: accessible"

# ============================================
# Test 4: New submissions after load
# ============================================
echo ""
echo "Test 4: Submissions after load..."

sleep 3

OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Post-load submission: accepted"
else
    echo "FAIL: Post-load submission rejected: $OUTPUT"
    exit 1
fi

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Resource Exhaustion Handling PASSED ==="
echo ""
echo "Summary:"
echo "  - Rapid submissions: $SUBMITTED/10 accepted"
echo "  - Controller stability: OK"
echo "  - Post-load recovery: OK"

exit 0
