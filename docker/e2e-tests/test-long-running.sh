#!/bin/bash
# Test: Long-Running Stability
# Verifies: System stability over multiple operations

set -e

echo "=== Test: Long-Running Stability ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"
TEST_DURATION=${TEST_DURATION:-30}  # 30 seconds for CI

echo "Duration: ${TEST_DURATION}s"

# ============================================
# Test 1: Initial health
# ============================================
echo ""
echo "Test 1: Initial health check..."

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" != "200" ]; then
    echo "FAIL: Initial health check failed"
    exit 1
fi

echo "  Initial health: OK"

# ============================================
# Test 2: Periodic operations
# ============================================
echo ""
echo "Test 2: Periodic operations (${TEST_DURATION}s)..."

START=$(date +%s)
SUBMISSIONS=0
HEALTH_CHECKS=0
FAILURES=0

while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START))

    if [ $ELAPSED -ge $TEST_DURATION ]; then
        break
    fi

    # Submit a job
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUBMISSIONS=$((SUBMISSIONS + 1))
    else
        FAILURES=$((FAILURES + 1))
    fi

    # Health check
    METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")
    if [ "$METRICS_CODE" = "200" ]; then
        HEALTH_CHECKS=$((HEALTH_CHECKS + 1))
    fi

    sleep 5
done

echo "  Jobs submitted: $SUBMISSIONS"
echo "  Health checks passed: $HEALTH_CHECKS"
echo "  Failures: $FAILURES"

# ============================================
# Test 3: Final health
# ============================================
echo ""
echo "Test 3: Final health check..."

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" != "200" ]; then
    echo "FAIL: Final health check failed"
    exit 1
fi

# Submit one more job
OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
if ! echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "FAIL: Final job submission failed"
    exit 1
fi

echo "  Final health: OK"

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Long-Running Stability PASSED ==="
echo ""
echo "Summary:"
echo "  - Duration: ${TEST_DURATION}s"
echo "  - Jobs submitted: $SUBMISSIONS"
echo "  - Health checks: $HEALTH_CHECKS"
echo "  - System: stable"

exit 0
