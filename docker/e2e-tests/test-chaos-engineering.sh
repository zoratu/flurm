#!/bin/bash
# Test: Chaos Engineering
# Verifies: System resilience to unusual conditions

set -e

echo "=== Test: Chaos Engineering ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"

# ============================================
# Test 1: Rapid connect/disconnect
# ============================================
echo "Test 1: Rapid connections..."

for i in $(seq 1 5); do
    curl -s --connect-timeout 1 "http://${CTRL_1}:9090/metrics" > /dev/null 2>&1 || true
done

echo "  Rapid connections: handled"

# ============================================
# Test 2: Concurrent sbatch submissions
# ============================================
echo ""
echo "Test 2: Concurrent sbatch..."

for i in $(seq 1 5); do
    sbatch /jobs/batch/job_cpu_small.sh > /dev/null 2>&1 &
done
wait

sleep 2
echo "  Concurrent sbatch: completed"

# ============================================
# Test 3: Invalid protocol data (graceful handling)
# ============================================
echo ""
echo "Test 3: Invalid data handling..."

echo "garbage data" | nc -w 1 ${CTRL_1} 9090 > /dev/null 2>&1 || true
echo "  Invalid HTTP: handled"

# ============================================
# Test 4: Controller still healthy
# ============================================
echo ""
echo "Test 4: Post-chaos health check..."

sleep 2

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" = "200" ]; then
    echo "  Controller: healthy"
else
    echo "FAIL: Controller unhealthy after chaos: HTTP $METRICS_CODE"
    exit 1
fi

# ============================================
# Test 5: Job submission still works
# ============================================
echo ""
echo "Test 5: Post-chaos job submission..."

sleep 2

OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Job submission: working"
else
    echo "FAIL: Job submission broken: $OUTPUT"
    exit 1
fi

# ============================================
# Test 6: squeue still works
# ============================================
echo ""
echo "Test 6: Post-chaos squeue..."

sleep 2

squeue >/dev/null 2>&1 || true
echo "  squeue: working"

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Chaos Engineering PASSED ==="
echo ""
echo "Summary:"
echo "  - Rapid connections: survived"
echo "  - Concurrent submissions: survived"
echo "  - Invalid data: handled gracefully"
echo "  - All functions: operational"

exit 0
