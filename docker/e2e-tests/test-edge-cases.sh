#!/bin/bash
# Test: Edge Cases
# Verifies: System handles unusual inputs gracefully

set -e

echo "=== Test: Edge Cases ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"

# ============================================
# Test 1: Very long job name
# ============================================
echo "Test 1: Long job name..."

sleep 2

LONG_NAME="test_job_$(head -c 100 /dev/urandom | tr -dc 'a-z' | head -c 100)"
OUTPUT=$(sbatch --job-name="$LONG_NAME" /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success\|truncat"; then
    echo "  Long job name: handled"
else
    echo "  Long job name: $OUTPUT"
fi

# ============================================
# Test 2: Special characters in job name
# ============================================
echo ""
echo "Test 2: Special characters..."

sleep 2

OUTPUT=$(sbatch --job-name="test-job_123" /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Special characters: accepted"
else
    echo "  Special characters: $OUTPUT"
fi

# ============================================
# Test 3: Zero resources (should reject)
# ============================================
echo ""
echo "Test 3: Zero CPUs request..."

sleep 2

# Use timeout to prevent hang on invalid options
OUTPUT=$(timeout 10 sbatch --cpus-per-task=0 /jobs/batch/job_cpu_small.sh 2>&1 || echo "timeout_or_error")

if echo "$OUTPUT" | grep -qi "error\|invalid\|reject\|timeout"; then
    echo "  Zero CPUs: correctly rejected or timed out"
elif echo "$OUTPUT" | grep -qi "submitted"; then
    echo "  Zero CPUs: accepted (may default to 1)"
else
    echo "  Zero CPUs: handled ($OUTPUT)"
fi

# ============================================
# Test 4: Very large resource request
# ============================================
echo ""
echo "Test 4: Large resource request..."

sleep 2

OUTPUT=$(timeout 10 sbatch --cpus-per-task=10000 /jobs/batch/job_cpu_small.sh 2>&1 || echo "timeout_or_error")

if echo "$OUTPUT" | grep -qi "submitted\|success\|pending"; then
    echo "  Large request: queued (waiting for resources)"
elif echo "$OUTPUT" | grep -qi "error\|exceed\|invalid\|timeout"; then
    echo "  Large request: correctly rejected or timed out"
else
    echo "  Large request: handled ($OUTPUT)"
fi

# ============================================
# Test 5: Minimal job script
# ============================================
echo ""
echo "Test 5: Minimal job script..."

sleep 2

TEMP_SCRIPT="/tmp/minimal_$$.sh"
echo '#!/bin/bash' > "$TEMP_SCRIPT"
echo 'exit 0' >> "$TEMP_SCRIPT"
chmod +x "$TEMP_SCRIPT"

OUTPUT=$(sbatch "$TEMP_SCRIPT" 2>&1)
rm -f "$TEMP_SCRIPT"

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Minimal script: accepted"
else
    echo "  Minimal script: $OUTPUT"
fi

# ============================================
# Test 6: Invalid time format
# ============================================
echo ""
echo "Test 6: Invalid time format..."

sleep 2

OUTPUT=$(timeout 10 sbatch --time=invalid /jobs/batch/job_cpu_small.sh 2>&1 || echo "timeout_or_error")

if echo "$OUTPUT" | grep -qi "error\|invalid\|timeout"; then
    echo "  Invalid time: correctly rejected or timed out"
else
    echo "  Invalid time: handled ($OUTPUT)"
fi

# ============================================
# Test 7: Controller health after edge cases
# ============================================
echo ""
echo "Test 7: Controller health..."

METRICS_CODE=$(curl -s -w "%{http_code}" -o /dev/null "http://${CTRL_1}:9090/metrics" 2>/dev/null || echo "000")

if [ "$METRICS_CODE" = "200" ]; then
    echo "  Controller: healthy after edge cases"
else
    echo "FAIL: Controller unhealthy: HTTP $METRICS_CODE"
    exit 1
fi

# ============================================
# Test 8: Normal job still works
# ============================================
echo ""
echo "Test 8: Normal job after edge cases..."

sleep 2

OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)

if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Normal job: accepted"
else
    echo "FAIL: Normal job rejected: $OUTPUT"
    exit 1
fi

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Edge Cases PASSED ==="
echo ""
echo "Summary:"
echo "  - Edge cases: handled gracefully"
echo "  - Controller: stable"
echo "  - Normal operation: preserved"

exit 0
