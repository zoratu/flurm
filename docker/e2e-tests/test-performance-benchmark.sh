#!/bin/bash
# Test: Performance Benchmark
# Verifies: Job submission latency, query latency

set -e

echo "=== Test: Performance Benchmark ==="

CTRL_1="${FLURM_CTRL_1:-localhost}"

# ============================================
# Test 1: Job Submission Latency
# ============================================
echo "Test 1: Job Submission Latency (5 samples)..."

TOTAL_MS=0
SUCCESS=0

for i in $(seq 1 5); do
    START=$(date +%s%N 2>/dev/null || echo "0")
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    END=$(date +%s%N 2>/dev/null || echo "1000000000")

    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
        LATENCY=$(( (END - START) / 1000000 ))
        TOTAL_MS=$((TOTAL_MS + LATENCY))
        echo "  Sample $i: ${LATENCY}ms"
    fi
    sleep 1
done

if [ $SUCCESS -gt 0 ]; then
    AVG=$((TOTAL_MS / SUCCESS))
    echo "  Average: ${AVG}ms over $SUCCESS samples"
else
    echo "FAIL: No successful submissions"
    exit 1
fi

# ============================================
# Test 2: squeue Query Latency
# ============================================
echo ""
echo "Test 2: squeue Query Latency..."

START=$(date +%s%N 2>/dev/null || echo "0")
squeue >/dev/null 2>&1 || true
END=$(date +%s%N 2>/dev/null || echo "1000000000")
SQUEUE_LATENCY=$(( (END - START) / 1000000 ))

echo "  squeue: ${SQUEUE_LATENCY}ms"

# ============================================
# Test 3: Metrics Endpoint Latency
# ============================================
echo ""
echo "Test 3: Metrics Endpoint Latency..."

START=$(date +%s%N 2>/dev/null || echo "0")
curl -s "http://${CTRL_1}:9090/metrics" >/dev/null 2>&1 || true
END=$(date +%s%N 2>/dev/null || echo "1000000000")
METRICS_LATENCY=$(( (END - START) / 1000000 ))

echo "  Metrics: ${METRICS_LATENCY}ms"

# ============================================
# Test 4: Throughput Test
# ============================================
echo ""
echo "Test 4: Throughput (5 rapid submissions)..."

sleep 3
START=$(date +%s)
SUBMITTED=0

for i in $(seq 1 5); do
    OUTPUT=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUBMITTED=$((SUBMITTED + 1))
    fi
done

END=$(date +%s)
DURATION=$((END - START))
if [ $DURATION -eq 0 ]; then DURATION=1; fi

THROUGHPUT=$((SUBMITTED / DURATION))
echo "  Submitted: $SUBMITTED in ${DURATION}s = ${THROUGHPUT} jobs/sec"

# ============================================
# Summary
# ============================================
echo ""
echo "=== Test: Performance Benchmark PASSED ==="
echo ""
echo "Summary:"
echo "  - Submission latency: ${AVG}ms avg"
echo "  - squeue latency: ${SQUEUE_LATENCY}ms"
echo "  - Metrics latency: ${METRICS_LATENCY}ms"
echo "  - Throughput: ~${THROUGHPUT} jobs/sec"

exit 0
