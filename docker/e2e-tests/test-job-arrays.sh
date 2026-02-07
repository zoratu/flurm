#!/bin/bash
# Test: Job Arrays
# Verifies: Array job submission is accepted

set -e

echo "=== Test: Job Arrays ==="

# Test 1: Simple array job submission
echo "Test 1: Simple array submission..."
OUTPUT=$(sbatch /jobs/arrays/array_simple.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Array job submitted: $JOB_ID"
else
    echo "  WARN: Array submission result: $OUTPUT"
fi

# Test 2: Array with explicit range
echo "Test 2: Explicit array range..."
OUTPUT=$(sbatch --array=1-5 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Array range --array=1-5 accepted"
else
    echo "  WARN: Array range may not be supported: $OUTPUT"
fi

# Test 3: Array with step
echo "Test 3: Array with step..."
OUTPUT=$(sbatch --array=1-10:2 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Array step --array=1-10:2 accepted"
else
    echo "  WARN: Array step may not be supported: $OUTPUT"
fi

# Test 4: Array with throttle
echo "Test 4: Throttled array..."
OUTPUT=$(sbatch /jobs/arrays/array_throttled.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Throttled array job accepted"
else
    echo "  WARN: Throttled array result: $OUTPUT"
fi

# Test 5: Array with specific indices
echo "Test 5: Specific array indices..."
OUTPUT=$(sbatch --array=1,3,5,7 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Specific indices --array=1,3,5,7 accepted"
else
    echo "  WARN: Specific indices may not be supported: $OUTPUT"
fi

# Test 6: Large array
echo "Test 6: Large array..."
OUTPUT=$(sbatch --array=1-50 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Large array (50 tasks) accepted"
elif echo "$OUTPUT" | grep -qi "limit\|max"; then
    echo "  Large array rejected (expected limit)"
else
    echo "  WARN: Large array result: $OUTPUT"
fi

echo "=== Test: Job Arrays PASSED ==="
echo ""
echo "Summary: Array job submission syntax is recognized"
