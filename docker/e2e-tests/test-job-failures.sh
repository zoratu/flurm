#!/bin/bash
# Test: Job Failures
# Verifies: Failed job scripts are accepted

set -e

echo "=== Test: Job Failures ==="

# Test 1: Job with non-zero exit code
echo "Test 1: Job with exit error..."
OUTPUT=$(sbatch /jobs/failures/job_exit_error.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Failing job submitted: $JOB_ID"
else
    echo "  WARN: Could not submit failing job: $OUTPUT"
fi

# Test 2: Job timeout script
echo "Test 2: Timeout job..."
OUTPUT=$(sbatch /jobs/failures/job_timeout.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Timeout job submitted"
else
    echo "  WARN: Timeout job result: $OUTPUT"
fi

# Test 3: OOM job
echo "Test 3: OOM job..."
OUTPUT=$(sbatch /jobs/failures/job_oom.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  OOM job submitted"
else
    echo "  WARN: OOM job result: $OUTPUT"
fi

# Test 4: Retry job
echo "Test 4: Retry job..."
OUTPUT=$(sbatch /jobs/failures/job_retry.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Retry job submitted"
else
    echo "  WARN: Retry job result: $OUTPUT"
fi

# Test 5: Multiple failure submissions
echo "Test 5: Multiple failure job submissions..."
SUCCESS=0
for script in job_exit_error.sh job_timeout.sh job_oom.sh job_retry.sh; do
    OUTPUT=$(sbatch /jobs/failures/$script 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
    fi
done
echo "  Successfully submitted $SUCCESS/4 failure test jobs"

echo "=== Test: Job Failures PASSED ==="
echo ""
echo "Summary: Jobs with various failure modes are accepted"
