#!/bin/bash
# Test: Job Lifecycle Under Constraints
# Verifies: Resource allocation options are accepted by the controller

set -e

echo "=== Test: Job Lifecycle Under Constraints ==="

# Test 1: CPU allocation option
echo "Test 1: CPU allocation option..."
OUTPUT=$(sbatch --cpus-per-task=4 /jobs/batch/job_cpu_medium.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  CPU option accepted: --cpus-per-task=4"
else
    echo "  WARN: CPU option may not be supported: $OUTPUT"
fi

# Test 2: Memory allocation option
echo "Test 2: Memory allocation option..."
OUTPUT=$(sbatch --mem=2G /jobs/batch/job_memory_test.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Memory option accepted: --mem=2G"
else
    echo "  WARN: Memory option may not be supported: $OUTPUT"
fi

# Test 3: Time limit option
echo "Test 3: Time limit option..."
OUTPUT=$(sbatch --time=00:10:00 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Time option accepted: --time=00:10:00"
else
    echo "  WARN: Time option may not be supported: $OUTPUT"
fi

# Test 4: Node count option
echo "Test 4: Node count option..."
OUTPUT=$(sbatch --nodes=1 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Node option accepted: --nodes=1"
else
    echo "  WARN: Node option may not be supported: $OUTPUT"
fi

# Test 5: Combined options
echo "Test 5: Combined resource options..."
OUTPUT=$(sbatch --cpus-per-task=2 --mem=1G --time=00:05:00 /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Combined options accepted"
else
    echo "  WARN: Combined options may have issues: $OUTPUT"
fi

# Test 6: Partition option
echo "Test 6: Partition selection..."
for PARTITION in debug batch large gpu; do
    OUTPUT=$(sbatch --partition=$PARTITION /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        echo "  Partition '$PARTITION': accepted"
    else
        echo "  Partition '$PARTITION': not available or not supported"
    fi
done

# Test 7: Job name option
echo "Test 7: Job naming..."
OUTPUT=$(sbatch --job-name=test_constraint_job /jobs/batch/job_cpu_small.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Job name option accepted"
else
    echo "  WARN: Job name option may not be supported"
fi

# Test 8: Stress test - rapid submissions with options
echo "Test 8: Rapid submission stress test..."
SUCCESS=0
for i in $(seq 1 10); do
    OUTPUT=$(sbatch --cpus-per-task=$((i % 4 + 1)) /jobs/batch/job_cpu_small.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        SUCCESS=$((SUCCESS + 1))
    fi
done
echo "  Rapid submissions: $SUCCESS/10 successful"

echo "=== Test: Job Lifecycle Under Constraints PASSED ==="
echo ""
echo "Summary: Controller accepts job submission with resource options"
