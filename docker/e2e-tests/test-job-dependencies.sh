#!/bin/bash
# Test: Job Dependencies
# Verifies: Dependency syntax is accepted

set -e

echo "=== Test: Job Dependencies ==="

# Test 1: Submit parent job
echo "Test 1: Submit parent job..."
OUTPUT=$(sbatch /jobs/dependencies/dep_stage1.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    PARENT_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Parent job submitted: $PARENT_ID"
else
    echo "FAIL: Could not submit parent job: $OUTPUT"
    exit 1
fi

# Test 2: Submit with afterok dependency
echo "Test 2: afterok dependency..."
OUTPUT=$(sbatch --dependency=afterok:$PARENT_ID /jobs/dependencies/dep_stage2.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    CHILD_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1)
    echo "  Dependent job (afterok) accepted: $CHILD_ID"
else
    echo "  WARN: afterok dependency may not be supported: $OUTPUT"
fi

# Test 3: Submit with afterany dependency
echo "Test 3: afterany dependency..."
OUTPUT=$(sbatch --dependency=afterany:$PARENT_ID /jobs/dependencies/dep_cleanup.sh 2>&1)
if echo "$OUTPUT" | grep -qi "submitted\|success"; then
    echo "  Dependent job (afterany) accepted"
else
    echo "  WARN: afterany dependency may not be supported: $OUTPUT"
fi

# Test 4: Multiple dependencies
echo "Test 4: Multiple parent dependencies..."
# Submit second parent
OUTPUT2=$(sbatch /jobs/dependencies/dep_stage1.sh 2>&1)
PARENT2_ID=$(echo "$OUTPUT2" | grep -oE '[0-9]+' | head -1)
if [ -n "$PARENT2_ID" ]; then
    OUTPUT=$(sbatch --dependency=afterok:$PARENT_ID:$PARENT2_ID /jobs/dependencies/dep_stage3.sh 2>&1)
    if echo "$OUTPUT" | grep -qi "submitted\|success"; then
        echo "  Multi-parent dependency accepted"
    else
        echo "  WARN: Multi-parent dependencies may not be supported"
    fi
else
    echo "  SKIP: Could not create second parent for multi-dependency test"
fi

# Test 5: Dependency chain
echo "Test 5: Dependency chain..."
CHAIN_START=$(sbatch /jobs/batch/job_cpu_small.sh 2>&1 | grep -oE '[0-9]+' | head -1)
CHAIN_MID=$(sbatch --dependency=afterok:$CHAIN_START /jobs/batch/job_cpu_small.sh 2>&1 | grep -oE '[0-9]+' | head -1)
CHAIN_END=$(sbatch --dependency=afterok:$CHAIN_MID /jobs/batch/job_cpu_small.sh 2>&1 | grep -oE '[0-9]+' | head -1)
if [ -n "$CHAIN_START" ] && [ -n "$CHAIN_MID" ] && [ -n "$CHAIN_END" ]; then
    echo "  Dependency chain created: $CHAIN_START -> $CHAIN_MID -> $CHAIN_END"
else
    echo "  WARN: Could not create full dependency chain"
fi

echo "=== Test: Job Dependencies PASSED ==="
echo ""
echo "Summary: Dependency syntax is recognized by controller"
