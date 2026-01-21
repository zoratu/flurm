#!/bin/bash
#
# FLURM CLI Compatibility Test Suite
#
# Tests SLURM CLI tool compatibility with FLURM.
# Run inside the Docker environment or with SLURM_CONF pointing to FLURM.
#
# Usage: ./test-cli.sh [--skip-srun]
#
# Exit codes:
#   0 - All tests passed
#   1 - Some tests failed
#

PASSED=0
FAILED=0
SKIPPED=0

# Parse arguments
SKIP_SRUN=false
for arg in "$@"; do
    case $arg in
        --skip-srun)
            SKIP_SRUN=true
            shift
            ;;
    esac
done

pass() {
    echo "[PASS] $1"
    PASSED=$((PASSED + 1))
}

fail() {
    echo "[FAIL] $1"
    echo "       $2"
    FAILED=$((FAILED + 1))
}

skip() {
    echo "[SKIP] $1"
    SKIPPED=$((SKIPPED + 1))
}

echo "========================================"
echo "FLURM CLI Compatibility Test Suite"
echo "========================================"
echo ""

# Test 1: sbatch basic submission
echo "Test 1: sbatch basic submission"
OUTPUT=$(sbatch --wrap="echo Hello from FLURM" 2>&1)
if echo "$OUTPUT" | grep -q "Submitted batch job"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+$')
    pass "sbatch submitted job $JOB_ID"
else
    fail "sbatch basic submission" "$OUTPUT"
fi

# Test 2: sbatch with job name
echo "Test 2: sbatch with job name"
OUTPUT=$(sbatch --job-name=test_job --wrap="echo test" 2>&1)
if echo "$OUTPUT" | grep -q "Submitted batch job"; then
    pass "sbatch with --job-name"
else
    fail "sbatch with --job-name" "$OUTPUT"
fi

# Test 3: squeue
echo "Test 3: squeue"
OUTPUT=$(squeue 2>&1)
if [ $? -eq 0 ] && echo "$OUTPUT" | grep -q "JOBID"; then
    pass "squeue"
else
    fail "squeue" "$OUTPUT"
fi

# Test 4: scancel
echo "Test 4: scancel"
OUTPUT=$(sbatch --wrap="sleep 30" 2>&1)
if echo "$OUTPUT" | grep -q "Submitted batch job"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+$')
    sleep 1
    CANCEL_OUTPUT=$(scancel "$JOB_ID" 2>&1)
    if [ $? -eq 0 ]; then
        pass "scancel job $JOB_ID"
    else
        fail "scancel" "$CANCEL_OUTPUT"
    fi
else
    fail "scancel (could not submit test job)" "$OUTPUT"
fi

# Test 5: scontrol show job
echo "Test 5: scontrol show job"
OUTPUT=$(sbatch --wrap="echo test" 2>&1)
if echo "$OUTPUT" | grep -q "Submitted batch job"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+$')
    sleep 1
    SCONTROL_OUTPUT=$(scontrol show job "$JOB_ID" 2>&1)
    if [ $? -eq 0 ] && echo "$SCONTROL_OUTPUT" | grep -q "JobId"; then
        pass "scontrol show job"
    else
        fail "scontrol show job" "$SCONTROL_OUTPUT"
    fi
else
    fail "scontrol show job (could not submit test job)" "$OUTPUT"
fi

# Test 6: scontrol show partition
echo "Test 6: scontrol show partition"
OUTPUT=$(scontrol show partition 2>&1)
if [ $? -eq 0 ] && echo "$OUTPUT" | grep -q "PartitionName"; then
    pass "scontrol show partition"
else
    fail "scontrol show partition" "$OUTPUT"
fi

# Test 7: scontrol show node
echo "Test 7: scontrol show node"
OUTPUT=$(scontrol show node 2>&1)
if [ $? -eq 0 ] && echo "$OUTPUT" | grep -q "NodeName"; then
    pass "scontrol show node"
else
    fail "scontrol show node" "$OUTPUT"
fi

# Test 8: sinfo
echo "Test 8: sinfo"
OUTPUT=$(sinfo 2>&1)
if [ $? -eq 0 ] && echo "$OUTPUT" | grep -q "PARTITION"; then
    pass "sinfo"
else
    fail "sinfo" "$OUTPUT"
fi

# Test 9: srun (interactive job)
echo "Test 9: srun"
if [ "$SKIP_SRUN" = true ]; then
    skip "srun (--skip-srun flag set, known protocol issue)"
else
    OUTPUT=$(timeout 10 srun echo "Hello from srun" 2>&1)
    if echo "$OUTPUT" | grep -q "Hello from srun"; then
        pass "srun"
    else
        fail "srun (known issue: RESPONSE_RESOURCE_ALLOCATION format)" "$OUTPUT"
    fi
fi

# Test 10: Multiple rapid job submissions
echo "Test 10: Multiple rapid submissions (10 jobs)"
SUCCESS=0
for i in 1 2 3 4 5 6 7 8 9 10; do
    OUTPUT=$(sbatch --wrap="echo job $i" 2>&1)
    if echo "$OUTPUT" | grep -q "Submitted batch job"; then
        SUCCESS=$((SUCCESS + 1))
    fi
done
if [ $SUCCESS -eq 10 ]; then
    pass "Multiple rapid submissions ($SUCCESS/10)"
else
    fail "Multiple rapid submissions" "$SUCCESS/10 succeeded"
fi

# Summary
echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo "Passed:  $PASSED"
echo "Failed:  $FAILED"
echo "Skipped: $SKIPPED"
echo ""

if [ $FAILED -gt 0 ]; then
    echo "Some tests failed!"
    exit 1
else
    echo "All tests passed!"
    exit 0
fi
