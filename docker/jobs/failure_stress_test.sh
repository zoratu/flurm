#!/bin/bash
# FLURM Failure Stress Test
# Submits jobs with various failure modes to test error handling

set -e

echo "=============================================="
echo "     FLURM Failure Stress Test"
echo "=============================================="
echo "Testing various failure scenarios..."
echo ""

TOTAL=0
SUCCESS=0
EXPECTED_FAIL=0

submit_and_track() {
    local desc="$1"
    local script="$2"
    local expect_fail="${3:-false}"

    ((TOTAL++))
    echo -n "[$TOTAL] $desc... "

    RESULT=$(sbatch "$script" 2>&1)
    if echo "$RESULT" | grep -q "Submitted batch job"; then
        JOB_ID=$(echo "$RESULT" | grep -o '[0-9]*' | tail -1)
        echo "submitted (job $JOB_ID)"
        if [ "$expect_fail" = "true" ]; then
            ((EXPECTED_FAIL++))
        else
            ((SUCCESS++))
        fi
    else
        echo "FAILED: $RESULT"
    fi
}

# Create various test scripts
mkdir -p /tmp/failure_tests

# Test 1: Normal job (should succeed)
cat > /tmp/failure_tests/normal.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=normal_test
#SBATCH --time=00:01:00
echo "Normal job running"
exit 0
EOF

# Test 2: Job that exits with error
cat > /tmp/failure_tests/exit_error.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=exit_error
#SBATCH --time=00:01:00
echo "Job exiting with error"
exit 42
EOF

# Test 3: Job that times out (longer than time limit)
cat > /tmp/failure_tests/timeout.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=timeout_test
#SBATCH --time=00:00:05
echo "This job will timeout..."
sleep 60
echo "Should never reach here"
EOF

# Test 4: Job with invalid command
cat > /tmp/failure_tests/invalid_cmd.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=invalid_cmd
#SBATCH --time=00:01:00
this_command_does_not_exist
EOF

# Test 5: Job that crashes (segfault simulation)
cat > /tmp/failure_tests/crash.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=crash_test
#SBATCH --time=00:01:00
echo "Simulating crash..."
kill -SEGV $$
EOF

# Test 6: Job with memory allocation failure
cat > /tmp/failure_tests/memory.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=memory_test
#SBATCH --time=00:01:00
#SBATCH --mem=10M
echo "Trying to allocate too much memory..."
# Try to allocate more than allowed
dd if=/dev/zero of=/tmp/bigfile bs=1M count=100 2>&1 || true
rm -f /tmp/bigfile
exit 0
EOF

# Test 7: Job with missing dependency
cat > /tmp/failure_tests/missing_dep.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=missing_dep
#SBATCH --time=00:01:00
#SBATCH --dependency=afterok:99999
echo "This should wait for non-existent job"
EOF

# Test 8: Rapid resubmission test
cat > /tmp/failure_tests/rapid.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=rapid_test
#SBATCH --time=00:00:30
echo "Rapid test job"
exit 0
EOF

chmod +x /tmp/failure_tests/*.sh

echo ""
echo "=== Submitting Test Jobs ==="
echo ""

submit_and_track "Normal job" /tmp/failure_tests/normal.sh
submit_and_track "Exit with error code 42" /tmp/failure_tests/exit_error.sh true
submit_and_track "Timeout job (5s limit, 60s sleep)" /tmp/failure_tests/timeout.sh true
submit_and_track "Invalid command" /tmp/failure_tests/invalid_cmd.sh true
submit_and_track "Crash simulation" /tmp/failure_tests/crash.sh true
submit_and_track "Memory test" /tmp/failure_tests/memory.sh
submit_and_track "Missing dependency" /tmp/failure_tests/missing_dep.sh

echo ""
echo "=== Rapid Submission Test (20 jobs in quick succession) ==="
echo ""
RAPID_START=$(date +%s%N)
for i in $(seq 1 20); do
    submit_and_track "Rapid job $i" /tmp/failure_tests/rapid.sh
done
RAPID_END=$(date +%s%N)
RAPID_MS=$(( (RAPID_END - RAPID_START) / 1000000 ))
echo "Rapid submission: 20 jobs in ${RAPID_MS}ms ($((20000 / (RAPID_MS + 1))) jobs/sec)"

echo ""
echo "=============================================="
echo "     Submission Summary"
echo "=============================================="
echo "Total submitted:    $TOTAL"
echo "Successful submits: $SUCCESS"
echo "Expected failures:  $EXPECTED_FAIL"
echo ""

echo "Current queue:"
squeue
echo ""

echo "Waiting 30 seconds for jobs to process..."
sleep 30

echo ""
echo "Final queue state:"
squeue
echo ""

# Cleanup
rm -rf /tmp/failure_tests
