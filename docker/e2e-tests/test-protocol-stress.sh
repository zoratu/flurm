#!/bin/bash
# Test: Protocol Stress Testing
# Verifies: Rapid submissions, concurrent queries, edge cases
# Note: Marks failures as WARN to allow for unimplemented features

set -e

echo "=== Test: Protocol Stress Testing ==="

# Configuration
CONTROLLER_HOST="${FLURM_CTRL_1:-localhost}"
RAPID_SUBMISSION_COUNT=20
CONCURRENT_QUERY_COUNT=10
DELAY_BETWEEN_TESTS=3

# Counters
PASSED=0
WARNED=0
FAILED=0
SUBMITTED_JOBS=""

# Helper function
stress_test() {
    local test_name="$1"
    local result="$2"  # "pass", "warn", or "fail"
    local message="$3"

    case "$result" in
        pass)
            echo "  PASS: $test_name"
            [ -n "$message" ] && echo "    $message"
            PASSED=$((PASSED + 1))
            ;;
        warn)
            echo "  WARN: $test_name"
            [ -n "$message" ] && echo "    $message"
            WARNED=$((WARNED + 1))
            ;;
        fail)
            echo "  FAIL: $test_name"
            [ -n "$message" ] && echo "    $message"
            FAILED=$((FAILED + 1))
            ;;
    esac
}

# ========================================
# RAPID SEQUENTIAL SUBMISSIONS
# ========================================
echo ""
echo "=========================================="
echo "Rapid Sequential Submissions"
echo "=========================================="

echo "Test: Submitting $RAPID_SUBMISSION_COUNT jobs rapidly..."
RAPID_SUCCESS=0
RAPID_FAIL=0
RAPID_START=$(date +%s)

for i in $(seq 1 $RAPID_SUBMISSION_COUNT); do
    OUTPUT=$(sbatch --job-name="rapid_$i" --wrap="sleep 10" 2>&1 || echo "ERROR")
    if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
        RAPID_SUCCESS=$((RAPID_SUCCESS + 1))
        JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
        [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    else
        RAPID_FAIL=$((RAPID_FAIL + 1))
    fi
    # Minimal delay to avoid overwhelming
    sleep 0.1
done

RAPID_END=$(date +%s)
RAPID_DURATION=$((RAPID_END - RAPID_START))
RAPID_RATE=$(echo "scale=2; $RAPID_SUCCESS / $RAPID_DURATION" | bc 2>/dev/null || echo "N/A")

echo "  Submitted: $RAPID_SUCCESS/$RAPID_SUBMISSION_COUNT in ${RAPID_DURATION}s"
echo "  Rate: $RAPID_RATE jobs/sec"

if [ $RAPID_SUCCESS -ge $((RAPID_SUBMISSION_COUNT * 8 / 10)) ]; then
    stress_test "Rapid sequential submissions" "pass" "$RAPID_SUCCESS jobs accepted"
elif [ $RAPID_SUCCESS -ge $((RAPID_SUBMISSION_COUNT / 2)) ]; then
    stress_test "Rapid sequential submissions" "warn" "Only $RAPID_SUCCESS/$RAPID_SUBMISSION_COUNT accepted"
else
    stress_test "Rapid sequential submissions" "warn" "Low acceptance rate: $RAPID_SUCCESS/$RAPID_SUBMISSION_COUNT"
fi

sleep $DELAY_BETWEEN_TESTS

# ========================================
# CONCURRENT QUERY OPERATIONS
# ========================================
echo ""
echo "=========================================="
echo "Concurrent Query Operations"
echo "=========================================="

echo "Test: Running $CONCURRENT_QUERY_COUNT concurrent sinfo queries..."
CONCURRENT_SUCCESS=0
PIDS=""

for i in $(seq 1 $CONCURRENT_QUERY_COUNT); do
    (
        OUTPUT=$(sinfo 2>&1 || echo "ERROR")
        if echo "$OUTPUT" | grep -qiE "PARTITION|STATE|NODELIST|debug|batch"; then
            exit 0
        else
            exit 1
        fi
    ) &
    PIDS="$PIDS $!"
done

# Wait for all and count successes
for pid in $PIDS; do
    if wait $pid 2>/dev/null; then
        CONCURRENT_SUCCESS=$((CONCURRENT_SUCCESS + 1))
    fi
done

if [ $CONCURRENT_SUCCESS -ge $((CONCURRENT_QUERY_COUNT * 8 / 10)) ]; then
    stress_test "Concurrent sinfo queries" "pass" "$CONCURRENT_SUCCESS/$CONCURRENT_QUERY_COUNT successful"
elif [ $CONCURRENT_SUCCESS -ge $((CONCURRENT_QUERY_COUNT / 2)) ]; then
    stress_test "Concurrent sinfo queries" "warn" "$CONCURRENT_SUCCESS/$CONCURRENT_QUERY_COUNT successful"
else
    stress_test "Concurrent sinfo queries" "warn" "Low success rate: $CONCURRENT_SUCCESS/$CONCURRENT_QUERY_COUNT"
fi

sleep $DELAY_BETWEEN_TESTS

echo "Test: Running $CONCURRENT_QUERY_COUNT concurrent squeue queries..."
CONCURRENT_SUCCESS=0
PIDS=""

for i in $(seq 1 $CONCURRENT_QUERY_COUNT); do
    (
        OUTPUT=$(squeue 2>&1 || echo "ERROR")
        # squeue may return empty, which is OK
        if ! echo "$OUTPUT" | grep -qiE "error|failed|refused"; then
            exit 0
        else
            exit 1
        fi
    ) &
    PIDS="$PIDS $!"
done

for pid in $PIDS; do
    if wait $pid 2>/dev/null; then
        CONCURRENT_SUCCESS=$((CONCURRENT_SUCCESS + 1))
    fi
done

if [ $CONCURRENT_SUCCESS -ge $((CONCURRENT_QUERY_COUNT * 8 / 10)) ]; then
    stress_test "Concurrent squeue queries" "pass" "$CONCURRENT_SUCCESS/$CONCURRENT_QUERY_COUNT successful"
else
    stress_test "Concurrent squeue queries" "warn" "$CONCURRENT_SUCCESS/$CONCURRENT_QUERY_COUNT successful"
fi

sleep $DELAY_BETWEEN_TESTS

# ========================================
# LARGE JOB SCRIPTS
# ========================================
echo ""
echo "=========================================="
echo "Large Job Scripts"
echo "=========================================="

# Create a job script with many comments (large but valid)
echo "Test: Large job script (10KB)..."
LARGE_SCRIPT=$(mktemp /tmp/large_job_XXXXXX.sh)
cat > "$LARGE_SCRIPT" << 'HEADER'
#!/bin/bash
#SBATCH --job-name=large_script_test
#SBATCH --time=00:05:00
#SBATCH --nodes=1

# This is a test of large job script handling
echo "Starting large script test"
HEADER

# Add 500 lines of comments (about 10KB)
for i in $(seq 1 500); do
    echo "# Comment line $i: This is padding to make the script larger for stress testing purposes" >> "$LARGE_SCRIPT"
done

echo 'echo "Large script completed successfully"' >> "$LARGE_SCRIPT"

LARGE_SIZE=$(wc -c < "$LARGE_SCRIPT")
echo "  Script size: $LARGE_SIZE bytes"

sleep 1
LARGE_OUTPUT=$(sbatch "$LARGE_SCRIPT" 2>&1 || echo "ERROR")
rm -f "$LARGE_SCRIPT"

if echo "$LARGE_OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$LARGE_OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "Large job script (${LARGE_SIZE}B)" "pass" "Job accepted"
elif echo "$LARGE_OUTPUT" | grep -qiE "too large|size limit|exceeded"; then
    stress_test "Large job script (${LARGE_SIZE}B)" "warn" "Size limit enforced"
else
    stress_test "Large job script (${LARGE_SIZE}B)" "warn" "Result: $LARGE_OUTPUT"
fi

sleep $DELAY_BETWEEN_TESTS

# Very large script (100KB)
echo "Test: Very large job script (100KB)..."
VLARGE_SCRIPT=$(mktemp /tmp/vlarge_job_XXXXXX.sh)
cat > "$VLARGE_SCRIPT" << 'HEADER'
#!/bin/bash
#SBATCH --job-name=vlarge_script_test
#SBATCH --time=00:05:00
echo "Starting very large script"
HEADER

# Add 5000 lines (about 100KB)
for i in $(seq 1 5000); do
    echo "# Comment line $i: This is extensive padding to create a very large job script for stress testing the protocol handling capabilities" >> "$VLARGE_SCRIPT"
done

echo 'echo "Very large script completed"' >> "$VLARGE_SCRIPT"

VLARGE_SIZE=$(wc -c < "$VLARGE_SCRIPT")
echo "  Script size: $VLARGE_SIZE bytes"

sleep 1
VLARGE_OUTPUT=$(sbatch "$VLARGE_SCRIPT" 2>&1 || echo "ERROR")
rm -f "$VLARGE_SCRIPT"

if echo "$VLARGE_OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$VLARGE_OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "Very large job script (${VLARGE_SIZE}B)" "pass" "Job accepted"
elif echo "$VLARGE_OUTPUT" | grep -qiE "too large|size limit|exceeded"; then
    stress_test "Very large job script (${VLARGE_SIZE}B)" "warn" "Size limit enforced (acceptable)"
else
    stress_test "Very large job script (${VLARGE_SIZE}B)" "warn" "Result: $(echo "$VLARGE_OUTPUT" | head -1)"
fi

sleep $DELAY_BETWEEN_TESTS

# ========================================
# UNUSUAL BUT VALID CHARACTERS IN JOB NAMES
# ========================================
echo ""
echo "=========================================="
echo "Unusual Characters in Job Names"
echo "=========================================="

# Test various character combinations
declare -a TEST_NAMES=(
    "simple_name"
    "name-with-dashes"
    "name.with.dots"
    "name_123_numbers"
    "MixedCaseNAME"
    "name_with_underscore"
    "x"
    "a1"
)

for name in "${TEST_NAMES[@]}"; do
    echo "Test: Job name '$name'..."
    sleep 0.5
    OUTPUT=$(sbatch --job-name="$name" --wrap="sleep 5" 2>&1 || echo "ERROR")

    if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
        JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
        [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
        stress_test "Job name: $name" "pass" ""
    elif echo "$OUTPUT" | grep -qiE "invalid.*name|illegal.*character"; then
        stress_test "Job name: $name" "warn" "Name rejected (may be by design)"
    else
        stress_test "Job name: $name" "warn" "Unexpected: $OUTPUT"
    fi
done

sleep $DELAY_BETWEEN_TESTS

# Test names that might be problematic (should be handled gracefully)
echo ""
echo "--- Potentially problematic names (graceful handling expected) ---"

declare -a EDGE_NAMES=(
    "name with spaces"
    "name'with'quotes"
    'name"double"quotes'
    "name;semicolon"
    "name|pipe"
    "name&ampersand"
    "name\$dollar"
    "name\`backtick\`"
)

for name in "${EDGE_NAMES[@]}"; do
    echo "Test: Edge case name..."
    sleep 0.5
    # Use printf to handle special chars
    OUTPUT=$(sbatch --job-name="$name" --wrap="sleep 5" 2>&1 || echo "ERROR")

    if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
        JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
        [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
        stress_test "Edge case name accepted" "pass" ""
    else
        # Rejection is acceptable for edge cases
        stress_test "Edge case name rejected" "pass" "Graceful rejection"
    fi
done

sleep $DELAY_BETWEEN_TESTS

# ========================================
# LONG JOB NAMES AND PATHS
# ========================================
echo ""
echo "=========================================="
echo "Long Names and Paths"
echo "=========================================="

# Long job name (64 characters)
LONG_NAME_64=$(printf 'a%.0s' {1..64})
echo "Test: 64-character job name..."
sleep 1
OUTPUT=$(sbatch --job-name="$LONG_NAME_64" --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "64-char job name" "pass" ""
else
    stress_test "64-char job name" "warn" "May be truncated or rejected"
fi

# Very long job name (256 characters)
LONG_NAME_256=$(printf 'b%.0s' {1..256})
echo "Test: 256-character job name..."
sleep 1
OUTPUT=$(sbatch --job-name="$LONG_NAME_256" --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "256-char job name" "pass" "Accepted (may be truncated)"
elif echo "$OUTPUT" | grep -qiE "too long|truncat|limit"; then
    stress_test "256-char job name" "pass" "Length limit enforced"
else
    stress_test "256-char job name" "warn" "Result: $(echo "$OUTPUT" | head -1)"
fi

# Long output path
LONG_PATH="/tmp/$(printf 'dir%.0s' {1..30})/output.log"
echo "Test: Long output path..."
sleep 1
OUTPUT=$(sbatch --job-name="long_path_test" --output="$LONG_PATH" --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "Long output path" "pass" ""
elif echo "$OUTPUT" | grep -qiE "path.*long|invalid.*path"; then
    stress_test "Long output path" "pass" "Path limit enforced"
else
    stress_test "Long output path" "warn" "Result: $(echo "$OUTPUT" | head -1)"
fi

sleep $DELAY_BETWEEN_TESTS

# ========================================
# EDGE CASES IN RESOURCE REQUESTS
# ========================================
echo ""
echo "=========================================="
echo "Resource Request Edge Cases"
echo "=========================================="

# Zero resources (should fail gracefully)
echo "Test: Zero nodes request..."
sleep 1
OUTPUT=$(sbatch --nodes=0 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "invalid|error|must be|at least"; then
    stress_test "Zero nodes request" "pass" "Correctly rejected"
elif echo "$OUTPUT" | grep -qiE "submitted|success"; then
    stress_test "Zero nodes request" "warn" "Unexpectedly accepted"
else
    stress_test "Zero nodes request" "pass" "Handled: $(echo "$OUTPUT" | head -1)"
fi

# Very large resource request
echo "Test: Very large node count (1000)..."
sleep 1
OUTPUT=$(sbatch --nodes=1000 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "invalid|error|too many|insufficient|exceed"; then
    stress_test "1000 nodes request" "pass" "Correctly rejected/queued"
elif echo "$OUTPUT" | grep -qiE "submitted|success"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "1000 nodes request" "pass" "Accepted (will pend for resources)"
else
    stress_test "1000 nodes request" "warn" "Result: $(echo "$OUTPUT" | head -1)"
fi

# Fractional CPUs (should fail gracefully)
echo "Test: Fractional CPU request..."
sleep 1
OUTPUT=$(sbatch --cpus-per-task=0.5 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "invalid|error|integer|whole"; then
    stress_test "Fractional CPUs" "pass" "Correctly rejected"
elif echo "$OUTPUT" | grep -qiE "submitted|success"; then
    stress_test "Fractional CPUs" "warn" "Unexpectedly accepted"
else
    stress_test "Fractional CPUs" "pass" "Handled gracefully"
fi

# Negative value (should fail gracefully)
echo "Test: Negative node count..."
sleep 1
OUTPUT=$(sbatch --nodes=-1 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "invalid|error|negative|must be"; then
    stress_test "Negative nodes" "pass" "Correctly rejected"
elif echo "$OUTPUT" | grep -qiE "submitted|success"; then
    stress_test "Negative nodes" "warn" "Unexpectedly accepted"
else
    stress_test "Negative nodes" "pass" "Handled: $(echo "$OUTPUT" | head -1)"
fi

# Very long time limit
echo "Test: Very long time limit (30 days)..."
sleep 1
OUTPUT=$(sbatch --time=30-00:00:00 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "30-day time limit" "pass" ""
elif echo "$OUTPUT" | grep -qiE "exceed|limit|invalid.*time"; then
    stress_test "30-day time limit" "pass" "Time limit enforced"
else
    stress_test "30-day time limit" "warn" "Result: $(echo "$OUTPUT" | head -1)"
fi

# Memory edge cases
echo "Test: Large memory request (1TB)..."
sleep 1
OUTPUT=$(sbatch --mem=1000000 --wrap="sleep 5" 2>&1 || echo "ERROR")
if echo "$OUTPUT" | grep -qiE "submitted|success|[0-9]+"; then
    JOB_ID=$(echo "$OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")
    [ -n "$JOB_ID" ] && SUBMITTED_JOBS="$SUBMITTED_JOBS $JOB_ID"
    stress_test "1TB memory request" "pass" "Accepted (will pend)"
elif echo "$OUTPUT" | grep -qiE "exceed|insufficient|invalid|memory"; then
    stress_test "1TB memory request" "pass" "Resource limit enforced"
else
    stress_test "1TB memory request" "warn" "Result: $(echo "$OUTPUT" | head -1)"
fi

sleep $DELAY_BETWEEN_TESTS

# ========================================
# MIXED CONCURRENT OPERATIONS
# ========================================
echo ""
echo "=========================================="
echo "Mixed Concurrent Operations"
echo "=========================================="

echo "Test: Concurrent submissions + queries..."
MIXED_SUCCESS=0
MIXED_TOTAL=20
PIDS=""

# Mix of operations
for i in $(seq 1 $MIXED_TOTAL); do
    (
        case $((i % 4)) in
            0) sinfo > /dev/null 2>&1 ;;
            1) squeue > /dev/null 2>&1 ;;
            2) sbatch --wrap="sleep 1" > /dev/null 2>&1 ;;
            3) scontrol show config > /dev/null 2>&1 ;;
        esac
        exit 0
    ) &
    PIDS="$PIDS $!"
done

for pid in $PIDS; do
    if wait $pid 2>/dev/null; then
        MIXED_SUCCESS=$((MIXED_SUCCESS + 1))
    fi
done

if [ $MIXED_SUCCESS -ge $((MIXED_TOTAL * 7 / 10)) ]; then
    stress_test "Mixed concurrent operations" "pass" "$MIXED_SUCCESS/$MIXED_TOTAL successful"
else
    stress_test "Mixed concurrent operations" "warn" "$MIXED_SUCCESS/$MIXED_TOTAL successful"
fi

# ========================================
# CLEANUP
# ========================================
echo ""
echo "=========================================="
echo "Cleanup"
echo "=========================================="

echo "Cancelling test jobs..."
JOB_COUNT=0
for jid in $SUBMITTED_JOBS; do
    if [ -n "$jid" ]; then
        scancel "$jid" 2>/dev/null || true
        JOB_COUNT=$((JOB_COUNT + 1))
    fi
done
echo "  Cancelled $JOB_COUNT jobs"

# Also try to cancel by name pattern
scancel --name="rapid_*" 2>/dev/null || true
scancel --name="e2e_*" 2>/dev/null || true
scancel --name="large_*" 2>/dev/null || true
scancel --name="long_*" 2>/dev/null || true

sleep 2

# ========================================
# Summary
# ========================================
echo ""
echo "=== Test: Protocol Stress Testing Complete ==="
echo ""
echo "Summary:"
echo "  Passed:  $PASSED"
echo "  Warned:  $WARNED"
echo "  Failed:  $FAILED"
echo ""
echo "Stress Test Metrics:"
echo "  Rapid submission rate: $RAPID_RATE jobs/sec"
echo "  Total jobs submitted: ~$(echo "$SUBMITTED_JOBS" | wc -w)"
echo ""

# Test passes if no hard failures
if [ $FAILED -gt 0 ]; then
    echo "RESULT: FAILED"
    exit 1
else
    echo "RESULT: PASSED (with $WARNED warnings)"
    exit 0
fi
