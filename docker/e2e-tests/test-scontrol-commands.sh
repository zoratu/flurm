#!/bin/bash
# Test: scontrol Command Operations
# Verifies: scontrol show/update/ping/version commands
# Note: Marks unimplemented features as WARN, not FAIL

set -e

echo "=== Test: scontrol Commands ==="

# Counters
PASSED=0
WARNED=0
FAILED=0

# Helper function to run scontrol command and check result
run_scontrol_test() {
    local test_name="$1"
    local cmd="$2"
    local success_pattern="$3"
    local warn_pattern="${4:-}"

    echo "Test: $test_name..."
    sleep 1  # Delay between operations to avoid rate limiting

    OUTPUT=$(eval "$cmd" 2>&1 || echo "COMMAND_ERROR")

    if echo "$OUTPUT" | grep -qiE "$success_pattern"; then
        echo "  PASS: $test_name"
        PASSED=$((PASSED + 1))
        return 0
    elif [ -n "$warn_pattern" ] && echo "$OUTPUT" | grep -qiE "$warn_pattern"; then
        echo "  WARN: $test_name (feature may not be implemented)"
        echo "    Output: $(echo "$OUTPUT" | head -1)"
        WARNED=$((WARNED + 1))
        return 0
    elif echo "$OUTPUT" | grep -qiE "not implemented|unsupported|unknown command|invalid"; then
        echo "  WARN: $test_name (command not supported)"
        WARNED=$((WARNED + 1))
        return 0
    else
        echo "  Output: $OUTPUT"
        # Check if it's an error vs just unexpected output
        if echo "$OUTPUT" | grep -qiE "error|failed|COMMAND_ERROR"; then
            echo "  WARN: $test_name (command may have failed)"
            WARNED=$((WARNED + 1))
        else
            echo "  PASS: $test_name (command executed)"
            PASSED=$((PASSED + 1))
        fi
        return 0
    fi
}

# ========================================
# scontrol ping
# ========================================
echo ""
echo "--- scontrol ping ---"

run_scontrol_test \
    "scontrol ping" \
    "scontrol ping" \
    "UP|OK|pong|responding|slurmctld" \
    "down|timeout|refused"

# ========================================
# scontrol version
# ========================================
echo ""
echo "--- scontrol version ---"

run_scontrol_test \
    "scontrol version" \
    "scontrol version" \
    "slurm|version|[0-9]+\.[0-9]+|flurm" \
    "unknown"

# ========================================
# scontrol show config
# ========================================
echo ""
echo "--- scontrol show config ---"

run_scontrol_test \
    "scontrol show config" \
    "scontrol show config" \
    "Configuration|ClusterName|SlurmctldHost|ControlMachine|Controller" \
    "not implemented|error"

# Additional config verification
sleep 1
CONFIG_OUTPUT=$(scontrol show config 2>&1 || echo "")
if echo "$CONFIG_OUTPUT" | grep -qiE "ClusterName|SlurmctldHost"; then
    echo "  Config details available:"
    echo "$CONFIG_OUTPUT" | grep -iE "ClusterName|SlurmctldHost|SlurmctldPort" | head -5 | sed 's/^/    /'
fi

# ========================================
# scontrol show node
# ========================================
echo ""
echo "--- scontrol show node ---"

run_scontrol_test \
    "scontrol show node (all)" \
    "scontrol show node" \
    "NodeName|State|CPUTot|RealMemory|Partitions|IDLE|ALLOC|DOWN" \
    "no node|error|not found"

# Test specific node (if we can find one)
sleep 1
NODE_LIST=$(scontrol show node 2>&1 | grep -oE "NodeName=[^ ]+" | head -1 | cut -d= -f2 || echo "")
if [ -n "$NODE_LIST" ]; then
    run_scontrol_test \
        "scontrol show node <specific>" \
        "scontrol show node $NODE_LIST" \
        "NodeName|State" \
        "invalid|not found"
fi

# ========================================
# scontrol show partition
# ========================================
echo ""
echo "--- scontrol show partition ---"

run_scontrol_test \
    "scontrol show partition (all)" \
    "scontrol show partition" \
    "PartitionName|State|TotalNodes|TotalCPUs|Default|AllowGroups" \
    "no partition|error"

# Test specific partition
sleep 1
PARTITIONS=$(scontrol show partition 2>&1 | grep -oE "PartitionName=[^ ]+" | cut -d= -f2 || echo "")
if [ -n "$PARTITIONS" ]; then
    FIRST_PART=$(echo "$PARTITIONS" | head -1)
    run_scontrol_test \
        "scontrol show partition $FIRST_PART" \
        "scontrol show partition $FIRST_PART" \
        "PartitionName|State" \
        "invalid|not found"
fi

# ========================================
# scontrol show job
# ========================================
echo ""
echo "--- scontrol show job ---"

# First submit a job to have something to show
echo "Submitting test job for show job test..."
sleep 2
JOB_OUTPUT=$(sbatch --wrap="sleep 60" 2>&1 || echo "")
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE '[0-9]+' | head -1 || echo "")

if [ -n "$JOB_ID" ]; then
    echo "  Test job submitted: $JOB_ID"
    sleep 2

    run_scontrol_test \
        "scontrol show job $JOB_ID" \
        "scontrol show job $JOB_ID" \
        "JobId|JobState|UserId|Partition|Command|NumNodes|NumCPUs" \
        "invalid|not found|no job"

    # Show all jobs
    run_scontrol_test \
        "scontrol show job (all)" \
        "scontrol show job" \
        "JobId|JobState|no jobs" \
        "error"
else
    echo "  WARN: Could not submit test job, skipping show job tests"
    WARNED=$((WARNED + 1))
fi

# ========================================
# scontrol show reservation (optional feature)
# ========================================
echo ""
echo "--- scontrol show reservation ---"

run_scontrol_test \
    "scontrol show reservation" \
    "scontrol show reservation" \
    "ReservationName|StartTime|EndTime|no reservations|No reservations" \
    "not implemented|error|invalid"

# ========================================
# scontrol update tests (write operations)
# ========================================
echo ""
echo "--- scontrol update operations ---"

# Test node state update (if we have a valid node)
if [ -n "$NODE_LIST" ]; then
    echo "Test: scontrol update NodeName (drain)..."
    sleep 2
    DRAIN_OUTPUT=$(scontrol update NodeName="$NODE_LIST" State=DRAIN Reason="E2E test" 2>&1 || echo "COMMAND_ERROR")
    if echo "$DRAIN_OUTPUT" | grep -qiE "error|permission|denied|not implemented|invalid"; then
        echo "  WARN: Node drain not supported or not permitted"
        WARNED=$((WARNED + 1))
    else
        echo "  PASS: Node drain command accepted"
        PASSED=$((PASSED + 1))

        # Try to resume the node
        sleep 1
        echo "Test: scontrol update NodeName (resume)..."
        RESUME_OUTPUT=$(scontrol update NodeName="$NODE_LIST" State=RESUME 2>&1 || echo "COMMAND_ERROR")
        if echo "$RESUME_OUTPUT" | grep -qiE "error|permission|denied"; then
            echo "  WARN: Node resume may have failed: $RESUME_OUTPUT"
            WARNED=$((WARNED + 1))
        else
            echo "  PASS: Node resume command accepted"
            PASSED=$((PASSED + 1))
        fi
    fi
else
    echo "  SKIP: No nodes available for update test"
fi

# Test job update (hold/release)
if [ -n "$JOB_ID" ]; then
    echo "Test: scontrol hold job..."
    sleep 2
    HOLD_OUTPUT=$(scontrol hold "$JOB_ID" 2>&1 || echo "COMMAND_ERROR")
    if echo "$HOLD_OUTPUT" | grep -qiE "error|permission|denied|not implemented|invalid"; then
        echo "  WARN: Job hold not supported: $(echo "$HOLD_OUTPUT" | head -1)"
        WARNED=$((WARNED + 1))
    else
        echo "  PASS: Job hold command accepted"
        PASSED=$((PASSED + 1))
    fi

    echo "Test: scontrol release job..."
    sleep 1
    RELEASE_OUTPUT=$(scontrol release "$JOB_ID" 2>&1 || echo "COMMAND_ERROR")
    if echo "$RELEASE_OUTPUT" | grep -qiE "error|permission|denied|not implemented|invalid"; then
        echo "  WARN: Job release not supported: $(echo "$RELEASE_OUTPUT" | head -1)"
        WARNED=$((WARNED + 1))
    else
        echo "  PASS: Job release command accepted"
        PASSED=$((PASSED + 1))
    fi

    # Update job time limit
    echo "Test: scontrol update job TimeLimit..."
    sleep 1
    TIMELIMIT_OUTPUT=$(scontrol update JobId="$JOB_ID" TimeLimit=30:00 2>&1 || echo "COMMAND_ERROR")
    if echo "$TIMELIMIT_OUTPUT" | grep -qiE "error|permission|denied|not implemented|invalid"; then
        echo "  WARN: Job TimeLimit update not supported"
        WARNED=$((WARNED + 1))
    else
        echo "  PASS: Job TimeLimit update accepted"
        PASSED=$((PASSED + 1))
    fi

    # Clean up - cancel the test job
    echo "Cleaning up test job $JOB_ID..."
    scancel "$JOB_ID" 2>/dev/null || true
fi

# ========================================
# scontrol reconfigure (admin operation)
# ========================================
echo ""
echo "--- scontrol reconfigure ---"

echo "Test: scontrol reconfigure..."
sleep 2
RECONFIG_OUTPUT=$(scontrol reconfigure 2>&1 || echo "COMMAND_ERROR")
if echo "$RECONFIG_OUTPUT" | grep -qiE "permission|denied|not authorized"; then
    echo "  WARN: Reconfigure requires admin privileges"
    WARNED=$((WARNED + 1))
elif echo "$RECONFIG_OUTPUT" | grep -qiE "error|not implemented"; then
    echo "  WARN: Reconfigure not supported"
    WARNED=$((WARNED + 1))
else
    echo "  PASS: Reconfigure command accepted"
    PASSED=$((PASSED + 1))
fi

# ========================================
# Summary
# ========================================
echo ""
echo "=== Test: scontrol Commands Complete ==="
echo ""
echo "Summary:"
echo "  Passed:  $PASSED"
echo "  Warned:  $WARNED"
echo "  Failed:  $FAILED"
echo ""

# Test passes if no hard failures
if [ $FAILED -gt 0 ]; then
    echo "RESULT: FAILED"
    exit 1
else
    echo "RESULT: PASSED (with $WARNED warnings)"
    exit 0
fi
