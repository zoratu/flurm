#!/bin/bash
#
# FLURM Standalone Mode Test Scenario
#
# Tests basic FLURM operation without HA or federation:
# - Single controller operation
# - Job submission and completion
# - Resource allocation verification
# - Partition routing
# - Basic metrics and health checks
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Test counters
PASSED=0
FAILED=0

log_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASSED=$((PASSED + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAILED=$((FAILED + 1))
}

# Execute command in test client container
exec_client() {
    docker exec flurm-test-client "$@"
}

# Wait for job to complete (max 60 seconds)
wait_job() {
    local job_id=$1
    local max_wait=${2:-60}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local state=$(exec_client squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "UNKNOWN")
        case $state in
            COMPLETED|FAILED|CANCELLED|TIMEOUT|"")
                return 0
                ;;
        esac
        sleep 2
        waited=$((waited + 2))
    done
    return 1
}

# Get job state
get_job_state() {
    local job_id=$1
    exec_client sacct -j "$job_id" -n -o State --parsable2 2>/dev/null | head -1 | tr -d ' '
}

main() {
    echo "=========================================="
    echo "FLURM Standalone Mode Tests"
    echo "=========================================="

    # Test 1: Health check endpoint
    log_test "Controller health check"
    if docker exec flurm-test-ctrl-1 curl -sf http://localhost:9090/health > /dev/null 2>&1; then
        log_pass "Controller health endpoint responds"
    else
        log_fail "Controller health endpoint not responding"
    fi

    # Test 2: Metrics endpoint
    log_test "Metrics endpoint"
    if docker exec flurm-test-ctrl-1 curl -sf http://localhost:9090/metrics | grep -q "flurm_"; then
        log_pass "Metrics endpoint returns FLURM metrics"
    else
        log_fail "Metrics endpoint missing FLURM metrics"
    fi

    # Test 3: sinfo command
    log_test "sinfo shows partitions"
    if exec_client sinfo -h | grep -q "debug\|batch\|large"; then
        log_pass "sinfo shows expected partitions"
    else
        log_fail "sinfo missing expected partitions"
    fi

    # Test 4: Submit small batch job to debug partition
    log_test "Submit job to debug partition"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null)
    if [ -n "$JOB_ID" ] && [ "$JOB_ID" -gt 0 ] 2>/dev/null; then
        log_pass "Job submitted: $JOB_ID"

        # Wait for completion
        log_test "Wait for job completion"
        if wait_job "$JOB_ID" 60; then
            STATE=$(get_job_state "$JOB_ID")
            if [ "$STATE" = "COMPLETED" ]; then
                log_pass "Job $JOB_ID completed successfully"
            else
                log_fail "Job $JOB_ID ended with state: $STATE"
            fi
        else
            log_fail "Job $JOB_ID timed out waiting"
        fi
    else
        log_fail "Failed to submit job"
    fi

    # Test 5: Submit job to batch partition
    log_test "Submit job to batch partition"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_cpu_medium.sh 2>/dev/null)
    if [ -n "$JOB_ID" ] && [ "$JOB_ID" -gt 0 ] 2>/dev/null; then
        log_pass "Batch job submitted: $JOB_ID"
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Batch job completed"
        else
            log_fail "Batch job state: $STATE"
        fi
    else
        log_fail "Failed to submit batch job"
    fi

    # Test 6: Verify resource allocation
    log_test "Verify resource allocation (4 CPUs, 2GB)"
    # Check last job's allocation
    ALLOC=$(exec_client sacct -j "$JOB_ID" -n -o AllocCPUS,ReqMem --parsable2 2>/dev/null | head -1)
    if echo "$ALLOC" | grep -q "4"; then
        log_pass "CPU allocation correct"
    else
        log_fail "CPU allocation incorrect: $ALLOC"
    fi

    # Test 7: squeue shows running jobs
    log_test "squeue command works"
    if exec_client squeue > /dev/null 2>&1; then
        log_pass "squeue executes successfully"
    else
        log_fail "squeue command failed"
    fi

    # Test 8: scancel works
    log_test "Submit and cancel job"
    JOB_ID=$(exec_client sbatch --parsable --time=10:00 /jobs/batch/job_cpu_small.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        sleep 2
        if exec_client scancel "$JOB_ID" 2>/dev/null; then
            sleep 2
            STATE=$(get_job_state "$JOB_ID")
            if [ "$STATE" = "CANCELLED" ] || [ -z "$STATE" ]; then
                log_pass "Job cancelled successfully"
            else
                log_fail "Job not cancelled, state: $STATE"
            fi
        else
            log_fail "scancel command failed"
        fi
    else
        log_fail "Failed to submit job for cancel test"
    fi

    # Test 9: Memory test job
    log_test "Memory allocation test"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_memory_test.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Memory test job completed"
        else
            log_fail "Memory test job state: $STATE"
        fi
    else
        log_fail "Failed to submit memory test job"
    fi

    # Test 10: Node status
    log_test "Node status check"
    NODES=$(exec_client sinfo -h -o "%n %t" 2>/dev/null | grep -c "idle\|alloc\|mix" || echo "0")
    if [ "$NODES" -gt 0 ]; then
        log_pass "Found $NODES active nodes"
    else
        log_fail "No active nodes found"
    fi

    # Summary
    echo ""
    echo "=========================================="
    echo "Standalone Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC} $PASSED"
    echo -e "${RED}Failed:${NC} $FAILED"

    if [ "$FAILED" -gt 0 ]; then
        return 1
    fi
    return 0
}

main
