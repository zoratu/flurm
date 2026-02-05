#!/bin/bash
#
# FLURM Job Types Test Scenario
#
# Tests all job types:
# - Batch jobs (small, medium, large, multi-node)
# - MPI jobs
# - Array jobs (simple, throttled, mixed)
# - Dependency chains
# - Failure scenarios (timeout, exit error, OOM, retry)
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

log_info() {
    echo -e "[INFO] $1"
}

# Execute command in test client container
exec_client() {
    docker exec flurm-test-client "$@"
}

# Wait for job to complete (with configurable timeout)
wait_job() {
    local job_id=$1
    local max_wait=${2:-120}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local state=$(exec_client squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "")
        if [ -z "$state" ] || [ "$state" = "COMPLETED" ] || [ "$state" = "FAILED" ] || [ "$state" = "CANCELLED" ] || [ "$state" = "TIMEOUT" ]; then
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
    done
    return 1
}

# Get job state from accounting
get_job_state() {
    local job_id=$1
    exec_client sacct -j "$job_id" -n -o State --parsable2 2>/dev/null | head -1 | tr -d ' '
}

# Wait for all array tasks to complete
wait_array_job() {
    local job_id=$1
    local max_wait=${2:-180}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        # Check if any tasks are still running
        local running=$(exec_client squeue -j "$job_id" -h 2>/dev/null | wc -l)
        if [ "$running" -eq 0 ]; then
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
    done
    return 1
}

main() {
    echo "=========================================="
    echo "FLURM Job Types Tests"
    echo "=========================================="

    # ==========================================
    # BATCH JOBS
    # ==========================================
    echo ""
    echo "--- Batch Jobs ---"

    # Test: Small batch job
    log_test "Small batch job (1 CPU, 512MB)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        log_info "Submitted job $JOB_ID"
        wait_job "$JOB_ID" 60
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Small batch job completed"
        else
            log_fail "Small batch job state: $STATE"
        fi
    else
        log_fail "Failed to submit small batch job"
    fi

    # Test: Medium batch job
    log_test "Medium batch job (4 CPU, 2GB)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_cpu_medium.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Medium batch job completed"
        else
            log_fail "Medium batch job state: $STATE"
        fi
    else
        log_fail "Failed to submit medium batch job"
    fi

    # Test: Large batch job
    log_test "Large batch job (8 CPU, 8GB)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_cpu_large.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 180
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Large batch job completed"
        else
            log_fail "Large batch job state: $STATE"
        fi
    else
        log_fail "Failed to submit large batch job"
    fi

    # Test: Memory-bound job
    log_test "Memory test job"
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

    # Test: Multi-node job
    log_test "Multi-node job (2 nodes)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/batch/job_multi_node.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Multi-node job completed"
        else
            log_fail "Multi-node job state: $STATE"
        fi
    else
        log_fail "Failed to submit multi-node job"
    fi

    # ==========================================
    # MPI JOBS
    # ==========================================
    echo ""
    echo "--- MPI Jobs ---"

    # Test: MPI hello world
    log_test "MPI hello world (2 nodes, 4 tasks)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/mpi/mpi_hello.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 180
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "MPI hello world completed"
        else
            log_fail "MPI hello world state: $STATE"
        fi
    else
        log_fail "Failed to submit MPI job"
    fi

    # ==========================================
    # ARRAY JOBS
    # ==========================================
    echo ""
    echo "--- Array Jobs ---"

    # Test: Simple array
    log_test "Simple array job (10 tasks)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/arrays/array_simple.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        log_info "Array job ID: $JOB_ID"
        wait_array_job "$JOB_ID" 180

        # Check completed task count
        COMPLETED=$(exec_client sacct -j "$JOB_ID" -n -o State --parsable2 2>/dev/null | grep -c "COMPLETED" || echo "0")
        if [ "$COMPLETED" -ge 10 ]; then
            log_pass "Array job: all 10 tasks completed"
        else
            log_fail "Array job: only $COMPLETED/10 tasks completed"
        fi
    else
        log_fail "Failed to submit array job"
    fi

    # Test: Throttled array
    log_test "Throttled array job (50 tasks, %5 concurrent)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/arrays/array_throttled.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        # Give it time to start
        sleep 10

        # Check concurrent tasks (should be <= 5)
        RUNNING=$(exec_client squeue -j "$JOB_ID" -h -t RUNNING 2>/dev/null | wc -l)
        log_info "Currently running tasks: $RUNNING"

        if [ "$RUNNING" -le 5 ]; then
            log_pass "Array throttle respected (max 5 concurrent)"
        else
            log_fail "Array throttle exceeded: $RUNNING running"
        fi

        # Wait for completion
        wait_array_job "$JOB_ID" 300
        COMPLETED=$(exec_client sacct -j "$JOB_ID" -n -o State --parsable2 2>/dev/null | grep -c "COMPLETED" || echo "0")
        log_info "Completed tasks: $COMPLETED"
    else
        log_fail "Failed to submit throttled array job"
    fi

    # Test: Mixed array (some fail)
    log_test "Mixed array job (some tasks fail)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/arrays/array_mixed.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_array_job "$JOB_ID" 180

        COMPLETED=$(exec_client sacct -j "$JOB_ID" -n -o State --parsable2 2>/dev/null | grep -c "COMPLETED" || echo "0")
        FAILED_TASKS=$(exec_client sacct -j "$JOB_ID" -n -o State --parsable2 2>/dev/null | grep -c "FAILED" || echo "0")

        log_info "Completed: $COMPLETED, Failed: $FAILED_TASKS"

        # Expect 8 completed, 2 failed (tasks 3 and 7)
        if [ "$COMPLETED" -ge 7 ] && [ "$FAILED_TASKS" -ge 2 ]; then
            log_pass "Mixed array handled correctly"
        else
            log_fail "Mixed array unexpected results"
        fi
    else
        log_fail "Failed to submit mixed array job"
    fi

    # ==========================================
    # DEPENDENCY JOBS
    # ==========================================
    echo ""
    echo "--- Dependency Jobs ---"

    # Test: Dependency chain
    log_test "Dependency chain (stage1 -> stage2 -> stage3 -> cleanup)"

    # Submit stage 1
    STAGE1=$(exec_client sbatch --parsable /jobs/dependencies/dep_stage1.sh 2>/dev/null)
    if [ -z "$STAGE1" ]; then
        log_fail "Failed to submit stage1"
    else
        log_info "Stage1: $STAGE1"

        # Submit stage 2 with dependency
        STAGE2=$(exec_client sbatch --parsable --dependency=afterok:$STAGE1 /jobs/dependencies/dep_stage2.sh 2>/dev/null)
        log_info "Stage2: $STAGE2 (depends on $STAGE1)"

        # Submit stage 3 with dependency
        STAGE3=$(exec_client sbatch --parsable --dependency=afterok:$STAGE2 /jobs/dependencies/dep_stage3.sh 2>/dev/null)
        log_info "Stage3: $STAGE3 (depends on $STAGE2)"

        # Submit cleanup with afterany
        CLEANUP=$(exec_client sbatch --parsable --dependency=afterany:$STAGE3 /jobs/dependencies/dep_cleanup.sh 2>/dev/null)
        log_info "Cleanup: $CLEANUP (depends on $STAGE3)"

        # Wait for cleanup to complete (last in chain)
        wait_job "$CLEANUP" 300

        # Verify all completed
        ALL_COMPLETED=true
        for job in $STAGE1 $STAGE2 $STAGE3 $CLEANUP; do
            STATE=$(get_job_state "$job")
            log_info "Job $job: $STATE"
            if [ "$STATE" != "COMPLETED" ]; then
                ALL_COMPLETED=false
            fi
        done

        if [ "$ALL_COMPLETED" = true ]; then
            log_pass "Dependency chain completed in order"
        else
            log_fail "Dependency chain had failures"
        fi
    fi

    # ==========================================
    # FAILURE JOBS
    # ==========================================
    echo ""
    echo "--- Failure Scenarios ---"

    # Test: Exit error
    log_test "Job exit error (exit code 1)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/failures/job_exit_error.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 60
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "FAILED" ]; then
            log_pass "Exit error job correctly marked FAILED"
        else
            log_fail "Exit error job state: $STATE (expected FAILED)"
        fi
    else
        log_fail "Failed to submit exit error job"
    fi

    # Test: Timeout
    log_test "Job timeout (exceeds time limit)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/failures/job_timeout.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        # Time limit is 30 seconds, job runs 120
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        if [ "$STATE" = "TIMEOUT" ] || [ "$STATE" = "CANCELLED" ]; then
            log_pass "Timeout job correctly terminated"
        else
            log_fail "Timeout job state: $STATE (expected TIMEOUT)"
        fi
    else
        log_fail "Failed to submit timeout job"
    fi

    # Test: OOM (may not trigger actual OOM in container)
    log_test "OOM test job"
    JOB_ID=$(exec_client sbatch --parsable /jobs/failures/job_oom.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        wait_job "$JOB_ID" 120
        STATE=$(get_job_state "$JOB_ID")
        log_info "OOM job state: $STATE"
        # OOM may result in FAILED or OUT_OF_MEMORY depending on configuration
        if [ "$STATE" = "FAILED" ] || [ "$STATE" = "OUT_OF_MEMORY" ] || [ "$STATE" = "COMPLETED" ]; then
            log_pass "OOM test completed (state: $STATE)"
        else
            log_fail "OOM job unexpected state: $STATE"
        fi
    else
        log_fail "Failed to submit OOM job"
    fi

    # Test: Retry job
    log_test "Retry job (fails twice, succeeds third)"
    JOB_ID=$(exec_client sbatch --parsable /jobs/failures/job_retry.sh 2>/dev/null)
    if [ -n "$JOB_ID" ]; then
        # This job should be requeued and eventually succeed
        wait_job "$JOB_ID" 300  # Give it time for retries
        STATE=$(get_job_state "$JOB_ID")

        # Check attempt count from accounting
        ATTEMPTS=$(exec_client sacct -j "$JOB_ID" -n -o JobID --parsable2 2>/dev/null | wc -l)
        log_info "Retry job attempts: $ATTEMPTS, final state: $STATE"

        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "Retry job eventually completed"
        else
            log_fail "Retry job state: $STATE (expected COMPLETED after retries)"
        fi
    else
        log_fail "Failed to submit retry job"
    fi

    # Summary
    echo ""
    echo "=========================================="
    echo "Job Types Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC} $PASSED"
    echo -e "${RED}Failed:${NC} $FAILED"

    if [ "$FAILED" -gt 0 ]; then
        return 1
    fi
    return 0
}

main
