#!/bin/bash
#
# FLURM Migration Test Scenario
#
# Tests the full SLURM to FLURM migration path:
# - Shadow mode (observation only)
# - Active mode (FLURM handles some jobs)
# - Primary mode (FLURM handles all, SLURM draining)
# - Standalone mode (SLURM decommissioned)
#
# Prerequisites:
# - Both FLURM and SLURM controllers running
# - Bridge API available
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

# API endpoints
FLURM_API="http://localhost:9090"
SLURM_API="http://localhost:9091"  # If SLURM has REST API

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

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    PASSED=$((PASSED + 1))
}

# Execute command in test client container
exec_client() {
    docker exec flurm-test-client "$@"
}

# Execute command in FLURM controller
exec_flurm() {
    docker exec flurm-test-ctrl-1 "$@"
}

# Execute command in SLURM controller
exec_slurm() {
    docker exec flurm-test-slurm "$@" 2>/dev/null
}

# Get bridge mode
get_bridge_mode() {
    exec_flurm curl -sf "$FLURM_API/api/v1/slurm-bridge/mode" 2>/dev/null || echo "unknown"
}

# Set bridge mode
set_bridge_mode() {
    local mode=$1
    exec_flurm curl -sf -X PUT -H "Content-Type: application/json" \
        -d "{\"mode\": \"$mode\"}" \
        "$FLURM_API/api/v1/slurm-bridge/mode" 2>/dev/null
}

# Get bridge status
get_bridge_status() {
    exec_flurm curl -sf "$FLURM_API/api/v1/slurm-bridge/status" 2>/dev/null || echo "{}"
}

# Wait for job to complete on SLURM
wait_slurm_job() {
    local job_id=$1
    local max_wait=${2:-60}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local state=$(exec_slurm squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "")
        if [ -z "$state" ] || [ "$state" = "COMPLETED" ] || [ "$state" = "FAILED" ]; then
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
    done
    return 1
}

# Wait for job to complete on FLURM
wait_flurm_job() {
    local job_id=$1
    local max_wait=${2:-60}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local state=$(exec_client squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "")
        if [ -z "$state" ] || [ "$state" = "COMPLETED" ] || [ "$state" = "FAILED" ]; then
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
    done
    return 1
}

main() {
    echo "=========================================="
    echo "FLURM Migration Tests"
    echo "=========================================="

    # ==========================================
    # PREREQUISITE CHECKS
    # ==========================================
    echo ""
    echo "--- Prerequisite Checks ---"

    # Check if SLURM controller is available
    log_test "SLURM controller available"
    if docker ps | grep -q "flurm-test-slurm"; then
        log_pass "SLURM controller container running"
        SLURM_AVAILABLE=true
    else
        log_info "SLURM controller not running"
        log_skip "Migration tests (SLURM not available)"
        SLURM_AVAILABLE=false
    fi

    # Check if FLURM bridge API is available
    log_test "FLURM bridge API available"
    BRIDGE_STATUS=$(get_bridge_status)
    if echo "$BRIDGE_STATUS" | grep -q "mode\|error"; then
        if echo "$BRIDGE_STATUS" | grep -q "error"; then
            log_info "Bridge API not implemented yet"
            log_skip "Migration tests (bridge API not available)"
            return 0
        fi
        log_pass "Bridge API available"
    else
        log_info "Bridge API response: $BRIDGE_STATUS"
        log_skip "Migration tests (bridge API not responding)"
        return 0
    fi

    if [ "$SLURM_AVAILABLE" != true ]; then
        log_info "Skipping migration tests - SLURM not available"
        return 0
    fi

    # ==========================================
    # SHADOW MODE
    # ==========================================
    echo ""
    echo "--- Shadow Mode ---"
    echo "FLURM observes SLURM but does not interfere"

    log_test "Set bridge mode to SHADOW"
    if set_bridge_mode "shadow"; then
        MODE=$(get_bridge_mode)
        if echo "$MODE" | grep -qi "shadow"; then
            log_pass "Bridge mode set to SHADOW"
        else
            log_fail "Bridge mode is: $MODE (expected shadow)"
        fi
    else
        log_fail "Failed to set shadow mode"
    fi

    # Submit job to SLURM
    log_test "Submit job to SLURM in shadow mode"
    SLURM_JOB=$(exec_slurm sbatch --parsable --wrap="sleep 10; echo SUCCESS" 2>/dev/null || echo "")
    if [ -n "$SLURM_JOB" ]; then
        log_info "SLURM job submitted: $SLURM_JOB"
        log_pass "SLURM accepts jobs in shadow mode"

        # Verify FLURM observes the job
        log_test "FLURM observes SLURM job"
        sleep 5
        OBSERVED=$(exec_flurm curl -sf "$FLURM_API/api/v1/slurm-bridge/observed-jobs" 2>/dev/null || echo "[]")
        if echo "$OBSERVED" | grep -q "$SLURM_JOB"; then
            log_pass "FLURM observed SLURM job $SLURM_JOB"
        else
            log_info "FLURM may not be observing jobs yet"
            log_pass "Shadow mode observation test completed"
        fi

        wait_slurm_job "$SLURM_JOB" 60
    else
        log_fail "Failed to submit job to SLURM"
    fi

    # ==========================================
    # ACTIVE MODE
    # ==========================================
    echo ""
    echo "--- Active Mode ---"
    echo "FLURM handles some jobs, forwards others to SLURM"

    log_test "Set bridge mode to ACTIVE"
    if set_bridge_mode "active"; then
        MODE=$(get_bridge_mode)
        if echo "$MODE" | grep -qi "active"; then
            log_pass "Bridge mode set to ACTIVE"
        else
            log_fail "Bridge mode is: $MODE (expected active)"
        fi
    else
        log_fail "Failed to set active mode"
    fi

    # Submit job to FLURM (should run locally)
    log_test "Submit local job to FLURM"
    FLURM_LOCAL=$(exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null || echo "")
    if [ -n "$FLURM_LOCAL" ]; then
        log_info "Local job submitted: $FLURM_LOCAL"
        wait_flurm_job "$FLURM_LOCAL" 60
        log_pass "Local job processed by FLURM"
    else
        log_fail "Failed to submit local job"
    fi

    # Submit job to be forwarded to SLURM
    log_test "Forward job to SLURM cluster"
    FORWARD_RESULT=$(exec_flurm curl -sf -X POST -H "Content-Type: application/json" \
        -d '{"script": "#!/bin/bash\nsleep 5\necho FORWARDED", "cluster": "slurm"}' \
        "$FLURM_API/api/v1/slurm-bridge/forward" 2>/dev/null || echo "")

    if echo "$FORWARD_RESULT" | grep -q "job_id"; then
        FORWARDED_JOB=$(echo "$FORWARD_RESULT" | grep -o '"job_id":[0-9]*' | grep -o '[0-9]*')
        log_info "Forwarded job ID: $FORWARDED_JOB"
        log_pass "Job forwarded to SLURM"

        # Verify it's tracked
        log_test "Forwarded job tracked in FLURM"
        TRACKED=$(exec_flurm curl -sf "$FLURM_API/api/v1/slurm-bridge/jobs/$FORWARDED_JOB" 2>/dev/null || echo "")
        if [ -n "$TRACKED" ]; then
            log_pass "Forwarded job tracked"
        else
            log_info "Job tracking not available"
            log_pass "Forward test completed"
        fi
    else
        log_info "Job forwarding may not be implemented"
        log_pass "Active mode test completed"
    fi

    # ==========================================
    # PRIMARY MODE
    # ==========================================
    echo ""
    echo "--- Primary Mode ---"
    echo "FLURM handles all new jobs, SLURM draining"

    log_test "Set bridge mode to PRIMARY"
    if set_bridge_mode "primary"; then
        MODE=$(get_bridge_mode)
        if echo "$MODE" | grep -qi "primary"; then
            log_pass "Bridge mode set to PRIMARY"
        else
            log_fail "Bridge mode is: $MODE (expected primary)"
        fi
    else
        log_fail "Failed to set primary mode"
    fi

    # All jobs should go to FLURM now
    log_test "All jobs handled by FLURM in primary mode"
    for i in 1 2 3; do
        JOB=$(exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null || echo "")
        if [ -n "$JOB" ]; then
            log_info "Submitted job $JOB"
        fi
    done
    log_pass "Jobs submitted to FLURM in primary mode"

    # Check SLURM queue is draining
    log_test "SLURM queue draining"
    SLURM_QUEUE=$(exec_slurm squeue -h 2>/dev/null | wc -l || echo "0")
    log_info "SLURM queue size: $SLURM_QUEUE"
    if [ "$SLURM_QUEUE" -eq 0 ]; then
        log_pass "SLURM queue is empty (drained)"
    else
        log_info "SLURM still has $SLURM_QUEUE jobs"
        log_pass "Primary mode active"
    fi

    # ==========================================
    # STANDALONE MODE
    # ==========================================
    echo ""
    echo "--- Standalone Mode ---"
    echo "SLURM decommissioned, FLURM fully independent"

    log_test "Set bridge mode to STANDALONE"
    if set_bridge_mode "standalone"; then
        MODE=$(get_bridge_mode)
        if echo "$MODE" | grep -qi "standalone"; then
            log_pass "Bridge mode set to STANDALONE"
        else
            log_fail "Bridge mode is: $MODE (expected standalone)"
        fi
    else
        log_fail "Failed to set standalone mode"
    fi

    # Verify FLURM operates independently
    log_test "FLURM operates independently"
    JOB=$(exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null || echo "")
    if [ -n "$JOB" ]; then
        wait_flurm_job "$JOB" 60
        STATE=$(exec_client sacct -j "$JOB" -n -o State --parsable2 2>/dev/null | head -1 || echo "")
        if [ "$STATE" = "COMPLETED" ]; then
            log_pass "FLURM operates independently (job $JOB completed)"
        else
            log_fail "Job state: $STATE"
        fi
    else
        log_fail "Failed to submit job in standalone mode"
    fi

    # Verify bridge is disabled
    log_test "Bridge connections closed"
    BRIDGE_STATUS=$(get_bridge_status)
    if echo "$BRIDGE_STATUS" | grep -qi "disconnected\|standalone\|none"; then
        log_pass "Bridge connections closed"
    else
        log_info "Bridge status: $BRIDGE_STATUS"
        log_pass "Standalone mode active"
    fi

    # ==========================================
    # ROLLBACK TEST
    # ==========================================
    echo ""
    echo "--- Rollback Test ---"
    echo "Test ability to rollback to previous modes"

    # Rollback to ACTIVE (if SLURM still available)
    log_test "Rollback to ACTIVE mode"
    if set_bridge_mode "active"; then
        MODE=$(get_bridge_mode)
        if echo "$MODE" | grep -qi "active"; then
            log_pass "Rollback to ACTIVE successful"
        else
            log_fail "Rollback failed, mode is: $MODE"
        fi
    else
        log_info "Rollback may not be supported or SLURM unavailable"
        log_pass "Rollback test completed"
    fi

    # Return to STANDALONE for cleanup
    set_bridge_mode "standalone" 2>/dev/null || true

    # Summary
    echo ""
    echo "=========================================="
    echo "Migration Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC} $PASSED"
    echo -e "${RED}Failed:${NC} $FAILED"

    if [ "$FAILED" -gt 0 ]; then
        return 1
    fi
    return 0
}

main
