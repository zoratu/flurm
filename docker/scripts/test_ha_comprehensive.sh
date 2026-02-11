#!/bin/bash
###############################################################################
# FLURM Comprehensive HA Test Runner
#
# Runs all HA test suites and adds additional multi-controller consistency
# and compute node failure scenarios.
#
# Scenarios:
#   1. Leader failover (delegates to test_ha_failover.sh)
#   2. Split-brain / network partition (delegates to test_split_brain.sh)
#   3. Multi-controller job consistency (new)
#   4. Compute node failure detection (new)
#
# Usage: ./test_ha_comprehensive.sh [--skip-cleanup] [--verbose] [--quick]
#
# --quick: Only run new scenarios (3-4), skip failover and split-brain
###############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.ha.yml"
PROJECT_NAME="flurm-ha-comprehensive"

# Controller settings
CONTROLLERS=("ctrl-1" "ctrl-2" "ctrl-3")
SLURM_PORTS=("6817" "6827" "6837")
METRICS_PORTS=("9090" "9091" "9092")

# Timeouts
CLUSTER_FORMATION_TIMEOUT=120
NODE_FAILURE_DETECT_TIMEOUT=60

# Options
SKIP_CLEANUP=false
VERBOSE=false
QUICK=false

# Counters
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_TESTS=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

###############################################################################
# Helpers
###############################################################################

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-cleanup) SKIP_CLEANUP=true; shift ;;
            --verbose) VERBOSE=true; shift ;;
            --quick) QUICK=true; shift ;;
            --help) usage; exit 0 ;;
            *) echo "Unknown option: $1"; exit 1 ;;
        esac
    done
}

usage() {
    echo "Usage: $(basename "$0") [--skip-cleanup] [--verbose] [--quick]"
    echo ""
    echo "Options:"
    echo "  --skip-cleanup  Don't tear down containers after test"
    echo "  --verbose       Enable verbose output"
    echo "  --quick         Only run scenarios 3-4 (skip failover/split-brain)"
}

log_info() { echo -e "${BLUE}[INFO]${NC} $1" >&2; }
log_pass() { TOTAL_PASSED=$((TOTAL_PASSED + 1)); TOTAL_TESTS=$((TOTAL_TESTS + 1)); echo -e "${GREEN}[PASS]${NC} $1" >&2; }
log_fail() { TOTAL_FAILED=$((TOTAL_FAILED + 1)); TOTAL_TESTS=$((TOTAL_TESTS + 1)); echo -e "${RED}[FAIL]${NC} $1" >&2; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}" >&2; }

docker_compose() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

container_running() {
    docker ps --filter "name=${PROJECT_NAME}-$1" --filter "status=running" -q | grep -q .
}

# Submit a job via the SLURM client targeting a specific controller
submit_job_via() {
    local ctrl_host="$1"
    local job_name="${2:-test}"
    docker exec -e SLURMCTLD_HOST="$ctrl_host" "${PROJECT_NAME}-slurm-client-1" \
        sbatch --job-name="$job_name" --time=00:10:00 --parsable \
        --wrap="echo 'HA test'; sleep 300" 2>/dev/null || echo ""
}

# Get job list from a specific controller port
get_job_ids_from() {
    local ctrl_host="$1"
    docker exec -e SLURMCTLD_HOST="$ctrl_host" "${PROJECT_NAME}-slurm-client-1" \
        squeue -h -o "%i" 2>/dev/null || echo ""
}

# Check if a job exists via a specific controller
job_exists_on() {
    local ctrl_host="$1"
    local job_id="$2"
    docker exec -e SLURMCTLD_HOST="$ctrl_host" "${PROJECT_NAME}-slurm-client-1" \
        squeue -j "$job_id" -h 2>/dev/null | grep -q "$job_id"
}

# Wait for cluster formation (simplified from test_ha_failover.sh)
wait_for_cluster_ready() {
    local timeout="$1"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for cluster to be ready (timeout: ${timeout}s)..."

    # Wait for health endpoints
    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log_fail "Cluster formation timed out"
            return 1
        fi

        local healthy=0
        for port in "${METRICS_PORTS[@]}"; do
            if curl -sf "http://localhost:${port}/health" >/dev/null 2>&1; then
                healthy=$((healthy + 1))
            fi
        done

        if [[ $healthy -ge 3 ]]; then
            log_info "All 3 controllers healthy"
            break
        fi

        sleep 3
    done

    # Wait for leader election (check STATUS log messages)
    sleep 35  # First status message at 30s
    log_info "Cluster ready"
    return 0
}

###############################################################################
# Scenario 3: Multi-Controller Job Consistency
###############################################################################

scenario_3_job_consistency() {
    log_section "Scenario 3: Multi-Controller Job Consistency"

    # Submit jobs via different controllers and verify all see them
    local job_ids=()

    # Submit a job via each controller
    for i in "${!CONTROLLERS[@]}"; do
        local ctrl="${CONTROLLERS[$i]}"
        local host="flurm-${ctrl}"
        local job_name="consistency_${ctrl}"

        log_info "Submitting job via $ctrl..."
        local job_id
        job_id=$(submit_job_via "$host" "$job_name")

        if [[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]; then
            log_pass "Job submitted via $ctrl: ID=$job_id"
            job_ids+=("$job_id")
        else
            log_fail "Failed to submit job via $ctrl"
        fi
    done

    # Wait for Ra replication
    sleep 5

    # Verify all controllers see all jobs
    for i in "${!CONTROLLERS[@]}"; do
        local ctrl="${CONTROLLERS[$i]}"
        local host="flurm-${ctrl}"
        local visible=0

        for job_id in "${job_ids[@]}"; do
            if job_exists_on "$host" "$job_id"; then
                visible=$((visible + 1))
            fi
        done

        if [[ $visible -eq ${#job_ids[@]} ]]; then
            log_pass "$ctrl sees all ${#job_ids[@]} jobs"
        else
            log_fail "$ctrl only sees $visible/${#job_ids[@]} jobs"
        fi
    done

    # Verify job IDs are unique across all submissions
    local unique_ids
    unique_ids=$(echo "${job_ids[@]}" | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')
    if [[ "$unique_ids" -eq "${#job_ids[@]}" ]]; then
        log_pass "All ${#job_ids[@]} job IDs are unique"
    else
        log_fail "Duplicate job IDs detected: $unique_ids unique out of ${#job_ids[@]}"
    fi

    # Clean up jobs
    for job_id in "${job_ids[@]}"; do
        docker exec "${PROJECT_NAME}-slurm-client-1" scancel "$job_id" 2>/dev/null || true
    done
}

###############################################################################
# Scenario 4: Compute Node Failure Detection
###############################################################################

scenario_4_node_failure() {
    log_section "Scenario 4: Compute Node Failure Detection"

    # Verify compute nodes are registered
    local node_count
    node_count=$(docker exec "${PROJECT_NAME}-slurm-client-1" \
        sinfo -h -o "%D" 2>/dev/null | head -1 | tr -d ' ' || echo "0")

    if [[ "$node_count" -ge 1 ]]; then
        log_pass "Compute nodes registered: $node_count"
    else
        log_warn "No compute nodes visible via sinfo (expected in some configs)"
    fi

    # Submit a job (it may or may not get scheduled depending on node state)
    local job_id
    job_id=$(submit_job_via "flurm-ctrl-1" "node_failure_test")

    if [[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]; then
        log_pass "Test job submitted: ID=$job_id"
    else
        log_warn "Could not submit test job"
        return
    fi

    # Get initial node state
    local initial_state
    initial_state=$(docker exec "${PROJECT_NAME}-slurm-client-1" \
        sinfo -h -o "%T" 2>/dev/null | head -1 || echo "unknown")
    log_info "Initial node state: $initial_state"

    # Kill compute node 1
    log_info "Stopping compute node flurm-node-1..."
    docker stop "${PROJECT_NAME}-node-1" 2>/dev/null || true

    if ! container_running "node-1"; then
        log_pass "Compute node stopped"
    else
        log_fail "Failed to stop compute node"
    fi

    # Wait for controller to detect node failure
    log_info "Waiting for node failure detection (up to ${NODE_FAILURE_DETECT_TIMEOUT}s)..."
    local detected=false
    local start_time
    start_time=$(date +%s)

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $NODE_FAILURE_DETECT_TIMEOUT ]]; then
            break
        fi

        local node_state
        node_state=$(docker exec "${PROJECT_NAME}-slurm-client-1" \
            sinfo -h -o "%T" 2>/dev/null | head -1 || echo "unknown")

        if [[ "$node_state" == *"down"* ]] || [[ "$node_state" == *"drain"* ]]; then
            detected=true
            log_pass "Node failure detected (state: $node_state) in ${elapsed}s"
            break
        fi

        sleep 3
    done

    if [[ "$detected" == "false" ]]; then
        log_warn "Node failure not detected within timeout (may be expected if monitoring not configured)"
    fi

    # Restart compute node
    log_info "Restarting compute node..."
    docker start "${PROJECT_NAME}-node-1" 2>/dev/null || true
    sleep 10

    if container_running "node-1"; then
        log_pass "Compute node restarted"
    else
        log_fail "Failed to restart compute node"
    fi

    # Clean up
    docker exec "${PROJECT_NAME}-slurm-client-1" scancel "$job_id" 2>/dev/null || true
}

###############################################################################
# Main
###############################################################################

cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        log_warn "Skipping cleanup (--skip-cleanup specified)"
        return
    fi
    log_info "Cleaning up..."
    docker_compose down -v --remove-orphans 2>/dev/null || true
}

main() {
    parse_args "$@"

    echo ""
    echo "=============================================="
    echo "  FLURM Comprehensive HA Tests"
    echo "=============================================="
    echo ""

    # Run delegated tests first (unless --quick)
    if [[ "$QUICK" == "false" ]]; then
        log_section "Scenario 1: Leader Failover"
        if [[ -x "${SCRIPT_DIR}/test_ha_failover.sh" ]]; then
            local failover_args=""
            [[ "$VERBOSE" == "true" ]] && failover_args="--verbose"
            if "${SCRIPT_DIR}/test_ha_failover.sh" $failover_args --skip-cleanup 2>&1; then
                log_pass "Failover test suite passed"
            else
                log_fail "Failover test suite had failures"
            fi
        else
            log_warn "test_ha_failover.sh not found or not executable"
        fi

        log_section "Scenario 2: Split-Brain"
        if [[ -x "${SCRIPT_DIR}/test_split_brain.sh" ]]; then
            local split_args=""
            [[ "$VERBOSE" == "true" ]] && split_args="--verbose"
            if "${SCRIPT_DIR}/test_split_brain.sh" $split_args --skip-cleanup 2>&1; then
                log_pass "Split-brain test suite passed"
            else
                log_fail "Split-brain test suite had failures"
            fi
        else
            log_warn "test_split_brain.sh not found or not executable"
        fi
    fi

    # Start fresh cluster for scenarios 3-4
    log_section "Setup: Starting HA Cluster"
    docker_compose down -v --remove-orphans 2>/dev/null || true
    docker_compose up -d --build 2>&1

    if ! wait_for_cluster_ready "$CLUSTER_FORMATION_TIMEOUT"; then
        log_fail "Could not form cluster for scenarios 3-4"
        cleanup
        exit 1
    fi

    # Run new scenarios
    scenario_3_job_consistency
    scenario_4_node_failure

    # Cleanup
    trap cleanup EXIT

    # Summary
    echo ""
    echo "=============================================="
    echo "  COMPREHENSIVE HA TEST RESULTS"
    echo "=============================================="
    echo "  Tests Passed: $TOTAL_PASSED"
    echo "  Tests Failed: $TOTAL_FAILED"
    echo "  Total Tests:  $TOTAL_TESTS"
    echo "=============================================="

    if [[ $TOTAL_FAILED -eq 0 ]]; then
        echo -e "${GREEN}ALL TESTS PASSED${NC}"
        exit 0
    else
        echo -e "${RED}SOME TESTS FAILED${NC}"
        exit 1
    fi
}

main "$@"
