#!/bin/bash
###############################################################################
# FLURM High-Availability Failover Test Script
#
# Tests leader failover in a 3-controller FLURM cluster:
# 1. Starts HA docker-compose environment (3 controllers)
# 2. Waits for cluster to form and elect a leader
# 3. Submits a job via the leader
# 4. Identifies the current leader
# 5. Kills the leader container
# 6. Verifies a new leader is elected
# 7. Verifies the job is still tracked
# 8. Submits another job to verify cluster still works
# 9. Brings back the killed node
# 10. Verifies it rejoins the cluster
#
# Usage: ./test_ha_failover.sh [--skip-cleanup] [--verbose]
###############################################################################

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$DOCKER_DIR")"
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.ha.yml"
PROJECT_NAME="flurm-ha-test"

# Test settings
CLUSTER_FORMATION_TIMEOUT=120  # seconds to wait for cluster to form
LEADER_ELECTION_TIMEOUT=60     # seconds to wait for new leader
NODE_REJOIN_TIMEOUT=60         # seconds to wait for node to rejoin
HEALTH_CHECK_INTERVAL=5        # seconds between health checks

# Controller settings
CONTROLLERS=("flurm-ctrl-1" "flurm-ctrl-2" "flurm-ctrl-3")
CONTROLLER_PORTS=("6817" "6827" "6837")
METRICS_PORTS=("9090" "9091" "9092")

# Test results
TEST_RESULTS_DIR="${TEST_RESULTS_DIR:-/tmp/flurm-ha-tests}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${TEST_RESULTS_DIR}/ha_failover_${TIMESTAMP}.log"

# Options
SKIP_CLEANUP=false
VERBOSE=false

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

###############################################################################
# Helper Functions
###############################################################################

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --skip-cleanup    Don't tear down containers after test
    --verbose         Enable verbose output
    --help            Show this help message

Example:
    $(basename "$0")
    $(basename "$0") --skip-cleanup --verbose
EOF
    exit 0
}

log() {
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "[$timestamp] $1" | tee -a "$LOG_FILE"
}

log_debug() {
    if [[ "$VERBOSE" == "true" ]]; then
        log "[DEBUG] $1"
    fi
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_pass() {
    TESTS_PASSED=$((TESTS_PASSED + 1))
    TESTS_TOTAL=$((TESTS_TOTAL + 1))
    echo -e "${GREEN}[PASS]${NC} $1" | tee -a "$LOG_FILE"
}

log_fail() {
    TESTS_FAILED=$((TESTS_FAILED + 1))
    TESTS_TOTAL=$((TESTS_TOTAL + 1))
    echo -e "${RED}[FAIL]${NC} $1" | tee -a "$LOG_FILE"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOG_FILE"
}

log_step() {
    echo ""
    echo -e "${BLUE}================================================================${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}  $1${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}================================================================${NC}" | tee -a "$LOG_FILE"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-cleanup)
                SKIP_CLEANUP=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --help)
                usage
                ;;
            *)
                echo "Unknown option: $1"
                usage
                ;;
        esac
    done
}

# Execute docker-compose command
docker_compose() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

# Check if a container is running
container_running() {
    local container="$1"
    docker ps --filter "name=${PROJECT_NAME}-${container}" --filter "status=running" -q | grep -q .
}

# Get container IP address
get_container_ip() {
    local container="$1"
    docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${PROJECT_NAME}-${container}-1" 2>/dev/null || echo ""
}

# Execute command in container
exec_in_container() {
    local container="$1"
    shift
    docker exec "${PROJECT_NAME}-${container}-1" "$@"
}

# Check controller health via metrics port
check_controller_health() {
    local host="$1"
    local port="$2"
    curl -sf "http://${host}:${port}/health" >/dev/null 2>&1
}

# Get cluster status from a controller
get_cluster_status() {
    local container="$1"
    exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'case catch flurm_controller_cluster:cluster_status() of
                   Status when is_map(Status) ->
                       io:format("~p~n", [Status]);
                   Error ->
                       io:format("error: ~p~n", [Error])
               end, halt(0).' 2>/dev/null || echo "error"
}

# Get the current leader node name
get_leader_from_container() {
    local container="$1"
    exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'case catch flurm_controller_cluster:get_leader() of
                   {ok, {_, Leader}} ->
                       io:format("~s~n", [Leader]);
                   Error ->
                       io:format("none~n")
               end, halt(0).' 2>/dev/null || echo "none"
}

# Check if a node is the leader
is_leader() {
    local container="$1"
    local result
    result=$(exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'case catch flurm_controller_cluster:is_leader() of
                   true -> io:format("true~n");
                   _ -> io:format("false~n")
               end, halt(0).' 2>/dev/null || echo "false")
    [[ "$result" == "true" ]]
}

# Find which container is the leader
find_leader_container() {
    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container" && is_leader "$container"; then
            echo "$container"
            return 0
        fi
    done
    echo ""
    return 1
}

# Find a follower container
find_follower_container() {
    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container" && ! is_leader "$container"; then
            echo "$container"
            return 0
        fi
    done
    echo ""
    return 1
}

# Get connected nodes count
get_connected_nodes_count() {
    local container="$1"
    exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'io:format("~p~n", [length(nodes())]), halt(0).' 2>/dev/null || echo "0"
}

# Wait for cluster to form with required number of nodes
wait_for_cluster() {
    local required_nodes="$1"
    local timeout="$2"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for cluster to form with $required_nodes nodes (timeout: ${timeout}s)..."

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log_fail "Cluster formation timed out after ${timeout}s"
            return 1
        fi

        # Check each controller
        local healthy_count=0
        local leader_found=false

        for container in "${CONTROLLERS[@]}"; do
            if container_running "$container"; then
                local connected
                connected=$(get_connected_nodes_count "$container")
                log_debug "Container $container has $connected connected nodes"

                if [[ "$connected" -ge $((required_nodes - 1)) ]]; then
                    healthy_count=$((healthy_count + 1))
                fi

                if is_leader "$container"; then
                    leader_found=true
                fi
            fi
        done

        log_debug "Healthy count: $healthy_count, Leader found: $leader_found"

        if [[ $healthy_count -ge $required_nodes ]] && [[ "$leader_found" == "true" ]]; then
            log_pass "Cluster formed with $required_nodes nodes and a leader elected"
            return 0
        fi

        sleep "$HEALTH_CHECK_INTERVAL"
    done
}

# Wait for new leader election after killing old leader
wait_for_new_leader() {
    local old_leader="$1"
    local timeout="$2"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for new leader election (timeout: ${timeout}s)..."

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log_fail "Leader election timed out after ${timeout}s"
            return 1
        fi

        # Find a new leader from remaining nodes
        for container in "${CONTROLLERS[@]}"; do
            if [[ "$container" != "$old_leader" ]] && container_running "$container"; then
                if is_leader "$container"; then
                    log_pass "New leader elected: $container"
                    echo "$container"
                    return 0
                fi
            fi
        done

        sleep 2
    done
}

# Wait for a node to rejoin the cluster
wait_for_node_rejoin() {
    local container="$1"
    local timeout="$2"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for $container to rejoin cluster (timeout: ${timeout}s)..."

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log_fail "Node rejoin timed out after ${timeout}s"
            return 1
        fi

        if container_running "$container"; then
            local connected
            connected=$(get_connected_nodes_count "$container")
            log_debug "Container $container has $connected connected nodes"

            if [[ "$connected" -ge 2 ]]; then
                log_pass "Node $container rejoined cluster with $connected peers"
                return 0
            fi
        fi

        sleep "$HEALTH_CHECK_INTERVAL"
    done
}

# Submit a test job via container
submit_test_job() {
    local container="$1"
    local job_name="${2:-test_job}"

    log_info "Submitting job '$job_name' via $container..."

    # Submit job via the SLURM client container
    local job_id
    job_id=$(docker exec "${PROJECT_NAME}-slurm-client-1" \
        sbatch --job-name="$job_name" --time=00:10:00 --parsable \
        --wrap="echo 'HA failover test'; sleep 300" 2>/dev/null || echo "")

    if [[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]; then
        log_pass "Job submitted successfully: ID=$job_id"
        echo "$job_id"
        return 0
    else
        log_warn "Could not submit job via SLURM client, trying API..."

        # Try via HTTP API as fallback
        local api_host
        api_host=$(get_container_ip "$container")

        if [[ -n "$api_host" ]]; then
            local response
            response=$(curl -sf -X POST "http://${api_host}:8080/api/jobs" \
                -H "Content-Type: application/json" \
                -d "{\"name\":\"$job_name\",\"script\":\"echo test\",\"time_limit\":600}" \
                2>/dev/null || echo "")

            if echo "$response" | grep -q '"id"'; then
                job_id=$(echo "$response" | grep -o '"id":[0-9]*' | cut -d: -f2)
                log_pass "Job submitted via API: ID=$job_id"
                echo "$job_id"
                return 0
            fi
        fi

        log_fail "Failed to submit job"
        return 1
    fi
}

# Check if a job is tracked
check_job_tracked() {
    local container="$1"
    local job_id="$2"

    log_info "Checking if job $job_id is tracked via $container..."

    # Try via SLURM client first
    local squeue_output
    squeue_output=$(docker exec "${PROJECT_NAME}-slurm-client-1" \
        squeue -j "$job_id" --noheader 2>/dev/null || echo "")

    if [[ -n "$squeue_output" ]]; then
        log_pass "Job $job_id is tracked in the cluster"
        return 0
    fi

    # Try via API
    local api_host
    api_host=$(get_container_ip "$container")

    if [[ -n "$api_host" ]]; then
        local response
        response=$(curl -sf "http://${api_host}:8080/api/jobs/$job_id" 2>/dev/null || echo "")

        if echo "$response" | grep -q '"id"'; then
            log_pass "Job $job_id is tracked via API"
            return 0
        fi
    fi

    log_fail "Job $job_id is NOT tracked"
    return 1
}

# Cleanup function
cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        log_warn "Skipping cleanup (--skip-cleanup specified)"
        log_info "To clean up manually: docker compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v"
        return
    fi

    log_step "Cleaning Up"
    log_info "Stopping and removing containers..."
    docker_compose down -v --remove-orphans 2>/dev/null || true
    log_info "Cleanup complete"
}

# Trap for cleanup on exit
trap_cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        log_warn "Test failed with exit code $exit_code"
    fi
    cleanup
    exit $exit_code
}

###############################################################################
# Test Phases
###############################################################################

phase_1_setup() {
    log_step "Phase 1: Setup - Starting HA Cluster"

    # Create results directory
    mkdir -p "$TEST_RESULTS_DIR"

    # Check prerequisites
    if ! command -v docker >/dev/null 2>&1; then
        log_fail "Docker is not installed"
        exit 1
    fi

    if ! docker compose version >/dev/null 2>&1; then
        log_fail "Docker Compose is not installed"
        exit 1
    fi

    # Check compose file exists
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_fail "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    log_pass "Prerequisites verified"

    # Tear down any existing cluster
    log_info "Cleaning up any existing containers..."
    docker_compose down -v --remove-orphans 2>/dev/null || true

    # Start the HA cluster
    log_info "Starting HA cluster with 3 controllers..."
    docker_compose up -d --build

    if [[ $? -eq 0 ]]; then
        log_pass "Docker containers started"
    else
        log_fail "Failed to start Docker containers"
        exit 1
    fi

    # List running containers
    log_info "Running containers:"
    docker_compose ps | tee -a "$LOG_FILE"
}

phase_2_cluster_formation() {
    log_step "Phase 2: Cluster Formation"

    # Wait for all controllers to be healthy
    if wait_for_cluster 3 "$CLUSTER_FORMATION_TIMEOUT"; then
        log_pass "All 3 controllers are healthy and connected"
    else
        log_fail "Cluster did not form within timeout"
        docker_compose logs | tail -100 >> "$LOG_FILE"
        exit 1
    fi

    # Identify the leader
    local leader
    leader=$(find_leader_container)

    if [[ -n "$leader" ]]; then
        log_pass "Leader identified: $leader"
        echo "$leader"
    else
        log_fail "Could not identify cluster leader"
        exit 1
    fi
}

phase_3_initial_job() {
    local leader="$1"
    log_step "Phase 3: Submit Initial Job"

    # Give cluster a moment to stabilize
    sleep 5

    # Submit a test job
    local job_id
    job_id=$(submit_test_job "$leader" "ha_test_initial")

    if [[ -n "$job_id" ]]; then
        log_pass "Initial job submitted: $job_id"
        echo "$job_id"
    else
        log_warn "Could not submit initial job (SLURM client may not be configured)"
        echo ""
    fi
}

phase_4_kill_leader() {
    local leader="$1"
    log_step "Phase 4: Kill Leader Node"

    log_info "Current leader: $leader"
    log_info "Stopping leader container..."

    # Stop the leader container
    docker stop "${PROJECT_NAME}-${leader}-1"

    if [[ $? -eq 0 ]]; then
        log_pass "Leader container $leader stopped"
    else
        log_fail "Failed to stop leader container"
        exit 1
    fi

    # Verify container is stopped
    if ! container_running "$leader"; then
        log_pass "Leader container confirmed stopped"
    else
        log_fail "Leader container is still running"
        exit 1
    fi
}

phase_5_new_leader() {
    local old_leader="$1"
    log_step "Phase 5: Verify New Leader Election"

    # Wait for new leader
    local new_leader
    new_leader=$(wait_for_new_leader "$old_leader" "$LEADER_ELECTION_TIMEOUT")

    if [[ -n "$new_leader" ]]; then
        log_pass "New leader elected: $new_leader"
        echo "$new_leader"
    else
        log_fail "No new leader was elected"
        exit 1
    fi
}

phase_6_verify_job() {
    local container="$1"
    local job_id="$2"
    log_step "Phase 6: Verify Job Still Tracked"

    if [[ -z "$job_id" ]]; then
        log_warn "No job ID to verify (skipping)"
        return 0
    fi

    # Give the cluster a moment to stabilize after failover
    sleep 5

    if check_job_tracked "$container" "$job_id"; then
        log_pass "Job $job_id is still tracked after failover"
    else
        log_fail "Job $job_id was lost during failover"
    fi
}

phase_7_new_job() {
    local leader="$1"
    log_step "Phase 7: Submit New Job After Failover"

    local job_id
    job_id=$(submit_test_job "$leader" "ha_test_after_failover")

    if [[ -n "$job_id" ]]; then
        log_pass "New job submitted after failover: $job_id"
        echo "$job_id"
    else
        log_warn "Could not submit new job (may be expected if SLURM client not configured)"
        echo ""
    fi
}

phase_8_rejoin_node() {
    local container="$1"
    log_step "Phase 8: Rejoin Killed Node"

    log_info "Restarting container: $container"

    # Start the container again
    docker start "${PROJECT_NAME}-${container}-1"

    if [[ $? -eq 0 ]]; then
        log_pass "Container $container restarted"
    else
        log_fail "Failed to restart container"
        exit 1
    fi

    # Wait for it to rejoin
    if wait_for_node_rejoin "$container" "$NODE_REJOIN_TIMEOUT"; then
        log_pass "Node $container successfully rejoined the cluster"
    else
        log_fail "Node $container failed to rejoin cluster"
    fi
}

phase_9_final_verification() {
    log_step "Phase 9: Final Cluster Verification"

    # Verify all 3 nodes are healthy
    local healthy_count=0

    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container"; then
            local connected
            connected=$(get_connected_nodes_count "$container")

            if [[ "$connected" -ge 2 ]]; then
                healthy_count=$((healthy_count + 1))
                log_info "Controller $container is healthy with $connected peers"
            else
                log_warn "Controller $container has only $connected peers"
            fi
        else
            log_warn "Controller $container is not running"
        fi
    done

    if [[ $healthy_count -eq 3 ]]; then
        log_pass "All 3 controllers are healthy and fully connected"
    elif [[ $healthy_count -ge 2 ]]; then
        log_warn "Only $healthy_count/3 controllers are fully healthy"
    else
        log_fail "Less than 2 controllers are healthy"
    fi

    # Verify there is exactly one leader
    local leader_count=0
    local current_leader=""

    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container" && is_leader "$container"; then
            leader_count=$((leader_count + 1))
            current_leader="$container"
        fi
    done

    if [[ $leader_count -eq 1 ]]; then
        log_pass "Exactly one leader exists: $current_leader"
    elif [[ $leader_count -eq 0 ]]; then
        log_fail "No leader in the cluster"
    else
        log_fail "Multiple leaders detected: $leader_count"
    fi
}

print_summary() {
    log_step "Test Summary"

    echo ""
    echo "=============================================="
    echo "  HA FAILOVER TEST RESULTS"
    echo "=============================================="
    echo "  Tests Passed: $TESTS_PASSED"
    echo "  Tests Failed: $TESTS_FAILED"
    echo "  Total Tests:  $TESTS_TOTAL"
    echo ""

    if [[ $TESTS_TOTAL -gt 0 ]]; then
        local pass_rate=$((TESTS_PASSED * 100 / TESTS_TOTAL))
        echo "  Pass Rate: ${pass_rate}%"
    fi

    echo ""
    echo "  Log File: $LOG_FILE"
    echo "=============================================="

    # Save summary JSON
    cat > "${TEST_RESULTS_DIR}/summary_${TIMESTAMP}.json" <<EOF
{
    "timestamp": "$(date -Iseconds)",
    "test": "ha_failover",
    "tests_passed": $TESTS_PASSED,
    "tests_failed": $TESTS_FAILED,
    "tests_total": $TESTS_TOTAL,
    "log_file": "$LOG_FILE"
}
EOF

    if [[ $TESTS_FAILED -eq 0 ]]; then
        echo ""
        echo -e "${GREEN}ALL TESTS PASSED${NC}"
        return 0
    else
        echo ""
        echo -e "${RED}SOME TESTS FAILED${NC}"
        return 1
    fi
}

###############################################################################
# Main
###############################################################################

main() {
    parse_args "$@"

    # Set up trap for cleanup
    trap trap_cleanup EXIT

    echo ""
    echo "=============================================="
    echo "  FLURM HA Failover Test"
    echo "=============================================="
    echo "  Compose File: $COMPOSE_FILE"
    echo "  Log File: $LOG_FILE"
    echo "  Skip Cleanup: $SKIP_CLEANUP"
    echo "  Verbose: $VERBOSE"
    echo "=============================================="
    echo ""

    # Phase 1: Setup
    phase_1_setup

    # Phase 2: Wait for cluster formation
    LEADER=$(phase_2_cluster_formation)

    # Phase 3: Submit initial job
    INITIAL_JOB_ID=$(phase_3_initial_job "$LEADER")

    # Phase 4: Kill the leader
    phase_4_kill_leader "$LEADER"

    # Phase 5: Wait for new leader
    NEW_LEADER=$(phase_5_new_leader "$LEADER")

    # Phase 6: Verify job is still tracked
    phase_6_verify_job "$NEW_LEADER" "$INITIAL_JOB_ID"

    # Phase 7: Submit new job
    NEW_JOB_ID=$(phase_7_new_job "$NEW_LEADER")

    # Phase 8: Rejoin the killed node
    phase_8_rejoin_node "$LEADER"

    # Phase 9: Final verification
    phase_9_final_verification

    # Print summary
    print_summary
}

main "$@"
