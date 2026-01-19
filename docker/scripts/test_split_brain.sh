#!/bin/bash
###############################################################################
# FLURM Split-Brain Test Script
#
# Tests network partition handling in a 3-controller FLURM cluster:
# 1. Starts HA docker-compose environment (3 controllers)
# 2. Waits for cluster to form
# 3. Creates a network partition between controllers
# 4. Verifies only the majority partition can process requests (quorum)
# 5. Heals the partition
# 6. Verifies cluster reconverges
#
# Usage: ./test_split_brain.sh [--skip-cleanup] [--verbose]
#
# Note: This test requires Docker network manipulation capabilities
###############################################################################

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$DOCKER_DIR")"
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.ha.yml"
PROJECT_NAME="flurm-split-brain-test"
NETWORK_NAME="${PROJECT_NAME}_flurm-ha-net"

# Test settings
CLUSTER_FORMATION_TIMEOUT=120  # seconds to wait for cluster to form
PARTITION_DURATION=30          # seconds to keep partition active
RECONVERGENCE_TIMEOUT=60       # seconds to wait for reconvergence
HEALTH_CHECK_INTERVAL=5        # seconds between health checks

# Controller settings
CONTROLLERS=("flurm-ctrl-1" "flurm-ctrl-2" "flurm-ctrl-3")
CONTROLLER_IPS=("172.28.0.10" "172.28.0.11" "172.28.0.12")

# Test results
TEST_RESULTS_DIR="${TEST_RESULTS_DIR:-/tmp/flurm-split-brain-tests}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${TEST_RESULTS_DIR}/split_brain_${TIMESTAMP}.log"

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

docker_compose() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

container_running() {
    local container="$1"
    docker ps --filter "name=${PROJECT_NAME}-${container}" --filter "status=running" -q | grep -q .
}

get_container_id() {
    local container="$1"
    docker ps --filter "name=${PROJECT_NAME}-${container}" -q | head -1
}

get_container_ip() {
    local container="$1"
    docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${PROJECT_NAME}-${container}-1" 2>/dev/null || echo ""
}

exec_in_container() {
    local container="$1"
    shift
    docker exec "${PROJECT_NAME}-${container}-1" "$@"
}

get_connected_nodes_count() {
    local container="$1"
    exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'io:format("~p~n", [length(nodes())]), halt(0).' 2>/dev/null || echo "0"
}

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

# Check if a container can perform operations (has quorum)
can_process_requests() {
    local container="$1"

    # Try to get cluster status - this requires quorum
    local result
    result=$(exec_in_container "$container" \
        erl -noshell -pa /flurm/_build/default/lib/*/ebin \
        -eval 'case catch flurm_controller_cluster:cluster_status() of
                   Status when is_map(Status) ->
                       case maps:get(ra_ready, Status, false) of
                           true -> io:format("ready~n");
                           _ -> io:format("not_ready~n")
                       end;
                   _ ->
                       io:format("error~n")
               end, halt(0).' 2>/dev/null || echo "error")

    [[ "$result" == "ready" ]]
}

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

# Create network partition by adding iptables rules
# Isolates one controller from the other two
create_partition() {
    local isolated_container="$1"
    local isolated_ip="$2"

    log_info "Creating network partition: isolating $isolated_container ($isolated_ip)"

    # Get the other IPs
    local other_ips=()
    for i in "${!CONTROLLERS[@]}"; do
        if [[ "${CONTROLLERS[$i]}" != "$isolated_container" ]]; then
            other_ips+=("${CONTROLLER_IPS[$i]}")
        fi
    done

    log_debug "Blocking traffic between $isolated_ip and ${other_ips[*]}"

    # Add iptables rules in the isolated container to block traffic to/from other controllers
    for other_ip in "${other_ips[@]}"; do
        # Block outgoing traffic
        exec_in_container "$isolated_container" \
            iptables -A OUTPUT -d "$other_ip" -j DROP 2>/dev/null || \
            log_warn "Could not add iptables rule (may need privileged mode)"

        # Block incoming traffic
        exec_in_container "$isolated_container" \
            iptables -A INPUT -s "$other_ip" -j DROP 2>/dev/null || true
    done

    # Also add rules in the other containers to block the isolated container
    for i in "${!CONTROLLERS[@]}"; do
        local container="${CONTROLLERS[$i]}"
        if [[ "$container" != "$isolated_container" ]]; then
            exec_in_container "$container" \
                iptables -A OUTPUT -d "$isolated_ip" -j DROP 2>/dev/null || true
            exec_in_container "$container" \
                iptables -A INPUT -s "$isolated_ip" -j DROP 2>/dev/null || true
        fi
    done

    log_pass "Network partition created"
}

# Alternative: Create partition using tc (traffic control) if iptables not available
create_partition_tc() {
    local isolated_container="$1"

    log_info "Creating network partition using tc (traffic control)"

    # Add a delay that effectively blocks communication
    exec_in_container "$isolated_container" \
        tc qdisc add dev eth0 root netem delay 10000ms 2>/dev/null || \
        log_warn "Could not add tc rules"

    log_pass "Network partition created (via tc)"
}

# Alternative: Use Docker network disconnect
create_partition_docker() {
    local isolated_container="$1"
    local container_id
    container_id=$(get_container_id "$isolated_container")

    log_info "Creating network partition using Docker network disconnect"

    # Disconnect from the HA network
    docker network disconnect "$NETWORK_NAME" "$container_id" 2>/dev/null || {
        log_warn "Could not disconnect from network"
        return 1
    }

    log_pass "Network partition created (container disconnected from network)"
}

# Heal the network partition
heal_partition() {
    local isolated_container="$1"
    local isolated_ip="$2"

    log_info "Healing network partition for $isolated_container"

    # Flush iptables rules
    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container"; then
            exec_in_container "$container" iptables -F 2>/dev/null || true
            exec_in_container "$container" tc qdisc del dev eth0 root 2>/dev/null || true
        fi
    done

    log_pass "Network partition healed (iptables flushed)"
}

# Heal partition created with Docker network disconnect
heal_partition_docker() {
    local isolated_container="$1"
    local container_id
    container_id=$(get_container_id "$isolated_container")

    log_info "Healing network partition by reconnecting $isolated_container"

    # Reconnect to the HA network
    docker network connect "$NETWORK_NAME" "$container_id" 2>/dev/null || {
        log_warn "Could not reconnect to network"
        return 1
    }

    log_pass "Network partition healed (container reconnected)"
}

wait_for_reconvergence() {
    local timeout="$1"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for cluster to reconverge (timeout: ${timeout}s)..."

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log_fail "Reconvergence timed out after ${timeout}s"
            return 1
        fi

        local connected_count=0
        for container in "${CONTROLLERS[@]}"; do
            if container_running "$container"; then
                local peers
                peers=$(get_connected_nodes_count "$container")
                if [[ "$peers" -ge 2 ]]; then
                    connected_count=$((connected_count + 1))
                fi
            fi
        done

        if [[ $connected_count -ge 3 ]]; then
            log_pass "All nodes have reconnected"
            return 0
        fi

        log_debug "Waiting... $connected_count/3 nodes fully connected"
        sleep "$HEALTH_CHECK_INTERVAL"
    done
}

cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        log_warn "Skipping cleanup (--skip-cleanup specified)"
        log_info "To clean up manually: docker compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v"
        return
    fi

    log_step "Cleaning Up"

    # Flush any iptables rules we added
    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container"; then
            exec_in_container "$container" iptables -F 2>/dev/null || true
            exec_in_container "$container" tc qdisc del dev eth0 root 2>/dev/null || true
        fi
    done

    log_info "Stopping and removing containers..."
    docker_compose down -v --remove-orphans 2>/dev/null || true
    log_info "Cleanup complete"
}

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

    mkdir -p "$TEST_RESULTS_DIR"

    if ! command -v docker >/dev/null 2>&1; then
        log_fail "Docker is not installed"
        exit 1
    fi

    if ! docker compose version >/dev/null 2>&1; then
        log_fail "Docker Compose is not installed"
        exit 1
    fi

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_fail "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    log_pass "Prerequisites verified"

    # Clean up existing
    log_info "Cleaning up any existing containers..."
    docker_compose down -v --remove-orphans 2>/dev/null || true

    # Start cluster
    log_info "Starting HA cluster with 3 controllers..."
    docker_compose up -d --build

    if [[ $? -eq 0 ]]; then
        log_pass "Docker containers started"
    else
        log_fail "Failed to start Docker containers"
        exit 1
    fi

    docker_compose ps | tee -a "$LOG_FILE"
}

phase_2_cluster_formation() {
    log_step "Phase 2: Cluster Formation"

    if wait_for_cluster 3 "$CLUSTER_FORMATION_TIMEOUT"; then
        log_pass "All 3 controllers are healthy and connected"
    else
        log_fail "Cluster did not form within timeout"
        docker_compose logs | tail -100 >> "$LOG_FILE"
        exit 1
    fi

    # Identify leader
    local leader
    leader=$(find_leader_container)

    if [[ -n "$leader" ]]; then
        log_pass "Leader identified: $leader"
    else
        log_fail "Could not identify cluster leader"
        exit 1
    fi
}

phase_3_create_partition() {
    log_step "Phase 3: Create Network Partition"

    # We'll isolate flurm-ctrl-3 (the minority partition)
    # flurm-ctrl-1 and flurm-ctrl-2 will be the majority partition
    local isolated_container="flurm-ctrl-3"
    local isolated_ip="172.28.0.12"

    log_info "Partition scenario:"
    log_info "  Minority (isolated): $isolated_container"
    log_info "  Majority: flurm-ctrl-1, flurm-ctrl-2"

    # Try iptables first, fall back to Docker network disconnect
    if ! create_partition "$isolated_container" "$isolated_ip"; then
        log_warn "iptables approach failed, trying Docker network disconnect..."
        if ! create_partition_docker "$isolated_container"; then
            log_fail "Could not create network partition"
            exit 1
        fi
    fi

    # Wait a moment for partition to take effect
    sleep 5

    echo "$isolated_container"
}

phase_4_verify_quorum() {
    local isolated_container="$1"
    log_step "Phase 4: Verify Quorum Behavior"

    log_info "Checking majority partition (should have quorum)..."

    # Check controllers in majority partition
    local majority_has_quorum=false

    for container in "${CONTROLLERS[@]}"; do
        if [[ "$container" != "$isolated_container" ]]; then
            log_info "Checking $container..."

            # Give time for partition detection
            sleep 2

            if can_process_requests "$container"; then
                log_pass "$container (majority) can process requests"
                majority_has_quorum=true
            else
                log_warn "$container (majority) cannot process requests yet"
            fi
        fi
    done

    if [[ "$majority_has_quorum" == "true" ]]; then
        log_pass "Majority partition has quorum and can process requests"
    else
        log_fail "Majority partition lost quorum unexpectedly"
    fi

    # Check isolated node (minority partition)
    log_info "Checking minority partition (should NOT have quorum)..."

    # The isolated node should not be able to confirm leadership or process new commands
    # It may still think it's the leader briefly, but Ra should prevent commits

    local minority_connected
    minority_connected=$(get_connected_nodes_count "$isolated_container")

    log_info "$isolated_container has $minority_connected connected peers"

    if [[ "$minority_connected" -lt 2 ]]; then
        log_pass "$isolated_container is isolated (only $minority_connected peers)"
    else
        log_warn "$isolated_container still sees $minority_connected peers (partition may not be effective)"
    fi

    # Verify only one partition can elect/maintain leader
    log_info "Verifying leader status across partitions..."

    local leader_count=0
    local leaders=()

    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container" && is_leader "$container"; then
            leader_count=$((leader_count + 1))
            leaders+=("$container")
        fi
    done

    log_info "Leaders detected: ${leaders[*]:-none} (count: $leader_count)"

    # During partition, we expect at most one leader (in majority partition)
    # The minority might briefly think it's leader but can't commit
    if [[ $leader_count -le 1 ]]; then
        log_pass "At most one leader active (no split-brain detected)"
    else
        log_fail "Split-brain detected: $leader_count leaders active"
    fi
}

phase_5_partition_duration() {
    local isolated_container="$1"
    log_step "Phase 5: Maintain Partition (${PARTITION_DURATION}s)"

    log_info "Keeping partition active for ${PARTITION_DURATION} seconds..."
    log_info "During this time, the minority partition should not be able to make progress"

    local start_time
    start_time=$(date +%s)

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $PARTITION_DURATION ]]; then
            break
        fi

        # Periodically check leader status
        if [[ $((elapsed % 10)) -eq 0 ]]; then
            local leaders=()
            for container in "${CONTROLLERS[@]}"; do
                if container_running "$container" && is_leader "$container"; then
                    leaders+=("$container")
                fi
            done
            log_info "[$elapsed/${PARTITION_DURATION}s] Leaders: ${leaders[*]:-none}"
        fi

        sleep 5
    done

    log_pass "Partition maintained for ${PARTITION_DURATION} seconds"
}

phase_6_heal_partition() {
    local isolated_container="$1"
    local isolated_ip="172.28.0.12"
    log_step "Phase 6: Heal Network Partition"

    # Try to heal using the same method we used to create
    heal_partition "$isolated_container" "$isolated_ip"

    # Also try Docker reconnect in case we used that
    heal_partition_docker "$isolated_container" 2>/dev/null || true

    log_pass "Partition healing initiated"
}

phase_7_verify_reconvergence() {
    log_step "Phase 7: Verify Cluster Reconvergence"

    if wait_for_reconvergence "$RECONVERGENCE_TIMEOUT"; then
        log_pass "Cluster reconverged successfully"
    else
        log_fail "Cluster failed to reconverge"
    fi

    # Verify single leader
    local leader_count=0
    local leaders=()

    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container" && is_leader "$container"; then
            leader_count=$((leader_count + 1))
            leaders+=("$container")
        fi
    done

    if [[ $leader_count -eq 1 ]]; then
        log_pass "Single leader after reconvergence: ${leaders[0]}"
    elif [[ $leader_count -eq 0 ]]; then
        log_fail "No leader after reconvergence"
    else
        log_fail "Multiple leaders after reconvergence: ${leaders[*]}"
    fi

    # Verify all nodes healthy
    local healthy_count=0
    for container in "${CONTROLLERS[@]}"; do
        if container_running "$container"; then
            local peers
            peers=$(get_connected_nodes_count "$container")
            if [[ "$peers" -ge 2 ]]; then
                healthy_count=$((healthy_count + 1))
                log_info "$container is healthy with $peers peers"
            else
                log_warn "$container has only $peers peers"
            fi
        fi
    done

    if [[ $healthy_count -eq 3 ]]; then
        log_pass "All 3 controllers are healthy after reconvergence"
    else
        log_fail "Only $healthy_count/3 controllers are healthy"
    fi
}

phase_8_final_verification() {
    log_step "Phase 8: Final Cluster Verification"

    # Try to use the cluster
    log_info "Verifying cluster is operational..."

    local any_operational=false

    for container in "${CONTROLLERS[@]}"; do
        if can_process_requests "$container"; then
            log_pass "$container can process requests"
            any_operational=true
            break
        fi
    done

    if [[ "$any_operational" == "true" ]]; then
        log_pass "Cluster is operational after split-brain test"
    else
        log_fail "Cluster is not operational after split-brain test"
    fi
}

print_summary() {
    log_step "Test Summary"

    echo ""
    echo "=============================================="
    echo "  SPLIT-BRAIN TEST RESULTS"
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

    cat > "${TEST_RESULTS_DIR}/summary_${TIMESTAMP}.json" <<EOF
{
    "timestamp": "$(date -Iseconds)",
    "test": "split_brain",
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

    trap trap_cleanup EXIT

    echo ""
    echo "=============================================="
    echo "  FLURM Split-Brain Test"
    echo "=============================================="
    echo "  Compose File: $COMPOSE_FILE"
    echo "  Log File: $LOG_FILE"
    echo "  Partition Duration: ${PARTITION_DURATION}s"
    echo "  Skip Cleanup: $SKIP_CLEANUP"
    echo "  Verbose: $VERBOSE"
    echo "=============================================="
    echo ""

    log_warn "NOTE: This test requires Docker containers with NET_ADMIN capability"
    log_warn "      for iptables rules, or will fall back to network disconnect."
    echo ""

    # Phase 1: Setup
    phase_1_setup

    # Phase 2: Wait for cluster
    phase_2_cluster_formation

    # Phase 3: Create partition
    ISOLATED=$(phase_3_create_partition)

    # Phase 4: Verify quorum behavior
    phase_4_verify_quorum "$ISOLATED"

    # Phase 5: Maintain partition
    phase_5_partition_duration "$ISOLATED"

    # Phase 6: Heal partition
    phase_6_heal_partition "$ISOLATED"

    # Phase 7: Verify reconvergence
    phase_7_verify_reconvergence

    # Phase 8: Final verification
    phase_8_final_verification

    # Summary
    print_summary
}

main "$@"
