#!/bin/bash
#
# FLURM HA Mode Test Scenario
#
# Tests high-availability features:
# - 3-node cluster formation
# - Leader election
# - Failover on leader kill
# - Job survival across failover
# - Node rejoin after failure
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

# Get leader from any controller
get_leader() {
    for ctrl in flurm-test-ctrl-1 flurm-test-ctrl-2 flurm-test-ctrl-3; do
        LEADER=$(docker exec "$ctrl" curl -sf http://localhost:9090/cluster/leader 2>/dev/null || true)
        if [ -n "$LEADER" ]; then
            echo "$LEADER"
            return 0
        fi
    done
    return 1
}

# Check if node is healthy
check_node_health() {
    local node=$1
    docker exec "$node" curl -sf http://localhost:9090/health > /dev/null 2>&1
}

# Wait for leader election (max 60 seconds)
wait_for_leader() {
    local max_wait=${1:-60}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        LEADER=$(get_leader)
        if [ -n "$LEADER" ]; then
            echo "$LEADER"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    return 1
}

# Submit a test job and return job ID
submit_test_job() {
    exec_client sbatch --parsable /jobs/batch/job_cpu_small.sh 2>/dev/null
}

# Get job state
get_job_state() {
    local job_id=$1
    exec_client sacct -j "$job_id" -n -o State --parsable2 2>/dev/null | head -1 | tr -d ' '
}

main() {
    echo "=========================================="
    echo "FLURM HA Mode Tests"
    echo "=========================================="

    # Test 1: All three controllers healthy
    log_test "All controllers healthy"
    HEALTHY_COUNT=0
    for ctrl in flurm-test-ctrl-1 flurm-test-ctrl-2 flurm-test-ctrl-3; do
        if check_node_health "$ctrl"; then
            HEALTHY_COUNT=$((HEALTHY_COUNT + 1))
            log_info "$ctrl is healthy"
        else
            log_info "$ctrl is NOT healthy"
        fi
    done

    if [ "$HEALTHY_COUNT" -eq 3 ]; then
        log_pass "All 3 controllers are healthy"
    else
        log_fail "Only $HEALTHY_COUNT/3 controllers healthy"
    fi

    # Test 2: Leader elected
    log_test "Leader election"
    LEADER=$(wait_for_leader 30)
    if [ -n "$LEADER" ]; then
        log_pass "Leader elected: $LEADER"
    else
        log_fail "No leader elected within 30 seconds"
        return 1
    fi

    # Test 3: Cluster size
    log_test "Cluster size is 3"
    CLUSTER_SIZE=$(docker exec flurm-test-ctrl-1 curl -sf http://localhost:9090/cluster/size 2>/dev/null || echo "0")
    if [ "$CLUSTER_SIZE" = "3" ]; then
        log_pass "Cluster size is 3"
    else
        log_fail "Cluster size is $CLUSTER_SIZE (expected 3)"
    fi

    # Test 4: Submit jobs before failover
    log_test "Submit 5 jobs before failover"
    JOB_IDS=()
    for i in $(seq 1 5); do
        JOB_ID=$(submit_test_job)
        if [ -n "$JOB_ID" ]; then
            JOB_IDS+=("$JOB_ID")
            log_info "Submitted job $JOB_ID"
        fi
    done

    if [ ${#JOB_IDS[@]} -eq 5 ]; then
        log_pass "All 5 jobs submitted"
    else
        log_fail "Only ${#JOB_IDS[@]}/5 jobs submitted"
    fi

    # Give jobs time to start
    sleep 5

    # Test 5: Kill the leader
    log_test "Kill leader node"
    LEADER_CONTAINER=""
    case "$LEADER" in
        *ctrl-1*) LEADER_CONTAINER="flurm-test-ctrl-1" ;;
        *ctrl-2*) LEADER_CONTAINER="flurm-test-ctrl-2" ;;
        *ctrl-3*) LEADER_CONTAINER="flurm-test-ctrl-3" ;;
    esac

    if [ -n "$LEADER_CONTAINER" ]; then
        log_info "Stopping $LEADER_CONTAINER..."
        docker stop "$LEADER_CONTAINER" > /dev/null 2>&1
        log_pass "Leader container stopped"
    else
        log_fail "Could not determine leader container"
        return 1
    fi

    # Test 6: New leader elected
    log_test "New leader election (max 30s)"
    sleep 5  # Give cluster time to detect failure

    NEW_LEADER=$(wait_for_leader 30)
    if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER" ]; then
        log_pass "New leader elected: $NEW_LEADER"
    elif [ "$NEW_LEADER" = "$LEADER" ]; then
        log_fail "Same leader reported (old leader should be dead)"
    else
        log_fail "No new leader elected within 30 seconds"
    fi

    # Test 7: Jobs still tracked after failover
    log_test "Jobs tracked after failover"
    TRACKED_COUNT=0
    for JOB_ID in "${JOB_IDS[@]}"; do
        STATE=$(get_job_state "$JOB_ID" 2>/dev/null || echo "")
        if [ -n "$STATE" ]; then
            TRACKED_COUNT=$((TRACKED_COUNT + 1))
            log_info "Job $JOB_ID state: $STATE"
        else
            log_info "Job $JOB_ID: not found"
        fi
    done

    if [ "$TRACKED_COUNT" -eq 5 ]; then
        log_pass "All 5 jobs still tracked"
    else
        log_fail "Only $TRACKED_COUNT/5 jobs tracked after failover"
    fi

    # Test 8: Can submit new jobs after failover
    log_test "Submit job after failover"
    NEW_JOB=$(submit_test_job)
    if [ -n "$NEW_JOB" ]; then
        log_pass "New job submitted after failover: $NEW_JOB"
    else
        log_fail "Cannot submit jobs after failover"
    fi

    # Test 9: Restart failed node
    log_test "Restart failed node"
    docker start "$LEADER_CONTAINER" > /dev/null 2>&1
    sleep 10  # Give node time to rejoin

    if check_node_health "$LEADER_CONTAINER"; then
        log_pass "Restarted node is healthy"
    else
        log_fail "Restarted node is not healthy"
    fi

    # Test 10: Cluster size restored
    log_test "Cluster size restored to 3"
    sleep 5  # Give cluster time to stabilize

    # Try all nodes to get cluster size
    CLUSTER_SIZE="0"
    for ctrl in flurm-test-ctrl-1 flurm-test-ctrl-2 flurm-test-ctrl-3; do
        SIZE=$(docker exec "$ctrl" curl -sf http://localhost:9090/cluster/size 2>/dev/null || true)
        if [ -n "$SIZE" ] && [ "$SIZE" -gt "$CLUSTER_SIZE" ] 2>/dev/null; then
            CLUSTER_SIZE="$SIZE"
        fi
    done

    if [ "$CLUSTER_SIZE" = "3" ]; then
        log_pass "Cluster size restored to 3"
    else
        log_fail "Cluster size is $CLUSTER_SIZE (expected 3)"
    fi

    # Test 11: All controllers healthy again
    log_test "All controllers healthy after recovery"
    HEALTHY_COUNT=0
    for ctrl in flurm-test-ctrl-1 flurm-test-ctrl-2 flurm-test-ctrl-3; do
        if check_node_health "$ctrl"; then
            HEALTHY_COUNT=$((HEALTHY_COUNT + 1))
        fi
    done

    if [ "$HEALTHY_COUNT" -eq 3 ]; then
        log_pass "All 3 controllers healthy after recovery"
    else
        log_fail "Only $HEALTHY_COUNT/3 controllers healthy after recovery"
    fi

    # Summary
    echo ""
    echo "=========================================="
    echo "HA Test Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC} $PASSED"
    echo -e "${RED}Failed:${NC} $FAILED"

    if [ "$FAILED" -gt 0 ]; then
        return 1
    fi
    return 0
}

main
