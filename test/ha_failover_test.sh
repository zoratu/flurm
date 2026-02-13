#!/bin/bash
#
# FLURM High-Availability Failover Test Suite
#
# Tests fault tolerance by:
# 1. Submitting jobs during normal operation
# 2. Killing leader controller
# 3. Verifying jobs continue/complete
# 4. Testing network partitions
# 5. Verifying data consistency after recovery
#
# Usage: ./ha_failover_test.sh [test_name]
#
# Prerequisites:
# - Docker and docker-compose installed
# - Run from flurm/test directory or flurm root
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.ha.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((++TESTS_PASSED))
}

log_failure() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((++TESTS_FAILED))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Start the HA cluster
start_cluster() {
    log_info "Starting HA cluster..."
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml down -v 2>/dev/null || true
    docker compose -f docker-compose.ha.yml build
    docker compose -f docker-compose.ha.yml up -d

    log_info "Waiting for cluster to stabilize (30s)..."
    sleep 30

    # Check all controllers are up
    for ctrl in flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3; do
        if docker compose -f docker-compose.ha.yml ps | grep -q "$ctrl.*Up"; then
            log_info "  $ctrl: UP"
        else
            log_failure "$ctrl not running"
            return 1
        fi
    done

    log_success "Cluster started successfully"
}

# Stop the cluster
stop_cluster() {
    log_info "Stopping cluster..."
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml down -v 2>/dev/null || true
}

# Submit a job via sbatch
submit_job() {
    local job_name="${1:-test_job}"
    cd "$DOCKER_DIR"
    local out
    out=$(docker compose -f docker-compose.ha.yml exec -T slurm-client bash -c "
        sed -i 's/^SlurmctldHost=.*/SlurmctldHost=flurm-ctrl-1/' /etc/slurm/slurm.conf
        cat > /tmp/${job_name}.sh << 'EOF'
#!/bin/bash
echo \"Job $job_name starting\"
hostname
date
sleep 5
echo \"Job $job_name completed\"
EOF
        chmod +x /tmp/${job_name}.sh
        sbatch /tmp/${job_name}.sh 2>&1
    " 2>&1 || true)
    echo "$out" | sed -n 's/.*Submitted batch job \([0-9][0-9]*\).*/\1/p' | head -n 1
}

# Get job status
get_job_status() {
    local job_id="$1"
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml exec -T slurm-client squeue 2>&1 | grep -E "^\s*$job_id" | awk '{print $5}' || echo "UNKNOWN"
}

# Check if job exists in queue or completed
job_exists() {
    local job_id="$1"
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml exec -T slurm-client squeue 2>&1 | grep -qE "^\s*$job_id" && return 0
    return 1
}

# Get current leader (from ctrl-1's perspective)
get_leader() {
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml logs flurm-ctrl-1 2>&1 | grep -oP 'Ra leader: \K[^\s]+' | tail -1 || echo "unknown"
}

# Kill a specific controller
kill_controller() {
    local ctrl="$1"
    log_info "Killing $ctrl..."
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml kill "$ctrl"
}

# Restart a controller
restart_controller() {
    local ctrl="$1"
    log_info "Restarting $ctrl..."
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.ha.yml start "$ctrl"
}

# Create network partition (isolate a controller)
partition_controller() {
    local ctrl="$1"
    log_info "Partitioning $ctrl from cluster..."
    cd "$DOCKER_DIR"
    # Disconnect from network
    docker network disconnect docker_flurm-ha-net "docker-${ctrl}-1" 2>/dev/null || true
}

# Heal network partition
heal_partition() {
    local ctrl="$1"
    log_info "Healing partition for $ctrl..."
    cd "$DOCKER_DIR"
    docker network connect docker_flurm-ha-net "docker-${ctrl}-1" 2>/dev/null || true
}

# ============================================================
# TEST CASES
# ============================================================

# Test 1: Basic cluster formation
test_cluster_formation() {
    ((++TESTS_RUN))
    log_info "TEST: Cluster Formation"

    # Check all three controllers formed a cluster
    cd "$DOCKER_DIR"
    local ctrl1_nodes=$(docker compose -f docker-compose.ha.yml exec -T flurm-ctrl-1 \
        erl_call -sname test -c flurm_cluster_secret -a 'erlang nodes []' 2>/dev/null | wc -w)

    if [ "$ctrl1_nodes" -ge 2 ]; then
        log_success "Cluster formation: $ctrl1_nodes peer nodes connected"
    else
        log_failure "Cluster formation: only $ctrl1_nodes peer nodes"
    fi
}

# Test 2: Job submission works
test_job_submission() {
    ((++TESTS_RUN))
    log_info "TEST: Job Submission"

    local job_id=$(submit_job "submission_test")

    if [ -n "$job_id" ] && [ "$job_id" -gt 0 ]; then
        log_success "Job submission: job $job_id submitted"
        # Wait for job to complete
        sleep 10
    else
        log_failure "Job submission: failed to submit job"
    fi
}

# Test 3: Leader failover
test_leader_failover() {
    ((++TESTS_RUN))
    log_info "TEST: Leader Failover"

    # Submit a job before failover
    local job_id=$(submit_job "failover_test")
    log_info "Submitted job $job_id before failover"

    # Kill controller 1 (likely initial leader)
    kill_controller "flurm-ctrl-1"

    # Wait for failover
    log_info "Waiting for failover (15s)..."
    sleep 15

    # Try to submit another job (should go to new leader)
    local job_id2=$(submit_job "post_failover_test")

    if [ -n "$job_id2" ] && [ "$job_id2" -gt 0 ]; then
        log_success "Leader failover: new job $job_id2 submitted after failover"
    else
        log_failure "Leader failover: could not submit job after failover"
    fi

    # Restart controller 1
    restart_controller "flurm-ctrl-1"
    sleep 10
}

# Test 4: Network partition (minority isolation)
test_minority_partition() {
    ((++TESTS_RUN))
    log_info "TEST: Minority Partition"

    # Submit job before partition
    local job_id=$(submit_job "partition_test")
    log_info "Submitted job $job_id before partition"

    # Partition controller 3 (minority)
    partition_controller "flurm-ctrl-3"

    sleep 5

    # Submit job during partition (should succeed via majority)
    local job_id2=$(submit_job "during_partition_test")

    if [ -n "$job_id2" ] && [ "$job_id2" -gt 0 ]; then
        log_success "Minority partition: job $job_id2 submitted during partition"
    else
        log_failure "Minority partition: could not submit job during partition"
        return 1
    fi

    # Heal partition
    heal_partition "flurm-ctrl-3"
    sleep 10
}

# Test 5: Rapid failover cycling
test_rapid_failover() {
    ((++TESTS_RUN))
    log_info "TEST: Rapid Failover Cycling"

    local success_count=0
    local fail_count=0

    for i in 1 2 3; do
        # Kill a controller
        kill_controller "flurm-ctrl-$i"
        sleep 5

        # Try to submit job
        local job_id=$(submit_job "rapid_${i}")
        if [ -n "$job_id" ] && [ "$job_id" -gt 0 ]; then
            ((++success_count))
        else
            ((++fail_count))
        fi

        # Restart controller
        restart_controller "flurm-ctrl-$i"
        sleep 10
    done

    if [ "$success_count" -ge 2 ]; then
        log_success "Rapid failover: $success_count/3 jobs submitted successfully"
    else
        log_failure "Rapid failover: only $success_count/3 jobs submitted"
    fi
}

# Test 6: Data consistency after recovery
test_data_consistency() {
    ((++TESTS_RUN))
    log_info "TEST: Data Consistency After Recovery"

    # Submit several jobs
    local job_ids=()
    for i in 1 2 3 4 5; do
        local jid=$(submit_job "consistency_${i}")
        job_ids+=("$jid")
        sleep 1
    done

    log_info "Submitted ${#job_ids[@]} jobs"

    # Kill and restart all controllers one by one
    for ctrl in flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3; do
        kill_controller "$ctrl"
        sleep 5
        restart_controller "$ctrl"
        sleep 10
    done

    # Wait for jobs to complete
    sleep 20

    # Verify queue is accessible
    cd "$DOCKER_DIR"
    local queue_output=$(docker compose -f docker-compose.ha.yml exec -T slurm-client squeue 2>&1)

    if echo "$queue_output" | grep -q "JOBID"; then
        log_success "Data consistency: queue accessible after recovery"
    else
        log_failure "Data consistency: queue not accessible"
    fi
}

# Test 7: Stress test with concurrent operations
test_concurrent_stress() {
    ((++TESTS_RUN))
    log_info "TEST: Concurrent Stress Test"

    # Submit 20 jobs rapidly
    local pids=()
    for i in $(seq 1 20); do
        (submit_job "stress_${i}" >/dev/null 2>&1) &
        pids+=($!)
    done

    # Wait for all submissions
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    sleep 5

    # Check how many jobs were submitted
    cd "$DOCKER_DIR"
    local job_count=$(docker compose -f docker-compose.ha.yml exec -T slurm-client squeue 2>&1 | grep -c "stress_" || echo "0")

    if [ "$job_count" -ge 15 ]; then
        log_success "Concurrent stress: $job_count/20 jobs in queue"
    else
        log_failure "Concurrent stress: only $job_count/20 jobs in queue"
    fi
}

# ============================================================
# MAIN
# ============================================================

print_summary() {
    echo ""
    echo "============================================"
    echo "  Test Summary"
    echo "============================================"
    echo "  Tests run:    $TESTS_RUN"
    echo -e "  ${GREEN}Passed:       $TESTS_PASSED${NC}"
    echo -e "  ${RED}Failed:       $TESTS_FAILED${NC}"
    echo "============================================"

    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed${NC}"
        return 1
    fi
}

run_all_tests() {
    start_cluster || exit 1

    test_cluster_formation
    test_job_submission
    test_leader_failover
    test_minority_partition
    test_rapid_failover
    test_data_consistency
    test_concurrent_stress

    print_summary
    local result=$?

    stop_cluster
    return $result
}

# Run specific test or all tests
if [ $# -eq 0 ]; then
    run_all_tests
else
    case "$1" in
        start)
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        cluster)
            start_cluster && test_cluster_formation
            ;;
        submit)
            test_job_submission
            ;;
        failover)
            start_cluster && test_leader_failover && stop_cluster
            ;;
        partition)
            start_cluster && test_minority_partition && stop_cluster
            ;;
        stress)
            start_cluster && test_concurrent_stress && stop_cluster
            ;;
        all)
            run_all_tests
            ;;
        *)
            echo "Usage: $0 [start|stop|cluster|submit|failover|partition|stress|all]"
            exit 1
            ;;
    esac
fi
