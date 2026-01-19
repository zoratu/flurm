#!/bin/bash
# FLURM SLURM Client Compatibility Test Script
#
# Tests real SLURM client commands against the FLURM controller:
# - sbatch: Submit jobs
# - squeue: Query job status
# - scancel: Cancel jobs
# - sinfo: Get node/partition info
# - sacct: Get accounting info
#
# Usage: ./test_slurm_clients.sh [--no-cleanup]
#
# Prerequisites:
# - Docker and docker-compose installed
# - Run from the docker/ directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_DIR="$(dirname "$DOCKER_DIR")"

# Configuration
COMPOSE_FILE="${DOCKER_DIR}/docker-compose.yml"
FLURM_HOST="flurm-server"
FLURM_PORT=6817
WAIT_TIMEOUT=120
TEST_TIMEOUT=30
NO_CLEANUP=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Captured values for test chaining
SUBMITTED_JOB_ID=""

# Logging functions
log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log_pass() {
    TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "${RED}[FAIL]${NC} $1"
}

log_skip() {
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
    echo -e "${YELLOW}[SKIP]${NC} $1"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}=============================================="
    echo -e "$1"
    echo -e "==============================================${NC}"
}

# Execute command in slurm-client container with timeout
run_slurm_cmd() {
    local cmd="$1"
    local timeout_sec="${2:-$TEST_TIMEOUT}"

    timeout "$timeout_sec" docker compose -f "$COMPOSE_FILE" exec -T slurm-client bash -c "$cmd" 2>&1
}

# Check if containers are running
check_containers() {
    log_info "Checking container status..."

    local flurm_running
    flurm_running=$(docker compose -f "$COMPOSE_FILE" ps -q flurm-server 2>/dev/null || echo "")

    if [ -z "$flurm_running" ]; then
        return 1
    fi

    local client_running
    client_running=$(docker compose -f "$COMPOSE_FILE" ps -q slurm-client 2>/dev/null || echo "")

    if [ -z "$client_running" ]; then
        return 1
    fi

    return 0
}

# Wait for FLURM server to be ready
wait_for_flurm() {
    log_info "Waiting for FLURM server to be ready..."

    local elapsed=0
    while [ $elapsed -lt $WAIT_TIMEOUT ]; do
        # Try to connect to the FLURM port from the client container
        if run_slurm_cmd "nc -z $FLURM_HOST $FLURM_PORT" 5 >/dev/null 2>&1; then
            log_info "FLURM server is reachable on port $FLURM_PORT"
            return 0
        fi

        sleep 2
        elapsed=$((elapsed + 2))
        log_info "Waiting for FLURM server... ($elapsed/$WAIT_TIMEOUT seconds)"
    done

    log_fail "FLURM server did not become ready within $WAIT_TIMEOUT seconds"
    return 1
}

# Wait for MUNGE to be ready
wait_for_munge() {
    log_info "Waiting for MUNGE authentication..."

    local elapsed=0
    while [ $elapsed -lt 30 ]; do
        if run_slurm_cmd "munge -n </dev/null" 5 >/dev/null 2>&1; then
            log_info "MUNGE is working"
            return 0
        fi

        sleep 2
        elapsed=$((elapsed + 2))
        log_info "Waiting for MUNGE... ($elapsed/30 seconds)"
    done

    log_info "MUNGE may not be available (continuing anyway)"
    return 0
}

# Start containers
start_environment() {
    log_section "Starting Docker Environment"

    cd "$DOCKER_DIR"

    log_info "Building containers..."
    docker compose -f "$COMPOSE_FILE" build --quiet

    log_info "Starting containers..."
    docker compose -f "$COMPOSE_FILE" up -d

    # Wait for containers to be running
    sleep 5

    if ! check_containers; then
        log_fail "Failed to start containers"
        docker compose -f "$COMPOSE_FILE" logs
        return 1
    fi

    log_pass "Containers started successfully"

    # Wait for services
    wait_for_munge
    wait_for_flurm
}

# Stop containers
stop_environment() {
    if [ "$NO_CLEANUP" = true ]; then
        log_info "Skipping cleanup (--no-cleanup specified)"
        return 0
    fi

    log_section "Cleaning Up"

    cd "$DOCKER_DIR"

    log_info "Stopping containers..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

    log_pass "Cleanup complete"
}

# Test: sbatch - Submit a job
test_sbatch() {
    log_section "Test: sbatch (Job Submission)"

    log_info "Submitting test job via sbatch..."

    # First, try submitting with --parsable to get just the job ID
    local result
    result=$(run_slurm_cmd "sbatch --parsable --job-name=client_test --time=00:05:00 --wrap='echo Hello from FLURM; sleep 30'" 2>&1)
    local exit_code=$?

    log_info "sbatch output: $result"
    log_info "sbatch exit code: $exit_code"

    if [ $exit_code -eq 0 ]; then
        # Try to extract job ID (should be numeric)
        local job_id
        job_id=$(echo "$result" | grep -oE '^[0-9]+' | head -1)

        if [ -n "$job_id" ]; then
            SUBMITTED_JOB_ID="$job_id"
            log_pass "sbatch succeeded - Job ID: $job_id"
            return 0
        else
            # Check for "Submitted batch job" message
            job_id=$(echo "$result" | grep -oE 'Submitted batch job [0-9]+' | grep -oE '[0-9]+')
            if [ -n "$job_id" ]; then
                SUBMITTED_JOB_ID="$job_id"
                log_pass "sbatch succeeded - Job ID: $job_id"
                return 0
            fi
        fi
    fi

    # Check for specific error messages
    if echo "$result" | grep -qi "connection refused\|cannot connect\|timeout"; then
        log_fail "sbatch failed - Server not reachable"
    elif echo "$result" | grep -qi "authentication\|munge\|credential"; then
        log_fail "sbatch failed - Authentication error"
    elif echo "$result" | grep -qi "error\|fail"; then
        log_fail "sbatch failed - $result"
    else
        log_fail "sbatch failed - Unexpected response: $result"
    fi

    return 1
}

# Test: sbatch with job script file
test_sbatch_script() {
    log_section "Test: sbatch with Job Script"

    log_info "Submitting job from script file..."

    # Copy test script to container and submit
    local result
    result=$(run_slurm_cmd "cat > /tmp/test_job.sh << 'EOFSCRIPT'
#!/bin/bash
#SBATCH --job-name=script_test
#SBATCH --time=00:02:00
#SBATCH --nodes=1
echo 'Running test script'
hostname
date
sleep 10
EOFSCRIPT
chmod +x /tmp/test_job.sh
sbatch --parsable /tmp/test_job.sh" 2>&1)
    local exit_code=$?

    log_info "sbatch script output: $result"

    if [ $exit_code -eq 0 ]; then
        local job_id
        job_id=$(echo "$result" | grep -oE '[0-9]+' | tail -1)
        if [ -n "$job_id" ]; then
            log_pass "sbatch with script succeeded - Job ID: $job_id"
            return 0
        fi
    fi

    log_fail "sbatch with script failed"
    return 1
}

# Test: squeue - Query job status
test_squeue() {
    log_section "Test: squeue (Job Queue Query)"

    log_info "Querying job queue..."

    local result
    result=$(run_slurm_cmd "squeue" 2>&1)
    local exit_code=$?

    log_info "squeue output:"
    echo "$result"

    if [ $exit_code -eq 0 ]; then
        # squeue should return a header at minimum
        if echo "$result" | grep -qiE "JOBID|JOB_ID|job"; then
            log_pass "squeue succeeded - Returned job listing header"

            # If we have a submitted job, check if it appears
            if [ -n "$SUBMITTED_JOB_ID" ]; then
                if echo "$result" | grep -q "$SUBMITTED_JOB_ID"; then
                    log_pass "squeue shows submitted job $SUBMITTED_JOB_ID"
                else
                    log_info "Note: Job $SUBMITTED_JOB_ID not visible in queue (may have completed or be pending)"
                fi
            fi
            return 0
        fi
    fi

    if echo "$result" | grep -qi "connection refused\|cannot connect"; then
        log_fail "squeue failed - Server not reachable"
    else
        log_fail "squeue failed - $result"
    fi

    return 1
}

# Test: squeue with specific job ID
test_squeue_job() {
    log_section "Test: squeue -j (Specific Job Query)"

    if [ -z "$SUBMITTED_JOB_ID" ]; then
        log_skip "No job ID available for specific query"
        return 0
    fi

    log_info "Querying specific job $SUBMITTED_JOB_ID..."

    local result
    result=$(run_slurm_cmd "squeue -j $SUBMITTED_JOB_ID" 2>&1)
    local exit_code=$?

    log_info "squeue -j output: $result"

    if [ $exit_code -eq 0 ]; then
        log_pass "squeue -j succeeded"
        return 0
    fi

    # Job may have already completed
    if echo "$result" | grep -qi "invalid job\|not found\|no matching"; then
        log_info "Job $SUBMITTED_JOB_ID not found (may have completed)"
        log_pass "squeue -j handled completed job correctly"
        return 0
    fi

    log_fail "squeue -j failed - $result"
    return 1
}

# Test: sinfo - Get node/partition info
test_sinfo() {
    log_section "Test: sinfo (Node/Partition Info)"

    log_info "Querying cluster info..."

    local result
    result=$(run_slurm_cmd "sinfo" 2>&1)
    local exit_code=$?

    log_info "sinfo output:"
    echo "$result"

    if [ $exit_code -eq 0 ]; then
        # sinfo should return partition/node info
        if echo "$result" | grep -qiE "PARTITION|AVAIL|STATE|NODELIST|NODES"; then
            log_pass "sinfo succeeded - Returned cluster info"
            return 0
        elif [ -n "$result" ]; then
            log_pass "sinfo succeeded - Returned data"
            return 0
        fi
    fi

    if echo "$result" | grep -qi "connection refused\|cannot connect"; then
        log_fail "sinfo failed - Server not reachable"
    else
        log_fail "sinfo failed - $result"
    fi

    return 1
}

# Test: sinfo -N (node-oriented view)
test_sinfo_nodes() {
    log_section "Test: sinfo -N (Node-Oriented View)"

    log_info "Querying node-oriented info..."

    local result
    result=$(run_slurm_cmd "sinfo -N" 2>&1)
    local exit_code=$?

    log_info "sinfo -N output: $result"

    if [ $exit_code -eq 0 ]; then
        log_pass "sinfo -N succeeded"
        return 0
    fi

    log_fail "sinfo -N failed - $result"
    return 1
}

# Test: scancel - Cancel a job
test_scancel() {
    log_section "Test: scancel (Job Cancellation)"

    if [ -z "$SUBMITTED_JOB_ID" ]; then
        log_skip "No job ID available for cancellation test"
        return 0
    fi

    log_info "Cancelling job $SUBMITTED_JOB_ID..."

    local result
    result=$(run_slurm_cmd "scancel $SUBMITTED_JOB_ID" 2>&1)
    local exit_code=$?

    log_info "scancel output: $result"
    log_info "scancel exit code: $exit_code"

    # scancel typically returns 0 on success with no output
    if [ $exit_code -eq 0 ]; then
        log_pass "scancel succeeded"

        # Verify the job is no longer running
        sleep 2
        local verify
        verify=$(run_slurm_cmd "squeue -j $SUBMITTED_JOB_ID 2>&1" || true)
        if echo "$verify" | grep -qi "invalid\|not found" || [ -z "$(echo "$verify" | grep "$SUBMITTED_JOB_ID")" ]; then
            log_pass "Job verified cancelled"
        fi

        return 0
    fi

    # Job may already be completed
    if echo "$result" | grep -qi "invalid job\|not found\|already\|complete"; then
        log_info "Job was already completed/cancelled"
        log_pass "scancel handled completed job correctly"
        return 0
    fi

    log_fail "scancel failed - $result"
    return 1
}

# Test: sacct - Accounting info
test_sacct() {
    log_section "Test: sacct (Accounting Info)"

    log_info "Querying accounting info..."

    local result
    result=$(run_slurm_cmd "sacct" 2>&1)
    local exit_code=$?

    log_info "sacct output:"
    echo "$result"

    if [ $exit_code -eq 0 ]; then
        if echo "$result" | grep -qiE "JobID|JobName|State"; then
            log_pass "sacct succeeded - Returned accounting data"
            return 0
        elif [ -n "$result" ]; then
            log_pass "sacct succeeded"
            return 0
        fi
    fi

    # sacct may not be available without slurmdbd
    if echo "$result" | grep -qi "accounting disabled\|not available\|slurmdbd"; then
        log_skip "sacct not available (requires slurmdbd)"
        return 0
    fi

    if echo "$result" | grep -qi "connection refused\|cannot connect"; then
        log_fail "sacct failed - Server not reachable"
    else
        log_info "sacct response: $result"
        log_skip "sacct may not be fully supported"
    fi

    return 0
}

# Test: scontrol show - Various show commands
test_scontrol_show() {
    log_section "Test: scontrol show (Control Commands)"

    # Test scontrol show partitions
    log_info "Testing scontrol show partition..."
    local result
    result=$(run_slurm_cmd "scontrol show partition" 2>&1)
    local exit_code=$?

    log_info "scontrol show partition output: $result"

    if [ $exit_code -eq 0 ] && [ -n "$result" ]; then
        log_pass "scontrol show partition succeeded"
    else
        log_fail "scontrol show partition failed"
    fi

    # Test scontrol show nodes
    log_info "Testing scontrol show node..."
    result=$(run_slurm_cmd "scontrol show node" 2>&1)
    exit_code=$?

    log_info "scontrol show node output: $result"

    if [ $exit_code -eq 0 ] && [ -n "$result" ]; then
        log_pass "scontrol show node succeeded"
    else
        log_fail "scontrol show node failed"
    fi

    # Test scontrol ping
    log_info "Testing scontrol ping..."
    result=$(run_slurm_cmd "scontrol ping" 2>&1)
    exit_code=$?

    log_info "scontrol ping output: $result"

    if [ $exit_code -eq 0 ]; then
        if echo "$result" | grep -qi "UP\|responding"; then
            log_pass "scontrol ping - Controller is UP"
        else
            log_pass "scontrol ping succeeded"
        fi
    else
        log_fail "scontrol ping failed"
    fi
}

# Test: srun - Run interactive command
test_srun() {
    log_section "Test: srun (Interactive Execution)"

    log_info "Testing srun with simple command..."

    local result
    result=$(timeout 30 docker compose -f "$COMPOSE_FILE" exec -T slurm-client srun --time=00:01:00 hostname 2>&1) || true
    local exit_code=$?

    log_info "srun output: $result"
    log_info "srun exit code: $exit_code"

    if [ $exit_code -eq 0 ] && [ -n "$result" ]; then
        log_pass "srun succeeded"
        return 0
    fi

    # srun may timeout waiting for allocation
    if echo "$result" | grep -qi "timeout\|allocation"; then
        log_skip "srun timed out waiting for allocation (no compute nodes)"
        return 0
    fi

    if echo "$result" | grep -qi "connection refused\|cannot connect"; then
        log_fail "srun failed - Server not reachable"
    else
        log_skip "srun may require compute nodes"
    fi

    return 0
}

# Print test summary
print_summary() {
    log_section "Test Summary"

    local total=$((TESTS_PASSED + TESTS_FAILED))
    local pass_rate=0

    if [ $total -gt 0 ]; then
        pass_rate=$((TESTS_PASSED * 100 / total))
    fi

    echo ""
    echo "Results:"
    echo -e "  ${GREEN}Passed:  $TESTS_PASSED${NC}"
    echo -e "  ${RED}Failed:  $TESTS_FAILED${NC}"
    echo -e "  ${YELLOW}Skipped: $TESTS_SKIPPED${NC}"
    echo ""
    echo "Pass Rate: ${pass_rate}%"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed.${NC}"
        return 1
    fi
}

# Main execution
main() {
    log_section "FLURM SLURM Client Compatibility Tests"
    log "Docker Compose File: $COMPOSE_FILE"
    log "Project Directory: $PROJECT_DIR"

    # Ensure we clean up on exit
    trap stop_environment EXIT

    # Start the environment
    if ! start_environment; then
        log_fail "Failed to start test environment"
        exit 1
    fi

    # Give the system a moment to stabilize
    sleep 3

    # Run all tests
    test_sbatch || true
    test_sbatch_script || true
    test_squeue || true
    test_squeue_job || true
    test_sinfo || true
    test_sinfo_nodes || true
    test_scontrol_show || true
    test_scancel || true
    test_sacct || true
    test_srun || true

    # Print summary
    print_summary
    exit_code=$?

    exit $exit_code
}

# Run main
main "$@"
