#!/bin/bash
#
# FLURM HA Failover Integration Test Runner
# Phase 8E: High-Availability Failover Testing
#
# This script orchestrates HA failover testing using Docker containers.
# It starts a 3-node FLURM controller cluster and runs integration tests.
#
# Usage:
#   ./run-ha-failover-tests.sh [OPTIONS]
#
# Options:
#   --rebuild       Rebuild Docker images before testing
#   --skip-cleanup  Don't stop containers after tests
#   --verbose       Enable verbose output
#   --quick         Run quick smoke tests only
#   --chaos         Enable chaos testing scenarios
#   --props         Run property-based tests
#   --all           Run all test suites
#   --help          Show this help message
#
# Examples:
#   ./run-ha-failover-tests.sh                    # Run standard HA tests
#   ./run-ha-failover-tests.sh --rebuild --all    # Rebuild and run all tests
#   ./run-ha-failover-tests.sh --chaos            # Run with chaos injection
#

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_DIR/docker"

# Default options
REBUILD=false
SKIP_CLEANUP=false
VERBOSE=false
QUICK=false
CHAOS=false
PROPS=false
RUN_ALL=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rebuild)
            REBUILD=true
            shift
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --chaos)
            CHAOS=true
            shift
            ;;
        --props)
            PROPS=true
            shift
            ;;
        --all)
            RUN_ALL=true
            CHAOS=true
            PROPS=true
            shift
            ;;
        --help|-h)
            head -40 "$0" | tail -30
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
}

# Cleanup function
cleanup() {
    if [ "$SKIP_CLEANUP" = false ]; then
        log_info "Cleaning up Docker containers..."
        cd "$DOCKER_DIR"
        docker-compose -f docker-compose.ha-failover.yml down --volumes --remove-orphans 2>/dev/null || true
    else
        log_warning "Skipping cleanup (--skip-cleanup specified)"
    fi
}

# Error handler
handle_error() {
    log_error "Test failed! Exit code: $?"
    cleanup
    exit 1
}

trap handle_error ERR
trap cleanup EXIT

# Main script
log_header "FLURM HA Failover Integration Tests"
echo "Project directory: $PROJECT_DIR"
echo "Docker directory: $DOCKER_DIR"
echo ""
echo "Options:"
echo "  Rebuild: $REBUILD"
echo "  Skip cleanup: $SKIP_CLEANUP"
echo "  Verbose: $VERBOSE"
echo "  Quick mode: $QUICK"
echo "  Chaos testing: $CHAOS"
echo "  Property tests: $PROPS"
echo ""

# Change to docker directory
cd "$DOCKER_DIR"

# Step 1: Build Docker images
log_header "Step 1: Building Docker Images"

if [ "$REBUILD" = true ]; then
    log_info "Rebuilding Docker images (--rebuild specified)..."
    docker-compose -f docker-compose.ha-failover.yml build --no-cache
else
    log_info "Building Docker images (use --rebuild to force rebuild)..."
    docker-compose -f docker-compose.ha-failover.yml build
fi
log_success "Docker images built"

# Step 2: Start the cluster
log_header "Step 2: Starting HA Cluster"

log_info "Stopping any existing containers..."
docker-compose -f docker-compose.ha-failover.yml down --volumes --remove-orphans 2>/dev/null || true

log_info "Starting 3-node controller cluster..."
docker-compose -f docker-compose.ha-failover.yml up -d \
    flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3 \
    flurm-node-1 flurm-node-2 \
    slurm-client

# Wait for containers to be healthy
log_info "Waiting for cluster to become healthy..."
MAX_RETRIES=60
RETRY=0

wait_for_healthy() {
    local container=$1
    local max_retries=${2:-$MAX_RETRIES}
    local retry=0

    while [ $retry -lt $max_retries ]; do
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "starting")
        if [ "$health" = "healthy" ]; then
            return 0
        fi
        if [ "$VERBOSE" = true ]; then
            echo "  Waiting for $container (status: $health)..."
        fi
        sleep 2
        ((retry++))
    done
    return 1
}

# Wait for each controller
for i in 1 2 3; do
    container="${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-$i"
    log_info "Waiting for controller $i ($container)..."
    if wait_for_healthy "$container" 60; then
        log_success "Controller $i is healthy"
    else
        log_error "Controller $i failed to become healthy"
        docker logs "$container" | tail -50
        exit 1
    fi
done

log_success "HA cluster is running"

# Step 3: Verify cluster formation
log_header "Step 3: Verifying Cluster Formation"

log_info "Checking cluster status..."

# Check if leader is elected
LEADER_CHECK=$(docker exec "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-1" \
    curl -s http://localhost:9090/health 2>/dev/null || echo "{}")

if echo "$LEADER_CHECK" | grep -q "healthy"; then
    log_success "Cluster health check passed"
else
    log_warning "Health check returned: $LEADER_CHECK"
fi

# Check node connectivity
log_info "Checking node connectivity..."
for i in 1 2 3; do
    container="${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-$i"
    if docker exec "$container" ping -c 1 flurm-ctrl-$((i % 3 + 1)) > /dev/null 2>&1; then
        log_success "Controller $i can reach other controllers"
    else
        log_warning "Controller $i network connectivity issue"
    fi
done

# Step 4: Run integration tests
log_header "Step 4: Running Integration Tests"

# Start test orchestrator
log_info "Starting test orchestrator..."
docker-compose -f docker-compose.ha-failover.yml up -d test-orchestrator

# Wait for test orchestrator to be ready
sleep 5

# Run Common Test suite
log_info "Running HA Failover SUITE..."

if [ "$QUICK" = true ]; then
    TEST_GROUPS="cluster_formation,leader_election"
    log_info "Quick mode: Running only $TEST_GROUPS"
else
    TEST_GROUPS="all"
fi

# Copy test files to orchestrator
docker cp "$PROJECT_DIR/apps" \
    "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-test-orchestrator:/flurm/"

# Run the tests
TEST_CMD="cd /flurm && rebar3 ct --suite=flurm_ha_failover_SUITE"
if [ "$VERBOSE" = true ]; then
    TEST_CMD="$TEST_CMD --verbose"
fi

log_info "Executing: $TEST_CMD"
if docker exec "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-test-orchestrator" \
    bash -c "$TEST_CMD" 2>&1; then
    log_success "Integration tests passed"
    INTEGRATION_RESULT=0
else
    log_warning "Integration tests had failures (this may be expected in mock mode)"
    INTEGRATION_RESULT=1
fi

# Step 5: Run chaos tests (if enabled)
if [ "$CHAOS" = true ]; then
    log_header "Step 5: Running Chaos Tests"

    log_info "Enabling chaos scenarios..."

    # Enable chaos on all controllers
    for i in 1 2 3; do
        container="${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-$i"
        docker exec "$container" bash -c "
            cd /flurm && \
            erl -pa _build/default/lib/*/ebin -noshell -eval '
                application:ensure_all_started(flurm_core),
                flurm_chaos:start_link(),
                flurm_chaos:enable(),
                flurm_chaos:enable_scenario(trigger_gc),
                flurm_chaos:enable_scenario(memory_pressure),
                io:format(\"Chaos enabled on node~n\"),
                halt(0).
            '" 2>/dev/null || log_warning "Could not enable chaos on controller $i"
    done

    # Run chaos tests
    CHAOS_CMD="cd /flurm && rebar3 eunit --module=flurm_ha_chaos_tests"
    log_info "Executing chaos tests..."
    if docker exec "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-test-orchestrator" \
        bash -c "$CHAOS_CMD" 2>&1; then
        log_success "Chaos tests passed"
        CHAOS_RESULT=0
    else
        log_warning "Chaos tests had failures"
        CHAOS_RESULT=1
    fi
else
    CHAOS_RESULT=0
fi

# Step 6: Run property-based tests (if enabled)
if [ "$PROPS" = true ]; then
    log_header "Step 6: Running Property-Based Tests"

    PROPS_CMD="cd /flurm && rebar3 proper --module=flurm_ha_prop_tests --numtests=50"
    log_info "Executing property tests..."
    if docker exec "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-test-orchestrator" \
        bash -c "$PROPS_CMD" 2>&1; then
        log_success "Property tests passed"
        PROPS_RESULT=0
    else
        log_warning "Property tests had failures"
        PROPS_RESULT=1
    fi
else
    PROPS_RESULT=0
fi

# Step 7: Run failover simulation
log_header "Step 7: Running Failover Simulation"

log_info "Testing leader failover..."

# Find current leader
LEADER=$(docker exec "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-1" \
    curl -s http://localhost:9090/health 2>/dev/null | grep -o '"leader":[^,}]*' || echo "unknown")
log_info "Current leader status: $LEADER"

# Simulate leader failure by stopping controller 1
log_info "Stopping controller 1 to simulate leader failure..."
docker stop "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-1"

# Wait for failover
log_info "Waiting for failover (10 seconds)..."
sleep 10

# Check if new leader elected
for i in 2 3; do
    container="${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-$i"
    NEW_LEADER=$(docker exec "$container" \
        curl -s http://localhost:9090/health 2>/dev/null | grep -o '"leader":[^,}]*' || echo "unknown")
    log_info "Controller $i leader status: $NEW_LEADER"
done

# Restart controller 1
log_info "Restarting controller 1..."
docker start "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-1"

# Wait for rejoin
log_info "Waiting for controller 1 to rejoin (15 seconds)..."
sleep 15

# Verify cluster is healthy again
if wait_for_healthy "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-1" 30; then
    log_success "Controller 1 rejoined cluster successfully"
    FAILOVER_RESULT=0
else
    log_warning "Controller 1 did not rejoin within timeout"
    FAILOVER_RESULT=1
fi

# Step 8: Generate report
log_header "Test Results Summary"

TOTAL_FAILURES=$((INTEGRATION_RESULT + CHAOS_RESULT + PROPS_RESULT + FAILOVER_RESULT))

echo "Integration Tests: $([ $INTEGRATION_RESULT -eq 0 ] && echo 'PASSED' || echo 'FAILED')"
if [ "$CHAOS" = true ]; then
    echo "Chaos Tests:       $([ $CHAOS_RESULT -eq 0 ] && echo 'PASSED' || echo 'FAILED')"
fi
if [ "$PROPS" = true ]; then
    echo "Property Tests:    $([ $PROPS_RESULT -eq 0 ] && echo 'PASSED' || echo 'FAILED')"
fi
echo "Failover Test:     $([ $FAILOVER_RESULT -eq 0 ] && echo 'PASSED' || echo 'FAILED')"
echo ""

if [ $TOTAL_FAILURES -eq 0 ]; then
    log_success "All tests passed!"
    EXIT_CODE=0
else
    log_warning "$TOTAL_FAILURES test suite(s) had failures"
    EXIT_CODE=1
fi

# Copy test results
log_info "Copying test results..."
mkdir -p "$PROJECT_DIR/test-results"
docker cp "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-test-orchestrator:/test-results/." \
    "$PROJECT_DIR/test-results/" 2>/dev/null || true

# Show container logs on failure (if verbose)
if [ $EXIT_CODE -ne 0 ] && [ "$VERBOSE" = true ]; then
    log_header "Container Logs (last 50 lines each)"
    for i in 1 2 3; do
        echo "--- Controller $i ---"
        docker logs "${COMPOSE_PROJECT_NAME:-flurm-ha-failover}-ctrl-$i" 2>&1 | tail -50
    done
fi

log_header "Test Run Complete"
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
