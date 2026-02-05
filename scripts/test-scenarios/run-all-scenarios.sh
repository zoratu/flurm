#!/bin/bash
#
# FLURM Comprehensive Test Scenarios Runner
#
# Runs all test scenarios in sequence:
# 1. standalone - Basic FLURM operation
# 2. ha - High-availability failover
# 3. jobs - All job types
# 4. migration - SLURM to FLURM migration (if SLURM available)
#
# Usage:
#   ./scripts/test-scenarios/run-all-scenarios.sh           # Run all
#   ./scripts/test-scenarios/run-all-scenarios.sh standalone  # Run specific
#
# Prerequisites:
#   - Docker Compose environment running
#   - SLURM tools installed in test client

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKER_DIR="$PROJECT_DIR/docker"
RESULTS_DIR="${RESULTS_DIR:-$PROJECT_DIR/test-results/$(date +%Y%m%d_%H%M%S)}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    TOTAL_PASSED=$((TOTAL_PASSED + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    TOTAL_FAILED=$((TOTAL_FAILED + 1))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
}

usage() {
    echo "Usage: $0 [scenario...]"
    echo ""
    echo "Scenarios:"
    echo "  standalone      Basic FLURM operation tests"
    echo "  ha              High-availability failover tests"
    echo "  jobs            All job types (batch, array, dependency, failure)"
    echo "  features        Feature verification (auth, metrics, accounting)"
    echo "  slurm-features  Advanced SLURM features (GPU, licenses, QOS, etc.)"
    echo "  migration       SLURM to FLURM migration tests"
    echo ""
    echo "Options:"
    echo "  --no-docker   Skip Docker Compose management"
    echo "  --keep        Keep containers running after tests"
    echo "  --help        Show this help"
    exit 0
}

# Parse arguments
SCENARIOS=()
MANAGE_DOCKER=true
KEEP_CONTAINERS=false

for arg in "$@"; do
    case $arg in
        --no-docker)
            MANAGE_DOCKER=false
            ;;
        --keep)
            KEEP_CONTAINERS=true
            ;;
        --help)
            usage
            ;;
        *)
            SCENARIOS+=("$arg")
            ;;
    esac
done

# Default to all scenarios if none specified
if [ ${#SCENARIOS[@]} -eq 0 ]; then
    SCENARIOS=(standalone ha jobs features slurm-features migration)
fi

main() {
    echo "=========================================="
    echo "FLURM Comprehensive Test Scenarios"
    echo "=========================================="
    echo "Project: $PROJECT_DIR"
    echo "Results: $RESULTS_DIR"
    echo "Scenarios: ${SCENARIOS[*]}"
    echo ""

    mkdir -p "$RESULTS_DIR"

    # Start Docker environment if needed
    if [ "$MANAGE_DOCKER" = true ]; then
        start_docker_environment
    fi

    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    wait_for_services

    # Run each scenario
    for scenario in "${SCENARIOS[@]}"; do
        echo ""
        echo "=========================================="
        echo "Running scenario: $scenario"
        echo "=========================================="

        local script="$SCRIPT_DIR/scenario-${scenario}.sh"

        if [ ! -f "$script" ]; then
            log_skip "Scenario $scenario: script not found ($script)"
            continue
        fi

        if bash "$script" > "$RESULTS_DIR/${scenario}.log" 2>&1; then
            log_pass "Scenario: $scenario"
        else
            log_fail "Scenario: $scenario (see $RESULTS_DIR/${scenario}.log)"
        fi
    done

    # Cleanup if requested
    if [ "$MANAGE_DOCKER" = true ] && [ "$KEEP_CONTAINERS" = false ]; then
        cleanup_docker_environment
    fi

    # Summary
    echo ""
    echo "=========================================="
    echo "Summary"
    echo "=========================================="
    echo -e "${GREEN}Passed:${NC}  $TOTAL_PASSED"
    echo -e "${RED}Failed:${NC}  $TOTAL_FAILED"
    echo -e "${YELLOW}Skipped:${NC} $TOTAL_SKIPPED"
    echo ""
    echo "Results saved to: $RESULTS_DIR"

    if [ "$TOTAL_FAILED" -gt 0 ]; then
        echo ""
        echo -e "${RED}Some scenarios failed!${NC}"
        return 1
    else
        echo ""
        echo -e "${GREEN}All scenarios passed!${NC}"
        return 0
    fi
}

start_docker_environment() {
    log_info "Starting Docker environment..."
    cd "$DOCKER_DIR"

    # Check if compose file exists
    if [ ! -f "docker-compose.comprehensive-test.yml" ]; then
        echo "ERROR: docker-compose.comprehensive-test.yml not found"
        exit 1
    fi

    # Start containers
    docker compose -f docker-compose.comprehensive-test.yml up -d --build 2>&1 || {
        echo "ERROR: Failed to start Docker environment"
        exit 1
    }

    log_info "Docker environment started"
}

wait_for_services() {
    local max_wait=120
    local waited=0

    log_info "Waiting for FLURM controllers (max ${max_wait}s)..."

    while [ $waited -lt $max_wait ]; do
        # Check if at least one controller is healthy
        if docker exec flurm-test-ctrl-1 curl -sf http://localhost:9090/health 2>/dev/null; then
            log_info "FLURM controller 1 is healthy"
            return 0
        fi

        sleep 5
        waited=$((waited + 5))
        echo "  Waiting... ($waited/$max_wait seconds)"
    done

    echo "WARNING: Timeout waiting for services (continuing anyway)"
}

cleanup_docker_environment() {
    log_info "Cleaning up Docker environment..."
    cd "$DOCKER_DIR"
    docker compose -f docker-compose.comprehensive-test.yml down -v 2>&1 || true
    log_info "Cleanup complete"
}

# Run main
main
exit $?
