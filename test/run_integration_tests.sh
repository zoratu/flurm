#!/bin/bash
# FLURM Integration Test Runner
#
# This script runs Common Test suites against a Docker-based FLURM cluster.
#
# Usage:
#   ./test/run_integration_tests.sh                    # Run all CT suites
#   ./test/run_integration_tests.sh --suite=flurm_ha_SUITE   # Run specific suite
#   ./test/run_integration_tests.sh --keep             # Keep cluster running after tests
#   ./test/run_integration_tests.sh --rebuild          # Force rebuild of images

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.integration-test.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
KEEP_RUNNING=false
REBUILD=false
SUITE_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --keep)
            KEEP_RUNNING=true
            shift
            ;;
        --rebuild)
            REBUILD=true
            shift
            ;;
        --suite=*)
            SUITE_ARGS="--suite=${1#*=}"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --keep          Keep Docker cluster running after tests"
            echo "  --rebuild       Force rebuild of Docker images"
            echo "  --suite=NAME    Run only the specified test suite"
            echo "  --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                              # Run all tests"
            echo "  $0 --suite=flurm_ha_SUITE      # Run HA tests only"
            echo "  $0 --keep --rebuild            # Rebuild and keep running"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    if [ "$KEEP_RUNNING" = false ]; then
        log_info "Cleaning up Docker containers..."
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    else
        log_info "Keeping Docker cluster running (use 'docker compose -f $COMPOSE_FILE down' to stop)"
    fi
}

# Trap exit to cleanup
trap cleanup EXIT

# Change to project root
cd "$PROJECT_ROOT"

log_info "Starting FLURM Integration Tests"
log_info "================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Build images if needed
if [ "$REBUILD" = true ]; then
    log_info "Rebuilding Docker images..."
    docker compose -f "$COMPOSE_FILE" build --no-cache
else
    log_info "Building Docker images (use --rebuild to force)..."
    docker compose -f "$COMPOSE_FILE" build
fi

# Stop any existing containers
log_info "Stopping any existing containers..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

# Start the cluster
log_info "Starting FLURM cluster..."
docker compose -f "$COMPOSE_FILE" up -d flurm-controller flurm-node-1 flurm-node-2 slurm-client

# Wait for health check
log_info "Waiting for cluster to be healthy..."
MAX_WAIT=60
WAIT_COUNT=0

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if docker compose -f "$COMPOSE_FILE" ps flurm-controller | grep -q "healthy"; then
        log_info "FLURM controller is healthy"
        break
    fi

    WAIT_COUNT=$((WAIT_COUNT + 1))
    if [ $WAIT_COUNT -eq $MAX_WAIT ]; then
        log_error "Timeout waiting for cluster to be healthy"
        docker compose -f "$COMPOSE_FILE" logs flurm-controller
        exit 1
    fi

    echo -n "."
    sleep 1
done
echo ""

# Give nodes time to register
log_info "Waiting for nodes to register..."
sleep 5

# Run the tests
log_info "Running Common Test suites..."
echo ""

# Build CT command
CT_CMD="rebar3 ct --readable=true"
if [ -n "$SUITE_ARGS" ]; then
    CT_CMD="$CT_CMD $SUITE_ARGS"
fi

# Run tests either in container or locally
if docker compose -f "$COMPOSE_FILE" config --services | grep -q "flurm-test"; then
    # Use the test container
    docker compose -f "$COMPOSE_FILE" run --rm flurm-test $CT_CMD
    EXIT_CODE=$?
else
    # Run locally with environment pointing to Docker cluster
    export FLURM_CONTROLLER_HOST=localhost
    export FLURM_CONTROLLER_PORT=6817
    $CT_CMD
    EXIT_CODE=$?
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    log_info "All tests passed!"
else
    log_error "Some tests failed (exit code: $EXIT_CODE)"
fi

# Show test logs location
log_info "Test logs available at: _build/test/logs/"

exit $EXIT_CODE
