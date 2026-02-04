#!/bin/bash
#
# FLURM SLURM Interoperability Test Runner
#
# This script manages the Docker-based test environment for running
# SLURM interop tests against FLURM.
#
# Usage:
#   ./scripts/run-slurm-interop-tests.sh [OPTIONS]
#
# Options:
#   --rebuild      Force rebuild of Docker images
#   --keep         Keep containers running after tests
#   --shell        Start interactive shell in test-runner container
#   --slurm-only   Only test against real SLURM (skip FLURM tests)
#   --flurm-only   Only test against FLURM (skip real SLURM tests)
#   --suite=NAME   Run specific test suite group
#   --help         Show this help message
#
# Environment Variables:
#   COMPOSE_PROJECT_NAME  Docker Compose project name (default: flurm-interop)
#   DOCKER_BUILDKIT       Enable BuildKit (default: 1)
#

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.slurm-interop.yml"

# Default settings
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-flurm-interop}"
export COMPOSE_PROJECT_NAME
export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}"

REBUILD=false
KEEP=false
SHELL_MODE=false
SUITE=""
EXTRA_CT_ARGS=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show help
show_help() {
    head -30 "$0" | grep "^#" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rebuild)
            REBUILD=true
            shift
            ;;
        --keep)
            KEEP=true
            shift
            ;;
        --shell)
            SHELL_MODE=true
            shift
            ;;
        --suite=*)
            SUITE="${1#*=}"
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            log_warn "Unknown option: $1"
            shift
            ;;
    esac
done

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi

    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not available"
        exit 1
    fi

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "Compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    log_success "Prerequisites OK"
}

# Cleanup function
cleanup() {
    if [[ "$KEEP" == "false" ]]; then
        log_info "Cleaning up containers..."
        cd "$PROJECT_ROOT/docker"
        docker compose -f docker-compose.slurm-interop.yml down -v --remove-orphans 2>/dev/null || true
    else
        log_info "Keeping containers running (use --keep flag was set)"
        log_info "To stop manually: docker compose -f docker/docker-compose.slurm-interop.yml down -v"
    fi
}

# Set up trap for cleanup
trap cleanup EXIT

# Build images
build_images() {
    log_info "Building Docker images..."
    cd "$PROJECT_ROOT/docker"

    BUILD_ARGS=""
    if [[ "$REBUILD" == "true" ]]; then
        BUILD_ARGS="--no-cache"
        log_info "Forcing rebuild (--no-cache)"
    fi

    docker compose -f docker-compose.slurm-interop.yml build $BUILD_ARGS

    log_success "Images built successfully"
}

# Start services
start_services() {
    log_info "Starting services..."
    cd "$PROJECT_ROOT/docker"

    # Start services (except test-runner)
    docker compose -f docker-compose.slurm-interop.yml up -d \
        munge-init \
        slurm-controller \
        slurm-node \
        flurm-controller \
        slurm-client-flurm \
        slurm-client-real

    log_info "Waiting for services to be healthy..."

    # Wait for FLURM controller
    log_info "Waiting for FLURM controller..."
    for i in $(seq 1 60); do
        if docker compose -f docker-compose.slurm-interop.yml exec -T flurm-controller curl -s -f http://localhost:6820/health &>/dev/null; then
            log_success "FLURM controller is healthy"
            break
        fi
        if [[ $i -eq 60 ]]; then
            log_error "FLURM controller did not become healthy in time"
            docker compose -f docker-compose.slurm-interop.yml logs flurm-controller
            exit 1
        fi
        sleep 2
    done

    # Wait for SLURM controller
    log_info "Waiting for SLURM controller..."
    for i in $(seq 1 60); do
        if docker compose -f docker-compose.slurm-interop.yml exec -T slurm-controller scontrol ping &>/dev/null; then
            log_success "SLURM controller is healthy"
            break
        fi
        if [[ $i -eq 60 ]]; then
            log_error "SLURM controller did not become healthy in time"
            docker compose -f docker-compose.slurm-interop.yml logs slurm-controller
            exit 1
        fi
        sleep 2
    done

    # Wait for SLURM node
    log_info "Waiting for SLURM compute node..."
    for i in $(seq 1 30); do
        if docker compose -f docker-compose.slurm-interop.yml exec -T slurm-controller scontrol show node slurm-node-001 2>/dev/null | grep -q "State="; then
            log_success "SLURM compute node registered"
            break
        fi
        if [[ $i -eq 30 ]]; then
            log_warn "SLURM compute node may not be fully registered"
        fi
        sleep 2
    done

    log_success "All services started"
}

# Run tests
run_tests() {
    log_info "Running SLURM interop tests..."
    cd "$PROJECT_ROOT/docker"

    # Build CT arguments
    CT_ARGS="--dir=apps/flurm_controller/integration_test --suite=flurm_slurm_interop_SUITE --readable=true"

    if [[ -n "$SUITE" ]]; then
        CT_ARGS="$CT_ARGS --group=$SUITE"
    fi

    CT_ARGS="$CT_ARGS $EXTRA_CT_ARGS"

    # Run test-runner container
    log_info "Executing: rebar3 ct $CT_ARGS"

    if docker compose -f docker-compose.slurm-interop.yml run --rm test-runner \
        rebar3 ct $CT_ARGS; then
        log_success "Tests completed successfully"
        TESTS_PASSED=true
    else
        log_error "Tests failed"
        TESTS_PASSED=false
    fi

    # Copy test results
    log_info "Copying test results..."
    mkdir -p "$PROJECT_ROOT/_build/test/logs/slurm-interop"

    # Copy logs from the container if it exists
    docker compose -f docker-compose.slurm-interop.yml cp \
        test-runner:/app/_build/test/logs/. \
        "$PROJECT_ROOT/_build/test/logs/slurm-interop/" 2>/dev/null || true

    if [[ "$TESTS_PASSED" == "false" ]]; then
        exit 1
    fi
}

# Interactive shell mode
run_shell() {
    log_info "Starting interactive shell in test-runner container..."
    cd "$PROJECT_ROOT/docker"

    docker compose -f docker-compose.slurm-interop.yml run --rm test-runner /bin/bash
}

# Show service status
show_status() {
    log_info "Service Status:"
    cd "$PROJECT_ROOT/docker"

    echo ""
    echo "FLURM Controller:"
    docker compose -f docker-compose.slurm-interop.yml exec -T flurm-controller curl -s http://localhost:6820/health 2>/dev/null || echo "  Not responding"

    echo ""
    echo "SLURM Controller:"
    docker compose -f docker-compose.slurm-interop.yml exec -T slurm-controller scontrol ping 2>/dev/null || echo "  Not responding"

    echo ""
    echo "SLURM Nodes:"
    docker compose -f docker-compose.slurm-interop.yml exec -T slurm-controller sinfo 2>/dev/null || echo "  Not available"

    echo ""
    echo "Container Status:"
    docker compose -f docker-compose.slurm-interop.yml ps
}

# Main execution
main() {
    log_info "=========================================="
    log_info "FLURM SLURM Interoperability Test Runner"
    log_info "=========================================="

    check_prerequisites
    build_images
    start_services

    # Show service status
    show_status

    if [[ "$SHELL_MODE" == "true" ]]; then
        KEEP=true  # Keep services when using shell mode
        run_shell
    else
        run_tests
    fi

    log_success "Test run completed"
}

# Run main
main
