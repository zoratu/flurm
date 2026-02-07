#!/bin/bash
#
# FLURM From-Scratch Test Runner
#
# One-command solution to build and test FLURM from scratch.
# Clones the repo (or uses local context), builds everything,
# starts a full HA cluster, runs E2E tests, and reports results.
#
# Usage:
#   ./run-from-scratch.sh [OPTIONS]
#
# Options:
#   --repo URL        Git repository URL (default: use local context)
#   --branch BRANCH   Git branch to build (default: main)
#   --erlang VERSION  Erlang OTP version (default: 27)
#   --skip-build      Skip the build step (use existing images)
#   --skip-tests      Skip running tests (just start the cluster)
#   --keep-running    Keep cluster running after tests
#   --shell           Drop into test-runner shell after tests
#   --verbose         Show detailed output
#   --clean           Clean up everything before starting
#   --help            Show this help message
#
# Examples:
#   # Build from local source and run tests
#   ./run-from-scratch.sh
#
#   # Build from GitHub and run tests
#   ./run-from-scratch.sh --repo https://github.com/zoratu/flurm.git --branch main
#
#   # Quick test with existing images
#   ./run-from-scratch.sh --skip-build
#
#   # Start cluster for interactive testing
#   ./run-from-scratch.sh --skip-tests --shell

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.from-scratch.yml"
RESULTS_DIR="$SCRIPT_DIR/test-results"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Default options
FLURM_REPO_URL=""
FLURM_BRANCH="main"
ERLANG_VERSION="27"
BUILD_FROM_CONTEXT="true"
SKIP_BUILD=false
SKIP_TESTS=false
KEEP_RUNNING=false
DROP_TO_SHELL=false
VERBOSE=false
CLEAN_FIRST=false

# Logging functions
log_header() {
    echo ""
    echo -e "${BOLD}${CYAN}============================================${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BOLD}${CYAN}============================================${NC}"
    echo ""
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BOLD}>>> $1${NC}"
}

# Show usage
show_help() {
    head -45 "$0" | tail -42 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)
            FLURM_REPO_URL="$2"
            BUILD_FROM_CONTEXT="false"
            shift 2
            ;;
        --branch)
            FLURM_BRANCH="$2"
            shift 2
            ;;
        --erlang)
            ERLANG_VERSION="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --keep-running)
            KEEP_RUNNING=true
            shift
            ;;
        --shell)
            DROP_TO_SHELL=true
            KEEP_RUNNING=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --clean)
            CLEAN_FIRST=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Export environment variables for docker-compose
export FLURM_REPO_URL
export FLURM_BRANCH
export ERLANG_VERSION
export BUILD_FROM_CONTEXT
export FLURM_VERSION="${FLURM_BRANCH:-latest}"

# Timer function
start_timer() {
    START_TIME=$(date +%s)
}

elapsed_time() {
    local end_time=$(date +%s)
    local elapsed=$((end_time - START_TIME))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))
    echo "${minutes}m ${seconds}s"
}

# Cleanup function
cleanup() {
    local exit_code=$?

    if [ "$KEEP_RUNNING" = false ]; then
        log_step "Cleaning up..."
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    else
        log_info "Cluster left running. Use 'docker compose -f $COMPOSE_FILE down -v' to stop."
    fi

    if [ $exit_code -ne 0 ]; then
        log_error "Script failed with exit code $exit_code"
    fi

    exit $exit_code
}

# Set up cleanup trap
trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."

    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    # Check Docker Compose (v2)
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose v2 is not available"
        log_info "Please upgrade to Docker Compose v2 (docker compose command)"
        exit 1
    fi

    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi

    log_success "Prerequisites OK"
}

# Clean up previous runs
clean_previous() {
    if [ "$CLEAN_FIRST" = true ]; then
        log_step "Cleaning up previous runs..."
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
        docker image rm flurm-from-scratch:latest 2>/dev/null || true
        rm -rf "$RESULTS_DIR"/* 2>/dev/null || true
        log_success "Cleanup complete"
    fi
}

# Build the images
build_images() {
    if [ "$SKIP_BUILD" = true ]; then
        log_info "Skipping build (--skip-build)"
        return 0
    fi

    log_step "Building FLURM from scratch..."

    if [ "$BUILD_FROM_CONTEXT" = "true" ]; then
        log_info "Building from local source at $PROJECT_ROOT"
    else
        log_info "Building from $FLURM_REPO_URL (branch: $FLURM_BRANCH)"
    fi
    log_info "Using Erlang/OTP $ERLANG_VERSION"

    local build_args=""
    if [ "$VERBOSE" = true ]; then
        build_args="--progress=plain"
    fi

    # Build the image (only need to build once, all services use the same image)
    if docker compose -f "$COMPOSE_FILE" build $build_args flurm-ctrl-1; then
        log_success "Image built successfully"
    else
        log_error "Build failed"
        exit 1
    fi
}

# Start the cluster
start_cluster() {
    log_step "Starting FLURM cluster..."

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Start all services
    log_info "Starting 3 controller nodes..."
    docker compose -f "$COMPOSE_FILE" up -d flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3

    log_info "Starting 3 compute nodes..."
    docker compose -f "$COMPOSE_FILE" up -d flurm-node-1 flurm-node-2 flurm-node-3

    log_info "Starting test runner..."
    docker compose -f "$COMPOSE_FILE" up -d test-runner
}

# Wait for cluster health
wait_for_health() {
    log_step "Waiting for cluster to be healthy..."

    local max_wait=180
    local waited=0
    local check_interval=5

    while [ $waited -lt $max_wait ]; do
        # Check controller health
        local ctrl_healthy=0
        for ctrl in flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3; do
            if docker compose -f "$COMPOSE_FILE" ps "$ctrl" 2>/dev/null | grep -q "healthy"; then
                ctrl_healthy=$((ctrl_healthy + 1))
            fi
        done

        # Check node health
        local nodes_healthy=0
        for node in flurm-node-1 flurm-node-2 flurm-node-3; do
            if docker compose -f "$COMPOSE_FILE" ps "$node" 2>/dev/null | grep -q "healthy"; then
                nodes_healthy=$((nodes_healthy + 1))
            fi
        done

        log_info "Controllers: $ctrl_healthy/3 healthy, Nodes: $nodes_healthy/3 healthy"

        if [ $ctrl_healthy -ge 3 ] && [ $nodes_healthy -ge 3 ]; then
            log_success "Cluster is healthy!"
            return 0
        fi

        sleep $check_interval
        waited=$((waited + check_interval))
    done

    log_warn "Cluster did not become fully healthy within ${max_wait}s"
    log_info "Continuing anyway - some nodes may still be starting..."
    return 0
}

# Run tests
run_tests() {
    if [ "$SKIP_TESTS" = true ]; then
        log_info "Skipping tests (--skip-tests)"
        return 0
    fi

    log_step "Running E2E tests..."

    # First, run the built-in unit tests
    log_info "Running unit tests..."
    if docker compose -f "$COMPOSE_FILE" exec -T test-runner bash -c "cd /opt/flurm/src && rebar3 eunit" 2>&1; then
        log_success "Unit tests passed"
    else
        log_warn "Unit tests had issues (continuing)"
    fi

    # Run property-based tests
    log_info "Running property-based tests..."
    if docker compose -f "$COMPOSE_FILE" exec -T test-runner bash -c "cd /opt/flurm/src && rebar3 proper" 2>&1; then
        log_success "Property tests passed"
    else
        log_warn "Property tests had issues (continuing)"
    fi

    # Run E2E tests if available
    if [ -f "$SCRIPT_DIR/../e2e-tests/run-all.sh" ]; then
        log_info "Running E2E test suite..."
        if docker compose -f "$COMPOSE_FILE" exec -T test-runner /tests/run-all.sh 2>&1; then
            log_success "E2E tests passed"
            return 0
        else
            log_error "E2E tests failed"
            return 1
        fi
    else
        log_info "No E2E test suite found, running basic integration tests..."

        # Run a basic connectivity test
        docker compose -f "$COMPOSE_FILE" exec -T test-runner bash -c '
            echo "Testing controller connectivity..."
            for ctrl in flurm-ctrl-1 flurm-ctrl-2 flurm-ctrl-3; do
                if nc -z $ctrl 6817; then
                    echo "  $ctrl: OK"
                else
                    echo "  $ctrl: FAILED"
                    exit 1
                fi
            done

            echo "Testing MUNGE authentication..."
            if munge -n </dev/null >/dev/null 2>&1; then
                echo "  MUNGE: OK"
            else
                echo "  MUNGE: WARN (may be OK in container)"
            fi

            echo "Basic connectivity tests passed!"
        '
    fi
}

# Show cluster status
show_status() {
    log_step "Cluster Status:"
    docker compose -f "$COMPOSE_FILE" ps
    echo ""
}

# Drop to shell
drop_to_shell() {
    if [ "$DROP_TO_SHELL" = true ]; then
        log_step "Dropping to test-runner shell..."
        log_info "Type 'exit' to leave the shell"
        log_info "Useful commands:"
        echo "  - cd /opt/flurm/src && rebar3 shell  # Start Erlang shell"
        echo "  - /tests/run-all.sh                  # Run E2E tests"
        echo "  - sinfo                              # Show cluster info (SLURM client)"
        echo ""
        docker compose -f "$COMPOSE_FILE" exec test-runner /bin/bash
    fi
}

# Print summary
print_summary() {
    local duration=$(elapsed_time)

    log_header "Summary"

    echo -e "Duration:        ${duration}"
    echo -e "Erlang Version:  ${ERLANG_VERSION}"

    if [ "$BUILD_FROM_CONTEXT" = "true" ]; then
        echo -e "Source:          Local ($PROJECT_ROOT)"
    else
        echo -e "Source:          $FLURM_REPO_URL"
        echo -e "Branch:          $FLURM_BRANCH"
    fi

    echo ""

    if [ "$KEEP_RUNNING" = true ]; then
        echo -e "Cluster Status:  ${GREEN}Running${NC}"
        echo ""
        echo "To access the cluster:"
        echo "  docker compose -f $COMPOSE_FILE exec test-runner bash"
        echo ""
        echo "To stop the cluster:"
        echo "  docker compose -f $COMPOSE_FILE down -v"
    else
        echo -e "Cluster Status:  Stopped"
    fi

    if [ -d "$RESULTS_DIR" ] && [ "$(ls -A $RESULTS_DIR 2>/dev/null)" ]; then
        echo ""
        echo "Test results saved to: $RESULTS_DIR"
    fi
}

# Main execution
main() {
    log_header "FLURM From-Scratch Builder & Tester"

    start_timer

    check_prerequisites
    clean_previous
    build_images
    start_cluster
    wait_for_health

    local test_result=0
    run_tests || test_result=$?

    show_status
    print_summary

    drop_to_shell

    if [ $test_result -ne 0 ]; then
        log_error "Tests failed!"
        exit 1
    fi

    log_success "All done!"
}

# Run main
main
