#!/bin/bash
#
# FLURM End-to-End Migration Test Orchestrator
#
# This script orchestrates the complete end-to-end migration testing
# from SLURM to FLURM, including:
# - Environment setup
# - Docker compose orchestration
# - Test execution
# - Result collection
# - Cleanup
#
# Usage:
#   ./run-migration-e2e-tests.sh [options]
#
# Options:
#   --rebuild        Rebuild Docker images before testing
#   --keep-running   Don't stop containers after tests
#   --verbose        Enable verbose output
#   --quick          Run quick smoke tests only
#   --stage STAGE    Run tests for specific stage only (shadow|active|primary|standalone)
#   --help           Show this help message
#
# Environment Variables:
#   FLURM_E2E_TIMEOUT     Test timeout in seconds (default: 600)
#   FLURM_E2E_RESULTS_DIR Directory for test results (default: ./test-results)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_DIR/docker"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.migration-e2e.yml"

# Default options
REBUILD=false
KEEP_RUNNING=false
VERBOSE=false
QUICK_MODE=false
SPECIFIC_STAGE=""
TIMEOUT="${FLURM_E2E_TIMEOUT:-600}"
RESULTS_DIR="${FLURM_E2E_RESULTS_DIR:-$DOCKER_DIR/test-results}"

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

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

# Usage information
usage() {
    head -35 "$0" | tail -25
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rebuild)
            REBUILD=true
            shift
            ;;
        --keep-running)
            KEEP_RUNNING=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --stage)
            SPECIFIC_STAGE="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate options
if [[ -n "$SPECIFIC_STAGE" ]]; then
    case $SPECIFIC_STAGE in
        shadow|active|primary|standalone|rollback|continuity|accounting)
            ;;
        *)
            log_error "Invalid stage: $SPECIFIC_STAGE"
            log_error "Valid stages: shadow, active, primary, standalone, rollback, continuity, accounting"
            exit 1
            ;;
    esac
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Start timestamp
START_TIME=$(date +%s)
RESULTS_FILE="$RESULTS_DIR/e2e-results-$(date +%Y%m%d-%H%M%S).log"

log_header "FLURM End-to-End Migration Tests"

log_info "Configuration:"
log_info "  Project directory: $PROJECT_DIR"
log_info "  Docker directory:  $DOCKER_DIR"
log_info "  Results directory: $RESULTS_DIR"
log_info "  Timeout:           $TIMEOUT seconds"
log_info "  Rebuild:           $REBUILD"
log_info "  Keep running:      $KEEP_RUNNING"
log_info "  Quick mode:        $QUICK_MODE"
if [[ -n "$SPECIFIC_STAGE" ]]; then
    log_info "  Specific stage:    $SPECIFIC_STAGE"
fi

# Cleanup function
cleanup() {
    log_header "Cleanup"

    if [[ "$KEEP_RUNNING" == "true" ]]; then
        log_info "Keeping containers running (--keep-running)"
        log_info "To stop: docker-compose -f $COMPOSE_FILE down -v"
    else
        log_info "Stopping containers..."
        docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    fi

    # Calculate duration
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log_info "Total duration: ${DURATION}s"
}

# Set up cleanup trap
trap cleanup EXIT

# Check prerequisites
log_header "Checking Prerequisites"

# Check Docker
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi
log_success "Docker found"

# Check docker-compose
if ! command -v docker-compose &> /dev/null; then
    log_error "docker-compose is not installed"
    exit 1
fi
log_success "docker-compose found"

# Check compose file exists
if [[ ! -f "$COMPOSE_FILE" ]]; then
    log_error "Compose file not found: $COMPOSE_FILE"
    exit 1
fi
log_success "Compose file found"

# Stop any existing containers
log_header "Environment Setup"

log_info "Stopping any existing containers..."
docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

# Optionally rebuild images
if [[ "$REBUILD" == "true" ]]; then
    log_info "Building Docker images (this may take a while)..."
    docker-compose -f "$COMPOSE_FILE" build --no-cache
    log_success "Images built"
fi

# Start the environment
log_header "Starting Test Environment"

log_info "Starting SLURM and FLURM clusters..."
docker-compose -f "$COMPOSE_FILE" up -d

# Wait for services to be healthy
log_info "Waiting for services to be healthy..."

wait_for_service() {
    local service=$1
    local max_attempts=60
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        status=$(docker-compose -f "$COMPOSE_FILE" ps --format json "$service" 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4 || echo "unknown")

        if [[ "$status" == "healthy" ]]; then
            log_success "$service is healthy"
            return 0
        fi

        if [[ "$VERBOSE" == "true" ]]; then
            log_info "  $service: $status (attempt $attempt/$max_attempts)"
        fi

        sleep 5
        ((attempt++))
    done

    log_error "$service failed to become healthy after $max_attempts attempts"
    return 1
}

# Wait for critical services
services=(
    "mysql"
    "slurm-dbd"
    "slurm-controller"
    "slurm-node-1"
    "slurm-node-2"
    "flurm-controller"
)

for service in "${services[@]}"; do
    wait_for_service "$service" || exit 1
done

log_success "All services are healthy"

# Run tests
log_header "Running Migration Tests"

# Determine which tests to run
if [[ "$QUICK_MODE" == "true" ]]; then
    TEST_GROUPS="shadow_mode,active_mode"
    log_info "Running quick smoke tests (shadow_mode, active_mode)"
elif [[ -n "$SPECIFIC_STAGE" ]]; then
    TEST_GROUPS="${SPECIFIC_STAGE}_mode"
    if [[ "$SPECIFIC_STAGE" == "rollback" || "$SPECIFIC_STAGE" == "continuity" || "$SPECIFIC_STAGE" == "accounting" ]]; then
        TEST_GROUPS="${SPECIFIC_STAGE}"
        if [[ "$SPECIFIC_STAGE" == "accounting" ]]; then
            TEST_GROUPS="accounting_sync"
        fi
        if [[ "$SPECIFIC_STAGE" == "continuity" ]]; then
            TEST_GROUPS="job_continuity"
        fi
    fi
    log_info "Running specific stage tests: $TEST_GROUPS"
else
    TEST_GROUPS=""
    log_info "Running all migration tests"
fi

# Execute tests inside the orchestrator container
log_info "Executing tests..."

TEST_CMD="cd /erlang-tests && "

if [[ -n "$TEST_GROUPS" ]]; then
    # Run specific groups
    TEST_CMD+="ct_run -dir . -suite flurm_migration_e2e_SUITE -group $TEST_GROUPS"
else
    # Run all tests
    TEST_CMD+="ct_run -dir . -suite flurm_migration_e2e_SUITE"
fi

TEST_CMD+=" -logdir /test-results -cover -cover_spec cover.spec 2>&1"

# Run the tests
docker-compose -f "$COMPOSE_FILE" exec -T test-orchestrator bash -c "$TEST_CMD" | tee "$RESULTS_FILE"
TEST_EXIT_CODE=${PIPESTATUS[0]}

# Collect additional logs
log_header "Collecting Logs"

log_info "Collecting container logs..."
docker-compose -f "$COMPOSE_FILE" logs --no-color > "$RESULTS_DIR/container-logs.txt" 2>&1

# Get FLURM controller logs
docker-compose -f "$COMPOSE_FILE" logs --no-color flurm-controller > "$RESULTS_DIR/flurm-controller.log" 2>&1

# Get SLURM controller logs
docker-compose -f "$COMPOSE_FILE" logs --no-color slurm-controller > "$RESULTS_DIR/slurm-controller.log" 2>&1

log_success "Logs collected in $RESULTS_DIR"

# Test result summary
log_header "Test Results Summary"

if [[ -f "$RESULTS_FILE" ]]; then
    # Extract test results
    PASSED=$(grep -c "OK" "$RESULTS_FILE" 2>/dev/null || echo "0")
    FAILED=$(grep -c "FAILED" "$RESULTS_FILE" 2>/dev/null || echo "0")
    SKIPPED=$(grep -c "SKIPPED" "$RESULTS_FILE" 2>/dev/null || echo "0")

    log_info "Test Results:"
    log_info "  Passed:  $PASSED"
    log_info "  Failed:  $FAILED"
    log_info "  Skipped: $SKIPPED"
fi

# Final status
if [[ $TEST_EXIT_CODE -eq 0 ]]; then
    log_success "All tests passed!"
    exit 0
else
    log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
    log_info "Check $RESULTS_FILE for details"
    exit $TEST_EXIT_CODE
fi
