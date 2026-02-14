#!/bin/bash
#
# FLURM Master Integration Test Runner
#
# Runs all integration test suites in sequence. Each suite runs in its own
# Docker environment and is automatically cleaned up.
#
# Usage:
#   ./scripts/run-all-integration-tests.sh [OPTIONS]
#
# Options:
#   --suite=NAME   Run only specified suite (slurm-interop, ha-failover, migration-e2e)
#   --parallel     Run suites in parallel (requires more resources)
#   --keep         Keep containers running after tests (for debugging)
#   --rebuild      Force rebuild of Docker images
#   --help         Show this help message
#
# Exit Codes:
#   0  All tests passed
#   1  One or more tests failed
#   2  Prerequisites not met
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }
log_header() { echo -e "\n${CYAN}════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}════════════════════════════════════════${NC}\n"; }

# Options
RUN_PARALLEL=false
KEEP_CONTAINERS=false
REBUILD=false
SUITE=""
EXTRA_ARGS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --suite=*) SUITE="${1#*=}"; shift ;;
        --parallel) RUN_PARALLEL=true; shift ;;
        --keep) KEEP_CONTAINERS=true; EXTRA_ARGS="$EXTRA_ARGS --keep"; shift ;;
        --rebuild) REBUILD=true; EXTRA_ARGS="$EXTRA_ARGS --rebuild"; shift ;;
        --help|-h)
            head -25 "$0" | grep "^#" | sed 's/^# //' | sed 's/^#//'
            exit 0
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
        log_error "Docker is not installed"
        exit 2
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 2
    fi

    # Check available memory (want at least 4GB free for full suite)
    if command -v free &> /dev/null; then
        FREE_MEM_MB=$(free -m | awk '/^Mem:/{print $7}')
        if [[ "$FREE_MEM_MB" -lt 4096 ]]; then
            log_warn "Low available memory: ${FREE_MEM_MB}MB (recommend 4GB+)"
        fi
    fi

    log_success "Prerequisites OK"
}

# Test suites configuration (portable for bash 3/macOS).
TEST_SUITE_NAMES=("slurm-interop" "ha-failover" "migration-e2e")
TEST_SUITE_SCRIPTS=("run-slurm-interop-tests.sh" "run-ha-failover-tests.sh" "run-migration-e2e-tests.sh")

lookup_suite_script() {
    local suite_name="$1"
    local i
    for i in "${!TEST_SUITE_NAMES[@]}"; do
        if [[ "${TEST_SUITE_NAMES[$i]}" == "$suite_name" ]]; then
            echo "${TEST_SUITE_SCRIPTS[$i]}"
            return 0
        fi
    done
    return 1
}

# Run unit tests first
run_unit_tests() {
    log_header "Running Unit Tests"

    cd "$PROJECT_ROOT"
    if rebar3 eunit; then
        log_success "Unit tests passed"
        return 0
    else
        log_error "Unit tests failed"
        return 1
    fi
}

# Run a single integration test suite
run_suite() {
    local suite_name=$1
    local script
    script="$(lookup_suite_script "$suite_name" || true)"

    if [[ -z "$script" ]]; then
        log_error "Unknown suite: $suite_name"
        return 1
    fi

    local script_path="$SCRIPT_DIR/$script"
    if [[ ! -x "$script_path" ]]; then
        log_warn "Suite script not executable: $script_path"
        chmod +x "$script_path" 2>/dev/null || true
    fi

    log_header "Running $suite_name Tests"

    if [[ -x "$script_path" ]]; then
        if "$script_path" $EXTRA_ARGS; then
            log_success "$suite_name tests passed"
            return 0
        else
            log_error "$suite_name tests failed"
            return 1
        fi
    else
        log_error "Script not found: $script_path"
        return 1
    fi
}

# Run all suites
run_all_suites() {
    local failed_suites=()
    local passed_suites=()

    for suite_name in "${TEST_SUITE_NAMES[@]}"; do
        if run_suite "$suite_name"; then
            passed_suites+=("$suite_name")
        else
            failed_suites+=("$suite_name")
        fi
    done

    # Print summary
    log_header "Test Summary"

    if [[ ${#passed_suites[@]} -gt 0 ]]; then
        log_success "Passed suites: ${passed_suites[*]}"
    fi

    if [[ ${#failed_suites[@]} -gt 0 ]]; then
        log_error "Failed suites: ${failed_suites[*]}"
        return 1
    fi

    return 0
}

# Main
main() {
    log_header "FLURM Integration Test Runner"

    check_prerequisites

    # Always run unit tests first
    if ! run_unit_tests; then
        log_error "Unit tests failed - skipping integration tests"
        exit 1
    fi

    # Run specific suite or all
    if [[ -n "$SUITE" ]]; then
        run_suite "$SUITE"
        exit $?
    else
        run_all_suites
        exit $?
    fi
}

main
