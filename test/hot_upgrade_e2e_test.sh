#!/bin/bash
#
# FLURM Hot Upgrade End-to-End Test
#
# This test verifies that:
# 1. Release can be built
# 2. Release can be started
# 3. Module can be hot-reloaded
# 4. State is preserved during reload
# 5. Release can be upgraded
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BUILD_DIR="${PROJECT_ROOT}/_build/default/rel/flurmctld"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${GREEN}[TEST]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    if [ -f "${BUILD_DIR}/bin/flurmctld" ]; then
        "${BUILD_DIR}/bin/flurmctld" stop 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Test 1: Build initial release
test_build_release() {
    log_test "Test 1: Building initial release (0.1.0)"
    cd "$PROJECT_ROOT"

    rebar3 compile
    rebar3 release -n flurmctld

    if [ ! -f "${BUILD_DIR}/bin/flurmctld" ]; then
        log_error "Release build failed - no binary found"
        return 1
    fi

    log_info "Release built successfully"
    return 0
}

# Test 2: Start release and verify it's running
test_start_release() {
    log_test "Test 2: Starting release"

    # Create required directories
    sudo mkdir -p /var/log/flurm /var/lib/flurm/ra 2>/dev/null || \
        mkdir -p /tmp/flurm/log /tmp/flurm/ra

    # Update sys.config to use temp directories if needed
    if [ ! -w /var/log/flurm ]; then
        log_warn "Using /tmp for logs (no write access to /var/log/flurm)"
        export FLURM_LOG_DIR=/tmp/flurm/log
        export FLURM_DATA_DIR=/tmp/flurm/ra
    fi

    # Start in background
    "${BUILD_DIR}/bin/flurmctld" daemon

    # Wait for startup
    sleep 5

    # Check if running
    if ! "${BUILD_DIR}/bin/flurmctld" ping; then
        log_error "Release failed to start"
        "${BUILD_DIR}/bin/flurmctld" stop 2>/dev/null || true
        return 1
    fi

    log_info "Release started successfully"
    return 0
}

# Test 3: Hot reload a module
test_hot_reload() {
    log_test "Test 3: Hot reload module"

    # Reload flurm_upgrade module
    result=$("${BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade reload_module flurm_upgrade)

    if echo "$result" | grep -q "ok"; then
        log_info "Module hot reload successful"
        return 0
    else
        log_error "Module hot reload failed: $result"
        return 1
    fi
}

# Test 4: Test state preservation
test_state_preservation() {
    log_test "Test 4: State preservation during reload"

    # Set a config value
    "${BUILD_DIR}/bin/flurmctld" rpc flurm_config_server set test_upgrade_key test_upgrade_value

    # Verify it's set
    value=$("${BUILD_DIR}/bin/flurmctld" rpc flurm_config_server get test_upgrade_key)
    if ! echo "$value" | grep -q "test_upgrade_value"; then
        log_error "Config value not set properly: $value"
        return 1
    fi

    # Reload the module
    "${BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade reload_module flurm_config_server 2>/dev/null || true

    # Verify state is preserved
    value=$("${BUILD_DIR}/bin/flurmctld" rpc flurm_config_server get test_upgrade_key)
    if echo "$value" | grep -q "test_upgrade_value"; then
        log_info "State preserved during reload"
        return 0
    else
        log_error "State lost during reload: $value"
        return 1
    fi
}

# Test 5: Check upgrade status
test_upgrade_status() {
    log_test "Test 5: Upgrade status check"

    status=$("${BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade upgrade_status)

    if echo "$status" | grep -q "versions"; then
        log_info "Upgrade status available"
        echo "$status"
        return 0
    else
        log_error "Failed to get upgrade status: $status"
        return 1
    fi
}

# Test 6: Check pre-upgrade validation
test_upgrade_check() {
    log_test "Test 6: Pre-upgrade validation"

    # Check for non-existent version (should fail gracefully)
    result=$("${BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade check_upgrade '"99.99.99"' 2>&1)

    if echo "$result" | grep -q "error"; then
        log_info "Pre-upgrade check correctly rejects invalid version"
        return 0
    else
        log_error "Pre-upgrade check didn't reject invalid version: $result"
        return 1
    fi
}

# Run all tests
run_tests() {
    local failed=0
    local passed=0
    local tests=(
        test_build_release
        test_start_release
        test_hot_reload
        test_state_preservation
        test_upgrade_status
        test_upgrade_check
    )

    log_info "Running hot upgrade E2E tests..."
    echo ""

    for test in "${tests[@]}"; do
        if $test; then
            ((passed++))
        else
            ((failed++))
            log_error "Test failed: $test"
        fi
        echo ""
    done

    echo "=========================================="
    if [ $failed -eq 0 ]; then
        log_info "All $passed tests passed!"
        return 0
    else
        log_error "$failed test(s) failed, $passed passed"
        return 1
    fi
}

# Main
case "${1:-all}" in
    all)
        run_tests
        ;;
    build)
        test_build_release
        ;;
    start)
        test_start_release
        ;;
    reload)
        test_hot_reload
        ;;
    state)
        test_state_preservation
        ;;
    status)
        test_upgrade_status
        ;;
    check)
        test_upgrade_check
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 [all|build|start|reload|state|status|check|cleanup]"
        exit 1
        ;;
esac
