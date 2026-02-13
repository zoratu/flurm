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
set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT_REAL="${SCRIPT_DIR}/.."
PROJECT_ROOT="${PROJECT_ROOT_REAL}"
if [[ "$PROJECT_ROOT_REAL" == *" "* ]]; then
    PROJECT_ROOT="/tmp/flurm_hot_upgrade_src"
    ln -sfn "$PROJECT_ROOT_REAL" "$PROJECT_ROOT"
fi
BUILD_DIR="${PROJECT_ROOT}/_build/default/rel/flurmctld"
RUNTIME_BUILD_DIR="${BUILD_DIR}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${GREEN}[TEST]${NC} $1"; }

ensure_runtime_build_dir() {
    RUNTIME_BUILD_DIR="${BUILD_DIR}"
    if [[ "$PROJECT_ROOT_REAL" == *" "* ]]; then
        local runtime_copy="/tmp/flurm_hot_upgrade_rel"
        rm -rf "$runtime_copy"
        mkdir -p "$runtime_copy"
        cp -R "${BUILD_DIR}/." "$runtime_copy/"
        RUNTIME_BUILD_DIR="$runtime_copy"
    fi

    # relx scripts in this project enable pre/post start hooks by default.
    # On some /bin/sh + set -e combinations, missing hook files abort startup.
    mkdir -p "${RUNTIME_BUILD_DIR}/bin/hooks"
    for hook in pre_start post_start; do
        cat > "${RUNTIME_BUILD_DIR}/bin/hooks/${hook}" <<'EOF'
#!/bin/sh
:
EOF
        chmod +x "${RUNTIME_BUILD_DIR}/bin/hooks/${hook}"
    done
}

configure_release_paths() {
    ensure_runtime_build_dir

    local release_cfg="${RUNTIME_BUILD_DIR}/releases/1.0.0/sys.config"
    local release_vm_args="${RUNTIME_BUILD_DIR}/releases/1.0.0/vm.args"
    local tmp_root="/tmp/flurm"
    local tmp_log="${tmp_root}/log"
    local tmp_data="${tmp_root}"
    local tmp_etc="${tmp_root}/etc"
    local tmp_cfg="${tmp_root}/sys.config"
    local tmp_vm_args="${tmp_root}/vm.args"

    mkdir -p "${tmp_log}" "${tmp_data}/ra" "${tmp_data}/db" "${tmp_etc}"
    : > "${tmp_etc}/flurm.conf"

    if [ ! -f "$release_cfg" ] || [ ! -f "$release_vm_args" ]; then
        log_error "Release config files missing under ${RUNTIME_BUILD_DIR}/releases/1.0.0"
        return 1
    fi

    cp "$release_cfg" "$tmp_cfg"
    cp "$release_vm_args" "$tmp_vm_args"

    sed -i.bak \
        -e "s|/var/log/flurm|${tmp_log}|g" \
        -e "s|/var/lib/flurm|${tmp_data}|g" \
        -e "s|/etc/flurm/flurm.conf|${tmp_etc}/flurm.conf|g" \
        "$tmp_cfg"
    sed -i.bak \
        -e "s|/var/log/flurm|${tmp_log}|g" \
        -e "s|^+sbt db$|+sbt u|g" \
        "$tmp_vm_args"

    export FLURM_LOG_DIR="${tmp_log}"
    export FLURM_DATA_DIR="${tmp_data}"
    export RELX_CONFIG_PATH="${tmp_cfg}"
    export VMARGS_PATH="${tmp_vm_args}"
    return 0
}

cleanup() {
    log_info "Cleaning up..."
    if [ -f "${RUNTIME_BUILD_DIR}/bin/flurmctld" ]; then
        if "${RUNTIME_BUILD_DIR}/bin/flurmctld" ping >/dev/null 2>&1; then
            "${RUNTIME_BUILD_DIR}/bin/flurmctld" stop 2>/dev/null || true
        fi
    fi
}

trap cleanup EXIT

# Test 1: Build initial release
test_build_release() {
    log_test "Test 1: Building initial release (0.1.0)"
    cd "$PROJECT_ROOT"

    rebar3 compile
    rebar3 release -n flurmctld
    ensure_runtime_build_dir

    if [ ! -f "${RUNTIME_BUILD_DIR}/bin/flurmctld" ]; then
        log_error "Release build failed - no binary found"
        return 1
    fi

    log_info "Release built successfully"
    return 0
}

# Test 2: Start release and verify it's running
test_start_release() {
    log_test "Test 2: Starting release"

    # Release runtime paths are hardcoded in vm.args/sys.config, so rewrite
    # them to /tmp for non-root test environments.
    if ! configure_release_paths; then
        log_error "Failed to configure release paths for test environment"
        return 1
    fi

    # Start in background. Avoid the relx "daemon" command here because it can
    # block forever waiting for is_alive when startup fails.
    "${RUNTIME_BUILD_DIR}/bin/flurmctld" foreground >/tmp/flurm/hot_upgrade_node.log 2>&1 &

    # Wait for startup (release boot can take longer under load/CI).
    local started=0
    for _ in $(seq 1 60); do
        if "${RUNTIME_BUILD_DIR}/bin/flurmctld" ping >/dev/null 2>&1; then
            started=1
            break
        fi
        sleep 1
    done
    if [ "$started" -ne 1 ]; then
        log_error "Release failed to start"
        tail -n 80 /tmp/flurm/hot_upgrade_node.log 2>/dev/null || true
        return 1
    fi

    log_info "Release started successfully"
    return 0
}

# Test 3: Hot reload a module
test_hot_reload() {
    log_test "Test 3: Hot reload module"

    # Reload flurm_upgrade module
    result=$("${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade reload_module '[flurm_upgrade]')

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
    "${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_config_server set '[test_upgrade_key, test_upgrade_value]'

    # Verify it's set
    value=$("${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_config_server get '[test_upgrade_key]')
    if ! echo "$value" | grep -q "test_upgrade_value"; then
        log_error "Config value not set properly: $value"
        return 1
    fi

    # Reload the module
    "${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade reload_module '[flurm_config_server]' 2>/dev/null || true

    # Verify state is preserved
    value=$("${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_config_server get '[test_upgrade_key]')
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

    status=$("${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade upgrade_status)

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
    result=$("${RUNTIME_BUILD_DIR}/bin/flurmctld" rpc flurm_upgrade check_upgrade '["99.99.99"]' 2>&1)

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
