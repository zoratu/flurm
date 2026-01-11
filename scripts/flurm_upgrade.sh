#!/bin/bash
#
# FLURM Hot Upgrade Script
#
# Usage:
#   flurm_upgrade check <version>       - Check if upgrade is safe
#   flurm_upgrade install <version>     - Install a new release
#   flurm_upgrade rollback              - Rollback to previous version
#   flurm_upgrade status                - Show current versions
#   flurm_upgrade reload <module>       - Hot reload a single module
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_ROOT="${SCRIPT_DIR}/.."
RELEASE_NAME="${RELEASE_NAME:-flurmctld}"
NODE_NAME="${NODE_NAME:-flurmctld@localhost}"
COOKIE="${COOKIE:-flurm_cluster_cookie}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Execute an Erlang expression on the running node
erl_eval() {
    local expr="$1"
    erl -noshell -sname "upgrade_$$" -setcookie "$COOKIE" \
        -eval "
            case net_adm:ping('${NODE_NAME}') of
                pong ->
                    Result = rpc:call('${NODE_NAME}', erlang, apply, [fun() -> $expr end, []]),
                    io:format(\"~p~n\", [Result]),
                    init:stop(0);
                pang ->
                    io:format(\"Error: Cannot connect to node ${NODE_NAME}~n\"),
                    init:stop(1)
            end
        "
}

# Check upgrade safety
cmd_check() {
    local version="$1"
    if [ -z "$version" ]; then
        log_error "Usage: flurm_upgrade check <version>"
        exit 1
    fi

    log_info "Checking upgrade to version $version..."

    result=$(erl_eval "flurm_upgrade:check_upgrade(\"$version\")")

    if echo "$result" | grep -q "^ok$"; then
        log_info "Upgrade check passed - safe to upgrade to $version"
    else
        log_error "Upgrade check failed:"
        echo "$result"
        exit 1
    fi
}

# Install a new release
cmd_install() {
    local version="$1"
    if [ -z "$version" ]; then
        log_error "Usage: flurm_upgrade install <version>"
        exit 1
    fi

    # Check for release tarball
    local tarball="${RELEASE_ROOT}/releases/${version}.tar.gz"
    if [ ! -f "$tarball" ]; then
        log_error "Release tarball not found: $tarball"
        log_info "Build the release first with: rebar3 release -n ${RELEASE_NAME}"
        exit 1
    fi

    log_info "Installing release $version..."

    # Pre-flight checks
    cmd_check "$version" || {
        log_error "Pre-flight checks failed. Aborting upgrade."
        exit 1
    }

    # Perform the upgrade
    log_info "Installing release..."
    result=$(erl_eval "flurm_upgrade:install_release(\"$version\")")

    if echo "$result" | grep -q "^ok$"; then
        log_info "Upgrade to $version completed successfully!"
        cmd_status
    else
        log_error "Upgrade failed:"
        echo "$result"
        exit 1
    fi
}

# Rollback to previous version
cmd_rollback() {
    log_info "Rolling back to previous release..."

    result=$(erl_eval "flurm_upgrade:rollback()")

    if echo "$result" | grep -q "^ok$"; then
        log_info "Rollback completed successfully!"
        cmd_status
    else
        log_error "Rollback failed:"
        echo "$result"
        exit 1
    fi
}

# Show current status
cmd_status() {
    log_info "Current release status:"
    echo ""

    # Get versions
    echo "Application Versions:"
    erl_eval "flurm_upgrade:versions()"
    echo ""

    # Get releases
    echo "Installed Releases:"
    erl_eval "flurm_upgrade:which_releases()"
    echo ""

    # Check for old code
    echo "Modules with old code:"
    erl_eval "
        case flurm_upgrade:upgrade_status() of
            #{old_code_modules := Mods} when Mods =/= [] ->
                Mods;
            _ ->
                'none'
        end
    "
}

# Hot reload a single module
cmd_reload() {
    local module="$1"
    if [ -z "$module" ]; then
        log_error "Usage: flurm_upgrade reload <module>"
        exit 1
    fi

    log_info "Hot reloading module: $module"

    result=$(erl_eval "flurm_upgrade:reload_module($module)")

    if echo "$result" | grep -q "^ok$"; then
        log_info "Module $module reloaded successfully!"
    else
        log_error "Module reload failed:"
        echo "$result"
        exit 1
    fi
}

# Generate a new release version
cmd_build() {
    local version="$1"
    if [ -z "$version" ]; then
        log_error "Usage: flurm_upgrade build <version>"
        exit 1
    fi

    log_info "Building release version $version..."

    cd "${RELEASE_ROOT}"

    # Update version in app.src files
    for app_src in apps/*/src/*.app.src; do
        sed -i.bak "s/{vsn, \"[^\"]*\"}/{vsn, \"$version\"}/" "$app_src"
        rm -f "${app_src}.bak"
    done

    # Update rebar.config release versions
    sed -i.bak "s/{release, {flurm, \"[^\"]*\"}}/{release, {flurm, \"$version\"}}/" rebar.config
    sed -i.bak "s/{release, {flurmctld, \"[^\"]*\"}}/{release, {flurmctld, \"$version\"}}/" rebar.config
    sed -i.bak "s/{release, {flurmnd, \"[^\"]*\"}}/{release, {flurmnd, \"$version\"}}/" rebar.config
    rm -f rebar.config.bak

    # Build the release
    rebar3 compile
    rebar3 release -n "${RELEASE_NAME}"

    # Generate relup if there's a previous version
    if [ -d "_build/default/rel/${RELEASE_NAME}/releases" ]; then
        local prev_version=$(ls -t "_build/default/rel/${RELEASE_NAME}/releases" | grep -v RELEASES | head -1)
        if [ -n "$prev_version" ] && [ "$prev_version" != "$version" ]; then
            log_info "Generating relup from $prev_version to $version..."
            rebar3 relup -n "${RELEASE_NAME}" -v "$version" -u "$prev_version"
        fi
    fi

    # Create tarball
    rebar3 tar -n "${RELEASE_NAME}"

    log_info "Release $version built successfully!"
    log_info "Tarball: _build/default/rel/${RELEASE_NAME}/${RELEASE_NAME}-${version}.tar.gz"
}

# Main dispatch
case "${1:-}" in
    check)
        cmd_check "$2"
        ;;
    install)
        cmd_install "$2"
        ;;
    rollback)
        cmd_rollback
        ;;
    status)
        cmd_status
        ;;
    reload)
        cmd_reload "$2"
        ;;
    build)
        cmd_build "$2"
        ;;
    -h|--help|help)
        echo "FLURM Hot Upgrade Tool"
        echo ""
        echo "Usage:"
        echo "  flurm_upgrade check <version>   - Check if upgrade is safe"
        echo "  flurm_upgrade install <version> - Install a new release"
        echo "  flurm_upgrade rollback          - Rollback to previous version"
        echo "  flurm_upgrade status            - Show current versions"
        echo "  flurm_upgrade reload <module>   - Hot reload a single module"
        echo "  flurm_upgrade build <version>   - Build a new release version"
        echo ""
        echo "Environment Variables:"
        echo "  NODE_NAME - Target node (default: flurmctld@localhost)"
        echo "  COOKIE    - Erlang cookie (default: flurm_cluster_cookie)"
        ;;
    *)
        log_error "Unknown command: ${1:-}"
        echo "Run 'flurm_upgrade help' for usage."
        exit 1
        ;;
esac
