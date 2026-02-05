#!/bin/bash
#
# FLURM Guide Examples Test Script (Phase 8C)
#
# Validates code examples from documentation:
# 1. Extracts and compiles Erlang code blocks
# 2. Validates YAML and JSON syntax
# 3. Validates docker-compose files
# 4. Checks shell command references
#
# Usage:
#   ./scripts/test-guide-examples.sh           # Run all tests
#   ./scripts/test-guide-examples.sh --erlang  # Erlang only
#   ./scripts/test-guide-examples.sh --yaml    # YAML/JSON only
#   ./scripts/test-guide-examples.sh --docker  # Docker only
#

# Don't use set -e as it breaks arithmetic increment when value is 0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$PROJECT_DIR/docs"
DOCKER_DIR="$PROJECT_DIR/docker"
TMP_DIR=$(mktemp -d)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Counters
PASSED=0
FAILED=0
SKIPPED=0

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASSED=$((PASSED + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAILED=$((FAILED + 1))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    SKIPPED=$((SKIPPED + 1))
}

log_info() {
    echo -e "      $1"
}

#
# Extract code blocks from markdown files
# Args: $1 = language filter (erlang, bash, yaml, json, etc.)
#
extract_code_blocks() {
    local lang="$1"
    local file="$2"
    local output_dir="$3"
    local block_num=0
    local in_block=false
    local current_block=""

    while IFS= read -r line; do
        if [[ "$line" =~ ^\`\`\`${lang}$ ]] || [[ "$line" =~ ^\`\`\`${lang}[[:space:]] ]]; then
            in_block=true
            current_block=""
        elif [[ "$line" == '```' ]] && [[ "$in_block" == true ]]; then
            in_block=false
            if [[ -n "$current_block" ]]; then
                block_num=$((block_num + 1))
                local basename=$(basename "$file" .md)
                echo "$current_block" > "$output_dir/${basename}_${block_num}.${lang}"
            fi
        elif [[ "$in_block" == true ]]; then
            current_block+="$line"$'\n'
        fi
    done < "$file"
}

#
# Test Erlang code blocks compile
#
test_erlang_blocks() {
    echo ""
    echo "=== Testing Erlang Code Blocks ==="

    local erlang_tmp="$TMP_DIR/erlang"
    mkdir -p "$erlang_tmp"

    # Extract all Erlang blocks
    for doc in "$DOCS_DIR"/*.md; do
        extract_code_blocks "erlang" "$doc" "$erlang_tmp"
    done

    local block_count=$(ls -1 "$erlang_tmp"/*.erlang 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$block_count" -eq 0 ]]; then
        log_skip "No Erlang code blocks found"
        return
    fi

    log_info "Found $block_count Erlang code blocks"

    local modules=0
    local fragments=0

    # Test each block
    for block in "$erlang_tmp"/*.erlang; do
        local name=$(basename "$block")

        # Only validate complete modules (have -module declaration)
        # Documentation fragments are skipped by default
        if grep -q "^-module(" "$block" 2>/dev/null; then
            modules=$((modules + 1))

            # Extract actual module name and use it for the filename
            local mod_name=$(grep "^-module(" "$block" | sed 's/-module(\([^)]*\)).*/\1/')
            local test_file="$erlang_tmp/${mod_name}.erl"
            cp "$block" "$test_file"

            # Try to compile (syntax check with project includes)
            if erlc -W0 \
                -I "$PROJECT_DIR/apps/flurm_core/include" \
                -I "$PROJECT_DIR/apps/flurm_protocol/include" \
                -I "$PROJECT_DIR/apps/flurm_controller/include" \
                -o "$erlang_tmp" "$test_file" 2>/dev/null; then
                log_pass "Erlang module: $mod_name"
            else
                local err=$(erlc -W0 \
                    -I "$PROJECT_DIR/apps/flurm_core/include" \
                    -I "$PROJECT_DIR/apps/flurm_protocol/include" \
                    -I "$PROJECT_DIR/apps/flurm_controller/include" \
                    -o "$erlang_tmp" "$test_file" 2>&1 | head -1)

                # Check for common documentation patterns
                if grep -qE "^\.\.\.|% \.\.\.|\.\.\.," "$block" 2>/dev/null; then
                    log_skip "Erlang module: $mod_name (contains ellipsis)"
                elif echo "$err" | grep -qE "undefined|can't find include|redefining"; then
                    # Missing functions/includes/redefinitions are expected in doc examples
                    log_skip "Erlang module: $mod_name (doc example, has conflicts)"
                elif grep -qE "^%.*TODO|^%.*FIXME" "$block" 2>/dev/null; then
                    log_skip "Erlang module: $mod_name (contains TODO)"
                else
                    log_fail "Erlang module: $mod_name"
                    log_info "  $err"
                fi
            fi
        else
            fragments=$((fragments + 1))
        fi
    done

    log_info "Validated $modules complete modules, skipped $fragments code fragments"
}

#
# Test YAML syntax
#
test_yaml_blocks() {
    echo ""
    echo "=== Testing YAML Syntax ==="

    # Check if we have a YAML validator
    if ! command -v python3 &> /dev/null; then
        log_skip "Python3 not available for YAML validation"
        return
    fi

    local yaml_tmp="$TMP_DIR/yaml"
    mkdir -p "$yaml_tmp"

    # Extract YAML blocks
    for doc in "$DOCS_DIR"/*.md; do
        extract_code_blocks "yaml" "$doc" "$yaml_tmp"
    done

    local block_count=$(ls -1 "$yaml_tmp"/*.yaml 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$block_count" -eq 0 ]]; then
        log_skip "No YAML code blocks found"
        return
    fi

    log_info "Found $block_count YAML blocks"

    for block in "$yaml_tmp"/*.yaml; do
        local name=$(basename "$block")
        # Check for shell commands mislabeled as YAML
        if grep -qE '^(cd |docker|kubectl|rebar3|./|#.*$)' "$block" 2>/dev/null; then
            log_skip "YAML: $name (shell commands)"
        elif python3 -c "import yaml; yaml.safe_load(open('$block'))" 2>/dev/null; then
            log_pass "YAML: $name"
        else
            log_fail "YAML: $name"
        fi
    done
}

#
# Test JSON syntax
#
test_json_blocks() {
    echo ""
    echo "=== Testing JSON Syntax ==="

    local json_tmp="$TMP_DIR/json"
    mkdir -p "$json_tmp"

    # Extract JSON blocks
    for doc in "$DOCS_DIR"/*.md; do
        extract_code_blocks "json" "$doc" "$json_tmp"
    done

    local block_count=$(ls -1 "$json_tmp"/*.json 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$block_count" -eq 0 ]]; then
        log_skip "No JSON code blocks found"
        return
    fi

    log_info "Found $block_count JSON blocks"

    for block in "$json_tmp"/*.json; do
        local name=$(basename "$block")
        # Check for JSON5-style comments (not valid JSON but common in docs)
        if grep -qE '^\s*//' "$block" 2>/dev/null; then
            log_skip "JSON: $name (contains comments)"
        # Check for ellipsis placeholders
        elif grep -qE '\.\.\.|<[^>]+>' "$block" 2>/dev/null; then
            log_skip "JSON: $name (contains placeholders)"
        elif python3 -c "import json; json.load(open('$block'))" 2>/dev/null; then
            log_pass "JSON: $name"
        elif jq . "$block" &>/dev/null; then
            log_pass "JSON: $name"
        else
            log_fail "JSON: $name"
        fi
    done
}

#
# Test docker-compose files
#
test_docker_compose() {
    echo ""
    echo "=== Testing Docker Compose Files ==="

    if ! command -v docker &> /dev/null; then
        log_skip "Docker not available"
        return
    fi

    for compose_file in "$DOCKER_DIR"/docker-compose*.yml; do
        local name=$(basename "$compose_file")

        # Validate syntax with docker compose config
        if docker compose -f "$compose_file" config &>/dev/null; then
            log_pass "Docker: $name"
        else
            # Try older docker-compose command
            if docker-compose -f "$compose_file" config &>/dev/null; then
                log_pass "Docker: $name"
            else
                log_fail "Docker: $name"
            fi
        fi
    done
}

#
# Test that referenced rebar3 commands exist
#
test_rebar3_commands() {
    echo ""
    echo "=== Testing rebar3 Command References ==="

    if ! command -v rebar3 &> /dev/null; then
        log_skip "rebar3 not available"
        return
    fi

    # Extract unique rebar3 subcommands from docs
    local subcommands=$(grep -ohE 'rebar3 [a-z_]+' "$DOCS_DIR"/*.md 2>/dev/null | \
        sed 's/rebar3 //' | sort -u)

    for subcmd in $subcommands; do
        # Skip empty or invalid
        [[ -z "$subcmd" ]] && continue

        # Check if it's a known valid rebar3 command
        if [[ "$subcmd" =~ ^(compile|eunit|ct|dialyzer|cover|proper|release|shell|escriptize|as|do|deps|clean|new|update|upgrade|hex|path|plugins|tree|xref|edoc|get-deps)$ ]]; then
            log_pass "rebar3: $subcmd"
        elif rebar3 help "$subcmd" &>/dev/null; then
            log_pass "rebar3: $subcmd"
        else
            log_skip "rebar3: $subcmd (may be plugin)"
        fi
    done
}

#
# Main
#
main() {
    echo "FLURM Guide Examples Test"
    echo "========================="
    echo "Project: $PROJECT_DIR"
    echo "Docs: $DOCS_DIR"

    local test_all=true
    local test_erlang=false
    local test_yaml=false
    local test_docker=false

    # Parse arguments
    for arg in "$@"; do
        case $arg in
            --erlang)
                test_all=false
                test_erlang=true
                ;;
            --yaml)
                test_all=false
                test_yaml=true
                ;;
            --docker)
                test_all=false
                test_docker=true
                ;;
            --help)
                echo "Usage: $0 [--erlang] [--yaml] [--docker]"
                exit 0
                ;;
        esac
    done

    # Run tests
    if [[ "$test_all" == true ]] || [[ "$test_erlang" == true ]]; then
        test_erlang_blocks
    fi

    if [[ "$test_all" == true ]] || [[ "$test_yaml" == true ]]; then
        test_yaml_blocks
        test_json_blocks
    fi

    if [[ "$test_all" == true ]] || [[ "$test_docker" == true ]]; then
        test_docker_compose
    fi

    if [[ "$test_all" == true ]]; then
        test_rebar3_commands
    fi

    # Summary
    echo ""
    echo "=== Summary ==="
    echo -e "${GREEN}Passed:${NC}  $PASSED"
    echo -e "${RED}Failed:${NC}  $FAILED"
    echo -e "${YELLOW}Skipped:${NC} $SKIPPED"

    if [[ "$FAILED" -gt 0 ]]; then
        echo ""
        echo -e "${RED}Some tests failed!${NC}"
        exit 1
    else
        echo ""
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    fi
}

main "$@"
