#!/bin/bash
# FLURM SLURM Client Compatibility Test Runner
#
# Wrapper script that:
# - Runs the SLURM client compatibility tests
# - Captures output to a timestamped log file
# - Generates a summary report in JSON and text formats
# - Supports CI/CD integration with exit codes
#
# Usage: ./run_client_tests.sh [options]
#
# Options:
#   --no-cleanup    Don't stop containers after tests
#   --verbose       Show full output during test run
#   --quiet         Suppress test output (only show summary)
#   --json          Output results as JSON only
#   --help          Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
SCRIPTS_DIR="${DOCKER_DIR}/scripts"
TEST_SCRIPT="${SCRIPTS_DIR}/test_slurm_clients.sh"

# Configuration
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${SCRIPT_DIR}/client_test_${TIMESTAMP}.log"
SUMMARY_FILE="${SCRIPT_DIR}/client_test_${TIMESTAMP}_summary.json"
REPORT_FILE="${SCRIPT_DIR}/client_test_${TIMESTAMP}_report.txt"

# Options
NO_CLEANUP=false
VERBOSE=false
QUIET=false
JSON_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --quiet)
            QUIET=true
            shift
            ;;
        --json)
            JSON_ONLY=true
            shift
            ;;
        --help)
            echo "FLURM SLURM Client Compatibility Test Runner"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --no-cleanup    Don't stop containers after tests"
            echo "  --verbose       Show full output during test run"
            echo "  --quiet         Suppress test output (only show summary)"
            echo "  --json          Output results as JSON only"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Colors (disabled if output is not a terminal or JSON mode)
if [ -t 1 ] && [ "$JSON_ONLY" = false ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

log() {
    if [ "$QUIET" = false ] && [ "$JSON_ONLY" = false ]; then
        echo -e "$1"
    fi
}

# Check prerequisites
check_prerequisites() {
    log "${BLUE}Checking prerequisites...${NC}"

    if ! command -v docker &> /dev/null; then
        echo "ERROR: docker is not installed or not in PATH"
        exit 1
    fi

    if ! docker compose version &> /dev/null; then
        echo "ERROR: docker compose is not available"
        exit 1
    fi

    if [ ! -f "$TEST_SCRIPT" ]; then
        echo "ERROR: Test script not found: $TEST_SCRIPT"
        exit 1
    fi

    if [ ! -x "$TEST_SCRIPT" ]; then
        log "${YELLOW}Making test script executable...${NC}"
        chmod +x "$TEST_SCRIPT"
    fi

    log "${GREEN}Prerequisites OK${NC}"
}

# Parse test results from log
parse_results() {
    local log_content="$1"

    # Count PASS/FAIL/SKIP occurrences
    local passed=$(echo "$log_content" | grep -c '\[PASS\]' || echo "0")
    local failed=$(echo "$log_content" | grep -c '\[FAIL\]' || echo "0")
    local skipped=$(echo "$log_content" | grep -c '\[SKIP\]' || echo "0")

    # Extract individual test results
    local pass_tests=$(echo "$log_content" | grep '\[PASS\]' | sed 's/.*\[PASS\] //' || echo "")
    local fail_tests=$(echo "$log_content" | grep '\[FAIL\]' | sed 's/.*\[FAIL\] //' || echo "")
    local skip_tests=$(echo "$log_content" | grep '\[SKIP\]' | sed 's/.*\[SKIP\] //' || echo "")

    echo "$passed|$failed|$skipped"
}

# Generate JSON summary
generate_json_summary() {
    local passed=$1
    local failed=$2
    local skipped=$3
    local duration=$4
    local exit_code=$5
    local log_content="$6"

    local total=$((passed + failed))
    local pass_rate=0
    if [ $total -gt 0 ]; then
        pass_rate=$((passed * 100 / total))
    fi

    local status="success"
    if [ $failed -gt 0 ]; then
        status="failure"
    fi

    # Extract test details
    local pass_tests=$(echo "$log_content" | grep '\[PASS\]' | sed 's/.*\[PASS\] //' | jq -R -s 'split("\n") | map(select(length > 0))' 2>/dev/null || echo "[]")
    local fail_tests=$(echo "$log_content" | grep '\[FAIL\]' | sed 's/.*\[FAIL\] //' | jq -R -s 'split("\n") | map(select(length > 0))' 2>/dev/null || echo "[]")
    local skip_tests=$(echo "$log_content" | grep '\[SKIP\]' | sed 's/.*\[SKIP\] //' | jq -R -s 'split("\n") | map(select(length > 0))' 2>/dev/null || echo "[]")

    cat > "$SUMMARY_FILE" << EOF
{
    "test_run": {
        "timestamp": "$(date -Iseconds)",
        "timestamp_unix": $(date +%s),
        "duration_seconds": $duration,
        "log_file": "$LOG_FILE",
        "report_file": "$REPORT_FILE"
    },
    "summary": {
        "status": "$status",
        "total_tests": $total,
        "passed": $passed,
        "failed": $failed,
        "skipped": $skipped,
        "pass_rate_percent": $pass_rate
    },
    "tests": {
        "passed": $pass_tests,
        "failed": $fail_tests,
        "skipped": $skip_tests
    },
    "environment": {
        "docker_version": "$(docker --version 2>/dev/null | head -1 || echo 'unknown')",
        "compose_version": "$(docker compose version 2>/dev/null | head -1 || echo 'unknown')",
        "hostname": "$(hostname)",
        "platform": "$(uname -s)",
        "arch": "$(uname -m)"
    },
    "exit_code": $exit_code
}
EOF
}

# Generate text report
generate_text_report() {
    local passed=$1
    local failed=$2
    local skipped=$3
    local duration=$4
    local exit_code=$5
    local log_content="$6"

    local total=$((passed + failed))
    local pass_rate=0
    if [ $total -gt 0 ]; then
        pass_rate=$((passed * 100 / total))
    fi

    cat > "$REPORT_FILE" << EOF
================================================================================
FLURM SLURM Client Compatibility Test Report
================================================================================

Test Run Information
--------------------
Timestamp:    $(date)
Duration:     ${duration} seconds
Log File:     $LOG_FILE
Exit Code:    $exit_code

Summary
-------
Total Tests:  $total
Passed:       $passed
Failed:       $failed
Skipped:      $skipped
Pass Rate:    ${pass_rate}%

Passed Tests
------------
$(echo "$log_content" | grep '\[PASS\]' | sed 's/.*\[PASS\] /  - /' || echo "  (none)")

Failed Tests
------------
$(echo "$log_content" | grep '\[FAIL\]' | sed 's/.*\[FAIL\] /  - /' || echo "  (none)")

Skipped Tests
-------------
$(echo "$log_content" | grep '\[SKIP\]' | sed 's/.*\[SKIP\] /  - /' || echo "  (none)")

Environment
-----------
Docker:       $(docker --version 2>/dev/null | head -1 || echo 'unknown')
Compose:      $(docker compose version 2>/dev/null | head -1 || echo 'unknown')
Hostname:     $(hostname)
Platform:     $(uname -s) $(uname -m)

================================================================================
End of Report
================================================================================
EOF
}

# Main execution
main() {
    log ""
    log "${BLUE}=============================================="
    log "FLURM SLURM Client Compatibility Test Runner"
    log "==============================================${NC}"
    log ""
    log "Timestamp: $(date)"
    log "Log File:  $LOG_FILE"
    log ""

    # Check prerequisites
    check_prerequisites

    # Build test command
    local test_cmd="$TEST_SCRIPT"
    if [ "$NO_CLEANUP" = true ]; then
        test_cmd="$test_cmd --no-cleanup"
    fi

    # Record start time
    local start_time=$(date +%s)

    # Run tests and capture output
    log "${BLUE}Running tests...${NC}"
    log ""

    local exit_code=0
    local log_content=""

    if [ "$VERBOSE" = true ]; then
        # Show output in real-time and capture it
        $test_cmd 2>&1 | tee "$LOG_FILE"
        exit_code=${PIPESTATUS[0]}
        log_content=$(cat "$LOG_FILE")
    else
        # Capture output silently
        log_content=$($test_cmd 2>&1) || exit_code=$?
        echo "$log_content" > "$LOG_FILE"

        if [ "$QUIET" = false ] && [ "$JSON_ONLY" = false ]; then
            # Show a progress indicator
            echo "$log_content" | grep -E '\[(PASS|FAIL|SKIP|INFO)\]' || true
        fi
    fi

    # Record end time and calculate duration
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    # Parse results
    local results=$(parse_results "$log_content")
    local passed=$(echo "$results" | cut -d'|' -f1)
    local failed=$(echo "$results" | cut -d'|' -f2)
    local skipped=$(echo "$results" | cut -d'|' -f3)

    # Generate reports
    generate_json_summary "$passed" "$failed" "$skipped" "$duration" "$exit_code" "$log_content"
    generate_text_report "$passed" "$failed" "$skipped" "$duration" "$exit_code" "$log_content"

    # Output results
    if [ "$JSON_ONLY" = true ]; then
        cat "$SUMMARY_FILE"
    else
        log ""
        log "${BLUE}=============================================="
        log "Test Results"
        log "==============================================${NC}"
        log ""
        log "Duration: ${duration} seconds"
        log ""

        local total=$((passed + failed))
        local pass_rate=0
        if [ $total -gt 0 ]; then
            pass_rate=$((passed * 100 / total))
        fi

        echo -e "${GREEN}Passed:  $passed${NC}"
        echo -e "${RED}Failed:  $failed${NC}"
        echo -e "${YELLOW}Skipped: $skipped${NC}"
        echo ""
        echo "Pass Rate: ${pass_rate}%"
        log ""
        log "Reports generated:"
        log "  Log:     $LOG_FILE"
        log "  Summary: $SUMMARY_FILE"
        log "  Report:  $REPORT_FILE"
        log ""

        if [ $failed -eq 0 ]; then
            log "${GREEN}All tests passed!${NC}"
        else
            log "${RED}Some tests failed. See log for details.${NC}"
        fi
    fi

    # Clean up old test results (keep last 10)
    log ""
    log "${BLUE}Cleaning up old test results...${NC}"
    cd "$SCRIPT_DIR"
    ls -t client_test_*.log 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true
    ls -t client_test_*_summary.json 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true
    ls -t client_test_*_report.txt 2>/dev/null | tail -n +11 | xargs rm -f 2>/dev/null || true

    exit $exit_code
}

# Run main
main "$@"
