#!/bin/bash
#
# TLC Model Checker Runner for FLURM TLA+ Specs
#
# This script runs the TLC model checker on the FLURM TLA+ specifications
# to verify safety properties and invariants.
#
# Prerequisites:
#   - Java 11+ installed and in PATH
#   - tla2tools.jar in the same directory (auto-downloaded if missing)
#
# Usage:
#   ./run_tlc.sh                      # Run all specs
#   ./run_tlc.sh FlurmFederation      # Run specific spec
#   ./run_tlc.sh --download           # Download tla2tools.jar only
#   ./run_tlc.sh --json               # Output results in JSON format
#   ./run_tlc.sh --workers 4          # Use 4 worker threads
#   ./run_tlc.sh --depth 100          # Limit search depth
#   ./run_tlc.sh --dot FlurmFederation # Generate state graph DOT file
#   ./run_tlc.sh --help               # Show help
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLA2TOOLS="$SCRIPT_DIR/tla2tools.jar"
TLA2TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"
STATES_DIR="$SCRIPT_DIR/states"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default options
WORKERS="auto"
DEPTH=""
JSON_OUTPUT=false
GENERATE_DOT=false
QUIET=false
MEMORY="4g"

# All specs to check
ALL_SPECS=(
    "FlurmFederation"
    "FlurmAccounting"
    "FlurmMigration"
    "FlurmConsensus"
    "FlurmFailover"
    "FlurmJobLifecycle"
    "FlurmScheduler"
)

# Results tracking
declare -A RESULTS
declare -A STATES_FOUND
declare -A TIME_ELAPSED

show_help() {
    cat << EOF
TLC Model Checker Runner for FLURM TLA+ Specifications

Usage: $0 [OPTIONS] [SPEC_NAME]

Options:
  --help, -h          Show this help message
  --download          Download tla2tools.jar only
  --json              Output results in machine-readable JSON format
  --workers N         Number of worker threads (default: auto)
  --depth N           Maximum state graph depth to explore
  --memory SIZE       Java heap size (default: 4g)
  --dot               Generate state graph in DOT format
  --quiet, -q         Suppress verbose output
  --list              List all available specifications

Specifications:
  FlurmFederation     Federated job scheduling across clusters
  FlurmAccounting     Distributed TRES (resource) accounting
  FlurmMigration      SLURM to FLURM migration state machine
  FlurmConsensus      Raft-based consensus protocol
  FlurmFailover       Controller failover handling
  FlurmJobLifecycle   Job state machine transitions
  FlurmScheduler      Core scheduler resource allocation

Examples:
  $0                            # Run all specs
  $0 FlurmFederation            # Run specific spec
  $0 --workers 8 FlurmConsensus # Run with 8 workers
  $0 --json --quiet             # JSON output, minimal verbosity
  $0 --dot FlurmScheduler       # Generate state graph

Environment:
  JAVA_HOME           Java installation directory
  TLC_WORKERS         Default number of workers
  TLC_MEMORY          Default Java heap size

Exit codes:
  0  All checks passed
  1  One or more checks failed (violation found)
  2  Runtime error (Java not found, etc.)
  3  Configuration error (spec not found, etc.)
EOF
}

download_tla2tools() {
    echo -e "${BLUE}Downloading tla2tools.jar...${NC}"
    if command -v curl &> /dev/null; then
        curl -L -o "$TLA2TOOLS" "$TLA2TOOLS_URL"
    elif command -v wget &> /dev/null; then
        wget -O "$TLA2TOOLS" "$TLA2TOOLS_URL"
    else
        echo -e "${RED}Error: Neither curl nor wget found. Please install one of them.${NC}"
        exit 2
    fi
    echo -e "${GREEN}Downloaded tla2tools.jar ($(du -h "$TLA2TOOLS" | cut -f1))${NC}"
}

check_java() {
    if ! command -v java &> /dev/null; then
        echo -e "${RED}Error: Java is not installed or not in PATH${NC}"
        echo ""
        echo "Please install Java 11 or later:"
        echo "  macOS:   brew install openjdk@11"
        echo "  Ubuntu:  sudo apt install openjdk-11-jdk"
        echo "  Fedora:  sudo dnf install java-11-openjdk"
        echo ""
        echo "Or visit: https://www.java.com/download/"
        exit 2
    fi

    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [[ "$JAVA_VERSION" =~ ^[0-9]+$ ]] && [ "$JAVA_VERSION" -lt 11 ] 2>/dev/null; then
        echo -e "${YELLOW}Warning: Java version $JAVA_VERSION detected. Java 11+ recommended.${NC}"
    fi

    if [ "$QUIET" = false ]; then
        echo -e "${CYAN}Java version: $(java -version 2>&1 | head -n 1)${NC}"
    fi
}

run_tlc() {
    local spec="$1"
    local tla_file="$SCRIPT_DIR/${spec}.tla"
    local cfg_file="$SCRIPT_DIR/${spec}.cfg"
    local output_file="$STATES_DIR/${spec}_output.txt"
    local json_file="$STATES_DIR/${spec}_result.json"
    local dot_file="$STATES_DIR/${spec}_states.dot"

    if [ ! -f "$tla_file" ]; then
        echo -e "${RED}Error: $tla_file not found${NC}"
        RESULTS[$spec]="NOTFOUND"
        return 3
    fi

    if [ ! -f "$cfg_file" ]; then
        echo -e "${RED}Error: $cfg_file not found${NC}"
        RESULTS[$spec]="NOTFOUND"
        return 3
    fi

    if [ "$QUIET" = false ]; then
        echo -e "${BLUE}============================================================${NC}"
        echo -e "${BLUE}Running TLC on ${spec}${NC}"
        echo -e "${BLUE}============================================================${NC}"
        echo ""
    fi

    # Build TLC arguments
    local tlc_args=(
        -XX:+UseParallelGC
        -Xmx${MEMORY}
        -cp "$TLA2TOOLS"
        tlc2.TLC
        -config "$cfg_file"
        -workers "$WORKERS"
        -deadlock
    )

    # Add depth limit if specified
    if [ -n "$DEPTH" ]; then
        tlc_args+=(-depth "$DEPTH")
    fi

    # Add DOT output if requested
    if [ "$GENERATE_DOT" = true ]; then
        tlc_args+=(-dump dot "$dot_file")
    fi

    # Add the TLA file
    tlc_args+=("$tla_file")

    # Run TLC and capture output
    local start_time=$(date +%s)

    if [ "$QUIET" = true ]; then
        java "${tlc_args[@]}" > "$output_file" 2>&1 || true
    else
        java "${tlc_args[@]}" 2>&1 | tee "$output_file" || true
    fi

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    TIME_ELAPSED[$spec]=$elapsed

    # Parse results
    local status="UNKNOWN"
    local states=""
    local error_msg=""

    if grep -q "Model checking completed" "$output_file"; then
        status="PASS"
        states=$(grep "distinct states found" "$output_file" | tail -1 | grep -oE '[0-9,]+ distinct' | head -1)
    elif grep -q "Invariant.*is violated" "$output_file"; then
        status="FAIL"
        error_msg=$(grep -A 5 "Invariant.*is violated" "$output_file" | head -6)
    elif grep -q "Error:" "$output_file"; then
        status="ERROR"
        error_msg=$(grep "Error:" "$output_file" | head -1)
    elif grep -q "Temporal properties were violated" "$output_file"; then
        status="FAIL"
        error_msg="Temporal property violated"
    elif grep -q "Deadlock reached" "$output_file"; then
        status="DEADLOCK"
        error_msg="Deadlock state found"
    fi

    RESULTS[$spec]=$status
    STATES_FOUND[$spec]=$states

    # Generate JSON result if requested
    if [ "$JSON_OUTPUT" = true ]; then
        cat > "$json_file" << JSONEOF
{
  "spec": "$spec",
  "status": "$status",
  "states": "$states",
  "elapsed_seconds": $elapsed,
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "config": {
    "workers": "$WORKERS",
    "depth": "${DEPTH:-unlimited}",
    "memory": "$MEMORY"
  }
}
JSONEOF
    fi

    echo ""
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}TLC completed successfully for ${spec} ($states, ${elapsed}s)${NC}"
    elif [ "$status" = "FAIL" ]; then
        echo -e "${RED}TLC found violations in ${spec}${NC}"
        if [ -n "$error_msg" ]; then
            echo -e "${RED}$error_msg${NC}"
        fi
    elif [ "$status" = "ERROR" ]; then
        echo -e "${RED}TLC encountered an error in ${spec}: $error_msg${NC}"
    else
        echo -e "${YELLOW}TLC status unknown for ${spec}${NC}"
    fi

    return 0
}

print_summary() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}                    SUMMARY                                 ${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""

    local all_passed=true
    local pass_count=0
    local fail_count=0
    local error_count=0
    local skip_count=0

    for spec in "${ALL_SPECS[@]}"; do
        local status="${RESULTS[$spec]:-NOTRUN}"
        local states="${STATES_FOUND[$spec]:-}"
        local elapsed="${TIME_ELAPSED[$spec]:-0}"

        case $status in
            PASS)
                echo -e "${GREEN}[PASS]${NC} ${spec}: $states (${elapsed}s)"
                ((pass_count++))
                ;;
            FAIL)
                echo -e "${RED}[FAIL]${NC} ${spec}: Violations found (${elapsed}s)"
                all_passed=false
                ((fail_count++))
                ;;
            ERROR)
                echo -e "${RED}[ERROR]${NC} ${spec}: Runtime error (${elapsed}s)"
                all_passed=false
                ((error_count++))
                ;;
            DEADLOCK)
                echo -e "${RED}[DEADLOCK]${NC} ${spec}: Deadlock found (${elapsed}s)"
                all_passed=false
                ((fail_count++))
                ;;
            NOTFOUND)
                echo -e "${YELLOW}[NOTFOUND]${NC} ${spec}: Spec or config not found"
                ((skip_count++))
                ;;
            NOTRUN)
                echo -e "${YELLOW}[SKIP]${NC} ${spec}: Not run"
                ((skip_count++))
                ;;
            *)
                echo -e "${YELLOW}[????]${NC} ${spec}: Status unknown"
                ((skip_count++))
                ;;
        esac
    done

    echo ""
    echo -e "${CYAN}Total: $pass_count passed, $fail_count failed, $error_count errors, $skip_count skipped${NC}"
    echo ""

    if [ "$JSON_OUTPUT" = true ]; then
        # Generate combined JSON summary
        local summary_file="$STATES_DIR/summary.json"
        echo "{" > "$summary_file"
        echo '  "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",' >> "$summary_file"
        echo "  \"total_pass\": $pass_count," >> "$summary_file"
        echo "  \"total_fail\": $fail_count," >> "$summary_file"
        echo "  \"total_error\": $error_count," >> "$summary_file"
        echo "  \"total_skip\": $skip_count," >> "$summary_file"
        echo "  \"all_passed\": $all_passed," >> "$summary_file"
        echo '  "results": [' >> "$summary_file"

        local first=true
        for spec in "${ALL_SPECS[@]}"; do
            local result_file="$STATES_DIR/${spec}_result.json"
            if [ -f "$result_file" ]; then
                if [ "$first" = true ]; then
                    first=false
                else
                    echo "," >> "$summary_file"
                fi
                cat "$result_file" | sed 's/^/    /' >> "$summary_file"
            fi
        done

        echo "" >> "$summary_file"
        echo "  ]" >> "$summary_file"
        echo "}" >> "$summary_file"

        echo -e "${CYAN}JSON summary written to: $summary_file${NC}"
    fi

    if [ "$all_passed" = true ]; then
        echo -e "${GREEN}All specifications passed!${NC}"
        return 0
    else
        echo -e "${RED}Some specifications failed. See output files in states/ for details.${NC}"
        return 1
    fi
}

list_specs() {
    echo "Available TLA+ Specifications:"
    echo ""
    for spec in "${ALL_SPECS[@]}"; do
        local tla_file="$SCRIPT_DIR/${spec}.tla"
        local cfg_file="$SCRIPT_DIR/${spec}.cfg"
        local status=""

        if [ -f "$tla_file" ] && [ -f "$cfg_file" ]; then
            status="${GREEN}[OK]${NC}"
        elif [ -f "$tla_file" ]; then
            status="${YELLOW}[NO CFG]${NC}"
        else
            status="${RED}[MISSING]${NC}"
        fi

        echo -e "  $status $spec"
    done
}

# Parse command line arguments
SPECS_TO_RUN=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --download)
            download_tla2tools
            exit 0
            ;;
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --depth)
            DEPTH="$2"
            shift 2
            ;;
        --memory)
            MEMORY="$2"
            shift 2
            ;;
        --dot)
            GENERATE_DOT=true
            shift
            ;;
        --quiet|-q)
            QUIET=true
            shift
            ;;
        --list)
            list_specs
            exit 0
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 3
            ;;
        *)
            SPECS_TO_RUN+=("$1")
            shift
            ;;
    esac
done

# Apply environment variable defaults
WORKERS="${TLC_WORKERS:-$WORKERS}"
MEMORY="${TLC_MEMORY:-$MEMORY}"

# Main script
main() {
    # Check for tla2tools.jar
    if [ ! -f "$TLA2TOOLS" ]; then
        echo -e "${YELLOW}tla2tools.jar not found. Downloading...${NC}"
        download_tla2tools
    fi

    # Check Java installation
    check_java

    # Create states directory if it doesn't exist
    mkdir -p "$STATES_DIR"

    if [ "$QUIET" = false ]; then
        echo -e "${CYAN}Configuration: workers=$WORKERS, memory=$MEMORY${DEPTH:+, depth=$DEPTH}${NC}"
        echo ""
    fi

    # Determine which specs to run
    if [ ${#SPECS_TO_RUN[@]} -gt 0 ]; then
        # Run specified specs
        for spec in "${SPECS_TO_RUN[@]}"; do
            # Validate spec name
            local valid=false
            for valid_spec in "${ALL_SPECS[@]}"; do
                if [ "$spec" = "$valid_spec" ]; then
                    valid=true
                    break
                fi
            done

            if [ "$valid" = false ]; then
                echo -e "${RED}Unknown spec: $spec${NC}"
                echo "Use --list to see available specifications"
                exit 3
            fi

            run_tlc "$spec"
        done
    else
        # Run all specs
        for spec in "${ALL_SPECS[@]}"; do
            run_tlc "$spec"
            echo ""
        done
    fi

    print_summary
}

main "$@"
