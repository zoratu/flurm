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
#   ./run_tlc.sh                    # Run all specs
#   ./run_tlc.sh FlurmFederation    # Run specific spec
#   ./run_tlc.sh --download         # Download tla2tools.jar only
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLA2TOOLS="$SCRIPT_DIR/tla2tools.jar"
TLA2TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Specs to check
SPECS=("FlurmFederation" "FlurmAccounting" "FlurmMigration")

download_tla2tools() {
    echo -e "${BLUE}Downloading tla2tools.jar...${NC}"
    if command -v curl &> /dev/null; then
        curl -L -o "$TLA2TOOLS" "$TLA2TOOLS_URL"
    elif command -v wget &> /dev/null; then
        wget -O "$TLA2TOOLS" "$TLA2TOOLS_URL"
    else
        echo -e "${RED}Error: Neither curl nor wget found. Please install one of them.${NC}"
        exit 1
    fi
    echo -e "${GREEN}Downloaded tla2tools.jar${NC}"
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
        exit 1
    fi

    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -lt 11 ] 2>/dev/null; then
        echo -e "${YELLOW}Warning: Java version $JAVA_VERSION detected. Java 11+ recommended.${NC}"
    fi
}

run_tlc() {
    local spec="$1"
    local tla_file="$SCRIPT_DIR/${spec}.tla"
    local cfg_file="$SCRIPT_DIR/${spec}.cfg"

    if [ ! -f "$tla_file" ]; then
        echo -e "${RED}Error: $tla_file not found${NC}"
        return 1
    fi

    if [ ! -f "$cfg_file" ]; then
        echo -e "${RED}Error: $cfg_file not found${NC}"
        return 1
    fi

    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}Running TLC on ${spec}${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""

    # Run TLC with appropriate settings
    # -workers auto: Use all available CPU cores
    # -deadlock: Check for deadlocks
    # -cleanup: Clean up temporary files
    java -XX:+UseParallelGC \
         -Xmx4g \
         -cp "$TLA2TOOLS" \
         tlc2.TLC \
         -config "$cfg_file" \
         -workers auto \
         -deadlock \
         "$tla_file" 2>&1 | tee "$SCRIPT_DIR/states/${spec}_output.txt"

    local status=$?

    echo ""
    if [ $status -eq 0 ]; then
        echo -e "${GREEN}TLC completed successfully for ${spec}${NC}"
    else
        echo -e "${RED}TLC found violations or errors in ${spec}${NC}"
    fi

    return $status
}

print_summary() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}                    SUMMARY                                 ${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""

    local all_passed=true

    for spec in "${SPECS[@]}"; do
        local output_file="$SCRIPT_DIR/states/${spec}_output.txt"
        if [ -f "$output_file" ]; then
            if grep -q "Model checking completed" "$output_file"; then
                local states=$(grep "distinct states found" "$output_file" | tail -1)
                echo -e "${GREEN}[PASS]${NC} ${spec}: $states"
            elif grep -q "Error:" "$output_file" || grep -q "Invariant.*is violated" "$output_file"; then
                echo -e "${RED}[FAIL]${NC} ${spec}: Violations found"
                all_passed=false
            else
                echo -e "${YELLOW}[????]${NC} ${spec}: Status unknown"
            fi
        else
            echo -e "${YELLOW}[SKIP]${NC} ${spec}: Not run"
        fi
    done

    echo ""
    if [ "$all_passed" = true ]; then
        echo -e "${GREEN}All specifications passed!${NC}"
        return 0
    else
        echo -e "${RED}Some specifications failed. See output files in states/ for details.${NC}"
        return 1
    fi
}

# Main script
main() {
    # Handle --download flag
    if [ "$1" = "--download" ]; then
        download_tla2tools
        exit 0
    fi

    # Check for tla2tools.jar
    if [ ! -f "$TLA2TOOLS" ]; then
        echo -e "${YELLOW}tla2tools.jar not found. Downloading...${NC}"
        download_tla2tools
    fi

    # Check Java installation
    check_java

    # Create states directory if it doesn't exist
    mkdir -p "$SCRIPT_DIR/states"

    # Run specific spec or all specs
    if [ -n "$1" ]; then
        run_tlc "$1"
        exit $?
    else
        local failed=0
        for spec in "${SPECS[@]}"; do
            if ! run_tlc "$spec"; then
                failed=$((failed + 1))
            fi
            echo ""
        done

        print_summary

        if [ $failed -gt 0 ]; then
            exit 1
        fi
    fi
}

main "$@"
