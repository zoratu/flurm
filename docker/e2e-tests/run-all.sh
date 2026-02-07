#!/bin/bash
# Master E2E Test Runner for FLURM
# Runs all E2E test scenarios and reports results

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="/results/$(date +%Y%m%d_%H%M%S)"
PASSED=0
FAILED=0
SKIPPED=0

mkdir -p "$RESULTS_DIR"

echo "======================================"
echo "FLURM E2E Test Suite"
echo "======================================"
echo "Results directory: $RESULTS_DIR"
echo "Started: $(date)"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

run_test() {
    local name="$1"
    local script="$2"
    local log_file="$RESULTS_DIR/${name}.log"

    echo -n "Running $name... "

    if [ ! -x "$script" ]; then
        echo -e "${YELLOW}SKIPPED${NC} (script not found or not executable)"
        SKIPPED=$((SKIPPED + 1))
        return
    fi

    if timeout 300 "$script" > "$log_file" 2>&1; then
        echo -e "${GREEN}PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAILED${NC} (see $log_file)"
        FAILED=$((FAILED + 1))
        # Show last 10 lines of log on failure
        echo "  Last 10 lines of log:"
        tail -10 "$log_file" | sed 's/^/    /'
    fi
}

# Wait for cluster to be ready
echo "Waiting for FLURM cluster to be ready..."
for i in $(seq 1 60); do
    if curl -s "http://${FLURM_CTRL_1}:9090/metrics" > /dev/null 2>&1; then
        echo "Cluster is ready!"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: Cluster not ready after 60 seconds"
        exit 1
    fi
    sleep 1
done

echo ""
echo "======================================"
echo "Running Test Scenarios"
echo "======================================"

# Add delay between tests to avoid hitting per-peer connection limits
DELAY_BETWEEN_TESTS=5

# Job Lifecycle Tests
run_test "job_lifecycle_basic" "$SCRIPT_DIR/test-job-lifecycle.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "job_lifecycle_constraints" "$SCRIPT_DIR/test-job-constraints.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "job_arrays" "$SCRIPT_DIR/test-job-arrays.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "job_dependencies" "$SCRIPT_DIR/test-job-dependencies.sh"
sleep $DELAY_BETWEEN_TESTS

# Failure Tests
run_test "job_failures" "$SCRIPT_DIR/test-job-failures.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "cascading_failures" "$SCRIPT_DIR/test-cascading-failures.sh"
sleep $DELAY_BETWEEN_TESTS

# HA Tests
run_test "ha_failover" "$SCRIPT_DIR/test-ha-failover.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "accounting_consistency" "$SCRIPT_DIR/test-accounting-consistency.sh"
sleep $DELAY_BETWEEN_TESTS

# Migration Tests
run_test "migration_concurrent" "$SCRIPT_DIR/test-migration-concurrent.sh"
sleep $DELAY_BETWEEN_TESTS

# Federation Tests
run_test "federation_accounting" "$SCRIPT_DIR/test-federation-accounting.sh"
sleep $DELAY_BETWEEN_TESTS

# Feature Tests
run_test "munge_auth" "$SCRIPT_DIR/test-munge-auth.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "metrics_endpoint" "$SCRIPT_DIR/test-metrics.sh"
sleep $DELAY_BETWEEN_TESTS

# Job Execution and Output Tests
run_test "job_execution" "$SCRIPT_DIR/test-job-execution.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "node_registration" "$SCRIPT_DIR/test-node-registration.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "job_output" "$SCRIPT_DIR/test-job-output.sh"
sleep $DELAY_BETWEEN_TESTS

# Command and Protocol Tests
run_test "scontrol_commands" "$SCRIPT_DIR/test-scontrol-commands.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "sinfo_squeue" "$SCRIPT_DIR/test-sinfo-squeue.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "protocol_stress" "$SCRIPT_DIR/test-protocol-stress.sh"
sleep $DELAY_BETWEEN_TESTS

# MPI and Advanced Scheduling Tests
run_test "mpi_jobs" "$SCRIPT_DIR/test-mpi-jobs.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "resource_exhaustion" "$SCRIPT_DIR/test-resource-exhaustion.sh"
sleep $DELAY_BETWEEN_TESTS

# Stress and Edge Case Tests
run_test "edge_cases" "$SCRIPT_DIR/test-edge-cases.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "chaos_engineering" "$SCRIPT_DIR/test-chaos-engineering.sh"
sleep $DELAY_BETWEEN_TESTS

# HA and Infrastructure Tests (documentation/manual tests)
run_test "ha_failover_real" "$SCRIPT_DIR/test-ha-failover-real.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "network_partition" "$SCRIPT_DIR/test-network-partition.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "data_persistence" "$SCRIPT_DIR/test-data-persistence.sh"
sleep $DELAY_BETWEEN_TESTS

# Performance and Scale Tests
run_test "performance_benchmark" "$SCRIPT_DIR/test-performance-benchmark.sh"
sleep $DELAY_BETWEEN_TESTS
run_test "scale_simulation" "$SCRIPT_DIR/test-scale-simulation.sh"
sleep $DELAY_BETWEEN_TESTS

# Long Running Test (runs last, configurable duration)
run_test "long_running" "$SCRIPT_DIR/test-long-running.sh"

echo ""
echo "======================================"
echo "Test Results Summary"
echo "======================================"
echo -e "Passed:  ${GREEN}$PASSED${NC}"
echo -e "Failed:  ${RED}$FAILED${NC}"
echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"
echo ""
echo "Completed: $(date)"

# Exit with failure if any tests failed
if [ $FAILED -gt 0 ]; then
    exit 1
fi
exit 0
