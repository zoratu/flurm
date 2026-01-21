#!/bin/bash
# Run all demo jobs to showcase FLURM capabilities

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=============================================="
echo "     FLURM Demo Jobs - Full Capability Test"
echo "=============================================="
echo ""
echo "This script submits various jobs to demonstrate:"
echo "  1. Data processing with output files"
echo "  2. Intentional failures and error handling"
echo "  3. Job arrays with parallel tasks"
echo "  4. Dependency chains (pipeline)"
echo "  5. Retry behavior for transient failures"
echo "  6. Resource-intensive workloads"
echo "  7. Timeout handling with checkpoints"
echo ""
read -p "Press Enter to start demo..."

# Ensure job scripts exist
if [ ! -f "${SCRIPT_DIR}/demo_data_processing.sh" ]; then
    echo "ERROR: Demo scripts not found in ${SCRIPT_DIR}"
    exit 1
fi

echo ""
echo "=== Demo 1: Data Processing Job ==="
JOB_DATA=$(sbatch --parsable "${SCRIPT_DIR}/demo_data_processing.sh")
echo "Submitted job $JOB_DATA"
echo ""

echo "=== Demo 2: Failing Job (for error handling) ==="
JOB_FAIL=$(sbatch --parsable "${SCRIPT_DIR}/demo_failing_job.sh")
echo "Submitted job $JOB_FAIL (will fail intentionally)"
echo ""

echo "=== Demo 3: Job Array (10 tasks, 3 concurrent) ==="
JOB_ARRAY=$(sbatch --parsable "${SCRIPT_DIR}/demo_job_array.sh")
echo "Submitted array job $JOB_ARRAY"
echo ""

echo "=== Demo 4: Dependency Chain Pipeline ==="
# First generate the job scripts
"${SCRIPT_DIR}/demo_dependency_chain.sh"
# Then submit them
echo "Submitting 4-stage pipeline..."
JOB_PIPE1=$(sbatch --parsable /tmp/job1_generate.sh)
JOB_PIPE2=$(sbatch --parsable --dependency=afterok:${JOB_PIPE1} /tmp/job2_validate.sh)
JOB_PIPE3=$(sbatch --parsable --dependency=afterok:${JOB_PIPE2} /tmp/job3_process.sh)
JOB_PIPE4=$(sbatch --parsable --dependency=afterok:${JOB_PIPE3} /tmp/job4_report.sh)
echo "Pipeline: $JOB_PIPE1 -> $JOB_PIPE2 -> $JOB_PIPE3 -> $JOB_PIPE4"
echo ""

echo "=== Demo 5: Retry Job (fails twice, succeeds third) ==="
JOB_RETRY=$(sbatch --parsable "${SCRIPT_DIR}/demo_retry_job.sh")
echo "Submitted job $JOB_RETRY (will retry on failure)"
echo ""

echo "=== Demo 6: Resource Intensive Job ==="
JOB_RESOURCE=$(sbatch --parsable "${SCRIPT_DIR}/demo_resource_intensive.sh")
echo "Submitted job $JOB_RESOURCE"
echo ""

echo "=== Demo 7: Timeout Job (30s limit, 60s work) ==="
JOB_TIMEOUT=$(sbatch --parsable "${SCRIPT_DIR}/demo_timeout_job.sh")
echo "Submitted job $JOB_TIMEOUT (will checkpoint on timeout)"
echo ""

echo "=============================================="
echo "              All Jobs Submitted"
echo "=============================================="
echo ""
echo "Job Summary:"
echo "  Data Processing: $JOB_DATA"
echo "  Failing Job:     $JOB_FAIL"
echo "  Job Array:       $JOB_ARRAY"
echo "  Pipeline:        $JOB_PIPE1 -> $JOB_PIPE2 -> $JOB_PIPE3 -> $JOB_PIPE4"
echo "  Retry Demo:      $JOB_RETRY"
echo "  Resource Test:   $JOB_RESOURCE"
echo "  Timeout Demo:    $JOB_TIMEOUT"
echo ""
echo "Commands to monitor:"
echo "  squeue                    # View job queue"
echo "  squeue -u \$USER          # View your jobs"
echo "  sacct -j <jobid>         # View job accounting"
echo "  scontrol show job <id>   # Detailed job info"
echo "  tail -f /tmp/*.out       # Watch job output"
echo ""
