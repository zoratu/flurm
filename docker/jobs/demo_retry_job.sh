#!/bin/bash
#SBATCH --job-name=retry_demo
#SBATCH --output=/tmp/retry_%j_%x.out
#SBATCH --error=/tmp/retry_%j_%x.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --requeue

# Job that fails intermittently to demonstrate retry behavior
# Uses a state file to track attempts

echo "=== Retry Demo Job ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Job Name: ${SLURM_JOB_NAME}"
echo "Restart Count: ${SLURM_RESTART_COUNT:-0}"
echo "Node: $(hostname)"
echo "Start: $(date -Iseconds)"
echo ""

# Track attempts via state file
STATE_FILE="/tmp/retry_state_${SLURM_JOB_ID}.txt"
ATTEMPT=$((${SLURM_RESTART_COUNT:-0} + 1))

echo "Attempt: ${ATTEMPT}"
echo "${ATTEMPT}" >> "${STATE_FILE}"

# Fail on first 2 attempts, succeed on 3rd
if [ ${ATTEMPT} -lt 3 ]; then
    echo ""
    echo "Simulating transient failure..."
    sleep 2

    case ${ATTEMPT} in
        1)
            echo "ERROR: Network timeout connecting to resource server"
            echo "ERROR: Will retry after backoff..."
            ;;
        2)
            echo "ERROR: Resource temporarily unavailable"
            echo "ERROR: Service returned 503, will retry..."
            ;;
    esac

    echo ""
    echo "End: $(date -Iseconds)"
    echo "=== Job Failed (attempt ${ATTEMPT}/3) - Will Retry ==="
    exit 1
else
    echo ""
    echo "All transient issues resolved!"
    echo "Proceeding with actual work..."
    echo ""

    # Do actual work
    echo "Step 1: Connecting to resource... OK"
    sleep 1
    echo "Step 2: Fetching data... OK"
    sleep 1
    echo "Step 3: Processing... OK"
    sleep 1
    echo "Step 4: Writing results... OK"

    # Write output
    cat > "/tmp/retry_result_${SLURM_JOB_ID}.json" << EOF
{
  "job_id": "${SLURM_JOB_ID}",
  "status": "success",
  "attempts": ${ATTEMPT},
  "previous_failures": $((ATTEMPT - 1)),
  "completed_at": "$(date -Iseconds)"
}
EOF

    echo ""
    echo "Result written to /tmp/retry_result_${SLURM_JOB_ID}.json"
    cat "/tmp/retry_result_${SLURM_JOB_ID}.json"

    # Cleanup state file
    rm -f "${STATE_FILE}"

    echo ""
    echo "End: $(date -Iseconds)"
    echo "=== Job Succeeded (after ${ATTEMPT} attempts) ==="
    exit 0
fi
