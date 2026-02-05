#!/bin/bash
#SBATCH --job-name=retry_test
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:02:00
#SBATCH --requeue
#SBATCH --output=/tmp/retry_%j.out
#SBATCH --error=/tmp/retry_%j.err

# This job fails twice, then succeeds on the third attempt

echo "=== FLURM Test Job: Retry Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Track attempts using a counter file
COUNTER_FILE="/tmp/retry_counter_${SLURM_JOB_NAME}_${SLURM_JOB_ID%%_*}"

# Read current attempt
if [ -f "$COUNTER_FILE" ]; then
    ATTEMPT=$(cat "$COUNTER_FILE")
    ATTEMPT=$((ATTEMPT + 1))
else
    ATTEMPT=1
fi

# Save new attempt count
echo $ATTEMPT > "$COUNTER_FILE"

echo "This is attempt #$ATTEMPT"

# Fail on first 2 attempts
if [ $ATTEMPT -lt 3 ]; then
    echo "Simulating failure on attempt $ATTEMPT..."
    echo "Job will be requeued and retried."
    echo "FAILURE - Attempt $ATTEMPT (intentional)"

    # Don't clean up counter file - need it for next attempt
    exit 1
fi

# Success on 3rd attempt
echo "Attempt $ATTEMPT succeeds!"
rm -f "$COUNTER_FILE"

echo "End time: $(date)"
echo "SUCCESS - Attempt $ATTEMPT (after 2 failures)"
