#!/bin/bash
#SBATCH --job-name=timeout_demo
#SBATCH --output=/tmp/timeout_%j.out
#SBATCH --error=/tmp/timeout_%j.err
#SBATCH --time=00:00:30
#SBATCH --nodes=1
#SBATCH --signal=B:USR1@10

# Job that demonstrates timeout handling
# Time limit is 30 seconds, but work takes 60 seconds
# Receives SIGUSR1 10 seconds before timeout

# Signal handler for graceful shutdown
cleanup() {
    echo ""
    echo ">>> RECEIVED TIMEOUT WARNING (SIGUSR1) <<<"
    echo ">>> 10 seconds until job termination <<<"
    echo ""
    echo "Saving checkpoint..."

    # Save current progress
    cat > /tmp/timeout_checkpoint_${SLURM_JOB_ID}.json << EOF
{
  "job_id": "${SLURM_JOB_ID}",
  "status": "checkpointed",
  "progress": ${PROGRESS:-0},
  "total": 60,
  "checkpoint_time": "$(date -Iseconds)",
  "can_resume": true
}
EOF

    echo "Checkpoint saved to /tmp/timeout_checkpoint_${SLURM_JOB_ID}.json"
    cat /tmp/timeout_checkpoint_${SLURM_JOB_ID}.json
    echo ""
    echo "Graceful shutdown complete"
    exit 0
}

trap cleanup SIGUSR1

echo "=== Timeout Demo Job ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Time Limit: 30 seconds"
echo "Work Duration: 60 seconds (will timeout)"
echo "Signal: SIGUSR1 at 10 seconds before timeout"
echo "Start: $(date -Iseconds)"
echo ""

# Check for checkpoint to resume from
CHECKPOINT="/tmp/timeout_checkpoint_${SLURM_JOB_ID}.json"
if [ -f "$CHECKPOINT" ]; then
    echo "Found checkpoint, resuming..."
    PROGRESS=$(grep -o '"progress": [0-9]*' "$CHECKPOINT" | grep -o '[0-9]*')
    echo "Resuming from progress: ${PROGRESS}"
else
    PROGRESS=0
fi

echo ""
echo "Starting work (this will take 60 seconds)..."
echo ""

# Simulate long-running work
for i in $(seq $((PROGRESS + 1)) 60); do
    PROGRESS=$i
    printf "\rProgress: %d/60 seconds" $i
    sleep 1
done

echo ""
echo ""
echo "Work completed!"
echo ""
echo "End: $(date -Iseconds)"
echo "=== Timeout Demo Complete ==="
exit 0
