#!/bin/bash
#SBATCH --job-name=timeout_test
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:00:30
#SBATCH --output=/tmp/timeout_%j.out
#SBATCH --error=/tmp/timeout_%j.err

# This job intentionally exceeds its time limit

echo "=== FLURM Test Job: Timeout Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Time limit: 30 seconds"
echo "Start time: $(date)"

# Set up signal handler for SIGTERM (sent before SIGKILL)
trap 'echo "Received SIGTERM - time limit approaching!"; exit 143' SIGTERM

echo "This job will run for 120 seconds but has a 30 second limit..."
echo "It should be killed due to timeout."

# Run longer than the time limit
for i in $(seq 1 120); do
    echo "Running for $i seconds..."
    sleep 1
done

# Should never reach here
echo "ERROR: Job should have been killed by now!"
echo "UNEXPECTED SUCCESS - This should not appear"
