#!/bin/bash
#SBATCH --job-name=array_throttled
#SBATCH --partition=debug
#SBATCH --array=1-50%5
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:10:00
#SBATCH --output=/tmp/array_throttle_%A_%a.out
#SBATCH --error=/tmp/array_throttle_%A_%a.err

# Note: %5 means max 5 tasks run concurrently

echo "=== FLURM Test Job: Throttled Array ==="
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Check which tasks are running concurrently
echo "My task: $SLURM_ARRAY_TASK_ID"

# Simulate work that takes 5-10 seconds
sleep_time=$((SLURM_ARRAY_TASK_ID % 6 + 5))
echo "Task $SLURM_ARRAY_TASK_ID working for $sleep_time seconds..."
sleep $sleep_time

# Each task produces a result
result=$((SLURM_ARRAY_TASK_ID * 2 + 100))
echo "Task $SLURM_ARRAY_TASK_ID: result = $result"

echo "End time: $(date)"
echo "SUCCESS - Task $SLURM_ARRAY_TASK_ID (throttled array)"
