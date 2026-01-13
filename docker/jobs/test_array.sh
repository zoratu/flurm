#!/bin/bash
#SBATCH --job-name=test_array
#SBATCH --output=/shared/jobs/test_array_%A_%a.out
#SBATCH --error=/shared/jobs/test_array_%A_%a.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --array=1-5

echo "=== Array Test Job ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Node: $(hostname)"
echo "Start Time: $(date)"

# Each array task sleeps for a different amount of time
SLEEP_TIME=$((SLURM_ARRAY_TASK_ID * 10))
echo "Sleeping for $SLEEP_TIME seconds..."
sleep $SLEEP_TIME

echo "End Time: $(date)"
echo "=== Task $SLURM_ARRAY_TASK_ID Complete ==="
