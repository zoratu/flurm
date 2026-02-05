#!/bin/bash
#SBATCH --job-name=array_simple
#SBATCH --partition=debug
#SBATCH --array=1-10
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:02:00
#SBATCH --output=/tmp/array_%A_%a.out
#SBATCH --error=/tmp/array_%A_%a.err

echo "=== FLURM Test Job: Simple Array ==="
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Simulate varying work per task
sleep_time=$((SLURM_ARRAY_TASK_ID % 5 + 1))
echo "Task $SLURM_ARRAY_TASK_ID sleeping for $sleep_time seconds..."
sleep $sleep_time

# Each task computes something different
result=$((SLURM_ARRAY_TASK_ID * SLURM_ARRAY_TASK_ID))
echo "Task $SLURM_ARRAY_TASK_ID: squared = $result"

echo "End time: $(date)"
echo "SUCCESS - Task $SLURM_ARRAY_TASK_ID"
