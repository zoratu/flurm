#!/bin/bash
#SBATCH --job-name=array_mixed
#SBATCH --partition=debug
#SBATCH --array=1-10
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:02:00
#SBATCH --output=/tmp/array_mixed_%A_%a.out
#SBATCH --error=/tmp/array_mixed_%A_%a.err

echo "=== FLURM Test Job: Mixed Success/Failure Array ==="
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Tasks 3 and 7 will fail (for testing partial failure handling)
if [ "$SLURM_ARRAY_TASK_ID" -eq 3 ] || [ "$SLURM_ARRAY_TASK_ID" -eq 7 ]; then
    echo "Task $SLURM_ARRAY_TASK_ID intentionally failing..."
    echo "FAILURE - Task $SLURM_ARRAY_TASK_ID (expected)"
    exit 1
fi

# Other tasks succeed
sleep 2
echo "Task $SLURM_ARRAY_TASK_ID completed successfully"

echo "End time: $(date)"
echo "SUCCESS - Task $SLURM_ARRAY_TASK_ID"
