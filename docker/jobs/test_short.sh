#!/bin/bash
#SBATCH --job-name=test_short
#SBATCH --output=/shared/jobs/test_short_%j.out
#SBATCH --error=/shared/jobs/test_short_%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "=== Short Test Job ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Node: $(hostname)"
echo "Start Time: $(date)"

sleep 30

echo "End Time: $(date)"
echo "=== Job Complete ==="
