#!/bin/bash
#SBATCH --job-name=test_long
#SBATCH --output=/shared/jobs/test_long_%j.out
#SBATCH --error=/shared/jobs/test_long_%j.err
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=debug

echo "=== Long Test Job ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Node: $(hostname)"
echo "Start Time: $(date)"

# Run for 10 minutes with periodic updates
for i in $(seq 1 20); do
    echo "Progress: $i/20 at $(date)"
    sleep 30
done

echo "End Time: $(date)"
echo "=== Job Complete ==="
