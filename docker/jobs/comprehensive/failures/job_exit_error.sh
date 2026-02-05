#!/bin/bash
#SBATCH --job-name=exit_error
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:01:00
#SBATCH --output=/tmp/exit_error_%j.out
#SBATCH --error=/tmp/exit_error_%j.err

# This job intentionally exits with error code 1

echo "=== FLURM Test Job: Exit Error Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

echo "This job will exit with error code 1..."
echo "Simulating application failure..."

# Do some work
sleep 2

# Exit with error
echo "FAILURE - Intentional exit code 1"
exit 1
