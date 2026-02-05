#!/bin/bash
#SBATCH --job-name=license_test
#SBATCH --partition=batch
#SBATCH --licenses=matlab:1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/license_test_%j.out
#SBATCH --error=/tmp/license_test_%j.err

echo "=== FLURM Test Job: License Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Check license environment
echo ""
echo "License Environment:"
echo "SLURM_JOB_LICENSES=$SLURM_JOB_LICENSES"

# Simulate license usage
echo ""
echo "Holding MATLAB license for 30 seconds..."
echo "In a real scenario, this would launch MATLAB or another licensed application."

sleep 30

echo ""
echo "Releasing license..."
echo "End time: $(date)"
echo "SUCCESS - License test completed"
