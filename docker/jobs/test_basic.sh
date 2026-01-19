#!/bin/bash
#SBATCH --job-name=test_basic
#SBATCH --output=/tmp/test_basic_%j.out
#SBATCH --error=/tmp/test_basic_%j.err
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

# Basic test job for SLURM client compatibility testing
# This job is designed to be quick and verifiable

echo "=== FLURM Basic Test Job ==="
echo "Job ID: ${SLURM_JOB_ID:-unknown}"
echo "Job Name: ${SLURM_JOB_NAME:-unknown}"
echo "Node: $(hostname)"
echo "User: $(whoami)"
echo "Working Directory: $(pwd)"
echo "Start Time: $(date -Iseconds)"

# Simple computation to verify job actually ran
echo ""
echo "Performing test computation..."
RESULT=$((42 * 2))
echo "42 * 2 = $RESULT"

# Environment check
echo ""
echo "SLURM Environment Variables:"
env | grep "^SLURM" | sort || echo "(none set)"

# Short sleep to allow job to be observed in queue
sleep 5

echo ""
echo "End Time: $(date -Iseconds)"
echo "Exit Status: 0"
echo "=== Job Complete ==="

exit 0
