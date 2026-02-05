#!/bin/bash
#SBATCH --job-name=cpu_small
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M
#SBATCH --time=00:01:00
#SBATCH --output=/tmp/job_%j.out
#SBATCH --error=/tmp/job_%j.err

echo "=== FLURM Test Job: CPU Small ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

# Verify resources
if [ "$SLURM_CPUS_PER_TASK" -ne 1 ]; then
    echo "ERROR: Expected 1 CPU, got $SLURM_CPUS_PER_TASK" >&2
    exit 1
fi

# Simple compute (prime sieve)
echo "Running prime sieve..."
count=$(seq 2 1000 | factor | grep -c "^[0-9]*: [0-9]*$")
echo "Found $count primes under 1000"

echo "End time: $(date)"
echo "SUCCESS"
