#!/bin/bash
#SBATCH --job-name=cpu_large
#SBATCH --partition=large
#SBATCH --cpus-per-task=8
#SBATCH --mem=8G
#SBATCH --time=00:10:00
#SBATCH --output=/tmp/job_%j.out
#SBATCH --error=/tmp/job_%j.err

echo "=== FLURM Test Job: CPU Large ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

# Verify resources
if [ "$SLURM_CPUS_PER_TASK" -lt 8 ]; then
    echo "ERROR: Expected 8 CPUs, got $SLURM_CPUS_PER_TASK" >&2
    exit 1
fi

# Simulate large compute workload
echo "Starting large computation..."
for i in $(seq 1 8); do
    (
        count=$(seq 2 50000 | factor | grep -c "^[0-9]*: [0-9]*$")
        echo "Thread $i: Found $count primes"
    ) &
done
wait

echo "End time: $(date)"
echo "SUCCESS"
