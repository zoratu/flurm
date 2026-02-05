#!/bin/bash
#SBATCH --job-name=cpu_medium
#SBATCH --partition=batch
#SBATCH --cpus-per-task=4
#SBATCH --mem=2G
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/job_%j.out
#SBATCH --error=/tmp/job_%j.err

echo "=== FLURM Test Job: CPU Medium ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

# Verify resources
if [ "$SLURM_CPUS_PER_TASK" -lt 4 ]; then
    echo "ERROR: Expected 4 CPUs, got $SLURM_CPUS_PER_TASK" >&2
    exit 1
fi

# Simulate medium compute workload
echo "Starting computation..."
for i in $(seq 1 4); do
    (
        # Each "thread" computes primes
        count=$(seq 2 10000 | factor | grep -c "^[0-9]*: [0-9]*$")
        echo "Thread $i: Found $count primes"
    ) &
done
wait

echo "End time: $(date)"
echo "SUCCESS"
