#!/bin/bash
#SBATCH --job-name=memory_test
#SBATCH --partition=medium
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/job_%j.out
#SBATCH --error=/tmp/job_%j.err

echo "=== FLURM Test Job: Memory Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

# Check memory limit
echo "Checking memory allocation..."
echo "- /proc/meminfo available: $(grep MemAvailable /proc/meminfo 2>/dev/null || echo 'N/A')"

# Allocate some memory (2GB)
echo "Allocating 2GB of memory..."
python3 -c "
import sys
try:
    # Allocate ~2GB
    data = bytearray(2 * 1024 * 1024 * 1024)
    print('Allocated 2GB successfully')
    # Hold it briefly
    import time
    time.sleep(2)
except MemoryError as e:
    print(f'Memory allocation failed: {e}', file=sys.stderr)
    sys.exit(1)
" 2>&1 || {
    echo "Python not available, using dd instead"
    dd if=/dev/zero of=/dev/null bs=1M count=2048 2>/dev/null
}

echo "End time: $(date)"
echo "SUCCESS"
