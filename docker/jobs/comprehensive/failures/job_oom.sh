#!/bin/bash
#SBATCH --job-name=oom_test
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:02:00
#SBATCH --output=/tmp/oom_%j.out
#SBATCH --error=/tmp/oom_%j.err

# This job attempts to exceed its memory limit

echo "=== FLURM Test Job: OOM Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Memory limit: 256M"
echo "Start time: $(date)"

echo "This job will try to allocate 1GB with only 256M allowed..."

# Try to allocate more memory than allowed
python3 -c "
import sys
try:
    # Try to allocate 1GB (should fail with 256M limit)
    data = bytearray(1024 * 1024 * 1024)
    print('ERROR: Allocation succeeded - memory limit not enforced')
except MemoryError as e:
    print(f'Memory allocation failed as expected: {e}')
    sys.exit(137)  # Typical OOM exit code
" 2>&1 || {
    echo "Python not available or allocation failed"

    # Alternative: try to use shell
    echo "Attempting shell-based memory allocation..."
    # This may or may not trigger OOM depending on cgroup configuration
    dd if=/dev/zero of=/dev/null bs=1G count=1 2>&1 || true
    echo "Shell allocation completed"
}

echo "End time: $(date)"
echo "Note: Actual OOM behavior depends on cgroup memory limits"
