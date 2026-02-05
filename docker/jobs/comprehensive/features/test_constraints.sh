#!/bin/bash
#SBATCH --job-name=constraint_test
#SBATCH --partition=batch
#SBATCH --constraint=avx2
#SBATCH --cpus-per-task=2
#SBATCH --mem=1G
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/constraint_%j.out
#SBATCH --error=/tmp/constraint_%j.err

echo "=== FLURM Test Job: Constraint Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

echo ""
echo "This job requested constraint: avx2"
echo "It should only run on nodes with AVX2 support."
echo ""

# Check CPU features
echo "CPU Features:"
if [ -f /proc/cpuinfo ]; then
    grep -m1 "flags" /proc/cpuinfo | tr ' ' '\n' | grep -E "^(avx|sse|fma)" | sort | head -10
else
    echo "Cannot read /proc/cpuinfo"
fi

# Verify AVX2 specifically
if grep -q "avx2" /proc/cpuinfo 2>/dev/null; then
    echo ""
    echo "AVX2 support: CONFIRMED"
else
    echo ""
    echo "AVX2 support: NOT DETECTED (constraint may be ignored in test env)"
fi

sleep 10

echo ""
echo "End time: $(date)"
echo "SUCCESS - Constraint test completed"
