#!/bin/bash
#SBATCH --job-name=exclusive_test
#SBATCH --partition=batch
#SBATCH --exclusive
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/exclusive_%j.out
#SBATCH --error=/tmp/exclusive_%j.err

echo "=== FLURM Test Job: Exclusive Node Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

echo ""
echo "This job has exclusive access to the node."
echo "No other jobs should be sharing these resources."
echo ""

# Show full node resources
echo "Node Resources:"
echo "  CPUs on node: $SLURM_CPUS_ON_NODE"
echo "  Memory: $SLURM_MEM_PER_NODE MB"
echo "  Job CPUs: $SLURM_JOB_CPUS_PER_NODE"
echo ""

# Verify we have the whole node
TOTAL_CPUS=$(nproc 2>/dev/null || echo "unknown")
echo "Total CPUs visible: $TOTAL_CPUS"

# Check no other jobs on this node (would show in ps)
echo ""
echo "Process count on node:"
ps aux | wc -l

sleep 30

echo ""
echo "End time: $(date)"
echo "SUCCESS - Exclusive test completed"
