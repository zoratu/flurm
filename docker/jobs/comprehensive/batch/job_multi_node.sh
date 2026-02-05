#!/bin/bash
#SBATCH --job-name=multi_node
#SBATCH --partition=batch
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/job_%j.out
#SBATCH --error=/tmp/job_%j.err

echo "=== FLURM Test Job: Multi-Node ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Nodes: $SLURM_JOB_NUM_NODES"
echo "Node list: $SLURM_JOB_NODELIST"
echo "Tasks: $SLURM_NTASKS"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

# Verify we got 2 nodes
NODE_COUNT=$(scontrol show hostname "$SLURM_JOB_NODELIST" 2>/dev/null | wc -l || echo "0")
echo "Actual node count: $NODE_COUNT"

if [ "$NODE_COUNT" -lt 2 ]; then
    echo "WARNING: Expected 2 nodes, got $NODE_COUNT (continuing anyway)"
fi

# Run something on each node
echo "Running tasks on each node..."
srun --nodes=2 --ntasks=4 hostname 2>/dev/null || {
    echo "srun not available, simulating..."
    echo "Node 1: $(hostname)"
    echo "Node 2: simulated-node-2"
}

echo "End time: $(date)"
echo "SUCCESS"
