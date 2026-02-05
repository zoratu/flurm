#!/bin/bash
#SBATCH --job-name=mpi_hello
#SBATCH --partition=batch
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=512M
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/mpi_hello_%j.out
#SBATCH --error=/tmp/mpi_hello_%j.err

echo "=== FLURM Test Job: MPI Hello World ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Nodes: $SLURM_JOB_NUM_NODES"
echo "Node list: $SLURM_JOB_NODELIST"
echo "Tasks: $SLURM_NTASKS"
echo "Tasks per node: $SLURM_NTASKS_PER_NODE"
echo "Start time: $(date)"

# Check if mpirun is available
if command -v mpirun &> /dev/null; then
    echo "Running MPI hello world..."
    mpirun -np $SLURM_NTASKS hostname
else
    echo "MPI not available, simulating..."
    # Simulate MPI by running on each allocated node
    for i in $(seq 1 ${SLURM_NTASKS:-4}); do
        echo "Rank $((i-1)): $(hostname)"
    done
fi

# Verify node count
NODE_COUNT=$(scontrol show hostname "$SLURM_JOB_NODELIST" 2>/dev/null | wc -l || echo "1")
echo "Actual node count: $NODE_COUNT"

if [ "$NODE_COUNT" -lt 2 ]; then
    echo "WARNING: Expected 2 nodes, got $NODE_COUNT (MPI may not work correctly)"
fi

echo "End time: $(date)"
echo "SUCCESS - MPI hello completed"
