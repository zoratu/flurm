#!/bin/bash
#SBATCH --job-name=gpu_test
#SBATCH --partition=gpu
#SBATCH --gres=gpu:2
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=00:10:00
#SBATCH --output=/tmp/gpu_test_%j.out
#SBATCH --error=/tmp/gpu_test_%j.err

echo "=== FLURM Test Job: GPU Resource Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Check GPU environment variables
echo ""
echo "GPU Environment:"
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
echo "SLURM_JOB_GPUS=$SLURM_JOB_GPUS"
echo "SLURM_GPUS_ON_NODE=$SLURM_GPUS_ON_NODE"

# Verify we got 2 GPUs
if [ -n "$CUDA_VISIBLE_DEVICES" ]; then
    GPU_COUNT=$(echo "$CUDA_VISIBLE_DEVICES" | tr ',' '\n' | wc -l)
    echo "GPU count from CUDA_VISIBLE_DEVICES: $GPU_COUNT"

    if [ "$GPU_COUNT" -ne 2 ]; then
        echo "WARNING: Expected 2 GPUs, got $GPU_COUNT"
    fi
elif [ -n "$SLURM_GPUS_ON_NODE" ]; then
    echo "GPU count from SLURM_GPUS_ON_NODE: $SLURM_GPUS_ON_NODE"
else
    echo "Note: No GPU environment variables set (GPU scheduling may be simulated)"
fi

# Try nvidia-smi if available
if command -v nvidia-smi &> /dev/null; then
    echo ""
    echo "nvidia-smi output:"
    nvidia-smi
else
    echo "nvidia-smi not available (simulated GPU test)"
fi

echo ""
echo "End time: $(date)"
echo "SUCCESS - GPU test completed"
