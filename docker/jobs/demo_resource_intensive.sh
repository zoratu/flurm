#!/bin/bash
#SBATCH --job-name=resource_hog
#SBATCH --output=/tmp/resource_%j.out
#SBATCH --error=/tmp/resource_%j.err
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=1G
#SBATCH --gres=gpu:1

# Resource-intensive job demonstrating GRES (GPU), memory, and CPU allocation

echo "=== Resource Intensive Job ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node: $(hostname)"
echo "Start: $(date -Iseconds)"
echo ""

echo "Allocated Resources:"
echo "  CPUs: ${SLURM_CPUS_PER_TASK:-unknown}"
echo "  Memory: ${SLURM_MEM_PER_NODE:-unknown}"
echo "  GPUs: ${SLURM_GPUS:-${CUDA_VISIBLE_DEVICES:-none}}"
echo "  Partition: ${SLURM_JOB_PARTITION:-default}"
echo ""

# CPU stress test
echo "=== CPU Utilization Test ==="
echo "Spawning ${SLURM_CPUS_PER_TASK:-4} parallel workers..."

cpu_worker() {
    local id=$1
    local iterations=1000000
    echo "  Worker $id starting ($iterations iterations)"

    sum=0
    for i in $(seq 1 $iterations); do
        sum=$((sum + i % 1000))
    done

    echo "  Worker $id complete (checksum: $sum)"
}

# Run CPU workers in parallel
for cpu in $(seq 1 ${SLURM_CPUS_PER_TASK:-4}); do
    cpu_worker $cpu &
done
wait
echo "All CPU workers complete"
echo ""

# Memory allocation test
echo "=== Memory Allocation Test ==="
MEM_TARGET_MB=512  # Allocate 512MB

echo "Allocating ${MEM_TARGET_MB}MB of memory..."
# Use dd to allocate memory
dd if=/dev/zero of=/tmp/memtest_${SLURM_JOB_ID}.dat bs=1M count=${MEM_TARGET_MB} 2>/dev/null
echo "Memory block allocated"

# Verify allocation
ACTUAL_SIZE=$(du -m /tmp/memtest_${SLURM_JOB_ID}.dat | cut -f1)
echo "Actual size: ${ACTUAL_SIZE}MB"

# Cleanup
rm -f /tmp/memtest_${SLURM_JOB_ID}.dat
echo "Memory released"
echo ""

# GPU simulation (if available)
echo "=== GPU Detection ==="
if command -v nvidia-smi &> /dev/null; then
    echo "NVIDIA GPU detected:"
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv
else
    echo "No NVIDIA GPU available (nvidia-smi not found)"
    echo "Simulating GPU workload..."

    # Simulate matrix multiplication timing
    echo "Simulated GPU matrix multiplication (1024x1024):"
    START=$(date +%s%N)
    # Just sleep to simulate
    sleep 2
    END=$(date +%s%N)
    ELAPSED=$(( (END - START) / 1000000 ))
    echo "  Simulated time: ${ELAPSED}ms"
fi
echo ""

# I/O throughput test
echo "=== I/O Throughput Test ==="
IO_FILE="/tmp/io_test_${SLURM_JOB_ID}.dat"

echo "Write test (100MB)..."
START=$(date +%s%N)
dd if=/dev/zero of=${IO_FILE} bs=1M count=100 conv=fdatasync 2>&1 | tail -1
END=$(date +%s%N)
WRITE_TIME=$(( (END - START) / 1000000 ))
echo "Write time: ${WRITE_TIME}ms"

echo "Read test..."
START=$(date +%s%N)
dd if=${IO_FILE} of=/dev/null bs=1M 2>&1 | tail -1
END=$(date +%s%N)
READ_TIME=$(( (END - START) / 1000000 ))
echo "Read time: ${READ_TIME}ms"

rm -f ${IO_FILE}
echo ""

# Summary
echo "=== Resource Usage Summary ==="
cat > /tmp/resource_summary_${SLURM_JOB_ID}.json << EOF
{
  "job_id": "${SLURM_JOB_ID}",
  "node": "$(hostname)",
  "resources": {
    "cpus_allocated": ${SLURM_CPUS_PER_TASK:-4},
    "memory_mb": ${MEM_TARGET_MB},
    "gpu_requested": true
  },
  "benchmarks": {
    "cpu_workers": ${SLURM_CPUS_PER_TASK:-4},
    "io_write_ms": ${WRITE_TIME},
    "io_read_ms": ${READ_TIME}
  },
  "completed_at": "$(date -Iseconds)"
}
EOF

echo "Summary:"
cat /tmp/resource_summary_${SLURM_JOB_ID}.json

echo ""
echo "End: $(date -Iseconds)"
echo "=== Resource Intensive Job Complete ==="
exit 0
