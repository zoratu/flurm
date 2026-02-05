#!/bin/bash
#SBATCH --job-name=preempt_high
#SBATCH --partition=high
#SBATCH --cpus-per-task=4
#SBATCH --mem=2G
#SBATCH --time=00:10:00
#SBATCH --output=/tmp/preempt_high_%j.out
#SBATCH --error=/tmp/preempt_high_%j.err

echo "=== FLURM Test Job: High Priority (Preemptor) ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

echo ""
echo "This high-priority job may have preempted a low-priority job."
echo "Running for 30 seconds to verify preemption..."
echo ""

# Do some work
for i in $(seq 1 6); do
    echo "[$(date +%H:%M:%S)] Working... $i/6"
    sleep 5
done

echo ""
echo "End time: $(date)"
echo "SUCCESS - High priority job completed"
