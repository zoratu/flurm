#!/bin/bash
#SBATCH --job-name=qos_priority
#SBATCH --partition=batch
#SBATCH --qos=priority
#SBATCH --cpus-per-task=2
#SBATCH --mem=1G
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/qos_priority_%j.out
#SBATCH --error=/tmp/qos_priority_%j.err

echo "=== FLURM Test Job: QOS Priority Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "QOS: $SLURM_JOB_QOS"
echo "Start time: $(date)"

echo ""
echo "This job runs with 'priority' QOS."
echo "It should be scheduled ahead of lower-priority jobs."
echo ""

# Run for 20 seconds
sleep 20

echo "End time: $(date)"
echo "SUCCESS - QOS priority job completed"
