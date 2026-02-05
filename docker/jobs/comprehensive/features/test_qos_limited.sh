#!/bin/bash
#SBATCH --job-name=qos_limited
#SBATCH --partition=batch
#SBATCH --qos=limited
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M
#SBATCH --time=00:05:00
#SBATCH --output=/tmp/qos_limited_%j.out
#SBATCH --error=/tmp/qos_limited_%j.err

echo "=== FLURM Test Job: QOS Limited Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "QOS: $SLURM_JOB_QOS"
echo "Start time: $(date)"

echo ""
echo "This job runs with 'limited' QOS."
echo "QOS may restrict:"
echo "  - MaxJobsPerUser"
echo "  - MaxJobsPerAccount"
echo "  - Priority"
echo "  - Preemption"
echo ""

# Run for 30 seconds
sleep 30

echo "End time: $(date)"
echo "SUCCESS - QOS limited job completed"
