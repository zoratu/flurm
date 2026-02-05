#!/bin/bash
#SBATCH --job-name=preempt_low
#SBATCH --partition=low
#SBATCH --cpus-per-task=4
#SBATCH --mem=2G
#SBATCH --time=01:00:00
#SBATCH --output=/tmp/preempt_low_%j.out
#SBATCH --error=/tmp/preempt_low_%j.err

echo "=== FLURM Test Job: Low Priority (Preemptible) ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"

echo ""
echo "This job runs in a low-priority partition."
echo "It will be preempted if a high-priority job needs these resources."
echo ""

# Set up signal handler for preemption
trap 'echo "Received SIGTERM - being preempted at $(date)"; exit 0' TERM
trap 'echo "Received SIGUSR1 - preemption warning at $(date)"' USR1

# Run until preempted or time limit
COUNTER=0
while true; do
    COUNTER=$((COUNTER + 1))
    echo "[$(date +%H:%M:%S)] Still running... iteration $COUNTER"
    sleep 10
done
