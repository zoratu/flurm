#!/bin/bash
#SBATCH --job-name=resv_test
#SBATCH --partition=batch
#SBATCH --reservation=maintenance
#SBATCH --cpus-per-task=2
#SBATCH --mem=1G
#SBATCH --time=00:10:00
#SBATCH --output=/tmp/resv_test_%j.out
#SBATCH --error=/tmp/resv_test_%j.err

echo "=== FLURM Test Job: Reservation Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Reservation: $SLURM_JOB_RESERVATION"
echo "Start time: $(date)"

echo ""
echo "This job runs within a reservation named 'maintenance'."
echo "Reservations pre-allocate resources for specific users/groups."
echo ""

# Verify reservation
if [ -n "$SLURM_JOB_RESERVATION" ]; then
    echo "Reservation confirmed: $SLURM_JOB_RESERVATION"
else
    echo "WARNING: No reservation environment variable set"
fi

# Show node list
echo ""
echo "Allocated nodes: $SLURM_JOB_NODELIST"

sleep 20

echo ""
echo "End time: $(date)"
echo "SUCCESS - Reservation test completed"
