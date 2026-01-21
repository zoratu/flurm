#!/bin/bash
#SBATCH --job-name=failing_job
#SBATCH --output=/tmp/failing_%j.out
#SBATCH --error=/tmp/failing_%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

# Job that fails intentionally to demonstrate error handling

echo "=== Failing Job Demo ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Start: $(date -Iseconds)"
echo ""

echo "Step 1: Initialization... OK"
sleep 2

echo "Step 2: Loading configuration... OK"
sleep 2

echo "Step 3: Processing data..."
sleep 2

# Simulate failure
echo ""
echo "ERROR: Critical failure detected!"
echo "ERROR: Unable to allocate required resources"
echo "ERROR: Stack trace:"
echo "  at process_data() line 42"
echo "  at main() line 15"
echo ""

echo "End: $(date -Iseconds)"
echo "=== Job Failed (exit 1) ==="
exit 1
