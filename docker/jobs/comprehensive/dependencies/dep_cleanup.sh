#!/bin/bash
#SBATCH --job-name=dep_cleanup
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:01:00
#SBATCH --output=/tmp/dep_cleanup_%j.out
#SBATCH --error=/tmp/dep_cleanup_%j.err

# NOTE: This job should be submitted with: --dependency=afterany:<stage3_job_id>
# It runs whether stage 3 succeeded or failed

echo "=== FLURM Test Job: Dependency Cleanup ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

echo "Cleaning up pipeline temporary files..."

# Count files to clean
DATA_COUNT=$(ls /tmp/pipeline_data_*.txt 2>/dev/null | wc -l || echo "0")
PROCESSED_COUNT=$(ls /tmp/pipeline_processed_*.txt 2>/dev/null | wc -l || echo "0")
FINAL_COUNT=$(ls /tmp/pipeline_final_*.txt 2>/dev/null | wc -l || echo "0")

echo "Found: $DATA_COUNT data files, $PROCESSED_COUNT processed files, $FINAL_COUNT final files"

# Clean up
rm -f /tmp/pipeline_data_*.txt 2>/dev/null
rm -f /tmp/pipeline_processed_*.txt 2>/dev/null
# Keep final files for inspection
# rm -f /tmp/pipeline_final_*.txt 2>/dev/null

echo "Cleanup complete!"
echo "Note: Final report files preserved for inspection"

echo "End time: $(date)"
echo "SUCCESS - Cleanup complete"
