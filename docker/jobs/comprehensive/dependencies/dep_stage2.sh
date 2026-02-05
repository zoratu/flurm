#!/bin/bash
#SBATCH --job-name=dep_stage2
#SBATCH --partition=debug
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:01:00
#SBATCH --output=/tmp/dep_stage2_%j.out
#SBATCH --error=/tmp/dep_stage2_%j.err

# NOTE: This job should be submitted with: --dependency=afterok:<stage1_job_id>

echo "=== FLURM Test Job: Dependency Stage 2 (Processing) ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start time: $(date)"

# Find data from stage 1
DATA_FILES=$(ls /tmp/pipeline_data_*.txt 2>/dev/null | head -1)

if [ -z "$DATA_FILES" ]; then
    echo "WARNING: No data file found from stage 1"
    echo "Creating placeholder data..."
    DATA_FILES="/tmp/pipeline_data_placeholder.txt"
    echo "placeholder=true" > "$DATA_FILES"
fi

echo "Processing data from: $DATA_FILES"

# Process the data
OUTPUT_FILE="/tmp/pipeline_processed_${SLURM_JOB_ID}.txt"
echo "# Processed by job $SLURM_JOB_ID" > "$OUTPUT_FILE"

# Count items and compute sum
item_count=$(grep -c "^item_" "$DATA_FILES" || echo "0")
item_sum=$(grep "^item_" "$DATA_FILES" | cut -d= -f2 | paste -sd+ | bc 2>/dev/null || echo "0")

echo "item_count=$item_count" >> "$OUTPUT_FILE"
echo "item_sum=$item_sum" >> "$OUTPUT_FILE"
echo "processed_at=$(date +%s)" >> "$OUTPUT_FILE"

echo "Processed $item_count items, sum = $item_sum"

echo "End time: $(date)"
echo "SUCCESS - Stage 2 complete"
echo "Output file: $OUTPUT_FILE"
