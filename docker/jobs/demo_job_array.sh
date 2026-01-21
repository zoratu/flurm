#!/bin/bash
#SBATCH --job-name=array_job
#SBATCH --output=/tmp/array_%A_%a.out
#SBATCH --error=/tmp/array_%A_%a.err
#SBATCH --time=00:05:00
#SBATCH --array=1-10%3
#SBATCH --nodes=1
#SBATCH --ntasks=1

# Job array demonstrating parallel task processing
# %3 means max 3 tasks run simultaneously

echo "=== Array Task ${SLURM_ARRAY_TASK_ID} of Job ${SLURM_ARRAY_JOB_ID} ==="
echo "Node: $(hostname)"
echo "Start: $(date -Iseconds)"
echo ""

# Each array task processes different data
INPUT_FILE="/data/input_${SLURM_ARRAY_TASK_ID}.dat"
OUTPUT_FILE="/tmp/output_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.dat"

echo "Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Input: ${INPUT_FILE}"
echo "Output: ${OUTPUT_FILE}"
echo ""

# Simulate processing with variable time based on task ID
WORK_TIME=$((SLURM_ARRAY_TASK_ID * 2))
echo "Processing for ${WORK_TIME} seconds..."

# Generate task-specific output
for i in $(seq 1 $WORK_TIME); do
    echo "Task ${SLURM_ARRAY_TASK_ID} - iteration $i" >> "${OUTPUT_FILE}"
    sleep 1
done

# Simulate occasional failures for odd task IDs > 5
if [ $((SLURM_ARRAY_TASK_ID % 2)) -eq 1 ] && [ ${SLURM_ARRAY_TASK_ID} -gt 5 ]; then
    echo "WARNING: Task ${SLURM_ARRAY_TASK_ID} encountered simulated error"
    # Don't actually fail - just log warning
fi

echo ""
echo "Output written to: ${OUTPUT_FILE}"
echo "Lines written: $(wc -l < ${OUTPUT_FILE})"
echo ""
echo "End: $(date -Iseconds)"
echo "=== Task ${SLURM_ARRAY_TASK_ID} Complete ==="
exit 0
