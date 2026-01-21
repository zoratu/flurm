#!/bin/bash
# This file contains multiple jobs that form a dependency chain
# Submit with: ./submit_dependency_chain.sh

# Job 1: Data generation (no dependencies)
cat > /tmp/job1_generate.sh << 'ENDJOB1'
#!/bin/bash
#SBATCH --job-name=stage1_generate
#SBATCH --output=/tmp/stage1_%j.out
#SBATCH --time=00:02:00

echo "=== Stage 1: Data Generation ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Start: $(date -Iseconds)"

WORK_DIR="/tmp/pipeline_${SLURM_JOB_ID}"
mkdir -p "$WORK_DIR"

# Generate raw data
echo "Generating raw dataset..."
for i in $(seq 1 1000); do
    echo "$RANDOM,$RANDOM,$RANDOM,$(date +%s%N)" >> "$WORK_DIR/raw_data.csv"
done

echo "Generated $(wc -l < $WORK_DIR/raw_data.csv) records"
echo "$WORK_DIR" > /tmp/pipeline_workdir.txt

echo "End: $(date -Iseconds)"
echo "=== Stage 1 Complete ==="
ENDJOB1

# Job 2: Data validation (depends on Job 1)
cat > /tmp/job2_validate.sh << 'ENDJOB2'
#!/bin/bash
#SBATCH --job-name=stage2_validate
#SBATCH --output=/tmp/stage2_%j.out
#SBATCH --time=00:02:00
#SBATCH --dependency=afterok:PREV_JOB_ID

echo "=== Stage 2: Data Validation ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Start: $(date -Iseconds)"

WORK_DIR=$(cat /tmp/pipeline_workdir.txt)
echo "Working directory: $WORK_DIR"

if [ ! -f "$WORK_DIR/raw_data.csv" ]; then
    echo "ERROR: Input file not found!"
    exit 1
fi

# Validate data
echo "Validating data format..."
VALID=0
INVALID=0
while IFS=',' read -r c1 c2 c3 c4; do
    if [[ "$c1" =~ ^[0-9]+$ ]] && [[ "$c2" =~ ^[0-9]+$ ]]; then
        ((VALID++))
    else
        ((INVALID++))
    fi
done < "$WORK_DIR/raw_data.csv"

echo "Valid records: $VALID"
echo "Invalid records: $INVALID"

if [ $INVALID -gt 0 ]; then
    echo "WARNING: Found invalid records"
fi

echo "VALIDATED" > "$WORK_DIR/validation_status.txt"
echo "End: $(date -Iseconds)"
echo "=== Stage 2 Complete ==="
ENDJOB2

# Job 3: Data processing (depends on Job 2)
cat > /tmp/job3_process.sh << 'ENDJOB3'
#!/bin/bash
#SBATCH --job-name=stage3_process
#SBATCH --output=/tmp/stage3_%j.out
#SBATCH --time=00:03:00
#SBATCH --cpus-per-task=2
#SBATCH --dependency=afterok:PREV_JOB_ID

echo "=== Stage 3: Data Processing ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "CPUs: ${SLURM_CPUS_PER_TASK}"
echo "Start: $(date -Iseconds)"

WORK_DIR=$(cat /tmp/pipeline_workdir.txt)

if [ ! -f "$WORK_DIR/validation_status.txt" ]; then
    echo "ERROR: Validation not completed!"
    exit 1
fi

echo "Processing validated data..."
awk -F',' '
BEGIN { sum1=0; sum2=0; min1=999999; max1=0; count=0 }
{
    sum1 += $1; sum2 += $2;
    if ($1 < min1) min1 = $1;
    if ($1 > max1) max1 = $1;
    count++;
}
END {
    print "Records processed: " count
    print "Column 1 - Sum: " sum1 ", Avg: " sum1/count ", Min: " min1 ", Max: " max1
    print "Column 2 - Sum: " sum2 ", Avg: " sum2/count
}' "$WORK_DIR/raw_data.csv" | tee "$WORK_DIR/statistics.txt"

echo "PROCESSED" > "$WORK_DIR/process_status.txt"
echo "End: $(date -Iseconds)"
echo "=== Stage 3 Complete ==="
ENDJOB3

# Job 4: Report generation (depends on Job 3)
cat > /tmp/job4_report.sh << 'ENDJOB4'
#!/bin/bash
#SBATCH --job-name=stage4_report
#SBATCH --output=/tmp/stage4_%j.out
#SBATCH --time=00:01:00
#SBATCH --dependency=afterok:PREV_JOB_ID

echo "=== Stage 4: Report Generation ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Start: $(date -Iseconds)"

WORK_DIR=$(cat /tmp/pipeline_workdir.txt)

echo "Generating final report..."
cat > "$WORK_DIR/report.json" << REPORT
{
  "pipeline_id": "${SLURM_JOB_ID}",
  "timestamp": "$(date -Iseconds)",
  "stages": {
    "generation": "complete",
    "validation": "$(cat $WORK_DIR/validation_status.txt 2>/dev/null || echo 'unknown')",
    "processing": "$(cat $WORK_DIR/process_status.txt 2>/dev/null || echo 'unknown')"
  },
  "statistics": "$(cat $WORK_DIR/statistics.txt 2>/dev/null | tr '\n' ' ')"
}
REPORT

echo "Report generated:"
cat "$WORK_DIR/report.json"

echo ""
echo "Pipeline artifacts in: $WORK_DIR"
ls -la "$WORK_DIR"

echo ""
echo "End: $(date -Iseconds)"
echo "=== Pipeline Complete ==="
ENDJOB4

echo "Dependency chain job scripts created in /tmp/"
echo "Use submit_dependency_chain.sh to submit the pipeline"
