#!/bin/bash
# Submit a 4-stage dependency chain pipeline

set -e

echo "=== Submitting Dependency Chain Pipeline ==="
echo ""

# Stage 1: Data Generation (no dependencies)
JOB1=$(sbatch --parsable /tmp/job1_generate.sh)
echo "Stage 1 (generate): Job ID $JOB1"

# Stage 2: Validation (depends on Stage 1)
sed -i "s/PREV_JOB_ID/$JOB1/" /tmp/job2_validate.sh
JOB2=$(sbatch --parsable /tmp/job2_validate.sh)
echo "Stage 2 (validate): Job ID $JOB2 (depends on $JOB1)"

# Stage 3: Processing (depends on Stage 2)
sed -i "s/PREV_JOB_ID/$JOB2/" /tmp/job3_process.sh
JOB3=$(sbatch --parsable /tmp/job3_process.sh)
echo "Stage 3 (process): Job ID $JOB3 (depends on $JOB2)"

# Stage 4: Report (depends on Stage 3)
sed -i "s/PREV_JOB_ID/$JOB3/" /tmp/job4_report.sh
JOB4=$(sbatch --parsable /tmp/job4_report.sh)
echo "Stage 4 (report): Job ID $JOB4 (depends on $JOB3)"

echo ""
echo "Pipeline submitted!"
echo "Monitor with: squeue"
echo "View dependency graph with: scontrol show job $JOB1,$JOB2,$JOB3,$JOB4"
echo ""
echo "Jobs: $JOB1 -> $JOB2 -> $JOB3 -> $JOB4"
