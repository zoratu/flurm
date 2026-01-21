#!/bin/bash
#SBATCH --job-name=data_processing
#SBATCH --output=/tmp/data_processing_%j.out
#SBATCH --error=/tmp/data_processing_%j.err
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=512M

# Simulate a data processing job that generates output files

echo "=== Data Processing Job ==="
echo "Job ID: ${SLURM_JOB_ID}"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-1}"
echo "Memory: ${SLURM_MEM_PER_NODE:-unknown}"
echo "Node: $(hostname)"
echo "Start: $(date -Iseconds)"
echo ""

# Create working directory
WORK_DIR="/tmp/job_${SLURM_JOB_ID}_data"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

echo "Working directory: $WORK_DIR"
echo ""

# Generate input data
echo "Generating input data..."
for i in $(seq 1 100); do
    echo "$RANDOM,$RANDOM,$RANDOM,$RANDOM" >> input.csv
done
echo "Created input.csv with $(wc -l < input.csv) rows"

# Process data (compute statistics)
echo ""
echo "Processing data..."
awk -F',' '
BEGIN { sum1=0; sum2=0; sum3=0; sum4=0; count=0 }
{
    sum1 += $1; sum2 += $2; sum3 += $3; sum4 += $4;
    count++
}
END {
    print "Column Statistics:"
    print "  Col1 avg: " sum1/count
    print "  Col2 avg: " sum2/count
    print "  Col3 avg: " sum3/count
    print "  Col4 avg: " sum4/count
    print "  Total rows: " count
}' input.csv | tee results.txt

# Generate summary report
echo ""
echo "Generating summary report..."
cat > summary.json << EOF
{
  "job_id": "${SLURM_JOB_ID}",
  "status": "completed",
  "input_rows": $(wc -l < input.csv),
  "output_file": "results.txt",
  "timestamp": "$(date -Iseconds)"
}
EOF

echo "Created summary.json:"
cat summary.json

# Cleanup
echo ""
echo "Output files in $WORK_DIR:"
ls -la "$WORK_DIR"

echo ""
echo "End: $(date -Iseconds)"
echo "=== Job Complete (exit 0) ==="
exit 0
