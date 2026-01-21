#!/bin/bash
# FLURM Stress Test - High-volume job submission
# Tests job acceptance rate and failure handling

set -e

NUM_JOBS=${1:-100}
BATCH_SIZE=${2:-10}
DELAY_MS=${3:-50}

echo "=============================================="
echo "     FLURM Stress Test"
echo "=============================================="
echo "Total jobs:    $NUM_JOBS"
echo "Batch size:    $BATCH_SIZE"
echo "Delay (ms):    $DELAY_MS"
echo "Start time:    $(date -Iseconds)"
echo "=============================================="
echo ""

# Create a simple test job template
JOB_SCRIPT=$(mktemp /tmp/stress_job_XXXXXX.sh)
cat > "$JOB_SCRIPT" << 'ENDJOB'
#!/bin/bash
#SBATCH --job-name=stress_%j
#SBATCH --output=/tmp/stress_%j.out
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Stress job $SLURM_JOB_ID started at $(date -Iseconds)"
sleep $((RANDOM % 3 + 1))
echo "Stress job $SLURM_JOB_ID completed at $(date -Iseconds)"
exit 0
ENDJOB
chmod +x "$JOB_SCRIPT"

# Tracking variables
SUBMITTED=0
FAILED=0
START_TIME=$(date +%s%N)

# Submit jobs in batches
echo "Submitting $NUM_JOBS jobs..."
for i in $(seq 1 $NUM_JOBS); do
    # Submit job
    RESULT=$(sbatch "$JOB_SCRIPT" 2>&1)

    if echo "$RESULT" | grep -q "Submitted batch job"; then
        ((SUBMITTED++))
    else
        ((FAILED++))
        echo "  Job $i failed: $RESULT"
    fi

    # Progress indicator
    if [ $((i % BATCH_SIZE)) -eq 0 ]; then
        ELAPSED=$(($(date +%s%N) - START_TIME))
        ELAPSED_MS=$((ELAPSED / 1000000))
        RATE=$((i * 1000 / (ELAPSED_MS + 1)))
        echo "  Progress: $i/$NUM_JOBS submitted ($SUBMITTED ok, $FAILED failed) - ${RATE} jobs/sec"
    fi

    # Small delay between submissions
    if [ $DELAY_MS -gt 0 ]; then
        sleep 0.$(printf "%03d" $DELAY_MS)
    fi
done

END_TIME=$(date +%s%N)
TOTAL_TIME_MS=$(( (END_TIME - START_TIME) / 1000000 ))
TOTAL_TIME_SEC=$((TOTAL_TIME_MS / 1000))

echo ""
echo "=============================================="
echo "     Submission Complete"
echo "=============================================="
echo "Jobs submitted:  $SUBMITTED"
echo "Jobs failed:     $FAILED"
echo "Total time:      ${TOTAL_TIME_SEC}s (${TOTAL_TIME_MS}ms)"
echo "Submission rate: $((SUBMITTED * 1000 / (TOTAL_TIME_MS + 1))) jobs/sec"
echo ""

# Wait for queue to drain
echo "Waiting for jobs to complete..."
WAIT_START=$(date +%s)
MAX_WAIT=120  # 2 minutes max

while true; do
    PENDING=$(squeue -h 2>/dev/null | wc -l)
    ELAPSED=$(($(date +%s) - WAIT_START))

    if [ $PENDING -eq 0 ]; then
        echo "  All jobs completed!"
        break
    fi

    if [ $ELAPSED -gt $MAX_WAIT ]; then
        echo "  Timeout waiting for jobs (${PENDING} still pending)"
        break
    fi

    echo "  Jobs pending: $PENDING (waited ${ELAPSED}s)"
    sleep 5
done

echo ""
echo "=============================================="
echo "     Final Results"
echo "=============================================="
echo "End time:        $(date -Iseconds)"

# Cleanup
rm -f "$JOB_SCRIPT"
