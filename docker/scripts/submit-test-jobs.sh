#!/bin/bash
# Submit test jobs to SLURM for migration testing

CONTROLLER=${SLURM_CONTROLLER:-slurm-controller}
NUM_JOBS=${1:-5}

echo "=== Submitting $NUM_JOBS Test Jobs to SLURM ==="
echo "Controller: $CONTROLLER"

# Wait for controller to be ready
echo "Checking SLURM controller status..."
for i in $(seq 1 30); do
    if scontrol ping 2>/dev/null | grep -q "UP"; then
        echo "SLURM controller is UP"
        break
    fi
    echo "Waiting for controller... ($i/30)"
    sleep 2
done

# Check node status
echo ""
echo "Node status:"
sinfo -N

# Submit different types of test jobs
echo ""
echo "Submitting test jobs..."

# Job 1: Short sleep job
echo "  Submitting short sleep job..."
sbatch --job-name="test_short" --time=00:05:00 --wrap="echo 'Short job'; sleep 30; echo 'Done'"

# Job 2: Long sleep job
echo "  Submitting long sleep job..."
sbatch --job-name="test_long" --time=00:30:00 --partition=debug --wrap="echo 'Long job'; sleep 600; echo 'Done'"

# Job 3: Array job
echo "  Submitting array job..."
sbatch --job-name="test_array" --array=1-3 --time=00:05:00 --wrap="echo 'Array task \$SLURM_ARRAY_TASK_ID'; sleep 60"

# Additional simple jobs
for i in $(seq 4 $NUM_JOBS); do
    echo "  Submitting job $i..."
    sbatch --job-name="test_job_$i" --time=00:10:00 --wrap="echo 'Test job $i'; sleep 120; echo 'Done'"
done

echo ""
echo "Jobs submitted. Current queue:"
squeue -l

echo ""
echo "=== Job Submission Complete ==="
