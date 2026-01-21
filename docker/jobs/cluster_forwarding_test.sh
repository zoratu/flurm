#!/bin/bash
# FLURM Cluster Forwarding Test
# Tests job submission through non-leader controllers (should forward to leader)

set -e

echo "=============================================="
echo "     FLURM Cluster Forwarding Test"
echo "=============================================="
echo ""

# Controller endpoints (mapped ports from docker-compose.cluster.yml)
CTRL1="localhost:6817"
CTRL2="localhost:6818"
CTRL3="localhost:6819"

# Create test job script
JOB_SCRIPT=$(mktemp /tmp/forward_test_XXXXXX.sh)
cat > "$JOB_SCRIPT" << 'ENDJOB'
#!/bin/bash
#SBATCH --job-name=forward_test_%j
#SBATCH --output=/tmp/forward_%j.out
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

echo "Job $SLURM_JOB_ID submitted through forwarding test"
echo "Running on $(hostname)"
sleep 2
echo "Done"
exit 0
ENDJOB
chmod +x "$JOB_SCRIPT"

echo "=== Test 1: Submit to each controller ==="
echo "Submitting jobs to each controller to test forwarding..."
echo ""

# Test submission through different controllers
submit_to_controller() {
    local name=$1
    local port=$2

    # Use SLURM_CONF to point to the right controller
    export SLURMCTLD_HOST="localhost"
    export SLURMCTLD_PORT="$port"

    echo -n "  Submitting to $name (port $port)... "
    RESULT=$(sbatch --export=ALL "$JOB_SCRIPT" 2>&1)

    if echo "$RESULT" | grep -q "Submitted batch job"; then
        JOB_ID=$(echo "$RESULT" | grep -o '[0-9]*' | tail -1)
        echo "OK (job $JOB_ID)"
        return 0
    else
        echo "FAILED: $RESULT"
        return 1
    fi
}

# Track results
SUBMITTED=0
FAILED=0

# Submit to controller 1 (likely leader)
if submit_to_controller "Controller 1" 6817; then
    ((SUBMITTED++))
else
    ((FAILED++))
fi

# Submit to controller 2 (should forward to leader)
if submit_to_controller "Controller 2" 6818; then
    ((SUBMITTED++))
else
    ((FAILED++))
fi

# Submit to controller 3 (should forward to leader)
if submit_to_controller "Controller 3" 6819; then
    ((SUBMITTED++))
else
    ((FAILED++))
fi

echo ""
echo "=== Test 2: Rapid forwarding test ==="
echo "Submitting 30 jobs across controllers..."

RAPID_START=$(date +%s%N)
for i in $(seq 1 30); do
    # Rotate through controllers
    case $((i % 3)) in
        0) PORT=6817 ;;
        1) PORT=6818 ;;
        2) PORT=6819 ;;
    esac

    export SLURMCTLD_PORT="$PORT"
    if sbatch --export=ALL "$JOB_SCRIPT" >/dev/null 2>&1; then
        ((SUBMITTED++))
    else
        ((FAILED++))
    fi
done
RAPID_END=$(date +%s%N)
RAPID_MS=$(( (RAPID_END - RAPID_START) / 1000000 ))
echo "  30 jobs in ${RAPID_MS}ms ($((30000 / (RAPID_MS + 1))) jobs/sec)"

echo ""
echo "=== Test 3: Query from any controller ==="
echo "Querying job state from each controller..."

for port in 6817 6818 6819; do
    export SLURMCTLD_PORT="$port"
    COUNT=$(squeue -h 2>/dev/null | wc -l)
    echo "  Port $port: $COUNT jobs in queue"
done

echo ""
echo "=============================================="
echo "     Forwarding Test Results"
echo "=============================================="
echo "Total submitted:  $SUBMITTED"
echo "Total failed:     $FAILED"
echo ""

# Wait for queue to drain
echo "Waiting 30 seconds for jobs to complete..."
sleep 30

echo ""
echo "Final queue state (from Controller 1):"
export SLURMCTLD_PORT=6817
squeue

# Cleanup
rm -f "$JOB_SCRIPT"

echo ""
echo "Test complete!"
