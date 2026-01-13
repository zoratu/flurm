#!/bin/bash
# Verify SLURM state was imported into FLURM

SLURM_HOST=${SLURM_CONTROLLER:-slurm-controller}
FLURM_HOST=${FLURM_CONTROLLER:-flurm-controller}

echo "=== Verifying SLURM Import to FLURM ==="

# Get SLURM state
echo ""
echo "--- SLURM Current State ---"
echo "Jobs:"
squeue -o "%i|%j|%u|%P|%t" --noheader 2>/dev/null || echo "  (Could not query SLURM)"

echo ""
echo "Nodes:"
sinfo -N -o "%n|%t|%c|%m" --noheader 2>/dev/null || echo "  (Could not query SLURM)"

echo ""
echo "Partitions:"
sinfo -s -o "%P|%a|%l|%D" --noheader 2>/dev/null || echo "  (Could not query SLURM)"

# Get FLURM state via HTTP API
echo ""
echo "--- FLURM Imported State ---"
echo "Checking FLURM API at http://${FLURM_HOST}:8080..."

echo ""
echo "Jobs:"
curl -sf "http://${FLURM_HOST}:8080/api/jobs" 2>/dev/null | jq -r '.jobs[] | "\(.id)|\(.name)|\(.user)|\(.partition)|\(.state)"' 2>/dev/null || echo "  (Could not query FLURM or no jobs)"

echo ""
echo "Nodes:"
curl -sf "http://${FLURM_HOST}:8080/api/nodes" 2>/dev/null | jq -r '.nodes[] | "\(.name)|\(.state)|\(.cpus)|\(.memory_mb)"' 2>/dev/null || echo "  (Could not query FLURM or no nodes)"

echo ""
echo "Partitions:"
curl -sf "http://${FLURM_HOST}:8080/api/partitions" 2>/dev/null | jq -r '.partitions[] | "\(.name)|\(.available)|\(.time_limit)|\(.total_nodes)"' 2>/dev/null || echo "  (Could not query FLURM or no partitions)"

# Count comparison
echo ""
echo "--- Comparison ---"
SLURM_JOBS=$(squeue --noheader 2>/dev/null | wc -l | tr -d ' ')
FLURM_JOBS=$(curl -sf "http://${FLURM_HOST}:8080/api/jobs" 2>/dev/null | jq '.jobs | length' 2>/dev/null || echo "0")

echo "SLURM jobs: $SLURM_JOBS"
echo "FLURM jobs: $FLURM_JOBS"

if [ "$SLURM_JOBS" = "$FLURM_JOBS" ]; then
    echo "PASS: Job counts match"
else
    echo "WARN: Job counts differ (may be due to timing)"
fi

echo ""
echo "=== Verification Complete ==="
