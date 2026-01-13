#!/bin/bash
# Trigger SLURM state import into FLURM

FLURM_HOST=${FLURM_CONTROLLER:-flurm-controller}
FLURM_HTTP_PORT=8080

echo "=== Triggering SLURM Import ==="

# Trigger import via HTTP API
curl -sf -X POST "http://${FLURM_HOST}:${FLURM_HTTP_PORT}/api/import" \
    -H "Content-Type: application/json" \
    -d '{"source": "slurm", "full": true}'

echo ""
echo "Import triggered. Check FLURM logs for details."
