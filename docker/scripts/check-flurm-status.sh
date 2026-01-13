#!/bin/bash
# Check FLURM status

FLURM_HOST=${FLURM_CONTROLLER:-flurm-controller}
FLURM_HTTP_PORT=8080

# Check HTTP health endpoint
if curl -sf "http://${FLURM_HOST}:${FLURM_HTTP_PORT}/health" >/dev/null 2>&1; then
    echo "FLURM is healthy"
    curl -s "http://${FLURM_HOST}:${FLURM_HTTP_PORT}/health" | jq .
    exit 0
else
    echo "FLURM health check failed"
    exit 1
fi
