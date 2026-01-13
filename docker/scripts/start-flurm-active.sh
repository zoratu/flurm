#!/bin/bash
# Start FLURM in Active Mode
# Takes over from SLURM as the primary scheduler
set -e

echo "=== Starting FLURM in Active Mode ==="
echo "FLURM Node: ${FLURM_NODE_NAME:-flurm@localhost}"

# Create data directories
mkdir -p /var/lib/flurm/ra /var/log/flurm
chown -R flurm:flurm /var/lib/flurm /var/log/flurm 2>/dev/null || true

# Start MUNGE if available
if [ -f /etc/munge/munge.key ]; then
    chown -R munge:munge /etc/munge /var/run/munge /var/log/munge 2>/dev/null || true
    chmod 700 /etc/munge 2>/dev/null || true
    chmod 400 /etc/munge/munge.key 2>/dev/null || true
    runuser -u munge -- /usr/sbin/munged 2>/dev/null || echo "MUNGE failed (may be OK)"
    sleep 1
fi

# Export environment
export FLURM_MODE=active
export FLURM_CONFIG=${FLURM_CONFIG:-/etc/flurm/flurm.config}
export ERL_LIBS=/opt/flurm/_build/default/lib

# Create health endpoint
(
    while true; do
        echo -e "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\",\"mode\":\"active\"}" | nc -l -p 8080 -q 1 2>/dev/null || sleep 1
    done
) &

# Start FLURM in active mode
echo "Starting FLURM controller in active mode..."
cd /opt/flurm

exec erl \
    -name ${FLURM_NODE_NAME:-flurm@localhost} \
    -setcookie ${FLURM_COOKIE:-flurm_migration_test} \
    -pa _build/default/lib/*/ebin \
    -config ${FLURM_CONFIG} \
    -s flurm_core \
    -s flurm_controller \
    -noshell \
    -eval "
        io:format(\"FLURM starting in active mode~n\"),
        receive stop -> ok end
    "
