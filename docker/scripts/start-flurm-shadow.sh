#!/bin/bash
# Start FLURM in Shadow Mode
# Imports state from SLURM and mirrors operations
set -e

echo "=== Starting FLURM in Shadow Mode ==="
echo "SLURM Controller: ${SLURM_CONTROLLER_HOST:-slurm-controller}:${SLURM_CONTROLLER_PORT:-6817}"
echo "FLURM Node: ${FLURM_NODE_NAME:-flurm@localhost}"

# Wait for MUNGE key to be available
echo "Waiting for MUNGE key..."
for i in $(seq 1 30); do
    if [ -f /etc/munge/munge.key ]; then
        echo "MUNGE key found"
        break
    fi
    sleep 2
done

if [ -f /etc/munge/munge.key ]; then
    # Fix MUNGE permissions
    chown -R munge:munge /etc/munge /var/run/munge /var/log/munge 2>/dev/null || true
    chmod 700 /etc/munge 2>/dev/null || true
    chmod 400 /etc/munge/munge.key 2>/dev/null || true

    # Start MUNGE daemon
    echo "Starting MUNGE daemon..."
    runuser -u munge -- /usr/sbin/munged 2>/dev/null || echo "MUNGE failed (may be OK)"
    sleep 1
fi

# Create data directories
mkdir -p /var/lib/flurm/ra /var/log/flurm
chown -R flurm:flurm /var/lib/flurm /var/log/flurm 2>/dev/null || true

# Wait for SLURM controller to be available
echo "Waiting for SLURM controller..."
SLURM_HOST=${SLURM_CONTROLLER_HOST:-slurm-controller}
SLURM_PORT=${SLURM_CONTROLLER_PORT:-6817}

for i in $(seq 1 60); do
    if nc -z "$SLURM_HOST" "$SLURM_PORT" 2>/dev/null; then
        echo "SLURM controller is reachable"
        break
    fi
    echo "Waiting for SLURM controller at $SLURM_HOST:$SLURM_PORT... ($i/60)"
    sleep 2
done

# Export environment for Erlang
export FLURM_MODE=shadow
export FLURM_CONFIG=${FLURM_CONFIG:-/etc/flurm/flurm.config}
export ERL_LIBS=/opt/flurm/_build/default/lib

# Create a simple HTTP health endpoint for Docker healthcheck
# (Run in background)
(
    while true; do
        echo -e "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\",\"mode\":\"shadow\"}" | nc -l -p 8080 -q 1 2>/dev/null || sleep 1
    done
) &

# Start FLURM
echo "Starting FLURM controller in shadow mode..."
cd /opt/flurm

# Run the FLURM controller with import enabled
exec erl \
    -name ${FLURM_NODE_NAME:-flurm@localhost} \
    -setcookie ${FLURM_COOKIE:-flurm_migration_test} \
    -pa _build/default/lib/*/ebin \
    -config ${FLURM_CONFIG} \
    -noshell \
    -eval "
        io:format(\"FLURM starting in shadow mode~n\"),
        %% Start required applications
        application:ensure_all_started(sasl),
        application:ensure_all_started(lager),
        application:ensure_all_started(ranch),
        application:ensure_all_started(ra),
        %% Start FLURM applications
        application:ensure_all_started(flurm_config),
        application:ensure_all_started(flurm_protocol),
        application:ensure_all_started(flurm_core),
        application:ensure_all_started(flurm_db),
        application:ensure_all_started(flurm_controller),
        io:format(\"FLURM controller started~n\"),
        %% Start SLURM import if available
        case application:ensure_all_started(flurm_slurm_import) of
            {ok, _} -> io:format(\"SLURM import module loaded~n\");
            _ -> io:format(\"SLURM import not available~n\")
        end,
        receive stop -> ok end
    "
