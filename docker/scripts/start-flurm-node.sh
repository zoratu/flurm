#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting FLURM Node ==="
echo "Controller: ${FLURM_CONTROLLER_HOST:-flurm-controller}:${FLURM_CONTROLLER_PORT:-6820}"
echo "Node name:  ${FLURM_NODE_NAME:-flurm@flurm-compute-1}"

mkdir -p /var/run/munge /var/log/munge
chown -R munge:munge /var/run/munge /var/log/munge

if [ -f /etc/munge/munge.key ]; then
  chmod 400 /etc/munge/munge.key || true
fi

runuser -u munge -- /usr/sbin/munged
sleep 1

if ! munge -n </dev/null >/dev/null 2>&1; then
  echo "ERROR: MUNGE authentication check failed"
  exit 1
fi

cd /opt/flurm
exec escript escripts/start_node.erl \
  "${FLURM_CONTROLLER_HOST:-flurm-controller}" \
  "${FLURM_CONTROLLER_PORT:-6820}"
