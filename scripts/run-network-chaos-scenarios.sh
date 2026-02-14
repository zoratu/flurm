#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCKER_DIR="$ROOT/docker"
COMPOSE_FILE="$DOCKER_DIR/docker-compose.ha.yml"
REQUIRED="${FLURM_NETWORK_FAULT_REQUIRED:-0}"

skip_or_fail() {
  local msg="$1"
  if [ "$REQUIRED" = "1" ]; then
    echo "network-chaos: REQUIRED and cannot run: $msg" >&2
    exit 1
  fi
  echo "network-chaos: SKIP ($msg)"
  exit 0
}

if ! command -v docker >/dev/null 2>&1; then
  skip_or_fail "docker not installed"
fi

if ! docker info >/dev/null 2>&1; then
  skip_or_fail "docker daemon unavailable"
fi

cd "$DOCKER_DIR"

ctrl_container="$(docker ps --format '{{.Names}}' | grep -E '^(docker-ctrl-1|docker-flurm-ctrl-1-1)$' | head -1 || true)"
if [ -z "$ctrl_container" ]; then
  skip_or_fail "HA cluster controller container not running"
fi

if ! docker exec "$ctrl_container" sh -lc 'command -v tc >/dev/null 2>&1'; then
  skip_or_fail "tc not available in ${ctrl_container}"
fi

echo "network-chaos: applying packet loss/jitter/reorder on ${ctrl_container}"

docker exec "$ctrl_container" sh -lc 'tc qdisc del dev eth0 root 2>/dev/null || true'
docker exec "$ctrl_container" sh -lc 'tc qdisc add dev eth0 root netem delay 120ms 30ms loss 7% reorder 10% 25%'

cleanup() {
  docker exec "$ctrl_container" sh -lc 'tc qdisc del dev eth0 root 2>/dev/null || true' || true
}
trap cleanup EXIT

# Verify control-plane remains responsive during impairment.
for i in $(seq 1 10); do
  if docker compose -f "$COMPOSE_FILE" exec -T slurm-client scontrol ping >/tmp/flurm-network-chaos.log 2>&1; then
    echo "network-chaos: ping $i/10 OK"
  else
    echo "network-chaos: ping $i/10 failed" >&2
    if [ "$REQUIRED" = "1" ]; then
      exit 1
    fi
  fi
  sleep 2
done

echo "network-chaos: OK"
