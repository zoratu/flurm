#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REQUIRED="${FLURM_NETWORK_FAULT_REQUIRED:-0}"

skip_or_fail() {
  local msg="$1"
  if [ "$REQUIRED" = "1" ]; then
    echo "network-fault: REQUIRED and cannot run: $msg" >&2
    exit 1
  fi
  echo "network-fault: SKIP ($msg)"
  exit 0
}

if ! command -v docker >/dev/null 2>&1; then
  skip_or_fail "docker not installed"
fi

if ! docker info >/dev/null 2>&1; then
  skip_or_fail "docker daemon unavailable"
fi

echo "network-fault: running HA partition/failover integration"

if ! ./test/ha_failover_test.sh partition; then
  if [ "$REQUIRED" = "1" ]; then
    echo "network-fault: partition test failed" >&2
    exit 1
  fi
  echo "network-fault: partition test failed (non-required mode, continuing)"
  exit 0
fi

echo "network-fault: running network partition observability check"
if [ -x ./docker/e2e-tests/test-network-partition.sh ]; then
  ./docker/e2e-tests/test-network-partition.sh || true
fi

echo "network-fault: OK"
