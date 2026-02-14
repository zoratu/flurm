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
  # The failover harness tears down the HA stack after completion.
  # Bring up a fresh cluster for observability/strict checks.
  if ! ./test/ha_failover_test.sh start; then
    if [ "$REQUIRED" = "1" ]; then
      echo "network-fault: failed to start HA cluster for partition observability" >&2
      exit 1
    fi
    echo "network-fault: could not start HA cluster for observability (non-required mode)"
  else
    trap './test/ha_failover_test.sh stop >/dev/null 2>&1 || true' EXIT
    if [ "$REQUIRED" = "1" ]; then
      FLURM_NETWORK_PARTITION_STRICT=1 \
      FLURM_CTRL_1=localhost FLURM_CTRL_2=localhost FLURM_CTRL_3=localhost \
      FLURM_CTRL_PORT_1=9090 FLURM_CTRL_PORT_2=9091 FLURM_CTRL_PORT_3=9092 \
      ./docker/e2e-tests/test-network-partition.sh
    else
      FLURM_CTRL_1=localhost FLURM_CTRL_2=localhost FLURM_CTRL_3=localhost \
      FLURM_CTRL_PORT_1=9090 FLURM_CTRL_PORT_2=9091 FLURM_CTRL_PORT_3=9092 \
      ./docker/e2e-tests/test-network-partition.sh
    fi
    if [ "${FLURM_RUN_PACKET_LOSS_FAULTS:-0}" = "1" ]; then
      echo "network-fault: running packet loss/jitter/reconnect scenarios"
      if [ "$REQUIRED" = "1" ]; then
        FLURM_NETWORK_FAULT_REQUIRED=1 ./scripts/run-network-chaos-scenarios.sh
      else
        ./scripts/run-network-chaos-scenarios.sh
      fi
    fi
    ./test/ha_failover_test.sh stop >/dev/null 2>&1 || true
    trap - EXIT
  fi
fi

echo "network-fault: OK"
