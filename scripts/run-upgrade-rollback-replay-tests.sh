#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REQUIRED="${FLURM_UPGRADE_REPLAY_REQUIRED:-0}"

skip_or_fail() {
  local msg="$1"
  if [ "$REQUIRED" = "1" ]; then
    echo "upgrade-replay: REQUIRED and cannot run: $msg" >&2
    exit 1
  fi
  echo "upgrade-replay: SKIP ($msg)"
  exit 0
}

if ! command -v docker >/dev/null 2>&1; then
  skip_or_fail "docker not installed"
fi
if ! docker info >/dev/null 2>&1; then
  skip_or_fail "docker daemon unavailable"
fi

echo "upgrade-replay: running rollback replay on persisted migration state"
./scripts/run-migration-e2e-tests.sh --stage rollback

echo "upgrade-replay: running continuity replay on persisted migration state"
./scripts/run-migration-e2e-tests.sh --stage continuity

echo "upgrade-replay: running hot upgrade/rollback e2e"
./test/hot_upgrade_e2e_test.sh all

if [ "${FLURM_RUN_UPGRADE_SNAPSHOT_REPLAY:-0}" = "1" ]; then
  echo "upgrade-replay: running persisted snapshot compatibility replay"
  ./scripts/run-upgrade-snapshot-replay.sh
fi

echo "upgrade-replay: OK"
