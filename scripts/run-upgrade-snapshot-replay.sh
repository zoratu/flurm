#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SNAP_DIR="${FLURM_MIGRATION_SNAPSHOT_DIR:-$ROOT/test/upgrade-snapshots}"
REQUIRED="${FLURM_UPGRADE_REPLAY_REQUIRED:-0}"

skip_or_fail() {
  local msg="$1"
  if [ "$REQUIRED" = "1" ]; then
    echo "upgrade-snapshot-replay: REQUIRED and cannot run: $msg" >&2
    exit 1
  fi
  echo "upgrade-snapshot-replay: SKIP ($msg)"
  exit 0
}

if [ ! -d "$SNAP_DIR" ]; then
  skip_or_fail "snapshot directory not found: $SNAP_DIR"
fi

shopt -s nullglob
snapshots=("$SNAP_DIR"/*.snapshot "$SNAP_DIR"/*.tgz "$SNAP_DIR"/*.tar.gz)
shopt -u nullglob

if [ "${#snapshots[@]}" -eq 0 ]; then
  skip_or_fail "no snapshot files in $SNAP_DIR"
fi

echo "upgrade-snapshot-replay: found ${#snapshots[@]} snapshot(s)"

for snap in "${snapshots[@]}"; do
  base="$(basename "$snap")"
  echo "upgrade-snapshot-replay: replaying $base"

  # Stage 1: rollback replay on persisted snapshot state.
  FLURM_MIGRATION_SNAPSHOT_FILE="$snap" ./scripts/run-migration-e2e-tests.sh --stage rollback

  # Stage 2: continuity replay on same snapshot.
  FLURM_MIGRATION_SNAPSHOT_FILE="$snap" ./scripts/run-migration-e2e-tests.sh --stage continuity

  # Stage 3: hot upgrade replay validation.
  ./test/hot_upgrade_e2e_test.sh all
done

echo "upgrade-snapshot-replay: OK"
