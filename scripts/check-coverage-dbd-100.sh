#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COVER_FILE="${1:-_build/test/cover/eunit.coverdata}"
export FLURM_COVER_MIN=100

./scripts/check-coverage-threshold.sh "$COVER_FILE"
