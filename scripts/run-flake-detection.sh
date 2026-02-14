#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${1:-rebar3 eunit --module=flurm_lifecycle_model_tests}"
RUNS="${FLURM_FLAKE_RUNS:-20}"
REPORT_DIR="${FLURM_FLAKE_REPORT_DIR:-$ROOT/test-results/flake}"
mkdir -p "$REPORT_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="$REPORT_DIR/flake-${STAMP}.json"
LOG="$REPORT_DIR/flake-${STAMP}.log"

pass=0
fail=0

echo "flake-detection: target='$TARGET' runs=$RUNS"
for i in $(seq 1 "$RUNS"); do
  echo "[run $i/$RUNS] $TARGET" | tee -a "$LOG"
  if bash -lc "$TARGET" >>"$LOG" 2>&1; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
  fi
done

rate=0
if [ "$RUNS" -gt 0 ]; then
  rate=$((fail * 100 / RUNS))
fi

cat > "$REPORT" <<JSON
{
  "timestamp": "$(date -Iseconds)",
  "target": $(printf '%s' "$TARGET" | sed 's/"/\\"/g' | awk '{printf "\"%s\"", $0}'),
  "runs": $RUNS,
  "passed": $pass,
  "failed": $fail,
  "flake_rate_pct": $rate,
  "log_file": "$LOG"
}
JSON

echo "flake-detection: passed=$pass failed=$fail rate=${rate}%"
if [ "$fail" -gt 0 ]; then
  echo "flake-detection: FAIL"
  exit 1
fi

echo "flake-detection: OK"
