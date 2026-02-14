#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CADENCE="${1:-short}"

case "$CADENCE" in
  short)
    DURATION_MS="${FLURM_SOAK_DURATION_MS_SHORT:-300000}"
    MAX_GROWTH="${FLURM_SOAK_MAX_GROWTH_BYTES_SHORT:-536870912}"
    ;;
  standard)
    DURATION_MS="${FLURM_SOAK_DURATION_MS_STANDARD:-1800000}"
    MAX_GROWTH="${FLURM_SOAK_MAX_GROWTH_BYTES_STANDARD:-2147483648}"
    ;;
  long)
    DURATION_MS="${FLURM_SOAK_DURATION_MS:-7200000}"
    MAX_GROWTH="${FLURM_SOAK_MAX_GROWTH_BYTES_LONG:-4294967296}"
    ;;
  *)
    echo "usage: $0 [short|standard|long]" >&2
    exit 2
    ;;
esac

MAX_ALERTS="${FLURM_SOAK_MAX_ALERTS:-0}"
MIN_JOBS="${FLURM_SOAK_MIN_JOBS:-1}"

echo "soak-cadence: ${CADENCE} (${DURATION_MS}ms, max_growth=${MAX_GROWTH}, max_alerts=${MAX_ALERTS}, min_jobs=${MIN_JOBS})"
rebar3 compile
rebar3 shell --eval " \
  io:format(\"~n=== SOAK TEST (${CADENCE}) ===~n\"), \
  Result = flurm_load_test:soak_test(${DURATION_MS}), \
  Jobs = maps:get(jobs_submitted, Result, 0), \
  Growth = maps:get(memory_growth_bytes, Result, 0), \
  Alerts = maps:get(leak_alerts, Result, 0), \
  io:format(\"~nJobs: ~p, Memory growth: ~p bytes, Alerts: ~p~n\", [Jobs, Growth, Alerts]), \
  Pass = (Jobs >= ${MIN_JOBS}) andalso (Growth =< ${MAX_GROWTH}) andalso (Alerts =< ${MAX_ALERTS}), \
  io:format(\"Threshold check: ~p~n\", [Pass]), \
  halt(case Pass of true -> 0; false -> 1 end)."

echo "soak-cadence: $CADENCE OK"
