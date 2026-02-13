#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CADENCE="${1:-short}"

case "$CADENCE" in
  short)
    echo "soak-cadence: short (5 min)"
    make test-soak-short
    ;;
  standard)
    echo "soak-cadence: standard (30 min)"
    make test-soak
    ;;
  long)
    DURATION_MS="${FLURM_SOAK_DURATION_MS:-7200000}"
    echo "soak-cadence: long (${DURATION_MS}ms)"
    rebar3 shell --eval " \
      io:format(\"~n=== SOAK TEST (long) ===~n\"), \
      Result = flurm_load_test:soak_test(${DURATION_MS}), \
      io:format(\"~nJobs: ~p, Memory growth: ~p bytes, Alerts: ~p~n\", \
                [maps:get(jobs_submitted, Result), maps:get(memory_growth_bytes, Result), maps:get(leak_alerts, Result)]), \
      halt(case maps:get(leak_alerts, Result) of 0 -> 0; _ -> 1 end)."
    ;;
  *)
    echo "usage: $0 [short|standard|long]" >&2
    exit 2
    ;;
esac

echo "soak-cadence: $CADENCE OK"
