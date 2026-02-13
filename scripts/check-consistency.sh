#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODE="${1:-full}"

case "$MODE" in
  quick)
    echo "consistency: quick"
    rebar3 compile
    rebar3 eunit --module=flurm_protocol_header,flurm_protocol_pack,flurm_job_executor_pure_tests,flurm_pmi_protocol_tests
    ;;
  prepush)
    echo "consistency: prepush"
    rebar3 as test eunit --app=flurm_dbd --cover
    ./scripts/check-coverage-threshold.sh
    rebar3 eunit --module=flurm_quality_gap_tests,flurm_fault_injection_tests
    rebar3 proper -m flurm_property_tests -n "${FLURM_PREPUSH_PROPER_N:-25}"
    ;;
  full)
    echo "consistency: full"
    rebar3 compile
    rebar3 eunit --cover
    ./scripts/check-coverage-threshold.sh
    rebar3 ct --cover
    if [ "${FLURM_CHECK_DOCKER:-0}" = "1" ]; then
      ./scripts/run-slurm-interop-tests.sh --quick
    fi
    ;;
  *)
    echo "usage: $0 [quick|prepush|full]"
    exit 2
    ;;
esac

echo "consistency: $MODE OK"
