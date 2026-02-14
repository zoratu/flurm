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
    FLURM_COVER_ADVANCED_SCOPE=dbd ./scripts/check-coverage-advanced.sh
    rebar3 eunit --module=flurm_quality_gap_tests,flurm_fault_injection_tests
    rebar3 proper -m flurm_property_tests -n "${FLURM_PREPUSH_PROPER_N:-25}"
    if [ "${FLURM_CHECK_MODEL_DETERMINISTIC:-0}" = "1" ]; then
      ./scripts/run-deterministic-model-tests.sh
    fi
    ;;
  full)
    echo "consistency: full"
    rebar3 compile
    rebar3 eunit --cover
    ./scripts/check-coverage-threshold.sh
    FLURM_COVER_ADVANCED_SCOPE=all ./scripts/check-coverage-advanced.sh
    rebar3 ct --cover
    if [ "${FLURM_CHECK_MODEL_DETERMINISTIC:-1}" = "1" ]; then
      ./scripts/run-deterministic-model-tests.sh
    fi
    if [ "${FLURM_CHECK_NETWORK_FAULT:-0}" = "1" ]; then
      FLURM_NETWORK_FAULT_REQUIRED=1 ./scripts/run-network-fault-injection-tests.sh
    fi
    if [ "${FLURM_CHECK_UPGRADE_REPLAY:-0}" = "1" ]; then
      FLURM_UPGRADE_REPLAY_REQUIRED=1 ./scripts/run-upgrade-rollback-replay-tests.sh
    fi
    if [ "${FLURM_CHECK_SOAK_CADENCE:-0}" = "1" ]; then
      ./scripts/run-soak-cadence.sh "${FLURM_SOAK_CADENCE:-short}"
    fi
    if [ "${FLURM_CHECK_MUTATION_SANITY:-0}" = "1" ]; then
      ./scripts/run-mutation-sanity.sh
    fi
    if [ "${FLURM_CHECK_FLAKE_DETECTION:-0}" = "1" ]; then
      ./scripts/run-flake-detection.sh
    fi
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
