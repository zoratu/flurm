#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "ci-local: deterministic local CI run"

./scripts/check-consistency.sh prepush
./scripts/run-deterministic-model-tests.sh
./scripts/check-coverage-dbd-100.sh
rebar3 eunit --module=flurm_protocol_codec_direct_tests,flurm_controller_acceptor_pure_tests,flurm_srun_acceptor_tests
if [ "${FLURM_CI_LOCAL_NETWORK_FAULT:-0}" = "1" ]; then
  FLURM_NETWORK_FAULT_REQUIRED=1 ./scripts/run-network-fault-injection-tests.sh
fi
if [ "${FLURM_CI_LOCAL_UPGRADE_REPLAY:-0}" = "1" ]; then
  FLURM_UPGRADE_REPLAY_REQUIRED=1 ./scripts/run-upgrade-rollback-replay-tests.sh
fi
if [ "${FLURM_CI_LOCAL_SOAK:-0}" = "1" ]; then
  ./scripts/run-soak-cadence.sh "${FLURM_CI_LOCAL_SOAK_MODE:-short}"
fi
./scripts/run_coverage.escript

echo "ci-local: OK"
