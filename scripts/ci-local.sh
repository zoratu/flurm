#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "ci-local: deterministic local CI run"

./scripts/check-consistency.sh prepush
./scripts/check-coverage-dbd-100.sh
rebar3 eunit --module=flurm_protocol_codec_direct_tests,flurm_controller_acceptor_pure_tests,flurm_srun_acceptor_tests
./scripts/run_coverage.escript

echo "ci-local: OK"
