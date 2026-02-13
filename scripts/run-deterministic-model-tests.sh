#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "deterministic-model: running reproducible lifecycle/model suites"

rebar3 eunit --module=flurm_lifecycle_model_tests

echo "deterministic-model: OK"
