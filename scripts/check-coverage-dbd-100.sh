#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# By default we run and merge two coverage passes:
# 1) full flurm_dbd app
# 2) targeted tail modules that hit RA/SUP wrapper paths after mock-heavy suites
RUN_MERGED="${FLURM_DBD_COVER_RUN_MERGED:-1}"
TAIL_MODULES="${FLURM_DBD_COVER_TAIL_MODULES:-zzzz_flurm_dbd_ra_api_wrappers_tests,zzzz_flurm_dbd_sup_ra_paths_tests}"
COVER_FILE="${1:-_build/test/cover/eunit.coverdata}"

if [ "$RUN_MERGED" = "1" ]; then
  TMP_DIR="$(mktemp -d /tmp/flurm_dbd_cover.XXXXXX)"
  trap 'rm -rf "$TMP_DIR"' EXIT

  echo "check-coverage-dbd-100: running flurm_dbd baseline coverage"
  rebar3 as test eunit --app=flurm_dbd --cover
  cp _build/test/cover/eunit.coverdata "$TMP_DIR/full.coverdata"

  echo "check-coverage-dbd-100: running tail module coverage ($TAIL_MODULES)"
  rebar3 as test eunit --cover --module="$TAIL_MODULES"
  cp _build/test/cover/eunit.coverdata "$TMP_DIR/tail.coverdata"

  export FLURM_COVER_FULL="$TMP_DIR/full.coverdata"
  export FLURM_COVER_TAIL="$TMP_DIR/tail.coverdata"
  export FLURM_COVER_MERGED="$TMP_DIR/merged.coverdata"

  erl -noshell -pa _build/test/lib/*/ebin -eval '
  Full = os:getenv("FLURM_COVER_FULL"),
  Tail = os:getenv("FLURM_COVER_TAIL"),
  Merged = os:getenv("FLURM_COVER_MERGED"),
  cover:start(),
  ok = cover:import(Full),
  ok = cover:import(Tail),
  ok = cover:export(Merged),
  halt(0).
  '

  COVER_FILE="$TMP_DIR/merged.coverdata"
fi

export FLURM_COVER_MIN=100

./scripts/check-coverage-threshold.sh "$COVER_FILE"
