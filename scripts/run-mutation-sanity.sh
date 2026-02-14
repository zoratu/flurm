#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WORKDIR="${FLURM_MUTATION_WORKDIR:-/tmp/flurm-mutation}"
TIMEOUT_SEC="${FLURM_MUTATION_TIMEOUT_SEC:-180}"
mkdir -p "$WORKDIR"

# mutation_case "module_path" "search" "replace" "test_cmd"
run_with_timeout() {
  local cmd="$1"
  local timeout="$2"
  local log_file="$3"

  bash -lc "$cmd" >"$log_file" 2>&1 &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$timeout" ]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$pid"
}

mutation_case() {
  local rel_path="$1"
  local from="$2"
  local to="$3"
  local test_cmd="$4"

  local abs="$ROOT/$rel_path"
  local backup="$WORKDIR/$(basename "$rel_path").bak"

  if [ ! -f "$abs" ]; then
    echo "mutation-sanity: missing file $rel_path"
    return 1
  fi

  cp "$abs" "$backup"
  if ! grep -qF "$from" "$abs"; then
    echo "mutation-sanity: pattern not found in $rel_path"
    cp "$backup" "$abs"
    return 1
  fi
  perl -0pi -e "s/\Q$from\E/$to/s" "$abs"

  local log_file="/tmp/flurm-mutation-$(basename "$rel_path").log"
  if run_with_timeout "$test_cmd" "$TIMEOUT_SEC" "$log_file"; then
    echo "mutation-sanity: SURVIVED $rel_path ($from -> $to)"
    cp "$backup" "$abs"
    return 1
  elif [ $? -eq 124 ]; then
    echo "mutation-sanity: TIMEOUT $rel_path after ${TIMEOUT_SEC}s"
    cp "$backup" "$abs"
    return 1
  else
    echo "mutation-sanity: KILLED $rel_path ($from -> $to)"
    cp "$backup" "$abs"
    return 0
  fi
}

killed=0
survived=0

if mutation_case "apps/flurm_protocol/src/flurm_protocol_codec.erl" "Length = byte_size(HeaderBin) + byte_size(BodyBin)," "Length = byte_size(HeaderBin) + byte_size(BodyBin) + 1," "rebar3 eunit --module=flurm_protocol_codec_direct_tests"; then
  killed=$((killed + 1))
else
  survived=$((survived + 1))
fi

if mutation_case "apps/flurm_core/src/flurm_job_array.erl" "completed => length([S || S <- States, S =:= completed])," "completed => 0," "rebar3 eunit --module=flurm_job_array_tests"; then
  killed=$((killed + 1))
else
  survived=$((survived + 1))
fi

if mutation_case "apps/flurm_dbd/src/flurm_dbd_acceptor.erl" "handle_message(MsgData, _State) when byte_size(MsgData) < 2 ->" "handle_message(MsgData, _State) when byte_size(MsgData) < 1 ->" "rebar3 eunit --module=flurm_dbd_acceptor_cover_tests,flurm_dbd_acceptor_direct_tests"; then
  killed=$((killed + 1))
else
  survived=$((survived + 1))
fi

echo "mutation-sanity: killed=$killed survived=$survived"
if [ "$survived" -gt 0 ]; then
  exit 1
fi

echo "mutation-sanity: OK"
