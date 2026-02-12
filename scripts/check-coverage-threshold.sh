#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COVER_FILE="${1:-_build/test/cover/eunit.coverdata}"
MIN="${FLURM_COVER_MIN:-90}"

if [ ! -f "$COVER_FILE" ]; then
  echo "coverage-threshold: missing coverdata file: $COVER_FILE"
  exit 2
fi

export FLURM_COVER_FILE="$COVER_FILE"
export FLURM_COVER_MIN="$MIN"

erl -noshell -pa _build/test/lib/*/ebin -eval '
CoverFile = os:getenv("FLURM_COVER_FILE"),
MinStr = os:getenv("FLURM_COVER_MIN", "90"),
Min = case string:to_float(MinStr) of
    {error, no_float} ->
        case string:to_integer(MinStr) of
            {I, _} -> I + 0.0;
            _ -> 90.0
        end;
    {F, _} -> F
end,
ok = cover:import(CoverFile),
Mods = [flurm_dbd_fragment, flurm_dbd_acceptor, flurm_dbd_storage, flurm_dbd_mysql, flurm_dbd_server],
io:format("coverage-threshold: minimum ~.2f%~n", [Min]),
Fails = lists:foldl(fun(M, Acc) ->
    {ok, L} = cover:analyse(M, coverage, line),
    Cov = lists:sum([C || {_, {C, _}} <- L]),
    NotCov = lists:sum([N || {_, {_, N}} <- L]),
    Tot = Cov + NotCov,
    Pct = case Tot of 0 -> 0.0; _ -> (Cov * 100) / Tot end,
    io:format("  ~p ~.2f% (~p/~p)~n", [M, Pct, Cov, Tot]),
    case Pct < Min of
        true -> [M | Acc];
        false -> Acc
    end
end, [], Mods),
case Fails of
    [] ->
        io:format("coverage-threshold: OK~n"),
        halt(0);
    _ ->
        io:format("coverage-threshold: FAIL (below threshold): ~p~n", [lists:reverse(Fails)]),
        halt(1)
end.
'
