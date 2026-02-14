#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COVER_FILE="${1:-_build/test/cover/eunit.coverdata}"
LINE_MIN="${FLURM_COVER_LINE_MIN:-90}"
CLAUSE_MIN="${FLURM_COVER_CLAUSE_MIN:-80}"
SCOPE="${FLURM_COVER_ADVANCED_SCOPE:-dbd}"

if [ ! -f "$COVER_FILE" ]; then
  echo "coverage-advanced: missing coverdata file: $COVER_FILE"
  exit 2
fi

export FLURM_COVER_FILE="$COVER_FILE"
export FLURM_COVER_LINE_MIN="$LINE_MIN"
export FLURM_COVER_CLAUSE_MIN="$CLAUSE_MIN"
export FLURM_COVER_ADVANCED_SCOPE="$SCOPE"

erl -noshell -pa _build/test/lib/*/ebin -eval '
CoverFile = os:getenv("FLURM_COVER_FILE"),
LineMinStr = os:getenv("FLURM_COVER_LINE_MIN", "90"),
ClauseMinStr = os:getenv("FLURM_COVER_CLAUSE_MIN", "80"),
Scope = os:getenv("FLURM_COVER_ADVANCED_SCOPE", "dbd"),
ToFloat = fun(S, D) ->
    case string:to_float(S) of
        {error, no_float} ->
            case string:to_integer(S) of
                {I, _} -> I + 0.0;
                _ -> D
            end;
        {F, _} -> F
    end
end,
LineMin = ToFloat(LineMinStr, 90.0),
ClauseMin = ToFloat(ClauseMinStr, 80.0),
ok = cover:import(CoverFile),
Mods = case Scope of
    "all" ->
        [
            flurm_dbd_fragment,
            flurm_dbd_acceptor,
            flurm_dbd_storage,
            flurm_dbd_mysql,
            flurm_dbd_server,
            flurm_preemption,
            flurm_reservation,
            flurm_scheduler_advanced,
            flurm_job_array,
            flurm_qos
        ];
    _ ->
        [
            flurm_dbd_fragment,
            flurm_dbd_acceptor,
            flurm_dbd_storage,
            flurm_dbd_mysql,
            flurm_dbd_server
        ]
end,
io:format("coverage-advanced: scope=~s line >= ~.2f%, clause >= ~.2f%~n", [Scope, LineMin, ClauseMin]),
Pct = fun(Cov, NotCov) ->
    Tot = Cov + NotCov,
    case Tot of 0 -> 0.0; _ -> (Cov * 100) / Tot end
end,
LinePct = fun(M) ->
    case catch cover:analyse(M, coverage, line) of
        {ok, L} ->
            Cov = lists:sum([C || {_, {C, _}} <- L]),
            NotCov = lists:sum([N || {_, {_, N}} <- L]),
            {ok, Pct(Cov, NotCov), Cov, Cov + NotCov};
        _ -> {error, unsupported}
    end
end,
ClausePct = fun(M) ->
    case catch cover:analyse(M, calls, clause) of
        {ok, L} ->
            Cov = lists:sum([case C > 0 of true -> 1; false -> 0 end || {_, C} <- L]),
            NotCov = lists:sum([case C > 0 of true -> 0; false -> 1 end || {_, C} <- L]),
            {ok, Pct(Cov, NotCov), Cov, Cov + NotCov};
        _ ->
            %% Fallback for OTP/tooling combinations lacking clause mode.
            case catch cover:analyse(M, calls, function) of
                {ok, F} ->
                    Cov = lists:sum([case C > 0 of true -> 1; false -> 0 end || {_, C} <- F]),
                    NotCov = lists:sum([case C > 0 of true -> 0; false -> 1 end || {_, C} <- F]),
                    {ok, Pct(Cov, NotCov), Cov, Cov + NotCov};
                _ -> {error, unsupported}
            end
    end
end,
Fails = lists:foldl(fun(M, Acc) ->
    case {LinePct(M), ClausePct(M)} of
        {{ok, LPct, LCov, LTot}, {ok, CPct, CCov, CTot}} ->
            io:format("  ~p line ~.2f% (~p/~p) clause ~.2f% (~p/~p)~n", [M, LPct, LCov, LTot, CPct, CCov, CTot]),
            case LPct >= LineMin andalso CPct >= ClauseMin of
                true -> Acc;
                false -> [M | Acc]
            end;
        _ ->
            io:format("  ~p unable to analyse advanced coverage~n", [M]),
            [M | Acc]
    end
end, [], Mods),
case Fails of
    [] ->
        io:format("coverage-advanced: OK~n"),
        halt(0);
    _ ->
        io:format("coverage-advanced: FAIL: ~p~n", [lists:reverse(Fails)]),
        halt(1)
end.
'
