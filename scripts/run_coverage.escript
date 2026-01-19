#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/test/lib/*/ebin

%% Custom coverage script that properly merges coverage across umbrella apps
%% Runs tests per-app to avoid meck interference with coverage
%% Usage: ./scripts/run_coverage.escript

-define(APPS, [flurm_protocol, flurm_config, flurm_core,
               flurm_db, flurm_controller, flurm_node_daemon, flurm_dbd]).

main(_Args) ->
    io:format("=== FLURM Coverage Analysis ===~n~n"),

    %% Ensure clean state
    os:cmd("rm -f /tmp/flurm_coverage_*.coverdata"),

    %% Run tests per app and save coverdata
    io:format("Running tests per app and collecting coverage...~n~n"),
    lists:foreach(fun(App) ->
        AppStr = atom_to_list(App),
        io:format("Testing ~s...~n", [AppStr]),
        CoverFile = "/tmp/flurm_coverage_" ++ AppStr ++ ".coverdata",
        Cmd = "cd /tmp/flurm_coverage && rebar3 eunit --app=" ++ AppStr ++ " --cover 2>&1",
        Output = os:cmd(Cmd),
        %% Extract test result line
        Lines = string:split(Output, "\n", all),
        TestLine = lists:filter(fun(L) ->
            string:find(L, "tests,") =/= nomatch
        end, Lines),
        case TestLine of
            [Result|_] -> io:format("  ~s~n", [string:trim(Result)]);
            [] -> io:format("  (no test output)~n")
        end,
        %% Export coverdata
        ExportCmd = "cd /tmp/flurm_coverage && cp _build/test/cover/eunit.coverdata " ++ CoverFile ++ " 2>/dev/null || true",
        os:cmd(ExportCmd)
    end, ?APPS),

    io:format("~nMerging coverage data...~n"),

    %% Start cover and import all coverdata
    cover:start(),

    %% Compile all modules for coverage from source
    SrcFiles = filelib:wildcard("/tmp/flurm_coverage/apps/*/src/*.erl"),
    lists:foreach(fun(File) ->
        case cover:compile(File, [{i, "/tmp/flurm_coverage/apps/flurm_protocol/include"},
                                  {i, "/tmp/flurm_coverage/apps/flurm_core/include"},
                                  {i, "/tmp/flurm_coverage/apps/flurm_config/include"}]) of
            {ok, _} -> ok;
            {error, Reason} ->
                io:format("  Warning: Could not compile ~s: ~p~n",
                         [filename:basename(File), Reason])
        end
    end, SrcFiles),

    %% Import all coverdata files
    CoverFiles = filelib:wildcard("/tmp/flurm_coverage_*.coverdata"),
    lists:foreach(fun(File) ->
        case cover:import(File) of
            ok -> io:format("  Imported: ~s~n", [filename:basename(File)]);
            {error, Reason} -> io:format("  Failed: ~s (~p)~n", [filename:basename(File), Reason])
        end
    end, CoverFiles),

    %% Analyze coverage
    io:format("~nAnalyzing coverage...~n~n"),
    Modules = cover:modules(),

    %% Filter to only FLURM modules
    FlurmModules = [M || M <- Modules,
                         lists:prefix("flurm_", atom_to_list(M)),
                         not lists:suffix("_meck_original", atom_to_list(M))],

    Results = lists:filtermap(fun(Mod) ->
        case cover:analyse(Mod, coverage, module) of
            {ok, {Mod, {Covered, NotCovered}}} ->
                Total = Covered + NotCovered,
                Pct = case Total of
                    0 -> 0;
                    _ -> round(Covered * 100 / Total)
                end,
                {true, {Mod, Pct, Covered, NotCovered}};
            _ ->
                false
        end
    end, FlurmModules),

    %% Sort by coverage percentage (ascending so 0% is at bottom)
    Sorted = lists:keysort(2, Results),

    %% Print results
    io:format("~-45s ~6s ~10s ~10s~n", ["Module", "Cover", "Lines", "Missing"]),
    io:format("~s~n", [lists:duplicate(75, $-)]),

    {TotalCovered, TotalMissing} = lists:foldl(fun({Mod, Pct, Cov, NotCov}, {AccCov, AccNot}) ->
        Color = if
            Pct >= 80 -> "\e[32m";  % Green
            Pct >= 50 -> "\e[33m";  % Yellow
            Pct > 0 -> "\e[36m";    % Cyan
            true -> "\e[31m"        % Red
        end,
        io:format("~s~-45s ~5B% ~10B ~10B\e[0m~n",
                  [Color, Mod, Pct, Cov, NotCov]),
        {AccCov + Cov, AccNot + NotCov}
    end, {0, 0}, lists:reverse(Sorted)),

    %% Print summary
    TotalLines = TotalCovered + TotalMissing,
    TotalPct = case TotalLines of
        0 -> 0;
        _ -> round(TotalCovered * 100 / TotalLines)
    end,

    io:format("~s~n", [lists:duplicate(75, $-)]),
    io:format("~-45s ~5B% ~10B ~10B~n", ["TOTAL", TotalPct, TotalCovered, TotalMissing]),
    io:format("~n"),

    %% Print summary stats
    {High, Med, Low, Zero} = lists:foldl(fun({_, Pct, _, _}, {H, M, L, Z}) ->
        if
            Pct >= 80 -> {H + 1, M, L, Z};
            Pct >= 50 -> {H, M + 1, L, Z};
            Pct > 0 -> {H, M, L + 1, Z};
            true -> {H, M, L, Z + 1}
        end
    end, {0, 0, 0, 0}, Sorted),

    io:format("Summary:~n"),
    io:format("  \e[32m>=80%%: ~B modules\e[0m~n", [High]),
    io:format("  \e[33m50-79%%: ~B modules\e[0m~n", [Med]),
    io:format("  \e[36m1-49%%: ~B modules\e[0m~n", [Low]),
    io:format("  \e[31m0%%: ~B modules\e[0m~n", [Zero]),
    io:format("  Total: ~B modules~n", [length(Sorted)]),

    ok.
