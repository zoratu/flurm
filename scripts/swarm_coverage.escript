#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% Swarm Testing Coverage Merger & Analyzer
%% =========================================
%% Merges coverdata files from multiple swarm configurations and
%% analyzes which feature omissions led to new coverage.
%%
%% Based on: Groce et al. "Swarm Testing" (ISSTA 2012)
%%
%% Usage:
%%   ./scripts/swarm_coverage.escript merge DIR     # Merge all .coverdata files
%%   ./scripts/swarm_coverage.escript analyze DIR   # Analyze feature significance
%%   ./scripts/swarm_coverage.escript generate N    # Generate N configs (Erlang-native)
%%

main(["generate", NStr]) ->
    N = list_to_integer(NStr),
    generate_swarm_configs(N);

main(["merge", Dir]) ->
    merge_coverdata(Dir);

main(["analyze", Dir]) ->
    analyze_coverage(Dir);

main(_) ->
    io:format("Swarm Testing Coverage Tool~n"),
    io:format("~nUsage:~n"),
    io:format("  ~s generate N    Generate N swarm configs~n", [escript:script_name()]),
    io:format("  ~s merge DIR     Merge coverdata files~n", [escript:script_name()]),
    io:format("  ~s analyze DIR   Analyze feature significance~n", [escript:script_name()]),
    halt(1).

%% ============================================================================
%% Configuration Generation
%% ============================================================================

generate_swarm_configs(N) ->
    %% Discover all test modules
    ProjectDir = get_project_dir(),
    Modules = discover_test_modules(ProjectDir),
    Total = length(Modules),

    io:format("=== Swarm Configuration Generator ===~n"),
    io:format("Total test modules (features): ~p~n", [Total]),
    io:format("Generating ~p configurations with 50% coin-toss...~n~n", [N]),

    %% Seed random number generator
    rand:seed(exsp),

    SwarmDir = filename:join([ProjectDir, "_build", "swarm", "configs"]),
    filelib:ensure_dir(filename:join(SwarmDir, "dummy")),

    %% Generate N configurations
    Configs = lists:map(fun(I) ->
        %% Coin-toss: each module included with 50% probability
        Included = [M || M <- Modules, rand:uniform() < 0.5],

        %% Ensure minimum size (at least 10 modules)
        Config = case length(Included) < 10 of
            true ->
                Excluded = Modules -- Included,
                Extra = lists:sublist(shuffle(Excluded), 10 - length(Included)),
                Included ++ Extra;
            false ->
                Included
        end,

        %% Write config file
        ConfigFile = filename:join(SwarmDir, io_lib:format("config_~p.txt", [I])),
        ConfigData = string:join([atom_to_list(M) || M <- Config], "\n") ++ "\n",
        ok = file:write_file(iolist_to_binary(ConfigFile), list_to_binary(ConfigData)),

        io:format("  Config ~p: ~p/~p modules (~p omitted)~n",
                  [I, length(Config), Total, Total - length(Config)]),
        {I, Config}
    end, lists:seq(1, N)),

    %% Write default config (C_D = all features)
    DefaultFile = filename:join(SwarmDir, "config_default.txt"),
    DefaultData = string:join([atom_to_list(M) || M <- Modules], "\n") ++ "\n",
    ok = file:write_file(iolist_to_binary(DefaultFile), list_to_binary(DefaultData)),
    io:format("~n  Default (C_D): ~p/~p modules~n", [Total, Total]),

    %% Analyze diversity
    analyze_config_diversity(Configs, Modules),
    ok.

analyze_config_diversity(Configs, AllModules) ->
    N = length(Configs),
    Total = length(AllModules),

    %% For each module, count how many configs include it
    ModuleCounts = lists:map(fun(M) ->
        Count = length([1 || {_, Config} <- Configs, lists:member(M, Config)]),
        {M, Count}
    end, AllModules),

    %% Statistics
    Counts = [C || {_, C} <- ModuleCounts],
    Avg = lists:sum(Counts) / length(Counts),
    Min = lists:min(Counts),
    Max = lists:max(Counts),

    io:format("~n=== Configuration Diversity ===~n"),
    io:format("  Modules per config: avg ~.1f / ~p~n", [Avg, Total]),
    io:format("  Module inclusion range: ~p - ~p (out of ~p configs)~n", [Min, Max, N]),

    %% Check pair coverage (how many pairs of modules appear together)
    %% From paper: with 50% coin-toss, probability of any pair appearing = 0.25 per config
    %% With N configs, prob of pair never appearing = (1 - 0.25)^N
    PairMissProb = math:pow(0.75, N),
    io:format("  Probability of missing any module pair: ~.6f~n", [PairMissProb]),
    io:format("  Expected pair coverage: ~.1f%~n", [(1 - PairMissProb) * 100]),
    ok.

%% ============================================================================
%% Coverage Merging
%% ============================================================================

merge_coverdata(Dir) ->
    io:format("=== Merging Swarm Coverage Data ===~n"),
    io:format("Directory: ~s~n~n", [Dir]),

    %% Find all .coverdata files
    CovFiles = filelib:wildcard(filename:join(Dir, "**/*.coverdata")),

    case CovFiles of
        [] ->
            io:format("ERROR: No .coverdata files found in ~s~n", [Dir]),
            halt(1);
        _ ->
            io:format("Found ~p coverdata files:~n", [length(CovFiles)]),
            lists:foreach(fun(F) ->
                io:format("  ~s~n", [F])
            end, CovFiles)
    end,

    %% Start cover
    cover:start(),

    %% Import all coverdata files
    io:format("~nImporting coverage data...~n"),
    lists:foreach(fun(File) ->
        case cover:import(File) of
            ok ->
                io:format("  Imported: ~s~n", [filename:basename(File)]);
            {error, Reason} ->
                io:format("  FAILED: ~s (~p)~n", [filename:basename(File), Reason])
        end
    end, CovFiles),

    %% Get merged coverage
    io:format("~n=== Merged Coverage Report ===~n"),
    Modules = cover:imported_modules(),
    io:format("Modules with coverage data: ~p~n~n", [length(Modules)]),

    %% Calculate per-module coverage
    Results = lists:filtermap(fun(Mod) ->
        case cover:analyse(Mod, coverage, module) of
            {ok, {Mod, {Covered, NotCovered}}} ->
                Total = Covered + NotCovered,
                Pct = case Total of
                    0 -> 0.0;
                    _ -> Covered / Total * 100
                end,
                {true, {Mod, Pct, Covered, NotCovered}};
            _ ->
                false
        end
    end, lists:sort(Modules)),

    %% Print results
    io:format("  ~-40s ~8s ~10s ~10s~n", ["Module", "Coverage", "Covered", "NotCovered"]),
    io:format("  ~s~n", [lists:duplicate(72, $-)]),

    {TotalCov, TotalNot} = lists:foldl(fun({Mod, Pct, Cov, Not}, {AccCov, AccNot}) ->
        io:format("  ~-40s ~6.1f% ~10p ~10p~n", [Mod, Pct, Cov, Not]),
        {AccCov + Cov, AccNot + Not}
    end, {0, 0}, Results),

    TotalLines = TotalCov + TotalNot,
    TotalPct = case TotalLines of
        0 -> 0.0;
        _ -> TotalCov / TotalLines * 100
    end,

    io:format("  ~s~n", [lists:duplicate(72, $-)]),
    io:format("  ~-40s ~6.1f% ~10p ~10p~n", ["TOTAL", TotalPct, TotalCov, TotalNot]),

    %% Export merged coverdata
    MergedFile = filename:join(Dir, "merged.coverdata"),
    cover:export(MergedFile),
    io:format("~nMerged coverdata written to: ~s~n", [MergedFile]),

    %% Compare with individual configs
    io:format("~n=== Swarm vs Default Coverage ===~n"),
    io:format("Merged (swarm): ~.1f%~n", [TotalPct]),
    io:format("Individual coverdata files contributed to this merged total.~n"),
    io:format("~nKey insight (Groce et al.): Swarm testing with diverse feature~n"),
    io:format("omission covers more code paths than a single 'all features' run~n"),
    io:format("because feature omission prevents active and passive suppression.~n"),

    cover:stop(),
    ok.

%% ============================================================================
%% Feature Significance Analysis
%% ============================================================================

analyze_coverage(Dir) ->
    io:format("=== Feature Significance Analysis ===~n"),
    io:format("(Which modules, when omitted, correlate with higher coverage)~n~n"),

    %% This would require per-config coverage data + config files
    %% For now, show the methodology
    io:format("Methodology (from Groce et al.):~n"),
    io:format("  1. For each test module M, partition configs into:~n"),
    io:format("     - C_with: configs that include M~n"),
    io:format("     - C_without: configs that omit M~n"),
    io:format("  2. Compare average coverage between C_with and C_without~n"),
    io:format("  3. If C_without has higher coverage, M is a 'suppressor'~n"),
    io:format("  4. If C_with has higher coverage, M is a 'trigger'~n"),
    io:format("~n"),
    io:format("Run 'swarm-test.sh run-spot 20' to collect data for this analysis.~n"),
    ok.

%% ============================================================================
%% Helpers
%% ============================================================================

get_project_dir() ->
    ScriptDir = filename:dirname(escript:script_name()),
    filename:dirname(ScriptDir).

discover_test_modules(ProjectDir) ->
    Pattern = filename:join([ProjectDir, "apps", "*", "test", "*_tests.erl"]),
    Files = filelib:wildcard(Pattern),
    [list_to_atom(filename:basename(F, ".erl")) || F <- lists:sort(Files)].

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- List])].
