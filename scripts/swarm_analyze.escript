#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% Swarm Testing Suppressor/Trigger Analysis
%% ==========================================
%% Analyzes swarm test results to identify which test modules
%% act as "triggers" (their presence correlates with finding bugs)
%% and "suppressors" (their presence prevents other bugs from being found).
%%
%% Based on: Groce et al. "Swarm Testing" (ISSTA 2012), Section 3.1.2
%%
%% Usage:
%%   ./scripts/swarm_analyze.escript <results_dir> <configs_dir>
%%

main([ResultsDir, ConfigsDir]) ->
    io:format("~n========================================~n"),
    io:format("  Swarm Testing: Feature Significance~n"),
    io:format("  (Groce et al. ISSTA 2012)~n"),
    io:format("========================================~n~n"),

    %% Load all config files
    Configs = load_configs(ConfigsDir),
    io:format("Loaded ~p configurations~n", [length(Configs)]),

    %% Load all result files
    Results = load_results(ResultsDir),
    io:format("Loaded ~p result sets~n~n", [length(Results)]),

    %% Get all unique modules across all configs
    AllModules = lists:usort(lists:flatten([Mods || {_, Mods} <- Configs])),
    io:format("Total unique modules (features): ~p~n~n", [length(AllModules)]),

    %% For each module, compute:
    %% 1. Average failure rate when module is PRESENT
    %% 2. Average failure rate when module is ABSENT
    %% 3. Trigger score = how much presence increases failures found
    %% 4. Suppressor score = how much presence decreases failures found
    Analysis = lists:filtermap(fun(Module) ->
        %% Partition configs into with/without this module
        WithConfigs = [Id || {Id, Mods} <- Configs, lists:member(Module, Mods)],
        WithoutConfigs = [Id || {Id, Mods} <- Configs, not lists:member(Module, Mods)],

        %% Get failure rates for each partition
        WithFailures = get_failure_rates(WithConfigs, Results),
        WithoutFailures = get_failure_rates(WithoutConfigs, Results),

        case {WithFailures, WithoutFailures} of
            {[], _} -> false;
            {_, []} -> false;
            _ ->
                AvgWith = avg(WithFailures),
                AvgWithout = avg(WithoutFailures),
                %% Positive = trigger (presence finds more failures)
                %% Negative = suppressor (presence prevents failures)
                Delta = AvgWith - AvgWithout,
                {true, {Module, Delta, AvgWith, AvgWithout,
                         length(WithConfigs), length(WithoutConfigs)}}
        end
    end, AllModules),

    %% Sort by delta (most suppressing first)
    Sorted = lists:sort(fun({_, D1, _, _, _, _}, {_, D2, _, _, _, _}) ->
        D1 < D2
    end, Analysis),

    %% Print top suppressors
    io:format("=== Top 20 SUPPRESSORS ===~n"),
    io:format("(Modules whose PRESENCE correlates with FEWER failures found)~n"),
    io:format("(These modules may prevent other bugs from being detected)~n~n"),
    io:format("  ~-50s ~8s ~10s ~10s ~6s~n",
              ["Module", "Delta", "With", "Without", "In"]),
    io:format("  ~s~n", [lists:duplicate(90, $-)]),

    TopSuppressors = lists:sublist(Sorted, 20),
    lists:foreach(fun({Mod, Delta, AvgW, AvgWO, InCount, _}) ->
        io:format("  ~-50s ~7.1f ~9.1f ~9.1f ~5p~n",
                  [Mod, Delta, AvgW, AvgWO, InCount])
    end, TopSuppressors),

    %% Print top triggers
    io:format("~n=== Top 20 TRIGGERS ===~n"),
    io:format("(Modules whose PRESENCE correlates with MORE failures found)~n"),
    io:format("(These modules help expose bugs in the system)~n~n"),
    io:format("  ~-50s ~8s ~10s ~10s ~6s~n",
              ["Module", "Delta", "With", "Without", "In"]),
    io:format("  ~s~n", [lists:duplicate(90, $-)]),

    TopTriggers = lists:sublist(lists:reverse(Sorted), 20),
    lists:foreach(fun({Mod, Delta, AvgW, AvgWO, InCount, _}) ->
        io:format("  ~-50s ~7.1f ~9.1f ~9.1f ~5p~n",
                  [Mod, Delta, AvgW, AvgWO, InCount])
    end, TopTriggers),

    %% Coverage analysis (if coverage data available)
    io:format("~n=== Coverage Impact ===~n"),
    CovResults = load_coverage_results(ResultsDir),
    case CovResults of
        [] ->
            io:format("No per-config coverage data found.~n");
        _ ->
            CovAnalysis = lists:filtermap(fun(Module) ->
                WithConfigs2 = [Id || {Id, Mods} <- Configs, lists:member(Module, Mods)],
                WithoutConfigs2 = [Id || {Id, Mods} <- Configs, not lists:member(Module, Mods)],
                WithCov = get_coverage_pcts(WithConfigs2, CovResults),
                WithoutCov = get_coverage_pcts(WithoutConfigs2, CovResults),
                case {WithCov, WithoutCov} of
                    {[], _} -> false;
                    {_, []} -> false;
                    _ ->
                        AvgWC = avg(WithCov),
                        AvgWOC = avg(WithoutCov),
                        CovDelta = AvgWC - AvgWOC,
                        {true, {Module, CovDelta, AvgWC, AvgWOC}}
                end
            end, AllModules),

            CovSorted = lists:sort(fun({_, D1, _, _}, {_, D2, _, _}) ->
                D1 > D2
            end, CovAnalysis),

            io:format("(Modules whose PRESENCE correlates with HIGHER overall coverage)~n~n"),
            io:format("  ~-50s ~8s ~10s ~10s~n",
                      ["Module", "Delta", "WithCov", "WithoutCov"]),
            io:format("  ~s~n", [lists:duplicate(82, $-)]),

            lists:foreach(fun({Mod, CovD, AvgWC2, AvgWOC2}) ->
                io:format("  ~-50s ~7.1f ~9.1f ~9.1f~n",
                          [Mod, CovD, AvgWC2, AvgWOC2])
            end, lists:sublist(CovSorted, 15))
    end,

    io:format("~n=== Recommendations ===~n"),
    io:format("1. Top suppressors should be investigated - they may have~n"),
    io:format("   meck mocks that override real behavior, preventing failures.~n"),
    io:format("2. Top triggers are valuable - they help expose real bugs.~n"),
    io:format("3. Consider running swarm configs that exclude top suppressors~n"),
    io:format("   for deeper coverage of difficult-to-reach code paths.~n"),
    io:format("~n"),
    ok;

main(_) ->
    io:format("Usage: ~s <results_dir> <configs_dir>~n", [escript:script_name()]),
    io:format("~n"),
    io:format("  results_dir: Directory containing summary.csv and coverage_*.txt files~n"),
    io:format("  configs_dir: Directory containing config_*.txt files~n"),
    halt(1).

%% ============================================================================
%% Data Loading
%% ============================================================================

load_configs(Dir) ->
    Pattern = filename:join(Dir, "config_*.txt"),
    Files = filelib:wildcard(Pattern),
    lists:filtermap(fun(File) ->
        Name = filename:basename(File, ".txt"),
        Id = case Name of
            "config_default" -> "default";
            "config_" ++ N -> N
        end,
        case file:read_file(File) of
            {ok, Bin} ->
                Modules = [list_to_atom(string:trim(L))
                           || L <- string:split(binary_to_list(Bin), "\n", all),
                              L =/= "", string:trim(L) =/= ""],
                {true, {Id, Modules}};
            _ ->
                false
        end
    end, Files).

load_results(Dir) ->
    %% Try to load summary.csv
    CsvFile = filename:join(Dir, "summary.csv"),
    case file:read_file(CsvFile) of
        {ok, Bin} ->
            Lines = string:split(binary_to_list(Bin), "\n", all),
            lists:filtermap(fun(Line) ->
                case string:split(string:trim(Line), ",", all) of
                    [Id, _Count, Tests, Failures, _Cancelled, _Cov, _Dur] ->
                        T = safe_int(Tests),
                        F = safe_int(Failures),
                        {true, {Id, T, F}};
                    [Id, _Count, Tests, Failures, _Cov, _Dur] ->
                        T = safe_int(Tests),
                        F = safe_int(Failures),
                        {true, {Id, T, F}};
                    _ -> false
                end
            end, Lines);
        _ ->
            %% Try individual output files
            OutputFiles = filelib:wildcard(filename:join(Dir, "output_*.txt")),
            lists:filtermap(fun(File) ->
                Name = filename:basename(File, ".txt"),
                Id = case Name of
                    "output_" ++ N -> N;
                    _ -> Name
                end,
                case file:read_file(File) of
                    {ok, Bin2} ->
                        Content = binary_to_list(Bin2),
                        %% Strip ANSI codes
                        Clean = re:replace(Content, "\e\\[[0-9;]*m", "", [global, {return, list}]),
                        Passes = length([1 || $. <- Clean]),
                        Fails = length([1 || $F <- Clean]),
                        {true, {Id, Passes + Fails, Fails}};
                    _ -> false
                end
            end, OutputFiles)
    end.

load_coverage_results(Dir) ->
    Files = filelib:wildcard(filename:join(Dir, "coverage_*.txt")),
    lists:filtermap(fun(File) ->
        Name = filename:basename(File, ".txt"),
        Id = case Name of
            "coverage_" ++ N -> N;
            _ -> Name
        end,
        case file:read_file(File) of
            {ok, Bin} ->
                Content = binary_to_list(Bin),
                case re:run(Content, "total\\s*\\|\\s*(\\d+)%",
                            [{capture, [1], list}]) of
                    {match, [PctStr]} ->
                        {true, {Id, list_to_integer(PctStr)}};
                    _ -> false
                end;
            _ -> false
        end
    end, Files).

%% ============================================================================
%% Analysis
%% ============================================================================

get_failure_rates(ConfigIds, Results) ->
    lists:filtermap(fun(Id) ->
        case lists:keyfind(Id, 1, Results) of
            {Id, Total, Failures} when Total > 0 ->
                {true, Failures / Total * 100};
            _ -> false
        end
    end, ConfigIds).

get_coverage_pcts(ConfigIds, CovResults) ->
    lists:filtermap(fun(Id) ->
        case lists:keyfind(Id, 1, CovResults) of
            {Id, Pct} -> {true, float(Pct)};
            _ -> false
        end
    end, ConfigIds).

avg([]) -> 0.0;
avg(List) -> lists:sum(List) / length(List).

safe_int(Str) ->
    S = string:trim(Str),
    case string:to_integer(S) of
        {Int, _} when is_integer(Int) -> Int;
        _ -> 0
    end.
