#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/test/lib/*/ebin

%% Merged full-project coverage report across umbrella apps.
%% Runs full EUnit + Common Test with cover, imports all coverdata, and writes:
%%   coverage/full_coverage_report.txt
%%   coverage/full_coverage.csv
%%   coverage/full_coverage_under_100.txt

main(_Args) ->
    Root = cwd(),
    io:format("=== FLURM Full Coverage (Merged) ===~n"),
    io:format("Root: ~s~n~n", [Root]),

    TmpDir = filename:join("/tmp", "flurm_full_coverage"),
    ok = ensure_clean_tmp(TmpDir),
    ok = ensure_dir(filename:join(Root, "coverage/placeholder")),

    EunitResult = run_eunit_coverage(Root, TmpDir),
    print_run_result("EUnit run", EunitResult),
    CtResult = run_ct_coverage(Root, TmpDir),
    print_run_result("Common Test run", CtResult),

    cover:start(),
    add_test_code_paths(Root),
    compile_all_source_modules(Root),
    import_all_coverdata(TmpDir),

    Mods = filtered_modules(),
    Rows = coverage_rows(Mods),
    Sorted = lists:reverse(lists:keysort(2, Rows)),

    {TotalCovered, TotalLines} = write_reports(Root, Sorted),
    print_summary(Sorted, TotalCovered, TotalLines),

    case [M || {M, P, _, _} <- Sorted, P < 100.0] of
        [] -> halt(0);
        Under100 ->
            io:format("~nModules under 100%: ~p~n", [length(Under100)]),
            halt(0)
    end.

cwd() ->
    {ok, Cwd} = file:get_cwd(),
    Cwd.

ensure_clean_tmp(TmpDir) ->
    _ = os:cmd("rm -rf " ++ TmpDir),
    ok = filelib:ensure_dir(filename:join(TmpDir, "x")),
    ok.

ensure_dir(Path) ->
    filelib:ensure_dir(Path).

run_eunit_coverage(Root, TmpDir) ->
    CoverFile = filename:join(TmpDir, "eunit.coverdata"),
    LogFile = filename:join(TmpDir, "eunit.log"),
    Cmd = io_lib:format(
            "cd ~s && rebar3 as test eunit --cover > ~s 2>&1; "
            "status=$?; "
            "if [ -f _build/test/cover/eunit.coverdata ]; then "
            "cp _build/test/cover/eunit.coverdata ~s; fi; "
            "echo EXIT:$status",
            [shell_escape(Root), shell_escape(LogFile), shell_escape(CoverFile)]),
    Out = os:cmd(lists:flatten(Cmd)),
    Exit = parse_exit(Out),
    {"eunit", Exit, LogFile, CoverFile}.

run_ct_coverage(Root, TmpDir) ->
    CoverFile = filename:join(TmpDir, "ct.coverdata"),
    LogFile = filename:join(TmpDir, "ct.log"),
    Cmd = io_lib:format(
            "cd ~s && rebar3 ct --cover > ~s 2>&1; "
            "status=$?; "
            "if [ -f _build/test/cover/ct.coverdata ]; then "
            "cp _build/test/cover/ct.coverdata ~s; "
            "elif [ -f _build/test/cover/eunit.coverdata ]; then "
            "cp _build/test/cover/eunit.coverdata ~s; "
            "fi; "
            "echo EXIT:$status",
            [shell_escape(Root), shell_escape(LogFile),
             shell_escape(CoverFile), shell_escape(CoverFile)]),
    Out = os:cmd(lists:flatten(Cmd)),
    Exit = parse_exit(Out),
    {"ct", Exit, LogFile, CoverFile}.

parse_exit(Output) ->
    case string:find(Output, "EXIT:") of
        nomatch -> 1;
        _ ->
            Parts = string:split(Output, "EXIT:", all),
            Tail = lists:last(Parts),
            case string:to_integer(string:trim(Tail)) of
                {I, _} -> I;
                _ -> 1
            end
    end.

print_run_result(Label, {_Tag, Exit, _Log, Cover}) ->
    CoverExists = filelib:is_file(Cover),
    Status = case {Exit, CoverExists} of
        {0, true} -> "ok";
        _ -> "fail"
    end,
    io:format("~s: ~-4s (exit=~p, cover=~p)~n~n",
              [Label, Status, Exit, CoverExists]).

add_test_code_paths(Root) ->
    EbinDirs = filelib:wildcard(filename:join(Root, "_build/test/lib/*/ebin")),
    lists:foreach(fun(Dir) ->
        _ = code:add_patha(Dir),
        ok
    end, EbinDirs).

compile_all_source_modules(Root) ->
    SrcFiles = filelib:wildcard(filename:join(Root, "apps/*/src/*.erl")),
    IncludeDirs = filelib:wildcard(filename:join(Root, "apps/*/include")) ++
                  filelib:wildcard(filename:join(Root, "_build/test/lib/*/include")),
    lists:foreach(fun(File) ->
        Opts = [{i, Dir} || Dir <- IncludeDirs],
        _ = cover:compile(File, Opts),
        ok
    end, SrcFiles).

import_all_coverdata(TmpDir) ->
    Files = filelib:wildcard(filename:join(TmpDir, "*.coverdata")),
    lists:foreach(fun(File) ->
        _ = cover:import(File),
        ok
    end, Files).

filtered_modules() ->
    [M || M <- cover:modules(),
          lists:prefix("flurm_", atom_to_list(M)),
          not lists:suffix("_tests", atom_to_list(M)),
          not lists:suffix("_test", atom_to_list(M)),
          not lists:suffix("_meck_original", atom_to_list(M))].

coverage_rows(Mods) ->
    lists:filtermap(
      fun(Mod) ->
          case cover:analyse(Mod, coverage, line) of
              {ok, Lines} ->
                  Covered = lists:sum([C || {_, {C, _}} <- Lines]),
                  Missed = lists:sum([N || {_, {_, N}} <- Lines]),
                  Total = Covered + Missed,
                  Pct = case Total of
                      0 -> 0.0;
                      _ -> (Covered * 100.0) / Total
                  end,
                  {true, {Mod, Pct, Covered, Total}};
              _ ->
                  false
          end
      end, Mods).

write_reports(Root, Rows) ->
    TxtPath = filename:join(Root, "coverage/full_coverage_report.txt"),
    CsvPath = filename:join(Root, "coverage/full_coverage.csv"),
    UnderPath = filename:join(Root, "coverage/full_coverage_under_100.txt"),

    Header = io_lib:format("~-45s ~8s ~10s ~10s~n", ["Module", "Cover%", "Covered", "Total"]),
    LineSep = lists:duplicate(80, $-) ++ "\n",
    TxtRows = [io_lib:format("~-45s ~7.2f% ~10B ~10B~n",
                             [atom_to_list(M), P, C, T]) || {M, P, C, T} <- Rows],
    Txt = lists:flatten([Header, LineSep, TxtRows]),
    ok = file:write_file(TxtPath, Txt),

    CsvHeader = "module,coverage_pct,covered,total\n",
    CsvRows = [io_lib:format("~s,~.2f,~B,~B~n", [atom_to_list(M), P, C, T]) || {M, P, C, T} <- Rows],
    ok = file:write_file(CsvPath, lists:flatten([CsvHeader, CsvRows])),

    Under = [io_lib:format("~s,~.2f~n", [atom_to_list(M), P]) || {M, P, _, _} <- Rows, P < 100.0],
    ok = file:write_file(UnderPath, lists:flatten(Under)),

    {lists:sum([C || {_, _, C, _} <- Rows]),
     lists:sum([T || {_, _, _, T} <- Rows])}.

print_summary(Rows, TotalCovered, TotalLines) ->
    TotalPct = case TotalLines of
        0 -> 0.0;
        _ -> (TotalCovered * 100.0) / TotalLines
    end,
    Count100 = length([ok || {_, P, _, _} <- Rows, P =:= 100.0]),
    Count95 = length([ok || {_, P, _, _} <- Rows, P >= 95.0]),
    Count90 = length([ok || {_, P, _, _} <- Rows, P >= 90.0]),
    CountUnder90 = length([ok || {_, P, _, _} <- Rows, P < 90.0]),
    io:format("Merged coverage summary:~n"),
    io:format("  modules:            ~p~n", [length(Rows)]),
    io:format("  exactly 100%%:       ~p~n", [Count100]),
    io:format("  >=95%%:              ~p~n", [Count95]),
    io:format("  >=90%%:              ~p~n", [Count90]),
    io:format("  <90%%:               ~p~n", [CountUnder90]),
    io:format("  total lines:        ~p/~p (~.2f%%)~n", [TotalCovered, TotalLines, TotalPct]),
    io:format("~nReports written:~n"),
    io:format("  coverage/full_coverage_report.txt~n"),
    io:format("  coverage/full_coverage.csv~n"),
    io:format("  coverage/full_coverage_under_100.txt~n").

shell_escape(Str) ->
    "'" ++ lists:flatten(string:replace(Str, "'", "'\"'\"'", all)) ++ "'".
