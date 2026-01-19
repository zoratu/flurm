#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/*/ebin -pa apps/*/ebin

%% FLURM Performance Benchmark Runner
%%
%% This script runs the complete FLURM benchmark suite and outputs results.
%%
%% Usage:
%%   ./scripts/run_benchmarks.escript [OPTIONS]
%%
%% Options:
%%   --quick          Run quick benchmarks (fewer iterations)
%%   --full           Run full benchmarks (more iterations)
%%   --output FILE    Save results to FILE
%%   --json           Output results in JSON format
%%   --help           Show this help message
%%
%% Examples:
%%   ./scripts/run_benchmarks.escript
%%   ./scripts/run_benchmarks.escript --quick
%%   ./scripts/run_benchmarks.escript --output results.txt
%%   ./scripts/run_benchmarks.escript --full --json --output results.json

-mode(compile).

-define(DEFAULT_JOB_ITERATIONS, 1000).
-define(DEFAULT_SCHEDULER_ITERATIONS, 100).
-define(DEFAULT_PROTOCOL_ITERATIONS, 10000).
-define(DEFAULT_PERSISTENCE_ITERATIONS, 1000).

-define(QUICK_JOB_ITERATIONS, 100).
-define(QUICK_SCHEDULER_ITERATIONS, 10).
-define(QUICK_PROTOCOL_ITERATIONS, 1000).
-define(QUICK_PERSISTENCE_ITERATIONS, 100).

-define(FULL_JOB_ITERATIONS, 10000).
-define(FULL_SCHEDULER_ITERATIONS, 1000).
-define(FULL_PROTOCOL_ITERATIONS, 100000).
-define(FULL_PERSISTENCE_ITERATIONS, 10000).

main(Args) ->
    %% Parse command line arguments
    Options = parse_args(Args),

    case maps:get(help, Options, false) of
        true ->
            print_help(),
            halt(0);
        false ->
            ok
    end,

    io:format("~n"),
    io:format("================================================================================~n"),
    io:format("                    FLURM Performance Benchmark Suite~n"),
    io:format("================================================================================~n"),
    io:format("~n"),

    %% Print system information
    print_system_info(),

    %% Ensure FLURM is compiled
    case ensure_compiled() of
        ok ->
            ok;
        {error, Reason} ->
            io:format("ERROR: Failed to compile FLURM: ~p~n", [Reason]),
            halt(1)
    end,

    %% Load necessary modules
    case load_modules() of
        ok ->
            ok;
        {error, LoadReason} ->
            io:format("ERROR: Failed to load modules: ~p~n", [LoadReason]),
            halt(1)
    end,

    %% Get iteration counts based on mode
    {JobIters, SchedIters, ProtoIters, PersistIters} = get_iterations(Options),

    io:format("~n"),
    io:format("Running benchmarks with:~n"),
    io:format("  Job submission:     ~p iterations~n", [JobIters]),
    io:format("  Scheduler cycle:    ~p iterations~n", [SchedIters]),
    io:format("  Protocol encoding:  ~p iterations~n", [ProtoIters]),
    io:format("  State persistence:  ~p iterations~n", [PersistIters]),
    io:format("~n"),

    %% Run benchmarks
    StartTime = erlang:monotonic_time(millisecond),

    Results = run_benchmarks(JobIters, SchedIters, ProtoIters, PersistIters),

    EndTime = erlang:monotonic_time(millisecond),
    TotalTime = EndTime - StartTime,

    io:format("~n"),
    io:format("================================================================================~n"),
    io:format("                           Benchmark Results Summary~n"),
    io:format("================================================================================~n"),
    io:format("~n"),

    %% Print summary table
    print_summary_table(Results),

    io:format("~n"),
    io:format("Total benchmark time: ~.2f seconds~n", [TotalTime / 1000]),
    io:format("~n"),

    %% Save results if requested
    case maps:get(output, Options, undefined) of
        undefined ->
            ok;
        OutputFile ->
            save_results(OutputFile, Results, Options)
    end,

    %% Generate formatted report
    FormattedReport = format_full_report(Results),
    io:format("~s", [FormattedReport]),

    halt(0).

%% Parse command line arguments
parse_args(Args) ->
    parse_args(Args, #{}).

parse_args([], Acc) ->
    Acc;
parse_args(["--help" | Rest], Acc) ->
    parse_args(Rest, Acc#{help => true});
parse_args(["--quick" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => quick});
parse_args(["--full" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => full});
parse_args(["--json" | Rest], Acc) ->
    parse_args(Rest, Acc#{json => true});
parse_args(["--output", File | Rest], Acc) ->
    parse_args(Rest, Acc#{output => File});
parse_args([_ | Rest], Acc) ->
    parse_args(Rest, Acc).

%% Print help message
print_help() ->
    io:format("FLURM Performance Benchmark Runner~n"),
    io:format("~n"),
    io:format("Usage: ./scripts/run_benchmarks.escript [OPTIONS]~n"),
    io:format("~n"),
    io:format("Options:~n"),
    io:format("  --quick          Run quick benchmarks (fewer iterations)~n"),
    io:format("  --full           Run full benchmarks (more iterations)~n"),
    io:format("  --output FILE    Save results to FILE~n"),
    io:format("  --json           Output results in JSON format~n"),
    io:format("  --help           Show this help message~n"),
    io:format("~n"),
    io:format("Examples:~n"),
    io:format("  ./scripts/run_benchmarks.escript~n"),
    io:format("  ./scripts/run_benchmarks.escript --quick~n"),
    io:format("  ./scripts/run_benchmarks.escript --output results.txt~n"),
    io:format("  ./scripts/run_benchmarks.escript --full --json --output results.json~n").

%% Print system information
print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Platform:       ~s~n", [get_platform()]),
    io:format("  Erlang/OTP:     ~s~n", [erlang:system_info(otp_release)]),
    io:format("  ERTS:           ~s~n", [erlang:system_info(version)]),
    io:format("  Schedulers:     ~p~n", [erlang:system_info(schedulers)]),
    io:format("  Word Size:      ~p bits~n", [erlang:system_info(wordsize) * 8]),
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    io:format("  Date/Time:      ~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B~n",
              [Y, M, D, H, Mi, S]),
    io:format("~n").

get_platform() ->
    case os:type() of
        {unix, darwin} -> "macOS";
        {unix, linux} -> "Linux";
        {unix, Os} -> atom_to_list(Os);
        {win32, _} -> "Windows";
        {Os, _} -> atom_to_list(Os)
    end.

%% Ensure FLURM is compiled
ensure_compiled() ->
    io:format("Checking FLURM compilation...~n"),
    case filelib:is_dir("_build/default/lib/flurm_core") of
        true ->
            io:format("  FLURM is compiled.~n"),
            ok;
        false ->
            io:format("  Compiling FLURM...~n"),
            case os:cmd("rebar3 compile 2>&1") of
                Output ->
                    case string:find(Output, "ERROR") of
                        nomatch ->
                            io:format("  Compilation successful.~n"),
                            ok;
                        _ ->
                            {error, Output}
                    end
            end
    end.

%% Load required modules
load_modules() ->
    io:format("Loading benchmark modules...~n"),

    %% Determine base directory from script location or current directory
    {ok, Cwd} = file:get_cwd(),
    ScriptName = escript:script_name(),
    BaseDir = case filename:dirname(ScriptName) of
        "." -> Cwd;
        ScriptDir ->
            case filename:pathtype(ScriptDir) of
                absolute -> filename:dirname(ScriptDir);
                relative -> filename:dirname(filename:join(Cwd, ScriptDir))
            end
    end,
    io:format("  Base directory: ~s~n", [BaseDir]),

    %% Add all possible code paths
    DefaultPaths = filelib:wildcard(filename:join([BaseDir, "_build", "default", "lib", "*", "ebin"])),
    TestPaths = filelib:wildcard(filename:join([BaseDir, "_build", "test", "lib", "*", "ebin"])),

    AllPaths = DefaultPaths ++ TestPaths,
    lists:foreach(fun(P) when is_list(P), length(P) > 0 ->
        code:add_pathz(P);
                    (_) -> ok
    end, AllPaths),

    Modules = [
        flurm_benchmark,
        flurm_protocol_codec
    ],

    Results = lists:map(fun(Mod) ->
        case code:ensure_loaded(Mod) of
            {module, Mod} ->
                io:format("  Loaded: ~s~n", [Mod]),
                ok;
            {error, Reason} ->
                io:format("  Failed to load ~s: ~p~n", [Mod, Reason]),
                {error, {Mod, Reason}}
        end
    end, Modules),

    case lists:filter(fun(R) -> R =/= ok end, Results) of
        [] -> ok;
        Errors -> {error, Errors}
    end.

%% Get iteration counts based on mode
get_iterations(Options) ->
    case maps:get(mode, Options, default) of
        quick ->
            {?QUICK_JOB_ITERATIONS, ?QUICK_SCHEDULER_ITERATIONS,
             ?QUICK_PROTOCOL_ITERATIONS, ?QUICK_PERSISTENCE_ITERATIONS};
        full ->
            {?FULL_JOB_ITERATIONS, ?FULL_SCHEDULER_ITERATIONS,
             ?FULL_PROTOCOL_ITERATIONS, ?FULL_PERSISTENCE_ITERATIONS};
        _ ->
            {?DEFAULT_JOB_ITERATIONS, ?DEFAULT_SCHEDULER_ITERATIONS,
             ?DEFAULT_PROTOCOL_ITERATIONS, ?DEFAULT_PERSISTENCE_ITERATIONS}
    end.

%% Run all benchmarks
run_benchmarks(JobIters, SchedIters, ProtoIters, PersistIters) ->
    [
        run_job_submission_benchmark(JobIters),
        run_scheduler_cycle_benchmark(SchedIters),
        run_protocol_encoding_benchmark(ProtoIters),
        run_state_persistence_benchmark(PersistIters)
    ].

%% Individual benchmark runners with error handling
run_job_submission_benchmark(N) ->
    io:format("~n--- Job Submission Benchmark ---~n"),
    try
        flurm_benchmark:benchmark_job_submission(N)
    catch
        Class:Reason:Stack ->
            io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
            #{name => job_submission, error => {Class, Reason}}
    end.

run_scheduler_cycle_benchmark(N) ->
    io:format("~n--- Scheduler Cycle Benchmark ---~n"),
    try
        flurm_benchmark:benchmark_scheduler_cycle(N)
    catch
        Class:Reason:Stack ->
            io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
            #{name => scheduler_cycle, error => {Class, Reason}}
    end.

run_protocol_encoding_benchmark(N) ->
    io:format("~n--- Protocol Encoding Benchmark ---~n"),
    try
        flurm_benchmark:benchmark_protocol_encoding(N)
    catch
        Class:Reason:Stack ->
            io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
            #{name => protocol_encoding, error => {Class, Reason}}
    end.

run_state_persistence_benchmark(N) ->
    io:format("~n--- State Persistence Benchmark ---~n"),
    try
        flurm_benchmark:benchmark_state_persistence(N)
    catch
        Class:Reason:Stack ->
            io:format("ERROR: ~p:~p~n~p~n", [Class, Reason, Stack]),
            #{name => state_persistence, error => {Class, Reason}}
    end.

%% Print summary table
print_summary_table(Results) ->
    io:format("~-25s ~15s ~15s ~12s~n",
              ["Benchmark", "Throughput", "Avg Latency", "Memory"]),
    io:format("~s~n", [lists:duplicate(70, $-)]),

    lists:foreach(fun(Result) ->
        print_result_row(Result)
    end, Results).

print_result_row(#{name := Name, error := _}) ->
    io:format("~-25s ~15s ~15s ~12s~n",
              [Name, "ERROR", "-", "-"]);
print_result_row(#{name := state_persistence} = Result) ->
    StoreOps = maps:get(store_ops_per_second, Result, 0),
    LookupOps = maps:get(lookup_ops_per_second, Result, 0),
    AvgOps = (StoreOps + LookupOps) / 2,
    MemDelta = maps:get(memory_delta, Result, 0),
    io:format("~-25s ~12.2f/s ~15s ~12s~n",
              [state_persistence, AvgOps, "(see details)", format_bytes(MemDelta)]);
print_result_row(#{name := Name, ops_per_second := OpsPerSec,
                   avg_latency_us := AvgLat, memory_delta := MemDelta}) ->
    io:format("~-25s ~12.2f/s ~12.2f us ~12s~n",
              [Name, OpsPerSec, AvgLat, format_bytes(MemDelta)]).

%% Format bytes for display
format_bytes(Bytes) when Bytes >= 1024 * 1024 ->
    io_lib:format("~.2f MB", [Bytes / (1024 * 1024)]);
format_bytes(Bytes) when Bytes >= 1024 ->
    io_lib:format("~.2f KB", [Bytes / 1024]);
format_bytes(Bytes) when Bytes < 0 ->
    io_lib:format("~p B", [Bytes]);
format_bytes(Bytes) ->
    io_lib:format("~p B", [Bytes]).

%% Save results to file
save_results(Filename, Results, Options) ->
    io:format("Saving results to ~s...~n", [Filename]),
    Content = case maps:get(json, Options, false) of
        true ->
            results_to_json(Results);
        false ->
            format_full_report(Results)
    end,
    case file:write_file(Filename, Content) of
        ok ->
            io:format("Results saved successfully.~n");
        {error, Reason} ->
            io:format("ERROR: Failed to save results: ~p~n", [Reason])
    end.

%% Convert results to JSON format
results_to_json(Results) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    Timestamp = io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0B",
                               [Y, M, D, H, Mi, S]),

    %% Build JSON manually (simple approach without jsx dependency)
    ResultsJson = lists:map(fun(Result) ->
        result_to_json_object(Result)
    end, Results),

    io_lib:format(
        "{~n"
        "  \"timestamp\": \"~s\",~n"
        "  \"platform\": \"~s\",~n"
        "  \"erlang_version\": \"~s\",~n"
        "  \"schedulers\": ~p,~n"
        "  \"results\": [~n~s  ]~n"
        "}~n",
        [Timestamp, get_platform(), erlang:system_info(otp_release),
         erlang:system_info(schedulers),
         string:join(ResultsJson, ",\n")]).

result_to_json_object(#{name := Name, error := {Class, Reason}}) ->
    io_lib:format(
        "    {~n"
        "      \"name\": \"~s\",~n"
        "      \"error\": \"~p:~p\"~n"
        "    }",
        [Name, Class, Reason]);
result_to_json_object(#{name := state_persistence} = Result) ->
    StoreOps = maps:get(store_ops_per_second, Result, 0),
    LookupOps = maps:get(lookup_ops_per_second, Result, 0),
    MemDelta = maps:get(memory_delta, Result, 0),
    io_lib:format(
        "    {~n"
        "      \"name\": \"state_persistence\",~n"
        "      \"store_ops_per_second\": ~.2f,~n"
        "      \"lookup_ops_per_second\": ~.2f,~n"
        "      \"memory_delta_bytes\": ~p~n"
        "    }",
        [StoreOps, LookupOps, MemDelta]);
result_to_json_object(#{name := Name, ops_per_second := OpsPerSec,
                        avg_latency_us := AvgLat, p99_latency_us := P99,
                        memory_delta := MemDelta, iterations := Iters}) ->
    io_lib:format(
        "    {~n"
        "      \"name\": \"~s\",~n"
        "      \"iterations\": ~p,~n"
        "      \"ops_per_second\": ~.2f,~n"
        "      \"avg_latency_us\": ~.2f,~n"
        "      \"p99_latency_us\": ~p,~n"
        "      \"memory_delta_bytes\": ~p~n"
        "    }",
        [Name, Iters, OpsPerSec, AvgLat, P99, MemDelta]).

%% Format full report
format_full_report(Results) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    Timestamp = io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                               [Y, M, D, H, Mi, S]),

    Header = io_lib:format(
        "~n"
        "================================================================================~n"
        "                    FLURM Performance Benchmark Report~n"
        "================================================================================~n"
        "~n"
        "Generated: ~s~n"
        "Platform:  ~s~n"
        "Erlang:    OTP ~s (ERTS ~s)~n"
        "~n",
        [Timestamp, get_platform(), erlang:system_info(otp_release),
         erlang:system_info(version)]),

    ResultSections = lists:map(fun(Result) ->
        format_result_section(Result)
    end, Results),

    Comparison = format_slurm_comparison(),

    iolist_to_binary([Header, ResultSections, Comparison]).

format_result_section(#{name := Name, error := {Class, Reason}}) ->
    io_lib:format(
        "~s~n"
        "--------------------------------------------------------------------------------~n"
        "  ERROR: ~p:~p~n~n",
        [Name, Class, Reason]);
format_result_section(#{name := state_persistence} = Result) ->
    StoreOps = maps:get(store_ops_per_second, Result, 0),
    LookupOps = maps:get(lookup_ops_per_second, Result, 0),
    StoreResult = maps:get(store, Result, #{}),
    LookupResult = maps:get(lookup, Result, #{}),
    MemDelta = maps:get(memory_delta, Result, 0),
    Iters = maps:get(iterations, Result, 0),
    io_lib:format(
        "state_persistence~n"
        "--------------------------------------------------------------------------------~n"
        "  Operations:     ~p (~p stores + ~p lookups)~n"
        "  Store:          ~.2f ops/sec~n"
        "                  avg: ~.2f us, p99: ~p us~n"
        "  Lookup:         ~.2f ops/sec~n"
        "                  avg: ~.2f us, p99: ~p us~n"
        "  Memory Delta:   ~s~n~n",
        [Iters, Iters div 2, Iters div 2,
         StoreOps, maps:get(avg_latency_us, StoreResult, 0),
         maps:get(p99_latency_us, StoreResult, 0),
         LookupOps, maps:get(avg_latency_us, LookupResult, 0),
         maps:get(p99_latency_us, LookupResult, 0),
         format_bytes(MemDelta)]);
format_result_section(#{name := Name, iterations := Iters, duration_ms := Duration,
                        ops_per_second := OpsPerSec, avg_latency_us := AvgLat,
                        p50_latency_us := P50, p95_latency_us := P95, p99_latency_us := P99,
                        memory_delta := MemDelta}) ->
    io_lib:format(
        "~s~n"
        "--------------------------------------------------------------------------------~n"
        "  Iterations:     ~p~n"
        "  Duration:       ~p ms~n"
        "  Throughput:     ~.2f ops/sec~n"
        "  Latency:        avg=~.2f us, p50=~p us, p95=~p us, p99=~p us~n"
        "  Memory Delta:   ~s~n~n",
        [Name, Iters, Duration, OpsPerSec, AvgLat, P50, P95, P99, format_bytes(MemDelta)]).

format_slurm_comparison() ->
    io_lib:format(
        "================================================================================~n"
        "                         Comparison with SLURM~n"
        "================================================================================~n"
        "~n"
        "FLURM is designed to provide significant improvements over SLURM in key areas:~n"
        "~n"
        "  | Metric              | SLURM           | FLURM (target)   | Improvement |~n"
        "  |---------------------|-----------------|------------------|-------------|~n"
        "  | Failover time       | 30-60 seconds   | < 1 second       | 30-60x      |~n"
        "  | Config reload       | Restart needed  | Hot reload       | N/A         |~n"
        "  | Scheduler lock      | Global          | None             | Eliminates  |~n"
        "  | Max controllers     | 2               | Unlimited        | N/A         |~n"
        "~n"
        "Notes:~n"
        "  - These benchmarks measure FLURM component performance in isolation~n"
        "  - Production performance depends on cluster size, workload, and network~n"
        "  - Run with --full flag for more accurate measurements~n"
        "~n", []).
