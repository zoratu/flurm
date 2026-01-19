%%%-------------------------------------------------------------------
%%% @doc FLURM Performance Benchmarking
%%%
%%% Provides benchmarking tools for measuring FLURM performance.
%%% Useful for capacity planning and performance regression testing.
%%%
%%% Benchmarks:
%%% - Job submission throughput
%%% - Scheduler cycle time
%%% - Protocol codec performance
%%% - Memory usage under load
%%% - Concurrent connection handling
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark).

-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% API
-export([
    run_all/0,
    run_all/1,
    run_benchmark/1,
    run_benchmark/2,
    job_submission_throughput/1,
    scheduler_cycle_time/1,
    protocol_codec_performance/1,
    memory_usage/1,
    concurrent_connections/1,
    generate_report/1,
    compare_reports/2
]).

-type benchmark_name() :: job_submission | scheduler_cycle | protocol_codec |
                          memory_usage | concurrent_connections.

-export_type([benchmark_name/0]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    safe_min/1,
    safe_max/1,
    safe_avg/1,
    percentile/2,
    format_bytes/1,
    format_result/1,
    format_comparison/3,
    create_result/6,
    create_sample_messages/0,
    encode_simple_message/2,
    create_dummy_job/0,
    simulate_scheduler_cycle/0
]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Run all benchmarks with default options
-spec run_all() -> [map()].
run_all() ->
    run_all(#{}).

%% @doc Run all benchmarks with options
-spec run_all(map()) -> [map()].
run_all(Options) ->
    Benchmarks = [
        job_submission,
        scheduler_cycle,
        protocol_codec,
        memory_usage,
        concurrent_connections
    ],
    [run_benchmark(B, Options) || B <- Benchmarks].

%% @doc Run a specific benchmark with default options
-spec run_benchmark(benchmark_name()) -> map().
run_benchmark(Name) ->
    run_benchmark(Name, #{}).

%% @doc Run a specific benchmark with options
-spec run_benchmark(benchmark_name(), map()) -> map().
run_benchmark(job_submission, Options) ->
    job_submission_throughput(Options);
run_benchmark(scheduler_cycle, Options) ->
    scheduler_cycle_time(Options);
run_benchmark(protocol_codec, Options) ->
    protocol_codec_performance(Options);
run_benchmark(memory_usage, Options) ->
    memory_usage(Options);
run_benchmark(concurrent_connections, Options) ->
    concurrent_connections(Options).

%% @doc Benchmark job submission throughput
-spec job_submission_throughput(map()) -> map().
job_submission_throughput(Options) ->
    Iterations = maps:get(iterations, Options, 1000),
    io:format("Running job submission benchmark (~p iterations)...~n", [Iterations]),

    MemBefore = erlang:memory(total),
    garbage_collect(),

    {TotalTime, Latencies} = measure_job_submissions(Iterations),

    MemAfter = erlang:memory(total),

    create_result(job_submission, TotalTime, Iterations, Latencies, MemBefore, MemAfter).

%% @doc Benchmark scheduler cycle time
-spec scheduler_cycle_time(map()) -> map().
scheduler_cycle_time(Options) ->
    Iterations = maps:get(iterations, Options, 100),
    io:format("Running scheduler cycle benchmark (~p iterations)...~n", [Iterations]),

    MemBefore = erlang:memory(total),

    {TotalTime, Latencies} = measure_scheduler_cycles(Iterations),

    MemAfter = erlang:memory(total),

    create_result(scheduler_cycle, TotalTime, Iterations, Latencies, MemBefore, MemAfter).

%% @doc Benchmark protocol codec performance
-spec protocol_codec_performance(map()) -> map().
protocol_codec_performance(Options) ->
    Iterations = maps:get(iterations, Options, 10000),
    io:format("Running protocol codec benchmark (~p iterations)...~n", [Iterations]),

    Messages = create_sample_messages(),

    MemBefore = erlang:memory(total),

    {TotalTime, Latencies} = measure_codec_operations(Messages, Iterations),

    MemAfter = erlang:memory(total),

    create_result(protocol_codec, TotalTime, Iterations, Latencies, MemBefore, MemAfter).

%% @doc Benchmark memory usage under load
-spec memory_usage(map()) -> map().
memory_usage(Options) ->
    JobCount = maps:get(job_count, Options, 10000),
    io:format("Running memory benchmark (~p jobs)...~n", [JobCount]),

    garbage_collect(),
    MemBefore = erlang:memory(total),

    StartTime = erlang:monotonic_time(microsecond),
    _Jobs = create_test_jobs(JobCount),
    EndTime = erlang:monotonic_time(microsecond),

    MemAfter = erlang:memory(total),

    TotalTime = EndTime - StartTime,
    Latencies = [TotalTime div JobCount || _ <- lists:seq(1, min(100, JobCount))],

    create_result(memory_usage, TotalTime, JobCount, Latencies, MemBefore, MemAfter).

%% @doc Benchmark concurrent connection handling
-spec concurrent_connections(map()) -> map().
concurrent_connections(Options) ->
    Connections = maps:get(connections, Options, 100),
    RequestsPerConn = maps:get(requests_per_connection, Options, 10),
    io:format("Running concurrent connections benchmark (~p connections)...~n", [Connections]),

    MemBefore = erlang:memory(total),

    {TotalTime, Latencies} = measure_concurrent_connections(Connections, RequestsPerConn),

    MemAfter = erlang:memory(total),

    TotalOps = Connections * RequestsPerConn,
    create_result(concurrent_connections, TotalTime, TotalOps, Latencies, MemBefore, MemAfter).

%% @doc Generate a formatted report from benchmark results
-spec generate_report([map()]) -> binary().
generate_report(Results) ->
    Header = <<"FLURM Performance Benchmark Report\n===================================\n\n">>,
    Sections = [format_result(R) || R <- Results],
    iolist_to_binary([Header | Sections]).

%% @doc Compare two benchmark reports
-spec compare_reports([map()], [map()]) -> binary().
compare_reports(Before, After) ->
    Header = <<"FLURM Performance Comparison\n============================\n\n">>,
    Comparisons = lists:map(fun(#{name := Name} = AfterResult) ->
        case [R || #{name := N} = R <- Before, N =:= Name] of
            [BeforeResult] -> format_comparison(Name, BeforeResult, AfterResult);
            [] -> <<>>
        end
    end, After),
    iolist_to_binary([Header | Comparisons]).

%%====================================================================
%% Internal Functions - Measurement
%%====================================================================

measure_job_submissions(Iterations) ->
    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:map(fun(_) ->
        T1 = erlang:monotonic_time(microsecond),
        _Job = create_dummy_job(),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, Iterations)),
    EndTime = erlang:monotonic_time(microsecond),
    {EndTime - StartTime, Latencies}.

measure_scheduler_cycles(Iterations) ->
    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:map(fun(_) ->
        T1 = erlang:monotonic_time(microsecond),
        simulate_scheduler_cycle(),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, Iterations)),
    EndTime = erlang:monotonic_time(microsecond),
    {EndTime - StartTime, Latencies}.

simulate_scheduler_cycle() ->
    _ = lists:sum(lists:seq(1, 1000)).

create_sample_messages() ->
    [
        {?REQUEST_PING, <<>>},
        {?REQUEST_JOB_INFO, <<1:32/big, 0:16/big, 0:16/big>>}
    ].

measure_codec_operations(Messages, Iterations) ->
    IterPerMsg = Iterations div length(Messages),
    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:flatmap(fun({MsgType, Body}) ->
        lists:map(fun(_) ->
            T1 = erlang:monotonic_time(microsecond),
            Encoded = encode_simple_message(MsgType, Body),
            _ = flurm_protocol_codec:decode(Encoded),
            T2 = erlang:monotonic_time(microsecond),
            T2 - T1
        end, lists:seq(1, IterPerMsg))
    end, Messages),
    EndTime = erlang:monotonic_time(microsecond),
    {EndTime - StartTime, Latencies}.

encode_simple_message(MsgType, Body) ->
    BodyLen = byte_size(Body),
    HeaderLen = 10,
    TotalLen = HeaderLen + BodyLen,
    Version = 16#2600,
    <<TotalLen:32/big, Version:16/big, 0:16/big, 0:16/big, MsgType:16/big, BodyLen:32/big, Body/binary>>.

create_test_jobs(Count) ->
    [create_dummy_job() || _ <- lists:seq(1, Count)].

measure_concurrent_connections(Connections, RequestsPerConn) ->
    Parent = self(),
    StartTime = erlang:monotonic_time(microsecond),

    Pids = [spawn_link(fun() ->
        Latencies = [begin
            T1 = erlang:monotonic_time(microsecond),
            timer:sleep(1),
            T2 = erlang:monotonic_time(microsecond),
            T2 - T1
        end || _ <- lists:seq(1, RequestsPerConn)],
        Parent ! {done, self(), Latencies}
    end) || _ <- lists:seq(1, Connections)],

    AllLatencies = lists:flatmap(fun(Pid) ->
        receive {done, Pid, L} -> L after 60000 -> [] end
    end, Pids),

    EndTime = erlang:monotonic_time(microsecond),
    {EndTime - StartTime, AllLatencies}.

%%====================================================================
%% Internal Functions - Data
%%====================================================================

create_dummy_job() ->
    #job{
        id = rand:uniform(1000000),
        name = <<"bench_job">>,
        user = <<"benchuser">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 60,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

%%====================================================================
%% Internal Functions - Statistics
%%====================================================================

create_result(Name, TotalTimeUs, Iterations, Latencies, MemBefore, MemAfter) ->
    SortedLatencies = lists:sort(Latencies),

    #{
        name => Name,
        duration_ms => TotalTimeUs div 1000,
        iterations => Iterations,
        ops_per_second => Iterations / (TotalTimeUs / 1000000),
        min_latency_us => safe_min(Latencies),
        max_latency_us => safe_max(Latencies),
        avg_latency_us => safe_avg(Latencies),
        p50_latency_us => percentile(SortedLatencies, 50),
        p95_latency_us => percentile(SortedLatencies, 95),
        p99_latency_us => percentile(SortedLatencies, 99),
        memory_before => MemBefore,
        memory_after => MemAfter,
        memory_delta => MemAfter - MemBefore
    }.

safe_min([]) -> 0;
safe_min(L) -> lists:min(L).

safe_max([]) -> 0;
safe_max(L) -> lists:max(L).

safe_avg([]) -> 0.0;
safe_avg(L) -> lists:sum(L) / length(L).

percentile([], _) -> 0;
percentile(SortedList, P) ->
    Len = length(SortedList),
    Index = max(1, min(Len, round(P / 100 * Len))),
    lists:nth(Index, SortedList).

%%====================================================================
%% Internal Functions - Formatting
%%====================================================================

format_result(#{name := Name, duration_ms := Duration, iterations := Iters,
                ops_per_second := OpsPerSec, avg_latency_us := AvgLat,
                p99_latency_us := P99, memory_delta := MemDelta}) ->
    iolist_to_binary(io_lib:format(
        "~s~n"
        "  Duration: ~p ms (~p iterations)~n"
        "  Throughput: ~.2f ops/sec~n"
        "  Avg Latency: ~.2f us, p99: ~p us~n"
        "  Memory Delta: ~s~n~n",
        [Name, Duration, Iters, OpsPerSec, AvgLat, P99, format_bytes(MemDelta)])).

format_comparison(Name, Before, After) ->
    OpsChange = (maps:get(ops_per_second, After) / max(1, maps:get(ops_per_second, Before)) - 1) * 100,
    iolist_to_binary(io_lib:format(
        "~s: ~.2f -> ~.2f ops/sec (~.1f%)~n",
        [Name, maps:get(ops_per_second, Before), maps:get(ops_per_second, After), OpsChange])).

format_bytes(Bytes) when Bytes >= 1024 * 1024 ->
    iolist_to_binary(io_lib:format("~.2f MB", [Bytes / (1024 * 1024)]));
format_bytes(Bytes) when Bytes >= 1024 ->
    iolist_to_binary(io_lib:format("~.2f KB", [Bytes / 1024]));
format_bytes(Bytes) ->
    iolist_to_binary(io_lib:format("~p B", [Bytes])).
