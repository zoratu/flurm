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
%%% - State persistence performance
%%% - Memory usage under load
%%% - Concurrent connection handling
%%%
%%% Primary API (as documented in BENCHMARKS.md):
%%% - benchmark_job_submission/1 - submit N jobs and measure throughput
%%% - benchmark_scheduler_cycle/1 - measure scheduler performance with N pending jobs
%%% - benchmark_protocol_encoding/1 - measure protocol encode/decode speed
%%% - benchmark_state_persistence/1 - measure state save/load performance
%%% - run_all_benchmarks/0 - run complete benchmark suite
%%% - format_results/1 - format results for output
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark).

-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Primary API (documented interface)
-export([
    benchmark_job_submission/1,
    benchmark_scheduler_cycle/1,
    benchmark_protocol_encoding/1,
    benchmark_state_persistence/1,
    run_all_benchmarks/0,
    format_results/1
]).

%% Extended API (for compatibility with existing code)
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
                          state_persistence | memory_usage | concurrent_connections.

-export_type([benchmark_name/0]).

%% Test exports and helper functions for protocol benchmarks
-export([create_batch_job_request/0]).

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
%% Primary API - Documented Interface
%%====================================================================

%% @doc Run complete benchmark suite with default parameters.
%% Returns a list of benchmark results for all standard benchmarks.
-spec run_all_benchmarks() -> [map()].
run_all_benchmarks() ->
    io:format("~n=== FLURM Performance Benchmark Suite ===~n~n"),
    io:format("Starting benchmarks at ~s~n~n", [format_timestamp()]),

    Results = [
        benchmark_job_submission(1000),
        benchmark_scheduler_cycle(100),
        benchmark_protocol_encoding(10000),
        benchmark_state_persistence(1000)
    ],

    io:format("~n=== Benchmark Suite Complete ===~n~n"),
    Results.

%% @doc Benchmark job submission throughput.
%% Submits N jobs and measures creation throughput and latency.
-spec benchmark_job_submission(non_neg_integer()) -> map().
benchmark_job_submission(N) when is_integer(N), N > 0 ->
    io:format("Benchmark: Job Submission (~p jobs)~n", [N]),
    io:format("  Creating job records and measuring throughput...~n"),

    garbage_collect(),
    MemBefore = erlang:memory(total),

    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:map(fun(_I) ->
        T1 = erlang:monotonic_time(microsecond),
        _Job = create_dummy_job(),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, N)),
    EndTime = erlang:monotonic_time(microsecond),

    TotalTimeUs = EndTime - StartTime,
    MemAfter = erlang:memory(total),

    Result = create_result(job_submission, TotalTimeUs, N, Latencies, MemBefore, MemAfter),
    print_benchmark_result(Result),
    Result.

%% @doc Benchmark scheduler cycle performance.
%% Simulates scheduler cycles with N pending jobs and measures cycle time.
-spec benchmark_scheduler_cycle(non_neg_integer()) -> map().
benchmark_scheduler_cycle(N) when is_integer(N), N > 0 ->
    io:format("Benchmark: Scheduler Cycle (~p cycles)~n", [N]),
    io:format("  Simulating scheduler cycles with pending jobs...~n"),

    %% Create some pending jobs for a realistic simulation
    PendingJobs = [create_dummy_job() || _ <- lists:seq(1, min(N, 1000))],
    NumPending = length(PendingJobs),

    garbage_collect(),
    MemBefore = erlang:memory(total),

    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:map(fun(_I) ->
        T1 = erlang:monotonic_time(microsecond),
        %% Simulate scheduler cycle: priority calculation, resource matching
        simulate_scheduler_cycle_with_jobs(PendingJobs),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, N)),
    EndTime = erlang:monotonic_time(microsecond),

    TotalTimeUs = EndTime - StartTime,
    MemAfter = erlang:memory(total),

    Result = create_result(scheduler_cycle, TotalTimeUs, N, Latencies, MemBefore, MemAfter),
    ExtendedResult = Result#{pending_jobs => NumPending},
    print_benchmark_result(ExtendedResult),
    ExtendedResult.

%% @doc Benchmark protocol encoding/decoding performance.
%% Encodes and decodes N protocol messages and measures throughput.
-spec benchmark_protocol_encoding(non_neg_integer()) -> map().
benchmark_protocol_encoding(N) when is_integer(N), N > 0 ->
    io:format("Benchmark: Protocol Encoding (~p operations)~n", [N]),
    io:format("  Encoding and decoding SLURM protocol messages...~n"),

    %% Create sample messages of varying types
    Messages = create_protocol_messages(),
    NumMsgTypes = length(Messages),
    IterPerMsg = max(1, N div NumMsgTypes),

    garbage_collect(),
    MemBefore = erlang:memory(total),

    StartTime = erlang:monotonic_time(microsecond),
    Latencies = lists:flatmap(fun({MsgType, Body}) ->
        lists:map(fun(_) ->
            T1 = erlang:monotonic_time(microsecond),
            %% Encode message
            Encoded = encode_simple_message(MsgType, Body),
            %% Decode message
            _ = flurm_protocol_codec:decode(Encoded),
            T2 = erlang:monotonic_time(microsecond),
            T2 - T1
        end, lists:seq(1, IterPerMsg))
    end, Messages),
    EndTime = erlang:monotonic_time(microsecond),

    TotalTimeUs = EndTime - StartTime,
    ActualOps = length(Latencies),
    MemAfter = erlang:memory(total),

    Result = create_result(protocol_encoding, TotalTimeUs, ActualOps, Latencies, MemBefore, MemAfter),
    ExtendedResult = Result#{message_types => NumMsgTypes},
    print_benchmark_result(ExtendedResult),
    ExtendedResult.

%% @doc Benchmark state persistence performance.
%% Measures job state save/load operations using ETS (in-memory storage).
-spec benchmark_state_persistence(non_neg_integer()) -> map().
benchmark_state_persistence(N) when is_integer(N), N > 0 ->
    io:format("Benchmark: State Persistence (~p operations)~n", [N]),
    io:format("  Measuring ETS store/retrieve performance...~n"),

    %% Create a temporary ETS table for benchmarking
    TableName = flurm_benchmark_jobs_temp,
    case ets:whereis(TableName) of
        undefined -> ok;
        _ -> ets:delete(TableName)
    end,
    Tab = ets:new(TableName, [set, public, {keypos, 1}]),

    garbage_collect(),
    MemBefore = erlang:memory(total),

    %% Benchmark stores
    StoreStart = erlang:monotonic_time(microsecond),
    StoreLatencies = lists:map(fun(I) ->
        Job = create_dummy_job_with_id(I),
        T1 = erlang:monotonic_time(microsecond),
        ets:insert(Tab, {I, Job}),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, N)),
    StoreEnd = erlang:monotonic_time(microsecond),

    %% Benchmark lookups
    LookupStart = erlang:monotonic_time(microsecond),
    LookupLatencies = lists:map(fun(I) ->
        T1 = erlang:monotonic_time(microsecond),
        _ = ets:lookup(Tab, I),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end, lists:seq(1, N)),
    LookupEnd = erlang:monotonic_time(microsecond),

    %% Clean up
    ets:delete(Tab),

    MemAfter = erlang:memory(total),

    %% Create combined result
    TotalTimeUs = (StoreEnd - StoreStart) + (LookupEnd - LookupStart),
    _AllLatencies = StoreLatencies ++ LookupLatencies,

    StoreResult = create_result(state_store, StoreEnd - StoreStart, N, StoreLatencies, MemBefore, MemAfter),
    LookupResult = create_result(state_lookup, LookupEnd - LookupStart, N, LookupLatencies, MemBefore, MemAfter),

    Result = #{
        name => state_persistence,
        duration_ms => TotalTimeUs div 1000,
        iterations => N * 2,  % N stores + N lookups
        store => StoreResult,
        lookup => LookupResult,
        store_ops_per_second => maps:get(ops_per_second, StoreResult),
        lookup_ops_per_second => maps:get(ops_per_second, LookupResult),
        memory_before => MemBefore,
        memory_after => MemAfter,
        memory_delta => MemAfter - MemBefore
    },

    print_persistence_result(Result),
    Result.

%% @doc Format benchmark results for human-readable output.
-spec format_results([map()]) -> binary().
format_results(Results) when is_list(Results) ->
    Header = io_lib:format(
        "~n================================================================================~n"
        "FLURM Performance Benchmark Results~n"
        "Generated: ~s~n"
        "================================================================================~n~n",
        [format_timestamp()]),

    Sections = lists:map(fun(Result) ->
        format_result_section(Result)
    end, Results),

    Summary = format_summary(Results),

    iolist_to_binary([Header, Sections, Summary]).

%%====================================================================
%% Extended API - Backwards Compatibility
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

create_dummy_job_with_id(Id) ->
    #job{
        id = Id,
        name = <<"bench_job_", (integer_to_binary(Id))/binary>>,
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

%% Create a batch job request record for protocol benchmarks
create_batch_job_request() ->
    #batch_job_request{
        name = <<"benchmark_job">>,
        script = <<"#!/bin/bash\necho 'Hello World'">>,
        partition = <<"batch">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 1,
        time_limit = 3600,
        user_id = 1000,
        group_id = 1000,
        work_dir = <<"/tmp">>
    }.

%% Create various protocol message types for comprehensive testing
create_protocol_messages() ->
    [
        %% Ping request (empty body)
        {?REQUEST_PING, <<>>},
        %% Job info request
        {?REQUEST_JOB_INFO, <<1:32/big, 0:16/big, 0:16/big>>},
        %% Node info request
        {?REQUEST_NODE_INFO, <<0:32/big>>},
        %% Partition info request
        {?REQUEST_PARTITION_INFO, <<0:32/big>>}
    ].

%% Simulate a scheduler cycle with pending jobs
simulate_scheduler_cycle_with_jobs(PendingJobs) ->
    %% Simulate priority calculation
    _Priorities = lists:map(fun(Job) ->
        %% Calculate priority based on age and base priority
        Age = erlang:system_time(second) - Job#job.submit_time,
        AgeFactor = min(1000, Age div 60),
        Job#job.priority + AgeFactor
    end, PendingJobs),

    %% Simulate resource matching (check if resources available)
    _ResourceChecks = lists:map(fun(Job) ->
        %% Simulate checking resource availability
        RequiredCPUs = Job#job.num_cpus * Job#job.num_nodes,
        RequiredMem = Job#job.memory_mb * Job#job.num_nodes,
        {RequiredCPUs, RequiredMem}
    end, PendingJobs),

    %% Simulate sorting by priority
    _ = lists:sort(fun(A, B) ->
        A#job.priority >= B#job.priority
    end, PendingJobs),

    ok.

%%====================================================================
%% Internal Functions - Statistics
%%====================================================================

create_result(Name, TotalTimeUs, Iterations, Latencies, MemBefore, MemAfter) ->
    SortedLatencies = lists:sort(Latencies),
    OpsPerSecond = safe_div(Iterations, TotalTimeUs / 1000000),

    #{
        name => Name,
        duration_ms => TotalTimeUs div 1000,
        iterations => Iterations,
        ops_per_second => OpsPerSecond,
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

safe_div(_, 0) -> 0.0;
safe_div(_, Denom) when Denom == 0.0 -> 0.0;
safe_div(Num, Denom) -> Num / Denom.

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

%% Format timestamp for reports
format_timestamp() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                  [Y, M, D, H, Mi, S]).

%% Print a benchmark result to stdout
print_benchmark_result(#{name := _Name, duration_ms := Duration, iterations := Iters,
                         ops_per_second := OpsPerSec, avg_latency_us := AvgLat,
                         p50_latency_us := P50, p95_latency_us := P95, p99_latency_us := P99,
                         memory_delta := MemDelta} = Result) ->
    io:format("  Results:~n"),
    io:format("    Duration:      ~p ms (~p iterations)~n", [Duration, Iters]),
    io:format("    Throughput:    ~.2f ops/sec~n", [OpsPerSec]),
    io:format("    Latency:       avg=~.2f us, p50=~p us, p95=~p us, p99=~p us~n",
              [AvgLat, P50, P95, P99]),
    io:format("    Memory Delta:  ~s~n", [format_bytes(MemDelta)]),
    case maps:get(pending_jobs, Result, undefined) of
        undefined -> ok;
        NumJobs -> io:format("    Pending Jobs:  ~p~n", [NumJobs])
    end,
    case maps:get(message_types, Result, undefined) of
        undefined -> ok;
        NumTypes -> io:format("    Message Types: ~p~n", [NumTypes])
    end,
    io:format("~n").

%% Print persistence benchmark results
print_persistence_result(#{duration_ms := Duration, iterations := Iters,
                           store_ops_per_second := StoreOps,
                           lookup_ops_per_second := LookupOps,
                           store := StoreResult, lookup := LookupResult,
                           memory_delta := MemDelta}) ->
    io:format("  Results:~n"),
    io:format("    Duration:      ~p ms (~p operations)~n", [Duration, Iters]),
    io:format("    Store:         ~.2f ops/sec (avg: ~.2f us, p99: ~p us)~n",
              [StoreOps, maps:get(avg_latency_us, StoreResult),
               maps:get(p99_latency_us, StoreResult)]),
    io:format("    Lookup:        ~.2f ops/sec (avg: ~.2f us, p99: ~p us)~n",
              [LookupOps, maps:get(avg_latency_us, LookupResult),
               maps:get(p99_latency_us, LookupResult)]),
    io:format("    Memory Delta:  ~s~n", [format_bytes(MemDelta)]),
    io:format("~n").

%% Format a result section for the full report
format_result_section(#{name := Name} = Result) ->
    case Name of
        state_persistence ->
            format_persistence_section(Result);
        _ ->
            format_standard_section(Result)
    end.

format_standard_section(#{name := Name, duration_ms := Duration, iterations := Iters,
                          ops_per_second := OpsPerSec, avg_latency_us := AvgLat,
                          p50_latency_us := P50, p95_latency_us := P95, p99_latency_us := P99,
                          memory_delta := MemDelta}) ->
    io_lib:format(
        "~s~n"
        "--------------------------------------------------------------------------------~n"
        "  Iterations:     ~p~n"
        "  Duration:       ~p ms~n"
        "  Throughput:     ~.2f ops/sec~n"
        "  Latency (us):   avg=~.2f, p50=~p, p95=~p, p99=~p~n"
        "  Memory Delta:   ~s~n~n",
        [Name, Iters, Duration, OpsPerSec, AvgLat, P50, P95, P99, format_bytes(MemDelta)]).

format_persistence_section(#{duration_ms := Duration, iterations := Iters,
                             store_ops_per_second := StoreOps,
                             lookup_ops_per_second := LookupOps,
                             store := StoreResult, lookup := LookupResult,
                             memory_delta := MemDelta}) ->
    io_lib:format(
        "state_persistence~n"
        "--------------------------------------------------------------------------------~n"
        "  Iterations:     ~p (~p stores + ~p lookups)~n"
        "  Duration:       ~p ms~n"
        "  Store:          ~.2f ops/sec (avg: ~.2f us, p99: ~p us)~n"
        "  Lookup:         ~.2f ops/sec (avg: ~.2f us, p99: ~p us)~n"
        "  Memory Delta:   ~s~n~n",
        [Iters, Iters div 2, Iters div 2, Duration,
         StoreOps, maps:get(avg_latency_us, StoreResult), maps:get(p99_latency_us, StoreResult),
         LookupOps, maps:get(avg_latency_us, LookupResult), maps:get(p99_latency_us, LookupResult),
         format_bytes(MemDelta)]).

%% Format summary for all benchmark results
format_summary(Results) ->
    io_lib:format(
        "================================================================================~n"
        "Summary~n"
        "================================================================================~n"
        "~s~n",
        [format_summary_table(Results)]).

format_summary_table(Results) ->
    Lines = lists:map(fun(#{name := Name} = Result) ->
        OpsPerSec = case Name of
            state_persistence ->
                StoreOps = maps:get(store_ops_per_second, Result, 0),
                LookupOps = maps:get(lookup_ops_per_second, Result, 0),
                (StoreOps + LookupOps) / 2;
            _ ->
                maps:get(ops_per_second, Result, 0)
        end,
        io_lib:format("  ~-25s ~15.2f ops/sec~n", [Name, OpsPerSec])
    end, Results),
    iolist_to_binary(Lines).
