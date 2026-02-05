%%%-------------------------------------------------------------------
%%% @doc FLURM Performance Baseline Tests
%%%
%%% Measures and validates critical performance metrics for FLURM
%%% components that can be tested in isolation:
%%%
%%% - TRES calculation performance
%%% - TRES aggregation performance
%%% - Memory stability under load
%%% - Throughput benchmarks
%%%
%%% For end-to-end performance testing (job submission latency, failover),
%%% use the integration test suites with Docker.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_perf_baseline_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_perf_baseline_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Configuration
%%====================================================================

%% Performance targets
-define(TRES_P99_TARGET_MS, 0.1).           % <0.1ms p99 TRES calculation
-define(TRES_ADD_P99_TARGET_MS, 0.1).       % <0.1ms p99 TRES add
-define(MIN_THROUGHPUT, 100000).            % >100k ops/sec

%% Test parameters
-define(ITERATIONS, 10000).
-define(WARMUP_ITERATIONS, 1000).

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% TRES Calculation Performance Tests
%%====================================================================

tres_performance_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
         {"TRES calculation p99 <0.1ms", fun tres_calculation_performance/0},
         {"TRES aggregation p99 <0.1ms", fun tres_aggregation_performance/0},
         {"TRES format/parse roundtrip", fun tres_format_parse_performance/0}
     ]}}.

tres_calculation_performance() ->
    %% Warmup
    JobInfo = make_test_job_info(),
    lists:foreach(fun(_) -> flurm_tres:from_job(JobInfo) end,
                  lists:seq(1, ?WARMUP_ITERATIONS)),

    %% Measure
    {Times, _} = measure_iterations(
        fun() -> flurm_tres:from_job(JobInfo) end,
        ?ITERATIONS
    ),
    Stats = calculate_stats(Times),

    ?debugFmt("~nTRES calculation: avg=~.4fms, p50=~.4fms, p95=~.4fms, p99=~.4fms~n",
              [maps:get(avg, Stats), maps:get(p50, Stats),
               maps:get(p95, Stats), maps:get(p99, Stats)]),

    P99 = maps:get(p99, Stats),
    ?assert(P99 < ?TRES_P99_TARGET_MS,
            lists:flatten(io_lib:format("TRES calc p99 ~.4fms exceeds target ~.4fms",
                         [P99, ?TRES_P99_TARGET_MS]))).

tres_aggregation_performance() ->
    Map1 = #{cpu_seconds => 1000, mem_seconds => 2000, gpu_seconds => 500,
             node_seconds => 100, job_count => 1, job_time => 3600},
    Map2 = #{cpu_seconds => 500, mem_seconds => 1000, gpu_seconds => 250,
             node_seconds => 50, job_count => 1, job_time => 1800},

    %% Warmup
    lists:foreach(fun(_) -> flurm_tres:add(Map1, Map2) end,
                  lists:seq(1, ?WARMUP_ITERATIONS)),

    %% Measure
    {Times, _} = measure_iterations(
        fun() -> flurm_tres:add(Map1, Map2) end,
        ?ITERATIONS
    ),
    Stats = calculate_stats(Times),

    ?debugFmt("TRES aggregation: avg=~.4fms, p50=~.4fms, p95=~.4fms, p99=~.4fms~n",
              [maps:get(avg, Stats), maps:get(p50, Stats),
               maps:get(p95, Stats), maps:get(p99, Stats)]),

    P99 = maps:get(p99, Stats),
    ?assert(P99 < ?TRES_ADD_P99_TARGET_MS,
            lists:flatten(io_lib:format("TRES add p99 ~.4fms exceeds target ~.4fms",
                         [P99, ?TRES_ADD_P99_TARGET_MS]))).

tres_format_parse_performance() ->
    TresMap = #{cpu_seconds => 14400, mem_seconds => 65536000,
                gpu_seconds => 7200, node_seconds => 3600,
                billing => 25000},

    %% Warmup
    lists:foreach(fun(_) ->
        Formatted = flurm_tres:format(TresMap),
        _ = flurm_tres:parse(Formatted)
    end, lists:seq(1, ?WARMUP_ITERATIONS)),

    %% Measure format
    {FormatTimes, _} = measure_iterations(
        fun() -> flurm_tres:format(TresMap) end,
        ?ITERATIONS
    ),
    FormatStats = calculate_stats(FormatTimes),

    %% Measure parse
    Formatted = flurm_tres:format(TresMap),
    {ParseTimes, _} = measure_iterations(
        fun() -> flurm_tres:parse(Formatted) end,
        ?ITERATIONS
    ),
    ParseStats = calculate_stats(ParseTimes),

    ?debugFmt("TRES format: avg=~.4fms, p99=~.4fms~n",
              [maps:get(avg, FormatStats), maps:get(p99, FormatStats)]),
    ?debugFmt("TRES parse: avg=~.4fms, p99=~.4fms~n",
              [maps:get(avg, ParseStats), maps:get(p99, ParseStats)]),

    ?assert(maps:get(p99, FormatStats) < 0.5),
    ?assert(maps:get(p99, ParseStats) < 0.5).

%%====================================================================
%% Memory and GC Impact Tests
%%====================================================================

memory_stability_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
         {"TRES operations don't leak memory", fun tres_memory_stability/0}
     ]}}.

tres_memory_stability() ->
    erlang:garbage_collect(),
    {memory, MemBefore} = erlang:process_info(self(), memory),

    %% Run many iterations with varying input
    lists:foreach(fun(I) ->
        JobInfo = #{elapsed => I rem 10000 + 1,
                    num_cpus => I rem 64 + 1,
                    req_mem => I rem 100000 + 1000,
                    num_nodes => I rem 10 + 1,
                    tres_alloc => #{gpu => I rem 8}},
        T1 = flurm_tres:from_job(JobInfo),
        T2 = flurm_tres:from_job(JobInfo),
        _ = flurm_tres:add(T1, T2),
        _ = flurm_tres:format(T1),
        _ = flurm_tres:parse(<<"cpu=100,mem=200">>)
    end, lists:seq(1, 50000)),

    erlang:garbage_collect(),
    {memory, MemAfter} = erlang:process_info(self(), memory),

    MemGrowthKb = (MemAfter - MemBefore) / 1024,
    ?debugFmt("~nTRES memory growth after 50k iterations: ~.2f KB~n", [MemGrowthKb]),

    %% Allow reasonable growth for test framework overhead
    ?assert(MemGrowthKb < 1024,
            lists:flatten(io_lib:format("Memory growth ~.2f KB exceeds 1MB limit",
                                        [MemGrowthKb]))).

%%====================================================================
%% Throughput Tests
%%====================================================================

throughput_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
         {"TRES throughput >100k/sec", fun tres_throughput/0}
     ]}}.

tres_throughput() ->
    JobInfo = make_test_job_info(),
    N = 100000,

    %% Warmup
    lists:foreach(fun(_) -> flurm_tres:from_job(JobInfo) end,
                  lists:seq(1, ?WARMUP_ITERATIONS)),

    %% Measure from_job throughput
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) ->
        _ = flurm_tres:from_job(JobInfo)
    end, lists:seq(1, N)),
    End = erlang:monotonic_time(microsecond),

    DurationSec = (End - Start) / 1000000,
    OpsPerSec = N / DurationSec,

    ?debugFmt("~nTRES from_job throughput: ~B ops/sec (~.2f sec for ~B ops)~n",
              [round(OpsPerSec), DurationSec, N]),

    ?assert(OpsPerSec > ?MIN_THROUGHPUT,
            lists:flatten(io_lib:format("Throughput ~B/sec below target ~B/sec",
                                        [round(OpsPerSec), ?MIN_THROUGHPUT]))).

%%====================================================================
%% Concurrent Access Tests
%%====================================================================

concurrent_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
         {"TRES concurrent access safe", fun tres_concurrent_safety/0}
     ]}}.

tres_concurrent_safety() ->
    %% Spawn multiple processes doing TRES operations concurrently
    Parent = self(),
    NumProcs = 10,
    OpsPerProc = 10000,

    Pids = [spawn_link(fun() ->
        JobInfo = make_test_job_info(),
        lists:foreach(fun(_) ->
            T = flurm_tres:from_job(JobInfo),
            _ = flurm_tres:add(T, T),
            _ = flurm_tres:format(T)
        end, lists:seq(1, OpsPerProc)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Wait for all processes
    lists:foreach(fun(Pid) ->
        receive {done, Pid} -> ok
        after 30000 -> error({timeout, Pid})
        end
    end, Pids),

    TotalOps = NumProcs * OpsPerProc,
    ?debugFmt("~nConcurrent TRES: ~B procs x ~B ops = ~B total ops completed~n",
              [NumProcs, OpsPerProc, TotalOps]),
    ok.

%%====================================================================
%% Performance Report
%%====================================================================

performance_report_test_() ->
    {timeout, 60, {"generate performance report", fun generate_performance_report/0}}.

generate_performance_report() ->
    ?debugFmt("~n~n======================================~n", []),
    ?debugFmt("    FLURM PERFORMANCE BASELINE~n", []),
    ?debugFmt("======================================~n~n", []),

    ?debugFmt("Test Environment:~n", []),
    ?debugFmt("  - Erlang: ~s~n", [erlang:system_info(otp_release)]),
    ?debugFmt("  - Schedulers: ~B~n", [erlang:system_info(schedulers)]),
    ?debugFmt("  - Process limit: ~B~n~n", [erlang:system_info(process_limit)]),

    %% TRES from_job benchmark
    JobInfo = make_test_job_info(),
    {FromJobTimes, _} = measure_iterations(
        fun() -> flurm_tres:from_job(JobInfo) end, 5000),
    FromJobStats = calculate_stats(FromJobTimes),

    %% TRES add benchmark
    T1 = flurm_tres:from_job(JobInfo),
    T2 = flurm_tres:from_job(JobInfo),
    {AddTimes, _} = measure_iterations(
        fun() -> flurm_tres:add(T1, T2) end, 5000),
    AddStats = calculate_stats(AddTimes),

    %% TRES format benchmark
    {FormatTimes, _} = measure_iterations(
        fun() -> flurm_tres:format(T1) end, 5000),
    FormatStats = calculate_stats(FormatTimes),

    ?debugFmt("TRES Performance (5000 iterations each):~n", []),
    ?debugFmt("  from_job: avg=~.4fms, p50=~.4fms, p95=~.4fms, p99=~.4fms~n",
              [maps:get(avg, FromJobStats), maps:get(p50, FromJobStats),
               maps:get(p95, FromJobStats), maps:get(p99, FromJobStats)]),
    ?debugFmt("  add:      avg=~.4fms, p50=~.4fms, p95=~.4fms, p99=~.4fms~n",
              [maps:get(avg, AddStats), maps:get(p50, AddStats),
               maps:get(p95, AddStats), maps:get(p99, AddStats)]),
    ?debugFmt("  format:   avg=~.4fms, p50=~.4fms, p95=~.4fms, p99=~.4fms~n~n",
              [maps:get(avg, FormatStats), maps:get(p50, FormatStats),
               maps:get(p95, FormatStats), maps:get(p99, FormatStats)]),

    %% Throughput
    N = 100000,
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) -> flurm_tres:from_job(JobInfo) end, lists:seq(1, N)),
    End = erlang:monotonic_time(microsecond),
    OpsPerSec = N / ((End - Start) / 1000000),

    ?debugFmt("Throughput: ~B TRES calculations/sec~n~n", [round(OpsPerSec)]),

    ?debugFmt("======================================~n~n", []),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @private Create test job info map
make_test_job_info() ->
    #{
        elapsed => 3600,
        num_cpus => 16,
        req_mem => 65536,
        num_nodes => 4,
        tres_alloc => #{gpu => 2}
    }.

%% @private Measure execution time for N iterations
-spec measure_iterations(fun(() -> term()), pos_integer()) -> {[float()], [term()]}.
measure_iterations(Fun, N) ->
    Results = lists:map(fun(_) ->
        Start = erlang:monotonic_time(microsecond),
        Result = Fun(),
        End = erlang:monotonic_time(microsecond),
        {(End - Start) / 1000, Result}  % Convert to milliseconds
    end, lists:seq(1, N)),
    lists:unzip(Results).

%% @private Calculate statistics from a list of timing values
-spec calculate_stats([float()]) -> map().
calculate_stats(Times) ->
    Sorted = lists:sort(Times),
    N = length(Sorted),
    Sum = lists:sum(Times),
    Avg = Sum / N,
    P50 = lists:nth(max(1, round(N * 0.50)), Sorted),
    P95 = lists:nth(max(1, round(N * 0.95)), Sorted),
    P99 = lists:nth(max(1, round(N * 0.99)), Sorted),
    Min = hd(Sorted),
    Max = lists:last(Sorted),
    #{
        avg => Avg,
        min => Min,
        max => Max,
        p50 => P50,
        p95 => P95,
        p99 => P99,
        count => N
    }.
