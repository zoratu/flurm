%%%-------------------------------------------------------------------
%%% @doc FLURM Latency Benchmarks
%%%
%%% Measures latency distributions for critical operations.
%%% Generates histograms and percentile data to understand
%%% tail latencies under various conditions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_latency_bench).

-export([
    run/0,
    measure_submission_latency/1,
    measure_query_latency/1,
    measure_cancel_latency/1,
    measure_under_load/2,
    histogram/2
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Public API
%%====================================================================

run() ->
    io:format("~n=== FLURM Latency Benchmarks ===~n~n"),
    
    io:format("--- Submission Latency ---~n"),
    L1 = measure_submission_latency(5000),
    print_latency_stats("Job Submission", L1),
    
    io:format("~n--- Query Latency ---~n"),
    L2 = measure_query_latency(5000),
    print_latency_stats("Job Query", L2),
    
    io:format("~n--- Cancel Latency ---~n"),
    L3 = measure_cancel_latency(1000),
    print_latency_stats("Job Cancel", L3),
    
    io:format("~n--- Latency Under Load ---~n"),
    measure_under_load(1000, 4),
    
    io:format("~n=== Latency Benchmarks Complete ===~n"),
    ok.

%% @doc Measure job submission latency
-spec measure_submission_latency(pos_integer()) -> [non_neg_integer()].
measure_submission_latency(N) ->
    io:format("Measuring ~p submission latencies...~n", [N]),
    [measure_single_submission() || _ <- lists:seq(1, N)].

%% @doc Measure job query latency
-spec measure_query_latency(pos_integer()) -> [non_neg_integer()].
measure_query_latency(N) ->
    io:format("Measuring ~p query latencies...~n", [N]),
    
    %% Create some jobs to query
    Jobs = maps:from_list([{I, make_job(I)} || I <- lists:seq(1, 1000)]),
    [measure_single_query(Jobs) || _ <- lists:seq(1, N)].

%% @doc Measure job cancellation latency
-spec measure_cancel_latency(pos_integer()) -> [non_neg_integer()].
measure_cancel_latency(N) ->
    io:format("Measuring ~p cancel latencies...~n", [N]),
    
    %% Create jobs to cancel
    Jobs = [make_job(I) || I <- lists:seq(1, N)],
    [measure_single_cancel(Job) || Job <- Jobs].

%% @doc Measure latency under concurrent load
-spec measure_under_load(pos_integer(), pos_integer()) -> ok.
measure_under_load(OpsPerWorker, NumWorkers) ->
    io:format("Measuring latency with ~p workers, ~p ops each...~n", 
              [NumWorkers, OpsPerWorker]),
    
    Parent = self(),
    
    %% Spawn workers
    Workers = [spawn_link(fun() ->
        Latencies = [measure_single_submission() || _ <- lists:seq(1, OpsPerWorker)],
        Parent ! {self(), Latencies}
    end) || _ <- lists:seq(1, NumWorkers)],
    
    %% Collect all latencies
    AllLatencies = lists:flatmap(fun(Worker) ->
        receive
            {Worker, Latencies} -> Latencies
        after 60000 ->
            []
        end
    end, Workers),
    
    print_latency_stats("Under Load", AllLatencies),
    
    %% Print histogram
    io:format("~n  Latency Histogram (microseconds):~n"),
    histogram(AllLatencies, 10),
    
    ok.

%% @doc Generate and print histogram
-spec histogram([non_neg_integer()], pos_integer()) -> ok.
histogram(Values, NumBuckets) ->
    case Values of
        [] -> 
            io:format("    No data~n");
        _ ->
            Min = lists:min(Values),
            Max = lists:max(Values),
            Range = max(1, Max - Min),
            BucketSize = max(1, Range div NumBuckets),
            
            %% Count values in each bucket
            Counts = lists:foldl(fun(V, Acc) ->
                BucketIdx = min(NumBuckets, (V - Min) div BucketSize + 1),
                maps:update_with(BucketIdx, fun(C) -> C + 1 end, 1, Acc)
            end, #{}, Values),
            
            %% Print histogram
            MaxCount = lists:max(maps:values(Counts)),
            BarWidth = 40,
            
            lists:foreach(fun(I) ->
                BucketStart = Min + (I - 1) * BucketSize,
                BucketEnd = Min + I * BucketSize,
                Count = maps:get(I, Counts, 0),
                BarLen = round(Count / max(1, MaxCount) * BarWidth),
                Bar = lists:duplicate(BarLen, $#),
                io:format("    ~6B-~6B us: ~s (~p)~n", 
                          [BucketStart, BucketEnd, Bar, Count])
            end, lists:seq(1, NumBuckets))
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

measure_single_submission() ->
    Start = erlang:monotonic_time(microsecond),
    
    %% Simulate full submission path
    JobSpec = make_job_spec(),
    _ = validate_job_spec(JobSpec),
    JobId = erlang:unique_integer([positive]),
    _ = create_job_record(JobId, JobSpec),
    
    End = erlang:monotonic_time(microsecond),
    End - Start.

measure_single_query(Jobs) ->
    Start = erlang:monotonic_time(microsecond),
    
    %% Query a random job
    JobId = rand:uniform(1000),
    _ = maps:get(JobId, Jobs, undefined),
    
    End = erlang:monotonic_time(microsecond),
    End - Start.

measure_single_cancel(#job{id = JobId} = Job) ->
    Start = erlang:monotonic_time(microsecond),

    %% Simulate cancel operation
    _ = Job#job{state = cancelled, end_time = erlang:system_time(second)},
    _ = JobId,  % Would update in ETS/storage

    End = erlang:monotonic_time(microsecond),
    End - Start.

make_job_spec() ->
    #job_submit_req{
        name = <<"latency_test">>,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 4096,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho test">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    }.

make_job(_Id) ->
    #job{
        id = erlang:unique_integer([positive]),
        name = <<"test">>,
        user = <<"testuser">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 4096,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

validate_job_spec(#job_submit_req{num_nodes = N, time_limit = T})
  when N > 0, T > 0 ->
    ok;
validate_job_spec(_) ->
    {error, invalid_spec}.

create_job_record(_JobId, #job_submit_req{} = Spec) ->
    #job{
        id = erlang:unique_integer([positive]),
        name = Spec#job_submit_req.name,
        user = <<"testuser">>,
        partition = Spec#job_submit_req.partition,
        state = pending,
        script = Spec#job_submit_req.script,
        num_nodes = Spec#job_submit_req.num_nodes,
        num_cpus = Spec#job_submit_req.num_cpus,
        memory_mb = Spec#job_submit_req.memory_mb,
        time_limit = Spec#job_submit_req.time_limit,
        priority = Spec#job_submit_req.priority,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

print_latency_stats(Name, Latencies) ->
    case Latencies of
        [] ->
            io:format("  ~s: No data~n", [Name]);
        _ ->
            Sorted = lists:sort(Latencies),
            Len = length(Sorted),
            
            Min = hd(Sorted),
            Max = lists:last(Sorted),
            Avg = lists:sum(Sorted) / Len,
            Median = lists:nth(max(1, Len div 2), Sorted),
            P90 = lists:nth(max(1, round(Len * 0.90)), Sorted),
            P95 = lists:nth(max(1, round(Len * 0.95)), Sorted),
            P99 = lists:nth(max(1, round(Len * 0.99)), Sorted),
            P999 = lists:nth(max(1, round(Len * 0.999)), Sorted),
            
            io:format("  ~s (~p samples):~n", [Name, Len]),
            io:format("    Min:    ~6B us~n", [Min]),
            io:format("    Avg:    ~6.1f us~n", [Avg]),
            io:format("    Median: ~6B us~n", [Median]),
            io:format("    P90:    ~6B us~n", [P90]),
            io:format("    P95:    ~6B us~n", [P95]),
            io:format("    P99:    ~6B us~n", [P99]),
            io:format("    P99.9:  ~6B us~n", [P999]),
            io:format("    Max:    ~6B us~n", [Max])
    end.
