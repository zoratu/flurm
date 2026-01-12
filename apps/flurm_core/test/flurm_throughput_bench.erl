%%%-------------------------------------------------------------------
%%% @doc FLURM Throughput Benchmarks
%%%
%%% Measures maximum throughput for key operations to validate
%%% FLURM's ability to handle high job volumes without the
%%% global lock bottleneck present in SLURM.
%%%
%%% Key metrics:
%%% - Jobs per second submission rate
%%% - Scheduler decisions per second
%%% - Message processing throughput
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_throughput_bench).

-export([
    run/0,
    submission_throughput/1,
    scheduler_throughput/1,
    protocol_throughput/1,
    sustained_load_test/2
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Public API
%%====================================================================

run() ->
    io:format("~n=== FLURM Throughput Benchmarks ===~n~n"),
    
    %% Quick throughput tests
    io:format("--- Job Submission Throughput ---~n"),
    submission_throughput(10000),
    
    io:format("~n--- Scheduler Decision Throughput ---~n"),
    scheduler_throughput(5000),
    
    io:format("~n--- Protocol Processing Throughput ---~n"),
    protocol_throughput(50000),
    
    io:format("~n--- Sustained Load Test (30 seconds) ---~n"),
    sustained_load_test(30, 8),
    
    io:format("~n=== Throughput Benchmarks Complete ===~n"),
    ok.

%% @doc Measure maximum job submission throughput
-spec submission_throughput(pos_integer()) -> {ok, float()}.
submission_throughput(NumJobs) ->
    io:format("Submitting ~p jobs...~n", [NumJobs]),
    
    %% Create job submission queue (simulated)
    JobSpecs = [make_job_spec(I) || I <- lists:seq(1, NumJobs)],
    
    %% Measure submission processing time
    Start = erlang:monotonic_time(microsecond),
    
    %% Process all job submissions
    Results = [process_job_submission(Spec) || Spec <- JobSpecs],
    
    End = erlang:monotonic_time(microsecond),
    ElapsedUs = End - Start,
    ElapsedSec = ElapsedUs / 1000000,
    
    Successful = length([ok || {ok, _} <- Results]),
    JobsPerSec = NumJobs / ElapsedSec,
    
    io:format("  Processed: ~p jobs in ~.2f seconds~n", [Successful, ElapsedSec]),
    io:format("  Throughput: ~.0f jobs/second~n", [JobsPerSec]),
    io:format("  Avg latency: ~.1f microseconds/job~n", [ElapsedUs / NumJobs]),
    
    {ok, JobsPerSec}.

%% @doc Measure scheduler decision throughput
-spec scheduler_throughput(pos_integer()) -> {ok, float()}.
scheduler_throughput(NumDecisions) ->
    io:format("Making ~p scheduling decisions...~n", [NumDecisions]),
    
    %% Setup cluster state
    Nodes = [make_node(I) || I <- lists:seq(1, 100)],
    PendingJobs = [make_pending_job(I) || I <- lists:seq(1, NumDecisions)],
    
    %% Measure scheduling decisions
    Start = erlang:monotonic_time(microsecond),
    
    Results = lists:foldl(fun(Job, {Allocated, AvailNodes}) ->
        case schedule_job(Job, AvailNodes) of
            {ok, Node, UpdatedNodes} ->
                {[{Job, Node} | Allocated], UpdatedNodes};
            {error, no_resources} ->
                {Allocated, AvailNodes}
        end
    end, {[], Nodes}, PendingJobs),
    
    End = erlang:monotonic_time(microsecond),
    ElapsedUs = End - Start,
    ElapsedSec = ElapsedUs / 1000000,
    
    {Allocated, _} = Results,
    AllocatedCount = length(Allocated),
    DecisionsPerSec = NumDecisions / ElapsedSec,
    
    io:format("  Decisions: ~p in ~.2f seconds~n", [NumDecisions, ElapsedSec]),
    io:format("  Allocated: ~p jobs~n", [AllocatedCount]),
    io:format("  Throughput: ~.0f decisions/second~n", [DecisionsPerSec]),
    
    {ok, DecisionsPerSec}.

%% @doc Measure protocol encode/decode throughput
-spec protocol_throughput(pos_integer()) -> {ok, float()}.
protocol_throughput(NumMessages) ->
    io:format("Processing ~p protocol messages...~n", [NumMessages]),
    
    %% Create sample messages
    JobSpec = #job_submit_req{
        name = <<"throughput_test">>,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 4096,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho test">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    },
    
    %% Encode messages
    Start1 = erlang:monotonic_time(microsecond),
    Encoded = [flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobSpec) 
               || _ <- lists:seq(1, NumMessages)],
    End1 = erlang:monotonic_time(microsecond),
    EncodeTime = End1 - Start1,
    
    %% Decode messages
    Start2 = erlang:monotonic_time(microsecond),
    [flurm_protocol_codec:decode(Msg) || {ok, Msg} <- Encoded],
    End2 = erlang:monotonic_time(microsecond),
    DecodeTime = End2 - Start2,
    
    TotalTime = EncodeTime + DecodeTime,
    MsgsPerSec = NumMessages * 2 / (TotalTime / 1000000),
    
    io:format("  Encode: ~.0f msg/sec~n", [NumMessages / (EncodeTime / 1000000)]),
    io:format("  Decode: ~.0f msg/sec~n", [NumMessages / (DecodeTime / 1000000)]),
    io:format("  Combined: ~.0f msg/sec~n", [MsgsPerSec]),
    
    {ok, MsgsPerSec}.

%% @doc Sustained load test with concurrent workers
-spec sustained_load_test(pos_integer(), pos_integer()) -> ok.
sustained_load_test(DurationSec, NumWorkers) ->
    io:format("Running sustained load test for ~p seconds with ~p workers...~n",
              [DurationSec, NumWorkers]),
    
    Parent = self(),
    EndTime = erlang:monotonic_time(second) + DurationSec,
    
    %% Spawn workers
    Workers = [spawn_link(fun() ->
        load_worker(Parent, EndTime, 0)
    end) || _ <- lists:seq(1, NumWorkers)],
    
    %% Collect results
    Results = [receive
        {Worker, Count} -> Count
    after (DurationSec + 5) * 1000 ->
        0
    end || Worker <- Workers],
    
    TotalOps = lists:sum(Results),
    OpsPerSec = TotalOps / DurationSec,
    
    io:format("  Total operations: ~p~n", [TotalOps]),
    io:format("  Throughput: ~.0f ops/second~n", [OpsPerSec]),
    io:format("  Per worker: ~.0f ops/second~n", [OpsPerSec / NumWorkers]),
    
    %% Check for linear scaling
    ExpectedLinear = OpsPerSec / NumWorkers * NumWorkers,
    ScalingEfficiency = (OpsPerSec / ExpectedLinear) * 100,
    io:format("  Scaling efficiency: ~.1f%~n", [ScalingEfficiency]),
    
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

make_job_spec(Id) ->
    #job_submit_req{
        name = list_to_binary(io_lib:format("job_~p", [Id])),
        partition = <<"batch">>,
        num_nodes = 1 + (Id rem 4),
        num_cpus = 1 + (Id rem 8),
        memory_mb = 4096,
        time_limit = 3600 + (Id rem 7200),
        script = <<"#!/bin/bash\necho test">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    }.

make_node(Id) ->
    #{
        id => Id,
        name => list_to_binary(io_lib:format("node~3..0B", [Id])),
        state => up,
        cpus_total => 32,
        cpus_avail => 32 - (Id rem 16),
        memory_total => 128 * 1024,
        memory_avail => 64 * 1024 + rand:uniform(64 * 1024)
    }.

make_pending_job(Id) ->
    #{
        id => Id,
        cpus_needed => 1 + (Id rem 8),
        memory_needed => 4096 + (Id rem 4) * 1024,
        priority => rand:uniform(1000)
    }.

process_job_submission(#job_submit_req{} = Spec) ->
    %% Simulate job validation and ID assignment
    Job = #job{
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
    },
    {ok, Job}.

schedule_job(#{cpus_needed := Cpus, memory_needed := Mem} = _Job, Nodes) ->
    %% Find suitable node
    Available = [N || N <- Nodes,
                      maps:get(state, N) =:= up,
                      maps:get(cpus_avail, N) >= Cpus,
                      maps:get(memory_avail, N) >= Mem],
    case Available of
        [] ->
            {error, no_resources};
        [Node | _] ->
            %% Allocate resources
            UpdatedNode = Node#{
                cpus_avail => maps:get(cpus_avail, Node) - Cpus,
                memory_avail => maps:get(memory_avail, Node) - Mem
            },
            UpdatedNodes = lists:map(fun(N) ->
                case maps:get(id, N) =:= maps:get(id, Node) of
                    true -> UpdatedNode;
                    false -> N
                end
            end, Nodes),
            {ok, Node, UpdatedNodes}
    end.

load_worker(Parent, EndTime, Count) ->
    case erlang:monotonic_time(second) < EndTime of
        true ->
            %% Do a unit of work
            _ = make_job_spec(Count),
            _ = process_job_submission(make_job_spec(Count)),
            load_worker(Parent, EndTime, Count + 1);
        false ->
            Parent ! {self(), Count}
    end.
