%%%-------------------------------------------------------------------
%%% @doc FLURM Performance Benchmarking Suite
%%%
%%% Comprehensive performance benchmarks for FLURM components.
%%% Measures throughput, latency, and scalability.
%%%
%%% Usage:
%%%   flurm_bench:run_all().           % Run all benchmarks
%%%   flurm_bench:bench_protocol(1000). % Protocol codec benchmark
%%%   flurm_bench:bench_scheduler(100). % Scheduler benchmark
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_bench).

-export([
    run_all/0,
    bench_protocol/1,
    bench_scheduler/1,
    bench_job_lifecycle/1,
    bench_node_management/1,
    bench_raft_consensus/1,
    bench_concurrent_submits/2,
    report/1
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-record(bench_result, {
    name :: atom(),
    iterations :: pos_integer(),
    total_time_us :: non_neg_integer(),
    min_us :: non_neg_integer(),
    max_us :: non_neg_integer(),
    avg_us :: float(),
    median_us :: non_neg_integer(),
    p95_us :: non_neg_integer(),
    p99_us :: non_neg_integer(),
    ops_per_sec :: float()
}).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run all benchmarks
-spec run_all() -> ok.
run_all() ->
    io:format("~n=== FLURM Performance Benchmarks ===~n~n"),
    
    %% Protocol benchmarks
    io:format("--- Protocol Codec Benchmarks ---~n"),
    R1 = bench_protocol(10000),
    report(R1),
    
    %% Scheduler benchmarks
    io:format("~n--- Scheduler Benchmarks ---~n"),
    R2 = bench_scheduler(1000),
    report(R2),
    
    %% Job lifecycle benchmarks
    io:format("~n--- Job Lifecycle Benchmarks ---~n"),
    R3 = bench_job_lifecycle(500),
    report(R3),
    
    %% Node management benchmarks
    io:format("~n--- Node Management Benchmarks ---~n"),
    R4 = bench_node_management(1000),
    report(R4),
    
    %% Concurrent submission benchmarks
    io:format("~n--- Concurrent Submission Benchmarks ---~n"),
    R5 = bench_concurrent_submits(100, 10),
    report(R5),
    
    io:format("~n=== Benchmarks Complete ===~n"),
    ok.

%% @doc Benchmark protocol encode/decode
-spec bench_protocol(pos_integer()) -> [#bench_result{}].
bench_protocol(N) ->
    io:format("Running protocol benchmarks (~p iterations)...~n", [N]),
    
    %% Benchmark message encoding
    EncodeResult = benchmark(protocol_encode, N, fun() ->
        JobSpec = make_sample_job_spec(),
        flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobSpec)
    end),
    
    %% Benchmark message decoding
    {ok, EncodedMsg} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, 
                                                    make_sample_job_spec()),
    DecodeResult = benchmark(protocol_decode, N, fun() ->
        flurm_protocol_codec:decode(EncodedMsg)
    end),
    
    %% Benchmark round-trip
    RoundtripResult = benchmark(protocol_roundtrip, N, fun() ->
        JobSpec = make_sample_job_spec(),
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, JobSpec),
        flurm_protocol_codec:decode(Encoded)
    end),
    
    %% Benchmark header parsing
    <<_Len:32/big, HeaderAndBody/binary>> = EncodedMsg,
    HeaderResult = benchmark(header_parse, N * 10, fun() ->
        flurm_protocol_codec:decode_header(HeaderAndBody)
    end),
    
    [EncodeResult, DecodeResult, RoundtripResult, HeaderResult].

%% @doc Benchmark scheduler operations
-spec bench_scheduler(pos_integer()) -> [#bench_result{}].
bench_scheduler(N) ->
    io:format("Running scheduler benchmarks (~p iterations)...~n", [N]),
    
    %% Setup mock cluster state
    Nodes = [make_mock_node(I) || I <- lists:seq(1, 100)],
    Jobs = [make_mock_job(I) || I <- lists:seq(1, N)],
    
    %% Benchmark job sorting/prioritization
    SortResult = benchmark(job_priority_sort, N div 10, fun() ->
        lists:sort(fun compare_job_priority/2, Jobs)
    end),
    
    %% Benchmark node selection
    SelectResult = benchmark(node_selection, N, fun() ->
        find_best_node(Nodes, 4, 8192)
    end),
    
    %% Benchmark resource matching
    MatchResult = benchmark(resource_matching, N, fun() ->
        Job = lists:nth(rand:uniform(length(Jobs)), Jobs),
        match_resources(Job, Nodes)
    end),
    
    %% Benchmark backfill calculation
    BackfillResult = benchmark(backfill_calc, N div 10, fun() ->
        calculate_backfill_window(Jobs, Nodes)
    end),
    
    [SortResult, SelectResult, MatchResult, BackfillResult].

%% @doc Benchmark job state machine lifecycle
-spec bench_job_lifecycle(pos_integer()) -> [#bench_result{}].
bench_job_lifecycle(N) ->
    io:format("Running job lifecycle benchmarks (~p iterations)...~n", [N]),
    
    %% Benchmark job record creation
    CreateResult = benchmark(job_create, N, fun() ->
        make_mock_job(rand:uniform(1000000))
    end),
    
    %% Benchmark state transitions (simulated)
    TransitionResult = benchmark(state_transition, N * 5, fun() ->
        simulate_state_transition()
    end),
    
    %% Benchmark job map operations
    JobMap = maps:from_list([{I, make_mock_job(I)} || I <- lists:seq(1, 1000)]),
    MapOpsResult = benchmark(job_map_ops, N * 10, fun() ->
        Key = rand:uniform(1000),
        case maps:get(Key, JobMap, undefined) of
            undefined -> ok;
            Job -> maps:put(Key, Job#job{state = running}, JobMap)
        end
    end),
    
    [CreateResult, TransitionResult, MapOpsResult].

%% @doc Benchmark node management operations
-spec bench_node_management(pos_integer()) -> [#bench_result{}].
bench_node_management(N) ->
    io:format("Running node management benchmarks (~p iterations)...~n", [N]),
    
    %% Create node registry
    Nodes = maps:from_list([{I, make_mock_node(I)} || I <- lists:seq(1, 500)]),
    
    %% Benchmark node lookup
    LookupResult = benchmark(node_lookup, N * 10, fun() ->
        maps:get(rand:uniform(500), Nodes, undefined)
    end),
    
    %% Benchmark node filtering (find available nodes)
    FilterResult = benchmark(node_filter, N, fun() ->
        [Node || {_, Node} <- maps:to_list(Nodes),
                 Node#node.state =:= up,
                 Node#node.cpus >= 4]
    end),

    %% Benchmark node state update
    UpdateResult = benchmark(node_update, N, fun() ->
        Key = rand:uniform(500),
        Node = maps:get(Key, Nodes),
        maps:put(Key, Node#node{free_memory_mb = rand:uniform(64 * 1024)}, Nodes)
    end),
    
    %% Benchmark heartbeat processing
    HeartbeatResult = benchmark(heartbeat_process, N, fun() ->
        process_heartbeat(rand:uniform(500), Nodes)
    end),
    
    [LookupResult, FilterResult, UpdateResult, HeartbeatResult].

%% @doc Benchmark Raft consensus operations (simulated)
-spec bench_raft_consensus(pos_integer()) -> [#bench_result{}].
bench_raft_consensus(N) ->
    io:format("Running Raft consensus benchmarks (~p iterations)...~n", [N]),
    
    %% Benchmark log entry serialization
    LogEntry = {job_submit, make_mock_job(1)},
    SerializeResult = benchmark(log_serialize, N, fun() ->
        term_to_binary(LogEntry)
    end),
    
    %% Benchmark log entry deserialization
    Serialized = term_to_binary(LogEntry),
    DeserializeResult = benchmark(log_deserialize, N, fun() ->
        binary_to_term(Serialized)
    end),
    
    [SerializeResult, DeserializeResult].

%% @doc Benchmark concurrent job submissions
-spec bench_concurrent_submits(pos_integer(), pos_integer()) -> [#bench_result{}].
bench_concurrent_submits(NumJobs, NumWorkers) ->
    io:format("Running concurrent submission benchmark (~p jobs, ~p workers)...~n", 
              [NumJobs, NumWorkers]),
    
    JobsPerWorker = NumJobs div NumWorkers,
    Parent = self(),
    
    %% Measure total time for concurrent submissions
    Start = erlang:monotonic_time(microsecond),
    
    Workers = [spawn_link(fun() ->
        Times = [begin
            T1 = erlang:monotonic_time(microsecond),
            _Job = make_mock_job(I),
            T2 = erlang:monotonic_time(microsecond),
            T2 - T1
        end || I <- lists:seq(1, JobsPerWorker)],
        Parent ! {self(), Times}
    end) || _ <- lists:seq(1, NumWorkers)],
    
    %% Collect results
    AllTimes = lists:flatmap(fun(Worker) ->
        receive
            {Worker, Times} -> Times
        after 60000 ->
            []
        end
    end, Workers),
    
    End = erlang:monotonic_time(microsecond),
    TotalTime = End - Start,
    
    [make_result(concurrent_submit, NumJobs, TotalTime, AllTimes)].

%% @doc Pretty-print benchmark results
-spec report([#bench_result{}]) -> ok.
report(Results) ->
    lists:foreach(fun(#bench_result{} = R) ->
        io:format("  ~-25s: ~.2f ops/sec (avg: ~.1f us, p95: ~p us, p99: ~p us)~n",
                  [R#bench_result.name, R#bench_result.ops_per_sec,
                   R#bench_result.avg_us, R#bench_result.p95_us, R#bench_result.p99_us])
    end, Results).

%%====================================================================
%% Internal Functions - Benchmarking Infrastructure
%%====================================================================

benchmark(Name, N, Fun) ->
    %% Warmup
    [Fun() || _ <- lists:seq(1, min(100, N div 10))],
    
    %% Actual benchmark
    Times = [begin
        T1 = erlang:monotonic_time(microsecond),
        Fun(),
        T2 = erlang:monotonic_time(microsecond),
        T2 - T1
    end || _ <- lists:seq(1, N)],
    
    TotalTime = lists:sum(Times),
    make_result(Name, N, TotalTime, Times).

make_result(Name, N, TotalTime, Times) ->
    Sorted = lists:sort(Times),
    Len = length(Sorted),
    P95Idx = max(1, round(Len * 0.95)),
    P99Idx = max(1, round(Len * 0.99)),
    MedianIdx = max(1, Len div 2),
    
    #bench_result{
        name = Name,
        iterations = N,
        total_time_us = TotalTime,
        min_us = hd(Sorted),
        max_us = lists:last(Sorted),
        avg_us = TotalTime / max(1, N),
        median_us = lists:nth(MedianIdx, Sorted),
        p95_us = lists:nth(P95Idx, Sorted),
        p99_us = lists:nth(P99Idx, Sorted),
        ops_per_sec = N * 1000000 / max(1, TotalTime)
    }.

%%====================================================================
%% Internal Functions - Mock Data Generators
%%====================================================================

make_sample_job_spec() ->
    #job_submit_req{
        name = <<"benchmark_job">>,
        partition = <<"batch">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 8192,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'benchmark'\nsleep 1">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    }.

make_mock_job(Id) ->
    #job{
        id = Id,
        name = list_to_binary(io_lib:format("job_~p", [Id])),
        user = <<"bench_user">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        priority = rand:uniform(1000),
        num_nodes = 1 + (Id rem 4),
        num_cpus = 4 * (1 + (Id rem 4)),
        memory_mb = 8192,
        time_limit = 3600,
        submit_time = erlang:system_time(second) - rand:uniform(3600),
        allocated_nodes = []
    }.

make_mock_node(Id) ->
    CpusTotal = 32 + (Id rem 4) * 8,
    #node{
        hostname = list_to_binary(io_lib:format("node~3..0B", [Id])),
        state = case rand:uniform(10) of
            1 -> down;
            2 -> drain;
            _ -> up
        end,
        cpus = CpusTotal,
        memory_mb = 128 * 1024,  % 128 GB
        features = [],
        partitions = [<<"batch">>],
        running_jobs = [],
        load_avg = rand:uniform() * 10,
        free_memory_mb = 64 * 1024 + rand:uniform(64 * 1024),
        last_heartbeat = erlang:system_time(second),
        allocations = #{}
    }.

%%====================================================================
%% Internal Functions - Simulated Operations
%%====================================================================

compare_job_priority(#job{priority = P1, submit_time = T1},
                     #job{priority = P2, submit_time = T2}) ->
    case P1 == P2 of
        true -> T1 =< T2;
        false -> P1 > P2
    end.

find_best_node(Nodes, CpusNeeded, MemNeeded) ->
    Available = [Node || Node <- Nodes,
                         Node#node.state =:= up,
                         Node#node.cpus >= CpusNeeded,
                         Node#node.free_memory_mb >= MemNeeded],
    case Available of
        [] -> none;
        _ ->
            %% Pick node with most available resources (best-fit)
            lists:min([{Node#node.cpus, Node} || Node <- Available])
    end.

match_resources(#job{num_cpus = Cpus, memory_mb = Mem}, Nodes) ->
    [Node || Node <- Nodes,
             Node#node.state =:= up,
             Node#node.cpus >= Cpus,
             Node#node.free_memory_mb >= Mem].

calculate_backfill_window(Jobs, Nodes) ->
    RunningJobs = [J || J <- Jobs, J#job.state =:= running],
    TotalCpus = lists:sum([N#node.cpus || N <- Nodes, N#node.state =:= up]),
    UsedCpus = lists:sum([J#job.num_cpus || J <- RunningJobs]),
    AvailCpus = TotalCpus - UsedCpus,
    {AvailCpus, length(RunningJobs)}.

simulate_state_transition() ->
    States = [pending, configuring, running, completing, completed],
    From = lists:nth(rand:uniform(4), States),
    To = lists:nth(rand:uniform(length(States) - 1) + 1, 
                   lists:nthtail(1, lists:dropwhile(fun(S) -> S =/= From end, States) 
                                     ++ [completed])),
    {From, To}.

process_heartbeat(NodeId, Nodes) ->
    case maps:get(NodeId, Nodes, undefined) of
        undefined -> {error, unknown_node};
        Node ->
            Now = erlang:system_time(second),
            UpdatedNode = Node#node{last_heartbeat = Now},
            {ok, maps:put(NodeId, UpdatedNode, Nodes)}
    end.
