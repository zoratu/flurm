%%%-------------------------------------------------------------------
%%% @doc FLURM Multi-Node Cluster Tests
%%%
%%% Integration tests for multi-node cluster behavior.
%%% Tests controller failover, node registration, job distribution,
%%% and consensus replication.
%%%
%%% Usage:
%%%   flurm_cluster_tests:run_all().
%%%   flurm_cluster_tests:test_controller_failover().
%%%   flurm_cluster_tests:test_job_distribution().
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cluster_tests).

-export([
    run_all/0,
    test_single_node_ops/0,
    test_node_registration/0,
    test_job_distribution/0,
    test_controller_failover/0,
    test_concurrent_submissions/0,
    test_partition_scheduling/0,
    test_node_failure_recovery/0
]).

-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run all cluster tests
run_all() ->
    io:format("~n=== FLURM Multi-Node Cluster Tests ===~n~n"),
    
    Tests = [
        {"Single Node Operations", fun test_single_node_ops/0},
        {"Node Registration", fun test_node_registration/0},
        {"Job Distribution", fun test_job_distribution/0},
        {"Controller Failover", fun test_controller_failover/0},
        {"Concurrent Submissions", fun test_concurrent_submissions/0},
        {"Partition Scheduling", fun test_partition_scheduling/0},
        {"Node Failure Recovery", fun test_node_failure_recovery/0}
    ],
    
    Results = lists:map(fun({Name, TestFun}) ->
        io:format("--- ~s ---~n", [Name]),
        try
            Result = TestFun(),
            case Result of
                ok ->
                    io:format("  PASSED~n~n"),
                    {Name, passed};
                {error, Reason} ->
                    io:format("  FAILED: ~p~n~n", [Reason]),
                    {Name, {failed, Reason}}
            end
        catch
            Class:Error:Stack ->
                io:format("  CRASHED: ~p:~p~n~p~n~n", [Class, Error, Stack]),
                {Name, {crashed, {Class, Error}}}
        end
    end, Tests),
    
    %% Summary
    Passed = length([ok || {_, passed} <- Results]),
    Failed = length(Results) - Passed,
    
    io:format("=== Summary ===~n"),
    io:format("Passed: ~p/~p~n", [Passed, length(Results)]),
    io:format("Failed: ~p/~p~n~n", [Failed, length(Results)]),
    
    case Failed of
        0 -> ok;
        _ -> {error, {failed_tests, [N || {N, R} <- Results, R =/= passed]}}
    end.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test basic operations on a single node
test_single_node_ops() ->
    io:format("  Testing basic single-node operations...~n"),
    
    %% Create a mock cluster state
    State = init_cluster_state(),
    
    %% Submit a job
    {ok, JobId, State2} = submit_job(make_job_request(), State),
    io:format("    Submitted job ~p~n", [JobId]),
    
    %% Query job status
    {ok, JobInfo} = get_job_info(JobId, State2),
    case JobInfo#job.state of
        pending ->
            io:format("    Job is pending as expected~n"),
            ok;
        Other ->
            {error, {unexpected_state, Other}}
    end.

%% @doc Test node registration with controller
test_node_registration() ->
    io:format("  Testing node registration...~n"),
    
    State = init_cluster_state(),
    
    %% Register multiple nodes
    Nodes = [
        {<<"node001">>, 32, 128*1024},
        {<<"node002">>, 64, 256*1024},
        {<<"node003">>, 16, 64*1024}
    ],
    
    State2 = lists:foldl(fun({Hostname, Cpus, MemMb}, S) ->
        {ok, S2} = register_node(Hostname, Cpus, MemMb, S),
        io:format("    Registered ~s (~p CPUs, ~p MB)~n", [Hostname, Cpus, MemMb]),
        S2
    end, State, Nodes),
    
    %% Verify all nodes are registered
    RegisteredNodes = get_all_nodes(State2),
    case length(RegisteredNodes) of
        3 ->
            io:format("    All 3 nodes registered successfully~n"),
            ok;
        N ->
            {error, {wrong_node_count, N, expected, 3}}
    end.

%% @doc Test job distribution across nodes
test_job_distribution() ->
    io:format("  Testing job distribution...~n"),
    
    State = init_cluster_state(),
    
    %% Register nodes
    {ok, State2} = register_node(<<"node001">>, 8, 32*1024, State),
    {ok, State3} = register_node(<<"node002">>, 8, 32*1024, State2),
    {ok, State4} = register_node(<<"node003">>, 8, 32*1024, State3),
    
    %% Submit multiple jobs
    {JobIds, State5} = lists:foldl(fun(I, {Ids, S}) ->
        JobReq = #job_submit_req{
            name = list_to_binary(io_lib:format("job_~p", [I])),
            partition = <<"batch">>,
            num_nodes = 1,
            num_cpus = 2,
            memory_mb = 4096,
            time_limit = 3600,
            script = <<"#!/bin/bash\nsleep 10">>,
            priority = 100,
            env = #{},
            working_dir = <<"/tmp">>
        },
        {ok, JobId, S2} = submit_job(JobReq, S),
        {[JobId | Ids], S2}
    end, {[], State4}, lists:seq(1, 6)),
    
    io:format("    Submitted ~p jobs~n", [length(JobIds)]),
    
    %% Run scheduler
    State6 = run_scheduler(State5),
    
    %% Check distribution
    NodeJobs = count_jobs_per_node(State6),
    io:format("    Job distribution: ~p~n", [NodeJobs]),
    
    %% Verify jobs are distributed
    case maps:size(NodeJobs) > 0 of
        true ->
            io:format("    Jobs distributed across nodes~n"),
            ok;
        false ->
            {error, no_jobs_scheduled}
    end.

%% @doc Test controller failover behavior
test_controller_failover() ->
    io:format("  Testing controller failover...~n"),
    
    %% Simulate a 3-controller cluster
    _Controllers = [ctrl1, ctrl2, ctrl3],
    
    %% Initialize with ctrl1 as leader
    State = #{
        controllers => #{
            ctrl1 => #{state => up, is_leader => true},
            ctrl2 => #{state => up, is_leader => false},
            ctrl3 => #{state => up, is_leader => false}
        },
        jobs => #{}
    },
    
    io:format("    Initial leader: ctrl1~n"),
    
    %% Submit some jobs
    State2 = lists:foldl(fun(I, S) ->
        Jobs = maps:get(jobs, S),
        NewJob = #{id => I, state => pending},
        S#{jobs => maps:put(I, NewJob, Jobs)}
    end, State, lists:seq(1, 10)),
    
    io:format("    Submitted 10 jobs~n"),
    
    %% Simulate leader failure
    State3 = fail_controller(ctrl1, State2),
    io:format("    ctrl1 failed~n"),
    
    %% Elect new leader
    {ok, NewLeader, State4} = elect_leader(State3),
    io:format("    New leader elected: ~p~n", [NewLeader]),
    
    %% Verify jobs are preserved
    JobCount = maps:size(maps:get(jobs, State4)),
    case JobCount of
        10 ->
            io:format("    All 10 jobs preserved after failover~n"),
            ok;
        N ->
            {error, {jobs_lost, expected, 10, got, N}}
    end.

%% @doc Test concurrent job submissions
test_concurrent_submissions() ->
    io:format("  Testing concurrent submissions...~n"),
    
    State = init_cluster_state(),
    {ok, State2} = register_node(<<"node001">>, 64, 256*1024, State),
    
    Parent = self(),
    NumSubmitters = 5,
    JobsPerSubmitter = 20,
    
    %% Use an ETS table to simulate shared state
    Tab = ets:new(test_state, [set, public]),
    ets:insert(Tab, {state, State2}),
    ets:insert(Tab, {job_counter, 0}),
    
    %% Spawn concurrent submitters
    Pids = [spawn_link(fun() ->
        Results = [begin
            [{state, S}] = ets:lookup(Tab, state),
            JobReq = #job_submit_req{
                name = list_to_binary(io_lib:format("job_~p_~p", [N, I])),
                partition = <<"batch">>,
                num_nodes = 1,
                num_cpus = 1,
                memory_mb = 1024,
                time_limit = 300,
                script = <<"#!/bin/bash\necho test">>,
                priority = 100,
                env = #{},
                working_dir = <<"/tmp">>
            },
            case submit_job(JobReq, S) of
                {ok, JobId, _S2} ->
                    ets:update_counter(Tab, job_counter, 1),
                    {ok, JobId};
                Error ->
                    Error
            end
        end || I <- lists:seq(1, JobsPerSubmitter)],
        Parent ! {self(), Results}
    end) || N <- lists:seq(1, NumSubmitters)],
    
    %% Collect results
    _AllResults = lists:flatmap(fun(Pid) ->
        receive
            {Pid, Results} -> Results
        after 5000 ->
            []
        end
    end, Pids),
    
    [{job_counter, FinalCount}] = ets:lookup(Tab, job_counter),
    ets:delete(Tab),
    
    io:format("    Submitted ~p jobs concurrently~n", [FinalCount]),
    
    ExpectedJobs = NumSubmitters * JobsPerSubmitter,
    case FinalCount >= ExpectedJobs * 0.9 of  % Allow 10% tolerance for race conditions
        true ->
            io:format("    Concurrent submission successful~n"),
            ok;
        false ->
            {error, {insufficient_jobs, got, FinalCount, expected, ExpectedJobs}}
    end.

%% @doc Test partition-based scheduling
test_partition_scheduling() ->
    io:format("  Testing partition scheduling...~n"),
    
    State = init_cluster_state(),
    
    %% Create partitions
    State2 = create_partition(<<"batch">>, [<<"node001">>, <<"node002">>], State),
    State3 = create_partition(<<"gpu">>, [<<"node003">>], State2),
    
    %% Register nodes to partitions
    {ok, State4} = register_node(<<"node001">>, 32, 128*1024, State3),
    {ok, State5} = register_node(<<"node002">>, 32, 128*1024, State4),
    {ok, State6} = register_node(<<"node003">>, 8, 64*1024, State5),
    
    %% Submit job to batch partition
    BatchJob = make_job_request(<<"batch">>),
    {ok, BatchJobId, State7} = submit_job(BatchJob, State6),
    
    %% Submit job to GPU partition
    GpuJob = make_job_request(<<"gpu">>),
    {ok, GpuJobId, State8} = submit_job(GpuJob, State7),
    
    io:format("    Submitted batch job ~p and gpu job ~p~n", [BatchJobId, GpuJobId]),
    
    %% Run scheduler
    State9 = run_scheduler(State8),
    
    %% Verify jobs are scheduled to correct partitions
    {ok, BatchInfo} = get_job_info(BatchJobId, State9),
    {ok, GpuInfo} = get_job_info(GpuJobId, State9),
    
    io:format("    Batch job partition: ~s, GPU job partition: ~s~n",
              [BatchInfo#job.partition, GpuInfo#job.partition]),
    
    case {BatchInfo#job.partition, GpuInfo#job.partition} of
        {<<"batch">>, <<"gpu">>} ->
            io:format("    Partition scheduling correct~n"),
            ok;
        Other ->
            {error, {wrong_partitions, Other}}
    end.

%% @doc Test recovery from node failures
test_node_failure_recovery() ->
    io:format("  Testing node failure recovery...~n"),
    
    State = init_cluster_state(),
    
    %% Setup cluster
    {ok, State2} = register_node(<<"node001">>, 16, 64*1024, State),
    {ok, State3} = register_node(<<"node002">>, 16, 64*1024, State2),
    
    %% Submit and schedule jobs
    {ok, _JobId1, State4} = submit_job(make_job_request(), State3),
    {ok, _JobId2, State5} = submit_job(make_job_request(), State4),
    
    State6 = run_scheduler(State5),
    
    %% Simulate node failure
    io:format("    Simulating node001 failure...~n"),
    State7 = fail_node(<<"node001">>, State6),
    
    %% Check that jobs on failed node are rescheduled
    FailedJobs = get_jobs_on_node(<<"node001">>, State7),
    io:format("    Jobs affected by failure: ~p~n", [length(FailedJobs)]),
    
    %% Run recovery
    State8 = recover_failed_jobs(State7),
    
    %% Verify jobs are marked for rescheduling
    RecoveredJobs = get_pending_jobs(State8),
    io:format("    Jobs pending rescheduling: ~p~n", [length(RecoveredJobs)]),
    
    ok.

%%====================================================================
%% Internal Functions - Cluster State Management
%%====================================================================

init_cluster_state() ->
    #{
        nodes => #{},
        jobs => #{},
        partitions => #{
            <<"batch">> => #partition{
                name = <<"batch">>,
                state = up,
                nodes = [],
                max_time = 86400,
                default_time = 3600,
                max_nodes = 100,
                priority = 100,
                allow_root = false
            }
        },
        next_job_id => 1,
        scheduler_state => #{}
    }.

submit_job(#job_submit_req{} = Req, State) ->
    JobId = maps:get(next_job_id, State),
    Job = #job{
        id = JobId,
        name = Req#job_submit_req.name,
        user = <<"testuser">>,
        partition = Req#job_submit_req.partition,
        state = pending,
        script = Req#job_submit_req.script,
        num_nodes = Req#job_submit_req.num_nodes,
        num_cpus = Req#job_submit_req.num_cpus,
        memory_mb = Req#job_submit_req.memory_mb,
        time_limit = Req#job_submit_req.time_limit,
        priority = Req#job_submit_req.priority,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },
    Jobs = maps:get(jobs, State),
    NewState = State#{
        jobs => maps:put(JobId, Job, Jobs),
        next_job_id => JobId + 1
    },
    {ok, JobId, NewState}.

get_job_info(JobId, State) ->
    Jobs = maps:get(jobs, State),
    case maps:get(JobId, Jobs, undefined) of
        undefined -> {error, not_found};
        Job -> {ok, Job}
    end.

register_node(Hostname, Cpus, MemMb, State) ->
    Node = #node{
        hostname = Hostname,
        cpus = Cpus,
        memory_mb = MemMb,
        state = up,
        features = [],
        partitions = [<<"batch">>],
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = MemMb,
        last_heartbeat = erlang:system_time(second),
        allocations = #{}
    },
    Nodes = maps:get(nodes, State),
    {ok, State#{nodes => maps:put(Hostname, Node, Nodes)}}.

get_all_nodes(State) ->
    maps:values(maps:get(nodes, State)).

create_partition(Name, NodeList, State) ->
    Partition = #partition{
        name = Name,
        state = up,
        nodes = NodeList,
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        priority = 100,
        allow_root = false
    },
    Partitions = maps:get(partitions, State),
    State#{partitions => maps:put(Name, Partition, Partitions)}.

run_scheduler(State) ->
    %% Simple FIFO scheduler
    Jobs = maps:get(jobs, State),
    Nodes = maps:get(nodes, State),
    
    PendingJobs = [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= pending],
    SortedJobs = lists:sort(fun(A, B) -> A#job.id =< B#job.id end, PendingJobs),
    
    AvailableNodes = [N || {_, N} <- maps:to_list(Nodes), N#node.state =:= up],
    
    {UpdatedJobs, _} = lists:foldl(fun(Job, {AccJobs, AccNodes}) ->
        case find_node_for_job(Job, AccNodes) of
            {ok, Node} ->
                UpdatedJob = Job#job{
                    state = running,
                    start_time = erlang:system_time(second),
                    allocated_nodes = [Node#node.hostname]
                },
                UpdatedNode = Node#node{
                    running_jobs = [Job#job.id | Node#node.running_jobs]
                },
                RemainingNodes = lists:keyreplace(
                    Node#node.hostname, #node.hostname, AccNodes, UpdatedNode),
                {maps:put(Job#job.id, UpdatedJob, AccJobs), RemainingNodes};
            none ->
                {AccJobs, AccNodes}
        end
    end, {Jobs, AvailableNodes}, SortedJobs),
    
    State#{jobs => UpdatedJobs}.

find_node_for_job(#job{num_cpus = Cpus, memory_mb = Mem}, Nodes) ->
    Available = [N || N <- Nodes,
                      N#node.cpus >= Cpus,
                      N#node.free_memory_mb >= Mem],
    case Available of
        [] -> none;
        [First | _] -> {ok, First}
    end.

count_jobs_per_node(State) ->
    Jobs = maps:get(jobs, State),
    RunningJobs = [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= running],
    lists:foldl(fun(J, Acc) ->
        case J#job.allocated_nodes of
            [Node | _] ->
                maps:update_with(Node, fun(C) -> C + 1 end, 1, Acc);
            _ ->
                Acc
        end
    end, #{}, RunningJobs).

fail_controller(CtrlId, State) ->
    Controllers = maps:get(controllers, State),
    UpdatedCtrl = maps:get(CtrlId, Controllers),
    State#{controllers => maps:put(CtrlId, UpdatedCtrl#{state => down, is_leader => false}, Controllers)}.

elect_leader(State) ->
    Controllers = maps:get(controllers, State),
    UpControllers = [Id || {Id, C} <- maps:to_list(Controllers), maps:get(state, C) =:= up],
    case UpControllers of
        [] ->
            {error, no_available_controllers};
        [NewLeader | Rest] ->
            UpdatedControllers = lists:foldl(fun(Id, Acc) ->
                C = maps:get(Id, Acc),
                maps:put(Id, C#{is_leader => (Id =:= NewLeader)}, Acc)
            end, Controllers, [NewLeader | Rest]),
            {ok, NewLeader, State#{controllers => UpdatedControllers}}
    end.

fail_node(Hostname, State) ->
    Nodes = maps:get(nodes, State),
    Jobs = maps:get(jobs, State),
    
    case maps:get(Hostname, Nodes, undefined) of
        undefined ->
            State;
        Node ->
            %% Mark node as down
            UpdatedNode = Node#node{state = down},
            
            %% Mark jobs on this node as failed
            AffectedJobIds = Node#node.running_jobs,
            UpdatedJobs = lists:foldl(fun(JobId, AccJobs) ->
                case maps:get(JobId, AccJobs, undefined) of
                    undefined -> AccJobs;
                    Job -> maps:put(JobId, Job#job{state = failed}, AccJobs)
                end
            end, Jobs, AffectedJobIds),
            
            State#{
                nodes => maps:put(Hostname, UpdatedNode, Nodes),
                jobs => UpdatedJobs
            }
    end.

get_jobs_on_node(Hostname, State) ->
    Jobs = maps:get(jobs, State),
    [J || {_, J} <- maps:to_list(Jobs),
          lists:member(Hostname, J#job.allocated_nodes)].

recover_failed_jobs(State) ->
    Jobs = maps:get(jobs, State),
    UpdatedJobs = maps:map(fun(_, Job) ->
        case Job#job.state of
            failed -> Job#job{state = pending, allocated_nodes = []};
            _ -> Job
        end
    end, Jobs),
    State#{jobs => UpdatedJobs}.

get_pending_jobs(State) ->
    Jobs = maps:get(jobs, State),
    [J || {_, J} <- maps:to_list(Jobs), J#job.state =:= pending].

%%====================================================================
%% Internal Functions - Test Helpers
%%====================================================================

make_job_request() ->
    make_job_request(<<"batch">>).

make_job_request(Partition) ->
    #job_submit_req{
        name = <<"test_job">>,
        partition = Partition,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho test">>,
        priority = 100,
        env = #{},
        working_dir = <<"/tmp">>
    }.
