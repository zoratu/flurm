%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Tests
%%%
%%% Unit and integration tests for the Ra state machine implementation.
%%% Tests cover:
%%% - Single-node cluster operations
%%% - Job submission and state transitions
%%% - Node registration and management
%%% - Partition creation and deletion
%%% - Query operations (local and consistent)
%%%
%%% Note: Multi-node cluster tests require a distributed environment
%%% and are marked with appropriate setup requirements.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

%% @doc Setup for single-node Ra cluster tests.
%% Creates a temporary data directory and starts the Ra application.
setup() ->
    %% Start Erlang distribution if not already running
    %% Ra requires the node to have distribution enabled (not nonode@nohost)
    case node() of
        nonode@nohost ->
            %% Generate a unique short node name for testing
            %% Use shortnames (no dots in hostname) for local testing
            NodeName = list_to_atom("flurm_test_" ++
                integer_to_list(erlang:system_time(millisecond))),
            case net_kernel:start([NodeName, shortnames]) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, Reason} ->
                    error_logger:warning_msg("Could not start distribution: ~p~n", [Reason])
            end;
        _ ->
            ok
    end,

    %% Create a unique temporary directory for this test run
    TmpDir = "/tmp/flurm_db_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    ok = filelib:ensure_dir(TmpDir ++ "/"),

    %% Configure Ra to use the temp directory
    application:set_env(ra, data_dir, TmpDir),
    application:set_env(flurm_db, data_dir, TmpDir),

    %% Start required applications
    {ok, _} = application:ensure_all_started(ra),

    %% Ensure the Ra default system is started
    %% This is required for Ra 2.x before starting clusters
    case ra_system:start_default() of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, RaReason} ->
            error_logger:error_msg("Failed to start Ra default system: ~p~n", [RaReason])
    end,

    %% Return cleanup context
    #{data_dir => TmpDir}.

%% @doc Cleanup after tests.
cleanup(#{data_dir := DataDir}) ->
    %% Stop Ra cluster if running
    catch flurm_db_cluster:leave_cluster(),

    %% Stop Ra application (no sleep needed - stop is synchronous)
    application:stop(ra),

    %% Remove temporary data directory
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

%% @doc Main test generator for Ra state machine tests.
ra_state_machine_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Context) ->
         {inorder, [
             {"Start single-node cluster", fun() -> test_start_single_node_cluster(Context) end},
             {"Submit job through Ra", fun() -> test_submit_job(Context) end},
             {"Cancel job", fun() -> test_cancel_job(Context) end},
             {"Update job state", fun() -> test_update_job_state(Context) end},
             {"Register node", fun() -> test_register_node(Context) end},
             {"Update node state", fun() -> test_update_node_state(Context) end},
             {"Unregister node", fun() -> test_unregister_node(Context) end},
             {"Create partition", fun() -> test_create_partition(Context) end},
             {"Delete partition", fun() -> test_delete_partition(Context) end},
             {"Query jobs", fun() -> test_query_jobs(Context) end},
             {"Query nodes", fun() -> test_query_nodes(Context) end}
         ]}
     end
    }.

%%====================================================================
%% Individual Tests
%%====================================================================

%% @doc Test starting a single-node Ra cluster.
test_start_single_node_cluster(_Context) ->
    %% Start the cluster with just this node
    Result = flurm_db_cluster:start_cluster([node()]),
    ?assertEqual(ok, Result),

    %% Verify we can get cluster status
    {ok, Status} = flurm_db_cluster:status(),
    ?assertMatch(#{state := _}, Status),

    %% Verify we're a member
    {ok, Members} = flurm_db_cluster:get_members(),
    ?assert(length(Members) >= 1),

    %% Verify leader election occurred (Ra leader election is synchronous with start_cluster)
    {ok, _Leader} = flurm_db_cluster:get_leader(),

    ok.

%% @doc Test submitting a job through Ra.
test_submit_job(_Context) ->
    JobSpec = #ra_job_spec{
        name = <<"test_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },

    %% Submit the job
    {ok, JobId} = flurm_db_ra:submit_job(JobSpec),
    ?assert(is_integer(JobId)),
    ?assert(JobId > 0),

    %% Verify the job was created
    {ok, Job} = flurm_db_ra:get_job(JobId),
    ?assertEqual(JobId, Job#ra_job.id),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(1, Job#ra_job.num_nodes),
    ?assertEqual(4, Job#ra_job.num_cpus),

    ok.

%% @doc Test cancelling a job.
test_cancel_job(_Context) ->
    %% Create a job to cancel
    JobSpec = #ra_job_spec{
        name = <<"cancel_test_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho cancel">>,
        num_nodes = 1,
        num_cpus = 2,
        memory_mb = 512,
        time_limit = 60,
        priority = 50
    },
    {ok, JobId} = flurm_db_ra:submit_job(JobSpec),

    %% Cancel the job
    Result = flurm_db_ra:cancel_job(JobId),
    ?assertEqual(ok, Result),

    %% Verify the job is cancelled
    {ok, Job} = flurm_db_ra:get_job(JobId),
    ?assertEqual(cancelled, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time),

    %% Try to cancel again - should fail
    Result2 = flurm_db_ra:cancel_job(JobId),
    ?assertEqual({error, already_terminal}, Result2),

    ok.

%% @doc Test updating job state.
test_update_job_state(_Context) ->
    %% Create a job
    JobSpec = #ra_job_spec{
        name = <<"state_test_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho state">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 30,
        priority = 100
    },
    {ok, JobId} = flurm_db_ra:submit_job(JobSpec),

    %% Update state to configuring
    ok = flurm_db_ra:update_job_state(JobId, configuring),
    {ok, Job1} = flurm_db_ra:get_job(JobId),
    ?assertEqual(configuring, Job1#ra_job.state),

    %% Update state to running
    ok = flurm_db_ra:update_job_state(JobId, running),
    {ok, Job2} = flurm_db_ra:get_job(JobId),
    ?assertEqual(running, Job2#ra_job.state),
    ?assertNotEqual(undefined, Job2#ra_job.start_time),

    %% Update state to completed
    ok = flurm_db_ra:update_job_state(JobId, completed),
    {ok, Job3} = flurm_db_ra:get_job(JobId),
    ?assertEqual(completed, Job3#ra_job.state),
    ?assertNotEqual(undefined, Job3#ra_job.end_time),

    ok.

%% @doc Test registering a node.
test_register_node(_Context) ->
    NodeSpec = #ra_node_spec{
        name = <<"compute-001">>,
        hostname = <<"compute-001.example.com">>,
        port = 7000,
        cpus = 32,
        memory_mb = 65536,
        gpus = 4,
        features = [gpu, ssd],
        partitions = [<<"default">>, <<"gpu">>]
    },

    %% Register the node
    {ok, registered} = flurm_db_ra:register_node(NodeSpec),

    %% Verify the node was registered
    {ok, Node} = flurm_db_ra:get_node(<<"compute-001">>),
    ?assertEqual(<<"compute-001">>, Node#ra_node.name),
    ?assertEqual(<<"compute-001.example.com">>, Node#ra_node.hostname),
    ?assertEqual(32, Node#ra_node.cpus),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(up, Node#ra_node.state),

    %% Re-register should return updated
    {ok, updated} = flurm_db_ra:register_node(NodeSpec),

    ok.

%% @doc Test updating node state.
test_update_node_state(_Context) ->
    %% First ensure node exists
    NodeSpec = #ra_node_spec{
        name = <<"compute-002">>,
        hostname = <<"compute-002.example.com">>,
        port = 7000,
        cpus = 16,
        memory_mb = 32768,
        gpus = 0,
        features = [ssd],
        partitions = [<<"default">>]
    },
    {ok, _} = flurm_db_ra:register_node(NodeSpec),

    %% Update to drain
    ok = flurm_db_ra:update_node_state(<<"compute-002">>, drain),
    {ok, Node1} = flurm_db_ra:get_node(<<"compute-002">>),
    ?assertEqual(drain, Node1#ra_node.state),

    %% Update to down
    ok = flurm_db_ra:update_node_state(<<"compute-002">>, down),
    {ok, Node2} = flurm_db_ra:get_node(<<"compute-002">>),
    ?assertEqual(down, Node2#ra_node.state),

    %% Update back to up
    ok = flurm_db_ra:update_node_state(<<"compute-002">>, up),
    {ok, Node3} = flurm_db_ra:get_node(<<"compute-002">>),
    ?assertEqual(up, Node3#ra_node.state),

    ok.

%% @doc Test unregistering a node.
test_unregister_node(_Context) ->
    %% Register a node to unregister
    NodeSpec = #ra_node_spec{
        name = <<"compute-temp">>,
        hostname = <<"compute-temp.example.com">>,
        port = 7000,
        cpus = 8,
        memory_mb = 16384,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, _} = flurm_db_ra:register_node(NodeSpec),

    %% Verify it exists
    {ok, _Node} = flurm_db_ra:get_node(<<"compute-temp">>),

    %% Unregister
    ok = flurm_db_ra:unregister_node(<<"compute-temp">>),

    %% Verify it's gone
    Result = flurm_db_ra:get_node(<<"compute-temp">>),
    ?assertEqual({error, not_found}, Result),

    %% Trying to unregister again should fail
    Result2 = flurm_db_ra:unregister_node(<<"compute-temp">>),
    ?assertEqual({error, not_found}, Result2),

    ok.

%% @doc Test creating a partition.
test_create_partition(_Context) ->
    PartSpec = #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"compute-001">>, <<"compute-002">>],
        max_time = 86400,       %% 24 hours
        default_time = 3600,    %% 1 hour
        max_nodes = 100,
        priority = 10
    },

    %% Create the partition
    ok = flurm_db_ra:create_partition(PartSpec),

    %% Verify it was created
    {ok, Partition} = flurm_db_ra:get_partition(<<"batch">>),
    ?assertEqual(<<"batch">>, Partition#ra_partition.name),
    ?assertEqual(up, Partition#ra_partition.state),
    ?assertEqual(86400, Partition#ra_partition.max_time),
    ?assertEqual(2, length(Partition#ra_partition.nodes)),

    %% Try to create again - should fail
    Result = flurm_db_ra:create_partition(PartSpec),
    ?assertEqual({error, already_exists}, Result),

    ok.

%% @doc Test deleting a partition.
test_delete_partition(_Context) ->
    %% Create a partition to delete
    PartSpec = #ra_partition_spec{
        name = <<"temp-partition">>,
        nodes = [],
        max_time = 3600,
        default_time = 60,
        max_nodes = 10,
        priority = 1
    },
    ok = flurm_db_ra:create_partition(PartSpec),

    %% Delete it
    ok = flurm_db_ra:delete_partition(<<"temp-partition">>),

    %% Verify it's gone
    Result = flurm_db_ra:get_partition(<<"temp-partition">>),
    ?assertEqual({error, not_found}, Result),

    %% Try to delete again - should fail
    Result2 = flurm_db_ra:delete_partition(<<"temp-partition">>),
    ?assertEqual({error, not_found}, Result2),

    ok.

%% @doc Test querying jobs.
test_query_jobs(_Context) ->
    %% Create some test jobs
    lists:foreach(fun(N) ->
        JobSpec = #ra_job_spec{
            name = list_to_binary("query_test_job_" ++ integer_to_list(N)),
            user = <<"queryuser">>,
            group = <<"querygroup">>,
            partition = <<"default">>,
            script = <<"#!/bin/bash\necho query">>,
            num_nodes = 1,
            num_cpus = 1,
            memory_mb = 128,
            time_limit = 60,
            priority = N * 10
        },
        {ok, _} = flurm_db_ra:submit_job(JobSpec)
    end, lists:seq(1, 5)),

    %% List all jobs
    {ok, AllJobs} = flurm_db_ra:list_jobs(),
    ?assert(length(AllJobs) >= 5),

    %% List pending jobs
    {ok, PendingJobs} = flurm_db_ra:get_jobs_by_state(pending),
    ?assert(length(PendingJobs) >= 5),

    %% All pending jobs should have state = pending
    lists:foreach(fun(Job) ->
        ?assertEqual(pending, Job#ra_job.state)
    end, PendingJobs),

    ok.

%% @doc Test querying nodes.
test_query_nodes(_Context) ->
    %% Create some test nodes
    lists:foreach(fun(N) ->
        NodeSpec = #ra_node_spec{
            name = list_to_binary("query-node-" ++ integer_to_list(N)),
            hostname = list_to_binary("query-node-" ++ integer_to_list(N) ++ ".example.com"),
            port = 7000 + N,
            cpus = N * 8,
            memory_mb = N * 8192,
            gpus = if N rem 2 == 0 -> 2; true -> 0 end,
            features = if N rem 2 == 0 -> [gpu]; true -> [] end,
            partitions = [<<"default">>]
        },
        {ok, _} = flurm_db_ra:register_node(NodeSpec)
    end, lists:seq(1, 4)),

    %% Set some nodes to different states
    ok = flurm_db_ra:update_node_state(<<"query-node-2">>, drain),
    ok = flurm_db_ra:update_node_state(<<"query-node-4">>, down),

    %% List all nodes
    {ok, AllNodes} = flurm_db_ra:list_nodes(),
    ?assert(length(AllNodes) >= 4),

    %% List up nodes
    {ok, UpNodes} = flurm_db_ra:get_nodes_by_state(up),
    %% Should have at least 2 up nodes (query-node-1 and query-node-3)
    UpNodeNames = [N#ra_node.name || N <- UpNodes],
    ?assert(lists:member(<<"query-node-1">>, UpNodeNames)),
    ?assert(lists:member(<<"query-node-3">>, UpNodeNames)),

    %% List drain nodes
    {ok, DrainNodes} = flurm_db_ra:get_nodes_by_state(drain),
    DrainNodeNames = [N#ra_node.name || N <- DrainNodes],
    ?assert(lists:member(<<"query-node-2">>, DrainNodeNames)),

    ok.

%%====================================================================
%% Ra Machine Unit Tests
%%====================================================================

%% @doc Test the Ra machine init/1 callback.
ra_machine_init_test() ->
    State = flurm_db_ra:init(#{}),
    ?assertMatch(#ra_state{}, State),
    ?assertEqual(#{}, State#ra_state.jobs),
    ?assertEqual(#{}, State#ra_state.nodes),
    ?assertEqual(#{}, State#ra_state.partitions),
    ?assertEqual(1, State#ra_state.job_counter).

%% @doc Test making a job record from a spec.
make_job_record_test() ->
    JobSpec = #ra_job_spec{
        name = <<"test">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"default">>,
        script = <<"script">>,
        num_nodes = 2,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 200
    },
    Job = flurm_db_ra:make_job_record(42, JobSpec),
    ?assertEqual(42, Job#ra_job.id),
    ?assertEqual(<<"test">>, Job#ra_job.name),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(200, Job#ra_job.priority),
    ?assertEqual([], Job#ra_job.allocated_nodes).

%% @doc Test making a job record with default priority.
make_job_record_default_priority_test() ->
    JobSpec = #ra_job_spec{
        name = <<"test">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"default">>,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = undefined  %% Should use default
    },
    Job = flurm_db_ra:make_job_record(1, JobSpec),
    ?assertEqual(100, Job#ra_job.priority).  %% Default priority

%% @doc Test making a node record from a spec.
make_node_record_test() ->
    NodeSpec = #ra_node_spec{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 7000,
        cpus = 32,
        memory_mb = 65536,
        gpus = 4,
        features = [gpu, ssd],
        partitions = [<<"default">>]
    },
    Node = flurm_db_ra:make_node_record(NodeSpec),
    ?assertEqual(<<"node1">>, Node#ra_node.name),
    ?assertEqual(32, Node#ra_node.cpus),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(up, Node#ra_node.state),
    ?assertEqual([], Node#ra_node.running_jobs).

%% @doc Test making a partition record from a spec.
make_partition_record_test() ->
    PartSpec = #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"n1">>, <<"n2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        priority = 10
    },
    Partition = flurm_db_ra:make_partition_record(PartSpec),
    ?assertEqual(<<"batch">>, Partition#ra_partition.name),
    ?assertEqual(up, Partition#ra_partition.state),
    ?assertEqual(2, length(Partition#ra_partition.nodes)).

%%====================================================================
%% Helper Functions
%%====================================================================

%% These would be used for multi-node testing in a distributed environment

%% @doc Wait for a condition to become true.
%% wait_until(Fun, Timeout) ->
%%     wait_until(Fun, Timeout, 100).
%%
%% wait_until(_Fun, Timeout, _Interval) when Timeout =< 0 ->
%%     {error, timeout};
%% wait_until(Fun, Timeout, Interval) ->
%%     case Fun() of
%%         true -> ok;
%%         false ->
%%             timer:sleep(Interval),
%%             wait_until(Fun, Timeout - Interval, Interval)
%%     end.
