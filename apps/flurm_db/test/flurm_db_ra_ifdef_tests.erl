%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra State Machine -ifdef(TEST) Exports Tests
%%%
%%% Tests for internal helper functions exported via -ifdef(TEST).
%%% These tests directly call the internal functions to achieve coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% update_job_state_record/2 Tests
%%====================================================================

update_job_state_record_test_() ->
    [
        {"update to running sets start_time when undefined", fun() ->
            Job = create_test_job(1, pending, undefined, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, running),
            ?assertEqual(running, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.start_time),
            ?assertEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to running keeps existing start_time", fun() ->
            Job = create_test_job(2, configuring, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, running),
            ?assertEqual(running, UpdatedJob#ra_job.state),
            %% start_time not set because it was already set (configuring state)
            ?assertEqual(1000, UpdatedJob#ra_job.start_time)
        end},

        {"update to completed sets end_time", fun() ->
            Job = create_test_job(3, running, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, completed),
            ?assertEqual(completed, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to failed sets end_time", fun() ->
            Job = create_test_job(4, running, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, failed),
            ?assertEqual(failed, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to cancelled sets end_time", fun() ->
            Job = create_test_job(5, pending, undefined, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, cancelled),
            ?assertEqual(cancelled, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to timeout sets end_time", fun() ->
            Job = create_test_job(6, running, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, timeout),
            ?assertEqual(timeout, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to node_fail sets end_time", fun() ->
            Job = create_test_job(7, running, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, node_fail),
            ?assertEqual(node_fail, UpdatedJob#ra_job.state),
            ?assertNotEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to configuring only changes state", fun() ->
            Job = create_test_job(8, pending, undefined, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, configuring),
            ?assertEqual(configuring, UpdatedJob#ra_job.state),
            ?assertEqual(undefined, UpdatedJob#ra_job.start_time),
            ?assertEqual(undefined, UpdatedJob#ra_job.end_time)
        end},

        {"update to completing only changes state", fun() ->
            Job = create_test_job(9, running, 1000, undefined),
            UpdatedJob = flurm_db_ra:update_job_state_record(Job, completing),
            ?assertEqual(completing, UpdatedJob#ra_job.state),
            ?assertEqual(1000, UpdatedJob#ra_job.start_time),
            ?assertEqual(undefined, UpdatedJob#ra_job.end_time)
        end}
    ].

%%====================================================================
%% Existing exports called via apply tests (for additional coverage)
%%====================================================================

make_job_record_with_default_priority_test() ->
    JobSpec = #ra_job_spec{
        name = <<"default_priority_job">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 2,
        memory_mb = 512,
        time_limit = 120,
        priority = undefined
    },
    Job = flurm_db_ra:make_job_record(100, JobSpec),
    ?assertEqual(100, Job#ra_job.id),
    ?assertEqual(pending, Job#ra_job.state),
    %% Should use default priority (100 from flurm_core.hrl)
    ?assertEqual(100, Job#ra_job.priority),
    ?assertEqual([], Job#ra_job.allocated_nodes).

make_job_record_with_explicit_priority_test() ->
    JobSpec = #ra_job_spec{
        name = <<"high_priority_job">>,
        user = <<"admin">>,
        group = <<"admin">>,
        partition = <<"high">>,
        script = <<"#!/bin/bash\nexit 0">>,
        num_nodes = 4,
        num_cpus = 16,
        memory_mb = 8192,
        time_limit = 7200,
        priority = 999
    },
    Job = flurm_db_ra:make_job_record(200, JobSpec),
    ?assertEqual(200, Job#ra_job.id),
    ?assertEqual(999, Job#ra_job.priority).

make_node_record_initializes_state_test() ->
    NodeSpec = #ra_node_spec{
        name = <<"test_node">>,
        hostname = <<"test.cluster.local">>,
        port = 7100,
        cpus = 64,
        memory_mb = 256000,
        gpus = 8,
        features = [gpu, nvme, highmem],
        partitions = [<<"gpu">>, <<"batch">>]
    },
    Node = flurm_db_ra:make_node_record(NodeSpec),
    ?assertEqual(<<"test_node">>, Node#ra_node.name),
    ?assertEqual(up, Node#ra_node.state),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(0, Node#ra_node.memory_used),
    ?assertEqual(0, Node#ra_node.gpus_used),
    ?assertEqual([], Node#ra_node.running_jobs),
    ?assertNotEqual(undefined, Node#ra_node.last_heartbeat).

make_partition_record_sets_up_state_test() ->
    PartSpec = #ra_partition_spec{
        name = <<"test_partition">>,
        nodes = [<<"n1">>, <<"n2">>, <<"n3">>, <<"n4">>],
        max_time = 172800,
        default_time = 3600,
        max_nodes = 200,
        priority = 50
    },
    Part = flurm_db_ra:make_partition_record(PartSpec),
    ?assertEqual(<<"test_partition">>, Part#ra_partition.name),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertEqual(4, length(Part#ra_partition.nodes)),
    ?assertEqual(172800, Part#ra_partition.max_time),
    ?assertEqual(50, Part#ra_partition.priority).

%%====================================================================
%% Helper Functions
%%====================================================================

create_test_job(Id, State, StartTime, EndTime) ->
    #ra_job{
        id = Id,
        name = <<"test_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        state = State,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = StartTime,
        end_time = EndTime,
        allocated_nodes = [],
        exit_code = undefined
    }.
