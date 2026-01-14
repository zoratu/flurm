%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra State Machine Unit Tests
%%%
%%% Unit tests for the flurm_db_ra module that test the state machine
%%% callbacks directly without requiring a running Ra cluster.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_unit_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    [
        {"init/1 creates empty state", fun() ->
            State = flurm_db_ra:init(#{}),
            ?assertMatch(#ra_state{}, State),
            ?assertEqual(#{}, State#ra_state.jobs),
            ?assertEqual(#{}, State#ra_state.nodes),
            ?assertEqual(#{}, State#ra_state.partitions),
            ?assertEqual(1, State#ra_state.job_counter),
            ?assertEqual(1, State#ra_state.version)
        end},
        {"init/1 with config still creates empty state", fun() ->
            State = flurm_db_ra:init(#{some_config => value}),
            ?assertMatch(#ra_state{}, State)
        end}
    ].

%%====================================================================
%% Apply Command Tests - Job Operations
%%====================================================================

apply_submit_job_test_() ->
    [
        {"submit_job creates job and increments counter", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"test_job">>,
                user = <<"testuser">>,
                group = <<"testgroup">>,
                partition = <<"default">>,
                script = <<"#!/bin/bash\necho test">>,
                num_nodes = 1,
                num_cpus = 4,
                memory_mb = 1024,
                time_limit = 3600,
                priority = 100
            },
            Meta = #{index => 1, term => 1},
            {State1, Result, Effects} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),
            ?assertMatch({ok, 1}, Result),
            ?assertEqual(2, State1#ra_state.job_counter),
            ?assertEqual(1, maps:size(State1#ra_state.jobs)),
            {ok, Job} = maps:find(1, State1#ra_state.jobs),
            ?assertEqual(<<"test_job">>, Job#ra_job.name),
            ?assertEqual(pending, Job#ra_job.state),
            ?assert(length(Effects) > 0)
        end},
        {"submit_job uses default priority when undefined", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"test">>,
                user = <<"u">>,
                group = <<"g">>,
                partition = <<"p">>,
                script = <<"s">>,
                num_nodes = 1,
                num_cpus = 1,
                memory_mb = 256,
                time_limit = 60,
                priority = undefined
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),
            {ok, Job} = maps:find(JobId, State1#ra_state.jobs),
            ?assertEqual(100, Job#ra_job.priority)
        end}
    ].

apply_cancel_job_test_() ->
    [
        {"cancel_job sets state to cancelled", fun() ->
            %% Setup: create a job first
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"cancel_test">>,
                user = <<"u">>,
                group = <<"g">>,
                partition = <<"p">>,
                script = <<"s">>,
                num_nodes = 1,
                num_cpus = 1,
                memory_mb = 256,
                time_limit = 60,
                priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            %% Cancel the job
            {State2, Result, _Effects} = flurm_db_ra:apply(Meta, {cancel_job, JobId}, State1),
            ?assertEqual(ok, Result),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(cancelled, Job#ra_job.state),
            ?assertNotEqual(undefined, Job#ra_job.end_time)
        end},
        {"cancel_job returns not_found for nonexistent job", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {State1, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, 9999}, State0),
            ?assertEqual({error, not_found}, Result),
            ?assertEqual(State0, State1),
            ?assertEqual([], Effects)
        end},
        {"cancel_job returns already_terminal for completed job", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            %% Set job to completed
            {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, completed}, State1),

            %% Try to cancel
            {State3, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, JobId}, State2),
            ?assertEqual({error, already_terminal}, Result),
            ?assertEqual(State2, State3),
            ?assertEqual([], Effects)
        end}
    ].

apply_update_job_state_test_() ->
    [
        {"update_job_state changes state", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, configuring}, State1),
            ?assertEqual(ok, Result),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(configuring, Job#ra_job.state)
        end},
        {"update_job_state to running sets start_time", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State1),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(running, Job#ra_job.state),
            ?assertNotEqual(undefined, Job#ra_job.start_time)
        end},
        {"update_job_state to completed sets end_time", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, completed}, State1),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(completed, Job#ra_job.state),
            ?assertNotEqual(undefined, Job#ra_job.end_time)
        end},
        {"update_job_state returns not_found for nonexistent job", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {State1, Result, Effects} = flurm_db_ra:apply(Meta, {update_job_state, 9999, running}, State0),
            ?assertEqual({error, not_found}, Result),
            ?assertEqual(State0, State1),
            ?assertEqual([], Effects)
        end}
    ].

apply_allocate_job_test_() ->
    [
        {"allocate_job sets nodes and state to configuring", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 2, num_cpus = 4, memory_mb = 1024,
                time_limit = 3600, priority = 100
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            Nodes = [<<"node1">>, <<"node2">>],
            {State2, Result, _} = flurm_db_ra:apply(Meta, {allocate_job, JobId, Nodes}, State1),
            ?assertEqual(ok, Result),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(configuring, Job#ra_job.state),
            ?assertEqual(Nodes, Job#ra_job.allocated_nodes),
            ?assertNotEqual(undefined, Job#ra_job.start_time)
        end},
        {"allocate_job fails for non-pending job", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),
            %% Change state to running
            {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State1),

            %% Try to allocate
            {State3, Result, Effects} = flurm_db_ra:apply(Meta, {allocate_job, JobId, [<<"n1">>]}, State2),
            ?assertMatch({error, {invalid_state, running}}, Result),
            ?assertEqual(State2, State3),
            ?assertEqual([], Effects)
        end},
        {"allocate_job returns not_found for nonexistent job", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {_, Result, _} = flurm_db_ra:apply(Meta, {allocate_job, 9999, [<<"n1">>]}, State0),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

apply_set_job_exit_code_test_() ->
    [
        {"set_job_exit_code with 0 sets completed", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, JobId, 0}, State1),
            ?assertEqual(ok, Result),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(completed, Job#ra_job.state),
            ?assertEqual(0, Job#ra_job.exit_code),
            ?assertNotEqual(undefined, Job#ra_job.end_time)
        end},
        {"set_job_exit_code with non-zero sets failed", fun() ->
            State0 = flurm_db_ra:init(#{}),
            JobSpec = #ra_job_spec{
                name = <<"t">>, user = <<"u">>, group = <<"g">>,
                partition = <<"p">>, script = <<"s">>,
                num_nodes = 1, num_cpus = 1, memory_mb = 256,
                time_limit = 60, priority = 50
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, JobId, 1}, State1),
            ?assertEqual(ok, Result),
            {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
            ?assertEqual(failed, Job#ra_job.state),
            ?assertEqual(1, Job#ra_job.exit_code)
        end},
        {"set_job_exit_code returns not_found", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {_, Result, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, 9999, 0}, State0),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

apply_allocate_job_id_test_() ->
    [
        {"allocate_job_id increments counter", fun() ->
            State0 = flurm_db_ra:init(#{}),
            ?assertEqual(1, State0#ra_state.job_counter),

            Meta = #{index => 1, term => 1},
            {State1, {ok, Id1}, []} = flurm_db_ra:apply(Meta, allocate_job_id, State0),
            ?assertEqual(1, Id1),
            ?assertEqual(2, State1#ra_state.job_counter),

            {State2, {ok, Id2}, []} = flurm_db_ra:apply(Meta, allocate_job_id, State1),
            ?assertEqual(2, Id2),
            ?assertEqual(3, State2#ra_state.job_counter)
        end}
    ].

%%====================================================================
%% Apply Command Tests - Node Operations
%%====================================================================

apply_register_node_test_() ->
    [
        {"register_node creates new node", fun() ->
            State0 = flurm_db_ra:init(#{}),
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
            Meta = #{index => 1, term => 1},
            {State1, Result, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),
            ?assertEqual({ok, registered}, Result),
            ?assertEqual(1, maps:size(State1#ra_state.nodes)),
            {ok, Node} = maps:find(<<"node1">>, State1#ra_state.nodes),
            ?assertEqual(<<"node1">>, Node#ra_node.name),
            ?assertEqual(32, Node#ra_node.cpus),
            ?assertEqual(0, Node#ra_node.cpus_used),
            ?assertEqual(up, Node#ra_node.state)
        end},
        {"register_node updates existing node", fun() ->
            State0 = flurm_db_ra:init(#{}),
            NodeSpec1 = #ra_node_spec{
                name = <<"node1">>,
                hostname = <<"node1.example.com">>,
                port = 7000,
                cpus = 32,
                memory_mb = 65536,
                gpus = 4,
                features = [gpu],
                partitions = [<<"default">>]
            },
            Meta = #{index => 1, term => 1},
            {State1, {ok, registered}, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec1}, State0),

            %% Re-register with updated specs
            NodeSpec2 = NodeSpec1#ra_node_spec{cpus = 64, gpus = 8},
            {State2, Result, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec2}, State1),
            ?assertEqual({ok, updated}, Result),
            {ok, Node} = maps:find(<<"node1">>, State2#ra_state.nodes),
            ?assertEqual(64, Node#ra_node.cpus),
            ?assertEqual(8, Node#ra_node.gpus)
        end}
    ].

apply_update_node_state_test_() ->
    [
        {"update_node_state changes state", fun() ->
            State0 = flurm_db_ra:init(#{}),
            NodeSpec = #ra_node_spec{
                name = <<"node1">>,
                hostname = <<"node1.example.com">>,
                port = 7000,
                cpus = 16,
                memory_mb = 32768,
                gpus = 0,
                features = [],
                partitions = [<<"default">>]
            },
            Meta = #{index => 1, term => 1},
            {State1, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {update_node_state, <<"node1">>, drain}, State1),
            ?assertEqual(ok, Result),
            {ok, Node} = maps:find(<<"node1">>, State2#ra_state.nodes),
            ?assertEqual(drain, Node#ra_node.state)
        end},
        {"update_node_state returns not_found", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {_, Result, _} = flurm_db_ra:apply(Meta, {update_node_state, <<"nonexistent">>, up}, State0),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

apply_unregister_node_test_() ->
    [
        {"unregister_node removes node", fun() ->
            State0 = flurm_db_ra:init(#{}),
            NodeSpec = #ra_node_spec{
                name = <<"temp_node">>,
                hostname = <<"temp.example.com">>,
                port = 7000,
                cpus = 8,
                memory_mb = 16384,
                gpus = 0,
                features = [],
                partitions = [<<"default">>]
            },
            Meta = #{index => 1, term => 1},
            {State1, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),
            ?assertEqual(1, maps:size(State1#ra_state.nodes)),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {unregister_node, <<"temp_node">>}, State1),
            ?assertEqual(ok, Result),
            ?assertEqual(0, maps:size(State2#ra_state.nodes))
        end},
        {"unregister_node returns not_found", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {_, Result, _} = flurm_db_ra:apply(Meta, {unregister_node, <<"nonexistent">>}, State0),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

%%====================================================================
%% Apply Command Tests - Partition Operations
%%====================================================================

apply_create_partition_test_() ->
    [
        {"create_partition creates new partition", fun() ->
            State0 = flurm_db_ra:init(#{}),
            PartSpec = #ra_partition_spec{
                name = <<"batch">>,
                nodes = [<<"n1">>, <<"n2">>],
                max_time = 86400,
                default_time = 3600,
                max_nodes = 100,
                priority = 10
            },
            Meta = #{index => 1, term => 1},
            {State1, Result, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),
            ?assertEqual(ok, Result),
            ?assertEqual(1, maps:size(State1#ra_state.partitions)),
            {ok, Part} = maps:find(<<"batch">>, State1#ra_state.partitions),
            ?assertEqual(<<"batch">>, Part#ra_partition.name),
            ?assertEqual(up, Part#ra_partition.state),
            ?assertEqual(86400, Part#ra_partition.max_time)
        end},
        {"create_partition returns already_exists", fun() ->
            State0 = flurm_db_ra:init(#{}),
            PartSpec = #ra_partition_spec{
                name = <<"batch">>,
                nodes = [],
                max_time = 3600,
                default_time = 60,
                max_nodes = 10,
                priority = 1
            },
            Meta = #{index => 1, term => 1},
            {State1, ok, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),
            {State2, Result, Effects} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State1),
            ?assertEqual({error, already_exists}, Result),
            ?assertEqual(State1, State2),
            ?assertEqual([], Effects)
        end}
    ].

apply_delete_partition_test_() ->
    [
        {"delete_partition removes partition", fun() ->
            State0 = flurm_db_ra:init(#{}),
            PartSpec = #ra_partition_spec{
                name = <<"temp">>,
                nodes = [],
                max_time = 3600,
                default_time = 60,
                max_nodes = 10,
                priority = 1
            },
            Meta = #{index => 1, term => 1},
            {State1, ok, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),

            {State2, Result, _} = flurm_db_ra:apply(Meta, {delete_partition, <<"temp">>}, State1),
            ?assertEqual(ok, Result),
            ?assertEqual(0, maps:size(State2#ra_state.partitions))
        end},
        {"delete_partition returns not_found", fun() ->
            State0 = flurm_db_ra:init(#{}),
            Meta = #{index => 1, term => 1},
            {_, Result, _} = flurm_db_ra:apply(Meta, {delete_partition, <<"nonexistent">>}, State0),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

%%====================================================================
%% Apply Unknown Command Test
%%====================================================================

apply_unknown_command_test() ->
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},
    {State1, Result, Effects} = flurm_db_ra:apply(Meta, {unknown_command, arg1, arg2}, State0),
    ?assertEqual({error, {unknown_command, {unknown_command, arg1, arg2}}}, Result),
    ?assertEqual(State0, State1),
    ?assertEqual([], Effects).

%%====================================================================
%% State Enter Tests
%%====================================================================

state_enter_test_() ->
    [
        {"state_enter leader generates effect", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(leader, State),
            ?assert(length(Effects) > 0)
        end},
        {"state_enter follower generates effect", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(follower, State),
            ?assert(length(Effects) > 0)
        end},
        {"state_enter recover generates no effects", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(recover, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter eol generates no effects", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(eol, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter other generates no effects", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(candidate, State),
            ?assertEqual([], Effects)
        end}
    ].

%%====================================================================
%% Snapshot Module Test
%%====================================================================

snapshot_module_test() ->
    Module = flurm_db_ra:snapshot_module(),
    ?assertEqual(ra_machine_simple, Module).

%%====================================================================
%% Make Record Tests
%%====================================================================

make_job_record_test_() ->
    [
        {"make_job_record creates correct job", fun() ->
            JobSpec = #ra_job_spec{
                name = <<"test">>,
                user = <<"user">>,
                group = <<"group">>,
                partition = <<"default">>,
                script = <<"#!/bin/bash">>,
                num_nodes = 2,
                num_cpus = 8,
                memory_mb = 2048,
                time_limit = 7200,
                priority = 150
            },
            Job = flurm_db_ra:make_job_record(42, JobSpec),
            ?assertEqual(42, Job#ra_job.id),
            ?assertEqual(<<"test">>, Job#ra_job.name),
            ?assertEqual(<<"user">>, Job#ra_job.user),
            ?assertEqual(pending, Job#ra_job.state),
            ?assertEqual(2, Job#ra_job.num_nodes),
            ?assertEqual(8, Job#ra_job.num_cpus),
            ?assertEqual(150, Job#ra_job.priority),
            ?assertEqual([], Job#ra_job.allocated_nodes),
            ?assertEqual(undefined, Job#ra_job.exit_code)
        end}
    ].

make_node_record_test_() ->
    [
        {"make_node_record creates correct node", fun() ->
            NodeSpec = #ra_node_spec{
                name = <<"compute1">>,
                hostname = <<"compute1.local">>,
                port = 7001,
                cpus = 48,
                memory_mb = 128000,
                gpus = 8,
                features = [gpu, nvme],
                partitions = [<<"gpu">>, <<"batch">>]
            },
            Node = flurm_db_ra:make_node_record(NodeSpec),
            ?assertEqual(<<"compute1">>, Node#ra_node.name),
            ?assertEqual(48, Node#ra_node.cpus),
            ?assertEqual(0, Node#ra_node.cpus_used),
            ?assertEqual(128000, Node#ra_node.memory_mb),
            ?assertEqual(0, Node#ra_node.memory_used),
            ?assertEqual(8, Node#ra_node.gpus),
            ?assertEqual(up, Node#ra_node.state),
            ?assertEqual([], Node#ra_node.running_jobs)
        end}
    ].

make_partition_record_test_() ->
    [
        {"make_partition_record creates correct partition", fun() ->
            PartSpec = #ra_partition_spec{
                name = <<"interactive">>,
                nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
                max_time = 14400,
                default_time = 1800,
                max_nodes = 50,
                priority = 100
            },
            Part = flurm_db_ra:make_partition_record(PartSpec),
            ?assertEqual(<<"interactive">>, Part#ra_partition.name),
            ?assertEqual(up, Part#ra_partition.state),
            ?assertEqual(3, length(Part#ra_partition.nodes)),
            ?assertEqual(14400, Part#ra_partition.max_time),
            ?assertEqual(100, Part#ra_partition.priority)
        end}
    ].
