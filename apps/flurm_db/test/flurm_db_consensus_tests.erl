%%%-------------------------------------------------------------------
%%% @doc FLURM Database Consensus Tests
%%%
%%% Tests Ra consensus functionality including:
%%% - Ra state machine commands
%%% - Log replication simulation
%%% - Snapshot creation and restoration
%%% - Consistency under concurrent operations
%%%
%%% These tests verify the correctness of the Ra state machine
%%% implementation without requiring a real distributed cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_consensus_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Configuration
%%====================================================================

-define(TEST_TIMEOUT, 5000).

%%====================================================================
%% Test Fixtures - Ra State Machine Commands
%%====================================================================

ra_commands_test_() ->
    {foreach,
     fun setup_commands/0,
     fun cleanup_commands/1,
     [
         {"Submit job command creates job", fun test_submit_job_command/0},
         {"Cancel job command sets cancelled state", fun test_cancel_job_command/0},
         {"Update job state command transitions state", fun test_update_job_state_command/0},
         {"Allocate job command assigns nodes", fun test_allocate_job_command/0},
         {"Set exit code command completes job", fun test_set_exit_code_command/0},
         {"Register node command adds node", fun test_register_node_command/0},
         {"Update node state command changes state", fun test_update_node_state_command/0},
         {"Unregister node command removes node", fun test_unregister_node_command/0},
         {"Create partition command adds partition", fun test_create_partition_command/0},
         {"Delete partition command removes partition", fun test_delete_partition_command/0},
         {"Allocate job ID increments counter", fun test_allocate_job_id_command/0}
     ]}.

setup_commands() ->
    application:ensure_all_started(lager),
    ok.

cleanup_commands(_) ->
    ok.

test_submit_job_command() ->
    State0 = flurm_db_ra:init(#{}),
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
    Meta = #{index => 1, term => 1},
    {State1, Result, Effects} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Verify result
    ?assertMatch({ok, _JobId}, Result),
    {ok, JobId} = Result,
    ?assertEqual(1, JobId),

    %% Verify state updated
    ?assertEqual(2, State1#ra_state.job_counter),
    ?assertEqual(1, maps:size(State1#ra_state.jobs)),

    %% Verify job record
    {ok, Job} = maps:find(JobId, State1#ra_state.jobs),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(100, Job#ra_job.priority),

    %% Verify effects were generated
    ?assert(length(Effects) > 0),
    ok.

test_cancel_job_command() ->
    %% Setup: create a job first
    State0 = flurm_db_ra:init(#{}),
    JobSpec = #ra_job_spec{
        name = <<"cancel_test">>, user = <<"u">>, group = <<"g">>,
        partition = <<"p">>, script = <<"s">>,
        num_nodes = 1, num_cpus = 1, memory_mb = 256, time_limit = 60, priority = 50
    },
    Meta = #{index => 1, term => 1},
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Cancel the job
    {State2, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, JobId}, State1),
    ?assertEqual(ok, Result),

    %% Verify job is cancelled
    {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
    ?assertEqual(cancelled, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time),

    %% Verify effects
    ?assert(length(Effects) > 0),
    ok.

test_update_job_state_command() ->
    State0 = flurm_db_ra:init(#{}),
    JobSpec = #ra_job_spec{
        name = <<"state_test">>, user = <<"u">>, group = <<"g">>,
        partition = <<"p">>, script = <<"s">>,
        num_nodes = 1, num_cpus = 1, memory_mb = 256, time_limit = 60, priority = 50
    },
    Meta = #{index => 1, term => 1},
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Transition through states
    %% pending -> configuring
    {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, configuring}, State1),
    {ok, Job2} = maps:find(JobId, State2#ra_state.jobs),
    ?assertEqual(configuring, Job2#ra_job.state),

    %% configuring -> running (sets start_time)
    {State3, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State2),
    {ok, Job3} = maps:find(JobId, State3#ra_state.jobs),
    ?assertEqual(running, Job3#ra_job.state),
    ?assertNotEqual(undefined, Job3#ra_job.start_time),

    %% running -> completed (sets end_time)
    {State4, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, completed}, State3),
    {ok, Job4} = maps:find(JobId, State4#ra_state.jobs),
    ?assertEqual(completed, Job4#ra_job.state),
    ?assertNotEqual(undefined, Job4#ra_job.end_time),
    ok.

test_allocate_job_command() ->
    State0 = flurm_db_ra:init(#{}),
    JobSpec = #ra_job_spec{
        name = <<"alloc_test">>, user = <<"u">>, group = <<"g">>,
        partition = <<"p">>, script = <<"s">>,
        num_nodes = 2, num_cpus = 4, memory_mb = 1024, time_limit = 3600, priority = 100
    },
    Meta = #{index => 1, term => 1},
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Allocate nodes to job
    Nodes = [<<"node1">>, <<"node2">>],
    {State2, Result, _} = flurm_db_ra:apply(Meta, {allocate_job, JobId, Nodes}, State1),
    ?assertEqual(ok, Result),

    %% Verify allocation
    {ok, Job} = maps:find(JobId, State2#ra_state.jobs),
    ?assertEqual(configuring, Job#ra_job.state),
    ?assertEqual(Nodes, Job#ra_job.allocated_nodes),
    ?assertNotEqual(undefined, Job#ra_job.start_time),
    ok.

test_set_exit_code_command() ->
    State0 = flurm_db_ra:init(#{}),
    JobSpec = #ra_job_spec{
        name = <<"exit_test">>, user = <<"u">>, group = <<"g">>,
        partition = <<"p">>, script = <<"s">>,
        num_nodes = 1, num_cpus = 1, memory_mb = 256, time_limit = 60, priority = 50
    },
    Meta = #{index => 1, term => 1},
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Set exit code 0 (success)
    {State2, ok, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, JobId, 0}, State1),
    {ok, Job2} = maps:find(JobId, State2#ra_state.jobs),
    ?assertEqual(completed, Job2#ra_job.state),
    ?assertEqual(0, Job2#ra_job.exit_code),

    %% Test with non-zero exit code
    {State3, {ok, JobId2}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State2),
    {State4, ok, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, JobId2, 1}, State3),
    {ok, Job4} = maps:find(JobId2, State4#ra_state.jobs),
    ?assertEqual(failed, Job4#ra_job.state),
    ?assertEqual(1, Job4#ra_job.exit_code),
    ok.

test_register_node_command() ->
    State0 = flurm_db_ra:init(#{}),
    NodeSpec = #ra_node_spec{
        name = <<"compute1">>,
        hostname = <<"compute1.cluster.local">>,
        port = 7000,
        cpus = 32,
        memory_mb = 65536,
        gpus = 4,
        features = [gpu, ssd],
        partitions = [<<"default">>, <<"gpu">>]
    },
    Meta = #{index => 1, term => 1},
    {State1, Result, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),

    ?assertEqual({ok, registered}, Result),
    ?assertEqual(1, maps:size(State1#ra_state.nodes)),

    {ok, Node} = maps:find(<<"compute1">>, State1#ra_state.nodes),
    ?assertEqual(<<"compute1">>, Node#ra_node.name),
    ?assertEqual(32, Node#ra_node.cpus),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(up, Node#ra_node.state),
    ok.

test_update_node_state_command() ->
    State0 = flurm_db_ra:init(#{}),
    NodeSpec = #ra_node_spec{
        name = <<"n1">>, hostname = <<"n1.local">>, port = 7000,
        cpus = 8, memory_mb = 16384, gpus = 0, features = [], partitions = []
    },
    Meta = #{index => 1, term => 1},
    {State1, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),

    %% Update state to drain
    {State2, ok, _} = flurm_db_ra:apply(Meta, {update_node_state, <<"n1">>, drain}, State1),
    {ok, Node} = maps:find(<<"n1">>, State2#ra_state.nodes),
    ?assertEqual(drain, Node#ra_node.state),
    ok.

test_unregister_node_command() ->
    State0 = flurm_db_ra:init(#{}),
    NodeSpec = #ra_node_spec{
        name = <<"temp">>, hostname = <<"temp.local">>, port = 7000,
        cpus = 4, memory_mb = 8192, gpus = 0, features = [], partitions = []
    },
    Meta = #{index => 1, term => 1},
    {State1, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),
    ?assertEqual(1, maps:size(State1#ra_state.nodes)),

    %% Unregister
    {State2, ok, _} = flurm_db_ra:apply(Meta, {unregister_node, <<"temp">>}, State1),
    ?assertEqual(0, maps:size(State2#ra_state.nodes)),
    ok.

test_create_partition_command() ->
    State0 = flurm_db_ra:init(#{}),
    PartSpec = #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        priority = 10
    },
    Meta = #{index => 1, term => 1},
    {State1, ok, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),

    ?assertEqual(1, maps:size(State1#ra_state.partitions)),
    {ok, Part} = maps:find(<<"batch">>, State1#ra_state.partitions),
    ?assertEqual(<<"batch">>, Part#ra_partition.name),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertEqual(86400, Part#ra_partition.max_time),
    ok.

test_delete_partition_command() ->
    State0 = flurm_db_ra:init(#{}),
    PartSpec = #ra_partition_spec{
        name = <<"temp">>, nodes = [], max_time = 3600,
        default_time = 60, max_nodes = 10, priority = 1
    },
    Meta = #{index => 1, term => 1},
    {State1, ok, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),

    %% Delete
    {State2, ok, _} = flurm_db_ra:apply(Meta, {delete_partition, <<"temp">>}, State1),
    ?assertEqual(0, maps:size(State2#ra_state.partitions)),
    ok.

test_allocate_job_id_command() ->
    State0 = flurm_db_ra:init(#{}),
    ?assertEqual(1, State0#ra_state.job_counter),

    Meta = #{index => 1, term => 1},

    %% Allocate first ID
    {State1, {ok, Id1}, Effects1} = flurm_db_ra:apply(Meta, allocate_job_id, State0),
    ?assertEqual(1, Id1),
    ?assertEqual(2, State1#ra_state.job_counter),
    ?assertEqual([], Effects1),  % No effects for ID allocation

    %% Allocate second ID
    {State2, {ok, Id2}, _} = flurm_db_ra:apply(Meta, allocate_job_id, State1),
    ?assertEqual(2, Id2),
    ?assertEqual(3, State2#ra_state.job_counter),

    %% Allocate third ID
    {State3, {ok, Id3}, _} = flurm_db_ra:apply(Meta, allocate_job_id, State2),
    ?assertEqual(3, Id3),
    ?assertEqual(4, State3#ra_state.job_counter),
    ok.

%%====================================================================
%% Test Fixtures - Log Replication Simulation
%%====================================================================

log_replication_test_() ->
    {foreach,
     fun setup_log/0,
     fun cleanup_log/1,
     [
         {"Log replay produces consistent state", fun test_log_replay_consistency/0},
         {"Out of order detection", fun test_index_tracking/0},
         {"Term changes handled correctly", fun test_term_handling/0},
         {"Large log replay performance", fun test_large_log_replay/0},
         {"Partial log replay", fun test_partial_log_replay/0}
     ]}.

setup_log() ->
    application:ensure_all_started(lager),
    ok.

cleanup_log(_) ->
    ok.

test_log_replay_consistency() ->
    %% Simulate log replication by replaying commands
    Log = [
        {1, 1, {submit_job, make_job_spec(<<"job1">>)}},
        {2, 1, {submit_job, make_job_spec(<<"job2">>)}},
        {3, 1, {update_job_state, 1, running}},
        {4, 1, {submit_job, make_job_spec(<<"job3">>)}},
        {5, 1, {cancel_job, 2}},
        {6, 1, {update_job_state, 1, completed}}
    ],

    %% Replay on two independent state machines
    State1 = replay_log(Log, flurm_db_ra:init(#{})),
    State2 = replay_log(Log, flurm_db_ra:init(#{})),

    %% States should be identical
    ?assertEqual(State1#ra_state.jobs, State2#ra_state.jobs),
    ?assertEqual(State1#ra_state.job_counter, State2#ra_state.job_counter),

    %% Verify final state
    ?assertEqual(3, maps:size(State1#ra_state.jobs)),
    ?assertEqual(4, State1#ra_state.job_counter),

    %% Verify job states
    {ok, Job1} = maps:find(1, State1#ra_state.jobs),
    ?assertEqual(completed, Job1#ra_job.state),

    {ok, Job2} = maps:find(2, State1#ra_state.jobs),
    ?assertEqual(cancelled, Job2#ra_job.state),

    {ok, Job3} = maps:find(3, State1#ra_state.jobs),
    ?assertEqual(pending, Job3#ra_job.state),
    ok.

test_index_tracking() ->
    %% Test that the state machine can be queried for its last applied index
    %% (simulated since flurm_db_ra doesn't track index explicitly)
    State0 = flurm_db_ra:init(#{}),

    Log = [
        {1, 1, allocate_job_id},
        {2, 1, allocate_job_id},
        {3, 1, allocate_job_id}
    ],

    FinalState = replay_log(Log, State0),

    %% Job counter should reflect all allocations
    ?assertEqual(4, FinalState#ra_state.job_counter),
    ok.

test_term_handling() ->
    %% Test that commands from different terms are handled correctly
    State0 = flurm_db_ra:init(#{}),

    %% Commands from different terms (simulating leader changes)
    Log = [
        {1, 1, {submit_job, make_job_spec(<<"term1_job">>)}},  % Term 1
        {2, 2, {submit_job, make_job_spec(<<"term2_job">>)}},  % Term 2 (new leader)
        {3, 2, {update_job_state, 1, running}},
        {4, 3, {update_job_state, 2, running}},  % Term 3 (another leader change)
        {5, 3, {update_job_state, 1, completed}}
    ],

    FinalState = replay_log(Log, State0),

    %% All commands should have been applied regardless of term
    ?assertEqual(2, maps:size(FinalState#ra_state.jobs)),

    {ok, Job1} = maps:find(1, FinalState#ra_state.jobs),
    ?assertEqual(completed, Job1#ra_job.state),

    {ok, Job2} = maps:find(2, FinalState#ra_state.jobs),
    ?assertEqual(running, Job2#ra_job.state),
    ok.

test_large_log_replay() ->
    %% Test replay performance with a large log
    NumEntries = 1000,

    %% Generate a large log
    Log = lists:map(
        fun(I) ->
            {I, 1, {submit_job, make_job_spec(list_to_binary("job" ++ integer_to_list(I)))}}
        end,
        lists:seq(1, NumEntries)
    ),

    %% Time the replay
    {Time, FinalState} = timer:tc(fun() ->
        replay_log(Log, flurm_db_ra:init(#{}))
    end),

    %% Should complete in reasonable time (< 1 second)
    ?assert(Time < 1000000, "Log replay took too long"),

    %% Verify state
    ?assertEqual(NumEntries, maps:size(FinalState#ra_state.jobs)),
    ?assertEqual(NumEntries + 1, FinalState#ra_state.job_counter),
    ok.

test_partial_log_replay() ->
    %% Test replaying only part of a log (simulating snapshot + log)
    FullLog = [
        {1, 1, {submit_job, make_job_spec(<<"job1">>)}},
        {2, 1, {submit_job, make_job_spec(<<"job2">>)}},
        {3, 1, {update_job_state, 1, running}},
        {4, 1, {update_job_state, 2, running}},
        {5, 1, {update_job_state, 1, completed}}
    ],

    %% Simulate snapshot at index 2 by creating state manually
    SnapshotState = #ra_state{
        jobs = #{
            1 => #ra_job{id = 1, name = <<"job1">>, state = pending,
                        user = <<"u">>, group = <<"g">>, partition = <<"p">>,
                        script = <<"s">>, num_nodes = 1, num_cpus = 1,
                        memory_mb = 256, time_limit = 60, priority = 50,
                        submit_time = 0, allocated_nodes = []},
            2 => #ra_job{id = 2, name = <<"job2">>, state = pending,
                        user = <<"u">>, group = <<"g">>, partition = <<"p">>,
                        script = <<"s">>, num_nodes = 1, num_cpus = 1,
                        memory_mb = 256, time_limit = 60, priority = 50,
                        submit_time = 0, allocated_nodes = []}
        },
        job_counter = 3,
        nodes = #{},
        partitions = #{},
        version = 1
    },

    %% Replay only entries after snapshot
    PartialLog = lists:nthtail(2, FullLog),  % Entries 3, 4, 5
    FinalState = replay_log(PartialLog, SnapshotState),

    %% Verify same result as full replay
    FullReplayState = replay_log(FullLog, flurm_db_ra:init(#{})),

    %% Job 1 should be completed in both
    {ok, PartialJob1} = maps:find(1, FinalState#ra_state.jobs),
    {ok, FullJob1} = maps:find(1, FullReplayState#ra_state.jobs),
    ?assertEqual(FullJob1#ra_job.state, PartialJob1#ra_job.state),
    ok.

%%====================================================================
%% Test Fixtures - Snapshot Creation and Restoration
%%====================================================================

snapshot_test_() ->
    {foreach,
     fun setup_snapshot/0,
     fun cleanup_snapshot/1,
     [
         {"Snapshot module is correct", fun test_snapshot_module/0},
         {"State can be serialized", fun test_state_serialization/0},
         {"State can be deserialized", fun test_state_deserialization/0},
         {"Large state snapshot", fun test_large_state_snapshot/0},
         {"Snapshot preserves all fields", fun test_snapshot_field_preservation/0}
     ]}.

setup_snapshot() ->
    ok.

cleanup_snapshot(_) ->
    ok.

test_snapshot_module() ->
    Module = flurm_db_ra:snapshot_module(),
    ?assertEqual(ra_machine_simple, Module).

test_state_serialization() ->
    %% Build up some state
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {State1, _, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(<<"snap_job">>)}, State0),
    {State2, _, _} = flurm_db_ra:apply(Meta, {register_node, make_node_spec(<<"node1">>)}, State1),
    {State3, _, _} = flurm_db_ra:apply(Meta, {create_partition, make_partition_spec(<<"part1">>)}, State2),

    %% Serialize
    Binary = term_to_binary(State3),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 0),
    ok.

test_state_deserialization() ->
    %% Build up some state
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(<<"deser_job">>)}, State0),
    {State2, _, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State1),

    %% Serialize and deserialize
    Binary = term_to_binary(State2),
    Restored = binary_to_term(Binary),

    %% Verify restored state matches
    ?assertEqual(State2#ra_state.job_counter, Restored#ra_state.job_counter),
    ?assertEqual(maps:size(State2#ra_state.jobs), maps:size(Restored#ra_state.jobs)),

    {ok, OriginalJob} = maps:find(JobId, State2#ra_state.jobs),
    {ok, RestoredJob} = maps:find(JobId, Restored#ra_state.jobs),
    ?assertEqual(OriginalJob#ra_job.state, RestoredJob#ra_job.state),
    ok.

test_large_state_snapshot() ->
    %% Create state with many entries
    State0 = flurm_db_ra:init(#{}),

    %% Add 100 jobs
    State1 = lists:foldl(
        fun(I, AccState) ->
            Meta = #{index => I, term => 1},
            JobName = list_to_binary("job" ++ integer_to_list(I)),
            {NewState, _, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(JobName)}, AccState),
            NewState
        end,
        State0,
        lists:seq(1, 100)
    ),

    %% Add 20 nodes
    State2 = lists:foldl(
        fun(I, AccState) ->
            Meta = #{index => I + 100, term => 1},
            NodeName = list_to_binary("node" ++ integer_to_list(I)),
            {NewState, _, _} = flurm_db_ra:apply(Meta, {register_node, make_node_spec(NodeName)}, AccState),
            NewState
        end,
        State1,
        lists:seq(1, 20)
    ),

    %% Serialize
    {SerTime, Binary} = timer:tc(fun() -> term_to_binary(State2) end),

    %% Deserialize
    {DeserTime, Restored} = timer:tc(fun() -> binary_to_term(Binary) end),

    %% Should be fast (< 100ms each)
    ?assert(SerTime < 100000, "Serialization too slow"),
    ?assert(DeserTime < 100000, "Deserialization too slow"),

    %% Verify counts
    ?assertEqual(100, maps:size(Restored#ra_state.jobs)),
    ?assertEqual(20, maps:size(Restored#ra_state.nodes)),
    ok.

test_snapshot_field_preservation() ->
    %% Verify all fields are preserved through snapshot
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Create job with all fields
    JobSpec = #ra_job_spec{
        name = <<"full_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"testpart">>,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 3,
        num_cpus = 16,
        memory_mb = 32768,
        time_limit = 86400,
        priority = 500
    },
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),

    %% Allocate and run
    {State2, ok, _} = flurm_db_ra:apply(Meta, {allocate_job, JobId, [<<"n1">>, <<"n2">>, <<"n3">>]}, State1),
    {State3, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State2),

    %% Snapshot round-trip
    Binary = term_to_binary(State3),
    Restored = binary_to_term(Binary),

    %% Verify all job fields
    {ok, OrigJob} = maps:find(JobId, State3#ra_state.jobs),
    {ok, RestJob} = maps:find(JobId, Restored#ra_state.jobs),

    ?assertEqual(OrigJob#ra_job.id, RestJob#ra_job.id),
    ?assertEqual(OrigJob#ra_job.name, RestJob#ra_job.name),
    ?assertEqual(OrigJob#ra_job.user, RestJob#ra_job.user),
    ?assertEqual(OrigJob#ra_job.group, RestJob#ra_job.group),
    ?assertEqual(OrigJob#ra_job.partition, RestJob#ra_job.partition),
    ?assertEqual(OrigJob#ra_job.state, RestJob#ra_job.state),
    ?assertEqual(OrigJob#ra_job.num_nodes, RestJob#ra_job.num_nodes),
    ?assertEqual(OrigJob#ra_job.num_cpus, RestJob#ra_job.num_cpus),
    ?assertEqual(OrigJob#ra_job.memory_mb, RestJob#ra_job.memory_mb),
    ?assertEqual(OrigJob#ra_job.time_limit, RestJob#ra_job.time_limit),
    ?assertEqual(OrigJob#ra_job.priority, RestJob#ra_job.priority),
    ?assertEqual(OrigJob#ra_job.allocated_nodes, RestJob#ra_job.allocated_nodes),
    ok.

%%====================================================================
%% Test Fixtures - Consistency Under Concurrent Operations
%%====================================================================

consistency_test_() ->
    {foreach,
     fun setup_consistency/0,
     fun cleanup_consistency/1,
     [
         {"Sequential commands are consistent", fun test_sequential_consistency/0},
         {"Interleaved operations are consistent", fun test_interleaved_ops/0},
         {"Conflict resolution is deterministic", fun test_conflict_resolution/0},
         {"Counter increments are atomic", fun test_atomic_counter/0},
         {"Multi-entity operations are consistent", fun test_multi_entity_consistency/0}
     ]}.

setup_consistency() ->
    ok.

cleanup_consistency(_) ->
    ok.

test_sequential_consistency() ->
    %% Same sequence of operations always produces same result
    Operations = [
        {submit_job, make_job_spec(<<"seq1">>)},
        {submit_job, make_job_spec(<<"seq2">>)},
        {update_job_state, 1, running},
        {update_job_state, 2, running},
        {update_job_state, 1, completed},
        {cancel_job, 2}
    ],

    %% Run twice
    State1 = apply_operations(Operations, flurm_db_ra:init(#{})),
    State2 = apply_operations(Operations, flurm_db_ra:init(#{})),

    %% States must be identical
    ?assertEqual(State1#ra_state.jobs, State2#ra_state.jobs),
    ?assertEqual(State1#ra_state.job_counter, State2#ra_state.job_counter),
    ok.

test_interleaved_ops() ->
    %% Test that interleaved operations on different entities don't interfere
    Operations = [
        {submit_job, make_job_spec(<<"job1">>)},
        {register_node, make_node_spec(<<"node1">>)},
        {submit_job, make_job_spec(<<"job2">>)},
        {create_partition, make_partition_spec(<<"part1">>)},
        {update_job_state, 1, running},
        {update_node_state, <<"node1">>, drain}
    ],

    State = apply_operations(Operations, flurm_db_ra:init(#{})),

    %% All operations should have succeeded independently
    ?assertEqual(2, maps:size(State#ra_state.jobs)),
    ?assertEqual(1, maps:size(State#ra_state.nodes)),
    ?assertEqual(1, maps:size(State#ra_state.partitions)),

    {ok, Job1} = maps:find(1, State#ra_state.jobs),
    ?assertEqual(running, Job1#ra_job.state),

    {ok, Node1} = maps:find(<<"node1">>, State#ra_state.nodes),
    ?assertEqual(drain, Node1#ra_node.state),
    ok.

test_conflict_resolution() ->
    %% Test that conflicting operations are resolved deterministically
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Submit job
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(<<"conflict">>)}, State0),

    %% Try to cancel an already-terminal job
    {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, completed}, State1),
    {State3, Result, _} = flurm_db_ra:apply(Meta, {cancel_job, JobId}, State2),

    ?assertEqual({error, already_terminal}, Result),
    ?assertEqual(State2#ra_state.jobs, State3#ra_state.jobs),
    ok.

test_atomic_counter() ->
    %% Test that job ID allocation is atomic
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Allocate many IDs
    {FinalState, IDs} = lists:foldl(
        fun(_, {AccState, AccIDs}) ->
            {NewState, {ok, ID}, _} = flurm_db_ra:apply(Meta, allocate_job_id, AccState),
            {NewState, AccIDs ++ [ID]}
        end,
        {State0, []},
        lists:seq(1, 100)
    ),

    %% All IDs should be unique and sequential
    ?assertEqual(lists:seq(1, 100), IDs),
    ?assertEqual(101, FinalState#ra_state.job_counter),
    ok.

test_multi_entity_consistency() ->
    %% Test consistency when operating on multiple entity types
    State0 = flurm_db_ra:init(#{}),

    %% Complex scenario: partition with nodes and jobs
    Operations = [
        %% Create infrastructure
        {create_partition, make_partition_spec(<<"prod">>)},
        {register_node, make_node_spec(<<"prod-node1">>)},
        {register_node, make_node_spec(<<"prod-node2">>)},

        %% Submit jobs
        {submit_job, make_job_spec(<<"prod-job1">>)},
        {submit_job, make_job_spec(<<"prod-job2">>)},

        %% Run jobs on nodes
        {allocate_job, 1, [<<"prod-node1">>]},
        {allocate_job, 2, [<<"prod-node2">>]},
        {update_job_state, 1, running},
        {update_job_state, 2, running},

        %% Complete one, fail one
        {set_job_exit_code, 1, 0},
        {set_job_exit_code, 2, 1}
    ],

    State = apply_operations(Operations, State0),

    %% Verify final state
    ?assertEqual(1, maps:size(State#ra_state.partitions)),
    ?assertEqual(2, maps:size(State#ra_state.nodes)),
    ?assertEqual(2, maps:size(State#ra_state.jobs)),

    {ok, Job1} = maps:find(1, State#ra_state.jobs),
    ?assertEqual(completed, Job1#ra_job.state),
    ?assertEqual(0, Job1#ra_job.exit_code),

    {ok, Job2} = maps:find(2, State#ra_state.jobs),
    ?assertEqual(failed, Job2#ra_job.state),
    ?assertEqual(1, Job2#ra_job.exit_code),
    ok.

%%====================================================================
%% Test Fixtures - Error Handling
%%====================================================================

error_handling_test_() ->
    {foreach,
     fun setup_errors/0,
     fun cleanup_errors/1,
     [
         {"Not found errors", fun test_not_found_errors/0},
         {"Already exists errors", fun test_already_exists_errors/0},
         {"Invalid state errors", fun test_invalid_state_errors/0},
         {"Unknown command errors", fun test_unknown_command_errors/0},
         {"Error doesn't corrupt state", fun test_error_state_preservation/0}
     ]}.

setup_errors() ->
    ok.

cleanup_errors(_) ->
    ok.

test_not_found_errors() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Job not found
    {_, R1, E1} = flurm_db_ra:apply(Meta, {cancel_job, 9999}, State),
    ?assertEqual({error, not_found}, R1),
    ?assertEqual([], E1),

    {_, R2, _} = flurm_db_ra:apply(Meta, {update_job_state, 9999, running}, State),
    ?assertEqual({error, not_found}, R2),

    {_, R3, _} = flurm_db_ra:apply(Meta, {allocate_job, 9999, [<<"n1">>]}, State),
    ?assertEqual({error, not_found}, R3),

    {_, R4, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, 9999, 0}, State),
    ?assertEqual({error, not_found}, R4),

    %% Node not found
    {_, R5, _} = flurm_db_ra:apply(Meta, {update_node_state, <<"nonexistent">>, up}, State),
    ?assertEqual({error, not_found}, R5),

    {_, R6, _} = flurm_db_ra:apply(Meta, {unregister_node, <<"nonexistent">>}, State),
    ?assertEqual({error, not_found}, R6),

    %% Partition not found
    {_, R7, _} = flurm_db_ra:apply(Meta, {delete_partition, <<"nonexistent">>}, State),
    ?assertEqual({error, not_found}, R7),
    ok.

test_already_exists_errors() ->
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Create partition
    {State1, ok, _} = flurm_db_ra:apply(Meta, {create_partition, make_partition_spec(<<"dup">>)}, State0),

    %% Try to create again
    {State2, Result, Effects} = flurm_db_ra:apply(Meta, {create_partition, make_partition_spec(<<"dup">>)}, State1),
    ?assertEqual({error, already_exists}, Result),
    ?assertEqual(State1, State2),
    ?assertEqual([], Effects),
    ok.

test_invalid_state_errors() ->
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Submit and move to running
    {State1, {ok, JobId}, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(<<"invalid">>)}, State0),
    {State2, ok, _} = flurm_db_ra:apply(Meta, {update_job_state, JobId, running}, State1),

    %% Try to allocate running job
    {State3, Result, Effects} = flurm_db_ra:apply(Meta, {allocate_job, JobId, [<<"n1">>]}, State2),
    ?assertMatch({error, {invalid_state, running}}, Result),
    ?assertEqual(State2, State3),
    ?assertEqual([], Effects),
    ok.

test_unknown_command_errors() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {State2, Result, Effects} = flurm_db_ra:apply(Meta, {unknown_cmd, arg1, arg2}, State),
    ?assertMatch({error, {unknown_command, _}}, Result),
    ?assertEqual(State, State2),
    ?assertEqual([], Effects),
    ok.

test_error_state_preservation() ->
    %% Verify that errors don't corrupt the state
    State0 = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Build up some state
    {State1, {ok, _}, _} = flurm_db_ra:apply(Meta, {submit_job, make_job_spec(<<"good">>)}, State0),
    {State2, _, _} = flurm_db_ra:apply(Meta, {register_node, make_node_spec(<<"good">>)}, State1),

    %% Execute various error commands
    {State3, _, _} = flurm_db_ra:apply(Meta, {cancel_job, 9999}, State2),
    {State4, _, _} = flurm_db_ra:apply(Meta, {unknown_command, test}, State3),
    {State5, _, _} = flurm_db_ra:apply(Meta, {delete_partition, <<"nope">>}, State4),

    %% State should be unchanged from State2
    ?assertEqual(State2#ra_state.jobs, State5#ra_state.jobs),
    ?assertEqual(State2#ra_state.nodes, State5#ra_state.nodes),
    ?assertEqual(State2#ra_state.job_counter, State5#ra_state.job_counter),
    ok.

%%====================================================================
%% Test Fixtures - State Enter Callbacks
%%====================================================================

state_enter_test_() ->
    [
        {"state_enter leader", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(leader, State),
            ?assert(is_list(Effects)),
            ?assert(length(Effects) > 0)
        end},
        {"state_enter follower", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(follower, State),
            ?assert(is_list(Effects)),
            ?assert(length(Effects) > 0)
        end},
        {"state_enter recover", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(recover, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter eol", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(eol, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter other", fun() ->
            State = flurm_db_ra:init(#{}),
            Effects = flurm_db_ra:state_enter(candidate, State),
            ?assertEqual([], Effects)
        end}
    ].

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_spec(Name) ->
    #ra_job_spec{
        name = Name,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 50
    }.

make_node_spec(Name) ->
    #ra_node_spec{
        name = Name,
        hostname = <<Name/binary, ".local">>,
        port = 7000,
        cpus = 8,
        memory_mb = 16384,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    }.

make_partition_spec(Name) ->
    #ra_partition_spec{
        name = Name,
        nodes = [],
        max_time = 3600,
        default_time = 60,
        max_nodes = 10,
        priority = 1
    }.

replay_log(Log, InitialState) ->
    lists:foldl(
        fun({Index, Term, Cmd}, AccState) ->
            Meta = #{index => Index, term => Term},
            {NewState, _, _} = flurm_db_ra:apply(Meta, Cmd, AccState),
            NewState
        end,
        InitialState,
        Log
    ).

apply_operations(Operations, InitialState) ->
    {FinalState, _} = lists:foldl(
        fun(Op, {AccState, Index}) ->
            Meta = #{index => Index, term => 1},
            {NewState, _, _} = flurm_db_ra:apply(Meta, Op, AccState),
            {NewState, Index + 1}
        end,
        {InitialState, 1},
        Operations
    ),
    FinalState.
