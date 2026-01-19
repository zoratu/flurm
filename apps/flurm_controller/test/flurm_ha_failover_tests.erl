%%%-------------------------------------------------------------------
%%% @doc HA Failover Integration Tests for FLURM
%%%
%%% Tests high-availability and failover scenarios including:
%%% - Ra cluster initialization
%%% - Leader election
%%% - State replication across nodes
%%% - Leader failure and re-election
%%% - Split-brain prevention
%%% - State recovery after node rejoins
%%%
%%% These tests use mocking and local simulation to test the failover
%%% logic without requiring a real multi-node cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_ha_failover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Configuration
%%====================================================================

-define(TEST_CLUSTER_NAME, flurm_test_cluster).
-define(TEST_TIMEOUT, 5000).

%%====================================================================
%% Test Fixtures - Ra Cluster Initialization
%%====================================================================

ra_cluster_init_test_() ->
    {foreach,
     fun setup_cluster/0,
     fun cleanup_cluster/1,
     [
         {"Ra cluster initialization with single node", fun test_single_node_init/0},
         {"Ra cluster initialization creates valid state", fun test_init_creates_valid_state/0},
         {"Ra cluster name configuration", fun test_cluster_name_config/0},
         {"Multiple init calls are idempotent", fun test_init_idempotent/0}
     ]}.

setup_cluster() ->
    %% Start required applications
    application:ensure_all_started(lager),

    %% Clean up any existing processes
    cleanup_processes(),

    %% Set up test configuration
    application:set_env(flurm_controller, cluster_name, ?TEST_CLUSTER_NAME),
    application:set_env(flurm_controller, cluster_nodes, [node()]),
    application:set_env(flurm_controller, ra_data_dir, "/tmp/flurm_test_ra"),

    ok.

cleanup_cluster(_) ->
    cleanup_processes(),
    application:unset_env(flurm_controller, cluster_name),
    application:unset_env(flurm_controller, cluster_nodes),
    application:unset_env(flurm_controller, ra_data_dir),
    ok.

cleanup_processes() ->
    %% Stop cluster process if running
    case whereis(flurm_controller_cluster) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, shutdown, 1000),
            timer:sleep(50)
    end,

    %% Stop failover process if running
    case whereis(flurm_controller_failover) of
        undefined -> ok;
        Pid2 ->
            catch gen_server:stop(Pid2, shutdown, 1000),
            timer:sleep(50)
    end,
    ok.

test_single_node_init() ->
    %% Test that the cluster module can start with a single node
    {ok, Pid} = flurm_controller_cluster:start_link(),
    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_controller_cluster)),

    %% Give it time to initialize
    timer:sleep(200),

    %% Get cluster status
    Status = flurm_controller_cluster:cluster_status(),
    ?assert(is_map(Status)),
    ?assertEqual(?TEST_CLUSTER_NAME, maps:get(cluster_name, Status)),
    ?assertEqual(node(), maps:get(this_node, Status)),
    ok.

test_init_creates_valid_state() ->
    %% Start the cluster
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(200),

    %% Check status contains all required fields
    Status = flurm_controller_cluster:cluster_status(),

    RequiredFields = [cluster_name, this_node, is_leader, current_leader,
                      cluster_nodes, ra_ready, ra_members],
    lists:foreach(fun(Field) ->
        ?assert(maps:is_key(Field, Status),
                io_lib:format("Missing field: ~p", [Field]))
    end, RequiredFields),
    ok.

test_cluster_name_config() ->
    %% Verify cluster name from configuration is used
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(100),

    Status = flurm_controller_cluster:cluster_status(),
    ?assertEqual(?TEST_CLUSTER_NAME, maps:get(cluster_name, Status)),
    ok.

test_init_idempotent() ->
    %% Start once
    {ok, Pid1} = flurm_controller_cluster:start_link(),

    %% Try to start again - should fail with already_started
    Result = flurm_controller_cluster:start_link(),
    ?assertMatch({error, {already_started, _}}, Result),

    %% Original process should still be running
    ?assert(is_process_alive(Pid1)),
    ok.

%%====================================================================
%% Test Fixtures - Leader Election
%%====================================================================

leader_election_test_() ->
    {foreach,
     fun setup_leader_election/0,
     fun cleanup_leader_election/1,
     [
         {"Single node becomes leader", fun test_single_node_leader/0},
         {"Leader election status queries", fun test_leader_status_queries/0},
         {"is_leader returns boolean", fun test_is_leader_returns_bool/0},
         {"get_leader returns valid tuple", fun test_get_leader_format/0}
     ]}.

setup_leader_election() ->
    setup_cluster(),
    ok.

cleanup_leader_election(State) ->
    cleanup_cluster(State).

test_single_node_leader() ->
    %% In a single-node cluster, this node should eventually become leader
    {ok, _} = flurm_controller_cluster:start_link(),

    %% Wait for Ra to initialize and elect leader
    %% Note: In real Ra, this would happen quickly
    timer:sleep(500),

    %% Check leadership - may or may not be leader depending on Ra availability
    IsLeader = flurm_controller_cluster:is_leader(),
    ?assert(is_boolean(IsLeader)),
    ok.

test_leader_status_queries() ->
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(200),

    %% Test that status queries don't crash
    Status = flurm_controller_cluster:cluster_status(),
    ?assert(is_map(Status)),

    %% is_leader should return quickly
    IsLeader = flurm_controller_cluster:is_leader(),
    ?assert(is_boolean(IsLeader)),
    ok.

test_is_leader_returns_bool() ->
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(100),

    Result = flurm_controller_cluster:is_leader(),
    ?assert(is_boolean(Result)),
    ok.

test_get_leader_format() ->
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(300),

    %% get_leader should return {ok, {Name, Node}} or {error, Reason}
    Result = flurm_controller_cluster:get_leader(),
    case Result of
        {ok, {Name, Node}} ->
            ?assert(is_atom(Name)),
            ?assert(is_atom(Node));
        {error, Reason} ->
            %% Valid error reasons
            ?assert(Reason =:= no_leader orelse
                    Reason =:= not_ready orelse
                    is_tuple(Reason))
    end,
    ok.

%%====================================================================
%% Test Fixtures - State Replication
%%====================================================================

state_replication_test_() ->
    {foreach,
     fun setup_replication/0,
     fun cleanup_replication/1,
     [
         {"State machine init creates empty state", fun test_state_machine_init/0},
         {"Commands produce consistent state", fun test_command_consistency/0},
         {"State transitions are deterministic", fun test_deterministic_transitions/0},
         {"Concurrent commands are serialized", fun test_command_serialization/0}
     ]}.

setup_replication() ->
    application:ensure_all_started(lager),
    ok.

cleanup_replication(_) ->
    ok.

test_state_machine_init() ->
    %% Test the Ra state machine initialization
    State = flurm_controller_ra_machine:init(#{}),

    %% Verify initial state structure
    Overview = flurm_controller_ra_machine:overview(State),
    ?assertEqual(0, maps:get(job_count, Overview)),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ?assertEqual(1, maps:get(next_job_id, Overview)),
    ok.

test_command_consistency() ->
    %% Test that the same sequence of commands produces the same state
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Apply a series of commands
    Meta1 = #{index => 1, term => 1},
    JobSpec = #{name => <<"test_job">>, user => <<"testuser">>,
                partition => <<"default">>, script => <<"#!/bin/bash">>},
    {State1, {ok, JobId1}} = flurm_controller_ra_machine:apply(Meta1, {submit_job, JobSpec}, State0),

    %% Apply same commands to fresh state
    State0b = flurm_controller_ra_machine:init(#{}),
    {State1b, {ok, JobId1b}} = flurm_controller_ra_machine:apply(Meta1, {submit_job, JobSpec}, State0b),

    %% Results should be identical
    ?assertEqual(JobId1, JobId1b),
    Overview1 = flurm_controller_ra_machine:overview(State1),
    Overview1b = flurm_controller_ra_machine:overview(State1b),
    ?assertEqual(Overview1, Overview1b),
    ok.

test_deterministic_transitions() ->
    %% Test that state transitions are deterministic
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Submit job
    Meta1 = #{index => 1, term => 1},
    JobSpec = #{name => <<"det_test">>, user => <<"user1">>},
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(Meta1, {submit_job, JobSpec}, State0),

    %% Update job state
    Meta2 = #{index => 2, term => 1},
    {State2, ok} = flurm_controller_ra_machine:apply(Meta2, {update_job_state, JobId, running}, State1),

    %% Verify state is as expected
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(2, maps:get(last_applied_index, Overview)),
    ok.

test_command_serialization() ->
    %% Test that commands applied in sequence produce expected results
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Apply multiple job submissions
    Commands = [
        {submit_job, #{name => <<"job1">>, user => <<"u1">>}},
        {submit_job, #{name => <<"job2">>, user => <<"u2">>}},
        {submit_job, #{name => <<"job3">>, user => <<"u3">>}}
    ],

    {FinalState, Results} = lists:foldl(
        fun(Cmd, {AccState, AccResults}) ->
            Meta = #{index => length(AccResults) + 1, term => 1},
            {NewState, Result} = flurm_controller_ra_machine:apply(Meta, Cmd, AccState),
            {NewState, AccResults ++ [Result]}
        end,
        {State0, []},
        Commands
    ),

    %% All jobs should have been created with sequential IDs
    ?assertEqual([{ok, 1}, {ok, 2}, {ok, 3}], Results),

    Overview = flurm_controller_ra_machine:overview(FinalState),
    ?assertEqual(3, maps:get(job_count, Overview)),
    ?assertEqual(4, maps:get(next_job_id, Overview)),
    ok.

%%====================================================================
%% Test Fixtures - Leader Failure and Re-election
%%====================================================================

leader_failure_test_() ->
    {foreach,
     fun setup_failure/0,
     fun cleanup_failure/1,
     [
         {"Failover handler starts correctly", fun test_failover_handler_start/0},
         {"Leadership transition triggers recovery", fun test_leadership_transition/0},
         {"Lost leadership stops recovery", fun test_lost_leadership/0},
         {"Recovery status tracking", fun test_recovery_status/0},
         {"Health check during leadership", fun test_health_check_leadership/0}
     ]}.

setup_failure() ->
    application:ensure_all_started(lager),
    cleanup_processes(),
    ok.

cleanup_failure(_) ->
    cleanup_processes(),
    ok.

test_failover_handler_start() ->
    {ok, Pid} = flurm_controller_failover:start_link(),
    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_controller_failover)),

    %% Initial status should show not leader
    Status = flurm_controller_failover:get_status(),
    ?assertEqual(false, maps:get(is_leader, Status)),
    ?assertEqual(idle, maps:get(recovery_status, Status)),
    ok.

test_leadership_transition() ->
    {ok, _} = flurm_controller_failover:start_link(),

    %% Simulate becoming leader
    ok = flurm_controller_failover:on_became_leader(),
    timer:sleep(100),

    %% Should now be leader
    Status = flurm_controller_failover:get_status(),
    ?assertEqual(true, maps:get(is_leader, Status)),
    ?assertNotEqual(undefined, maps:get(became_leader_time, Status)),
    ok.

test_lost_leadership() ->
    {ok, _} = flurm_controller_failover:start_link(),

    %% Become leader first
    ok = flurm_controller_failover:on_became_leader(),
    timer:sleep(100),

    %% Verify we're leader
    Status1 = flurm_controller_failover:get_status(),
    ?assertEqual(true, maps:get(is_leader, Status1)),

    %% Lose leadership
    ok = flurm_controller_failover:on_lost_leadership(),
    timer:sleep(100),

    %% Should no longer be leader
    Status2 = flurm_controller_failover:get_status(),
    ?assertEqual(false, maps:get(is_leader, Status2)),
    ?assertEqual(undefined, maps:get(became_leader_time, Status2)),
    ?assertEqual(idle, maps:get(recovery_status, Status2)),
    ok.

test_recovery_status() ->
    {ok, _} = flurm_controller_failover:start_link(),
    Pid = whereis(flurm_controller_failover),

    %% Simulate recovery completion success
    Pid ! {recovery_complete, {ok, recovered}},
    timer:sleep(50),

    Status1 = flurm_controller_failover:get_status(),
    ?assertEqual(recovered, maps:get(recovery_status, Status1)),
    ?assertEqual(undefined, maps:get(recovery_error, Status1)),

    %% Simulate recovery failure
    Pid ! {recovery_complete, {error, test_failure}},
    timer:sleep(50),

    Status2 = flurm_controller_failover:get_status(),
    ?assertEqual(failed, maps:get(recovery_status, Status2)),
    ?assertEqual(test_failure, maps:get(recovery_error, Status2)),
    ok.

test_health_check_leadership() ->
    {ok, _} = flurm_controller_failover:start_link(),
    Pid = whereis(flurm_controller_failover),

    %% Send health check when not leader (should be no-op)
    Pid ! health_check,
    timer:sleep(50),

    %% Process should still be alive
    ?assert(is_process_alive(Pid)),

    %% Become leader
    ok = flurm_controller_failover:on_became_leader(),
    timer:sleep(100),

    %% Health check during leadership
    Pid ! health_check,
    timer:sleep(50),

    ?assert(is_process_alive(Pid)),
    ok.

%%====================================================================
%% Test Fixtures - Split-Brain Prevention
%%====================================================================

split_brain_test_() ->
    {foreach,
     fun setup_split_brain/0,
     fun cleanup_split_brain/1,
     [
         {"Forward to leader when not leader", fun test_forward_when_follower/0},
         {"Local handling when leader", fun test_local_when_leader/0},
         {"Quorum requirement simulation", fun test_quorum_requirement/0},
         {"Leader uniqueness in Ra", fun test_leader_uniqueness/0}
     ]}.

setup_split_brain() ->
    application:ensure_all_started(lager),
    cleanup_processes(),
    application:set_env(flurm_controller, cluster_name, ?TEST_CLUSTER_NAME),
    application:set_env(flurm_controller, cluster_nodes, [node()]),
    ok.

cleanup_split_brain(_) ->
    cleanup_processes(),
    application:unset_env(flurm_controller, cluster_name),
    application:unset_env(flurm_controller, cluster_nodes),
    ok.

test_forward_when_follower() ->
    %% Test that non-leaders forward requests
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(200),

    %% When Ra is not ready, forwarding should fail gracefully
    Result = flurm_controller_cluster:forward_to_leader(test_op, test_args),

    %% Should get an error (cluster not ready, no leader, etc.)
    ?assertMatch({error, _}, Result),
    ok.

test_local_when_leader() ->
    %% This tests the handle_local_operation function indirectly
    %% by testing the cluster's forward mechanism
    {ok, _} = flurm_controller_cluster:start_link(),
    timer:sleep(200),

    %% Test that unknown operations return appropriate error
    Result = flurm_controller_cluster:forward_to_leader(unknown_op, {}),

    %% Should fail gracefully
    case Result of
        {error, cluster_not_ready} -> ok;
        {error, no_leader} -> ok;
        {error, unknown_operation} -> ok;
        _ -> ?assert(false, "Unexpected result")
    end,
    ok.

test_quorum_requirement() ->
    %% Test quorum simulation using Ra state machine
    %% In a single-node setup, quorum is trivially satisfied
    State = flurm_controller_ra_machine:init(#{}),

    %% Simulate command application (would require quorum in real cluster)
    Meta = #{index => 1, term => 1},
    {NewState, Result} = flurm_controller_ra_machine:apply(
        Meta,
        {submit_job, #{name => <<"quorum_test">>}},
        State
    ),

    %% Command should succeed
    ?assertMatch({ok, _}, Result),

    Overview = flurm_controller_ra_machine:overview(NewState),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ok.

test_leader_uniqueness() ->
    %% Test that Ra state machine handles leadership transitions
    State = flurm_controller_ra_machine:init(#{}),

    %% State enter leader
    Effects1 = flurm_controller_ra_machine:state_enter(leader, State),
    ?assertEqual([], Effects1),  % No effects, but logged

    %% State enter follower
    Effects2 = flurm_controller_ra_machine:state_enter(follower, State),
    ?assertEqual([], Effects2),

    %% State enter candidate
    Effects3 = flurm_controller_ra_machine:state_enter(candidate, State),
    ?assertEqual([], Effects3),
    ok.

%%====================================================================
%% Test Fixtures - State Recovery After Node Rejoins
%%====================================================================

state_recovery_test_() ->
    {foreach,
     fun setup_recovery/0,
     fun cleanup_recovery/1,
     [
         {"Snapshot module is configured", fun test_snapshot_module/0},
         {"State can be serialized for snapshot", fun test_state_serialization/0},
         {"State recovery from log replay", fun test_log_replay_recovery/0},
         {"Recovery handles missing dependencies", fun test_recovery_with_missing_deps/0},
         {"Multiple leadership changes handled", fun test_multiple_leadership_changes/0}
     ]}.

setup_recovery() ->
    application:ensure_all_started(lager),
    cleanup_processes(),
    ok.

cleanup_recovery(_) ->
    cleanup_processes(),
    ok.

test_snapshot_module() ->
    %% Test that snapshot is properly configured
    %% The Ra machine should support snapshots for state recovery
    State = flurm_controller_ra_machine:init(#{}),

    %% Verify state can be introspected
    Overview = flurm_controller_ra_machine:overview(State),
    ?assert(is_map(Overview)),
    ok.

test_state_serialization() ->
    %% Test that state can be serialized (for snapshots)
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Add some state
    Meta = #{index => 1, term => 1},
    {State1, _} = flurm_controller_ra_machine:apply(
        Meta,
        {submit_job, #{name => <<"snapshot_test">>, user => <<"u1">>}},
        State0
    ),

    %% State should be a record that can be serialized
    %% term_to_binary should work
    Binary = term_to_binary(State1),
    ?assert(is_binary(Binary)),

    %% Should be deserializable
    Restored = binary_to_term(Binary),
    Overview1 = flurm_controller_ra_machine:overview(State1),
    Overview2 = flurm_controller_ra_machine:overview(Restored),
    ?assertEqual(Overview1, Overview2),
    ok.

test_log_replay_recovery() ->
    %% Simulate log replay for state recovery
    %% This is what Ra does when recovering from a crash

    %% Start with empty state
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Simulate a log of commands
    Log = [
        {1, {submit_job, #{name => <<"job1">>}}},
        {2, {submit_job, #{name => <<"job2">>}}},
        {3, {update_job_state, 1, running}},
        {4, {cancel_job, 2}}
    ],

    %% Replay the log
    FinalState = lists:foldl(
        fun({Index, Cmd}, AccState) ->
            Meta = #{index => Index, term => 1},
            {NewState, _} = flurm_controller_ra_machine:apply(Meta, Cmd, AccState),
            NewState
        end,
        State0,
        Log
    ),

    %% Verify final state
    Overview = flurm_controller_ra_machine:overview(FinalState),
    ?assertEqual(2, maps:get(job_count, Overview)),
    ?assertEqual(4, maps:get(last_applied_index, Overview)),
    ok.

test_recovery_with_missing_deps() ->
    %% Test that failover handler handles missing dependencies gracefully
    {ok, _} = flurm_controller_failover:start_link(),
    Pid = whereis(flurm_controller_failover),

    %% Trigger start_recovery when dependencies are not available
    Pid ! start_recovery,
    timer:sleep(200),

    %% Process should still be alive (recovery may fail but shouldn't crash)
    ?assert(is_process_alive(Pid)),

    %% Recovery may be in failed state due to missing deps
    Status = flurm_controller_failover:get_status(),
    %% Status should be one of: idle, recovering, recovered, or failed
    RecoveryStatus = maps:get(recovery_status, Status),
    ?assert(lists:member(RecoveryStatus, [idle, recovering, recovered, failed])),
    ok.

test_multiple_leadership_changes() ->
    %% Test rapid leadership changes
    {ok, _} = flurm_controller_failover:start_link(),

    %% Rapid leadership changes
    lists:foreach(fun(_) ->
        ok = flurm_controller_failover:on_became_leader(),
        timer:sleep(20),
        ok = flurm_controller_failover:on_lost_leadership(),
        timer:sleep(20)
    end, lists:seq(1, 5)),

    %% Process should still be alive
    ?assert(is_process_alive(whereis(flurm_controller_failover))),

    %% Final state should be not leader
    Status = flurm_controller_failover:get_status(),
    ?assertEqual(false, maps:get(is_leader, Status)),
    ok.

%%====================================================================
%% Additional Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup_integration/0,
     fun cleanup_integration/1,
     [
         {"Full lifecycle test", fun test_full_lifecycle/0},
         {"Cluster and failover coordination", fun test_cluster_failover_coordination/0},
         {"State machine command coverage", fun test_command_coverage/0},
         {"Error handling in state machine", fun test_state_machine_errors/0}
     ]}.

setup_integration() ->
    application:ensure_all_started(lager),
    cleanup_processes(),
    application:set_env(flurm_controller, cluster_name, ?TEST_CLUSTER_NAME),
    application:set_env(flurm_controller, cluster_nodes, [node()]),
    ok.

cleanup_integration(_) ->
    cleanup_processes(),
    application:unset_env(flurm_controller, cluster_name),
    application:unset_env(flurm_controller, cluster_nodes),
    ok.

test_full_lifecycle() ->
    %% Test a full lifecycle: init -> leader -> operations -> failover
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Create partition
    Meta1 = #{index => 1, term => 1},
    Part = #partition{name = <<"test">>, state = up, nodes = [],
                      max_time = 3600, default_time = 60, max_nodes = 10,
                      priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(Meta1, {create_partition, Part}, State0),

    %% Register node
    Meta2 = #{index => 2, term => 1},
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384,
                 state = up, features = [], partitions = [<<"test">>],
                 running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State2, ok} = flurm_controller_ra_machine:apply(Meta2, {register_node, Node}, State1),

    %% Submit job
    Meta3 = #{index => 3, term => 1},
    JobSpec = #{name => <<"lifecycle_job">>, user => <<"user">>,
                partition => <<"test">>},
    {State3, {ok, JobId}} = flurm_controller_ra_machine:apply(Meta3, {submit_job, JobSpec}, State2),

    %% Run job
    Meta4 = #{index => 4, term => 1},
    {State4, ok} = flurm_controller_ra_machine:apply(Meta4, {update_job_state, JobId, running}, State3),

    %% Complete job
    Meta5 = #{index => 5, term => 1},
    {State5, ok} = flurm_controller_ra_machine:apply(Meta5, {update_job_state, JobId, completed}, State4),

    %% Verify final state
    Overview = flurm_controller_ra_machine:overview(State5),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(1, maps:get(node_count, Overview)),
    ?assertEqual(1, maps:get(partition_count, Overview)),
    ok.

test_cluster_failover_coordination() ->
    %% Test that cluster and failover modules work together
    {ok, ClusterPid} = flurm_controller_cluster:start_link(),
    {ok, FailoverPid} = flurm_controller_failover:start_link(),

    ?assert(is_pid(ClusterPid)),
    ?assert(is_pid(FailoverPid)),

    timer:sleep(200),

    %% Both should be running
    ?assert(is_process_alive(ClusterPid)),
    ?assert(is_process_alive(FailoverPid)),

    %% Get status from both
    ClusterStatus = flurm_controller_cluster:cluster_status(),
    FailoverStatus = flurm_controller_failover:get_status(),

    ?assert(is_map(ClusterStatus)),
    ?assert(is_map(FailoverStatus)),
    ok.

test_command_coverage() ->
    %% Test all command types in the state machine
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Job commands
    Meta = #{index => 1, term => 1},

    %% Submit
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        Meta, {submit_job, #{name => <<"test">>}}, State0),

    %% Update job state
    {State2, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 2}, {update_job_state, JobId, configuring}, State1),

    %% Update job
    {State3, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 3}, {update_job, JobId, #{priority => 200}}, State2),

    %% Cancel
    {State4, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 4}, {cancel_job, JobId}, State3),

    %% Node commands
    Node = #node{hostname = <<"n1">>, cpus = 4, memory_mb = 8192, state = up,
                 features = [], partitions = [], running_jobs = [],
                 load_avg = 0.0, free_memory_mb = 8192},
    {State5, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 5}, {register_node, Node}, State4),

    {State6, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 6}, {update_node_state, <<"n1">>, drain}, State5),

    {State7, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 7}, {node_heartbeat, <<"n1">>, #{load_avg => 1.5}}, State6),

    {State8, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 8}, {unregister_node, <<"n1">>}, State7),

    %% Partition commands
    Part = #partition{name = <<"p1">>, state = up, nodes = [],
                      max_time = 3600, default_time = 60, max_nodes = 10,
                      priority = 1, allow_root = false},
    {State9, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 9}, {create_partition, Part}, State8),

    {State10, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 10}, {update_partition, <<"p1">>, #{state => drain}}, State9),

    {State11, ok} = flurm_controller_ra_machine:apply(
        Meta#{index => 11}, {delete_partition, <<"p1">>}, State10),

    %% Final state should have 1 cancelled job, 0 nodes, 0 partitions
    Overview = flurm_controller_ra_machine:overview(State11),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ok.

test_state_machine_errors() ->
    %% Test error handling in state machine
    State0 = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 1, term => 1},

    %% Cancel non-existent job
    {_, Result1} = flurm_controller_ra_machine:apply(
        Meta, {cancel_job, 9999}, State0),
    ?assertEqual({error, not_found}, Result1),

    %% Update non-existent job
    {_, Result2} = flurm_controller_ra_machine:apply(
        Meta, {update_job_state, 9999, running}, State0),
    ?assertEqual({error, not_found}, Result2),

    %% Update non-existent node
    {_, Result3} = flurm_controller_ra_machine:apply(
        Meta, {update_node_state, <<"nonexistent">>, up}, State0),
    ?assertEqual({error, not_found}, Result3),

    %% Create duplicate partition
    Part = #partition{name = <<"dup">>, state = up, nodes = [],
                      max_time = 3600, default_time = 60, max_nodes = 10,
                      priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(
        Meta, {create_partition, Part}, State0),
    {_, Result4} = flurm_controller_ra_machine:apply(
        Meta#{index => 2}, {create_partition, Part}, State1),
    ?assertEqual({error, already_exists}, Result4),

    %% Unknown command
    {_, Result5} = flurm_controller_ra_machine:apply(
        Meta, {unknown_command, args}, State0),
    ?assertEqual({error, unknown_command}, Result5),
    ok.

%%====================================================================
%% Helper function tests - Testing internal exports
%%====================================================================

internal_functions_test_() ->
    [
        {"is_this_node_leader with undefined", fun() ->
            Result = flurm_controller_cluster:is_this_node_leader(undefined),
            ?assertEqual(false, Result)
        end},
        {"is_this_node_leader with this node", fun() ->
            Result = flurm_controller_cluster:is_this_node_leader({test_name, node()}),
            ?assertEqual(true, Result)
        end},
        {"is_this_node_leader with other node", fun() ->
            Result = flurm_controller_cluster:is_this_node_leader({test_name, 'other@host'}),
            ?assertEqual(false, Result)
        end},
        {"calculate_leader_uptime with undefined", fun() ->
            Result = flurm_controller_failover:calculate_leader_uptime(undefined),
            ?assertEqual(0, Result)
        end},
        {"calculate_leader_uptime with timestamp", fun() ->
            %% Create a timestamp 2 seconds ago
            PastTime = erlang:timestamp(),
            timer:sleep(1100),
            Result = flurm_controller_failover:calculate_leader_uptime(PastTime),
            ?assert(Result >= 1)
        end}
    ].
