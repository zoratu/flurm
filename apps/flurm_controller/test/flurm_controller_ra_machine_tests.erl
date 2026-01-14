%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_ra_machine module
%%%
%%% Tests the Ra state machine callbacks: init, apply, state_enter,
%%% snapshot_installed, and overview.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_ra_machine_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

ra_machine_test_() ->
    [
        {"init/1 creates empty state", fun test_init/0},
        {"overview/1 returns correct statistics", fun test_overview/0},
        {"state_enter callbacks don't crash", fun test_state_enter/0},
        {"snapshot_installed callback works", fun test_snapshot_installed/0},
        %% Job commands
        {"apply submit_job creates job", fun test_apply_submit_job/0},
        {"apply submit_job increments next_job_id", fun test_submit_job_increments_id/0},
        {"apply cancel_job cancels job", fun test_apply_cancel_job/0},
        {"apply cancel_job for non-existent job returns error", fun test_cancel_nonexistent_job/0},
        {"apply update_job_state changes state", fun test_apply_update_job_state/0},
        {"apply update_job applies updates", fun test_apply_update_job/0},
        %% Node commands
        {"apply register_node adds node", fun test_apply_register_node/0},
        {"apply unregister_node removes node", fun test_apply_unregister_node/0},
        {"apply update_node_state changes state", fun test_apply_update_node_state/0},
        {"apply node_heartbeat updates node", fun test_apply_node_heartbeat/0},
        %% Partition commands
        {"apply create_partition creates partition", fun test_apply_create_partition/0},
        {"apply create_partition duplicate returns error", fun test_create_partition_duplicate/0},
        {"apply update_partition updates partition", fun test_apply_update_partition/0},
        {"apply delete_partition removes partition", fun test_apply_delete_partition/0},
        %% Unknown commands
        {"apply unknown command returns error", fun test_apply_unknown_command/0}
    ].

%%====================================================================
%% Test Cases - Initialization
%%====================================================================

test_init() ->
    State = flurm_controller_ra_machine:init(#{}),
    Overview = flurm_controller_ra_machine:overview(State),

    ?assertEqual(0, maps:get(job_count, Overview)),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ?assertEqual(1, maps:get(next_job_id, Overview)),
    ?assertEqual(0, maps:get(last_applied_index, Overview)),
    ok.

test_overview() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Add a job
    Meta1 = #{index => 1},
    JobSpec = #{name => <<"test">>, script => <<"echo hi">>},
    {State1, _} = flurm_controller_ra_machine:apply(Meta1, {submit_job, JobSpec}, State0),

    %% Add a node
    Meta2 = #{index => 2},
    Node = #node{hostname = <<"node1">>, cpus = 4, memory_mb = 8192},
    {State2, _} = flurm_controller_ra_machine:apply(Meta2, {register_node, Node}, State1),

    %% Check overview
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(1, maps:get(node_count, Overview)),
    ?assertEqual(2, maps:get(next_job_id, Overview)),
    ?assertEqual(2, maps:get(last_applied_index, Overview)),
    ok.

test_state_enter() ->
    State = flurm_controller_ra_machine:init(#{}),

    %% Test all state transitions
    Effects1 = flurm_controller_ra_machine:state_enter(leader, State),
    ?assertEqual([], Effects1),

    Effects2 = flurm_controller_ra_machine:state_enter(follower, State),
    ?assertEqual([], Effects2),

    Effects3 = flurm_controller_ra_machine:state_enter(candidate, State),
    ?assertEqual([], Effects3),

    Effects4 = flurm_controller_ra_machine:state_enter(unknown_state, State),
    ?assertEqual([], Effects4),
    ok.

test_snapshot_installed() ->
    State = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 100},

    %% Should not crash
    Result = flurm_controller_ra_machine:snapshot_installed(Meta, State),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Test Cases - Job Commands
%%====================================================================

test_apply_submit_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 1},

    JobSpec = #{
        name => <<"my_job">>,
        script => <<"#!/bin/bash\necho test">>,
        partition => <<"default">>,
        num_nodes => 2,
        num_cpus => 4,
        time_limit => 7200
    },

    {State1, Result} = flurm_controller_ra_machine:apply(Meta, {submit_job, JobSpec}, State0),

    ?assertMatch({ok, 1}, Result),

    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(2, maps:get(next_job_id, Overview)),
    ok.

test_submit_job_increments_id() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Submit first job
    {State1, {ok, JobId1}} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {submit_job, #{name => <<"job1">>}},
        State0
    ),
    ?assertEqual(1, JobId1),

    %% Submit second job
    {State2, {ok, JobId2}} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {submit_job, #{name => <<"job2">>}},
        State1
    ),
    ?assertEqual(2, JobId2),

    %% Submit third job
    {_State3, {ok, JobId3}} = flurm_controller_ra_machine:apply(
        #{index => 3},
        {submit_job, #{name => <<"job3">>}},
        State2
    ),
    ?assertEqual(3, JobId3),
    ok.

test_apply_cancel_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Submit a job first
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {submit_job, #{name => <<"cancel_me">>}},
        State0
    ),

    %% Cancel the job
    {State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {cancel_job, JobId},
        State1
    ),

    ?assertEqual(ok, Result),

    %% Job should still exist but be cancelled
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ok.

test_cancel_nonexistent_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {cancel_job, 9999},
        State0
    ),

    ?assertEqual({error, not_found}, Result),
    ok.

test_apply_update_job_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Submit a job
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {submit_job, #{name => <<"state_test">>}},
        State0
    ),

    %% Update to running
    {State2, Result1} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {update_job_state, JobId, running},
        State1
    ),
    ?assertEqual(ok, Result1),

    %% Update to completed
    {_State3, Result2} = flurm_controller_ra_machine:apply(
        #{index => 3},
        {update_job_state, JobId, completed},
        State2
    ),
    ?assertEqual(ok, Result2),
    ok.

test_apply_update_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Submit a job
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {submit_job, #{name => <<"update_test">>}},
        State0
    ),

    %% Update multiple fields
    Updates = #{
        priority => 200,
        allocated_nodes => [<<"node1">>, <<"node2">>]
    },

    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {update_job, JobId, Updates},
        State1
    ),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Test Cases - Node Commands
%%====================================================================

test_apply_register_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    Node = #node{
        hostname = <<"compute-001">>,
        cpus = 16,
        memory_mb = 65536,
        state = idle
    },

    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {register_node, Node},
        State0
    ),

    ?assertEqual(ok, Result),

    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(node_count, Overview)),
    ok.

test_apply_unregister_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Register a node first
    Node = #node{hostname = <<"unregister-test">>, cpus = 4},
    {State1, _} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {register_node, Node},
        State0
    ),

    %% Unregister the node
    {State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {unregister_node, <<"unregister-test">>},
        State1
    ),

    ?assertEqual(ok, Result),

    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ok.

test_apply_update_node_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Register a node first
    Node = #node{hostname = <<"state-node">>, cpus = 4, state = idle},
    {State1, _} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {register_node, Node},
        State0
    ),

    %% Update state to allocated
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {update_node_state, <<"state-node">>, allocated},
        State1
    ),

    ?assertEqual(ok, Result),
    ok.

test_apply_node_heartbeat() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Register a node first
    Node = #node{hostname = <<"hb-node">>, cpus = 4},
    {State1, _} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {register_node, Node},
        State0
    ),

    %% Send heartbeat
    HeartbeatData = #{
        load_avg => 2.5,
        free_memory_mb => 4096,
        running_jobs => [1, 2, 3]
    },

    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {node_heartbeat, <<"hb-node">>, HeartbeatData},
        State1
    ),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Test Cases - Partition Commands
%%====================================================================

test_apply_create_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    Partition = #partition{
        name = <<"gpu">>,
        state = up,
        nodes = [<<"gpu-001">>, <<"gpu-002">>],
        max_time = 86400
    },

    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {create_partition, Partition},
        State0
    ),

    ?assertEqual(ok, Result),

    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(partition_count, Overview)),
    ok.

test_create_partition_duplicate() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    Partition = #partition{name = <<"duplicate">>, state = up},

    %% Create first time
    {State1, Result1} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {create_partition, Partition},
        State0
    ),
    ?assertEqual(ok, Result1),

    %% Try to create again
    {_State2, Result2} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {create_partition, Partition},
        State1
    ),
    ?assertEqual({error, already_exists}, Result2),
    ok.

test_apply_update_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Create partition first
    Partition = #partition{name = <<"update-part">>, state = up, priority = 1},
    {State1, _} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {create_partition, Partition},
        State0
    ),

    %% Update partition
    Updates = #{
        state => down,
        priority => 10
    },

    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {update_partition, <<"update-part">>, Updates},
        State1
    ),

    ?assertEqual(ok, Result),
    ok.

test_apply_delete_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    %% Create partition first
    Partition = #partition{name = <<"delete-me">>, state = up},
    {State1, _} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {create_partition, Partition},
        State0
    ),

    %% Delete partition
    {State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2},
        {delete_partition, <<"delete-me">>},
        State1
    ),

    ?assertEqual(ok, Result),

    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ok.

%%====================================================================
%% Test Cases - Error Handling
%%====================================================================

test_apply_unknown_command() ->
    State0 = flurm_controller_ra_machine:init(#{}),

    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {unknown_command, some_args},
        State0
    ),

    ?assertEqual({error, unknown_command}, Result),
    ok.

%%====================================================================
%% Additional Tests - Job State Transitions
%%====================================================================

job_state_transitions_test_() ->
    [
        {"pending to running sets start_time", fun() ->
            State0 = flurm_controller_ra_machine:init(#{}),
            {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
                #{index => 1},
                {submit_job, #{name => <<"state-test">>}},
                State0
            ),
            {_State2, ok} = flurm_controller_ra_machine:apply(
                #{index => 2},
                {update_job_state, JobId, running},
                State1
            ),
            ok
        end},
        {"running to completed sets end_time", fun() ->
            State0 = flurm_controller_ra_machine:init(#{}),
            {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
                #{index => 1},
                {submit_job, #{name => <<"end-test">>}},
                State0
            ),
            {State2, _} = flurm_controller_ra_machine:apply(
                #{index => 2},
                {update_job_state, JobId, running},
                State1
            ),
            {_State3, ok} = flurm_controller_ra_machine:apply(
                #{index => 3},
                {update_job_state, JobId, completed},
                State2
            ),
            ok
        end},
        {"update non-existent job returns error", fun() ->
            State0 = flurm_controller_ra_machine:init(#{}),
            {_State1, Result} = flurm_controller_ra_machine:apply(
                #{index => 1},
                {update_job_state, 9999, running},
                State0
            ),
            ?assertEqual({error, not_found}, Result)
        end}
    ].
