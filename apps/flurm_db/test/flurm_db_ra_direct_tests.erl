%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_ra module
%%%
%%% These tests call flurm_db_ra functions directly to get code coverage.
%%% We test the Ra state machine callbacks (init, apply, state_enter)
%%% directly without needing a running Ra cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

ra_machine_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Init creates empty state", fun init_test/0},
      {"Submit job command", fun submit_job_command_test/0},
      {"Cancel job command - success", fun cancel_job_success_test/0},
      {"Cancel job command - not found", fun cancel_job_not_found_test/0},
      {"Cancel job command - already terminal", fun cancel_job_terminal_test/0},
      {"Update job state command - success", fun update_job_state_success_test/0},
      {"Update job state command - not found", fun update_job_state_not_found_test/0},
      {"Allocate job command - success", fun allocate_job_success_test/0},
      {"Allocate job command - not found", fun allocate_job_not_found_test/0},
      {"Allocate job command - invalid state", fun allocate_job_invalid_state_test/0},
      {"Set job exit code command - success", fun set_job_exit_code_success_test/0},
      {"Set job exit code command - not found", fun set_job_exit_code_not_found_test/0},
      {"Allocate job ID command", fun allocate_job_id_test/0},
      {"Register node command - new", fun register_node_new_test/0},
      {"Register node command - update", fun register_node_update_test/0},
      {"Update node state command - success", fun update_node_state_success_test/0},
      {"Update node state command - not found", fun update_node_state_not_found_test/0},
      {"Unregister node command - success", fun unregister_node_success_test/0},
      {"Unregister node command - not found", fun unregister_node_not_found_test/0},
      {"Create partition command - success", fun create_partition_success_test/0},
      {"Create partition command - already exists", fun create_partition_exists_test/0},
      {"Delete partition command - success", fun delete_partition_success_test/0},
      {"Delete partition command - not found", fun delete_partition_not_found_test/0},
      {"Unknown command", fun unknown_command_test/0},
      {"State enter - leader", fun state_enter_leader_test/0},
      {"State enter - follower", fun state_enter_follower_test/0},
      {"State enter - recover", fun state_enter_recover_test/0},
      {"State enter - eol", fun state_enter_eol_test/0},
      {"State enter - other", fun state_enter_other_test/0},
      {"Snapshot module", fun snapshot_module_test/0}
     ]}.

helper_functions_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Make job record", fun make_job_record_test/0},
      {"Make job record with default priority", fun make_job_record_default_priority_test/0},
      {"Make node record", fun make_node_record_test/0},
      {"Make partition record", fun make_partition_record_test/0}
     ]}.

update_job_state_transitions_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Update job to running sets start time", fun update_job_running_test/0},
      {"Update job to completed sets end time", fun update_job_completed_test/0},
      {"Update job to failed sets end time", fun update_job_failed_test/0},
      {"Update job to cancelled sets end time", fun update_job_cancelled_test/0},
      {"Update job to timeout sets end time", fun update_job_timeout_test/0},
      {"Update job to node_fail sets end time", fun update_job_node_fail_test/0},
      {"Update job to configuring", fun update_job_configuring_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    %% Mock the effects module so effects don't fail during tests
    meck:new(flurm_db_ra_effects, [passthrough]),
    meck:expect(flurm_db_ra_effects, job_submitted, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, job_cancelled, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, job_state_changed, fun(_, _, _) -> ok end),
    meck:expect(flurm_db_ra_effects, job_allocated, fun(_, _) -> ok end),
    meck:expect(flurm_db_ra_effects, job_completed, fun(_, _) -> ok end),
    meck:expect(flurm_db_ra_effects, node_registered, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, node_updated, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, node_state_changed, fun(_, _, _) -> ok end),
    meck:expect(flurm_db_ra_effects, node_unregistered, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, partition_created, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, partition_deleted, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, became_leader, fun(_) -> ok end),
    meck:expect(flurm_db_ra_effects, became_follower, fun(_) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(flurm_db_ra_effects).

%%====================================================================
%% Ra Machine Callback Tests
%%====================================================================

init_test() ->
    State = flurm_db_ra:init(#{}),
    ?assertMatch(#ra_state{jobs = #{}, nodes = #{}, partitions = #{},
                           job_counter = 1, version = 1}, State).

submit_job_command_test() ->
    State = flurm_db_ra:init(#{}),
    JobSpec = make_test_job_spec(),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State),

    ?assertEqual({ok, 1}, Result),
    ?assertEqual(2, NewState#ra_state.job_counter),
    ?assert(maps:is_key(1, NewState#ra_state.jobs)),
    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_submitted, [_]}], Effects).

cancel_job_success_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, 1}, State),

    ?assertEqual(ok, Result),
    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(cancelled, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_cancelled, [_]}], Effects).

cancel_job_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, 999}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

cancel_job_terminal_test() ->
    %% Create a state with a completed job
    State0 = state_with_pending_job(),
    Job = maps:get(1, State0#ra_state.jobs),
    CompletedJob = Job#ra_job{state = completed},
    State = State0#ra_state{jobs = #{1 => CompletedJob}},
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {cancel_job, 1}, State),

    ?assertEqual({error, already_terminal}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

update_job_state_success_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {update_job_state, 1, running}, State),

    ?assertEqual(ok, Result),
    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(running, Job#ra_job.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_state_changed, [1, pending, running]}], Effects).

update_job_state_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {update_job_state, 999, running}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

allocate_job_success_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    Nodes = [<<"node1">>, <<"node2">>],

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {allocate_job, 1, Nodes}, State),

    ?assertEqual(ok, Result),
    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(configuring, Job#ra_job.state),
    ?assertEqual(Nodes, Job#ra_job.allocated_nodes),
    ?assertNotEqual(undefined, Job#ra_job.start_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_allocated, [_, Nodes]}], Effects).

allocate_job_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {allocate_job, 999, [<<"node1">>]}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

allocate_job_invalid_state_test() ->
    %% Job in running state cannot be allocated
    State0 = state_with_pending_job(),
    Job = maps:get(1, State0#ra_state.jobs),
    RunningJob = Job#ra_job{state = running},
    State = State0#ra_state{jobs = #{1 => RunningJob}},
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {allocate_job, 1, [<<"node1">>]}, State),

    ?assertEqual({error, {invalid_state, running}}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

set_job_exit_code_success_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},

    %% Exit code 0 -> completed
    {NewState1, Result1, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, 1, 0}, State),
    ?assertEqual(ok, Result1),
    Job1 = maps:get(1, NewState1#ra_state.jobs),
    ?assertEqual(completed, Job1#ra_job.state),
    ?assertEqual(0, Job1#ra_job.exit_code),

    %% Exit code non-zero -> failed
    {NewState2, Result2, _} = flurm_db_ra:apply(Meta, {set_job_exit_code, 1, 1}, State),
    ?assertEqual(ok, Result2),
    Job2 = maps:get(1, NewState2#ra_state.jobs),
    ?assertEqual(failed, Job2#ra_job.state),
    ?assertEqual(1, Job2#ra_job.exit_code).

set_job_exit_code_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {set_job_exit_code, 999, 0}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

allocate_job_id_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState1, Result1, Effects1} = flurm_db_ra:apply(Meta, allocate_job_id, State),
    ?assertEqual({ok, 1}, Result1),
    ?assertEqual(2, NewState1#ra_state.job_counter),
    ?assertEqual([], Effects1),

    {NewState2, Result2, _} = flurm_db_ra:apply(Meta, allocate_job_id, NewState1),
    ?assertEqual({ok, 2}, Result2),
    ?assertEqual(3, NewState2#ra_state.job_counter).

register_node_new_test() ->
    State = flurm_db_ra:init(#{}),
    NodeSpec = make_test_node_spec(),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State),

    ?assertEqual({ok, registered}, Result),
    ?assert(maps:is_key(<<"node1">>, NewState#ra_state.nodes)),
    Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
    ?assertEqual(<<"node1">>, Node#ra_node.name),
    ?assertEqual(up, Node#ra_node.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_registered, [_]}], Effects).

register_node_update_test() ->
    State0 = flurm_db_ra:init(#{}),
    NodeSpec = make_test_node_spec(),
    Meta = #{index => 1, term => 1},

    %% First registration
    {State1, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),

    %% Second registration - update
    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State1),

    ?assertEqual({ok, updated}, Result),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_updated, [_]}], Effects).

update_node_state_success_test() ->
    State = state_with_node(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {update_node_state, <<"node1">>, drain}, State),

    ?assertEqual(ok, Result),
    Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
    ?assertEqual(drain, Node#ra_node.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_state_changed, [<<"node1">>, up, drain]}], Effects).

update_node_state_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {update_node_state, <<"unknown">>, drain}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

unregister_node_success_test() ->
    State = state_with_node(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {unregister_node, <<"node1">>}, State),

    ?assertEqual(ok, Result),
    ?assertNot(maps:is_key(<<"node1">>, NewState#ra_state.nodes)),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_unregistered, [_]}], Effects).

unregister_node_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {unregister_node, <<"unknown">>}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

create_partition_success_test() ->
    State = flurm_db_ra:init(#{}),
    PartSpec = make_test_partition_spec(),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State),

    ?assertEqual(ok, Result),
    ?assert(maps:is_key(<<"batch">>, NewState#ra_state.partitions)),
    Part = maps:get(<<"batch">>, NewState#ra_state.partitions),
    ?assertEqual(<<"batch">>, Part#ra_partition.name),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, partition_created, [_]}], Effects).

create_partition_exists_test() ->
    State = state_with_partition(),
    PartSpec = make_test_partition_spec(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State),

    ?assertEqual({error, already_exists}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

delete_partition_success_test() ->
    State = state_with_partition(),
    Meta = #{index => 2, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {delete_partition, <<"batch">>}, State),

    ?assertEqual(ok, Result),
    ?assertNot(maps:is_key(<<"batch">>, NewState#ra_state.partitions)),
    ?assertMatch([{mod_call, flurm_db_ra_effects, partition_deleted, [_]}], Effects).

delete_partition_not_found_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {delete_partition, <<"unknown">>}, State),

    ?assertEqual({error, not_found}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

unknown_command_test() ->
    State = flurm_db_ra:init(#{}),
    Meta = #{index => 1, term => 1},

    {NewState, Result, Effects} = flurm_db_ra:apply(Meta, {unknown_command, foo}, State),

    ?assertEqual({error, {unknown_command, {unknown_command, foo}}}, Result),
    ?assertEqual(State, NewState),
    ?assertEqual([], Effects).

%%====================================================================
%% State Enter Tests
%%====================================================================

state_enter_leader_test() ->
    State = flurm_db_ra:init(#{}),
    Effects = flurm_db_ra:state_enter(leader, State),
    ?assertMatch([{mod_call, flurm_db_ra_effects, became_leader, [_]}], Effects).

state_enter_follower_test() ->
    State = flurm_db_ra:init(#{}),
    Effects = flurm_db_ra:state_enter(follower, State),
    ?assertMatch([{mod_call, flurm_db_ra_effects, became_follower, [_]}], Effects).

state_enter_recover_test() ->
    State = flurm_db_ra:init(#{}),
    Effects = flurm_db_ra:state_enter(recover, State),
    ?assertEqual([], Effects).

state_enter_eol_test() ->
    State = flurm_db_ra:init(#{}),
    Effects = flurm_db_ra:state_enter(eol, State),
    ?assertEqual([], Effects).

state_enter_other_test() ->
    State = flurm_db_ra:init(#{}),
    Effects = flurm_db_ra:state_enter(candidate, State),
    ?assertEqual([], Effects).

snapshot_module_test() ->
    ?assertEqual(ra_machine_simple, flurm_db_ra:snapshot_module()).

%%====================================================================
%% Helper Function Tests
%%====================================================================

make_job_record_test() ->
    JobSpec = make_test_job_spec(),
    Job = flurm_db_ra:make_job_record(1, JobSpec),

    ?assertEqual(1, Job#ra_job.id),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    ?assertEqual(<<"user1">>, Job#ra_job.user),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(100, Job#ra_job.priority),
    ?assertEqual([], Job#ra_job.allocated_nodes).

make_job_record_default_priority_test() ->
    JobSpec = #ra_job_spec{
        name = <<"test">>,
        user = <<"user1">>,
        group = <<"users">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = undefined  %% Should use default
    },
    Job = flurm_db_ra:make_job_record(1, JobSpec),
    ?assertEqual(100, Job#ra_job.priority).  %% DEFAULT_PRIORITY

make_node_record_test() ->
    NodeSpec = make_test_node_spec(),
    Node = flurm_db_ra:make_node_record(NodeSpec),

    ?assertEqual(<<"node1">>, Node#ra_node.name),
    ?assertEqual(<<"node1.example.com">>, Node#ra_node.hostname),
    ?assertEqual(16, Node#ra_node.cpus),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(up, Node#ra_node.state),
    ?assertEqual([], Node#ra_node.running_jobs).

make_partition_record_test() ->
    PartSpec = make_test_partition_spec(),
    Part = flurm_db_ra:make_partition_record(PartSpec),

    ?assertEqual(<<"batch">>, Part#ra_partition.name),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertEqual([<<"node1">>, <<"node2">>], Part#ra_partition.nodes),
    ?assertEqual(86400, Part#ra_partition.max_time).

%%====================================================================
%% Job State Transition Tests
%%====================================================================

update_job_running_test() ->
    State = state_with_pending_job(),
    Job = maps:get(1, State#ra_state.jobs),
    ?assertEqual(undefined, Job#ra_job.start_time),

    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, running}, State),

    UpdatedJob = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(running, UpdatedJob#ra_job.state),
    ?assertNotEqual(undefined, UpdatedJob#ra_job.start_time).

update_job_completed_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, completed}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(completed, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

update_job_failed_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, failed}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(failed, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

update_job_cancelled_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, cancelled}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(cancelled, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

update_job_timeout_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, timeout}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(timeout, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

update_job_node_fail_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, node_fail}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(node_fail, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

update_job_configuring_test() ->
    State = state_with_pending_job(),
    Meta = #{index => 2, term => 1},
    {NewState, _, _} = flurm_db_ra:apply(Meta, {update_job_state, 1, configuring}, State),

    Job = maps:get(1, NewState#ra_state.jobs),
    ?assertEqual(configuring, Job#ra_job.state),
    %% configuring doesn't set end_time
    ?assertEqual(undefined, Job#ra_job.end_time).

%%====================================================================
%% Test Helpers
%%====================================================================

make_test_job_spec() ->
    #ra_job_spec{
        name = <<"test_job">>,
        user = <<"user1">>,
        group = <<"users">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    }.

make_test_node_spec() ->
    #ra_node_spec{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 6818,
        cpus = 16,
        memory_mb = 32768,
        gpus = 2,
        features = [gpu, fast],
        partitions = [<<"batch">>]
    }.

make_test_partition_spec() ->
    #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 1
    }.

state_with_pending_job() ->
    State0 = flurm_db_ra:init(#{}),
    JobSpec = make_test_job_spec(),
    Meta = #{index => 1, term => 1},
    {State, _, _} = flurm_db_ra:apply(Meta, {submit_job, JobSpec}, State0),
    State.

state_with_node() ->
    State0 = flurm_db_ra:init(#{}),
    NodeSpec = make_test_node_spec(),
    Meta = #{index => 1, term => 1},
    {State, _, _} = flurm_db_ra:apply(Meta, {register_node, NodeSpec}, State0),
    State.

state_with_partition() ->
    State0 = flurm_db_ra:init(#{}),
    PartSpec = make_test_partition_spec(),
    Meta = #{index => 1, term => 1},
    {State, _, _} = flurm_db_ra:apply(Meta, {create_partition, PartSpec}, State0),
    State.
