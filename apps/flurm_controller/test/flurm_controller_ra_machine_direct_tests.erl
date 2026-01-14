%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_controller_ra_machine
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. No mocking of the module under test.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_ra_machine_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

ra_machine_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates empty state", fun test_init/0},
      {"overview returns state summary", fun test_overview/0},
      {"submit_job creates new job", fun test_submit_job/0},
      {"cancel_job marks job cancelled", fun test_cancel_job/0},
      {"cancel_job returns error for non-existent job", fun test_cancel_nonexistent_job/0},
      {"update_job_state changes job state", fun test_update_job_state/0},
      {"update_job_state to running sets start_time", fun test_update_job_state_running/0},
      {"update_job_state to completed sets end_time", fun test_update_job_state_completed/0},
      {"update_job applies updates to job", fun test_update_job/0},
      {"register_node adds node", fun test_register_node/0},
      {"unregister_node removes node", fun test_unregister_node/0},
      {"update_node_state changes node state", fun test_update_node_state/0},
      {"node_heartbeat updates node", fun test_node_heartbeat/0},
      {"create_partition adds partition", fun test_create_partition/0},
      {"create_partition rejects duplicate", fun test_create_partition_duplicate/0},
      {"update_partition modifies partition", fun test_update_partition/0},
      {"delete_partition removes partition", fun test_delete_partition/0},
      {"state_enter leader logs info", fun test_state_enter_leader/0},
      {"state_enter follower logs info", fun test_state_enter_follower/0},
      {"state_enter candidate logs debug", fun test_state_enter_candidate/0},
      {"state_enter other returns empty", fun test_state_enter_other/0},
      {"snapshot_installed returns ok", fun test_snapshot_installed/0},
      {"unknown command returns error", fun test_unknown_command/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    State = flurm_controller_ra_machine:init(#{}),
    ?assertMatch({state, _, _, _, _, _}, State),
    Overview = flurm_controller_ra_machine:overview(State),
    ?assertEqual(0, maps:get(job_count, Overview)),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ?assertEqual(1, maps:get(next_job_id, Overview)),
    ?assertEqual(0, maps:get(last_applied_index, Overview)).

test_overview() ->
    State = flurm_controller_ra_machine:init(#{}),
    Overview = flurm_controller_ra_machine:overview(State),
    ?assert(is_map(Overview)),
    ?assert(maps:is_key(job_count, Overview)),
    ?assert(maps:is_key(node_count, Overview)),
    ?assert(maps:is_key(partition_count, Overview)),
    ?assert(maps:is_key(next_job_id, Overview)),
    ?assert(maps:is_key(last_applied_index, Overview)).

test_submit_job() ->
    State = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 1},
    JobSpec = #{name => <<"test_job">>, user => <<"testuser">>, partition => <<"default">>},
    {NewState, Result} = flurm_controller_ra_machine:apply(Meta, {submit_job, JobSpec}, State),
    ?assertMatch({ok, 1}, Result),
    Overview = flurm_controller_ra_machine:overview(NewState),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(2, maps:get(next_job_id, Overview)),
    ?assertEqual(1, maps:get(last_applied_index, Overview)).

test_cancel_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    %% First submit a job
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    %% Then cancel it
    {State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {cancel_job, JobId}, State1),
    ?assertEqual(ok, Result),
    ?assertEqual(2, element(6, State2)). % last_applied_index

test_cancel_nonexistent_job() ->
    State = flurm_controller_ra_machine:init(#{}),
    {NewState, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {cancel_job, 9999}, State),
    ?assertMatch({error, not_found}, Result),
    ?assertEqual(1, element(6, NewState)).

test_update_job_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, configuring}, State1),
    ?assertEqual(ok, Result),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(2, maps:get(last_applied_index, Overview)).

test_update_job_state_running() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, running}, State1),
    ?assertEqual(ok, Result).

test_update_job_state_completed() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, completed}, State1),
    ?assertEqual(ok, Result).

test_update_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    Updates = #{priority => 200, allocated_nodes => [<<"node1">>]},
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job, JobId, Updates}, State1),
    ?assertEqual(ok, Result).

test_register_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{
        hostname = <<"node1">>,
        cpus = 8,
        memory_mb = 16384,
        state = idle,
        features = [],
        partitions = [<<"default">>],
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = 16384
    },
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {register_node, Node}, State0),
    ?assertEqual(ok, Result),
    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(node_count, Overview)).

test_unregister_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    {State2, Result} = flurm_controller_ra_machine:apply(#{index => 2}, {unregister_node, <<"node1">>}, State1),
    ?assertEqual(ok, Result),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(node_count, Overview)).

test_update_node_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_node_state, <<"node1">>, allocated}, State1),
    ?assertEqual(ok, Result).

test_node_heartbeat() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    HeartbeatData = #{load_avg => 1.5, free_memory_mb => 8192, running_jobs => [1, 2]},
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {node_heartbeat, <<"node1">>, HeartbeatData}, State1),
    ?assertEqual(ok, Result).

test_create_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Partition = #partition{
        name = <<"compute">>,
        state = up,
        nodes = [],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        priority = 100,
        allow_root = false
    },
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {create_partition, Partition}, State0),
    ?assertEqual(ok, Result),
    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(partition_count, Overview)).

test_create_partition_duplicate() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Partition = #partition{name = <<"compute">>, state = up, nodes = [], max_time = 86400,
                           default_time = 3600, max_nodes = 100, priority = 100, allow_root = false},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Partition}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(#{index => 2}, {create_partition, Partition}, State1),
    ?assertMatch({error, already_exists}, Result).

test_update_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Partition = #partition{name = <<"compute">>, state = up, nodes = [], max_time = 86400,
                           default_time = 3600, max_nodes = 100, priority = 100, allow_root = false},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Partition}, State0),
    Updates = #{state => down, priority => 50},
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_partition, <<"compute">>, Updates}, State1),
    ?assertEqual(ok, Result).

test_delete_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Partition = #partition{name = <<"compute">>, state = up, nodes = [], max_time = 86400,
                           default_time = 3600, max_nodes = 100, priority = 100, allow_root = false},
    {State1, _} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Partition}, State0),
    {State2, Result} = flurm_controller_ra_machine:apply(#{index => 2}, {delete_partition, <<"compute">>}, State1),
    ?assertEqual(ok, Result),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(partition_count, Overview)).

test_state_enter_leader() ->
    State = flurm_controller_ra_machine:init(#{}),
    Effects = flurm_controller_ra_machine:state_enter(leader, State),
    ?assertEqual([], Effects).

test_state_enter_follower() ->
    State = flurm_controller_ra_machine:init(#{}),
    Effects = flurm_controller_ra_machine:state_enter(follower, State),
    ?assertEqual([], Effects).

test_state_enter_candidate() ->
    State = flurm_controller_ra_machine:init(#{}),
    Effects = flurm_controller_ra_machine:state_enter(candidate, State),
    ?assertEqual([], Effects).

test_state_enter_other() ->
    State = flurm_controller_ra_machine:init(#{}),
    Effects = flurm_controller_ra_machine:state_enter(some_other_state, State),
    ?assertEqual([], Effects).

test_snapshot_installed() ->
    State = flurm_controller_ra_machine:init(#{}),
    Result = flurm_controller_ra_machine:snapshot_installed(#{}, State),
    ?assertEqual(ok, Result).

test_unknown_command() ->
    State = flurm_controller_ra_machine:init(#{}),
    {_NewState, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {unknown_command, some_arg}, State),
    ?assertMatch({error, unknown_command}, Result).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
     {"update_job_state failed sets end_time", fun test_update_job_state_failed/0},
     {"update_job_state cancelled sets end_time", fun test_update_job_state_cancelled/0},
     {"update_job_state timeout sets end_time", fun test_update_job_state_timeout/0},
     {"update_job_state node_fail sets end_time", fun test_update_job_state_node_fail/0},
     {"update_job with various fields", fun test_update_job_various_fields/0},
     {"node_heartbeat nonexistent node", fun test_node_heartbeat_nonexistent/0},
     {"update_node_state nonexistent node", fun test_update_node_state_nonexistent/0},
     {"update_partition nonexistent partition", fun test_update_partition_nonexistent/0},
     {"update_job nonexistent job", fun test_update_job_nonexistent/0},
     {"update_job_state nonexistent job", fun test_update_job_state_nonexistent/0}
    ].

test_update_job_state_failed() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, failed}, State1),
    ?assertEqual(ok, Result).

test_update_job_state_cancelled() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, cancelled}, State1),
    ?assertEqual(ok, Result).

test_update_job_state_timeout() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, timeout}, State1),
    ?assertEqual(ok, Result).

test_update_job_state_node_fail() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job_state, JobId, node_fail}, State1),
    ?assertEqual(ok, Result).

test_update_job_various_fields() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, JobId}} = flurm_controller_ra_machine:apply(
        #{index => 1}, {submit_job, #{name => <<"job1">>}}, State0),
    Updates = #{
        state => running,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        start_time => erlang:system_time(second),
        end_time => erlang:system_time(second) + 3600,
        exit_code => 0,
        priority => 500,
        unknown_field => ignored
    },
    {_State2, Result} = flurm_controller_ra_machine:apply(
        #{index => 2}, {update_job, JobId, Updates}, State1),
    ?assertEqual(ok, Result).

test_node_heartbeat_nonexistent() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    HeartbeatData = #{load_avg => 1.5},
    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {node_heartbeat, <<"nonexistent">>, HeartbeatData}, State0),
    ?assertMatch({error, not_found}, Result).

test_update_node_state_nonexistent() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {update_node_state, <<"nonexistent">>, down}, State0),
    ?assertMatch({error, not_found}, Result).

test_update_partition_nonexistent() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {update_partition, <<"nonexistent">>, #{state => down}}, State0),
    ?assertMatch({error, not_found}, Result).

test_update_job_nonexistent() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {update_job, 9999, #{priority => 100}}, State0),
    ?assertMatch({error, not_found}, Result).

test_update_job_state_nonexistent() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {_State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1}, {update_job_state, 9999, running}, State0),
    ?assertMatch({error, not_found}, Result).
