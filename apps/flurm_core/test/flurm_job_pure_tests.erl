%%%-------------------------------------------------------------------
%%% @doc FLURM Job Pure Unit Tests
%%%
%%% Pure unit tests for the flurm_job gen_statem module. These tests
%%% directly call the callback functions without spawning processes
%%% or using any mocking framework.
%%%
%%% NO MECK - tests callback functions directly with real values.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Data Helpers
%%====================================================================

%% Create a test job_spec record
make_job_spec() ->
    make_job_spec(#{}).

make_job_spec(Overrides) ->
    Defaults = #{
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #job_spec{
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props),
        priority = maps:get(priority, Props)
    }.

%% Create a test job_data record for testing state callbacks
make_job_data() ->
    make_job_data(#{}).

make_job_data(Overrides) ->
    Defaults = #{
        job_id => 12345,
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        allocated_nodes => [],
        submit_time => erlang:timestamp(),
        start_time => undefined,
        end_time => undefined,
        exit_code => undefined,
        priority => 100,
        state_version => ?JOB_STATE_VERSION
    },
    Props = maps:merge(Defaults, Overrides),
    #job_data{
        job_id = maps:get(job_id, Props),
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props),
        allocated_nodes = maps:get(allocated_nodes, Props),
        submit_time = maps:get(submit_time, Props),
        start_time = maps:get(start_time, Props),
        end_time = maps:get(end_time, Props),
        exit_code = maps:get(exit_code, Props),
        priority = maps:get(priority, Props),
        state_version = maps:get(state_version, Props)
    }.

%% Fake caller reference for gen_statem calls
fake_from() ->
    {self(), make_ref()}.

%%====================================================================
%% callback_mode/0 Tests
%%====================================================================

callback_mode_test() ->
    Mode = flurm_job:callback_mode(),
    ?assertEqual([state_functions, state_enter], Mode).

callback_mode_has_state_functions_test() ->
    Mode = flurm_job:callback_mode(),
    ?assert(lists:member(state_functions, Mode)).

callback_mode_has_state_enter_test() ->
    Mode = flurm_job:callback_mode(),
    ?assert(lists:member(state_enter, Mode)).

%%====================================================================
%% init/1 Tests
%%====================================================================

init_basic_test() ->
    JobSpec = make_job_spec(),
    Result = flurm_job:init([JobSpec]),
    ?assertMatch({ok, pending, #job_data{}}, Result).

init_returns_pending_state_test() ->
    JobSpec = make_job_spec(),
    {ok, State, _Data} = flurm_job:init([JobSpec]),
    ?assertEqual(pending, State).

init_sets_job_id_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assert(is_integer(Data#job_data.job_id)),
    ?assert(Data#job_data.job_id > 0).

init_preserves_user_id_test() ->
    JobSpec = make_job_spec(#{user_id => 2001}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(2001, Data#job_data.user_id).

init_preserves_group_id_test() ->
    JobSpec = make_job_spec(#{group_id => 3001}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(3001, Data#job_data.group_id).

init_preserves_partition_test() ->
    JobSpec = make_job_spec(#{partition => <<"compute">>}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(<<"compute">>, Data#job_data.partition).

init_preserves_num_nodes_test() ->
    JobSpec = make_job_spec(#{num_nodes => 10}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(10, Data#job_data.num_nodes).

init_preserves_num_cpus_test() ->
    JobSpec = make_job_spec(#{num_cpus => 32}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(32, Data#job_data.num_cpus).

init_preserves_time_limit_test() ->
    JobSpec = make_job_spec(#{time_limit => 7200}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(7200, Data#job_data.time_limit).

init_preserves_script_test() ->
    Script = <<"#!/bin/bash\necho 'test script'">>,
    JobSpec = make_job_spec(#{script => Script}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(Script, Data#job_data.script).

init_sets_empty_allocated_nodes_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual([], Data#job_data.allocated_nodes).

init_sets_submit_time_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertMatch({_, _, _}, Data#job_data.submit_time).

init_leaves_start_time_undefined_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(undefined, Data#job_data.start_time).

init_leaves_end_time_undefined_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(undefined, Data#job_data.end_time).

init_leaves_exit_code_undefined_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(undefined, Data#job_data.exit_code).

init_sets_state_version_test() ->
    JobSpec = make_job_spec(),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?JOB_STATE_VERSION, Data#job_data.state_version).

%% Priority handling tests
init_uses_default_priority_when_undefined_test() ->
    JobSpec = #job_spec{
        user_id = 1000,
        group_id = 1000,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 1,
        time_limit = 3600,
        script = <<"test">>,
        priority = undefined
    },
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?DEFAULT_PRIORITY, Data#job_data.priority).

init_clamps_high_priority_test() ->
    JobSpec = make_job_spec(#{priority => 999999}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?MAX_PRIORITY, Data#job_data.priority).

init_clamps_negative_priority_test() ->
    JobSpec = make_job_spec(#{priority => -500}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?MIN_PRIORITY, Data#job_data.priority).

init_accepts_valid_priority_test() ->
    JobSpec = make_job_spec(#{priority => 500}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(500, Data#job_data.priority).

init_accepts_min_priority_test() ->
    JobSpec = make_job_spec(#{priority => ?MIN_PRIORITY}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?MIN_PRIORITY, Data#job_data.priority).

init_accepts_max_priority_test() ->
    JobSpec = make_job_spec(#{priority => ?MAX_PRIORITY}),
    {ok, _State, Data} = flurm_job:init([JobSpec]),
    ?assertEqual(?MAX_PRIORITY, Data#job_data.priority).

%%====================================================================
%% terminate/3 Tests
%% Note: terminate calls flurm_job_registry:unregister_job which
%% requires the registry to be running. We test by catching the exit
%% and verifying the function was called, which exercises the code path.
%%====================================================================

terminate_returns_ok_test() ->
    %% The terminate function calls unregister_job which may fail if registry
    %% isn't running. We verify the function is called by catching the error.
    Data = make_job_data(),
    %% Start the registry for this test
    {ok, Pid} = flurm_job_registry:start_link(),
    try
        Result = flurm_job:terminate(normal, pending, Data),
        ?assertEqual(ok, Result)
    after
        case is_process_alive(Pid) of
            true ->
                Ref = monitor(process, Pid),
                unlink(Pid),
                catch gen_server:stop(Pid, shutdown, 5000),
                receive
                    {'DOWN', Ref, process, Pid, _} -> ok
                after 5000 ->
                    demonitor(Ref, [flush]),
                    catch exit(Pid, kill)
                end;
            false ->
                ok
        end
    end.

terminate_with_shutdown_reason_test() ->
    Data = make_job_data(),
    {ok, Pid} = flurm_job_registry:start_link(),
    try
        Result = flurm_job:terminate(shutdown, running, Data),
        ?assertEqual(ok, Result)
    after
        case is_process_alive(Pid) of
            true ->
                Ref = monitor(process, Pid),
                unlink(Pid),
                catch gen_server:stop(Pid, shutdown, 5000),
                receive
                    {'DOWN', Ref, process, Pid, _} -> ok
                after 5000 ->
                    demonitor(Ref, [flush]),
                    catch exit(Pid, kill)
                end;
            false ->
                ok
        end
    end.

terminate_with_error_reason_test() ->
    Data = make_job_data(),
    {ok, Pid} = flurm_job_registry:start_link(),
    try
        Result = flurm_job:terminate({error, some_reason}, configuring, Data),
        ?assertEqual(ok, Result)
    after
        case is_process_alive(Pid) of
            true ->
                Ref = monitor(process, Pid),
                unlink(Pid),
                catch gen_server:stop(Pid, shutdown, 5000),
                receive
                    {'DOWN', Ref, process, Pid, _} -> ok
                after 5000 ->
                    demonitor(Ref, [flush]),
                    catch exit(Pid, kill)
                end;
            false ->
                ok
        end
    end.

%%====================================================================
%% code_change/4 Tests
%%====================================================================

code_change_returns_ok_test() ->
    Data = make_job_data(),
    Result = flurm_job:code_change("1.0", pending, Data, []),
    ?assertMatch({ok, pending, #job_data{}}, Result).

code_change_preserves_state_test() ->
    Data = make_job_data(),
    {ok, State, _NewData} = flurm_job:code_change("1.0", running, Data, []),
    ?assertEqual(running, State).

code_change_upgrades_old_version_test() ->
    %% Test that old state version gets upgraded
    OldData = make_job_data(#{state_version => 0}),
    {ok, _State, NewData} = flurm_job:code_change("1.0", pending, OldData, []),
    ?assertEqual(?JOB_STATE_VERSION, NewData#job_data.state_version).

code_change_keeps_current_version_test() ->
    Data = make_job_data(#{state_version => ?JOB_STATE_VERSION}),
    {ok, _State, NewData} = flurm_job:code_change("1.0", pending, Data, []),
    ?assertEqual(?JOB_STATE_VERSION, NewData#job_data.state_version).

%%====================================================================
%% pending/3 State Callback Tests
%%====================================================================

pending_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:pending(enter, configuring, Data),
    ?assertMatch({keep_state, #job_data{}, _}, Result).

pending_enter_sets_timeout_test() ->
    Data = make_job_data(),
    {keep_state, _Data, Actions} = flurm_job:pending(enter, configuring, Data),
    ?assert(lists:any(fun({state_timeout, _, _}) -> true; (_) -> false end, Actions)).

pending_get_job_id_test() ->
    Data = make_job_data(#{job_id => 99999}),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 99999}]}, Result).

pending_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

pending_get_info_returns_map_test() ->
    Data = make_job_data(#{job_id => 123, user_id => 1001}),
    From = fake_from(),
    {keep_state, _, [{reply, _, {ok, Info}}]} = flurm_job:pending({call, From}, get_info, Data),
    ?assert(is_map(Info)),
    ?assertEqual(123, maps:get(job_id, Info)),
    ?assertEqual(1001, maps:get(user_id, Info)),
    ?assertEqual(pending, maps:get(state, Info)).

pending_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, pending}}]}, Result).

pending_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, cancel, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

pending_cancel_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:pending({call, From}, cancel, Data),
    ?assertMatch({_, _, _}, NewData#job_data.end_time).

pending_allocate_sufficient_nodes_test() ->
    Data = make_job_data(#{num_nodes => 2}),
    From = fake_from(),
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_job:pending({call, From}, {allocate, Nodes}, Data),
    ?assertMatch({next_state, configuring, #job_data{}, [{reply, _, ok}]}, Result).

pending_allocate_sets_allocated_nodes_test() ->
    Data = make_job_data(#{num_nodes => 2}),
    From = fake_from(),
    Nodes = [<<"node1">>, <<"node2">>],
    {next_state, configuring, NewData, _} = flurm_job:pending({call, From}, {allocate, Nodes}, Data),
    ?assertEqual(Nodes, NewData#job_data.allocated_nodes).

pending_allocate_extra_nodes_test() ->
    Data = make_job_data(#{num_nodes => 2}),
    From = fake_from(),
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
    Result = flurm_job:pending({call, From}, {allocate, Nodes}, Data),
    ?assertMatch({next_state, configuring, #job_data{}, [{reply, _, ok}]}, Result).

pending_allocate_insufficient_nodes_test() ->
    Data = make_job_data(#{num_nodes => 3}),
    From = fake_from(),
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_job:pending({call, From}, {allocate, Nodes}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, insufficient_nodes}}]}, Result).

pending_invalid_operation_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, invalid_operation}}]}, Result).

pending_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:pending(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

pending_state_timeout_test() ->
    Data = make_job_data(),
    Result = flurm_job:pending(state_timeout, pending_timeout, Data),
    ?assertMatch({next_state, timeout, #job_data{}}, Result).

pending_state_timeout_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    {next_state, timeout, NewData} = flurm_job:pending(state_timeout, pending_timeout, Data),
    ?assertMatch({_, _, _}, NewData#job_data.end_time).

pending_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:pending(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% configuring/3 State Callback Tests
%%====================================================================

configuring_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:configuring(enter, pending, Data),
    ?assertMatch({keep_state, #job_data{}, _}, Result).

configuring_enter_sets_timeout_test() ->
    Data = make_job_data(),
    {keep_state, _Data, Actions} = flurm_job:configuring(enter, pending, Data),
    ?assert(lists:any(fun({state_timeout, _, config_timeout}) -> true; (_) -> false end, Actions)).

configuring_get_job_id_test() ->
    Data = make_job_data(#{job_id => 88888}),
    From = fake_from(),
    Result = flurm_job:configuring({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 88888}]}, Result).

configuring_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:configuring({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

configuring_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:configuring({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, configuring}}]}, Result).

configuring_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:configuring({call, From}, cancel, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

configuring_invalid_operation_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:configuring({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, invalid_operation}}]}, Result).

configuring_config_complete_test() ->
    Data = make_job_data(#{time_limit => 3600}),
    Result = flurm_job:configuring(cast, config_complete, Data),
    ?assertMatch({next_state, running, #job_data{}, _}, Result).

configuring_config_complete_sets_start_time_test() ->
    Data = make_job_data(#{start_time => undefined, time_limit => 3600}),
    {next_state, running, NewData, _} = flurm_job:configuring(cast, config_complete, Data),
    ?assertMatch({_, _, _}, NewData#job_data.start_time).

configuring_config_complete_sets_running_timeout_test() ->
    Data = make_job_data(#{time_limit => 3600}),
    {next_state, running, _NewData, Actions} = flurm_job:configuring(cast, config_complete, Data),
    ?assert(lists:any(fun({state_timeout, 3600000, job_timeout}) -> true; (_) -> false end, Actions)).

configuring_node_failure_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>, <<"node2">>]}),
    Result = flurm_job:configuring(cast, {node_failure, <<"node1">>}, Data),
    ?assertMatch({next_state, node_fail, #job_data{}}, Result).

configuring_node_failure_not_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>, <<"node2">>]}),
    Result = flurm_job:configuring(cast, {node_failure, <<"node3">>}, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

configuring_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:configuring(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

configuring_state_timeout_test() ->
    Data = make_job_data(),
    Result = flurm_job:configuring(state_timeout, config_timeout, Data),
    ?assertMatch({next_state, failed, #job_data{}}, Result).

configuring_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:configuring(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% running/3 State Callback Tests
%%====================================================================

running_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:running(enter, configuring, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

running_get_job_id_test() ->
    Data = make_job_data(#{job_id => 77777}),
    From = fake_from(),
    Result = flurm_job:running({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 77777}]}, Result).

running_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

running_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, running}}]}, Result).

running_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, cancel, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

running_preempt_requeue_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>], start_time => erlang:timestamp()}),
    From = fake_from(),
    Result = flurm_job:running({call, From}, {preempt, requeue, 30}, Data),
    ?assertMatch({next_state, pending, #job_data{}, [{reply, _, ok}]}, Result).

running_preempt_requeue_clears_nodes_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>], start_time => erlang:timestamp()}),
    From = fake_from(),
    {next_state, pending, NewData, _} = flurm_job:running({call, From}, {preempt, requeue, 30}, Data),
    ?assertEqual([], NewData#job_data.allocated_nodes),
    ?assertEqual(undefined, NewData#job_data.start_time).

running_preempt_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, {preempt, cancel, 60}, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

running_preempt_checkpoint_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>], start_time => erlang:timestamp()}),
    From = fake_from(),
    Result = flurm_job:running({call, From}, {preempt, checkpoint, 120}, Data),
    ?assertMatch({next_state, pending, #job_data{}, [{reply, _, ok}]}, Result).

running_suspend_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, suspend, Data),
    ?assertMatch({next_state, suspended, #job_data{}, [{reply, _, ok}]}, Result).

running_set_priority_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    Result = flurm_job:running({call, From}, {set_priority, 500}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, ok}]}, Result).

running_set_priority_updates_value_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    {keep_state, NewData, _} = flurm_job:running({call, From}, {set_priority, 500}, Data),
    ?assertEqual(500, NewData#job_data.priority).

running_set_priority_clamps_high_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    {keep_state, NewData, _} = flurm_job:running({call, From}, {set_priority, 999999}, Data),
    ?assertEqual(?MAX_PRIORITY, NewData#job_data.priority).

running_set_priority_clamps_low_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    {keep_state, NewData, _} = flurm_job:running({call, From}, {set_priority, -1000}, Data),
    ?assertEqual(?MIN_PRIORITY, NewData#job_data.priority).

running_invalid_operation_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:running({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, invalid_operation}}]}, Result).

running_job_complete_test() ->
    Data = make_job_data(),
    Result = flurm_job:running(cast, {job_complete, 0}, Data),
    ?assertMatch({next_state, completing, #job_data{}}, Result).

running_job_complete_sets_exit_code_test() ->
    Data = make_job_data(),
    {next_state, completing, NewData} = flurm_job:running(cast, {job_complete, 42}, Data),
    ?assertEqual(42, NewData#job_data.exit_code).

running_job_complete_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    {next_state, completing, NewData} = flurm_job:running(cast, {job_complete, 0}, Data),
    ?assertMatch({_, _, _}, NewData#job_data.end_time).

running_node_failure_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>, <<"node2">>]}),
    Result = flurm_job:running(cast, {node_failure, <<"node1">>}, Data),
    ?assertMatch({next_state, node_fail, #job_data{}}, Result).

running_node_failure_not_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>, <<"node2">>]}),
    Result = flurm_job:running(cast, {node_failure, <<"node3">>}, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

running_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:running(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

running_state_timeout_test() ->
    Data = make_job_data(),
    Result = flurm_job:running(state_timeout, job_timeout, Data),
    ?assertMatch({next_state, timeout, #job_data{}}, Result).

running_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:running(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% suspended/3 State Callback Tests
%%====================================================================

suspended_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:suspended(enter, running, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

suspended_get_job_id_test() ->
    Data = make_job_data(#{job_id => 66666}),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 66666}]}, Result).

suspended_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

suspended_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, suspended}}]}, Result).

suspended_resume_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, resume, Data),
    ?assertMatch({next_state, running, #job_data{}, [{reply, _, ok}]}, Result).

suspended_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, cancel, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

suspended_preempt_requeue_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>], start_time => erlang:timestamp()}),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, {preempt, requeue, 30}, Data),
    ?assertMatch({next_state, pending, #job_data{}, [{reply, _, ok}]}, Result).

suspended_preempt_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, {preempt, cancel, 60}, Data),
    ?assertMatch({next_state, cancelled, #job_data{}, [{reply, _, ok}]}, Result).

suspended_set_priority_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    {keep_state, NewData, _} = flurm_job:suspended({call, From}, {set_priority, 200}, Data),
    ?assertEqual(200, NewData#job_data.priority).

suspended_set_priority_clamps_test() ->
    Data = make_job_data(#{priority => 100}),
    From = fake_from(),
    {keep_state, NewData, _} = flurm_job:suspended({call, From}, {set_priority, 99999}, Data),
    ?assertEqual(?MAX_PRIORITY, NewData#job_data.priority).

suspended_invalid_operation_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:suspended({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, invalid_operation}}]}, Result).

suspended_node_failure_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>]}),
    Result = flurm_job:suspended(cast, {node_failure, <<"node1">>}, Data),
    ?assertMatch({next_state, node_fail, #job_data{}}, Result).

suspended_node_failure_not_allocated_test() ->
    Data = make_job_data(#{allocated_nodes => [<<"node1">>]}),
    Result = flurm_job:suspended(cast, {node_failure, <<"node2">>}, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

suspended_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:suspended(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

suspended_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:suspended(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% completing/3 State Callback Tests
%%====================================================================

completing_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:completing(enter, running, Data),
    ?assertMatch({keep_state, #job_data{}, _}, Result).

completing_enter_sets_timeout_test() ->
    Data = make_job_data(),
    {keep_state, _Data, Actions} = flurm_job:completing(enter, running, Data),
    ?assert(lists:any(fun({state_timeout, _, cleanup_timeout}) -> true; (_) -> false end, Actions)).

completing_get_job_id_test() ->
    Data = make_job_data(#{job_id => 55555}),
    From = fake_from(),
    Result = flurm_job:completing({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 55555}]}, Result).

completing_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completing({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

completing_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completing({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, completing}}]}, Result).

completing_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completing({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, ok}]}, Result).

completing_invalid_operation_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completing({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, invalid_operation}}]}, Result).

completing_cleanup_complete_exit_0_test() ->
    Data = make_job_data(#{exit_code => 0}),
    Result = flurm_job:completing(cast, cleanup_complete, Data),
    ?assertMatch({next_state, completed, #job_data{}}, Result).

completing_cleanup_complete_exit_nonzero_test() ->
    Data = make_job_data(#{exit_code => 1}),
    Result = flurm_job:completing(cast, cleanup_complete, Data),
    ?assertMatch({next_state, failed, #job_data{}}, Result).

completing_cleanup_complete_exit_negative_test() ->
    Data = make_job_data(#{exit_code => -1}),
    Result = flurm_job:completing(cast, cleanup_complete, Data),
    ?assertMatch({next_state, failed, #job_data{}}, Result).

completing_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:completing(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

completing_state_timeout_exit_0_test() ->
    Data = make_job_data(#{exit_code => 0}),
    Result = flurm_job:completing(state_timeout, cleanup_timeout, Data),
    ?assertMatch({next_state, completed, #job_data{}}, Result).

completing_state_timeout_exit_nonzero_test() ->
    Data = make_job_data(#{exit_code => 127}),
    Result = flurm_job:completing(state_timeout, cleanup_timeout, Data),
    ?assertMatch({next_state, failed, #job_data{}}, Result).

completing_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:completing(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% completed/3 State Callback Tests (Terminal State)
%%====================================================================

completed_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:completed(enter, completing, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

completed_enter_with_undefined_exit_code_test() ->
    Data = make_job_data(#{exit_code => undefined}),
    Result = flurm_job:completed(enter, completing, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

completed_get_job_id_test() ->
    Data = make_job_data(#{job_id => 44444}),
    From = fake_from(),
    Result = flurm_job:completed({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 44444}]}, Result).

completed_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completed({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

completed_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completed({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, completed}}]}, Result).

completed_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completed({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, already_completed}}]}, Result).

completed_other_call_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:completed({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, job_completed}}]}, Result).

completed_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:completed(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

completed_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:completed(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% cancelled/3 State Callback Tests (Terminal State)
%%====================================================================

cancelled_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:cancelled(enter, pending, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

cancelled_get_job_id_test() ->
    Data = make_job_data(#{job_id => 33333}),
    From = fake_from(),
    Result = flurm_job:cancelled({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 33333}]}, Result).

cancelled_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:cancelled({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

cancelled_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:cancelled({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, cancelled}}]}, Result).

cancelled_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:cancelled({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, already_cancelled}}]}, Result).

cancelled_other_call_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:cancelled({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, job_cancelled}}]}, Result).

cancelled_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:cancelled(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

cancelled_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:cancelled(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% failed/3 State Callback Tests (Terminal State)
%%====================================================================

failed_enter_test() ->
    Data = make_job_data(#{exit_code => 1}),
    Result = flurm_job:failed(enter, completing, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

failed_enter_with_undefined_exit_code_test() ->
    Data = make_job_data(#{exit_code => undefined}),
    Result = flurm_job:failed(enter, completing, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

failed_get_job_id_test() ->
    Data = make_job_data(#{job_id => 22222}),
    From = fake_from(),
    Result = flurm_job:failed({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 22222}]}, Result).

failed_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:failed({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

failed_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:failed({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, failed}}]}, Result).

failed_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:failed({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, already_failed}}]}, Result).

failed_other_call_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:failed({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, job_failed}}]}, Result).

failed_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:failed(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

failed_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:failed(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% timeout/3 State Callback Tests (Terminal State)
%%====================================================================

timeout_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:timeout(enter, running, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

timeout_get_job_id_test() ->
    Data = make_job_data(#{job_id => 11111}),
    From = fake_from(),
    Result = flurm_job:timeout({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 11111}]}, Result).

timeout_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:timeout({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

timeout_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:timeout({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, timeout}}]}, Result).

timeout_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:timeout({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, already_timed_out}}]}, Result).

timeout_other_call_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:timeout({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, job_timed_out}}]}, Result).

timeout_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:timeout(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

timeout_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:timeout(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% node_fail/3 State Callback Tests (Terminal State)
%%====================================================================

node_fail_enter_test() ->
    Data = make_job_data(),
    Result = flurm_job:node_fail(enter, running, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

node_fail_get_job_id_test() ->
    Data = make_job_data(#{job_id => 10101}),
    From = fake_from(),
    Result = flurm_job:node_fail({call, From}, get_job_id, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, 10101}]}, Result).

node_fail_get_info_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:node_fail({call, From}, get_info, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, _}}]}, Result).

node_fail_get_state_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:node_fail({call, From}, get_state, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {ok, node_fail}}]}, Result).

node_fail_cancel_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:node_fail({call, From}, cancel, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, already_failed}}]}, Result).

node_fail_other_call_test() ->
    Data = make_job_data(),
    From = fake_from(),
    Result = flurm_job:node_fail({call, From}, {unknown_call, args}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, node_failure}}]}, Result).

node_fail_cast_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:node_fail(cast, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

node_fail_info_ignored_test() ->
    Data = make_job_data(),
    Result = flurm_job:node_fail(info, random_message, Data),
    ?assertMatch({keep_state, #job_data{}}, Result).

%%====================================================================
%% build_job_info Tests (via get_info calls)
%%====================================================================

build_job_info_complete_test() ->
    Data = make_job_data(#{
        job_id => 999,
        user_id => 2001,
        group_id => 3001,
        partition => <<"compute">>,
        num_nodes => 5,
        num_cpus => 40,
        time_limit => 7200,
        script => <<"#!/bin/bash\necho test">>,
        allocated_nodes => [<<"n1">>, <<"n2">>],
        priority => 500
    }),
    From = fake_from(),
    {keep_state, _, [{reply, _, {ok, Info}}]} = flurm_job:running({call, From}, get_info, Data),
    ?assertEqual(999, maps:get(job_id, Info)),
    ?assertEqual(2001, maps:get(user_id, Info)),
    ?assertEqual(3001, maps:get(group_id, Info)),
    ?assertEqual(<<"compute">>, maps:get(partition, Info)),
    ?assertEqual(running, maps:get(state, Info)),
    ?assertEqual(5, maps:get(num_nodes, Info)),
    ?assertEqual(40, maps:get(num_cpus, Info)),
    ?assertEqual(7200, maps:get(time_limit, Info)),
    ?assertEqual(<<"#!/bin/bash\necho test">>, maps:get(script, Info)),
    ?assertEqual([<<"n1">>, <<"n2">>], maps:get(allocated_nodes, Info)),
    ?assertEqual(500, maps:get(priority, Info)).

%%====================================================================
%% Edge Case Tests
%%====================================================================

allocate_empty_nodes_list_test() ->
    Data = make_job_data(#{num_nodes => 1}),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, {allocate, []}, Data),
    ?assertMatch({keep_state, #job_data{}, [{reply, _, {error, insufficient_nodes}}]}, Result).

allocate_single_node_when_one_required_test() ->
    Data = make_job_data(#{num_nodes => 1}),
    From = fake_from(),
    Result = flurm_job:pending({call, From}, {allocate, [<<"node1">>]}, Data),
    ?assertMatch({next_state, configuring, #job_data{}, [{reply, _, ok}]}, Result).

multiple_job_ids_are_unique_test() ->
    JobSpec = make_job_spec(),
    {ok, _, Data1} = flurm_job:init([JobSpec]),
    {ok, _, Data2} = flurm_job:init([JobSpec]),
    {ok, _, Data3} = flurm_job:init([JobSpec]),
    ?assertNotEqual(Data1#job_data.job_id, Data2#job_data.job_id),
    ?assertNotEqual(Data2#job_data.job_id, Data3#job_data.job_id),
    ?assertNotEqual(Data1#job_data.job_id, Data3#job_data.job_id).

submit_time_is_set_test() ->
    JobSpec = make_job_spec(),
    {ok, _, Data} = flurm_job:init([JobSpec]),
    ?assertMatch({_, _, _}, Data#job_data.submit_time),
    {MegaSecs, Secs, MicroSecs} = Data#job_data.submit_time,
    ?assert(is_integer(MegaSecs)),
    ?assert(is_integer(Secs)),
    ?assert(is_integer(MicroSecs)).

%% Test state transitions set end_time
cancel_from_pending_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:pending({call, From}, cancel, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

cancel_from_configuring_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:configuring({call, From}, cancel, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

cancel_from_running_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:running({call, From}, cancel, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

cancel_from_suspended_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:suspended({call, From}, cancel, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

node_failure_sets_end_time_configuring_test() ->
    Data = make_job_data(#{end_time => undefined, allocated_nodes => [<<"node1">>]}),
    {next_state, node_fail, NewData} = flurm_job:configuring(cast, {node_failure, <<"node1">>}, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

node_failure_sets_end_time_running_test() ->
    Data = make_job_data(#{end_time => undefined, allocated_nodes => [<<"node1">>]}),
    {next_state, node_fail, NewData} = flurm_job:running(cast, {node_failure, <<"node1">>}, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

node_failure_sets_end_time_suspended_test() ->
    Data = make_job_data(#{end_time => undefined, allocated_nodes => [<<"node1">>]}),
    {next_state, node_fail, NewData} = flurm_job:suspended(cast, {node_failure, <<"node1">>}, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

preempt_cancel_sets_end_time_running_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:running({call, From}, {preempt, cancel, 30}, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

preempt_cancel_sets_end_time_suspended_test() ->
    Data = make_job_data(#{end_time => undefined}),
    From = fake_from(),
    {next_state, cancelled, NewData, _} = flurm_job:suspended({call, From}, {preempt, cancel, 30}, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

config_timeout_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    {next_state, failed, NewData} = flurm_job:configuring(state_timeout, config_timeout, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).

running_timeout_sets_end_time_test() ->
    Data = make_job_data(#{end_time => undefined}),
    {next_state, timeout, NewData} = flurm_job:running(state_timeout, job_timeout, Data),
    ?assertNotEqual(undefined, NewData#job_data.end_time).
