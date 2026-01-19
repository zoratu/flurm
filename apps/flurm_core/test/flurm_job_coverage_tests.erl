%%%-------------------------------------------------------------------
%%% @doc FLURM Job Coverage Tests
%%%
%%% Comprehensive EUnit tests specifically designed to maximize code
%%% coverage for the flurm_job gen_statem module. Tests all states,
%%% transitions, callbacks, and error paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start sasl if needed
    application:ensure_all_started(sasl),

    %% Kill any lingering flurm_job processes from previous tests
    kill_all_flurm_job_processes(),

    %% Clean up any existing mocks
    catch meck:unload(flurm_job_registry),
    catch meck:unload(flurm_job_dispatcher),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_node_manager),

    %% Start meck for mocking external dependencies
    meck:new(flurm_job_registry, [passthrough, no_link]),
    meck:new(flurm_job_dispatcher, [passthrough, no_link]),
    meck:new(flurm_metrics, [non_strict, no_link]),
    meck:new(flurm_scheduler, [non_strict, no_link]),
    meck:new(flurm_node_manager, [non_strict, no_link]),

    %% Default mocks
    meck:expect(flurm_job_registry, register_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_registry, unregister_job, fun(_) -> ok end),
    meck:expect(flurm_job_registry, update_state, fun(_, _) -> ok end),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher, cancel_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher, requeue_job, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, gauge, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    meck:expect(flurm_scheduler, job_completed, fun(_) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_) -> ok end),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    ok.

cleanup(_) ->
    %% Kill any flurm_job processes started during the test
    kill_all_flurm_job_processes(),

    %% Unload all mocks
    catch meck:unload(flurm_job_registry),
    catch meck:unload(flurm_job_dispatcher),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_node_manager),
    ok.

%% @doc Kill all processes running the flurm_job module
kill_all_flurm_job_processes() ->
    lists:foreach(
        fun(Pid) ->
            case erlang:process_info(Pid, [dictionary]) of
                [{dictionary, Dict}] ->
                    case proplists:get_value('$initial_call', Dict) of
                        {flurm_job, _, _} ->
                            catch gen_statem:stop(Pid, shutdown, 100);
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end
        end,
        erlang:processes()),
    %% Also try to find processes by their registered module in sys info
    lists:foreach(
        fun(Pid) ->
            try
                case sys:get_status(Pid, 50) of
                    {status, _, {module, gen_statem}, [_, _, _, _, [_, {data, Data} | _]]} ->
                        case proplists:get_value("StateName", Data) of
                            State when State =/= undefined ->
                                %% Likely a gen_statem, check if it's flurm_job
                                catch gen_statem:stop(Pid, shutdown, 100);
                            _ ->
                                ok
                        end;
                    _ ->
                        ok
                end
            catch
                _:_ -> ok
            end
        end,
        erlang:processes()),
    ok.

%% @doc Helper to safely stop a process
safe_stop(Pid) when is_pid(Pid) ->
    catch gen_statem:stop(Pid, shutdown, 1000),
    ok;
safe_stop(_) ->
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

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

%%====================================================================
%% Test Fixtures
%%====================================================================

job_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates job", fun test_start_link/0},
        {"submit creates and registers job", fun test_submit/0},
        {"callback_mode returns correct mode", fun test_callback_mode/0},
        {"init with valid spec", fun test_init/0},
        {"init with undefined priority", fun test_init_undefined_priority/0},
        {"init with clamped priority", fun test_init_clamped_priority/0}
     ]}.

%%====================================================================
%% Basic Tests
%%====================================================================

test_start_link() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    {ok, State} = flurm_job:get_state(Pid),
    ?assertEqual(pending, State),

    safe_stop(Pid),
    ok.

test_submit() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),

    {ok, State} = flurm_job:get_state(Pid),
    ?assertEqual(pending, State),

    safe_stop(Pid),
    ok.

test_callback_mode() ->
    Mode = flurm_job:callback_mode(),
    ?assertEqual([state_functions, state_enter], Mode),
    ok.

test_init() ->
    JobSpec = make_job_spec(#{priority => 500}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_init_undefined_priority() ->
    JobSpec = #job_spec{
        user_id = 1000,
        group_id = 1000,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"test">>,
        priority = undefined
    },
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(?DEFAULT_PRIORITY, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_init_clamped_priority() ->
    %% Test max clamping
    JobSpec1 = make_job_spec(#{priority => 999999}),
    {ok, Pid1} = flurm_job:start_link(JobSpec1),
    {ok, Info1} = flurm_job:get_info(Pid1),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info1)),
    safe_stop(Pid1),

    %% Test min clamping
    JobSpec2 = make_job_spec(#{priority => -100}),
    {ok, Pid2} = flurm_job:start_link(JobSpec2),
    {ok, Info2} = flurm_job:get_info(Pid2),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info2)),
    safe_stop(Pid2),
    ok.

%%====================================================================
%% Pending State Tests
%%====================================================================

pending_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"pending state enter action", fun test_pending_enter/0},
        {"pending get_info", fun test_pending_get_info/0},
        {"pending get_state", fun test_pending_get_state/0},
        {"pending get_job_id", fun test_pending_get_job_id/0},
        {"pending allocate success", fun test_pending_allocate_success/0},
        {"pending allocate insufficient_nodes", fun test_pending_allocate_insufficient/0},
        {"pending cancel", fun test_pending_cancel/0},
        {"pending set_priority", fun test_pending_set_priority/0},
        {"pending invalid operation", fun test_pending_invalid_operation/0},
        {"pending ignored cast", fun test_pending_ignored_cast/0},
        {"pending ignored info", fun test_pending_ignored_info/0},
        {"pending timeout", fun test_pending_timeout/0}
     ]}.

test_pending_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_get_info() ->
    JobSpec = make_job_spec(#{user_id => 1001, partition => <<"compute">>}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(pending, maps:get(state, Info)),
    ?assertEqual(1001, maps:get(user_id, Info)),
    ?assertEqual(<<"compute">>, maps:get(partition, Info)),
    ?assertEqual([], maps:get(allocated_nodes, Info)),

    safe_stop(Pid),
    ok.

test_pending_get_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {ok, State} = flurm_job:get_state(Pid),
    ?assertEqual(pending, State),

    safe_stop(Pid),
    ok.

test_pending_get_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    Result = gen_statem:call(Pid, get_job_id),
    ?assertEqual(JobId, Result),

    safe_stop(Pid),
    ok.

test_pending_allocate_success() ->
    JobSpec = make_job_spec(#{num_nodes => 1}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_allocate_insufficient() ->
    JobSpec = make_job_spec(#{num_nodes => 3}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    {error, insufficient_nodes} = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_set_priority() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:set_priority(Pid, 500),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_pending_invalid_operation() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    %% Signal config complete in pending is invalid
    {error, invalid_operation} = gen_statem:call(Pid, signal_config_complete),

    safe_stop(Pid),
    ok.

test_pending_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    gen_statem:cast(Pid, config_complete),
    gen_statem:cast(Pid, {job_complete, 0}),
    gen_statem:cast(Pid, unknown_cast),
    timer:sleep(50),

    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    Pid ! random_message,
    Pid ! {some, tuple},
    timer:sleep(50),

    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_pending_timeout() ->
    %% We can't easily test the 24hr timeout, but we can verify
    %% the state timeout mechanism doesn't crash
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    timer:sleep(100),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Configuring State Tests
%%====================================================================

configuring_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"configuring state enter", fun test_configuring_enter/0},
        {"configuring get_info", fun test_configuring_get_info/0},
        {"configuring signal_config_complete", fun test_configuring_complete/0},
        {"configuring cancel", fun test_configuring_cancel/0},
        {"configuring node_failure allocated", fun test_configuring_node_failure_allocated/0},
        {"configuring node_failure not_allocated", fun test_configuring_node_failure_not_allocated/0},
        {"configuring invalid allocate", fun test_configuring_invalid_allocate/0},
        {"configuring set_priority", fun test_configuring_set_priority/0},
        {"configuring ignored cast", fun test_configuring_ignored_cast/0},
        {"configuring ignored info", fun test_configuring_ignored_info/0}
     ]}.

test_configuring_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_get_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(configuring, maps:get(state, Info)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(allocated_nodes, Info)),

    safe_stop(Pid),
    ok.

test_configuring_complete() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_node_failure_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_node_failure_not_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_node_failure(Pid, <<"node3">>),
    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_invalid_allocate() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node2">>]),

    safe_stop(Pid),
    ok.

test_configuring_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {error, invalid_operation} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_configuring_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    gen_statem:cast(Pid, unknown_cast),
    gen_statem:cast(Pid, {job_complete, 0}),
    timer:sleep(50),

    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_configuring_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    Pid ! random_info,
    timer:sleep(50),

    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Running State Tests
%%====================================================================

running_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"running state enter", fun test_running_enter/0},
        {"running get_info", fun test_running_get_info/0},
        {"running signal_job_complete zero exit", fun test_running_complete_zero/0},
        {"running signal_job_complete non-zero exit", fun test_running_complete_nonzero/0},
        {"running cancel", fun test_running_cancel/0},
        {"running node_failure allocated", fun test_running_node_failure_allocated/0},
        {"running node_failure not_allocated", fun test_running_node_failure_not_allocated/0},
        {"running suspend", fun test_running_suspend/0},
        {"running preempt requeue", fun test_running_preempt_requeue/0},
        {"running preempt cancel", fun test_running_preempt_cancel/0},
        {"running preempt checkpoint", fun test_running_preempt_checkpoint/0},
        {"running set_priority", fun test_running_set_priority/0},
        {"running timeout", fun test_running_timeout/0},
        {"running ignored cast", fun test_running_ignored_cast/0},
        {"running ignored info", fun test_running_ignored_info/0}
     ]}.

test_running_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_get_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(running, maps:get(state, Info)),
    ?assertNotEqual(undefined, maps:get(start_time, Info)),

    safe_stop(Pid),
    ok.

test_running_complete_zero() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_complete_nonzero() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 127),
    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_node_failure_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_node_failure_not_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"other_node">>),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_suspend() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_preempt_requeue() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:preempt(Pid, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_preempt_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:preempt(Pid, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_preempt_checkpoint() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:preempt(Pid, checkpoint, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_set_priority() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:set_priority(Pid, 500),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_running_timeout() ->
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),

    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    gen_statem:cast(Pid, unknown_cast),
    gen_statem:cast(Pid, config_complete),
    timer:sleep(50),

    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_running_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    Pid ! random_info,
    timer:sleep(50),

    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Completing State Tests
%%====================================================================

completing_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"completing state enter", fun test_completing_enter/0},
        {"completing get_info", fun test_completing_get_info/0},
        {"completing cleanup_complete to completed", fun test_completing_to_completed/0},
        {"completing cleanup_complete to failed", fun test_completing_to_failed/0},
        {"completing cancel acknowledged", fun test_completing_cancel/0},
        {"completing set_priority invalid", fun test_completing_set_priority/0},
        {"completing ignored cast", fun test_completing_ignored_cast/0},
        {"completing ignored info", fun test_completing_ignored_info/0}
     ]}.

test_completing_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completing_get_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 42),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(completing, maps:get(state, Info)),
    ?assertEqual(42, maps:get(exit_code, Info)),

    safe_stop(Pid),
    ok.

test_completing_to_completed() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completing_to_failed() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, failed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completing_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:cancel(Pid),
    %% Cancel is acknowledged but stays in completing
    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completing_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    {error, invalid_operation} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_completing_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    gen_statem:cast(Pid, unknown_cast),
    timer:sleep(50),

    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completing_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    Pid ! random_info,
    timer:sleep(50),

    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Suspended State Tests
%%====================================================================

suspended_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"suspended state enter", fun test_suspended_enter/0},
        {"suspended get_info", fun test_suspended_get_info/0},
        {"suspended resume", fun test_suspended_resume/0},
        {"suspended cancel", fun test_suspended_cancel/0},
        {"suspended preempt requeue", fun test_suspended_preempt_requeue/0},
        {"suspended preempt cancel", fun test_suspended_preempt_cancel/0},
        {"suspended node_failure allocated", fun test_suspended_node_failure_allocated/0},
        {"suspended node_failure not_allocated", fun test_suspended_node_failure_not_allocated/0},
        {"suspended set_priority", fun test_suspended_set_priority/0},
        {"suspended allocate invalid", fun test_suspended_allocate_invalid/0},
        {"suspended ignored cast", fun test_suspended_ignored_cast/0},
        {"suspended ignored info", fun test_suspended_ignored_info/0}
     ]}.

test_suspended_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_get_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(suspended, maps:get(state, Info)),

    safe_stop(Pid),
    ok.

test_suspended_resume() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:resume(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_preempt_requeue() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:preempt(Pid, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_preempt_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:preempt(Pid, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_node_failure_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_node_failure_not_allocated() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"other_node">>),
    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_set_priority() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:set_priority(Pid, 300),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(300, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_suspended_allocate_invalid() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node2">>]),

    safe_stop(Pid),
    ok.

test_suspended_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    gen_statem:cast(Pid, unknown_cast),
    timer:sleep(50),

    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspended_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    Pid ! random_info,
    timer:sleep(50),

    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Terminal State Tests
%%====================================================================

terminal_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"completed state enter", fun test_completed_enter/0},
        {"completed cancel error", fun test_completed_cancel/0},
        {"completed allocate error", fun test_completed_allocate/0},
        {"completed set_priority error", fun test_completed_set_priority/0},
        {"completed get_job_id", fun test_completed_get_job_id/0},
        {"completed ignored cast", fun test_completed_ignored_cast/0},
        {"completed ignored info", fun test_completed_ignored_info/0},

        {"failed state enter", fun test_failed_enter/0},
        {"failed cancel error", fun test_failed_cancel/0},
        {"failed suspend error", fun test_failed_suspend/0},
        {"failed set_priority error", fun test_failed_set_priority/0},

        {"cancelled state enter", fun test_cancelled_enter/0},
        {"cancelled cancel error", fun test_cancelled_cancel/0},
        {"cancelled set_priority error", fun test_cancelled_set_priority/0},

        {"timeout state enter", fun test_timeout_enter/0},
        {"timeout cancel error", fun test_timeout_cancel/0},
        {"timeout set_priority error", fun test_timeout_set_priority/0},

        {"node_fail state enter", fun test_node_fail_enter/0},
        {"node_fail cancel error", fun test_node_fail_cancel/0},
        {"node_fail resume error", fun test_node_fail_resume/0},
        {"node_fail set_priority error", fun test_node_fail_set_priority/0}
     ]}.

test_completed_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completed_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, already_completed} = flurm_job:cancel(Pid),

    safe_stop(Pid),
    ok.

test_completed_allocate() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, job_completed} = flurm_job:allocate(Pid, [<<"node2">>]),

    safe_stop(Pid),
    ok.

test_completed_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, job_completed} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_completed_get_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),

    Result = gen_statem:call(Pid, get_job_id),
    ?assertEqual(JobId, Result),

    safe_stop(Pid),
    ok.

test_completed_ignored_cast() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    gen_statem:cast(Pid, unknown_cast),
    timer:sleep(50),

    {ok, completed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_completed_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    Pid ! random_info,
    timer:sleep(50),

    {ok, completed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_failed_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, failed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_failed_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, already_failed} = flurm_job:cancel(Pid),

    safe_stop(Pid),
    ok.

test_failed_suspend() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, job_failed} = flurm_job:suspend(Pid),

    safe_stop(Pid),
    ok.

test_failed_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {error, job_failed} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_cancelled_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_cancelled_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:cancel(Pid),
    {error, already_cancelled} = flurm_job:cancel(Pid),

    safe_stop(Pid),
    ok.

test_cancelled_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:cancel(Pid),
    {error, job_cancelled} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_timeout_enter() ->
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_timeout_cancel() ->
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    timer:sleep(1500),
    {error, already_timed_out} = flurm_job:cancel(Pid),

    safe_stop(Pid),
    ok.

test_timeout_set_priority() ->
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    timer:sleep(1500),
    {error, job_timed_out} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

test_node_fail_enter() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_node_fail_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {error, already_failed} = flurm_job:cancel(Pid),

    safe_stop(Pid),
    ok.

test_node_fail_resume() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {error, node_failure} = flurm_job:resume(Pid),

    safe_stop(Pid),
    ok.

test_node_fail_set_priority() ->
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {error, node_failure} = gen_statem:call(Pid, {set_priority, 500}),

    safe_stop(Pid),
    ok.

%%====================================================================
%% Job ID API Variant Tests
%%====================================================================

job_id_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_info by job_id", fun test_get_info_by_job_id/0},
        {"get_state by job_id", fun test_get_state_by_job_id/0},
        {"allocate by job_id", fun test_allocate_by_job_id/0},
        {"cancel by job_id", fun test_cancel_by_job_id/0},
        {"signal_config_complete by job_id", fun test_signal_config_complete_by_job_id/0},
        {"signal_job_complete by job_id", fun test_signal_job_complete_by_job_id/0},
        {"signal_cleanup_complete by job_id", fun test_signal_cleanup_complete_by_job_id/0},
        {"signal_node_failure by job_id", fun test_signal_node_failure_by_job_id/0},
        {"preempt by job_id", fun test_preempt_by_job_id/0},
        {"suspend by job_id", fun test_suspend_by_job_id/0},
        {"resume by job_id", fun test_resume_by_job_id/0},
        {"set_priority by job_id", fun test_set_priority_by_job_id/0},
        {"not_found by job_id", fun test_not_found_by_job_id/0}
     ]}.

test_get_info_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    {ok, Info} = flurm_job:get_info(JobId),
    ?assertEqual(pending, maps:get(state, Info)),

    safe_stop(Pid),
    ok.

test_get_state_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    {ok, State} = flurm_job:get_state(JobId),
    ?assertEqual(pending, State),

    safe_stop(Pid),
    ok.

test_allocate_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(JobId, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_cancel_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:cancel(JobId),
    {ok, cancelled} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_signal_config_complete_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(JobId),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_signal_job_complete_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(JobId, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_signal_cleanup_complete_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(JobId),
    {ok, completed} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_signal_node_failure_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(JobId, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_preempt_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:preempt(JobId, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_suspend_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(JobId),
    {ok, suspended} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_resume_by_job_id() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:resume(JobId),
    {ok, running} = flurm_job:get_state(Pid),

    safe_stop(Pid),
    ok.

test_set_priority_by_job_id() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    meck:expect(flurm_job_registry, lookup_job, fun(Id) when Id =:= JobId -> {ok, Pid} end),

    ok = flurm_job:set_priority(JobId, 500),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_not_found_by_job_id() ->
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    {error, job_not_found} = flurm_job:get_info(999999),
    {error, job_not_found} = flurm_job:get_state(999999),
    {error, job_not_found} = flurm_job:cancel(999999),
    {error, job_not_found} = flurm_job:allocate(999999, [<<"node1">>]),
    {error, job_not_found} = flurm_job:signal_config_complete(999999),
    {error, job_not_found} = flurm_job:signal_job_complete(999999, 0),
    {error, job_not_found} = flurm_job:signal_cleanup_complete(999999),
    {error, job_not_found} = flurm_job:signal_node_failure(999999, <<"node1">>),
    {error, job_not_found} = flurm_job:preempt(999999, requeue, 30),
    {error, job_not_found} = flurm_job:suspend(999999),
    {error, job_not_found} = flurm_job:resume(999999),
    {error, job_not_found} = flurm_job:set_priority(999999, 100),
    ok.

%%====================================================================
%% Priority Clamping Tests
%%====================================================================

priority_clamping_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_priority clamps to max", fun test_set_priority_clamps_max/0},
        {"set_priority clamps to min", fun test_set_priority_clamps_min/0}
     ]}.

test_set_priority_clamps_max() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:set_priority(Pid, 999999),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.

test_set_priority_clamps_min() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid} = flurm_job:start_link(JobSpec),

    ok = flurm_job:set_priority(Pid, -500),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info)),

    safe_stop(Pid),
    ok.
