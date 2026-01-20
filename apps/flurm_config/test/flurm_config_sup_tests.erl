%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_config_sup supervisor module
%%%
%%% Comprehensive EUnit tests covering:
%%% - Supervisor start_link
%%% - Supervisor init callback
%%% - Supervisor flags verification
%%% - Child spec verification
%%% - Supervision tree behavior
%%% - Child process management
%%% - Restart strategy testing
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing supervisor
    case whereis(flurm_config_sup) of
        undefined -> ok;
        Pid -> flurm_test_utils:kill_and_wait(Pid)
    end,
    %% Also stop config server if running
    case whereis(flurm_config_server) of
        undefined -> ok;
        ServerPid -> flurm_test_utils:kill_and_wait(ServerPid)
    end,
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_config_sup) of
        undefined -> ok;
        Pid -> flurm_test_utils:kill_and_wait(Pid)
    end,
    %% Also stop config server if running separately
    case whereis(flurm_config_server) of
        undefined -> ok;
        ServerPid -> flurm_test_utils:kill_and_wait(ServerPid)
    end,
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

config_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Basic supervisor tests
      {"start_link starts supervisor", fun test_start_link/0},
      {"supervisor registers with correct name", fun test_registered_name/0},
      {"supervisor starts with one_for_one strategy", fun test_strategy/0},
      {"supervisor has correct intensity", fun test_intensity/0},
      {"supervisor has correct period", fun test_period/0},

      %% Child spec tests
      {"supervisor has one child", fun test_child_count/0},
      {"child is flurm_config_server", fun test_child_id/0},
      {"child has correct start spec", fun test_child_start/0},
      {"child has permanent restart", fun test_child_restart/0},
      {"child has correct shutdown", fun test_child_shutdown/0},
      {"child is worker type", fun test_child_type/0},
      {"child has correct modules", fun test_child_modules/0},

      %% Behavior tests
      {"supervisor starts child process", fun test_starts_child/0},
      {"config_server is running after start", fun test_config_server_running/0},
      {"supervisor restarts crashed child", fun test_restart_crashed_child/0},

      %% init/1 callback tests
      {"init returns correct structure", fun test_init_return_structure/0},
      {"init sup_flags has all required keys", fun test_init_sup_flags_keys/0},
      {"init children is a list", fun test_init_children_list/0}
     ]}.

%%====================================================================
%% Basic Supervisor Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_config_sup:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

test_registered_name() ->
    {ok, _Pid} = flurm_config_sup:start_link(),
    ?assertNotEqual(undefined, whereis(flurm_config_sup)).

test_strategy() ->
    {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)).

test_intensity() ->
    {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
    Intensity = maps:get(intensity, SupFlags),
    ?assertEqual(5, Intensity),
    ?assert(is_integer(Intensity)),
    ?assert(Intensity > 0).

test_period() ->
    {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
    Period = maps:get(period, SupFlags),
    ?assertEqual(10, Period),
    ?assert(is_integer(Period)),
    ?assert(Period > 0).

%%====================================================================
%% Child Spec Tests
%%====================================================================

test_child_count() ->
    {ok, {_SupFlags, Children}} = flurm_config_sup:init([]),
    ?assertEqual(1, length(Children)).

test_child_id() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    ?assertEqual(flurm_config_server, maps:get(id, Child)).

test_child_start() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    StartSpec = maps:get(start, Child),
    ?assertEqual({flurm_config_server, start_link, []}, StartSpec).

test_child_restart() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    ?assertEqual(permanent, maps:get(restart, Child)).

test_child_shutdown() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    Shutdown = maps:get(shutdown, Child),
    ?assertEqual(5000, Shutdown),
    ?assert(is_integer(Shutdown)),
    ?assert(Shutdown > 0).

test_child_type() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    ?assertEqual(worker, maps:get(type, Child)).

test_child_modules() ->
    {ok, {_SupFlags, [Child | _]}} = flurm_config_sup:init([]),
    ?assertEqual([flurm_config_server], maps:get(modules, Child)).

%%====================================================================
%% Behavior Tests
%%====================================================================

test_starts_child() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    %% Wait for child to be registered
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),
    Children = supervisor:which_children(SupPid),
    ?assertEqual(1, length(Children)),
    [{Id, ChildPid, Type, Modules}] = Children,
    ?assertEqual(flurm_config_server, Id),
    ?assert(is_pid(ChildPid)),
    ?assertEqual(worker, Type),
    ?assertEqual([flurm_config_server], Modules).

test_config_server_running() ->
    {ok, _SupPid} = flurm_config_sup:start_link(),
    ServerPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(ServerPid)),
    ?assert(is_process_alive(ServerPid)).

test_restart_crashed_child() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    OriginalPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(OriginalPid)),

    %% Crash the child and wait for death
    exit(OriginalPid, kill),
    flurm_test_utils:wait_for_death(OriginalPid),

    %% Wait for supervisor to restart the child
    NewPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(NewPid)),
    ?assert(is_process_alive(NewPid)),
    ?assertNotEqual(OriginalPid, NewPid),

    %% Supervisor should still be alive
    ?assert(is_process_alive(SupPid)).

%%====================================================================
%% init/1 Callback Tests
%%====================================================================

test_init_return_structure() ->
    Result = flurm_config_sup:init([]),
    ?assertMatch({ok, {_SupFlags, _Children}}, Result),
    {ok, {SupFlags, Children}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(Children)).

test_init_sup_flags_keys() ->
    {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
    ?assert(maps:is_key(strategy, SupFlags)),
    ?assert(maps:is_key(intensity, SupFlags)),
    ?assert(maps:is_key(period, SupFlags)).

test_init_children_list() ->
    {ok, {_SupFlags, Children}} = flurm_config_sup:init([]),
    ?assert(is_list(Children)),
    %% Each child should be a map with required keys
    lists:foreach(fun(Child) ->
        ?assert(is_map(Child)),
        ?assert(maps:is_key(id, Child)),
        ?assert(maps:is_key(start, Child)),
        ?assert(maps:is_key(restart, Child)),
        ?assert(maps:is_key(shutdown, Child)),
        ?assert(maps:is_key(type, Child)),
        ?assert(maps:is_key(modules, Child))
    end, Children).

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"supervisor manages child lifecycle", fun test_child_lifecycle/0},
      {"multiple restarts within period succeed", fun test_multiple_restarts/0},
      {"which_children returns correct info", fun test_which_children/0},
      {"count_children returns correct counts", fun test_count_children/0}
     ]}.

test_child_lifecycle() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% Child should be running
    ChildPid1 = whereis(flurm_config_server),
    ?assert(is_process_alive(ChildPid1)),

    %% Stop child gracefully via supervisor
    ok = supervisor:terminate_child(SupPid, flurm_config_server),
    flurm_test_utils:wait_for_unregistered(flurm_config_server),

    %% Child should be stopped but can be restarted
    ?assertEqual(undefined, whereis(flurm_config_server)),

    %% Restart child
    {ok, ChildPid2} = supervisor:restart_child(SupPid, flurm_config_server),
    ?assert(is_process_alive(ChildPid2)).

test_multiple_restarts() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% Crash child multiple times (but less than intensity)
    lists:foreach(fun(_) ->
        Pid = flurm_test_utils:wait_for_registered(flurm_config_server),
        ?assert(is_pid(Pid)),
        exit(Pid, kill),
        flurm_test_utils:wait_for_death(Pid)
    end, lists:seq(1, 3)),

    %% Supervisor should still be alive
    ?assert(is_process_alive(SupPid)),

    %% Child should be restarted
    FinalPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(FinalPid)),
    ?assert(is_process_alive(FinalPid)).

test_which_children() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    Children = supervisor:which_children(SupPid),
    ?assertEqual(1, length(Children)),

    [{Id, Pid, Type, Modules}] = Children,
    ?assertEqual(flurm_config_server, Id),
    ?assert(is_pid(Pid)),
    ?assertEqual(worker, Type),
    ?assertEqual([flurm_config_server], Modules).

test_count_children() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    Counts = supervisor:count_children(SupPid),
    ?assertEqual(1, proplists:get_value(specs, Counts)),
    ?assertEqual(1, proplists:get_value(active, Counts)),
    ?assertEqual(0, proplists:get_value(supervisors, Counts)),
    ?assertEqual(1, proplists:get_value(workers, Counts)).

%%====================================================================
%% Supervisor API Tests
%%====================================================================

supervisor_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_childspec returns child spec", fun test_get_childspec/0},
      {"delete_child fails for active child", fun test_delete_active_child/0},
      {"delete_child succeeds after terminate", fun test_delete_terminated_child/0}
     ]}.

test_get_childspec() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    {ok, ChildSpec} = supervisor:get_childspec(SupPid, flurm_config_server),
    ?assert(is_map(ChildSpec)),
    ?assertEqual(flurm_config_server, maps:get(id, ChildSpec)).

test_delete_active_child() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% Cannot delete an active child
    Result = supervisor:delete_child(SupPid, flurm_config_server),
    ?assertEqual({error, running}, Result).

test_delete_terminated_child() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% First terminate the child
    ok = supervisor:terminate_child(SupPid, flurm_config_server),
    flurm_test_utils:wait_for_unregistered(flurm_config_server),

    %% Now delete should succeed
    Result = supervisor:delete_child(SupPid, flurm_config_server),
    ?assertEqual(ok, Result),

    %% Child should not be in the list anymore
    Children = supervisor:which_children(SupPid),
    ?assertEqual([], Children).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link when already running returns error", fun test_start_when_running/0},
      {"supervisor survives child exit with reason normal", fun test_normal_exit/0},
      {"supervisor survives child exit with reason shutdown", fun test_shutdown_exit/0}
     ]}.

test_start_when_running() ->
    {ok, Pid1} = flurm_config_sup:start_link(),
    ?assert(is_process_alive(Pid1)),

    %% Try to start again - should return error since name is registered
    Result = flurm_config_sup:start_link(),
    ?assertMatch({error, {already_started, Pid1}}, Result).

test_normal_exit() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% Exit child with normal reason using gen_server:stop (exit/2 with normal is ignored by trap_exit)
    ChildPid = whereis(flurm_config_server),
    gen_server:stop(ChildPid, normal, 5000),

    %% Supervisor should still be alive
    ?assert(is_process_alive(SupPid)),

    %% Child should be restarted (permanent restart)
    NewChildPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(NewChildPid)).

test_shutdown_exit() ->
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    %% Exit child with shutdown reason using gen_server:stop
    ChildPid = whereis(flurm_config_server),
    gen_server:stop(ChildPid, shutdown, 5000),

    %% Supervisor should still be alive
    ?assert(is_process_alive(SupPid)),

    %% Child should be restarted (permanent restart)
    NewChildPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assert(is_pid(NewChildPid)).

%%====================================================================
%% Strategy Verification Tests
%%====================================================================

strategy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"one_for_one does not affect other children", fun test_one_for_one_isolation/0}
     ]}.

test_one_for_one_isolation() ->
    %% This test verifies that when a child crashes, only that child restarts
    %% Since we only have one child, we just verify the strategy is set correctly
    {ok, {SupFlags, _Children}} = flurm_config_sup:init([]),
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),

    %% Start supervisor and verify behavior
    {ok, SupPid} = flurm_config_sup:start_link(),
    _ = flurm_test_utils:wait_for_registered(flurm_config_server),

    OriginalPid = whereis(flurm_config_server),
    exit(OriginalPid, kill),
    flurm_test_utils:wait_for_death(OriginalPid),

    %% Only the config_server should have restarted
    %% (the supervisor should not have restarted)
    ?assert(is_process_alive(SupPid)),
    NewPid = flurm_test_utils:wait_for_registered(flurm_config_server),
    ?assertNotEqual(OriginalPid, NewPid),
    ?assert(is_process_alive(NewPid)).

%%====================================================================
%% Module Attribute Tests
%%====================================================================

module_test_() ->
    [
     {"module exports start_link/0", fun test_exports_start_link/0},
     {"module exports init/1", fun test_exports_init/0},
     {"module implements supervisor behaviour", fun test_implements_supervisor/0}
    ].

test_exports_start_link() ->
    Exports = flurm_config_sup:module_info(exports),
    ?assert(lists:member({start_link, 0}, Exports)).

test_exports_init() ->
    Exports = flurm_config_sup:module_info(exports),
    ?assert(lists:member({init, 1}, Exports)).

test_implements_supervisor() ->
    %% Check that the module behaves as a supervisor
    %% by verifying init/1 returns the correct format
    Result = flurm_config_sup:init([]),
    ?assertMatch({ok, {#{strategy := _, intensity := _, period := _}, _}}, Result).
