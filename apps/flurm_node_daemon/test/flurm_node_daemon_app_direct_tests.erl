%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_node_daemon_app module
%%%
%%% Tests the application callbacks directly. Mocks external modules
%%% but not the module being tested.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_start_no_saved_state/1,
      fun test_start_with_saved_state_no_jobs/1,
      fun test_start_with_orphaned_jobs/1,
      fun test_prep_stop_saves_state/1,
      fun test_stop_returns_ok/1
     ]}.

setup() ->
    %% Set required application environment
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6817),

    %% Mock external dependencies
    meck:new(flurm_state_persistence, [passthrough, non_strict]),
    meck:new(flurm_node_daemon_sup, [passthrough, non_strict]),
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_system_monitor, [passthrough, non_strict]),

    %% Start lager for logging
    catch application:start(lager),
    ok.

cleanup(_) ->
    meck:unload(flurm_state_persistence),
    meck:unload(flurm_node_daemon_sup),
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),

    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_no_saved_state(_) ->
    {"start/2 works when no saved state exists",
     fun() ->
         %% Setup mocks
         meck:expect(flurm_state_persistence, load_state, fun() ->
             {error, not_found}
         end),
         meck:expect(flurm_node_daemon_sup, start_link, fun() ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),

         %% Call start
         Result = flurm_node_daemon_app:start(normal, []),

         ?assertMatch({ok, _Pid}, Result),
         ?assert(meck:called(flurm_state_persistence, load_state, [])),
         ?assert(meck:called(flurm_node_daemon_sup, start_link, []))
     end}.

test_start_with_saved_state_no_jobs(_) ->
    {"start/2 handles saved state with no orphaned jobs",
     fun() ->
         SavedState = #{
             saved_at => erlang:system_time(millisecond) - 1000,
             running_jobs => 0,
             gpu_allocation => #{}
         },

         meck:expect(flurm_state_persistence, load_state, fun() ->
             {ok, SavedState}
         end),
         meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),
         meck:expect(flurm_node_daemon_sup, start_link, fun() ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),

         Result = flurm_node_daemon_app:start(normal, []),

         ?assertMatch({ok, _Pid}, Result),
         ?assert(meck:called(flurm_state_persistence, clear_state, []))
     end}.

test_start_with_orphaned_jobs(_) ->
    {"start/2 cleans up orphaned jobs from previous run",
     fun() ->
         SavedState = #{
             saved_at => erlang:system_time(millisecond) - 5000,
             running_jobs => 3,
             gpu_allocation => #{0 => 1001, 1 => 1001}
         },

         meck:expect(flurm_state_persistence, load_state, fun() ->
             {ok, SavedState}
         end),
         meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),
         meck:expect(flurm_node_daemon_sup, start_link, fun() ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),

         Result = flurm_node_daemon_app:start(normal, []),

         ?assertMatch({ok, _Pid}, Result),
         %% The orphaned job cleanup happens internally
         ?assert(meck:called(flurm_state_persistence, clear_state, []))
     end}.

test_prep_stop_saves_state(_) ->
    {"prep_stop/1 saves current state before stopping",
     fun() ->
         %% Setup connector state mock
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{running_jobs => 2, draining => false, drain_reason => undefined}
         end),
         meck:expect(flurm_system_monitor, get_gpu_allocation, fun() ->
             #{0 => 1001}
         end),
         meck:expect(flurm_state_persistence, save_state, fun(State) ->
             %% Verify state contains expected fields
             ?assert(maps:is_key(running_jobs, State)),
             ?assert(maps:is_key(gpu_allocation, State)),
             ok
         end),

         %% Call prep_stop
         TestState = some_state,
         Result = flurm_node_daemon_app:prep_stop(TestState),

         %% Should return the state unchanged
         ?assertEqual(TestState, Result),
         ?assert(meck:called(flurm_state_persistence, save_state, ['_']))
     end}.

test_stop_returns_ok(_) ->
    {"stop/1 returns ok",
     fun() ->
         Result = flurm_node_daemon_app:stop(some_state),
         ?assertEqual(ok, Result)
     end}.

%%====================================================================
%% Additional coverage tests for internal functions
%%====================================================================

internal_cleanup_test_() ->
    {setup,
     fun setup_cleanup_tests/0,
     fun cleanup_cleanup_tests/1,
     [
      {"save_current_state handles errors gracefully", fun test_save_state_error_handling/0}
     ]}.

setup_cleanup_tests() ->
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6817),
    meck:new(flurm_state_persistence, [passthrough, non_strict]),
    meck:new(flurm_node_daemon_sup, [passthrough, non_strict]),
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_system_monitor, [passthrough, non_strict]),
    catch application:start(lager),
    ok.

cleanup_cleanup_tests(_) ->
    meck:unload(flurm_state_persistence),
    meck:unload(flurm_node_daemon_sup),
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),
    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

test_save_state_error_handling() ->
    %% Test that prep_stop handles errors in save_state gracefully
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 1}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) ->
        {error, eacces}
    end),

    %% Should not crash, should return state
    Result = flurm_node_daemon_app:prep_stop(test_state),
    ?assertEqual(test_state, Result).
