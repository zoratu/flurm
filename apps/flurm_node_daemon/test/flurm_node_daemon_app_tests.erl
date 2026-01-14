%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Application Tests
%%%
%%% Comprehensive EUnit tests for the flurm_node_daemon_app module.
%%% Tests application callbacks, state persistence handling,
%%% and cleanup operations.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),

    %% Set required environment variables
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6817),

    %% Create mocks for dependencies
    meck:new(flurm_state_persistence, [non_strict]),
    meck:new(flurm_node_daemon_sup, [non_strict]),
    meck:new(flurm_controller_connector, [non_strict]),
    meck:new(flurm_system_monitor, [non_strict]),

    ok.

cleanup(_) ->
    %% Unload mocks
    meck:unload(flurm_state_persistence),
    meck:unload(flurm_node_daemon_sup),
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),

    %% Unset environment
    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module_info/0 returns module information", fun() ->
            Info = flurm_node_daemon_app:module_info(),
            ?assert(is_list(Info)),
            ?assertMatch({module, flurm_node_daemon_app}, lists:keyfind(module, 1, Info)),
            ?assertMatch({exports, _}, lists:keyfind(exports, 1, Info))
        end},
        {"module_info/1 returns exports", fun() ->
            Exports = flurm_node_daemon_app:module_info(exports),
            ?assert(is_list(Exports)),
            %% Check application callback exports
            ?assert(lists:member({start, 2}, Exports)),
            ?assert(lists:member({stop, 1}, Exports)),
            ?assert(lists:member({prep_stop, 1}, Exports))
        end},
        {"module_info/1 returns attributes", fun() ->
            Attrs = flurm_node_daemon_app:module_info(attributes),
            ?assert(is_list(Attrs)),
            %% Should have behaviour attribute
            BehaviourAttr = proplists:get_value(behaviour, Attrs,
                            proplists:get_value(behavior, Attrs, [])),
            ?assert(lists:member(application, BehaviourAttr))
        end}
    ].

%%====================================================================
%% Application Callback Tests
%%====================================================================

application_callback_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start/2 starts the application successfully with no saved state", fun test_start_no_saved_state/0},
         {"start/2 handles restored state with orphaned jobs", fun test_start_with_orphaned_jobs/0},
         {"start/2 handles restored state with no orphaned jobs", fun test_start_with_clean_state/0},
         {"stop/1 returns ok", fun test_stop/0},
         {"prep_stop/1 saves state and returns state", fun test_prep_stop/0}
     ]}.

test_start_no_saved_state() ->
    %% Mock state persistence to return no state
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, no_state} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),

    %% Mock supervisor start_link
    TestPid = self(),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

    %% Call start
    Result = flurm_node_daemon_app:start(normal, []),

    ?assertEqual({ok, TestPid}, Result),
    ?assert(meck:called(flurm_state_persistence, load_state, [])),
    ?assert(meck:called(flurm_node_daemon_sup, start_link, [])),
    ok.

test_start_with_orphaned_jobs() ->
    %% Mock state persistence with orphaned jobs
    SavedState = #{
        saved_at => erlang:system_time(millisecond) - 1000,
        running_jobs => 3,
        gpu_allocation => #{},
        draining => false
    },
    meck:expect(flurm_state_persistence, load_state, fun() -> {ok, SavedState} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),

    %% Mock supervisor start_link
    TestPid = spawn_link(fun() -> receive stop -> ok end end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

    %% Call start
    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _}, Result),
    ?assert(meck:called(flurm_state_persistence, load_state, [])),
    ?assert(meck:called(flurm_state_persistence, clear_state, [])),

    %% Cleanup
    TestPid ! stop,
    ok.

test_start_with_clean_state() ->
    %% Mock state persistence with zero orphaned jobs
    SavedState = #{
        saved_at => erlang:system_time(millisecond) - 1000,
        running_jobs => 0,
        gpu_allocation => #{},
        draining => false
    },
    meck:expect(flurm_state_persistence, load_state, fun() -> {ok, SavedState} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),

    %% Mock supervisor start_link
    TestPid = spawn_link(fun() -> receive stop -> ok end end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

    %% Call start
    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _}, Result),
    ?assert(meck:called(flurm_state_persistence, load_state, [])),
    ?assert(meck:called(flurm_state_persistence, clear_state, [])),

    %% Cleanup
    TestPid ! stop,
    ok.

test_stop() ->
    %% stop/1 should just return ok
    Result = flurm_node_daemon_app:stop(some_state),
    ?assertEqual(ok, Result),
    ok.

test_prep_stop() ->
    %% Mock dependencies for save_current_state
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 2, draining => false, drain_reason => undefined}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),

    %% Call prep_stop
    InputState = some_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    %% prep_stop should return the state unchanged
    ?assertEqual(InputState, Result),

    %% Verify save_state was called
    ?assert(meck:called(flurm_state_persistence, save_state, '_')),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"prep_stop handles connector errors gracefully", fun test_prep_stop_connector_error/0},
         {"prep_stop handles save_state errors gracefully", fun test_prep_stop_save_error/0}
     ]}.

test_prep_stop_connector_error() ->
    %% Mock connector to throw error
    meck:expect(flurm_controller_connector, get_state, fun() ->
        error(connector_not_running)
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),

    %% Call prep_stop - should not crash
    InputState = test_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    %% Should still return the state
    ?assertEqual(InputState, Result),
    ok.

test_prep_stop_save_error() ->
    %% Mock dependencies
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 0, draining => false, drain_reason => undefined}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> {error, disk_full} end),

    %% Call prep_stop - should not crash even with save error
    InputState = test_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    %% Should still return the state
    ?assertEqual(InputState, Result),
    ok.

%%====================================================================
%% Start Type Tests
%%====================================================================

start_type_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start/2 accepts normal start type", fun() ->
             meck:expect(flurm_state_persistence, load_state, fun() -> {error, no_state} end),
             TestPid = spawn_link(fun() -> receive stop -> ok end end),
             meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

             Result = flurm_node_daemon_app:start(normal, []),
             ?assertMatch({ok, _}, Result),
             TestPid ! stop
         end},
         {"start/2 accepts takeover start type", fun() ->
             meck:expect(flurm_state_persistence, load_state, fun() -> {error, no_state} end),
             TestPid = spawn_link(fun() -> receive stop -> ok end end),
             meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

             Result = flurm_node_daemon_app:start({takeover, 'node@host'}, []),
             ?assertMatch({ok, _}, Result),
             TestPid ! stop
         end},
         {"start/2 accepts failover start type", fun() ->
             meck:expect(flurm_state_persistence, load_state, fun() -> {error, no_state} end),
             TestPid = spawn_link(fun() -> receive stop -> ok end end),
             meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, TestPid} end),

             Result = flurm_node_daemon_app:start({failover, 'node@host'}, []),
             ?assertMatch({ok, _}, Result),
             TestPid ! stop
         end}
     ]}.

%%====================================================================
%% Environment Configuration Tests
%%====================================================================

env_config_test_() ->
    [
        {"start fails without controller_host config", fun() ->
            application:unset_env(flurm_node_daemon, controller_host),
            application:set_env(flurm_node_daemon, controller_port, 6817),

            %% Should fail when trying to get controller_host
            ?assertException(error, {badmatch, undefined},
                             flurm_node_daemon_app:start(normal, [])),

            %% Restore
            application:set_env(flurm_node_daemon, controller_host, "localhost")
        end},
        {"start fails without controller_port config", fun() ->
            application:set_env(flurm_node_daemon, controller_host, "localhost"),
            application:unset_env(flurm_node_daemon, controller_port),

            %% Mock to get past the load_state call
            meck:new(flurm_state_persistence, [non_strict]),
            meck:expect(flurm_state_persistence, load_state, fun() -> {error, no_state} end),

            %% Should fail when trying to get controller_port
            ?assertException(error, {badmatch, undefined},
                             flurm_node_daemon_app:start(normal, [])),

            meck:unload(flurm_state_persistence),
            %% Restore
            application:set_env(flurm_node_daemon, controller_port, 6817)
        end}
    ].

%%====================================================================
%% Behaviour Implementation Tests
%%====================================================================

behaviour_test_() ->
    [
        {"implements application behaviour", fun() ->
            Attrs = flurm_node_daemon_app:module_info(attributes),
            Behaviours = proplists:get_value(behaviour, Attrs, []) ++
                         proplists:get_value(behavior, Attrs, []),
            ?assert(lists:member(application, Behaviours))
        end},
        {"exports required application callbacks", fun() ->
            Exports = flurm_node_daemon_app:module_info(exports),
            ?assert(lists:member({start, 2}, Exports)),
            ?assert(lists:member({stop, 1}, Exports))
        end},
        {"exports optional prep_stop callback", fun() ->
            Exports = flurm_node_daemon_app:module_info(exports),
            ?assert(lists:member({prep_stop, 1}, Exports))
        end}
    ].
