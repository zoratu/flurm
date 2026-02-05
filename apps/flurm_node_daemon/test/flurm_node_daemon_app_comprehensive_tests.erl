%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_node_daemon_app Module
%%%
%%% Tests all application callback functions including start/2, stop/1,
%%% and prep_stop/1. Also tests internal functions like state
%%% persistence and orphaned job handling.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app_comprehensive_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Application Start Tests
%%====================================================================

app_start_test_() ->
    {foreach,
     fun setup_app_start/0,
     fun cleanup_app_start/1,
     [
        {"start/2 with valid config starts supervisor", fun test_app_start_normal/0},
        {"start/2 handles saved state with orphaned jobs", fun test_app_start_orphaned_jobs/0},
        {"start/2 handles saved state with no jobs", fun test_app_start_saved_state_no_jobs/0},
        {"start/2 handles load_state error", fun test_app_start_no_saved_state/0}
     ]}.

setup_app_start() ->
    meck:new(lager, [non_strict, passthrough]),
    meck:new(flurm_state_persistence, [passthrough, non_strict]),
    meck:new(flurm_node_daemon_sup, [passthrough, non_strict]),
    %% Set up required environment
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6818),
    ok.

cleanup_app_start(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_state_persistence),
    catch meck:unload(flurm_node_daemon_sup),
    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

test_app_start_normal() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, not_found} end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),

    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _Pid}, Result),
    ?assert(meck:called(flurm_node_daemon_sup, start_link, [])),
    ok.

test_app_start_orphaned_jobs() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    SavedState = #{
        saved_at => erlang:system_time(millisecond) - 300000,
        running_jobs => 5,
        version => 1
    },
    meck:expect(flurm_state_persistence, load_state, fun() -> {ok, SavedState} end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),

    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _Pid}, Result),
    ?assert(meck:called(flurm_state_persistence, clear_state, [])),
    ok.

test_app_start_saved_state_no_jobs() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    SavedState = #{
        saved_at => erlang:system_time(millisecond) - 60000,
        running_jobs => 0,
        version => 1
    },
    meck:expect(flurm_state_persistence, load_state, fun() -> {ok, SavedState} end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),

    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _Pid}, Result),
    ok.

test_app_start_no_saved_state() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, not_found} end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),

    Result = flurm_node_daemon_app:start(normal, []),

    ?assertMatch({ok, _Pid}, Result),
    ok.

%%====================================================================
%% Application Stop Tests
%%====================================================================

app_stop_test_() ->
    {foreach,
     fun setup_app_stop/0,
     fun cleanup_app_stop/1,
     [
        {"stop/1 logs and returns ok", fun test_app_stop/0}
     ]}.

setup_app_stop() ->
    meck:new(lager, [non_strict, passthrough]),
    ok.

cleanup_app_stop(_) ->
    catch meck:unload(lager),
    ok.

test_app_stop() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),

    Result = flurm_node_daemon_app:stop(test_state),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% prep_stop Tests
%%====================================================================

prep_stop_test_() ->
    {foreach,
     fun setup_prep_stop/0,
     fun cleanup_prep_stop/1,
     [
        {"prep_stop saves state successfully", fun test_prep_stop_success/0},
        {"prep_stop handles save error", fun test_prep_stop_save_error/0},
        {"prep_stop handles exception in get_state", fun test_prep_stop_exception/0}
     ]}.

setup_prep_stop() ->
    meck:new(lager, [non_strict, passthrough]),
    meck:new(flurm_state_persistence, [passthrough, non_strict]),
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_system_monitor, [passthrough, non_strict]),
    ok.

cleanup_prep_stop(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_state_persistence),
    catch meck:unload(flurm_controller_connector),
    catch meck:unload(flurm_system_monitor),
    ok.

test_prep_stop_success() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 3, draining => false, drain_reason => undefined}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{0 => 123, 1 => 456} end),
    meck:expect(flurm_state_persistence, save_state, fun(_State) -> ok end),

    InputState = my_app_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    ?assertEqual(InputState, Result),
    ?assert(meck:called(flurm_state_persistence, save_state, ['_'])),
    ok.

test_prep_stop_save_error() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 1}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_State) ->
        {error, eacces}
    end),

    InputState = my_app_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    ?assertEqual(InputState, Result),
    ok.

test_prep_stop_exception() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    meck:expect(flurm_controller_connector, get_state, fun() ->
        error(some_error)
    end),

    InputState = my_app_state,
    Result = flurm_node_daemon_app:prep_stop(InputState),

    ?assertEqual(InputState, Result),
    ok.

%%====================================================================
%% Integration Tests - Full Lifecycle
%%====================================================================

lifecycle_test_() ->
    {foreach,
     fun setup_lifecycle/0,
     fun cleanup_lifecycle/1,
     [
        {"full app lifecycle", fun test_full_lifecycle/0}
     ]}.

setup_lifecycle() ->
    meck:new(lager, [non_strict, passthrough]),
    meck:new(flurm_state_persistence, [passthrough, non_strict]),
    meck:new(flurm_node_daemon_sup, [passthrough, non_strict]),
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_system_monitor, [passthrough, non_strict]),
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6818),
    ok.

cleanup_lifecycle(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_state_persistence),
    catch meck:unload(flurm_node_daemon_sup),
    catch meck:unload(flurm_controller_connector),
    catch meck:unload(flurm_system_monitor),
    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

test_full_lifecycle() ->
    %% Setup mocks for full lifecycle
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, not_found} end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 2, draining => false}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_) -> ok end),

    %% Start
    {ok, _Pid} = flurm_node_daemon_app:start(normal, []),

    %% Prep stop (save state)
    AppState = test_state,
    PrepResult = flurm_node_daemon_app:prep_stop(AppState),
    ?assertEqual(AppState, PrepResult),

    %% Stop
    StopResult = flurm_node_daemon_app:stop(AppState),
    ?assertEqual(ok, StopResult),

    ok.
