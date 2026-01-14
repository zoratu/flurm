%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Module Tests
%%%
%%% Comprehensive tests for flurm_node_daemon, flurm_node_daemon_sup,
%%% and flurm_node_daemon_app modules.
%%% These tests aim for 100% code coverage using meck for mocking.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_module_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% flurm_node_daemon API Tests
%%====================================================================

daemon_api_test_() ->
    {foreach,
     fun setup_daemon_mocks/0,
     fun cleanup_daemon_mocks/1,
     [
        {"get_status returns status map", fun test_get_status/0},
        {"get_metrics returns metrics", fun test_get_metrics/0},
        {"is_connected returns boolean", fun test_is_connected/0},
        {"is_connected returns false when not connected", fun test_is_connected_false/0},
        {"list_running_jobs returns job list", fun test_list_running_jobs/0},
        {"list_running_jobs returns empty when no jobs", fun test_list_running_jobs_empty/0},
        {"get_job_status returns status", fun test_get_job_status/0},
        {"get_job_status returns error for dead process", fun test_get_job_status_not_found/0},
        {"cancel_job cancels successfully", fun test_cancel_job/0},
        {"cancel_job returns error for dead process", fun test_cancel_job_not_found/0}
     ]}.

setup_daemon_mocks() ->
    meck:new(flurm_controller_connector, [non_strict]),
    meck:new(flurm_system_monitor, [non_strict]),
    meck:new(flurm_job_executor, [non_strict]),
    ok.

cleanup_daemon_mocks(_) ->
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),
    meck:unload(flurm_job_executor),
    ok.

test_get_status() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => true, registered => true, node_id => <<"node001">>}
    end),
    meck:expect(flurm_system_monitor, get_metrics, fun() ->
        #{hostname => <<"testhost">>, cpus => 8, load_avg => 1.5}
    end),
    Status = flurm_node_daemon:get_status(),
    ?assertEqual(true, maps:get(connected, Status)),
    ?assertEqual(true, maps:get(registered, Status)),
    ?assertEqual(<<"node001">>, maps:get(node_id, Status)),
    ?assert(is_map(maps:get(metrics, Status))),
    ok.

test_get_metrics() ->
    ExpectedMetrics = #{hostname => <<"host1">>, cpus => 16, memory_mb => 32768},
    meck:expect(flurm_system_monitor, get_metrics, fun() -> ExpectedMetrics end),
    Metrics = flurm_node_daemon:get_metrics(),
    ?assertEqual(ExpectedMetrics, Metrics),
    ok.

test_is_connected() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => true}
    end),
    ?assertEqual(true, flurm_node_daemon:is_connected()),
    ok.

test_is_connected_false() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => false}
    end),
    ?assertEqual(false, flurm_node_daemon:is_connected()),
    ok.

test_list_running_jobs() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 5}
    end),
    Jobs = flurm_node_daemon:list_running_jobs(),
    ?assertEqual([{count, 5}], Jobs),
    ok.

test_list_running_jobs_empty() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => undefined}
    end),
    Jobs = flurm_node_daemon:list_running_jobs(),
    ?assertEqual([], Jobs),
    ok.

test_get_job_status() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ExpectedStatus = #{status => running, job_id => 123},
    meck:expect(flurm_job_executor, get_status, fun(Pid) when Pid =:= TestPid ->
        ExpectedStatus
    end),
    Status = flurm_node_daemon:get_job_status(TestPid),
    ?assertEqual(ExpectedStatus, Status),
    TestPid ! stop,
    ok.

test_get_job_status_not_found() ->
    %% Create and immediately kill a process
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(10),
    meck:expect(flurm_job_executor, get_status, fun(_Pid) ->
        exit({noproc, {gen_server, call, [self(), get_status]}})
    end),
    Result = flurm_node_daemon:get_job_status(DeadPid),
    ?assertEqual({error, not_found}, Result),
    ok.

test_cancel_job() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_job_executor, cancel, fun(Pid) when Pid =:= TestPid -> ok end),
    Result = flurm_node_daemon:cancel_job(TestPid),
    ?assertEqual(ok, Result),
    TestPid ! stop,
    ok.

test_cancel_job_not_found() ->
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(10),
    meck:expect(flurm_job_executor, cancel, fun(_Pid) ->
        exit({noproc, {gen_server, cast, [self(), cancel]}})
    end),
    Result = flurm_node_daemon:cancel_job(DeadPid),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% flurm_node_daemon_sup Tests
%%====================================================================

daemon_sup_test_() ->
    {"Supervisor init callback", fun test_daemon_sup_init/0}.

test_daemon_sup_init() ->
    {ok, {SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(10, maps:get(period, SupFlags)),
    %% Should have 3 children: system_monitor, controller_connector, job_executor_sup
    ?assertEqual(3, length(Children)),
    ChildIds = [maps:get(id, C) || C <- Children],
    ?assert(lists:member(flurm_system_monitor, ChildIds)),
    ?assert(lists:member(flurm_controller_connector, ChildIds)),
    ?assert(lists:member(flurm_job_executor_sup, ChildIds)),
    ok.

%%====================================================================
%% flurm_node_daemon_app Tests
%%====================================================================

daemon_app_test_() ->
    {foreach,
     fun setup_app_mocks/0,
     fun cleanup_app_mocks/1,
     [
        {"Application start with config", fun test_app_start/0},
        {"Application start handles restored state with jobs", fun test_app_start_with_orphaned_jobs/0},
        {"Application start handles no saved state", fun test_app_start_no_state/0},
        {"Application prep_stop saves state", fun test_app_prep_stop/0},
        {"Application stop", fun test_app_stop/0}
     ]}.

setup_app_mocks() ->
    meck:new(flurm_state_persistence, [non_strict]),
    meck:new(flurm_node_daemon_sup, [non_strict]),
    meck:new(flurm_controller_connector, [non_strict]),
    meck:new(flurm_system_monitor, [non_strict]),
    meck:new(lager, [non_strict, passthrough]),
    %% Set up application env
    application:set_env(flurm_node_daemon, controller_host, "localhost"),
    application:set_env(flurm_node_daemon, controller_port, 6818),
    ok.

cleanup_app_mocks(_) ->
    meck:unload(flurm_state_persistence),
    meck:unload(flurm_node_daemon_sup),
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),
    meck:unload(lager),
    application:unset_env(flurm_node_daemon, controller_host),
    application:unset_env(flurm_node_daemon, controller_port),
    ok.

test_app_start() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, not_found} end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),
    {ok, Pid} = flurm_node_daemon_app:start(normal, []),
    ?assert(is_pid(Pid)),
    ok.

test_app_start_with_orphaned_jobs() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    SavedState = #{
        saved_at => erlang:system_time(millisecond) - 60000,
        running_jobs => 3
    },
    meck:expect(flurm_state_persistence, load_state, fun() -> {ok, SavedState} end),
    meck:expect(flurm_state_persistence, clear_state, fun() -> ok end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),
    {ok, Pid} = flurm_node_daemon_app:start(normal, []),
    ?assert(is_pid(Pid)),
    %% Verify clear_state was called
    ?assert(meck:called(flurm_state_persistence, clear_state, [])),
    ok.

test_app_start_no_state() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_state_persistence, load_state, fun() -> {error, not_found} end),
    meck:expect(flurm_node_daemon_sup, start_link, fun() -> {ok, self()} end),
    {ok, Pid} = flurm_node_daemon_app:start(normal, []),
    ?assert(is_pid(Pid)),
    ok.

test_app_prep_stop() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 2, draining => false, drain_reason => undefined}
    end),
    meck:expect(flurm_system_monitor, get_gpu_allocation, fun() -> #{} end),
    meck:expect(flurm_state_persistence, save_state, fun(_State) -> ok end),
    State = test_state,
    Result = flurm_node_daemon_app:prep_stop(State),
    ?assertEqual(State, Result),
    ?assert(meck:called(flurm_state_persistence, save_state, ['_'])),
    ok.

test_app_stop() ->
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    Result = flurm_node_daemon_app:stop(test_state),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% flurm_state_persistence Tests
%%====================================================================

state_persistence_test_() ->
    {foreach,
     fun setup_state_persistence/0,
     fun cleanup_state_persistence/1,
     [
        {"get_state_file with env", fun test_get_state_file_env/0},
        {"get_state_file default", fun test_get_state_file_default/0},
        {"save_state and load_state round trip", fun test_save_load_state/0},
        {"load_state file not found", fun test_load_state_not_found/0},
        {"load_state corrupted file", fun test_load_state_corrupted/0},
        {"load_state invalid state", fun test_load_state_invalid/0},
        {"clear_state", fun test_clear_state/0},
        {"clear_state when file doesn't exist", fun test_clear_state_not_exists/0}
     ]}.

setup_state_persistence() ->
    %% Use a temp directory for tests
    TempDir = "/tmp/flurm_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(TempDir ++ "/"),
    file:make_dir(TempDir),
    StateFile = TempDir ++ "/test_state.dat",
    application:set_env(flurm_node_daemon, state_file, StateFile),
    #{temp_dir => TempDir, state_file => StateFile}.

cleanup_state_persistence(#{temp_dir := TempDir, state_file := StateFile}) ->
    file:delete(StateFile),
    file:del_dir(TempDir),
    application:unset_env(flurm_node_daemon, state_file),
    ok.

test_get_state_file_env() ->
    application:set_env(flurm_node_daemon, state_file, "/custom/path/state.dat"),
    ?assertEqual("/custom/path/state.dat", flurm_state_persistence:get_state_file()),
    ok.

test_get_state_file_default() ->
    application:unset_env(flurm_node_daemon, state_file),
    ?assertEqual("/var/lib/flurm/node_state.dat", flurm_state_persistence:get_state_file()),
    %% Restore for other tests
    application:set_env(flurm_node_daemon, state_file, "/tmp/test_state.dat"),
    ok.

test_save_load_state() ->
    State = #{running_jobs => 5, gpu_allocation => #{0 => 123}, custom => <<"data">>},
    ok = flurm_state_persistence:save_state(State),
    {ok, LoadedState} = flurm_state_persistence:load_state(),
    ?assertEqual(5, maps:get(running_jobs, LoadedState)),
    ?assertEqual(#{0 => 123}, maps:get(gpu_allocation, LoadedState)),
    ?assertEqual(1, maps:get(version, LoadedState)),
    ?assert(maps:is_key(saved_at, LoadedState)),
    ok.

test_load_state_not_found() ->
    application:set_env(flurm_node_daemon, state_file, "/tmp/nonexistent_state_file.dat"),
    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, not_found}, Result),
    ok.

test_load_state_corrupted() ->
    StateFile = flurm_state_persistence:get_state_file(),
    ok = file:write_file(StateFile, <<"not valid erlang term">>),
    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, corrupted}, Result),
    ok.

test_load_state_invalid() ->
    StateFile = flurm_state_persistence:get_state_file(),
    %% Write a valid erlang term but without version
    InvalidState = #{running_jobs => 1},
    ok = file:write_file(StateFile, term_to_binary(InvalidState)),
    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    %% Write with unknown version
    InvalidState2 = #{version => 999, running_jobs => 1},
    ok = file:write_file(StateFile, term_to_binary(InvalidState2)),
    Result2 = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result2),
    %% Write non-map data
    ok = file:write_file(StateFile, term_to_binary([1, 2, 3])),
    Result3 = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result3),
    ok.

test_clear_state() ->
    State = #{running_jobs => 1},
    ok = flurm_state_persistence:save_state(State),
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(filelib:is_regular(StateFile)),
    ok = flurm_state_persistence:clear_state(),
    ?assertNot(filelib:is_regular(StateFile)),
    ok.

test_clear_state_not_exists() ->
    application:set_env(flurm_node_daemon, state_file, "/tmp/nonexistent_file.dat"),
    Result = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% flurm_job_executor_sup Tests
%%====================================================================

job_executor_sup_test_() ->
    {"Job executor supervisor init callback", fun test_job_executor_sup_init/0}.

test_job_executor_sup_init() ->
    {ok, {SupFlags, Children}} = flurm_job_executor_sup:init([]),
    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(10, maps:get(intensity, SupFlags)),
    ?assertEqual(60, maps:get(period, SupFlags)),
    ?assertEqual(1, length(Children)),
    [ChildSpec] = Children,
    ?assertEqual(flurm_job_executor, maps:get(id, ChildSpec)),
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),
    ?assertEqual(30000, maps:get(shutdown, ChildSpec)),
    ok.
