%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_node_daemon module
%%%
%%% Tests the flurm_node_daemon API functions directly with mocked
%%% dependencies (NOT the module under test).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_node_daemon_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_status returns complete status map", fun test_get_status_complete/0},
      {"get_status with missing fields uses defaults", fun test_get_status_defaults/0},
      {"get_metrics delegates to system monitor", fun test_get_metrics/0},
      {"is_connected returns true when connected", fun test_is_connected_true/0},
      {"is_connected returns false when not connected", fun test_is_connected_false/0},
      {"is_connected defaults to false with empty map", fun test_is_connected_default/0},
      {"list_running_jobs with integer count", fun test_list_running_jobs_count/0},
      {"list_running_jobs with zero count", fun test_list_running_jobs_zero/0},
      {"list_running_jobs with non-integer", fun test_list_running_jobs_non_integer/0},
      {"list_running_jobs with missing key", fun test_list_running_jobs_missing/0},
      {"get_job_status success", fun test_get_job_status_ok/0},
      {"get_job_status returns error when process dead", fun test_get_job_status_noproc/0},
      {"cancel_job success", fun test_cancel_job_ok/0},
      {"cancel_job returns error when process dead", fun test_cancel_job_noproc/0}
     ]}.

setup() ->
    meck:new(flurm_controller_connector, [non_strict]),
    meck:new(flurm_system_monitor, [non_strict]),
    meck:new(flurm_job_executor, [non_strict]),
    ok.

cleanup(_) ->
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),
    meck:unload(flurm_job_executor),
    ok.

%%====================================================================
%% get_status Tests
%%====================================================================

test_get_status_complete() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => true, registered => true, node_id => <<"node-001">>}
    end),
    meck:expect(flurm_system_monitor, get_metrics, fun() ->
        #{cpus => 8, memory_mb => 16384}
    end),

    Status = flurm_node_daemon:get_status(),

    ?assertEqual(true, maps:get(connected, Status)),
    ?assertEqual(true, maps:get(registered, Status)),
    ?assertEqual(<<"node-001">>, maps:get(node_id, Status)),
    ?assertEqual(#{cpus => 8, memory_mb => 16384}, maps:get(metrics, Status)).

test_get_status_defaults() ->
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),
    meck:expect(flurm_system_monitor, get_metrics, fun() -> #{} end),

    Status = flurm_node_daemon:get_status(),

    ?assertEqual(false, maps:get(connected, Status)),
    ?assertEqual(false, maps:get(registered, Status)),
    ?assertEqual(undefined, maps:get(node_id, Status)).

%%====================================================================
%% get_metrics Tests
%%====================================================================

test_get_metrics() ->
    ExpectedMetrics = #{hostname => <<"host1">>, cpus => 4, load => 0.5},
    meck:expect(flurm_system_monitor, get_metrics, fun() -> ExpectedMetrics end),

    Result = flurm_node_daemon:get_metrics(),

    ?assertEqual(ExpectedMetrics, Result),
    ?assert(meck:called(flurm_system_monitor, get_metrics, [])).

%%====================================================================
%% is_connected Tests
%%====================================================================

test_is_connected_true() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => true}
    end),

    ?assertEqual(true, flurm_node_daemon:is_connected()).

test_is_connected_false() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => false}
    end),

    ?assertEqual(false, flurm_node_daemon:is_connected()).

test_is_connected_default() ->
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),

    ?assertEqual(false, flurm_node_daemon:is_connected()).

%%====================================================================
%% list_running_jobs Tests
%%====================================================================

test_list_running_jobs_count() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 5}
    end),

    ?assertEqual([{count, 5}], flurm_node_daemon:list_running_jobs()).

test_list_running_jobs_zero() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 0}
    end),

    ?assertEqual([{count, 0}], flurm_node_daemon:list_running_jobs()).

test_list_running_jobs_non_integer() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => not_an_integer}
    end),

    ?assertEqual([], flurm_node_daemon:list_running_jobs()).

test_list_running_jobs_missing() ->
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),

    ?assertEqual([{count, 0}], flurm_node_daemon:list_running_jobs()).

%%====================================================================
%% get_job_status Tests
%%====================================================================

test_get_job_status_ok() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    ExpectedStatus = #{job_id => 123, state => running},
    meck:expect(flurm_job_executor, get_status, fun(P) when P =:= Pid ->
        ExpectedStatus
    end),

    Result = flurm_node_daemon:get_job_status(Pid),

    ?assertEqual(ExpectedStatus, Result),
    Pid ! stop.

test_get_job_status_noproc() ->
    Pid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(Pid),
    meck:expect(flurm_job_executor, get_status, fun(_) ->
        meck:exception(exit, {noproc, {gen_server, call, [self(), get_status]}})
    end),

    Result = flurm_node_daemon:get_job_status(Pid),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% cancel_job Tests
%%====================================================================

test_cancel_job_ok() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_job_executor, cancel, fun(P) when P =:= Pid -> ok end),

    Result = flurm_node_daemon:cancel_job(Pid),

    ?assertEqual(ok, Result),
    Pid ! stop.

test_cancel_job_noproc() ->
    Pid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(Pid),
    meck:expect(flurm_job_executor, cancel, fun(_) ->
        meck:exception(exit, {noproc, {gen_server, cast, [self(), cancel]}})
    end),

    Result = flurm_node_daemon:cancel_job(Pid),

    ?assertEqual({error, not_found}, Result).
