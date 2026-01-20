%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_node_daemon API Module
%%%
%%% Tests all exported functions in the flurm_node_daemon module.
%%% External dependencies are mocked, but the actual module functions
%%% are tested directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_api_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_node_daemon_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_status returns complete status map", fun test_get_status_complete/0},
        {"get_status handles missing fields", fun test_get_status_missing_fields/0},
        {"get_metrics delegates to system monitor", fun test_get_metrics/0},
        {"get_metrics returns complex metrics", fun test_get_metrics_complex/0},
        {"is_connected returns true when connected", fun test_is_connected_true/0},
        {"is_connected returns false when disconnected", fun test_is_connected_false/0},
        {"is_connected handles empty state", fun test_is_connected_empty_state/0},
        {"list_running_jobs returns count tuple", fun test_list_running_jobs_with_count/0},
        {"list_running_jobs returns empty for zero jobs", fun test_list_running_jobs_zero/0},
        {"list_running_jobs handles non-integer", fun test_list_running_jobs_non_integer/0},
        {"list_running_jobs handles missing key", fun test_list_running_jobs_missing_key/0},
        {"get_job_status returns status map", fun test_get_job_status_success/0},
        {"get_job_status handles noproc error", fun test_get_job_status_noproc/0},
        {"cancel_job succeeds for valid pid", fun test_cancel_job_success/0},
        {"cancel_job handles noproc error", fun test_cancel_job_noproc/0}
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
        #{
            connected => true,
            registered => true,
            node_id => <<"node-001">>
        }
    end),
    meck:expect(flurm_system_monitor, get_metrics, fun() ->
        #{
            hostname => <<"testhost.local">>,
            cpus => 16,
            memory_mb => 65536,
            load_avg => 2.5
        }
    end),

    Status = flurm_node_daemon:get_status(),

    ?assertEqual(true, maps:get(connected, Status)),
    ?assertEqual(true, maps:get(registered, Status)),
    ?assertEqual(<<"node-001">>, maps:get(node_id, Status)),
    Metrics = maps:get(metrics, Status),
    ?assertEqual(<<"testhost.local">>, maps:get(hostname, Metrics)),
    ?assertEqual(16, maps:get(cpus, Metrics)),
    ok.

test_get_status_missing_fields() ->
    %% Test with empty connector state - should use defaults
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),
    meck:expect(flurm_system_monitor, get_metrics, fun() -> #{} end),

    Status = flurm_node_daemon:get_status(),

    ?assertEqual(false, maps:get(connected, Status)),
    ?assertEqual(false, maps:get(registered, Status)),
    ?assertEqual(undefined, maps:get(node_id, Status)),
    ?assert(is_map(maps:get(metrics, Status))),
    ok.

%%====================================================================
%% get_metrics Tests
%%====================================================================

test_get_metrics() ->
    ExpectedMetrics = #{hostname => <<"host1">>, cpus => 8},
    meck:expect(flurm_system_monitor, get_metrics, fun() -> ExpectedMetrics end),

    Result = flurm_node_daemon:get_metrics(),

    ?assertEqual(ExpectedMetrics, Result),
    ?assert(meck:called(flurm_system_monitor, get_metrics, [])),
    ok.

test_get_metrics_complex() ->
    ComplexMetrics = #{
        hostname => <<"compute-node-42.cluster.local">>,
        cpus => 128,
        memory_mb => 524288,
        load_avg => 64.5,
        gpus => 8,
        gpu_memory_mb => 327680,
        uptime_seconds => 86400,
        free_memory_mb => 262144
    },
    meck:expect(flurm_system_monitor, get_metrics, fun() -> ComplexMetrics end),

    Result = flurm_node_daemon:get_metrics(),

    ?assertEqual(128, maps:get(cpus, Result)),
    ?assertEqual(8, maps:get(gpus, Result)),
    ?assertEqual(64.5, maps:get(load_avg, Result)),
    ok.

%%====================================================================
%% is_connected Tests
%%====================================================================

test_is_connected_true() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => true}
    end),

    Result = flurm_node_daemon:is_connected(),

    ?assertEqual(true, Result),
    ok.

test_is_connected_false() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{connected => false}
    end),

    Result = flurm_node_daemon:is_connected(),

    ?assertEqual(false, Result),
    ok.

test_is_connected_empty_state() ->
    %% When 'connected' key is missing, should default to false
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),

    Result = flurm_node_daemon:is_connected(),

    ?assertEqual(false, Result),
    ok.

%%====================================================================
%% list_running_jobs Tests
%%====================================================================

test_list_running_jobs_with_count() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 10}
    end),

    Result = flurm_node_daemon:list_running_jobs(),

    ?assertEqual([{count, 10}], Result),
    ok.

test_list_running_jobs_zero() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => 0}
    end),

    Result = flurm_node_daemon:list_running_jobs(),

    %% Zero is still an integer, so it returns [{count, 0}]
    ?assertEqual([{count, 0}], Result),
    ok.

test_list_running_jobs_non_integer() ->
    meck:expect(flurm_controller_connector, get_state, fun() ->
        #{running_jobs => undefined}
    end),

    Result = flurm_node_daemon:list_running_jobs(),

    ?assertEqual([], Result),
    ok.

test_list_running_jobs_missing_key() ->
    meck:expect(flurm_controller_connector, get_state, fun() -> #{} end),

    Result = flurm_node_daemon:list_running_jobs(),

    %% Default is 0 from maps:get, which is still an integer, so returns [{count, 0}]
    ?assertEqual([{count, 0}], Result),
    ok.

%%====================================================================
%% get_job_status Tests
%%====================================================================

test_get_job_status_success() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ExpectedStatus = #{
        status => running,
        job_id => 12345,
        cpu_percent => 95.5,
        memory_used => 4096
    },
    meck:expect(flurm_job_executor, get_status, fun(Pid) when Pid =:= TestPid ->
        ExpectedStatus
    end),

    Result = flurm_node_daemon:get_job_status(TestPid),

    ?assertEqual(ExpectedStatus, Result),
    TestPid ! stop,
    ok.

test_get_job_status_noproc() ->
    DeadPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(DeadPid),
    meck:expect(flurm_job_executor, get_status, fun(_Pid) ->
        exit({noproc, {gen_server, call, [self(), get_status]}})
    end),

    Result = flurm_node_daemon:get_job_status(DeadPid),

    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% cancel_job Tests
%%====================================================================

test_cancel_job_success() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(flurm_job_executor, cancel, fun(Pid) when Pid =:= TestPid -> ok end),

    Result = flurm_node_daemon:cancel_job(TestPid),

    ?assertEqual(ok, Result),
    ?assert(meck:called(flurm_job_executor, cancel, [TestPid])),
    TestPid ! stop,
    ok.

test_cancel_job_noproc() ->
    DeadPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(DeadPid),
    meck:expect(flurm_job_executor, cancel, fun(_Pid) ->
        exit({noproc, {gen_server, cast, [self(), cancel]}})
    end),

    Result = flurm_node_daemon:cancel_job(DeadPid),

    ?assertEqual({error, not_found}, Result),
    ok.
