%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_node_daemon module
%%%
%%% Tests the main node daemon API functions directly.
%%% Mocks external dependencies (flurm_controller_connector,
%%% flurm_system_monitor, flurm_job_executor) but NOT the module
%%% being tested.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

node_daemon_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_get_status_connected/1,
      fun test_get_status_disconnected/1,
      fun test_get_metrics/1,
      fun test_is_connected_true/1,
      fun test_is_connected_false/1,
      fun test_list_running_jobs_with_count/1,
      fun test_list_running_jobs_empty/1,
      fun test_get_job_status_success/1,
      fun test_get_job_status_not_found/1,
      fun test_cancel_job_success/1,
      fun test_cancel_job_not_found/1
     ]}.

setup() ->
    %% Start meck for external dependencies only
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_system_monitor, [passthrough, non_strict]),
    meck:new(flurm_job_executor, [passthrough, non_strict]),
    ok.

cleanup(_) ->
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_system_monitor),
    meck:unload(flurm_job_executor),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_get_status_connected(_) ->
    {"get_status returns full status when connected",
     fun() ->
         %% Setup mocks
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{connected => true, registered => true, node_id => <<"node001">>}
         end),
         meck:expect(flurm_system_monitor, get_metrics, fun() ->
             #{cpus => 8, memory_mb => 16384, load_avg => 0.5}
         end),

         %% Call the actual function
         Status = flurm_node_daemon:get_status(),

         %% Verify result
         ?assertEqual(true, maps:get(connected, Status)),
         ?assertEqual(true, maps:get(registered, Status)),
         ?assertEqual(<<"node001">>, maps:get(node_id, Status)),
         ?assert(is_map(maps:get(metrics, Status))),
         ?assertEqual(8, maps:get(cpus, maps:get(metrics, Status)))
     end}.

test_get_status_disconnected(_) ->
    {"get_status returns defaults when disconnected",
     fun() ->
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{}  % Empty state - all fields missing
         end),
         meck:expect(flurm_system_monitor, get_metrics, fun() ->
             #{cpus => 4}
         end),

         Status = flurm_node_daemon:get_status(),

         ?assertEqual(false, maps:get(connected, Status)),
         ?assertEqual(false, maps:get(registered, Status)),
         ?assertEqual(undefined, maps:get(node_id, Status))
     end}.

test_get_metrics(_) ->
    {"get_metrics delegates to system monitor",
     fun() ->
         ExpectedMetrics = #{
             hostname => <<"testhost">>,
             cpus => 16,
             total_memory_mb => 32768,
             load_avg => 2.5
         },
         meck:expect(flurm_system_monitor, get_metrics, fun() -> ExpectedMetrics end),

         Result = flurm_node_daemon:get_metrics(),

         ?assertEqual(ExpectedMetrics, Result),
         ?assert(meck:called(flurm_system_monitor, get_metrics, []))
     end}.

test_is_connected_true(_) ->
    {"is_connected returns true when connected",
     fun() ->
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{connected => true}
         end),

         Result = flurm_node_daemon:is_connected(),

         ?assertEqual(true, Result)
     end}.

test_is_connected_false(_) ->
    {"is_connected returns false when not connected",
     fun() ->
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{connected => false}
         end),

         Result = flurm_node_daemon:is_connected(),

         ?assertEqual(false, Result)
     end}.

test_list_running_jobs_with_count(_) ->
    {"list_running_jobs returns job count",
     fun() ->
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{running_jobs => 5}
         end),

         Result = flurm_node_daemon:list_running_jobs(),

         ?assertEqual([{count, 5}], Result)
     end}.

test_list_running_jobs_empty(_) ->
    {"list_running_jobs returns empty when no jobs",
     fun() ->
         meck:expect(flurm_controller_connector, get_state, fun() ->
             #{running_jobs => undefined}  % Not an integer
         end),

         Result = flurm_node_daemon:list_running_jobs(),

         ?assertEqual([], Result)
     end}.

test_get_job_status_success(_) ->
    {"get_job_status returns status for valid pid",
     fun() ->
         TestPid = spawn(fun() -> receive stop -> ok end end),
         ExpectedStatus = #{
             job_id => 12345,
             status => running,
             exit_code => undefined
         },
         meck:expect(flurm_job_executor, get_status, fun(Pid) when Pid =:= TestPid ->
             ExpectedStatus
         end),

         Result = flurm_node_daemon:get_job_status(TestPid),

         ?assertEqual(ExpectedStatus, Result),
         TestPid ! stop
     end}.

test_get_job_status_not_found(_) ->
    {"get_job_status returns error for dead process",
     fun() ->
         %% Create and kill a process to get a dead pid
         TestPid = spawn(fun() -> ok end),
         flurm_test_utils:wait_for_death(TestPid),

         meck:expect(flurm_job_executor, get_status, fun(_Pid) ->
             meck:exception(exit, {noproc, {gen_server, call, [self(), get_status]}})
         end),

         Result = flurm_node_daemon:get_job_status(TestPid),

         ?assertEqual({error, not_found}, Result)
     end}.

test_cancel_job_success(_) ->
    {"cancel_job returns ok for valid job",
     fun() ->
         TestPid = spawn(fun() -> receive stop -> ok end end),
         meck:expect(flurm_job_executor, cancel, fun(Pid) when Pid =:= TestPid ->
             ok
         end),

         Result = flurm_node_daemon:cancel_job(TestPid),

         ?assertEqual(ok, Result),
         ?assert(meck:called(flurm_job_executor, cancel, [TestPid])),
         TestPid ! stop
     end}.

test_cancel_job_not_found(_) ->
    {"cancel_job returns error for dead process",
     fun() ->
         TestPid = spawn(fun() -> ok end),
         flurm_test_utils:wait_for_death(TestPid),

         meck:expect(flurm_job_executor, cancel, fun(_Pid) ->
             meck:exception(exit, {noproc, {gen_server, cast, [self(), cancel]}})
         end),

         Result = flurm_node_daemon:cancel_job(TestPid),

         ?assertEqual({error, not_found}, Result)
     end}.
