%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_node_daemon module
%%%
%%% Tests the node daemon API functions. The flurm_node_daemon module
%%% is a thin facade over other components, so these tests verify
%%% the API contracts and error handling.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% API Function Export Tests
%%====================================================================

%% Test that all API functions are exported
api_exports_test_() ->
    [
     {"get_status/0 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({get_status, 0}, Exports))
      end},
     {"get_metrics/0 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({get_metrics, 0}, Exports))
      end},
     {"is_connected/0 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({is_connected, 0}, Exports))
      end},
     {"list_running_jobs/0 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({list_running_jobs, 0}, Exports))
      end},
     {"get_job_status/1 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({get_job_status, 1}, Exports))
      end},
     {"cancel_job/1 is exported",
      fun() ->
          Exports = flurm_node_daemon:module_info(exports),
          ?assert(lists:member({cancel_job, 1}, Exports))
      end}
    ].

%%====================================================================
%% get_job_status Tests
%%====================================================================

get_job_status_test_() ->
    [
     {"get_job_status with dead pid returns not_found",
      fun() ->
          %% Create a process that immediately exits
          Pid = spawn(fun() -> ok end),
          timer:sleep(10),  %% Ensure it's dead
          Result = flurm_node_daemon:get_job_status(Pid),
          ?assertEqual({error, not_found}, Result)
      end}
    ].

%%====================================================================
%% cancel_job Tests
%%====================================================================

cancel_job_test_() ->
    [
     {"cancel_job with dead pid returns not_found",
      fun() ->
          %% Create a process that immediately exits
          Pid = spawn(fun() -> ok end),
          timer:sleep(10),  %% Ensure it's dead
          Result = flurm_node_daemon:cancel_job(Pid),
          ?assertEqual({error, not_found}, Result)
      end}
    ].

%%====================================================================
%% Module Documentation Tests
%%====================================================================

module_info_test_() ->
    [
     {"module has correct name",
      fun() ->
          ?assertEqual(flurm_node_daemon, flurm_node_daemon:module_info(module))
      end},
     {"module compiles without errors",
      fun() ->
          %% If we got here, the module compiled
          ?assert(is_list(flurm_node_daemon:module_info(exports)))
      end}
    ].
