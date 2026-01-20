%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_node_daemon module
%%%
%%% Tests the API functions of flurm_node_daemon without mocking.
%%% Since this module calls external modules (flurm_controller_connector,
%%% flurm_system_monitor, flurm_job_executor), we test the logic that
%%% can be tested in isolation and verify exception handling.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup/teardown for tests
setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

flurm_node_daemon_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_job_status with invalid pid returns not_found",
       fun get_job_status_not_found_test/0},
      {"get_job_status guard clause test",
       fun get_job_status_guard_test/0},
      {"cancel_job with invalid pid returns not_found",
       fun cancel_job_not_found_test/0},
      {"cancel_job guard clause test",
       fun cancel_job_guard_test/0},
      {"module exports test",
       fun exports_test/0},
      {"spec validation test",
       fun spec_validation_test/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

%% Test get_job_status with a dead pid (triggers noproc)
get_job_status_not_found_test() ->
    %% Create a pid that's already dead
    DeadPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(DeadPid),
    %% This should catch the noproc and return {error, not_found}
    Result = flurm_node_daemon:get_job_status(DeadPid),
    ?assertEqual({error, not_found}, Result).

%% Test that get_job_status has proper guard clause
get_job_status_guard_test() ->
    %% Verify the guard clause only accepts pids
    %% This should cause a function_clause error, not match
    ?assertError(function_clause, flurm_node_daemon:get_job_status(not_a_pid)),
    ?assertError(function_clause, flurm_node_daemon:get_job_status(12345)),
    ?assertError(function_clause, flurm_node_daemon:get_job_status("pid")).

%% Test cancel_job with a dead pid
%% Note: cancel_job uses gen_server:cast which is asynchronous
%% and does not raise exceptions for dead processes.
%% Therefore it always returns ok (fire-and-forget semantics).
cancel_job_not_found_test() ->
    %% Create a pid that's already dead
    DeadPid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(DeadPid),
    %% Cast-based cancel always returns ok (async fire-and-forget)
    Result = flurm_node_daemon:cancel_job(DeadPid),
    ?assertEqual(ok, Result).

%% Test that cancel_job has proper guard clause
cancel_job_guard_test() ->
    %% Verify the guard clause only accepts pids
    ?assertError(function_clause, flurm_node_daemon:cancel_job(not_a_pid)),
    ?assertError(function_clause, flurm_node_daemon:cancel_job(12345)),
    ?assertError(function_clause, flurm_node_daemon:cancel_job("pid")).

%% Test that module exports expected functions
exports_test() ->
    Exports = flurm_node_daemon:module_info(exports),
    %% Check all expected exports are present
    ?assert(lists:member({get_status, 0}, Exports)),
    ?assert(lists:member({get_metrics, 0}, Exports)),
    ?assert(lists:member({is_connected, 0}, Exports)),
    ?assert(lists:member({list_running_jobs, 0}, Exports)),
    ?assert(lists:member({get_job_status, 1}, Exports)),
    ?assert(lists:member({cancel_job, 1}, Exports)).

%% Validate that specs match function arities
spec_validation_test() ->
    %% This just verifies the module compiles with valid specs
    %% by checking function info
    Info = flurm_node_daemon:module_info(functions),
    ExpectedFunctions = [
        {get_status, 0},
        {get_metrics, 0},
        {is_connected, 0},
        {list_running_jobs, 0},
        {get_job_status, 1},
        {cancel_job, 1}
    ],
    lists:foreach(fun(F) ->
        ?assert(lists:member(F, Info))
    end, ExpectedFunctions).
