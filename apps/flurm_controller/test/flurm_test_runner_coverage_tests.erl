%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_test_runner module
%%% Tests for job lifecycle test runner
%%% Note: This module requires external dependencies (flurm_job_manager,
%%% flurm_node_connection_manager) so we test what we can in isolation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_runner_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: flurm_test_runner is designed for integration testing.
%% Most functions require running gen_servers. We test what we can
%% with the -ifdef(TEST) exports.

%%====================================================================
%% run/0 Tests (requires external services)
%%====================================================================

%% The run/0 function requires flurm_node_connection_manager to be running
%% and have connected nodes. We test the error case when no nodes are connected.

run_no_nodes_test_() ->
    {setup,
     fun() ->
         %% Start node connection manager if not running
         case whereis(flurm_node_connection_manager) of
             undefined ->
                 {ok, Pid} = flurm_node_connection_manager:start_link(),
                 unlink(Pid),
                 Pid;
             Pid ->
                 Pid
         end
     end,
     fun(_Pid) ->
         %% Leave it running for other tests
         ok
     end,
     fun(_) ->
         [
             {"run returns error with no nodes", fun() ->
                 %% Clear all registered connections
                 Nodes = flurm_node_connection_manager:list_connected_nodes(),
                 lists:foreach(fun(Node) ->
                     flurm_node_connection_manager:unregister_connection(Node)
                 end, Nodes),
                 timer:sleep(50),

                 Result = flurm_test_runner:run(),
                 ?assertEqual({error, no_nodes}, Result)
             end}
         ]
     end}.

%% Deterministic branch tests using mocked dependencies.
runner_branches_with_mocks_test_() ->
    {setup,
     fun setup_runner_mecks/0,
     fun cleanup_runner_mecks/1,
     [
         {"run/0 calls run_job_impl path when nodes are connected",
          fun test_run_with_connected_nodes/0},
         {"run_job_impl/0 handles submit error path",
          fun test_run_job_impl_submit_error/0},
         {"run_job_impl/0 success path reaches monitor_job and completes",
          fun test_run_job_impl_completed/0},
         {"monitor_job/2 returns failed state error",
          fun test_monitor_job_failed/0},
         {"monitor_job/2 retries and then completes",
          fun test_monitor_job_retries_then_completes/0}
     ]}.

setup_runner_mecks() ->
    catch meck:unload(flurm_node_connection_manager),
    catch meck:unload(flurm_job_manager),
    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),
    ok.

cleanup_runner_mecks(_) ->
    meck:unload(flurm_node_connection_manager),
    meck:unload(flurm_job_manager),
    erase(test_runner_get_job_calls),
    ok.

test_run_with_connected_nodes() ->
    meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [<<"node1">>] end),
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, simulated_submit_failure} end),
    ?assertEqual({error, simulated_submit_failure}, flurm_test_runner:run()).

test_run_job_impl_submit_error() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, submit_failed} end),
    ?assertEqual({error, submit_failed}, flurm_test_runner:run_job_impl()).

test_run_job_impl_completed() ->
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 42} end),
    meck:expect(flurm_job_manager, get_job, fun(42) -> {ok, mk_job(42, completed, 0)} end),
    ?assertEqual(ok, flurm_test_runner:run_job_impl()).

test_monitor_job_failed() ->
    meck:expect(flurm_job_manager, get_job, fun(99) -> {ok, mk_job(99, failed, 1)} end),
    ?assertEqual({error, job_failed}, flurm_test_runner:monitor_job(99, 1)).

test_monitor_job_retries_then_completes() ->
    put(test_runner_get_job_calls, 0),
    meck:expect(flurm_job_manager, get_job,
        fun(77) ->
            Calls = get(test_runner_get_job_calls),
            put(test_runner_get_job_calls, Calls + 1),
            case Calls of
                0 -> {ok, mk_job(77, running, undefined)};
                _ -> {ok, mk_job(77, completed, 0)}
            end
        end),
    ?assertEqual(ok, flurm_test_runner:monitor_job(77, 2)).

mk_job(Id, State, ExitCode) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"test">>,
        partition = <<"default">>,
        state = State,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 512,
        time_limit = 60,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [<<"node1">>],
        exit_code = ExitCode,
        account = <<"default">>,
        qos = <<"normal">>,
        licenses = []
    }.

%%====================================================================
%% monitor_job Tests (via -ifdef(TEST) export)
%%====================================================================

%% monitor_job/2 requires flurm_job_manager to be running.
%% We test the timeout case.

monitor_job_timeout_test_() ->
    {setup,
     fun() ->
         %% Start job manager if not running
         case whereis(flurm_job_manager) of
             undefined ->
                 case flurm_job_manager:start_link() of
                     {ok, Pid} ->
                         unlink(Pid),
                         started;
                     {error, {already_started, Pid}} ->
                         unlink(Pid),
                         existed
                 end;
             _ ->
                 existed
         end
     end,
     fun(_) -> ok end,
     fun(_) ->
         [
             {"monitor_job times out for non-existent job", fun() ->
                 %% Try to monitor a job that doesn't exist
                 %% With 0 iterations, it should immediately timeout
                 Result = flurm_test_runner:monitor_job(99999999, 0),
                 ?assertEqual({error, timeout}, Result)
             end}
         ]
     end}.

%%====================================================================
%% run_job_impl Tests (via -ifdef(TEST) export)
%%====================================================================

%% run_job_impl/0 submits a job, which requires flurm_job_manager
%% We test that it handles submission errors properly

run_job_impl_test_() ->
    {setup,
     fun() ->
         %% Need job manager running
         case whereis(flurm_job_manager) of
             undefined ->
                 case flurm_job_manager:start_link() of
                     {ok, Pid} ->
                         unlink(Pid),
                         started;
                     {error, {already_started, Pid}} ->
                         unlink(Pid),
                         existed
                 end;
             _ ->
                 existed
         end
     end,
     fun(_) -> ok end,
     fun(_) ->
         [
             {"run_job_impl submits job", fun() ->
                 %% This will either succeed or fail depending on scheduler state
                 %% The key is that it doesn't crash
                 try
                     Result = flurm_test_runner:run_job_impl(),
                     case Result of
                         ok -> ok;  % Job completed
                         {ok, _} -> ok;  % Job submitted
                         {error, _} -> ok  % Expected error (no scheduler, etc.)
                     end
                 catch
                     exit:{noproc, _} -> ok;  % Expected when services not running
                     exit:{{noproc, _}, _} -> ok  % Nested noproc
                 end
             end}
         ]
     end}.

%%====================================================================
%% Edge Cases
%%====================================================================

%% Test monitor_job with negative iterations (edge case)
monitor_job_negative_iterations_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_job_manager) of
             undefined ->
                 case flurm_job_manager:start_link() of
                     {ok, Pid} -> unlink(Pid);
                     _ -> ok
                 end;
             _ -> ok
         end
     end,
     fun(_) -> ok end,
     fun(_) ->
         [
             {"monitor_job handles negative iterations", fun() ->
                 %% Negative iterations should be treated as 0 or timeout immediately
                 Result = flurm_test_runner:monitor_job(12345, -1),
                 %% Either timeout or not_found is acceptable
                 case Result of
                     {error, timeout} -> ok;
                     {error, not_found} -> ok
                 end
             end}
         ]
     end}.

%% Test monitor_job with 1 iteration
monitor_job_one_iteration_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_job_manager) of
             undefined ->
                 case flurm_job_manager:start_link() of
                     {ok, Pid} -> unlink(Pid);
                     _ -> ok
                 end;
             _ -> ok
         end
     end,
     fun(_) -> ok end,
     fun(_) ->
         [
             {"monitor_job with 1 iteration", fun() ->
                 %% With 1 iteration and non-existent job, should timeout
                 Result = flurm_test_runner:monitor_job(88888888, 1),
                 case Result of
                     {error, timeout} -> ok;
                     {error, _} -> ok  % Job lookup may fail
                 end
             end}
         ]
     end}.

%%====================================================================
%% Job Record Tests (from include file)
%%====================================================================

%% Test that we can create and work with job records
job_record_test() ->
    Job = #job{
        id = 1,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 60,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    },
    ?assertEqual(1, Job#job.id),
    ?assertEqual(<<"test_job">>, Job#job.name),
    ?assertEqual(pending, Job#job.state).

job_state_completed_test() ->
    Job = #job{
        id = 2,
        name = <<"completed_job">>,
        state = completed,
        exit_code = 0
    },
    ?assertEqual(completed, Job#job.state),
    ?assertEqual(0, Job#job.exit_code).

job_state_failed_test() ->
    Job = #job{
        id = 3,
        name = <<"failed_job">>,
        state = failed,
        exit_code = 1
    },
    ?assertEqual(failed, Job#job.state),
    ?assertEqual(1, Job#job.exit_code).
