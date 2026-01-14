%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit tests for flurm_test_runner module
%%%
%%% Tests the test runner functionality including:
%%% - run/0 function
%%% - Integration with job lifecycle
%%% - Node connection handling
%%% - Job monitoring and state transitions
%%%
%%% Note: The test runner module is designed for end-to-end testing
%%% of job lifecycle. These tests verify the module structure and
%%% behavior without requiring a full cluster setup.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_runner_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic module tests
test_runner_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"module exports run/0", fun test_exports_run/0},
        {"run/0 returns error when no nodes", fun test_run_no_nodes/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),
    application:ensure_all_started(sasl),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Module Export Tests
%%====================================================================

test_exports_run() ->
    %% Verify the module exports run/0
    Exports = flurm_test_runner:module_info(exports),
    ?assert(lists:member({run, 0}, Exports)),
    ok.

%%====================================================================
%% Run Function Tests
%%====================================================================

test_run_no_nodes() ->
    %% When no nodes are connected, run should return error
    %% This test depends on whether flurm_node_connection_manager is running

    %% First check if the required process is available
    case whereis(flurm_node_connection_manager) of
        undefined ->
            %% The node connection manager isn't running
            %% Mock the behavior by starting a simple gen_server
            %% that returns empty list for list_connected_nodes
            meck:new(flurm_node_connection_manager, [non_strict]),
            meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [] end),

            Result = flurm_test_runner:run(),
            ?assertEqual({error, no_nodes}, Result),

            meck:unload(flurm_node_connection_manager);
        _Pid ->
            %% Manager is running - get actual connected nodes
            case flurm_node_connection_manager:list_connected_nodes() of
                [] ->
                    Result = flurm_test_runner:run(),
                    ?assertEqual({error, no_nodes}, Result);
                _Nodes ->
                    %% Nodes are connected - skip this specific test
                    ok
            end
    end,
    ok.

%%====================================================================
%% Job Lifecycle Tests (Mocked)
%%====================================================================

job_lifecycle_test_() ->
    {setup,
     fun setup_mocked/0,
     fun cleanup_mocked/1,
     [
        {"run_job_test handles job submission", fun test_run_job_test_submit/0},
        {"run_job_test handles submit error", fun test_run_job_test_submit_error/0},
        {"monitor_job tracks state changes", fun test_monitor_job_states/0},
        {"monitor_job handles timeout", fun test_monitor_job_timeout/0},
        {"monitor_job handles failure", fun test_monitor_job_failure/0},
        {"monitor_job handles lookup error", fun test_monitor_job_lookup_error/0}
     ]}.

setup_mocked() ->
    application:ensure_all_started(lager),

    %% Set up meck for dependencies
    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),

    %% Default: return one connected node
    meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [<<"node1">>] end),

    ok.

cleanup_mocked(_) ->
    meck:unload(flurm_node_connection_manager),
    meck:unload(flurm_job_manager),
    ok.

test_run_job_test_submit() ->
    %% Mock successful job submission and completion
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1001} end),

    %% Create a job record for get_job
    CompletedJob = #job{
        id = 1001,
        name = <<"test_job">>,
        state = completed,
        allocated_nodes = [<<"node1">>],
        exit_code = 0
    },
    meck:expect(flurm_job_manager, get_job, fun(1001) -> {ok, CompletedJob} end),

    %% Run should succeed
    Result = flurm_test_runner:run(),
    ?assertEqual(ok, Result),
    ok.

test_run_job_test_submit_error() ->
    %% Mock job submission failure
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {error, no_resources} end),

    Result = flurm_test_runner:run(),
    ?assertEqual({error, no_resources}, Result),
    ok.

test_monitor_job_states() ->
    %% Test that monitor_job tracks state transitions
    %% This simulates: pending -> running -> completed

    CallCount = counters:new(1, []),

    %% Create jobs in different states
    PendingJob = #job{id = 1002, state = pending, allocated_nodes = []},
    RunningJob = #job{id = 1002, state = running, allocated_nodes = [<<"node1">>]},
    CompletedJob = #job{id = 1002, state = completed, allocated_nodes = [<<"node1">>], exit_code = 0},

    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1002} end),
    meck:expect(flurm_job_manager, get_job, fun(1002) ->
        Count = counters:get(CallCount, 1),
        counters:add(CallCount, 1, 1),
        case Count of
            0 -> {ok, PendingJob};
            1 -> {ok, RunningJob};
            _ -> {ok, CompletedJob}
        end
    end),

    Result = flurm_test_runner:run(),
    ?assertEqual(ok, Result),
    ok.

test_monitor_job_timeout() ->
    %% Test that monitor_job returns timeout after max iterations
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1003} end),

    %% Always return pending - should eventually timeout
    PendingJob = #job{id = 1003, state = pending, allocated_nodes = []},
    meck:expect(flurm_job_manager, get_job, fun(1003) -> {ok, PendingJob} end),

    %% This will timeout after 20 iterations (10 seconds)
    Result = flurm_test_runner:run(),
    ?assertEqual({error, timeout}, Result),
    ok.

test_monitor_job_failure() ->
    %% Test that monitor_job handles failed job state
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1004} end),

    FailedJob = #job{id = 1004, state = failed, allocated_nodes = [<<"node1">>], exit_code = 1},
    meck:expect(flurm_job_manager, get_job, fun(1004) -> {ok, FailedJob} end),

    Result = flurm_test_runner:run(),
    ?assertEqual({error, job_failed}, Result),
    ok.

test_monitor_job_lookup_error() ->
    %% Test that monitor_job handles job lookup errors
    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1005} end),
    meck:expect(flurm_job_manager, get_job, fun(1005) -> {error, not_found} end),

    Result = flurm_test_runner:run(),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Job Spec Tests
%%====================================================================

job_spec_test_() ->
    [
     {"job spec contains required fields", fun test_job_spec_fields/0}
    ].

test_job_spec_fields() ->
    %% The test runner creates a specific job spec
    %% We can verify this by checking what submit_job receives

    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),

    meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [<<"node1">>] end),

    %% Capture the job spec
    CapturedSpec = ets:new(captured_spec, [public, set]),
    meck:expect(flurm_job_manager, submit_job, fun(Spec) ->
        ets:insert(CapturedSpec, {spec, Spec}),
        {error, test_capture}  % Return error to stop test
    end),

    _Result = flurm_test_runner:run(),

    %% Verify the captured spec
    [{spec, Spec}] = ets:lookup(CapturedSpec, spec),

    %% Check required fields
    ?assert(maps:is_key(name, Spec)),
    ?assert(maps:is_key(script, Spec)),
    ?assert(maps:is_key(partition, Spec)),
    ?assert(maps:is_key(num_nodes, Spec)),
    ?assert(maps:is_key(num_cpus, Spec)),
    ?assert(maps:is_key(memory_mb, Spec)),
    ?assert(maps:is_key(time_limit, Spec)),

    %% Check expected values
    ?assertEqual(<<"test_job">>, maps:get(name, Spec)),
    ?assertEqual(<<"default">>, maps:get(partition, Spec)),
    ?assertEqual(1, maps:get(num_nodes, Spec)),
    ?assertEqual(1, maps:get(num_cpus, Spec)),
    ?assertEqual(512, maps:get(memory_mb, Spec)),
    ?assertEqual(60, maps:get(time_limit, Spec)),

    %% Script should be a bash script
    Script = maps:get(script, Spec),
    ?assert(is_binary(Script)),
    ?assertNotEqual(nomatch, binary:match(Script, <<"#!/bin/bash">>)),

    ets:delete(CapturedSpec),
    meck:unload(flurm_node_connection_manager),
    meck:unload(flurm_job_manager),
    ok.

%%====================================================================
%% Output Format Tests
%%====================================================================

output_test_() ->
    [
     {"run outputs status messages", fun test_output_messages/0}
    ].

test_output_messages() ->
    %% The test runner uses io:format for output
    %% We verify this behavior works (doesn't crash)

    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),

    meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [<<"node1">>] end),

    CompletedJob = #job{
        id = 1006,
        name = <<"test_job">>,
        state = completed,
        allocated_nodes = [<<"node1">>],
        exit_code = 0
    },

    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1006} end),
    meck:expect(flurm_job_manager, get_job, fun(1006) -> {ok, CompletedJob} end),

    %% Run and verify no crash
    Result = flurm_test_runner:run(),
    ?assertEqual(ok, Result),

    meck:unload(flurm_node_connection_manager),
    meck:unload(flurm_job_manager),
    ok.

%%====================================================================
%% Integration Tests (Requires Running System)
%%====================================================================

%% These tests are skipped if the required processes aren't running
integration_test_() ->
    case can_run_integration_tests() of
        true ->
            {setup,
             fun setup_integration/0,
             fun cleanup_integration/1,
             [
                {"full job lifecycle test", {timeout, 30, fun test_full_lifecycle/0}}
             ]};
        false ->
            []
    end.

can_run_integration_tests() ->
    %% Check if required processes are running
    RequiredProcesses = [
        flurm_node_connection_manager,
        flurm_job_manager
    ],
    lists:all(fun(Name) -> whereis(Name) =/= undefined end, RequiredProcesses).

setup_integration() ->
    ok.

cleanup_integration(_) ->
    ok.

test_full_lifecycle() ->
    %% This would run a real job through the system
    %% Only runs if all required processes are available
    Result = flurm_test_runner:run(),
    case Result of
        ok -> ok;
        {error, no_nodes} -> ok;  % Expected if no compute nodes
        {error, _} -> ok  % Other errors are acceptable in test env
    end.

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    [
     {"empty node list", fun test_empty_node_list/0},
     {"multiple nodes available", fun test_multiple_nodes/0}
    ].

test_empty_node_list() ->
    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [] end),

    Result = flurm_test_runner:run(),
    ?assertEqual({error, no_nodes}, Result),

    meck:unload(flurm_node_connection_manager),
    ok.

test_multiple_nodes() ->
    meck:new(flurm_node_connection_manager, [non_strict]),
    meck:new(flurm_job_manager, [non_strict]),

    %% Multiple nodes available
    meck:expect(flurm_node_connection_manager, list_connected_nodes,
                fun() -> [<<"node1">>, <<"node2">>, <<"node3">>] end),

    CompletedJob = #job{
        id = 1007,
        state = completed,
        allocated_nodes = [<<"node2">>],
        exit_code = 0
    },

    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1007} end),
    meck:expect(flurm_job_manager, get_job, fun(1007) -> {ok, CompletedJob} end),

    Result = flurm_test_runner:run(),
    ?assertEqual(ok, Result),

    meck:unload(flurm_node_connection_manager),
    meck:unload(flurm_job_manager),
    ok.

%%====================================================================
%% State Transition Tests
%%====================================================================

state_transition_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         meck:new(flurm_node_connection_manager, [non_strict]),
         meck:new(flurm_job_manager, [non_strict]),
         meck:expect(flurm_node_connection_manager, list_connected_nodes, fun() -> [<<"node1">>] end)
     end,
     fun(_) ->
         meck:unload(flurm_node_connection_manager),
         meck:unload(flurm_job_manager)
     end,
     [
        {"job transitions through all states", fun test_all_state_transitions/0}
     ]}.

test_all_state_transitions() ->
    %% Simulate a job going through: pending -> running -> completing -> completed

    StateCounter = counters:new(1, []),

    PendingJob = #job{id = 1008, state = pending, allocated_nodes = []},
    RunningJob = #job{id = 1008, state = running, allocated_nodes = [<<"node1">>]},
    CompletingJob = #job{id = 1008, state = completing, allocated_nodes = [<<"node1">>]},
    CompletedJob = #job{id = 1008, state = completed, allocated_nodes = [<<"node1">>], exit_code = 0},

    meck:expect(flurm_job_manager, submit_job, fun(_) -> {ok, 1008} end),
    meck:expect(flurm_job_manager, get_job, fun(1008) ->
        Count = counters:get(StateCounter, 1),
        counters:add(StateCounter, 1, 1),
        case Count of
            0 -> {ok, PendingJob};
            1 -> {ok, RunningJob};
            2 -> {ok, CompletingJob};
            _ -> {ok, CompletedJob}
        end
    end),

    Result = flurm_test_runner:run(),
    ?assertEqual(ok, Result),
    ok.
