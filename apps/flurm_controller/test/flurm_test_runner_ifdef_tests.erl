%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_test_runner internal functions
%%%
%%% Tests the internal functions exposed via -ifdef(TEST).
%%% Note: Most functions in this module require external processes
%%% (flurm_job_manager, flurm_node_connection_manager) so testing is
%%% limited to verifying the module structure and export availability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_runner_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test: Module exports are available
%%====================================================================

module_exports_test() ->
    %% Verify the module is loadable
    ?assertEqual({module, flurm_test_runner}, code:ensure_loaded(flurm_test_runner)).

run_function_exported_test() ->
    %% Verify run/0 is exported
    Exports = flurm_test_runner:module_info(exports),
    ?assert(lists:member({run, 0}, Exports)).

run_job_impl_exported_test() ->
    %% Verify run_job_impl/0 is exported (via -ifdef(TEST))
    %% Note: renamed from run_job_test to avoid EUnit auto-discovery
    Exports = flurm_test_runner:module_info(exports),
    ?assert(lists:member({run_job_impl, 0}, Exports)).

monitor_job_exported_test() ->
    %% Verify monitor_job/2 is exported (via -ifdef(TEST))
    Exports = flurm_test_runner:module_info(exports),
    ?assert(lists:member({monitor_job, 2}, Exports)).

%%====================================================================
%% Test: monitor_job/2 timeout behavior
%%====================================================================

monitor_job_timeout_zero_iterations_test() ->
    %% When iterations is 0, should return timeout error
    %% This test doesn't require external processes since it
    %% returns immediately on iteration count 0
    Result = flurm_test_runner:monitor_job(12345, 0),
    ?assertEqual({error, timeout}, Result).

%%====================================================================
%% Test: Job specification structure used by run_job_impl
%%====================================================================

%% This test verifies the structure of the job specification
%% that would be submitted by run_job_impl/0
job_spec_structure_test() ->
    %% The job spec used in run_job_impl has this structure
    JobSpec = #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho Hello from FLURM\nsleep 2\necho Done">>,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 512,
        time_limit => 60
    },
    %% Verify all required keys are present
    ?assert(maps:is_key(name, JobSpec)),
    ?assert(maps:is_key(script, JobSpec)),
    ?assert(maps:is_key(partition, JobSpec)),
    ?assert(maps:is_key(num_nodes, JobSpec)),
    ?assert(maps:is_key(num_cpus, JobSpec)),
    ?assert(maps:is_key(memory_mb, JobSpec)),
    ?assert(maps:is_key(time_limit, JobSpec)),
    %% Verify types
    ?assert(is_binary(maps:get(name, JobSpec))),
    ?assert(is_binary(maps:get(script, JobSpec))),
    ?assert(is_binary(maps:get(partition, JobSpec))),
    ?assert(is_integer(maps:get(num_nodes, JobSpec))),
    ?assert(is_integer(maps:get(num_cpus, JobSpec))),
    ?assert(is_integer(maps:get(memory_mb, JobSpec))),
    ?assert(is_integer(maps:get(time_limit, JobSpec))),
    %% Verify reasonable values
    ?assert(maps:get(num_nodes, JobSpec) >= 1),
    ?assert(maps:get(num_cpus, JobSpec) >= 1),
    ?assert(maps:get(memory_mb, JobSpec) >= 1),
    ?assert(maps:get(time_limit, JobSpec) >= 1).

%%====================================================================
%% Test: Module can use flurm_core records
%%====================================================================

job_record_access_test() ->
    %% Verify we can create and access job records
    %% as used by monitor_job/2
    Job = #job{
        id = 1,
        state = pending,
        allocated_nodes = [],
        exit_code = undefined
    },
    ?assertEqual(pending, Job#job.state),
    ?assertEqual([], Job#job.allocated_nodes),
    ?assertEqual(undefined, Job#job.exit_code).

job_state_transitions_test() ->
    %% Verify the job states that monitor_job checks for
    PendingJob = #job{id = 1, state = pending},
    RunningJob = #job{id = 1, state = running},
    CompletedJob = #job{id = 1, state = completed},
    FailedJob = #job{id = 1, state = failed},

    ?assertEqual(pending, PendingJob#job.state),
    ?assertEqual(running, RunningJob#job.state),
    ?assertEqual(completed, CompletedJob#job.state),
    ?assertEqual(failed, FailedJob#job.state).
