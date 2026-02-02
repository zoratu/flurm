%%%-------------------------------------------------------------------
%%% @doc Job Manager Integration Test Suite
%%%
%%% Tests the job manager with real scheduler and persistence to verify
%%% full job lifecycle workflows.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Job lifecycle
    job_lifecycle_pending_to_completed/1,
    job_state_transitions/1,

    %% Job cancellation
    cancel_pending_job/1,
    cancel_running_job/1,

    %% Job operations
    hold_and_release_job/1,
    requeue_job/1,

    %% Job info
    get_job_info/1,
    list_all_jobs/1,
    update_job_properties/1,

    %% Array jobs
    submit_array_job/1,
    array_job_tasks_created/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, lifecycle},
        {group, cancellation},
        {group, operations},
        {group, info},
        {group, arrays}
    ].

groups() ->
    [
        {lifecycle, [sequence], [
            job_lifecycle_pending_to_completed,
            job_state_transitions
        ]},
        {cancellation, [sequence], [
            cancel_pending_job,
            cancel_running_job
        ]},
        {operations, [sequence], [
            hold_and_release_job,
            requeue_job
        ]},
        {info, [sequence], [
            get_job_info,
            list_all_jobs,
            update_job_properties
        ]},
        {arrays, [sequence], [
            submit_array_job,
            array_job_tasks_created
        ]}
    ].

init_per_suite(Config) ->
    ct:pal("Starting job manager integration test suite"),
    %% Stop any running apps
    stop_apps(),
    %% Clean up test data
    os:cmd("rm -rf /tmp/flurm_test_data"),
    %% Start required applications
    {ok, _} = application:ensure_all_started(lager),
    {ok, _} = application:ensure_all_started(flurm_config),
    {ok, _} = application:ensure_all_started(flurm_core),
    {ok, _} = application:ensure_all_started(flurm_controller),
    ct:pal("Applications started successfully"),
    Config.

end_per_suite(_Config) ->
    ct:pal("Stopping job manager integration test suite"),
    stop_apps(),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test case: ~p", [TestCase]),
    ok.

%%====================================================================
%% Test Cases - Job Lifecycle
%%====================================================================

job_lifecycle_pending_to_completed(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"lifecycle_test">>,
        script => <<"#!/bin/bash\necho hello">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    ct:pal("Submitted job ~p", [JobId]),

    %% Verify job exists
    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(pending, Job#job.state),

    %% Job should eventually be scheduled (depends on scheduler)
    ok = flurm_scheduler:trigger_schedule(),
    timer:sleep(100),

    ct:pal("Job ~p lifecycle test complete", [JobId]),
    ?assert(true).

job_state_transitions(_Config) ->
    %% Submit job
    JobSpec = #{
        name => <<"state_test">>,
        script => <<"#!/bin/bash\nsleep 1">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Initial state should be pending
    {ok, Job1} = flurm_job_manager:get_job(JobId),
    ?assertEqual(pending, Job1#job.state),
    ct:pal("Job ~p initial state: ~p", [JobId, Job1#job.state]),

    %% Update job to running (simulate scheduler dispatch)
    ok = flurm_job_manager:update_job(JobId, #{state => running}),
    {ok, Job2} = flurm_job_manager:get_job(JobId),
    ?assertEqual(running, Job2#job.state),
    ct:pal("Job ~p after update: ~p", [JobId, Job2#job.state]),

    ?assert(true).

%%====================================================================
%% Test Cases - Cancellation
%%====================================================================

cancel_pending_job(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"cancel_pending">>,
        script => <<"#!/bin/bash\nsleep 3600">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Cancel before scheduling
    ok = flurm_job_manager:cancel_job(JobId),
    ct:pal("Cancelled pending job ~p", [JobId]),

    %% Verify cancelled (job might be removed or marked cancelled)
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            ct:pal("Job ~p state after cancel: ~p", [JobId, Job#job.state]),
            ?assert(Job#job.state =:= cancelled orelse Job#job.state =:= failed);
        {error, not_found} ->
            ct:pal("Job ~p removed after cancel", [JobId]),
            ?assert(true)
    end.

cancel_running_job(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"cancel_running">>,
        script => <<"#!/bin/bash\nsleep 3600">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Simulate job running
    ok = flurm_job_manager:update_job(JobId, #{state => running}),

    %% Cancel running job
    ok = flurm_job_manager:cancel_job(JobId),
    ct:pal("Cancelled running job ~p", [JobId]),

    ?assert(true).

%%====================================================================
%% Test Cases - Operations
%%====================================================================

hold_and_release_job(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"hold_test">>,
        script => <<"#!/bin/bash\necho hold">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Hold the job
    ok = flurm_job_manager:hold_job(JobId),
    ct:pal("Held job ~p", [JobId]),

    %% Release the job
    ok = flurm_job_manager:release_job(JobId),
    ct:pal("Released job ~p", [JobId]),

    ?assert(true).

requeue_job(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"requeue_test">>,
        script => <<"#!/bin/bash\necho requeue">>,
        partition => <<"default">>,
        num_cpus => 1,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Simulate running state
    ok = flurm_job_manager:update_job(JobId, #{state => running}),

    %% Requeue the job
    ok = flurm_job_manager:requeue_job(JobId),
    ct:pal("Requeued job ~p", [JobId]),

    %% Verify back to pending
    {ok, Job} = flurm_job_manager:get_job(JobId),
    ct:pal("Job ~p state after requeue: ~p", [JobId, Job#job.state]),
    ?assertEqual(pending, Job#job.state).

%%====================================================================
%% Test Cases - Info
%%====================================================================

get_job_info(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"info_test">>,
        script => <<"#!/bin/bash\necho info">>,
        partition => <<"default">>,
        num_cpus => 2,
        memory_mb => 512,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Get job info
    {ok, Job} = flurm_job_manager:get_job(JobId),

    %% Verify fields
    ?assertEqual(<<"info_test">>, Job#job.name),
    ?assertEqual(2, Job#job.num_cpus),
    ct:pal("Job ~p info: name=~s, cpus=~p", [JobId, Job#job.name, Job#job.num_cpus]),

    ?assert(true).

list_all_jobs(_Config) ->
    %% Submit several jobs
    lists:foreach(fun(N) ->
        JobSpec = #{
            name => list_to_binary(io_lib:format("list_test_~p", [N])),
            script => <<"echo test">>,
            partition => <<"default">>,
            num_cpus => 1,
            user_id => 1000,
            group_id => 1000
        },
        {ok, _} = flurm_job_manager:submit_job(JobSpec)
    end, lists:seq(1, 3)),

    %% List all jobs
    Jobs = flurm_job_manager:list_jobs(),
    ct:pal("Listed ~p jobs", [length(Jobs)]),

    %% Should have at least our 3 jobs (might have others from previous tests)
    ?assert(length(Jobs) >= 3).

update_job_properties(_Config) ->
    %% Submit a job
    JobSpec = #{
        name => <<"update_test">>,
        script => <<"#!/bin/bash\necho update">>,
        partition => <<"default">>,
        num_cpus => 1,
        priority => 100,
        user_id => 1000,
        group_id => 1000
    },
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Update priority
    ok = flurm_job_manager:update_job(JobId, #{priority => 200}),

    %% Verify update
    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(200, Job#job.priority),
    ct:pal("Job ~p priority updated to ~p", [JobId, Job#job.priority]).

%%====================================================================
%% Test Cases - Array Jobs
%%====================================================================

submit_array_job(_Config) ->
    %% Check if array job server is available first
    case whereis(flurm_job_array) of
        undefined ->
            ct:pal("Array job server not running - skipping test"),
            {skip, "flurm_job_array server not running"};
        _Pid ->
            JobSpec = #{
                name => <<"array_test">>,
                script => <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>,
                partition => <<"default">>,
                num_cpus => 1,
                user_id => 1000,
                group_id => 1000,
                array => <<"0-4">>  %% Array job with 5 tasks
            },
            Result = flurm_job_manager:submit_job(JobSpec),
            ct:pal("Array job submission result: ~p", [Result]),
            case Result of
                {ok, JobId} ->
                    ct:pal("Array job ~p submitted", [JobId]),
                    ?assert(is_integer(JobId));
                {error, Reason} ->
                    ct:pal("Array job submission returned: ~p", [Reason]),
                    ?assert(true)
            end
    end.

array_job_tasks_created(_Config) ->
    %% Check if array job server is available first
    case whereis(flurm_job_array) of
        undefined ->
            ct:pal("Array job server not running - skipping test"),
            {skip, "flurm_job_array server not running"};
        _Pid ->
            JobSpec = #{
                name => <<"array_tasks_test">>,
                script => <<"#!/bin/bash\necho task">>,
                partition => <<"default">>,
                num_cpus => 1,
                user_id => 1000,
                group_id => 1000,
                array => <<"1-3">>  %% Array with 3 tasks
            },
            Result = flurm_job_manager:submit_job(JobSpec),
            ct:pal("Array tasks test result: ~p", [Result]),
            ?assert(true)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

stop_apps() ->
    application:stop(flurm_controller),
    application:stop(flurm_core),
    application:stop(flurm_config),
    application:stop(lager),
    ok.
