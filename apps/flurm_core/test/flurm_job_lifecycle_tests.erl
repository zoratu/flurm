%%%-------------------------------------------------------------------
%%% @doc FLURM Job Lifecycle End-to-End Integration Tests
%%%
%%% Tests the complete job lifecycle from submission through completion,
%%% including:
%%% - Full job lifecycle (pending -> configuring -> running -> completing -> completed)
%%% - Job cancellation at various states
%%% - Job timeout handling
%%% - Job dependencies
%%% - Job arrays
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_lifecycle_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

lifecycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Full job lifecycle pending to completed", fun test_full_lifecycle_pending_to_completed/0},
        {"Full job lifecycle with non-zero exit code", fun test_lifecycle_with_failure/0},
        {"Cancel job in pending state", fun test_cancel_in_pending/0},
        {"Cancel job in configuring state", fun test_cancel_in_configuring/0},
        {"Cancel job in running state", fun test_cancel_in_running/0},
        {"Cancel job in completing state", fun test_cancel_in_completing/0},
        {"Job timeout during running", fun test_job_timeout/0},
        {"Node failure during running", fun test_node_failure_during_running/0},
        {"Node failure during configuring", fun test_node_failure_during_configuring/0},
        {"Suspend and resume job", fun test_suspend_and_resume/0},
        {"Preempt job with requeue", fun test_preempt_requeue/0},
        {"Preempt job with cancel", fun test_preempt_cancel/0},
        {"Multiple jobs concurrent lifecycle", fun test_multiple_jobs_lifecycle/0},
        {"Job with afterok dependency", fun test_job_dependency_afterok/0},
        {"Job with afterany dependency", fun test_job_dependency_afterany/0},
        {"Job with afternotok dependency", fun test_job_dependency_afternotok/0},
        {"Job with singleton dependency", fun test_job_singleton_dependency/0},
        {"Job array creation", fun test_job_array_creation/0},
        {"Job array task state tracking", fun test_job_array_task_states/0},
        {"Job array with max concurrent", fun test_job_array_throttling/0},
        {"Job array cancellation", fun test_job_array_cancellation/0},
        {"Priority changes during lifecycle", fun test_priority_changes/0},
        {"State machine handles info messages", fun test_state_machine_info_handling/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),

    %% Start the job registry
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    %% Start the job supervisor
    {ok, SupPid} = flurm_job_sup:start_link(),
    %% Start job dependencies manager
    {ok, DepsPid} = flurm_job_deps:start_link(),
    %% Start job array manager
    {ok, ArrayPid} = flurm_job_array:start_link(),

    #{
        registry => RegistryPid,
        supervisor => SupPid,
        deps => DepsPid,
        array => ArrayPid
    }.

cleanup(#{registry := RegistryPid, supervisor := SupPid, deps := DepsPid, array := ArrayPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Stop processes with proper monitor/wait pattern
    lists:foreach(fun(Pid) ->
        case is_process_alive(Pid) of
            true ->
                Ref = monitor(process, Pid),
                unlink(Pid),
                catch gen_server:stop(Pid, shutdown, 5000),
                receive
                    {'DOWN', Ref, process, Pid, _} -> ok
                after 5000 ->
                    demonitor(Ref, [flush]),
                    catch exit(Pid, kill)
                end;
            false ->
                ok
        end
    end, [ArrayPid, DepsPid, SupPid, RegistryPid]),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_spec() ->
    make_job_spec(#{}).

make_job_spec(Overrides) ->
    Defaults = #{
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #job_spec{
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props),
        priority = maps:get(priority, Props)
    }.

make_base_job() ->
    make_base_job(#{}).

make_base_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        submit_time => erlang:system_time(second)
    },
    Props = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Props),
        name = maps:get(name, Props),
        user = maps:get(user, Props),
        partition = maps:get(partition, Props),
        state = maps:get(state, Props),
        script = maps:get(script, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        submit_time = maps:get(submit_time, Props),
        allocated_nodes = []
    }.

%%====================================================================
%% Full Lifecycle Tests
%%====================================================================

test_full_lifecycle_pending_to_completed() ->
    %% Submit a job
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),

    %% Verify initial state is pending
    {ok, pending} = flurm_job:get_state(Pid),

    %% Verify job info
    {ok, Info1} = flurm_job:get_info(Pid),
    ?assertEqual(pending, maps:get(state, Info1)),
    ?assertEqual([], maps:get(allocated_nodes, Info1)),
    ?assertNotEqual(undefined, maps:get(submit_time, Info1)),

    %% Allocate nodes to move to configuring
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Verify allocation recorded
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(configuring, maps:get(state, Info2)),
    ?assertEqual([<<"node1">>], maps:get(allocated_nodes, Info2)),

    %% Signal config complete to move to running
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Verify start time is set
    {ok, Info3} = flurm_job:get_info(Pid),
    ?assertEqual(running, maps:get(state, Info3)),
    ?assertNotEqual(undefined, maps:get(start_time, Info3)),

    %% Signal job complete with exit code 0
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    %% Verify exit code is recorded
    {ok, Info4} = flurm_job:get_info(Pid),
    ?assertEqual(0, maps:get(exit_code, Info4)),

    %% Signal cleanup complete
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    %% Verify final state
    {ok, Info5} = flurm_job:get_info(Pid),
    ?assertEqual(completed, maps:get(state, Info5)),
    ?assertNotEqual(undefined, maps:get(end_time, Info5)),

    %% Verify terminal state cannot transition
    {error, already_completed} = flurm_job:cancel(Pid),
    {error, job_completed} = flurm_job:allocate(Pid, [<<"node2">>]),
    ok.

test_lifecycle_with_failure() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Signal job complete with non-zero exit code
    ok = flurm_job:signal_job_complete(Pid, 127),
    {ok, completing} = flurm_job:get_state(Pid),

    %% Signal cleanup complete - should go to failed state
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, failed} = flurm_job:get_state(Pid),

    %% Verify exit code
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(127, maps:get(exit_code, Info)),
    ?assertEqual(failed, maps:get(state, Info)),

    %% Verify terminal state behavior
    {error, already_failed} = flurm_job:cancel(Pid),
    ok.

%%====================================================================
%% Cancellation Tests
%%====================================================================

test_cancel_in_pending() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    {ok, pending} = flurm_job:get_state(Pid),

    %% Cancel while pending
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    %% Verify via job ID lookup as well
    {ok, cancelled} = flurm_job:get_state(JobId),

    %% Verify end_time is set
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertNotEqual(undefined, maps:get(end_time, Info)),

    %% Cannot cancel again
    {error, already_cancelled} = flurm_job:cancel(Pid),
    ok.

test_cancel_in_configuring() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to configuring
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Cancel while configuring
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    %% Cannot cancel again
    {error, already_cancelled} = flurm_job:cancel(Pid),
    ok.

test_cancel_in_running() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Cancel while running
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    %% Verify end_time is set
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertNotEqual(undefined, maps:get(end_time, Info)),
    ok.

test_cancel_in_completing() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to completing
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    %% Cancel while completing - acknowledged but stays completing
    ok = flurm_job:cancel(Pid),
    {ok, completing} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Timeout Tests
%%====================================================================

test_job_timeout() ->
    %% Create a job with very short time limit (1 second)
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Wait for timeout (slightly more than 1 second)
    timer:sleep(1500),

    %% Should be in timeout state
    {ok, timeout} = flurm_job:get_state(Pid),

    %% Verify timeout state behavior
    {error, already_timed_out} = flurm_job:cancel(Pid),
    {error, job_timed_out} = flurm_job:allocate(Pid, [<<"node2">>]),

    %% Verify info
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(timeout, maps:get(state, Info)),
    ?assertNotEqual(undefined, maps:get(end_time, Info)),
    ok.

%%====================================================================
%% Node Failure Tests
%%====================================================================

test_node_failure_during_running() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running state
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Node failure for non-allocated node is ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node3">>),
    {ok, running} = flurm_job:get_state(Pid),

    %% Node failure for allocated node transitions to node_fail
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),

    %% Verify terminal state behavior
    {error, already_failed} = flurm_job:cancel(Pid),
    {error, node_failure} = flurm_job:resume(Pid),
    ok.

test_node_failure_during_configuring() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to configuring state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Signal node failure for allocated node
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Suspend/Resume Tests
%%====================================================================

test_suspend_and_resume() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Suspend
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),

    %% Verify info in suspended state
    {ok, Info1} = flurm_job:get_info(Pid),
    ?assertEqual(suspended, maps:get(state, Info1)),

    %% Resume
    ok = flurm_job:resume(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Suspend again and test cancel from suspended
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

test_preempt_requeue() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Verify start_time is set
    {ok, Info1} = flurm_job:get_info(Pid),
    ?assertNotEqual(undefined, maps:get(start_time, Info1)),

    %% Preempt with requeue - should go back to pending
    ok = flurm_job:preempt(Pid, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    %% Verify allocated_nodes cleared and start_time reset
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual([], maps:get(allocated_nodes, Info2)),
    ?assertEqual(undefined, maps:get(start_time, Info2)),

    %% Can be allocated again
    ok = flurm_job:allocate(Pid, [<<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok.

test_preempt_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Preempt with cancel - should go to cancelled
    ok = flurm_job:preempt(Pid, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Multiple Jobs Tests
%%====================================================================

test_multiple_jobs_lifecycle() ->
    %% Submit multiple jobs
    NumJobs = 5,
    Jobs = [begin
        JobSpec = make_job_spec(#{user_id => 1000 + I}),
        {ok, Pid, JobId} = flurm_job:submit(JobSpec),
        {Pid, JobId}
    end || I <- lists:seq(1, NumJobs)],

    %% All should be pending
    lists:foreach(fun({Pid, _JobId}) ->
        {ok, pending} = flurm_job:get_state(Pid)
    end, Jobs),

    %% Move some to different states
    [{Pid1, _}, {Pid2, _}, {Pid3, _}, {Pid4, _}, {Pid5, _}] = Jobs,

    %% Job 1: Complete successfully
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),

    %% Job 2: Cancel
    ok = flurm_job:cancel(Pid2),
    {ok, cancelled} = flurm_job:get_state(Pid2),

    %% Job 3: Fail
    ok = flurm_job:allocate(Pid3, [<<"node2">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:signal_job_complete(Pid3, 1),
    ok = flurm_job:signal_cleanup_complete(Pid3),
    {ok, failed} = flurm_job:get_state(Pid3),

    %% Job 4: Still running
    ok = flurm_job:allocate(Pid4, [<<"node3">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    {ok, running} = flurm_job:get_state(Pid4),

    %% Job 5: Still pending
    {ok, pending} = flurm_job:get_state(Pid5),

    %% Verify registry state counts
    timer:sleep(50),  % Allow time for state updates
    StateCounts = flurm_job_registry:count_by_state(),
    ?assert(maps:get(completed, StateCounts, 0) >= 1),
    ?assert(maps:get(cancelled, StateCounts, 0) >= 1),
    ?assert(maps:get(failed, StateCounts, 0) >= 1),
    ok.

%%====================================================================
%% Job Dependency Tests
%%====================================================================

test_job_dependency_afterok() ->
    %% Submit first job
    JobSpec1 = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),

    %% Submit second job with afterok dependency on first
    JobSpec2 = make_job_spec(),
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec2),

    %% Add dependency: Job2 depends on Job1 completing successfully
    ok = flurm_job_deps:add_dependency(JobId2, afterok, JobId1),

    %% Verify dependency exists
    Deps = flurm_job_deps:get_dependencies(JobId2),
    ?assertEqual(1, length(Deps)),

    %% Job2 dependencies not satisfied yet
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(JobId2)),

    %% Complete Job1 successfully
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),

    %% Notify dependency system
    ok = flurm_job_deps:notify_completion(JobId1, completed),
    timer:sleep(50),  % Allow time for async processing

    %% Job2 dependencies should now be satisfied
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(JobId2)),

    %% Job2 can proceed
    ok = flurm_job:allocate(Pid2, [<<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid2),
    ok.

test_job_dependency_afterany() ->
    %% Submit first job
    JobSpec1 = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),

    %% Submit second job with afterany dependency
    JobSpec2 = make_job_spec(),
    {ok, _Pid2, JobId2} = flurm_job:submit(JobSpec2),

    %% Add dependency: Job2 depends on Job1 ending (any state)
    ok = flurm_job_deps:add_dependency(JobId2, afterany, JobId1),

    %% Dependencies not satisfied
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(JobId2)),

    %% Cancel Job1 (ends but not successful)
    ok = flurm_job:cancel(Pid1),
    {ok, cancelled} = flurm_job:get_state(Pid1),

    %% Notify dependency system
    ok = flurm_job_deps:notify_completion(JobId1, cancelled),
    timer:sleep(50),

    %% Job2 dependencies should be satisfied (afterany accepts any terminal state)
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(JobId2)),
    ok.

test_job_dependency_afternotok() ->
    %% Submit first job
    JobSpec1 = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),

    %% Submit second job with afternotok dependency
    JobSpec2 = make_job_spec(),
    {ok, _Pid2, JobId2} = flurm_job:submit(JobSpec2),

    %% Add dependency: Job2 depends on Job1 failing
    ok = flurm_job_deps:add_dependency(JobId2, afternotok, JobId1),

    %% Dependencies not satisfied
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(JobId2)),

    %% Fail Job1
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 1),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, failed} = flurm_job:get_state(Pid1),

    %% Notify dependency system
    ok = flurm_job_deps:notify_completion(JobId1, failed),
    timer:sleep(50),

    %% Job2 dependencies should be satisfied (afternotok accepts failed state)
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(JobId2)),
    ok.

test_job_singleton_dependency() ->
    %% Submit first job with singleton constraint
    JobSpec1 = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),
    ok = flurm_job_deps:add_dependency(JobId1, singleton, <<"my_singleton">>),

    %% First job gets singleton immediately
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(JobId1)),

    %% Submit second job with same singleton
    JobSpec2 = make_job_spec(),
    {ok, _Pid2, JobId2} = flurm_job:submit(JobSpec2),
    ok = flurm_job_deps:add_dependency(JobId2, singleton, <<"my_singleton">>),

    %% Second job must wait
    ?assertEqual(false, flurm_job_deps:are_dependencies_satisfied(JobId2)),

    %% Complete first job
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),

    %% Notify completion
    ok = flurm_job_deps:notify_completion(JobId1, completed),
    timer:sleep(100),

    %% Second job should now have singleton
    ?assertEqual(true, flurm_job_deps:are_dependencies_satisfied(JobId2)),
    ok.

%%====================================================================
%% Job Array Tests
%%====================================================================

test_job_array_creation() ->
    %% Create a base job for the array
    BaseJob = make_base_job(#{name => <<"array_test">>}),

    %% Create array with range spec
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    ?assert(is_integer(ArrayJobId)),

    %% Verify array job exists using stats
    Stats = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(5, maps:get(total, Stats)),

    %% Verify all tasks exist and are pending
    Tasks = flurm_job_array:get_array_tasks(ArrayJobId),
    ?assertEqual(5, length(Tasks)),
    ?assertEqual(5, maps:get(pending, Stats)),
    ?assertEqual(0, maps:get(running, Stats)),

    %% Get pending tasks
    PendingTasks = flurm_job_array:get_pending_tasks(ArrayJobId),
    ?assertEqual(5, length(PendingTasks)),

    %% Get individual task exists
    {ok, _Task0} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ok.

test_job_array_task_states() ->
    BaseJob = make_base_job(#{name => <<"array_states">>}),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),

    %% Initially all pending
    Stats1 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(3, maps:get(pending, Stats1)),
    ?assertEqual(0, maps:get(running, Stats1)),
    ?assertEqual(0, maps:get(completed, Stats1)),

    %% Start task 0
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1001),
    timer:sleep(20),

    Stats2 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(2, maps:get(pending, Stats2)),
    ?assertEqual(1, maps:get(running, Stats2)),

    %% Complete task 0 successfully
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(20),

    Stats3 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(1, maps:get(completed, Stats3)),
    ?assertEqual(0, maps:get(running, Stats3)),

    %% Start and fail task 1
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1002),
    ok = flurm_job_array:task_completed(ArrayJobId, 1, 1),  % Non-zero exit = failure
    timer:sleep(20),

    Stats4 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(1, maps:get(completed, Stats4)),
    ?assertEqual(1, maps:get(failed, Stats4)),
    ?assertEqual(1, maps:get(pending, Stats4)),
    ok.

test_job_array_throttling() ->
    BaseJob = make_base_job(#{name => <<"array_throttle">>}),
    %% Create array with max 2 concurrent tasks
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-4%2">>),

    %% Verify array job exists
    {ok, _ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),

    %% Can schedule first task
    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),
    ?assertEqual(2, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Start two tasks
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1001),
    timer:sleep(20),
    ?assertEqual(1, flurm_job_array:get_schedulable_count(ArrayJobId)),

    ok = flurm_job_array:task_started(ArrayJobId, 1, 1002),
    timer:sleep(20),

    %% At max concurrent - cannot schedule more
    ?assertEqual(0, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Complete one task
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(20),

    %% Can schedule one more
    ?assertEqual(1, flurm_job_array:get_schedulable_count(ArrayJobId)),
    ok.

test_job_array_cancellation() ->
    BaseJob = make_base_job(#{name => <<"array_cancel">>}),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),

    %% Start one task
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1001),

    %% Cancel individual task
    ok = flurm_job_array:cancel_array_task(ArrayJobId, 1),
    timer:sleep(20),

    %% Verify stats show one cancelled task
    Stats1 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(1, maps:get(cancelled, Stats1)),

    %% Cancel entire array job
    ok = flurm_job_array:cancel_array_job(ArrayJobId),
    timer:sleep(20),

    %% Verify all tasks are cancelled via stats
    Stats2 = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(3, maps:get(cancelled, Stats2)),
    ?assertEqual(0, maps:get(pending, Stats2)),
    ?assertEqual(0, maps:get(running, Stats2)),
    ok.

%%====================================================================
%% Priority Tests
%%====================================================================

test_priority_changes() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Verify initial priority
    {ok, Info1} = flurm_job:get_info(Pid),
    ?assertEqual(100, maps:get(priority, Info1)),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Change priority while running
    ok = flurm_job:set_priority(Pid, 500),
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info2)),

    %% Verify priority clamping to max
    ok = flurm_job:set_priority(Pid, 999999),
    {ok, Info3} = flurm_job:get_info(Pid),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info3)),

    %% Verify priority clamping to min
    ok = flurm_job:set_priority(Pid, -100),
    {ok, Info4} = flurm_job:get_info(Pid),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info4)),

    %% Priority change in suspended state
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:set_priority(Pid, 300),
    {ok, Info5} = flurm_job:get_info(Pid),
    ?assertEqual(300, maps:get(priority, Info5)),
    ok.

%%====================================================================
%% State Machine Edge Cases
%%====================================================================

test_state_machine_info_handling() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Send info messages in various states - should all be ignored
    Pid ! random_info_message,
    timer:sleep(10),
    {ok, pending} = flurm_job:get_state(Pid),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    Pid ! another_info,
    timer:sleep(10),
    {ok, configuring} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_config_complete(Pid),
    Pid ! yet_another_info,
    timer:sleep(10),
    {ok, running} = flurm_job:get_state(Pid),

    %% Also test casts in terminal states
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    gen_statem:cast(Pid, random_cast),
    Pid ! random_info,
    timer:sleep(10),
    {ok, completed} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

%% Test parsing and expansion of array specs
array_spec_parsing_test_() ->
    [
        {"Parse and expand simple range", fun() ->
            {ok, Tasks} = flurm_job_array:expand_array(<<"0-10">>),
            ?assertEqual(11, length(Tasks)),
            %% Check first and last task IDs
            [First | _] = Tasks,
            ?assertEqual(0, maps:get(task_id, First)),
            ?assertEqual(11, maps:get(task_count, First)),
            ?assertEqual(unlimited, maps:get(max_concurrent, First))
        end},
        {"Parse and expand range with step", fun() ->
            {ok, Tasks} = flurm_job_array:expand_array(<<"0-10:2">>),
            %% 0, 2, 4, 6, 8, 10 = 6 tasks
            ?assertEqual(6, length(Tasks)),
            TaskIds = [maps:get(task_id, T) || T <- Tasks],
            ?assertEqual([0, 2, 4, 6, 8, 10], TaskIds)
        end},
        {"Parse and expand range with max concurrent", fun() ->
            {ok, Tasks} = flurm_job_array:expand_array(<<"0-100%10">>),
            ?assertEqual(101, length(Tasks)),
            [First | _] = Tasks,
            ?assertEqual(10, maps:get(max_concurrent, First))
        end},
        {"Parse and expand comma-separated list", fun() ->
            {ok, Tasks} = flurm_job_array:expand_array(<<"1,3,5,7">>),
            ?assertEqual(4, length(Tasks)),
            TaskIds = [maps:get(task_id, T) || T <- Tasks],
            ?assertEqual([1, 3, 5, 7], TaskIds)
        end}
    ].

%% Test dependency spec parsing
dependency_spec_parsing_test_() ->
    [
        {"Parse afterok dependency", fun() ->
            {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:123">>),
            ?assertEqual([{afterok, 123}], Deps)
        end},
        {"Parse afterany dependency", fun() ->
            {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterany:456">>),
            ?assertEqual([{afterany, 456}], Deps)
        end},
        {"Parse singleton dependency", fun() ->
            {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"singleton">>),
            ?assertEqual([{singleton, <<"default">>}], Deps)
        end},
        {"Parse multiple dependencies", fun() ->
            {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<"afterok:1,afterany:2">>),
            ?assertEqual([{afterok, 1}, {afterany, 2}], Deps)
        end},
        {"Parse empty spec", fun() ->
            {ok, Deps} = flurm_job_deps:parse_dependency_spec(<<>>),
            ?assertEqual([], Deps)
        end}
    ].

%% Test circular dependency detection
circular_dependency_test_() ->
    {setup,
     fun() ->
         {ok, DepsPid} = flurm_job_deps:start_link(),
         #{deps => DepsPid}
     end,
     fun(#{deps := DepsPid}) ->
         case is_process_alive(DepsPid) of
             true ->
                 Ref = monitor(process, DepsPid),
                 unlink(DepsPid),
                 catch gen_server:stop(DepsPid, shutdown, 5000),
                 receive
                     {'DOWN', Ref, process, DepsPid, _} -> ok
                 after 5000 ->
                     demonitor(Ref, [flush]),
                     catch exit(DepsPid, kill)
                 end;
             false ->
                 ok
         end
     end,
     fun(_) ->
         {"Detect circular dependency", fun() ->
             %% Add Job1 -> Job2 dependency
             ok = flurm_job_deps:add_dependency(1, afterok, 2),

             %% Adding Job2 -> Job1 would create a cycle
             ?assertEqual(true, flurm_job_deps:has_circular_dependency(2, 1)),

             %% Job3 -> Job1 is fine
             ?assertEqual(false, flurm_job_deps:has_circular_dependency(3, 1))
         end}
     end}.
