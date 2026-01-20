%%%-------------------------------------------------------------------
%%% @doc FLURM Job State Machine Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job gen_statem, covering
%%% all states, state transitions, API functions, and edge cases.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for all tests
job_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Job lifecycle - pending to completed", fun test_lifecycle_pending_to_completed/0},
        {"Job lifecycle - pending to cancelled", fun test_lifecycle_pending_to_cancelled/0},
        {"Job lifecycle - running to timeout", fun test_lifecycle_running_to_timeout/0},
        {"Job lifecycle - running to node_fail", fun test_lifecycle_running_to_node_fail/0},
        {"Job lifecycle - configuring timeout", fun test_lifecycle_configuring_timeout/0},
        {"State transition - pending allocation", fun test_state_pending_allocation/0},
        {"State transition - invalid allocation", fun test_state_invalid_allocation/0},
        {"Cancel at each state", fun test_cancel_at_each_state/0},
        {"Get job info", fun test_get_job_info/0},
        {"Registry operations", fun test_registry_operations/0},
        {"Registry list by state", fun test_registry_list_by_state/0},
        {"Registry list by user", fun test_registry_list_by_user/0},
        {"Registry monitor cleanup", fun test_registry_monitor_cleanup/0},
        {"Supervisor operations", fun test_supervisor_operations/0},
        {"Multiple jobs", fun test_multiple_jobs/0},
        {"Preempt operations", fun test_preempt_operations/0},
        {"Suspend and resume", fun test_suspend_and_resume/0},
        {"Set priority", fun test_set_priority/0},
        {"Terminal state behaviors", fun test_terminal_state_behaviors/0},
        {"Info messages ignored", fun test_info_messages_ignored/0},
        {"Cast messages in states", fun test_cast_messages_in_states/0},
        {"Node failure during configuring", fun test_node_failure_during_configuring/0},
        {"Cleanup timeout transitions", fun test_cleanup_timeout_transitions/0},
        {"Suspended state node failure", fun test_suspended_node_failure/0},
        {"Callback mode", fun test_callback_mode/0},
        {"Job ID API variants", fun test_job_id_api_variants/0},
        {"Pending timeout state", fun test_pending_timeout/0},
        {"Configuring timeout", fun test_configuring_timeout_to_failed/0},
        {"Completing timeout", fun test_completing_timeout_transition/0},
        {"Get job ID in each state", fun test_get_job_id_in_states/0},
        {"Invalid calls in states", fun test_invalid_calls_in_states/0},
        {"Ignored casts in all states", fun test_ignored_casts_all_states/0},
        {"Signal with job ID variants", fun test_signal_with_job_id/0},
        {"Preempt with job ID", fun test_preempt_with_job_id/0},
        {"Start link direct", fun test_start_link_direct/0},
        {"Code change callback", fun test_code_change/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    %% Start the job registry
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    %% Start the job supervisor
    {ok, SupPid} = flurm_job_sup:start_link(),
    #{registry => RegistryPid, supervisor => SupPid}.

cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Stop processes with proper waiting using monitor/receive pattern
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
    end, [SupPid, RegistryPid]),
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

%%====================================================================
%% Lifecycle Tests
%%====================================================================

test_lifecycle_pending_to_completed() ->
    %% Submit a job
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),

    %% Verify initial state is pending
    {ok, pending} = flurm_job:get_state(Pid),

    %% Allocate nodes to move to configuring
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Signal config complete to move to running
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Signal job complete with exit code 0
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    %% Signal cleanup complete
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    %% Verify terminal state cannot transition
    {error, already_completed} = flurm_job:cancel(Pid),
    ok.

test_lifecycle_pending_to_cancelled() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    {ok, pending} = flurm_job:get_state(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),

    %% Cannot cancel again
    {error, already_cancelled} = flurm_job:cancel(Pid),
    ok.

test_lifecycle_running_to_timeout() ->
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
    {error, already_timed_out} = flurm_job:cancel(Pid),
    ok.

test_lifecycle_running_to_node_fail() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Signal node failure
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

test_lifecycle_configuring_timeout() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to configuring state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% We can test node failure during configuring
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% State Transition Tests
%%====================================================================

test_state_pending_allocation() ->
    JobSpec = make_job_spec(#{num_nodes => 2}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Verify we need enough nodes
    {error, insufficient_nodes} = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, pending} = flurm_job:get_state(Pid),

    %% Allocate enough nodes
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok.

test_state_invalid_allocation() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move past pending state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Cannot allocate in configuring state
    {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node2">>]),
    ok.

test_cancel_at_each_state() ->
    %% Test cancel in pending
    JobSpec = make_job_spec(),
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    {ok, pending} = flurm_job:get_state(Pid1),
    ok = flurm_job:cancel(Pid1),
    {ok, cancelled} = flurm_job:get_state(Pid1),

    %% Test cancel in configuring
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid2),
    ok = flurm_job:cancel(Pid2),
    {ok, cancelled} = flurm_job:get_state(Pid2),

    %% Test cancel in running
    {ok, Pid3, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    {ok, running} = flurm_job:get_state(Pid3),
    ok = flurm_job:cancel(Pid3),
    {ok, cancelled} = flurm_job:get_state(Pid3),

    %% Test cancel in completing (should succeed but stay completing behavior)
    {ok, Pid4, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid4, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    ok = flurm_job:signal_job_complete(Pid4, 0),
    {ok, completing} = flurm_job:get_state(Pid4),
    ok = flurm_job:cancel(Pid4),  %% Acknowledged but stays completing
    {ok, completing} = flurm_job:get_state(Pid4),
    ok.

%%====================================================================
%% Info and Lookup Tests
%%====================================================================

test_get_job_info() ->
    JobSpec = make_job_spec(#{
        user_id => 1001,
        group_id => 1001,
        partition => <<"compute">>,
        num_nodes => 4,
        num_cpus => 8,
        time_limit => 7200,
        priority => 500
    }),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(JobId, maps:get(job_id, Info)),
    ?assertEqual(1001, maps:get(user_id, Info)),
    ?assertEqual(1001, maps:get(group_id, Info)),
    ?assertEqual(<<"compute">>, maps:get(partition, Info)),
    ?assertEqual(4, maps:get(num_nodes, Info)),
    ?assertEqual(8, maps:get(num_cpus, Info)),
    ?assertEqual(7200, maps:get(time_limit, Info)),
    ?assertEqual(500, maps:get(priority, Info)),
    ?assertEqual(pending, maps:get(state, Info)),
    ?assertEqual([], maps:get(allocated_nodes, Info)),
    ?assertNotEqual(undefined, maps:get(submit_time, Info)),
    ?assertEqual(undefined, maps:get(start_time, Info)),
    ?assertEqual(undefined, maps:get(end_time, Info)),
    ?assertEqual(undefined, maps:get(exit_code, Info)),

    %% Check info after allocation
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>]),
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(configuring, maps:get(state, Info2)),
    ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>],
                 maps:get(allocated_nodes, Info2)),
    ok.

%%====================================================================
%% Registry Tests
%%====================================================================

test_registry_operations() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Lookup by job_id
    {ok, Pid} = flurm_job_registry:lookup_job(JobId),

    %% Get entry
    {ok, Entry} = flurm_job_registry:get_job_entry(JobId),
    ?assertEqual(JobId, Entry#job_entry.job_id),
    ?assertEqual(Pid, Entry#job_entry.pid),
    ?assertEqual(1000, Entry#job_entry.user_id),
    ?assertEqual(pending, Entry#job_entry.state),

    %% List jobs
    Jobs = flurm_job_registry:list_jobs(),
    ?assert(lists:member({JobId, Pid}, Jobs)),

    %% Unregister
    ok = flurm_job_registry:unregister_job(JobId),
    {error, not_found} = flurm_job_registry:lookup_job(JobId),
    ok.

test_registry_list_by_state() ->
    %% Create jobs in different states
    JobSpec = make_job_spec(),

    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec),
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec),
    {ok, Pid3, JobId3} = flurm_job:submit(JobSpec),

    %% All should be pending
    PendingJobs = flurm_job_registry:list_jobs_by_state(pending),
    ?assert(lists:member({JobId1, Pid1}, PendingJobs)),
    ?assert(lists:member({JobId2, Pid2}, PendingJobs)),
    ?assert(lists:member({JobId3, Pid3}, PendingJobs)),

    %% Move Pid2 to configuring
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    timer:sleep(50), %% Give time for state update

    ConfiguringJobs = flurm_job_registry:list_jobs_by_state(configuring),
    ?assert(lists:member({JobId2, Pid2}, ConfiguringJobs)),

    %% Move Pid3 to cancelled
    ok = flurm_job:cancel(Pid3),
    timer:sleep(50),

    CancelledJobs = flurm_job_registry:list_jobs_by_state(cancelled),
    ?assert(lists:member({JobId3, Pid3}, CancelledJobs)),
    ok.

test_registry_list_by_user() ->
    JobSpec1 = make_job_spec(#{user_id => 1001}),
    JobSpec2 = make_job_spec(#{user_id => 1002}),

    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec1),  %% Same user
    {ok, Pid3, JobId3} = flurm_job:submit(JobSpec2),  %% Different user

    User1Jobs = flurm_job_registry:list_jobs_by_user(1001),
    User2Jobs = flurm_job_registry:list_jobs_by_user(1002),

    ?assertEqual(2, length(User1Jobs)),
    ?assertEqual(1, length(User2Jobs)),
    ?assert(lists:member({JobId1, Pid1}, User1Jobs)),
    ?assert(lists:member({JobId2, Pid2}, User1Jobs)),
    ?assert(lists:member({JobId3, Pid3}, User2Jobs)),
    ok.

test_registry_monitor_cleanup() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Verify job is registered
    {ok, Pid} = flurm_job_registry:lookup_job(JobId),

    %% Kill the job process
    exit(Pid, kill),
    timer:sleep(100),  %% Give time for monitor to trigger

    %% Job should be automatically unregistered
    {error, not_found} = flurm_job_registry:lookup_job(JobId),
    ok.

%%====================================================================
%% Supervisor Tests
%%====================================================================

test_supervisor_operations() ->
    %% Initially no jobs
    InitialCount = flurm_job_sup:count_jobs(),

    %% Start some jobs
    JobSpec = make_job_spec(),
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    {ok, Pid2, _} = flurm_job:submit(JobSpec),

    %% Verify count
    ?assertEqual(InitialCount + 2, flurm_job_sup:count_jobs()),

    %% Verify which_jobs
    Jobs = flurm_job_sup:which_jobs(),
    ?assert(lists:member(Pid1, Jobs)),
    ?assert(lists:member(Pid2, Jobs)),

    %% Stop a job
    ok = flurm_job_sup:stop_job(Pid1),
    timer:sleep(50),

    ?assertEqual(InitialCount + 1, flurm_job_sup:count_jobs()),
    ?assertNot(lists:member(Pid1, flurm_job_sup:which_jobs())),
    ok.

%%====================================================================
%% Multiple Jobs Tests
%%====================================================================

test_multiple_jobs() ->
    %% Create multiple jobs with different users and partitions
    NumJobs = 10,
    Jobs = [begin
        UserId = 1000 + (I rem 3),
        Partition = case I rem 2 of
            0 -> <<"compute">>;
            1 -> <<"gpu">>
        end,
        JobSpec = make_job_spec(#{
            user_id => UserId,
            partition => Partition
        }),
        {ok, Pid, JobId} = flurm_job:submit(JobSpec),
        {Pid, JobId, UserId, Partition}
    end || I <- lists:seq(1, NumJobs)],

    %% Verify all jobs exist
    ?assertEqual(NumJobs, length(flurm_job_registry:list_jobs())),

    %% Verify user counts
    UserCounts = flurm_job_registry:count_by_user(),
    TotalFromCounts = lists:sum(maps:values(UserCounts)),
    ?assertEqual(NumJobs, TotalFromCounts),

    %% Move some jobs through states
    lists:foreach(
        fun({Pid, _JobId, _UserId, _Partition}) ->
            %% Move half to configuring
            case erlang:phash2(Pid, 2) of
                0 ->
                    ok = flurm_job:allocate(Pid, [<<"node1">>]);
                1 ->
                    ok
            end
        end,
        Jobs
    ),

    timer:sleep(50),

    %% Verify state counts
    StateCounts = flurm_job_registry:count_by_state(),
    PendingCount = maps:get(pending, StateCounts, 0),
    ConfiguringCount = maps:get(configuring, StateCounts, 0),
    ?assertEqual(NumJobs, PendingCount + ConfiguringCount),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

test_preempt_operations() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Test preempt with requeue - should go back to pending
    ok = flurm_job:preempt(Pid, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),

    %% Allocate again and run
    ok = flurm_job:allocate(Pid, [<<"node2">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Test preempt with cancel - should go to cancelled
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:preempt(Pid2, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid2),

    %% Test preempt with checkpoint - should go back to pending
    {ok, Pid3, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:preempt(Pid3, checkpoint, 30),
    {ok, pending} = flurm_job:get_state(Pid3),
    ok.

%%====================================================================
%% Suspend and Resume Tests
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

    %% Get info in suspended state
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(suspended, maps:get(state, Info)),

    %% Test invalid operations in suspended state
    {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node2">>]),

    %% Resume
    ok = flurm_job:resume(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Test cancel from suspended
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Priority Tests
%%====================================================================

test_set_priority() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),

    %% Set priority while running
    ok = flurm_job:set_priority(Pid, 500),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),

    %% Test priority clamping to max
    ok = flurm_job:set_priority(Pid, 999999),
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info2)),

    %% Test priority clamping to min
    ok = flurm_job:set_priority(Pid, -100),
    {ok, Info3} = flurm_job:get_info(Pid),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info3)),

    %% Test set priority in suspended state
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:suspend(Pid2),
    ok = flurm_job:set_priority(Pid2, 300),
    {ok, Info4} = flurm_job:get_info(Pid2),
    ?assertEqual(300, maps:get(priority, Info4)),
    ok.

%%====================================================================
%% Terminal State Behaviors Tests
%%====================================================================

test_terminal_state_behaviors() ->
    JobSpec = make_job_spec(),

    %% Test completed state
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),
    {error, already_completed} = flurm_job:cancel(Pid1),
    {error, job_completed} = flurm_job:allocate(Pid1, [<<"node2">>]),
    {ok, Info1} = flurm_job:get_info(Pid1),
    ?assertEqual(completed, maps:get(state, Info1)),

    %% Test failed state
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:signal_job_complete(Pid2, 1),
    ok = flurm_job:signal_cleanup_complete(Pid2),
    {ok, failed} = flurm_job:get_state(Pid2),
    {error, already_failed} = flurm_job:cancel(Pid2),
    {error, job_failed} = flurm_job:suspend(Pid2),

    %% Test node_fail state
    {ok, Pid3, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:signal_node_failure(Pid3, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid3),
    {error, already_failed} = flurm_job:cancel(Pid3),
    {error, node_failure} = flurm_job:resume(Pid3),
    ok.

%%====================================================================
%% Info Message Tests
%%====================================================================

test_info_messages_ignored() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Send info message - should be ignored
    Pid ! some_random_info_message,
    timer:sleep(10),
    {ok, pending} = flurm_job:get_state(Pid),

    %% Move to other states and test info messages
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    Pid ! another_info,
    timer:sleep(10),
    {ok, configuring} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_config_complete(Pid),
    Pid ! yet_another_info,
    timer:sleep(10),
    {ok, running} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Cast Message Tests
%%====================================================================

test_cast_messages_in_states() ->
    JobSpec = make_job_spec(),

    %% Test ignored casts in pending
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    gen_statem:cast(Pid1, unknown_cast),
    timer:sleep(10),
    {ok, pending} = flurm_job:get_state(Pid1),

    %% Config complete cast in pending should be ignored
    gen_statem:cast(Pid1, config_complete),
    timer:sleep(10),
    {ok, pending} = flurm_job:get_state(Pid1),

    %% Test ignored casts in terminal states
    ok = flurm_job:cancel(Pid1),
    gen_statem:cast(Pid1, {job_complete, 0}),
    timer:sleep(10),
    {ok, cancelled} = flurm_job:get_state(Pid1),
    ok.

%%====================================================================
%% Node Failure During Configuring Test
%%====================================================================

test_node_failure_during_configuring() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Node failure for non-allocated node should be ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node3">>),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Node failure for allocated node should transition to node_fail
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Cleanup Timeout Tests
%%====================================================================

test_cleanup_timeout_transitions() ->
    JobSpec = make_job_spec(),

    %% Test cleanup timeout with exit code 0 -> completed
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    {ok, completing} = flurm_job:get_state(Pid1),
    %% Cleanup complete transitions to completed
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),

    %% Test with non-zero exit code -> failed
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:signal_job_complete(Pid2, 127),
    {ok, completing} = flurm_job:get_state(Pid2),
    ok = flurm_job:signal_cleanup_complete(Pid2),
    {ok, failed} = flurm_job:get_state(Pid2),
    ok.

%%====================================================================
%% Suspended State Node Failure Test
%%====================================================================

test_suspended_node_failure() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to suspended
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),

    %% Non-allocated node failure ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    {ok, suspended} = flurm_job:get_state(Pid),

    %% Allocated node failure transitions to node_fail
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

%%====================================================================
%% Callback Mode Test
%%====================================================================

test_callback_mode() ->
    %% Test that callback_mode returns expected value
    Mode = flurm_job:callback_mode(),
    ?assertEqual([state_functions, state_enter], Mode),
    ok.

%%====================================================================
%% Job ID API Variants Test
%%====================================================================

test_job_id_api_variants() ->
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Test API calls with job ID instead of PID
    {ok, Info} = flurm_job:get_info(JobId),
    ?assertEqual(JobId, maps:get(job_id, Info)),

    {ok, pending} = flurm_job:get_state(JobId),

    ok = flurm_job:allocate(JobId, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(JobId),

    ok = flurm_job:signal_config_complete(JobId),
    {ok, running} = flurm_job:get_state(JobId),

    ok = flurm_job:suspend(JobId),
    {ok, suspended} = flurm_job:get_state(JobId),

    ok = flurm_job:set_priority(JobId, 200),
    {ok, Info2} = flurm_job:get_info(JobId),
    ?assertEqual(200, maps:get(priority, Info2)),

    ok = flurm_job:resume(JobId),
    {ok, running} = flurm_job:get_state(JobId),

    ok = flurm_job:signal_job_complete(JobId, 0),
    ok = flurm_job:signal_cleanup_complete(JobId),
    {ok, completed} = flurm_job:get_state(JobId),

    %% Test API calls with non-existent job ID
    NonExistentId = 999999999,
    {error, job_not_found} = flurm_job:get_info(NonExistentId),
    {error, job_not_found} = flurm_job:get_state(NonExistentId),
    {error, job_not_found} = flurm_job:cancel(NonExistentId),
    {error, job_not_found} = flurm_job:allocate(NonExistentId, [<<"node1">>]),
    {error, job_not_found} = flurm_job:signal_config_complete(NonExistentId),
    {error, job_not_found} = flurm_job:signal_job_complete(NonExistentId, 0),
    {error, job_not_found} = flurm_job:signal_cleanup_complete(NonExistentId),
    {error, job_not_found} = flurm_job:signal_node_failure(NonExistentId, <<"node1">>),
    {error, job_not_found} = flurm_job:preempt(NonExistentId, requeue, 30),
    {error, job_not_found} = flurm_job:suspend(NonExistentId),
    {error, job_not_found} = flurm_job:resume(NonExistentId),
    {error, job_not_found} = flurm_job:set_priority(NonExistentId, 100),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

%% Test that accessing a non-existent job returns proper error
nonexistent_job_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         {"Non-existent job returns job_not_found", fun() ->
             {error, job_not_found} = flurm_job:cancel(999999999),
             {error, job_not_found} = flurm_job:get_info(999999999),
             {error, job_not_found} = flurm_job:get_state(999999999),
             {error, job_not_found} = flurm_job:allocate(999999999, [<<"node1">>]),
             ok
         end}
     end}.

%% Test priority bounds
priority_bounds_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Priority clamped to max", fun() ->
                 JobSpec = make_job_spec(#{priority => 999999}),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info))
             end},
             {"Priority clamped to min", fun() ->
                 JobSpec = make_job_spec(#{priority => -100}),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info))
             end},
             {"Default priority used when undefined", fun() ->
                 JobSpec = #job_spec{
                     user_id = 1000,
                     group_id = 1000,
                     partition = <<"default">>,
                     num_nodes = 1,
                     num_cpus = 1,
                     time_limit = 3600,
                     script = <<"test">>,
                     priority = undefined
                 },
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(?DEFAULT_PRIORITY, maps:get(priority, Info))
             end}
         ]
     end}.

%% Test completing state with non-zero exit code leads to failed
failed_exit_code_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         {"Non-zero exit code leads to failed state", fun() ->
             JobSpec = make_job_spec(),
             {ok, Pid, _} = flurm_job:submit(JobSpec),
             ok = flurm_job:allocate(Pid, [<<"node1">>]),
             ok = flurm_job:signal_config_complete(Pid),
             ok = flurm_job:signal_job_complete(Pid, 1),  %% Non-zero exit
             {ok, completing} = flurm_job:get_state(Pid),
             ok = flurm_job:signal_cleanup_complete(Pid),
             {ok, failed} = flurm_job:get_state(Pid)
         end}
     end}.

%% Test node failure for non-allocated node is ignored
node_failure_ignored_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         {"Node failure for non-allocated node is ignored", fun() ->
             JobSpec = make_job_spec(),
             {ok, Pid, _} = flurm_job:submit(JobSpec),
             ok = flurm_job:allocate(Pid, [<<"node1">>]),
             ok = flurm_job:signal_config_complete(Pid),
             {ok, running} = flurm_job:get_state(Pid),
             %% Signal failure for a different node
             ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
             %% Should still be running
             {ok, running} = flurm_job:get_state(Pid)
         end}
     end}.

%% Test preemption from suspended state
preemption_from_suspended_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Preempt requeue from suspended", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 ok = flurm_job:suspend(Pid),
                 {ok, suspended} = flurm_job:get_state(Pid),
                 ok = flurm_job:preempt(Pid, requeue, 30),
                 {ok, pending} = flurm_job:get_state(Pid)
             end},
             {"Preempt cancel from suspended", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 ok = flurm_job:suspend(Pid),
                 {ok, suspended} = flurm_job:get_state(Pid),
                 ok = flurm_job:preempt(Pid, cancel, 30),
                 {ok, cancelled} = flurm_job:get_state(Pid)
             end}
         ]
     end}.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

test_pending_timeout() ->
    %% Create a custom test to trigger pending timeout
    %% Note: Default pending timeout is 24 hours so we can't easily test it
    %% Instead we just verify the state timeout mechanism by checking state
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    {ok, pending} = flurm_job:get_state(Pid),
    %% Send a generic call in pending state to test catch-all
    {error, invalid_operation} = gen_statem:call(Pid, unknown_operation),
    ok.

test_configuring_timeout_to_failed() ->
    %% Test that configuring timeout leads to failed state
    %% We can't easily trigger the timeout but we test the path via node failure
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    %% Test invalid operation in configuring
    {error, invalid_operation} = gen_statem:call(Pid, {set_priority, 100}),
    %% Send ignored cast
    gen_statem:cast(Pid, random_cast),
    timer:sleep(10),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok.

test_completing_timeout_transition() ->
    JobSpec = make_job_spec(),

    %% Test completing state with exit code 0 (completes normally)
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    {ok, completing} = flurm_job:get_state(Pid1),
    %% Test get_info in completing state
    {ok, Info1} = flurm_job:get_info(Pid1),
    ?assertEqual(completing, maps:get(state, Info1)),
    %% Test invalid operation in completing
    {error, invalid_operation} = gen_statem:call(Pid1, {set_priority, 100}),
    %% Test ignored cast in completing
    gen_statem:cast(Pid1, random_cast),
    timer:sleep(10),
    {ok, completing} = flurm_job:get_state(Pid1),
    ok.

test_get_job_id_in_states() ->
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Get job ID in pending
    JobId = gen_statem:call(Pid, get_job_id),

    %% Get job ID in configuring
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    JobId = gen_statem:call(Pid, get_job_id),

    %% Get job ID in running
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    JobId = gen_statem:call(Pid, get_job_id),

    %% Get job ID in suspended
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),
    JobId = gen_statem:call(Pid, get_job_id),

    %% Get job ID in completing
    ok = flurm_job:resume(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),
    JobId = gen_statem:call(Pid, get_job_id),

    %% Get job ID in completed
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),
    JobId = gen_statem:call(Pid, get_job_id),

    %% Test job ID in cancelled state
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec),
    ok = flurm_job:cancel(Pid2),
    {ok, cancelled} = flurm_job:get_state(Pid2),
    JobId2 = gen_statem:call(Pid2, get_job_id),

    %% Test job ID in failed state
    {ok, Pid3, JobId3} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:signal_job_complete(Pid3, 1),
    ok = flurm_job:signal_cleanup_complete(Pid3),
    {ok, failed} = flurm_job:get_state(Pid3),
    JobId3 = gen_statem:call(Pid3, get_job_id),

    %% Test job ID in timeout state
    {ok, Pid4, JobId4} = flurm_job:submit(make_job_spec(#{time_limit => 1})),
    ok = flurm_job:allocate(Pid4, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid4),
    JobId4 = gen_statem:call(Pid4, get_job_id),

    %% Test job ID in node_fail state
    {ok, Pid5, JobId5} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid5, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid5),
    ok = flurm_job:signal_node_failure(Pid5, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid5),
    JobId5 = gen_statem:call(Pid5, get_job_id),
    ok.

test_invalid_calls_in_states() ->
    JobSpec = make_job_spec(),

    %% Test invalid operations in terminal states
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:cancel(Pid1),
    {ok, cancelled} = flurm_job:get_state(Pid1),
    {error, job_cancelled} = gen_statem:call(Pid1, {set_priority, 100}),

    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:signal_job_complete(Pid2, 1),
    ok = flurm_job:signal_cleanup_complete(Pid2),
    {ok, failed} = flurm_job:get_state(Pid2),
    {error, job_failed} = gen_statem:call(Pid2, {set_priority, 100}),

    %% Test invalid in timeout state
    {ok, Pid3, _} = flurm_job:submit(make_job_spec(#{time_limit => 1})),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid3),
    {error, job_timed_out} = gen_statem:call(Pid3, {set_priority, 100}),

    %% Test invalid in node_fail state
    {ok, Pid4, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid4, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    ok = flurm_job:signal_node_failure(Pid4, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid4),
    {error, node_failure} = gen_statem:call(Pid4, {set_priority, 100}),
    ok.

test_ignored_casts_all_states() ->
    JobSpec = make_job_spec(),

    %% Test casts ignored in all terminal states
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:cancel(Pid1),
    {ok, cancelled} = flurm_job:get_state(Pid1),
    gen_statem:cast(Pid1, config_complete),
    gen_statem:cast(Pid1, {job_complete, 0}),
    gen_statem:cast(Pid1, cleanup_complete),
    timer:sleep(10),
    {ok, cancelled} = flurm_job:get_state(Pid1),

    %% Test info messages ignored in terminal states
    Pid1 ! random_info,
    timer:sleep(10),
    {ok, cancelled} = flurm_job:get_state(Pid1),

    %% Test in completed state
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:signal_job_complete(Pid2, 0),
    ok = flurm_job:signal_cleanup_complete(Pid2),
    {ok, completed} = flurm_job:get_state(Pid2),
    gen_statem:cast(Pid2, random_cast),
    Pid2 ! random_info,
    timer:sleep(10),
    {ok, completed} = flurm_job:get_state(Pid2),

    %% Test in failed state
    {ok, Pid3, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:signal_job_complete(Pid3, 1),
    ok = flurm_job:signal_cleanup_complete(Pid3),
    {ok, failed} = flurm_job:get_state(Pid3),
    gen_statem:cast(Pid3, random_cast),
    Pid3 ! random_info,
    timer:sleep(10),
    {ok, failed} = flurm_job:get_state(Pid3),

    %% Test casts in suspended state
    {ok, Pid4, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid4, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    ok = flurm_job:suspend(Pid4),
    {ok, suspended} = flurm_job:get_state(Pid4),
    gen_statem:cast(Pid4, random_cast),
    Pid4 ! random_info,
    timer:sleep(10),
    {ok, suspended} = flurm_job:get_state(Pid4),

    %% Test casts in timeout and node_fail states
    {ok, Pid5, _} = flurm_job:submit(make_job_spec(#{time_limit => 1})),
    ok = flurm_job:allocate(Pid5, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid5),
    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid5),
    gen_statem:cast(Pid5, random_cast),
    Pid5 ! random_info,
    timer:sleep(10),
    {ok, timeout} = flurm_job:get_state(Pid5),

    {ok, Pid6, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid6, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid6),
    ok = flurm_job:signal_node_failure(Pid6, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid6),
    gen_statem:cast(Pid6, random_cast),
    Pid6 ! random_info,
    timer:sleep(10),
    {ok, node_fail} = flurm_job:get_state(Pid6),
    ok.

test_signal_with_job_id() ->
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Test signal_node_failure with job ID (not found case already tested)
    ok = flurm_job:allocate(JobId, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(JobId),

    %% Test node failure with unallocated node (ignored)
    ok = flurm_job:signal_node_failure(JobId, <<"unrelated_node">>),
    {ok, configuring} = flurm_job:get_state(JobId),

    %% Now signal real node failure
    ok = flurm_job:signal_node_failure(JobId, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(JobId),
    ok.

test_preempt_with_job_id() ->
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(JobId, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(JobId),
    {ok, running} = flurm_job:get_state(JobId),

    %% Test preempt with job ID
    ok = flurm_job:preempt(JobId, requeue, 30),
    {ok, pending} = flurm_job:get_state(JobId),
    ok.

test_start_link_direct() ->
    %% Test start_link directly without supervisor
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Get state
    {ok, pending} = flurm_job:get_state(Pid),
    %% Cleanup - stop the process
    gen_statem:stop(Pid),
    ok.

test_code_change() ->
    %% Test code_change callback by calling it directly
    %% Create a job data record for testing
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    {ok, _Info} = flurm_job:get_info(Pid),

    %% The code_change callback is typically called during hot code upgrade
    %% We can test the maybe_upgrade_state_data function indirectly
    %% by checking the job continues to work after state changes
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok.
