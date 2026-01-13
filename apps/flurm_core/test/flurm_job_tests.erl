%%%-------------------------------------------------------------------
%%% @doc FLURM Job State Machine Tests
%%%
%%% EUnit tests for the flurm_job gen_statem, flurm_job_sup supervisor,
%%% and flurm_job_registry gen_server.
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
        {"Multiple jobs", fun test_multiple_jobs/0}
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
    %% Unlink before stopping to prevent shutdown propagation to test process
    catch unlink(SupPid),
    catch unlink(RegistryPid),
    %% Stop supervisor and registry properly using gen_server:stop
    catch gen_server:stop(SupPid, shutdown, 5000),
    catch gen_server:stop(RegistryPid, shutdown, 5000),
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

    %% The configuring timeout is 5 minutes by default
    %% For testing, we'll just verify the state and not wait
    %% In production, you'd want shorter timeouts for testing

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
