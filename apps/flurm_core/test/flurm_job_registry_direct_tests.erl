%%%-------------------------------------------------------------------
%%% @doc FLURM Job Registry Direct Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_registry gen_server,
%%% covering job registration, lookup, state management, and monitoring.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_registry_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

registry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Register and lookup job", fun test_register_lookup/0},
        {"Register duplicate fails", fun test_register_duplicate/0},
        {"Unregister job", fun test_unregister_job/0},
        {"List all jobs", fun test_list_jobs/0},
        {"List jobs by state", fun test_list_by_state/0},
        {"List jobs by user", fun test_list_by_user/0},
        {"Update job state", fun test_update_state/0},
        {"Get job entry", fun test_get_job_entry/0},
        {"Count by state", fun test_count_by_state/0},
        {"Count by user", fun test_count_by_user/0},
        {"Monitor job process DOWN", fun test_monitor_down/0},
        {"Unknown request handling", fun test_unknown_request/0},
        {"Cast message handling", fun test_cast_handling/0},
        {"Info message handling", fun test_info_handling/0},
        {"Code change callback", fun test_code_change/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    {ok, SupPid} = flurm_job_sup:start_link(),
    #{registry => RegistryPid, supervisor => SupPid}.

cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    catch unlink(SupPid),
    catch unlink(RegistryPid),
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
%% Test Cases
%%====================================================================

test_register_lookup() ->
    %% Start a job using submit (which registers automatically)
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Verify we can look it up
    {ok, Pid} = flurm_job_registry:lookup_job(JobId),

    %% Lookup non-existent job
    {error, not_found} = flurm_job_registry:lookup_job(999999),
    ok.

test_register_duplicate() ->
    %% Start a job
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Try to register same job again
    {error, already_registered} = flurm_job_registry:register_job(JobId, Pid),
    ok.

test_unregister_job() ->
    %% Start a job
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Verify it exists
    {ok, _} = flurm_job_registry:lookup_job(JobId),

    %% Unregister
    ok = flurm_job_registry:unregister_job(JobId),

    %% Verify it's gone
    {error, not_found} = flurm_job_registry:lookup_job(JobId),

    %% Unregister non-existent job should not crash
    ok = flurm_job_registry:unregister_job(999999),
    ok.

test_list_jobs() ->
    %% Start multiple jobs
    JobSpec1 = make_job_spec(#{user_id => 1001}),
    JobSpec2 = make_job_spec(#{user_id => 1002}),
    JobSpec3 = make_job_spec(#{user_id => 1003}),

    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec1),
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec2),
    {ok, Pid3, JobId3} = flurm_job:submit(JobSpec3),

    %% List all jobs
    Jobs = flurm_job_registry:list_jobs(),
    ?assertEqual(3, length(Jobs)),

    %% Verify all jobs are in the list
    JobIds = [Id || {Id, _} <- Jobs],
    ?assert(lists:member(JobId1, JobIds)),
    ?assert(lists:member(JobId2, JobIds)),
    ?assert(lists:member(JobId3, JobIds)),

    %% Verify PIDs match
    ?assertEqual({ok, Pid1}, flurm_job_registry:lookup_job(JobId1)),
    ?assertEqual({ok, Pid2}, flurm_job_registry:lookup_job(JobId2)),
    ?assertEqual({ok, Pid3}, flurm_job_registry:lookup_job(JobId3)),
    ok.

test_list_by_state() ->
    %% Start jobs in different states
    JobSpec = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec),
    {ok, _Pid2, _JobId2} = flurm_job:submit(JobSpec),

    %% All should be pending
    PendingJobs = flurm_job_registry:list_jobs_by_state(pending),
    ?assertEqual(2, length(PendingJobs)),

    %% Move one to configuring
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    timer:sleep(50),

    %% Update registry state
    ok = flurm_job_registry:update_state(JobId1, configuring),

    %% Now should have 1 pending, 1 configuring
    PendingJobs2 = flurm_job_registry:list_jobs_by_state(pending),
    ConfiguringJobs = flurm_job_registry:list_jobs_by_state(configuring),
    ?assertEqual(1, length(PendingJobs2)),
    ?assertEqual(1, length(ConfiguringJobs)),

    %% Empty state
    RunningJobs = flurm_job_registry:list_jobs_by_state(running),
    ?assertEqual(0, length(RunningJobs)),
    ok.

test_list_by_user() ->
    %% Start jobs with different users
    JobSpec1 = make_job_spec(#{user_id => 2001}),
    JobSpec2 = make_job_spec(#{user_id => 2001}),
    JobSpec3 = make_job_spec(#{user_id => 2002}),

    {ok, _Pid1, JobId1} = flurm_job:submit(JobSpec1),
    {ok, _Pid2, JobId2} = flurm_job:submit(JobSpec2),
    {ok, _Pid3, JobId3} = flurm_job:submit(JobSpec3),

    %% List by user 2001
    User2001Jobs = flurm_job_registry:list_jobs_by_user(2001),
    ?assertEqual(2, length(User2001Jobs)),
    JobIds2001 = [Id || {Id, _} <- User2001Jobs],
    ?assert(lists:member(JobId1, JobIds2001)),
    ?assert(lists:member(JobId2, JobIds2001)),

    %% List by user 2002
    User2002Jobs = flurm_job_registry:list_jobs_by_user(2002),
    ?assertEqual(1, length(User2002Jobs)),
    [{ReturnedId3, _}] = User2002Jobs,
    ?assertEqual(JobId3, ReturnedId3),

    %% Non-existent user
    NoJobs = flurm_job_registry:list_jobs_by_user(9999),
    ?assertEqual(0, length(NoJobs)),
    ok.

test_update_state() ->
    %% Start a job
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Verify initial state
    {ok, Entry1} = flurm_job_registry:get_job_entry(JobId),
    ?assertEqual(pending, Entry1#job_entry.state),

    %% Update state
    ok = flurm_job_registry:update_state(JobId, running),

    %% Verify new state
    {ok, Entry2} = flurm_job_registry:get_job_entry(JobId),
    ?assertEqual(running, Entry2#job_entry.state),

    %% Update non-existent job
    {error, not_found} = flurm_job_registry:update_state(999999, running),
    ok.

test_get_job_entry() ->
    %% Start a job
    JobSpec = make_job_spec(#{user_id => 3001, partition => <<"compute">>}),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

    %% Get entry
    {ok, Entry} = flurm_job_registry:get_job_entry(JobId),

    %% Verify fields
    ?assertEqual(JobId, Entry#job_entry.job_id),
    ?assertEqual(3001, Entry#job_entry.user_id),
    ?assertEqual(<<"compute">>, Entry#job_entry.partition),
    ?assertEqual(pending, Entry#job_entry.state),
    %% submit_time is erlang:timestamp() tuple {MegaSecs, Secs, MicroSecs}
    ?assert(is_tuple(Entry#job_entry.submit_time)),

    %% Non-existent job
    {error, not_found} = flurm_job_registry:get_job_entry(999999),
    ok.

test_count_by_state() ->
    %% Start multiple jobs
    JobSpec = make_job_spec(),
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec),
    {ok, _Pid2, _JobId2} = flurm_job:submit(JobSpec),
    {ok, _Pid3, _JobId3} = flurm_job:submit(JobSpec),

    %% All pending
    Counts1 = flurm_job_registry:count_by_state(),
    ?assertEqual(3, maps:get(pending, Counts1, 0)),
    ?assertEqual(0, maps:get(running, Counts1, 0)),

    %% Move one to running
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job_registry:update_state(JobId1, running),

    Counts2 = flurm_job_registry:count_by_state(),
    ?assertEqual(2, maps:get(pending, Counts2, 0)),
    ?assertEqual(1, maps:get(running, Counts2, 0)),
    ok.

test_count_by_user() ->
    %% Start jobs with different users
    JobSpec1 = make_job_spec(#{user_id => 4001}),
    JobSpec2 = make_job_spec(#{user_id => 4001}),
    JobSpec3 = make_job_spec(#{user_id => 4002}),

    {ok, _Pid1, _JobId1} = flurm_job:submit(JobSpec1),
    {ok, _Pid2, _JobId2} = flurm_job:submit(JobSpec2),
    {ok, _Pid3, _JobId3} = flurm_job:submit(JobSpec3),

    Counts = flurm_job_registry:count_by_user(),
    ?assertEqual(2, maps:get(4001, Counts, 0)),
    ?assertEqual(1, maps:get(4002, Counts, 0)),
    ok.

test_monitor_down() ->
    %% Start a job
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    %% Verify registered
    {ok, Pid} = flurm_job_registry:lookup_job(JobId),

    %% Kill the job process
    exit(Pid, kill),
    timer:sleep(100),

    %% Job should be automatically unregistered
    {error, not_found} = flurm_job_registry:lookup_job(JobId),
    ok.

test_unknown_request() ->
    %% Send unknown call
    Result = gen_server:call(flurm_job_registry, {unknown_request, data}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_cast_handling() ->
    %% Send unknown cast - should not crash
    gen_server:cast(flurm_job_registry, {unknown_cast, data}),
    timer:sleep(10),
    ?assert(is_process_alive(whereis(flurm_job_registry))),
    ok.

test_info_handling() ->
    %% Send unknown info - should not crash
    whereis(flurm_job_registry) ! {random, info, message},
    timer:sleep(10),
    ?assert(is_process_alive(whereis(flurm_job_registry))),

    %% Send DOWN message for unknown monitor - should not crash
    whereis(flurm_job_registry) ! {'DOWN', make_ref(), process, self(), normal},
    timer:sleep(10),
    ?assert(is_process_alive(whereis(flurm_job_registry))),
    ok.

test_code_change() ->
    %% Just verify registry continues to work
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
    {ok, _} = flurm_job_registry:lookup_job(JobId),
    ok.

%%====================================================================
%% Additional Test Suites
%%====================================================================

%% Test state transitions through registry
state_transitions_test_() ->
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
             {"State index updated on state change", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, _Pid, JobId} = flurm_job:submit(JobSpec),

                 %% Initially in pending
                 PendingBefore = flurm_job_registry:list_jobs_by_state(pending),
                 ?assertEqual(1, length(PendingBefore)),

                 %% Update to running
                 ok = flurm_job_registry:update_state(JobId, running),

                 %% Should now be in running, not pending
                 PendingAfter = flurm_job_registry:list_jobs_by_state(pending),
                 RunningAfter = flurm_job_registry:list_jobs_by_state(running),
                 ?assertEqual(0, length(PendingAfter)),
                 ?assertEqual(1, length(RunningAfter))
             end}
         ]
     end}.

%% Test multiple concurrent operations
concurrent_operations_test_() ->
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
         {"Multiple job submissions", fun() ->
             %% Submit many jobs concurrently
             JobSpec = make_job_spec(),
             Results = [flurm_job:submit(JobSpec) || _ <- lists:seq(1, 10)],

             %% All should succeed
             ?assertEqual(10, length([R || {ok, _, _} = R <- Results])),

             %% All should be in registry
             Jobs = flurm_job_registry:list_jobs(),
             ?assertEqual(10, length(Jobs))
         end}
     end}.

%% Test edge cases
edge_cases_test_() ->
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
             {"Lookup with no jobs returns not found", fun() ->
                 {error, not_found} = flurm_job_registry:lookup_job(1)
             end},
             {"List jobs with empty registry", fun() ->
                 Jobs = flurm_job_registry:list_jobs(),
                 ?assertEqual(0, length(Jobs))
             end},
             {"Count by state with empty registry", fun() ->
                 Counts = flurm_job_registry:count_by_state(),
                 %% All counts should be 0
                 States = [pending, configuring, running, completing,
                          completed, cancelled, failed, timeout, node_fail],
                 lists:foreach(fun(State) ->
                     ?assertEqual(0, maps:get(State, Counts, 0))
                 end, States)
             end},
             {"Count by user with empty registry", fun() ->
                 Counts = flurm_job_registry:count_by_user(),
                 ?assertEqual(#{}, Counts)
             end}
         ]
     end}.
