%%%-------------------------------------------------------------------
%%% @doc FLURM Job Registry Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_registry gen_server,
%%% covering job registration, lookup, state tracking, and monitoring.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_registry_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_registry_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
            {"Register job", fun test_register_job/0},
            {"Register job already registered", fun test_register_already_registered/0},
            {"Unregister job", fun test_unregister_job/0},
            {"Lookup job by ID", fun test_lookup_job/0},
            {"Lookup non-existent job", fun test_lookup_nonexistent/0},
            {"Get job entry", fun test_get_job_entry/0},
            {"List all jobs", fun test_list_jobs/0},
            {"List jobs by state", fun test_list_jobs_by_state/0},
            {"List jobs by user", fun test_list_jobs_by_user/0},
            {"Update job state", fun test_update_job_state/0},
            {"Update job state not found", fun test_update_job_state_not_found/0},
            {"Count by state", fun test_count_by_state/0},
            {"Count by user", fun test_count_by_user/0},
            {"Monitor cleanup", fun test_monitor_cleanup/0},
            {"Unknown request handling", fun test_unknown_request/0},
            {"Unknown cast handling", fun test_unknown_cast/0},
            {"Unknown info handling", fun test_unknown_info/0},
            {"Multiple state updates", fun test_multiple_state_updates/0},
            {"Code change", fun test_code_change/0},
            {"Empty registry list", fun test_empty_list/0},
            {"Empty registry count by state", fun test_empty_count_by_state/0},
            {"Empty registry count by user", fun test_empty_count_by_user/0},
            %% Terminate test must be LAST as it stops the registry
            {"Terminate handling", fun test_terminate_handling/0}
         ]
     end}.

setup() ->
    application:ensure_all_started(sasl),
    meck:new(flurm_job, [non_strict]),
    meck:expect(flurm_job, get_info, fun(_Pid) ->
        {ok, #{
            user_id => 1000,
            partition => <<"default">>,
            submit_time => erlang:system_time(second),
            state => pending
        }}
    end),
    {ok, Pid} = flurm_job_registry:start_link(),
    %% Unlink immediately to prevent EUnit process from receiving EXIT signals
    unlink(Pid),
    #{registry_pid => Pid}.

cleanup(#{registry_pid := Pid}) ->
    catch meck:unload(flurm_job),
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
    end,
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

mock_job_info(UserId, Partition, State) ->
    meck:expect(flurm_job, get_info, fun(_Pid) ->
        {ok, #{
            user_id => UserId,
            partition => Partition,
            submit_time => erlang:system_time(second),
            state => State
        }}
    end).

make_job_pid() ->
    spawn(fun() -> receive stop -> ok end end).

%%====================================================================
%% Registration Tests
%%====================================================================

test_register_job() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid = make_job_pid(),
    Result = flurm_job_registry:register_job(1, JobPid),
    ?assertEqual(ok, Result),

    %% Verify registration
    {ok, JobPid} = flurm_job_registry:lookup_job(1),
    exit(JobPid, kill),
    timer:sleep(50),
    ok.

test_register_already_registered() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),

    ok = flurm_job_registry:register_job(2, JobPid1),
    Result = flurm_job_registry:register_job(2, JobPid2),
    ?assertEqual({error, already_registered}, Result),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    timer:sleep(50),
    ok.

test_unregister_job() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(3, JobPid),

    %% Verify registered
    {ok, _} = flurm_job_registry:lookup_job(3),

    %% Unregister
    ok = flurm_job_registry:unregister_job(3),

    %% Verify unregistered
    {error, not_found} = flurm_job_registry:lookup_job(3),

    %% Unregister non-existent should be ok
    ok = flurm_job_registry:unregister_job(3),

    exit(JobPid, kill),
    ok.

%%====================================================================
%% Lookup Tests
%%====================================================================

test_lookup_job() ->
    mock_job_info(1001, <<"compute">>, pending),
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(4, JobPid),

    %% Lookup returns the pid
    {ok, JobPid} = flurm_job_registry:lookup_job(4),

    exit(JobPid, kill),
    timer:sleep(50),
    ok.

test_lookup_nonexistent() ->
    {error, not_found} = flurm_job_registry:lookup_job(999999),
    ok.

test_get_job_entry() ->
    mock_job_info(1002, <<"gpu">>, pending),
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(5, JobPid),

    {ok, Entry} = flurm_job_registry:get_job_entry(5),
    ?assertEqual(5, Entry#job_entry.job_id),
    ?assertEqual(1002, Entry#job_entry.user_id),
    ?assertEqual(<<"gpu">>, Entry#job_entry.partition),

    %% Non-existent
    {error, not_found} = flurm_job_registry:get_job_entry(999999),

    exit(JobPid, kill),
    timer:sleep(50),
    ok.

%%====================================================================
%% List Tests
%%====================================================================

test_list_jobs() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),
    JobPid3 = make_job_pid(),

    ok = flurm_job_registry:register_job(10, JobPid1),
    ok = flurm_job_registry:register_job(11, JobPid2),
    ok = flurm_job_registry:register_job(12, JobPid3),

    Jobs = flurm_job_registry:list_jobs(),
    ?assert(length(Jobs) >= 3),
    ?assert(lists:keymember(10, 1, Jobs)),
    ?assert(lists:keymember(11, 1, Jobs)),
    ?assert(lists:keymember(12, 1, Jobs)),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    exit(JobPid3, kill),
    timer:sleep(50),
    ok.

test_list_jobs_by_state() ->
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),
    JobPid3 = make_job_pid(),
    JobPid4 = make_job_pid(),

    mock_job_info(1000, <<"default">>, pending),
    ok = flurm_job_registry:register_job(20, JobPid1),

    mock_job_info(1000, <<"default">>, running),
    ok = flurm_job_registry:register_job(21, JobPid2),

    mock_job_info(1000, <<"default">>, pending),
    ok = flurm_job_registry:register_job(22, JobPid3),

    mock_job_info(1000, <<"default">>, completed),
    ok = flurm_job_registry:register_job(23, JobPid4),

    PendingJobs = flurm_job_registry:list_jobs_by_state(pending),
    RunningJobs = flurm_job_registry:list_jobs_by_state(running),
    CompletedJobs = flurm_job_registry:list_jobs_by_state(completed),

    ?assert(length(PendingJobs) >= 2),
    ?assert(length(RunningJobs) >= 1),
    ?assert(length(CompletedJobs) >= 1),

    ?assert(lists:keymember(20, 1, PendingJobs)),
    ?assert(lists:keymember(22, 1, PendingJobs)),
    ?assert(lists:keymember(21, 1, RunningJobs)),
    ?assert(lists:keymember(23, 1, CompletedJobs)),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    exit(JobPid3, kill),
    exit(JobPid4, kill),
    timer:sleep(50),
    ok.

test_list_jobs_by_user() ->
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),
    JobPid3 = make_job_pid(),

    mock_job_info(2001, <<"default">>, pending),
    ok = flurm_job_registry:register_job(30, JobPid1),

    mock_job_info(2002, <<"default">>, pending),
    ok = flurm_job_registry:register_job(31, JobPid2),

    mock_job_info(2001, <<"default">>, pending),
    ok = flurm_job_registry:register_job(32, JobPid3),

    User1Jobs = flurm_job_registry:list_jobs_by_user(2001),
    User2Jobs = flurm_job_registry:list_jobs_by_user(2002),

    ?assertEqual(2, length(User1Jobs)),
    ?assertEqual(1, length(User2Jobs)),
    ?assert(lists:keymember(30, 1, User1Jobs)),
    ?assert(lists:keymember(32, 1, User1Jobs)),
    ?assert(lists:keymember(31, 1, User2Jobs)),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    exit(JobPid3, kill),
    timer:sleep(50),
    ok.

%%====================================================================
%% State Update Tests
%%====================================================================

test_update_job_state() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(40, JobPid),

    %% Initial state
    {ok, Entry1} = flurm_job_registry:get_job_entry(40),
    ?assertEqual(pending, Entry1#job_entry.state),

    %% Update to running
    ok = flurm_job_registry:update_state(40, running),
    {ok, Entry2} = flurm_job_registry:get_job_entry(40),
    ?assertEqual(running, Entry2#job_entry.state),

    %% Update to completed
    ok = flurm_job_registry:update_state(40, completed),
    {ok, Entry3} = flurm_job_registry:get_job_entry(40),
    ?assertEqual(completed, Entry3#job_entry.state),

    exit(JobPid, kill),
    timer:sleep(50),
    ok.

test_update_job_state_not_found() ->
    %% Update non-existent job
    {error, not_found} = flurm_job_registry:update_state(999999, running),
    ok.

test_multiple_state_updates() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(45, JobPid),

    States = [configuring, running, completing, completed],
    lists:foreach(fun(State) ->
        ok = flurm_job_registry:update_state(45, State),
        {ok, E} = flurm_job_registry:get_job_entry(45),
        ?assertEqual(State, E#job_entry.state)
    end, States),

    exit(JobPid, kill),
    timer:sleep(50),
    ok.

%%====================================================================
%% Count Tests
%%====================================================================

test_count_by_state() ->
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),
    JobPid3 = make_job_pid(),

    mock_job_info(1000, <<"default">>, pending),
    ok = flurm_job_registry:register_job(50, JobPid1),
    ok = flurm_job_registry:register_job(51, JobPid2),

    mock_job_info(1000, <<"default">>, running),
    ok = flurm_job_registry:register_job(52, JobPid3),

    Counts = flurm_job_registry:count_by_state(),
    PendingCount = maps:get(pending, Counts, 0),
    RunningCount = maps:get(running, Counts, 0),

    ?assert(PendingCount >= 2),
    ?assert(RunningCount >= 1),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    exit(JobPid3, kill),
    timer:sleep(50),
    ok.

test_count_by_user() ->
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),
    JobPid3 = make_job_pid(),

    mock_job_info(6001, <<"default">>, pending),
    ok = flurm_job_registry:register_job(60, JobPid1),
    ok = flurm_job_registry:register_job(61, JobPid2),

    mock_job_info(6002, <<"default">>, pending),
    ok = flurm_job_registry:register_job(62, JobPid3),

    Counts = flurm_job_registry:count_by_user(),
    User1Count = maps:get(6001, Counts, 0),
    User2Count = maps:get(6002, Counts, 0),

    ?assertEqual(2, User1Count),
    ?assertEqual(1, User2Count),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    exit(JobPid3, kill),
    timer:sleep(50),
    ok.

%%====================================================================
%% Monitor Tests
%%====================================================================

test_monitor_cleanup() ->
    mock_job_info(1000, <<"default">>, running),
    %% Create a process that will be monitored
    JobPid = make_job_pid(),
    ok = flurm_job_registry:register_job(80, JobPid),

    %% Verify registered
    {ok, JobPid} = flurm_job_registry:lookup_job(80),

    %% Kill the process
    exit(JobPid, kill),
    timer:sleep(100),  %% Wait for monitor to fire

    %% Should be unregistered
    {error, not_found} = flurm_job_registry:lookup_job(80),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_unknown_request() ->
    Result = gen_server:call(flurm_job_registry, unknown_request),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    gen_server:cast(flurm_job_registry, unknown_cast),
    timer:sleep(10),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_job_registry))),
    ok.

test_unknown_info() ->
    flurm_job_registry ! {unknown_info_message},
    timer:sleep(10),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_job_registry))),
    ok.

%%====================================================================
%% Code Change Tests
%%====================================================================

test_code_change() ->
    Pid = whereis(flurm_job_registry),
    sys:suspend(Pid),
    Result = sys:change_code(Pid, flurm_job_registry, "1.0.0", []),
    ?assertEqual(ok, Result),
    sys:resume(Pid),

    %% Verify process still works
    Counts = flurm_job_registry:count_by_state(),
    ?assert(is_map(Counts)),
    ok.

%%====================================================================
%% Empty Registry Tests
%%====================================================================

test_empty_list() ->
    %% List jobs by a user that doesn't exist
    Jobs = flurm_job_registry:list_jobs_by_user(999888),
    ?assertEqual([], Jobs),
    ok.

test_empty_count_by_state() ->
    %% count_by_state returns map with all states (even when empty)
    Counts = flurm_job_registry:count_by_state(),
    ?assert(is_map(Counts)),
    ?assert(maps:is_key(pending, Counts)),
    ?assert(maps:is_key(running, Counts)),
    ?assert(maps:is_key(completed, Counts)),
    ok.

test_empty_count_by_user() ->
    %% This just verifies the function works (count_by_user is cumulative)
    Counts = flurm_job_registry:count_by_user(),
    ?assert(is_map(Counts)),
    ok.

%%====================================================================
%% Terminate Tests - MUST BE LAST
%%====================================================================

test_terminate_handling() ->
    mock_job_info(1000, <<"default">>, pending),
    JobPid1 = make_job_pid(),
    JobPid2 = make_job_pid(),

    ok = flurm_job_registry:register_job(90, JobPid1),
    ok = flurm_job_registry:register_job(91, JobPid2),

    %% Get the pid
    Pid = whereis(flurm_job_registry),

    %% Stop gracefully (already unlinked in setup)
    Ref = monitor(process, Pid),
    gen_server:stop(Pid, shutdown, 5000),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        demonitor(Ref, [flush])
    end,

    %% Verify stopped
    ?assertEqual(undefined, whereis(flurm_job_registry)),

    exit(JobPid1, kill),
    exit(JobPid2, kill),
    ok.
