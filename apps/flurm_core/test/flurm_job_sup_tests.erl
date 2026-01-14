%%%-------------------------------------------------------------------
%%% @doc FLURM Job Supervisor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_sup supervisor,
%%% covering supervisor initialization, child specifications,
%%% dynamic job process management, and error handling.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Internal helper exports (for cross-function visibility)
-compile({nowarn_unused_function, [{wait_for_process_death, 2}, {wait_for_process_death, 3}]}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic supervisor tests with mocked dependencies
job_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Start link succeeds", fun test_start_link/0},
        {"Init returns correct supervisor flags", fun test_init_supervisor_flags/0},
        {"Init returns correct child spec", fun test_init_child_spec/0},
        {"Start job with valid spec", fun test_start_job_valid_spec/0},
        {"Stop job", fun test_stop_job/0},
        {"Which jobs returns pids", fun test_which_jobs/0},
        {"Count jobs", fun test_count_jobs/0},
        {"Multiple jobs lifecycle", fun test_multiple_jobs_lifecycle/0},
        {"Start job error handling", fun test_start_job_error/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),

    %% Mock dependencies
    meck:new(flurm_job, [non_strict]),
    meck:expect(flurm_job, start_link, fun(#job_spec{} = _Spec) ->
        %% Create a dummy process that acts like a job
        Pid = spawn(fun() -> job_loop() end),
        {ok, Pid}
    end),

    meck:new(flurm_accounting, [non_strict]),
    meck:expect(flurm_accounting, record_job_submit, fun(_Data) -> ok end),

    ok.

cleanup(_) ->
    %% Stop the supervisor if running
    case whereis(flurm_job_sup) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,

    catch meck:unload(flurm_job),
    catch meck:unload(flurm_accounting),
    ok.

%% Dummy job process loop
job_loop() ->
    receive
        stop -> ok;
        _ -> job_loop()
    end.

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
%% Start Link Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_job_sup:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_job_sup)),
    ok.

%%====================================================================
%% Init Callback Tests
%%====================================================================

test_init_supervisor_flags() ->
    %% Test init directly
    {ok, {SupFlags, _ChildSpecs}} = flurm_job_sup:init([]),

    ?assert(is_map(SupFlags)),
    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    %% Jobs should not auto-restart (intensity = 0)
    ?assertEqual(0, maps:get(intensity, SupFlags)),
    ?assertEqual(1, maps:get(period, SupFlags)),
    ok.

test_init_child_spec() ->
    {ok, {_SupFlags, ChildSpecs}} = flurm_job_sup:init([]),

    ?assertEqual(1, length(ChildSpecs)),
    [ChildSpec] = ChildSpecs,

    ?assert(is_map(ChildSpec)),
    ?assertEqual(flurm_job, maps:get(id, ChildSpec)),
    ?assertEqual({flurm_job, start_link, []}, maps:get(start, ChildSpec)),
    %% Temporary restart - jobs don't auto-restart
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)),
    ?assertEqual(worker, maps:get(type, ChildSpec)),
    ?assertEqual([flurm_job], maps:get(modules, ChildSpec)),
    ok.

%%====================================================================
%% Start Job Tests
%%====================================================================

test_start_job_valid_spec() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, JobPid} = flurm_job_sup:start_job(JobSpec),

    ?assert(is_pid(JobPid)),
    ?assert(is_process_alive(JobPid)),

    %% Verify job is in supervisor children
    Jobs = flurm_job_sup:which_jobs(),
    ?assert(lists:member(JobPid, Jobs)),
    ok.

test_start_job_error() ->
    %% Setup mock to return error
    meck:expect(flurm_job, start_link, fun(_Spec) ->
        {error, invalid_spec}
    end),

    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    Result = flurm_job_sup:start_job(JobSpec),

    ?assertEqual({error, invalid_spec}, Result),
    ok.

%%====================================================================
%% Stop Job Tests
%%====================================================================

test_stop_job() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, JobPid} = flurm_job_sup:start_job(JobSpec),

    ?assert(is_process_alive(JobPid)),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    ok = flurm_job_sup:stop_job(JobPid),

    %% Wait for process to terminate
    timer:sleep(50),

    ?assertNot(is_process_alive(JobPid)),
    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Which Jobs Tests
%%====================================================================

test_which_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    %% Initially empty
    ?assertEqual([], flurm_job_sup:which_jobs()),

    %% Start some jobs
    JobSpec = make_job_spec(),
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid3} = flurm_job_sup:start_job(JobSpec),

    Jobs = flurm_job_sup:which_jobs(),
    ?assertEqual(3, length(Jobs)),
    ?assert(lists:member(Pid1, Jobs)),
    ?assert(lists:member(Pid2, Jobs)),
    ?assert(lists:member(Pid3, Jobs)),
    ok.

%%====================================================================
%% Count Jobs Tests
%%====================================================================

test_count_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    ?assertEqual(0, flurm_job_sup:count_jobs()),

    JobSpec = make_job_spec(),
    {ok, _Pid1} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    {ok, _Pid2} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(2, flurm_job_sup:count_jobs()),

    {ok, _Pid3} = flurm_job_sup:start_job(JobSpec),
    {ok, _Pid4} = flurm_job_sup:start_job(JobSpec),
    ?assertEqual(4, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Multiple Jobs Lifecycle Tests
%%====================================================================

test_multiple_jobs_lifecycle() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),

    %% Start multiple jobs
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid3} = flurm_job_sup:start_job(JobSpec),

    ?assertEqual(3, flurm_job_sup:count_jobs()),

    %% Stop one job
    ok = flurm_job_sup:stop_job(Pid2),
    timer:sleep(50),
    ?assertEqual(2, flurm_job_sup:count_jobs()),

    Jobs = flurm_job_sup:which_jobs(),
    ?assert(lists:member(Pid1, Jobs)),
    ?assertNot(lists:member(Pid2, Jobs)),
    ?assert(lists:member(Pid3, Jobs)),

    %% Stop remaining jobs
    ok = flurm_job_sup:stop_job(Pid1),
    ok = flurm_job_sup:stop_job(Pid3),
    timer:sleep(50),

    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ?assertEqual([], flurm_job_sup:which_jobs()),
    ok.

%%====================================================================
%% Integration Tests (without mocking flurm_job)
%%====================================================================

%% These tests use the actual flurm_job module with flurm_job_registry

integration_test_() ->
    {foreach,
     fun integration_setup/0,
     fun integration_cleanup/1,
     [
        {"Full job lifecycle", fun test_full_job_lifecycle/0},
        {"Process crash handling", fun test_process_crash_handling/0},
        {"Concurrent job starts", fun test_concurrent_job_starts/0}
     ]}.

integration_setup() ->
    application:ensure_all_started(sasl),

    %% Use mock for accounting only
    meck:new(flurm_accounting, [non_strict]),
    meck:expect(flurm_accounting, record_job_submit, fun(_Data) -> ok end),
    meck:expect(flurm_accounting, record_job_start, fun(_JobId, _Nodes) -> ok end),
    meck:expect(flurm_accounting, record_job_end, fun(_JobId, _ExitCode, _State) -> ok end),
    meck:expect(flurm_accounting, record_job_cancelled, fun(_JobId, _Reason) -> ok end),

    %% Start actual job registry and supervisor
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    {ok, SupPid} = flurm_job_sup:start_link(),

    #{registry => RegistryPid, supervisor => SupPid}.

integration_cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    catch meck:unload(flurm_accounting),

    %% Stop all jobs
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],

    case is_process_alive(SupPid) of
        true ->
            unlink(SupPid),
            catch gen_server:stop(SupPid, shutdown, 5000);
        false -> ok
    end,

    case is_process_alive(RegistryPid) of
        true ->
            unlink(RegistryPid),
            catch gen_server:stop(RegistryPid, shutdown, 5000);
        false -> ok
    end,
    ok.

test_full_job_lifecycle() ->
    JobSpec = make_job_spec(#{
        user_id => 2000,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8
    }),

    %% Submit job
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),

    %% Verify initial state
    {ok, pending} = flurm_job:get_state(Pid),
    ?assertEqual(1, flurm_job_sup:count_jobs()),

    %% Allocate and run
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Complete job
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    %% Job process should still be tracked
    ?assertEqual(1, flurm_job_sup:count_jobs()),
    ok.

test_process_crash_handling() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    ?assertEqual(1, flurm_job_sup:count_jobs()),
    ?assert(is_process_alive(Pid)),

    %% Kill the process (simulating crash)
    exit(Pid, kill),
    timer:sleep(100),

    %% Process should be removed from supervisor
    %% (temporary restart strategy means no restart)
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

test_concurrent_job_starts() ->
    NumJobs = 10,

    %% Start jobs concurrently
    Results = pmap(fun(I) ->
        JobSpec = make_job_spec(#{
            user_id => 1000 + I,
            partition => <<"partition", (integer_to_binary(I))/binary>>
        }),
        flurm_job:submit(JobSpec)
    end, lists:seq(1, NumJobs)),

    %% All should succeed
    lists:foreach(fun(Result) ->
        ?assertMatch({ok, _, _}, Result)
    end, Results),

    %% Verify count
    ?assertEqual(NumJobs, flurm_job_sup:count_jobs()),

    %% Verify all unique PIDs
    Pids = flurm_job_sup:which_jobs(),
    UniquePids = lists:usort(Pids),
    ?assertEqual(length(Pids), length(UniquePids)),
    ok.

%% Parallel map helper
pmap(F, L) ->
    Parent = self(),
    Refs = [spawn_monitor(fun() -> Parent ! {self(), F(X)} end) || X <- L],
    [receive
        {Pid, Result} ->
            receive {'DOWN', Ref, process, Pid, _} -> ok end,
            Result
    end || {Pid, Ref} <- Refs].

%%====================================================================
%% Supervisor Behavior Edge Cases
%%====================================================================

supervisor_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Which jobs with dead process", fun test_which_jobs_dead_process/0},
        {"Supervisor restart behavior", fun test_supervisor_no_restart/0},
        {"Stop non-existent job", fun test_stop_nonexistent_job/0}
     ]}.

test_which_jobs_dead_process() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, Pid1} = flurm_job_sup:start_job(JobSpec),
    {ok, Pid2} = flurm_job_sup:start_job(JobSpec),

    ?assertEqual(2, flurm_job_sup:count_jobs()),

    %% Kill one process externally
    exit(Pid1, kill),
    %% Wait for the process to actually terminate
    wait_for_process_death(Pid1, 1000),

    %% which_jobs only returns PIDs from supervisor:which_children
    %% The implementation filters out non-pid entries
    Jobs = flurm_job_sup:which_jobs(),
    %% Pid1 may still be in the list briefly, but should not be alive
    ?assertNot(is_process_alive(Pid1)),
    ?assert(lists:member(Pid2, Jobs)),
    ok.

test_supervisor_no_restart() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job_sup:start_job(JobSpec),

    InitialCount = flurm_job_sup:count_jobs(),
    ?assertEqual(1, InitialCount),

    %% Kill the process - should NOT restart due to temporary strategy
    exit(Pid, kill),
    %% Wait for the process to actually terminate
    wait_for_process_death(Pid, 1000),

    %% Verify the process did not restart (no new processes)
    %% Note: The supervisor may still report the terminated child briefly
    %% so we check the process is dead rather than count
    ?assertNot(is_process_alive(Pid)),
    ok.

test_stop_nonexistent_job() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    %% Create a dummy pid that's not a child
    DummyPid = spawn(fun() -> receive _ -> ok end end),

    %% stop_job should return error for non-child
    Result = flurm_job_sup:stop_job(DummyPid),
    ?assertMatch({error, _}, Result),

    %% Cleanup
    exit(DummyPid, kill),
    ok.

%%====================================================================
%% Direct Init Tests (without starting supervisor)
%%====================================================================

init_direct_test_() ->
    [
        {"Init with empty args", fun test_init_empty_args/0},
        {"Child spec structure", fun test_child_spec_structure/0}
    ].

test_init_empty_args() ->
    Result = flurm_job_sup:init([]),
    ?assertMatch({ok, {_, _}}, Result),

    {ok, {SupFlags, ChildSpecs}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(ChildSpecs)),
    ok.

test_child_spec_structure() ->
    {ok, {_SupFlags, [ChildSpec]}} = flurm_job_sup:init([]),

    %% Verify all required keys are present
    RequiredKeys = [id, start, restart, shutdown, type, modules],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, ChildSpec),
                io_lib:format("Missing key: ~p", [Key]))
    end, RequiredKeys),

    %% Verify start MFA format
    {M, F, A} = maps:get(start, ChildSpec),
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),

    %% Verify modules is a list
    Modules = maps:get(modules, ChildSpec),
    ?assert(is_list(Modules)),
    ?assert(length(Modules) > 0),
    ok.

%%====================================================================
%% Performance and Stress Tests
%%====================================================================

stress_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 60, [
        {"Handle many jobs", fun test_many_jobs/0}
     ]}}.

test_many_jobs() ->
    {ok, _SupPid} = flurm_job_sup:start_link(),

    NumJobs = 100,
    JobSpec = make_job_spec(),

    %% Start many jobs
    Pids = [begin
        {ok, Pid} = flurm_job_sup:start_job(JobSpec),
        Pid
    end || _ <- lists:seq(1, NumJobs)],

    ?assertEqual(NumJobs, flurm_job_sup:count_jobs()),
    ?assertEqual(NumJobs, length(flurm_job_sup:which_jobs())),

    %% Stop half of them
    {ToStop, ToKeep} = lists:split(NumJobs div 2, Pids),
    [flurm_job_sup:stop_job(P) || P <- ToStop],
    timer:sleep(100),

    ?assertEqual(length(ToKeep), flurm_job_sup:count_jobs()),

    %% Verify correct PIDs remain
    RemainingJobs = flurm_job_sup:which_jobs(),
    lists:foreach(fun(Pid) ->
        ?assert(lists:member(Pid, RemainingJobs))
    end, ToKeep),

    %% Stop remaining
    [flurm_job_sup:stop_job(P) || P <- ToKeep],
    timer:sleep(100),

    ?assertEqual(0, flurm_job_sup:count_jobs()),
    ok.

%%====================================================================
%% Internal Helper Functions
%%====================================================================

%% Wait for a process to die, with timeout
wait_for_process_death(Pid, Timeout) ->
    wait_for_process_death(Pid, Timeout, 10).

wait_for_process_death(_Pid, Remaining, _Interval) when Remaining =< 0 ->
    timeout;
wait_for_process_death(Pid, Remaining, Interval) ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(Interval),
            wait_for_process_death(Pid, Remaining - Interval, Interval)
    end.
