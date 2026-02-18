%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_sup module
%%%
%%% Tests the simple_one_for_one supervisor for job processes.
%%% Validates init/1 returns valid specs, and tests dynamic child
%%% management with mocked job processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_sup_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_valid_spec_test() ->
    {ok, {SupFlags, ChildSpecs}} = flurm_job_sup:init([]),
    %% For simple_one_for_one, should have exactly one child template
    ?assertMatch(#{strategy := simple_one_for_one}, SupFlags),
    ?assertEqual(1, length(ChildSpecs)).

init_sup_flags_test() ->
    {ok, {SupFlags, _}} = flurm_job_sup:init([]),
    %% Verify all expected supervisor flags
    ?assertMatch(#{strategy := simple_one_for_one,
                   intensity := 0,
                   period := 1}, SupFlags).

child_spec_valid_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    %% Validate child spec fields
    ?assertMatch(#{id := flurm_job,
                   start := {flurm_job, start_link, []},
                   restart := temporary,
                   shutdown := 5000,
                   type := worker,
                   modules := [flurm_job]}, ChildSpec).

child_spec_id_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    ?assertEqual(flurm_job, maps:get(id, ChildSpec)).

child_spec_start_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    ?assertEqual({flurm_job, start_link, []}, maps:get(start, ChildSpec)).

child_spec_restart_temporary_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    %% Jobs use temporary restart - not restarted automatically
    %% Crashed jobs should be resubmitted explicitly
    ?assertEqual(temporary, maps:get(restart, ChildSpec)).

child_spec_shutdown_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    %% 5 second graceful shutdown
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)).

child_spec_type_worker_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    ?assertEqual(worker, maps:get(type, ChildSpec)).

child_spec_modules_test() ->
    {ok, {_, [ChildSpec]}} = flurm_job_sup:init([]),
    ?assertEqual([flurm_job], maps:get(modules, ChildSpec)).

%%====================================================================
%% Dynamic Child Management Tests (with mocking)
%%====================================================================

start_child_test_() ->
    {setup,
     fun setup_mocked_supervisor/0,
     fun cleanup_mocked_supervisor/1,
     fun(_Pid) ->
         [
             {"can start child with job spec", fun test_start_child/0},
             {"can start multiple children", fun test_start_multiple_children/0},
             {"which_jobs returns running jobs", fun test_which_jobs/0},
             {"count_jobs returns correct count", fun test_count_jobs/0},
             {"stop_job terminates child", fun test_stop_job/0}
         ]
     end}.

setup_mocked_supervisor() ->
    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_job),
    %% Mock the flurm_job module to avoid starting real processes
    meck:new(flurm_job, [passthrough, non_strict]),
    meck:expect(flurm_job, start_link, fun(_JobSpec) ->
        %% Use spawn (not spawn_link) to avoid linked exits affecting test runner
        Pid = spawn(fun() -> mock_job_loop() end),
        {ok, Pid}
    end),

    %% Start the supervisor (unlinked from test process)
    {ok, Pid} = flurm_job_sup:start_link(),
    unlink(Pid),
    Pid.

cleanup_mocked_supervisor(Pid) ->
    %% Unload meck first to prevent issues
    catch meck:unload(flurm_job),

    %% Stop all children first
    case erlang:is_process_alive(Pid) of
        true ->
            lists:foreach(fun(ChildPid) ->
                catch flurm_job_sup:stop_job(ChildPid)
            end, catch flurm_job_sup:which_jobs()),
            %% Stop the supervisor with proper monitor/wait pattern
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

mock_job_loop() ->
    receive
        stop -> ok;
        _ -> mock_job_loop()
    after
        60000 -> ok  %% Auto-terminate after 60s to prevent test hangs
    end.

test_start_child() ->
    JobSpec = #job_spec{
        user_id = 1001,
        group_id = 1001,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'Hello World'\n">>,
        priority = 100
    },
    {ok, Child} = flurm_job_sup:start_job(JobSpec),
    ?assert(is_pid(Child)),
    ?assert(is_process_alive(Child)),
    %% Clean up
    flurm_job_sup:stop_job(Child).

test_start_multiple_children() ->
    JobSpec1 = #job_spec{
        user_id = 1001,
        group_id = 1001,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 2,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'Job 1'\n">>,
        priority = 100
    },
    JobSpec2 = #job_spec{
        user_id = 1002,
        group_id = 1002,
        partition = <<"compute">>,
        num_nodes = 2,
        num_cpus = 8,
        time_limit = 7200,
        script = <<"#!/bin/bash\necho 'Job 2'\n">>,
        priority = 200
    },
    {ok, Child1} = flurm_job_sup:start_job(JobSpec1),
    {ok, Child2} = flurm_job_sup:start_job(JobSpec2),
    ?assert(is_pid(Child1)),
    ?assert(is_pid(Child2)),
    ?assertNotEqual(Child1, Child2),
    %% Clean up
    flurm_job_sup:stop_job(Child1),
    flurm_job_sup:stop_job(Child2).

test_which_jobs() ->
    %% Start a job
    JobSpec = #job_spec{
        user_id = 1001,
        group_id = 1001,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'Which job'\n">>,
        priority = 100
    },
    {ok, Child} = flurm_job_sup:start_job(JobSpec),

    Jobs = flurm_job_sup:which_jobs(),
    ?assert(is_list(Jobs)),
    ?assert(lists:member(Child, Jobs)),

    %% Clean up
    flurm_job_sup:stop_job(Child).

test_count_jobs() ->
    InitialCount = flurm_job_sup:count_jobs(),

    JobSpec = #job_spec{
        user_id = 1001,
        group_id = 1001,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'Count job'\n">>,
        priority = 100
    },
    {ok, Child} = flurm_job_sup:start_job(JobSpec),

    NewCount = flurm_job_sup:count_jobs(),
    ?assertEqual(InitialCount + 1, NewCount),

    %% Clean up
    flurm_job_sup:stop_job(Child).

test_stop_job() ->
    JobSpec = #job_spec{
        user_id = 1001,
        group_id = 1001,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        script = <<"#!/bin/bash\necho 'Stop job'\n">>,
        priority = 100
    },
    {ok, Child} = flurm_job_sup:start_job(JobSpec),
    ?assert(is_process_alive(Child)),

    ok = flurm_job_sup:stop_job(Child),
    timer:sleep(50),
    ?assertNot(is_process_alive(Child)).

%%====================================================================
%% Supervisor Spec Validation Tests
%%====================================================================

validate_supervisor_spec_test_() ->
    [
        {"init returns {ok, {SupFlags, ChildSpecs}}", fun() ->
            Result = flurm_job_sup:init([]),
            ?assertMatch({ok, {_, _}}, Result),
            {ok, {SupFlags, ChildSpecs}} = Result,
            ?assert(is_map(SupFlags)),
            ?assert(is_list(ChildSpecs))
        end},
        {"strategy is valid OTP strategy", fun() ->
            {ok, {SupFlags, _}} = flurm_job_sup:init([]),
            Strategy = maps:get(strategy, SupFlags),
            ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
            ?assert(lists:member(Strategy, ValidStrategies))
        end},
        {"intensity is non-negative integer", fun() ->
            {ok, {SupFlags, _}} = flurm_job_sup:init([]),
            Intensity = maps:get(intensity, SupFlags),
            ?assert(is_integer(Intensity)),
            ?assert(Intensity >= 0)
        end},
        {"period is positive integer", fun() ->
            {ok, {SupFlags, _}} = flurm_job_sup:init([]),
            Period = maps:get(period, SupFlags),
            ?assert(is_integer(Period)),
            ?assert(Period > 0)
        end},
        {"child spec has valid id", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            Id = maps:get(id, Spec),
            ?assert(is_atom(Id))
        end},
        {"child spec has valid MFA", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            {M, F, A} = maps:get(start, Spec),
            ?assert(is_atom(M)),
            ?assert(is_atom(F)),
            ?assert(is_list(A))
        end},
        {"child spec restart is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            Restart = maps:get(restart, Spec),
            ?assert(lists:member(Restart, [permanent, transient, temporary]))
        end},
        {"child spec shutdown is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            Shutdown = maps:get(shutdown, Spec),
            ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                            orelse Shutdown =:= brutal_kill
                            orelse Shutdown =:= infinity,
            ?assert(ValidShutdown)
        end},
        {"child spec type is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            Type = maps:get(type, Spec),
            ?assert(lists:member(Type, [worker, supervisor]))
        end},
        {"child spec modules is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_job_sup:init([]),
            Modules = maps:get(modules, Spec),
            ValidModules = (Modules =:= dynamic)
                           orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
            ?assert(ValidModules)
        end}
    ].
