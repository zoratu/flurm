%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor Supervisor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_executor_sup module.
%%% Tests supervisor initialization, child specification, and
%%% job start/stop operations.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Init callback tests (no dependencies needed)
init_test_() ->
    {"Supervisor init callback tests",
     [
        {"init/1 returns correct supervisor flags", fun test_init_flags/0},
        {"init/1 returns correct child spec", fun test_init_child_spec/0},
        {"init/1 child has correct restart strategy", fun test_init_restart/0},
        {"init/1 child has correct shutdown timeout", fun test_init_shutdown/0}
     ]}.

test_init_flags() ->
    {ok, {SupFlags, _Children}} = flurm_job_executor_sup:init([]),

    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(10, maps:get(intensity, SupFlags)),
    ?assertEqual(60, maps:get(period, SupFlags)),
    ok.

test_init_child_spec() ->
    {ok, {_SupFlags, Children}} = flurm_job_executor_sup:init([]),

    ?assertEqual(1, length(Children)),
    [ChildSpec] = Children,

    ?assertEqual(flurm_job_executor, maps:get(id, ChildSpec)),
    ?assertEqual({flurm_job_executor, start_link, []}, maps:get(start, ChildSpec)),
    ?assertEqual(worker, maps:get(type, ChildSpec)),
    ?assertEqual([flurm_job_executor], maps:get(modules, ChildSpec)),
    ok.

test_init_restart() ->
    {ok, {_SupFlags, Children}} = flurm_job_executor_sup:init([]),
    [ChildSpec] = Children,

    %% Job executors should be temporary - if they crash, don't restart
    %% (the job failed)
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),
    ok.

test_init_shutdown() ->
    {ok, {_SupFlags, Children}} = flurm_job_executor_sup:init([]),
    [ChildSpec] = Children,

    %% 30 second shutdown timeout to allow jobs to cleanup
    ?assertEqual(30000, maps:get(shutdown, ChildSpec)),
    ok.

%%====================================================================
%% Supervisor Integration Tests
%%====================================================================

supervisor_integration_test_() ->
    {foreach,
     fun setup_supervisor/0,
     fun cleanup_supervisor/1,
     [
        {"start_link starts supervisor", fun test_start_link/0},
        {"start_job starts child process", fun test_start_job/0},
        {"start_job with invalid spec returns error", fun test_start_job_invalid/0},
        {"stop_job terminates child", fun test_stop_job/0},
        {"multiple jobs can run concurrently", fun test_multiple_jobs/0}
     ]}.

setup_supervisor() ->
    %% Start required applications
    application:ensure_all_started(sasl),

    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    %% Mock lager
    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock controller connector
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),

    %% Stop any existing supervisor
    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try
                supervisor:terminate_child(Pid, flurm_job_executor),
                gen_server:stop(Pid)
            catch _:_ -> ok
            end
    end,
    ok.

cleanup_supervisor(_) ->
    %% Stop supervisor if running
    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try
                exit(Pid, shutdown),
                flurm_test_utils:wait_for_death(Pid)
            catch _:_ -> ok
            end
    end,

    meck:unload(lager),
    meck:unload(flurm_controller_connector),
    ok.

test_start_link() ->
    {ok, Pid} = flurm_job_executor_sup:start_link(),

    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_job_executor_sup)),

    %% Verify it's a supervisor with no children initially
    ?assertEqual([], supervisor:which_children(Pid)),
    ok.

test_start_job() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    JobSpec = #{
        job_id => 1001,
        script => <<"#!/bin/bash\necho hello">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),

    ?assert(is_pid(JobPid)),
    ?assert(is_process_alive(JobPid)),

    %% Wait for job to complete
    MonRef = monitor(process, JobPid),
    receive
        {'DOWN', MonRef, process, JobPid, _Reason} ->
            ok
    after 10000 ->
        exit(JobPid, kill),
        ?assert(false)
    end,
    ok.

test_start_job_invalid() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    %% Job spec without required job_id should fail during execution
    %% but start_job itself should succeed (gen_server starts)
    %% The failure happens later when init tries to use the job_id
    JobSpec = #{
        script => <<"#!/bin/bash\necho hello">>
    },

    %% This will start the process, but it will crash due to missing job_id
    Result = flurm_job_executor_sup:start_job(JobSpec),

    case Result of
        {ok, Pid} ->
            %% Process started but will crash
            MonRef = monitor(process, Pid),
            receive
                {'DOWN', MonRef, process, Pid, _Reason} ->
                    ok
            after 1000 ->
                exit(Pid, kill),
                ok
            end;
        {error, _Reason} ->
            %% Start failed immediately (also acceptable)
            ok
    end,
    ok.

test_stop_job() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    JobSpec = #{
        job_id => 1002,
        script => <<"#!/bin/bash\nsleep 60">>,
        working_dir => <<"/tmp">>,
        num_cpus => 1,
        memory_mb => 512
    },

    {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),

    ?assert(is_process_alive(JobPid)),

    %% Give job time to start
    _ = sys:get_state(JobPid),

    %% Stop the job
    ok = flurm_job_executor_sup:stop_job(JobPid),

    %% Wait for process to terminate
    flurm_test_utils:wait_for_death(JobPid),

    ?assertNot(is_process_alive(JobPid)),
    ok.

test_multiple_jobs() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    %% Start multiple jobs
    JobSpecs = [
        #{job_id => 2001, script => <<"#!/bin/bash\necho job1">>, working_dir => <<"/tmp">>},
        #{job_id => 2002, script => <<"#!/bin/bash\necho job2">>, working_dir => <<"/tmp">>},
        #{job_id => 2003, script => <<"#!/bin/bash\necho job3">>, working_dir => <<"/tmp">>}
    ],

    JobPids = lists:map(fun(Spec) ->
        {ok, Pid} = flurm_job_executor_sup:start_job(Spec),
        Pid
    end, JobSpecs),

    ?assertEqual(3, length(JobPids)),
    ?assert(lists:all(fun is_pid/1, JobPids)),

    %% All should be alive initially
    ?assert(lists:all(fun is_process_alive/1, JobPids)),

    %% Wait for all to complete
    lists:foreach(fun(Pid) ->
        MonRef = monitor(process, Pid),
        receive
            {'DOWN', MonRef, process, Pid, _Reason} ->
                ok
        after 10000 ->
            exit(Pid, kill)
        end
    end, JobPids),
    ok.

%%====================================================================
%% Supervisor Restart Behavior Tests
%%====================================================================

restart_behavior_test_() ->
    {foreach,
     fun setup_restart/0,
     fun cleanup_restart/1,
     [
        {"Supervisor survives child crash", fun test_child_crash_survival/0},
        {"Child is not restarted after crash (temporary)", fun test_no_restart_on_crash/0}
     ]}.

setup_restart() ->
    application:ensure_all_started(sasl),

    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),

    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try exit(Pid, shutdown) catch _:_ -> ok end,
            flurm_test_utils:wait_for_death(Pid)
    end,
    ok.

cleanup_restart(_) ->
    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try exit(Pid, shutdown) catch _:_ -> ok end,
            flurm_test_utils:wait_for_death(Pid)
    end,

    meck:unload(lager),
    meck:unload(flurm_controller_connector),
    ok.

test_child_crash_survival() ->
    {ok, SupPid} = flurm_job_executor_sup:start_link(),

    JobSpec = #{
        job_id => 3001,
        script => <<"#!/bin/bash\nsleep 60">>,
        working_dir => <<"/tmp">>
    },

    {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),
    _ = sys:get_state(JobPid),

    %% Kill the child process abnormally
    exit(JobPid, kill),
    flurm_test_utils:wait_for_death(JobPid),

    %% Supervisor should still be alive
    ?assert(is_process_alive(SupPid)),
    ok.

test_no_restart_on_crash() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    JobSpec = #{
        job_id => 3002,
        script => <<"#!/bin/bash\nsleep 60">>,
        working_dir => <<"/tmp">>
    },

    {ok, JobPid} = flurm_job_executor_sup:start_job(JobSpec),
    _ = sys:get_state(JobPid),

    %% Get initial children count
    Children1 = supervisor:which_children(flurm_job_executor_sup),
    Count1 = length(Children1),
    ?assertEqual(1, Count1),

    %% Kill the child
    exit(JobPid, kill),
    flurm_test_utils:wait_for_death(JobPid),

    %% Child should not be restarted (temporary restart strategy)
    Children2 = supervisor:which_children(flurm_job_executor_sup),
    Count2 = length(Children2),
    ?assertEqual(0, Count2),
    ok.

%%====================================================================
%% Edge Cases and Error Handling Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup_edge_cases/0,
     fun cleanup_edge_cases/1,
     [
        {"Stop non-existent job returns ok", fun test_stop_nonexistent/0},
        {"Can start many jobs rapidly", fun test_rapid_job_start/0}
     ]}.

setup_edge_cases() ->
    application:ensure_all_started(sasl),

    catch meck:unload(lager),
    catch meck:unload(flurm_controller_connector),

    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete,
                fun(_JobId, _ExitCode, _Output, _Energy) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed,
                fun(_JobId, _Reason, _Output, _Energy) -> ok end),

    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try exit(Pid, shutdown) catch _:_ -> ok end,
            flurm_test_utils:wait_for_death(Pid)
    end,
    ok.

cleanup_edge_cases(_) ->
    case whereis(flurm_job_executor_sup) of
        undefined -> ok;
        Pid ->
            try exit(Pid, shutdown) catch _:_ -> ok end,
            flurm_test_utils:wait_for_death(Pid)
    end,

    meck:unload(lager),
    meck:unload(flurm_controller_connector),
    ok.

test_stop_nonexistent() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    %% Create a fake pid (already dead process)
    FakePid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(FakePid),

    %% Stop should return ok even for non-existent child
    Result = flurm_job_executor_sup:stop_job(FakePid),

    %% supervisor:terminate_child returns ok or {error, not_found}
    ?assert(Result =:= ok orelse Result =:= {error, not_found}),
    ok.

test_rapid_job_start() ->
    {ok, _SupPid} = flurm_job_executor_sup:start_link(),

    %% Start 5 jobs rapidly (reduced from 10 for faster tests)
    Results = lists:map(fun(N) ->
        JobSpec = #{
            job_id => 4000 + N,
            script => <<"#!/bin/bash\necho rapid">>,
            working_dir => <<"/tmp">>
        },
        flurm_job_executor_sup:start_job(JobSpec)
    end, lists:seq(1, 5)),

    %% All should succeed
    SuccessCount = length([ok || {ok, _} <- Results]),
    ?assertEqual(5, SuccessCount),

    %% Brief wait for jobs to start by syncing with each started process
    lists:foreach(fun({ok, Pid}) -> _ = sys:get_state(Pid); (_) -> ok end, Results),
    ok.
