%%%-------------------------------------------------------------------
%%% @doc FLURM Scheduler Coverage Integration Tests
%%%
%%% Integration tests designed to call through public APIs to improve
%%% code coverage tracking. Cover only tracks calls from test modules
%%% to the actual module code, so these tests call public functions
%%% that exercise internal code paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

scheduler_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates scheduler process", fun test_start_link/0},
        {"submit_job adds job to pending queue", fun test_submit_job/0},
        {"job_completed updates stats", fun test_job_completed/0},
        {"job_failed updates stats", fun test_job_failed/0},
        {"trigger_schedule forces cycle", fun test_trigger_schedule/0},
        {"get_stats returns map", fun test_get_stats/0},
        {"job_deps_satisfied notification", fun test_job_deps_satisfied/0},
        {"multiple job submission", fun test_multiple_jobs/0},
        {"schedule with available nodes", fun test_schedule_with_nodes/0},
        {"config change notifications", fun test_config_changes/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    %% Start required dependencies
    {ok, JobRegistryPid} = flurm_job_registry:start_link(),
    {ok, JobSupPid} = flurm_job_sup:start_link(),
    {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
    {ok, NodeSupPid} = flurm_node_sup:start_link(),
    {ok, LimitsPid} = flurm_limits:start_link(),
    {ok, LicensePid} = flurm_license:start_link(),
    {ok, JobManagerPid} = flurm_job_manager:start_link(),
    {ok, SchedulerPid} = flurm_scheduler:start_link(),
    #{
        job_registry => JobRegistryPid,
        job_sup => JobSupPid,
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid,
        limits => LimitsPid,
        license => LicensePid,
        job_manager => JobManagerPid,
        scheduler => SchedulerPid
    }.

cleanup(Pids) ->
    %% Stop all jobs and nodes first
    [catch flurm_job_sup:stop_job(P) || P <- flurm_job_sup:which_jobs()],
    [catch flurm_node_sup:stop_node(P) || P <- flurm_node_sup:which_nodes()],
    %% Stop processes with proper waiting using monitor/receive pattern
    %% This prevents "already_started" errors in subsequent tests
    maps:foreach(fun(_Key, Pid) ->
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
    end, Pids),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_map() ->
    make_job_map(#{}).

make_job_map(Overrides) ->
    Defaults = #{
        user => <<"testuser">>,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 2,
        memory_mb => 1024,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho test">>,
        priority => 100,
        name => <<"test_job">>
    },
    maps:merge(Defaults, Overrides).

make_node_spec(Name) ->
    make_node_spec(Name, #{}).

make_node_spec(Name, Overrides) ->
    Defaults = #{
        hostname => <<"localhost">>,
        port => 5555,
        cpus => 8,
        memory => 16384,
        gpus => 0,
        features => [],
        partitions => [<<"default">>]
    },
    Props = maps:merge(Defaults, Overrides),
    #node_spec{
        name = Name,
        hostname = maps:get(hostname, Props),
        port = maps:get(port, Props),
        cpus = maps:get(cpus, Props),
        memory = maps:get(memory, Props),
        gpus = maps:get(gpus, Props),
        features = maps:get(features, Props),
        partitions = maps:get(partitions, Props)
    }.

register_node(Name) ->
    register_node(Name, #{}).

register_node(Name, Overrides) ->
    NodeSpec = make_node_spec(Name, Overrides),
    {ok, Pid, Name} = flurm_node:register_node(NodeSpec),
    Pid.

%%====================================================================
%% Coverage Tests - Public API Calls
%%====================================================================

test_start_link() ->
    %% Scheduler is already started by setup, verify it's running
    ?assert(is_pid(whereis(flurm_scheduler))),
    ok.

test_submit_job() ->
    %% Submit job via job_manager (which calls scheduler:submit_job)
    JobMap = make_job_map(),
    {ok, JobId} = flurm_job_manager:submit_job(JobMap),
    ?assert(is_integer(JobId)),
    ?assert(JobId > 0),
    %% Give scheduler time to process
    _ = sys:get_state(flurm_scheduler),
    %% Verify stats show pending job
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 1),
    ok.

test_job_completed() ->
    %% Register a node
    _NodePid = register_node(<<"node1">>),
    %% Submit a job
    JobMap = make_job_map(),
    {ok, JobId} = flurm_job_manager:submit_job(JobMap),
    _ = sys:get_state(flurm_scheduler),
    %% Get initial completed count
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),
    %% Mark job as completed
    ok = flurm_scheduler:job_completed(JobId),
    _ = sys:get_state(flurm_scheduler),
    %% Verify completed count increased
    {ok, NewStats} = flurm_scheduler:get_stats(),
    NewCompleted = maps:get(completed_count, NewStats),
    ?assertEqual(InitCompleted + 1, NewCompleted),
    ok.

test_job_failed() ->
    %% Submit a job
    JobMap = make_job_map(),
    {ok, JobId} = flurm_job_manager:submit_job(JobMap),
    _ = sys:get_state(flurm_scheduler),
    %% Get initial failed count
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitFailed = maps:get(failed_count, InitStats),
    %% Mark job as failed
    ok = flurm_scheduler:job_failed(JobId),
    _ = sys:get_state(flurm_scheduler),
    %% Verify failed count increased
    {ok, NewStats} = flurm_scheduler:get_stats(),
    NewFailed = maps:get(failed_count, NewStats),
    ?assertEqual(InitFailed + 1, NewFailed),
    ok.

test_trigger_schedule() ->
    %% Get initial cycle count
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),
    %% Trigger a schedule cycle
    ok = flurm_scheduler:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler),
    %% Verify cycle count increased
    {ok, NewStats} = flurm_scheduler:get_stats(),
    NewCycles = maps:get(schedule_cycles, NewStats),
    ?assert(NewCycles > InitCycles),
    ok.

test_get_stats() ->
    %% Verify get_stats returns expected structure
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(pending_count, Stats)),
    ?assert(maps:is_key(running_count, Stats)),
    ?assert(maps:is_key(completed_count, Stats)),
    ?assert(maps:is_key(failed_count, Stats)),
    ?assert(maps:is_key(schedule_cycles, Stats)),
    %% Verify values are non-negative integers
    ?assert(maps:get(pending_count, Stats) >= 0),
    ?assert(maps:get(running_count, Stats) >= 0),
    ?assert(maps:get(completed_count, Stats) >= 0),
    ?assert(maps:get(failed_count, Stats) >= 0),
    ?assert(maps:get(schedule_cycles, Stats) >= 0),
    ok.

test_job_deps_satisfied() ->
    %% Notify scheduler that job dependencies are satisfied
    ok = flurm_scheduler:job_deps_satisfied(12345),
    _ = sys:get_state(flurm_scheduler),
    %% Should not crash, verify scheduler still responds
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_multiple_jobs() ->
    %% Submit multiple jobs
    JobIds = lists:map(fun(I) ->
        JobMap = make_job_map(#{name => list_to_binary("job_" ++ integer_to_list(I))}),
        {ok, JobId} = flurm_job_manager:submit_job(JobMap),
        JobId
    end, lists:seq(1, 5)),
    _ = sys:get_state(flurm_scheduler),
    %% Verify all jobs were submitted
    ?assertEqual(5, length(JobIds)),
    %% Verify stats show pending jobs
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 5),
    ok.

test_schedule_with_nodes() ->
    %% Register nodes with sufficient resources
    _Node1 = register_node(<<"compute1">>, #{cpus => 16, memory => 32768}),
    _Node2 = register_node(<<"compute2">>, #{cpus => 16, memory => 32768}),
    %% Submit a job
    JobMap = make_job_map(#{num_cpus => 4, memory_mb => 2048}),
    {ok, JobId} = flurm_job_manager:submit_job(JobMap),
    %% Wait for scheduling with sync
    _ = sys:get_state(flurm_scheduler),
    %% Check if job was scheduled
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = Job#job.state,
            %% Job should be scheduled (configuring or running) or still pending
            ?assert(State =:= pending orelse State =:= configuring orelse State =:= running);
        {error, _} ->
            ok % Job might have completed quickly
    end,
    ok.

test_config_changes() ->
    %% Send config change notifications directly to scheduler
    flurm_scheduler ! {config_changed, partitions, [], [#{name => <<"default">>}]},
    _ = sys:get_state(flurm_scheduler),
    {ok, _} = flurm_scheduler:get_stats(),

    flurm_scheduler ! {config_changed, nodes, [], [<<"node1">>]},
    _ = sys:get_state(flurm_scheduler),
    {ok, _} = flurm_scheduler:get_stats(),

    flurm_scheduler ! {config_changed, schedulertype, fifo, backfill},
    _ = sys:get_state(flurm_scheduler),
    {ok, _} = flurm_scheduler:get_stats(),

    %% Unknown config changes should be ignored
    flurm_scheduler ! {config_changed, unknown_key, old, new},
    _ = sys:get_state(flurm_scheduler),
    {ok, _} = flurm_scheduler:get_stats(),
    ok.

%%====================================================================
%% Additional Public API Tests
%%====================================================================

scheduler_gen_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0}
     ]}.

test_unknown_call() ->
    %% Send unknown request via gen_server:call
    Result = gen_server:call(flurm_scheduler, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    %% Send unknown cast - should not crash
    gen_server:cast(flurm_scheduler, {unknown_cast, test}),
    _ = sys:get_state(flurm_scheduler),
    %% Verify scheduler still works
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_unknown_info() ->
    %% Send unknown info message - should not crash
    flurm_scheduler ! {unknown_info, test},
    _ = sys:get_state(flurm_scheduler),
    %% Verify scheduler still works
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.
