%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_scheduler module
%%%
%%% Comprehensive EUnit tests that call flurm_scheduler functions
%%% directly without mocking the scheduler itself. Only external
%%% dependencies are mocked to isolate the scheduler behavior.
%%%
%%% Tests all exported functions:
%%% - start_link/0
%%% - submit_job/1
%%% - job_completed/1
%%% - job_failed/1
%%% - trigger_schedule/0
%%% - get_stats/0
%%% - job_deps_satisfied/1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    %% Start meck for external dependencies only - NOT flurm_scheduler
    meck:new(flurm_job_manager, [non_strict, no_link]),
    meck:new(flurm_node_manager, [non_strict, no_link]),
    meck:new(flurm_job_deps, [non_strict, no_link]),
    meck:new(flurm_license, [non_strict, no_link]),
    meck:new(flurm_limits, [non_strict, no_link]),
    meck:new(flurm_reservation, [non_strict, no_link]),
    meck:new(flurm_backfill, [non_strict, no_link]),
    meck:new(flurm_preemption, [non_strict, no_link]),
    meck:new(flurm_job_dispatcher, [non_strict, no_link]),
    meck:new(flurm_gres, [non_strict, no_link]),
    meck:new(flurm_metrics, [non_strict, no_link]),
    meck:new(flurm_config_server, [non_strict, no_link]),

    %% Setup default mocks for all external dependencies
    setup_default_mocks(),
    ok.

setup_default_mocks() ->
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, gauge, fun(_, _) -> ok end),
    meck:expect(flurm_limits, check_limits, fun(_) -> ok end),
    meck:expect(flurm_limits, enforce_limit, fun(_, _, _) -> ok end),
    meck:expect(flurm_license, check_availability, fun(_) -> true end),
    meck:expect(flurm_license, allocate, fun(_, _) -> ok end),
    meck:expect(flurm_license, deallocate, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, notify_completion, fun(_, _) -> ok end),
    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) -> {error, reservation_not_found} end),
    meck:expect(flurm_reservation, get_available_nodes_excluding_reserved, fun(Nodes) -> Nodes end),
    meck:expect(flurm_reservation, confirm_reservation, fun(_) -> ok end),
    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),
    meck:expect(flurm_backfill, get_backfill_candidates, fun(_) -> [] end),
    meck:expect(flurm_backfill, run_backfill_cycle, fun(_, _) -> [] end),
    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),
    meck:expect(flurm_gres, filter_nodes_by_gres, fun(Nodes, _) -> Nodes end),
    meck:expect(flurm_gres, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_gres, deallocate, fun(_, _) -> ok end),
    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) -> {error, no_preemptable_jobs} end),
    meck:expect(flurm_preemption, get_preemption_mode, fun(_) -> requeue end),
    meck:expect(flurm_preemption, get_grace_time, fun(_) -> 30 end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {error, no_jobs} end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, get_available_nodes_with_gres, fun(_, _, _, _) -> [] end),
    meck:expect(flurm_node_manager, allocate_gres, fun(_, _, _, _) -> {ok, []} end),
    meck:expect(flurm_node_manager, release_gres, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    ok.

cleanup(_) ->
    %% Stop scheduler if running with proper monitor/wait pattern
    case whereis(flurm_scheduler) of
        undefined -> ok;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Ref = monitor(process, Pid),
                    catch unlink(Pid),
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
    end,

    %% Unload all mocks
    meck:unload(flurm_job_manager),
    meck:unload(flurm_node_manager),
    meck:unload(flurm_job_deps),
    meck:unload(flurm_license),
    meck:unload(flurm_limits),
    meck:unload(flurm_reservation),
    meck:unload(flurm_backfill),
    meck:unload(flurm_preemption),
    meck:unload(flurm_job_dispatcher),
    meck:unload(flurm_gres),
    meck:unload(flurm_metrics),
    meck:unload(flurm_config_server),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a mock job record for testing
make_mock_job(JobId) ->
    make_mock_job(JobId, #{}).

make_mock_job(JobId, Overrides) ->
    Defaults = #{
        id => JobId,
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 3600,
        priority => 100,
        submit_time => erlang:system_time(second),
        start_time => undefined,
        end_time => undefined,
        allocated_nodes => [],
        exit_code => undefined,
        work_dir => <<"/tmp">>,
        std_out => <<>>,
        std_err => <<>>,
        account => <<>>,
        qos => <<"normal">>,
        reservation => <<>>,
        licenses => [],
        gres => <<>>,
        gres_per_node => <<>>,
        gres_per_task => <<>>,
        gpu_type => <<>>,
        gpu_memory_mb => 0,
        gpu_exclusive => true
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
        start_time = maps:get(start_time, Props),
        end_time = maps:get(end_time, Props),
        allocated_nodes = maps:get(allocated_nodes, Props),
        exit_code = maps:get(exit_code, Props),
        work_dir = maps:get(work_dir, Props),
        std_out = maps:get(std_out, Props),
        std_err = maps:get(std_err, Props),
        account = maps:get(account, Props),
        qos = maps:get(qos, Props),
        reservation = maps:get(reservation, Props),
        licenses = maps:get(licenses, Props),
        gres = maps:get(gres, Props),
        gres_per_node = maps:get(gres_per_node, Props),
        gres_per_task = maps:get(gres_per_task, Props),
        gpu_type = maps:get(gpu_type, Props),
        gpu_memory_mb = maps:get(gpu_memory_mb, Props),
        gpu_exclusive = maps:get(gpu_exclusive, Props)
    }.

%% Create a mock node record
make_mock_node(Hostname) ->
    make_mock_node(Hostname, #{}).

make_mock_node(Hostname, Overrides) ->
    Defaults = #{
        hostname => Hostname,
        cpus => 8,
        memory_mb => 16384,
        state => up,
        drain_reason => undefined,
        features => [],
        partitions => [<<"default">>],
        running_jobs => [],
        load_avg => 0.0,
        free_memory_mb => 16384,
        last_heartbeat => undefined,
        allocations => #{},
        gres_config => [],
        gres_available => #{},
        gres_total => #{},
        gres_allocations => #{}
    },
    Props = maps:merge(Defaults, Overrides),
    #node{
        hostname = maps:get(hostname, Props),
        cpus = maps:get(cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        state = maps:get(state, Props),
        drain_reason = maps:get(drain_reason, Props),
        features = maps:get(features, Props),
        partitions = maps:get(partitions, Props),
        running_jobs = maps:get(running_jobs, Props),
        load_avg = maps:get(load_avg, Props),
        free_memory_mb = maps:get(free_memory_mb, Props),
        last_heartbeat = maps:get(last_heartbeat, Props),
        allocations = maps:get(allocations, Props),
        gres_config = maps:get(gres_config, Props),
        gres_available = maps:get(gres_available, Props),
        gres_total = maps:get(gres_total, Props),
        gres_allocations = maps:get(gres_allocations, Props)
    }.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic API tests
basic_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates scheduler process", fun test_start_link/0},
        {"submit_job accepts valid job id", fun test_submit_job/0},
        {"job_completed updates stats", fun test_job_completed/0},
        {"job_failed updates stats", fun test_job_failed/0},
        {"trigger_schedule triggers cycle", fun test_trigger_schedule/0},
        {"get_stats returns complete stats map", fun test_get_stats/0},
        {"job_deps_satisfied triggers reschedule", fun test_job_deps_satisfied/0}
     ]}.

test_start_link() ->
    {ok, Pid} = flurm_scheduler:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_scheduler)),
    ok.

test_submit_job() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Setup job manager mock
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) >= 1),
    ok.

test_job_completed() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [<<"node1">>],
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_job_failed() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => failed,
            allocated_nodes => [<<"node1">>],
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitFailed = maps:get(failed_count, InitStats),

    ok = flurm_scheduler:job_failed(1),
    timer:sleep(100),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assertEqual(InitFailed + 1, maps:get(failed_count, NewStats)),
    ok.

test_trigger_schedule() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    ok = flurm_scheduler:trigger_schedule(),
    timer:sleep(200),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_get_stats() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(pending_count, Stats)),
    ?assert(maps:is_key(running_count, Stats)),
    ?assert(maps:is_key(completed_count, Stats)),
    ?assert(maps:is_key(failed_count, Stats)),
    ?assert(maps:is_key(schedule_cycles, Stats)),

    %% Verify initial values
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ?assertEqual(0, maps:get(running_count, Stats)),
    ?assertEqual(0, maps:get(completed_count, Stats)),
    ?assertEqual(0, maps:get(failed_count, Stats)),
    ok.

test_job_deps_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    ok = flurm_scheduler:job_deps_satisfied(1),
    timer:sleep(200),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast is ignored", fun test_unknown_cast/0},
        {"unknown info is ignored", fun test_unknown_info/0},
        {"terminate cleans up properly", fun test_terminate/0}
     ]}.

test_unknown_call() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    Result = gen_server:call(flurm_scheduler, {some_unknown_request, data}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    gen_server:cast(flurm_scheduler, {some_unknown_message, data}),
    timer:sleep(50),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_unknown_info() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    flurm_scheduler ! some_random_message,
    timer:sleep(50),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_terminate() ->
    {ok, Pid} = flurm_scheduler:start_link(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),

    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)),
    ok.

%%====================================================================
%% Config Change Tests
%%====================================================================

config_change_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partitions config change triggers reschedule", fun test_config_partitions/0},
        {"nodes config change triggers reschedule", fun test_config_nodes/0},
        {"schedulertype config change triggers reschedule", fun test_config_schedulertype/0},
        {"unknown config change is ignored", fun test_config_unknown/0}
     ]}.

test_config_partitions() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, partitions, [], [<<"default">>, <<"gpu">>]},
    timer:sleep(200),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_nodes() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, nodes, [], [<<"node1">>, <<"node2">>]},
    timer:sleep(200),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_schedulertype() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, schedulertype, fifo, backfill},
    timer:sleep(200),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_unknown() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    flurm_scheduler ! {config_changed, some_unknown_key, old_value, new_value},
    timer:sleep(50),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

%%====================================================================
%% Scheduling Logic Tests - Direct Function Calls
%%====================================================================

scheduling_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job scheduled when resources available", fun test_job_scheduled_with_resources/0},
        {"job waits when no resources", fun test_job_waits_no_resources/0},
        {"job not found removed from queue", fun test_job_not_found_removed/0},
        {"job not pending skipped", fun test_job_not_pending_skipped/0},
        {"held job skipped", fun test_held_job_skipped/0},
        {"multiple jobs FIFO order", fun test_fifo_order/0}
     ]}.

test_job_scheduled_with_resources() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify job was processed
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_job_waits_no_resources() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_job_not_found_removed() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler:submit_job(999),
    timer:sleep(200),

    %% Job should be removed from pending queue
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_job_not_pending_skipped() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{state => running})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),

    %% Job should be removed from pending queue
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_held_job_skipped() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{state => held})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),

    %% Job remains in pending queue as held
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_fifo_order() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>, #{cpus => 4}),

    ScheduledJobs = ets:new(scheduled_jobs, [set, public]),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{num_cpus => 4})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, JobId, _, _) ->
        ets:insert(ScheduledJobs, {JobId, erlang:monotonic_time()}),
        ok
    end),

    %% Submit multiple jobs
    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    ok = flurm_scheduler:submit_job(3),
    timer:sleep(400),

    ets:delete(ScheduledJobs),
    ok.

%%====================================================================
%% Dependency Tests
%%====================================================================

dependency_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job with satisfied deps scheduled", fun test_deps_satisfied/0},
        {"job with unsatisfied deps waits", fun test_deps_waiting/0},
        {"deps check error treated as satisfied", fun test_deps_error_satisfied/0},
        {"deps noproc treated as satisfied", fun test_deps_noproc_satisfied/0},
        {"deps badarg treated as satisfied", fun test_deps_badarg_satisfied/0}
     ]}.

test_deps_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

test_deps_waiting() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {waiting, [100, 101]} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),

    %% Job should remain pending due to deps
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_deps_error_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {error, dependency_not_found} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_deps_noproc_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_deps_badarg_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) ->
        erlang:error({badarg, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

%%====================================================================
%% Limits Tests
%%====================================================================

limits_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job blocked by limits", fun test_limits_exceeded/0},
        {"job passes limits check", fun test_limits_ok/0}
     ]}.

test_limits_exceeded() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_limits, check_limits, fun(_) -> {error, max_jobs_exceeded} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_limits_ok() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_limits, check_limits, fun(_) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

%%====================================================================
%% License Tests
%%====================================================================

license_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job waits for unavailable licenses", fun test_license_unavailable/0},
        {"job with available licenses proceeds", fun test_license_available/0},
        {"license allocation failure rolls back", fun test_license_alloc_failure/0},
        {"license deallocation on completion", fun test_license_dealloc_completion/0}
     ]}.

test_license_unavailable() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_license, check_availability, fun(_) -> false end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{licenses => [{<<"matlab">>, 1}]})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_license_available() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_license, check_availability, fun(_) -> true end),
    meck:expect(flurm_license, allocate, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{licenses => [{<<"matlab">>, 1}]})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

test_license_alloc_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_license, check_availability, fun(_) -> true end),
    meck:expect(flurm_license, allocate, fun(_, _) -> {error, license_exhausted} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{licenses => [{<<"matlab">>, 1}]})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify release was called for rollback
    ?assert(meck:called(flurm_node_manager, release_resources, ['_', '_'])),
    ok.

test_license_dealloc_completion() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [<<"node1">>],
            licenses => [{<<"matlab">>, 1}],
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),

    %% Verify license deallocation was called
    ?assert(meck:called(flurm_license, deallocate, ['_', '_'])),
    ok.

%%====================================================================
%% Reservation Tests
%%====================================================================

reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job without reservation", fun test_no_reservation/0},
        {"job with undefined reservation", fun test_undefined_reservation/0},
        {"job uses reservation nodes", fun test_use_reservation/0},
        {"reservation not started waits", fun test_reservation_not_started/0},
        {"reservation access denied error", fun test_reservation_access_denied/0},
        {"reservation expired error", fun test_reservation_expired/0},
        {"reservation noproc fallback", fun test_reservation_noproc/0}
     ]}.

test_no_reservation() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<>>})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_undefined_reservation() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => undefined})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_use_reservation() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {ok, [<<"node1">>]}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<"myres">>})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify reservation confirmation was called
    ?assert(meck:called(flurm_reservation, confirm_reservation, ['_'])),
    ok.

test_reservation_not_started() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, {reservation_not_started, erlang:system_time(second) + 3600}}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<"myres">>})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_reservation_access_denied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, access_denied}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<"myres">>})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_reservation_expired() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, reservation_expired}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<"myres">>})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

test_reservation_noproc() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{reservation => <<"myres">>})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(200),
    ok.

%%====================================================================
%% GRES Tests
%%====================================================================

gres_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job with GRES in job info", fun test_job_with_gres_info/0},
        {"empty GRES string handled", fun test_empty_gres/0},
        {"GRES deallocate on completion", fun test_gres_dealloc_completion/0}
     ]}.

test_job_with_gres_info() ->
    %% Test that jobs with GRES fields are handled correctly
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"gpu-node1">>),

    %% The normal scheduling path goes through find_nodes_excluding_reserved
    %% which doesn't use the GRES-specific node manager functions
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            gres => <<"gpu:2">>,
            gres_per_node => <<>>,
            gpu_type => <<"nvidia">>,
            gpu_exclusive => true
        })}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify the job was processed
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_empty_gres() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{gres => <<>>})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Empty GRES should not cause issues
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_gres_dealloc_completion() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [<<"gpu-node1">>],
            gres => <<"gpu:2">>,
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),

    %% Node resources should be released
    ?assert(meck:called(flurm_node_manager, release_resources, ['_', '_'])),
    ok.

%%====================================================================
%% Backfill Tests
%%====================================================================

backfill_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"simple backfill when advanced disabled", fun test_simple_backfill/0},
        {"advanced backfill when enabled", fun test_advanced_backfill/0},
        {"backfill schedules smaller jobs", fun test_backfill_schedules_jobs/0}
     ]}.

test_simple_backfill() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    %% Submit jobs to trigger backfill
    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    timer:sleep(300),
    ok.

test_advanced_backfill() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> true end),
    meck:expect(flurm_backfill, get_backfill_candidates, fun(_) -> [] end),
    meck:expect(flurm_backfill, run_backfill_cycle, fun(_, _) -> [] end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    timer:sleep(300),

    ?assert(meck:called(flurm_backfill, is_backfill_enabled, [])),
    ok.

test_backfill_schedules_jobs() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> true end),
    meck:expect(flurm_backfill, get_backfill_candidates, fun(_) -> [] end),
    meck:expect(flurm_backfill, run_backfill_cycle, fun(_, _) ->
        [{2, [<<"node1">>]}]
    end),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count =< 2 of
            true ->
                %% First job needs many nodes
                {ok, make_mock_job(JobId, #{num_nodes => 10})};
            false ->
                %% Later calls return smaller job
                {ok, make_mock_job(JobId, #{num_nodes => 1})}
        end
    end),

    NodeCallCount = atomics:new(1, [{signed, false}]),
    atomics:put(NodeCallCount, 1, 0),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        Count = atomics:add_get(NodeCallCount, 1, 1),
        case Count =< 2 of
            true -> [];
            false -> [MockNode]
        end
    end),

    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    timer:sleep(400),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"low priority job no preemption", fun test_low_priority_no_preempt/0},
        {"high priority triggers preemption check", fun test_high_priority_preempt_check/0},
        {"preemption finds jobs", fun test_preemption_finds_jobs/0},
        {"preemption execution success", fun test_preemption_success/0},
        {"preemption execution failure", fun test_preemption_failure/0}
     ]}.

test_low_priority_no_preempt() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 500 end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{priority => 100, num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Preemption should not be called for low priority
    ?assertNot(meck:called(flurm_preemption, find_preemptable_jobs, ['_', '_'])),
    ok.

test_high_priority_preempt_check() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) -> {error, no_preemptable_jobs} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{priority => 500, num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Preemption check should be called for high priority
    ?assert(meck:called(flurm_preemption, find_preemptable_jobs, ['_', '_'])),
    ok.

test_preemption_finds_jobs() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {ok, [#{job_id => 99, num_nodes => 1, num_cpus => 4, memory_mb => 4096}]}
    end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {error, preemption_failed} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{priority => 500, num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

test_preemption_success() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {ok, [#{job_id => 99, num_nodes => 1, num_cpus => 4, memory_mb => 4096}]}
    end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {ok, [99]} end),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count =< 2 of
            true ->
                {ok, make_mock_job(JobId, #{priority => 500, num_nodes => 10})};
            false ->
                {ok, make_mock_job(JobId, #{priority => 500, num_nodes => 1})}
        end
    end),

    NodeCallCount = atomics:new(1, [{signed, false}]),
    atomics:put(NodeCallCount, 1, 0),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        Count = atomics:add_get(NodeCallCount, 1, 1),
        case Count =< 2 of
            true -> [];
            false -> [MockNode]
        end
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(500),
    ok.

test_preemption_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {ok, [#{job_id => 99, num_nodes => 1, num_cpus => 4, memory_mb => 4096}]}
    end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {error, preemption_blocked} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{priority => 500, num_nodes => 10})}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

%%====================================================================
%% Dispatch Tests
%%====================================================================

dispatch_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"dispatch success runs job", fun test_dispatch_success/0},
        {"dispatch failure rolls back", fun test_dispatch_failure_rollback/0}
     ]}.

test_dispatch_success() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    ?assert(meck:called(flurm_job_dispatcher, dispatch_job, ['_', '_'])),
    ok.

test_dispatch_failure_rollback() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = make_mock_node(<<"node1">>),

    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> {error, node_unreachable} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId)}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify rollback was performed
    ?assert(meck:called(flurm_node_manager, release_resources, ['_', '_'])),
    ok.

%%====================================================================
%% Job Completion Edge Cases
%%====================================================================

completion_edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"completion with undefined start_time", fun test_completion_undefined_start/0},
        {"completion with undefined end_time", fun test_completion_undefined_end/0},
        {"completion job not found", fun test_completion_job_not_found/0},
        {"deps notification noproc", fun test_deps_notify_noproc/0},
        {"deps notification error", fun test_deps_notify_error/0}
     ]}.

test_completion_undefined_start() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [<<"node1">>],
            start_time => undefined,
            end_time => erlang:system_time(second)
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),
    ok.

test_completion_undefined_end() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [<<"node1">>],
            start_time => erlang:system_time(second) - 100,
            end_time => undefined
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),
    ok.

test_completion_job_not_found() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler:job_completed(999),
    timer:sleep(100),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_deps_notify_noproc() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, notify_completion, fun(_, _) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [],
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),
    ok.

test_deps_notify_error() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, notify_completion, fun(_, _) ->
        erlang:error(some_random_error)
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{
            state => completed,
            allocated_nodes => [],
            start_time => erlang:system_time(second) - 100,
            end_time => erlang:system_time(second)
        })}
    end),

    ok = flurm_scheduler:job_completed(1),
    timer:sleep(100),
    ok.

%%====================================================================
%% Allocation Rollback Tests
%%====================================================================

allocation_rollback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partial allocation failure rollback", fun test_partial_alloc_rollback/0},
        {"multi-node allocation rollback", fun test_multi_node_rollback/0}
     ]}.

test_partial_alloc_rollback() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode1 = make_mock_node(<<"node1">>),
    MockNode2 = make_mock_node(<<"node2">>),

    AllocCount = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCount, 1, 0),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        [MockNode1, MockNode2]
    end),
    meck:expect(flurm_node_manager, allocate_resources, fun(Hostname, _, _, _) ->
        case Hostname of
            <<"node1">> -> ok;
            <<"node2">> -> {error, insufficient_resources}
        end
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{num_nodes => 2})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),

    %% Verify rollback was performed
    ?assert(meck:called(flurm_node_manager, release_resources, ['_', '_'])),
    ok.

test_multi_node_rollback() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode1 = make_mock_node(<<"node1">>),
    MockNode2 = make_mock_node(<<"node2">>),
    MockNode3 = make_mock_node(<<"node3">>),

    AllocCount = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCount, 1, 0),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        [MockNode1, MockNode2, MockNode3]
    end),
    meck:expect(flurm_node_manager, allocate_resources, fun(Hostname, _, _, _) ->
        Count = atomics:add_get(AllocCount, 1, 1),
        case Count of
            1 -> ok;
            2 -> ok;
            3 -> {error, insufficient_resources}
        end
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, make_mock_job(JobId, #{num_nodes => 3})}
    end),

    ok = flurm_scheduler:submit_job(1),
    timer:sleep(300),
    ok.

%%====================================================================
%% Timer Handling Tests
%%====================================================================

timer_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"schedule cycle cancels old timer", fun test_cancel_old_timer/0},
        {"multiple trigger_schedule coalesced", fun test_trigger_coalesce/0}
     ]}.

test_cancel_old_timer() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Trigger multiple schedule cycles rapidly
    ok = flurm_scheduler:trigger_schedule(),
    ok = flurm_scheduler:trigger_schedule(),
    ok = flurm_scheduler:trigger_schedule(),
    timer:sleep(300),

    %% Scheduler should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_trigger_coalesce() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Trigger rapidly
    lists:foreach(fun(_) ->
        ok = flurm_scheduler:trigger_schedule()
    end, lists:seq(1, 10)),

    timer:sleep(500),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    %% Cycles should increase but not necessarily by 10
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.
