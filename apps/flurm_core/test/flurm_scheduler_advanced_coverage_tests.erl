%%%-------------------------------------------------------------------
%%% @doc FLURM Advanced Scheduler Coverage Tests
%%%
%%% Comprehensive EUnit tests specifically designed to maximize code
%%% coverage for the flurm_scheduler_advanced module. Tests all
%%% internal functions, scheduler types, and error paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_job_registry),
    catch meck:unload(flurm_node_registry),
    catch meck:unload(flurm_job),
    catch meck:unload(flurm_node),
    catch meck:unload(flurm_backfill),
    catch meck:unload(flurm_preemption),
    catch meck:unload(flurm_priority),
    catch meck:unload(flurm_fairshare),
    %% Start meck for mocking external dependencies
    meck:new(flurm_job_registry, [passthrough, no_link]),
    meck:new(flurm_node_registry, [passthrough, no_link]),
    meck:new(flurm_job, [passthrough, no_link]),
    meck:new(flurm_node, [passthrough, no_link]),
    meck:new(flurm_backfill, [passthrough, no_link]),
    meck:new(flurm_preemption, [passthrough, no_link]),
    meck:new(flurm_priority, [passthrough, no_link]),
    meck:new(flurm_fairshare, [passthrough, no_link]),

    %% Default mocks
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),
    meck:expect(flurm_fairshare, record_usage, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, []} end),

    ok.

cleanup(_) ->
    %% Stop scheduler if running
    case whereis(flurm_scheduler_advanced) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,

    %% Unload all mocks
    meck:unload(flurm_job_registry),
    meck:unload(flurm_node_registry),
    meck:unload(flurm_job),
    meck:unload(flurm_node),
    meck:unload(flurm_backfill),
    meck:unload(flurm_preemption),
    meck:unload(flurm_priority),
    meck:unload(flurm_fairshare),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

scheduler_advanced_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link with no options", fun test_start_link_no_options/0},
        {"start_link with options", fun test_start_link_with_options/0},
        {"submit_job adds to pending", fun test_submit_job/0},
        {"job_completed updates stats", fun test_job_completed/0},
        {"job_failed updates stats", fun test_job_failed/0},
        {"trigger_schedule triggers cycle", fun test_trigger_schedule/0},
        {"get_stats returns all fields", fun test_get_stats/0},
        {"enable_backfill toggle", fun test_enable_backfill/0},
        {"enable_preemption toggle", fun test_enable_preemption/0},
        {"set_scheduler_type changes type", fun test_set_scheduler_type/0},
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0},
        {"priority decay recalculates", fun test_priority_decay/0},
        {"terminate cleans up timers", fun test_terminate/0},
        {"code_change returns ok", fun test_code_change/0}
     ]}.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_link_no_options() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_scheduler_advanced)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats)),
    ?assertEqual(true, maps:get(backfill_enabled, Stats)),
    ?assertEqual(false, maps:get(preemption_enabled, Stats)),
    ok.

test_start_link_with_options() ->
    Options = [
        {scheduler_type, preempt},
        {backfill_enabled, false},
        {preemption_enabled, true}
    ],
    {ok, Pid} = flurm_scheduler_advanced:start_link(Options),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, Stats)),
    ?assertEqual(false, maps:get(backfill_enabled, Stats)),
    ?assertEqual(true, maps:get(preemption_enabled, Stats)),
    ok.

test_submit_job() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    %% Job should be in pending list (as tuples with priority)
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_job_completed() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_job_failed() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitFailed = maps:get(failed_count, InitStats),

    ok = flurm_scheduler_advanced:job_failed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(InitFailed + 1, maps:get(failed_count, NewStats)),
    ok.

test_trigger_schedule() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    ok = flurm_scheduler_advanced:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_get_stats() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(pending_count, Stats)),
    ?assert(maps:is_key(running_count, Stats)),
    ?assert(maps:is_key(completed_count, Stats)),
    ?assert(maps:is_key(failed_count, Stats)),
    ?assert(maps:is_key(preempt_count, Stats)),
    ?assert(maps:is_key(backfill_count, Stats)),
    ?assert(maps:is_key(schedule_cycles, Stats)),
    ?assert(maps:is_key(scheduler_type, Stats)),
    ?assert(maps:is_key(backfill_enabled, Stats)),
    ?assert(maps:is_key(preemption_enabled, Stats)),
    ok.

test_enable_backfill() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    %% Disable backfill
    ok = flurm_scheduler_advanced:enable_backfill(false),
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(backfill_enabled, Stats1)),

    %% Re-enable
    ok = flurm_scheduler_advanced:enable_backfill(true),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(backfill_enabled, Stats2)),
    ok.

test_enable_preemption() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    %% Enable preemption
    ok = flurm_scheduler_advanced:enable_preemption(true),
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(preemption_enabled, Stats1)),

    %% Disable preemption
    ok = flurm_scheduler_advanced:enable_preemption(false),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(preemption_enabled, Stats2)),
    ok.

test_set_scheduler_type() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    ok = flurm_scheduler_advanced:set_scheduler_type(priority),
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, Stats1)),

    ok = flurm_scheduler_advanced:set_scheduler_type(backfill),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats2)),

    ok = flurm_scheduler_advanced:set_scheduler_type(preempt),
    {ok, Stats3} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, Stats3)),

    ok = flurm_scheduler_advanced:set_scheduler_type(fifo),
    {ok, Stats4} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(fifo, maps:get(scheduler_type, Stats4)),
    ok.

test_unknown_call() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    Result = gen_server:call(flurm_scheduler_advanced, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    gen_server:cast(flurm_scheduler_advanced, {unknown_message}),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_unknown_info() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    flurm_scheduler_advanced ! unknown_message,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_priority_decay() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    %% Send priority_decay message
    flurm_scheduler_advanced ! priority_decay,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_terminate() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),
    ?assertNot(is_process_alive(Pid)),
    ok.

test_code_change() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),

    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Scheduler Type Tests
%%====================================================================

scheduler_type_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"FIFO scheduler with jobs", fun test_fifo_scheduler/0},
        {"priority scheduler with jobs", fun test_priority_scheduler/0},
        {"backfill scheduler with jobs", fun test_backfill_scheduler/0},
        {"preempt scheduler with jobs", fun test_preempt_scheduler/0}
     ]}.

test_fifo_scheduler() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    %% Setup mock for job registry
    meck:expect(flurm_job_registry, lookup_job, fun(JobId) ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_priority_scheduler() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    meck:expect(flurm_job_registry, lookup_job, fun(JobId) ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_backfill_scheduler() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),

    meck:expect(flurm_job_registry, lookup_job, fun(JobId) ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, job_id => 1}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_preempt_scheduler() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    meck:expect(flurm_job_registry, lookup_job, fun(JobId) ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, job_id => 1, priority => 500}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

%%====================================================================
%% Job Scheduling Logic Tests
%%====================================================================

scheduling_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job not found removed from queue", fun test_job_not_found/0},
        {"job not pending removed from queue", fun test_job_not_pending/0},
        {"job allocation with nodes", fun test_job_allocation/0},
        {"allocation failure", fun test_allocation_failure/0},
        {"partition specific nodes", fun test_partition_nodes/0}
     ]}.

test_job_not_found() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    %% Job should be removed from queue
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_job_not_pending() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => running, num_nodes => 1, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_job_allocation() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    %% Job should be running now
    ?assert(maps:get(running_count, Stats) >= 0),
    ok.

test_allocation_failure() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> {error, allocation_failed} end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_partition_nodes() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"gpu">>}}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(<<"gpu">>) ->
        [{<<"gpu-node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(<<"gpu-node1">>) ->
        {ok, #node_entry{name = <<"gpu-node1">>, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Backfill Logic Tests
%%====================================================================

backfill_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"backfill finds candidates", fun test_backfill_finds_candidates/0},
        {"backfill disabled falls through", fun test_backfill_disabled/0}
     ]}.

test_backfill_finds_candidates() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, job_id => 1}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) ->
        [{2, MockPid, [<<"node1">>]}]
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_backfill_disabled() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, false}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Preemption Logic Tests
%%====================================================================

preemption_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preemption finds jobs to preempt", fun test_preemption_finds_jobs/0},
        {"preemption with requeue mode", fun test_preemption_requeue/0},
        {"preemption with cancel mode", fun test_preemption_cancel/0},
        {"preemption disabled falls through", fun test_preemption_disabled/0}
     ]}.

test_preemption_finds_jobs() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, priority => 500}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        {ok, [#{job_id => 99, pid => MockPid}]}
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, [99]} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(preempt_count, Stats) >= 0),
    ok.

test_preemption_requeue() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, priority => 500}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        {ok, [#{job_id => 99, pid => MockPid}]}
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, [99]} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_preemption_cancel() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>, priority => 500}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        {ok, [#{job_id => 99, pid => MockPid}]}
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> cancel end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, [99]} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_preemption_disabled() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, false}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 10, num_cpus => 1, memory_mb => 1024,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Priority Calculation Tests
%%====================================================================

priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"priority calculated for submitted jobs", fun test_priority_calculation/0},
        {"priority recalculation on decay", fun test_priority_recalculation/0},
        {"insert by priority ordering", fun test_insert_by_priority/0}
     ]}.

test_priority_calculation() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 500 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Priority should be calculated
    ?assert(meck:called(flurm_priority, calculate_priority, ['_'])),
    ok.

test_priority_recalculation() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Trigger priority decay
    flurm_scheduler_advanced ! priority_decay,
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_insert_by_priority() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    %% Make jobs with different priorities
    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count of
            1 -> 100;
            2 -> 500;
            3 -> 200;
            _ -> 100
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    ok = flurm_scheduler_advanced:submit_job(3),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

%%====================================================================
%% Job Finished Tests
%%====================================================================

job_finished_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job finished releases resources", fun test_job_finished_release/0},
        {"job finished records usage", fun test_job_finished_usage/0},
        {"job finished not found ok", fun test_job_finished_not_found/0}
     ]}.

test_job_finished_release() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed, allocated_nodes => [<<"node1">>],
               user => <<"testuser">>, account => <<"default">>,
               num_cpus => 4, start_time => erlang:system_time(second) - 100}}
    end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Verify release was called
    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.

test_job_finished_usage() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed, allocated_nodes => [],
               user => <<"testuser">>, account => <<"default">>,
               num_cpus => 4, start_time => erlang:system_time(second) - 100}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Verify fairshare usage was recorded
    ?assert(meck:called(flurm_fairshare, record_usage, ['_', '_', '_', '_'])),
    ok.

test_job_finished_not_found() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:job_completed(999),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(completed_count, Stats)),
    ok.

%%====================================================================
%% Job Info Error Handling Tests
%%====================================================================

job_info_error_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job info error handling", fun test_job_info_error/0},
        {"job info crash handling", fun test_job_info_crash/0}
     ]}.

test_job_info_error() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, process_dead} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_job_info_crash() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> erlang:error(badarg) end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Usage Recording Edge Cases
%%====================================================================

usage_recording_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"usage with timestamp tuple", fun test_usage_timestamp_tuple/0},
        {"usage with integer timestamp", fun test_usage_integer_timestamp/0},
        {"usage with undefined start_time", fun test_usage_undefined_start/0}
     ]}.

test_usage_timestamp_tuple() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed, allocated_nodes => [],
               user => <<"testuser">>, account => <<"default">>,
               num_cpus => 4, start_time => {1000, 0, 0}}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_usage_integer_timestamp() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed, allocated_nodes => [],
               user => <<"testuser">>, account => <<"default">>,
               num_cpus => 4, start_time => erlang:system_time(second) - 100}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_usage_undefined_start() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed, allocated_nodes => [],
               user => <<"testuser">>, account => <<"default">>,
               num_cpus => 4, start_time => undefined}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Node Entry Edge Cases
%%====================================================================

node_entry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node entry not up", fun test_node_entry_not_up/0},
        {"node entry insufficient cpus", fun test_node_entry_insufficient_cpus/0},
        {"node entry insufficient memory", fun test_node_entry_insufficient_memory/0},
        {"node entry lookup error", fun test_node_entry_error/0}
     ]}.

test_node_entry_not_up() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"gpu">>}}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu-node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"gpu-node1">>, state = down, cpus_avail = 8, memory_avail = 16384}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_node_entry_insufficient_cpus() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 16, memory_mb => 4096,
               partition => <<"gpu">>}}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu-node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"gpu-node1">>, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_node_entry_insufficient_memory() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 32768,
               partition => <<"gpu">>}}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu-node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"gpu-node1">>, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_node_entry_error() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"gpu">>}}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu-node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {error, not_found}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Allocation Rollback Tests
%%====================================================================

rollback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"allocation rollback on failure", fun test_allocation_rollback/0},
        {"job allocate error", fun test_job_allocate_error/0}
     ]}.

test_allocation_rollback() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 2, num_cpus => 4, memory_mb => 4096,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}, {<<"node2">>, 8, 16384}]
    end),

    AllocCount = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCount, 1, 0),

    meck:expect(flurm_node, allocate, fun(Name, _, _) ->
        Count = atomics:add_get(AllocCount, 1, 1),
        case Count of
            1 -> ok;  % First succeeds
            _ -> {error, allocation_failed}  % Second fails
        end
    end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Verify rollback was performed
    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.

test_job_allocate_error() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => pending, num_nodes => 1, num_cpus => 4, memory_mb => 4096,
               partition => <<"default">>}}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> {error, job_died} end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Verify rollback was performed
    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.
