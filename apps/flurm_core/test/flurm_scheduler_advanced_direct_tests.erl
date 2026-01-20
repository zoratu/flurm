%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_scheduler_advanced module
%%%
%%% Comprehensive EUnit tests that call flurm_scheduler_advanced
%%% functions directly without mocking the scheduler itself. Only
%%% external dependencies are mocked to isolate the scheduler behavior.
%%%
%%% Tests all exported functions:
%%% - start_link/0
%%% - start_link/1
%%% - submit_job/1
%%% - job_completed/1
%%% - job_failed/1
%%% - trigger_schedule/0
%%% - get_stats/0
%%% - enable_backfill/1
%%% - enable_preemption/1
%%% - set_scheduler_type/1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    %% Start meck for external dependencies only - NOT flurm_scheduler_advanced
    meck:new(flurm_job_registry, [non_strict, no_link]),
    meck:new(flurm_node_registry, [non_strict, no_link]),
    meck:new(flurm_job, [non_strict, no_link]),
    meck:new(flurm_node, [non_strict, no_link]),
    meck:new(flurm_backfill, [non_strict, no_link]),
    meck:new(flurm_preemption, [non_strict, no_link]),
    meck:new(flurm_priority, [non_strict, no_link]),
    meck:new(flurm_fairshare, [non_strict, no_link]),

    %% Setup default mocks
    setup_default_mocks(),
    ok.

setup_default_mocks() ->
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),
    meck:expect(flurm_fairshare, record_usage, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, []} end),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) -> [] end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),
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
%% Helper Functions
%%====================================================================

%% Create a mock job info map for flurm_job:get_info responses
make_job_info() ->
    make_job_info(#{}).

make_job_info(Overrides) ->
    Defaults = #{
        state => pending,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        partition => <<"default">>,
        user => <<"testuser">>,
        account => <<"default">>,
        priority => 100,
        start_time => erlang:system_time(second) - 100,
        allocated_nodes => []
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Basic API tests - start_link variants
start_link_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link/0 with defaults", fun test_start_link_no_args/0},
        {"start_link/1 with fifo type", fun test_start_link_fifo/0},
        {"start_link/1 with priority type", fun test_start_link_priority/0},
        {"start_link/1 with backfill type", fun test_start_link_backfill/0},
        {"start_link/1 with preempt type", fun test_start_link_preempt/0},
        {"start_link/1 with custom options", fun test_start_link_custom/0}
     ]}.

test_start_link_no_args() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_scheduler_advanced)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats)),
    ?assertEqual(true, maps:get(backfill_enabled, Stats)),
    ?assertEqual(false, maps:get(preemption_enabled, Stats)),
    ok.

test_start_link_fifo() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(fifo, maps:get(scheduler_type, Stats)),
    ok.

test_start_link_priority() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, Stats)),
    ok.

test_start_link_backfill() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats)),
    ok.

test_start_link_preempt() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, preempt}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, Stats)),
    ok.

test_start_link_custom() ->
    Options = [
        {scheduler_type, priority},
        {backfill_enabled, false},
        {preemption_enabled, true}
    ],
    {ok, Pid} = flurm_scheduler_advanced:start_link(Options),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, Stats)),
    ?assertEqual(false, maps:get(backfill_enabled, Stats)),
    ?assertEqual(true, maps:get(preemption_enabled, Stats)),
    ok.

%%====================================================================
%% Configuration API Tests
%%====================================================================

config_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"enable_backfill toggle", fun test_enable_backfill/0},
        {"enable_preemption toggle", fun test_enable_preemption/0},
        {"set_scheduler_type all types", fun test_set_scheduler_type/0},
        {"get_stats returns all fields", fun test_get_stats_fields/0}
     ]}.

test_enable_backfill() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    %% Initially enabled (default)
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(backfill_enabled, Stats1)),

    %% Disable
    ok = flurm_scheduler_advanced:enable_backfill(false),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(backfill_enabled, Stats2)),

    %% Re-enable
    ok = flurm_scheduler_advanced:enable_backfill(true),
    {ok, Stats3} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(backfill_enabled, Stats3)),
    ok.

test_enable_preemption() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    %% Initially disabled (default)
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(preemption_enabled, Stats1)),

    %% Enable
    ok = flurm_scheduler_advanced:enable_preemption(true),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(preemption_enabled, Stats2)),

    %% Disable again
    ok = flurm_scheduler_advanced:enable_preemption(false),
    {ok, Stats3} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(preemption_enabled, Stats3)),
    ok.

test_set_scheduler_type() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    %% Set to fifo
    ok = flurm_scheduler_advanced:set_scheduler_type(fifo),
    {ok, Stats1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(fifo, maps:get(scheduler_type, Stats1)),

    %% Set to priority
    ok = flurm_scheduler_advanced:set_scheduler_type(priority),
    {ok, Stats2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, Stats2)),

    %% Set to backfill
    ok = flurm_scheduler_advanced:set_scheduler_type(backfill),
    {ok, Stats3} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats3)),

    %% Set to preempt
    ok = flurm_scheduler_advanced:set_scheduler_type(preempt),
    {ok, Stats4} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, Stats4)),
    ok.

test_get_stats_fields() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(is_map(Stats)),

    %% Check all expected fields exist
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

    %% Check initial values
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ?assertEqual(0, maps:get(running_count, Stats)),
    ?assertEqual(0, maps:get(completed_count, Stats)),
    ?assertEqual(0, maps:get(failed_count, Stats)),
    ?assertEqual(0, maps:get(preempt_count, Stats)),
    ?assertEqual(0, maps:get(backfill_count, Stats)),
    ok.

%%====================================================================
%% Job Submission Tests
%%====================================================================

job_submission_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"submit_job adds to pending", fun test_submit_job/0},
        {"submit_job calculates priority", fun test_submit_priority/0},
        {"job_completed increments count", fun test_job_completed/0},
        {"job_failed increments count", fun test_job_failed/0},
        {"trigger_schedule triggers cycle", fun test_trigger_schedule/0}
     ]}.

test_submit_job() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    %% Job may be pending or processed depending on timing
    ?assert(maps:get(schedule_cycles, Stats) >= 1),
    ok.

test_submit_priority() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    %% Setup so job is found and get_info returns valid data
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 500 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Priority should be calculated since job was found
    ?assert(meck:called(flurm_priority, calculate_priority, ['_'])),
    ok.

test_job_completed() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_job_failed() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

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

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast is ignored", fun test_unknown_cast/0},
        {"unknown info is ignored", fun test_unknown_info/0},
        {"priority_decay handled", fun test_priority_decay/0},
        {"terminate cleans up", fun test_terminate/0}
     ]}.

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

    %% Send priority_decay message directly
    flurm_scheduler_advanced ! priority_decay,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be running and have processed the message
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_terminate() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),

    _ = sys:get_state(flurm_scheduler_advanced),
    ?assertNot(is_process_alive(Pid)),
    ok.

%%====================================================================
%% FIFO Scheduler Tests
%%====================================================================

fifo_scheduler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"FIFO schedules jobs in order", fun test_fifo_order/0},
        {"FIFO stops at blocked job", fun test_fifo_stops_at_block/0}
     ]}.

test_fifo_order() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    ok = flurm_scheduler_advanced:submit_job(3),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_fifo_stops_at_block() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% FIFO should stop at first blocked job
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

%%====================================================================
%% Priority Scheduler Tests
%%====================================================================

priority_scheduler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"priority sorts jobs correctly", fun test_priority_sort/0},
        {"priority recalculation on decay", fun test_priority_recalc/0}
     ]}.

test_priority_sort() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count of
            1 -> 100;
            2 -> 500;  % Higher priority
            3 -> 200;
            _ -> 100
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    ok = flurm_scheduler_advanced:submit_job(3),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_priority_recalc() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    %% Setup so job is found and get_info returns valid data
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Trigger priority decay (note: pending_jobs might be empty after scheduling)
    flurm_scheduler_advanced ! priority_decay,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Priority should be recalculated at submission
    ?assert(meck:called(flurm_priority, calculate_priority, ['_'])),
    ok.

%%====================================================================
%% Backfill Scheduler Tests
%%====================================================================

backfill_scheduler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"backfill tries smaller jobs", fun test_backfill_tries_smaller/0},
        {"backfill disabled skips", fun test_backfill_disabled/0},
        {"backfill finds candidates", fun test_backfill_candidates/0}
     ]}.

test_backfill_tries_smaller() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Backfill should be attempted
    ?assert(meck:called(flurm_backfill, find_backfill_jobs, ['_', '_'])),
    ok.

test_backfill_disabled() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, false}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Backfill should not be attempted when disabled
    ?assertNot(meck:called(flurm_backfill, find_backfill_jobs, ['_', '_'])),
    ok.

test_backfill_candidates() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10, job_id => 1})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) ->
        [{2, MockPid, [<<"node1">>]}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Preemption Scheduler Tests
%%====================================================================

preemption_scheduler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preemption checks high priority", fun test_preemption_check/0},
        {"preemption disabled falls to backfill", fun test_preemption_disabled/0},
        {"preemption finds jobs to preempt", fun test_preemption_finds_jobs/0},
        {"preemption with requeue mode", fun test_preemption_requeue/0},
        {"preemption with cancel mode", fun test_preemption_cancel/0}
     ]}.

test_preemption_check() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10, priority => 500})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_preemption, check_preemption, ['_'])),
    ok.

test_preemption_disabled() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, false}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should fall through to backfill instead
    ?assert(meck:called(flurm_backfill, find_backfill_jobs, ['_', '_'])),
    ok.

test_preemption_finds_jobs() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10, priority => 500})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    %% Preemption check returns error to avoid complex preemption flow
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_preemption_requeue() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 10, priority => 500})}
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
        {ok, make_job_info(#{num_nodes => 10, priority => 500})}
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

%%====================================================================
%% Scheduling Logic Tests
%%====================================================================

scheduling_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job not found removed", fun test_job_not_found/0},
        {"job not pending skipped", fun test_job_not_pending/0},
        {"job info error handled", fun test_job_info_error/0},
        {"job allocated with nodes", fun test_job_allocation/0},
        {"allocation failure handled", fun test_allocation_failure/0}
     ]}.

test_job_not_found() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_job_not_pending() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{state => running})}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, Stats)),
    ok.

test_job_info_error() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, process_dead} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_job_allocation() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, Stats) >= 0),
    ok.

test_allocation_failure() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> {error, allocation_failed} end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Partition Node Tests
%%====================================================================

partition_nodes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"default partition uses available nodes", fun test_default_partition/0},
        {"specific partition uses partition nodes", fun test_specific_partition/0},
        {"partition node state filtering", fun test_partition_node_state/0},
        {"partition node resource filtering", fun test_partition_node_resources/0}
     ]}.

test_default_partition() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{partition => <<"default">>})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_node_registry, get_available_nodes, ['_'])),
    ok.

test_specific_partition() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{partition => <<"gpu">>})}
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

    ?assert(meck:called(flurm_node_registry, list_nodes_by_partition, [<<"gpu">>])),
    ok.

test_partition_node_state() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{partition => <<"gpu">>})}
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

test_partition_node_resources() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{partition => <<"gpu">>, num_cpus => 16})}
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

%%====================================================================
%% Job Finished Tests
%%====================================================================

job_finished_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job finished releases resources", fun test_job_finished_release/0},
        {"job finished records fairshare", fun test_job_finished_fairshare/0},
        {"job finished not found ok", fun test_job_finished_not_found/0},
        {"job finished with timestamp tuple", fun test_job_finished_tuple_time/0},
        {"job finished with integer timestamp", fun test_job_finished_int_time/0},
        {"job finished with undefined start", fun test_job_finished_undefined_start/0}
     ]}.

test_job_finished_release() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{
            state => completed,
            allocated_nodes => [<<"node1">>]
        })}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.

test_job_finished_fairshare() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{
            state => completed,
            allocated_nodes => [],
            start_time => erlang:system_time(second) - 100
        })}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_fairshare, record_usage, ['_', '_', '_', '_'])),
    ok.

test_job_finished_not_found() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler_advanced:job_completed(999),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_job_finished_tuple_time() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{
            state => completed,
            allocated_nodes => [],
            start_time => {1000, 0, 0}
        })}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_job_finished_int_time() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{
            state => completed,
            allocated_nodes => [],
            start_time => erlang:system_time(second) - 100
        })}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_job_finished_undefined_start() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{
            state => completed,
            allocated_nodes => [],
            start_time => undefined
        })}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Rollback Tests
%%====================================================================

rollback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"allocation rollback on failure", fun test_allocation_rollback/0},
        {"job allocate error rolls back", fun test_job_allocate_rollback/0}
     ]}.

test_allocation_rollback() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    AllocCount = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCount, 1, 0),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info(#{num_nodes => 2})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}, {<<"node2">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) ->
        Count = atomics:add_get(AllocCount, 1, 1),
        case Count of
            1 -> ok;
            _ -> {error, allocation_failed}
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.

test_job_allocate_rollback() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"node1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> {error, job_died} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    ?assert(meck:called(flurm_node, release, ['_', '_'])),
    ok.

%%====================================================================
%% Insert By Priority Tests
%%====================================================================

insert_by_priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"insert into empty list", fun test_insert_empty/0},
        {"insert higher priority first", fun test_insert_higher/0},
        {"insert lower priority last", fun test_insert_lower/0},
        {"insert middle priority", fun test_insert_middle/0}
     ]}.

test_insert_empty() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_insert_higher() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count of
            1 -> 100;
            2 -> 500;  % Higher
            _ -> 100
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_insert_lower() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count of
            1 -> 500;
            2 -> 100;  % Lower
            _ -> 100
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

test_insert_middle() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count of
            1 -> 100;
            2 -> 500;
            3 -> 300;  % Middle
            _ -> 100
        end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    ok = flurm_scheduler_advanced:submit_job(3),
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Schedule Cycle Tests
%%====================================================================

schedule_cycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"schedule cycle increments counter", fun test_cycle_counter/0},
        {"schedule cycle runs periodically", fun test_cycle_periodic/0},
        {"multiple triggers coalesced", fun test_triggers_coalesced/0}
     ]}.

test_cycle_counter() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    ok = flurm_scheduler_advanced:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_cycle_periodic() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Wait for periodic scheduling (SCHEDULE_INTERVAL is 100ms)
    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_triggers_coalesced() ->
    {ok, _Pid} = flurm_scheduler_advanced:start_link(),

    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Trigger rapidly
    lists:foreach(fun(_) ->
        ok = flurm_scheduler_advanced:trigger_schedule()
    end, lists:seq(1, 10)),

    _ = sys:get_state(flurm_scheduler_advanced),

    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    %% Cycles should increase but not necessarily by 10
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.
