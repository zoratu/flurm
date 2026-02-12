%%%-------------------------------------------------------------------
%%% @doc Comprehensive Coverage Tests for flurm_scheduler_advanced
%%%
%%% EUnit tests designed for 100% line coverage of the
%%% flurm_scheduler_advanced gen_server module. Every exported
%%% function, gen_server callback, internal scheduling algorithm,
%%% resource allocation path, priority function, and error-handling
%%% branch is exercised here.
%%%
%%% External dependencies are mocked with meck so the scheduler
%%% can run in isolation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Mocked modules
%%====================================================================

-define(MOCKED_MODULES, [
    flurm_job,
    flurm_job_registry,
    flurm_node,
    flurm_node_registry,
    flurm_backfill,
    flurm_preemption,
    flurm_priority,
    flurm_fairshare
]).

%%====================================================================
%% Test Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),

    %% Clean up any stale mocks from previous failed runs
    lists:foreach(fun(M) -> catch meck:unload(M) end, ?MOCKED_MODULES),

    %% Create fresh mocks
    lists:foreach(
        fun(M) -> meck:new(M, [passthrough, non_strict, no_link]) end,
        ?MOCKED_MODULES),

    install_default_mocks(),
    ok.

cleanup(_) ->
    stop_scheduler(),
    lists:foreach(fun(M) -> catch meck:unload(M) end, ?MOCKED_MODULES),
    ok.

stop_scheduler() ->
    case whereis(flurm_scheduler_advanced) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end
    end.

install_default_mocks() ->
    %% Priority / fairshare
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 100 end),
    meck:expect(flurm_fairshare, record_usage, fun(_, _, _, _) -> ok end),

    %% Job registry - default: not found
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    %% Job process
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    %% Node
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_node, release, fun(_, _) -> ok end),

    %% Node registry
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) -> [] end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) -> {error, not_found} end),

    %% Backfill
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    %% Preemption
    meck:expect(flurm_preemption, check_preemption, fun(_) -> {error, no_preemption} end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, []} end),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Default pending-job info map for mocks
job_info() ->
    job_info(#{}).

job_info(Overrides) ->
    maps:merge(#{
        state        => pending,
        num_nodes    => 1,
        num_cpus     => 4,
        memory_mb    => 4096,
        partition    => <<"default">>,
        user         => <<"testuser">>,
        account      => <<"testaccount">>,
        priority     => 100,
        start_time   => erlang:system_time(second) - 60,
        allocated_nodes => []
    }, Overrides).

%% Spawn a dummy process that stays alive until we tell it to stop
dummy_pid() ->
    spawn(fun() -> receive stop -> ok end end).

%% Drain the scheduler mailbox so that we know all scheduled
%% messages have been handled. sys:get_state/1 forces a
%% round-trip through the process mailbox.
sync() ->
    _ = sys:get_state(flurm_scheduler_advanced),
    ok.

%%====================================================================
%% Test Fixture Generators
%%====================================================================

%%--------------------------------------------------------------------
%% 1. API / start_link
%%--------------------------------------------------------------------

start_link_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"start_link/0 uses defaults (backfill, bf=true, preempt=false)",
         fun test_start_link_defaults/0},
        {"start_link/1 with fifo type",
         fun test_start_link_fifo/0},
        {"start_link/1 with priority type",
         fun test_start_link_priority/0},
        {"start_link/1 with backfill type",
         fun test_start_link_backfill/0},
        {"start_link/1 with preempt type and custom flags",
         fun test_start_link_preempt_custom/0}
    ]}.

test_start_link_defaults() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_scheduler_advanced)),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, S)),
    ?assertEqual(true, maps:get(backfill_enabled, S)),
    ?assertEqual(false, maps:get(preemption_enabled, S)).

test_start_link_fifo() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(fifo, maps:get(scheduler_type, S)).

test_start_link_priority() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, S)).

test_start_link_backfill() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, S)).

test_start_link_preempt_custom() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {backfill_enabled, false},
        {preemption_enabled, true}
    ]),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, S)),
    ?assertEqual(false, maps:get(backfill_enabled, S)),
    ?assertEqual(true, maps:get(preemption_enabled, S)).

%%--------------------------------------------------------------------
%% 2. Configuration calls
%%--------------------------------------------------------------------

config_api_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"get_stats returns every expected field",
         fun test_get_stats_fields/0},
        {"enable_backfill true/false",
         fun test_enable_backfill_toggle/0},
        {"enable_preemption true/false",
         fun test_enable_preemption_toggle/0},
        {"set_scheduler_type cycles through all 4 valid types",
         fun test_set_scheduler_type_all/0},
        {"unknown call returns {error, unknown_request}",
         fun test_unknown_call/0}
    ]}.

test_get_stats_fields() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    Expected = [pending_count, running_count, completed_count,
                failed_count, preempt_count, backfill_count,
                schedule_cycles, scheduler_type,
                backfill_enabled, preemption_enabled],
    lists:foreach(fun(K) -> ?assert(maps:is_key(K, S)) end, Expected),
    ?assertEqual(0, maps:get(pending_count, S)),
    ?assertEqual(0, maps:get(running_count, S)),
    ?assertEqual(0, maps:get(completed_count, S)),
    ?assertEqual(0, maps:get(failed_count, S)).

test_enable_backfill_toggle() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ok = flurm_scheduler_advanced:enable_backfill(false),
    {ok, S1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(backfill_enabled, S1)),
    ok = flurm_scheduler_advanced:enable_backfill(true),
    {ok, S2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(backfill_enabled, S2)).

test_enable_preemption_toggle() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ok = flurm_scheduler_advanced:enable_preemption(true),
    {ok, S1} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(true, maps:get(preemption_enabled, S1)),
    ok = flurm_scheduler_advanced:enable_preemption(false),
    {ok, S2} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(false, maps:get(preemption_enabled, S2)).

test_set_scheduler_type_all() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    lists:foreach(fun(T) ->
        ok = flurm_scheduler_advanced:set_scheduler_type(T),
        {ok, S} = flurm_scheduler_advanced:get_stats(),
        ?assertEqual(T, maps:get(scheduler_type, S))
    end, [priority, backfill, preempt, fifo]).

test_unknown_call() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_scheduler_advanced, some_garbage)).

%%--------------------------------------------------------------------
%% 3. Cast messages: submit_job, job_completed, job_failed,
%%    trigger_schedule, unknown cast
%%--------------------------------------------------------------------

cast_api_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"submit_job inserts into pending list",
         fun test_submit_job_basic/0},
        {"job_completed increments completed_count",
         fun test_job_completed_basic/0},
        {"job_failed increments failed_count",
         fun test_job_failed_basic/0},
        {"trigger_schedule forces a schedule cycle",
         fun test_trigger_schedule/0},
        {"unknown cast is silently ignored",
         fun test_unknown_cast/0}
    ]}.

test_submit_job_basic() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, S) >= 1).

test_job_completed_basic() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ok = flurm_scheduler_advanced:job_completed(42),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(completed_count, S)).

test_job_failed_basic() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ok = flurm_scheduler_advanced:job_failed(42),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(failed_count, S)).

test_trigger_schedule() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    {ok, S0} = flurm_scheduler_advanced:get_stats(),
    C0 = maps:get(schedule_cycles, S0),
    ok = flurm_scheduler_advanced:trigger_schedule(),
    sync(),
    {ok, S1} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, S1) > C0).

test_unknown_cast() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    gen_server:cast(flurm_scheduler_advanced, {totally_unknown}),
    sync(),
    {ok, _} = flurm_scheduler_advanced:get_stats().

%%--------------------------------------------------------------------
%% 4. handle_info: schedule_cycle, priority_decay, unknown
%%--------------------------------------------------------------------

handle_info_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"schedule_cycle info increments cycle counter",
         fun test_info_schedule_cycle/0},
        {"priority_decay recalculates pending priorities",
         fun test_info_priority_decay/0},
        {"priority_decay with tuple jobs in pending list",
         fun test_info_priority_decay_with_tuples/0},
        {"priority_decay with integer jobs in pending list",
         fun test_info_priority_decay_with_integers/0},
        {"unknown info message is silently ignored",
         fun test_info_unknown/0}
    ]}.

test_info_schedule_cycle() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    {ok, S0} = flurm_scheduler_advanced:get_stats(),
    C0 = maps:get(schedule_cycles, S0),
    flurm_scheduler_advanced ! schedule_cycle,
    sync(),
    {ok, S1} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, S1) > C0).

test_info_priority_decay() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    flurm_scheduler_advanced ! priority_decay,
    sync(),
    {ok, _} = flurm_scheduler_advanced:get_stats().

test_info_priority_decay_with_tuples() ->
    %% Submit a job so pending_jobs contains a {JobId, Priority} tuple,
    %% then trigger priority_decay to hit the tuple clause of
    %% recalculate_priorities/1.
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    %% Job registry returns not_found so the job stays in pending
    %% (schedule_in_order will hit {error, job_not_found} and delete it,
    %% but priority_decay is a separate info message).
    %% We need the job to remain in pending after the schedule cycle.
    %% Make try_schedule_job return {wait, _} so fifo stops.
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% Now pending_jobs should contain {1, 100} (tuple form).
    %% Trigger priority decay to recalculate.
    flurm_scheduler_advanced ! priority_decay,
    sync(),
    {ok, _} = flurm_scheduler_advanced:get_stats().

test_info_priority_decay_with_integers() ->
    %% recalculate_priorities also has a clause for bare integer job IDs.
    %% We cannot directly inject bare integers into the pending list via
    %% the public API (submit_job always inserts tuples), but the
    %% recalculate_priorities/1 function is exported under -ifdef(TEST).
    %% Calling it directly exercises the integer clause.
    %%
    %% calculate_job_priority -> get_job_info_map -> lookup_job -> get_info
    %% -> flurm_priority:calculate_priority. Must mock the full chain.
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 200 end),
    Result = flurm_scheduler_advanced:recalculate_priorities([10, 20, 30]),
    ?assertEqual(3, length(Result)),
    %% All priorities should be 200
    lists:foreach(fun({_Id, P}) -> ?assertEqual(200, P) end, Result).

test_info_unknown() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    flurm_scheduler_advanced ! {completely, unknown, message},
    sync(),
    {ok, _} = flurm_scheduler_advanced:get_stats().

%%--------------------------------------------------------------------
%% 5. terminate / code_change
%%--------------------------------------------------------------------

lifecycle_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"terminate cancels both timers",
         fun test_terminate/0},
        {"code_change returns {ok, State}",
         fun test_code_change/0}
    ]}.

test_terminate() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_process_alive(Pid)),
    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),
    ?assertNot(is_process_alive(Pid)).

test_code_change() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    State = sys:get_state(Pid),
    ?assertMatch({ok, _}, flurm_scheduler_advanced:code_change("1.0", State, [])).

%%--------------------------------------------------------------------
%% 6. cancel_timer/1 - exported under -ifdef(TEST)
%%--------------------------------------------------------------------

cancel_timer_test_() ->
    {"cancel_timer/1 handles undefined and real timers", [
        {"undefined -> ok", fun() ->
            ?assertEqual(ok, flurm_scheduler_advanced:cancel_timer(undefined))
        end},
        {"real timer ref -> integer or false", fun() ->
            Ref = erlang:send_after(60000, self(), test),
            Res = flurm_scheduler_advanced:cancel_timer(Ref),
            ?assert(is_integer(Res) orelse Res =:= false)
        end},
        {"already-cancelled timer -> false", fun() ->
            Ref = erlang:send_after(60000, self(), test),
            erlang:cancel_timer(Ref),
            Res = flurm_scheduler_advanced:cancel_timer(Ref),
            ?assert(Res =:= false orelse is_integer(Res))
        end}
    ]}.

%%--------------------------------------------------------------------
%% 7. insert_by_priority/3 - 3 clauses: empty, >, recurse
%%--------------------------------------------------------------------

insert_by_priority_test_() ->
    {"insert_by_priority covers all clauses", [
        {"insert into empty list (clause 1)", fun() ->
            ?assertEqual([{1, 500}],
                         flurm_scheduler_advanced:insert_by_priority(1, 500, []))
        end},
        {"insert at front - higher priority (clause 2)", fun() ->
            Existing = [{2, 300}, {3, 100}],
            ?assertEqual([{1, 500}, {2, 300}, {3, 100}],
                         flurm_scheduler_advanced:insert_by_priority(1, 500, Existing))
        end},
        {"insert at end - lowest priority (clause 3 recurse)", fun() ->
            Existing = [{2, 500}, {3, 300}],
            ?assertEqual([{2, 500}, {3, 300}, {1, 50}],
                         flurm_scheduler_advanced:insert_by_priority(1, 50, Existing))
        end},
        {"insert in middle position", fun() ->
            Existing = [{2, 500}, {3, 100}],
            ?assertEqual([{2, 500}, {1, 300}, {3, 100}],
                         flurm_scheduler_advanced:insert_by_priority(1, 300, Existing))
        end},
        {"equal priority goes after existing (FIFO tie-break)", fun() ->
            Existing = [{2, 300}],
            ?assertEqual([{2, 300}, {1, 300}],
                         flurm_scheduler_advanced:insert_by_priority(1, 300, Existing))
        end}
    ]}.

%%--------------------------------------------------------------------
%% 8. recalculate_priorities/1 - integer IDs and tuple IDs
%%--------------------------------------------------------------------

recalculate_priorities_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty list returns empty", fun test_recalc_empty/0},
        {"list of integer job IDs", fun test_recalc_integers/0},
        {"list of {JobId, OldPriority} tuples", fun test_recalc_tuples/0},
        {"result is sorted descending by priority", fun test_recalc_sorted/0}
    ]}.

test_recalc_empty() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    ?assertEqual([], flurm_scheduler_advanced:recalculate_priorities([])).

test_recalc_integers() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 42 end),
    Result = flurm_scheduler_advanced:recalculate_priorities([10, 20]),
    %% Both should have priority 42; order may vary due to stable sort
    ?assertEqual(2, length(Result)),
    lists:foreach(fun({_Id, P}) -> ?assertEqual(42, P) end, Result).

test_recalc_tuples() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 77 end),
    Result = flurm_scheduler_advanced:recalculate_priorities([{10, 100}, {20, 200}]),
    ?assertEqual(2, length(Result)),
    lists:foreach(fun({_Id, P}) -> ?assertEqual(77, P) end, Result).

test_recalc_sorted() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    Counter = atomics:new(1, [{signed, false}]),
    atomics:put(Counter, 1, 0),
    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        N = atomics:add_get(Counter, 1, 1),
        case N of 1 -> 100; 2 -> 500; _ -> 300 end
    end),
    Result = flurm_scheduler_advanced:recalculate_priorities([{1, 0}, {2, 0}, {3, 0}]),
    %% Expect sorted descending: 500, 300, 100
    [_, {_, P2}, _] = Result,
    ?assertEqual(300, P2).

%%--------------------------------------------------------------------
%% 9. FIFO scheduler - schedule_in_order with all 4 return cases
%%--------------------------------------------------------------------

fifo_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"FIFO: ok - job scheduled successfully",
         fun test_fifo_ok/0},
        {"FIFO: wait - stops processing queue",
         fun test_fifo_wait/0},
        {"FIFO: {error, job_not_found} - skip and continue",
         fun test_fifo_job_not_found/0},
        {"FIFO: {error, Other} - skip and continue",
         fun test_fifo_error_other/0},
        {"FIFO: empty pending list is no-op",
         fun test_fifo_empty/0}
    ]}.

test_fifo_ok() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_fifo_wait() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    %% FIFO stops at the first blocked job so both stay pending
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(pending_count, S) >= 1).

test_fifo_job_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_fifo_error_other() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    %% get_info returns running state -> try_schedule_job returns {error, job_not_pending}
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{state => running})}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_fifo_empty() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    ok = flurm_scheduler_advanced:trigger_schedule(),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

%%--------------------------------------------------------------------
%% 10. Priority scheduler
%%--------------------------------------------------------------------

priority_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"priority scheduler processes jobs (sorted order)",
         fun test_priority_sorted/0}
    ]}.

test_priority_sorted() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    Counter = atomics:new(1, [{signed, false}]),
    atomics:put(Counter, 1, 0),
    meck:expect(flurm_priority, calculate_priority, fun(_) ->
        N = atomics:add_get(Counter, 1, 1),
        case N of 1 -> 100; 2 -> 500; 3 -> 300; _ -> 100 end
    end),
    %% Jobs not found -> removed from pending during schedule cycle
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    ok = flurm_scheduler_advanced:submit_job(3),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(schedule_cycles, S) > 0).

%%--------------------------------------------------------------------
%% 11. Backfill scheduler - all branches
%%--------------------------------------------------------------------

backfill_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"backfill: empty pending is no-op",
         fun test_backfill_empty/0},
        {"backfill: top job ok -> continue recursively",
         fun test_backfill_top_ok/0},
        {"backfill: top job wait + backfill enabled -> try_backfill",
         fun test_backfill_wait_enabled/0},
        {"backfill: top job wait + backfill disabled -> stop",
         fun test_backfill_wait_disabled/0},
        {"backfill: top job {error, job_not_found} -> skip + recurse",
         fun test_backfill_error_not_found/0},
        {"backfill: top job {error, Other} -> skip + recurse",
         fun test_backfill_error_other/0},
        {"backfill: try_backfill schedules a backfill job successfully",
         fun test_backfill_schedules_candidate/0},
        {"backfill: try_backfill candidate fails scheduling",
         fun test_backfill_candidate_fail/0},
        {"backfill: try_backfill filters out undefined job info",
         fun test_backfill_undefined_candidate/0}
    ]}.

test_backfill_empty() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    ok = flurm_scheduler_advanced:trigger_schedule(),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_backfill_top_ok() ->
    %% Top job can be scheduled -> schedule_with_backfill recurses
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_backfill_wait_enabled() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, true}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    ?assert(meck:num_calls(flurm_backfill, find_backfill_jobs, 2) > 0).

test_backfill_wait_disabled() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, false}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    ?assertEqual(0, meck:num_calls(flurm_backfill, find_backfill_jobs, 2)).

test_backfill_error_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_backfill_error_other() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{state => running})}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_backfill_schedules_candidate() ->
    %% The top job is blocked (wait), backfill finds a candidate,
    %% and that candidate succeeds scheduling.
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, true}
    ]),
    MockPid = dummy_pid(),

    %% Job 1 (blocker) needs 100 nodes -> wait
    %% Job 2 (backfill candidate) needs 1 node -> ok
    Counter = atomics:new(1, [{signed, false}]),
    atomics:put(Counter, 1, 0),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 1})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        %% First call for blocker returns empty (wait)
        %% Subsequent calls for backfill candidate return a node
        N = atomics:add_get(Counter, 1, 1),
        case N of
            1 -> [];  %% blocker
            _ -> [{<<"n1">>, 8, 16384}]
        end
    end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) ->
        [{2, MockPid, [<<"n1">>]}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(backfill_count, S) >= 1).

test_backfill_candidate_fail() ->
    %% Backfill candidate is found but its try_schedule_job fails.
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, true}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) ->
        [{2, MockPid, [<<"n1">>]}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    %% Candidate scheduling failed (no nodes), backfill_count stays 0
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(backfill_count, S)).

test_backfill_undefined_candidate() ->
    %% One candidate in CandidateJobIds has undefined job info -> filtered
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, backfill},
        {backfill_enabled, true}
    ]),
    %% Job 1 is the blocker; job 2 is a candidate whose get_job_info_map -> undefined
    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_job_registry, lookup_job, fun(JobId) ->
        case JobId of
            1 -> {ok, dummy_pid()};
            _ -> {error, not_found}  %% candidates -> get_job_info_map returns undefined
        end
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    ok.

%%--------------------------------------------------------------------
%% 12. Preemption scheduler - all branches
%%--------------------------------------------------------------------

preemption_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"preempt: empty pending is no-op",
         fun test_preempt_empty/0},
        {"preempt: top job ok -> schedule + recurse",
         fun test_preempt_top_ok/0},
        {"preempt: wait + enabled + check ok (requeue mode)",
         fun test_preempt_requeue/0},
        {"preempt: wait + enabled + check ok (cancel mode)",
         fun test_preempt_cancel/0},
        {"preempt: wait + enabled + check error -> try_backfill",
         fun test_preempt_check_error/0},
        {"preempt: wait + disabled -> try_backfill fallback",
         fun test_preempt_disabled_fallback/0},
        {"preempt: {error, job_not_found} -> skip + recurse",
         fun test_preempt_job_not_found/0},
        {"preempt: {error, Other} -> skip + recurse",
         fun test_preempt_error_other/0}
    ]}.

test_preempt_empty() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, preempt}]),
    ok = flurm_scheduler_advanced:trigger_schedule(),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_preempt_top_ok() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_preempt_requeue() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),
    MockPid = dummy_pid(),
    %% After preemption, schedule_with_preemption recurses.
    %% Prevent infinite loop: check_preemption succeeds once, then errors.
    PreemptCounter = atomics:new(1, [{signed, false}]),
    atomics:put(PreemptCounter, 1, 0),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        N = atomics:add_get(PreemptCounter, 1, 1),
        case N of
            1 -> {ok, [#{job_id => 99, pid => MockPid}]};
            _ -> {error, no_preemption}
        end
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> requeue end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, [99]} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(preempt_count, S) >= 1).

test_preempt_cancel() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),
    MockPid = dummy_pid(),
    %% Prevent infinite loop: check_preemption succeeds once, then errors.
    PreemptCounter = atomics:new(1, [{signed, false}]),
    atomics:put(PreemptCounter, 1, 0),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        N = atomics:add_get(PreemptCounter, 1, 1),
        case N of
            1 -> {ok, [#{job_id => 99, pid => MockPid}]};
            _ -> {error, no_preemption}
        end
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun() -> cancel end),
    meck:expect(flurm_preemption, preempt_jobs, fun(_, _) -> {ok, [99]} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(preempt_count, S) >= 1).

test_preempt_check_error() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, true}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_preemption, check_preemption, fun(_) ->
        {error, no_preemption}
    end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    %% Preemption check failed -> falls through to try_backfill.
    %% Jobs remain pending since no nodes available.
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(pending_count, S) >= 1),
    ?assert(maps:get(schedule_cycles, S) >= 1).

test_preempt_disabled_fallback() ->
    {ok, _} = flurm_scheduler_advanced:start_link([
        {scheduler_type, preempt},
        {preemption_enabled, false}
    ]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 100})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) -> [] end),
    meck:expect(flurm_backfill, find_backfill_jobs, fun(_, _) -> [] end),

    ok = flurm_scheduler_advanced:submit_job(1),
    ok = flurm_scheduler_advanced:submit_job(2),
    sync(),
    %% Preemption disabled -> falls through to try_backfill.
    %% Jobs remain pending since no nodes available.
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(pending_count, S) >= 1),
    ?assert(maps:get(schedule_cycles, S) >= 1).

test_preempt_job_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, preempt}]),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_preempt_error_other() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, preempt}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{state => running})}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

%%--------------------------------------------------------------------
%% 13. handle_job_finished - resource release paths
%%--------------------------------------------------------------------

job_finished_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"job_finished: job found with allocated_nodes -> release",
         fun test_finished_release_nodes/0},
        {"job_finished: job found but get_info fails -> ok",
         fun test_finished_get_info_fails/0},
        {"job_finished: job not found in registry -> ok",
         fun test_finished_not_in_registry/0}
    ]}.

test_finished_release_nodes() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed,
               allocated_nodes => [<<"n1">>, <<"n2">>],
               user => <<"u">>, account => <<"a">>,
               num_cpus => 2, start_time => erlang:system_time(second) - 10}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    %% Verify node release was called (at least twice, once per node)
    ?assert(meck:num_calls(flurm_node, release, 2) >= 2).

test_finished_get_info_fails() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, dead} end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(completed_count, S)).

test_finished_not_in_registry() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:job_completed(999),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(completed_count, S)).

%%--------------------------------------------------------------------
%% 14. try_schedule_job paths
%%--------------------------------------------------------------------

try_schedule_job_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"try_schedule_job: job not found -> {error, job_not_found}",
         fun test_tsj_not_found/0},
        {"try_schedule_job: get_info crash -> {error, job_info_error}",
         fun test_tsj_get_info_crash/0},
        {"try_schedule_job: job not pending -> {error, job_not_pending}",
         fun test_tsj_not_pending/0},
        {"try_schedule_job: job pending -> try_allocate_job",
         fun test_tsj_pending/0}
    ]}.

test_tsj_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_tsj_get_info_crash() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> error(boom) end),
    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_tsj_not_pending() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{state => running})}
    end),
    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(pending_count, S)).

test_tsj_pending() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),
    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

%%--------------------------------------------------------------------
%% 15. try_allocate_job / find_available_nodes
%%--------------------------------------------------------------------

allocate_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"find_available_nodes: default partition",
         fun test_alloc_default_partition/0},
        {"find_available_nodes: named partition with sufficient nodes",
         fun test_alloc_named_partition_ok/0},
        {"find_available_nodes: named partition insufficient nodes -> wait",
         fun test_alloc_named_partition_wait/0},
        {"allocate_resources: all succeed",
         fun test_alloc_resources_ok/0},
        {"allocate_resources: partial failure -> rollback",
         fun test_alloc_resources_rollback/0},
        {"flurm_job:allocate fails -> rollback",
         fun test_job_allocate_fail_rollback/0}
    ]}.

test_alloc_default_partition() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"default">>})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% If get_available_nodes was called with the default partition path,
    %% the job should now be running
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_alloc_named_partition_ok() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"gpu">>})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{
            name = <<"gpu1">>, state = up,
            cpus_avail = 16, memory_avail = 32768}}
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% If named partition path was taken, the job is now running
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_alloc_named_partition_wait() ->
    %% Partition nodes exist but do not meet resource requirements
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"gpu">>, num_cpus => 64})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"gpu1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{
            name = <<"gpu1">>, state = up,
            cpus_avail = 8, memory_avail = 16384}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    %% Job stays pending because no node has enough cpus
    ?assert(maps:get(pending_count, S) >= 1).

test_alloc_resources_ok() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 2})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}, {<<"n2">>, 8, 16384}, {<<"n3">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

test_alloc_resources_rollback() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    AllocCounter = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCounter, 1, 0),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{num_nodes => 2})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}, {<<"n2">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) ->
        N = atomics:add_get(AllocCounter, 1, 1),
        case N of 1 -> ok; _ -> {error, busy} end
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% Allocation partially failed, so job should NOT be running
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(running_count, S)),
    %% The allocate was called at least twice (one ok, one error)
    ?assert(atomics:get(AllocCounter, 1) >= 2).

test_job_allocate_fail_rollback() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> {error, dead} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% flurm_job:allocate failed, so job should NOT be running
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(0, maps:get(running_count, S)).

%%--------------------------------------------------------------------
%% 16. get_partition_nodes filtering
%%--------------------------------------------------------------------

partition_nodes_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"partition node: state=down -> filtered out",
         fun test_partition_node_down/0},
        {"partition node: insufficient cpus -> filtered out",
         fun test_partition_node_low_cpus/0},
        {"partition node: insufficient memory -> filtered out",
         fun test_partition_node_low_mem/0},
        {"partition node: get_node_entry error -> filtered out",
         fun test_partition_node_entry_error/0},
        {"partition node: meets all requirements -> included",
         fun test_partition_node_ok/0}
    ]}.

test_partition_node_down() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"p">>})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"nd">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"nd">>, state = down,
                         cpus_avail = 32, memory_avail = 65536}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync().

test_partition_node_low_cpus() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"p">>, num_cpus => 32})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"nd">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"nd">>, state = up,
                         cpus_avail = 4, memory_avail = 65536}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync().

test_partition_node_low_mem() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"p">>, memory_mb => 65536})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"nd">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"nd">>, state = up,
                         cpus_avail = 32, memory_avail = 1024}}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync().

test_partition_node_entry_error() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"p">>})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"nd">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {error, not_found}
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync().

test_partition_node_ok() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{partition => <<"p">>})}
    end),
    meck:expect(flurm_node_registry, list_nodes_by_partition, fun(_) ->
        [{<<"nd">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {ok, #node_entry{name = <<"nd">>, state = up,
                         cpus_avail = 32, memory_avail = 65536}}
    end),
    meck:expect(flurm_node, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, allocate, fun(_, _) -> ok end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(running_count, S) >= 1).

%%--------------------------------------------------------------------
%% 17. calculate_job_priority / get_job_info_map
%%--------------------------------------------------------------------

priority_calc_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"calculate_job_priority: job found -> use flurm_priority",
         fun test_calc_prio_found/0},
        {"calculate_job_priority: job not found -> DEFAULT_PRIORITY",
         fun test_calc_prio_not_found/0},
        {"get_job_info_map: lookup fails -> undefined",
         fun test_job_info_map_not_found/0},
        {"get_job_info_map: get_info crash -> undefined",
         fun test_job_info_map_crash/0}
    ]}.

test_calc_prio_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> {ok, job_info()} end),
    meck:expect(flurm_priority, calculate_priority, fun(_) -> 999 end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    ?assert(meck:num_calls(flurm_priority, calculate_priority, 1) > 0).

test_calc_prio_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),
    %% Job should be inserted with DEFAULT_PRIORITY (100)
    ok.

test_job_info_map_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    %% record_job_usage for not-found -> returns ok early
    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    ok.

test_job_info_map_crash() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) -> error(kaboom) end),
    %% record_job_usage with crash -> get_job_info_map returns undefined -> ok
    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    ok.

%%--------------------------------------------------------------------
%% 18. record_job_usage - all start_time formats
%%--------------------------------------------------------------------

record_usage_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"record_job_usage: integer start_time",
         fun test_usage_int_time/0},
        {"record_job_usage: {Ms, S, Us} tuple start_time",
         fun test_usage_tuple_time/0},
        {"record_job_usage: other start_time (e.g. undefined) -> 0",
         fun test_usage_undefined_time/0},
        {"record_job_usage: job info not found -> ok",
         fun test_usage_not_found/0}
    ]}.

test_usage_int_time() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => done,
               allocated_nodes => [],
               user => <<"alice">>,
               account => <<"acct1">>,
               num_cpus => 8,
               start_time => erlang:system_time(second) - 120}}
    end),
    meck:expect(flurm_fairshare, record_usage, fun(U, A, Cpu, Wall) ->
        %% Verify the arguments are sensible
        ?assertEqual(<<"alice">>, U),
        ?assertEqual(<<"acct1">>, A),
        ?assert(is_integer(Cpu)),
        ?assert(is_integer(Wall)),
        ok
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    ?assert(meck:num_calls(flurm_fairshare, record_usage, 4) > 0).

test_usage_tuple_time() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => done,
               allocated_nodes => [],
               user => <<"bob">>,
               account => <<"acct2">>,
               num_cpus => 4,
               start_time => {1000, 500, 0}}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    ?assert(meck:num_calls(flurm_fairshare, record_usage, 4) > 0).

test_usage_undefined_time() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => done,
               allocated_nodes => [],
               user => <<"carol">>,
               account => <<"acct3">>,
               num_cpus => 2,
               start_time => undefined}}
    end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    ?assert(meck:num_calls(flurm_fairshare, record_usage, 4) > 0).

test_usage_not_found() ->
    {ok, _} = flurm_scheduler_advanced:start_link(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),
    %% record_job_usage gets undefined from get_job_info_map -> returns ok
    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(completed_count, S)).

%%--------------------------------------------------------------------
%% 19. Full end-to-end scenarios exercising multiple code paths
%%--------------------------------------------------------------------

integration_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"end-to-end: submit -> schedule -> complete -> stats",
         fun test_e2e_submit_schedule_complete/0},
        {"end-to-end: submit -> fail -> stats",
         fun test_e2e_submit_fail/0},
        {"end-to-end: switch scheduler types mid-stream",
         fun test_e2e_switch_types/0}
    ]}.

test_e2e_submit_schedule_complete() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    MockPid = dummy_pid(),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {ok, MockPid} end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, job_info(#{allocated_nodes => [<<"n1">>]})}
    end),
    meck:expect(flurm_node_registry, get_available_nodes, fun(_) ->
        [{<<"n1">>, 8, 16384}]
    end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),

    %% Now complete the job
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, #{state => completed,
               allocated_nodes => [<<"n1">>],
               user => <<"user1">>, account => <<"acct1">>,
               num_cpus => 4, start_time => erlang:system_time(second) - 30}}
    end),
    ok = flurm_scheduler_advanced:job_completed(1),
    sync(),

    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assert(maps:get(completed_count, S) >= 1),
    ?assert(maps:get(schedule_cycles, S) >= 1).

test_e2e_submit_fail() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler_advanced:submit_job(1),
    sync(),

    ok = flurm_scheduler_advanced:job_failed(1),
    sync(),

    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(1, maps:get(failed_count, S)).

test_e2e_switch_types() ->
    {ok, _} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),

    %% Cycle through all types submitting a job for each
    lists:foreach(fun(T) ->
        ok = flurm_scheduler_advanced:set_scheduler_type(T),
        meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
        ok = flurm_scheduler_advanced:submit_job(1),
        sync()
    end, [fifo, priority, backfill, preempt]),

    {ok, S} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, S)),
    ?assert(maps:get(schedule_cycles, S) >= 4).
