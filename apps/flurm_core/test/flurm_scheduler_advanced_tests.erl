%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_scheduler_advanced module
%%%
%%% Tests the advanced scheduler with:
%%% - Different scheduler types (FIFO, priority, backfill, preempt)
%%% - Backfill scheduling
%%% - Preemption scheduling
%%% - Priority-based scheduling
%%% - Fair-share integration
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Start registries and supervisors
    {ok, JobRegistryPid} = flurm_job_registry:start_link(),
    {ok, JobSupPid} = flurm_job_sup:start_link(),
    {ok, NodeRegistryPid} = flurm_node_registry:start_link(),
    {ok, NodeSupPid} = flurm_node_sup:start_link(),

    #{
        job_registry => JobRegistryPid,
        job_sup => JobSupPid,
        node_registry => NodeRegistryPid,
        node_sup => NodeSupPid
    }.

cleanup(#{job_registry := JobRegistryPid,
          job_sup := JobSupPid,
          node_registry := NodeRegistryPid,
          node_sup := NodeSupPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Stop all nodes
    [flurm_node_sup:stop_node(Pid) || Pid <- flurm_node_sup:which_nodes()],
    %% Stop processes with proper monitor/wait pattern
    lists:foreach(fun(Pid) ->
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
    end, [NodeSupPid, NodeRegistryPid, JobSupPid, JobRegistryPid]),
    ok.

%%====================================================================
%% Scheduler Start/Stop Tests
%%====================================================================

scheduler_lifecycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start with default options", fun test_start_default/0},
        {"start with fifo scheduler type", fun test_start_fifo/0},
        {"start with priority scheduler type", fun test_start_priority/0},
        {"start with backfill scheduler type", fun test_start_backfill/0},
        {"start with preempt scheduler type", fun test_start_preempt/0},
        {"start with custom options", fun test_start_custom_options/0}
     ]}.

test_start_default() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Check default stats
    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats)),
    ?assertEqual(true, maps:get(backfill_enabled, Stats)),
    ?assertEqual(false, maps:get(preemption_enabled, Stats)),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

test_start_fifo() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(fifo, maps:get(scheduler_type, Stats)),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

test_start_priority() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, priority}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(priority, maps:get(scheduler_type, Stats)),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

test_start_backfill() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, backfill}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(backfill, maps:get(scheduler_type, Stats)),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

test_start_preempt() ->
    {ok, Pid} = flurm_scheduler_advanced:start_link([{scheduler_type, preempt}]),
    ?assert(is_pid(Pid)),

    {ok, Stats} = flurm_scheduler_advanced:get_stats(),
    ?assertEqual(preempt, maps:get(scheduler_type, Stats)),

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

test_start_custom_options() ->
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

    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Configuration API Tests
%%====================================================================

config_api_test_() ->
    {foreach,
     fun() ->
         State = setup(),
         {ok, SchedPid} = flurm_scheduler_advanced:start_link(),
         State#{scheduler => SchedPid}
     end,
     fun(#{scheduler := SchedPid} = State) ->
         catch unlink(SchedPid),
         catch gen_server:stop(SchedPid, shutdown, 5000),
         cleanup(maps:remove(scheduler, State))
     end,
     [
        {"enable and disable backfill", fun test_enable_backfill/0},
        {"enable and disable preemption", fun test_enable_preemption/0},
        {"set scheduler type", fun test_set_scheduler_type/0},
        {"get stats returns all fields", fun test_get_stats_fields/0}
     ]}.

test_enable_backfill() ->
    %% Initially enabled
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
    %% Initially disabled
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
    %% Set to FIFO
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
    {ok, Stats} = flurm_scheduler_advanced:get_stats(),

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
     fun() ->
         State = setup(),
         {ok, SchedPid} = flurm_scheduler_advanced:start_link([{scheduler_type, fifo}]),
         State#{scheduler => SchedPid}
     end,
     fun(#{scheduler := SchedPid} = State) ->
         catch unlink(SchedPid),
         catch gen_server:stop(SchedPid, shutdown, 5000),
         cleanup(maps:remove(scheduler, State))
     end,
     [
        {"trigger schedule doesn't crash", fun test_trigger_schedule/0},
        {"job completed increases count", fun test_job_completed/0},
        {"job failed increases count", fun test_job_failed/0}
     ]}.

test_trigger_schedule() ->
    %% Should not crash
    ok = flurm_scheduler_advanced:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be able to get stats
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_job_completed() ->
    %% Get initial stats
    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    %% Use a job ID that doesn't exist - this should still work
    %% as job_completed just updates internal counters
    ok = flurm_scheduler_advanced:job_completed(99999),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Completed count should increase
    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    NewCompleted = maps:get(completed_count, NewStats),
    ?assertEqual(InitCompleted + 1, NewCompleted),
    ok.

test_job_failed() ->
    %% Get initial stats
    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitFailed = maps:get(failed_count, InitStats),

    %% Use a job ID that doesn't exist
    ok = flurm_scheduler_advanced:job_failed(99998),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Failed count should increase
    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    NewFailed = maps:get(failed_count, NewStats),
    ?assertEqual(InitFailed + 1, NewFailed),
    ok.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun() ->
         State = setup(),
         {ok, SchedPid} = flurm_scheduler_advanced:start_link(),
         State#{scheduler => SchedPid}
     end,
     fun(#{scheduler := SchedPid} = State) ->
         catch unlink(SchedPid),
         catch gen_server:stop(SchedPid, shutdown, 5000),
         cleanup(maps:remove(scheduler, State))
     end,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast doesn't crash", fun test_unknown_cast/0},
        {"unknown info doesn't crash", fun test_unknown_info/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_scheduler_advanced, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    %% Should not crash
    gen_server:cast(flurm_scheduler_advanced, {unknown_message}),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be able to get stats
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

test_unknown_info() ->
    %% Send unknown info message directly
    flurm_scheduler_advanced ! unknown_message,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be able to get stats
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

%%====================================================================
%% Priority Decay Tests
%%====================================================================

priority_decay_test_() ->
    {setup,
     fun() ->
         State = setup(),
         {ok, SchedPid} = flurm_scheduler_advanced:start_link(),
         State#{scheduler => SchedPid}
     end,
     fun(#{scheduler := SchedPid} = State) ->
         catch unlink(SchedPid),
         catch gen_server:stop(SchedPid, shutdown, 5000),
         cleanup(maps:remove(scheduler, State))
     end,
     fun(_) ->
         [
             {"priority decay message handled", fun test_priority_decay/0}
         ]
     end}.

test_priority_decay() ->
    %% Send priority_decay message directly
    flurm_scheduler_advanced ! priority_decay,
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Should still be able to get stats
    {ok, _Stats} = flurm_scheduler_advanced:get_stats(),
    ok.

%%====================================================================
%% Schedule Cycle Tests
%%====================================================================

schedule_cycle_test_() ->
    {foreach,
     fun() ->
         State = setup(),
         {ok, SchedPid} = flurm_scheduler_advanced:start_link(),
         State#{scheduler => SchedPid}
     end,
     fun(#{scheduler := SchedPid} = State) ->
         catch unlink(SchedPid),
         catch gen_server:stop(SchedPid, shutdown, 5000),
         cleanup(maps:remove(scheduler, State))
     end,
     [
        {"schedule cycle increments counter", fun test_schedule_cycle_counter/0},
        {"schedule cycle runs periodically", fun test_schedule_cycle_periodic/0}
     ]}.

test_schedule_cycle_counter() ->
    %% Get initial cycle count
    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Trigger schedule
    ok = flurm_scheduler_advanced:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler_advanced),

    %% Cycle count should increase
    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    NewCycles = maps:get(schedule_cycles, NewStats),
    ?assert(NewCycles > InitCycles),
    ok.

test_schedule_cycle_periodic() ->
    %% Get initial cycle count
    {ok, InitStats} = flurm_scheduler_advanced:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    %% Wait for periodic scheduling (legitimate wait for 100ms timer-based scheduler interval)
    timer:sleep(500),

    %% Cycle count should increase
    {ok, NewStats} = flurm_scheduler_advanced:get_stats(),
    NewCycles = maps:get(schedule_cycles, NewStats),
    ?assert(NewCycles > InitCycles),
    ok.

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"clean shutdown", fun test_clean_shutdown/0}
         ]
     end}.

test_clean_shutdown() ->
    %% Start scheduler
    {ok, Pid} = flurm_scheduler_advanced:start_link(),
    ?assert(is_process_alive(Pid)),

    %% Unlink before stopping to prevent EXIT signal to test process
    catch unlink(Pid),
    %% Stop cleanly
    ok = gen_server:stop(Pid, shutdown, 5000),
    ?assertNot(is_process_alive(Pid)),
    ok.
