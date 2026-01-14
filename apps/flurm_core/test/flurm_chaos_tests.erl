%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_chaos module
%%%
%%% Tests chaos engineering functionality including:
%%% - Enable/disable chaos injection
%%% - Scenario configuration and probability
%%% - Per-scenario enable/disable
%%% - Message delay injection
%%% - Process kill configuration
%%% - Network partition simulation
%%% - GC pressure testing
%%% - Scheduler suspension
%%% - Configuration helpers
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_chaos_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing chaos server
    catch gen_server:stop(flurm_chaos, normal, 1000),
    timer:sleep(50),

    %% Start fresh
    {ok, Pid} = flurm_chaos:start_link(),
    #{pid => Pid}.

cleanup(#{pid := Pid}) ->
    %% Disable all scenarios before cleanup
    catch flurm_chaos:disable(),
    catch flurm_chaos:disable_all_scenarios(),
    catch flurm_chaos:heal_all_partitions(),
    catch unlink(Pid),
    catch gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

chaos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Initial state is disabled", fun test_initial_state/0},
      {"Enable/disable chaos globally", fun test_enable_disable/0},
      {"Get/set scenarios", fun test_get_set_scenarios/0},
      {"Enable/disable specific scenarios", fun test_scenario_enable_disable/0},
      {"Enable/disable all scenarios", fun test_enable_disable_all_scenarios/0},
      {"Inject scenario once", fun test_inject_once/0},
      {"Get status", fun test_status/0},
      {"Set and get delay config", fun test_delay_config/0},
      {"Set and get kill config", fun test_kill_config/0},
      {"Set and get partition config", fun test_partition_config/0},
      {"Set and get GC config", fun test_gc_config/0},
      {"Set and get scheduler config", fun test_scheduler_config/0},
      {"Mark/unmark process protected", fun test_mark_process_protected/0},
      {"Wrap call with delay", fun test_wrap_call/0},
      {"Maybe delay call", fun test_maybe_delay_call/0},
      {"Partition node operations", fun test_partition_operations/0},
      {"GC operations", fun test_gc_operations/0},
      {"Scheduler suspension", fun test_scheduler_suspension/0},
      {"Slow disk helpers", fun test_slow_disk_helpers/0}
     ]}.

%%====================================================================
%% Basic State Tests
%%====================================================================

test_initial_state() ->
    %% Should be disabled initially
    ?assertNot(flurm_chaos:is_enabled()),

    %% All scenarios should be disabled
    ?assertNot(flurm_chaos:is_scenario_enabled(kill_random_process)),
    ?assertNot(flurm_chaos:is_scenario_enabled(delay_message)),
    ?assertNot(flurm_chaos:is_scenario_enabled(gc_pressure)),
    ?assertNot(flurm_chaos:is_scenario_enabled(memory_pressure)),
    ?assertNot(flurm_chaos:is_scenario_enabled(cpu_burn)),
    ?assertNot(flurm_chaos:is_scenario_enabled(network_partition)),
    ?assertNot(flurm_chaos:is_scenario_enabled(scheduler_suspend)),

    %% No partitions
    ?assertEqual([], flurm_chaos:get_partitions()),
    ok.

test_enable_disable() ->
    %% Enable chaos globally
    ok = flurm_chaos:enable(),
    ?assert(flurm_chaos:is_enabled()),

    %% Disable chaos
    ok = flurm_chaos:disable(),
    ?assertNot(flurm_chaos:is_enabled()),
    ok.

%%====================================================================
%% Scenario Configuration Tests
%%====================================================================

test_get_set_scenarios() ->
    %% Get default scenarios
    Scenarios = flurm_chaos:get_scenarios(),
    ?assert(is_map(Scenarios)),

    %% Default probabilities are set
    ?assert(maps:is_key(kill_random_process, Scenarios)),
    ?assert(maps:is_key(delay_message, Scenarios)),
    ?assert(maps:is_key(gc_pressure, Scenarios)),
    ?assert(maps:is_key(memory_pressure, Scenarios)),

    %% Set custom probability
    ok = flurm_chaos:set_scenario(kill_random_process, 0.5),
    NewScenarios = flurm_chaos:get_scenarios(),
    ?assertEqual(0.5, maps:get(kill_random_process, NewScenarios)),

    %% Set probability to 0
    ok = flurm_chaos:set_scenario(delay_message, 0.0),
    NewScenarios2 = flurm_chaos:get_scenarios(),
    ?assertEqual(0.0, maps:get(delay_message, NewScenarios2)),

    %% Set probability to 1
    ok = flurm_chaos:set_scenario(gc_pressure, 1.0),
    NewScenarios3 = flurm_chaos:get_scenarios(),
    ?assertEqual(1.0, maps:get(gc_pressure, NewScenarios3)),
    ok.

test_scenario_enable_disable() ->
    %% Enable a specific scenario
    ok = flurm_chaos:enable_scenario(delay_message),
    ?assert(flurm_chaos:is_scenario_enabled(delay_message)),

    %% Other scenarios still disabled
    ?assertNot(flurm_chaos:is_scenario_enabled(kill_random_process)),

    %% Disable the scenario
    ok = flurm_chaos:disable_scenario(delay_message),
    ?assertNot(flurm_chaos:is_scenario_enabled(delay_message)),
    ok.

test_enable_disable_all_scenarios() ->
    %% Enable all scenarios
    ok = flurm_chaos:enable_all_scenarios(),

    ?assert(flurm_chaos:is_scenario_enabled(kill_random_process)),
    ?assert(flurm_chaos:is_scenario_enabled(delay_message)),
    ?assert(flurm_chaos:is_scenario_enabled(gc_pressure)),
    ?assert(flurm_chaos:is_scenario_enabled(memory_pressure)),
    ?assert(flurm_chaos:is_scenario_enabled(cpu_burn)),
    ?assert(flurm_chaos:is_scenario_enabled(network_partition)),
    ?assert(flurm_chaos:is_scenario_enabled(scheduler_suspend)),

    %% Disable all scenarios
    ok = flurm_chaos:disable_all_scenarios(),

    ?assertNot(flurm_chaos:is_scenario_enabled(kill_random_process)),
    ?assertNot(flurm_chaos:is_scenario_enabled(delay_message)),
    ?assertNot(flurm_chaos:is_scenario_enabled(gc_pressure)),
    ok.

test_inject_once() ->
    %% Inject trigger_gc scenario (safe to run)
    Result = flurm_chaos:inject_once(trigger_gc),
    ?assertEqual(ok, Result),

    %% Inject gc_pressure scenario
    Result2 = flurm_chaos:inject_once(gc_pressure),
    ?assertEqual(ok, Result2),

    %% Inject memory_pressure scenario
    Result3 = flurm_chaos:inject_once(memory_pressure),
    ?assertEqual(ok, Result3),

    %% Inject cpu_burn scenario
    Result4 = flurm_chaos:inject_once(cpu_burn),
    ?assertEqual(ok, Result4),

    %% Inject delay_message scenario
    Result5 = flurm_chaos:inject_once(delay_message),
    ?assertEqual(ok, Result5),

    %% Inject drop_message scenario
    Result6 = flurm_chaos:inject_once(drop_message),
    ?assertEqual(ok, Result6),

    %% Inject slow_disk scenario
    Result7 = flurm_chaos:inject_once(slow_disk),
    ?assertEqual(ok, Result7),

    %% Unknown scenario returns error
    Result8 = flurm_chaos:inject_once(unknown_scenario),
    ?assertEqual({error, unknown_scenario}, Result8),
    ok.

test_status() ->
    %% Get status
    Status = flurm_chaos:status(),
    ?assert(is_map(Status)),

    %% Check all expected keys
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(scenarios, Status)),
    ?assert(maps:is_key(scenario_enabled, Status)),
    ?assert(maps:is_key(stats, Status)),
    ?assert(maps:is_key(tick_ms, Status)),
    ?assert(maps:is_key(protected_apps, Status)),
    ?assert(maps:is_key(protected_pids, Status)),
    ?assert(maps:is_key(partitioned_nodes, Status)),
    ?assert(maps:is_key(delay_config, Status)),
    ?assert(maps:is_key(kill_config, Status)),
    ?assert(maps:is_key(partition_config, Status)),
    ?assert(maps:is_key(gc_config, Status)),
    ?assert(maps:is_key(scheduler_config, Status)),

    %% Verify default values
    ?assertEqual(false, maps:get(enabled, Status)),
    ?assertEqual(1000, maps:get(tick_ms, Status)),
    ?assertEqual(0, maps:get(protected_pids, Status)),
    ?assertEqual([], maps:get(partitioned_nodes, Status)),
    ok.

%%====================================================================
%% Configuration Tests
%%====================================================================

test_delay_config() ->
    %% Get default config
    Config = flurm_chaos:get_delay_config(),
    ?assert(is_tuple(Config)),

    %% Set custom config via map
    ok = flurm_chaos:set_delay_config(#{
        min_delay_ms => 50,
        max_delay_ms => 200,
        target_modules => [test_module],
        exclude_modules => [kernel, stdlib]
    }),

    %% Verify via status
    Status = flurm_chaos:status(),
    DelayConfig = maps:get(delay_config, Status),
    ?assertEqual(50, maps:get(min_delay_ms, DelayConfig)),
    ?assertEqual(200, maps:get(max_delay_ms, DelayConfig)),
    ?assertEqual([test_module], maps:get(target_modules, DelayConfig)),
    ok.

test_kill_config() ->
    %% Get default config
    Config = flurm_chaos:get_kill_config(),
    ?assert(is_tuple(Config)),

    %% Set custom config
    ok = flurm_chaos:set_kill_config(#{
        max_kills_per_tick => 5,
        kill_signal => kill,
        respect_links => false
    }),

    %% Verify via status
    Status = flurm_chaos:status(),
    KillConfig = maps:get(kill_config, Status),
    ?assertEqual(5, maps:get(max_kills_per_tick, KillConfig)),
    ?assertEqual(kill, maps:get(kill_signal, KillConfig)),
    ?assertEqual(false, maps:get(respect_links, KillConfig)),
    ok.

test_partition_config() ->
    %% Get default config
    Config = flurm_chaos:get_partition_config(),
    ?assert(is_tuple(Config)),

    %% Set custom config
    ok = flurm_chaos:set_partition_config(#{
        duration_ms => 10000,
        auto_heal => false,
        block_mode => filter
    }),

    %% Verify via status
    Status = flurm_chaos:status(),
    PartitionConfig = maps:get(partition_config, Status),
    ?assertEqual(10000, maps:get(duration_ms, PartitionConfig)),
    ?assertEqual(false, maps:get(auto_heal, PartitionConfig)),
    ?assertEqual(filter, maps:get(block_mode, PartitionConfig)),
    ok.

test_gc_config() ->
    %% Get default config
    Config = flurm_chaos:get_gc_config(),
    ?assert(is_tuple(Config)),

    %% Set custom config
    ok = flurm_chaos:set_gc_config(#{
        max_processes_per_tick => 20,
        target_heap_size => 10000,
        aggressive => true
    }),

    %% Verify via status
    Status = flurm_chaos:status(),
    GcConfig = maps:get(gc_config, Status),
    ?assertEqual(20, maps:get(max_processes_per_tick, GcConfig)),
    ?assertEqual(10000, maps:get(target_heap_size, GcConfig)),
    ?assertEqual(true, maps:get(aggressive, GcConfig)),
    ok.

test_scheduler_config() ->
    %% Get default config
    Config = flurm_chaos:get_scheduler_config(),
    ?assert(is_tuple(Config)),

    %% Set custom config
    ok = flurm_chaos:set_scheduler_config(#{
        min_suspend_ms => 5,
        max_suspend_ms => 50,
        affect_dirty_schedulers => true
    }),

    %% Verify via status
    Status = flurm_chaos:status(),
    SchedConfig = maps:get(scheduler_config, Status),
    ?assertEqual(5, maps:get(min_suspend_ms, SchedConfig)),
    ?assertEqual(50, maps:get(max_suspend_ms, SchedConfig)),
    ?assertEqual(true, maps:get(affect_dirty_schedulers, SchedConfig)),
    ok.

%%====================================================================
%% Process Protection Tests
%%====================================================================

test_mark_process_protected() ->
    %% Create a test process
    TestPid = spawn(fun() -> receive stop -> ok end end),

    %% Initially no protected pids
    Status1 = flurm_chaos:status(),
    ?assertEqual(0, maps:get(protected_pids, Status1)),

    %% Mark protected
    ok = flurm_chaos:mark_process_protected(TestPid),
    Status2 = flurm_chaos:status(),
    ?assertEqual(1, maps:get(protected_pids, Status2)),

    %% Unmark protected
    ok = flurm_chaos:unmark_process_protected(TestPid),
    Status3 = flurm_chaos:status(),
    ?assertEqual(0, maps:get(protected_pids, Status3)),

    %% Clean up
    TestPid ! stop,
    ok.

%%====================================================================
%% Delay Injection Tests
%%====================================================================

test_wrap_call() ->
    %% Test wrap_call with the chaos server itself
    %% When chaos is disabled, it should just pass through

    %% First ensure chaos is disabled
    ok = flurm_chaos:disable(),

    %% wrap_call should work normally
    Result = flurm_chaos:wrap_call(flurm_chaos, flurm_chaos, status),
    ?assert(is_map(Result)),
    ok.

test_maybe_delay_call() ->
    %% When disabled, should not delay
    ok = flurm_chaos:disable(),

    %% Should return immediately
    ok = flurm_chaos:maybe_delay_call(),
    ok = flurm_chaos:maybe_delay_call(test_module),

    %% Enable chaos and delay_message scenario
    ok = flurm_chaos:enable(),
    ok = flurm_chaos:enable_scenario(delay_message),
    ok = flurm_chaos:set_scenario(delay_message, 1.0),  % 100% probability

    %% Set minimal delay for fast test
    ok = flurm_chaos:set_delay_config(#{
        min_delay_ms => 1,
        max_delay_ms => 2
    }),

    %% May or may not delay (depends on RNG), but shouldn't crash
    ok = flurm_chaos:maybe_delay_call(),
    ok = flurm_chaos:maybe_delay_call(test_module),
    ok.

%%====================================================================
%% Partition Tests
%%====================================================================

test_partition_operations() ->
    %% Initially no partitions
    ?assertEqual([], flurm_chaos:get_partitions()),

    %% Try to partition a non-connected node
    Result = flurm_chaos:partition_node('nonexistent@node'),
    ?assertEqual({error, node_not_connected}, Result),

    %% Check if partitioned
    ?assertNot(flurm_chaos:is_partitioned('nonexistent@node')),

    %% Heal non-partitioned node
    Result2 = flurm_chaos:heal_partition('nonexistent@node'),
    ?assertEqual({error, not_partitioned}, Result2),

    %% Heal all partitions (should work even if empty)
    ok = flurm_chaos:heal_all_partitions(),
    ?assertEqual([], flurm_chaos:get_partitions()),
    ok.

%%====================================================================
%% GC Tests
%%====================================================================

test_gc_operations() ->
    %% Force GC on all processes
    {ok, Count} = flurm_chaos:gc_all_processes(),
    ?assert(is_integer(Count)),
    ?assert(Count > 0),

    %% GC a specific process
    TestPid = spawn(fun() -> receive stop -> ok end end),
    Result = flurm_chaos:gc_process(TestPid),
    ?assertEqual(ok, Result),

    %% GC non-existent process
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(50),  % Let it die
    Result2 = flurm_chaos:gc_process(DeadPid),
    ?assertEqual({error, process_not_found}, Result2),

    %% Clean up
    TestPid ! stop,
    ok.

%%====================================================================
%% Scheduler Suspension Tests
%%====================================================================

test_scheduler_suspension() ->
    %% Suspend for short duration (should not block tests)
    ok = flurm_chaos:suspend_schedulers(1),

    %% Suspend for slightly longer
    ok = flurm_chaos:suspend_schedulers(10),
    ok.

%%====================================================================
%% Helper Function Tests
%%====================================================================

test_slow_disk_helpers() ->
    %% Initially not in slow disk mode (process dictionary is local to calling process)
    ?assertNot(flurm_chaos:should_delay_io()),

    %% maybe_delay_io should not delay when not in slow disk mode
    ok = flurm_chaos:maybe_delay_io(100),

    %% Manually set slow disk mode in current process dictionary
    %% (The inject_once spawns a process that sets it in THAT process's dictionary)
    put(chaos_slow_disk, true),

    %% Now should_delay_io returns true
    ?assert(flurm_chaos:should_delay_io()),

    %% maybe_delay_io should actually delay now
    Start = erlang:system_time(millisecond),
    ok = flurm_chaos:maybe_delay_io(10),
    End = erlang:system_time(millisecond),
    ?assert((End - Start) >= 1),  % At least 1ms delay

    %% Clean up
    erase(chaos_slow_disk),
    ?assertNot(flurm_chaos:should_delay_io()),
    ok.

%%====================================================================
%% Tick and Timer Tests
%%====================================================================

tick_test_() ->
    {"Tick fires when enabled",
     {setup,
      fun setup/0,
      fun cleanup/1,
      fun(_) -> fun test_tick_fires/0 end}}.

stats_test_() ->
    {"Stats accumulate on scenario execution",
     {setup,
      fun setup/0,
      fun cleanup/1,
      fun(_) -> fun test_stats_accumulate/0 end}}.

test_tick_fires() ->
    %% Enable chaos
    ok = flurm_chaos:enable(),

    %% Enable a safe scenario with high probability
    ok = flurm_chaos:enable_scenario(trigger_gc),
    ok = flurm_chaos:set_scenario(trigger_gc, 1.0),

    %% Wait for tick to fire (default 1000ms, but we'll wait less since
    %% the test might have already had some ticks)
    timer:sleep(1500),

    %% Check stats
    Status = flurm_chaos:status(),
    Stats = maps:get(stats, Status),

    %% trigger_gc should have been executed at least once
    ?assert(maps:get(trigger_gc, Stats, 0) >= 1),
    ok.

test_stats_accumulate() ->
    %% Manually inject scenarios multiple times (fresh server instance)
    ok = flurm_chaos:inject_once(trigger_gc),
    ok = flurm_chaos:inject_once(trigger_gc),
    ok = flurm_chaos:inject_once(gc_pressure),

    Status = flurm_chaos:status(),
    Stats = maps:get(stats, Status),

    %% These are exact counts since we have a fresh server
    ?assertEqual(2, maps:get(trigger_gc, Stats)),
    ?assertEqual(1, maps:get(gc_pressure, Stats)),
    ok.

%%====================================================================
%% Options Tests
%%====================================================================

options_test_() ->
    {"Start with custom options",
     {setup,
      fun() ->
          catch gen_server:stop(flurm_chaos, normal, 1000),
          timer:sleep(50),
          {ok, Pid} = flurm_chaos:start_link(#{
              tick_ms => 500,
              scenarios => #{kill_random_process => 0.0, trigger_gc => 0.5},
              scenario_enabled => #{trigger_gc => true},
              protected_apps => [myapp],
              delay_config => #{min_delay_ms => 100, max_delay_ms => 300},
              kill_config => #{max_kills_per_tick => 3},
              gc_config => #{aggressive => true}
          }),
          #{pid => Pid}
      end,
      fun cleanup/1,
      fun(_) ->
          [
           {"Custom tick_ms is used", fun() ->
               Status = flurm_chaos:status(),
               ?assertEqual(500, maps:get(tick_ms, Status))
           end},
           {"Custom scenario probability is used", fun() ->
               Scenarios = flurm_chaos:get_scenarios(),
               ?assertEqual(0.0, maps:get(kill_random_process, Scenarios)),
               ?assertEqual(0.5, maps:get(trigger_gc, Scenarios))
           end},
           {"Custom scenario_enabled is used", fun() ->
               ?assert(flurm_chaos:is_scenario_enabled(trigger_gc))
           end},
           {"Custom protected_apps is used", fun() ->
               Status = flurm_chaos:status(),
               ?assert(lists:member(myapp, maps:get(protected_apps, Status)))
           end},
           {"Custom delay_config is used", fun() ->
               Status = flurm_chaos:status(),
               DelayConfig = maps:get(delay_config, Status),
               ?assertEqual(100, maps:get(min_delay_ms, DelayConfig)),
               ?assertEqual(300, maps:get(max_delay_ms, DelayConfig))
           end},
           {"Custom kill_config is used", fun() ->
               Status = flurm_chaos:status(),
               KillConfig = maps:get(kill_config, Status),
               ?assertEqual(3, maps:get(max_kills_per_tick, KillConfig))
           end},
           {"Custom gc_config is used", fun() ->
               Status = flurm_chaos:status(),
               GcConfig = maps:get(gc_config, Status),
               ?assertEqual(true, maps:get(aggressive, GcConfig))
           end}
          ]
      end}}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Unknown info message handled", fun test_unknown_info/0},
      {"Unknown cast message handled", fun test_unknown_cast/0},
      {"Code change handled", fun test_code_change/0}
     ]}.

test_unknown_info() ->
    %% Send unknown info message
    flurm_chaos ! {unknown_info, some_data},
    timer:sleep(50),

    %% Server should still be running
    Status = flurm_chaos:status(),
    ?assert(is_map(Status)),
    ok.

test_unknown_cast() ->
    %% Send unknown cast message
    gen_server:cast(flurm_chaos, {unknown_cast, data}),
    timer:sleep(50),

    %% Server should still be running
    Status = flurm_chaos:status(),
    ?assert(is_map(Status)),
    ok.

test_code_change() ->
    %% Test that code_change callback exists and is properly defined
    %% by suspending the server, triggering code_change, and resuming

    %% Suspend the server
    ok = sys:suspend(flurm_chaos),

    %% Trigger code_change
    Result = sys:change_code(flurm_chaos, flurm_chaos, old_vsn, extra),
    ?assertEqual(ok, Result),

    %% Resume the server
    ok = sys:resume(flurm_chaos),

    %% Server should still be running
    Status = flurm_chaos:status(),
    ?assert(is_map(Status)),
    ok.

%%====================================================================
%% Kill Process Tests
%%====================================================================

kill_process_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          {"Kill random process scenario", fun test_kill_random_process/0}
         ]
     end}.

test_kill_random_process() ->
    %% Create several test processes
    TestPids = [spawn(fun loop/0) || _ <- lists:seq(1, 10)],

    %% Inject kill_random_process scenario
    %% This may or may not kill a process (depends on finding killable ones)
    Result = flurm_chaos:inject_once(kill_random_process),
    %% Result can be ok or {error, no_killable_process} or {ok, Count}
    ?assert(Result =:= ok orelse
            Result =:= {error, no_killable_process} orelse
            (is_tuple(Result) andalso element(1, Result) =:= ok)),

    %% Clean up remaining test processes
    lists:foreach(fun(Pid) ->
        catch exit(Pid, kill)
    end, TestPids),
    ok.

%% Helper for test processes
loop() ->
    receive
        stop -> ok
    after 60000 -> loop()
    end.

%%====================================================================
%% Network Partition Scenario Tests
%%====================================================================

network_partition_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          {"Network partition scenario with no nodes", fun test_network_partition_no_nodes/0}
         ]
     end}.

test_network_partition_no_nodes() ->
    %% When there are no connected nodes, partition scenario fails
    Result = flurm_chaos:inject_once(network_partition),
    ?assertEqual({error, no_nodes_available}, Result),
    ok.
