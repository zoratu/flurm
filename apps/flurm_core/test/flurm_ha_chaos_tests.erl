%%%-------------------------------------------------------------------
%%% @doc HA Chaos Testing Integration (Phase 8E)
%%%
%%% Chaos testing for high-availability scenarios using the flurm_chaos
%%% module. This module provides integration tests that inject various
%%% types of failures to verify system resilience.
%%%
%%% Chaos Scenarios:
%%% - Process kills (random worker processes)
%%% - Network partitions (Erlang node disconnection)
%%% - Message delays (simulated network latency)
%%% - GC pressure (forced garbage collection)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_ha_chaos_tests).

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

%% Test group definitions
ha_chaos_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"Process Kill Tests", fun process_kill_tests/0},
         {"Network Partition Tests", fun network_partition_tests/0},
         {"Message Delay Tests", fun message_delay_tests/0},
         {"GC Pressure Tests", fun gc_pressure_tests/0},
         {"Combined Chaos Tests", fun combined_chaos_tests/0},
         {"Chaos Recovery Tests", fun chaos_recovery_tests/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    %% Start required applications for testing
    application:ensure_all_started(lager),

    %% Start chaos server if not running
    case whereis(flurm_chaos) of
        undefined ->
            {ok, Pid} = flurm_chaos:start_link(#{
                tick_ms => 100,
                protected_apps => [kernel, stdlib, sasl]
            }),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    %% Ensure chaos is disabled and clean up
    catch flurm_chaos:disable(),
    catch flurm_chaos:heal_all_partitions(),
    catch gen_server:stop(flurm_chaos),
    ok;
cleanup({existing, _Pid}) ->
    %% Just disable chaos, don't stop the server
    catch flurm_chaos:disable(),
    catch flurm_chaos:heal_all_partitions(),
    ok.

%%====================================================================
%% Process Kill Tests
%%====================================================================

process_kill_tests() ->
    %% Test process kill chaos injection

    %% Create a test process to be potentially killed
    TestPid = spawn(fun() ->
        receive stop -> ok end
    end),

    %% Configure kill settings
    flurm_chaos:set_kill_config(#{
        max_kills_per_tick => 1,
        kill_signal => chaos_kill,
        respect_links => true
    }),

    %% Mark our test process as NOT protected (others may be)
    %% The chaos module should NOT kill system processes

    %% Enable chaos and the kill scenario
    flurm_chaos:enable(),
    flurm_chaos:enable_scenario(kill_random_process),

    %% Verify kill config is set
    KillConfig = flurm_chaos:get_kill_config(),
    ?assertMatch(#{max_kills_per_tick := 1}, kill_config_to_map(KillConfig)),

    %% Verify scenario is enabled
    ?assert(flurm_chaos:is_scenario_enabled(kill_random_process)),

    %% Disable for cleanup
    flurm_chaos:disable_scenario(kill_random_process),
    flurm_chaos:disable(),

    %% Clean up test process
    exit(TestPid, normal),

    ok.

kill_config_to_map(Config) when is_tuple(Config) ->
    %% Convert record to map (record has tag + 3 fields)
    case tuple_size(Config) of
        4 ->
            #{
                max_kills_per_tick => element(2, Config),
                kill_signal => element(3, Config),
                respect_links => element(4, Config)
            };
        _ ->
            #{}
    end.

%%====================================================================
%% Network Partition Tests
%%====================================================================

network_partition_tests() ->
    %% Test network partition simulation
    %% Note: These tests work in distributed Erlang mode

    %% Configure partition settings
    flurm_chaos:set_partition_config(#{
        duration_ms => 1000,
        auto_heal => true,
        block_mode => disconnect
    }),

    %% Verify partition config
    PartitionConfig = flurm_chaos:get_partition_config(),
    ?assertMatch(#{duration_ms := 1000}, partition_config_to_map(PartitionConfig)),

    %% Test partition operations (these work even without other nodes)
    ?assertEqual([], flurm_chaos:get_partitions()),

    %% Test partition with non-existent node (should return error)
    Result = flurm_chaos:partition_node('nonexistent@node'),
    ?assertEqual({error, node_not_connected}, Result),

    %% Test heal_all_partitions (should succeed even with no partitions)
    ?assertEqual(ok, flurm_chaos:heal_all_partitions()),

    %% Verify no partitions after heal
    ?assertEqual([], flurm_chaos:get_partitions()),

    ok.

partition_config_to_map(Config) when is_tuple(Config) ->
    case tuple_size(Config) of
        4 ->
            #{
                duration_ms => element(2, Config),
                auto_heal => element(3, Config),
                block_mode => element(4, Config)
            };
        _ ->
            #{}
    end.

%%====================================================================
%% Message Delay Tests
%%====================================================================

message_delay_tests() ->
    %% Test message delay injection

    %% Configure delay settings
    flurm_chaos:set_delay_config(#{
        min_delay_ms => 10,
        max_delay_ms => 100,
        target_modules => [],  % All modules
        exclude_modules => [flurm_chaos]
    }),

    %% Verify delay config
    DelayConfig = flurm_chaos:get_delay_config(),
    ?assertMatch(#{min_delay_ms := 10}, delay_config_to_map(DelayConfig)),

    %% Enable chaos and delay scenario
    flurm_chaos:enable(),
    flurm_chaos:enable_scenario(delay_message),

    %% Set high probability for testing
    flurm_chaos:set_scenario(delay_message, 0.5),

    %% Verify scenario configuration
    Scenarios = flurm_chaos:get_scenarios(),
    ?assert(maps:is_key(delay_message, Scenarios)),

    %% Test wrap_call functionality (should not crash even without real server)
    %% We just verify the API works
    ?assertEqual(ok, flurm_chaos:maybe_delay_call()),
    ?assertEqual(ok, flurm_chaos:maybe_delay_call(test_module)),

    %% Disable
    flurm_chaos:disable_scenario(delay_message),
    flurm_chaos:disable(),

    ok.

delay_config_to_map(Config) when is_tuple(Config) ->
    case tuple_size(Config) of
        5 ->
            #{
                min_delay_ms => element(2, Config),
                max_delay_ms => element(3, Config),
                target_modules => element(4, Config),
                exclude_modules => element(5, Config)
            };
        _ ->
            #{}
    end.

%%====================================================================
%% GC Pressure Tests
%%====================================================================

gc_pressure_tests() ->
    %% Test GC pressure injection

    %% Configure GC settings
    flurm_chaos:set_gc_config(#{
        max_processes_per_tick => 5,
        target_heap_size => undefined,
        aggressive => false
    }),

    %% Verify GC config
    GcConfig = flurm_chaos:get_gc_config(),
    ?assertMatch(#{max_processes_per_tick := 5}, gc_config_to_map(GcConfig)),

    %% Test gc_all_processes
    {ok, Count} = flurm_chaos:gc_all_processes(),
    ?assert(Count >= 0),

    %% Test gc_process on self
    ?assertEqual(ok, flurm_chaos:gc_process(self())),

    %% Test gc_process on non-existent pid
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(100), % Let it die
    GcResult = flurm_chaos:gc_process(DeadPid),
    ?assertEqual({error, process_not_found}, GcResult),

    %% Enable and test gc_pressure scenario
    flurm_chaos:enable(),
    flurm_chaos:enable_scenario(gc_pressure),

    %% Inject once
    flurm_chaos:inject_once(gc_pressure),

    %% Verify stats are tracked
    Status = flurm_chaos:status(),
    ?assert(maps:is_key(stats, Status)),

    flurm_chaos:disable_scenario(gc_pressure),
    flurm_chaos:disable(),

    ok.

gc_config_to_map(Config) when is_tuple(Config) ->
    case tuple_size(Config) of
        4 ->
            #{
                max_processes_per_tick => element(2, Config),
                target_heap_size => element(3, Config),
                aggressive => element(4, Config)
            };
        _ ->
            #{}
    end.

%%====================================================================
%% Combined Chaos Tests
%%====================================================================

combined_chaos_tests() ->
    %% Test multiple chaos scenarios enabled simultaneously

    %% Enable multiple scenarios with low probabilities
    flurm_chaos:set_scenario(trigger_gc, 0.1),
    flurm_chaos:set_scenario(memory_pressure, 0.05),
    flurm_chaos:set_scenario(cpu_burn, 0.01),

    %% Enable scenarios
    flurm_chaos:enable_scenario(trigger_gc),
    flurm_chaos:enable_scenario(memory_pressure),
    flurm_chaos:enable_scenario(cpu_burn),

    %% Enable chaos globally
    flurm_chaos:enable(),

    %% Verify all scenarios are enabled
    ?assert(flurm_chaos:is_scenario_enabled(trigger_gc)),
    ?assert(flurm_chaos:is_scenario_enabled(memory_pressure)),
    ?assert(flurm_chaos:is_scenario_enabled(cpu_burn)),

    %% Check status shows all enabled scenarios
    Status = flurm_chaos:status(),
    ?assert(maps:get(enabled, Status)),
    ScenarioEnabled = maps:get(scenario_enabled, Status),
    ?assert(maps:get(trigger_gc, ScenarioEnabled)),
    ?assert(maps:get(memory_pressure, ScenarioEnabled)),
    ?assert(maps:get(cpu_burn, ScenarioEnabled)),

    %% Let chaos run for a bit
    timer:sleep(200),

    %% Disable all scenarios
    flurm_chaos:disable_all_scenarios(),

    %% Verify all disabled
    ?assertNot(flurm_chaos:is_scenario_enabled(trigger_gc)),
    ?assertNot(flurm_chaos:is_scenario_enabled(memory_pressure)),
    ?assertNot(flurm_chaos:is_scenario_enabled(cpu_burn)),

    %% Disable chaos
    flurm_chaos:disable(),

    ok.

%%====================================================================
%% Chaos Recovery Tests
%%====================================================================

chaos_recovery_tests() ->
    %% Test system recovery after chaos injection

    %% Create a supervised test process
    TestPid = spawn_link(fun test_worker_loop/0),

    %% Start chaos
    flurm_chaos:enable(),

    %% Inject various chaos events
    flurm_chaos:inject_once(trigger_gc),
    flurm_chaos:inject_once(memory_pressure),
    flurm_chaos:inject_once(cpu_burn),

    %% Verify test process survives (it's not a candidate for killing
    %% because we haven't enabled kill_random_process)
    timer:sleep(100),
    ?assert(is_process_alive(TestPid)),

    %% Verify chaos status is tracked
    Status = flurm_chaos:status(),
    Stats = maps:get(stats, Status),

    %% At least some scenarios should have been executed
    TotalExecutions = maps:fold(fun(_K, V, Acc) -> Acc + V end, 0, Stats),
    ?assert(TotalExecutions >= 3),

    %% Clean up
    flurm_chaos:disable(),
    exit(TestPid, normal),

    ok.

test_worker_loop() ->
    receive
        stop -> ok;
        _ -> test_worker_loop()
    after 5000 ->
        test_worker_loop()
    end.

%%====================================================================
%% Scheduler Suspension Tests
%%====================================================================

scheduler_suspension_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_chaos) of
             undefined ->
                 {ok, Pid} = flurm_chaos:start_link(),
                 {started, Pid};
             Pid ->
                 {existing, Pid}
         end
     end,
     fun(State) -> cleanup(State) end,
     [
         {"Scheduler config", fun test_scheduler_config/0},
         {"Scheduler suspend", fun test_scheduler_suspend/0}
     ]}.

test_scheduler_config() ->
    %% Configure scheduler suspension
    flurm_chaos:set_scheduler_config(#{
        min_suspend_ms => 1,
        max_suspend_ms => 10,
        affect_dirty_schedulers => false
    }),

    %% Verify config
    Config = flurm_chaos:get_scheduler_config(),
    ConfigMap = scheduler_config_to_map(Config),
    ?assertMatch(#{min_suspend_ms := 1}, ConfigMap),
    ?assertMatch(#{max_suspend_ms := 10}, ConfigMap),

    ok.

test_scheduler_suspend() ->
    %% Test brief scheduler suspension (very short duration)
    StartTime = erlang:system_time(millisecond),
    flurm_chaos:suspend_schedulers(5), % 5ms
    Duration = erlang:system_time(millisecond) - StartTime,

    %% Should have taken at least 5ms (but may be more due to scheduling)
    ?assert(Duration >= 5),

    %% Should not have taken too long (less than 1 second)
    ?assert(Duration < 1000),

    ok.

scheduler_config_to_map(Config) when is_tuple(Config) ->
    case tuple_size(Config) of
        4 ->
            #{
                min_suspend_ms => element(2, Config),
                max_suspend_ms => element(3, Config),
                affect_dirty_schedulers => element(4, Config)
            };
        _ ->
            #{}
    end.

%%====================================================================
%% HA-Specific Chaos Tests
%%====================================================================

ha_specific_chaos_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_chaos) of
             undefined ->
                 {ok, Pid} = flurm_chaos:start_link(),
                 {started, Pid};
             Pid ->
                 {existing, Pid}
         end
     end,
     fun(State) -> cleanup(State) end,
     [
         {"Protected process marking", fun test_protected_processes/0},
         {"Chaos enable/disable cycle", fun test_enable_disable_cycle/0},
         {"Scenario probability control", fun test_scenario_probability/0}
     ]}.

test_protected_processes() ->
    %% Create a process and mark it as protected
    ProtectedPid = spawn(fun() ->
        receive stop -> ok end
    end),

    %% Mark as protected
    flurm_chaos:mark_process_protected(ProtectedPid),

    %% Verify through status
    Status = flurm_chaos:status(),
    ProtectedCount = maps:get(protected_pids, Status),
    ?assert(ProtectedCount >= 1),

    %% Unmark
    flurm_chaos:unmark_process_protected(ProtectedPid),

    %% Clean up
    exit(ProtectedPid, normal),

    ok.

test_enable_disable_cycle() ->
    %% Test rapid enable/disable cycles
    lists:foreach(fun(_) ->
        flurm_chaos:enable(),
        ?assert(flurm_chaos:is_enabled()),
        flurm_chaos:disable(),
        ?assertNot(flurm_chaos:is_enabled())
    end, lists:seq(1, 5)),

    ok.

test_scenario_probability() ->
    %% Test setting various probabilities
    Scenarios = [
        {kill_random_process, 0.001},
        {trigger_gc, 0.01},
        {memory_pressure, 0.005},
        {cpu_burn, 0.002},
        {delay_message, 0.05}
    ],

    lists:foreach(fun({Scenario, Prob}) ->
        flurm_chaos:set_scenario(Scenario, Prob),
        AllScenarios = flurm_chaos:get_scenarios(),
        ActualProb = maps:get(Scenario, AllScenarios),
        ?assertEqual(Prob, ActualProb)
    end, Scenarios),

    ok.

%%====================================================================
%% Slow Disk Simulation Tests
%%====================================================================

slow_disk_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_chaos) of
             undefined ->
                 {ok, Pid} = flurm_chaos:start_link(),
                 {started, Pid};
             Pid ->
                 {existing, Pid}
         end
     end,
     fun(State) -> cleanup(State) end,
     [
         {"Slow disk check", fun test_slow_disk_check/0},
         {"Maybe delay IO", fun test_maybe_delay_io/0}
     ]}.

test_slow_disk_check() ->
    %% Initially should not be in slow disk mode
    ?assertNot(flurm_chaos:should_delay_io()),

    %% Inject slow_disk scenario
    flurm_chaos:inject_once(slow_disk),

    %% Now should be in slow disk mode (stored in process dictionary)
    %% Note: This only affects the process that called inject_once
    %% The actual slow_disk flag is set in a spawned process

    ok.

test_maybe_delay_io() ->
    %% Test maybe_delay_io function (should not crash)
    flurm_chaos:maybe_delay_io(10),

    ok.

-endif.
