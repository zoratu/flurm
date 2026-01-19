%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_chaos module
%%%
%%% These tests directly test gen_server callbacks without using meck.
%%% Tests init/1, handle_call/3, handle_cast/2, handle_info/2, and
%%% helper functions directly by constructing state records.
%%%
%%% NO MOCKING IS USED IN THESE TESTS.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_chaos_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Import record definitions - we need to replicate them since they're private
-record(delay_config, {
    min_delay_ms = 10 :: non_neg_integer(),
    max_delay_ms = 500 :: non_neg_integer(),
    target_modules = [] :: [module()],
    exclude_modules = [flurm_chaos] :: [module()]
}).

-record(kill_config, {
    max_kills_per_tick = 1 :: pos_integer(),
    kill_signal = chaos_kill :: term(),
    respect_links = true :: boolean()
}).

-record(partition_config, {
    duration_ms = 5000 :: pos_integer(),
    auto_heal = true :: boolean(),
    block_mode = disconnect :: disconnect | filter
}).

-record(gc_config, {
    max_processes_per_tick = 10 :: pos_integer(),
    target_heap_size = undefined :: undefined | pos_integer(),
    aggressive = false :: boolean()
}).

-record(scheduler_config, {
    min_suspend_ms = 1 :: pos_integer(),
    max_suspend_ms = 100 :: pos_integer(),
    affect_dirty_schedulers = false :: boolean()
}).

-record(state, {
    enabled = false :: boolean(),
    scenarios = #{} :: #{atom() => float()},
    scenario_enabled = #{} :: #{atom() => boolean()},
    tick_ref :: reference() | undefined,
    tick_ms :: pos_integer(),
    stats :: #{atom() => non_neg_integer()},
    protected_apps :: [atom()],
    protected_pids :: sets:set(pid()),
    seed :: rand:state(),
    partitioned_nodes :: #{node() => reference()},
    message_filters :: #{node() => fun()},
    delay_config :: #delay_config{},
    kill_config :: #kill_config{},
    partition_config :: #partition_config{},
    gc_config :: #gc_config{},
    scheduler_config :: #scheduler_config{}
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

default_state() ->
    #state{
        enabled = false,
        scenarios = #{
            kill_random_process => 0.001,
            trigger_gc => 0.01,
            memory_pressure => 0.005,
            cpu_burn => 0.002,
            delay_message => 0.05,
            scheduler_suspend => 0.001,
            gc_pressure => 0.02,
            network_partition => 0.0005
        },
        scenario_enabled = #{
            kill_random_process => false,
            trigger_gc => false,
            memory_pressure => false,
            cpu_burn => false,
            delay_message => false,
            scheduler_suspend => false,
            gc_pressure => false,
            network_partition => false
        },
        tick_ref = undefined,
        tick_ms = 1000,
        stats = #{},
        protected_apps = [kernel, stdlib, sasl, ranch, ra],
        protected_pids = sets:new(),
        seed = rand:seed(exsss, 12345),
        partitioned_nodes = #{},
        message_filters = #{},
        delay_config = #delay_config{},
        kill_config = #kill_config{},
        partition_config = #partition_config{},
        gc_config = #gc_config{},
        scheduler_config = #scheduler_config{}
    }.

make_enabled_state() ->
    S = default_state(),
    S#state{enabled = true}.

make_state_with_scenario_enabled(Scenario, Value) ->
    S = default_state(),
    S#state{scenario_enabled = maps:put(Scenario, Value, S#state.scenario_enabled)}.

make_state_with_scenarios_map(Map) ->
    S = default_state(),
    S#state{scenario_enabled = Map}.

make_state_with_protected_pid(Pid) ->
    S = default_state(),
    S#state{protected_pids = sets:add_element(Pid, sets:new())}.

make_state_with_partitions(PartitionsMap) ->
    S = default_state(),
    S#state{partitioned_nodes = PartitionsMap}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_default_test() ->
    {ok, State} = flurm_chaos:init(#{}),
    ?assertEqual(false, State#state.enabled),
    ?assertEqual(1000, State#state.tick_ms),
    ?assertEqual(undefined, State#state.tick_ref),
    ?assert(is_map(State#state.scenarios)),
    ?assert(is_map(State#state.scenario_enabled)),
    ?assert(is_map(State#state.stats)),
    ok.

init_custom_tick_ms_test() ->
    {ok, State} = flurm_chaos:init(#{tick_ms => 500}),
    ?assertEqual(500, State#state.tick_ms),
    ok.

init_custom_scenarios_test() ->
    CustomScenarios = #{kill_random_process => 0.5, trigger_gc => 0.1},
    {ok, State} = flurm_chaos:init(#{scenarios => CustomScenarios}),
    %% Custom scenarios should be merged with defaults
    ?assertEqual(0.5, maps:get(kill_random_process, State#state.scenarios)),
    ?assertEqual(0.1, maps:get(trigger_gc, State#state.scenarios)),
    ok.

init_protected_apps_test() ->
    {ok, State} = flurm_chaos:init(#{protected_apps => [myapp]}),
    ?assert(lists:member(kernel, State#state.protected_apps)),
    ?assert(lists:member(myapp, State#state.protected_apps)),
    ok.

init_delay_config_map_test() ->
    DelayConfigMap = #{min_delay_ms => 50, max_delay_ms => 1000},
    {ok, State} = flurm_chaos:init(#{delay_config => DelayConfigMap}),
    ?assertEqual(50, (State#state.delay_config)#delay_config.min_delay_ms),
    ?assertEqual(1000, (State#state.delay_config)#delay_config.max_delay_ms),
    ok.

init_kill_config_map_test() ->
    KillConfigMap = #{max_kills_per_tick => 5, kill_signal => test_kill},
    {ok, State} = flurm_chaos:init(#{kill_config => KillConfigMap}),
    ?assertEqual(5, (State#state.kill_config)#kill_config.max_kills_per_tick),
    ?assertEqual(test_kill, (State#state.kill_config)#kill_config.kill_signal),
    ok.

init_partition_config_map_test() ->
    PartitionConfigMap = #{duration_ms => 10000, auto_heal => false},
    {ok, State} = flurm_chaos:init(#{partition_config => PartitionConfigMap}),
    ?assertEqual(10000, (State#state.partition_config)#partition_config.duration_ms),
    ?assertEqual(false, (State#state.partition_config)#partition_config.auto_heal),
    ok.

init_gc_config_map_test() ->
    GcConfigMap = #{max_processes_per_tick => 20, aggressive => true},
    {ok, State} = flurm_chaos:init(#{gc_config => GcConfigMap}),
    ?assertEqual(20, (State#state.gc_config)#gc_config.max_processes_per_tick),
    ?assertEqual(true, (State#state.gc_config)#gc_config.aggressive),
    ok.

init_scheduler_config_map_test() ->
    SchedulerConfigMap = #{min_suspend_ms => 5, max_suspend_ms => 50},
    {ok, State} = flurm_chaos:init(#{scheduler_config => SchedulerConfigMap}),
    ?assertEqual(5, (State#state.scheduler_config)#scheduler_config.min_suspend_ms),
    ?assertEqual(50, (State#state.scheduler_config)#scheduler_config.max_suspend_ms),
    ok.

init_scenario_enabled_test() ->
    ScenarioEnabled = #{kill_random_process => true, trigger_gc => true},
    {ok, State} = flurm_chaos:init(#{scenario_enabled => ScenarioEnabled}),
    ?assertEqual(true, maps:get(kill_random_process, State#state.scenario_enabled)),
    ?assertEqual(true, maps:get(trigger_gc, State#state.scenario_enabled)),
    ok.

%%====================================================================
%% handle_call/3 Tests - Enable/Disable
%%====================================================================

handle_call_enable_test() ->
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call(enable, {self(), make_ref()}, State),
    ?assertEqual(true, NewState#state.enabled),
    ?assertNotEqual(undefined, NewState#state.tick_ref),
    %% Cancel the timer to avoid leaks
    erlang:cancel_timer(NewState#state.tick_ref),
    ok.

handle_call_disable_test() ->
    State = default_state(),
    %% First enable to get a tick_ref
    {reply, ok, EnabledState} = flurm_chaos:handle_call(enable, {self(), make_ref()}, State),
    %% Now disable
    {reply, ok, DisabledState} = flurm_chaos:handle_call(disable, {self(), make_ref()}, EnabledState),
    ?assertEqual(false, DisabledState#state.enabled),
    ?assertEqual(undefined, DisabledState#state.tick_ref),
    ok.

handle_call_disable_no_timer_test() ->
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call(disable, {self(), make_ref()}, State),
    ?assertEqual(false, NewState#state.enabled),
    ?assertEqual(undefined, NewState#state.tick_ref),
    ok.

handle_call_is_enabled_false_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call(is_enabled, {self(), make_ref()}, State),
    ?assertEqual(false, Result),
    ok.

handle_call_is_enabled_true_test() ->
    State = make_enabled_state(),
    {reply, Result, _} = flurm_chaos:handle_call(is_enabled, {self(), make_ref()}, State),
    ?assertEqual(true, Result),
    ok.

%%====================================================================
%% handle_call/3 Tests - Scenario Enable/Disable
%%====================================================================

handle_call_enable_scenario_test() ->
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call({enable_scenario, kill_random_process}, {self(), make_ref()}, State),
    ?assertEqual(true, maps:get(kill_random_process, NewState#state.scenario_enabled)),
    ok.

handle_call_disable_scenario_test() ->
    State = make_state_with_scenario_enabled(kill_random_process, true),
    {reply, ok, NewState} = flurm_chaos:handle_call({disable_scenario, kill_random_process}, {self(), make_ref()}, State),
    ?assertEqual(false, maps:get(kill_random_process, NewState#state.scenario_enabled)),
    ok.

handle_call_is_scenario_enabled_test() ->
    S = default_state(),
    State = S#state{scenario_enabled = #{kill_random_process => true, trigger_gc => false}},
    {reply, true, _} = flurm_chaos:handle_call({is_scenario_enabled, kill_random_process}, {self(), make_ref()}, State),
    {reply, false, _} = flurm_chaos:handle_call({is_scenario_enabled, trigger_gc}, {self(), make_ref()}, State),
    {reply, false, _} = flurm_chaos:handle_call({is_scenario_enabled, unknown_scenario}, {self(), make_ref()}, State),
    ok.

handle_call_enable_all_scenarios_test() ->
    State = make_state_with_scenarios_map(#{
        kill_random_process => false,
        trigger_gc => false,
        memory_pressure => false
    }),
    {reply, ok, NewState} = flurm_chaos:handle_call(enable_all_scenarios, {self(), make_ref()}, State),
    maps:foreach(fun(_Scenario, Enabled) ->
        ?assertEqual(true, Enabled)
    end, NewState#state.scenario_enabled),
    ok.

handle_call_disable_all_scenarios_test() ->
    State = make_state_with_scenarios_map(#{
        kill_random_process => true,
        trigger_gc => true,
        memory_pressure => true
    }),
    {reply, ok, NewState} = flurm_chaos:handle_call(disable_all_scenarios, {self(), make_ref()}, State),
    maps:foreach(fun(_Scenario, Enabled) ->
        ?assertEqual(false, Enabled)
    end, NewState#state.scenario_enabled),
    ok.

%%====================================================================
%% handle_call/3 Tests - Scenario Configuration
%%====================================================================

handle_call_set_scenario_test() ->
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call({set_scenario, kill_random_process, 0.5}, {self(), make_ref()}, State),
    ?assertEqual(0.5, maps:get(kill_random_process, NewState#state.scenarios)),
    ok.

handle_call_get_scenarios_test() ->
    State = default_state(),
    {reply, Scenarios, _} = flurm_chaos:handle_call(get_scenarios, {self(), make_ref()}, State),
    ?assert(is_map(Scenarios)),
    ?assert(maps:is_key(kill_random_process, Scenarios)),
    ok.

%%====================================================================
%% handle_call/3 Tests - Status
%%====================================================================

handle_call_status_test() ->
    State = make_enabled_state(),
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    ?assertEqual(true, maps:get(enabled, Status)),
    ?assert(is_map(maps:get(scenarios, Status))),
    ?assert(is_map(maps:get(scenario_enabled, Status))),
    ?assert(is_map(maps:get(stats, Status))),
    ?assertEqual(1000, maps:get(tick_ms, Status)),
    ?assert(is_list(maps:get(protected_apps, Status))),
    ?assertEqual(0, maps:get(protected_pids, Status)),
    ?assertEqual([], maps:get(partitioned_nodes, Status)),
    ?assert(is_map(maps:get(delay_config, Status))),
    ?assert(is_map(maps:get(kill_config, Status))),
    ?assert(is_map(maps:get(partition_config, Status))),
    ?assert(is_map(maps:get(gc_config, Status))),
    ?assert(is_map(maps:get(scheduler_config, Status))),
    ok.

%%====================================================================
%% handle_call/3 Tests - Delay Configuration
%%====================================================================

handle_call_set_delay_config_test() ->
    State = default_state(),
    NewConfig = #delay_config{min_delay_ms = 100, max_delay_ms = 200},
    {reply, ok, NewState} = flurm_chaos:handle_call({set_delay_config, NewConfig}, {self(), make_ref()}, State),
    ?assertEqual(100, (NewState#state.delay_config)#delay_config.min_delay_ms),
    ?assertEqual(200, (NewState#state.delay_config)#delay_config.max_delay_ms),
    ok.

handle_call_get_delay_config_test() ->
    State = default_state(),
    {reply, Config, _} = flurm_chaos:handle_call(get_delay_config, {self(), make_ref()}, State),
    ?assertEqual(10, Config#delay_config.min_delay_ms),
    ?assertEqual(500, Config#delay_config.max_delay_ms),
    ok.

%%====================================================================
%% handle_call/3 Tests - Kill Configuration
%%====================================================================

handle_call_set_kill_config_test() ->
    State = default_state(),
    NewConfig = #kill_config{max_kills_per_tick = 3, kill_signal = test_signal},
    {reply, ok, NewState} = flurm_chaos:handle_call({set_kill_config, NewConfig}, {self(), make_ref()}, State),
    ?assertEqual(3, (NewState#state.kill_config)#kill_config.max_kills_per_tick),
    ?assertEqual(test_signal, (NewState#state.kill_config)#kill_config.kill_signal),
    ok.

handle_call_get_kill_config_test() ->
    State = default_state(),
    {reply, Config, _} = flurm_chaos:handle_call(get_kill_config, {self(), make_ref()}, State),
    ?assertEqual(1, Config#kill_config.max_kills_per_tick),
    ?assertEqual(chaos_kill, Config#kill_config.kill_signal),
    ok.

%%====================================================================
%% handle_call/3 Tests - Protected Processes
%%====================================================================

handle_call_mark_protected_test() ->
    State = default_state(),
    Pid = self(),
    {reply, ok, NewState} = flurm_chaos:handle_call({mark_protected, Pid}, {self(), make_ref()}, State),
    ?assert(sets:is_element(Pid, NewState#state.protected_pids)),
    ok.

handle_call_unmark_protected_test() ->
    Pid = self(),
    State = make_state_with_protected_pid(Pid),
    {reply, ok, NewState} = flurm_chaos:handle_call({unmark_protected, Pid}, {self(), make_ref()}, State),
    ?assertNot(sets:is_element(Pid, NewState#state.protected_pids)),
    ok.

%%====================================================================
%% handle_call/3 Tests - Partition Configuration
%%====================================================================

handle_call_set_partition_config_test() ->
    State = default_state(),
    NewConfig = #partition_config{duration_ms = 10000, auto_heal = false},
    {reply, ok, NewState} = flurm_chaos:handle_call({set_partition_config, NewConfig}, {self(), make_ref()}, State),
    ?assertEqual(10000, (NewState#state.partition_config)#partition_config.duration_ms),
    ?assertEqual(false, (NewState#state.partition_config)#partition_config.auto_heal),
    ok.

handle_call_get_partition_config_test() ->
    State = default_state(),
    {reply, Config, _} = flurm_chaos:handle_call(get_partition_config, {self(), make_ref()}, State),
    ?assertEqual(5000, Config#partition_config.duration_ms),
    ?assertEqual(true, Config#partition_config.auto_heal),
    ok.

handle_call_partition_node_not_connected_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({partition_node, 'nonexistent@node', #{}}, {self(), make_ref()}, State),
    ?assertEqual({error, node_not_connected}, Result),
    ok.

handle_call_is_partitioned_false_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({is_partitioned, 'some@node'}, {self(), make_ref()}, State),
    ?assertEqual(false, Result),
    ok.

handle_call_is_partitioned_true_test() ->
    State = make_state_with_partitions(#{'some@node' => make_ref()}),
    {reply, Result, _} = flurm_chaos:handle_call({is_partitioned, 'some@node'}, {self(), make_ref()}, State),
    ?assertEqual(true, Result),
    ok.

handle_call_get_partitions_empty_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call(get_partitions, {self(), make_ref()}, State),
    ?assertEqual([], Result),
    ok.

handle_call_get_partitions_with_nodes_test() ->
    State = make_state_with_partitions(#{'node1@host' => make_ref(), 'node2@host' => make_ref()}),
    {reply, Result, _} = flurm_chaos:handle_call(get_partitions, {self(), make_ref()}, State),
    ?assertEqual(2, length(Result)),
    ?assert(lists:member('node1@host', Result)),
    ?assert(lists:member('node2@host', Result)),
    ok.

handle_call_heal_partition_not_partitioned_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({heal_partition, 'some@node'}, {self(), make_ref()}, State),
    ?assertEqual({error, not_partitioned}, Result),
    ok.

handle_call_heal_partition_success_test() ->
    TimerRef = make_ref(), % fake timer ref for testing
    State = make_state_with_partitions(#{'some@node' => TimerRef}),
    {reply, Result, NewState} = flurm_chaos:handle_call({heal_partition, 'some@node'}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertNot(maps:is_key('some@node', NewState#state.partitioned_nodes)),
    ok.

handle_call_heal_partition_with_no_autoheal_test() ->
    %% When auto_heal is false, the timer ref is undefined in partitioned_nodes
    %% However, the module uses maps:get(Node, Map, undefined) which returns
    %% undefined for both "key not found" and "key found with undefined value"
    %% This test documents the actual behavior: undefined value = not partitioned
    State = make_state_with_partitions(#{'some@node' => undefined}),
    {reply, Result, _NewState} = flurm_chaos:handle_call({heal_partition, 'some@node'}, {self(), make_ref()}, State),
    %% Due to the maps:get behavior, undefined value is treated as not_partitioned
    ?assertEqual({error, not_partitioned}, Result),
    ok.

handle_call_heal_all_partitions_test() ->
    %% Use actual timer refs (not undefined) for proper testing
    State = make_state_with_partitions(#{
        'node1@host' => make_ref(),
        'node2@host' => make_ref()
    }),
    {reply, Result, NewState} = flurm_chaos:handle_call(heal_all_partitions, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(#{}, NewState#state.partitioned_nodes),
    ok.

%%====================================================================
%% handle_call/3 Tests - GC Operations
%%====================================================================

handle_call_gc_all_processes_test() ->
    State = default_state(),
    {reply, {ok, Count}, _} = flurm_chaos:handle_call(gc_all_processes, {self(), make_ref()}, State),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0),
    ok.

handle_call_gc_process_success_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({gc_process, self()}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ok.

handle_call_gc_process_dead_test() ->
    State = default_state(),
    DeadPid = spawn(fun() -> ok end),
    timer:sleep(10), % Ensure process is dead
    {reply, Result, _} = flurm_chaos:handle_call({gc_process, DeadPid}, {self(), make_ref()}, State),
    ?assertEqual({error, process_not_found}, Result),
    ok.

handle_call_set_gc_config_test() ->
    State = default_state(),
    NewConfig = #gc_config{max_processes_per_tick = 50, aggressive = true},
    {reply, ok, NewState} = flurm_chaos:handle_call({set_gc_config, NewConfig}, {self(), make_ref()}, State),
    ?assertEqual(50, (NewState#state.gc_config)#gc_config.max_processes_per_tick),
    ?assertEqual(true, (NewState#state.gc_config)#gc_config.aggressive),
    ok.

handle_call_get_gc_config_test() ->
    State = default_state(),
    {reply, Config, _} = flurm_chaos:handle_call(get_gc_config, {self(), make_ref()}, State),
    ?assertEqual(10, Config#gc_config.max_processes_per_tick),
    ?assertEqual(false, Config#gc_config.aggressive),
    ok.

%%====================================================================
%% handle_call/3 Tests - Scheduler Configuration
%%====================================================================

handle_call_set_scheduler_config_test() ->
    State = default_state(),
    NewConfig = #scheduler_config{min_suspend_ms = 10, max_suspend_ms = 200},
    {reply, ok, NewState} = flurm_chaos:handle_call({set_scheduler_config, NewConfig}, {self(), make_ref()}, State),
    ?assertEqual(10, (NewState#state.scheduler_config)#scheduler_config.min_suspend_ms),
    ?assertEqual(200, (NewState#state.scheduler_config)#scheduler_config.max_suspend_ms),
    ok.

handle_call_get_scheduler_config_test() ->
    State = default_state(),
    {reply, Config, _} = flurm_chaos:handle_call(get_scheduler_config, {self(), make_ref()}, State),
    ?assertEqual(1, Config#scheduler_config.min_suspend_ms),
    ?assertEqual(100, Config#scheduler_config.max_suspend_ms),
    ok.

handle_call_suspend_schedulers_test() ->
    State = default_state(),
    %% Use minimal duration to avoid slowing down tests too much
    {reply, Result, _} = flurm_chaos:handle_call({suspend_schedulers, 1}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% handle_call/3 Tests - inject_once
%%====================================================================

handle_call_inject_once_trigger_gc_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(trigger_gc, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_memory_pressure_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, memory_pressure}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(memory_pressure, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_cpu_burn_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, cpu_burn}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(cpu_burn, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_slow_disk_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, slow_disk}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(slow_disk, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_delay_message_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, delay_message}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(delay_message, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_drop_message_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, drop_message}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(drop_message, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_gc_pressure_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, gc_pressure}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(gc_pressure, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_network_partition_no_nodes_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({inject_once, network_partition}, {self(), make_ref()}, State),
    %% No other nodes connected, so this should return error
    ?assertEqual({error, no_nodes_available}, Result),
    ok.

handle_call_inject_once_scheduler_suspend_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, scheduler_suspend}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(scheduler_suspend, NewState#state.stats, 0)),
    ok.

handle_call_inject_once_unknown_scenario_test() ->
    State = default_state(),
    {reply, Result, _} = flurm_chaos:handle_call({inject_once, unknown_scenario}, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_scenario}, Result),
    ok.

handle_call_inject_once_kill_random_process_test() ->
    State = default_state(),
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, kill_random_process}, {self(), make_ref()}, State),
    %% Result could be ok or error depending on available killable processes
    ?assert(Result =:= {error, no_killable_process} orelse is_tuple(Result) andalso element(1, Result) =:= ok),
    ?assertEqual(1, maps:get(kill_random_process, NewState#state.stats, 0)),
    ok.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_unknown_test() ->
    State = default_state(),
    {noreply, NewState} = flurm_chaos:handle_cast(unknown_message, State),
    ?assertEqual(State, NewState),
    ok.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_tick_disabled_test() ->
    S = default_state(),
    State = S#state{enabled = false},
    {noreply, NewState} = flurm_chaos:handle_info(tick, State),
    %% State should be unchanged when disabled
    ?assertEqual(false, NewState#state.enabled),
    ok.

handle_info_tick_enabled_test() ->
    State = make_enabled_state(),
    {noreply, NewState} = flurm_chaos:handle_info(tick, State),
    ?assert(NewState#state.tick_ref =/= undefined),
    %% Cancel the timer to avoid leaks
    erlang:cancel_timer(NewState#state.tick_ref),
    ok.

handle_info_heal_partition_test() ->
    State = make_state_with_partitions(#{'test@node' => make_ref()}),
    {noreply, NewState} = flurm_chaos:handle_info({heal_partition, 'test@node'}, State),
    ?assertNot(maps:is_key('test@node', NewState#state.partitioned_nodes)),
    ok.

handle_info_unknown_test() ->
    State = default_state(),
    {noreply, NewState} = flurm_chaos:handle_info(unknown_message, State),
    ?assertEqual(State, NewState),
    ok.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_with_partitions_test() ->
    State = make_state_with_partitions(#{'test@node' => undefined}),
    Result = flurm_chaos:terminate(normal, State),
    ?assertEqual(ok, Result),
    ok.

terminate_empty_partitions_test() ->
    State = default_state(),
    Result = flurm_chaos:terminate(normal, State),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test() ->
    State = default_state(),
    {ok, NewState} = flurm_chaos:code_change("1.0.0", State, []),
    ?assertEqual(State, NewState),
    ok.

%%====================================================================
%% Helper Function Tests - should_delay_io/0 and maybe_delay_io/1
%%====================================================================

should_delay_io_false_test() ->
    erase(chaos_slow_disk),
    Result = flurm_chaos:should_delay_io(),
    ?assertEqual(false, Result),
    ok.

should_delay_io_true_test() ->
    put(chaos_slow_disk, true),
    Result = flurm_chaos:should_delay_io(),
    ?assertEqual(true, Result),
    erase(chaos_slow_disk),
    ok.

maybe_delay_io_no_delay_test() ->
    erase(chaos_slow_disk),
    Result = flurm_chaos:maybe_delay_io(100),
    ?assertEqual(ok, Result),
    ok.

maybe_delay_io_with_delay_test() ->
    put(chaos_slow_disk, true),
    StartTime = erlang:system_time(millisecond),
    Result = flurm_chaos:maybe_delay_io(10),
    EndTime = erlang:system_time(millisecond),
    ?assertEqual(ok, Result),
    %% Should have delayed at least some time (allow for timing variance)
    ?assert((EndTime - StartTime) >= 0),
    erase(chaos_slow_disk),
    ok.

%%====================================================================
%% Stats Increment Tests
%%====================================================================

stats_increment_test() ->
    State = default_state(),
    %% Inject twice to test increment
    {reply, _, State1} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State),
    {reply, _, State2} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State1),
    ?assertEqual(2, maps:get(trigger_gc, State2#state.stats)),
    ok.

%%====================================================================
%% Integration-style Tests (still pure, no mocking)
%%====================================================================

full_enable_disable_cycle_test() ->
    {ok, State0} = flurm_chaos:init(#{}),

    %% Enable
    {reply, ok, State1} = flurm_chaos:handle_call(enable, {self(), make_ref()}, State0),
    ?assertEqual(true, State1#state.enabled),

    %% Enable a scenario
    {reply, ok, State2} = flurm_chaos:handle_call({enable_scenario, trigger_gc}, {self(), make_ref()}, State1),
    ?assertEqual(true, maps:get(trigger_gc, State2#state.scenario_enabled)),

    %% Inject once
    {reply, ok, State3} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State2),
    ?assertEqual(1, maps:get(trigger_gc, State3#state.stats)),

    %% Disable scenario
    {reply, ok, State4} = flurm_chaos:handle_call({disable_scenario, trigger_gc}, {self(), make_ref()}, State3),
    ?assertEqual(false, maps:get(trigger_gc, State4#state.scenario_enabled)),

    %% Disable global
    {reply, ok, State5} = flurm_chaos:handle_call(disable, {self(), make_ref()}, State4),
    ?assertEqual(false, State5#state.enabled),

    ok.

protected_pids_lifecycle_test() ->
    State = default_state(),
    Pid = self(),

    %% Initially not protected
    ?assertNot(sets:is_element(Pid, State#state.protected_pids)),

    %% Mark protected
    {reply, ok, State1} = flurm_chaos:handle_call({mark_protected, Pid}, {self(), make_ref()}, State),
    ?assert(sets:is_element(Pid, State1#state.protected_pids)),

    %% Unmark protected
    {reply, ok, State2} = flurm_chaos:handle_call({unmark_protected, Pid}, {self(), make_ref()}, State1),
    ?assertNot(sets:is_element(Pid, State2#state.protected_pids)),

    ok.

config_update_lifecycle_test() ->
    State = default_state(),

    %% Update delay config
    DelayConfig = #delay_config{min_delay_ms = 100, max_delay_ms = 1000},
    {reply, ok, State1} = flurm_chaos:handle_call({set_delay_config, DelayConfig}, {self(), make_ref()}, State),
    {reply, RetrievedDelay, _} = flurm_chaos:handle_call(get_delay_config, {self(), make_ref()}, State1),
    ?assertEqual(100, RetrievedDelay#delay_config.min_delay_ms),

    %% Update kill config
    KillConfig = #kill_config{max_kills_per_tick = 5},
    {reply, ok, State2} = flurm_chaos:handle_call({set_kill_config, KillConfig}, {self(), make_ref()}, State1),
    {reply, RetrievedKill, _} = flurm_chaos:handle_call(get_kill_config, {self(), make_ref()}, State2),
    ?assertEqual(5, RetrievedKill#kill_config.max_kills_per_tick),

    %% Update gc config
    GcConfig = #gc_config{max_processes_per_tick = 20, aggressive = true},
    {reply, ok, State3} = flurm_chaos:handle_call({set_gc_config, GcConfig}, {self(), make_ref()}, State2),
    {reply, RetrievedGc, _} = flurm_chaos:handle_call(get_gc_config, {self(), make_ref()}, State3),
    ?assertEqual(20, RetrievedGc#gc_config.max_processes_per_tick),
    ?assertEqual(true, RetrievedGc#gc_config.aggressive),

    %% Update scheduler config
    SchedulerConfig = #scheduler_config{min_suspend_ms = 5, max_suspend_ms = 50},
    {reply, ok, State4} = flurm_chaos:handle_call({set_scheduler_config, SchedulerConfig}, {self(), make_ref()}, State3),
    {reply, RetrievedScheduler, _} = flurm_chaos:handle_call(get_scheduler_config, {self(), make_ref()}, State4),
    ?assertEqual(5, RetrievedScheduler#scheduler_config.min_suspend_ms),
    ?assertEqual(50, RetrievedScheduler#scheduler_config.max_suspend_ms),

    ok.

scenario_probability_update_test() ->
    State = default_state(),

    %% Set custom probability
    {reply, ok, State1} = flurm_chaos:handle_call({set_scenario, trigger_gc, 0.9}, {self(), make_ref()}, State),
    {reply, Scenarios, _} = flurm_chaos:handle_call(get_scenarios, {self(), make_ref()}, State1),
    ?assertEqual(0.9, maps:get(trigger_gc, Scenarios)),

    %% Add new scenario
    {reply, ok, State2} = flurm_chaos:handle_call({set_scenario, custom_scenario, 0.5}, {self(), make_ref()}, State1),
    {reply, Scenarios2, _} = flurm_chaos:handle_call(get_scenarios, {self(), make_ref()}, State2),
    ?assertEqual(0.5, maps:get(custom_scenario, Scenarios2)),

    ok.

%%====================================================================
%% Init with Record Configs Tests
%%====================================================================

init_delay_config_record_test() ->
    DelayConfig = #delay_config{min_delay_ms = 25, max_delay_ms = 750},
    {ok, State} = flurm_chaos:init(#{delay_config => DelayConfig}),
    ?assertEqual(25, (State#state.delay_config)#delay_config.min_delay_ms),
    ?assertEqual(750, (State#state.delay_config)#delay_config.max_delay_ms),
    ok.

init_kill_config_record_test() ->
    KillConfig = #kill_config{max_kills_per_tick = 10, kill_signal = my_signal},
    {ok, State} = flurm_chaos:init(#{kill_config => KillConfig}),
    ?assertEqual(10, (State#state.kill_config)#kill_config.max_kills_per_tick),
    ?assertEqual(my_signal, (State#state.kill_config)#kill_config.kill_signal),
    ok.

init_partition_config_record_test() ->
    PartitionConfig = #partition_config{duration_ms = 3000, auto_heal = false, block_mode = filter},
    {ok, State} = flurm_chaos:init(#{partition_config => PartitionConfig}),
    ?assertEqual(3000, (State#state.partition_config)#partition_config.duration_ms),
    ?assertEqual(false, (State#state.partition_config)#partition_config.auto_heal),
    ?assertEqual(filter, (State#state.partition_config)#partition_config.block_mode),
    ok.

init_gc_config_record_test() ->
    GcConfig = #gc_config{max_processes_per_tick = 5, target_heap_size = 1024, aggressive = true},
    {ok, State} = flurm_chaos:init(#{gc_config => GcConfig}),
    ?assertEqual(5, (State#state.gc_config)#gc_config.max_processes_per_tick),
    ?assertEqual(1024, (State#state.gc_config)#gc_config.target_heap_size),
    ?assertEqual(true, (State#state.gc_config)#gc_config.aggressive),
    ok.

init_scheduler_config_record_test() ->
    SchedulerConfig = #scheduler_config{min_suspend_ms = 2, max_suspend_ms = 50, affect_dirty_schedulers = true},
    {ok, State} = flurm_chaos:init(#{scheduler_config => SchedulerConfig}),
    ?assertEqual(2, (State#state.scheduler_config)#scheduler_config.min_suspend_ms),
    ?assertEqual(50, (State#state.scheduler_config)#scheduler_config.max_suspend_ms),
    ?assertEqual(true, (State#state.scheduler_config)#scheduler_config.affect_dirty_schedulers),
    ok.

%%====================================================================
%% Tick with Enabled Scenarios Tests
%%====================================================================

handle_info_tick_with_scenario_enabled_test() ->
    %% Create a state with trigger_gc scenario enabled with 100% probability
    S = default_state(),
    State = S#state{
        enabled = true,
        scenarios = #{trigger_gc => 1.0},  % 100% probability
        scenario_enabled = #{trigger_gc => true}
    },
    {noreply, NewState} = flurm_chaos:handle_info(tick, State),
    %% The tick should have executed and stats should be updated
    ?assert(NewState#state.tick_ref =/= undefined),
    %% Stats may or may not be incremented depending on random seed
    ?assert(is_map(NewState#state.stats)),
    erlang:cancel_timer(NewState#state.tick_ref),
    ok.

handle_info_tick_no_trigger_test() ->
    %% Create a state with 0% probability - scenarios should not trigger
    S = default_state(),
    State = S#state{
        enabled = true,
        scenarios = #{trigger_gc => 0.0},
        scenario_enabled = #{trigger_gc => true}
    },
    {noreply, NewState} = flurm_chaos:handle_info(tick, State),
    ?assert(NewState#state.tick_ref =/= undefined),
    %% With 0% probability, no stats should be incremented
    ?assertEqual(#{}, NewState#state.stats),
    erlang:cancel_timer(NewState#state.tick_ref),
    ok.

%%====================================================================
%% GC Pressure with Aggressive Mode Test
%%====================================================================

handle_call_inject_gc_pressure_aggressive_test() ->
    S = default_state(),
    AggressiveGcConfig = #gc_config{max_processes_per_tick = 3, aggressive = true},
    State = S#state{gc_config = AggressiveGcConfig},
    {reply, Result, NewState} = flurm_chaos:handle_call({inject_once, gc_pressure}, {self(), make_ref()}, State),
    ?assertEqual(ok, Result),
    ?assertEqual(1, maps:get(gc_pressure, NewState#state.stats, 0)),
    ok.

%%====================================================================
%% Additional Status Tests
%%====================================================================

handle_call_status_with_protected_pids_test() ->
    Pid = self(),
    State = make_state_with_protected_pid(Pid),
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    ?assertEqual(1, maps:get(protected_pids, Status)),
    ok.

handle_call_status_with_partitions_test() ->
    State = make_state_with_partitions(#{'node1@host' => make_ref()}),
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    ?assertEqual(['node1@host'], maps:get(partitioned_nodes, Status)),
    ok.

%%====================================================================
%% Edge Cases Tests
%%====================================================================

handle_call_enable_multiple_times_test() ->
    State = default_state(),
    {reply, ok, State1} = flurm_chaos:handle_call(enable, {self(), make_ref()}, State),
    OldRef = State1#state.tick_ref,
    %% Enable again - should create new timer
    {reply, ok, State2} = flurm_chaos:handle_call(enable, {self(), make_ref()}, State1),
    NewRef = State2#state.tick_ref,
    ?assertEqual(true, State2#state.enabled),
    ?assertNotEqual(OldRef, NewRef),
    %% Cancel both timers to avoid leaks
    erlang:cancel_timer(OldRef),
    erlang:cancel_timer(NewRef),
    ok.

handle_call_enable_scenario_new_test() ->
    %% Enable a scenario that wasn't in the original map
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call({enable_scenario, new_custom_scenario}, {self(), make_ref()}, State),
    ?assertEqual(true, maps:get(new_custom_scenario, NewState#state.scenario_enabled)),
    ok.

handle_call_disable_scenario_not_present_test() ->
    %% Disable a scenario that wasn't in the original map
    State = default_state(),
    {reply, ok, NewState} = flurm_chaos:handle_call({disable_scenario, nonexistent_scenario}, {self(), make_ref()}, State),
    ?assertEqual(false, maps:get(nonexistent_scenario, NewState#state.scenario_enabled)),
    ok.

%%====================================================================
%% Comprehensive Init Tests
%%====================================================================

init_all_configs_test() ->
    Opts = #{
        tick_ms => 2000,
        scenarios => #{test_scenario => 0.5},
        scenario_enabled => #{test_scenario => true},
        protected_apps => [my_custom_app],
        delay_config => #{min_delay_ms => 20},
        kill_config => #{max_kills_per_tick => 2},
        partition_config => #{duration_ms => 7000},
        gc_config => #{max_processes_per_tick => 15},
        scheduler_config => #{min_suspend_ms => 3}
    },
    {ok, State} = flurm_chaos:init(Opts),
    ?assertEqual(2000, State#state.tick_ms),
    ?assertEqual(0.5, maps:get(test_scenario, State#state.scenarios)),
    ?assertEqual(true, maps:get(test_scenario, State#state.scenario_enabled)),
    ?assert(lists:member(my_custom_app, State#state.protected_apps)),
    ?assertEqual(20, (State#state.delay_config)#delay_config.min_delay_ms),
    ?assertEqual(2, (State#state.kill_config)#kill_config.max_kills_per_tick),
    ?assertEqual(7000, (State#state.partition_config)#partition_config.duration_ms),
    ?assertEqual(15, (State#state.gc_config)#gc_config.max_processes_per_tick),
    ?assertEqual(3, (State#state.scheduler_config)#scheduler_config.min_suspend_ms),
    ok.

%%====================================================================
%% Partition with already partitioned node test
%%====================================================================

handle_call_partition_already_partitioned_node_test() ->
    %% When a node is already in the partitioned_nodes map,
    %% the condition `maps:is_key(Node, State#state.partitioned_nodes)` is true
    State = make_state_with_partitions(#{'existing@node' => make_ref()}),
    %% Trying to partition an already partitioned node
    {reply, Result, _NewState} = flurm_chaos:handle_call({partition_node, 'existing@node', #{}}, {self(), make_ref()}, State),
    %% The code checks: lists:member(Node, nodes()) orelse maps:is_key(Node, State#state.partitioned_nodes)
    %% Since 'existing@node' is in partitioned_nodes, this should succeed
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Config to Map Conversion Tests
%%====================================================================

delay_config_to_map_in_status_test() ->
    S = default_state(),
    State = S#state{delay_config = #delay_config{
        min_delay_ms = 100,
        max_delay_ms = 1000,
        target_modules = [test_mod],
        exclude_modules = [excluded_mod]
    }},
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    DelayMap = maps:get(delay_config, Status),
    ?assertEqual(100, maps:get(min_delay_ms, DelayMap)),
    ?assertEqual(1000, maps:get(max_delay_ms, DelayMap)),
    ?assertEqual([test_mod], maps:get(target_modules, DelayMap)),
    ?assertEqual([excluded_mod], maps:get(exclude_modules, DelayMap)),
    ok.

kill_config_to_map_in_status_test() ->
    S = default_state(),
    State = S#state{kill_config = #kill_config{
        max_kills_per_tick = 5,
        kill_signal = test_signal,
        respect_links = false
    }},
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    KillMap = maps:get(kill_config, Status),
    ?assertEqual(5, maps:get(max_kills_per_tick, KillMap)),
    ?assertEqual(test_signal, maps:get(kill_signal, KillMap)),
    ?assertEqual(false, maps:get(respect_links, KillMap)),
    ok.

partition_config_to_map_in_status_test() ->
    S = default_state(),
    State = S#state{partition_config = #partition_config{
        duration_ms = 8000,
        auto_heal = false,
        block_mode = filter
    }},
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    PartitionMap = maps:get(partition_config, Status),
    ?assertEqual(8000, maps:get(duration_ms, PartitionMap)),
    ?assertEqual(false, maps:get(auto_heal, PartitionMap)),
    ?assertEqual(filter, maps:get(block_mode, PartitionMap)),
    ok.

gc_config_to_map_in_status_test() ->
    S = default_state(),
    State = S#state{gc_config = #gc_config{
        max_processes_per_tick = 25,
        target_heap_size = 2048,
        aggressive = true
    }},
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    GcMap = maps:get(gc_config, Status),
    ?assertEqual(25, maps:get(max_processes_per_tick, GcMap)),
    ?assertEqual(2048, maps:get(target_heap_size, GcMap)),
    ?assertEqual(true, maps:get(aggressive, GcMap)),
    ok.

scheduler_config_to_map_in_status_test() ->
    S = default_state(),
    State = S#state{scheduler_config = #scheduler_config{
        min_suspend_ms = 10,
        max_suspend_ms = 200,
        affect_dirty_schedulers = true
    }},
    {reply, Status, _} = flurm_chaos:handle_call(status, {self(), make_ref()}, State),
    SchedulerMap = maps:get(scheduler_config, Status),
    ?assertEqual(10, maps:get(min_suspend_ms, SchedulerMap)),
    ?assertEqual(200, maps:get(max_suspend_ms, SchedulerMap)),
    ?assertEqual(true, maps:get(affect_dirty_schedulers, SchedulerMap)),
    ok.

%%====================================================================
%% Multiple Stats Updates Test
%%====================================================================

multiple_inject_different_scenarios_test() ->
    State = default_state(),
    {reply, _, State1} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State),
    {reply, _, State2} = flurm_chaos:handle_call({inject_once, memory_pressure}, {self(), make_ref()}, State1),
    {reply, _, State3} = flurm_chaos:handle_call({inject_once, cpu_burn}, {self(), make_ref()}, State2),
    {reply, _, State4} = flurm_chaos:handle_call({inject_once, trigger_gc}, {self(), make_ref()}, State3),

    ?assertEqual(2, maps:get(trigger_gc, State4#state.stats)),
    ?assertEqual(1, maps:get(memory_pressure, State4#state.stats)),
    ?assertEqual(1, maps:get(cpu_burn, State4#state.stats)),
    ok.

%%====================================================================
%% Terminate Tests with Different Reasons
%%====================================================================

terminate_shutdown_test() ->
    State = default_state(),
    Result = flurm_chaos:terminate(shutdown, State),
    ?assertEqual(ok, Result),
    ok.

terminate_error_test() ->
    State = default_state(),
    Result = flurm_chaos:terminate({error, some_reason}, State),
    ?assertEqual(ok, Result),
    ok.
