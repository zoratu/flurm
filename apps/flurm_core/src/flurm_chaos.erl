%%%-------------------------------------------------------------------
%%% @doc FLURM Chaos Engineering Module
%%%
%%% Runtime chaos injection for testing fault tolerance. This module
%%% allows controlled failure injection in development/staging to
%%% verify system resilience.
%%%
%%% WARNING: Never enable in production unless you know what you're doing!
%%%
%%% Features:
%%% - Kill random processes (respects supervisors)
%%% - Inject message delays on gen_server calls
%%% - Drop messages probabilistically
%%% - Force garbage collection pressure
%%% - Network partition simulation (block messages between nodes)
%%% - Scheduler suspension (brief BEAM scheduler pauses)
%%% - Memory pressure injection
%%% - CPU burn injection
%%%
%%% Each chaos injection type can be enabled/disabled independently
%%% with configurable probability-based triggering.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_chaos).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([enable/0, disable/0, is_enabled/0]).
-export([set_scenario/2, get_scenarios/0]).
-export([inject_once/1]).
-export([status/0]).

%% Per-scenario enable/disable controls
-export([enable_scenario/1, disable_scenario/1, is_scenario_enabled/1]).
-export([enable_all_scenarios/0, disable_all_scenarios/0]).

%% Message delay injection API
-export([wrap_call/3, wrap_call/4]).
-export([maybe_delay_call/0, maybe_delay_call/1]).
-export([set_delay_config/1, get_delay_config/0]).

%% Process kill injection API
-export([set_kill_config/1, get_kill_config/0]).
-export([mark_process_protected/1, unmark_process_protected/1]).

%% Network partition simulation API
-export([partition_node/1, partition_node/2, heal_partition/1, heal_all_partitions/0]).
-export([is_partitioned/1, get_partitions/0]).
-export([set_partition_config/1, get_partition_config/0]).

%% GC pressure testing API
-export([gc_all_processes/0, gc_process/1]).
-export([set_gc_config/1, get_gc_config/0]).

%% Scheduler suspension API
-export([suspend_schedulers/1, set_scheduler_config/1, get_scheduler_config/0]).

%% Helper functions for chaos-aware code
-export([should_delay_io/0, maybe_delay_io/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TICK_MS, 1000).

-record(delay_config, {
    min_delay_ms = 10 :: non_neg_integer(),
    max_delay_ms = 500 :: non_neg_integer(),
    target_modules = [] :: [module()],  % Empty = all modules
    exclude_modules = [?MODULE] :: [module()]
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
    %% Configuration records
    delay_config :: #delay_config{},
    kill_config :: #kill_config{},
    partition_config :: #partition_config{},
    gc_config :: #gc_config{},
    scheduler_config :: #scheduler_config{}
}).

-type scenario() :: kill_random_process
                  | delay_message
                  | drop_message
                  | trigger_gc
                  | slow_disk
                  | memory_pressure
                  | cpu_burn
                  | network_partition
                  | scheduler_suspend
                  | gc_pressure.

-export_type([scenario/0]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    increment_stat/2,
    is_system_process/1,
    is_supervisor/1,
    shuffle_list/1,
    map_to_delay_config/1,
    map_to_kill_config/1,
    map_to_partition_config/1,
    map_to_gc_config/1,
    map_to_scheduler_config/1,
    delay_config_to_map/1,
    kill_config_to_map/1,
    partition_config_to_map/1,
    gc_config_to_map/1,
    scheduler_config_to_map/1,
    should_delay_module/2
]).
-endif.

%%====================================================================
%% Default Scenarios
%%====================================================================

-define(DEFAULT_SCENARIOS, #{
    kill_random_process => 0.001,    % 0.1% chance per tick
    trigger_gc => 0.01,              % 1% chance per tick
    memory_pressure => 0.005,        % 0.5% chance per tick
    cpu_burn => 0.002,               % 0.2% chance per tick
    delay_message => 0.05,           % 5% chance per tick
    scheduler_suspend => 0.001,      % 0.1% chance per tick
    gc_pressure => 0.02,             % 2% chance per tick
    network_partition => 0.0005      % 0.05% chance per tick
}).

%% Default scenario enabled states
-define(DEFAULT_SCENARIO_ENABLED, #{
    kill_random_process => false,
    trigger_gc => false,
    memory_pressure => false,
    cpu_burn => false,
    delay_message => false,
    scheduler_suspend => false,
    gc_pressure => false,
    network_partition => false,
    slow_disk => false,
    drop_message => false
}).

%% Apps that should never have processes killed
-define(PROTECTED_APPS, [kernel, stdlib, sasl, ranch, ra]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the chaos server with default settings
start_link() ->
    start_link(#{}).

%% @doc Start with options
%% Options:
%%   tick_ms - How often to check for chaos injection (default: 1000)
%%   scenarios - Map of scenario => probability
%%   protected_apps - Additional apps to protect
%%   delay_config - #delay_config{} or map
%%   kill_config - #kill_config{} or map
%%   partition_config - #partition_config{} or map
%%   gc_config - #gc_config{} or map
%%   scheduler_config - #scheduler_config{} or map
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Enable chaos injection globally
-spec enable() -> ok.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable chaos injection globally
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Check if chaos is enabled globally
-spec is_enabled() -> boolean().
is_enabled() ->
    gen_server:call(?SERVER, is_enabled).

%% @doc Enable a specific chaos scenario
-spec enable_scenario(scenario()) -> ok.
enable_scenario(Scenario) ->
    gen_server:call(?SERVER, {enable_scenario, Scenario}).

%% @doc Disable a specific chaos scenario
-spec disable_scenario(scenario()) -> ok.
disable_scenario(Scenario) ->
    gen_server:call(?SERVER, {disable_scenario, Scenario}).

%% @doc Check if a specific scenario is enabled
-spec is_scenario_enabled(scenario()) -> boolean().
is_scenario_enabled(Scenario) ->
    gen_server:call(?SERVER, {is_scenario_enabled, Scenario}).

%% @doc Enable all chaos scenarios
-spec enable_all_scenarios() -> ok.
enable_all_scenarios() ->
    gen_server:call(?SERVER, enable_all_scenarios).

%% @doc Disable all chaos scenarios
-spec disable_all_scenarios() -> ok.
disable_all_scenarios() ->
    gen_server:call(?SERVER, disable_all_scenarios).

%% @doc Set probability for a scenario
-spec set_scenario(scenario(), float()) -> ok.
set_scenario(Scenario, Probability) when Probability >= 0.0, Probability =< 1.0 ->
    gen_server:call(?SERVER, {set_scenario, Scenario, Probability}).

%% @doc Get current scenario configuration
-spec get_scenarios() -> #{scenario() => float()}.
get_scenarios() ->
    gen_server:call(?SERVER, get_scenarios).

%% @doc Inject a specific scenario immediately
-spec inject_once(scenario()) -> ok | {error, term()}.
inject_once(Scenario) ->
    gen_server:call(?SERVER, {inject_once, Scenario}).

%% @doc Get chaos statistics
-spec status() -> #{atom() => term()}.
status() ->
    gen_server:call(?SERVER, status).

%%====================================================================
%% Message Delay Injection API
%%====================================================================

%% @doc Wrap a gen_server call with potential chaos delay
-spec wrap_call(module(), pid() | atom(), term()) -> term().
wrap_call(Module, ServerRef, Request) ->
    wrap_call(Module, ServerRef, Request, 5000).

%% @doc Wrap a gen_server call with potential chaos delay and timeout
-spec wrap_call(module(), pid() | atom(), term(), timeout()) -> term().
wrap_call(Module, ServerRef, Request, Timeout) ->
    maybe_delay_call(Module),
    gen_server:call(ServerRef, Request, Timeout).

%% @doc Maybe inject a delay based on current chaos configuration
-spec maybe_delay_call() -> ok.
maybe_delay_call() ->
    maybe_delay_call(undefined).

%% @doc Maybe inject a delay for a specific module
-spec maybe_delay_call(module() | undefined) -> ok.
maybe_delay_call(Module) ->
    try
        case is_enabled() andalso is_scenario_enabled(delay_message) of
            true ->
                Config = get_delay_config(),
                case should_delay_module(Module, Config) of
                    true ->
                        {Roll, _} = rand:uniform_s(rand:seed(exsss)),
                        Probability = maps:get(delay_message, get_scenarios(), 0.0),
                        case Roll < Probability of
                            true ->
                                MinDelay = Config#delay_config.min_delay_ms,
                                MaxDelay = Config#delay_config.max_delay_ms,
                                Delay = MinDelay + rand:uniform(max(1, MaxDelay - MinDelay)),
                                logger:debug("[CHAOS] Injecting ~pms delay for ~p", [Delay, Module]),
                                timer:sleep(Delay);
                            false ->
                                ok
                        end;
                    false ->
                        ok
                end;
            false ->
                ok
        end
    catch
        _:_ -> ok  % Chaos server not running, skip
    end.

%% @doc Set delay injection configuration
-spec set_delay_config(map() | #delay_config{}) -> ok.
set_delay_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_delay_config, map_to_delay_config(Config)});
set_delay_config(Config) when is_record(Config, delay_config) ->
    gen_server:call(?SERVER, {set_delay_config, Config}).

%% @doc Get current delay configuration
-spec get_delay_config() -> #delay_config{}.
get_delay_config() ->
    gen_server:call(?SERVER, get_delay_config).

%%====================================================================
%% Process Kill Injection API
%%====================================================================

%% @doc Set kill injection configuration
-spec set_kill_config(map() | #kill_config{}) -> ok.
set_kill_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_kill_config, map_to_kill_config(Config)});
set_kill_config(Config) when is_record(Config, kill_config) ->
    gen_server:call(?SERVER, {set_kill_config, Config}).

%% @doc Get current kill configuration
-spec get_kill_config() -> #kill_config{}.
get_kill_config() ->
    gen_server:call(?SERVER, get_kill_config).

%% @doc Mark a process as protected from chaos kill
-spec mark_process_protected(pid()) -> ok.
mark_process_protected(Pid) ->
    gen_server:call(?SERVER, {mark_protected, Pid}).

%% @doc Remove protection from a process
-spec unmark_process_protected(pid()) -> ok.
unmark_process_protected(Pid) ->
    gen_server:call(?SERVER, {unmark_protected, Pid}).

%%====================================================================
%% Network Partition Simulation API
%%====================================================================

%% @doc Partition from a specific node
-spec partition_node(node()) -> ok | {error, term()}.
partition_node(Node) ->
    partition_node(Node, #{}).

%% @doc Partition from a specific node with options
-spec partition_node(node(), map()) -> ok | {error, term()}.
partition_node(Node, Opts) ->
    gen_server:call(?SERVER, {partition_node, Node, Opts}).

%% @doc Heal partition with a specific node
-spec heal_partition(node()) -> ok | {error, term()}.
heal_partition(Node) ->
    gen_server:call(?SERVER, {heal_partition, Node}).

%% @doc Heal all partitions
-spec heal_all_partitions() -> ok.
heal_all_partitions() ->
    gen_server:call(?SERVER, heal_all_partitions).

%% @doc Check if a node is partitioned
-spec is_partitioned(node()) -> boolean().
is_partitioned(Node) ->
    gen_server:call(?SERVER, {is_partitioned, Node}).

%% @doc Get all partitioned nodes
-spec get_partitions() -> [node()].
get_partitions() ->
    gen_server:call(?SERVER, get_partitions).

%% @doc Set partition configuration
-spec set_partition_config(map() | #partition_config{}) -> ok.
set_partition_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_partition_config, map_to_partition_config(Config)});
set_partition_config(Config) when is_record(Config, partition_config) ->
    gen_server:call(?SERVER, {set_partition_config, Config}).

%% @doc Get current partition configuration
-spec get_partition_config() -> #partition_config{}.
get_partition_config() ->
    gen_server:call(?SERVER, get_partition_config).

%%====================================================================
%% GC Pressure Testing API
%%====================================================================

%% @doc Force garbage collection on all processes
-spec gc_all_processes() -> {ok, non_neg_integer()}.
gc_all_processes() ->
    gen_server:call(?SERVER, gc_all_processes, 30000).

%% @doc Force garbage collection on a specific process
-spec gc_process(pid()) -> ok | {error, term()}.
gc_process(Pid) ->
    gen_server:call(?SERVER, {gc_process, Pid}).

%% @doc Set GC pressure configuration
-spec set_gc_config(map() | #gc_config{}) -> ok.
set_gc_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_gc_config, map_to_gc_config(Config)});
set_gc_config(Config) when is_record(Config, gc_config) ->
    gen_server:call(?SERVER, {set_gc_config, Config}).

%% @doc Get current GC configuration
-spec get_gc_config() -> #gc_config{}.
get_gc_config() ->
    gen_server:call(?SERVER, get_gc_config).

%%====================================================================
%% Scheduler Suspension API
%%====================================================================

%% @doc Suspend schedulers for a specified duration
-spec suspend_schedulers(pos_integer()) -> ok.
suspend_schedulers(DurationMs) when DurationMs > 0, DurationMs =< 1000 ->
    gen_server:call(?SERVER, {suspend_schedulers, DurationMs}).

%% @doc Set scheduler suspension configuration
-spec set_scheduler_config(map() | #scheduler_config{}) -> ok.
set_scheduler_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_scheduler_config, map_to_scheduler_config(Config)});
set_scheduler_config(Config) when is_record(Config, scheduler_config) ->
    gen_server:call(?SERVER, {set_scheduler_config, Config}).

%% @doc Get current scheduler configuration
-spec get_scheduler_config() -> #scheduler_config{}.
get_scheduler_config() ->
    gen_server:call(?SERVER, get_scheduler_config).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    TickMs = maps:get(tick_ms, Opts, ?DEFAULT_TICK_MS),
    Scenarios = maps:merge(?DEFAULT_SCENARIOS, maps:get(scenarios, Opts, #{})),
    ScenarioEnabled = maps:merge(?DEFAULT_SCENARIO_ENABLED, maps:get(scenario_enabled, Opts, #{})),
    ProtectedApps = ?PROTECTED_APPS ++ maps:get(protected_apps, Opts, []),
    Seed = rand:seed(exsss, erlang:system_time()),

    DelayConfig = init_delay_config(maps:get(delay_config, Opts, #{})),
    KillConfig = init_kill_config(maps:get(kill_config, Opts, #{})),
    PartitionConfig = init_partition_config(maps:get(partition_config, Opts, #{})),
    GcConfig = init_gc_config(maps:get(gc_config, Opts, #{})),
    SchedulerConfig = init_scheduler_config(maps:get(scheduler_config, Opts, #{})),

    State = #state{
        enabled = false,
        scenarios = Scenarios,
        scenario_enabled = ScenarioEnabled,
        tick_ms = TickMs,
        stats = #{},
        protected_apps = ProtectedApps,
        protected_pids = sets:new(),
        seed = Seed,
        partitioned_nodes = #{},
        message_filters = #{},
        delay_config = DelayConfig,
        kill_config = KillConfig,
        partition_config = PartitionConfig,
        gc_config = GcConfig,
        scheduler_config = SchedulerConfig
    },
    {ok, State}.

handle_call(enable, _From, State) ->
    TickRef = erlang:send_after(State#state.tick_ms, self(), tick),
    logger:warning("[CHAOS] Chaos engineering ENABLED"),
    {reply, ok, State#state{enabled = true, tick_ref = TickRef}};

handle_call(disable, _From, State) ->
    case State#state.tick_ref of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,
    logger:info("[CHAOS] Chaos engineering disabled"),
    {reply, ok, State#state{enabled = false, tick_ref = undefined}};

handle_call(is_enabled, _From, State) ->
    {reply, State#state.enabled, State};

handle_call({enable_scenario, Scenario}, _From, State) ->
    NewEnabled = maps:put(Scenario, true, State#state.scenario_enabled),
    logger:info("[CHAOS] Scenario ~p ENABLED", [Scenario]),
    {reply, ok, State#state{scenario_enabled = NewEnabled}};

handle_call({disable_scenario, Scenario}, _From, State) ->
    NewEnabled = maps:put(Scenario, false, State#state.scenario_enabled),
    logger:info("[CHAOS] Scenario ~p disabled", [Scenario]),
    {reply, ok, State#state{scenario_enabled = NewEnabled}};

handle_call({is_scenario_enabled, Scenario}, _From, State) ->
    Enabled = maps:get(Scenario, State#state.scenario_enabled, false),
    {reply, Enabled, State};

handle_call(enable_all_scenarios, _From, State) ->
    NewEnabled = maps:map(fun(_, _) -> true end, State#state.scenario_enabled),
    logger:warning("[CHAOS] ALL scenarios ENABLED"),
    {reply, ok, State#state{scenario_enabled = NewEnabled}};

handle_call(disable_all_scenarios, _From, State) ->
    NewEnabled = maps:map(fun(_, _) -> false end, State#state.scenario_enabled),
    logger:info("[CHAOS] All scenarios disabled"),
    {reply, ok, State#state{scenario_enabled = NewEnabled}};

handle_call({set_scenario, Scenario, Prob}, _From, State) ->
    NewScenarios = maps:put(Scenario, Prob, State#state.scenarios),
    {reply, ok, State#state{scenarios = NewScenarios}};

handle_call(get_scenarios, _From, State) ->
    {reply, State#state.scenarios, State};

handle_call({inject_once, Scenario}, _From, State) ->
    Result = execute_scenario(Scenario, State),
    NewStats = increment_stat(Scenario, State#state.stats),
    {reply, Result, State#state{stats = NewStats}};

handle_call(status, _From, State) ->
    Status = #{
        enabled => State#state.enabled,
        scenarios => State#state.scenarios,
        scenario_enabled => State#state.scenario_enabled,
        stats => State#state.stats,
        tick_ms => State#state.tick_ms,
        protected_apps => State#state.protected_apps,
        protected_pids => sets:size(State#state.protected_pids),
        partitioned_nodes => maps:keys(State#state.partitioned_nodes),
        delay_config => delay_config_to_map(State#state.delay_config),
        kill_config => kill_config_to_map(State#state.kill_config),
        partition_config => partition_config_to_map(State#state.partition_config),
        gc_config => gc_config_to_map(State#state.gc_config),
        scheduler_config => scheduler_config_to_map(State#state.scheduler_config)
    },
    {reply, Status, State};

%% Delay config
handle_call({set_delay_config, Config}, _From, State) ->
    {reply, ok, State#state{delay_config = Config}};

handle_call(get_delay_config, _From, State) ->
    {reply, State#state.delay_config, State};

%% Kill config
handle_call({set_kill_config, Config}, _From, State) ->
    {reply, ok, State#state{kill_config = Config}};

handle_call(get_kill_config, _From, State) ->
    {reply, State#state.kill_config, State};

handle_call({mark_protected, Pid}, _From, State) ->
    NewProtected = sets:add_element(Pid, State#state.protected_pids),
    {reply, ok, State#state{protected_pids = NewProtected}};

handle_call({unmark_protected, Pid}, _From, State) ->
    NewProtected = sets:del_element(Pid, State#state.protected_pids),
    {reply, ok, State#state{protected_pids = NewProtected}};

%% Partition operations
handle_call({partition_node, Node, Opts}, _From, State) ->
    case lists:member(Node, nodes()) orelse maps:is_key(Node, State#state.partitioned_nodes) of
        false ->
            {reply, {error, node_not_connected}, State};
        true ->
            Config = State#state.partition_config,
            Duration = maps:get(duration_ms, Opts, Config#partition_config.duration_ms),
            AutoHeal = maps:get(auto_heal, Opts, Config#partition_config.auto_heal),

            logger:warning("[CHAOS] Creating partition from node ~p for ~pms", [Node, Duration]),
            erlang:disconnect_node(Node),

            NewState = case AutoHeal of
                true ->
                    TimerRef = erlang:send_after(Duration, self(), {heal_partition, Node}),
                    State#state{partitioned_nodes = maps:put(Node, TimerRef, State#state.partitioned_nodes)};
                false ->
                    State#state{partitioned_nodes = maps:put(Node, undefined, State#state.partitioned_nodes)}
            end,
            {reply, ok, NewState}
    end;

handle_call({heal_partition, Node}, _From, State) ->
    case maps:get(Node, State#state.partitioned_nodes, undefined) of
        undefined ->
            {reply, {error, not_partitioned}, State};
        TimerRef ->
            case TimerRef of
                undefined -> ok;
                _ -> erlang:cancel_timer(TimerRef)
            end,
            logger:info("[CHAOS] Healing partition from node ~p", [Node]),
            net_adm:ping(Node),
            NewPartitions = maps:remove(Node, State#state.partitioned_nodes),
            {reply, ok, State#state{partitioned_nodes = NewPartitions}}
    end;

handle_call(heal_all_partitions, _From, State) ->
    maps:foreach(fun(Node, TimerRef) ->
        case TimerRef of
            undefined -> ok;
            _ -> erlang:cancel_timer(TimerRef)
        end,
        logger:info("[CHAOS] Healing partition from node ~p", [Node]),
        net_adm:ping(Node)
    end, State#state.partitioned_nodes),
    {reply, ok, State#state{partitioned_nodes = #{}}};

handle_call({is_partitioned, Node}, _From, State) ->
    {reply, maps:is_key(Node, State#state.partitioned_nodes), State};

handle_call(get_partitions, _From, State) ->
    {reply, maps:keys(State#state.partitioned_nodes), State};

handle_call({set_partition_config, Config}, _From, State) ->
    {reply, ok, State#state{partition_config = Config}};

handle_call(get_partition_config, _From, State) ->
    {reply, State#state.partition_config, State};

%% GC operations
handle_call(gc_all_processes, _From, State) ->
    Procs = erlang:processes(),
    Count = lists:foldl(fun(Pid, Acc) ->
        case catch erlang:garbage_collect(Pid) of
            true -> Acc + 1;
            _ -> Acc
        end
    end, 0, Procs),
    logger:info("[CHAOS] Forced GC on ~p processes", [Count]),
    {reply, {ok, Count}, State};

handle_call({gc_process, Pid}, _From, State) ->
    case catch erlang:garbage_collect(Pid) of
        true -> {reply, ok, State};
        _ -> {reply, {error, process_not_found}, State}
    end;

handle_call({set_gc_config, Config}, _From, State) ->
    {reply, ok, State#state{gc_config = Config}};

handle_call(get_gc_config, _From, State) ->
    {reply, State#state.gc_config, State};

%% Scheduler operations
handle_call({suspend_schedulers, DurationMs}, _From, State) ->
    logger:warning("[CHAOS] Suspending schedulers for ~pms", [DurationMs]),
    do_suspend_schedulers(DurationMs, State#state.scheduler_config),
    {reply, ok, State};

handle_call({set_scheduler_config, Config}, _From, State) ->
    {reply, ok, State#state{scheduler_config = Config}};

handle_call(get_scheduler_config, _From, State) ->
    {reply, State#state.scheduler_config, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, #state{enabled = false} = State) ->
    {noreply, State};
handle_info(tick, State) ->
    {NewSeed, NewStats} = run_chaos_tick(State),
    TickRef = erlang:send_after(State#state.tick_ms, self(), tick),
    {noreply, State#state{
        tick_ref = TickRef,
        seed = NewSeed,
        stats = NewStats
    }};

handle_info({heal_partition, Node}, State) ->
    logger:info("[CHAOS] Auto-healing partition from node ~p", [Node]),
    net_adm:ping(Node),
    NewPartitions = maps:remove(Node, State#state.partitioned_nodes),
    {noreply, State#state{partitioned_nodes = NewPartitions}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Heal all partitions on shutdown
    maps:foreach(fun(Node, _) ->
        net_adm:ping(Node)
    end, State#state.partitioned_nodes),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

run_chaos_tick(#state{scenarios = Scenarios, scenario_enabled = ScenarioEnabled,
                      seed = Seed, stats = Stats} = State) ->
    maps:fold(fun(Scenario, Probability, {AccSeed, AccStats}) ->
        %% Check if this specific scenario is enabled
        case maps:get(Scenario, ScenarioEnabled, false) of
            false ->
                {AccSeed, AccStats};
            true ->
                {Roll, NewSeed} = rand:uniform_s(AccSeed),
                case Roll < Probability of
                    true ->
                        execute_scenario(Scenario, State),
                        {NewSeed, increment_stat(Scenario, AccStats)};
                    false ->
                        {NewSeed, AccStats}
                end
        end
    end, {Seed, Stats}, Scenarios).

increment_stat(Scenario, Stats) ->
    maps:update_with(Scenario, fun(V) -> V + 1 end, 1, Stats).

execute_scenario(kill_random_process, State) ->
    KillConfig = State#state.kill_config,
    MaxKills = KillConfig#kill_config.max_kills_per_tick,
    Signal = KillConfig#kill_config.kill_signal,

    Killed = lists:foldl(fun(_, Acc) ->
        case find_killable_process(State#state.protected_apps, State#state.protected_pids) of
            {ok, Pid} ->
                logger:warning("[CHAOS] Killing process ~p with signal ~p", [Pid, Signal]),
                exit(Pid, Signal),
                Acc + 1;
            none ->
                Acc
        end
    end, 0, lists:seq(1, MaxKills)),

    case Killed of
        0 -> {error, no_killable_process};
        _ -> {ok, Killed}
    end;

execute_scenario(trigger_gc, _State) ->
    %% Force garbage collection on random processes
    Procs = erlang:processes(),
    case Procs of
        [] -> ok;
        _ ->
            Target = lists:nth(rand:uniform(length(Procs)), Procs),
            logger:debug("[CHAOS] Forcing GC on ~p", [Target]),
            catch erlang:garbage_collect(Target),
            ok
    end;

execute_scenario(gc_pressure, State) ->
    %% Force garbage collection on multiple random processes
    Config = State#state.gc_config,
    MaxProcs = Config#gc_config.max_processes_per_tick,
    Aggressive = Config#gc_config.aggressive,

    Procs = erlang:processes(),
    NumToGC = min(MaxProcs, length(Procs)),
    Shuffled = shuffle_list(Procs),
    Targets = lists:sublist(Shuffled, NumToGC),

    lists:foreach(fun(Pid) ->
        case Aggressive of
            true ->
                %% Force multiple GC passes for aggressive mode
                catch erlang:garbage_collect(Pid),
                catch erlang:garbage_collect(Pid),
                catch erlang:garbage_collect(Pid);
            false ->
                catch erlang:garbage_collect(Pid)
        end
    end, Targets),

    logger:debug("[CHAOS] GC pressure applied to ~p processes", [NumToGC]),
    ok;

execute_scenario(memory_pressure, _State) ->
    %% Allocate memory temporarily to trigger pressure
    logger:debug("[CHAOS] Injecting memory pressure"),
    spawn(fun() ->
        %% Allocate ~10MB
        _Data = binary:copy(<<0>>, 10 * 1024 * 1024),
        timer:sleep(100),
        ok
    end),
    ok;

execute_scenario(cpu_burn, _State) ->
    %% Burn CPU cycles briefly
    logger:debug("[CHAOS] Injecting CPU burn"),
    spawn(fun() ->
        EndTime = erlang:system_time(millisecond) + 50,
        cpu_burn_loop(EndTime)
    end),
    ok;

execute_scenario(slow_disk, _State) ->
    %% Inject delay into file operations
    %% This requires cooperation from I/O-doing code
    logger:debug("[CHAOS] Slow disk mode activated"),
    put(chaos_slow_disk, true),
    spawn(fun() ->
        timer:sleep(5000),
        erase(chaos_slow_disk)
    end),
    ok;

execute_scenario(delay_message, State) ->
    %% Message delay is handled via wrap_call/maybe_delay_call
    %% This scenario just logs that it was triggered
    Config = State#state.delay_config,
    MinDelay = Config#delay_config.min_delay_ms,
    MaxDelay = Config#delay_config.max_delay_ms,
    logger:debug("[CHAOS] Message delay scenario active (range: ~p-~pms)", [MinDelay, MaxDelay]),
    ok;

execute_scenario(drop_message, _State) ->
    %% Note: Actual message dropping requires tracing infrastructure
    logger:debug("[CHAOS] Message drop scenario triggered"),
    ok;

execute_scenario(network_partition, State) ->
    %% Simulate network partition by disconnecting from random node
    Nodes = nodes(),
    PartitionedNodes = maps:keys(State#state.partitioned_nodes),
    AvailableNodes = Nodes -- PartitionedNodes,

    case AvailableNodes of
        [] ->
            {error, no_nodes_available};
        _ ->
            Node = lists:nth(rand:uniform(length(AvailableNodes)), AvailableNodes),
            Config = State#state.partition_config,
            Duration = Config#partition_config.duration_ms,

            logger:warning("[CHAOS] Creating random partition from ~p for ~pms", [Node, Duration]),
            erlang:disconnect_node(Node),

            spawn(fun() ->
                timer:sleep(Duration),
                logger:info("[CHAOS] Auto-healing partition from ~p", [Node]),
                net_adm:ping(Node)
            end),
            ok
    end;

execute_scenario(scheduler_suspend, State) ->
    Config = State#state.scheduler_config,
    MinSuspend = Config#scheduler_config.min_suspend_ms,
    MaxSuspend = Config#scheduler_config.max_suspend_ms,
    Duration = MinSuspend + rand:uniform(max(1, MaxSuspend - MinSuspend)),

    logger:debug("[CHAOS] Suspending schedulers for ~pms", [Duration]),
    do_suspend_schedulers(Duration, Config),
    ok;

execute_scenario(Unknown, _State) ->
    logger:warning("[CHAOS] Unknown scenario: ~p", [Unknown]),
    {error, unknown_scenario}.

%% Find a process that can be safely killed
find_killable_process(ProtectedApps, ProtectedPids) ->
    Procs = erlang:processes(),
    Killable = lists:filter(fun(P) ->
        is_killable(P, ProtectedApps, ProtectedPids)
    end, Procs),
    case Killable of
        [] -> none;
        _ -> {ok, lists:nth(rand:uniform(length(Killable)), Killable)}
    end.

is_killable(Pid, ProtectedApps, ProtectedPids) ->
    try
        %% Check if explicitly protected
        case sets:is_element(Pid, ProtectedPids) of
            true -> false;
            false ->
                case process_info(Pid, [registered_name, dictionary, initial_call]) of
                    undefined ->
                        false;
                    Info ->
                        %% Don't kill system processes
                        not is_system_process(Info) andalso
                        %% Don't kill supervisors
                        not is_supervisor(Info) andalso
                        %% Don't kill protected app processes
                        not is_protected_app_process(Pid, ProtectedApps) andalso
                        %% Don't kill ourselves
                        Pid =/= self()
                end
        end
    catch
        _:_ -> false
    end.

is_system_process(Info) ->
    case proplists:get_value(registered_name, Info) of
        [] -> false;
        Name when is_atom(Name) ->
            %% Check for common system process names
            lists:member(Name, [
                init, erl_prim_loader, error_logger, logger,
                application_controller, kernel_sup, code_server,
                global_name_server, inet_db, file_server_2,
                user, standard_error, standard_error_sup
            ])
    end.

is_supervisor(Info) ->
    case proplists:get_value(initial_call, Info) of
        {supervisor, _, _} -> true;
        _ ->
            %% Check dictionary for supervisor behavior
            Dict = proplists:get_value(dictionary, Info, []),
            lists:keymember('$ancestors', 1, Dict) andalso
            proplists:get_value('$initial_call', Dict) =:= {supervisor, kernel, 1}
    end.

is_protected_app_process(Pid, ProtectedApps) ->
    case application:get_application(Pid) of
        {ok, App} -> lists:member(App, ProtectedApps);
        undefined -> false
    end.

cpu_burn_loop(EndTime) ->
    case erlang:system_time(millisecond) >= EndTime of
        true -> ok;
        false ->
            %% Do useless work
            _ = lists:sum(lists:seq(1, 1000)),
            cpu_burn_loop(EndTime)
    end.

%% Suspend schedulers briefly using dirty scheduler trick
do_suspend_schedulers(DurationMs, _Config) ->
    %% Use erlang:yield() repeatedly to cause brief pauses
    %% This is a cooperative pause that doesn't truly suspend but creates
    %% scheduling pressure
    NumSchedulers = erlang:system_info(schedulers),

    %% Spawn processes on each scheduler to create busy-wait pause
    Pids = lists:map(fun(SchedId) ->
        spawn_opt(fun() ->
            EndTime = erlang:system_time(millisecond) + DurationMs,
            scheduler_pause_loop(EndTime)
        end, [{scheduler, SchedId}])
    end, lists:seq(1, NumSchedulers)),

    %% Wait for all pause processes to complete
    lists:foreach(fun(Pid) ->
        Ref = monitor(process, Pid),
        receive
            {'DOWN', Ref, process, Pid, _} -> ok
        after DurationMs + 100 ->
            exit(Pid, kill),
            ok
        end
    end, Pids).

scheduler_pause_loop(EndTime) ->
    case erlang:system_time(millisecond) >= EndTime of
        true -> ok;
        false ->
            %% Busy wait - consumes scheduler time
            erlang:yield(),
            scheduler_pause_loop(EndTime)
    end.

should_delay_module(undefined, _Config) ->
    true;
should_delay_module(Module, #delay_config{target_modules = [], exclude_modules = Exclude}) ->
    not lists:member(Module, Exclude);
should_delay_module(Module, #delay_config{target_modules = Targets, exclude_modules = Exclude}) ->
    lists:member(Module, Targets) andalso not lists:member(Module, Exclude).

shuffle_list(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%====================================================================
%% Configuration helpers
%%====================================================================

init_delay_config(Opts) when is_map(Opts) ->
    map_to_delay_config(Opts);
init_delay_config(Config) when is_record(Config, delay_config) ->
    Config.

init_kill_config(Opts) when is_map(Opts) ->
    map_to_kill_config(Opts);
init_kill_config(Config) when is_record(Config, kill_config) ->
    Config.

init_partition_config(Opts) when is_map(Opts) ->
    map_to_partition_config(Opts);
init_partition_config(Config) when is_record(Config, partition_config) ->
    Config.

init_gc_config(Opts) when is_map(Opts) ->
    map_to_gc_config(Opts);
init_gc_config(Config) when is_record(Config, gc_config) ->
    Config.

init_scheduler_config(Opts) when is_map(Opts) ->
    map_to_scheduler_config(Opts);
init_scheduler_config(Config) when is_record(Config, scheduler_config) ->
    Config.

map_to_delay_config(Map) ->
    #delay_config{
        min_delay_ms = maps:get(min_delay_ms, Map, 10),
        max_delay_ms = maps:get(max_delay_ms, Map, 500),
        target_modules = maps:get(target_modules, Map, []),
        exclude_modules = maps:get(exclude_modules, Map, [?MODULE])
    }.

map_to_kill_config(Map) ->
    #kill_config{
        max_kills_per_tick = maps:get(max_kills_per_tick, Map, 1),
        kill_signal = maps:get(kill_signal, Map, chaos_kill),
        respect_links = maps:get(respect_links, Map, true)
    }.

map_to_partition_config(Map) ->
    #partition_config{
        duration_ms = maps:get(duration_ms, Map, 5000),
        auto_heal = maps:get(auto_heal, Map, true),
        block_mode = maps:get(block_mode, Map, disconnect)
    }.

map_to_gc_config(Map) ->
    #gc_config{
        max_processes_per_tick = maps:get(max_processes_per_tick, Map, 10),
        target_heap_size = maps:get(target_heap_size, Map, undefined),
        aggressive = maps:get(aggressive, Map, false)
    }.

map_to_scheduler_config(Map) ->
    #scheduler_config{
        min_suspend_ms = maps:get(min_suspend_ms, Map, 1),
        max_suspend_ms = maps:get(max_suspend_ms, Map, 100),
        affect_dirty_schedulers = maps:get(affect_dirty_schedulers, Map, false)
    }.

delay_config_to_map(#delay_config{} = C) ->
    #{
        min_delay_ms => C#delay_config.min_delay_ms,
        max_delay_ms => C#delay_config.max_delay_ms,
        target_modules => C#delay_config.target_modules,
        exclude_modules => C#delay_config.exclude_modules
    }.

kill_config_to_map(#kill_config{} = C) ->
    #{
        max_kills_per_tick => C#kill_config.max_kills_per_tick,
        kill_signal => C#kill_config.kill_signal,
        respect_links => C#kill_config.respect_links
    }.

partition_config_to_map(#partition_config{} = C) ->
    #{
        duration_ms => C#partition_config.duration_ms,
        auto_heal => C#partition_config.auto_heal,
        block_mode => C#partition_config.block_mode
    }.

gc_config_to_map(#gc_config{} = C) ->
    #{
        max_processes_per_tick => C#gc_config.max_processes_per_tick,
        target_heap_size => C#gc_config.target_heap_size,
        aggressive => C#gc_config.aggressive
    }.

scheduler_config_to_map(#scheduler_config{} = C) ->
    #{
        min_suspend_ms => C#scheduler_config.min_suspend_ms,
        max_suspend_ms => C#scheduler_config.max_suspend_ms,
        affect_dirty_schedulers => C#scheduler_config.affect_dirty_schedulers
    }.

%%====================================================================
%% Helper functions for code that wants to check chaos state
%%====================================================================

-spec should_delay_io() -> boolean().
should_delay_io() ->
    get(chaos_slow_disk) =:= true.

-spec maybe_delay_io(pos_integer()) -> ok.
maybe_delay_io(MaxDelayMs) ->
    case should_delay_io() of
        true -> timer:sleep(rand:uniform(MaxDelayMs));
        false -> ok
    end.
