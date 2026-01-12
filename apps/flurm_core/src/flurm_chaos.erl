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
%%% - Inject message delays
%%% - Drop messages probabilistically
%%% - Force garbage collection
%%% - Network partition simulation
%%% - Clock skew injection
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

%% Helper functions for chaos-aware code
-export([should_delay_io/0, maybe_delay_io/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TICK_MS, 1000).

-record(state, {
    enabled = false :: boolean(),
    scenarios = #{} :: #{atom() => float()},
    tick_ref :: reference() | undefined,
    tick_ms :: pos_integer(),
    stats :: #{atom() => non_neg_integer()},
    protected_apps :: [atom()],
    seed :: rand:state()
}).

-type scenario() :: kill_random_process
                  | delay_message
                  | drop_message
                  | trigger_gc
                  | slow_disk
                  | memory_pressure
                  | cpu_burn
                  | network_partition.

-export_type([scenario/0]).

%%====================================================================
%% Default Scenarios
%%====================================================================

-define(DEFAULT_SCENARIOS, #{
    kill_random_process => 0.001,    % 0.1% chance per tick
    trigger_gc => 0.01,              % 1% chance per tick
    memory_pressure => 0.005,        % 0.5% chance per tick
    cpu_burn => 0.002                % 0.2% chance per tick
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
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Enable chaos injection
-spec enable() -> ok.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable chaos injection
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Check if chaos is enabled
-spec is_enabled() -> boolean().
is_enabled() ->
    gen_server:call(?SERVER, is_enabled).

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
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    TickMs = maps:get(tick_ms, Opts, ?DEFAULT_TICK_MS),
    Scenarios = maps:merge(?DEFAULT_SCENARIOS, maps:get(scenarios, Opts, #{})),
    ProtectedApps = ?PROTECTED_APPS ++ maps:get(protected_apps, Opts, []),
    Seed = rand:seed(exsss, erlang:system_time()),

    State = #state{
        enabled = false,
        scenarios = Scenarios,
        tick_ms = TickMs,
        stats = #{},
        protected_apps = ProtectedApps,
        seed = Seed
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
        stats => State#state.stats,
        tick_ms => State#state.tick_ms,
        protected_apps => State#state.protected_apps
    },
    {reply, Status, State}.

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
    }}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

run_chaos_tick(#state{scenarios = Scenarios, seed = Seed, stats = Stats} = State) ->
    maps:fold(fun(Scenario, Probability, {AccSeed, AccStats}) ->
        {Roll, NewSeed} = rand:uniform_s(AccSeed),
        case Roll < Probability of
            true ->
                execute_scenario(Scenario, State),
                {NewSeed, increment_stat(Scenario, AccStats)};
            false ->
                {NewSeed, AccStats}
        end
    end, {Seed, Stats}, Scenarios).

increment_stat(Scenario, Stats) ->
    maps:update_with(Scenario, fun(V) -> V + 1 end, 1, Stats).

execute_scenario(kill_random_process, State) ->
    case find_killable_process(State#state.protected_apps) of
        {ok, Pid} ->
            logger:warning("[CHAOS] Killing process ~p", [Pid]),
            exit(Pid, chaos_kill),
            ok;
        none ->
            {error, no_killable_process}
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

execute_scenario(delay_message, _State) ->
    %% Note: Actual message delay requires tracing infrastructure
    %% This is a placeholder that can be enhanced with sys:trace
    logger:debug("[CHAOS] Message delay scenario triggered"),
    ok;

execute_scenario(drop_message, _State) ->
    %% Note: Actual message dropping requires tracing infrastructure
    logger:debug("[CHAOS] Message drop scenario triggered"),
    ok;

execute_scenario(network_partition, _State) ->
    %% Simulate network partition by disconnecting from random node
    Nodes = nodes(),
    case Nodes of
        [] ->
            {error, no_nodes_connected};
        _ ->
            Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
            logger:warning("[CHAOS] Simulating partition from ~p", [Node]),
            spawn(fun() ->
                erlang:disconnect_node(Node),
                timer:sleep(5000),
                net_adm:ping(Node)  % Reconnect after 5 seconds
            end),
            ok
    end;

execute_scenario(Unknown, _State) ->
    logger:warning("[CHAOS] Unknown scenario: ~p", [Unknown]),
    {error, unknown_scenario}.

%% Find a process that can be safely killed
find_killable_process(ProtectedApps) ->
    Procs = erlang:processes(),
    Killable = lists:filter(fun(P) ->
        is_killable(P, ProtectedApps)
    end, Procs),
    case Killable of
        [] -> none;
        _ -> {ok, lists:nth(rand:uniform(length(Killable)), Killable)}
    end.

is_killable(Pid, ProtectedApps) ->
    try
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
                application_controller, kernel_sup, code_server
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
