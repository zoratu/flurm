%%%-------------------------------------------------------------------
%%% @doc FLURM Power Management
%%%
%%% Implements SLURM-compatible power saving features for cluster
%%% energy efficiency.
%%%
%%% Power states:
%%% - up: Node is powered on and available
%%% - down: Node is powered off
%%% - powering_up: Node is booting
%%% - powering_down: Node is shutting down
%%% - resume: Node is waking from suspend
%%% - suspend: Node is in low-power state
%%%
%%% Features:
%%% - Automatic node suspend/resume based on load
%%% - Configurable idle timeouts
%%% - Wake-on-LAN support
%%% - IPMI power control
%%% - Cloud instance start/stop
%%% - Power budgeting and capping
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    suspend_node/1,
    resume_node/1,
    power_off_node/1,
    power_on_node/1,
    get_node_power_state/1,
    set_power_policy/2,
    get_power_policy/1,
    get_power_stats/0,
    set_idle_timeout/1,
    get_idle_timeout/0,
    set_resume_timeout/1,
    check_idle_nodes/0,
    set_power_budget/1,
    get_power_budget/0,
    get_current_power_usage/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(POWER_TABLE, flurm_power_state).
-define(DEFAULT_IDLE_TIMEOUT, 300).      % 5 minutes
-define(DEFAULT_RESUME_TIMEOUT, 120).    % 2 minutes
-define(DEFAULT_POWER_CHECK_INTERVAL, 60000).  % 1 minute

-type power_state() :: up | down | powering_up | powering_down |
                       resume | suspend.
-type power_method() :: wake_on_lan | ipmi | cloud | script.

-record(node_power, {
    name :: binary(),
    state :: power_state(),
    method :: power_method(),
    last_state_change :: non_neg_integer(),
    last_job_end :: non_neg_integer() | undefined,
    suspend_count :: non_neg_integer(),
    resume_count :: non_neg_integer(),
    power_watts :: non_neg_integer() | undefined,
    mac_address :: binary() | undefined,
    ipmi_address :: binary() | undefined
}).

-record(state, {
    idle_timeout :: non_neg_integer(),
    resume_timeout :: non_neg_integer(),
    power_budget :: non_neg_integer() | undefined,  % Watts
    power_check_timer :: reference(),
    enabled :: boolean()
}).

-export_type([power_state/0, power_method/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Suspend a node (low-power state, quick resume)
-spec suspend_node(binary()) -> ok | {error, term()}.
suspend_node(NodeName) ->
    gen_server:call(?SERVER, {suspend, NodeName}).

%% @doc Resume a suspended node
-spec resume_node(binary()) -> ok | {error, term()}.
resume_node(NodeName) ->
    gen_server:call(?SERVER, {resume, NodeName}).

%% @doc Power off a node completely
-spec power_off_node(binary()) -> ok | {error, term()}.
power_off_node(NodeName) ->
    gen_server:call(?SERVER, {power_off, NodeName}).

%% @doc Power on a node
-spec power_on_node(binary()) -> ok | {error, term()}.
power_on_node(NodeName) ->
    gen_server:call(?SERVER, {power_on, NodeName}).

%% @doc Get power state of a node
-spec get_node_power_state(binary()) -> {ok, power_state()} | {error, not_found}.
get_node_power_state(NodeName) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{state = State}] -> {ok, State};
        [] -> {error, not_found}
    end.

%% @doc Set power policy for a node
-spec set_power_policy(binary(), map()) -> ok.
set_power_policy(NodeName, Policy) ->
    gen_server:call(?SERVER, {set_policy, NodeName, Policy}).

%% @doc Get power policy for a node
-spec get_power_policy(binary()) -> {ok, map()} | {error, not_found}.
get_power_policy(NodeName) ->
    gen_server:call(?SERVER, {get_policy, NodeName}).

%% @doc Get power statistics
-spec get_power_stats() -> map().
get_power_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Set the idle timeout before suspending nodes
-spec set_idle_timeout(non_neg_integer()) -> ok.
set_idle_timeout(Seconds) ->
    gen_server:call(?SERVER, {set_idle_timeout, Seconds}).

%% @doc Get current idle timeout
-spec get_idle_timeout() -> non_neg_integer().
get_idle_timeout() ->
    gen_server:call(?SERVER, get_idle_timeout).

%% @doc Set resume timeout (max time to wait for node to come up)
-spec set_resume_timeout(non_neg_integer()) -> ok.
set_resume_timeout(Seconds) ->
    gen_server:call(?SERVER, {set_resume_timeout, Seconds}).

%% @doc Check for idle nodes and suspend them if needed
-spec check_idle_nodes() -> non_neg_integer().
check_idle_nodes() ->
    gen_server:call(?SERVER, check_idle).

%% @doc Set cluster power budget in watts
-spec set_power_budget(non_neg_integer()) -> ok.
set_power_budget(Watts) ->
    gen_server:call(?SERVER, {set_budget, Watts}).

%% @doc Get power budget
-spec get_power_budget() -> non_neg_integer() | undefined.
get_power_budget() ->
    gen_server:call(?SERVER, get_budget).

%% @doc Get current estimated power usage
-spec get_current_power_usage() -> non_neg_integer().
get_current_power_usage() ->
    gen_server:call(?SERVER, get_power_usage).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?POWER_TABLE, [
        named_table, public, set,
        {keypos, #node_power.name}
    ]),

    %% Start power check timer
    Timer = erlang:send_after(?DEFAULT_POWER_CHECK_INTERVAL, self(), check_power),

    {ok, #state{
        idle_timeout = ?DEFAULT_IDLE_TIMEOUT,
        resume_timeout = ?DEFAULT_RESUME_TIMEOUT,
        power_budget = undefined,
        power_check_timer = Timer,
        enabled = true
    }}.

handle_call({suspend, NodeName}, _From, State) ->
    Result = do_suspend(NodeName),
    {reply, Result, State};

handle_call({resume, NodeName}, _From, State) ->
    Result = do_resume(NodeName, State#state.resume_timeout),
    {reply, Result, State};

handle_call({power_off, NodeName}, _From, State) ->
    Result = do_power_off(NodeName),
    {reply, Result, State};

handle_call({power_on, NodeName}, _From, State) ->
    Result = do_power_on(NodeName, State#state.resume_timeout),
    {reply, Result, State};

handle_call({set_policy, NodeName, Policy}, _From, State) ->
    do_set_policy(NodeName, Policy),
    {reply, ok, State};

handle_call({get_policy, NodeName}, _From, State) ->
    Result = do_get_policy(NodeName),
    {reply, Result, State};

handle_call(get_stats, _From, State) ->
    Stats = compute_stats(),
    {reply, Stats, State};

handle_call({set_idle_timeout, Seconds}, _From, State) ->
    {reply, ok, State#state{idle_timeout = Seconds}};

handle_call(get_idle_timeout, _From, State) ->
    {reply, State#state.idle_timeout, State};

handle_call({set_resume_timeout, Seconds}, _From, State) ->
    {reply, ok, State#state{resume_timeout = Seconds}};

handle_call(check_idle, _From, State) ->
    Count = do_check_idle_nodes(State#state.idle_timeout),
    {reply, Count, State};

handle_call({set_budget, Watts}, _From, State) ->
    {reply, ok, State#state{power_budget = Watts}};

handle_call(get_budget, _From, State) ->
    {reply, State#state.power_budget, State};

handle_call(get_power_usage, _From, State) ->
    Usage = calculate_power_usage(),
    {reply, Usage, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_power, State) ->
    case State#state.enabled of
        true ->
            do_check_idle_nodes(State#state.idle_timeout),
            check_power_budget(State#state.power_budget),
            check_pending_transitions();
        false ->
            ok
    end,
    Timer = erlang:send_after(?DEFAULT_POWER_CHECK_INTERVAL, self(), check_power),
    {noreply, State#state{power_check_timer = Timer}};

handle_info({power_transition_complete, NodeName, NewState}, State) ->
    complete_transition(NodeName, NewState),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

do_suspend(NodeName) ->
    case get_or_create_power_state(NodeName) of
        #node_power{state = up} = Power ->
            %% Check if node has running jobs
            case node_has_jobs(NodeName) of
                true ->
                    {error, node_has_running_jobs};
                false ->
                    execute_suspend(Power)
            end;
        #node_power{state = State} ->
            {error, {invalid_state, State}}
    end.

do_resume(NodeName, Timeout) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{state = suspend} = Power] ->
            execute_resume(Power, Timeout);
        [#node_power{state = down} = Power] ->
            execute_power_on(Power, Timeout);
        [#node_power{state = State}] ->
            {error, {invalid_state, State}};
        [] ->
            {error, not_found}
    end.

do_power_off(NodeName) ->
    case get_or_create_power_state(NodeName) of
        #node_power{state = up} = Power ->
            case node_has_jobs(NodeName) of
                true ->
                    {error, node_has_running_jobs};
                false ->
                    execute_power_off(Power)
            end;
        #node_power{state = suspend} = Power ->
            execute_power_off(Power);
        #node_power{state = State} ->
            {error, {invalid_state, State}}
    end.

do_power_on(NodeName, Timeout) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{state = down} = Power] ->
            execute_power_on(Power, Timeout);
        [#node_power{state = State}] ->
            {error, {invalid_state, State}};
        [] ->
            %% Node not in power table, assume it's down
            Power = #node_power{name = NodeName, state = down, method = script},
            ets:insert(?POWER_TABLE, Power),
            execute_power_on(Power, Timeout)
    end.

execute_suspend(#node_power{name = Name, method = Method} = Power) ->
    Now = erlang:system_time(second),

    %% Update state to powering_down
    UpdatedPower = Power#node_power{
        state = powering_down,
        last_state_change = Now,
        suspend_count = Power#node_power.suspend_count + 1
    },
    ets:insert(?POWER_TABLE, UpdatedPower),

    %% Execute suspend command
    case execute_power_command(Method, suspend, Name) of
        ok ->
            %% Schedule completion check
            erlang:send_after(5000, self(), {power_transition_complete, Name, suspend}),
            ok;
        Error ->
            %% Rollback state
            ets:insert(?POWER_TABLE, Power),
            Error
    end.

execute_resume(#node_power{name = Name, method = Method} = Power, Timeout) ->
    Now = erlang:system_time(second),

    UpdatedPower = Power#node_power{
        state = resume,
        last_state_change = Now,
        resume_count = Power#node_power.resume_count + 1
    },
    ets:insert(?POWER_TABLE, UpdatedPower),

    case execute_power_command(Method, resume, Name) of
        ok ->
            %% Schedule timeout check
            erlang:send_after(Timeout * 1000, self(),
                {power_timeout, Name, resume}),
            ok;
        Error ->
            ets:insert(?POWER_TABLE, Power),
            Error
    end.

execute_power_off(#node_power{name = Name, method = Method} = Power) ->
    Now = erlang:system_time(second),

    UpdatedPower = Power#node_power{
        state = powering_down,
        last_state_change = Now
    },
    ets:insert(?POWER_TABLE, UpdatedPower),

    case execute_power_command(Method, power_off, Name) of
        ok ->
            erlang:send_after(10000, self(), {power_transition_complete, Name, down}),
            ok;
        Error ->
            ets:insert(?POWER_TABLE, Power),
            Error
    end.

execute_power_on(#node_power{name = Name, method = Method} = Power, Timeout) ->
    Now = erlang:system_time(second),

    UpdatedPower = Power#node_power{
        state = powering_up,
        last_state_change = Now
    },
    ets:insert(?POWER_TABLE, UpdatedPower),

    case execute_power_command(Method, power_on, Name) of
        ok ->
            erlang:send_after(Timeout * 1000, self(),
                {power_timeout, Name, powering_up}),
            ok;
        Error ->
            ets:insert(?POWER_TABLE, Power),
            Error
    end.

execute_power_command(wake_on_lan, power_on, NodeName) ->
    case get_mac_address(NodeName) of
        {ok, MAC} ->
            send_wake_on_lan(MAC);
        Error ->
            Error
    end;
execute_power_command(ipmi, Action, NodeName) ->
    case get_ipmi_address(NodeName) of
        {ok, Address} ->
            execute_ipmi_command(Address, Action);
        Error ->
            Error
    end;
execute_power_command(cloud, Action, NodeName) ->
    execute_cloud_command(NodeName, Action);
execute_power_command(script, Action, NodeName) ->
    execute_script_command(NodeName, Action);
execute_power_command(_, _, _) ->
    {error, unsupported_method}.

get_mac_address(NodeName) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{mac_address = MAC}] when MAC =/= undefined ->
            {ok, MAC};
        _ ->
            {error, no_mac_address}
    end.

get_ipmi_address(NodeName) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{ipmi_address = Addr}] when Addr =/= undefined ->
            {ok, Addr};
        _ ->
            {error, no_ipmi_address}
    end.

send_wake_on_lan(MAC) ->
    %% Build magic packet
    MagicPacket = build_wol_packet(MAC),
    %% Send UDP broadcast on port 9
    case gen_udp:open(0, [binary, {broadcast, true}]) of
        {ok, Socket} ->
            Result = gen_udp:send(Socket, {255,255,255,255}, 9, MagicPacket),
            gen_udp:close(Socket),
            Result;
        Error ->
            Error
    end.

build_wol_packet(MACBinary) when is_binary(MACBinary) ->
    %% MAC should be 6 bytes
    %% Magic packet: 6 bytes of 0xFF followed by MAC repeated 16 times
    Header = binary:copy(<<255>>, 6),
    Body = binary:copy(MACBinary, 16),
    <<Header/binary, Body/binary>>.

execute_ipmi_command(Address, Action) ->
    Command = case Action of
        power_on -> "chassis power on";
        power_off -> "chassis power off";
        suspend -> "chassis power soft";
        resume -> "chassis power on"
    end,
    %% Execute ipmitool command
    FullCommand = io_lib:format("ipmitool -H ~s -U admin -P admin ~s",
                                [Address, Command]),
    case os:cmd(lists:flatten(FullCommand)) of
        "Chassis Power Control: " ++ _ -> ok;
        _ -> {error, ipmi_command_failed}
    end.

execute_cloud_command(NodeName, Action) ->
    %% Delegate to cloud scaling module
    case Action of
        power_on -> catch flurm_cloud_scaling:start_instance(NodeName);
        power_off -> catch flurm_cloud_scaling:stop_instance(NodeName);
        suspend -> catch flurm_cloud_scaling:stop_instance(NodeName);
        resume -> catch flurm_cloud_scaling:start_instance(NodeName)
    end,
    ok.

execute_script_command(NodeName, Action) ->
    Script = case Action of
        power_on -> "/etc/flurm/power/power_on.sh";
        power_off -> "/etc/flurm/power/power_off.sh";
        suspend -> "/etc/flurm/power/suspend.sh";
        resume -> "/etc/flurm/power/resume.sh"
    end,
    case filelib:is_file(Script) of
        true ->
            os:cmd(io_lib:format("~s ~s", [Script, NodeName])),
            ok;
        false ->
            {error, {script_not_found, Script}}
    end.

complete_transition(NodeName, NewState) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [Power] ->
            ets:insert(?POWER_TABLE, Power#node_power{
                state = NewState,
                last_state_change = erlang:system_time(second)
            }),
            %% Notify scheduler
            case NewState of
                up -> catch flurm_scheduler:node_up(NodeName);
                down -> catch flurm_scheduler:node_down(NodeName);
                suspend -> catch flurm_scheduler:node_suspended(NodeName);
                _ -> ok
            end;
        [] ->
            ok
    end.

check_pending_transitions() ->
    %% Check for stuck transitions
    Now = erlang:system_time(second),
    TransitionStates = [powering_up, powering_down, resume],

    AllNodes = ets:tab2list(?POWER_TABLE),
    lists:foreach(fun(#node_power{name = Name, state = State,
                                   last_state_change = Changed}) ->
        case lists:member(State, TransitionStates) of
            true ->
                TimeSinceChange = Now - Changed,
                case TimeSinceChange > 300 of  % 5 minute timeout
                    true ->
                        error_logger:warning_msg(
                            "Power transition timeout for node ~s in state ~p~n",
                            [Name, State]),
                        %% Force to down state
                        complete_transition(Name, down);
                    false ->
                        ok
                end;
            false ->
                ok
        end
    end, AllNodes).

get_or_create_power_state(NodeName) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [Power] ->
            Power;
        [] ->
            Power = #node_power{
                name = NodeName,
                state = up,
                method = script,
                last_state_change = erlang:system_time(second),
                suspend_count = 0,
                resume_count = 0
            },
            ets:insert(?POWER_TABLE, Power),
            Power
    end.

node_has_jobs(NodeName) ->
    case catch flurm_node_registry:get_node(NodeName) of
        {ok, #node_entry{}} ->
            case catch flurm_node:get_running_jobs(NodeName) of
                {ok, Jobs} -> length(Jobs) > 0;
                _ -> false
            end;
        _ ->
            false
    end.

do_check_idle_nodes(IdleTimeout) ->
    Now = erlang:system_time(second),
    AllNodes = ets:tab2list(?POWER_TABLE),

    SuspendCount = lists:foldl(fun(#node_power{name = Name, state = up,
                                                last_job_end = LastJob}, Acc) ->
        case LastJob of
            undefined ->
                Acc;
            Time when Now - Time > IdleTimeout ->
                case do_suspend(Name) of
                    ok -> Acc + 1;
                    _ -> Acc
                end;
            _ ->
                Acc
        end
    end, 0, AllNodes),
    SuspendCount.

check_power_budget(undefined) ->
    ok;
check_power_budget(Budget) ->
    CurrentUsage = calculate_power_usage(),
    case CurrentUsage > Budget of
        true ->
            %% Need to reduce power usage
            %% Find idle nodes to suspend
            IdleNodes = find_idle_nodes(),
            suspend_nodes_for_budget(IdleNodes, CurrentUsage - Budget);
        false ->
            ok
    end.

calculate_power_usage() ->
    AllNodes = ets:tab2list(?POWER_TABLE),
    lists:foldl(fun(#node_power{state = State, power_watts = Watts}, Acc) ->
        case State of
            up when Watts =/= undefined -> Acc + Watts;
            up -> Acc + 500;  % Default 500W estimate
            _ -> Acc
        end
    end, 0, AllNodes).

find_idle_nodes() ->
    AllNodes = ets:tab2list(?POWER_TABLE),
    lists:filter(fun(#node_power{name = Name, state = up}) ->
        not node_has_jobs(Name);
    (_) ->
        false
    end, AllNodes).

suspend_nodes_for_budget([], _NeededReduction) ->
    ok;
suspend_nodes_for_budget(_Nodes, NeededReduction) when NeededReduction =< 0 ->
    ok;
suspend_nodes_for_budget([#node_power{name = Name, power_watts = Watts} | Rest],
                         NeededReduction) ->
    case do_suspend(Name) of
        ok ->
            NodePower = case Watts of
                undefined -> 500;
                W -> W
            end,
            suspend_nodes_for_budget(Rest, NeededReduction - NodePower);
        _ ->
            suspend_nodes_for_budget(Rest, NeededReduction)
    end.

do_set_policy(NodeName, Policy) ->
    Power = get_or_create_power_state(NodeName),
    UpdatedPower = maps:fold(fun(Key, Value, Acc) ->
        case Key of
            method -> Acc#node_power{method = Value};
            mac_address -> Acc#node_power{mac_address = Value};
            ipmi_address -> Acc#node_power{ipmi_address = Value};
            power_watts -> Acc#node_power{power_watts = Value};
            _ -> Acc
        end
    end, Power, Policy),
    ets:insert(?POWER_TABLE, UpdatedPower).

do_get_policy(NodeName) ->
    case ets:lookup(?POWER_TABLE, NodeName) of
        [#node_power{method = Method, mac_address = MAC,
                     ipmi_address = IPMI, power_watts = Watts}] ->
            {ok, #{
                method => Method,
                mac_address => MAC,
                ipmi_address => IPMI,
                power_watts => Watts
            }};
        [] ->
            {error, not_found}
    end.

compute_stats() ->
    AllNodes = ets:tab2list(?POWER_TABLE),

    %% Count nodes by state
    StateCounts = lists:foldl(fun(#node_power{state = State}, Acc) ->
        maps:update_with(State, fun(V) -> V + 1 end, 1, Acc)
    end, #{}, AllNodes),

    %% Total suspend/resume counts
    {TotalSuspends, TotalResumes} = lists:foldl(
        fun(#node_power{suspend_count = S, resume_count = R}, {AccS, AccR}) ->
            {AccS + S, AccR + R}
        end, {0, 0}, AllNodes),

    #{
        nodes_by_state => StateCounts,
        total_nodes => length(AllNodes),
        current_power_watts => calculate_power_usage(),
        total_suspends => TotalSuspends,
        total_resumes => TotalResumes
    }.
