%%%-------------------------------------------------------------------
%%% @doc FLURM Power Management
%%%
%%% Implements comprehensive power saving features for cluster
%%% energy efficiency with SLURM-compatible semantics.
%%%
%%% Power states:
%%% - powered_on: Node is powered on and available
%%% - powered_off: Node is powered off
%%% - powering_up: Node is booting
%%% - powering_down: Node is shutting down
%%% - suspended: Node is in low-power state
%%%
%%% Features:
%%% - Automatic node suspend/resume based on load
%%% - Configurable idle timeouts and power policies
%%% - Wake-on-LAN support
%%% - IPMI power control
%%% - Cloud instance start/stop
%%% - Custom script support
%%% - Power budgeting and capping
%%% - Energy tracking per node and job
%%% - Scheduler integration for power-aware scheduling
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% Power management infrastructure API
-export([
    start_link/0,
    enable/0,
    disable/0,
    get_status/0
]).

%% Node power states API
-export([
    set_node_power_state/2,
    get_node_power_state/1,
    get_nodes_by_power_state/1
]).

%% Power control methods API
-export([
    execute_power_command/3,
    configure_node_power/2
]).

%% Power policies API
-export([
    set_policy/1,
    get_policy/0
]).

%% Energy tracking API
-export([
    record_power_usage/2,
    get_node_energy/2,
    get_cluster_energy/1,
    record_job_energy/3,
    get_job_energy/1
]).

%% Scheduler integration API
-export([
    request_node_power_on/1,
    release_node_for_power_off/1,
    get_available_powered_nodes/0,
    notify_job_start/2,
    notify_job_end/1
]).

%% Power capping API
-export([
    set_power_cap/1,
    get_power_cap/0,
    get_current_power/0
]).

%% Legacy API (kept for compatibility)
-export([
    suspend_node/1,
    resume_node/1,
    power_off_node/1,
    power_on_node/1,
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
-define(POWER_STATE_TABLE, flurm_power_state).
-define(ENERGY_TABLE, flurm_energy_metrics).
-define(JOB_ENERGY_TABLE, flurm_job_energy).
-define(NODE_RELEASES_TABLE, flurm_node_releases).
-define(DEFAULT_IDLE_TIMEOUT, 300).      % 5 minutes
-define(DEFAULT_RESUME_TIMEOUT, 120).    % 2 minutes
-define(DEFAULT_POWER_CHECK_INTERVAL, 60000).  % 1 minute
-define(ENERGY_SAMPLE_INTERVAL, 10000).  % 10 seconds

%% Power states
-type power_state() :: powered_on | powered_off | powering_up | powering_down | suspended.
-type power_method() :: ipmi | wake_on_lan | cloud | script.
-type power_policy() :: aggressive | balanced | conservative.

%% Node power state record (stored in ETS)
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
    ipmi_address :: binary() | undefined,
    ipmi_user :: binary() | undefined,
    ipmi_password :: binary() | undefined,
    cloud_instance_id :: binary() | undefined,
    cloud_provider :: atom() | undefined,
    power_script :: binary() | undefined,
    pending_power_on_requests :: non_neg_integer()
}).

%% Energy sample record (stored in ETS)
-record(energy_sample, {
    key :: {binary(), non_neg_integer()},  % {NodeName, Timestamp}
    power_watts :: non_neg_integer(),
    cpu_power :: non_neg_integer() | undefined,
    memory_power :: non_neg_integer() | undefined,
    gpu_power :: non_neg_integer() | undefined
}).

%% Job energy record (stored in ETS)
-record(job_energy, {
    job_id :: pos_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer() | undefined,
    nodes :: [binary()],
    total_energy_wh :: float(),
    samples :: non_neg_integer()
}).

%% Gen server state
-record(state, {
    enabled :: boolean(),
    policy :: power_policy(),
    idle_timeout :: non_neg_integer(),       % seconds before power-down
    resume_timeout :: non_neg_integer(),     % max time to wait for power-on
    min_powered_nodes :: non_neg_integer(),  % minimum nodes to keep powered
    resume_threshold :: non_neg_integer(),   % queue depth triggers power-on
    power_cap :: non_neg_integer() | undefined,  % cluster power cap in watts
    power_check_timer :: reference() | undefined,
    energy_sample_timer :: reference() | undefined,
    pending_power_requests :: #{binary() => {reference(), pid()}}
}).

-export_type([power_state/0, power_method/0, power_policy/0]).

%%====================================================================
%% API - Power Management Infrastructure
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Enable power management
-spec enable() -> ok.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable power management
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Get power management status
-spec get_status() -> map().
get_status() ->
    gen_server:call(?SERVER, get_status).

%%====================================================================
%% API - Node Power States
%%====================================================================

%% @doc Set node power state (on, off, suspend, resume)
-spec set_node_power_state(binary(), atom()) -> ok | {error, term()}.
set_node_power_state(NodeName, TargetState) when is_binary(NodeName) ->
    gen_server:call(?SERVER, {set_node_power_state, NodeName, TargetState}).

%% @doc Get current power state of a node
-spec get_node_power_state(binary()) -> {ok, power_state()} | {error, not_found}.
get_node_power_state(NodeName) ->
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [#node_power{state = State}] -> {ok, State};
        [] -> {error, not_found}
    end.

%% @doc List nodes in a specific power state
-spec get_nodes_by_power_state(power_state()) -> [binary()].
get_nodes_by_power_state(State) ->
    MatchSpec = [{#node_power{name = '$1', state = State, _ = '_'}, [], ['$1']}],
    ets:select(?POWER_STATE_TABLE, MatchSpec).

%%====================================================================
%% API - Power Control Methods
%%====================================================================

%% @doc Execute power command on node
-spec execute_power_command(binary(), atom(), map()) -> ok | {error, term()}.
execute_power_command(NodeName, Command, Options) ->
    gen_server:call(?SERVER, {execute_power_command, NodeName, Command, Options}, 30000).

%% @doc Configure power control method for a node
-spec configure_node_power(binary(), map()) -> ok.
configure_node_power(NodeName, Config) ->
    gen_server:call(?SERVER, {configure_node_power, NodeName, Config}).

%%====================================================================
%% API - Power Policies
%%====================================================================

%% @doc Set cluster power policy
-spec set_policy(power_policy()) -> ok.
set_policy(Policy) when Policy =:= aggressive;
                        Policy =:= balanced;
                        Policy =:= conservative ->
    gen_server:call(?SERVER, {set_policy, Policy}).

%% @doc Get current power policy
-spec get_policy() -> power_policy().
get_policy() ->
    gen_server:call(?SERVER, get_policy).

%%====================================================================
%% API - Energy Tracking
%%====================================================================

%% @doc Record power usage for a node
-spec record_power_usage(binary(), non_neg_integer()) -> ok.
record_power_usage(NodeName, PowerWatts) ->
    gen_server:cast(?SERVER, {record_power_usage, NodeName, PowerWatts}).

%% @doc Get energy usage for a node over time range
-spec get_node_energy(binary(), {non_neg_integer(), non_neg_integer()}) ->
    {ok, float()} | {error, term()}.
get_node_energy(NodeName, {StartTime, EndTime}) ->
    get_energy_for_node(NodeName, StartTime, EndTime).

%% @doc Get total cluster energy usage over time range
-spec get_cluster_energy({non_neg_integer(), non_neg_integer()}) ->
    {ok, float()} | {error, term()}.
get_cluster_energy({StartTime, EndTime}) ->
    get_total_cluster_energy(StartTime, EndTime).

%% @doc Record energy usage for a job
-spec record_job_energy(pos_integer(), [binary()], float()) -> ok.
record_job_energy(JobId, Nodes, EnergyWh) ->
    gen_server:cast(?SERVER, {record_job_energy, JobId, Nodes, EnergyWh}).

%% @doc Get energy usage for a job
-spec get_job_energy(pos_integer()) -> {ok, float()} | {error, not_found}.
get_job_energy(JobId) ->
    case ets:lookup(?JOB_ENERGY_TABLE, JobId) of
        [#job_energy{total_energy_wh = Energy}] -> {ok, Energy};
        [] -> {error, not_found}
    end.

%%====================================================================
%% API - Scheduler Integration
%%====================================================================

%% @doc Request node power-on for scheduling
-spec request_node_power_on(binary()) -> ok | {error, term()}.
request_node_power_on(NodeName) ->
    gen_server:call(?SERVER, {request_power_on, NodeName}).

%% @doc Mark node as available for power-off
-spec release_node_for_power_off(binary()) -> ok.
release_node_for_power_off(NodeName) ->
    gen_server:cast(?SERVER, {release_for_power_off, NodeName}).

%% @doc Get nodes currently powered and available
-spec get_available_powered_nodes() -> [binary()].
get_available_powered_nodes() ->
    MatchSpec = [{#node_power{name = '$1', state = powered_on, _ = '_'}, [], ['$1']}],
    ets:select(?POWER_STATE_TABLE, MatchSpec).

%% @doc Notify power module when a job starts on nodes
-spec notify_job_start(pos_integer(), [binary()]) -> ok.
notify_job_start(JobId, Nodes) ->
    gen_server:cast(?SERVER, {job_start, JobId, Nodes}).

%% @doc Notify power module when a job ends
-spec notify_job_end(pos_integer()) -> ok.
notify_job_end(JobId) ->
    gen_server:cast(?SERVER, {job_end, JobId}).

%%====================================================================
%% API - Power Capping
%%====================================================================

%% @doc Set cluster-wide power cap in watts
-spec set_power_cap(non_neg_integer()) -> ok.
set_power_cap(Watts) when is_integer(Watts), Watts > 0 ->
    gen_server:call(?SERVER, {set_power_cap, Watts}).

%% @doc Get current power cap
-spec get_power_cap() -> non_neg_integer() | undefined.
get_power_cap() ->
    gen_server:call(?SERVER, get_power_cap).

%% @doc Get current cluster power draw
-spec get_current_power() -> non_neg_integer().
get_current_power() ->
    gen_server:call(?SERVER, get_current_power).

%%====================================================================
%% Legacy API (for compatibility)
%%====================================================================

suspend_node(NodeName) ->
    set_node_power_state(NodeName, suspend).

resume_node(NodeName) ->
    set_node_power_state(NodeName, resume).

power_off_node(NodeName) ->
    set_node_power_state(NodeName, off).

power_on_node(NodeName) ->
    set_node_power_state(NodeName, on).

set_power_policy(NodeName, Policy) ->
    configure_node_power(NodeName, Policy).

get_power_policy(NodeName) ->
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
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

get_power_stats() ->
    gen_server:call(?SERVER, get_stats).

set_idle_timeout(Seconds) ->
    gen_server:call(?SERVER, {set_idle_timeout, Seconds}).

get_idle_timeout() ->
    gen_server:call(?SERVER, get_idle_timeout).

set_resume_timeout(Seconds) ->
    gen_server:call(?SERVER, {set_resume_timeout, Seconds}).

check_idle_nodes() ->
    gen_server:call(?SERVER, check_idle).

set_power_budget(Watts) ->
    set_power_cap(Watts).

get_power_budget() ->
    get_power_cap().

get_current_power_usage() ->
    get_current_power().

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?POWER_STATE_TABLE, [
        named_table, public, set,
        {keypos, #node_power.name}
    ]),

    ets:new(?ENERGY_TABLE, [
        named_table, public, ordered_set,
        {keypos, #energy_sample.key}
    ]),

    ets:new(?JOB_ENERGY_TABLE, [
        named_table, public, set,
        {keypos, #job_energy.job_id}
    ]),

    ets:new(?NODE_RELEASES_TABLE, [
        named_table, public, set
    ]),

    %% Load policy settings based on defaults
    PolicySettings = get_policy_settings(balanced),

    %% Start timers
    PowerTimer = erlang:send_after(?DEFAULT_POWER_CHECK_INTERVAL, self(), check_power),
    EnergyTimer = erlang:send_after(?ENERGY_SAMPLE_INTERVAL, self(), sample_energy),

    {ok, #state{
        enabled = true,
        policy = balanced,
        idle_timeout = maps:get(idle_timeout, PolicySettings, ?DEFAULT_IDLE_TIMEOUT),
        resume_timeout = ?DEFAULT_RESUME_TIMEOUT,
        min_powered_nodes = maps:get(min_powered_nodes, PolicySettings, 1),
        resume_threshold = maps:get(resume_threshold, PolicySettings, 5),
        power_cap = undefined,
        power_check_timer = PowerTimer,
        energy_sample_timer = EnergyTimer,
        pending_power_requests = #{}
    }}.

handle_call(enable, _From, State) ->
    {reply, ok, State#state{enabled = true}};

handle_call(disable, _From, State) ->
    {reply, ok, State#state{enabled = false}};

handle_call(get_status, _From, State) ->
    Status = #{
        enabled => State#state.enabled,
        policy => State#state.policy,
        idle_timeout => State#state.idle_timeout,
        resume_timeout => State#state.resume_timeout,
        min_powered_nodes => State#state.min_powered_nodes,
        resume_threshold => State#state.resume_threshold,
        power_cap => State#state.power_cap,
        current_power => calculate_current_power(),
        nodes_powered_on => length(get_nodes_by_power_state(powered_on)),
        nodes_powered_off => length(get_nodes_by_power_state(powered_off)),
        nodes_suspended => length(get_nodes_by_power_state(suspended)),
        nodes_transitioning => length(get_nodes_by_power_state(powering_up)) +
                               length(get_nodes_by_power_state(powering_down))
    },
    {reply, Status, State};

handle_call({set_node_power_state, NodeName, TargetState}, From, State) ->
    case do_set_node_power_state(NodeName, TargetState, From, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {pending, NewState} ->
            %% Will reply async when power-on completes
            {noreply, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({execute_power_command, NodeName, Command, Options}, _From, State) ->
    Result = do_execute_power_command(NodeName, Command, Options),
    {reply, Result, State};

handle_call({configure_node_power, NodeName, Config}, _From, State) ->
    do_configure_node_power(NodeName, Config),
    {reply, ok, State};

handle_call({set_policy, Policy}, _From, State) ->
    PolicySettings = get_policy_settings(Policy),
    NewState = State#state{
        policy = Policy,
        idle_timeout = maps:get(idle_timeout, PolicySettings, State#state.idle_timeout),
        min_powered_nodes = maps:get(min_powered_nodes, PolicySettings, State#state.min_powered_nodes),
        resume_threshold = maps:get(resume_threshold, PolicySettings, State#state.resume_threshold)
    },
    {reply, ok, NewState};

handle_call(get_policy, _From, State) ->
    {reply, State#state.policy, State};

handle_call({set_power_cap, Watts}, _From, State) ->
    {reply, ok, State#state{power_cap = Watts}};

handle_call(get_power_cap, _From, State) ->
    {reply, State#state.power_cap, State};

handle_call(get_current_power, _From, State) ->
    Power = calculate_current_power(),
    {reply, Power, State};

handle_call({request_power_on, NodeName}, _From, State) ->
    case do_request_power_on(NodeName, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(get_stats, _From, State) ->
    Stats = compute_stats(State),
    {reply, Stats, State};

handle_call({set_idle_timeout, Seconds}, _From, State) ->
    {reply, ok, State#state{idle_timeout = Seconds}};

handle_call(get_idle_timeout, _From, State) ->
    {reply, State#state.idle_timeout, State};

handle_call({set_resume_timeout, Seconds}, _From, State) ->
    {reply, ok, State#state{resume_timeout = Seconds}};

handle_call(check_idle, _From, State) ->
    Count = do_check_idle_nodes(State),
    {reply, Count, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({record_power_usage, NodeName, PowerWatts}, State) ->
    Now = erlang:system_time(second),
    Sample = #energy_sample{
        key = {NodeName, Now},
        power_watts = PowerWatts
    },
    ets:insert(?ENERGY_TABLE, Sample),

    %% Update node's current power reading
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [Power] ->
            ets:insert(?POWER_STATE_TABLE, Power#node_power{power_watts = PowerWatts});
        [] ->
            ok
    end,
    {noreply, State};

handle_cast({record_job_energy, JobId, Nodes, EnergyWh}, State) ->
    Now = erlang:system_time(second),
    case ets:lookup(?JOB_ENERGY_TABLE, JobId) of
        [#job_energy{} = Entry] ->
            ets:insert(?JOB_ENERGY_TABLE, Entry#job_energy{
                total_energy_wh = Entry#job_energy.total_energy_wh + EnergyWh,
                samples = Entry#job_energy.samples + 1
            });
        [] ->
            Entry = #job_energy{
                job_id = JobId,
                start_time = Now,
                nodes = Nodes,
                total_energy_wh = EnergyWh,
                samples = 1
            },
            ets:insert(?JOB_ENERGY_TABLE, Entry)
    end,
    {noreply, State};

handle_cast({release_for_power_off, NodeName}, State) ->
    ets:insert(?NODE_RELEASES_TABLE, {NodeName, erlang:system_time(second)}),
    %% Decrement pending power on requests
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [#node_power{pending_power_on_requests = N} = Power] when N > 0 ->
            ets:insert(?POWER_STATE_TABLE, Power#node_power{
                pending_power_on_requests = N - 1
            });
        _ ->
            ok
    end,
    {noreply, State};

handle_cast({job_start, JobId, Nodes}, State) ->
    Now = erlang:system_time(second),
    Entry = #job_energy{
        job_id = JobId,
        start_time = Now,
        nodes = Nodes,
        total_energy_wh = 0.0,
        samples = 0
    },
    ets:insert(?JOB_ENERGY_TABLE, Entry),

    %% Mark nodes as having active jobs
    lists:foreach(fun(NodeName) ->
        case ets:lookup(?POWER_STATE_TABLE, NodeName) of
            [Power] ->
                ets:insert(?POWER_STATE_TABLE, Power#node_power{
                    last_job_end = undefined
                });
            [] ->
                ok
        end
    end, Nodes),
    {noreply, State};

handle_cast({job_end, JobId}, State) ->
    Now = erlang:system_time(second),
    case ets:lookup(?JOB_ENERGY_TABLE, JobId) of
        [#job_energy{nodes = Nodes} = Entry] ->
            ets:insert(?JOB_ENERGY_TABLE, Entry#job_energy{end_time = Now}),

            %% Update last_job_end for nodes
            lists:foreach(fun(NodeName) ->
                case ets:lookup(?POWER_STATE_TABLE, NodeName) of
                    [Power] ->
                        ets:insert(?POWER_STATE_TABLE, Power#node_power{
                            last_job_end = Now
                        });
                    [] ->
                        ok
                end
            end, Nodes);
        [] ->
            ok
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_power, State) ->
    case State#state.enabled of
        true ->
            do_check_idle_nodes(State),
            check_power_cap(State),
            check_pending_transitions(),
            check_queue_for_power_on(State);
        false ->
            ok
    end,
    Timer = erlang:send_after(?DEFAULT_POWER_CHECK_INTERVAL, self(), check_power),
    {noreply, State#state{power_check_timer = Timer}};

handle_info(sample_energy, State) ->
    sample_all_node_energy(),
    update_job_energy(),
    cleanup_old_energy_samples(),
    Timer = erlang:send_after(?ENERGY_SAMPLE_INTERVAL, self(), sample_energy),
    {noreply, State#state{energy_sample_timer = Timer}};

handle_info({power_transition_complete, NodeName, NewState}, State) ->
    complete_transition(NodeName, NewState, State),
    {noreply, State};

handle_info({power_timeout, NodeName, ExpectedState}, State) ->
    handle_power_timeout(NodeName, ExpectedState, State),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions - Power State Management
%%====================================================================

do_set_node_power_state(NodeName, TargetState, From, State) ->
    Power = get_or_create_power_state(NodeName),
    CurrentState = Power#node_power.state,

    case validate_transition(CurrentState, TargetState) of
        ok ->
            execute_transition(NodeName, Power, TargetState, From, State);
        {error, Reason} ->
            {error, Reason}
    end.

validate_transition(Current, Target) ->
    ValidTransitions = #{
        powered_on => [off, suspend],
        powered_off => [on, resume],
        suspended => [resume, off],
        powering_up => [],      % Can't interrupt
        powering_down => []     % Can't interrupt
    },

    %% Normalize target state
    NormalizedTarget = case Target of
        on -> powered_on;
        off -> powered_off;
        suspend -> suspended;
        resume -> powered_on;
        Other -> Other
    end,

    AllowedTargets = maps:get(Current, ValidTransitions, []),
    case lists:member(Target, AllowedTargets) orelse
         lists:member(NormalizedTarget, AllowedTargets) orelse
         Current =:= NormalizedTarget of
        true -> ok;
        false -> {error, {invalid_transition, Current, Target}}
    end.

execute_transition(NodeName, Power, on, _From, State) ->
    execute_power_on(NodeName, Power, State);
execute_transition(NodeName, Power, resume, _From, State) ->
    execute_power_on(NodeName, Power, State);
execute_transition(NodeName, Power, off, _From, State) ->
    execute_power_off(NodeName, Power, State);
execute_transition(NodeName, Power, suspend, _From, State) ->
    execute_suspend(NodeName, Power, State);
execute_transition(_NodeName, _Power, _Target, _From, State) ->
    {error, {unsupported_transition, State}}.

execute_power_on(NodeName, #node_power{state = CurrentState} = Power, State)
  when CurrentState =:= powered_off; CurrentState =:= suspended ->
    Now = erlang:system_time(second),
    Method = Power#node_power.method,

    UpdatedPower = Power#node_power{
        state = powering_up,
        last_state_change = Now,
        resume_count = Power#node_power.resume_count + 1
    },
    ets:insert(?POWER_STATE_TABLE, UpdatedPower),

    case do_execute_power_method(Method, power_on, Power) of
        ok ->
            %% Schedule timeout check
            erlang:send_after(State#state.resume_timeout * 1000, self(),
                {power_timeout, NodeName, powering_up}),
            {ok, State};
        Error ->
            ets:insert(?POWER_STATE_TABLE, Power),
            Error
    end;
execute_power_on(_NodeName, #node_power{state = powered_on}, State) ->
    {ok, State};
execute_power_on(_NodeName, _Power, _State) ->
    {error, invalid_state}.

execute_power_off(NodeName, #node_power{state = CurrentState} = Power, State)
  when CurrentState =:= powered_on; CurrentState =:= suspended ->
    %% Check if node has running jobs
    case node_has_jobs(NodeName) of
        true ->
            {error, node_has_running_jobs};
        false ->
            Now = erlang:system_time(second),
            Method = Power#node_power.method,

            UpdatedPower = Power#node_power{
                state = powering_down,
                last_state_change = Now
            },
            ets:insert(?POWER_STATE_TABLE, UpdatedPower),

            case do_execute_power_method(Method, power_off, Power) of
                ok ->
                    erlang:send_after(30000, self(),
                        {power_transition_complete, NodeName, powered_off}),
                    {ok, State};
                Error ->
                    ets:insert(?POWER_STATE_TABLE, Power),
                    Error
            end
    end;
execute_power_off(_NodeName, _Power, _State) ->
    {error, invalid_state}.

execute_suspend(NodeName, #node_power{state = powered_on} = Power, State) ->
    case node_has_jobs(NodeName) of
        true ->
            {error, node_has_running_jobs};
        false ->
            Now = erlang:system_time(second),
            Method = Power#node_power.method,

            UpdatedPower = Power#node_power{
                state = powering_down,
                last_state_change = Now,
                suspend_count = Power#node_power.suspend_count + 1
            },
            ets:insert(?POWER_STATE_TABLE, UpdatedPower),

            case do_execute_power_method(Method, suspend, Power) of
                ok ->
                    erlang:send_after(10000, self(),
                        {power_transition_complete, NodeName, suspended}),
                    {ok, State};
                Error ->
                    ets:insert(?POWER_STATE_TABLE, Power),
                    Error
            end
    end;
execute_suspend(_NodeName, _Power, _State) ->
    {error, invalid_state}.

%%====================================================================
%% Internal Functions - Power Control Methods
%%====================================================================

do_execute_power_command(NodeName, Command, Options) ->
    Power = get_or_create_power_state(NodeName),
    Method = maps:get(method, Options, Power#node_power.method),
    do_execute_power_method(Method, Command, Power).

do_execute_power_method(ipmi, Command, Power) ->
    execute_ipmi_command(Power, Command);
do_execute_power_method(wake_on_lan, power_on, Power) ->
    send_wake_on_lan(Power#node_power.mac_address);
do_execute_power_method(wake_on_lan, _, _Power) ->
    {error, wake_on_lan_only_supports_power_on};
do_execute_power_method(cloud, Command, Power) ->
    execute_cloud_command(Power, Command);
do_execute_power_method(script, Command, Power) ->
    execute_script_command(Power, Command);
do_execute_power_method(_, _, _) ->
    {error, unsupported_method}.

execute_ipmi_command(#node_power{ipmi_address = undefined}, _Command) ->
    {error, no_ipmi_address};
execute_ipmi_command(#node_power{ipmi_address = Address,
                                  ipmi_user = User,
                                  ipmi_password = Password}, Command) ->
    IpmiCmd = case Command of
        power_on -> "chassis power on";
        power_off -> "chassis power off";
        suspend -> "chassis power soft";
        reset -> "chassis power reset";
        status -> "chassis power status"
    end,

    UserArg = case User of
        undefined -> "admin";
        U -> binary_to_list(U)
    end,
    PassArg = case Password of
        undefined -> "admin";
        P -> binary_to_list(P)
    end,

    FullCommand = io_lib:format("ipmitool -H ~s -U ~s -P ~s ~s",
                                [binary_to_list(Address), UserArg, PassArg, IpmiCmd]),
    case os:cmd(lists:flatten(FullCommand)) of
        "Chassis Power Control: " ++ _ -> ok;
        "Chassis Power is " ++ _ -> ok;
        Output ->
            error_logger:warning_msg("IPMI command output: ~s~n", [Output]),
            {error, {ipmi_command_failed, Output}}
    end.

send_wake_on_lan(undefined) ->
    {error, no_mac_address};
send_wake_on_lan(MAC) when is_binary(MAC) ->
    MACBytes = parse_mac_address(MAC),
    MagicPacket = build_wol_packet(MACBytes),
    case gen_udp:open(0, [binary, {broadcast, true}]) of
        {ok, Socket} ->
            Result = gen_udp:send(Socket, {255,255,255,255}, 9, MagicPacket),
            gen_udp:close(Socket),
            Result;
        Error ->
            Error
    end.

parse_mac_address(MAC) when is_binary(MAC) ->
    %% Parse MAC address in format "AA:BB:CC:DD:EE:FF" or "AA-BB-CC-DD-EE-FF"
    Parts = binary:split(MAC, [<<":">>, <<"-">>], [global]),
    list_to_binary([binary_to_integer(P, 16) || P <- Parts]).

build_wol_packet(MACBytes) when is_binary(MACBytes), byte_size(MACBytes) =:= 6 ->
    Header = binary:copy(<<255>>, 6),
    Body = binary:copy(MACBytes, 16),
    <<Header/binary, Body/binary>>.

execute_cloud_command(#node_power{cloud_provider = undefined}, _Command) ->
    {error, no_cloud_provider};
execute_cloud_command(#node_power{cloud_provider = Provider,
                                   cloud_instance_id = InstanceId}, Command) ->
    case Provider of
        aws ->
            execute_aws_command(InstanceId, Command);
        gcp ->
            execute_gcp_command(InstanceId, Command);
        azure ->
            execute_azure_command(InstanceId, Command);
        _ ->
            %% Try to use flurm_cloud_scaling module
            case Command of
                power_on ->
                    catch flurm_cloud_scaling:start_instance(InstanceId),
                    ok;
                power_off ->
                    catch flurm_cloud_scaling:stop_instance(InstanceId),
                    ok;
                _ ->
                    {error, {unsupported_cloud_command, Command}}
            end
    end.

execute_aws_command(InstanceId, Command) when is_binary(InstanceId) ->
    Action = case Command of
        power_on -> "start-instances";
        power_off -> "stop-instances";
        _ -> undefined
    end,
    case Action of
        undefined ->
            {error, {unsupported_aws_command, Command}};
        _ ->
            Cmd = io_lib:format("aws ec2 ~s --instance-ids ~s",
                               [Action, binary_to_list(InstanceId)]),
            os:cmd(lists:flatten(Cmd)),
            ok
    end.

execute_gcp_command(InstanceId, Command) when is_binary(InstanceId) ->
    Action = case Command of
        power_on -> "start";
        power_off -> "stop";
        _ -> undefined
    end,
    case Action of
        undefined ->
            {error, {unsupported_gcp_command, Command}};
        _ ->
            Cmd = io_lib:format("gcloud compute instances ~s ~s",
                               [Action, binary_to_list(InstanceId)]),
            os:cmd(lists:flatten(Cmd)),
            ok
    end.

execute_azure_command(InstanceId, Command) when is_binary(InstanceId) ->
    Action = case Command of
        power_on -> "start";
        power_off -> "stop";
        _ -> undefined
    end,
    case Action of
        undefined ->
            {error, {unsupported_azure_command, Command}};
        _ ->
            Cmd = io_lib:format("az vm ~s --name ~s",
                               [Action, binary_to_list(InstanceId)]),
            os:cmd(lists:flatten(Cmd)),
            ok
    end.

execute_script_command(#node_power{power_script = undefined, name = Name}, Command) ->
    %% Default script paths
    Script = case Command of
        power_on -> "/etc/flurm/power/power_on.sh";
        power_off -> "/etc/flurm/power/power_off.sh";
        suspend -> "/etc/flurm/power/suspend.sh";
        resume -> "/etc/flurm/power/resume.sh";
        _ -> undefined
    end,
    case Script of
        undefined ->
            {error, {unsupported_script_command, Command}};
        _ ->
            case filelib:is_file(Script) of
                true ->
                    os:cmd(io_lib:format("~s ~s", [Script, binary_to_list(Name)])),
                    ok;
                false ->
                    {error, {script_not_found, Script}}
            end
    end;
execute_script_command(#node_power{power_script = Script, name = Name}, Command) ->
    case filelib:is_file(binary_to_list(Script)) of
        true ->
            Cmd = io_lib:format("~s ~s ~s",
                               [binary_to_list(Script), atom_to_list(Command),
                                binary_to_list(Name)]),
            os:cmd(lists:flatten(Cmd)),
            ok;
        false ->
            {error, {script_not_found, Script}}
    end.

%%====================================================================
%% Internal Functions - Configuration
%%====================================================================

do_configure_node_power(NodeName, Config) ->
    Power = get_or_create_power_state(NodeName),
    UpdatedPower = maps:fold(fun(Key, Value, Acc) ->
        case Key of
            method -> Acc#node_power{method = Value};
            mac_address -> Acc#node_power{mac_address = Value};
            ipmi_address -> Acc#node_power{ipmi_address = Value};
            ipmi_user -> Acc#node_power{ipmi_user = Value};
            ipmi_password -> Acc#node_power{ipmi_password = Value};
            cloud_provider -> Acc#node_power{cloud_provider = Value};
            cloud_instance_id -> Acc#node_power{cloud_instance_id = Value};
            power_script -> Acc#node_power{power_script = Value};
            power_watts -> Acc#node_power{power_watts = Value};
            _ -> Acc
        end
    end, Power, Config),
    ets:insert(?POWER_STATE_TABLE, UpdatedPower).

get_policy_settings(aggressive) ->
    #{
        idle_timeout => 60,           % 1 minute
        min_powered_nodes => 1,
        resume_threshold => 3
    };
get_policy_settings(balanced) ->
    #{
        idle_timeout => 300,          % 5 minutes
        min_powered_nodes => 2,
        resume_threshold => 5
    };
get_policy_settings(conservative) ->
    #{
        idle_timeout => 900,          % 15 minutes
        min_powered_nodes => 5,
        resume_threshold => 10
    }.

%%====================================================================
%% Internal Functions - Idle Node Management
%%====================================================================

do_check_idle_nodes(State) ->
    Now = erlang:system_time(second),
    IdleTimeout = State#state.idle_timeout,
    MinPowered = State#state.min_powered_nodes,

    %% Get all powered-on nodes
    AllPowered = ets:tab2list(?POWER_STATE_TABLE),
    PoweredOn = [P || P = #node_power{state = powered_on} <- AllPowered],

    %% Don't power down if we're at minimum
    case length(PoweredOn) =< MinPowered of
        true ->
            0;
        false ->
            %% Find idle nodes
            IdleNodes = lists:filter(fun(#node_power{name = Name,
                                                      last_job_end = LastJob,
                                                      pending_power_on_requests = Pending}) ->
                %% Don't power down nodes with pending requests
                Pending =:= 0 andalso
                %% Check if released for power-off
                case ets:lookup(?NODE_RELEASES_TABLE, Name) of
                    [{_, _ReleaseTime}] -> true;
                    [] ->
                        %% Check idle timeout
                        case LastJob of
                            undefined -> false;
                            Time -> (Now - Time) > IdleTimeout
                        end
                end andalso
                not node_has_jobs(Name)
            end, PoweredOn),

            %% Power down idle nodes, keeping minimum
            MaxToSuspend = length(PoweredOn) - MinPowered,
            NodesToSuspend = lists:sublist(IdleNodes, MaxToSuspend),

            lists:foldl(fun(#node_power{name = Name}, Count) ->
                case execute_suspend(Name, get_or_create_power_state(Name), State) of
                    {ok, _} ->
                        ets:delete(?NODE_RELEASES_TABLE, Name),
                        Count + 1;
                    _ -> Count
                end
            end, 0, NodesToSuspend)
    end.

check_queue_for_power_on(State) ->
    %% Check if queue depth warrants powering on more nodes
    QueueDepth = get_pending_job_count(),
    ResumeThreshold = State#state.resume_threshold,

    case QueueDepth >= ResumeThreshold of
        true ->
            %% Get suspended/powered-off nodes
            Suspended = get_nodes_by_power_state(suspended),
            PoweredOff = get_nodes_by_power_state(powered_off),
            Available = Suspended ++ PoweredOff,

            case Available of
                [] -> ok;
                [NodeName | _] ->
                    %% Power on one node
                    Power = get_or_create_power_state(NodeName),
                    execute_power_on(NodeName, Power, State)
            end;
        false ->
            ok
    end.

%%====================================================================
%% Internal Functions - Power Capping
%%====================================================================

check_power_cap(#state{power_cap = undefined}) ->
    ok;
check_power_cap(#state{power_cap = Cap} = State) ->
    CurrentPower = calculate_current_power(),
    case CurrentPower > Cap of
        true ->
            %% Need to reduce power
            Excess = CurrentPower - Cap,
            reduce_power_usage(Excess, State);
        false ->
            ok
    end.

reduce_power_usage(ExcessWatts, State) ->
    %% Find idle nodes to suspend
    IdleNodes = find_idle_powered_nodes(),
    suspend_nodes_for_power_reduction(IdleNodes, ExcessWatts, State).

find_idle_powered_nodes() ->
    AllPowered = ets:tab2list(?POWER_STATE_TABLE),
    [P || P = #node_power{state = powered_on, name = Name} <- AllPowered,
          not node_has_jobs(Name)].

suspend_nodes_for_power_reduction([], _Needed, _State) ->
    ok;
suspend_nodes_for_power_reduction(_Nodes, Needed, _State) when Needed =< 0 ->
    ok;
suspend_nodes_for_power_reduction([#node_power{name = Name, power_watts = Watts} | Rest],
                                   Needed, State) ->
    NodePower = case Watts of
        undefined -> 500;  % Default estimate
        W -> W
    end,
    case execute_suspend(Name, get_or_create_power_state(Name), State) of
        {ok, _} ->
            suspend_nodes_for_power_reduction(Rest, Needed - NodePower, State);
        _ ->
            suspend_nodes_for_power_reduction(Rest, Needed, State)
    end.

%%====================================================================
%% Internal Functions - Energy Tracking
%%====================================================================

sample_all_node_energy() ->
    Now = erlang:system_time(second),
    AllNodes = ets:tab2list(?POWER_STATE_TABLE),
    lists:foreach(fun(#node_power{name = Name, state = powered_on, power_watts = Watts}) ->
        %% For powered-on nodes, record current power
        %% In production, this would query actual power sensors
        Power = case Watts of
            undefined -> estimate_node_power(Name);
            W -> W
        end,
        Sample = #energy_sample{
            key = {Name, Now},
            power_watts = Power
        },
        ets:insert(?ENERGY_TABLE, Sample);
    (_) ->
        ok
    end, AllNodes).

estimate_node_power(NodeName) ->
    %% Estimate power based on load
    %% In production, this would use actual sensors or IPMI
    case node_has_jobs(NodeName) of
        true -> 400;   % Estimated active power
        false -> 150   % Estimated idle power
    end.

update_job_energy() ->
    _Now = erlang:system_time(second),
    SampleInterval = ?ENERGY_SAMPLE_INTERVAL div 1000,  % Convert to seconds

    %% Get all active jobs
    AllJobs = ets:tab2list(?JOB_ENERGY_TABLE),
    ActiveJobs = [J || J = #job_energy{end_time = undefined} <- AllJobs],

    lists:foreach(fun(#job_energy{job_id = _JobId, nodes = Nodes} = Entry) ->
        %% Calculate energy for this interval
        TotalPower = lists:foldl(fun(NodeName, Acc) ->
            case ets:lookup(?POWER_STATE_TABLE, NodeName) of
                [#node_power{power_watts = Watts}] when Watts =/= undefined ->
                    Acc + Watts;
                _ ->
                    Acc + estimate_node_power(NodeName)
            end
        end, 0, Nodes),

        %% Convert to Wh (power * time_in_hours)
        EnergyWh = TotalPower * (SampleInterval / 3600),

        ets:insert(?JOB_ENERGY_TABLE, Entry#job_energy{
            total_energy_wh = Entry#job_energy.total_energy_wh + EnergyWh,
            samples = Entry#job_energy.samples + 1
        })
    end, ActiveJobs).

cleanup_old_energy_samples() ->
    %% Keep only last 24 hours of samples
    CutoffTime = erlang:system_time(second) - 86400,
    MatchSpec = [{#energy_sample{key = {'_', '$1'}, _ = '_'},
                  [{'<', '$1', CutoffTime}],
                  [true]}],
    ets:select_delete(?ENERGY_TABLE, MatchSpec).

get_energy_for_node(NodeName, StartTime, EndTime) ->
    %% Sum energy from samples in time range
    MatchSpec = [{#energy_sample{key = {NodeName, '$1'}, power_watts = '$2', _ = '_'},
                  [{'>=', '$1', StartTime}, {'=<', '$1', EndTime}],
                  [{{'$1', '$2'}}]}],
    Samples = ets:select(?ENERGY_TABLE, MatchSpec),

    case Samples of
        [] ->
            {ok, 0.0};
        _ ->
            %% Calculate energy from power samples
            %% Assuming uniform sampling interval
            SampleInterval = ?ENERGY_SAMPLE_INTERVAL div 1000,
            TotalWh = lists:foldl(fun({_Time, Watts}, Acc) ->
                Acc + (Watts * SampleInterval / 3600)
            end, 0.0, Samples),
            {ok, TotalWh}
    end.

get_total_cluster_energy(StartTime, EndTime) ->
    AllNodes = ets:tab2list(?POWER_STATE_TABLE),
    TotalEnergy = lists:foldl(fun(#node_power{name = Name}, Acc) ->
        case get_energy_for_node(Name, StartTime, EndTime) of
            {ok, Energy} -> Acc + Energy;
            _ -> Acc
        end
    end, 0.0, AllNodes),
    {ok, TotalEnergy}.

%%====================================================================
%% Internal Functions - Scheduler Integration
%%====================================================================

do_request_power_on(NodeName, State) ->
    case get_node_power_state(NodeName) of
        {ok, powered_on} ->
            {ok, State};
        {ok, powering_up} ->
            %% Already powering up, increment request count
            case ets:lookup(?POWER_STATE_TABLE, NodeName) of
                [Power] ->
                    ets:insert(?POWER_STATE_TABLE, Power#node_power{
                        pending_power_on_requests = Power#node_power.pending_power_on_requests + 1
                    });
                [] ->
                    ok
            end,
            {ok, State};
        {ok, _OtherState} ->
            %% Power on the node
            Power = get_or_create_power_state(NodeName),
            UpdatedPower = Power#node_power{
                pending_power_on_requests = Power#node_power.pending_power_on_requests + 1
            },
            ets:insert(?POWER_STATE_TABLE, UpdatedPower),
            execute_power_on(NodeName, UpdatedPower, State);
        {error, not_found} ->
            %% Create entry and power on
            Power = #node_power{
                name = NodeName,
                state = powered_off,
                method = script,
                last_state_change = erlang:system_time(second),
                suspend_count = 0,
                resume_count = 0,
                pending_power_on_requests = 1
            },
            ets:insert(?POWER_STATE_TABLE, Power),
            execute_power_on(NodeName, Power, State)
    end.

%%====================================================================
%% Internal Functions - Utilities
%%====================================================================

get_or_create_power_state(NodeName) ->
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [Power] ->
            Power;
        [] ->
            Power = #node_power{
                name = NodeName,
                state = powered_on,
                method = script,
                last_state_change = erlang:system_time(second),
                suspend_count = 0,
                resume_count = 0,
                pending_power_on_requests = 0
            },
            ets:insert(?POWER_STATE_TABLE, Power),
            Power
    end.

node_has_jobs(NodeName) ->
    case catch flurm_node_registry:get_node(NodeName) of
        {ok, #node_entry{}} ->
            case catch flurm_node:list_jobs(NodeName) of
                {ok, Jobs} -> length(Jobs) > 0;
                _ -> false
            end;
        _ ->
            false
    end.

get_pending_job_count() ->
    case catch flurm_scheduler:get_stats() of
        {ok, #{pending_count := Count}} -> Count;
        _ -> 0
    end.

calculate_current_power() ->
    AllNodes = ets:tab2list(?POWER_STATE_TABLE),
    lists:foldl(fun(#node_power{state = State, power_watts = Watts}, Acc) ->
        case State of
            powered_on when Watts =/= undefined -> Acc + Watts;
            powered_on -> Acc + 500;  % Default estimate
            _ -> Acc
        end
    end, 0, AllNodes).

complete_transition(NodeName, NewState, _State) ->
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [Power] ->
            ets:insert(?POWER_STATE_TABLE, Power#node_power{
                state = NewState,
                last_state_change = erlang:system_time(second)
            }),
            %% Notify scheduler
            case NewState of
                powered_on ->
                    catch flurm_scheduler:trigger_schedule();
                _ ->
                    ok
            end;
        [] ->
            ok
    end.

check_pending_transitions() ->
    Now = erlang:system_time(second),
    TransitionStates = [powering_up, powering_down],

    AllNodes = ets:tab2list(?POWER_STATE_TABLE),
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
                        %% Force to appropriate state
                        FinalState = case State of
                            powering_up -> powered_off;
                            powering_down -> powered_off
                        end,
                        complete_transition(Name, FinalState, undefined);
                    false ->
                        ok
                end;
            false ->
                ok
        end
    end, AllNodes).

handle_power_timeout(NodeName, ExpectedState, _State) ->
    case ets:lookup(?POWER_STATE_TABLE, NodeName) of
        [#node_power{state = ExpectedState}] ->
            %% Still in transition state - force completion
            error_logger:warning_msg(
                "Power operation timeout for node ~s in state ~p~n",
                [NodeName, ExpectedState]),
            case ExpectedState of
                powering_up ->
                    complete_transition(NodeName, powered_off, undefined);
                powering_down ->
                    complete_transition(NodeName, powered_off, undefined);
                _ ->
                    ok
            end;
        _ ->
            %% State changed, timeout is irrelevant
            ok
    end.

compute_stats(State) ->
    AllNodes = ets:tab2list(?POWER_STATE_TABLE),

    %% Count nodes by state
    StateCounts = lists:foldl(fun(#node_power{state = NodeState}, Acc) ->
        maps:update_with(NodeState, fun(V) -> V + 1 end, 1, Acc)
    end, #{}, AllNodes),

    %% Total suspend/resume counts
    {TotalSuspends, TotalResumes} = lists:foldl(
        fun(#node_power{suspend_count = S, resume_count = R}, {AccS, AccR}) ->
            {AccS + S, AccR + R}
        end, {0, 0}, AllNodes),

    #{
        enabled => State#state.enabled,
        policy => State#state.policy,
        nodes_by_state => StateCounts,
        total_nodes => length(AllNodes),
        current_power_watts => calculate_current_power(),
        power_cap => State#state.power_cap,
        total_suspends => TotalSuspends,
        total_resumes => TotalResumes,
        idle_timeout => State#state.idle_timeout,
        min_powered_nodes => State#state.min_powered_nodes,
        resume_threshold => State#state.resume_threshold
    }.
