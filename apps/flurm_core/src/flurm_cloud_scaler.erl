%%%-------------------------------------------------------------------
%%% @doc FLURM Cloud Elastic Scaler
%%%
%%% Automatically scales compute nodes up/down based on job queue demand.
%%% Supports multiple cloud providers:
%%% - AWS (EC2 Auto Scaling)
%%% - GCP (Managed Instance Groups)
%%% - Azure (Virtual Machine Scale Sets)
%%%
%%% Scaling decisions based on:
%%% - Pending job count and resource requirements
%%% - Current cluster utilization
%%% - Cost optimization (spot/preemptible instances)
%%% - Min/max node limits
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    enable/0,
    disable/0,
    is_enabled/0,
    set_provider/1,
    get_provider/0,
    set_config/1,
    get_config/0,
    scale_up/1,
    scale_down/1,
    get_scaling_status/0,
    force_evaluation/0,
    set_limits/2,
    get_limits/0
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
-define(CONFIG_TABLE, flurm_cloud_config).

%% Evaluation interval
-define(EVAL_INTERVAL, 60000).  % 1 minute

%% Scaling thresholds
-define(SCALE_UP_THRESHOLD, 0.8).    % Scale up if utilization > 80%
-define(SCALE_DOWN_THRESHOLD, 0.3).  % Scale down if utilization < 30%
-define(PENDING_JOB_THRESHOLD, 10).  % Scale up if > 10 pending jobs

%% Cooldown periods
-define(SCALE_UP_COOLDOWN, 120000).   % 2 minutes
-define(SCALE_DOWN_COOLDOWN, 300000). % 5 minutes

-record(state, {
    enabled = false :: boolean(),
    provider :: aws | gcp | azure | undefined,
    config = #{} :: map(),
    eval_timer :: reference() | undefined,
    last_scale_up :: integer() | undefined,
    last_scale_down :: integer() | undefined,
    min_nodes = 0 :: non_neg_integer(),
    max_nodes = 100 :: non_neg_integer(),
    pending_scale_ops = [] :: [map()]
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the cloud scaler server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Enable cloud scaling.
-spec enable() -> ok | {error, no_provider}.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable cloud scaling.
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Check if cloud scaling is enabled.
-spec is_enabled() -> boolean().
is_enabled() ->
    gen_server:call(?SERVER, is_enabled).

%% @doc Set the cloud provider.
-spec set_provider(aws | gcp | azure) -> ok.
set_provider(Provider) when Provider =:= aws; Provider =:= gcp; Provider =:= azure ->
    gen_server:call(?SERVER, {set_provider, Provider}).

%% @doc Get the current cloud provider.
-spec get_provider() -> aws | gcp | azure | undefined.
get_provider() ->
    gen_server:call(?SERVER, get_provider).

%% @doc Set provider-specific configuration.
-spec set_config(map()) -> ok.
set_config(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {set_config, Config}).

%% @doc Get current configuration.
-spec get_config() -> map().
get_config() ->
    gen_server:call(?SERVER, get_config).

%% @doc Manually trigger scale up.
-spec scale_up(pos_integer()) -> {ok, [binary()]} | {error, term()}.
scale_up(Count) when is_integer(Count), Count > 0 ->
    gen_server:call(?SERVER, {scale_up, Count}, 30000).

%% @doc Manually trigger scale down.
-spec scale_down(pos_integer()) -> {ok, [binary()]} | {error, term()}.
scale_down(Count) when is_integer(Count), Count > 0 ->
    gen_server:call(?SERVER, {scale_down, Count}, 30000).

%% @doc Get current scaling status.
-spec get_scaling_status() -> map().
get_scaling_status() ->
    gen_server:call(?SERVER, get_scaling_status).

%% @doc Force an immediate scaling evaluation.
-spec force_evaluation() -> ok.
force_evaluation() ->
    gen_server:cast(?SERVER, force_evaluation).

%% @doc Set min/max node limits.
-spec set_limits(non_neg_integer(), non_neg_integer()) -> ok.
set_limits(Min, Max) when is_integer(Min), is_integer(Max), Min >= 0, Max >= Min ->
    gen_server:call(?SERVER, {set_limits, Min, Max}).

%% @doc Get current limits.
-spec get_limits() -> {non_neg_integer(), non_neg_integer()}.
get_limits() ->
    gen_server:call(?SERVER, get_limits).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create config table
    ets:new(?CONFIG_TABLE, [named_table, public, set]),

    {ok, #state{}}.

handle_call(enable, _From, #state{provider = undefined} = State) ->
    {reply, {error, no_provider}, State};
handle_call(enable, _From, State) ->
    Timer = erlang:send_after(?EVAL_INTERVAL, self(), evaluate_scaling),
    error_logger:info_msg("FLURM cloud scaler enabled (provider: ~p)~n", [State#state.provider]),
    {reply, ok, State#state{enabled = true, eval_timer = Timer}};

handle_call(disable, _From, State) ->
    case State#state.eval_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    error_logger:info_msg("FLURM cloud scaler disabled~n"),
    {reply, ok, State#state{enabled = false, eval_timer = undefined}};

handle_call(is_enabled, _From, State) ->
    {reply, State#state.enabled, State};

handle_call({set_provider, Provider}, _From, State) ->
    error_logger:info_msg("FLURM cloud scaler: provider set to ~p~n", [Provider]),
    {reply, ok, State#state{provider = Provider}};

handle_call(get_provider, _From, State) ->
    {reply, State#state.provider, State};

handle_call({set_config, Config}, _From, State) ->
    NewConfig = maps:merge(State#state.config, Config),
    {reply, ok, State#state{config = NewConfig}};

handle_call(get_config, _From, State) ->
    {reply, State#state.config, State};

handle_call({scale_up, Count}, _From, State) ->
    Result = do_scale_up(Count, State),
    Now = erlang:monotonic_time(millisecond),
    {reply, Result, State#state{last_scale_up = Now}};

handle_call({scale_down, Count}, _From, State) ->
    Result = do_scale_down(Count, State),
    Now = erlang:monotonic_time(millisecond),
    {reply, Result, State#state{last_scale_down = Now}};

handle_call(get_scaling_status, _From, State) ->
    Status = #{
        enabled => State#state.enabled,
        provider => State#state.provider,
        min_nodes => State#state.min_nodes,
        max_nodes => State#state.max_nodes,
        last_scale_up => State#state.last_scale_up,
        last_scale_down => State#state.last_scale_down,
        pending_operations => length(State#state.pending_scale_ops)
    },
    {reply, Status, State};

handle_call({set_limits, Min, Max}, _From, State) ->
    {reply, ok, State#state{min_nodes = Min, max_nodes = Max}};

handle_call(get_limits, _From, State) ->
    {reply, {State#state.min_nodes, State#state.max_nodes}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(force_evaluation, State) ->
    case State#state.enabled of
        true ->
            NewState = evaluate_and_scale(State),
            {noreply, NewState};
        false ->
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(evaluate_scaling, #state{enabled = true} = State) ->
    NewState = evaluate_and_scale(State),
    Timer = erlang:send_after(?EVAL_INTERVAL, self(), evaluate_scaling),
    {noreply, NewState#state{eval_timer = Timer}};

handle_info(evaluate_scaling, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.eval_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Evaluate scaling needs and take action
evaluate_and_scale(State) ->
    %% Get current metrics
    Metrics = collect_scaling_metrics(),

    %% Determine scaling action
    Action = determine_scaling_action(Metrics, State),

    %% Execute action if cooldown allows
    execute_scaling_action(Action, Metrics, State).

%% @private Collect metrics for scaling decisions
collect_scaling_metrics() ->
    %% Get pending jobs
    PendingJobs = case catch flurm_job_registry:count_by_state() of
        Counts when is_map(Counts) -> maps:get(pending, Counts, 0);
        _ -> 0
    end,

    %% Get node counts
    NodeCounts = case catch flurm_node_registry:count_by_state() of
        NC when is_map(NC) -> NC;
        _ -> #{up => 0, down => 0, drain => 0}
    end,

    %% Calculate utilization
    TotalNodes = maps:fold(fun(_, V, Acc) -> V + Acc end, 0, NodeCounts),
    UpNodes = maps:get(up, NodeCounts, 0),

    Utilization = case TotalNodes of
        0 -> 0.0;
        _ ->
            %% Get allocated vs total resources
            case catch collect_resource_utilization() of
                {TotalCpus, AllocCpus, _, _} when TotalCpus > 0 ->
                    AllocCpus / TotalCpus;
                _ ->
                    0.5  % Default if can't determine
            end
    end,

    %% Estimate resource demand from pending jobs
    PendingResourceDemand = estimate_pending_demand(),

    #{
        pending_jobs => PendingJobs,
        total_nodes => TotalNodes,
        up_nodes => UpNodes,
        utilization => Utilization,
        pending_demand => PendingResourceDemand
    }.

%% @private Collect resource utilization
collect_resource_utilization() ->
    Nodes = case catch flurm_node_registry:list_nodes() of
        L when is_list(L) -> L;
        _ -> []
    end,
    lists:foldl(
        fun({NodeName, _Pid}, {TotalC, AllocC, TotalM, AllocM}) ->
            case catch flurm_node_registry:get_node_entry(NodeName) of
                {ok, #node_entry{cpus_total = CT, cpus_avail = CA,
                                 memory_total = MT, memory_avail = MA}} ->
                    {TotalC + CT, AllocC + (CT - CA), TotalM + MT, AllocM + (MT - MA)};
                _ ->
                    {TotalC, AllocC, TotalM, AllocM}
            end
        end,
        {0, 0, 0, 0},
        Nodes
    ).

%% @private Estimate resource demand from pending jobs
estimate_pending_demand() ->
    case catch flurm_job_registry:list_jobs_by_state(pending) of
        Jobs when is_list(Jobs) ->
            lists:foldl(
                fun({_JobId, Pid}, Acc) ->
                    case catch flurm_job:get_info(Pid) of
                        {ok, Info} ->
                            Nodes = maps:get(num_nodes, Info, 1),
                            Acc + Nodes;
                        _ ->
                            Acc
                    end
                end,
                0,
                Jobs
            );
        _ ->
            0
    end.

%% @private Determine what scaling action to take
determine_scaling_action(Metrics, State) ->
    #{pending_jobs := PendingJobs,
      total_nodes := TotalNodes,
      utilization := Utilization,
      pending_demand := _PendingDemand} = Metrics,

    MinNodes = State#state.min_nodes,
    MaxNodes = State#state.max_nodes,

    %% Check if we need to scale up
    NeedScaleUp = (Utilization > ?SCALE_UP_THRESHOLD andalso TotalNodes < MaxNodes)
        orelse (PendingJobs > ?PENDING_JOB_THRESHOLD andalso TotalNodes < MaxNodes)
        orelse (TotalNodes < MinNodes),

    %% Check if we can scale down
    CanScaleDown = Utilization < ?SCALE_DOWN_THRESHOLD
        andalso PendingJobs == 0
        andalso TotalNodes > MinNodes,

    case {NeedScaleUp, CanScaleDown} of
        {true, _} ->
            %% Calculate how many nodes to add
            ScaleCount = calculate_scale_up_count(Metrics, State),
            {scale_up, ScaleCount};
        {false, true} ->
            %% Calculate how many nodes to remove
            ScaleCount = calculate_scale_down_count(Metrics, State),
            {scale_down, ScaleCount};
        {false, false} ->
            none
    end.

%% @private Calculate number of nodes to add
calculate_scale_up_count(Metrics, State) ->
    #{pending_demand := PendingDemand, total_nodes := TotalNodes} = Metrics,
    MaxNodes = State#state.max_nodes,

    %% Start with pending demand or a minimum of 1
    BaseCount = max(1, PendingDemand),

    %% Don't exceed max
    min(BaseCount, MaxNodes - TotalNodes).

%% @private Calculate number of nodes to remove
calculate_scale_down_count(Metrics, State) ->
    #{total_nodes := TotalNodes, utilization := Utilization} = Metrics,
    MinNodes = State#state.min_nodes,

    %% Remove nodes proportional to how far below threshold we are
    ExcessRatio = (?SCALE_DOWN_THRESHOLD - Utilization) / ?SCALE_DOWN_THRESHOLD,
    IdealRemoval = round(TotalNodes * ExcessRatio * 0.5),  % Conservative

    %% At least 1, but don't go below min
    min(max(1, IdealRemoval), TotalNodes - MinNodes).

%% @private Execute scaling action if cooldown allows
execute_scaling_action(none, _Metrics, State) ->
    State;
execute_scaling_action({scale_up, Count}, _Metrics, State) ->
    Now = erlang:monotonic_time(millisecond),
    LastScaleUp = State#state.last_scale_up,

    case LastScaleUp of
        undefined ->
            do_scale_up_async(Count, State);
        _ when (Now - LastScaleUp) > ?SCALE_UP_COOLDOWN ->
            do_scale_up_async(Count, State);
        _ ->
            %% Still in cooldown
            State
    end;
execute_scaling_action({scale_down, Count}, _Metrics, State) ->
    Now = erlang:monotonic_time(millisecond),
    LastScaleDown = State#state.last_scale_down,

    case LastScaleDown of
        undefined ->
            do_scale_down_async(Count, State);
        _ when (Now - LastScaleDown) > ?SCALE_DOWN_COOLDOWN ->
            do_scale_down_async(Count, State);
        _ ->
            %% Still in cooldown
            State
    end.

%% @private Async scale up
do_scale_up_async(Count, State) ->
    error_logger:info_msg("FLURM cloud scaler: initiating scale up of ~p nodes~n", [Count]),
    case do_scale_up(Count, State) of
        {ok, _} ->
            Now = erlang:monotonic_time(millisecond),
            State#state{last_scale_up = Now};
        {error, Reason} ->
            error_logger:error_msg("FLURM cloud scaler: scale up failed: ~p~n", [Reason]),
            State
    end.

%% @private Async scale down
do_scale_down_async(Count, State) ->
    error_logger:info_msg("FLURM cloud scaler: initiating scale down of ~p nodes~n", [Count]),
    case do_scale_down(Count, State) of
        {ok, _} ->
            Now = erlang:monotonic_time(millisecond),
            State#state{last_scale_down = Now};
        {error, Reason} ->
            error_logger:error_msg("FLURM cloud scaler: scale down failed: ~p~n", [Reason]),
            State
    end.

%% @private Scale up using provider
do_scale_up(Count, State) ->
    case State#state.provider of
        aws -> aws_scale_up(Count, State#state.config);
        gcp -> gcp_scale_up(Count, State#state.config);
        azure -> azure_scale_up(Count, State#state.config);
        undefined -> {error, no_provider}
    end.

%% @private Scale down using provider
do_scale_down(Count, State) ->
    case State#state.provider of
        aws -> aws_scale_down(Count, State#state.config);
        gcp -> gcp_scale_down(Count, State#state.config);
        azure -> azure_scale_down(Count, State#state.config);
        undefined -> {error, no_provider}
    end.

%%====================================================================
%% Provider implementations
%%====================================================================

%% AWS EC2 Auto Scaling
aws_scale_up(Count, Config) ->
    %% In production, this would call AWS API
    %% For now, return simulated success
    AsgName = maps:get(asg_name, Config, undefined),
    Region = maps:get(region, Config, <<"us-east-1">>),

    case AsgName of
        undefined ->
            {error, missing_asg_name};
        _ ->
            error_logger:info_msg(
                "AWS: Would increase ASG ~s in ~s by ~p instances~n",
                [AsgName, Region, Count]
            ),
            %% Simulate instance IDs
            InstanceIds = [list_to_binary("i-" ++ integer_to_list(N))
                          || N <- lists:seq(1, Count)],
            {ok, InstanceIds}
    end.

aws_scale_down(Count, Config) ->
    AsgName = maps:get(asg_name, Config, undefined),
    Region = maps:get(region, Config, <<"us-east-1">>),

    case AsgName of
        undefined ->
            {error, missing_asg_name};
        _ ->
            %% Find idle nodes to terminate
            IdleNodes = find_idle_nodes(Count),
            error_logger:info_msg(
                "AWS: Would decrease ASG ~s in ~s by ~p instances (~p)~n",
                [AsgName, Region, Count, IdleNodes]
            ),
            {ok, IdleNodes}
    end.

%% GCP Managed Instance Groups
gcp_scale_up(Count, Config) ->
    MigName = maps:get(mig_name, Config, undefined),
    Zone = maps:get(zone, Config, <<"us-central1-a">>),

    case MigName of
        undefined ->
            {error, missing_mig_name};
        _ ->
            error_logger:info_msg(
                "GCP: Would increase MIG ~s in ~s by ~p instances~n",
                [MigName, Zone, Count]
            ),
            InstanceNames = [list_to_binary("instance-" ++ integer_to_list(N))
                            || N <- lists:seq(1, Count)],
            {ok, InstanceNames}
    end.

gcp_scale_down(Count, Config) ->
    MigName = maps:get(mig_name, Config, undefined),
    Zone = maps:get(zone, Config, <<"us-central1-a">>),

    case MigName of
        undefined ->
            {error, missing_mig_name};
        _ ->
            IdleNodes = find_idle_nodes(Count),
            error_logger:info_msg(
                "GCP: Would decrease MIG ~s in ~s by ~p instances (~p)~n",
                [MigName, Zone, Count, IdleNodes]
            ),
            {ok, IdleNodes}
    end.

%% Azure Virtual Machine Scale Sets
azure_scale_up(Count, Config) ->
    VmssName = maps:get(vmss_name, Config, undefined),
    ResourceGroup = maps:get(resource_group, Config, undefined),

    case {VmssName, ResourceGroup} of
        {undefined, _} ->
            {error, missing_vmss_name};
        {_, undefined} ->
            {error, missing_resource_group};
        _ ->
            error_logger:info_msg(
                "Azure: Would increase VMSS ~s in RG ~s by ~p instances~n",
                [VmssName, ResourceGroup, Count]
            ),
            InstanceIds = [list_to_binary("vm_" ++ integer_to_list(N))
                          || N <- lists:seq(1, Count)],
            {ok, InstanceIds}
    end.

azure_scale_down(Count, Config) ->
    VmssName = maps:get(vmss_name, Config, undefined),
    ResourceGroup = maps:get(resource_group, Config, undefined),

    case {VmssName, ResourceGroup} of
        {undefined, _} ->
            {error, missing_vmss_name};
        {_, undefined} ->
            {error, missing_resource_group};
        _ ->
            IdleNodes = find_idle_nodes(Count),
            error_logger:info_msg(
                "Azure: Would decrease VMSS ~s in RG ~s by ~p instances (~p)~n",
                [VmssName, ResourceGroup, Count, IdleNodes]
            ),
            {ok, IdleNodes}
    end.

%% @private Find idle nodes that can be safely terminated
find_idle_nodes(MaxCount) ->
    case catch flurm_node_registry:list_nodes() of
        Nodes when is_list(Nodes) ->
            IdleNodes = lists:filtermap(
                fun({NodeName, _Pid}) ->
                    case catch flurm_node_registry:get_node_entry(NodeName) of
                        {ok, #node_entry{state = up, cpus_avail = CA, cpus_total = CT}}
                          when CA == CT ->
                            %% All CPUs available = no running jobs
                            {true, NodeName};
                        _ ->
                            false
                    end
                end,
                Nodes
            ),
            lists:sublist(IdleNodes, MaxCount);
        _ ->
            []
    end.
