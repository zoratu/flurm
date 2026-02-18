%%%-------------------------------------------------------------------
%%% @doc FLURM Cloud Elastic Scaling
%%%
%%% Provides elastic scaling integration for cloud environments.
%%% Automatically scales compute nodes up/down based on workload.
%%%
%%% Supports:
%%% - AWS EC2 (Auto Scaling Groups, Spot Instances)
%%% - Google Cloud (Managed Instance Groups)
%%% - Azure (Virtual Machine Scale Sets)
%%% - Generic cloud via webhooks
%%%
%%% Features:
%%% - Auto-scaling based on queue depth and idle node thresholds
%%% - Cost tracking with budget limits
%%% - Node lifecycle management (request, terminate, status)
%%% - Cloud provider abstraction layer
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    configure_provider/2,
    get_provider_config/0,
    request_scale_up/2,
    request_scale_down/2,
    get_scaling_status/0,
    set_scaling_policy/1,
    enable/0,
    disable/0,
    is_enabled/0,
    get_stats/0,
    %% Node lifecycle API
    request_nodes/1,
    terminate_nodes/1,
    list_cloud_instances/0,
    get_instance_info/1,
    %% Cost tracking API
    get_cost_summary/0,
    set_budget_limit/1,
    get_budget_status/0
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
-define(SCALE_CHECK_INTERVAL, 60000).  % Check every minute
-define(COOLDOWN_PERIOD, 300000).       % 5 minute cooldown between scaling actions
-define(IDLE_CHECK_INTERVAL, 30000).   % Check idle nodes every 30 seconds
-define(DEFAULT_IDLE_THRESHOLD, 300).  % 5 minutes idle before termination candidate

%% Cloud provider types
-type provider() :: aws | gcp | azure | generic.
-type instance_type() :: binary().

-record(scaling_policy, {
    min_nodes :: non_neg_integer(),
    max_nodes :: non_neg_integer(),
    target_pending_jobs :: non_neg_integer(),   % Scale up if more pending
    target_idle_nodes :: non_neg_integer(),     % Scale down if more idle
    scale_up_increment :: pos_integer(),        % Nodes to add at once
    scale_down_increment :: pos_integer(),      % Nodes to remove at once
    cooldown_seconds :: pos_integer(),          % Time between scaling actions
    instance_types :: [instance_type()],        % Preferred instance types
    spot_enabled :: boolean(),                  % Use spot/preemptible instances
    spot_max_price :: float(),                  % Max spot price ($/hr)
    idle_threshold_seconds :: pos_integer(),    % Time before node considered idle
    queue_depth_threshold :: pos_integer()      % Queue depth to trigger scale up
}).

-record(scaling_action, {
    id :: binary(),
    type :: scale_up | scale_down,
    requested_count :: pos_integer(),
    actual_count :: non_neg_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer() | undefined,
    status :: pending | in_progress | completed | failed,
    error :: binary() | undefined,
    instance_ids :: [binary()]
}).

%% Cloud instance tracking record
-record(cloud_instance, {
    instance_id :: binary(),
    provider :: provider(),
    instance_type :: binary(),
    launch_time :: non_neg_integer(),
    state :: pending | running | stopping | terminated,
    public_ip :: binary() | undefined,
    private_ip :: binary() | undefined,
    node_name :: binary() | undefined,
    last_job_time :: non_neg_integer(),  % Last time a job ran on this instance
    hourly_cost :: float()
}).

%% Cost tracking record
-record(cost_tracking, {
    total_instance_hours :: float(),
    total_cost :: float(),
    budget_limit :: float(),
    budget_alert_threshold :: float(),  % Alert when reaching this % of budget
    cost_per_hour :: map(),             % #{instance_type => cost}
    daily_costs :: map(),               % #{date => cost}
    monthly_costs :: map()              % #{month => cost}
}).

-record(state, {
    enabled :: boolean(),
    provider :: provider() | undefined,
    provider_config :: map(),
    policy :: #scaling_policy{},
    current_cloud_nodes :: non_neg_integer(),
    pending_actions :: [#scaling_action{}],
    action_history :: [#scaling_action{}],
    last_scale_time :: non_neg_integer(),
    check_timer :: reference() | undefined,
    idle_check_timer :: reference() | undefined,
    %% Cloud instances tracking
    cloud_instances :: map(),  % #{instance_id => #cloud_instance{}}
    %% Cost tracking
    cost_tracking :: #cost_tracking{}
}).

-export_type([provider/0, instance_type/0]).

%% Test exports for coverage
-ifdef(TEST).
-export([
    validate_provider_config/2,
    update_policy/2,
    format_policy/1,
    collect_stats/1,
    generate_action_id/0,
    generate_instance_id/1,
    default_instance_costs/0,
    get_instance_cost/2,
    calculate_budget_status/1,
    date_to_binary/1,
    month_to_binary/1,
    format_instance/1,
    format_cloud_instances/1,
    add_security_groups/3,
    add_instance_ids/3,
    build_query_string/1,
    uri_encode/1,
    extract_host/1,
    format_canonical_headers/1,
    binary_to_hex/1
]).
-endif.

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Configure cloud provider
-spec configure_provider(provider(), map()) -> ok | {error, term()}.
configure_provider(Provider, Config) ->
    gen_server:call(?SERVER, {configure_provider, Provider, Config}).

%% @doc Get current provider configuration
-spec get_provider_config() -> {ok, provider(), map()} | {error, not_configured}.
get_provider_config() ->
    gen_server:call(?SERVER, get_provider_config).

%% @doc Request scale up
-spec request_scale_up(pos_integer(), map()) -> {ok, binary()} | {error, term()}.
request_scale_up(Count, Options) ->
    gen_server:call(?SERVER, {scale_up, Count, Options}, 30000).

%% @doc Request scale down
-spec request_scale_down(pos_integer(), map()) -> {ok, binary()} | {error, term()}.
request_scale_down(Count, Options) ->
    gen_server:call(?SERVER, {scale_down, Count, Options}, 30000).

%% @doc Get current scaling status
-spec get_scaling_status() -> map().
get_scaling_status() ->
    gen_server:call(?SERVER, get_scaling_status).

%% @doc Set scaling policy
-spec set_scaling_policy(map()) -> ok.
set_scaling_policy(PolicyMap) ->
    gen_server:call(?SERVER, {set_policy, PolicyMap}).

%% @doc Enable auto-scaling
-spec enable() -> ok.
enable() ->
    gen_server:call(?SERVER, enable).

%% @doc Disable auto-scaling
-spec disable() -> ok.
disable() ->
    gen_server:call(?SERVER, disable).

%% @doc Check if auto-scaling is enabled
-spec is_enabled() -> boolean().
is_enabled() ->
    gen_server:call(?SERVER, is_enabled).

%% @doc Get scaling statistics
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Request new cloud instances
%% Options can include: instance_type, spot, tags, etc.
-spec request_nodes(map()) -> {ok, [binary()]} | {error, term()}.
request_nodes(Options) ->
    Count = maps:get(count, Options, 1),
    gen_server:call(?SERVER, {request_nodes, Count, Options}, 60000).

%% @doc Terminate cloud instances
%% Can specify instance IDs or let system choose idle instances
-spec terminate_nodes([binary()] | map()) -> {ok, [binary()]} | {error, term()}.
terminate_nodes(InstanceIds) when is_list(InstanceIds) ->
    gen_server:call(?SERVER, {terminate_nodes, InstanceIds}, 60000);
terminate_nodes(Options) when is_map(Options) ->
    gen_server:call(?SERVER, {terminate_nodes_by_criteria, Options}, 60000).

%% @doc List all managed cloud instances
-spec list_cloud_instances() -> [map()].
list_cloud_instances() ->
    gen_server:call(?SERVER, list_cloud_instances).

%% @doc Get detailed info about a specific cloud instance
-spec get_instance_info(binary()) -> {ok, map()} | {error, not_found}.
get_instance_info(InstanceId) ->
    gen_server:call(?SERVER, {get_instance_info, InstanceId}).

%% @doc Get cost summary
-spec get_cost_summary() -> map().
get_cost_summary() ->
    gen_server:call(?SERVER, get_cost_summary).

%% @doc Set monthly budget limit (0 = unlimited)
-spec set_budget_limit(float()) -> ok.
set_budget_limit(Limit) when is_number(Limit), Limit >= 0 ->
    gen_server:call(?SERVER, {set_budget_limit, float(Limit)}).

%% @doc Get current budget status
-spec get_budget_status() -> map().
get_budget_status() ->
    gen_server:call(?SERVER, get_budget_status).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure inets is started for httpc
    application:ensure_all_started(inets),
    application:ensure_all_started(ssl),

    DefaultPolicy = #scaling_policy{
        min_nodes = 0,
        max_nodes = 100,
        target_pending_jobs = 10,
        target_idle_nodes = 2,
        scale_up_increment = 5,
        scale_down_increment = 1,
        cooldown_seconds = 300,
        instance_types = [<<"c5.xlarge">>],
        spot_enabled = false,
        spot_max_price = 0.0,
        idle_threshold_seconds = ?DEFAULT_IDLE_THRESHOLD,
        queue_depth_threshold = 10
    },

    DefaultCostTracking = #cost_tracking{
        total_instance_hours = 0.0,
        total_cost = 0.0,
        budget_limit = 0.0,  % 0 = unlimited
        budget_alert_threshold = 0.8,  % 80%
        cost_per_hour = default_instance_costs(),
        daily_costs = #{},
        monthly_costs = #{}
    },

    State = #state{
        enabled = false,
        provider = undefined,
        provider_config = #{},
        policy = DefaultPolicy,
        current_cloud_nodes = 0,
        pending_actions = [],
        action_history = [],
        last_scale_time = 0,
        check_timer = undefined,
        idle_check_timer = undefined,
        cloud_instances = #{},
        cost_tracking = DefaultCostTracking
    },

    {ok, State}.

handle_call({configure_provider, Provider, Config}, _From, State) ->
    case validate_provider_config(Provider, Config) of
        ok ->
            {reply, ok, State#state{
                provider = Provider,
                provider_config = Config
            }};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(get_provider_config, _From, State) ->
    case State#state.provider of
        undefined ->
            {reply, {error, not_configured}, State};
        Provider ->
            {reply, {ok, Provider, State#state.provider_config}, State}
    end;

handle_call({scale_up, Count, Options}, _From, State) ->
    case do_scale_up(Count, Options, State) of
        {ok, ActionId, NewState} ->
            {reply, {ok, ActionId}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({scale_down, Count, Options}, _From, State) ->
    case do_scale_down(Count, Options, State) of
        {ok, ActionId, NewState} ->
            {reply, {ok, ActionId}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(get_scaling_status, _From, State) ->
    Status = #{
        enabled => State#state.enabled,
        provider => State#state.provider,
        current_cloud_nodes => State#state.current_cloud_nodes,
        pending_actions => length(State#state.pending_actions),
        last_scale_time => State#state.last_scale_time,
        policy => format_policy(State#state.policy)
    },
    {reply, Status, State};

handle_call({set_policy, PolicyMap}, _From, State) ->
    NewPolicy = update_policy(State#state.policy, PolicyMap),
    {reply, ok, State#state{policy = NewPolicy}};

handle_call(enable, _From, State) ->
    Timer = erlang:send_after(?SCALE_CHECK_INTERVAL, self(), check_scaling),
    IdleTimer = erlang:send_after(?IDLE_CHECK_INTERVAL, self(), check_idle_nodes),
    _CostTimer = erlang:send_after(3600000, self(), update_costs),  % Every hour
    {reply, ok, State#state{
        enabled = true,
        check_timer = Timer,
        idle_check_timer = IdleTimer
    }};

handle_call(disable, _From, State) ->
    case State#state.check_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    case State#state.idle_check_timer of
        undefined -> ok;
        IdleTimer -> erlang:cancel_timer(IdleTimer)
    end,
    {reply, ok, State#state{
        enabled = false,
        check_timer = undefined,
        idle_check_timer = undefined
    }};

handle_call(is_enabled, _From, State) ->
    {reply, State#state.enabled, State};

handle_call(get_stats, _From, State) ->
    Stats = collect_stats(State),
    {reply, Stats, State};

%% Node lifecycle handlers
handle_call({request_nodes, Count, Options}, _From, State) ->
    case do_request_nodes(Count, Options, State) of
        {ok, InstanceIds, NewState} ->
            {reply, {ok, InstanceIds}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({terminate_nodes, InstanceIds}, _From, State) ->
    case do_terminate_nodes(InstanceIds, State) of
        {ok, TerminatedIds, NewState} ->
            {reply, {ok, TerminatedIds}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({terminate_nodes_by_criteria, Options}, _From, State) ->
    InstanceIds = select_instances_for_termination(Options, State),
    case do_terminate_nodes(InstanceIds, State) of
        {ok, TerminatedIds, NewState} ->
            {reply, {ok, TerminatedIds}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(list_cloud_instances, _From, State) ->
    Instances = format_cloud_instances(State#state.cloud_instances),
    {reply, Instances, State};

handle_call({get_instance_info, InstanceId}, _From, State) ->
    case maps:get(InstanceId, State#state.cloud_instances, undefined) of
        undefined ->
            {reply, {error, not_found}, State};
        Instance ->
            {reply, {ok, format_instance(Instance)}, State}
    end;

%% Cost tracking handlers
handle_call(get_cost_summary, _From, State) ->
    Summary = format_cost_summary(State#state.cost_tracking, State#state.cloud_instances),
    {reply, Summary, State};

handle_call({set_budget_limit, Limit}, _From, State) ->
    CostTracking = State#state.cost_tracking,
    NewCostTracking = CostTracking#cost_tracking{budget_limit = Limit},
    {reply, ok, State#state{cost_tracking = NewCostTracking}};

handle_call(get_budget_status, _From, State) ->
    Status = calculate_budget_status(State#state.cost_tracking),
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_scaling, #state{enabled = true} = State) ->
    NewState = auto_scale_check(State),
    Timer = erlang:send_after(?SCALE_CHECK_INTERVAL, self(), check_scaling),
    {noreply, NewState#state{check_timer = Timer}};

handle_info(check_scaling, State) ->
    {noreply, State};

handle_info(check_idle_nodes, #state{enabled = true} = State) ->
    NewState = check_and_terminate_idle_nodes(State),
    IdleTimer = erlang:send_after(?IDLE_CHECK_INTERVAL, self(), check_idle_nodes),
    {noreply, NewState#state{idle_check_timer = IdleTimer}};

handle_info(check_idle_nodes, State) ->
    {noreply, State};

handle_info(update_costs, #state{enabled = true} = State) ->
    NewState = update_cost_tracking(State),
    erlang:send_after(3600000, self(), update_costs),  % Every hour
    {noreply, NewState};

handle_info(update_costs, State) ->
    {noreply, State};

handle_info({scale_action_complete, ActionId, Result}, State) ->
    NewState = handle_action_complete(ActionId, Result, State),
    {noreply, NewState};

handle_info({instance_launched, InstanceId, InstanceInfo}, State) ->
    NewState = register_cloud_instance(InstanceId, InstanceInfo, State),
    {noreply, NewState};

handle_info({instance_terminated, InstanceId}, State) ->
    NewState = unregister_cloud_instance(InstanceId, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

validate_provider_config(aws, Config) ->
    RequiredKeys = [region, access_key_id, secret_access_key],
    case lists:all(fun(K) -> maps:is_key(K, Config) end, RequiredKeys) of
        true -> ok;
        false -> {error, missing_required_config}
    end;
validate_provider_config(gcp, Config) ->
    RequiredKeys = [project_id, zone, credentials_file],
    case lists:all(fun(K) -> maps:is_key(K, Config) end, RequiredKeys) of
        true -> ok;
        false -> {error, missing_required_config}
    end;
validate_provider_config(azure, Config) ->
    RequiredKeys = [subscription_id, resource_group, tenant_id],
    case lists:all(fun(K) -> maps:is_key(K, Config) end, RequiredKeys) of
        true -> ok;
        false -> {error, missing_required_config}
    end;
validate_provider_config(generic, Config) ->
    RequiredKeys = [scale_up_webhook, scale_down_webhook],
    case lists:all(fun(K) -> maps:is_key(K, Config) end, RequiredKeys) of
        true -> ok;
        false -> {error, missing_required_config}
    end;
validate_provider_config(_, _) ->
    {error, unknown_provider}.

do_scale_up(_Count, _Options, #state{provider = undefined}) ->
    {error, provider_not_configured};
do_scale_up(Count, Options, State) ->
    #state{
        provider = Provider,
        provider_config = Config,
        policy = Policy,
        current_cloud_nodes = CurrentNodes
    } = State,

    %% Check limits
    MaxNodes = Policy#scaling_policy.max_nodes,
    ActualCount = min(Count, MaxNodes - CurrentNodes),

    case ActualCount > 0 of
        true ->
            ActionId = generate_action_id(),
            Action = #scaling_action{
                id = ActionId,
                type = scale_up,
                requested_count = Count,
                actual_count = ActualCount,
                start_time = erlang:system_time(second),
                status = in_progress,
                instance_ids = []
            },

            %% Execute scaling
            spawn_link(fun() ->
                Result = execute_scale_up(Provider, Config, ActualCount, Options),
                ?SERVER ! {scale_action_complete, ActionId, Result}
            end),

            NewState = State#state{
                pending_actions = [Action | State#state.pending_actions],
                last_scale_time = erlang:system_time(second)
            },
            {ok, ActionId, NewState};
        false ->
            {error, at_max_capacity}
    end.

do_scale_down(_Count, _Options, #state{provider = undefined}) ->
    {error, provider_not_configured};
do_scale_down(Count, Options, State) ->
    #state{
        provider = Provider,
        provider_config = Config,
        policy = Policy,
        current_cloud_nodes = CurrentNodes
    } = State,

    %% Check limits
    MinNodes = Policy#scaling_policy.min_nodes,
    ActualCount = min(Count, CurrentNodes - MinNodes),

    case ActualCount > 0 of
        true ->
            ActionId = generate_action_id(),
            Action = #scaling_action{
                id = ActionId,
                type = scale_down,
                requested_count = Count,
                actual_count = ActualCount,
                start_time = erlang:system_time(second),
                status = in_progress,
                instance_ids = []
            },

            spawn_link(fun() ->
                Result = execute_scale_down(Provider, Config, ActualCount, Options),
                ?SERVER ! {scale_action_complete, ActionId, Result}
            end),

            NewState = State#state{
                pending_actions = [Action | State#state.pending_actions],
                last_scale_time = erlang:system_time(second)
            },
            {ok, ActionId, NewState};
        false ->
            {error, at_min_capacity}
    end.

auto_scale_check(State) ->
    #state{policy = Policy, last_scale_time = LastScale} = State,

    %% Check cooldown
    Now = erlang:system_time(second),
    CooldownExpired = (Now - LastScale) >= Policy#scaling_policy.cooldown_seconds,

    case CooldownExpired of
        false ->
            State;
        true ->
            %% Get cluster metrics
            case get_cluster_metrics() of
                {ok, Metrics} ->
                    evaluate_scaling_decision(Metrics, State);
                {error, _} ->
                    State
            end
    end.

get_cluster_metrics() ->
    try
        PendingJobs = case catch flurm_job_registry:count_by_state() of
            Counts when is_map(Counts) -> maps:get(pending, Counts, 0);
            _ -> 0
        end,
        %% Get idle nodes from node registry (nodes in 'up' state with no running jobs)
        IdleNodes = case catch flurm_node_registry:count_by_state() of
            NodeCounts when is_map(NodeCounts) ->
                %% Count nodes that are 'up' but have no jobs - approximate idle count
                %% In a real implementation, we'd check running_jobs for each node
                maps:get(up, NodeCounts, 0);
            _ -> 0
        end,
        TotalNodes = case catch flurm_node_registry:count_nodes() of
            T when is_integer(T) -> T;
            _ -> 0
        end,
        {ok, #{
            pending_jobs => PendingJobs,
            idle_nodes => IdleNodes,
            total_nodes => TotalNodes
        }}
    catch
        _:_ -> {error, metrics_unavailable}
    end.

evaluate_scaling_decision(Metrics, State) ->
    #state{policy = Policy, current_cloud_nodes = CloudNodes} = State,
    #{pending_jobs := PendingJobs, idle_nodes := IdleNodes} = Metrics,

    %% Determine if we should scale up
    ShouldScaleUp = PendingJobs > Policy#scaling_policy.target_pending_jobs
                    andalso CloudNodes < Policy#scaling_policy.max_nodes,

    %% Determine if we should scale down
    ShouldScaleDown = IdleNodes > Policy#scaling_policy.target_idle_nodes
                      andalso CloudNodes > Policy#scaling_policy.min_nodes,

    case {ShouldScaleUp, ShouldScaleDown} of
        {true, _} ->
            {ok, _, NewState} = do_scale_up(
                Policy#scaling_policy.scale_up_increment,
                #{auto => true},
                State
            ),
            NewState;
        {false, true} ->
            {ok, _, NewState} = do_scale_down(
                Policy#scaling_policy.scale_down_increment,
                #{auto => true},
                State
            ),
            NewState;
        {false, false} ->
            State
    end.

execute_scale_up(aws, Config, Count, Options) ->
    execute_aws_scale_up(Config, Count, Options);
execute_scale_up(gcp, Config, Count, Options) ->
    execute_gcp_scale_up(Config, Count, Options);
execute_scale_up(azure, Config, Count, Options) ->
    execute_azure_scale_up(Config, Count, Options);
execute_scale_up(generic, Config, Count, Options) ->
    execute_generic_scale_up(Config, Count, Options).

execute_scale_down(aws, Config, Count, Options) ->
    execute_aws_scale_down(Config, Count, Options);
execute_scale_down(gcp, Config, Count, Options) ->
    execute_gcp_scale_down(Config, Count, Options);
execute_scale_down(azure, Config, Count, Options) ->
    execute_azure_scale_down(Config, Count, Options);
execute_scale_down(generic, Config, Count, Options) ->
    execute_generic_scale_down(Config, Count, Options).

%% AWS implementation
execute_aws_scale_up(Config, Count, _Options) ->
    %% In production, this would use AWS SDK
    %% For now, return mock success
    _Region = maps:get(region, Config),
    _AsgName = maps:get(asg_name, Config, <<"flurm-compute">>),

    %% Would call: aws autoscaling set-desired-capacity
    InstanceIds = [generate_instance_id(aws) || _ <- lists:seq(1, Count)],
    {ok, #{instance_ids => InstanceIds, count => Count}}.

execute_aws_scale_down(Config, Count, _Options) ->
    _Region = maps:get(region, Config),
    %% Would call: aws autoscaling terminate-instance-in-auto-scaling-group
    {ok, #{terminated_count => Count}}.

%% GCP implementation
execute_gcp_scale_up(Config, Count, _Options) ->
    _ProjectId = maps:get(project_id, Config),
    _Zone = maps:get(zone, Config),
    _InstanceGroup = maps:get(instance_group, Config, <<"flurm-mig">>),

    %% Would call: gcloud compute instance-groups managed resize
    InstanceIds = [generate_instance_id(gcp) || _ <- lists:seq(1, Count)],
    {ok, #{instance_ids => InstanceIds, count => Count}}.

execute_gcp_scale_down(Config, Count, _Options) ->
    _ProjectId = maps:get(project_id, Config),
    {ok, #{terminated_count => Count}}.

%% Azure implementation
execute_azure_scale_up(Config, Count, _Options) ->
    _SubscriptionId = maps:get(subscription_id, Config),
    _ResourceGroup = maps:get(resource_group, Config),
    _ScaleSetName = maps:get(scale_set_name, Config, <<"flurm-vmss">>),

    %% Would call: az vmss scale
    InstanceIds = [generate_instance_id(azure) || _ <- lists:seq(1, Count)],
    {ok, #{instance_ids => InstanceIds, count => Count}}.

execute_azure_scale_down(Config, Count, _Options) ->
    _SubscriptionId = maps:get(subscription_id, Config),
    {ok, #{terminated_count => Count}}.

%% Generic webhook implementation
execute_generic_scale_up(Config, Count, Options) ->
    Webhook = maps:get(scale_up_webhook, Config),
    Payload = jsx:encode(#{
        action => <<"scale_up">>,
        count => Count,
        options => Options
    }),
    call_webhook(Webhook, Payload).

execute_generic_scale_down(Config, Count, Options) ->
    Webhook = maps:get(scale_down_webhook, Config),
    Payload = jsx:encode(#{
        action => <<"scale_down">>,
        count => Count,
        options => Options
    }),
    call_webhook(Webhook, Payload).

call_webhook(Url, Payload) ->
    %% Simple HTTP POST
    case httpc:request(post, {binary_to_list(Url), [], "application/json", Payload}, [], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, jsx:decode(list_to_binary(Body), [return_maps])};
        {ok, {{_, Code, _}, _, Body}} ->
            {error, {http_error, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

handle_action_complete(ActionId, Result, State) ->
    case lists:keyfind(ActionId, #scaling_action.id, State#state.pending_actions) of
        false ->
            State;
        Action ->
            {NewAction, NodeDelta} = case Result of
                %% Atom keys (from AWS/GCP/Azure implementations)
                {ok, #{instance_ids := Ids}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second),
                        instance_ids = Ids
                    }, length(Ids)};
                {ok, #{count := Count}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second)
                    }, Count};
                {ok, #{terminated_count := Count}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second)
                    }, -Count};
                %% Binary keys (from generic webhook JSON responses)
                {ok, #{<<"instance_ids">> := Ids}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second),
                        instance_ids = Ids
                    }, length(Ids)};
                {ok, #{<<"count">> := Count}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second)
                    }, Count};
                {ok, #{<<"terminated_count">> := Count}} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second)
                    }, -Count};
                %% Generic success without specific count (assume requested_count)
                {ok, _} ->
                    {Action#scaling_action{
                        status = completed,
                        end_time = erlang:system_time(second)
                    }, Action#scaling_action.requested_count};
                {error, Reason} ->
                    {Action#scaling_action{
                        status = failed,
                        end_time = erlang:system_time(second),
                        error = iolist_to_binary(io_lib:format("~p", [Reason]))
                    }, 0}
            end,

            NewPending = lists:keydelete(ActionId, #scaling_action.id, State#state.pending_actions),
            NewHistory = [NewAction | lists:sublist(State#state.action_history, 99)],  % Keep last 100

            State#state{
                pending_actions = NewPending,
                action_history = NewHistory,
                current_cloud_nodes = max(0, State#state.current_cloud_nodes + NodeDelta)
            }
    end.

update_policy(Policy, Map) ->
    Policy#scaling_policy{
        min_nodes = maps:get(min_nodes, Map, Policy#scaling_policy.min_nodes),
        max_nodes = maps:get(max_nodes, Map, Policy#scaling_policy.max_nodes),
        target_pending_jobs = maps:get(target_pending_jobs, Map, Policy#scaling_policy.target_pending_jobs),
        target_idle_nodes = maps:get(target_idle_nodes, Map, Policy#scaling_policy.target_idle_nodes),
        scale_up_increment = maps:get(scale_up_increment, Map, Policy#scaling_policy.scale_up_increment),
        scale_down_increment = maps:get(scale_down_increment, Map, Policy#scaling_policy.scale_down_increment),
        cooldown_seconds = maps:get(cooldown_seconds, Map, Policy#scaling_policy.cooldown_seconds),
        instance_types = maps:get(instance_types, Map, Policy#scaling_policy.instance_types),
        spot_enabled = maps:get(spot_enabled, Map, Policy#scaling_policy.spot_enabled),
        spot_max_price = maps:get(spot_max_price, Map, Policy#scaling_policy.spot_max_price),
        idle_threshold_seconds = maps:get(idle_threshold_seconds, Map, Policy#scaling_policy.idle_threshold_seconds),
        queue_depth_threshold = maps:get(queue_depth_threshold, Map, Policy#scaling_policy.queue_depth_threshold)
    }.

format_policy(#scaling_policy{} = P) ->
    #{
        min_nodes => P#scaling_policy.min_nodes,
        max_nodes => P#scaling_policy.max_nodes,
        target_pending_jobs => P#scaling_policy.target_pending_jobs,
        target_idle_nodes => P#scaling_policy.target_idle_nodes,
        scale_up_increment => P#scaling_policy.scale_up_increment,
        scale_down_increment => P#scaling_policy.scale_down_increment,
        cooldown_seconds => P#scaling_policy.cooldown_seconds,
        instance_types => P#scaling_policy.instance_types,
        spot_enabled => P#scaling_policy.spot_enabled,
        spot_max_price => P#scaling_policy.spot_max_price,
        idle_threshold_seconds => P#scaling_policy.idle_threshold_seconds,
        queue_depth_threshold => P#scaling_policy.queue_depth_threshold
    }.

collect_stats(State) ->
    CompletedActions = [A || A <- State#state.action_history, A#scaling_action.status =:= completed],
    FailedActions = [A || A <- State#state.action_history, A#scaling_action.status =:= failed],

    ScaleUps = [A || A <- CompletedActions, A#scaling_action.type =:= scale_up],
    ScaleDowns = [A || A <- CompletedActions, A#scaling_action.type =:= scale_down],

    #{
        enabled => State#state.enabled,
        provider => State#state.provider,
        current_cloud_nodes => State#state.current_cloud_nodes,
        pending_actions => length(State#state.pending_actions),
        completed_scale_ups => length(ScaleUps),
        completed_scale_downs => length(ScaleDowns),
        failed_actions => length(FailedActions),
        total_instances_launched => lists:sum([A#scaling_action.actual_count || A <- ScaleUps]),
        total_instances_terminated => lists:sum([A#scaling_action.actual_count || A <- ScaleDowns])
    }.

generate_action_id() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"scale-", Timestamp/binary, "-", Random/binary>>.

generate_instance_id(aws) ->
    Random = integer_to_binary(rand:uniform(16#FFFFFFFF), 16),
    <<"i-", Random/binary>>;
generate_instance_id(gcp) ->
    Random = integer_to_binary(rand:uniform(16#FFFFFFFF), 16),
    <<"gce-", Random/binary>>;
generate_instance_id(azure) ->
    Random = integer_to_binary(rand:uniform(16#FFFFFFFF), 16),
    <<"azure-", Random/binary>>;
generate_instance_id(generic) ->
    Random = integer_to_binary(rand:uniform(16#FFFFFFFF), 16),
    <<"node-", Random/binary>>.

%%====================================================================
%% Node Lifecycle Functions
%%====================================================================

%% @private Request new cloud instances
do_request_nodes(_Count, _Options, #state{provider = undefined}) ->
    {error, provider_not_configured};
do_request_nodes(Count, Options, State) ->
    #state{
        provider = Provider,
        provider_config = Config,
        policy = Policy,
        current_cloud_nodes = CurrentNodes,
        cost_tracking = CostTracking
    } = State,

    %% Check max nodes limit
    MaxNodes = Policy#scaling_policy.max_nodes,
    ActualCount = min(Count, MaxNodes - CurrentNodes),

    %% Check budget limit
    case check_budget_allows_scaling(ActualCount, Options, CostTracking) of
        false ->
            {error, budget_exceeded};
        true when ActualCount > 0 ->
            %% Execute the cloud provider request
            InstanceType = maps:get(instance_type, Options,
                                   hd(Policy#scaling_policy.instance_types)),
            UseSpot = maps:get(spot, Options, Policy#scaling_policy.spot_enabled),

            Result = execute_request_instances(Provider, Config, ActualCount,
                                               InstanceType, UseSpot, Options),
            case Result of
                {ok, InstanceIds} ->
                    %% Register instances in tracking
                    Now = erlang:system_time(second),
                    HourlyCost = get_instance_cost(InstanceType, CostTracking),
                    NewInstances = lists:foldl(
                        fun(Id, Acc) ->
                            Instance = #cloud_instance{
                                instance_id = Id,
                                provider = Provider,
                                instance_type = InstanceType,
                                launch_time = Now,
                                state = pending,
                                last_job_time = Now,
                                hourly_cost = HourlyCost
                            },
                            maps:put(Id, Instance, Acc)
                        end,
                        State#state.cloud_instances,
                        InstanceIds
                    ),
                    NewState = State#state{
                        cloud_instances = NewInstances,
                        current_cloud_nodes = CurrentNodes + length(InstanceIds)
                    },
                    {ok, InstanceIds, NewState};
                {error, Reason} ->
                    {error, Reason}
            end;
        true ->
            {error, at_max_capacity}
    end.

%% @private Terminate specific cloud instances
do_terminate_nodes([], State) ->
    {ok, [], State};
do_terminate_nodes(_InstanceIds, #state{provider = undefined}) ->
    {error, provider_not_configured};
do_terminate_nodes(InstanceIds, State) ->
    #state{
        provider = Provider,
        provider_config = Config,
        policy = Policy,
        current_cloud_nodes = CurrentNodes,
        cloud_instances = CloudInstances
    } = State,

    %% Check min nodes limit
    MinNodes = Policy#scaling_policy.min_nodes,
    MaxTerminate = CurrentNodes - MinNodes,

    %% Only terminate instances we know about
    ValidIds = [Id || Id <- InstanceIds, maps:is_key(Id, CloudInstances)],
    ActualIds = lists:sublist(ValidIds, MaxTerminate),

    case ActualIds of
        [] ->
            {error, at_min_capacity};
        _ ->
            Result = execute_terminate_instances(Provider, Config, ActualIds),
            case Result of
                {ok, TerminatedIds} ->
                    %% Update cost tracking before removing instances
                    NewCostTracking = update_costs_for_terminated(TerminatedIds, State),

                    %% Remove instances from tracking
                    NewInstances = lists:foldl(
                        fun(Id, Acc) -> maps:remove(Id, Acc) end,
                        CloudInstances,
                        TerminatedIds
                    ),
                    NewState = State#state{
                        cloud_instances = NewInstances,
                        current_cloud_nodes = max(0, CurrentNodes - length(TerminatedIds)),
                        cost_tracking = NewCostTracking
                    },
                    {ok, TerminatedIds, NewState};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private Select instances for termination based on criteria
select_instances_for_termination(Options, State) ->
    #state{
        cloud_instances = CloudInstances,
        policy = Policy
    } = State,

    Count = maps:get(count, Options, Policy#scaling_policy.scale_down_increment),
    Criteria = maps:get(criteria, Options, idle),

    Instances = maps:values(CloudInstances),

    %% Sort instances based on criteria
    SortedInstances = case Criteria of
        idle ->
            %% Sort by last job time (oldest first = longest idle)
            lists:sort(
                fun(A, B) ->
                    A#cloud_instance.last_job_time =< B#cloud_instance.last_job_time
                end,
                Instances
            );
        oldest ->
            %% Sort by launch time (oldest first)
            lists:sort(
                fun(A, B) ->
                    A#cloud_instance.launch_time =< B#cloud_instance.launch_time
                end,
                Instances
            );
        cheapest ->
            %% Sort by hourly cost (lowest first)
            lists:sort(
                fun(A, B) ->
                    A#cloud_instance.hourly_cost =< B#cloud_instance.hourly_cost
                end,
                Instances
            );
        _ ->
            Instances
    end,

    %% Return the first N instance IDs
    [I#cloud_instance.instance_id || I <- lists:sublist(SortedInstances, Count)].

%%====================================================================
%% Cloud Provider Execution (AWS EC2 using httpc)
%%====================================================================

%% @private Execute instance request for different providers
execute_request_instances(aws, Config, Count, InstanceType, UseSpot, Options) ->
    aws_ec2_run_instances(Config, Count, InstanceType, UseSpot, Options);
execute_request_instances(gcp, Config, Count, InstanceType, _UseSpot, Options) ->
    gcp_compute_create_instances(Config, Count, InstanceType, Options);
execute_request_instances(azure, Config, Count, InstanceType, _UseSpot, Options) ->
    azure_vm_create_instances(Config, Count, InstanceType, Options);
execute_request_instances(generic, Config, Count, InstanceType, _UseSpot, Options) ->
    generic_webhook_create(Config, Count, InstanceType, Options).

%% @private Execute instance termination for different providers
execute_terminate_instances(aws, Config, InstanceIds) ->
    aws_ec2_terminate_instances(Config, InstanceIds);
execute_terminate_instances(gcp, Config, InstanceIds) ->
    gcp_compute_delete_instances(Config, InstanceIds);
execute_terminate_instances(azure, Config, InstanceIds) ->
    azure_vm_delete_instances(Config, InstanceIds);
execute_terminate_instances(generic, Config, InstanceIds) ->
    generic_webhook_terminate(Config, InstanceIds).

%%====================================================================
%% AWS EC2 API Implementation
%%====================================================================

%% @private Run EC2 instances using AWS API
aws_ec2_run_instances(Config, Count, InstanceType, UseSpot, Options) ->
    Region = maps:get(region, Config),
    AccessKey = maps:get(access_key_id, Config),
    SecretKey = maps:get(secret_access_key, Config),

    %% Build EC2 RunInstances parameters
    ImageId = maps:get(ami_id, Config, <<"ami-0c55b159cbfafe1f0">>),
    SubnetId = maps:get(subnet_id, Config, undefined),
    SecurityGroups = maps:get(security_groups, Config, []),
    KeyName = maps:get(key_name, Config, undefined),
    UserData = maps:get(user_data, Options, <<>>),

    %% Build query parameters
    Params0 = [
        {<<"Action">>, <<"RunInstances">>},
        {<<"Version">>, <<"2016-11-15">>},
        {<<"ImageId">>, ImageId},
        {<<"InstanceType">>, InstanceType},
        {<<"MinCount">>, integer_to_binary(Count)},
        {<<"MaxCount">>, integer_to_binary(Count)}
    ],

    Params1 = case SubnetId of
        undefined -> Params0;
        _ -> [{<<"SubnetId">>, SubnetId} | Params0]
    end,

    Params2 = case KeyName of
        undefined -> Params1;
        _ -> [{<<"KeyName">>, KeyName} | Params1]
    end,

    Params3 = add_security_groups(Params2, SecurityGroups, 1),

    Params4 = case UserData of
        <<>> -> Params3;
        _ -> [{<<"UserData">>, base64:encode(UserData)} | Params3]
    end,

    %% Add spot instance request if enabled
    Params5 = case UseSpot of
        true ->
            MaxPrice = maps:get(spot_max_price, Config, <<"0.10">>),
            [{<<"InstanceMarketOptions.MarketType">>, <<"spot">>},
             {<<"InstanceMarketOptions.SpotOptions.MaxPrice">>, MaxPrice} | Params4];
        false ->
            Params4
    end,

    %% Make the API request
    Endpoint = io_lib:format("https://ec2.~s.amazonaws.com/", [Region]),
    case aws_signed_request(Endpoint, Params5, AccessKey, SecretKey, Region, <<"ec2">>) of
        {ok, ResponseBody} ->
            parse_ec2_run_instances_response(ResponseBody);
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Terminate EC2 instances
aws_ec2_terminate_instances(Config, InstanceIds) ->
    Region = maps:get(region, Config),
    AccessKey = maps:get(access_key_id, Config),
    SecretKey = maps:get(secret_access_key, Config),

    %% Build TerminateInstances parameters
    Params0 = [
        {<<"Action">>, <<"TerminateInstances">>},
        {<<"Version">>, <<"2016-11-15">>}
    ],

    Params1 = add_instance_ids(Params0, InstanceIds, 1),

    Endpoint = io_lib:format("https://ec2.~s.amazonaws.com/", [Region]),
    case aws_signed_request(Endpoint, Params1, AccessKey, SecretKey, Region, <<"ec2">>) of
        {ok, _ResponseBody} ->
            {ok, InstanceIds};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Add security groups to params
add_security_groups(Params, [], _N) -> Params;
add_security_groups(Params, [SG | Rest], N) ->
    Key = iolist_to_binary([<<"SecurityGroupId.">>, integer_to_binary(N)]),
    add_security_groups([{Key, SG} | Params], Rest, N + 1).

%% @private Add instance IDs to params
add_instance_ids(Params, [], _N) -> Params;
add_instance_ids(Params, [Id | Rest], N) ->
    Key = iolist_to_binary([<<"InstanceId.">>, integer_to_binary(N)]),
    add_instance_ids([{Key, Id} | Params], Rest, N + 1).

%% @private Make AWS signed request using Signature Version 4
aws_signed_request(Endpoint, Params, AccessKey, SecretKey, Region, Service) ->
    %% Build query string
    QueryString = build_query_string(Params),

    %% Current timestamp
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
    AmzDate = io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ",
                           [Year, Month, Day, Hour, Min, Sec]),
    DateStamp = io_lib:format("~4..0B~2..0B~2..0B", [Year, Month, Day]),

    %% Canonical request
    Method = <<"POST">>,
    CanonicalUri = <<"/">>,
    CanonicalQuerystring = <<>>,
    PayloadHash = crypto:hash(sha256, QueryString),
    PayloadHashHex = binary_to_hex(PayloadHash),

    Headers = [
        {<<"content-type">>, <<"application/x-www-form-urlencoded">>},
        {<<"host">>, extract_host(Endpoint)},
        {<<"x-amz-date">>, iolist_to_binary(AmzDate)}
    ],

    SignedHeaders = <<"content-type;host;x-amz-date">>,
    CanonicalHeaders = format_canonical_headers(Headers),

    CanonicalRequest = iolist_to_binary([
        Method, <<"\n">>,
        CanonicalUri, <<"\n">>,
        CanonicalQuerystring, <<"\n">>,
        CanonicalHeaders, <<"\n">>,
        SignedHeaders, <<"\n">>,
        PayloadHashHex
    ]),

    %% String to sign
    Algorithm = <<"AWS4-HMAC-SHA256">>,
    CredentialScope = iolist_to_binary([DateStamp, <<"/">>, Region, <<"/">>,
                                        Service, <<"/aws4_request">>]),
    CanonicalRequestHash = binary_to_hex(crypto:hash(sha256, CanonicalRequest)),

    StringToSign = iolist_to_binary([
        Algorithm, <<"\n">>,
        AmzDate, <<"\n">>,
        CredentialScope, <<"\n">>,
        CanonicalRequestHash
    ]),

    %% Calculate signature
    KDate = crypto:mac(hmac, sha256, <<"AWS4", SecretKey/binary>>, iolist_to_binary(DateStamp)),
    KRegion = crypto:mac(hmac, sha256, KDate, Region),
    KService = crypto:mac(hmac, sha256, KRegion, Service),
    KSigning = crypto:mac(hmac, sha256, KService, <<"aws4_request">>),
    Signature = binary_to_hex(crypto:mac(hmac, sha256, KSigning, StringToSign)),

    %% Authorization header
    AuthHeader = iolist_to_binary([
        Algorithm, <<" ">>,
        <<"Credential=">>, AccessKey, <<"/">>, CredentialScope, <<", ">>,
        <<"SignedHeaders=">>, SignedHeaders, <<", ">>,
        <<"Signature=">>, Signature
    ]),

    %% Make HTTP request
    HttpHeaders = [
        {"content-type", "application/x-www-form-urlencoded"},
        {"x-amz-date", binary_to_list(iolist_to_binary(AmzDate))},
        {"authorization", binary_to_list(AuthHeader)}
    ],

    Request = {binary_to_list(iolist_to_binary(Endpoint)), HttpHeaders,
               "application/x-www-form-urlencoded", QueryString},

    case httpc:request(post, Request, [{timeout, 30000}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, list_to_binary(Body)};
        {ok, {{_, Code, _}, _, Body}} ->
            {error, {aws_error, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Build URL-encoded query string
build_query_string(Params) ->
    Parts = [iolist_to_binary([K, <<"=">>, uri_encode(V)]) || {K, V} <- Params],
    iolist_to_binary(lists:join(<<"&">>, Parts)).

%% @private URL encode a value
uri_encode(Value) when is_binary(Value) ->
    uri_string:quote(Value);
uri_encode(Value) when is_list(Value) ->
    uri_string:quote(list_to_binary(Value)).

%% @private Extract host from URL
extract_host(Url) ->
    UrlStr = iolist_to_binary(Url),
    case binary:split(UrlStr, <<"://">>) of
        [_, Rest] ->
            case binary:split(Rest, <<"/">>) of
                [Host | _] -> Host;
                _ -> Rest
            end;
        _ -> UrlStr
    end.

%% @private Format canonical headers
format_canonical_headers(Headers) ->
    Sorted = lists:sort(fun({A, _}, {B, _}) -> A =< B end, Headers),
    iolist_to_binary([[K, <<":">>, V, <<"\n">>] || {K, V} <- Sorted]).

%% @private Convert binary to hex string
binary_to_hex(Bin) ->
    << <<(integer_to_binary(B, 16))/binary>> || <<B:4>> <= Bin >>.

%% @private Parse EC2 RunInstances response
parse_ec2_run_instances_response(Body) ->
    %% Simple XML parsing - extract instance IDs
    %% In production, use a proper XML parser
    case re:run(Body, <<"<instanceId>([^<]+)</instanceId>">>,
                [global, {capture, all_but_first, binary}]) of
        {match, Matches} ->
            InstanceIds = [hd(M) || M <- Matches],
            {ok, InstanceIds};
        nomatch ->
            %% Check for error
            case re:run(Body, <<"<Message>([^<]+)</Message>">>,
                        [{capture, all_but_first, binary}]) of
                {match, [[ErrorMsg]]} ->
                    {error, {ec2_error, ErrorMsg}};
                nomatch ->
                    {error, invalid_response}
            end
    end.

%%====================================================================
%% GCP Compute Engine Implementation (Stub)
%%====================================================================

gcp_compute_create_instances(_Config, Count, _InstanceType, _Options) ->
    %% GCP implementation would use Compute Engine API
    %% For now, return mock instance IDs
    InstanceIds = [generate_instance_id(gcp) || _ <- lists:seq(1, Count)],
    {ok, InstanceIds}.

gcp_compute_delete_instances(_Config, InstanceIds) ->
    {ok, InstanceIds}.

%%====================================================================
%% Azure VM Implementation (Stub)
%%====================================================================

azure_vm_create_instances(_Config, Count, _InstanceType, _Options) ->
    %% Azure implementation would use Azure Resource Manager API
    %% For now, return mock instance IDs
    InstanceIds = [generate_instance_id(azure) || _ <- lists:seq(1, Count)],
    {ok, InstanceIds}.

azure_vm_delete_instances(_Config, InstanceIds) ->
    {ok, InstanceIds}.

%%====================================================================
%% Generic Webhook Implementation
%%====================================================================

generic_webhook_create(Config, Count, InstanceType, Options) ->
    Webhook = maps:get(scale_up_webhook, Config),
    Payload = jsx:encode(#{
        action => <<"create_instances">>,
        count => Count,
        instance_type => InstanceType,
        options => Options
    }),
    case call_webhook(Webhook, Payload) of
        {ok, #{<<"instance_ids">> := Ids}} ->
            {ok, Ids};
        {ok, _} ->
            %% Generate IDs if not returned
            {ok, [generate_instance_id(generic) || _ <- lists:seq(1, Count)]};
        {error, Reason} ->
            {error, Reason}
    end.

generic_webhook_terminate(Config, InstanceIds) ->
    Webhook = maps:get(scale_down_webhook, Config),
    Payload = jsx:encode(#{
        action => <<"terminate_instances">>,
        instance_ids => InstanceIds
    }),
    case call_webhook(Webhook, Payload) of
        {ok, _} -> {ok, InstanceIds};
        {error, Reason} -> {error, Reason}
    end.

%%====================================================================
%% Idle Node Management
%%====================================================================

%% @private Check and terminate idle nodes
check_and_terminate_idle_nodes(State) ->
    #state{
        policy = Policy,
        cloud_instances = CloudInstances,
        current_cloud_nodes = CurrentNodes
    } = State,

    Now = erlang:system_time(second),
    IdleThreshold = Policy#scaling_policy.idle_threshold_seconds,
    MinNodes = Policy#scaling_policy.min_nodes,

    %% Find instances that have been idle longer than threshold
    IdleInstances = maps:fold(
        fun(Id, Instance, Acc) ->
            TimeSinceLastJob = Now - Instance#cloud_instance.last_job_time,
            case TimeSinceLastJob > IdleThreshold of
                true -> [Id | Acc];
                false -> Acc
            end
        end,
        [],
        CloudInstances
    ),

    %% Only terminate if we have more than min_nodes
    case {IdleInstances, CurrentNodes > MinNodes} of
        {[], _} ->
            State;
        {_, false} ->
            State;
        {Candidates, true} ->
            %% Terminate up to scale_down_increment instances
            ToTerminate = lists:sublist(Candidates,
                                        min(Policy#scaling_policy.scale_down_increment,
                                            CurrentNodes - MinNodes)),
            case do_terminate_nodes(ToTerminate, State) of
                {ok, _, NewState} -> NewState;
                {error, _} -> State
            end
    end.

%%====================================================================
%% Cost Tracking Functions
%%====================================================================

%% @private Default instance costs ($/hour)
default_instance_costs() ->
    #{
        %% AWS EC2 On-Demand prices (us-east-1, approximate)
        <<"t3.micro">> => 0.0104,
        <<"t3.small">> => 0.0208,
        <<"t3.medium">> => 0.0416,
        <<"t3.large">> => 0.0832,
        <<"t3.xlarge">> => 0.1664,
        <<"c5.large">> => 0.085,
        <<"c5.xlarge">> => 0.17,
        <<"c5.2xlarge">> => 0.34,
        <<"c5.4xlarge">> => 0.68,
        <<"m5.large">> => 0.096,
        <<"m5.xlarge">> => 0.192,
        <<"m5.2xlarge">> => 0.384,
        <<"r5.large">> => 0.126,
        <<"r5.xlarge">> => 0.252,
        %% GPU instances
        <<"p3.2xlarge">> => 3.06,
        <<"p3.8xlarge">> => 12.24,
        <<"g4dn.xlarge">> => 0.526,
        <<"g4dn.2xlarge">> => 0.752
    }.

%% @private Get cost for instance type
get_instance_cost(InstanceType, #cost_tracking{cost_per_hour = CostMap}) ->
    maps:get(InstanceType, CostMap, 0.10).  % Default to $0.10/hr

%% @private Check if budget allows scaling
check_budget_allows_scaling(_Count, _Options, #cost_tracking{budget_limit = +0.0}) ->
    true;  % No budget limit
check_budget_allows_scaling(Count, Options, CostTracking) ->
    #cost_tracking{
        total_cost = CurrentCost,
        budget_limit = BudgetLimit,
        cost_per_hour = CostMap
    } = CostTracking,

    InstanceType = maps:get(instance_type, Options, <<"c5.xlarge">>),
    HourlyCost = maps:get(InstanceType, CostMap, 0.10),

    %% Estimate cost for remaining month (assume 730 hours)
    EstimatedAddedCost = HourlyCost * Count * 730,  % Conservative estimate
    ProjectedCost = CurrentCost + EstimatedAddedCost,

    ProjectedCost < BudgetLimit.

%% @private Update cost tracking periodically
update_cost_tracking(State) ->
    #state{
        cloud_instances = CloudInstances,
        cost_tracking = CostTracking
    } = State,

    Now = erlang:system_time(second),
    {Date, _} = calendar:universal_time(),
    DateKey = date_to_binary(Date),
    MonthKey = month_to_binary(Date),

    %% Calculate costs for running instances (1 hour elapsed)
    HourlyCost = maps:fold(
        fun(_Id, Instance, Acc) ->
            case Instance#cloud_instance.state of
                running -> Acc + Instance#cloud_instance.hourly_cost;
                _ -> Acc
            end
        end,
        0.0,
        CloudInstances
    ),

    %% Calculate instance hours
    InstanceHours = maps:fold(
        fun(_Id, Instance, Acc) ->
            case Instance#cloud_instance.state of
                running ->
                    RunningHours = (Now - Instance#cloud_instance.launch_time) / 3600,
                    Acc + RunningHours;
                _ -> Acc
            end
        end,
        0.0,
        CloudInstances
    ),

    %% Update daily costs
    DailyCosts = CostTracking#cost_tracking.daily_costs,
    CurrentDailyCost = maps:get(DateKey, DailyCosts, 0.0),
    NewDailyCosts = maps:put(DateKey, CurrentDailyCost + HourlyCost, DailyCosts),

    %% Update monthly costs
    MonthlyCosts = CostTracking#cost_tracking.monthly_costs,
    CurrentMonthlyCost = maps:get(MonthKey, MonthlyCosts, 0.0),
    NewMonthlyCosts = maps:put(MonthKey, CurrentMonthlyCost + HourlyCost, MonthlyCosts),

    NewCostTracking = CostTracking#cost_tracking{
        total_instance_hours = CostTracking#cost_tracking.total_instance_hours + InstanceHours,
        total_cost = CostTracking#cost_tracking.total_cost + HourlyCost,
        daily_costs = NewDailyCosts,
        monthly_costs = NewMonthlyCosts
    },

    %% Check budget alert
    check_budget_alert(NewCostTracking),

    State#state{cost_tracking = NewCostTracking}.

%% @private Update costs for terminated instances
update_costs_for_terminated(InstanceIds, State) ->
    #state{
        cloud_instances = CloudInstances,
        cost_tracking = CostTracking
    } = State,

    Now = erlang:system_time(second),

    %% Calculate final costs for terminated instances
    {AddedHours, AddedCost} = lists:foldl(
        fun(Id, {Hours, Cost}) ->
            case maps:get(Id, CloudInstances, undefined) of
                undefined -> {Hours, Cost};
                Instance ->
                    RunningHours = (Now - Instance#cloud_instance.launch_time) / 3600,
                    InstanceCost = RunningHours * Instance#cloud_instance.hourly_cost,
                    {Hours + RunningHours, Cost + InstanceCost}
            end
        end,
        {0.0, 0.0},
        InstanceIds
    ),

    CostTracking#cost_tracking{
        total_instance_hours = CostTracking#cost_tracking.total_instance_hours + AddedHours,
        total_cost = CostTracking#cost_tracking.total_cost + AddedCost
    }.

%% @private Check and send budget alert if threshold exceeded
check_budget_alert(#cost_tracking{budget_limit = +0.0}) ->
    ok;
check_budget_alert(CostTracking) ->
    #cost_tracking{
        total_cost = TotalCost,
        budget_limit = BudgetLimit,
        budget_alert_threshold = Threshold
    } = CostTracking,

    Percentage = TotalCost / BudgetLimit,
    case Percentage >= Threshold of
        true ->
            %% Log warning - in production, send alert
            error_logger:warning_msg(
                "Cloud scaling budget alert: ~.1f% of budget used ($~.2f / $~.2f)~n",
                [Percentage * 100, TotalCost, BudgetLimit]
            );
        false ->
            ok
    end.

%% @private Format date to binary
date_to_binary({Year, Month, Day}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Year, Month, Day])).

%% @private Format month to binary
month_to_binary({Year, Month, _Day}) ->
    iolist_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%% @private Calculate budget status
calculate_budget_status(#cost_tracking{budget_limit = +0.0} = CT) ->
    #{
        budget_limit => 0.0,
        total_cost => CT#cost_tracking.total_cost,
        percentage_used => 0.0,
        remaining => unlimited,
        alert_threshold => CT#cost_tracking.budget_alert_threshold
    };
calculate_budget_status(CostTracking) ->
    #cost_tracking{
        total_cost = TotalCost,
        budget_limit = BudgetLimit,
        budget_alert_threshold = AlertThreshold
    } = CostTracking,

    #{
        budget_limit => BudgetLimit,
        total_cost => TotalCost,
        percentage_used => (TotalCost / BudgetLimit) * 100,
        remaining => max(0, BudgetLimit - TotalCost),
        alert_threshold => AlertThreshold * 100,
        over_budget => TotalCost > BudgetLimit
    }.

%%====================================================================
%% Instance Tracking Functions
%%====================================================================

%% @private Register a new cloud instance
register_cloud_instance(InstanceId, InstanceInfo, State) ->
    #state{cloud_instances = CloudInstances, cost_tracking = CostTracking} = State,

    Now = erlang:system_time(second),
    InstanceType = maps:get(instance_type, InstanceInfo, <<"unknown">>),

    Instance = #cloud_instance{
        instance_id = InstanceId,
        provider = maps:get(provider, InstanceInfo, State#state.provider),
        instance_type = InstanceType,
        launch_time = Now,
        state = running,
        public_ip = maps:get(public_ip, InstanceInfo, undefined),
        private_ip = maps:get(private_ip, InstanceInfo, undefined),
        node_name = maps:get(node_name, InstanceInfo, undefined),
        last_job_time = Now,
        hourly_cost = get_instance_cost(InstanceType, CostTracking)
    },

    NewInstances = maps:put(InstanceId, Instance, CloudInstances),
    State#state{
        cloud_instances = NewInstances,
        current_cloud_nodes = State#state.current_cloud_nodes + 1
    }.

%% @private Unregister a cloud instance
unregister_cloud_instance(InstanceId, State) ->
    #state{cloud_instances = CloudInstances} = State,

    case maps:get(InstanceId, CloudInstances, undefined) of
        undefined ->
            State;
        _Instance ->
            NewCostTracking = update_costs_for_terminated([InstanceId], State),
            NewInstances = maps:remove(InstanceId, CloudInstances),
            State#state{
                cloud_instances = NewInstances,
                current_cloud_nodes = max(0, State#state.current_cloud_nodes - 1),
                cost_tracking = NewCostTracking
            }
    end.

%% @private Format cloud instances for API response
format_cloud_instances(CloudInstances) ->
    [format_instance(I) || I <- maps:values(CloudInstances)].

%% @private Format single instance for API response
format_instance(#cloud_instance{} = I) ->
    #{
        instance_id => I#cloud_instance.instance_id,
        provider => I#cloud_instance.provider,
        instance_type => I#cloud_instance.instance_type,
        launch_time => I#cloud_instance.launch_time,
        state => I#cloud_instance.state,
        public_ip => I#cloud_instance.public_ip,
        private_ip => I#cloud_instance.private_ip,
        node_name => I#cloud_instance.node_name,
        last_job_time => I#cloud_instance.last_job_time,
        hourly_cost => I#cloud_instance.hourly_cost,
        running_hours => (erlang:system_time(second) - I#cloud_instance.launch_time) / 3600
    }.

%% @private Format cost summary
format_cost_summary(CostTracking, CloudInstances) ->
    #cost_tracking{
        total_instance_hours = TotalHours,
        total_cost = TotalCost,
        budget_limit = BudgetLimit,
        daily_costs = DailyCosts,
        monthly_costs = MonthlyCosts
    } = CostTracking,

    %% Calculate current running costs
    CurrentHourlyCost = maps:fold(
        fun(_Id, Instance, Acc) ->
            case Instance#cloud_instance.state of
                running -> Acc + Instance#cloud_instance.hourly_cost;
                _ -> Acc
            end
        end,
        0.0,
        CloudInstances
    ),

    #{
        total_instance_hours => TotalHours,
        total_cost => TotalCost,
        budget_limit => BudgetLimit,
        current_hourly_cost => CurrentHourlyCost,
        projected_daily_cost => CurrentHourlyCost * 24,
        projected_monthly_cost => CurrentHourlyCost * 730,
        daily_costs => DailyCosts,
        monthly_costs => MonthlyCosts,
        running_instances => maps:size(CloudInstances)
    }.
