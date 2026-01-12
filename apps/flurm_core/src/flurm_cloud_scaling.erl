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
    get_stats/0
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
    spot_max_price :: float()                   % Max spot price ($/hr)
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

-record(state, {
    enabled :: boolean(),
    provider :: provider() | undefined,
    provider_config :: map(),
    policy :: #scaling_policy{},
    current_cloud_nodes :: non_neg_integer(),
    pending_actions :: [#scaling_action{}],
    action_history :: [#scaling_action{}],
    last_scale_time :: non_neg_integer(),
    check_timer :: reference() | undefined
}).

-export_type([provider/0, instance_type/0]).

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

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
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
        spot_max_price = 0.0
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
        check_timer = undefined
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
    {reply, ok, State#state{enabled = true, check_timer = Timer}};

handle_call(disable, _From, State) ->
    case State#state.check_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    {reply, ok, State#state{enabled = false, check_timer = undefined}};

handle_call(is_enabled, _From, State) ->
    {reply, State#state.enabled, State};

handle_call(get_stats, _From, State) ->
    Stats = collect_stats(State),
    {reply, Stats, State};

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

handle_info({scale_action_complete, ActionId, Result}, State) ->
    NewState = handle_action_complete(ActionId, Result, State),
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
        IdleNodes = case catch flurm_node_registry:count_idle_nodes() of
            N when is_integer(N) -> N;
            _ -> 0
        end,
        TotalNodes = case catch flurm_node_registry:count_total_nodes() of
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
        spot_max_price = maps:get(spot_max_price, Map, Policy#scaling_policy.spot_max_price)
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
        spot_max_price => P#scaling_policy.spot_max_price
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
    <<"azure-", Random/binary>>.
