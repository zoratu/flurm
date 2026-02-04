%%%-------------------------------------------------------------------
%%% @doc FLURM Federation Support
%%%
%%% Enables multi-cluster federation for job submission across clusters.
%%% Compatible with SLURM's federation feature.
%%%
%%% Features:
%%% - Cluster registration and discovery
%%% - Cross-cluster job submission (sibling jobs)
%%% - Federated job tracking
%%% - Cluster state synchronization
%%% - Origin/Sibling cluster coordination
%%% - Automatic health monitoring and failover
%%% - Configurable routing policies
%%%
%%% ETS Tables:
%%%   flurm_fed_clusters - Registry of federated clusters
%%%   flurm_fed_jobs - Cross-cluster job tracking
%%%   flurm_fed_partition_map - Partition to cluster mapping
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation).
-behaviour(gen_server).

-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% API - Cluster Management
-export([
    start_link/0,
    add_cluster/2,
    remove_cluster/1,
    update_settings/1,
    list_clusters/0,
    get_cluster_status/1
]).

%% API - Cross-cluster Job Submission
-export([
    submit_job/2,
    route_job/1,
    get_cluster_for_partition/1
]).

%% API - Job Tracking
-export([
    track_remote_job/3,
    get_remote_job_status/2,
    sync_job_state/2
]).

%% API - Resource Aggregation
-export([
    get_federation_resources/0,
    get_federation_jobs/0,
    get_federation_stats/0
]).

%% API - Sibling Job Coordination (Phase 7D)
%% TLA+ Safety Invariants:
%%   - SiblingExclusivity: At most one sibling runs at any time
%%   - OriginAwareness: Origin cluster tracks which sibling is running
%%   - NoJobLoss: Jobs must have at least one active sibling or be terminal
-export([
    create_sibling_jobs/2,
    notify_job_started/2,
    revoke_siblings/2,
    handle_sibling_revoke/1,
    get_running_cluster/1,
    get_sibling_job_state/2
]).

%% API - Federation Message Handling (Phase 7D)
%% Called by protocol handler when receiving inter-cluster messages
-export([
    handle_federation_message/2
]).

%% API - Legacy/Compatibility
-export([
    create_federation/2,
    join_federation/2,
    leave_federation/0,
    get_federation_info/0,
    get_cluster_info/1,
    submit_federated_job/2,
    get_federated_job/1,
    sync_cluster_state/1,
    set_cluster_features/1,
    is_federated/0,
    get_local_cluster/0
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
-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_JOBS_TABLE, flurm_fed_jobs).
-define(FED_PARTITION_MAP, flurm_fed_partition_map).
-define(FED_REMOTE_JOBS, flurm_fed_remote_jobs).
-define(FED_SIBLING_JOBS, flurm_fed_sibling_jobs).  % Phase 7D: Sibling job tracking
-define(SYNC_INTERVAL, 30000).       % Sync every 30 seconds
-define(HEALTH_CHECK_INTERVAL, 10000). % Health check every 10 seconds
-define(CLUSTER_TIMEOUT, 5000).      % Timeout for cluster communication
-define(MAX_RETRY_COUNT, 3).         % Max retries for failed operations

%% Routing policies
-type routing_policy() :: round_robin | least_loaded | partition_affinity | random | weighted.

-type cluster_name() :: binary().
-type federation_name() :: binary().

%% Cluster configuration
-record(cluster_config, {
    host :: binary(),
    port :: pos_integer(),
    auth :: map(),                    % Authentication config
    weight = 1 :: pos_integer(),      % Scheduling weight
    features = [] :: [binary()],      % Cluster features
    partitions = [] :: [binary()],    % Available partitions
    routing_policy = least_loaded :: routing_policy()
}).

-record(federation, {
    name :: federation_name(),
    created :: non_neg_integer(),
    clusters :: [cluster_name()],
    options :: map()
}).

-record(fed_cluster, {
    name :: cluster_name(),
    host :: binary(),
    port :: pos_integer(),
    auth = #{} :: map(),              % Authentication config
    state :: up | down | drain | unknown,
    weight :: pos_integer(),          % Scheduling weight
    features :: [binary()],           % Cluster features (gpu, highspeed, etc)
    partitions = [] :: [binary()],    % Available partitions
    node_count :: non_neg_integer(),
    cpu_count :: non_neg_integer(),
    memory_mb :: non_neg_integer(),
    gpu_count :: non_neg_integer(),
    pending_jobs :: non_neg_integer(),
    running_jobs :: non_neg_integer(),
    available_cpus :: non_neg_integer(),
    available_memory :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    last_health_check :: non_neg_integer(),
    consecutive_failures :: non_neg_integer(),
    properties :: map()
}).

-record(fed_job, {
    id :: {cluster_name(), job_id()},     % {origin_cluster, job_id}
    federation_id :: binary(),             % Federation-wide ID
    origin_cluster :: cluster_name(),
    sibling_clusters :: [cluster_name()],
    sibling_jobs :: #{cluster_name() => job_id()},
    state :: pending | running | completed | failed | cancelled,
    submit_time :: non_neg_integer(),
    features_required :: [binary()],
    cluster_constraint :: [cluster_name()] | any
}).

%% Remote job tracking
-record(remote_job, {
    local_ref :: binary(),                 % Local reference ID
    remote_cluster :: cluster_name(),
    remote_job_id :: job_id(),
    local_job_id :: job_id() | undefined,
    state :: pending | running | completed | failed | cancelled | unknown,
    submit_time :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    job_spec :: map()
}).

%% Partition to cluster mapping
-record(partition_map, {
    partition :: binary(),
    cluster :: cluster_name(),
    priority :: non_neg_integer()
}).

-record(state, {
    local_cluster :: cluster_name(),
    federation :: #federation{} | undefined,
    sync_timer :: reference() | undefined,
    health_timer :: reference() | undefined,
    routing_policy :: routing_policy(),
    round_robin_index :: non_neg_integer(),
    http_client :: module()               % HTTP client module (for testing)
}).

-export_type([cluster_name/0, federation_name/0, routing_policy/0]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    build_url/3,
    build_auth_headers/1,
    headers_to_proplist/1,
    cluster_to_map/1,
    generate_local_ref/0,
    generate_federation_id/0,
    get_job_partition/1,
    get_job_features/1,
    get_job_cpus/1,
    get_job_memory/1,
    job_to_map/1,
    map_to_job/1,
    has_required_features/2,
    aggregate_resources/1,
    calculate_load/1
]).
-endif.

%%====================================================================
%% API - Cluster Management
%%====================================================================

%% @doc Start the federation coordination gen_server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Add a remote cluster to the federation.
%% Config should contain: host, port, and optionally auth, weight, features.
-spec add_cluster(cluster_name(), map() | #cluster_config{}) -> ok | {error, term()}.
add_cluster(Name, Config) when is_binary(Name) ->
    gen_server:call(?SERVER, {add_cluster, Name, Config}).

%% @doc Remove a cluster from the federation.
-spec remove_cluster(cluster_name()) -> ok | {error, term()}.
remove_cluster(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {remove_cluster, Name}).

%% @doc Update federation-wide settings.
%% Settings map may contain: routing_policy, sync_interval, health_check_interval
-spec update_settings(map()) -> ok | {error, term()}.
update_settings(Settings) when is_map(Settings) ->
    gen_server:call(?SERVER, {update_settings, Settings}).

%% @doc List all federated clusters with their status.
-spec list_clusters() -> [map()].
list_clusters() ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
    [cluster_to_map(C) || C <- Clusters].

%% @doc Get detailed status of a specific cluster.
-spec get_cluster_status(cluster_name()) -> {ok, map()} | {error, not_found}.
get_cluster_status(Name) when is_binary(Name) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, Name) of
        [Cluster] -> {ok, cluster_to_map(Cluster)};
        [] -> {error, not_found}
    end.

%%====================================================================
%% API - Cross-cluster Job Submission
%%====================================================================

%% @doc Submit a job to a specific cluster or auto-route.
%% Options:
%%   - cluster: specific cluster name (optional)
%%   - partition: partition name (used for routing if no cluster specified)
%%   - features: required features
%%   - routing_policy: override default routing policy
-spec submit_job(#job{} | map(), map()) -> {ok, {cluster_name(), job_id()}} | {error, term()}.
submit_job(Job, Options) ->
    gen_server:call(?SERVER, {submit_job, Job, Options}, ?CLUSTER_TIMEOUT * 2).

%% @doc Determine the best cluster for a job based on resources and policies.
-spec route_job(#job{} | map()) -> {ok, cluster_name()} | {error, term()}.
route_job(Job) ->
    gen_server:call(?SERVER, {route_job, Job}).

%% @doc Get the cluster that owns a specific partition.
-spec get_cluster_for_partition(binary()) -> {ok, cluster_name()} | {error, not_found}.
get_cluster_for_partition(Partition) when is_binary(Partition) ->
    case ets:lookup(?FED_PARTITION_MAP, Partition) of
        [#partition_map{cluster = Cluster}] -> {ok, Cluster};
        [] -> {error, not_found}
    end.

%%====================================================================
%% API - Job Tracking
%%====================================================================

%% @doc Track a job submitted to a remote cluster.
-spec track_remote_job(cluster_name(), job_id(), map()) -> {ok, binary()} | {error, term()}.
track_remote_job(Cluster, RemoteJobId, JobSpec) ->
    gen_server:call(?SERVER, {track_remote_job, Cluster, RemoteJobId, JobSpec}).

%% @doc Query a remote cluster for job status.
-spec get_remote_job_status(cluster_name(), job_id()) -> {ok, map()} | {error, term()}.
get_remote_job_status(Cluster, JobId) ->
    gen_server:call(?SERVER, {get_remote_job_status, Cluster, JobId}, ?CLUSTER_TIMEOUT).

%% @doc Synchronize job state from a remote cluster.
-spec sync_job_state(cluster_name(), job_id()) -> ok | {error, term()}.
sync_job_state(Cluster, JobId) ->
    gen_server:call(?SERVER, {sync_job_state, Cluster, JobId}, ?CLUSTER_TIMEOUT).

%%====================================================================
%% API - Resource Aggregation
%%====================================================================

%% @doc Get aggregated resources across all federated clusters.
-spec get_federation_resources() -> map().
get_federation_resources() ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
    aggregate_resources(Clusters).

%% @doc Get all jobs across the federation.
-spec get_federation_jobs() -> [map()].
get_federation_jobs() ->
    gen_server:call(?SERVER, get_federation_jobs, ?CLUSTER_TIMEOUT * 3).

%% @doc Get federation statistics for metrics collection.
%% Returns cluster counts for Prometheus metrics.
-spec get_federation_stats() -> map().
get_federation_stats() ->
    try
        Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
        {Healthy, Unhealthy} = lists:foldl(
            fun(#fed_cluster{state = State}, {H, U}) ->
                case State of
                    up -> {H + 1, U};
                    _ -> {H, U + 1}
                end
            end,
            {0, 0},
            Clusters
        ),
        #{
            clusters_total => length(Clusters),
            clusters_healthy => Healthy,
            clusters_unhealthy => Unhealthy
        }
    catch
        _:_ ->
            #{clusters_total => 0, clusters_healthy => 0, clusters_unhealthy => 0}
    end.

%%====================================================================
%% API - Legacy/Compatibility
%%====================================================================

%% @doc Create a new federation
-spec create_federation(federation_name(), [cluster_name()]) -> ok | {error, term()}.
create_federation(Name, InitialClusters) ->
    gen_server:call(?SERVER, {create_federation, Name, InitialClusters}).

%% @doc Join an existing federation
-spec join_federation(federation_name(), binary()) -> ok | {error, term()}.
join_federation(FederationName, OriginClusterHost) ->
    gen_server:call(?SERVER, {join_federation, FederationName, OriginClusterHost}).

%% @doc Leave the current federation
-spec leave_federation() -> ok | {error, term()}.
leave_federation() ->
    gen_server:call(?SERVER, leave_federation).

%% @doc Get federation information
-spec get_federation_info() -> {ok, map()} | {error, not_federated}.
get_federation_info() ->
    gen_server:call(?SERVER, get_federation_info).

%% @doc Get info about specific cluster (legacy)
-spec get_cluster_info(cluster_name()) -> {ok, #fed_cluster{}} | {error, not_found}.
get_cluster_info(ClusterName) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] -> {ok, Cluster};
        [] -> {error, not_found}
    end.

%% @doc Submit a job that can run on any federated cluster
-spec submit_federated_job(#job{}, map()) -> {ok, binary()} | {error, term()}.
submit_federated_job(Job, Options) ->
    gen_server:call(?SERVER, {submit_federated_job, Job, Options}).

%% @doc Get federated job information
-spec get_federated_job(binary()) -> {ok, #fed_job{}} | {error, not_found}.
get_federated_job(FederationId) ->
    case ets:match_object(?FED_JOBS_TABLE, #fed_job{federation_id = FederationId, _ = '_'}) of
        [Job] -> {ok, Job};
        [] -> {error, not_found}
    end.

%% @doc Synchronize state with a remote cluster
-spec sync_cluster_state(cluster_name()) -> ok | {error, term()}.
sync_cluster_state(ClusterName) ->
    gen_server:call(?SERVER, {sync_cluster, ClusterName}).

%% @doc Set features for local cluster
-spec set_cluster_features([binary()]) -> ok.
set_cluster_features(Features) ->
    gen_server:call(?SERVER, {set_features, Features}).

%% @doc Check if cluster is in a federation
-spec is_federated() -> boolean().
is_federated() ->
    gen_server:call(?SERVER, is_federated).

%% @doc Get local cluster name
-spec get_local_cluster() -> cluster_name().
get_local_cluster() ->
    gen_server:call(?SERVER, get_local_cluster).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?FED_CLUSTERS_TABLE, [
        named_table, public, set,
        {keypos, #fed_cluster.name},
        {read_concurrency, true}
    ]),
    ets:new(?FED_JOBS_TABLE, [
        named_table, public, set,
        {keypos, #fed_job.id}
    ]),
    ets:new(?FED_PARTITION_MAP, [
        named_table, public, bag,
        {keypos, #partition_map.partition}
    ]),
    ets:new(?FED_REMOTE_JOBS, [
        named_table, public, set,
        {keypos, #remote_job.local_ref}
    ]),

    %% Phase 7D: Sibling job tracking table
    %% Key: federation_job_id, stores #fed_job_tracker{} records
    ets:new(?FED_SIBLING_JOBS, [
        named_table, public, set,
        {keypos, #fed_job_tracker.federation_job_id}
    ]),

    %% Get local cluster name from config
    LocalCluster = get_local_cluster_name(),
    RoutingPolicy = get_routing_policy(),

    %% Register local cluster
    LocalEntry = #fed_cluster{
        name = LocalCluster,
        host = get_local_host(),
        port = get_local_port(),
        auth = #{},
        state = up,
        weight = 1,
        features = [],
        partitions = get_local_partitions(),
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        pending_jobs = 0,
        running_jobs = 0,
        available_cpus = 0,
        available_memory = 0,
        last_sync = erlang:system_time(second),
        last_health_check = erlang:system_time(second),
        consecutive_failures = 0,
        properties = #{}
    },
    ets:insert(?FED_CLUSTERS_TABLE, LocalEntry),

    %% Register local partitions
    register_local_partitions(LocalCluster),

    %% Start health check timer
    HealthTimer = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),

    {ok, #state{
        local_cluster = LocalCluster,
        federation = undefined,
        sync_timer = undefined,
        health_timer = HealthTimer,
        routing_policy = RoutingPolicy,
        round_robin_index = 0,
        http_client = httpc
    }}.

handle_call({add_cluster, Name, Config}, _From, State) ->
    Result = do_add_cluster(Name, Config, State),
    {reply, Result, State};

handle_call({remove_cluster, Name}, _From, State) ->
    case Name =:= State#state.local_cluster of
        true ->
            {reply, {error, cannot_remove_local}, State};
        false ->
            ets:delete(?FED_CLUSTERS_TABLE, Name),
            %% Remove partition mappings for this cluster
            ets:match_delete(?FED_PARTITION_MAP, #partition_map{cluster = Name, _ = '_'}),
            {reply, ok, State}
    end;

handle_call({update_settings, Settings}, _From, State) ->
    %% Update federation-wide settings
    NewState = apply_settings(Settings, State),
    {reply, ok, NewState};

handle_call({submit_job, Job, Options}, _From, State) ->
    Result = do_submit_job(Job, Options, State),
    {reply, Result, State};

handle_call({route_job, Job}, _From, State) ->
    Result = do_route_job(Job, State),
    {reply, Result, State};

handle_call({track_remote_job, Cluster, RemoteJobId, JobSpec}, _From, State) ->
    LocalRef = generate_local_ref(),
    RemoteJob = #remote_job{
        local_ref = LocalRef,
        remote_cluster = Cluster,
        remote_job_id = RemoteJobId,
        local_job_id = undefined,
        state = pending,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = JobSpec
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),
    {reply, {ok, LocalRef}, State};

handle_call({get_remote_job_status, Cluster, JobId}, _From, State) ->
    Result = do_get_remote_job_status(Cluster, JobId, State),
    {reply, Result, State};

handle_call({sync_job_state, Cluster, JobId}, _From, State) ->
    Result = do_sync_job_state(Cluster, JobId, State),
    {reply, Result, State};

handle_call(get_federation_jobs, _From, State) ->
    Jobs = do_get_federation_jobs(State),
    {reply, Jobs, State};

handle_call({create_federation, Name, InitialClusters}, _From, State) ->
    case State#state.federation of
        undefined ->
            Federation = #federation{
                name = Name,
                created = erlang:system_time(second),
                clusters = [State#state.local_cluster | InitialClusters],
                options = #{}
            },
            %% Start sync timer
            Timer = erlang:send_after(?SYNC_INTERVAL, self(), sync_all),
            {reply, ok, State#state{federation = Federation, sync_timer = Timer}};
        _ ->
            {reply, {error, already_federated}, State}
    end;

handle_call({join_federation, FederationName, OriginHost}, _From, State) ->
    case State#state.federation of
        undefined ->
            case fetch_federation_info(OriginHost) of
                {ok, Federation} when Federation#federation.name =:= FederationName ->
                    case register_with_cluster(OriginHost, State#state.local_cluster) of
                        ok ->
                            Timer = erlang:send_after(?SYNC_INTERVAL, self(), sync_all),
                            {reply, ok, State#state{federation = Federation, sync_timer = Timer}};
                        {error, Reason} ->
                            {reply, {error, Reason}, State}
                    end;
                {ok, _} ->
                    {reply, {error, federation_mismatch}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        _ ->
            {reply, {error, already_federated}, State}
    end;

handle_call(leave_federation, _From, State) ->
    case State#state.federation of
        undefined ->
            {reply, {error, not_federated}, State};
        _Federation ->
            case State#state.sync_timer of
                undefined -> ok;
                Timer -> erlang:cancel_timer(Timer)
            end,
            %% Clear remote clusters
            ets:foldl(fun(#fed_cluster{name = Name}, _) ->
                case Name =:= State#state.local_cluster of
                    true -> ok;
                    false -> ets:delete(?FED_CLUSTERS_TABLE, Name)
                end
            end, ok, ?FED_CLUSTERS_TABLE),
            {reply, ok, State#state{federation = undefined, sync_timer = undefined}}
    end;

handle_call(get_federation_info, _From, State) ->
    case State#state.federation of
        undefined ->
            {reply, {error, not_federated}, State};
        #federation{name = Name, clusters = Clusters} ->
            Info = #{
                name => Name,
                clusters => Clusters,
                local_cluster => State#state.local_cluster
            },
            {reply, {ok, Info}, State}
    end;

handle_call({submit_federated_job, Job, Options}, _From, State) ->
    case State#state.federation of
        undefined ->
            {reply, {error, not_federated}, State};
        _ ->
            Result = do_submit_federated_job(Job, Options, State),
            {reply, Result, State}
    end;

handle_call({sync_cluster, ClusterName}, _From, State) ->
    Result = do_sync_cluster(ClusterName, State),
    {reply, Result, State};

handle_call({set_features, Features}, _From, State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, State#state.local_cluster) of
        [Local] ->
            ets:insert(?FED_CLUSTERS_TABLE, Local#fed_cluster{features = Features});
        [] ->
            ok
    end,
    {reply, ok, State};

handle_call(is_federated, _From, State) ->
    IsFed = State#state.federation =/= undefined orelse
            ets:info(?FED_CLUSTERS_TABLE, size) > 1,
    {reply, IsFed, State};

handle_call(get_local_cluster, _From, State) ->
    {reply, State#state.local_cluster, State};

%% Sibling Job Coordination (Phase 7D)
handle_call({create_sibling_jobs, JobSpec, TargetClusters}, _From, State) ->
    Result = do_create_sibling_jobs(JobSpec, TargetClusters, State),
    {reply, Result, State};

handle_call({notify_job_started, FederationJobId, LocalJobId}, _From, State) ->
    Result = do_notify_job_started(FederationJobId, LocalJobId, State),
    {reply, Result, State};

handle_call({revoke_siblings, FederationJobId, RunningCluster}, _From, State) ->
    Result = do_revoke_siblings(FederationJobId, RunningCluster, State),
    {reply, Result, State};

handle_call({handle_sibling_revoke, Msg}, _From, State) ->
    Result = do_handle_sibling_revoke(Msg, State),
    {reply, Result, State};

handle_call({get_running_cluster, FederationJobId}, _From, State) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{running_cluster = RunningCluster}] ->
            {reply, {ok, RunningCluster}, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_sibling_job_state, FederationJobId, ClusterName}, _From, State) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = SiblingStates}] ->
            case maps:get(ClusterName, SiblingStates, undefined) of
                undefined -> {reply, {error, not_found}, State};
                SiblingState -> {reply, {ok, SiblingState}, State}
            end;
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({handle_federation_message, MsgType, EncodedMsg}, _From, State) ->
    Result = do_handle_federation_message(MsgType, EncodedMsg, State),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync_all, State) ->
    case State#state.federation of
        undefined ->
            %% Still sync with any manually added clusters
            sync_all_clusters(State);
        #federation{clusters = Clusters} ->
            lists:foreach(fun(ClusterName) ->
                case ClusterName =:= State#state.local_cluster of
                    true -> ok;
                    false -> do_sync_cluster(ClusterName, State)
                end
            end, Clusters)
    end,

    %% Update local cluster stats
    update_local_stats(State#state.local_cluster),

    Timer = erlang:send_after(?SYNC_INTERVAL, self(), sync_all),
    {noreply, State#state{sync_timer = Timer}};

handle_info(health_check, State) ->
    NewState = do_health_check(State),
    HealthTimer = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),
    {noreply, NewState#state{health_timer = HealthTimer}};

handle_info({cluster_update, ClusterName, Stats}, State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = update_cluster_from_stats(Cluster, Stats),
            ets:insert(?FED_CLUSTERS_TABLE, Updated);
        [] ->
            ok
    end,
    {noreply, State};

handle_info({cluster_health, ClusterName, Status}, State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = case Status of
                up ->
                    Cluster#fed_cluster{
                        state = up,
                        consecutive_failures = 0,
                        last_health_check = erlang:system_time(second)
                    };
                down ->
                    Failures = Cluster#fed_cluster.consecutive_failures + 1,
                    NewState = case Failures >= ?MAX_RETRY_COUNT of
                        true -> down;
                        false -> Cluster#fed_cluster.state
                    end,
                    Cluster#fed_cluster{
                        state = NewState,
                        consecutive_failures = Failures,
                        last_health_check = erlang:system_time(second)
                    }
            end,
            ets:insert(?FED_CLUSTERS_TABLE, Updated);
        [] ->
            ok
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.sync_timer of
        undefined -> ok;
        SyncTimer -> erlang:cancel_timer(SyncTimer)
    end,
    case State#state.health_timer of
        undefined -> ok;
        HealthTimer -> erlang:cancel_timer(HealthTimer)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions - Cluster Management
%%====================================================================

do_add_cluster(Name, Config, State) when is_map(Config) ->
    Host = maps:get(host, Config, <<"localhost">>),
    Port = maps:get(port, Config, 6817),
    Auth = maps:get(auth, Config, #{}),
    Weight = maps:get(weight, Config, 1),
    Features = maps:get(features, Config, []),
    Partitions = maps:get(partitions, Config, []),

    ClusterEntry = #fed_cluster{
        name = Name,
        host = Host,
        port = Port,
        auth = Auth,
        state = unknown,
        weight = Weight,
        features = Features,
        partitions = Partitions,
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        pending_jobs = 0,
        running_jobs = 0,
        available_cpus = 0,
        available_memory = 0,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 0,
        properties = #{}
    },
    ets:insert(?FED_CLUSTERS_TABLE, ClusterEntry),

    %% Register partitions for this cluster
    lists:foreach(fun(P) ->
        ets:insert(?FED_PARTITION_MAP, #partition_map{
            partition = P,
            cluster = Name,
            priority = Weight
        })
    end, Partitions),

    %% Trigger initial sync
    spawn(fun() ->
        do_sync_cluster(Name, State),
        self() ! {cluster_health, Name, up}
    end),

    ok;

do_add_cluster(Name, #cluster_config{} = Config, State) ->
    do_add_cluster(Name, #{
        host => Config#cluster_config.host,
        port => Config#cluster_config.port,
        auth => Config#cluster_config.auth,
        weight => Config#cluster_config.weight,
        features => Config#cluster_config.features,
        partitions => Config#cluster_config.partitions
    }, State).

%%====================================================================
%% Internal Functions - Job Submission and Routing
%%====================================================================

do_submit_job(Job, Options, State) ->
    %% Track routing decision timing
    StartTime = erlang:monotonic_time(millisecond),

    %% Determine target cluster
    TargetCluster = case maps:get(cluster, Options, undefined) of
        undefined ->
            %% Auto-route based on partition or resources
            case do_route_job(Job, State) of
                {ok, Cluster} -> Cluster;
                {error, _} -> State#state.local_cluster
            end;
        Cluster ->
            Cluster
    end,

    %% Record routing decision metrics
    RoutingDuration = erlang:monotonic_time(millisecond) - StartTime,
    catch flurm_metrics:histogram(flurm_federation_routing_duration_ms, RoutingDuration),
    catch flurm_metrics:increment(flurm_federation_routing_decisions_total),

    case TargetCluster =:= State#state.local_cluster of
        true ->
            %% Submit locally
            catch flurm_metrics:increment(flurm_federation_routing_local_total),
            submit_local_job(Job);
        false ->
            %% Submit to remote cluster
            catch flurm_metrics:increment(flurm_federation_routing_remote_total),
            submit_remote_job(TargetCluster, Job, Options)
    end.

do_route_job(Job, State) ->
    %% Extract job requirements
    Partition = get_job_partition(Job),
    Features = get_job_features(Job),
    NumCpus = get_job_cpus(Job),
    MemoryMb = get_job_memory(Job),

    %% Get all eligible clusters
    EligibleClusters = find_eligible_clusters(Partition, Features, NumCpus, MemoryMb),

    case EligibleClusters of
        [] ->
            %% Fall back to local cluster
            {ok, State#state.local_cluster};
        Clusters ->
            %% Apply routing policy
            Policy = State#state.routing_policy,
            select_cluster_by_policy(Clusters, Policy, State)
    end.

find_eligible_clusters(Partition, Features, NumCpus, MemoryMb) ->
    AllClusters = ets:tab2list(?FED_CLUSTERS_TABLE),

    %% Filter by partition if specified
    ClustersWithPartition = case Partition of
        undefined -> AllClusters;
        <<>> -> AllClusters;
        _ ->
            case ets:lookup(?FED_PARTITION_MAP, Partition) of
                [] -> AllClusters;  % Partition not mapped, check all
                Mappings ->
                    ClusterNames = [M#partition_map.cluster || M <- Mappings],
                    [C || C <- AllClusters, lists:member(C#fed_cluster.name, ClusterNames)]
            end
    end,

    %% Filter by features, state, and resources
    [C || C <- ClustersWithPartition,
        C#fed_cluster.state =:= up,
        has_required_features(C#fed_cluster.features, Features),
        C#fed_cluster.available_cpus >= NumCpus,
        C#fed_cluster.available_memory >= MemoryMb].

select_cluster_by_policy(Clusters, round_robin, State) ->
    Index = State#state.round_robin_index rem length(Clusters),
    Cluster = lists:nth(Index + 1, Clusters),
    %% Update index via cast for next selection
    gen_server:cast(self(), {update_rr_index, Index + 1}),
    {ok, Cluster#fed_cluster.name};

select_cluster_by_policy(Clusters, least_loaded, _State) ->
    %% Sort by load (pending_jobs + running_jobs) / cpu_count
    Sorted = lists:sort(fun(A, B) ->
        LoadA = calculate_load(A),
        LoadB = calculate_load(B),
        LoadA =< LoadB
    end, Clusters),
    case Sorted of
        [Best | _] -> {ok, Best#fed_cluster.name};
        [] -> {error, no_cluster_available}
    end;

select_cluster_by_policy(Clusters, weighted, _State) ->
    %% Weighted random selection
    TotalWeight = lists:sum([C#fed_cluster.weight || C <- Clusters]),
    case TotalWeight of
        0 -> {ok, (hd(Clusters))#fed_cluster.name};
        _ ->
            Random = rand:uniform(TotalWeight),
            select_by_weight(Clusters, Random, 0)
    end;

select_cluster_by_policy(Clusters, partition_affinity, _State) ->
    %% Already filtered by partition, just pick first up cluster
    case [C || C <- Clusters, C#fed_cluster.state =:= up] of
        [First | _] -> {ok, First#fed_cluster.name};
        [] -> {error, no_cluster_available}
    end;

select_cluster_by_policy(Clusters, random, _State) ->
    Index = rand:uniform(length(Clusters)),
    Cluster = lists:nth(Index, Clusters),
    {ok, Cluster#fed_cluster.name}.

select_by_weight([C | Rest], Random, Acc) ->
    NewAcc = Acc + C#fed_cluster.weight,
    case Random =< NewAcc of
        true -> {ok, C#fed_cluster.name};
        false -> select_by_weight(Rest, Random, NewAcc)
    end;
select_by_weight([], _, _) ->
    {error, no_cluster_available}.

calculate_load(#fed_cluster{cpu_count = 0}) ->
    infinity;
calculate_load(#fed_cluster{pending_jobs = Pending, running_jobs = Running, cpu_count = CpuCount}) ->
    (Pending + Running) / CpuCount.

submit_local_job(Job) when is_record(Job, job) ->
    case flurm_scheduler:submit_job(Job) of
        {ok, JobId} -> {ok, {get_local_cluster_name(), JobId}};
        Error -> Error
    end;
submit_local_job(JobSpec) when is_map(JobSpec) ->
    %% Convert map to job record
    Job = map_to_job(JobSpec),
    submit_local_job(Job).

submit_remote_job(ClusterName, Job, _Options) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth, state = up}] ->
            StartTime = erlang:monotonic_time(millisecond),
            case remote_submit_job(Host, Port, Auth, Job) of
                {ok, RemoteJobId} ->
                    %% Record successful remote submission metrics
                    Duration = erlang:monotonic_time(millisecond) - StartTime,
                    catch flurm_metrics:histogram(flurm_federation_remote_submit_duration_ms, Duration),
                    catch flurm_metrics:increment(flurm_federation_jobs_submitted_total),

                    %% Track the remote job
                    LocalRef = generate_local_ref(),
                    RemoteJob = #remote_job{
                        local_ref = LocalRef,
                        remote_cluster = ClusterName,
                        remote_job_id = RemoteJobId,
                        state = pending,
                        submit_time = erlang:system_time(second),
                        last_sync = erlang:system_time(second),
                        job_spec = job_to_map(Job)
                    },
                    ets:insert(?FED_REMOTE_JOBS, RemoteJob),
                    {ok, {ClusterName, RemoteJobId}};
                Error ->
                    %% Record failed submission duration
                    Duration = erlang:monotonic_time(millisecond) - StartTime,
                    catch flurm_metrics:histogram(flurm_federation_remote_submit_duration_ms, Duration),
                    Error
            end;
        [#fed_cluster{state = State}] ->
            {error, {cluster_unavailable, State}};
        [] ->
            {error, cluster_not_found}
    end.

%%====================================================================
%% Internal Functions - Job Tracking
%%====================================================================

do_get_remote_job_status(Cluster, JobId, _State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, Cluster) of
        [#fed_cluster{host = Host, port = Port, auth = Auth}] ->
            fetch_remote_job_status(Host, Port, Auth, JobId);
        [] ->
            {error, cluster_not_found}
    end.

do_sync_job_state(Cluster, JobId, State) ->
    case do_get_remote_job_status(Cluster, JobId, State) of
        {ok, Status} ->
            %% Update local tracking record
            Pattern = #remote_job{remote_cluster = Cluster, remote_job_id = JobId, _ = '_'},
            case ets:match_object(?FED_REMOTE_JOBS, Pattern) of
                [RemoteJob] ->
                    NewState = maps:get(state, Status, RemoteJob#remote_job.state),
                    Updated = RemoteJob#remote_job{
                        state = NewState,
                        last_sync = erlang:system_time(second)
                    },
                    ets:insert(?FED_REMOTE_JOBS, Updated),
                    ok;
                [] ->
                    ok
            end;
        Error ->
            Error
    end.

do_get_federation_jobs(State) ->
    %% Get local jobs
    LocalJobs = case catch flurm_job_registry:list_jobs() of
        Jobs when is_list(Jobs) ->
            [#{cluster => State#state.local_cluster, job_id => Id, pid => Pid}
             || {Id, Pid} <- Jobs];
        _ ->
            []
    end,

    %% Get tracked remote jobs
    RemoteJobs = ets:foldl(fun(#remote_job{} = RJ, Acc) ->
        [#{
            cluster => RJ#remote_job.remote_cluster,
            job_id => RJ#remote_job.remote_job_id,
            local_ref => RJ#remote_job.local_ref,
            state => RJ#remote_job.state
        } | Acc]
    end, [], ?FED_REMOTE_JOBS),

    LocalJobs ++ RemoteJobs.

%%====================================================================
%% Internal Functions - Health Monitoring
%%====================================================================

do_health_check(State) ->
    %% Check health of all remote clusters
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
    LocalCluster = State#state.local_cluster,

    lists:foreach(fun(#fed_cluster{name = Name, host = Host, port = Port}) ->
        case Name =:= LocalCluster of
            true -> ok;
            false ->
                spawn(fun() ->
                    Status = check_cluster_health(Host, Port),
                    self() ! {cluster_health, Name, Status}
                end)
        end
    end, Clusters),

    %% Update local cluster stats
    update_local_stats(LocalCluster),
    State.

check_cluster_health(Host, Port) ->
    %% Try to connect to the cluster's health endpoint
    Url = build_url(Host, Port, <<"/api/v1/health">>),
    catch flurm_metrics:increment(flurm_federation_health_checks_total),
    case http_get(Url, ?CLUSTER_TIMEOUT) of
        {ok, _Response} -> up;
        {error, _} ->
            catch flurm_metrics:increment(flurm_federation_health_check_failures_total),
            down
    end.

sync_all_clusters(State) ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
    LocalCluster = State#state.local_cluster,

    lists:foreach(fun(#fed_cluster{name = Name}) ->
        case Name =:= LocalCluster of
            true -> ok;
            false -> do_sync_cluster(Name, State)
        end
    end, Clusters).

do_sync_cluster(ClusterName, _State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth}] ->
            case fetch_cluster_stats(Host, Port, Auth) of
                {ok, Stats} ->
                    self() ! {cluster_update, ClusterName, Stats},
                    ok;
                {error, Reason} ->
                    %% Mark cluster as potentially down
                    self() ! {cluster_health, ClusterName, down},
                    {error, Reason}
            end;
        [] ->
            {error, cluster_not_found}
    end.

%%====================================================================
%% Internal Functions - Resource Aggregation
%%====================================================================

aggregate_resources(Clusters) ->
    lists:foldl(fun(#fed_cluster{state = State} = C, Acc) ->
        case State of
            up ->
                #{
                    total_nodes => maps:get(total_nodes, Acc, 0) + C#fed_cluster.node_count,
                    total_cpus => maps:get(total_cpus, Acc, 0) + C#fed_cluster.cpu_count,
                    total_memory_mb => maps:get(total_memory_mb, Acc, 0) + C#fed_cluster.memory_mb,
                    total_gpus => maps:get(total_gpus, Acc, 0) + C#fed_cluster.gpu_count,
                    available_cpus => maps:get(available_cpus, Acc, 0) + C#fed_cluster.available_cpus,
                    available_memory_mb => maps:get(available_memory_mb, Acc, 0) + C#fed_cluster.available_memory,
                    pending_jobs => maps:get(pending_jobs, Acc, 0) + C#fed_cluster.pending_jobs,
                    running_jobs => maps:get(running_jobs, Acc, 0) + C#fed_cluster.running_jobs,
                    clusters_up => maps:get(clusters_up, Acc, 0) + 1,
                    clusters_down => maps:get(clusters_down, Acc, 0)
                };
            _ ->
                Acc#{clusters_down => maps:get(clusters_down, Acc, 0) + 1}
        end
    end, #{
        total_nodes => 0,
        total_cpus => 0,
        total_memory_mb => 0,
        total_gpus => 0,
        available_cpus => 0,
        available_memory_mb => 0,
        pending_jobs => 0,
        running_jobs => 0,
        clusters_up => 0,
        clusters_down => 0
    }, Clusters).

%%====================================================================
%% Internal Functions - Remote Communication
%%====================================================================

remote_submit_job(Host, Port, Auth, Job) ->
    Url = build_url(Host, Port, <<"/api/v1/jobs">>),
    Body = jsx:encode(job_to_map(Job)),
    Headers = build_auth_headers(Auth),
    case http_post(Url, Headers, Body, ?CLUSTER_TIMEOUT) of
        {ok, Response} ->
            case jsx:decode(Response, [return_maps]) of
                #{<<"job_id">> := JobId} -> {ok, JobId};
                #{<<"error">> := Error} -> {error, Error};
                _ -> {error, invalid_response}
            end;
        Error ->
            Error
    end.

fetch_remote_job_status(Host, Port, Auth, JobId) ->
    Url = build_url(Host, Port, <<"/api/v1/jobs/", (integer_to_binary(JobId))/binary>>),
    Headers = build_auth_headers(Auth),
    case http_get(Url, Headers, ?CLUSTER_TIMEOUT) of
        {ok, Response} ->
            case jsx:decode(Response, [return_maps]) of
                #{<<"error">> := Error} -> {error, Error};
                Status when is_map(Status) -> {ok, Status}
            end;
        Error ->
            Error
    end.

fetch_cluster_stats(Host, Port, Auth) ->
    Url = build_url(Host, Port, <<"/api/v1/cluster/stats">>),
    Headers = build_auth_headers(Auth),
    case http_get(Url, Headers, ?CLUSTER_TIMEOUT) of
        {ok, Response} ->
            case jsx:decode(Response, [return_maps]) of
                Stats when is_map(Stats) -> {ok, Stats};
                _ -> {error, invalid_response}
            end;
        Error ->
            Error
    end.

fetch_federation_info(_Host) ->
    %% In production, fetch from remote cluster
    {ok, #federation{
        name = <<"default-federation">>,
        created = erlang:system_time(second),
        clusters = [],
        options = #{}
    }}.

register_with_cluster(_Host, _LocalCluster) ->
    %% In production, register this cluster with remote
    ok.

%%====================================================================
%% Internal Functions - HTTP Helpers
%%====================================================================

build_url(Host, Port, Path) ->
    <<"http://", Host/binary, ":", (integer_to_binary(Port))/binary, Path/binary>>.

build_auth_headers(#{token := Token}) ->
    [{<<"Authorization">>, <<"Bearer ", Token/binary>>}];
build_auth_headers(#{api_key := ApiKey}) ->
    [{<<"X-API-Key">>, ApiKey}];
build_auth_headers(_) ->
    [].

http_get(Url, Timeout) ->
    http_get(Url, [], Timeout).

http_get(Url, Headers, Timeout) ->
    Request = {binary_to_list(Url), headers_to_proplist(Headers)},
    case httpc:request(get, Request, [{timeout, Timeout}], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            {ok, list_to_binary(Body)};
        {ok, {{_, Status, _}, _, Body}} ->
            {error, {http_error, Status, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

http_post(Url, Headers, Body, Timeout) ->
    AllHeaders = [{<<"Content-Type">>, <<"application/json">>} | Headers],
    Request = {
        binary_to_list(Url),
        headers_to_proplist(AllHeaders),
        "application/json",
        binary_to_list(Body)
    },
    case httpc:request(post, Request, [{timeout, Timeout}], []) of
        {ok, {{_, 200, _}, _, ResponseBody}} ->
            {ok, list_to_binary(ResponseBody)};
        {ok, {{_, 201, _}, _, ResponseBody}} ->
            {ok, list_to_binary(ResponseBody)};
        {ok, {{_, Status, _}, _, ResponseBody}} ->
            {error, {http_error, Status, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.

headers_to_proplist(Headers) ->
    [{binary_to_list(K), binary_to_list(V)} || {K, V} <- Headers].

%%====================================================================
%% Internal Functions - Settings Management
%%====================================================================

%% @doc Apply federation settings to state.
%% Supports: routing_policy, sync_interval, health_check_interval
-spec apply_settings(map(), #state{}) -> #state{}.
apply_settings(Settings, State) ->
    State1 = case maps:find(routing_policy, Settings) of
        {ok, PolicyBin} when is_binary(PolicyBin) ->
            Policy = try binary_to_existing_atom(PolicyBin, utf8)
                     catch _:_ -> State#state.routing_policy
                     end,
            State#state{routing_policy = Policy};
        {ok, Policy} when is_atom(Policy) ->
            State#state{routing_policy = Policy};
        error ->
            State
    end,
    State2 = case maps:find(sync_interval, Settings) of
        {ok, Interval} when is_integer(Interval), Interval > 0 ->
            %% Cancel old timer and start new one
            case State1#state.sync_timer of
                undefined -> ok;
                OldTimer -> erlang:cancel_timer(OldTimer)
            end,
            NewTimer = erlang:send_after(Interval, self(), sync_clusters),
            State1#state{sync_timer = NewTimer};
        _ ->
            State1
    end,
    State3 = case maps:find(health_check_interval, Settings) of
        {ok, HCInterval} when is_integer(HCInterval), HCInterval > 0 ->
            %% Cancel old timer and start new one
            case State2#state.health_timer of
                undefined -> ok;
                OldHCTimer -> erlang:cancel_timer(OldHCTimer)
            end,
            NewHCTimer = erlang:send_after(HCInterval, self(), health_check),
            State2#state{health_timer = NewHCTimer};
        _ ->
            State2
    end,
    State3.

%%====================================================================
%% Internal Functions - Helpers
%%====================================================================

get_local_cluster_name() ->
    application:get_env(flurm_core, cluster_name, <<"default">>).

get_local_host() ->
    case application:get_env(flurm_core, controller_host) of
        {ok, Host} when is_list(Host) -> list_to_binary(Host);
        {ok, Host} when is_binary(Host) -> Host;
        undefined -> <<"localhost">>
    end.

get_local_port() ->
    application:get_env(flurm_core, controller_port, 6817).

get_local_partitions() ->
    case catch flurm_partition_registry:list_partitions() of
        Partitions when is_list(Partitions) ->
            [maps:get(name, P, <<>>) || P <- Partitions];
        _ ->
            []
    end.

get_routing_policy() ->
    application:get_env(flurm_core, federation_routing_policy, least_loaded).

register_local_partitions(LocalCluster) ->
    Partitions = get_local_partitions(),
    lists:foreach(fun(P) ->
        ets:insert(?FED_PARTITION_MAP, #partition_map{
            partition = P,
            cluster = LocalCluster,
            priority = 100  % High priority for local
        })
    end, Partitions).

update_local_stats(ClusterName) ->
    Stats = collect_local_stats(),
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = update_cluster_from_stats(Cluster, Stats),
            ets:insert(?FED_CLUSTERS_TABLE, Updated);
        [] ->
            ok
    end.

collect_local_stats() ->
    NodeCount = case catch flurm_node_registry:count_total_nodes() of
        N when is_integer(N) -> N;
        _ -> 0
    end,
    JobCounts = case catch flurm_job_registry:count_by_state() of
        Counts when is_map(Counts) -> Counts;
        _ -> #{}
    end,
    %% Try to get more detailed resource info
    {TotalCpus, AvailCpus, TotalMem, AvailMem, GpuCount} =
        case catch flurm_node_registry:get_aggregate_resources() of
            #{total_cpus := TC, available_cpus := AC,
              total_memory := TM, available_memory := AM,
              total_gpus := TG} ->
                {TC, AC, TM, AM, TG};
            _ ->
                %% Estimate from node count
                {NodeCount * 32, NodeCount * 16, NodeCount * 64000, NodeCount * 32000, 0}
        end,
    #{
        node_count => NodeCount,
        cpu_count => TotalCpus,
        available_cpus => AvailCpus,
        memory_mb => TotalMem,
        available_memory => AvailMem,
        gpu_count => GpuCount,
        pending_jobs => maps:get(pending, JobCounts, 0),
        running_jobs => maps:get(running, JobCounts, 0)
    }.

update_cluster_from_stats(Cluster, Stats) ->
    Cluster#fed_cluster{
        node_count = maps:get(node_count, Stats, Cluster#fed_cluster.node_count),
        cpu_count = maps:get(cpu_count, Stats, Cluster#fed_cluster.cpu_count),
        memory_mb = maps:get(memory_mb, Stats, Cluster#fed_cluster.memory_mb),
        gpu_count = maps:get(gpu_count, Stats, Cluster#fed_cluster.gpu_count),
        available_cpus = maps:get(available_cpus, Stats, Cluster#fed_cluster.available_cpus),
        available_memory = maps:get(available_memory, Stats, Cluster#fed_cluster.available_memory),
        pending_jobs = maps:get(pending_jobs, Stats, Cluster#fed_cluster.pending_jobs),
        running_jobs = maps:get(running_jobs, Stats, Cluster#fed_cluster.running_jobs),
        last_sync = erlang:system_time(second),
        state = up
    }.

cluster_to_map(#fed_cluster{} = C) ->
    #{
        name => C#fed_cluster.name,
        host => C#fed_cluster.host,
        port => C#fed_cluster.port,
        state => C#fed_cluster.state,
        weight => C#fed_cluster.weight,
        features => C#fed_cluster.features,
        partitions => C#fed_cluster.partitions,
        node_count => C#fed_cluster.node_count,
        cpu_count => C#fed_cluster.cpu_count,
        memory_mb => C#fed_cluster.memory_mb,
        gpu_count => C#fed_cluster.gpu_count,
        pending_jobs => C#fed_cluster.pending_jobs,
        running_jobs => C#fed_cluster.running_jobs,
        available_cpus => C#fed_cluster.available_cpus,
        available_memory => C#fed_cluster.available_memory,
        last_sync => C#fed_cluster.last_sync,
        last_health_check => C#fed_cluster.last_health_check,
        consecutive_failures => C#fed_cluster.consecutive_failures
    }.

generate_local_ref() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"ref-", Timestamp/binary, "-", Random/binary>>.

generate_federation_id() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"fed-", Timestamp/binary, "-", Random/binary>>.

%% Job helper functions
get_job_partition(#job{partition = P}) -> P;
get_job_partition(#{partition := P}) -> P;
get_job_partition(_) -> undefined.

get_job_features(#job{}) -> [];  % Jobs don't have features field directly
get_job_features(#{features := F}) -> F;
get_job_features(_) -> [].

get_job_cpus(#job{num_cpus = C}) -> C;
get_job_cpus(#{num_cpus := C}) -> C;
get_job_cpus(_) -> 1.

get_job_memory(#job{memory_mb = M}) -> M;
get_job_memory(#{memory_mb := M}) -> M;
get_job_memory(_) -> 1024.

job_to_map(#job{} = J) ->
    #{
        id => J#job.id,
        name => J#job.name,
        user => J#job.user,
        partition => J#job.partition,
        state => J#job.state,
        script => J#job.script,
        num_nodes => J#job.num_nodes,
        num_cpus => J#job.num_cpus,
        memory_mb => J#job.memory_mb,
        time_limit => J#job.time_limit,
        priority => J#job.priority,
        work_dir => J#job.work_dir,
        account => J#job.account,
        qos => J#job.qos
    };
job_to_map(M) when is_map(M) ->
    M.

map_to_job(M) when is_map(M) ->
    #job{
        id = maps:get(id, M, 0),
        name = maps:get(name, M, <<"unnamed">>),
        user = maps:get(user, M, <<"unknown">>),
        partition = maps:get(partition, M, <<"default">>),
        state = maps:get(state, M, pending),
        script = maps:get(script, M, <<>>),
        num_nodes = maps:get(num_nodes, M, 1),
        num_cpus = maps:get(num_cpus, M, 1),
        memory_mb = maps:get(memory_mb, M, 1024),
        time_limit = maps:get(time_limit, M, 3600),
        priority = maps:get(priority, M, 100),
        work_dir = maps:get(work_dir, M, <<"/tmp">>),
        account = maps:get(account, M, <<>>),
        qos = maps:get(qos, M, <<"normal">>)
    }.

has_required_features(ClusterFeatures, RequiredFeatures) ->
    lists:all(fun(F) -> lists:member(F, ClusterFeatures) end, RequiredFeatures).

%%====================================================================
%% Legacy Internal Functions
%%====================================================================

do_submit_federated_job(Job, Options, State) ->
    FederationId = generate_federation_id(),
    RequiredFeatures = maps:get(features, Options, []),
    ClusterConstraint = maps:get(clusters, Options, any),

    %% Find suitable clusters
    SuitableClusters = find_suitable_clusters_legacy(RequiredFeatures, ClusterConstraint),

    case SuitableClusters of
        [] ->
            {error, no_suitable_cluster};
        _ ->
            %% Create sibling jobs on all suitable clusters
            SiblingJobs = lists:foldl(fun(ClusterName, Acc) ->
                case submit_to_cluster(ClusterName, Job) of
                    {ok, JobId} -> maps:put(ClusterName, JobId, Acc);
                    {error, _} -> Acc
                end
            end, #{}, SuitableClusters),

            case maps:size(SiblingJobs) of
                0 ->
                    {error, submission_failed};
                _ ->
                    %% Record federated job
                    FedJob = #fed_job{
                        id = {State#state.local_cluster, maps:get(State#state.local_cluster, SiblingJobs, 0)},
                        federation_id = FederationId,
                        origin_cluster = State#state.local_cluster,
                        sibling_clusters = maps:keys(SiblingJobs),
                        sibling_jobs = SiblingJobs,
                        state = pending,
                        submit_time = erlang:system_time(second),
                        features_required = RequiredFeatures,
                        cluster_constraint = ClusterConstraint
                    },
                    ets:insert(?FED_JOBS_TABLE, FedJob),
                    {ok, FederationId}
            end
    end.

find_suitable_clusters_legacy(RequiredFeatures, ClusterConstraint) ->
    AllClusters = ets:tab2list(?FED_CLUSTERS_TABLE),

    %% Filter by constraint
    ConstrainedClusters = case ClusterConstraint of
        any -> AllClusters;
        Names when is_list(Names) ->
            [C || C <- AllClusters, lists:member(C#fed_cluster.name, Names)]
    end,

    %% Filter by features and state
    [C#fed_cluster.name || C <- ConstrainedClusters,
        C#fed_cluster.state =:= up,
        has_required_features(C#fed_cluster.features, RequiredFeatures)].

submit_to_cluster(ClusterName, Job) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth}] ->
            remote_submit_job(Host, Port, Auth, Job);
        [] ->
            {error, cluster_not_found}
    end.

%%====================================================================
%% API - Sibling Job Coordination (Phase 7D)
%%====================================================================
%%
%% TLA+ Safety Invariants enforced:
%%   - SiblingExclusivity: At most one sibling runs at any time
%%   - OriginAwareness: Origin cluster tracks which sibling is running
%%   - NoJobLoss: Jobs must have at least one active sibling or be terminal
%%
%% Protocol flow:
%%   1. submit_federated_job -> create_sibling_jobs on suitable clusters
%%   2. Each cluster independently attempts to schedule its sibling
%%   3. When a sibling starts: notify_job_started -> origin revokes other siblings
%%   4. Other clusters receive MSG_FED_SIBLING_REVOKE -> cancel their sibling
%%   5. When job completes/fails: origin is notified via MSG_FED_JOB_COMPLETED/FAILED
%%====================================================================

%% @doc Create sibling jobs on federated clusters for a given job spec.
%% This implements the first step of the federation job submission protocol.
%% Returns {ok, FederationJobId} on success with the federation-wide job identifier.
%%
%% TLA+ Invariant: NoJobLoss - at least one sibling must be created
-spec create_sibling_jobs(map(), [cluster_name()]) -> {ok, binary()} | {error, term()}.
create_sibling_jobs(JobSpec, TargetClusters) when is_map(JobSpec), is_list(TargetClusters) ->
    gen_server:call(?SERVER, {create_sibling_jobs, JobSpec, TargetClusters}, ?CLUSTER_TIMEOUT * 2).

%% @doc Notify the origin cluster that a sibling job has started running.
%% This triggers revocation of all other siblings to maintain SiblingExclusivity.
%%
%% TLA+ Invariant: SiblingExclusivity - this is the trigger for revocation
-spec notify_job_started(binary(), non_neg_integer()) -> ok | {error, term()}.
notify_job_started(FederationJobId, LocalJobId) when is_binary(FederationJobId) ->
    gen_server:call(?SERVER, {notify_job_started, FederationJobId, LocalJobId}).

%% @doc Revoke sibling jobs on other clusters (called by origin after receiving start notification).
%% This sends MSG_FED_SIBLING_REVOKE to all clusters except the one running the job.
%%
%% TLA+ Invariant: SiblingExclusivity - ensures only one sibling runs
-spec revoke_siblings(binary(), cluster_name()) -> ok | {error, term()}.
revoke_siblings(FederationJobId, RunningCluster) when is_binary(FederationJobId), is_binary(RunningCluster) ->
    gen_server:call(?SERVER, {revoke_siblings, FederationJobId, RunningCluster}).

%% @doc Handle incoming sibling revocation request.
%% Cancels the local sibling job if it hasn't started running.
-spec handle_sibling_revoke(#fed_sibling_revoke_msg{}) -> ok | {error, term()}.
handle_sibling_revoke(#fed_sibling_revoke_msg{} = Msg) ->
    gen_server:call(?SERVER, {handle_sibling_revoke, Msg}).

%% @doc Get the cluster currently running a federated job.
%% Returns {ok, ClusterName} if a sibling is running, {ok, undefined} if not started.
%%
%% TLA+ Invariant: OriginAwareness - origin tracks which sibling is running
-spec get_running_cluster(binary()) -> {ok, cluster_name() | undefined} | {error, term()}.
get_running_cluster(FederationJobId) when is_binary(FederationJobId) ->
    gen_server:call(?SERVER, {get_running_cluster, FederationJobId}).

%% @doc Get the state of a sibling job on a specific cluster.
%% Returns the sibling job state record with current status.
-spec get_sibling_job_state(binary(), cluster_name()) -> {ok, #sibling_job_state{}} | {error, term()}.
get_sibling_job_state(FederationJobId, ClusterName) when is_binary(FederationJobId), is_binary(ClusterName) ->
    gen_server:call(?SERVER, {get_sibling_job_state, FederationJobId, ClusterName}).

%%====================================================================
%% Internal - Sibling Job Coordination
%%====================================================================

%% @doc Create sibling jobs on multiple clusters (internal implementation).
%% Called via gen_server to ensure atomicity.
-spec do_create_sibling_jobs(map(), [cluster_name()], #state{}) -> {ok, binary()} | {error, term()}.
do_create_sibling_jobs(JobSpec, TargetClusters, State) ->
    %% Generate federation-wide job ID
    FederationJobId = generate_federation_id(),
    OriginCluster = State#state.local_cluster,
    SubmitTime = erlang:system_time(second),

    %% Initialize sibling states map
    InitialSiblingStates = lists:foldl(fun(Cluster, Acc) ->
        SiblingState = #sibling_job_state{
            federation_job_id = FederationJobId,
            sibling_cluster = Cluster,
            origin_cluster = OriginCluster,
            local_job_id = 0,
            state = ?SIBLING_STATE_NULL,
            submit_time = SubmitTime
        },
        maps:put(Cluster, SiblingState, Acc)
    end, #{}, TargetClusters),

    %% Create federation job tracker at origin
    Tracker = #fed_job_tracker{
        federation_job_id = FederationJobId,
        origin_cluster = OriginCluster,
        origin_job_id = 0,  % Will be set when local sibling is created
        running_cluster = undefined,
        sibling_states = InitialSiblingStates,
        submit_time = SubmitTime,
        job_spec = JobSpec
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    %% Send sibling creation messages to each target cluster
    SuccessCount = lists:foldl(fun(Cluster, Count) ->
        Msg = #fed_job_submit_msg{
            federation_job_id = FederationJobId,
            origin_cluster = OriginCluster,
            target_cluster = Cluster,
            job_spec = JobSpec,
            submit_time = SubmitTime
        },
        case send_sibling_create_msg(Cluster, Msg, State) of
            ok ->
                %% Update sibling state to PENDING
                update_sibling_state(FederationJobId, Cluster, ?SIBLING_STATE_PENDING),
                Count + 1;
            {error, Reason} ->
                lager:warning("Failed to create sibling on ~s: ~p", [Cluster, Reason]),
                Count
        end
    end, 0, TargetClusters),

    %% TLA+ NoJobLoss: Ensure at least one sibling was created
    case SuccessCount of
        0 ->
            %% No siblings created - remove tracker and return error
            ets:delete(?FED_SIBLING_JOBS, FederationJobId),
            {error, no_siblings_created};
        _ ->
            catch flurm_metrics:increment(flurm_federation_sibling_jobs_created_total, SuccessCount),
            {ok, FederationJobId}
    end.

%% @doc Send a sibling creation message to a cluster.
-spec send_sibling_create_msg(cluster_name(), #fed_job_submit_msg{}, #state{}) -> ok | {error, term()}.
send_sibling_create_msg(ClusterName, Msg, State) ->
    case ClusterName =:= State#state.local_cluster of
        true ->
            %% Local cluster - submit directly
            handle_local_sibling_create(Msg);
        false ->
            %% Remote cluster - send via protocol
            send_federation_msg(ClusterName, ?MSG_FED_JOB_SUBMIT, Msg, State)
    end.

%% @doc Handle creating a sibling job locally.
-spec handle_local_sibling_create(#fed_job_submit_msg{}) -> ok | {error, term()}.
handle_local_sibling_create(#fed_job_submit_msg{federation_job_id = FedJobId, job_spec = JobSpec}) ->
    %% Convert job spec to job record and submit
    Job = map_to_job(JobSpec),
    case catch flurm_scheduler:submit_job(Job) of
        {ok, LocalJobId} ->
            %% Update local sibling state with job ID
            update_sibling_state(FedJobId, get_local_cluster_name(), ?SIBLING_STATE_PENDING),
            update_sibling_local_job_id(FedJobId, get_local_cluster_name(), LocalJobId),
            ok;
        {error, Reason} ->
            {error, Reason};
        {'EXIT', Reason} ->
            {error, {scheduler_not_available, Reason}}
    end.

%% @doc Handle notification that a sibling job started (internal implementation).
-spec do_notify_job_started(binary(), non_neg_integer(), #state{}) -> ok | {error, term()}.
do_notify_job_started(FederationJobId, LocalJobId, State) ->
    LocalCluster = State#state.local_cluster,
    StartTime = erlang:system_time(second),

    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{origin_cluster = OriginCluster} = Tracker] ->
            %% Update local sibling state to RUNNING
            update_sibling_state(FederationJobId, LocalCluster, ?SIBLING_STATE_RUNNING),
            update_sibling_start_time(FederationJobId, LocalCluster, StartTime),

            case LocalCluster =:= OriginCluster of
                true ->
                    %% We are the origin - directly revoke other siblings
                    %% TLA+ SiblingExclusivity: At most one sibling runs
                    UpdatedTracker = Tracker#fed_job_tracker{running_cluster = LocalCluster},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker),
                    do_revoke_siblings(FederationJobId, LocalCluster, State);
                false ->
                    %% Notify origin cluster that we started
                    Msg = #fed_job_started_msg{
                        federation_job_id = FederationJobId,
                        running_cluster = LocalCluster,
                        local_job_id = LocalJobId,
                        start_time = StartTime
                    },
                    send_federation_msg(OriginCluster, ?MSG_FED_JOB_STARTED, Msg, State)
            end;
        [] ->
            {error, federation_job_not_found}
    end.

%% @doc Revoke siblings on all clusters except the running one (internal implementation).
%% TLA+ SiblingExclusivity: This ensures only one sibling runs at any time.
-spec do_revoke_siblings(binary(), cluster_name(), #state{}) -> ok | {error, term()}.
do_revoke_siblings(FederationJobId, RunningCluster, State) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = SiblingStates} = Tracker] ->
            %% Update tracker with running cluster
            %% TLA+ OriginAwareness: Origin tracks which sibling is running
            UpdatedTracker = Tracker#fed_job_tracker{running_cluster = RunningCluster},
            ets:insert(?FED_SIBLING_JOBS, UpdatedTracker),

            %% Send revoke messages to all other clusters with non-terminal siblings
            RevokeMsg = #fed_sibling_revoke_msg{
                federation_job_id = FederationJobId,
                running_cluster = RunningCluster,
                revoke_reason = <<"sibling_started">>
            },

            maps:fold(fun(Cluster, #sibling_job_state{state = SibState}, Acc) ->
                case Cluster =:= RunningCluster of
                    true ->
                        Acc;  % Don't revoke the running sibling
                    false ->
                        case is_sibling_revocable(SibState) of
                            true ->
                                case Cluster =:= State#state.local_cluster of
                                    true ->
                                        %% Local revocation
                                        do_handle_sibling_revoke(RevokeMsg, State);
                                    false ->
                                        %% Remote revocation
                                        send_federation_msg(Cluster, ?MSG_FED_SIBLING_REVOKE, RevokeMsg, State)
                                end,
                                Acc + 1;
                            false ->
                                Acc  % Already terminal, skip
                        end
                end
            end, 0, SiblingStates),

            catch flurm_metrics:increment(flurm_federation_sibling_revocations_total),
            ok;
        [] ->
            {error, federation_job_not_found}
    end.

%% @doc Check if a sibling state is revocable (not already terminal or running elsewhere).
-spec is_sibling_revocable(non_neg_integer()) -> boolean().
is_sibling_revocable(?SIBLING_STATE_PENDING) -> true;
is_sibling_revocable(?SIBLING_STATE_NULL) -> true;
is_sibling_revocable(_) -> false.

%% @doc Handle incoming sibling revocation (internal implementation).
-spec do_handle_sibling_revoke(#fed_sibling_revoke_msg{}, #state{}) -> ok | {error, term()}.
do_handle_sibling_revoke(#fed_sibling_revoke_msg{
    federation_job_id = FederationJobId,
    running_cluster = RunningCluster
}, State) ->
    LocalCluster = State#state.local_cluster,

    %% Don't revoke if we are the running cluster
    case LocalCluster =:= RunningCluster of
        true ->
            ok;
        false ->
            case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
                [#fed_job_tracker{sibling_states = SiblingStates}] ->
                    case maps:get(LocalCluster, SiblingStates, undefined) of
                        undefined ->
                            ok;  % No local sibling
                        #sibling_job_state{state = SibState, local_job_id = LocalJobId} ->
                            case is_sibling_revocable(SibState) of
                                true ->
                                    %% Cancel the local job if it has a valid ID
                                    case LocalJobId > 0 of
                                        true ->
                                            catch flurm_scheduler:cancel_job(LocalJobId);
                                        false ->
                                            ok
                                    end,
                                    %% Update state to REVOKED
                                    %% TLA+ RevokedIsTerminal: Once revoked, stays revoked
                                    update_sibling_state(FederationJobId, LocalCluster, ?SIBLING_STATE_REVOKED),
                                    catch flurm_metrics:increment(flurm_federation_sibling_revoked_total),
                                    ok;
                                false ->
                                    %% Already running or terminal - cannot revoke
                                    ok
                            end
                    end;
                [] ->
                    %% No tracker - might be received before sibling create
                    %% Store the revocation intent for later
                    ok
            end
    end.

%% @doc Update a sibling's state in the tracker.
-spec update_sibling_state(binary(), cluster_name(), non_neg_integer()) -> ok.
update_sibling_state(FederationJobId, Cluster, NewState) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = States} = Tracker] ->
            case maps:get(Cluster, States, undefined) of
                undefined ->
                    ok;
                SibState ->
                    UpdatedSibState = SibState#sibling_job_state{state = NewState},
                    UpdatedStates = maps:put(Cluster, UpdatedSibState, States),
                    UpdatedTracker = Tracker#fed_job_tracker{sibling_states = UpdatedStates},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker)
            end;
        [] ->
            ok
    end.

%% @doc Update a sibling's local job ID in the tracker.
-spec update_sibling_local_job_id(binary(), cluster_name(), non_neg_integer()) -> ok.
update_sibling_local_job_id(FederationJobId, Cluster, LocalJobId) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = States} = Tracker] ->
            case maps:get(Cluster, States, undefined) of
                undefined ->
                    ok;
                SibState ->
                    UpdatedSibState = SibState#sibling_job_state{local_job_id = LocalJobId},
                    UpdatedStates = maps:put(Cluster, UpdatedSibState, States),
                    UpdatedTracker = Tracker#fed_job_tracker{sibling_states = UpdatedStates},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker)
            end;
        [] ->
            ok
    end.

%% @doc Update a sibling's start time in the tracker.
-spec update_sibling_start_time(binary(), cluster_name(), non_neg_integer()) -> ok.
update_sibling_start_time(FederationJobId, Cluster, StartTime) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = States} = Tracker] ->
            case maps:get(Cluster, States, undefined) of
                undefined ->
                    ok;
                SibState ->
                    UpdatedSibState = SibState#sibling_job_state{start_time = StartTime},
                    UpdatedStates = maps:put(Cluster, UpdatedSibState, States),
                    UpdatedTracker = Tracker#fed_job_tracker{sibling_states = UpdatedStates},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker)
            end;
        [] ->
            ok
    end.

%% @doc Send a federation message to a cluster.
-spec send_federation_msg(cluster_name(), non_neg_integer(), term(), #state{}) -> ok | {error, term()}.
send_federation_msg(ClusterName, MsgType, Msg, _State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth, state = up}] ->
            %% Encode the message
            case flurm_protocol_codec:encode(MsgType, Msg) of
                {ok, EncodedMsg} ->
                    %% Send via HTTP (federation messages use REST API)
                    Url = build_url(Host, Port, <<"/api/v1/federation/message">>),
                    Body = jsx:encode(#{
                        msg_type => MsgType,
                        payload => base64:encode(EncodedMsg)
                    }),
                    Headers = build_auth_headers(Auth),
                    case http_post(Url, Headers, Body, ?CLUSTER_TIMEOUT) of
                        {ok, _Response} ->
                            ok;
                        {error, Reason} ->
                            {error, {send_failed, Reason}}
                    end;
                {error, Reason} ->
                    {error, {encode_failed, Reason}}
            end;
        [#fed_cluster{state = State}] ->
            {error, {cluster_unavailable, State}};
        [] ->
            {error, cluster_not_found}
    end.

%% @doc Handle incoming federation message (called from controller).
-spec handle_federation_message(non_neg_integer(), binary()) -> ok | {error, term()}.
handle_federation_message(MsgType, EncodedMsg) ->
    gen_server:call(?SERVER, {handle_federation_message, MsgType, EncodedMsg}).

%% @doc Process incoming federation message (internal).
-spec do_handle_federation_message(non_neg_integer(), binary(), #state{}) -> ok | {error, term()}.
do_handle_federation_message(MsgType, EncodedMsg, State) ->
    case flurm_protocol_codec:decode_body(MsgType, EncodedMsg) of
        {ok, Msg} ->
            case MsgType of
                ?MSG_FED_JOB_SUBMIT ->
                    handle_local_sibling_create(Msg);
                ?MSG_FED_JOB_STARTED ->
                    handle_remote_job_started(Msg, State);
                ?MSG_FED_SIBLING_REVOKE ->
                    do_handle_sibling_revoke(Msg, State);
                ?MSG_FED_JOB_COMPLETED ->
                    handle_job_completed(Msg, State);
                ?MSG_FED_JOB_FAILED ->
                    handle_job_failed(Msg, State);
                _ ->
                    {error, {unknown_message_type, MsgType}}
            end;
        {error, Reason} ->
            {error, {decode_failed, Reason}}
    end.

%% @doc Handle remote job started notification (at origin).
-spec handle_remote_job_started(#fed_job_started_msg{}, #state{}) -> ok | {error, term()}.
handle_remote_job_started(#fed_job_started_msg{
    federation_job_id = FederationJobId,
    running_cluster = RunningCluster,
    local_job_id = LocalJobId,
    start_time = StartTime
}, State) ->
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{origin_cluster = OriginCluster} = Tracker] ->
            case State#state.local_cluster =:= OriginCluster of
                true ->
                    %% We are origin - update tracker and revoke other siblings
                    UpdatedTracker = Tracker#fed_job_tracker{running_cluster = RunningCluster},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker),

                    %% Update sibling state
                    update_sibling_state(FederationJobId, RunningCluster, ?SIBLING_STATE_RUNNING),
                    update_sibling_local_job_id(FederationJobId, RunningCluster, LocalJobId),
                    update_sibling_start_time(FederationJobId, RunningCluster, StartTime),

                    %% Revoke all other siblings
                    %% TLA+ SiblingExclusivity: At most one sibling runs
                    do_revoke_siblings(FederationJobId, RunningCluster, State);
                false ->
                    %% Not origin - forward to origin
                    Msg = #fed_job_started_msg{
                        federation_job_id = FederationJobId,
                        running_cluster = RunningCluster,
                        local_job_id = LocalJobId,
                        start_time = StartTime
                    },
                    send_federation_msg(OriginCluster, ?MSG_FED_JOB_STARTED, Msg, State)
            end;
        [] ->
            {error, federation_job_not_found}
    end.

%% @doc Handle job completed notification.
-spec handle_job_completed(#fed_job_completed_msg{}, #state{}) -> ok | {error, term()}.
handle_job_completed(#fed_job_completed_msg{
    federation_job_id = FederationJobId,
    running_cluster = RunningCluster,
    end_time = EndTime,
    exit_code = ExitCode
}, _State) ->
    update_sibling_state(FederationJobId, RunningCluster, ?SIBLING_STATE_COMPLETED),
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = States} = Tracker] ->
            case maps:get(RunningCluster, States, undefined) of
                undefined ->
                    ok;
                SibState ->
                    UpdatedSibState = SibState#sibling_job_state{
                        state = ?SIBLING_STATE_COMPLETED,
                        end_time = EndTime,
                        exit_code = ExitCode
                    },
                    UpdatedStates = maps:put(RunningCluster, UpdatedSibState, States),
                    UpdatedTracker = Tracker#fed_job_tracker{sibling_states = UpdatedStates},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker)
            end;
        [] ->
            ok
    end,
    catch flurm_metrics:increment(flurm_federation_sibling_completed_total),
    ok.

%% @doc Handle job failed notification.
-spec handle_job_failed(#fed_job_failed_msg{}, #state{}) -> ok | {error, term()}.
handle_job_failed(#fed_job_failed_msg{
    federation_job_id = FederationJobId,
    running_cluster = RunningCluster,
    end_time = EndTime,
    exit_code = ExitCode
}, _State) ->
    update_sibling_state(FederationJobId, RunningCluster, ?SIBLING_STATE_FAILED),
    case ets:lookup(?FED_SIBLING_JOBS, FederationJobId) of
        [#fed_job_tracker{sibling_states = States} = Tracker] ->
            case maps:get(RunningCluster, States, undefined) of
                undefined ->
                    ok;
                SibState ->
                    UpdatedSibState = SibState#sibling_job_state{
                        state = ?SIBLING_STATE_FAILED,
                        end_time = EndTime,
                        exit_code = ExitCode
                    },
                    UpdatedStates = maps:put(RunningCluster, UpdatedSibState, States),
                    UpdatedTracker = Tracker#fed_job_tracker{sibling_states = UpdatedStates},
                    ets:insert(?FED_SIBLING_JOBS, UpdatedTracker)
            end;
        [] ->
            ok
    end,
    catch flurm_metrics:increment(flurm_federation_sibling_failed_total),
    ok.
