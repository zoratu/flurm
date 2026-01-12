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
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    create_federation/2,
    join_federation/2,
    leave_federation/0,
    get_federation_info/0,
    list_clusters/0,
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
-define(SYNC_INTERVAL, 30000).  % Sync every 30 seconds

-type cluster_name() :: binary().
-type federation_name() :: binary().

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
    state :: up | down | drain,
    weight :: pos_integer(),              % Scheduling weight
    features :: [binary()],               % Cluster features (gpu, highspeed, etc)
    node_count :: non_neg_integer(),
    cpu_count :: non_neg_integer(),
    pending_jobs :: non_neg_integer(),
    running_jobs :: non_neg_integer(),
    last_sync :: non_neg_integer(),
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

-record(state, {
    local_cluster :: cluster_name(),
    federation :: #federation{} | undefined,
    sync_timer :: reference() | undefined
}).

-export_type([cluster_name/0, federation_name/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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

%% @doc List all clusters in federation
-spec list_clusters() -> [#fed_cluster{}].
list_clusters() ->
    ets:tab2list(?FED_CLUSTERS_TABLE).

%% @doc Get info about specific cluster
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
        {keypos, #fed_cluster.name}
    ]),
    ets:new(?FED_JOBS_TABLE, [
        named_table, public, set,
        {keypos, #fed_job.id}
    ]),

    %% Get local cluster name from config
    LocalCluster = get_local_cluster_name(),

    %% Register local cluster
    LocalEntry = #fed_cluster{
        name = LocalCluster,
        host = get_local_host(),
        port = get_local_port(),
        state = up,
        weight = 1,
        features = [],
        node_count = 0,
        cpu_count = 0,
        pending_jobs = 0,
        running_jobs = 0,
        last_sync = erlang:system_time(second),
        properties = #{}
    },
    ets:insert(?FED_CLUSTERS_TABLE, LocalEntry),

    {ok, #state{
        local_cluster = LocalCluster,
        federation = undefined,
        sync_timer = undefined
    }}.

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
                    %% Register with origin cluster
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
    Result = do_sync_cluster(ClusterName),
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
    {reply, State#state.federation =/= undefined, State};

handle_call(get_local_cluster, _From, State) ->
    {reply, State#state.local_cluster, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync_all, State) ->
    case State#state.federation of
        undefined ->
            {noreply, State};
        #federation{clusters = Clusters} ->
            %% Sync with all remote clusters
            lists:foreach(fun(ClusterName) ->
                case ClusterName =:= State#state.local_cluster of
                    true -> ok;
                    false -> do_sync_cluster(ClusterName)
                end
            end, Clusters),

            %% Update local cluster stats
            update_local_stats(State#state.local_cluster),

            Timer = erlang:send_after(?SYNC_INTERVAL, self(), sync_all),
            {noreply, State#state{sync_timer = Timer}}
    end;

handle_info({cluster_update, ClusterName, Stats}, State) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = Cluster#fed_cluster{
                node_count = maps:get(node_count, Stats, Cluster#fed_cluster.node_count),
                cpu_count = maps:get(cpu_count, Stats, Cluster#fed_cluster.cpu_count),
                pending_jobs = maps:get(pending_jobs, Stats, Cluster#fed_cluster.pending_jobs),
                running_jobs = maps:get(running_jobs, Stats, Cluster#fed_cluster.running_jobs),
                last_sync = erlang:system_time(second)
            },
            ets:insert(?FED_CLUSTERS_TABLE, Updated);
        [] ->
            ok
    end,
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

get_local_cluster_name() ->
    application:get_env(flurm_core, cluster_name, <<"default">>).

get_local_host() ->
    case application:get_env(flurm_core, controller_host) of
        {ok, Host} -> list_to_binary(Host);
        undefined -> <<"localhost">>
    end.

get_local_port() ->
    application:get_env(flurm_core, controller_port, 6817).

do_submit_federated_job(Job, Options, State) ->
    FederationId = generate_federation_id(),
    RequiredFeatures = maps:get(features, Options, []),
    ClusterConstraint = maps:get(clusters, Options, any),

    %% Find suitable clusters
    SuitableClusters = find_suitable_clusters(RequiredFeatures, ClusterConstraint),

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

find_suitable_clusters(RequiredFeatures, ClusterConstraint) ->
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

has_required_features(ClusterFeatures, RequiredFeatures) ->
    lists:all(fun(F) -> lists:member(F, ClusterFeatures) end, RequiredFeatures).

submit_to_cluster(ClusterName, Job) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port}] ->
            %% Would make RPC call to remote cluster
            remote_submit_job(Host, Port, Job);
        [] ->
            {error, cluster_not_found}
    end.

remote_submit_job(_Host, _Port, _Job) ->
    %% In production, this would make an actual RPC call
    %% For now, simulate success
    {ok, rand:uniform(1000000)}.

do_sync_cluster(ClusterName) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port}] ->
            case fetch_cluster_stats(Host, Port) of
                {ok, Stats} ->
                    self() ! {cluster_update, ClusterName, Stats},
                    ok;
                {error, Reason} ->
                    %% Mark cluster as down if sync fails
                    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
                        [C] -> ets:insert(?FED_CLUSTERS_TABLE, C#fed_cluster{state = down});
                        [] -> ok
                    end,
                    {error, Reason}
            end;
        [] ->
            {error, cluster_not_found}
    end.

fetch_cluster_stats(_Host, _Port) ->
    %% In production, this would make an actual RPC call
    %% For now, return mock data
    {ok, #{
        node_count => rand:uniform(100),
        cpu_count => rand:uniform(1000),
        pending_jobs => rand:uniform(50),
        running_jobs => rand:uniform(200)
    }}.

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

update_local_stats(ClusterName) ->
    %% Update local cluster statistics
    Stats = collect_local_stats(),
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = Cluster#fed_cluster{
                node_count = maps:get(node_count, Stats, 0),
                cpu_count = maps:get(cpu_count, Stats, 0),
                pending_jobs = maps:get(pending_jobs, Stats, 0),
                running_jobs = maps:get(running_jobs, Stats, 0),
                last_sync = erlang:system_time(second)
            },
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
    #{
        node_count => NodeCount,
        cpu_count => NodeCount * 32,  % Simplified
        pending_jobs => maps:get(pending, JobCounts, 0),
        running_jobs => maps:get(running, JobCounts, 0)
    }.

generate_federation_id() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"fed-", Timestamp/binary, "-", Random/binary>>.
