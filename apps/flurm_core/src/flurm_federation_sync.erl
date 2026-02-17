%%%-------------------------------------------------------------------
%%% @doc FLURM Federation Synchronization
%%%
%%% Handles cluster synchronization, health checks, state replication,
%%% and sibling job coordination for multi-cluster federation.
%%%
%%% This module is used by flurm_federation to:
%%% - Perform health checks on remote clusters
%%% - Synchronize cluster state
%%% - Handle sibling job coordination (Phase 7D)
%%% - Manage remote communication
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_sync).

-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% API - Health Monitoring
-export([
    check_cluster_health/2,
    do_health_check/2,
    sync_all_clusters/2,
    do_sync_cluster/2,
    update_local_stats/1,
    collect_local_stats/0,
    update_cluster_from_stats/2
]).

%% API - Remote Communication
-export([
    build_url/3,
    build_auth_headers/1,
    headers_to_proplist/1,
    http_get/2,
    http_get/3,
    http_post/4,
    fetch_cluster_stats/3,
    fetch_remote_job_status/4,
    remote_submit_job/4,
    fetch_federation_info/1,
    register_with_cluster/2
]).

%% API - Sibling Job Coordination (Phase 7D)
-export([
    send_federation_msg/4,
    handle_local_sibling_create/1,
    update_sibling_state/3,
    update_sibling_local_job_id/3,
    update_sibling_start_time/3,
    is_sibling_revocable/1
]).

%% Internal types
-type cluster_name() :: binary().

%% Constants
-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_SIBLING_JOBS, flurm_fed_sibling_jobs).
-define(CLUSTER_TIMEOUT, 5000).

%% Records (local copies for this module)
-record(fed_cluster, {
    name :: cluster_name(),
    host :: binary(),
    port :: pos_integer(),
    auth = #{} :: map(),
    state :: up | down | drain | unknown,
    weight :: pos_integer(),
    features :: [binary()],
    partitions = [] :: [binary()],
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

%%====================================================================
%% API - Health Monitoring
%%====================================================================

%% @doc Check health of a specific cluster.
-spec check_cluster_health(binary(), pos_integer()) -> up | down.
check_cluster_health(Host, Port) ->
    Url = build_url(Host, Port, <<"/api/v1/health">>),
    catch flurm_metrics:increment(flurm_federation_health_checks_total),
    case http_get(Url, ?CLUSTER_TIMEOUT) of
        {ok, _Response} -> up;
        {error, _} ->
            catch flurm_metrics:increment(flurm_federation_health_check_failures_total),
            down
    end.

%% @doc Perform health check on all remote clusters.
-spec do_health_check(cluster_name(), pid()) -> ok.
do_health_check(LocalCluster, Self) ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),

    lists:foreach(fun(#fed_cluster{name = Name, host = Host, port = Port}) ->
        case Name =:= LocalCluster of
            true -> ok;
            false ->
                spawn(fun() ->
                    Status = check_cluster_health(Host, Port),
                    Self ! {cluster_health, Name, Status}
                end)
        end
    end, Clusters),

    %% Update local cluster stats
    update_local_stats(LocalCluster),
    ok.

%% @doc Synchronize state with all remote clusters.
-spec sync_all_clusters(cluster_name(), pid()) -> ok.
sync_all_clusters(LocalCluster, _Self) ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),

    lists:foreach(fun(#fed_cluster{name = Name}) ->
        case Name =:= LocalCluster of
            true -> ok;
            false -> do_sync_cluster(Name, self())
        end
    end, Clusters),
    ok.

%% @doc Synchronize state with a specific cluster.
-spec do_sync_cluster(cluster_name(), pid()) -> ok | {error, term()}.
do_sync_cluster(ClusterName, Self) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth}] ->
            case fetch_cluster_stats(Host, Port, Auth) of
                {ok, Stats} ->
                    Self ! {cluster_update, ClusterName, Stats},
                    ok;
                {error, Reason} ->
                    Self ! {cluster_health, ClusterName, down},
                    {error, Reason}
            end;
        [] ->
            {error, cluster_not_found}
    end.

%% @doc Update local cluster statistics.
-spec update_local_stats(cluster_name()) -> ok.
update_local_stats(ClusterName) ->
    Stats = collect_local_stats(),
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [Cluster] ->
            Updated = update_cluster_from_stats(Cluster, Stats),
            ets:insert(?FED_CLUSTERS_TABLE, Updated);
        [] ->
            ok
    end,
    ok.

%% @doc Collect statistics from local cluster.
-spec collect_local_stats() -> map().
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

%% @doc Update cluster record from stats.
-spec update_cluster_from_stats(#fed_cluster{}, map()) -> #fed_cluster{}.
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

%%====================================================================
%% API - Remote Communication
%%====================================================================

%% @doc Build URL for remote cluster API.
-spec build_url(binary(), pos_integer(), binary()) -> binary().
build_url(Host, Port, Path) ->
    <<"http://", Host/binary, ":", (integer_to_binary(Port))/binary, Path/binary>>.

%% @doc Build authentication headers from auth config.
-spec build_auth_headers(map()) -> [{binary(), binary()}].
build_auth_headers(#{token := Token}) ->
    [{<<"Authorization">>, <<"Bearer ", Token/binary>>}];
build_auth_headers(#{api_key := ApiKey}) ->
    [{<<"X-API-Key">>, ApiKey}];
build_auth_headers(_) ->
    [].

%% @doc Convert headers to proplist format for httpc.
-spec headers_to_proplist([{binary(), binary()}]) -> [{string(), string()}].
headers_to_proplist(Headers) ->
    [{binary_to_list(K), binary_to_list(V)} || {K, V} <- Headers].

%% @doc HTTP GET request with timeout.
-spec http_get(binary(), pos_integer()) -> {ok, binary()} | {error, term()}.
http_get(Url, Timeout) ->
    http_get(Url, [], Timeout).

%% @doc HTTP GET request with headers and timeout.
-spec http_get(binary(), [{binary(), binary()}], pos_integer()) -> {ok, binary()} | {error, term()}.
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

%% @doc HTTP POST request with headers, body, and timeout.
-spec http_post(binary(), [{binary(), binary()}], binary(), pos_integer()) -> {ok, binary()} | {error, term()}.
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

%% @doc Fetch statistics from a remote cluster.
-spec fetch_cluster_stats(binary(), pos_integer(), map()) -> {ok, map()} | {error, term()}.
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

%% @doc Fetch job status from a remote cluster.
-spec fetch_remote_job_status(binary(), pos_integer(), map(), non_neg_integer()) -> {ok, map()} | {error, term()}.
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

%% @doc Submit a job to a remote cluster.
-spec remote_submit_job(binary(), pos_integer(), map(), #job{} | map()) -> {ok, non_neg_integer()} | {error, term()}.
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

%% @doc Fetch federation info from a remote cluster (stub).
-spec fetch_federation_info(binary()) -> {ok, term()} | {error, term()}.
fetch_federation_info(_Host) ->
    %% In production, fetch from remote cluster
    %% This is a record from flurm_federation - we return a map instead
    {ok, #{
        name => <<"default-federation">>,
        created => erlang:system_time(second),
        clusters => [],
        options => #{}
    }}.

%% @doc Register this cluster with a remote cluster (stub).
-spec register_with_cluster(binary(), cluster_name()) -> ok.
register_with_cluster(_Host, _LocalCluster) ->
    %% In production, register this cluster with remote
    ok.

%%====================================================================
%% API - Sibling Job Coordination (Phase 7D)
%%====================================================================

%% @doc Send a federation message to a cluster.
-spec send_federation_msg(cluster_name(), non_neg_integer(), term(), cluster_name()) -> ok | {error, term()}.
send_federation_msg(ClusterName, MsgType, Msg, LocalCluster) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth, state = up}] ->
            %% Encode the message
            case flurm_protocol_codec:encode(MsgType, Msg) of
                {ok, EncodedMsg} ->
                    %% Send via HTTP (federation messages use REST API)
                    Url = build_url(Host, Port, <<"/api/v1/federation/message">>),
                    Body = jsx:encode(#{
                        msg_type => MsgType,
                        payload => base64:encode(EncodedMsg),
                        source_cluster => LocalCluster
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

%% @doc Handle creating a sibling job locally.
-spec handle_local_sibling_create(#fed_job_submit_msg{}) -> ok | {error, term()}.
handle_local_sibling_create(#fed_job_submit_msg{federation_job_id = FedJobId, job_spec = JobSpec}) ->
    %% Convert job spec to job record and submit
    Job = map_to_job(JobSpec),
    case catch flurm_scheduler:submit_job(Job) of
        {ok, LocalJobId} ->
            %% Update local sibling state with job ID
            LocalCluster = get_local_cluster_name(),
            update_sibling_state(FedJobId, LocalCluster, ?SIBLING_STATE_PENDING),
            update_sibling_local_job_id(FedJobId, LocalCluster, LocalJobId),
            ok;
        {error, Reason} ->
            {error, Reason};
        {'EXIT', Reason} ->
            {error, {scheduler_not_available, Reason}}
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

%% @doc Check if a sibling state is revocable (not already terminal or running elsewhere).
-spec is_sibling_revocable(non_neg_integer()) -> boolean().
is_sibling_revocable(?SIBLING_STATE_PENDING) -> true;
is_sibling_revocable(?SIBLING_STATE_NULL) -> true;
is_sibling_revocable(_) -> false.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Get local cluster name from config.
-spec get_local_cluster_name() -> cluster_name().
get_local_cluster_name() ->
    application:get_env(flurm_core, cluster_name, <<"default">>).

%% @doc Convert job record to map.
-spec job_to_map(#job{} | map()) -> map().
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

%% @doc Convert map to job record.
-spec map_to_job(map()) -> #job{}.
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
