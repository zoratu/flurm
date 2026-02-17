%%%-------------------------------------------------------------------
%%% @doc FLURM Federation Routing
%%%
%%% Handles job routing, resource queries, and job tracking across
%%% federated clusters.
%%%
%%% This module provides:
%%% - Job routing based on partition, features, and resources
%%% - Multiple routing policies (round_robin, least_loaded, weighted, etc.)
%%% - Resource aggregation across clusters
%%% - Job tracking and status management
%%% - Helper functions for job/cluster data conversion
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_routing).

-include("flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% API - Job Routing
-export([
    do_route_job/3,
    find_eligible_clusters/4,
    select_cluster_by_policy/3,
    calculate_load/1
]).

%% API - Resource Aggregation
-export([
    aggregate_resources/1,
    get_federation_resources/0
]).

%% API - Job Submission
-export([
    submit_local_job/1,
    submit_remote_job/3
]).

%% API - Job Tracking
-export([
    do_get_remote_job_status/2,
    do_sync_job_state/2,
    do_get_federation_jobs/1,
    generate_local_ref/0,
    generate_federation_id/0
]).

%% API - Data Conversion
-export([
    cluster_to_map/1,
    job_to_map/1,
    map_to_job/1,
    get_job_partition/1,
    get_job_features/1,
    get_job_cpus/1,
    get_job_memory/1,
    has_required_features/2
]).

%% Internal types
-type cluster_name() :: binary().
-type routing_policy() :: round_robin | least_loaded | partition_affinity | random | weighted.

%% Constants
-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_PARTITION_MAP, flurm_fed_partition_map).
-define(FED_REMOTE_JOBS, flurm_fed_remote_jobs).
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

-record(partition_map, {
    partition :: binary(),
    cluster :: cluster_name(),
    priority :: non_neg_integer()
}).

-record(remote_job, {
    local_ref :: binary(),
    remote_cluster :: cluster_name(),
    remote_job_id :: non_neg_integer(),
    local_job_id :: non_neg_integer() | undefined,
    state :: pending | running | completed | failed | cancelled | unknown,
    submit_time :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    job_spec :: map()
}).

%%====================================================================
%% API - Job Routing
%%====================================================================

%% @doc Route a job to the best cluster based on resources and policies.
-spec do_route_job(#job{} | map(), routing_policy(), cluster_name()) -> {ok, cluster_name()} | {error, term()}.
do_route_job(Job, RoutingPolicy, LocalCluster) ->
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
            {ok, LocalCluster};
        Clusters ->
            %% Apply routing policy
            select_cluster_by_policy(Clusters, RoutingPolicy, LocalCluster)
    end.

%% @doc Find clusters that meet the job requirements.
-spec find_eligible_clusters(binary() | undefined, [binary()], non_neg_integer(), non_neg_integer()) -> [#fed_cluster{}].
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

%% @doc Select a cluster based on the routing policy.
-spec select_cluster_by_policy([#fed_cluster{}], routing_policy(), cluster_name()) -> {ok, cluster_name()} | {error, term()}.
select_cluster_by_policy(Clusters, round_robin, _LocalCluster) ->
    %% Simple round robin - pick first available
    %% Note: In full implementation, state would track index
    case Clusters of
        [First | _] -> {ok, First#fed_cluster.name};
        [] -> {error, no_cluster_available}
    end;

select_cluster_by_policy(Clusters, least_loaded, _LocalCluster) ->
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

select_cluster_by_policy(Clusters, weighted, _LocalCluster) ->
    %% Weighted random selection
    TotalWeight = lists:sum([C#fed_cluster.weight || C <- Clusters]),
    case TotalWeight of
        0 -> {ok, (hd(Clusters))#fed_cluster.name};
        _ ->
            Random = rand:uniform(TotalWeight),
            select_by_weight(Clusters, Random, 0)
    end;

select_cluster_by_policy(Clusters, partition_affinity, _LocalCluster) ->
    %% Already filtered by partition, just pick first up cluster
    case [C || C <- Clusters, C#fed_cluster.state =:= up] of
        [First | _] -> {ok, First#fed_cluster.name};
        [] -> {error, no_cluster_available}
    end;

select_cluster_by_policy(Clusters, random, _LocalCluster) ->
    Index = rand:uniform(length(Clusters)),
    Cluster = lists:nth(Index, Clusters),
    {ok, Cluster#fed_cluster.name}.

%% @doc Calculate load for a cluster.
-spec calculate_load(#fed_cluster{}) -> number().
calculate_load(#fed_cluster{cpu_count = 0}) ->
    infinity;
calculate_load(#fed_cluster{pending_jobs = Pending, running_jobs = Running, cpu_count = CpuCount}) ->
    (Pending + Running) / CpuCount.

%% Internal helper for weighted selection
select_by_weight([C | Rest], Random, Acc) ->
    NewAcc = Acc + C#fed_cluster.weight,
    case Random =< NewAcc of
        true -> {ok, C#fed_cluster.name};
        false -> select_by_weight(Rest, Random, NewAcc)
    end;
select_by_weight([], _, _) ->
    {error, no_cluster_available}.

%%====================================================================
%% API - Resource Aggregation
%%====================================================================

%% @doc Aggregate resources across all clusters.
-spec aggregate_resources([#fed_cluster{}]) -> map().
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

%% @doc Get aggregated resources across all federated clusters.
-spec get_federation_resources() -> map().
get_federation_resources() ->
    Clusters = ets:tab2list(?FED_CLUSTERS_TABLE),
    aggregate_resources(Clusters).

%%====================================================================
%% API - Job Submission
%%====================================================================

%% @doc Submit a job locally.
-spec submit_local_job(#job{} | map()) -> {ok, {cluster_name(), non_neg_integer()}} | {error, term()}.
submit_local_job(Job) when is_record(Job, job) ->
    LocalCluster = get_local_cluster_name(),
    case flurm_scheduler:submit_job(Job) of
        {ok, JobId} -> {ok, {LocalCluster, JobId}};
        Error -> Error
    end;
submit_local_job(JobSpec) when is_map(JobSpec) ->
    Job = map_to_job(JobSpec),
    submit_local_job(Job).

%% @doc Submit a job to a remote cluster.
-spec submit_remote_job(cluster_name(), #job{} | map(), map()) -> {ok, {cluster_name(), non_neg_integer()}} | {error, term()}.
submit_remote_job(ClusterName, Job, _Options) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, ClusterName) of
        [#fed_cluster{host = Host, port = Port, auth = Auth, state = up}] ->
            StartTime = erlang:monotonic_time(millisecond),
            case flurm_federation_sync:remote_submit_job(Host, Port, Auth, Job) of
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
%% API - Job Tracking
%%====================================================================

%% @doc Get status of a remote job.
-spec do_get_remote_job_status(cluster_name(), non_neg_integer()) -> {ok, map()} | {error, term()}.
do_get_remote_job_status(Cluster, JobId) ->
    case ets:lookup(?FED_CLUSTERS_TABLE, Cluster) of
        [#fed_cluster{host = Host, port = Port, auth = Auth}] ->
            flurm_federation_sync:fetch_remote_job_status(Host, Port, Auth, JobId);
        [] ->
            {error, cluster_not_found}
    end.

%% @doc Sync job state from a remote cluster.
-spec do_sync_job_state(cluster_name(), non_neg_integer()) -> ok | {error, term()}.
do_sync_job_state(Cluster, JobId) ->
    case do_get_remote_job_status(Cluster, JobId) of
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

%% @doc Get all jobs across the federation.
-spec do_get_federation_jobs(cluster_name()) -> [map()].
do_get_federation_jobs(LocalCluster) ->
    %% Get local jobs
    LocalJobs = case catch flurm_job_registry:list_jobs() of
        Jobs when is_list(Jobs) ->
            [#{cluster => LocalCluster, job_id => Id, pid => Pid}
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

%% @doc Generate a unique local reference ID.
-spec generate_local_ref() -> binary().
generate_local_ref() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"ref-", Timestamp/binary, "-", Random/binary>>.

%% @doc Generate a unique federation job ID.
-spec generate_federation_id() -> binary().
generate_federation_id() ->
    Timestamp = integer_to_binary(erlang:system_time(microsecond)),
    Random = integer_to_binary(rand:uniform(1000000)),
    <<"fed-", Timestamp/binary, "-", Random/binary>>.

%%====================================================================
%% API - Data Conversion
%%====================================================================

%% @doc Convert cluster record to map.
-spec cluster_to_map(#fed_cluster{}) -> map().
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

%% @doc Extract partition from job.
-spec get_job_partition(#job{} | map()) -> binary() | undefined.
get_job_partition(#job{partition = P}) -> P;
get_job_partition(#{partition := P}) -> P;
get_job_partition(_) -> undefined.

%% @doc Extract features from job.
-spec get_job_features(#job{} | map()) -> [binary()].
get_job_features(#job{}) -> [];  % Jobs don't have features field directly
get_job_features(#{features := F}) -> F;
get_job_features(_) -> [].

%% @doc Extract CPU count from job.
-spec get_job_cpus(#job{} | map()) -> non_neg_integer().
get_job_cpus(#job{num_cpus = C}) -> C;
get_job_cpus(#{num_cpus := C}) -> C;
get_job_cpus(_) -> 1.

%% @doc Extract memory from job.
-spec get_job_memory(#job{} | map()) -> non_neg_integer().
get_job_memory(#job{memory_mb = M}) -> M;
get_job_memory(#{memory_mb := M}) -> M;
get_job_memory(_) -> 1024.

%% @doc Check if cluster has all required features.
-spec has_required_features([binary()], [binary()]) -> boolean().
has_required_features(ClusterFeatures, RequiredFeatures) ->
    lists:all(fun(F) -> lists:member(F, ClusterFeatures) end, RequiredFeatures).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Get local cluster name from config.
-spec get_local_cluster_name() -> cluster_name().
get_local_cluster_name() ->
    application:get_env(flurm_core, cluster_name, <<"default">>).
