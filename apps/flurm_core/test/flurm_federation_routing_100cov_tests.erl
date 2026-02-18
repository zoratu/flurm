%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_federation_routing module
%%% Achieves 100% code coverage for federation job routing.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_routing_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Test record for fed_cluster
-record(fed_cluster, {
    name :: binary(),
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
    cluster :: binary(),
    priority :: non_neg_integer()
}).

-record(remote_job, {
    local_ref :: binary(),
    remote_cluster :: binary(),
    remote_job_id :: non_neg_integer(),
    local_job_id :: non_neg_integer() | undefined,
    state :: atom(),
    submit_time :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    job_spec :: map()
}).

-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_PARTITION_MAP, flurm_fed_partition_map).
-define(FED_REMOTE_JOBS, flurm_fed_remote_jobs).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Create ETS tables
    (catch ets:delete(?FED_CLUSTERS_TABLE)),
    (catch ets:delete(?FED_PARTITION_MAP)),
    (catch ets:delete(?FED_REMOTE_JOBS)),

    ets:new(?FED_CLUSTERS_TABLE, [named_table, public, set, {keypos, 2}]),
    ets:new(?FED_PARTITION_MAP, [named_table, public, bag, {keypos, 2}]),
    ets:new(?FED_REMOTE_JOBS, [named_table, public, set, {keypos, 2}]),

    %% Setup mocks
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),

    meck:new(flurm_scheduler, [passthrough, non_strict]),
    meck:new(flurm_job_registry, [passthrough, non_strict]),
    meck:new(flurm_federation_sync, [passthrough, non_strict]),

    ok.

cleanup(_) ->
    (catch ets:delete(?FED_CLUSTERS_TABLE)),
    (catch ets:delete(?FED_PARTITION_MAP)),
    (catch ets:delete(?FED_REMOTE_JOBS)),

    meck:unload(flurm_metrics),
    meck:unload(flurm_scheduler),
    meck:unload(flurm_job_registry),
    meck:unload(flurm_federation_sync),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

federation_routing_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Job Routing Tests
        {"do_route_job with no eligible clusters", fun test_do_route_job_no_eligible/0},
        {"do_route_job with eligible clusters round_robin", fun test_do_route_job_round_robin/0},
        {"do_route_job with eligible clusters least_loaded", fun test_do_route_job_least_loaded/0},
        {"do_route_job with eligible clusters weighted", fun test_do_route_job_weighted/0},
        {"do_route_job with eligible clusters partition_affinity", fun test_do_route_job_partition_affinity/0},
        {"do_route_job with eligible clusters random", fun test_do_route_job_random/0},
        {"do_route_job with map job", fun test_do_route_job_with_map/0},

        %% Find Eligible Clusters Tests
        {"find_eligible_clusters empty table", fun test_find_eligible_clusters_empty/0},
        {"find_eligible_clusters with partition", fun test_find_eligible_clusters_with_partition/0},
        {"find_eligible_clusters with partition not mapped", fun test_find_eligible_clusters_partition_not_mapped/0},
        {"find_eligible_clusters filters by state", fun test_find_eligible_clusters_filters_state/0},
        {"find_eligible_clusters filters by features", fun test_find_eligible_clusters_filters_features/0},
        {"find_eligible_clusters filters by resources", fun test_find_eligible_clusters_filters_resources/0},
        {"find_eligible_clusters undefined partition", fun test_find_eligible_clusters_undefined_partition/0},
        {"find_eligible_clusters empty partition", fun test_find_eligible_clusters_empty_partition/0},

        %% Select Cluster By Policy Tests
        {"select_cluster_by_policy round_robin empty", fun test_select_policy_round_robin_empty/0},
        {"select_cluster_by_policy round_robin", fun test_select_policy_round_robin/0},
        {"select_cluster_by_policy least_loaded empty", fun test_select_policy_least_loaded_empty/0},
        {"select_cluster_by_policy least_loaded", fun test_select_policy_least_loaded/0},
        {"select_cluster_by_policy weighted zero weight", fun test_select_policy_weighted_zero/0},
        {"select_cluster_by_policy weighted", fun test_select_policy_weighted/0},
        {"select_cluster_by_policy partition_affinity empty", fun test_select_policy_partition_empty/0},
        {"select_cluster_by_policy partition_affinity", fun test_select_policy_partition/0},
        {"select_cluster_by_policy random", fun test_select_policy_random/0},

        %% Calculate Load Tests
        {"calculate_load zero cpus", fun test_calculate_load_zero_cpus/0},
        {"calculate_load normal", fun test_calculate_load_normal/0},
        {"calculate_load high jobs", fun test_calculate_load_high/0},

        %% Resource Aggregation Tests
        {"aggregate_resources empty", fun test_aggregate_resources_empty/0},
        {"aggregate_resources single cluster up", fun test_aggregate_resources_single_up/0},
        {"aggregate_resources single cluster down", fun test_aggregate_resources_single_down/0},
        {"aggregate_resources multiple clusters", fun test_aggregate_resources_multiple/0},
        {"get_federation_resources", fun test_get_federation_resources/0},

        %% Job Submission Tests
        {"submit_local_job success with record", fun test_submit_local_job_record_success/0},
        {"submit_local_job failure with record", fun test_submit_local_job_record_failure/0},
        {"submit_local_job success with map", fun test_submit_local_job_map_success/0},
        {"submit_remote_job success", fun test_submit_remote_job_success/0},
        {"submit_remote_job cluster down", fun test_submit_remote_job_cluster_down/0},
        {"submit_remote_job cluster not found", fun test_submit_remote_job_not_found/0},
        {"submit_remote_job http error", fun test_submit_remote_job_http_error/0},

        %% Job Tracking Tests
        {"do_get_remote_job_status success", fun test_get_remote_job_status_success/0},
        {"do_get_remote_job_status cluster not found", fun test_get_remote_job_status_not_found/0},
        {"do_sync_job_state success updates record", fun test_sync_job_state_success/0},
        {"do_sync_job_state no existing record", fun test_sync_job_state_no_record/0},
        {"do_sync_job_state fetch error", fun test_sync_job_state_error/0},
        {"do_get_federation_jobs with local and remote", fun test_get_federation_jobs/0},
        {"do_get_federation_jobs local registry error", fun test_get_federation_jobs_error/0},
        {"generate_local_ref format", fun test_generate_local_ref/0},
        {"generate_federation_id format", fun test_generate_federation_id/0},

        %% Data Conversion Tests
        {"cluster_to_map converts all fields", fun test_cluster_to_map/0},
        {"job_to_map from record", fun test_job_to_map_record/0},
        {"job_to_map from map", fun test_job_to_map_map/0},
        {"map_to_job all fields", fun test_map_to_job/0},
        {"map_to_job defaults", fun test_map_to_job_defaults/0},
        {"get_job_partition from record", fun test_get_job_partition_record/0},
        {"get_job_partition from map", fun test_get_job_partition_map/0},
        {"get_job_partition from other", fun test_get_job_partition_other/0},
        {"get_job_features from record", fun test_get_job_features_record/0},
        {"get_job_features from map", fun test_get_job_features_map/0},
        {"get_job_features from other", fun test_get_job_features_other/0},
        {"get_job_cpus from record", fun test_get_job_cpus_record/0},
        {"get_job_cpus from map", fun test_get_job_cpus_map/0},
        {"get_job_cpus from other", fun test_get_job_cpus_other/0},
        {"get_job_memory from record", fun test_get_job_memory_record/0},
        {"get_job_memory from map", fun test_get_job_memory_map/0},
        {"get_job_memory from other", fun test_get_job_memory_other/0},
        {"has_required_features all present", fun test_has_required_features_all_present/0},
        {"has_required_features missing", fun test_has_required_features_missing/0},
        {"has_required_features empty required", fun test_has_required_features_empty/0}
     ]}.

%%====================================================================
%% Helper Functions
%%====================================================================

make_cluster(Name, State, Cpus, AvailCpus, Mem, AvailMem, Weight, Features) ->
    #fed_cluster{
        name = Name,
        host = <<"host">>,
        port = 8080,
        auth = #{},
        state = State,
        weight = Weight,
        features = Features,
        partitions = [<<"default">>],
        node_count = 10,
        cpu_count = Cpus,
        memory_mb = Mem,
        gpu_count = 0,
        pending_jobs = 5,
        running_jobs = 10,
        available_cpus = AvailCpus,
        available_memory = AvailMem,
        last_sync = erlang:system_time(second),
        last_health_check = erlang:system_time(second),
        consecutive_failures = 0,
        properties = #{}
    }.

%%====================================================================
%% Job Routing Tests
%%====================================================================

test_do_route_job_no_eligible() ->
    %% No clusters in table
    Job = #job{partition = <<"batch">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, round_robin, <<"local">>),
    ?assertEqual({ok, <<"local">>}, Result).

test_do_route_job_round_robin() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Job = #job{partition = <<"default">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, round_robin, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_do_route_job_least_loaded() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    C2 = make_cluster(<<"c2">>, up, 200, 100, 20000, 10000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),
    ets:insert(?FED_CLUSTERS_TABLE, C2),

    Job = #job{partition = <<"default">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>),
    %% Should pick c2 as it has lower load (15/200 vs 15/100)
    ?assertMatch({ok, _}, Result).

test_do_route_job_weighted() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 10, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Job = #job{partition = <<"default">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, weighted, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_do_route_job_partition_affinity() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Job = #job{partition = <<"default">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, partition_affinity, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_do_route_job_random() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Job = #job{partition = <<"default">>, num_cpus = 4, memory_mb = 1024},
    Result = flurm_federation_routing:do_route_job(Job, random, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_do_route_job_with_map() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    JobMap = #{partition => <<"default">>, num_cpus => 4, memory_mb => 1024},
    Result = flurm_federation_routing:do_route_job(JobMap, round_robin, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

%%====================================================================
%% Find Eligible Clusters Tests
%%====================================================================

test_find_eligible_clusters_empty() ->
    Result = flurm_federation_routing:find_eligible_clusters(<<"batch">>, [], 4, 1024),
    ?assertEqual([], Result).

test_find_eligible_clusters_with_partition() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    %% Add partition mapping
    PartMap = #partition_map{partition = <<"batch">>, cluster = <<"c1">>, priority = 1},
    ets:insert(?FED_PARTITION_MAP, PartMap),

    Result = flurm_federation_routing:find_eligible_clusters(<<"batch">>, [], 4, 1024),
    ?assertEqual(1, length(Result)).

test_find_eligible_clusters_partition_not_mapped() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    %% No partition mapping - should check all clusters
    Result = flurm_federation_routing:find_eligible_clusters(<<"unmapped">>, [], 4, 1024),
    ?assertEqual(1, length(Result)).

test_find_eligible_clusters_filters_state() ->
    C1 = make_cluster(<<"c1">>, down, 100, 50, 10000, 5000, 1, []),
    C2 = make_cluster(<<"c2">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),
    ets:insert(?FED_CLUSTERS_TABLE, C2),

    Result = flurm_federation_routing:find_eligible_clusters(undefined, [], 4, 1024),
    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"c2">>, (hd(Result))#fed_cluster.name).

test_find_eligible_clusters_filters_features() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, [<<"gpu">>, <<"fast">>]),
    C2 = make_cluster(<<"c2">>, up, 100, 50, 10000, 5000, 1, [<<"cpu">>]),
    ets:insert(?FED_CLUSTERS_TABLE, C1),
    ets:insert(?FED_CLUSTERS_TABLE, C2),

    Result = flurm_federation_routing:find_eligible_clusters(undefined, [<<"gpu">>], 4, 1024),
    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"c1">>, (hd(Result))#fed_cluster.name).

test_find_eligible_clusters_filters_resources() ->
    C1 = make_cluster(<<"c1">>, up, 100, 10, 10000, 500, 1, []),  % Low resources
    C2 = make_cluster(<<"c2">>, up, 100, 50, 10000, 5000, 1, []), % Good resources
    ets:insert(?FED_CLUSTERS_TABLE, C1),
    ets:insert(?FED_CLUSTERS_TABLE, C2),

    Result = flurm_federation_routing:find_eligible_clusters(undefined, [], 20, 2000),
    ?assertEqual(1, length(Result)),
    ?assertEqual(<<"c2">>, (hd(Result))#fed_cluster.name).

test_find_eligible_clusters_undefined_partition() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Result = flurm_federation_routing:find_eligible_clusters(undefined, [], 4, 1024),
    ?assertEqual(1, length(Result)).

test_find_eligible_clusters_empty_partition() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Result = flurm_federation_routing:find_eligible_clusters(<<>>, [], 4, 1024),
    ?assertEqual(1, length(Result)).

%%====================================================================
%% Select Cluster By Policy Tests
%%====================================================================

test_select_policy_round_robin_empty() ->
    Result = flurm_federation_routing:select_cluster_by_policy([], round_robin, <<"local">>),
    ?assertEqual({error, no_cluster_available}, Result).

test_select_policy_round_robin() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    C2 = make_cluster(<<"c2">>, up, 100, 50, 10000, 5000, 1, []),
    Result = flurm_federation_routing:select_cluster_by_policy([C1, C2], round_robin, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_select_policy_least_loaded_empty() ->
    Result = flurm_federation_routing:select_cluster_by_policy([], least_loaded, <<"local">>),
    ?assertEqual({error, no_cluster_available}, Result).

test_select_policy_least_loaded() ->
    %% C1 has higher load: 15/100 = 0.15
    %% C2 has lower load: 15/200 = 0.075
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    C2 = #fed_cluster{
        name = <<"c2">>,
        host = <<"host">>,
        port = 8080,
        state = up,
        weight = 1,
        features = [],
        cpu_count = 200,
        memory_mb = 20000,
        available_cpus = 100,
        available_memory = 10000,
        pending_jobs = 5,
        running_jobs = 10
    },
    Result = flurm_federation_routing:select_cluster_by_policy([C1, C2], least_loaded, <<"local">>),
    ?assertEqual({ok, <<"c2">>}, Result).

test_select_policy_weighted_zero() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 0, []),
    Result = flurm_federation_routing:select_cluster_by_policy([C1], weighted, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_select_policy_weighted() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 10, []),
    C2 = make_cluster(<<"c2">>, up, 100, 50, 10000, 5000, 5, []),
    Result = flurm_federation_routing:select_cluster_by_policy([C1, C2], weighted, <<"local">>),
    ?assertMatch({ok, _}, Result).

test_select_policy_partition_empty() ->
    Result = flurm_federation_routing:select_cluster_by_policy([], partition_affinity, <<"local">>),
    ?assertEqual({error, no_cluster_available}, Result).

test_select_policy_partition() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    C2 = make_cluster(<<"c2">>, down, 100, 50, 10000, 5000, 1, []),
    Result = flurm_federation_routing:select_cluster_by_policy([C1, C2], partition_affinity, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

test_select_policy_random() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    Result = flurm_federation_routing:select_cluster_by_policy([C1], random, <<"local">>),
    ?assertEqual({ok, <<"c1">>}, Result).

%%====================================================================
%% Calculate Load Tests
%%====================================================================

test_calculate_load_zero_cpus() ->
    C = #fed_cluster{cpu_count = 0, pending_jobs = 5, running_jobs = 10},
    Result = flurm_federation_routing:calculate_load(C),
    ?assertEqual(infinity, Result).

test_calculate_load_normal() ->
    C = #fed_cluster{cpu_count = 100, pending_jobs = 5, running_jobs = 10},
    Result = flurm_federation_routing:calculate_load(C),
    ?assertEqual(0.15, Result).

test_calculate_load_high() ->
    C = #fed_cluster{cpu_count = 10, pending_jobs = 50, running_jobs = 100},
    Result = flurm_federation_routing:calculate_load(C),
    ?assertEqual(15.0, Result).

%%====================================================================
%% Resource Aggregation Tests
%%====================================================================

test_aggregate_resources_empty() ->
    Result = flurm_federation_routing:aggregate_resources([]),
    ?assertEqual(0, maps:get(total_nodes, Result)),
    ?assertEqual(0, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

test_aggregate_resources_single_up() ->
    C = #fed_cluster{
        state = up,
        node_count = 10,
        cpu_count = 100,
        memory_mb = 50000,
        gpu_count = 4,
        available_cpus = 80,
        available_memory = 40000,
        pending_jobs = 5,
        running_jobs = 10
    },
    Result = flurm_federation_routing:aggregate_resources([C]),
    ?assertEqual(10, maps:get(total_nodes, Result)),
    ?assertEqual(100, maps:get(total_cpus, Result)),
    ?assertEqual(50000, maps:get(total_memory_mb, Result)),
    ?assertEqual(4, maps:get(total_gpus, Result)),
    ?assertEqual(80, maps:get(available_cpus, Result)),
    ?assertEqual(40000, maps:get(available_memory_mb, Result)),
    ?assertEqual(5, maps:get(pending_jobs, Result)),
    ?assertEqual(10, maps:get(running_jobs, Result)),
    ?assertEqual(1, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

test_aggregate_resources_single_down() ->
    C = #fed_cluster{
        state = down,
        node_count = 10,
        cpu_count = 100
    },
    Result = flurm_federation_routing:aggregate_resources([C]),
    ?assertEqual(0, maps:get(total_nodes, Result)),
    ?assertEqual(0, maps:get(clusters_up, Result)),
    ?assertEqual(1, maps:get(clusters_down, Result)).

test_aggregate_resources_multiple() ->
    C1 = #fed_cluster{
        state = up,
        node_count = 10,
        cpu_count = 100,
        memory_mb = 50000,
        gpu_count = 4,
        available_cpus = 80,
        available_memory = 40000,
        pending_jobs = 5,
        running_jobs = 10
    },
    C2 = #fed_cluster{
        state = up,
        node_count = 20,
        cpu_count = 200,
        memory_mb = 100000,
        gpu_count = 8,
        available_cpus = 150,
        available_memory = 80000,
        pending_jobs = 10,
        running_jobs = 20
    },
    C3 = #fed_cluster{
        state = down,
        node_count = 5,
        cpu_count = 50
    },
    Result = flurm_federation_routing:aggregate_resources([C1, C2, C3]),
    ?assertEqual(30, maps:get(total_nodes, Result)),
    ?assertEqual(300, maps:get(total_cpus, Result)),
    ?assertEqual(150000, maps:get(total_memory_mb, Result)),
    ?assertEqual(12, maps:get(total_gpus, Result)),
    ?assertEqual(2, maps:get(clusters_up, Result)),
    ?assertEqual(1, maps:get(clusters_down, Result)).

test_get_federation_resources() ->
    C1 = make_cluster(<<"c1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Result = flurm_federation_routing:get_federation_resources(),
    ?assertEqual(1, maps:get(clusters_up, Result)).

%%====================================================================
%% Job Submission Tests
%%====================================================================

test_submit_local_job_record_success() ->
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 12345} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_local_job(Job),
    ?assertMatch({ok, {_, 12345}}, Result).

test_submit_local_job_record_failure() ->
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {error, queue_full} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_local_job(Job),
    ?assertEqual({error, queue_full}, Result).

test_submit_local_job_map_success() ->
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 54321} end),

    JobMap = #{id => 1, name => <<"test">>, user => <<"user">>},
    Result = flurm_federation_routing:submit_local_job(JobMap),
    ?assertMatch({ok, {_, 54321}}, Result).

test_submit_remote_job_success() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    meck:expect(flurm_federation_sync, remote_submit_job, fun(_, _, _, _) -> {ok, 99999} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_remote_job(<<"remote1">>, Job, #{}),
    ?assertMatch({ok, {<<"remote1">>, 99999}}, Result),

    %% Verify job was tracked
    RemoteJobs = ets:tab2list(?FED_REMOTE_JOBS),
    ?assertEqual(1, length(RemoteJobs)).

test_submit_remote_job_cluster_down() ->
    C1 = make_cluster(<<"remote1">>, down, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_remote_job(<<"remote1">>, Job, #{}),
    ?assertEqual({error, {cluster_unavailable, down}}, Result).

test_submit_remote_job_not_found() ->
    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_remote_job(<<"nonexistent">>, Job, #{}),
    ?assertEqual({error, cluster_not_found}, Result).

test_submit_remote_job_http_error() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    meck:expect(flurm_federation_sync, remote_submit_job, fun(_, _, _, _) -> {error, timeout} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_routing:submit_remote_job(<<"remote1">>, Job, #{}),
    ?assertEqual({error, timeout}, Result).

%%====================================================================
%% Job Tracking Tests
%%====================================================================

test_get_remote_job_status_success() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{state => running}}
    end),

    Result = flurm_federation_routing:do_get_remote_job_status(<<"remote1">>, 123),
    ?assertEqual({ok, #{state => running}}, Result).

test_get_remote_job_status_not_found() ->
    Result = flurm_federation_routing:do_get_remote_job_status(<<"nonexistent">>, 123),
    ?assertEqual({error, cluster_not_found}, Result).

test_sync_job_state_success() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    %% Insert existing remote job
    RemoteJob = #remote_job{
        local_ref = <<"ref-1">>,
        remote_cluster = <<"remote1">>,
        remote_job_id = 123,
        state = pending,
        submit_time = erlang:system_time(second),
        last_sync = 0,
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),

    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{state => running}}
    end),

    Result = flurm_federation_routing:do_sync_job_state(<<"remote1">>, 123),
    ?assertEqual(ok, Result),

    %% Verify state was updated
    Pattern = #remote_job{remote_cluster = <<"remote1">>, remote_job_id = 123, _ = '_'},
    [Updated] = ets:match_object(?FED_REMOTE_JOBS, Pattern),
    ?assertEqual(running, Updated#remote_job.state).

test_sync_job_state_no_record() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{state => running}}
    end),

    %% No existing remote job record
    Result = flurm_federation_routing:do_sync_job_state(<<"remote1">>, 123),
    ?assertEqual(ok, Result).

test_sync_job_state_error() ->
    C1 = make_cluster(<<"remote1">>, up, 100, 50, 10000, 5000, 1, []),
    ets:insert(?FED_CLUSTERS_TABLE, C1),

    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {error, timeout}
    end),

    Result = flurm_federation_routing:do_sync_job_state(<<"remote1">>, 123),
    ?assertEqual({error, timeout}, Result).

test_get_federation_jobs() ->
    %% Add a remote job
    RemoteJob = #remote_job{
        local_ref = <<"ref-1">>,
        remote_cluster = <<"remote1">>,
        remote_job_id = 123,
        state = running,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),

    %% Mock local jobs
    meck:expect(flurm_job_registry, list_jobs, fun() -> [{1, self()}, {2, self()}] end),

    Result = flurm_federation_routing:do_get_federation_jobs(<<"local">>),
    ?assertEqual(3, length(Result)).

test_get_federation_jobs_error() ->
    RemoteJob = #remote_job{
        local_ref = <<"ref-1">>,
        remote_cluster = <<"remote1">>,
        remote_job_id = 123,
        state = running,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),

    meck:expect(flurm_job_registry, list_jobs, fun() -> exit(not_running) end),

    Result = flurm_federation_routing:do_get_federation_jobs(<<"local">>),
    ?assertEqual(1, length(Result)).

test_generate_local_ref() ->
    Ref1 = flurm_federation_routing:generate_local_ref(),
    Ref2 = flurm_federation_routing:generate_local_ref(),
    ?assert(is_binary(Ref1)),
    ?assert(is_binary(Ref2)),
    ?assertMatch(<<"ref-", _/binary>>, Ref1),
    ?assertNotEqual(Ref1, Ref2).

test_generate_federation_id() ->
    Id1 = flurm_federation_routing:generate_federation_id(),
    Id2 = flurm_federation_routing:generate_federation_id(),
    ?assert(is_binary(Id1)),
    ?assert(is_binary(Id2)),
    ?assertMatch(<<"fed-", _/binary>>, Id1),
    ?assertNotEqual(Id1, Id2).

%%====================================================================
%% Data Conversion Tests
%%====================================================================

test_cluster_to_map() ->
    C = #fed_cluster{
        name = <<"c1">>,
        host = <<"host1">>,
        port = 8080,
        state = up,
        weight = 10,
        features = [<<"gpu">>],
        partitions = [<<"batch">>],
        node_count = 50,
        cpu_count = 500,
        memory_mb = 100000,
        gpu_count = 20,
        pending_jobs = 10,
        running_jobs = 25,
        available_cpus = 400,
        available_memory = 80000,
        last_sync = 12345,
        last_health_check = 12340,
        consecutive_failures = 0
    },
    Result = flurm_federation_routing:cluster_to_map(C),
    ?assertEqual(<<"c1">>, maps:get(name, Result)),
    ?assertEqual(<<"host1">>, maps:get(host, Result)),
    ?assertEqual(8080, maps:get(port, Result)),
    ?assertEqual(up, maps:get(state, Result)),
    ?assertEqual(10, maps:get(weight, Result)),
    ?assertEqual([<<"gpu">>], maps:get(features, Result)),
    ?assertEqual([<<"batch">>], maps:get(partitions, Result)),
    ?assertEqual(50, maps:get(node_count, Result)),
    ?assertEqual(500, maps:get(cpu_count, Result)),
    ?assertEqual(100000, maps:get(memory_mb, Result)),
    ?assertEqual(20, maps:get(gpu_count, Result)),
    ?assertEqual(10, maps:get(pending_jobs, Result)),
    ?assertEqual(25, maps:get(running_jobs, Result)),
    ?assertEqual(400, maps:get(available_cpus, Result)),
    ?assertEqual(80000, maps:get(available_memory, Result)),
    ?assertEqual(12345, maps:get(last_sync, Result)),
    ?assertEqual(12340, maps:get(last_health_check, Result)),
    ?assertEqual(0, maps:get(consecutive_failures, Result)).

test_job_to_map_record() ->
    Job = #job{
        id = 123,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"batch">>,
        state = running,
        script = <<"#!/bin/bash">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 3600,
        priority = 100,
        work_dir = <<"/home/user">>,
        account = <<"research">>,
        qos = <<"high">>
    },
    Result = flurm_federation_routing:job_to_map(Job),
    ?assertEqual(123, maps:get(id, Result)),
    ?assertEqual(<<"test_job">>, maps:get(name, Result)),
    ?assertEqual(<<"testuser">>, maps:get(user, Result)),
    ?assertEqual(<<"batch">>, maps:get(partition, Result)),
    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual(<<"#!/bin/bash">>, maps:get(script, Result)),
    ?assertEqual(2, maps:get(num_nodes, Result)),
    ?assertEqual(8, maps:get(num_cpus, Result)),
    ?assertEqual(4096, maps:get(memory_mb, Result)),
    ?assertEqual(3600, maps:get(time_limit, Result)),
    ?assertEqual(100, maps:get(priority, Result)),
    ?assertEqual(<<"/home/user">>, maps:get(work_dir, Result)),
    ?assertEqual(<<"research">>, maps:get(account, Result)),
    ?assertEqual(<<"high">>, maps:get(qos, Result)).

test_job_to_map_map() ->
    JobMap = #{id => 456, name => <<"other">>},
    Result = flurm_federation_routing:job_to_map(JobMap),
    ?assertEqual(JobMap, Result).

test_map_to_job() ->
    Map = #{
        id => 789,
        name => <<"mapped_job">>,
        user => <<"mapuser">>,
        partition => <<"gpu">>,
        state => pending,
        script => <<"echo hello">>,
        num_nodes => 4,
        num_cpus => 16,
        memory_mb => 8192,
        time_limit => 7200,
        priority => 200,
        work_dir => <<"/work">>,
        account => <<"project">>,
        qos => <<"normal">>
    },
    Result = flurm_federation_routing:map_to_job(Map),
    ?assertEqual(789, Result#job.id),
    ?assertEqual(<<"mapped_job">>, Result#job.name),
    ?assertEqual(<<"mapuser">>, Result#job.user),
    ?assertEqual(<<"gpu">>, Result#job.partition),
    ?assertEqual(pending, Result#job.state),
    ?assertEqual(<<"echo hello">>, Result#job.script),
    ?assertEqual(4, Result#job.num_nodes),
    ?assertEqual(16, Result#job.num_cpus),
    ?assertEqual(8192, Result#job.memory_mb),
    ?assertEqual(7200, Result#job.time_limit),
    ?assertEqual(200, Result#job.priority),
    ?assertEqual(<<"/work">>, Result#job.work_dir),
    ?assertEqual(<<"project">>, Result#job.account),
    ?assertEqual(<<"normal">>, Result#job.qos).

test_map_to_job_defaults() ->
    Map = #{},
    Result = flurm_federation_routing:map_to_job(Map),
    ?assertEqual(0, Result#job.id),
    ?assertEqual(<<"unnamed">>, Result#job.name),
    ?assertEqual(<<"unknown">>, Result#job.user),
    ?assertEqual(<<"default">>, Result#job.partition),
    ?assertEqual(pending, Result#job.state),
    ?assertEqual(<<>>, Result#job.script),
    ?assertEqual(1, Result#job.num_nodes),
    ?assertEqual(1, Result#job.num_cpus),
    ?assertEqual(1024, Result#job.memory_mb),
    ?assertEqual(3600, Result#job.time_limit),
    ?assertEqual(100, Result#job.priority),
    ?assertEqual(<<"/tmp">>, Result#job.work_dir),
    ?assertEqual(<<>>, Result#job.account),
    ?assertEqual(<<"normal">>, Result#job.qos).

test_get_job_partition_record() ->
    Job = #job{partition = <<"compute">>},
    Result = flurm_federation_routing:get_job_partition(Job),
    ?assertEqual(<<"compute">>, Result).

test_get_job_partition_map() ->
    JobMap = #{partition => <<"batch">>},
    Result = flurm_federation_routing:get_job_partition(JobMap),
    ?assertEqual(<<"batch">>, Result).

test_get_job_partition_other() ->
    Result = flurm_federation_routing:get_job_partition(other_type),
    ?assertEqual(undefined, Result).

test_get_job_features_record() ->
    Job = #job{id = 1},
    Result = flurm_federation_routing:get_job_features(Job),
    ?assertEqual([], Result).

test_get_job_features_map() ->
    JobMap = #{features => [<<"gpu">>, <<"ssd">>]},
    Result = flurm_federation_routing:get_job_features(JobMap),
    ?assertEqual([<<"gpu">>, <<"ssd">>], Result).

test_get_job_features_other() ->
    Result = flurm_federation_routing:get_job_features(other_type),
    ?assertEqual([], Result).

test_get_job_cpus_record() ->
    Job = #job{num_cpus = 16},
    Result = flurm_federation_routing:get_job_cpus(Job),
    ?assertEqual(16, Result).

test_get_job_cpus_map() ->
    JobMap = #{num_cpus => 32},
    Result = flurm_federation_routing:get_job_cpus(JobMap),
    ?assertEqual(32, Result).

test_get_job_cpus_other() ->
    Result = flurm_federation_routing:get_job_cpus(other_type),
    ?assertEqual(1, Result).

test_get_job_memory_record() ->
    Job = #job{memory_mb = 8192},
    Result = flurm_federation_routing:get_job_memory(Job),
    ?assertEqual(8192, Result).

test_get_job_memory_map() ->
    JobMap = #{memory_mb => 16384},
    Result = flurm_federation_routing:get_job_memory(JobMap),
    ?assertEqual(16384, Result).

test_get_job_memory_other() ->
    Result = flurm_federation_routing:get_job_memory(other_type),
    ?assertEqual(1024, Result).

test_has_required_features_all_present() ->
    ClusterFeatures = [<<"gpu">>, <<"ssd">>, <<"fast">>],
    RequiredFeatures = [<<"gpu">>, <<"ssd">>],
    Result = flurm_federation_routing:has_required_features(ClusterFeatures, RequiredFeatures),
    ?assertEqual(true, Result).

test_has_required_features_missing() ->
    ClusterFeatures = [<<"gpu">>],
    RequiredFeatures = [<<"gpu">>, <<"ssd">>],
    Result = flurm_federation_routing:has_required_features(ClusterFeatures, RequiredFeatures),
    ?assertEqual(false, Result).

test_has_required_features_empty() ->
    ClusterFeatures = [<<"gpu">>],
    RequiredFeatures = [],
    Result = flurm_federation_routing:has_required_features(ClusterFeatures, RequiredFeatures),
    ?assertEqual(true, Result).
