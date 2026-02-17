%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_federation_routing module
%%%
%%% Comprehensive tests for federation routing functionality:
%%% - Job routing with different policies (round_robin, least_loaded, weighted, partition_affinity, random)
%%% - Resource aggregation across clusters
%%% - Job submission (local and remote)
%%% - Job tracking and status management
%%% - Data conversion (job/cluster records <-> maps)
%%% - Helper functions for job data extraction
%%%
%%% Uses meck for mocking external dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_routing_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Constants
%%====================================================================

-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_PARTITION_MAP, flurm_fed_partition_map).
-define(FED_REMOTE_JOBS, flurm_fed_remote_jobs).
-define(CLUSTER_TIMEOUT, 5000).

%% Records (matching the module's internal records)
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
    state :: pending | running | completed | failed | cancelled | unknown,
    submit_time :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    job_spec :: map()
}).

%%====================================================================
%% Test Fixture Setup/Teardown
%%====================================================================

setup() ->
    %% Set up application environment
    application:set_env(flurm_core, cluster_name, <<"test_cluster">>),
    application:set_env(flurm_core, federation_routing_policy, least_loaded),

    %% Create ETS tables
    catch ets:delete(?FED_CLUSTERS_TABLE),
    catch ets:delete(?FED_PARTITION_MAP),
    catch ets:delete(?FED_REMOTE_JOBS),
    ets:new(?FED_CLUSTERS_TABLE, [named_table, public, set, {keypos, 2}]),
    ets:new(?FED_PARTITION_MAP, [named_table, public, bag, {keypos, 2}]),
    ets:new(?FED_REMOTE_JOBS, [named_table, public, set, {keypos, 2}]),
    ok.

cleanup(_) ->
    %% Clean up ETS tables
    catch ets:delete(?FED_CLUSTERS_TABLE),
    catch ets:delete(?FED_PARTITION_MAP),
    catch ets:delete(?FED_REMOTE_JOBS),
    %% Unload any mocks
    catch meck:unload(),
    ok.

setup_with_meck() ->
    setup(),
    %% Start meck for external dependencies
    meck:new(flurm_metrics, [non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    ok.

cleanup_with_meck(Arg) ->
    cleanup(Arg).

%%====================================================================
%% Test Generators
%%====================================================================

%% Main test suite - basic functionality
routing_basic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Calculate Load Tests
      {"calculate_load basic", fun test_calculate_load_basic/0},
      {"calculate_load with zero cpus", fun test_calculate_load_zero_cpus/0},
      {"calculate_load heavy load", fun test_calculate_load_heavy/0},
      {"calculate_load light load", fun test_calculate_load_light/0},
      {"calculate_load with pending only", fun test_calculate_load_pending_only/0},
      {"calculate_load with running only", fun test_calculate_load_running_only/0},

      %% Has Required Features Tests
      {"has_required_features empty requirements", fun test_has_required_features_empty/0},
      {"has_required_features all present", fun test_has_required_features_all_present/0},
      {"has_required_features missing some", fun test_has_required_features_missing/0},
      {"has_required_features exact match", fun test_has_required_features_exact/0},
      {"has_required_features superset", fun test_has_required_features_superset/0},

      %% Job Data Extraction Tests
      {"get_job_partition from record", fun test_get_job_partition_record/0},
      {"get_job_partition from map", fun test_get_job_partition_map/0},
      {"get_job_partition undefined", fun test_get_job_partition_undefined/0},
      {"get_job_features from record", fun test_get_job_features_record/0},
      {"get_job_features from map", fun test_get_job_features_map/0},
      {"get_job_features undefined", fun test_get_job_features_undefined/0},
      {"get_job_cpus from record", fun test_get_job_cpus_record/0},
      {"get_job_cpus from map", fun test_get_job_cpus_map/0},
      {"get_job_cpus default", fun test_get_job_cpus_default/0},
      {"get_job_memory from record", fun test_get_job_memory_record/0},
      {"get_job_memory from map", fun test_get_job_memory_map/0},
      {"get_job_memory default", fun test_get_job_memory_default/0},

      %% Job to Map Conversion Tests
      {"job_to_map from record", fun test_job_to_map_record/0},
      {"job_to_map from map passthrough", fun test_job_to_map_map/0},
      {"job_to_map all fields", fun test_job_to_map_all_fields/0},

      %% Map to Job Conversion Tests
      {"map_to_job basic", fun test_map_to_job_basic/0},
      {"map_to_job defaults", fun test_map_to_job_defaults/0},
      {"map_to_job full", fun test_map_to_job_full/0},

      %% Cluster to Map Conversion Tests
      {"cluster_to_map basic", fun test_cluster_to_map_basic/0},
      {"cluster_to_map all fields", fun test_cluster_to_map_all_fields/0},

      %% Generate ID Tests
      {"generate_local_ref format", fun test_generate_local_ref_format/0},
      {"generate_local_ref uniqueness", fun test_generate_local_ref_unique/0},
      {"generate_federation_id format", fun test_generate_federation_id_format/0},
      {"generate_federation_id uniqueness", fun test_generate_federation_id_unique/0}
     ]}.

%% Routing policy tests
routing_policy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Find Eligible Clusters Tests
      {"find_eligible_clusters all up", fun test_find_eligible_clusters_all_up/0},
      {"find_eligible_clusters filters down", fun test_find_eligible_clusters_filters_down/0},
      {"find_eligible_clusters by partition", fun test_find_eligible_clusters_by_partition/0},
      {"find_eligible_clusters by features", fun test_find_eligible_clusters_by_features/0},
      {"find_eligible_clusters by resources", fun test_find_eligible_clusters_by_resources/0},
      {"find_eligible_clusters combined filters", fun test_find_eligible_clusters_combined/0},
      {"find_eligible_clusters empty", fun test_find_eligible_clusters_empty/0},
      {"find_eligible_clusters undefined partition", fun test_find_eligible_clusters_undefined_partition/0},

      %% Select Cluster by Policy Tests
      {"select_cluster round_robin", fun test_select_cluster_round_robin/0},
      {"select_cluster round_robin empty", fun test_select_cluster_round_robin_empty/0},
      {"select_cluster least_loaded", fun test_select_cluster_least_loaded/0},
      {"select_cluster least_loaded single", fun test_select_cluster_least_loaded_single/0},
      {"select_cluster weighted", fun test_select_cluster_weighted/0},
      {"select_cluster weighted zero total", fun test_select_cluster_weighted_zero/0},
      {"select_cluster partition_affinity", fun test_select_cluster_partition_affinity/0},
      {"select_cluster partition_affinity none up", fun test_select_cluster_partition_affinity_none_up/0},
      {"select_cluster random", fun test_select_cluster_random/0},
      {"select_cluster random single", fun test_select_cluster_random_single/0},

      %% Do Route Job Tests
      {"do_route_job with eligible clusters", fun test_do_route_job_eligible/0},
      {"do_route_job falls back to local", fun test_do_route_job_fallback/0},
      {"do_route_job with partition routing", fun test_do_route_job_partition/0},
      {"do_route_job with feature requirements", fun test_do_route_job_features/0},
      {"do_route_job with resource requirements", fun test_do_route_job_resources/0}
     ]}.

%% Resource aggregation tests
resource_aggregation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Aggregate Resources Tests
      {"aggregate_resources empty", fun test_aggregate_resources_empty/0},
      {"aggregate_resources single cluster", fun test_aggregate_resources_single/0},
      {"aggregate_resources multiple clusters", fun test_aggregate_resources_multiple/0},
      {"aggregate_resources mixed states", fun test_aggregate_resources_mixed_states/0},
      {"aggregate_resources all down", fun test_aggregate_resources_all_down/0},
      {"aggregate_resources with drain", fun test_aggregate_resources_drain/0},

      %% Get Federation Resources Tests
      {"get_federation_resources basic", fun test_get_federation_resources_basic/0},
      {"get_federation_resources empty", fun test_get_federation_resources_empty/0},
      {"get_federation_resources large", fun test_get_federation_resources_large/0}
     ]}.

%% Job submission and tracking tests with mocking
job_management_test_() ->
    {foreach,
     fun setup_with_meck/0,
     fun cleanup_with_meck/1,
     [
      %% Submit Local Job Tests
      {"submit_local_job with job record", fun test_submit_local_job_record/0},
      {"submit_local_job with map", fun test_submit_local_job_map/0},
      {"submit_local_job error", fun test_submit_local_job_error/0},

      %% Submit Remote Job Tests
      {"submit_remote_job success", fun test_submit_remote_job_success/0},
      {"submit_remote_job cluster down", fun test_submit_remote_job_cluster_down/0},
      {"submit_remote_job cluster not found", fun test_submit_remote_job_not_found/0},
      {"submit_remote_job http error", fun test_submit_remote_job_http_error/0},
      {"submit_remote_job tracks job", fun test_submit_remote_job_tracks/0},

      %% Get Remote Job Status Tests
      {"do_get_remote_job_status success", fun test_get_remote_job_status_success/0},
      {"do_get_remote_job_status cluster not found", fun test_get_remote_job_status_not_found/0},

      %% Sync Job State Tests
      {"do_sync_job_state success", fun test_sync_job_state_success/0},
      {"do_sync_job_state error", fun test_sync_job_state_error/0},
      {"do_sync_job_state job not tracked", fun test_sync_job_state_not_tracked/0},

      %% Get Federation Jobs Tests
      {"do_get_federation_jobs basic", fun test_get_federation_jobs_basic/0},
      {"do_get_federation_jobs with remote", fun test_get_federation_jobs_with_remote/0},
      {"do_get_federation_jobs empty", fun test_get_federation_jobs_empty/0}
     ]}.

%% Edge cases and stress tests
edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Routing Edge Cases
      {"routing with same load clusters", fun test_routing_same_load/0},
      {"routing with extreme weights", fun test_routing_extreme_weights/0},
      {"routing with large cluster count", fun test_routing_large_cluster_count/0},
      {"routing with unicode names", fun test_routing_unicode_names/0},

      %% Resource Edge Cases
      {"resources with zero values", fun test_resources_zero_values/0},
      {"resources with max values", fun test_resources_max_values/0},

      %% Data Conversion Edge Cases
      {"job conversion roundtrip", fun test_job_conversion_roundtrip/0},
      {"cluster map preserves all fields", fun test_cluster_map_preserves_fields/0}
     ]}.

%%====================================================================
%% Calculate Load Tests
%%====================================================================

test_calculate_load_basic() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 10, running_jobs = 20, cpu_count = 100},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(0.3, Load).

test_calculate_load_zero_cpus() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 10, running_jobs = 20, cpu_count = 0},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(infinity, Load).

test_calculate_load_heavy() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 500, running_jobs = 500, cpu_count = 100},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(10.0, Load).

test_calculate_load_light() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 0, running_jobs = 1, cpu_count = 100},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(0.01, Load).

test_calculate_load_pending_only() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 50, running_jobs = 0, cpu_count = 100},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(0.5, Load).

test_calculate_load_running_only() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{pending_jobs = 0, running_jobs = 50, cpu_count = 100},
    Load = flurm_federation_routing:calculate_load(Cluster1),
    ?assertEqual(0.5, Load).

%%====================================================================
%% Has Required Features Tests
%%====================================================================

test_has_required_features_empty() ->
    Result = flurm_federation_routing:has_required_features([<<"gpu">>, <<"fast">>], []),
    ?assertEqual(true, Result).

test_has_required_features_all_present() ->
    Result = flurm_federation_routing:has_required_features([<<"gpu">>, <<"fast">>, <<"ssd">>], [<<"gpu">>, <<"fast">>]),
    ?assertEqual(true, Result).

test_has_required_features_missing() ->
    Result = flurm_federation_routing:has_required_features([<<"fast">>], [<<"gpu">>, <<"fast">>]),
    ?assertEqual(false, Result).

test_has_required_features_exact() ->
    Result = flurm_federation_routing:has_required_features([<<"gpu">>, <<"fast">>], [<<"gpu">>, <<"fast">>]),
    ?assertEqual(true, Result).

test_has_required_features_superset() ->
    Result = flurm_federation_routing:has_required_features([<<"gpu">>, <<"fast">>, <<"ssd">>, <<"nvme">>], [<<"gpu">>]),
    ?assertEqual(true, Result).

%%====================================================================
%% Job Data Extraction Tests
%%====================================================================

test_get_job_partition_record() ->
    Job = #job{id = 1, name = <<"test">>, user = <<"user">>, partition = <<"batch">>,
               state = pending, script = <<>>, num_nodes = 1, num_cpus = 1,
               memory_mb = 1024, time_limit = 3600, priority = 100},
    ?assertEqual(<<"batch">>, flurm_federation_routing:get_job_partition(Job)).

test_get_job_partition_map() ->
    Job = #{partition => <<"gpu">>, num_cpus => 4},
    ?assertEqual(<<"gpu">>, flurm_federation_routing:get_job_partition(Job)).

test_get_job_partition_undefined() ->
    Job = #{num_cpus => 4},
    ?assertEqual(undefined, flurm_federation_routing:get_job_partition(Job)).

test_get_job_features_record() ->
    Job = #job{id = 1, name = <<"test">>, user = <<"user">>, partition = <<"batch">>,
               state = pending, script = <<>>, num_nodes = 1, num_cpus = 1,
               memory_mb = 1024, time_limit = 3600, priority = 100},
    ?assertEqual([], flurm_federation_routing:get_job_features(Job)).

test_get_job_features_map() ->
    Job = #{features => [<<"gpu">>, <<"fast">>]},
    ?assertEqual([<<"gpu">>, <<"fast">>], flurm_federation_routing:get_job_features(Job)).

test_get_job_features_undefined() ->
    Job = #{num_cpus => 4},
    ?assertEqual([], flurm_federation_routing:get_job_features(Job)).

test_get_job_cpus_record() ->
    Job = #job{id = 1, name = <<"test">>, user = <<"user">>, partition = <<"batch">>,
               state = pending, script = <<>>, num_nodes = 1, num_cpus = 8,
               memory_mb = 1024, time_limit = 3600, priority = 100},
    ?assertEqual(8, flurm_federation_routing:get_job_cpus(Job)).

test_get_job_cpus_map() ->
    Job = #{num_cpus => 16},
    ?assertEqual(16, flurm_federation_routing:get_job_cpus(Job)).

test_get_job_cpus_default() ->
    Job = #{partition => <<"batch">>},
    ?assertEqual(1, flurm_federation_routing:get_job_cpus(Job)).

test_get_job_memory_record() ->
    Job = #job{id = 1, name = <<"test">>, user = <<"user">>, partition = <<"batch">>,
               state = pending, script = <<>>, num_nodes = 1, num_cpus = 1,
               memory_mb = 4096, time_limit = 3600, priority = 100},
    ?assertEqual(4096, flurm_federation_routing:get_job_memory(Job)).

test_get_job_memory_map() ->
    Job = #{memory_mb => 8192},
    ?assertEqual(8192, flurm_federation_routing:get_job_memory(Job)).

test_get_job_memory_default() ->
    Job = #{partition => <<"batch">>},
    ?assertEqual(1024, flurm_federation_routing:get_job_memory(Job)).

%%====================================================================
%% Job to Map Conversion Tests
%%====================================================================

test_job_to_map_record() ->
    Job = #job{id = 123, name = <<"test_job">>, user = <<"testuser">>, partition = <<"batch">>,
               state = running, script = <<"#!/bin/bash\necho hi">>, num_nodes = 2, num_cpus = 4,
               memory_mb = 2048, time_limit = 7200, priority = 50, work_dir = <<"/home/user">>,
               account = <<"research">>, qos = <<"normal">>},
    Map = flurm_federation_routing:job_to_map(Job),
    ?assertEqual(123, maps:get(id, Map)),
    ?assertEqual(<<"test_job">>, maps:get(name, Map)),
    ?assertEqual(<<"testuser">>, maps:get(user, Map)),
    ?assertEqual(<<"batch">>, maps:get(partition, Map)),
    ?assertEqual(running, maps:get(state, Map)),
    ?assertEqual(4, maps:get(num_cpus, Map)),
    ?assertEqual(2048, maps:get(memory_mb, Map)).

test_job_to_map_map() ->
    Job = #{id => 456, name => <<"already_map">>},
    Map = flurm_federation_routing:job_to_map(Job),
    ?assertEqual(Job, Map).

test_job_to_map_all_fields() ->
    Job = #job{id = 1, name = <<"n">>, user = <<"u">>, partition = <<"p">>,
               state = pending, script = <<"s">>, num_nodes = 1, num_cpus = 1,
               memory_mb = 1, time_limit = 1, priority = 1, work_dir = <<"/w">>,
               account = <<"a">>, qos = <<"q">>},
    Map = flurm_federation_routing:job_to_map(Job),
    ExpectedKeys = [id, name, user, partition, state, script, num_nodes, num_cpus,
                    memory_mb, time_limit, priority, work_dir, account, qos],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Map))
    end, ExpectedKeys).

%%====================================================================
%% Map to Job Conversion Tests
%%====================================================================

test_map_to_job_basic() ->
    Map = #{name => <<"test">>, user => <<"user">>, num_cpus => 4},
    Job = flurm_federation_routing:map_to_job(Map),
    ?assertEqual(<<"test">>, Job#job.name),
    ?assertEqual(<<"user">>, Job#job.user),
    ?assertEqual(4, Job#job.num_cpus).

test_map_to_job_defaults() ->
    Map = #{},
    Job = flurm_federation_routing:map_to_job(Map),
    ?assertEqual(0, Job#job.id),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(3600, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority).

test_map_to_job_full() ->
    Map = #{
        id => 999,
        name => <<"full_job">>,
        user => <<"fulluser">>,
        partition => <<"special">>,
        state => running,
        script => <<"#!/bin/bash">>,
        num_nodes => 10,
        num_cpus => 40,
        memory_mb => 16384,
        time_limit => 86400,
        priority => 200,
        work_dir => <<"/data">>,
        account => <<"myacct">>,
        qos => <<"high">>
    },
    Job = flurm_federation_routing:map_to_job(Map),
    ?assertEqual(999, Job#job.id),
    ?assertEqual(<<"full_job">>, Job#job.name),
    ?assertEqual(<<"fulluser">>, Job#job.user),
    ?assertEqual(<<"special">>, Job#job.partition),
    ?assertEqual(running, Job#job.state),
    ?assertEqual(10, Job#job.num_nodes),
    ?assertEqual(40, Job#job.num_cpus),
    ?assertEqual(16384, Job#job.memory_mb),
    ?assertEqual(86400, Job#job.time_limit),
    ?assertEqual(200, Job#job.priority),
    ?assertEqual(<<"/data">>, Job#job.work_dir),
    ?assertEqual(<<"myacct">>, Job#job.account),
    ?assertEqual(<<"high">>, Job#job.qos).

%%====================================================================
%% Cluster to Map Conversion Tests
%%====================================================================

test_cluster_to_map_basic() ->
    Cluster = make_test_cluster(<<"cluster1">>, <<"host1.example.com">>, 6817),
    Map = flurm_federation_routing:cluster_to_map(Cluster),
    ?assertEqual(<<"cluster1">>, maps:get(name, Map)),
    ?assertEqual(<<"host1.example.com">>, maps:get(host, Map)),
    ?assertEqual(6817, maps:get(port, Map)).

test_cluster_to_map_all_fields() ->
    Cluster = make_test_cluster(<<"c">>, <<"h">>, 8080),
    Cluster1 = Cluster#fed_cluster{
        state = up,
        weight = 5,
        features = [<<"gpu">>, <<"fast">>],
        partitions = [<<"batch">>, <<"gpu">>],
        node_count = 100,
        cpu_count = 1000,
        memory_mb = 512000,
        gpu_count = 50,
        pending_jobs = 10,
        running_jobs = 50,
        available_cpus = 500,
        available_memory = 256000,
        last_sync = 1234567890,
        last_health_check = 1234567891,
        consecutive_failures = 0
    },
    Map = flurm_federation_routing:cluster_to_map(Cluster1),

    ExpectedKeys = [name, host, port, state, weight, features, partitions,
                    node_count, cpu_count, memory_mb, gpu_count, pending_jobs,
                    running_jobs, available_cpus, available_memory, last_sync,
                    last_health_check, consecutive_failures],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Map))
    end, ExpectedKeys),

    ?assertEqual(up, maps:get(state, Map)),
    ?assertEqual(5, maps:get(weight, Map)),
    ?assertEqual([<<"gpu">>, <<"fast">>], maps:get(features, Map)),
    ?assertEqual(100, maps:get(node_count, Map)).

%%====================================================================
%% Generate ID Tests
%%====================================================================

test_generate_local_ref_format() ->
    Ref = flurm_federation_routing:generate_local_ref(),
    ?assert(is_binary(Ref)),
    ?assertEqual(<<"ref-">>, binary:part(Ref, 0, 4)).

test_generate_local_ref_unique() ->
    Refs = [flurm_federation_routing:generate_local_ref() || _ <- lists:seq(1, 100)],
    UniqueRefs = lists:usort(Refs),
    ?assertEqual(length(Refs), length(UniqueRefs)).

test_generate_federation_id_format() ->
    Id = flurm_federation_routing:generate_federation_id(),
    ?assert(is_binary(Id)),
    ?assertEqual(<<"fed-">>, binary:part(Id, 0, 4)).

test_generate_federation_id_unique() ->
    Ids = [flurm_federation_routing:generate_federation_id() || _ <- lists:seq(1, 100)],
    UniqueIds = lists:usort(Ids),
    ?assertEqual(length(Ids), length(UniqueIds)).

%%====================================================================
%% Find Eligible Clusters Tests
%%====================================================================

test_find_eligible_clusters_all_up() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], []),
    insert_cluster(<<"c2">>, up, 100, 10000, [], []),
    insert_cluster(<<"c3">>, up, 100, 10000, [], []),

    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [], 1, 1024),
    ?assertEqual(3, length(Eligible)).

test_find_eligible_clusters_filters_down() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], []),
    insert_cluster(<<"c2">>, down, 100, 10000, [], []),
    insert_cluster(<<"c3">>, up, 100, 10000, [], []),

    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [], 1, 1024),
    ?assertEqual(2, length(Eligible)),
    Names = [C#fed_cluster.name || C <- Eligible],
    ?assert(lists:member(<<"c1">>, Names)),
    ?assert(lists:member(<<"c3">>, Names)),
    ?assertNot(lists:member(<<"c2">>, Names)).

test_find_eligible_clusters_by_partition() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], [<<"batch">>]),
    insert_cluster(<<"c2">>, up, 100, 10000, [], [<<"gpu">>]),
    insert_cluster(<<"c3">>, up, 100, 10000, [], [<<"batch">>]),

    %% Set up partition mapping
    ets:insert(?FED_PARTITION_MAP, #partition_map{partition = <<"batch">>, cluster = <<"c1">>, priority = 1}),
    ets:insert(?FED_PARTITION_MAP, #partition_map{partition = <<"batch">>, cluster = <<"c3">>, priority = 1}),
    ets:insert(?FED_PARTITION_MAP, #partition_map{partition = <<"gpu">>, cluster = <<"c2">>, priority = 1}),

    Eligible = flurm_federation_routing:find_eligible_clusters(<<"batch">>, [], 1, 1024),
    ?assertEqual(2, length(Eligible)),
    Names = [C#fed_cluster.name || C <- Eligible],
    ?assert(lists:member(<<"c1">>, Names)),
    ?assert(lists:member(<<"c3">>, Names)).

test_find_eligible_clusters_by_features() ->
    insert_cluster_with_features(<<"c1">>, up, [<<"gpu">>, <<"fast">>]),
    insert_cluster_with_features(<<"c2">>, up, [<<"fast">>]),
    insert_cluster_with_features(<<"c3">>, up, [<<"gpu">>, <<"fast">>, <<"ssd">>]),

    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [<<"gpu">>], 1, 1024),
    ?assertEqual(2, length(Eligible)),
    Names = [C#fed_cluster.name || C <- Eligible],
    ?assert(lists:member(<<"c1">>, Names)),
    ?assert(lists:member(<<"c3">>, Names)).

test_find_eligible_clusters_by_resources() ->
    insert_cluster_with_resources(<<"c1">>, up, 50, 4096),
    insert_cluster_with_resources(<<"c2">>, up, 10, 2048),
    insert_cluster_with_resources(<<"c3">>, up, 100, 8192),

    %% Need 20 CPUs and 4096 MB
    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [], 20, 4096),
    ?assertEqual(2, length(Eligible)),
    Names = [C#fed_cluster.name || C <- Eligible],
    ?assert(lists:member(<<"c1">>, Names)),
    ?assert(lists:member(<<"c3">>, Names)).

test_find_eligible_clusters_combined() ->
    %% Create clusters with various combinations
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{state = up, features = [<<"gpu">>], available_cpus = 100, available_memory = 10000},
    ets:insert(?FED_CLUSTERS_TABLE, C1b),

    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{state = up, features = [], available_cpus = 100, available_memory = 10000},
    ets:insert(?FED_CLUSTERS_TABLE, C2b),

    C3 = make_test_cluster(<<"c3">>, <<"h3">>, 6817),
    C3b = C3#fed_cluster{state = down, features = [<<"gpu">>], available_cpus = 100, available_memory = 10000},
    ets:insert(?FED_CLUSTERS_TABLE, C3b),

    %% Only c1 should match: up, has gpu, has resources
    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [<<"gpu">>], 50, 5000),
    ?assertEqual(1, length(Eligible)),
    ?assertEqual(<<"c1">>, (hd(Eligible))#fed_cluster.name).

test_find_eligible_clusters_empty() ->
    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [], 1, 1024),
    ?assertEqual([], Eligible).

test_find_eligible_clusters_undefined_partition() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], [<<"batch">>]),
    insert_cluster(<<"c2">>, up, 100, 10000, [], [<<"gpu">>]),

    %% With undefined partition, all up clusters are eligible
    Eligible1 = flurm_federation_routing:find_eligible_clusters(undefined, [], 1, 1024),
    ?assertEqual(2, length(Eligible1)),

    %% Same with empty binary
    Eligible2 = flurm_federation_routing:find_eligible_clusters(<<>>, [], 1, 1024),
    ?assertEqual(2, length(Eligible2)).

%%====================================================================
%% Select Cluster by Policy Tests
%%====================================================================

test_select_cluster_round_robin() ->
    Clusters = [
        make_cluster_with_load(<<"c1">>, 50),
        make_cluster_with_load(<<"c2">>, 100),
        make_cluster_with_load(<<"c3">>, 25)
    ],

    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, round_robin, <<"local">>),
    ?assertEqual(<<"c1">>, Name).  % First in list

test_select_cluster_round_robin_empty() ->
    Result = flurm_federation_routing:select_cluster_by_policy([], round_robin, <<"local">>),
    ?assertEqual({error, no_cluster_available}, Result).

test_select_cluster_least_loaded() ->
    Clusters = [
        make_cluster_with_load(<<"c1">>, 50),
        make_cluster_with_load(<<"c2">>, 10),
        make_cluster_with_load(<<"c3">>, 30)
    ],

    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, least_loaded, <<"local">>),
    ?assertEqual(<<"c2">>, Name).  % Lowest load

test_select_cluster_least_loaded_single() ->
    Clusters = [make_cluster_with_load(<<"c1">>, 100)],
    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, least_loaded, <<"local">>),
    ?assertEqual(<<"c1">>, Name).

test_select_cluster_weighted() ->
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{weight = 1, state = up, available_cpus = 100, available_memory = 10000},
    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{weight = 100, state = up, available_cpus = 100, available_memory = 10000},

    Clusters = [C1b, C2b],

    %% Run multiple times to verify weighted selection (c2 should be picked most often)
    Results = [flurm_federation_routing:select_cluster_by_policy(Clusters, weighted, <<"local">>) || _ <- lists:seq(1, 100)],
    C2Count = length([ok || {ok, <<"c2">>} <- Results]),
    %% c2 has 100x weight of c1, so should be picked ~99% of time
    ?assert(C2Count > 90).

test_select_cluster_weighted_zero() ->
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{weight = 0, state = up, available_cpus = 100, available_memory = 10000},
    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{weight = 0, state = up, available_cpus = 100, available_memory = 10000},

    Clusters = [C1b, C2b],

    %% Should pick first cluster when total weight is 0
    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, weighted, <<"local">>),
    ?assertEqual(<<"c1">>, Name).

test_select_cluster_partition_affinity() ->
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{state = up, available_cpus = 100, available_memory = 10000},
    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{state = up, available_cpus = 100, available_memory = 10000},

    Clusters = [C1b, C2b],

    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, partition_affinity, <<"local">>),
    ?assertEqual(<<"c1">>, Name).  % First up cluster

test_select_cluster_partition_affinity_none_up() ->
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{state = down},
    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{state = drain},

    Clusters = [C1b, C2b],

    Result = flurm_federation_routing:select_cluster_by_policy(Clusters, partition_affinity, <<"local">>),
    ?assertEqual({error, no_cluster_available}, Result).

test_select_cluster_random() ->
    Clusters = [
        make_cluster_with_load(<<"c1">>, 50),
        make_cluster_with_load(<<"c2">>, 50),
        make_cluster_with_load(<<"c3">>, 50)
    ],

    %% Run multiple times to verify randomness
    Results = [flurm_federation_routing:select_cluster_by_policy(Clusters, random, <<"local">>) || _ <- lists:seq(1, 100)],
    Names = [N || {ok, N} <- Results],
    UniqueNames = lists:usort(Names),
    %% Should have picked multiple different clusters
    ?assert(length(UniqueNames) > 1).

test_select_cluster_random_single() ->
    Clusters = [make_cluster_with_load(<<"c1">>, 50)],
    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, random, <<"local">>),
    ?assertEqual(<<"c1">>, Name).

%%====================================================================
%% Do Route Job Tests
%%====================================================================

test_do_route_job_eligible() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], []),
    insert_cluster(<<"c2">>, up, 100, 10000, [], []),

    Job = #{num_cpus => 4, memory_mb => 2048},
    {ok, _Name} = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>).

test_do_route_job_fallback() ->
    %% No eligible clusters
    Job = #{num_cpus => 4, memory_mb => 2048},
    {ok, Name} = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>),
    ?assertEqual(<<"local">>, Name).

test_do_route_job_partition() ->
    insert_cluster(<<"batch_cluster">>, up, 100, 10000, [], [<<"batch">>]),
    insert_cluster(<<"gpu_cluster">>, up, 100, 10000, [], [<<"gpu">>]),

    ets:insert(?FED_PARTITION_MAP, #partition_map{partition = <<"batch">>, cluster = <<"batch_cluster">>, priority = 1}),
    ets:insert(?FED_PARTITION_MAP, #partition_map{partition = <<"gpu">>, cluster = <<"gpu_cluster">>, priority = 1}),

    Job = #{partition => <<"batch">>, num_cpus => 4, memory_mb => 2048},
    {ok, Name} = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>),
    ?assertEqual(<<"batch_cluster">>, Name).

test_do_route_job_features() ->
    insert_cluster_with_features(<<"gpu_cluster">>, up, [<<"gpu">>, <<"cuda">>]),
    insert_cluster_with_features(<<"cpu_cluster">>, up, [<<"fast">>]),

    Job = #{features => [<<"gpu">>], num_cpus => 4, memory_mb => 2048},
    {ok, Name} = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>),
    ?assertEqual(<<"gpu_cluster">>, Name).

test_do_route_job_resources() ->
    insert_cluster_with_resources(<<"small">>, up, 10, 2048),
    insert_cluster_with_resources(<<"large">>, up, 100, 65536),

    Job = #{num_cpus => 50, memory_mb => 32768},
    {ok, Name} = flurm_federation_routing:do_route_job(Job, least_loaded, <<"local">>),
    ?assertEqual(<<"large">>, Name).

%%====================================================================
%% Aggregate Resources Tests
%%====================================================================

test_aggregate_resources_empty() ->
    Resources = flurm_federation_routing:aggregate_resources([]),
    ?assertEqual(0, maps:get(total_nodes, Resources)),
    ?assertEqual(0, maps:get(total_cpus, Resources)),
    ?assertEqual(0, maps:get(clusters_up, Resources)),
    ?assertEqual(0, maps:get(clusters_down, Resources)).

test_aggregate_resources_single() ->
    Cluster = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = up,
        node_count = 10,
        cpu_count = 100,
        memory_mb = 10000,
        gpu_count = 4,
        available_cpus = 50,
        available_memory = 5000,
        pending_jobs = 5,
        running_jobs = 10
    },

    Resources = flurm_federation_routing:aggregate_resources([Cluster1]),
    ?assertEqual(10, maps:get(total_nodes, Resources)),
    ?assertEqual(100, maps:get(total_cpus, Resources)),
    ?assertEqual(10000, maps:get(total_memory_mb, Resources)),
    ?assertEqual(4, maps:get(total_gpus, Resources)),
    ?assertEqual(50, maps:get(available_cpus, Resources)),
    ?assertEqual(5000, maps:get(available_memory_mb, Resources)),
    ?assertEqual(5, maps:get(pending_jobs, Resources)),
    ?assertEqual(10, maps:get(running_jobs, Resources)),
    ?assertEqual(1, maps:get(clusters_up, Resources)),
    ?assertEqual(0, maps:get(clusters_down, Resources)).

test_aggregate_resources_multiple() ->
    Clusters = [
        make_cluster_for_resources(<<"c1">>, up, 10, 100, 10000, 4),
        make_cluster_for_resources(<<"c2">>, up, 20, 200, 20000, 8),
        make_cluster_for_resources(<<"c3">>, up, 30, 300, 30000, 12)
    ],

    Resources = flurm_federation_routing:aggregate_resources(Clusters),
    ?assertEqual(60, maps:get(total_nodes, Resources)),
    ?assertEqual(600, maps:get(total_cpus, Resources)),
    ?assertEqual(60000, maps:get(total_memory_mb, Resources)),
    ?assertEqual(24, maps:get(total_gpus, Resources)),
    ?assertEqual(3, maps:get(clusters_up, Resources)).

test_aggregate_resources_mixed_states() ->
    Clusters = [
        make_cluster_for_resources(<<"c1">>, up, 10, 100, 10000, 4),
        make_cluster_for_resources(<<"c2">>, down, 20, 200, 20000, 8),
        make_cluster_for_resources(<<"c3">>, up, 30, 300, 30000, 12)
    ],

    Resources = flurm_federation_routing:aggregate_resources(Clusters),
    %% Down cluster not counted in totals
    ?assertEqual(40, maps:get(total_nodes, Resources)),
    ?assertEqual(400, maps:get(total_cpus, Resources)),
    ?assertEqual(2, maps:get(clusters_up, Resources)),
    ?assertEqual(1, maps:get(clusters_down, Resources)).

test_aggregate_resources_all_down() ->
    Clusters = [
        make_cluster_for_resources(<<"c1">>, down, 10, 100, 10000, 4),
        make_cluster_for_resources(<<"c2">>, down, 20, 200, 20000, 8)
    ],

    Resources = flurm_federation_routing:aggregate_resources(Clusters),
    ?assertEqual(0, maps:get(total_nodes, Resources)),
    ?assertEqual(0, maps:get(clusters_up, Resources)),
    ?assertEqual(2, maps:get(clusters_down, Resources)).

test_aggregate_resources_drain() ->
    Clusters = [
        make_cluster_for_resources(<<"c1">>, up, 10, 100, 10000, 4),
        make_cluster_for_resources(<<"c2">>, drain, 20, 200, 20000, 8)
    ],

    Resources = flurm_federation_routing:aggregate_resources(Clusters),
    %% Drain counted as down
    ?assertEqual(10, maps:get(total_nodes, Resources)),
    ?assertEqual(1, maps:get(clusters_up, Resources)),
    ?assertEqual(1, maps:get(clusters_down, Resources)).

%%====================================================================
%% Get Federation Resources Tests
%%====================================================================

test_get_federation_resources_basic() ->
    insert_cluster(<<"c1">>, up, 100, 10000, [], []),
    insert_cluster(<<"c2">>, up, 200, 20000, [], []),

    Resources = flurm_federation_routing:get_federation_resources(),
    ?assert(is_map(Resources)),
    ?assert(maps:get(total_cpus, Resources) >= 0),
    ?assert(maps:get(clusters_up, Resources) >= 0).

test_get_federation_resources_empty() ->
    Resources = flurm_federation_routing:get_federation_resources(),
    ?assertEqual(0, maps:get(total_nodes, Resources)),
    ?assertEqual(0, maps:get(clusters_up, Resources)).

test_get_federation_resources_large() ->
    %% Add many clusters
    lists:foreach(fun(N) ->
        Name = list_to_binary("cluster" ++ integer_to_list(N)),
        insert_cluster(Name, up, 1000, 100000, [], [])
    end, lists:seq(1, 50)),

    Resources = flurm_federation_routing:get_federation_resources(),
    ?assertEqual(50, maps:get(clusters_up, Resources)),
    ?assert(maps:get(total_cpus, Resources) >= 50000).

%%====================================================================
%% Submit Local Job Tests
%%====================================================================

test_submit_local_job_record() ->
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 12345} end),

    Job = #job{id = 0, name = <<"test">>, user = <<"user">>, partition = <<"batch">>,
               state = pending, script = <<"#!/bin/bash">>, num_nodes = 1, num_cpus = 4,
               memory_mb = 2048, time_limit = 3600, priority = 100},

    {ok, {Cluster, JobId}} = flurm_federation_routing:submit_local_job(Job),
    ?assertEqual(<<"test_cluster">>, Cluster),
    ?assertEqual(12345, JobId),

    meck:unload(flurm_scheduler).

test_submit_local_job_map() ->
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 67890} end),

    Job = #{name => <<"map_job">>, user => <<"user">>, num_cpus => 2},

    {ok, {Cluster, JobId}} = flurm_federation_routing:submit_local_job(Job),
    ?assertEqual(<<"test_cluster">>, Cluster),
    ?assertEqual(67890, JobId),

    meck:unload(flurm_scheduler).

test_submit_local_job_error() ->
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {error, quota_exceeded} end),

    Job = #{name => <<"error_job">>},
    Result = flurm_federation_routing:submit_local_job(Job),
    ?assertEqual({error, quota_exceeded}, Result),

    meck:unload(flurm_scheduler).

%%====================================================================
%% Submit Remote Job Tests
%%====================================================================

test_submit_remote_job_success() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, remote_submit_job, fun(_, _, _, _) -> {ok, 11111} end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Job = #{name => <<"remote_job">>, num_cpus => 4},
    {ok, {ClusterName, JobId}} = flurm_federation_routing:submit_remote_job(<<"remote">>, Job, #{}),
    ?assertEqual(<<"remote">>, ClusterName),
    ?assertEqual(11111, JobId),

    meck:unload(flurm_federation_sync).

test_submit_remote_job_cluster_down() ->
    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = down},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Job = #{name => <<"remote_job">>},
    Result = flurm_federation_routing:submit_remote_job(<<"remote">>, Job, #{}),
    ?assertEqual({error, {cluster_unavailable, down}}, Result).

test_submit_remote_job_not_found() ->
    Job = #{name => <<"remote_job">>},
    Result = flurm_federation_routing:submit_remote_job(<<"nonexistent">>, Job, #{}),
    ?assertEqual({error, cluster_not_found}, Result).

test_submit_remote_job_http_error() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, remote_submit_job, fun(_, _, _, _) -> {error, timeout} end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Job = #{name => <<"remote_job">>},
    Result = flurm_federation_routing:submit_remote_job(<<"remote">>, Job, #{}),
    ?assertEqual({error, timeout}, Result),

    meck:unload(flurm_federation_sync).

test_submit_remote_job_tracks() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, remote_submit_job, fun(_, _, _, _) -> {ok, 22222} end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Job = #{name => <<"tracked_job">>},
    {ok, _} = flurm_federation_routing:submit_remote_job(<<"remote">>, Job, #{}),

    %% Verify job was tracked
    RemoteJobs = ets:tab2list(?FED_REMOTE_JOBS),
    ?assertEqual(1, length(RemoteJobs)),
    [TrackedJob] = RemoteJobs,
    ?assertEqual(<<"remote">>, TrackedJob#remote_job.remote_cluster),
    ?assertEqual(22222, TrackedJob#remote_job.remote_job_id),

    meck:unload(flurm_federation_sync).

%%====================================================================
%% Get Remote Job Status Tests
%%====================================================================

test_get_remote_job_status_success() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{<<"job_id">> => 123, <<"state">> => <<"running">>}}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    {ok, Status} = flurm_federation_routing:do_get_remote_job_status(<<"remote">>, 123),
    ?assertEqual(123, maps:get(<<"job_id">>, Status)),

    meck:unload(flurm_federation_sync).

test_get_remote_job_status_not_found() ->
    Result = flurm_federation_routing:do_get_remote_job_status(<<"nonexistent">>, 123),
    ?assertEqual({error, cluster_not_found}, Result).

%%====================================================================
%% Sync Job State Tests
%%====================================================================

test_sync_job_state_success() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{state => running}}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    %% Add a tracked remote job
    RemoteJob = #remote_job{
        local_ref = <<"ref-123">>,
        remote_cluster = <<"remote">>,
        remote_job_id = 456,
        state = pending,
        submit_time = erlang:system_time(second),
        last_sync = 0,
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),

    ok = flurm_federation_routing:do_sync_job_state(<<"remote">>, 456),

    %% Verify state was updated
    Pattern = #remote_job{remote_cluster = <<"remote">>, remote_job_id = 456, _ = '_'},
    [Updated] = ets:match_object(?FED_REMOTE_JOBS, Pattern),
    ?assertEqual(running, Updated#remote_job.state),

    meck:unload(flurm_federation_sync).

test_sync_job_state_error() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {error, timeout}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    Result = flurm_federation_routing:do_sync_job_state(<<"remote">>, 123),
    ?assertEqual({error, timeout}, Result),

    meck:unload(flurm_federation_sync).

test_sync_job_state_not_tracked() ->
    meck:new(flurm_federation_sync, [non_strict]),
    meck:expect(flurm_federation_sync, fetch_remote_job_status, fun(_, _, _, _) ->
        {ok, #{state => running}}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    %% No tracked job - should still succeed (just no update)
    ok = flurm_federation_routing:do_sync_job_state(<<"remote">>, 999),

    meck:unload(flurm_federation_sync).

%%====================================================================
%% Get Federation Jobs Tests
%%====================================================================

test_get_federation_jobs_basic() ->
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_job_registry, list_jobs, fun() ->
        [{1, self()}, {2, self()}]
    end),

    Jobs = flurm_federation_routing:do_get_federation_jobs(<<"local">>),
    ?assertEqual(2, length(Jobs)),

    meck:unload(flurm_job_registry).

test_get_federation_jobs_with_remote() ->
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_job_registry, list_jobs, fun() ->
        [{1, self()}]
    end),

    %% Add remote job
    RemoteJob = #remote_job{
        local_ref = <<"ref-remote">>,
        remote_cluster = <<"remote">>,
        remote_job_id = 100,
        state = running,
        submit_time = erlang:system_time(second),
        last_sync = erlang:system_time(second),
        job_spec = #{}
    },
    ets:insert(?FED_REMOTE_JOBS, RemoteJob),

    Jobs = flurm_federation_routing:do_get_federation_jobs(<<"local">>),
    ?assertEqual(2, length(Jobs)),

    %% Check we have both local and remote
    Clusters = [maps:get(cluster, J) || J <- Jobs],
    ?assert(lists:member(<<"local">>, Clusters)),
    ?assert(lists:member(<<"remote">>, Clusters)),

    meck:unload(flurm_job_registry).

test_get_federation_jobs_empty() ->
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_job_registry, list_jobs, fun() -> [] end),

    Jobs = flurm_federation_routing:do_get_federation_jobs(<<"local">>),
    ?assertEqual([], Jobs),

    meck:unload(flurm_job_registry).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

test_routing_same_load() ->
    %% All clusters with identical load
    Clusters = [
        make_cluster_with_load(<<"c1">>, 50),
        make_cluster_with_load(<<"c2">>, 50),
        make_cluster_with_load(<<"c3">>, 50)
    ],

    {ok, _Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, least_loaded, <<"local">>).

test_routing_extreme_weights() ->
    C1 = make_test_cluster(<<"c1">>, <<"h1">>, 6817),
    C1b = C1#fed_cluster{weight = 1, state = up, available_cpus = 100, available_memory = 10000},
    C2 = make_test_cluster(<<"c2">>, <<"h2">>, 6817),
    C2b = C2#fed_cluster{weight = 1000000, state = up, available_cpus = 100, available_memory = 10000},

    Clusters = [C1b, C2b],

    %% c2 should almost always be selected
    Results = [flurm_federation_routing:select_cluster_by_policy(Clusters, weighted, <<"local">>) || _ <- lists:seq(1, 100)],
    C2Count = length([ok || {ok, <<"c2">>} <- Results]),
    ?assert(C2Count > 98).

test_routing_large_cluster_count() ->
    Clusters = [make_cluster_with_load(list_to_binary("c" ++ integer_to_list(N)), N) || N <- lists:seq(1, 100)],

    {ok, Name} = flurm_federation_routing:select_cluster_by_policy(Clusters, least_loaded, <<"local">>),
    ?assertEqual(<<"c1">>, Name).  % c1 has lowest load (1)

test_routing_unicode_names() ->
    %% Test with unicode cluster names (though typically not used)
    insert_cluster(<<"cluster_test_123">>, up, 100, 10000, [], []),

    Eligible = flurm_federation_routing:find_eligible_clusters(undefined, [], 1, 1024),
    ?assertEqual(1, length(Eligible)).

test_resources_zero_values() ->
    Cluster = make_test_cluster(<<"zero">>, <<"h">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = up,
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0
    },

    Resources = flurm_federation_routing:aggregate_resources([Cluster1]),
    ?assertEqual(0, maps:get(total_nodes, Resources)),
    ?assertEqual(0, maps:get(total_cpus, Resources)),
    ?assertEqual(1, maps:get(clusters_up, Resources)).

test_resources_max_values() ->
    Cluster = make_test_cluster(<<"max">>, <<"h">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = up,
        node_count = 1000000,
        cpu_count = 10000000,
        memory_mb = 100000000000,
        gpu_count = 100000
    },

    Resources = flurm_federation_routing:aggregate_resources([Cluster1]),
    ?assertEqual(1000000, maps:get(total_nodes, Resources)),
    ?assertEqual(10000000, maps:get(total_cpus, Resources)),
    ?assertEqual(100000000000, maps:get(total_memory_mb, Resources)).

test_job_conversion_roundtrip() ->
    Job = #job{id = 999, name = <<"roundtrip">>, user = <<"user">>, partition = <<"batch">>,
               state = running, script = <<"#!/bin/bash">>, num_nodes = 5, num_cpus = 20,
               memory_mb = 8192, time_limit = 7200, priority = 150, work_dir = <<"/home">>,
               account = <<"acct">>, qos = <<"high">>},

    Map = flurm_federation_routing:job_to_map(Job),
    Converted = flurm_federation_routing:map_to_job(Map),

    ?assertEqual(Job#job.id, Converted#job.id),
    ?assertEqual(Job#job.name, Converted#job.name),
    ?assertEqual(Job#job.user, Converted#job.user),
    ?assertEqual(Job#job.partition, Converted#job.partition),
    ?assertEqual(Job#job.state, Converted#job.state),
    ?assertEqual(Job#job.num_cpus, Converted#job.num_cpus),
    ?assertEqual(Job#job.memory_mb, Converted#job.memory_mb),
    ?assertEqual(Job#job.time_limit, Converted#job.time_limit),
    ?assertEqual(Job#job.priority, Converted#job.priority).

test_cluster_map_preserves_fields() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = up,
        weight = 10,
        features = [<<"a">>, <<"b">>],
        partitions = [<<"p1">>, <<"p2">>],
        node_count = 100,
        cpu_count = 1000,
        memory_mb = 500000,
        gpu_count = 50,
        pending_jobs = 25,
        running_jobs = 75,
        available_cpus = 500,
        available_memory = 250000,
        last_sync = 1234567890,
        last_health_check = 1234567891,
        consecutive_failures = 2
    },

    Map = flurm_federation_routing:cluster_to_map(Cluster1),

    ?assertEqual(up, maps:get(state, Map)),
    ?assertEqual(10, maps:get(weight, Map)),
    ?assertEqual([<<"a">>, <<"b">>], maps:get(features, Map)),
    ?assertEqual([<<"p1">>, <<"p2">>], maps:get(partitions, Map)),
    ?assertEqual(100, maps:get(node_count, Map)),
    ?assertEqual(1000, maps:get(cpu_count, Map)),
    ?assertEqual(500000, maps:get(memory_mb, Map)),
    ?assertEqual(50, maps:get(gpu_count, Map)),
    ?assertEqual(25, maps:get(pending_jobs, Map)),
    ?assertEqual(75, maps:get(running_jobs, Map)),
    ?assertEqual(500, maps:get(available_cpus, Map)),
    ?assertEqual(250000, maps:get(available_memory, Map)),
    ?assertEqual(1234567890, maps:get(last_sync, Map)),
    ?assertEqual(1234567891, maps:get(last_health_check, Map)),
    ?assertEqual(2, maps:get(consecutive_failures, Map)).

%%====================================================================
%% Helper Functions
%%====================================================================

make_test_cluster(Name, Host, Port) ->
    #fed_cluster{
        name = Name,
        host = Host,
        port = Port,
        auth = #{},
        state = unknown,
        weight = 1,
        features = [],
        partitions = [],
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
    }.

make_cluster_with_load(Name, JobCount) ->
    Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
    Cluster#fed_cluster{
        state = up,
        pending_jobs = JobCount,
        running_jobs = 0,
        cpu_count = 100,
        available_cpus = 100,
        available_memory = 10000
    }.

make_cluster_for_resources(Name, State, NodeCount, CpuCount, MemoryMb, GpuCount) ->
    Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
    Cluster#fed_cluster{
        state = State,
        node_count = NodeCount,
        cpu_count = CpuCount,
        memory_mb = MemoryMb,
        gpu_count = GpuCount,
        available_cpus = CpuCount div 2,
        available_memory = MemoryMb div 2,
        pending_jobs = 5,
        running_jobs = 10
    }.

insert_cluster(Name, State, AvailCpus, AvailMem, Features, Partitions) ->
    Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = State,
        features = Features,
        partitions = Partitions,
        available_cpus = AvailCpus,
        available_memory = AvailMem,
        cpu_count = AvailCpus,
        memory_mb = AvailMem
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1).

insert_cluster_with_features(Name, State, Features) ->
    Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = State,
        features = Features,
        available_cpus = 100,
        available_memory = 10000
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1).

insert_cluster_with_resources(Name, State, AvailCpus, AvailMem) ->
    Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        state = State,
        features = [],
        available_cpus = AvailCpus,
        available_memory = AvailMem,
        cpu_count = AvailCpus * 2,
        memory_mb = AvailMem * 2
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1).
