%%%-------------------------------------------------------------------
%%% @doc FLURM Federation Coverage Tests
%%%
%%% Unit tests for the pure functions exported under -ifdef(TEST)
%%% in flurm_federation module. These tests cover URL building,
%%% header building, job accessors, job conversion, resource
%%% computation, cluster serialization, and ID generation.
%%%
%%% Target: 50+ tests covering all 14 TEST-exported pure functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Groups
%%====================================================================

%% URL & Header Building Tests
url_and_header_test_() ->
    {"URL and Header Building Functions",
     [
        {"build_url basic", fun test_build_url_basic/0},
        {"build_url with different ports", fun test_build_url_ports/0},
        {"build_url with different hosts", fun test_build_url_hosts/0},
        {"build_url with various paths", fun test_build_url_paths/0},
        {"build_url with empty path", fun test_build_url_empty_path/0},
        {"build_url with deep path", fun test_build_url_deep_path/0},
        {"build_auth_headers with token", fun test_build_auth_headers_token/0},
        {"build_auth_headers with api_key", fun test_build_auth_headers_api_key/0},
        {"build_auth_headers with both token and api_key", fun test_build_auth_headers_both/0},
        {"build_auth_headers with neither", fun test_build_auth_headers_empty/0},
        {"build_auth_headers with empty map", fun test_build_auth_headers_empty_map/0},
        {"headers_to_proplist basic", fun test_headers_to_proplist_basic/0},
        {"headers_to_proplist empty", fun test_headers_to_proplist_empty/0},
        {"headers_to_proplist multiple headers", fun test_headers_to_proplist_multiple/0}
     ]}.

%% Job Accessors Tests
job_accessors_test_() ->
    {"Job Accessor Functions",
     [
        {"get_job_partition with job record", fun test_get_job_partition_record/0},
        {"get_job_partition with map", fun test_get_job_partition_map/0},
        {"get_job_partition with missing partition", fun test_get_job_partition_missing/0},
        {"get_job_features with job record", fun test_get_job_features_record/0},
        {"get_job_features with map", fun test_get_job_features_map/0},
        {"get_job_features with missing features", fun test_get_job_features_missing/0},
        {"get_job_cpus with job record", fun test_get_job_cpus_record/0},
        {"get_job_cpus with map", fun test_get_job_cpus_map/0},
        {"get_job_cpus with missing cpus", fun test_get_job_cpus_missing/0},
        {"get_job_memory with job record", fun test_get_job_memory_record/0},
        {"get_job_memory with map", fun test_get_job_memory_map/0},
        {"get_job_memory with missing memory", fun test_get_job_memory_missing/0}
     ]}.

%% Job Conversion Tests
job_conversion_test_() ->
    {"Job Conversion Functions",
     [
        {"job_to_map with all fields", fun test_job_to_map_all_fields/0},
        {"job_to_map with defaults", fun test_job_to_map_defaults/0},
        {"job_to_map passthrough for maps", fun test_job_to_map_passthrough/0},
        {"map_to_job with all fields", fun test_map_to_job_all_fields/0},
        {"map_to_job with defaults", fun test_map_to_job_defaults/0},
        {"map_to_job with partial fields", fun test_map_to_job_partial/0},
        {"has_required_features subset", fun test_has_required_features_subset/0},
        {"has_required_features exact match", fun test_has_required_features_exact/0},
        {"has_required_features superset", fun test_has_required_features_superset/0},
        {"has_required_features empty required", fun test_has_required_features_empty_required/0},
        {"has_required_features empty cluster", fun test_has_required_features_empty_cluster/0},
        {"has_required_features no match", fun test_has_required_features_no_match/0}
     ]}.

%% Resource Computation Tests
resource_computation_test_() ->
    {"Resource Computation Functions",
     [
        {"aggregate_resources single cluster", fun test_aggregate_resources_single/0},
        {"aggregate_resources multiple clusters", fun test_aggregate_resources_multiple/0},
        {"aggregate_resources empty list", fun test_aggregate_resources_empty/0},
        {"aggregate_resources mixed states", fun test_aggregate_resources_mixed_states/0},
        {"aggregate_resources all down", fun test_aggregate_resources_all_down/0},
        {"calculate_load normal", fun test_calculate_load_normal/0},
        {"calculate_load zero cpus", fun test_calculate_load_zero_cpus/0},
        {"calculate_load high load", fun test_calculate_load_high/0},
        {"calculate_load low load", fun test_calculate_load_low/0},
        {"calculate_load no jobs", fun test_calculate_load_no_jobs/0}
     ]}.

%% Cluster Serialization Tests
cluster_serialization_test_() ->
    {"Cluster Serialization Functions",
     [
        {"cluster_to_map basic", fun test_cluster_to_map_basic/0},
        {"cluster_to_map with features", fun test_cluster_to_map_features/0},
        {"cluster_to_map with partitions", fun test_cluster_to_map_partitions/0},
        {"cluster_to_map all fields", fun test_cluster_to_map_all_fields/0}
     ]}.

%% ID Generation Tests
id_generation_test_() ->
    {"ID Generation Functions",
     [
        {"generate_local_ref returns binary", fun test_generate_local_ref_type/0},
        {"generate_local_ref unique values", fun test_generate_local_ref_unique/0},
        {"generate_local_ref format", fun test_generate_local_ref_format/0},
        {"generate_federation_id returns binary", fun test_generate_federation_id_type/0},
        {"generate_federation_id unique values", fun test_generate_federation_id_unique/0},
        {"generate_federation_id format", fun test_generate_federation_id_format/0}
     ]}.

%%====================================================================
%% URL Building Tests
%%====================================================================

test_build_url_basic() ->
    Url = flurm_federation:build_url(<<"localhost">>, 6817, <<"/api/v1/health">>),
    ?assertEqual(<<"http://localhost:6817/api/v1/health">>, Url).

test_build_url_ports() ->
    Url1 = flurm_federation:build_url(<<"host">>, 80, <<"/path">>),
    ?assertEqual(<<"http://host:80/path">>, Url1),
    Url2 = flurm_federation:build_url(<<"host">>, 443, <<"/path">>),
    ?assertEqual(<<"http://host:443/path">>, Url2),
    Url3 = flurm_federation:build_url(<<"host">>, 8080, <<"/path">>),
    ?assertEqual(<<"http://host:8080/path">>, Url3).

test_build_url_hosts() ->
    Url1 = flurm_federation:build_url(<<"192.168.1.100">>, 6817, <<"/api">>),
    ?assertEqual(<<"http://192.168.1.100:6817/api">>, Url1),
    Url2 = flurm_federation:build_url(<<"cluster.example.com">>, 6817, <<"/api">>),
    ?assertEqual(<<"http://cluster.example.com:6817/api">>, Url2),
    Url3 = flurm_federation:build_url(<<"node-001.hpc.local">>, 6817, <<"/api">>),
    ?assertEqual(<<"http://node-001.hpc.local:6817/api">>, Url3).

test_build_url_paths() ->
    Url1 = flurm_federation:build_url(<<"host">>, 6817, <<"/api/v1/jobs">>),
    ?assertEqual(<<"http://host:6817/api/v1/jobs">>, Url1),
    Url2 = flurm_federation:build_url(<<"host">>, 6817, <<"/api/v1/jobs/123">>),
    ?assertEqual(<<"http://host:6817/api/v1/jobs/123">>, Url2),
    Url3 = flurm_federation:build_url(<<"host">>, 6817, <<"/api/v1/cluster/stats">>),
    ?assertEqual(<<"http://host:6817/api/v1/cluster/stats">>, Url3).

test_build_url_empty_path() ->
    Url = flurm_federation:build_url(<<"host">>, 6817, <<>>),
    ?assertEqual(<<"http://host:6817">>, Url).

test_build_url_deep_path() ->
    Url = flurm_federation:build_url(<<"host">>, 6817, <<"/api/v1/federation/clusters/west/jobs">>),
    ?assertEqual(<<"http://host:6817/api/v1/federation/clusters/west/jobs">>, Url).

%%====================================================================
%% Auth Header Tests
%%====================================================================

test_build_auth_headers_token() ->
    Auth = #{token => <<"my-secret-token-123">>},
    Headers = flurm_federation:build_auth_headers(Auth),
    ?assertEqual([{<<"Authorization">>, <<"Bearer my-secret-token-123">>}], Headers).

test_build_auth_headers_api_key() ->
    Auth = #{api_key => <<"api-key-xyz">>},
    Headers = flurm_federation:build_auth_headers(Auth),
    ?assertEqual([{<<"X-API-Key">>, <<"api-key-xyz">>}], Headers).

test_build_auth_headers_both() ->
    %% When both are present, token takes precedence (first matching clause)
    Auth = #{token => <<"token123">>, api_key => <<"key456">>},
    Headers = flurm_federation:build_auth_headers(Auth),
    ?assertEqual([{<<"Authorization">>, <<"Bearer token123">>}], Headers).

test_build_auth_headers_empty() ->
    Headers = flurm_federation:build_auth_headers(#{}),
    ?assertEqual([], Headers).

test_build_auth_headers_empty_map() ->
    Headers = flurm_federation:build_auth_headers(#{other_key => <<"value">>}),
    ?assertEqual([], Headers).

%%====================================================================
%% Headers to Proplist Tests
%%====================================================================

test_headers_to_proplist_basic() ->
    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    Result = flurm_federation:headers_to_proplist(Headers),
    ?assertEqual([{"Content-Type", "application/json"}], Result).

test_headers_to_proplist_empty() ->
    Result = flurm_federation:headers_to_proplist([]),
    ?assertEqual([], Result).

test_headers_to_proplist_multiple() ->
    Headers = [
        {<<"Authorization">>, <<"Bearer token">>},
        {<<"Content-Type">>, <<"application/json">>},
        {<<"X-Request-ID">>, <<"req-123">>}
    ],
    Result = flurm_federation:headers_to_proplist(Headers),
    ?assertEqual([
        {"Authorization", "Bearer token"},
        {"Content-Type", "application/json"},
        {"X-Request-ID", "req-123"}
    ], Result).

%%====================================================================
%% Job Partition Accessor Tests
%%====================================================================

test_get_job_partition_record() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"compute">>,
        state = pending,
        script = <<"test">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },
    ?assertEqual(<<"compute">>, flurm_federation:get_job_partition(Job)).

test_get_job_partition_map() ->
    JobMap = #{partition => <<"gpu">>},
    ?assertEqual(<<"gpu">>, flurm_federation:get_job_partition(JobMap)).

test_get_job_partition_missing() ->
    ?assertEqual(undefined, flurm_federation:get_job_partition(#{})),
    ?assertEqual(undefined, flurm_federation:get_job_partition(not_a_job)).

%%====================================================================
%% Job Features Accessor Tests
%%====================================================================

test_get_job_features_record() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"compute">>,
        state = pending,
        script = <<"test">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },
    %% Job record doesn't have features field directly, returns []
    ?assertEqual([], flurm_federation:get_job_features(Job)).

test_get_job_features_map() ->
    JobMap = #{features => [<<"gpu">>, <<"highspeed">>]},
    ?assertEqual([<<"gpu">>, <<"highspeed">>], flurm_federation:get_job_features(JobMap)).

test_get_job_features_missing() ->
    ?assertEqual([], flurm_federation:get_job_features(#{})),
    ?assertEqual([], flurm_federation:get_job_features(some_atom)).

%%====================================================================
%% Job CPUs Accessor Tests
%%====================================================================

test_get_job_cpus_record() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"compute">>,
        state = pending,
        script = <<"test">>,
        num_nodes = 1,
        num_cpus = 8,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },
    ?assertEqual(8, flurm_federation:get_job_cpus(Job)).

test_get_job_cpus_map() ->
    JobMap = #{num_cpus => 16},
    ?assertEqual(16, flurm_federation:get_job_cpus(JobMap)).

test_get_job_cpus_missing() ->
    ?assertEqual(1, flurm_federation:get_job_cpus(#{})),
    ?assertEqual(1, flurm_federation:get_job_cpus(undefined)).

%%====================================================================
%% Job Memory Accessor Tests
%%====================================================================

test_get_job_memory_record() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"compute">>,
        state = pending,
        script = <<"test">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 8192,
        time_limit = 3600,
        priority = 100
    },
    ?assertEqual(8192, flurm_federation:get_job_memory(Job)).

test_get_job_memory_map() ->
    JobMap = #{memory_mb => 16384},
    ?assertEqual(16384, flurm_federation:get_job_memory(JobMap)).

test_get_job_memory_missing() ->
    ?assertEqual(1024, flurm_federation:get_job_memory(#{})),
    ?assertEqual(1024, flurm_federation:get_job_memory(<<"not a job">>)).

%%====================================================================
%% Job to Map Conversion Tests
%%====================================================================

test_job_to_map_all_fields() ->
    Job = #job{
        id = 42,
        name = <<"myjob">>,
        user = <<"alice">>,
        partition = <<"compute">>,
        state = running,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 7200,
        priority = 500,
        work_dir = <<"/home/alice/work">>,
        account = <<"research">>,
        qos = <<"high">>
    },
    Result = flurm_federation:job_to_map(Job),
    ?assertEqual(42, maps:get(id, Result)),
    ?assertEqual(<<"myjob">>, maps:get(name, Result)),
    ?assertEqual(<<"alice">>, maps:get(user, Result)),
    ?assertEqual(<<"compute">>, maps:get(partition, Result)),
    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, maps:get(script, Result)),
    ?assertEqual(2, maps:get(num_nodes, Result)),
    ?assertEqual(8, maps:get(num_cpus, Result)),
    ?assertEqual(4096, maps:get(memory_mb, Result)),
    ?assertEqual(7200, maps:get(time_limit, Result)),
    ?assertEqual(500, maps:get(priority, Result)),
    ?assertEqual(<<"/home/alice/work">>, maps:get(work_dir, Result)),
    ?assertEqual(<<"research">>, maps:get(account, Result)),
    ?assertEqual(<<"high">>, maps:get(qos, Result)).

test_job_to_map_defaults() ->
    Job = #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<>>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },
    Result = flurm_federation:job_to_map(Job),
    ?assert(is_map(Result)),
    ?assertEqual(1, maps:get(id, Result)).

test_job_to_map_passthrough() ->
    %% Maps are passed through unchanged
    JobMap = #{id => 123, name => <<"test">>},
    Result = flurm_federation:job_to_map(JobMap),
    ?assertEqual(JobMap, Result).

%%====================================================================
%% Map to Job Conversion Tests
%%====================================================================

test_map_to_job_all_fields() ->
    Map = #{
        id => 99,
        name => <<"fullspec">>,
        user => <<"bob">>,
        partition => <<"gpu">>,
        state => pending,
        script => <<"#!/bin/bash\necho full">>,
        num_nodes => 4,
        num_cpus => 32,
        memory_mb => 65536,
        time_limit => 86400,
        priority => 1000,
        work_dir => <<"/scratch/bob">>,
        account => <<"physics">>,
        qos => <<"premium">>
    },
    Job = flurm_federation:map_to_job(Map),
    ?assertEqual(99, Job#job.id),
    ?assertEqual(<<"fullspec">>, Job#job.name),
    ?assertEqual(<<"bob">>, Job#job.user),
    ?assertEqual(<<"gpu">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<"#!/bin/bash\necho full">>, Job#job.script),
    ?assertEqual(4, Job#job.num_nodes),
    ?assertEqual(32, Job#job.num_cpus),
    ?assertEqual(65536, Job#job.memory_mb),
    ?assertEqual(86400, Job#job.time_limit),
    ?assertEqual(1000, Job#job.priority),
    ?assertEqual(<<"/scratch/bob">>, Job#job.work_dir),
    ?assertEqual(<<"physics">>, Job#job.account),
    ?assertEqual(<<"premium">>, Job#job.qos).

test_map_to_job_defaults() ->
    %% Empty map should get all defaults
    Job = flurm_federation:map_to_job(#{}),
    ?assertEqual(0, Job#job.id),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<>>, Job#job.script),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(3600, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority),
    ?assertEqual(<<"/tmp">>, Job#job.work_dir),
    ?assertEqual(<<>>, Job#job.account),
    ?assertEqual(<<"normal">>, Job#job.qos).

test_map_to_job_partial() ->
    %% Only some fields provided
    Map = #{
        id => 50,
        name => <<"partial">>,
        num_cpus => 16
    },
    Job = flurm_federation:map_to_job(Map),
    ?assertEqual(50, Job#job.id),
    ?assertEqual(<<"partial">>, Job#job.name),
    ?assertEqual(16, Job#job.num_cpus),
    %% Rest should be defaults
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1024, Job#job.memory_mb).

%%====================================================================
%% Has Required Features Tests
%%====================================================================

test_has_required_features_subset() ->
    ClusterFeatures = [<<"gpu">>, <<"highspeed">>, <<"ssd">>],
    RequiredFeatures = [<<"gpu">>],
    ?assert(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

test_has_required_features_exact() ->
    ClusterFeatures = [<<"gpu">>, <<"highspeed">>],
    RequiredFeatures = [<<"gpu">>, <<"highspeed">>],
    ?assert(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

test_has_required_features_superset() ->
    ClusterFeatures = [<<"gpu">>, <<"highspeed">>, <<"ssd">>, <<"nvme">>],
    RequiredFeatures = [<<"gpu">>, <<"ssd">>],
    ?assert(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

test_has_required_features_empty_required() ->
    ClusterFeatures = [<<"gpu">>, <<"highspeed">>],
    RequiredFeatures = [],
    ?assert(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

test_has_required_features_empty_cluster() ->
    ClusterFeatures = [],
    RequiredFeatures = [<<"gpu">>],
    ?assertNot(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

test_has_required_features_no_match() ->
    ClusterFeatures = [<<"cpu-only">>, <<"standard">>],
    RequiredFeatures = [<<"gpu">>],
    ?assertNot(flurm_federation:has_required_features(ClusterFeatures, RequiredFeatures)).

%%====================================================================
%% Aggregate Resources Tests
%%====================================================================

%% Define fed_cluster record for testing (matches flurm_federation.erl internal record)
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

make_fed_cluster(Name, State, NodeCount, CpuCount, MemoryMb, GpuCount,
                 AvailCpus, AvailMem, Pending, Running) ->
    #fed_cluster{
        name = Name,
        host = <<"localhost">>,
        port = 6817,
        auth = #{},
        state = State,
        weight = 1,
        features = [],
        partitions = [],
        node_count = NodeCount,
        cpu_count = CpuCount,
        memory_mb = MemoryMb,
        gpu_count = GpuCount,
        pending_jobs = Pending,
        running_jobs = Running,
        available_cpus = AvailCpus,
        available_memory = AvailMem,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 0,
        properties = #{}
    }.

test_aggregate_resources_single() ->
    Cluster = make_fed_cluster(<<"cluster1">>, up, 10, 100, 512000, 8, 50, 256000, 5, 10),
    Result = flurm_federation:aggregate_resources([Cluster]),
    ?assertEqual(10, maps:get(total_nodes, Result)),
    ?assertEqual(100, maps:get(total_cpus, Result)),
    ?assertEqual(512000, maps:get(total_memory_mb, Result)),
    ?assertEqual(8, maps:get(total_gpus, Result)),
    ?assertEqual(50, maps:get(available_cpus, Result)),
    ?assertEqual(256000, maps:get(available_memory_mb, Result)),
    ?assertEqual(5, maps:get(pending_jobs, Result)),
    ?assertEqual(10, maps:get(running_jobs, Result)),
    ?assertEqual(1, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

test_aggregate_resources_multiple() ->
    Cluster1 = make_fed_cluster(<<"cluster1">>, up, 10, 100, 512000, 4, 50, 256000, 5, 10),
    Cluster2 = make_fed_cluster(<<"cluster2">>, up, 20, 200, 1024000, 8, 100, 512000, 10, 20),
    Cluster3 = make_fed_cluster(<<"cluster3">>, up, 5, 50, 256000, 2, 25, 128000, 2, 5),
    Result = flurm_federation:aggregate_resources([Cluster1, Cluster2, Cluster3]),
    ?assertEqual(35, maps:get(total_nodes, Result)),
    ?assertEqual(350, maps:get(total_cpus, Result)),
    ?assertEqual(1792000, maps:get(total_memory_mb, Result)),
    ?assertEqual(14, maps:get(total_gpus, Result)),
    ?assertEqual(175, maps:get(available_cpus, Result)),
    ?assertEqual(896000, maps:get(available_memory_mb, Result)),
    ?assertEqual(17, maps:get(pending_jobs, Result)),
    ?assertEqual(35, maps:get(running_jobs, Result)),
    ?assertEqual(3, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

test_aggregate_resources_empty() ->
    Result = flurm_federation:aggregate_resources([]),
    ?assertEqual(0, maps:get(total_nodes, Result)),
    ?assertEqual(0, maps:get(total_cpus, Result)),
    ?assertEqual(0, maps:get(total_memory_mb, Result)),
    ?assertEqual(0, maps:get(total_gpus, Result)),
    ?assertEqual(0, maps:get(available_cpus, Result)),
    ?assertEqual(0, maps:get(available_memory_mb, Result)),
    ?assertEqual(0, maps:get(pending_jobs, Result)),
    ?assertEqual(0, maps:get(running_jobs, Result)),
    ?assertEqual(0, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

test_aggregate_resources_mixed_states() ->
    ClusterUp = make_fed_cluster(<<"up">>, up, 10, 100, 512000, 4, 50, 256000, 5, 10),
    ClusterDown = make_fed_cluster(<<"down">>, down, 10, 100, 512000, 4, 50, 256000, 5, 10),
    ClusterDrain = make_fed_cluster(<<"drain">>, drain, 5, 50, 256000, 2, 25, 128000, 2, 5),
    Result = flurm_federation:aggregate_resources([ClusterUp, ClusterDown, ClusterDrain]),
    %% Only "up" clusters contribute to resource totals
    ?assertEqual(10, maps:get(total_nodes, Result)),
    ?assertEqual(100, maps:get(total_cpus, Result)),
    ?assertEqual(1, maps:get(clusters_up, Result)),
    ?assertEqual(2, maps:get(clusters_down, Result)).

test_aggregate_resources_all_down() ->
    Cluster1 = make_fed_cluster(<<"c1">>, down, 10, 100, 512000, 4, 50, 256000, 5, 10),
    Cluster2 = make_fed_cluster(<<"c2">>, drain, 10, 100, 512000, 4, 50, 256000, 5, 10),
    Result = flurm_federation:aggregate_resources([Cluster1, Cluster2]),
    ?assertEqual(0, maps:get(total_nodes, Result)),
    ?assertEqual(0, maps:get(total_cpus, Result)),
    ?assertEqual(0, maps:get(clusters_up, Result)),
    ?assertEqual(2, maps:get(clusters_down, Result)).

%%====================================================================
%% Calculate Load Tests
%%====================================================================

test_calculate_load_normal() ->
    Cluster = make_fed_cluster(<<"test">>, up, 10, 100, 512000, 4, 50, 256000, 10, 20),
    Load = flurm_federation:calculate_load(Cluster),
    %% (10 pending + 20 running) / 100 cpus = 0.3
    ?assertEqual(0.3, Load).

test_calculate_load_zero_cpus() ->
    Cluster = make_fed_cluster(<<"test">>, up, 0, 0, 0, 0, 0, 0, 10, 20),
    Load = flurm_federation:calculate_load(Cluster),
    ?assertEqual(infinity, Load).

test_calculate_load_high() ->
    Cluster = make_fed_cluster(<<"test">>, up, 10, 10, 512000, 4, 5, 256000, 50, 100),
    Load = flurm_federation:calculate_load(Cluster),
    %% (50 + 100) / 10 = 15.0
    ?assertEqual(15.0, Load).

test_calculate_load_low() ->
    Cluster = make_fed_cluster(<<"test">>, up, 100, 1000, 512000, 100, 800, 400000, 1, 2),
    Load = flurm_federation:calculate_load(Cluster),
    %% (1 + 2) / 1000 = 0.003
    ?assertEqual(0.003, Load).

test_calculate_load_no_jobs() ->
    Cluster = make_fed_cluster(<<"test">>, up, 10, 100, 512000, 4, 100, 512000, 0, 0),
    Load = flurm_federation:calculate_load(Cluster),
    %% (0 + 0) / 100 = 0.0
    ?assertEqual(0.0, Load).

%%====================================================================
%% Cluster to Map Tests
%%====================================================================

test_cluster_to_map_basic() ->
    Cluster = #fed_cluster{
        name = <<"basic-cluster">>,
        host = <<"192.168.1.1">>,
        port = 6817,
        state = up,
        weight = 1,
        features = [],
        partitions = [],
        node_count = 10,
        cpu_count = 100,
        memory_mb = 512000,
        gpu_count = 0,
        pending_jobs = 5,
        running_jobs = 10,
        available_cpus = 50,
        available_memory = 256000,
        last_sync = 1234567890,
        last_health_check = 1234567891,
        consecutive_failures = 0
    },
    Result = flurm_federation:cluster_to_map(Cluster),
    ?assertEqual(<<"basic-cluster">>, maps:get(name, Result)),
    ?assertEqual(<<"192.168.1.1">>, maps:get(host, Result)),
    ?assertEqual(6817, maps:get(port, Result)),
    ?assertEqual(up, maps:get(state, Result)),
    ?assertEqual(1, maps:get(weight, Result)),
    ?assertEqual([], maps:get(features, Result)),
    ?assertEqual([], maps:get(partitions, Result)),
    ?assertEqual(10, maps:get(node_count, Result)),
    ?assertEqual(100, maps:get(cpu_count, Result)),
    ?assertEqual(512000, maps:get(memory_mb, Result)),
    ?assertEqual(0, maps:get(gpu_count, Result)),
    ?assertEqual(5, maps:get(pending_jobs, Result)),
    ?assertEqual(10, maps:get(running_jobs, Result)),
    ?assertEqual(50, maps:get(available_cpus, Result)),
    ?assertEqual(256000, maps:get(available_memory, Result)),
    ?assertEqual(1234567890, maps:get(last_sync, Result)),
    ?assertEqual(1234567891, maps:get(last_health_check, Result)),
    ?assertEqual(0, maps:get(consecutive_failures, Result)).

test_cluster_to_map_features() ->
    Cluster = #fed_cluster{
        name = <<"gpu-cluster">>,
        host = <<"gpu.example.com">>,
        port = 6817,
        state = up,
        weight = 2,
        features = [<<"gpu">>, <<"nvlink">>, <<"a100">>],
        partitions = [],
        node_count = 8,
        cpu_count = 256,
        memory_mb = 2048000,
        gpu_count = 32,
        pending_jobs = 20,
        running_jobs = 30,
        available_cpus = 128,
        available_memory = 1024000,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 0
    },
    Result = flurm_federation:cluster_to_map(Cluster),
    ?assertEqual([<<"gpu">>, <<"nvlink">>, <<"a100">>], maps:get(features, Result)),
    ?assertEqual(32, maps:get(gpu_count, Result)).

test_cluster_to_map_partitions() ->
    Cluster = #fed_cluster{
        name = <<"multi-partition">>,
        host = <<"localhost">>,
        port = 6817,
        state = up,
        weight = 1,
        features = [],
        partitions = [<<"compute">>, <<"gpu">>, <<"debug">>, <<"preempt">>],
        node_count = 50,
        cpu_count = 1000,
        memory_mb = 5120000,
        gpu_count = 16,
        pending_jobs = 100,
        running_jobs = 200,
        available_cpus = 400,
        available_memory = 2048000,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 0
    },
    Result = flurm_federation:cluster_to_map(Cluster),
    ?assertEqual([<<"compute">>, <<"gpu">>, <<"debug">>, <<"preempt">>],
                 maps:get(partitions, Result)).

test_cluster_to_map_all_fields() ->
    Cluster = #fed_cluster{
        name = <<"full-cluster">>,
        host = <<"10.0.0.1">>,
        port = 8080,
        state = drain,
        weight = 5,
        features = [<<"feature1">>, <<"feature2">>],
        partitions = [<<"part1">>, <<"part2">>],
        node_count = 100,
        cpu_count = 4000,
        memory_mb = 20480000,
        gpu_count = 64,
        pending_jobs = 500,
        running_jobs = 1000,
        available_cpus = 2000,
        available_memory = 10240000,
        last_sync = 9999999999,
        last_health_check = 9999999998,
        consecutive_failures = 3
    },
    Result = flurm_federation:cluster_to_map(Cluster),
    ?assertEqual(18, maps:size(Result)),
    ?assertEqual(drain, maps:get(state, Result)),
    ?assertEqual(5, maps:get(weight, Result)),
    ?assertEqual(3, maps:get(consecutive_failures, Result)).

%%====================================================================
%% Generate Local Ref Tests
%%====================================================================

test_generate_local_ref_type() ->
    Ref = flurm_federation:generate_local_ref(),
    ?assert(is_binary(Ref)).

test_generate_local_ref_unique() ->
    Refs = [flurm_federation:generate_local_ref() || _ <- lists:seq(1, 100)],
    UniqueRefs = lists:usort(Refs),
    ?assertEqual(100, length(UniqueRefs)).

test_generate_local_ref_format() ->
    Ref = flurm_federation:generate_local_ref(),
    ?assertMatch(<<"ref-", _/binary>>, Ref),
    %% Should contain timestamp and random number separated by dash
    Parts = binary:split(Ref, <<"-">>, [global]),
    ?assert(length(Parts) >= 3).

%%====================================================================
%% Generate Federation ID Tests
%%====================================================================

test_generate_federation_id_type() ->
    Id = flurm_federation:generate_federation_id(),
    ?assert(is_binary(Id)).

test_generate_federation_id_unique() ->
    Ids = [flurm_federation:generate_federation_id() || _ <- lists:seq(1, 100)],
    UniqueIds = lists:usort(Ids),
    ?assertEqual(100, length(UniqueIds)).

test_generate_federation_id_format() ->
    Id = flurm_federation:generate_federation_id(),
    ?assertMatch(<<"fed-", _/binary>>, Id),
    %% Should contain timestamp and random number separated by dash
    Parts = binary:split(Id, <<"-">>, [global]),
    ?assert(length(Parts) >= 3).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {"Edge Cases",
     [
        {"job_to_map and map_to_job roundtrip", fun test_job_roundtrip/0},
        {"multiple auth header precedence", fun test_auth_header_precedence/0},
        {"large resource aggregation", fun test_large_aggregation/0}
     ]}.

test_job_roundtrip() ->
    %% Create a job, convert to map, convert back, verify fields match
    OriginalJob = #job{
        id = 12345,
        name = <<"roundtrip-test">>,
        user = <<"testuser">>,
        partition = <<"compute">>,
        state = pending,
        script = <<"#!/bin/bash\necho roundtrip">>,
        num_nodes = 3,
        num_cpus = 12,
        memory_mb = 32768,
        time_limit = 14400,
        priority = 750,
        work_dir = <<"/home/testuser/project">>,
        account = <<"department">>,
        qos = <<"standard">>
    },
    Map = flurm_federation:job_to_map(OriginalJob),
    RestoredJob = flurm_federation:map_to_job(Map),
    ?assertEqual(OriginalJob#job.id, RestoredJob#job.id),
    ?assertEqual(OriginalJob#job.name, RestoredJob#job.name),
    ?assertEqual(OriginalJob#job.user, RestoredJob#job.user),
    ?assertEqual(OriginalJob#job.partition, RestoredJob#job.partition),
    ?assertEqual(OriginalJob#job.state, RestoredJob#job.state),
    ?assertEqual(OriginalJob#job.script, RestoredJob#job.script),
    ?assertEqual(OriginalJob#job.num_nodes, RestoredJob#job.num_nodes),
    ?assertEqual(OriginalJob#job.num_cpus, RestoredJob#job.num_cpus),
    ?assertEqual(OriginalJob#job.memory_mb, RestoredJob#job.memory_mb),
    ?assertEqual(OriginalJob#job.time_limit, RestoredJob#job.time_limit),
    ?assertEqual(OriginalJob#job.priority, RestoredJob#job.priority),
    ?assertEqual(OriginalJob#job.work_dir, RestoredJob#job.work_dir),
    ?assertEqual(OriginalJob#job.account, RestoredJob#job.account),
    ?assertEqual(OriginalJob#job.qos, RestoredJob#job.qos).

test_auth_header_precedence() ->
    %% Token should take precedence over api_key
    Auth = #{token => <<"tok">>, api_key => <<"key">>},
    Headers = flurm_federation:build_auth_headers(Auth),
    ?assertEqual(1, length(Headers)),
    {HeaderName, _} = hd(Headers),
    ?assertEqual(<<"Authorization">>, HeaderName).

test_large_aggregation() ->
    %% Test with many clusters
    Clusters = [make_fed_cluster(
        list_to_binary("cluster" ++ integer_to_list(N)),
        up, 10, 100, 512000, 4, 50, 256000, 5, 10
    ) || N <- lists:seq(1, 50)],
    Result = flurm_federation:aggregate_resources(Clusters),
    ?assertEqual(500, maps:get(total_nodes, Result)),
    ?assertEqual(5000, maps:get(total_cpus, Result)),
    ?assertEqual(50, maps:get(clusters_up, Result)).
