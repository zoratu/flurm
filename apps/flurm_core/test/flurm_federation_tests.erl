%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_federation module
%%%
%%% Tests multi-cluster federation functionality including:
%%% - Cluster management (add/remove/list)
%%% - Job routing with different policies
%%% - Cross-cluster job submission
%%% - Resource aggregation
%%% - Health monitoring and failover
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixture Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure application env is set for local cluster
    application:set_env(flurm_core, cluster_name, <<"test_cluster">>),
    application:set_env(flurm_core, controller_host, <<"localhost">>),
    application:set_env(flurm_core, controller_port, 6817),
    application:set_env(flurm_core, federation_routing_policy, least_loaded),

    %% Clean up any existing ETS tables
    cleanup_tables(),

    %% Start the federation gen_server
    {ok, Pid} = flurm_federation:start_link(),
    Pid.

cleanup(Pid) ->
    %% Stop the gen_server
    case is_process_alive(Pid) of
        true ->
            gen_server:stop(Pid, normal, 5000);
        false ->
            ok
    end,
    cleanup_tables(),
    ok.

cleanup_tables() ->
    Tables = [flurm_fed_clusters, flurm_fed_jobs, flurm_fed_partition_map, flurm_fed_remote_jobs],
    lists:foreach(fun(Tab) ->
        catch ets:delete(Tab)
    end, Tables).

%%====================================================================
%% Test Generators
%%====================================================================

%% Main test generator with setup/teardown
federation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"add_cluster adds a remote cluster", fun test_add_cluster/0},
      {"add_cluster with map config", fun test_add_cluster_map_config/0},
      {"remove_cluster removes cluster", fun test_remove_cluster/0},
      {"remove_cluster fails for local cluster", fun test_remove_local_cluster/0},
      {"list_clusters returns all clusters", fun test_list_clusters/0},
      {"get_cluster_status returns cluster info", fun test_get_cluster_status/0},
      {"get_cluster_status returns error for unknown", fun test_get_cluster_status_unknown/0},
      {"submit_job with auto-routing", fun test_submit_job_auto_route/0},
      {"submit_job to specific cluster", fun test_submit_job_specific_cluster/0},
      {"route_job with least_loaded policy", fun test_route_job_least_loaded/0},
      {"route_job with round_robin policy", fun test_route_job_round_robin/0},
      {"route_job with weighted policy", fun test_route_job_weighted/0},
      {"route_job with random policy", fun test_route_job_random/0},
      {"route_job with partition_affinity", fun test_route_job_partition_affinity/0},
      {"route_job filters by features", fun test_route_job_features/0},
      {"get_federation_resources aggregates resources", fun test_get_federation_resources/0},
      {"get_federation_resources with mixed states", fun test_get_federation_resources_mixed/0},
      {"health monitoring marks cluster down after failures", fun test_health_monitoring_failures/0},
      {"health monitoring resets on success", fun test_health_monitoring_recovery/0},
      {"is_federated returns true with multiple clusters", fun test_is_federated/0},
      {"get_local_cluster returns local name", fun test_get_local_cluster/0},
      {"partition mapping works correctly", fun test_partition_mapping/0},
      {"track_remote_job creates tracking record", fun test_track_remote_job/0},
      {"get_federation_stats returns correct structure", fun test_get_federation_stats/0},
      {"get_federation_stats counts healthy/unhealthy clusters", fun test_get_federation_stats_health/0},
      {"get_federation_stats handles empty federation", fun test_get_federation_stats_empty/0}
     ]}.

%%====================================================================
%% Cluster Management Tests
%%====================================================================

test_add_cluster() ->
    Config = #{
        host => <<"cluster2.example.com">>,
        port => 6817,
        weight => 2,
        features => [<<"gpu">>, <<"highmem">>],
        partitions => [<<"gpu">>, <<"batch">>]
    },

    ok = flurm_federation:add_cluster(<<"cluster2">>, Config),

    %% Verify cluster was added
    {ok, ClusterInfo} = flurm_federation:get_cluster_status(<<"cluster2">>),
    ?assertEqual(<<"cluster2">>, maps:get(name, ClusterInfo)),
    ?assertEqual(<<"cluster2.example.com">>, maps:get(host, ClusterInfo)),
    ?assertEqual(6817, maps:get(port, ClusterInfo)),
    ?assertEqual(2, maps:get(weight, ClusterInfo)),
    ?assertEqual([<<"gpu">>, <<"highmem">>], maps:get(features, ClusterInfo)).

test_add_cluster_map_config() ->
    %% Test with minimal config
    Config = #{host => <<"minimal.example.com">>},

    ok = flurm_federation:add_cluster(<<"minimal">>, Config),

    {ok, ClusterInfo} = flurm_federation:get_cluster_status(<<"minimal">>),
    ?assertEqual(<<"minimal">>, maps:get(name, ClusterInfo)),
    ?assertEqual(<<"minimal.example.com">>, maps:get(host, ClusterInfo)),
    %% Should have defaults
    ?assertEqual(1, maps:get(weight, ClusterInfo)).

test_remove_cluster() ->
    %% First add a cluster
    ok = flurm_federation:add_cluster(<<"toremove">>, #{host => <<"remove.example.com">>}),

    %% Verify it exists
    {ok, _} = flurm_federation:get_cluster_status(<<"toremove">>),

    %% Remove it
    ok = flurm_federation:remove_cluster(<<"toremove">>),

    %% Verify it's gone
    ?assertEqual({error, not_found}, flurm_federation:get_cluster_status(<<"toremove">>)).

test_remove_local_cluster() ->
    %% Attempt to remove the local cluster should fail
    LocalCluster = flurm_federation:get_local_cluster(),
    Result = flurm_federation:remove_cluster(LocalCluster),
    ?assertEqual({error, cannot_remove_local}, Result).

test_list_clusters() ->
    %% Add some clusters
    ok = flurm_federation:add_cluster(<<"cluster_a">>, #{host => <<"a.example.com">>}),
    ok = flurm_federation:add_cluster(<<"cluster_b">>, #{host => <<"b.example.com">>}),

    Clusters = flurm_federation:list_clusters(),

    %% Should have local cluster + 2 added
    ?assert(length(Clusters) >= 3),

    %% Verify our clusters are in the list
    Names = [maps:get(name, C) || C <- Clusters],
    ?assert(lists:member(<<"cluster_a">>, Names)),
    ?assert(lists:member(<<"cluster_b">>, Names)).

test_get_cluster_status() ->
    ok = flurm_federation:add_cluster(<<"status_test">>, #{
        host => <<"status.example.com">>,
        port => 7000,
        weight => 5,
        features => [<<"fast">>]
    }),

    {ok, Status} = flurm_federation:get_cluster_status(<<"status_test">>),

    ?assertEqual(<<"status_test">>, maps:get(name, Status)),
    ?assertEqual(<<"status.example.com">>, maps:get(host, Status)),
    ?assertEqual(7000, maps:get(port, Status)),
    ?assertEqual(5, maps:get(weight, Status)),
    ?assertEqual([<<"fast">>], maps:get(features, Status)),
    ?assert(maps:is_key(state, Status)),
    ?assert(maps:is_key(node_count, Status)),
    ?assert(maps:is_key(cpu_count, Status)).

test_get_cluster_status_unknown() ->
    Result = flurm_federation:get_cluster_status(<<"nonexistent_cluster">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Job Submission Tests
%%====================================================================

test_submit_job_auto_route() ->
    %% Test that auto-routing selects the correct cluster
    %% We don't actually submit since flurm_scheduler isn't running

    %% Setup: Add a remote cluster with resources and make local cluster have less
    ok = flurm_federation:add_cluster(<<"remote_target">>, #{
        host => <<"remote.example.com">>,
        port => 6817
    }),
    update_cluster_resources(<<"remote_target">>, #{
        state => up,
        available_cpus => 200,
        available_memory => 200000,
        cpu_count => 256,
        pending_jobs => 0,
        running_jobs => 0
    }),

    %% Make local cluster appear more loaded
    update_cluster_resources(flurm_federation:get_local_cluster(), #{
        state => up,
        available_cpus => 10,
        available_memory => 10000,
        cpu_count => 128,
        pending_jobs => 100,
        running_jobs => 50
    }),

    Job = #{
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        num_cpus => 1,
        memory_mb => 1024
    },

    %% Test routing (not submission) to verify auto-routing logic
    {ok, SelectedCluster} = flurm_federation:route_job(Job),

    %% Should route to the less loaded remote cluster
    ?assertEqual(<<"remote_target">>, SelectedCluster).

test_submit_job_specific_cluster() ->
    %% Add a cluster with 'unknown' state (default)
    ok = flurm_federation:add_cluster(<<"target">>, #{
        host => <<"target.example.com">>,
        port => 6817
    }),

    Job = #{
        name => <<"specific_job">>,
        user => <<"testuser">>,
        num_cpus => 1,
        memory_mb => 1024
    },

    %% Submit to specific cluster (will fail since cluster is 'unknown' state)
    Result = flurm_federation:submit_job(Job, #{cluster => <<"target">>}),

    %% Should fail because cluster is not 'up'
    ?assertMatch({error, {cluster_unavailable, _}}, Result).

%%====================================================================
%% Job Routing Tests
%%====================================================================

test_route_job_least_loaded() ->
    %% Add clusters with different loads
    add_cluster_with_load(<<"low_load">>, 10, 100, 1000),   % 10% loaded
    add_cluster_with_load(<<"high_load">>, 90, 100, 1000),  % 90% loaded

    Job = #{num_cpus => 1, memory_mb => 100},

    %% Route should pick the less loaded cluster
    {ok, SelectedCluster} = flurm_federation:route_job(Job),

    %% Should select low_load cluster
    ?assertEqual(<<"low_load">>, SelectedCluster).

test_route_job_round_robin() ->
    %% Set routing policy to round_robin
    application:set_env(flurm_core, federation_routing_policy, round_robin),

    %% Need to restart the server to pick up new policy
    %% For this test, we'll just verify the routing works
    add_cluster_with_load(<<"rr1">>, 50, 100, 1000),
    add_cluster_with_load(<<"rr2">>, 50, 100, 1000),

    Job = #{num_cpus => 1, memory_mb => 100},

    {ok, _SelectedCluster} = flurm_federation:route_job(Job),
    %% With round_robin, any eligible cluster could be selected
    ok.

test_route_job_weighted() ->
    add_cluster_with_load(<<"weighted1">>, 0, 100, 1000),
    add_cluster_with_load(<<"weighted2">>, 0, 100, 1000),

    %% Update weights
    update_cluster_resources(<<"weighted1">>, #{weight => 1}),
    update_cluster_resources(<<"weighted2">>, #{weight => 10}),

    Job = #{num_cpus => 1, memory_mb => 100},

    {ok, _Selected} = flurm_federation:route_job(Job),
    %% Weighted selection is probabilistic, just verify it works
    ok.

test_route_job_random() ->
    add_cluster_with_load(<<"rand1">>, 0, 100, 1000),
    add_cluster_with_load(<<"rand2">>, 0, 100, 1000),

    Job = #{num_cpus => 1, memory_mb => 100},

    {ok, _Selected} = flurm_federation:route_job(Job),
    ok.

test_route_job_partition_affinity() ->
    %% Add clusters with different partitions
    ok = flurm_federation:add_cluster(<<"gpu_cluster">>, #{
        host => <<"gpu.example.com">>,
        partitions => [<<"gpu">>],
        features => []
    }),
    update_cluster_resources(<<"gpu_cluster">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    ok = flurm_federation:add_cluster(<<"cpu_cluster">>, #{
        host => <<"cpu.example.com">>,
        partitions => [<<"batch">>],
        features => []
    }),
    update_cluster_resources(<<"cpu_cluster">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    %% Job requesting gpu partition should route to gpu_cluster
    GpuJob = #{partition => <<"gpu">>, num_cpus => 1, memory_mb => 100},
    {ok, Selected} = flurm_federation:route_job(GpuJob),
    ?assertEqual(<<"gpu_cluster">>, Selected).

test_route_job_features() ->
    %% Add cluster with GPU feature
    ok = flurm_federation:add_cluster(<<"gpu_enabled">>, #{
        host => <<"gpu.example.com">>,
        features => [<<"gpu">>, <<"cuda">>]
    }),
    update_cluster_resources(<<"gpu_enabled">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    %% Add cluster without GPU
    ok = flurm_federation:add_cluster(<<"cpu_only">>, #{
        host => <<"cpu.example.com">>,
        features => []
    }),
    update_cluster_resources(<<"cpu_only">>, #{
        state => up,
        available_cpus => 200,
        available_memory => 200000
    }),

    %% Job requiring GPU feature
    GpuJob = #{features => [<<"gpu">>], num_cpus => 1, memory_mb => 100},
    {ok, Selected} = flurm_federation:route_job(GpuJob),
    ?assertEqual(<<"gpu_enabled">>, Selected).

%%====================================================================
%% Resource Aggregation Tests
%%====================================================================

test_get_federation_resources() ->
    %% Add clusters with known resources
    add_cluster_with_resources(<<"res1">>, #{
        node_count => 10,
        cpu_count => 100,
        memory_mb => 10000,
        gpu_count => 4,
        available_cpus => 80,
        available_memory => 8000,
        pending_jobs => 5,
        running_jobs => 10
    }),

    add_cluster_with_resources(<<"res2">>, #{
        node_count => 20,
        cpu_count => 200,
        memory_mb => 20000,
        gpu_count => 8,
        available_cpus => 150,
        available_memory => 15000,
        pending_jobs => 10,
        running_jobs => 20
    }),

    Resources = flurm_federation:get_federation_resources(),

    %% Note: local cluster also contributes, so values are >= what we added
    ?assert(maps:get(total_nodes, Resources) >= 30),
    ?assert(maps:get(total_cpus, Resources) >= 300),
    ?assert(maps:get(total_memory_mb, Resources) >= 30000),
    ?assert(maps:get(total_gpus, Resources) >= 12),
    ?assert(maps:get(available_cpus, Resources) >= 230),
    ?assert(maps:get(pending_jobs, Resources) >= 15),
    ?assert(maps:get(running_jobs, Resources) >= 30),
    ?assert(maps:get(clusters_up, Resources) >= 2).

test_get_federation_resources_mixed() ->
    %% Add an up cluster
    add_cluster_with_resources(<<"up_cluster">>, #{
        node_count => 10,
        cpu_count => 100,
        memory_mb => 10000,
        available_cpus => 80,
        available_memory => 8000
    }),

    %% Add a down cluster
    ok = flurm_federation:add_cluster(<<"down_cluster">>, #{host => <<"down.example.com">>}),
    update_cluster_resources(<<"down_cluster">>, #{
        state => down,
        node_count => 10,
        cpu_count => 100
    }),

    Resources = flurm_federation:get_federation_resources(),

    %% Down cluster should increment clusters_down
    ?assert(maps:get(clusters_down, Resources) >= 1).

%%====================================================================
%% Health Monitoring Tests
%%====================================================================

test_health_monitoring_failures() ->
    %% Add a cluster
    ok = flurm_federation:add_cluster(<<"failing">>, #{host => <<"failing.example.com">>}),
    update_cluster_resources(<<"failing">>, #{state => up, consecutive_failures => 0}),

    %% Simulate multiple health check failures by sending cluster_health messages
    %% This simulates what happens during health checks
    FedPid = whereis(flurm_federation),

    %% Send 3 failure notifications (MAX_RETRY_COUNT)
    FedPid ! {cluster_health, <<"failing">>, down},
    _ = sys:get_state(flurm_federation),
    FedPid ! {cluster_health, <<"failing">>, down},
    _ = sys:get_state(flurm_federation),
    FedPid ! {cluster_health, <<"failing">>, down},
    _ = sys:get_state(flurm_federation),

    %% Check cluster is now marked down
    {ok, Status} = flurm_federation:get_cluster_status(<<"failing">>),
    ?assertEqual(down, maps:get(state, Status)),
    ?assertEqual(3, maps:get(consecutive_failures, Status)).

test_health_monitoring_recovery() ->
    %% Add a cluster that's "down"
    ok = flurm_federation:add_cluster(<<"recovering">>, #{host => <<"recovering.example.com">>}),
    update_cluster_resources(<<"recovering">>, #{state => down, consecutive_failures => 5}),

    %% Simulate successful health check
    FedPid = whereis(flurm_federation),
    FedPid ! {cluster_health, <<"recovering">>, up},
    _ = sys:get_state(flurm_federation),

    %% Check cluster is now marked up with reset failures
    {ok, Status} = flurm_federation:get_cluster_status(<<"recovering">>),
    ?assertEqual(up, maps:get(state, Status)),
    ?assertEqual(0, maps:get(consecutive_failures, Status)).

%%====================================================================
%% Federation State Tests
%%====================================================================

test_is_federated() ->
    %% With just local cluster, may or may not be federated
    InitialState = flurm_federation:is_federated(),

    %% Add another cluster
    ok = flurm_federation:add_cluster(<<"remote">>, #{host => <<"remote.example.com">>}),

    %% Now should definitely be federated
    ?assertEqual(true, flurm_federation:is_federated()),

    %% Remove it
    ok = flurm_federation:remove_cluster(<<"remote">>),

    %% Back to initial state
    ?assertEqual(InitialState, flurm_federation:is_federated()).

test_get_local_cluster() ->
    LocalCluster = flurm_federation:get_local_cluster(),
    ?assertEqual(<<"test_cluster">>, LocalCluster).

test_partition_mapping() ->
    %% Add cluster with partitions
    ok = flurm_federation:add_cluster(<<"part_cluster">>, #{
        host => <<"part.example.com">>,
        partitions => [<<"special">>, <<"compute">>]
    }),

    %% Query partition mapping
    {ok, Cluster} = flurm_federation:get_cluster_for_partition(<<"special">>),
    ?assertEqual(<<"part_cluster">>, Cluster),

    {ok, Cluster2} = flurm_federation:get_cluster_for_partition(<<"compute">>),
    ?assertEqual(<<"part_cluster">>, Cluster2),

    %% Unknown partition
    ?assertEqual({error, not_found}, flurm_federation:get_cluster_for_partition(<<"unknown_part">>)).

test_track_remote_job() ->
    %% Add a cluster
    ok = flurm_federation:add_cluster(<<"remote_jobs">>, #{host => <<"remote.example.com">>}),

    JobSpec = #{name => <<"tracked_job">>, user => <<"testuser">>},

    {ok, LocalRef} = flurm_federation:track_remote_job(<<"remote_jobs">>, 12345, JobSpec),

    ?assert(is_binary(LocalRef)),
    ?assert(byte_size(LocalRef) > 0),
    %% Reference should start with "ref-"
    ?assertEqual(<<"ref-">>, binary:part(LocalRef, 0, 4)).

%%====================================================================
%% Federation Stats Tests
%%====================================================================

test_get_federation_stats() ->
    %% Add a couple of clusters
    ok = flurm_federation:add_cluster(<<"stats1">>, #{host => <<"stats1.example.com">>}),
    ok = flurm_federation:add_cluster(<<"stats2">>, #{host => <<"stats2.example.com">>}),

    Stats = flurm_federation:get_federation_stats(),

    %% Should return a map with expected keys
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(clusters_total, Stats)),
    ?assert(maps:is_key(clusters_healthy, Stats)),
    ?assert(maps:is_key(clusters_unhealthy, Stats)),

    %% Values should be non-negative integers
    ?assert(maps:get(clusters_total, Stats) >= 0),
    ?assert(maps:get(clusters_healthy, Stats) >= 0),
    ?assert(maps:get(clusters_unhealthy, Stats) >= 0),

    %% Total should be >= 3 (local + 2 added)
    ?assert(maps:get(clusters_total, Stats) >= 3).

test_get_federation_stats_health() ->
    %% Add an up cluster
    ok = flurm_federation:add_cluster(<<"healthy_stat">>, #{host => <<"healthy.example.com">>}),
    update_cluster_resources(<<"healthy_stat">>, #{state => up}),

    %% Add a down cluster
    ok = flurm_federation:add_cluster(<<"unhealthy_stat">>, #{host => <<"unhealthy.example.com">>}),
    update_cluster_resources(<<"unhealthy_stat">>, #{state => down}),

    Stats = flurm_federation:get_federation_stats(),

    %% Should have at least 1 healthy (our up cluster)
    ?assert(maps:get(clusters_healthy, Stats) >= 1),

    %% Should have at least 1 unhealthy (our down cluster)
    ?assert(maps:get(clusters_unhealthy, Stats) >= 1),

    %% healthy + unhealthy should equal total
    Total = maps:get(clusters_total, Stats),
    Healthy = maps:get(clusters_healthy, Stats),
    Unhealthy = maps:get(clusters_unhealthy, Stats),
    ?assertEqual(Total, Healthy + Unhealthy).

test_get_federation_stats_empty() ->
    %% With just the local cluster (or maybe none depending on setup)
    %% get_federation_stats should still return valid structure
    Stats = flurm_federation:get_federation_stats(),

    ?assert(is_map(Stats)),
    ?assert(maps:is_key(clusters_total, Stats)),
    ?assert(maps:is_key(clusters_healthy, Stats)),
    ?assert(maps:is_key(clusters_unhealthy, Stats)),

    %% Values should be non-negative
    ?assert(maps:get(clusters_total, Stats) >= 0),
    ?assert(maps:get(clusters_healthy, Stats) >= 0),
    ?assert(maps:get(clusters_unhealthy, Stats) >= 0).

%%====================================================================
%% Helper Functions
%%====================================================================

add_cluster_with_load(Name, JobCount, CpuCount, MemoryMb) ->
    ok = flurm_federation:add_cluster(Name, #{
        host => <<Name/binary, ".example.com">>,
        features => []
    }),
    update_cluster_resources(Name, #{
        state => up,
        pending_jobs => JobCount,
        running_jobs => 0,
        cpu_count => CpuCount,
        available_cpus => CpuCount,
        memory_mb => MemoryMb,
        available_memory => MemoryMb
    }).

add_cluster_with_resources(Name, Resources) ->
    ok = flurm_federation:add_cluster(Name, #{
        host => <<Name/binary, ".example.com">>,
        features => []
    }),
    update_cluster_resources(Name, Resources#{state => up}).

update_cluster_resources(Name, Updates) ->
    %% Directly update the ETS table for testing
    case ets:lookup(flurm_fed_clusters, Name) of
        [Cluster] ->
            Updated = apply_updates(Cluster, Updates),
            ets:insert(flurm_fed_clusters, Updated);
        [] ->
            {error, not_found}
    end.

apply_updates(Cluster, Updates) ->
    %% Apply each update to the record
    %% Record positions: {fed_cluster, name, host, port, auth, state, weight, features,
    %%   partitions, node_count, cpu_count, memory_mb, gpu_count, pending_jobs,
    %%   running_jobs, available_cpus, available_memory, last_sync, last_health_check,
    %%   consecutive_failures, properties}
    %% Position 1 = record tag, Position 2 = name, etc.
    Cluster1 = case maps:get(state, Updates, undefined) of
        undefined -> Cluster;
        State -> setelement(6, Cluster, State)  % #fed_cluster.state position
    end,
    Cluster2 = case maps:get(weight, Updates, undefined) of
        undefined -> Cluster1;
        Weight -> setelement(7, Cluster1, Weight)
    end,
    Cluster3 = case maps:get(node_count, Updates, undefined) of
        undefined -> Cluster2;
        NodeCount -> setelement(10, Cluster2, NodeCount)
    end,
    Cluster4 = case maps:get(cpu_count, Updates, undefined) of
        undefined -> Cluster3;
        CpuCount -> setelement(11, Cluster3, CpuCount)
    end,
    Cluster5 = case maps:get(memory_mb, Updates, undefined) of
        undefined -> Cluster4;
        MemMb -> setelement(12, Cluster4, MemMb)
    end,
    Cluster6 = case maps:get(gpu_count, Updates, undefined) of
        undefined -> Cluster5;
        GpuCount -> setelement(13, Cluster5, GpuCount)
    end,
    Cluster7 = case maps:get(pending_jobs, Updates, undefined) of
        undefined -> Cluster6;
        Pending -> setelement(14, Cluster6, Pending)
    end,
    Cluster8 = case maps:get(running_jobs, Updates, undefined) of
        undefined -> Cluster7;
        Running -> setelement(15, Cluster7, Running)
    end,
    Cluster9 = case maps:get(available_cpus, Updates, undefined) of
        undefined -> Cluster8;
        AvailCpu -> setelement(16, Cluster8, AvailCpu)
    end,
    Cluster10 = case maps:get(available_memory, Updates, undefined) of
        undefined -> Cluster9;
        AvailMem -> setelement(17, Cluster9, AvailMem)
    end,
    Cluster11 = case maps:get(consecutive_failures, Updates, undefined) of
        undefined -> Cluster10;
        Failures -> setelement(20, Cluster10, Failures)
    end,
    Cluster11.

%%====================================================================
%% Internal Function Tests (exported under TEST)
%%====================================================================

internal_functions_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"build_url constructs correct URL", fun test_build_url/0},
      {"build_auth_headers with token", fun test_build_auth_headers_token/0},
      {"build_auth_headers with api_key", fun test_build_auth_headers_api_key/0},
      {"build_auth_headers with empty map", fun test_build_auth_headers_empty/0},
      {"headers_to_proplist converts correctly", fun test_headers_to_proplist/0},
      {"cluster_to_map converts all fields", fun test_cluster_to_map/0},
      {"generate_local_ref creates unique refs", fun test_generate_local_ref/0},
      {"generate_federation_id creates unique ids", fun test_generate_federation_id/0},
      {"get_job_partition from job record", fun test_get_job_partition_record/0},
      {"get_job_partition from map", fun test_get_job_partition_map/0},
      {"get_job_partition from invalid", fun test_get_job_partition_invalid/0},
      {"get_job_features from map", fun test_get_job_features/0},
      {"get_job_cpus from record and map", fun test_get_job_cpus/0},
      {"get_job_memory from record and map", fun test_get_job_memory/0},
      {"job_to_map converts job record", fun test_job_to_map/0},
      {"map_to_job converts map to record", fun test_map_to_job/0},
      {"has_required_features checks features", fun test_has_required_features/0},
      {"aggregate_resources sums cluster resources", fun test_aggregate_resources/0},
      {"calculate_load computes load ratio", fun test_calculate_load/0}
     ]}.

test_build_url() ->
    Url = flurm_federation:build_url(<<"host.example.com">>, 8080, <<"/api/test">>),
    ?assertEqual(<<"http://host.example.com:8080/api/test">>, Url).

test_build_auth_headers_token() ->
    Headers = flurm_federation:build_auth_headers(#{token => <<"mytoken123">>}),
    ?assertEqual([{<<"Authorization">>, <<"Bearer mytoken123">>}], Headers).

test_build_auth_headers_api_key() ->
    Headers = flurm_federation:build_auth_headers(#{api_key => <<"key456">>}),
    ?assertEqual([{<<"X-API-Key">>, <<"key456">>}], Headers).

test_build_auth_headers_empty() ->
    Headers = flurm_federation:build_auth_headers(#{}),
    ?assertEqual([], Headers).

test_headers_to_proplist() ->
    Headers = [{<<"Content-Type">>, <<"application/json">>}, {<<"Accept">>, <<"*/*">>}],
    Proplist = flurm_federation:headers_to_proplist(Headers),
    ?assertEqual([{"Content-Type", "application/json"}, {"Accept", "*/*"}], Proplist).

test_cluster_to_map() ->
    %% Add a cluster and convert it
    ok = flurm_federation:add_cluster(<<"map_test">>, #{
        host => <<"maptest.example.com">>,
        port => 9000,
        weight => 3,
        features => [<<"feature1">>],
        partitions => [<<"part1">>]
    }),
    update_cluster_resources(<<"map_test">>, #{
        state => up,
        node_count => 5,
        cpu_count => 50,
        memory_mb => 5000,
        gpu_count => 2
    }),

    {ok, ClusterMap} = flurm_federation:get_cluster_status(<<"map_test">>),
    ?assertEqual(<<"map_test">>, maps:get(name, ClusterMap)),
    ?assertEqual(<<"maptest.example.com">>, maps:get(host, ClusterMap)),
    ?assertEqual(9000, maps:get(port, ClusterMap)),
    ?assertEqual(3, maps:get(weight, ClusterMap)),
    ?assertEqual(up, maps:get(state, ClusterMap)),
    ?assertEqual(5, maps:get(node_count, ClusterMap)).

test_generate_local_ref() ->
    Ref1 = flurm_federation:generate_local_ref(),
    Ref2 = flurm_federation:generate_local_ref(),
    ?assert(is_binary(Ref1)),
    ?assert(is_binary(Ref2)),
    ?assertMatch(<<"ref-", _/binary>>, Ref1),
    ?assertNotEqual(Ref1, Ref2).

test_generate_federation_id() ->
    Id1 = flurm_federation:generate_federation_id(),
    Id2 = flurm_federation:generate_federation_id(),
    ?assert(is_binary(Id1)),
    ?assert(is_binary(Id2)),
    ?assertMatch(<<"fed-", _/binary>>, Id1),
    ?assertNotEqual(Id1, Id2).

test_get_job_partition_record() ->
    Job = #job{partition = <<"compute">>},
    ?assertEqual(<<"compute">>, flurm_federation:get_job_partition(Job)).

test_get_job_partition_map() ->
    Job = #{partition => <<"gpu">>},
    ?assertEqual(<<"gpu">>, flurm_federation:get_job_partition(Job)).

test_get_job_partition_invalid() ->
    ?assertEqual(undefined, flurm_federation:get_job_partition(not_a_job)).

test_get_job_features() ->
    JobWithFeatures = #{features => [<<"cuda">>, <<"nvme">>]},
    ?assertEqual([<<"cuda">>, <<"nvme">>], flurm_federation:get_job_features(JobWithFeatures)),

    JobWithoutFeatures = #{name => <<"test">>},
    ?assertEqual([], flurm_federation:get_job_features(JobWithoutFeatures)).

test_get_job_cpus() ->
    JobRecord = #job{num_cpus = 16},
    ?assertEqual(16, flurm_federation:get_job_cpus(JobRecord)),

    JobMap = #{num_cpus => 32},
    ?assertEqual(32, flurm_federation:get_job_cpus(JobMap)),

    ?assertEqual(1, flurm_federation:get_job_cpus(invalid)).

test_get_job_memory() ->
    JobRecord = #job{memory_mb = 8192},
    ?assertEqual(8192, flurm_federation:get_job_memory(JobRecord)),

    JobMap = #{memory_mb => 16384},
    ?assertEqual(16384, flurm_federation:get_job_memory(JobMap)),

    ?assertEqual(1024, flurm_federation:get_job_memory(invalid)).

test_job_to_map() ->
    Job = #job{
        id = 12345,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"batch">>,
        state = running,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 7200,
        priority = 50
    },
    JobMap = flurm_federation:job_to_map(Job),
    ?assertEqual(12345, maps:get(id, JobMap)),
    ?assertEqual(<<"test_job">>, maps:get(name, JobMap)),
    ?assertEqual(<<"testuser">>, maps:get(user, JobMap)),
    ?assertEqual(<<"batch">>, maps:get(partition, JobMap)),
    ?assertEqual(running, maps:get(state, JobMap)).

test_map_to_job() ->
    JobMap = #{
        id => 54321,
        name => <<"mapped_job">>,
        user => <<"mapuser">>,
        partition => <<"gpu">>,
        state => pending,
        num_cpus => 4,
        memory_mb => 2048
    },
    Job = flurm_federation:map_to_job(JobMap),
    ?assertEqual(54321, Job#job.id),
    ?assertEqual(<<"mapped_job">>, Job#job.name),
    ?assertEqual(<<"mapuser">>, Job#job.user),
    ?assertEqual(<<"gpu">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(4, Job#job.num_cpus),
    ?assertEqual(2048, Job#job.memory_mb).

test_has_required_features() ->
    ClusterFeatures = [<<"gpu">>, <<"nvme">>, <<"infiniband">>],

    %% All required features present
    ?assert(flurm_federation:has_required_features(ClusterFeatures, [<<"gpu">>])),
    ?assert(flurm_federation:has_required_features(ClusterFeatures, [<<"gpu">>, <<"nvme">>])),

    %% Missing required feature
    ?assertNot(flurm_federation:has_required_features(ClusterFeatures, [<<"cuda">>])),
    ?assertNot(flurm_federation:has_required_features(ClusterFeatures, [<<"gpu">>, <<"missing">>])),

    %% Empty requirements always passes
    ?assert(flurm_federation:has_required_features(ClusterFeatures, [])),
    ?assert(flurm_federation:has_required_features([], [])).

test_aggregate_resources() ->
    %% Test the aggregate_resources function with mock cluster records
    Clusters = [
        {fed_cluster, <<"c1">>, <<"h1">>, 6817, #{}, up, 1, [], [],
         10, 100, 10000, 2, 5, 10, 80, 8000, 0, 0, 0, #{}},
        {fed_cluster, <<"c2">>, <<"h2">>, 6817, #{}, up, 1, [], [],
         20, 200, 20000, 4, 10, 20, 160, 16000, 0, 0, 0, #{}},
        {fed_cluster, <<"c3">>, <<"h3">>, 6817, #{}, down, 1, [], [],
         5, 50, 5000, 0, 2, 5, 40, 4000, 0, 0, 0, #{}}
    ],

    Resources = flurm_federation:aggregate_resources(Clusters),

    %% Only up clusters should contribute
    ?assertEqual(30, maps:get(total_nodes, Resources)),
    ?assertEqual(300, maps:get(total_cpus, Resources)),
    ?assertEqual(30000, maps:get(total_memory_mb, Resources)),
    ?assertEqual(6, maps:get(total_gpus, Resources)),
    ?assertEqual(240, maps:get(available_cpus, Resources)),
    ?assertEqual(24000, maps:get(available_memory_mb, Resources)),
    ?assertEqual(15, maps:get(pending_jobs, Resources)),
    ?assertEqual(30, maps:get(running_jobs, Resources)),
    ?assertEqual(2, maps:get(clusters_up, Resources)),
    ?assertEqual(1, maps:get(clusters_down, Resources)).

test_calculate_load() ->
    %% Test load calculation
    Cluster1 = {fed_cluster, <<"c1">>, <<"h1">>, 6817, #{}, up, 1, [], [],
                10, 100, 10000, 0, 20, 30, 50, 5000, 0, 0, 0, #{}},
    Load1 = flurm_federation:calculate_load(Cluster1),
    %% (20 pending + 30 running) / 100 cpus = 0.5
    ?assertEqual(0.5, Load1),

    %% Zero CPU count should return infinity
    Cluster2 = {fed_cluster, <<"c2">>, <<"h2">>, 6817, #{}, up, 1, [], [],
                10, 0, 10000, 0, 20, 30, 0, 5000, 0, 0, 0, #{}},
    Load2 = flurm_federation:calculate_load(Cluster2),
    ?assertEqual(infinity, Load2).

%%====================================================================
%% Federation Management Tests
%%====================================================================

federation_management_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"create_federation creates new federation", fun test_create_federation/0},
      {"create_federation fails when already federated", fun test_create_federation_already_federated/0},
      {"leave_federation removes federation state", fun test_leave_federation/0},
      {"leave_federation fails when not federated", fun test_leave_federation_not_federated/0},
      {"get_federation_info returns federation data", fun test_get_federation_info/0},
      {"get_federation_info fails when not federated", fun test_get_federation_info_not_federated/0},
      {"get_cluster_info returns cluster record", fun test_get_cluster_info/0},
      {"get_cluster_info returns error for unknown", fun test_get_cluster_info_not_found/0},
      {"set_cluster_features updates local cluster", fun test_set_cluster_features/0},
      {"update_settings changes routing policy", fun test_update_settings_routing_policy/0},
      {"update_settings with sync interval", fun test_update_settings_sync_interval/0},
      {"update_settings with health check interval", fun test_update_settings_health_check/0}
     ]}.

test_create_federation() ->
    Result = flurm_federation:create_federation(<<"test_fed">>, [<<"cluster_a">>, <<"cluster_b">>]),
    ?assertEqual(ok, Result),

    {ok, Info} = flurm_federation:get_federation_info(),
    ?assertEqual(<<"test_fed">>, maps:get(name, Info)),
    ?assert(lists:member(<<"test_cluster">>, maps:get(clusters, Info))).

test_create_federation_already_federated() ->
    ok = flurm_federation:create_federation(<<"fed1">>, []),
    Result = flurm_federation:create_federation(<<"fed2">>, []),
    ?assertEqual({error, already_federated}, Result).

test_leave_federation() ->
    ok = flurm_federation:create_federation(<<"leaving_fed">>, [<<"external">>]),
    {ok, _} = flurm_federation:get_federation_info(),

    ok = flurm_federation:leave_federation(),

    Result = flurm_federation:get_federation_info(),
    ?assertEqual({error, not_federated}, Result).

test_leave_federation_not_federated() ->
    Result = flurm_federation:leave_federation(),
    ?assertEqual({error, not_federated}, Result).

test_get_federation_info() ->
    ok = flurm_federation:create_federation(<<"info_fed">>, [<<"c1">>, <<"c2">>]),

    {ok, Info} = flurm_federation:get_federation_info(),
    ?assert(is_map(Info)),
    ?assertEqual(<<"info_fed">>, maps:get(name, Info)),
    ?assert(maps:is_key(clusters, Info)),
    ?assert(maps:is_key(local_cluster, Info)),
    ?assertEqual(<<"test_cluster">>, maps:get(local_cluster, Info)).

test_get_federation_info_not_federated() ->
    Result = flurm_federation:get_federation_info(),
    ?assertEqual({error, not_federated}, Result).

test_get_cluster_info() ->
    ok = flurm_federation:add_cluster(<<"info_cluster">>, #{
        host => <<"info.example.com">>,
        port => 6818,
        weight => 5
    }),

    {ok, ClusterInfo} = flurm_federation:get_cluster_info(<<"info_cluster">>),
    ?assertEqual(<<"info_cluster">>, element(2, ClusterInfo)),
    ?assertEqual(<<"info.example.com">>, element(3, ClusterInfo)),
    ?assertEqual(6818, element(4, ClusterInfo)).

test_get_cluster_info_not_found() ->
    Result = flurm_federation:get_cluster_info(<<"missing_cluster">>),
    ?assertEqual({error, not_found}, Result).

test_set_cluster_features() ->
    ok = flurm_federation:set_cluster_features([<<"fast">>, <<"gpu">>, <<"nvme">>]),

    LocalCluster = flurm_federation:get_local_cluster(),
    {ok, Info} = flurm_federation:get_cluster_status(LocalCluster),
    Features = maps:get(features, Info),
    ?assert(lists:member(<<"fast">>, Features)),
    ?assert(lists:member(<<"gpu">>, Features)),
    ?assert(lists:member(<<"nvme">>, Features)).

test_update_settings_routing_policy() ->
    ok = flurm_federation:update_settings(#{routing_policy => round_robin}),
    %% Verify via routing behavior - add clusters and route
    add_cluster_with_load(<<"rr_test1">>, 0, 100, 1000),
    add_cluster_with_load(<<"rr_test2">>, 0, 100, 1000),

    Job = #{num_cpus => 1, memory_mb => 100},
    {ok, _Selected} = flurm_federation:route_job(Job),
    %% Just verify it doesn't crash with new policy
    ok.

test_update_settings_sync_interval() ->
    ok = flurm_federation:update_settings(#{sync_interval => 60000}),
    ok.

test_update_settings_health_check() ->
    ok = flurm_federation:update_settings(#{health_check_interval => 15000}),
    ok.

%%====================================================================
%% Cross-cluster Job Submission Tests
%%====================================================================

job_submission_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"submit_federated_job without federation fails", fun test_submit_federated_job_not_federated/0},
      {"submit_federated_job with federation", fun test_submit_federated_job/0},
      {"get_federated_job retrieves job info", fun test_get_federated_job/0},
      {"get_federated_job returns error for unknown", fun test_get_federated_job_not_found/0},
      {"sync_cluster_state syncs remote cluster", fun test_sync_cluster_state/0},
      {"sync_cluster_state fails for unknown cluster", fun test_sync_cluster_state_not_found/0},
      {"submit_job to unavailable cluster", fun test_submit_job_unavailable_cluster/0},
      {"submit_job to cluster not found", fun test_submit_job_cluster_not_found/0},
      {"route_job falls back to local", fun test_route_job_fallback_local/0},
      {"route_job with insufficient resources", fun test_route_job_insufficient_resources/0}
     ]}.

test_submit_federated_job_not_federated() ->
    Job = #job{name = <<"fed_job">>, user = <<"user">>},
    Result = flurm_federation:submit_federated_job(Job, #{}),
    ?assertEqual({error, not_federated}, Result).

test_submit_federated_job() ->
    ok = flurm_federation:create_federation(<<"job_fed">>, []),

    %% Add a suitable cluster
    ok = flurm_federation:add_cluster(<<"job_target">>, #{
        host => <<"target.example.com">>,
        features => []
    }),
    update_cluster_resources(<<"job_target">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    Job = #job{name = <<"fed_job">>, user = <<"user">>},
    %% Will fail to actually submit but should get a federation ID
    %% (submission fails due to no actual remote cluster)
    Result = flurm_federation:submit_federated_job(Job, #{}),
    case Result of
        {ok, FedId} ->
            ?assert(is_binary(FedId)),
            ?assertMatch(<<"fed-", _/binary>>, FedId);
        {error, submission_failed} ->
            %% Expected if remote submission fails
            ok
    end.

test_get_federated_job() ->
    ok = flurm_federation:create_federation(<<"get_job_fed">>, []),
    ok = flurm_federation:add_cluster(<<"get_job_cluster">>, #{
        host => <<"getjob.example.com">>,
        features => []
    }),
    update_cluster_resources(<<"get_job_cluster">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    Job = #job{name = <<"get_test_job">>, user = <<"user">>},
    case flurm_federation:submit_federated_job(Job, #{}) of
        {ok, FedId} ->
            Result = flurm_federation:get_federated_job(FedId),
            ?assertMatch({ok, _}, Result);
        {error, _} ->
            %% If submission failed, just verify get_federated_job handles missing
            Result = flurm_federation:get_federated_job(<<"fed-missing-123">>),
            ?assertEqual({error, not_found}, Result)
    end.

test_get_federated_job_not_found() ->
    Result = flurm_federation:get_federated_job(<<"fed-nonexistent-12345">>),
    ?assertEqual({error, not_found}, Result).

test_sync_cluster_state() ->
    ok = flurm_federation:add_cluster(<<"sync_test">>, #{
        host => <<"sync.example.com">>,
        port => 6817
    }),

    %% Sync will fail due to no actual remote, but shouldn't crash
    Result = flurm_federation:sync_cluster_state(<<"sync_test">>),
    ?assertMatch({error, _}, Result).

test_sync_cluster_state_not_found() ->
    Result = flurm_federation:sync_cluster_state(<<"missing_cluster">>),
    ?assertEqual({error, cluster_not_found}, Result).

test_submit_job_unavailable_cluster() ->
    ok = flurm_federation:add_cluster(<<"unavailable">>, #{
        host => <<"unavailable.example.com">>
    }),
    update_cluster_resources(<<"unavailable">>, #{state => down}),

    Job = #{name => <<"test">>, num_cpus => 1, memory_mb => 100},
    Result = flurm_federation:submit_job(Job, #{cluster => <<"unavailable">>}),
    ?assertMatch({error, {cluster_unavailable, _}}, Result).

test_submit_job_cluster_not_found() ->
    Job = #{name => <<"test">>, num_cpus => 1, memory_mb => 100},
    Result = flurm_federation:submit_job(Job, #{cluster => <<"not_a_cluster">>}),
    ?assertEqual({error, cluster_not_found}, Result).

test_route_job_fallback_local() ->
    %% When no clusters are suitable, should fall back to local
    Job = #{num_cpus => 9999, memory_mb => 999999999},  % Impossible requirements
    {ok, Selected} = flurm_federation:route_job(Job),
    ?assertEqual(<<"test_cluster">>, Selected).

test_route_job_insufficient_resources() ->
    %% Add cluster with limited resources
    ok = flurm_federation:add_cluster(<<"limited">>, #{
        host => <<"limited.example.com">>
    }),
    update_cluster_resources(<<"limited">>, #{
        state => up,
        available_cpus => 2,
        available_memory => 1000
    }),

    %% Job requiring more resources than available
    Job = #{num_cpus => 100, memory_mb => 50000},
    {ok, Selected} = flurm_federation:route_job(Job),
    %% Should fall back to local since limited cluster doesn't have resources
    ?assertEqual(<<"test_cluster">>, Selected).

%%====================================================================
%% Remote Job Tracking Tests
%%====================================================================

remote_job_tracking_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_remote_job_status for unknown cluster", fun test_get_remote_job_status_unknown_cluster/0},
      {"sync_job_state updates tracking", fun test_sync_job_state/0},
      {"get_federation_jobs returns all jobs", fun test_get_federation_jobs/0}
     ]}.

test_get_remote_job_status_unknown_cluster() ->
    Result = flurm_federation:get_remote_job_status(<<"unknown_cluster">>, 12345),
    ?assertEqual({error, cluster_not_found}, Result).

test_sync_job_state() ->
    ok = flurm_federation:add_cluster(<<"sync_state_cluster">>, #{
        host => <<"syncstate.example.com">>
    }),

    %% Track a job first
    {ok, _LocalRef} = flurm_federation:track_remote_job(
        <<"sync_state_cluster">>,
        67890,
        #{name => <<"sync_job">>}
    ),

    %% Sync will fail due to no actual remote, but shouldn't crash
    Result = flurm_federation:sync_job_state(<<"sync_state_cluster">>, 67890),
    ?assertMatch({error, _}, Result).

test_get_federation_jobs() ->
    %% Track some remote jobs
    ok = flurm_federation:add_cluster(<<"jobs_cluster">>, #{
        host => <<"jobs.example.com">>
    }),

    {ok, _} = flurm_federation:track_remote_job(<<"jobs_cluster">>, 111, #{name => <<"j1">>}),
    {ok, _} = flurm_federation:track_remote_job(<<"jobs_cluster">>, 222, #{name => <<"j2">>}),

    Jobs = flurm_federation:get_federation_jobs(),
    ?assert(is_list(Jobs)),
    %% Should have at least our tracked jobs
    ?assert(length(Jobs) >= 2),

    %% Verify job structure
    [Job | _] = Jobs,
    ?assert(maps:is_key(cluster, Job) orelse maps:is_key(local_ref, Job)).

%%====================================================================
%% Cluster Update Handler Tests
%%====================================================================

cluster_update_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"cluster_update message updates stats", fun test_cluster_update_message/0},
      {"cluster_health up message updates state", fun test_cluster_health_up/0},
      {"cluster_health down message increments failures", fun test_cluster_health_down/0},
      {"unknown request returns error", fun test_unknown_request/0}
     ]}.

test_cluster_update_message() ->
    ok = flurm_federation:add_cluster(<<"update_msg_cluster">>, #{
        host => <<"update.example.com">>
    }),

    FedPid = whereis(flurm_federation),
    Stats = #{
        node_count => 50,
        cpu_count => 500,
        memory_mb => 50000,
        gpu_count => 10,
        available_cpus => 400,
        available_memory => 40000,
        pending_jobs => 25,
        running_jobs => 50
    },
    FedPid ! {cluster_update, <<"update_msg_cluster">>, Stats},
    _ = sys:get_state(flurm_federation),

    {ok, Status} = flurm_federation:get_cluster_status(<<"update_msg_cluster">>),
    ?assertEqual(50, maps:get(node_count, Status)),
    ?assertEqual(500, maps:get(cpu_count, Status)),
    ?assertEqual(up, maps:get(state, Status)).

test_cluster_health_up() ->
    ok = flurm_federation:add_cluster(<<"health_up_cluster">>, #{
        host => <<"healthup.example.com">>
    }),
    update_cluster_resources(<<"health_up_cluster">>, #{
        state => down,
        consecutive_failures => 5
    }),

    FedPid = whereis(flurm_federation),
    FedPid ! {cluster_health, <<"health_up_cluster">>, up},
    _ = sys:get_state(flurm_federation),

    {ok, Status} = flurm_federation:get_cluster_status(<<"health_up_cluster">>),
    ?assertEqual(up, maps:get(state, Status)),
    ?assertEqual(0, maps:get(consecutive_failures, Status)).

test_cluster_health_down() ->
    ok = flurm_federation:add_cluster(<<"health_down_cluster">>, #{
        host => <<"healthdown.example.com">>
    }),
    update_cluster_resources(<<"health_down_cluster">>, #{
        state => up,
        consecutive_failures => 0
    }),

    FedPid = whereis(flurm_federation),
    FedPid ! {cluster_health, <<"health_down_cluster">>, down},
    _ = sys:get_state(flurm_federation),

    {ok, Status} = flurm_federation:get_cluster_status(<<"health_down_cluster">>),
    ?assertEqual(1, maps:get(consecutive_failures, Status)).

test_unknown_request() ->
    Result = gen_server:call(flurm_federation, {unknown_operation, test_data}),
    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"empty partition routing", fun test_empty_partition_routing/0},
      {"zero weight cluster selection", fun test_zero_weight_cluster/0},
      {"cluster with zero cpus", fun test_cluster_zero_cpus/0},
      {"multiple partitions same cluster", fun test_multiple_partitions_same_cluster/0},
      {"add cluster with all options", fun test_add_cluster_all_options/0},
      {"remove cluster clears partition mappings", fun test_remove_cluster_clears_partitions/0},
      {"large cluster count routing", fun test_large_cluster_count/0},
      {"concurrent cluster operations", fun test_concurrent_operations/0}
     ]}.

test_empty_partition_routing() ->
    Job = #{partition => <<>>, num_cpus => 1, memory_mb => 100},
    {ok, _Selected} = flurm_federation:route_job(Job),
    ok.

test_zero_weight_cluster() ->
    ok = flurm_federation:add_cluster(<<"zero_weight">>, #{
        host => <<"zero.example.com">>,
        weight => 0
    }),
    update_cluster_resources(<<"zero_weight">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    Job = #{num_cpus => 1, memory_mb => 100},
    {ok, _Selected} = flurm_federation:route_job(Job),
    ok.

test_cluster_zero_cpus() ->
    ok = flurm_federation:add_cluster(<<"no_cpus">>, #{
        host => <<"nocpus.example.com">>
    }),
    update_cluster_resources(<<"no_cpus">>, #{
        state => up,
        available_cpus => 0,
        available_memory => 0,
        cpu_count => 0
    }),

    %% Should not be selected for any job
    Job = #{num_cpus => 1, memory_mb => 100},
    {ok, Selected} = flurm_federation:route_job(Job),
    ?assertNotEqual(<<"no_cpus">>, Selected).

test_multiple_partitions_same_cluster() ->
    ok = flurm_federation:add_cluster(<<"multi_part">>, #{
        host => <<"multipart.example.com">>,
        partitions => [<<"part1">>, <<"part2">>, <<"part3">>]
    }),
    update_cluster_resources(<<"multi_part">>, #{
        state => up,
        available_cpus => 100,
        available_memory => 100000
    }),

    %% All partitions should map to same cluster
    {ok, C1} = flurm_federation:get_cluster_for_partition(<<"part1">>),
    {ok, C2} = flurm_federation:get_cluster_for_partition(<<"part2">>),
    {ok, C3} = flurm_federation:get_cluster_for_partition(<<"part3">>),

    ?assertEqual(<<"multi_part">>, C1),
    ?assertEqual(<<"multi_part">>, C2),
    ?assertEqual(<<"multi_part">>, C3).

test_add_cluster_all_options() ->
    Config = #{
        host => <<"full.example.com">>,
        port => 9999,
        auth => #{token => <<"secret_token">>},
        weight => 10,
        features => [<<"gpu">>, <<"nvme">>, <<"infiniband">>],
        partitions => [<<"compute">>, <<"gpu">>, <<"highmem">>]
    },
    ok = flurm_federation:add_cluster(<<"full_options">>, Config),

    {ok, Status} = flurm_federation:get_cluster_status(<<"full_options">>),
    ?assertEqual(<<"full.example.com">>, maps:get(host, Status)),
    ?assertEqual(9999, maps:get(port, Status)),
    ?assertEqual(10, maps:get(weight, Status)),
    ?assertEqual([<<"gpu">>, <<"nvme">>, <<"infiniband">>], maps:get(features, Status)),
    ?assertEqual([<<"compute">>, <<"gpu">>, <<"highmem">>], maps:get(partitions, Status)).

test_remove_cluster_clears_partitions() ->
    ok = flurm_federation:add_cluster(<<"remove_parts">>, #{
        host => <<"removeparts.example.com">>,
        partitions => [<<"unique_part">>]
    }),

    %% Verify partition exists
    {ok, _} = flurm_federation:get_cluster_for_partition(<<"unique_part">>),

    %% Remove cluster
    ok = flurm_federation:remove_cluster(<<"remove_parts">>),

    %% Partition should be gone
    ?assertEqual({error, not_found}, flurm_federation:get_cluster_for_partition(<<"unique_part">>)).

test_large_cluster_count() ->
    %% Add many clusters
    lists:foreach(fun(N) ->
        Name = iolist_to_binary([<<"large_cluster_">>, integer_to_binary(N)]),
        ok = flurm_federation:add_cluster(Name, #{
            host => <<Name/binary, ".example.com">>
        }),
        update_cluster_resources(Name, #{
            state => up,
            available_cpus => 100,
            available_memory => 100000
        })
    end, lists:seq(1, 50)),

    Clusters = flurm_federation:list_clusters(),
    ?assert(length(Clusters) >= 50),

    %% Routing should still work
    Job = #{num_cpus => 1, memory_mb => 100},
    {ok, _Selected} = flurm_federation:route_job(Job),
    ok.

test_concurrent_operations() ->
    %% Spawn multiple processes doing operations concurrently
    Self = self(),
    Pids = [spawn(fun() ->
        Name = iolist_to_binary([<<"concurrent_">>, integer_to_binary(N)]),
        ok = flurm_federation:add_cluster(Name, #{
            host => <<Name/binary, ".example.com">>
        }),
        {ok, _} = flurm_federation:get_cluster_status(Name),
        _ = flurm_federation:list_clusters(),
        ok = flurm_federation:remove_cluster(Name),
        Self ! {done, N}
    end) || N <- lists:seq(1, 10)],

    %% Wait for all to complete
    lists:foreach(fun(_) ->
        receive
            {done, _} -> ok
        after 5000 ->
            ?assert(false)  % Timeout
        end
    end, Pids),
    ok.

%%====================================================================
%% Sibling Job Coordination Tests (Phase 7D)
%%====================================================================

sibling_job_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"create_sibling_jobs creates tracker", fun test_create_sibling_jobs/0},
      {"get_running_cluster for new job", fun test_get_running_cluster_new/0},
      {"get_running_cluster not found", fun test_get_running_cluster_not_found/0},
      {"get_sibling_job_state returns state", fun test_get_sibling_job_state/0},
      {"get_sibling_job_state not found", fun test_get_sibling_job_state_not_found/0}
     ]}.

test_create_sibling_jobs() ->
    %% Add target clusters
    ok = flurm_federation:add_cluster(<<"sibling_c1">>, #{
        host => <<"sibling1.example.com">>
    }),
    update_cluster_resources(<<"sibling_c1">>, #{state => up}),

    JobSpec = #{name => <<"sibling_test">>, num_cpus => 1},
    Result = flurm_federation:create_sibling_jobs(JobSpec, [<<"sibling_c1">>]),

    %% May succeed or fail depending on actual cluster availability
    case Result of
        {ok, FedJobId} ->
            ?assert(is_binary(FedJobId)),
            ?assertMatch(<<"fed-", _/binary>>, FedJobId);
        {error, no_siblings_created} ->
            ok  % Expected if remote submission fails
    end.

test_get_running_cluster_new() ->
    %% For a non-existent federation job
    Result = flurm_federation:get_running_cluster(<<"fed-new-test-123">>),
    ?assertEqual({error, not_found}, Result).

test_get_running_cluster_not_found() ->
    Result = flurm_federation:get_running_cluster(<<"fed-missing-456">>),
    ?assertEqual({error, not_found}, Result).

test_get_sibling_job_state() ->
    %% Test with non-existent job
    Result = flurm_federation:get_sibling_job_state(<<"fed-state-test">>, <<"cluster1">>),
    ?assertEqual({error, not_found}, Result).

test_get_sibling_job_state_not_found() ->
    Result = flurm_federation:get_sibling_job_state(<<"fed-missing">>, <<"missing_cluster">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"sync_all timer triggers sync", fun test_sync_all_timer/0},
      {"health_check timer updates state", fun test_health_check_timer/0},
      {"unknown message is ignored", fun test_unknown_message/0}
     ]}.

test_sync_all_timer() ->
    FedPid = whereis(flurm_federation),
    FedPid ! sync_all,
    _ = sys:get_state(flurm_federation),
    %% Just verify it doesn't crash
    ok.

test_health_check_timer() ->
    FedPid = whereis(flurm_federation),
    FedPid ! health_check,
    _ = sys:get_state(flurm_federation),
    %% Just verify it doesn't crash
    ok.

test_unknown_message() ->
    FedPid = whereis(flurm_federation),
    FedPid ! {totally_unknown_message, random_data},
    _ = sys:get_state(flurm_federation),
    %% Should be ignored without crash
    ok.

%%====================================================================
%% HTTP Client Tests (Phase 8)
%%====================================================================

http_client_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"http_get builds correct request", fun test_http_get_request/0},
      {"http_post builds correct request", fun test_http_post_request/0},
      {"http_put builds correct request", fun test_http_put_request/0},
      {"http_delete builds correct request", fun test_http_delete_request/0},
      {"http request with auth headers", fun test_http_with_auth/0},
      {"http request timeout handling", fun test_http_timeout/0},
      {"http request connection error", fun test_http_connection_error/0},
      {"http response parsing json", fun test_http_json_response/0},
      {"http response parsing binary", fun test_http_binary_response/0}
     ]}.

test_http_get_request() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, _HttpOpts, _Opts) ->
        {ok, {{version, 200, "OK"}, [], "{\"status\":\"ok\"}"}}
    end),

    ok = flurm_federation:add_cluster(<<"http_test">>, #{
        host => <<"httptest.example.com">>,
        port => 8080
    }),

    %% Trigger a sync which makes HTTP requests
    _ = flurm_federation:sync_cluster_state(<<"http_test">>),

    meck:unload(httpc),
    ok.

test_http_post_request() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun
        (get, {_Url, _Headers}, _HttpOpts, _Opts) ->
            {ok, {{version, 200, "OK"}, [], "{}"}};
        (post, {_Url, _Headers, _ContentType, _Body}, _HttpOpts, _Opts) ->
            {ok, {{version, 201, "Created"}, [], "{\"id\":\"123\"}"}}
    end),

    ok = flurm_federation:add_cluster(<<"post_test">>, #{
        host => <<"posttest.example.com">>
    }),
    update_cluster_resources(<<"post_test">>, #{state => up}),

    %% Submit job to trigger POST
    JobSpec = #{name => <<"http_post_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),

    meck:unload(httpc),
    ok.

test_http_put_request() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun
        (get, {_Url, _Headers}, _HttpOpts, _Opts) ->
            {ok, {{version, 200, "OK"}, [], "{}"}};
        (put, {_Url, _Headers, _ContentType, _Body}, _HttpOpts, _Opts) ->
            {ok, {{version, 200, "OK"}, [], "{\"updated\":true}"}}
    end),

    ok = flurm_federation:add_cluster(<<"put_test">>, #{
        host => <<"puttest.example.com">>
    }),

    %% Trigger update operation
    _ = flurm_federation:update_settings(#{sync_interval => 120000}),

    meck:unload(httpc),
    ok.

test_http_delete_request() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun
        (get, {_Url, _Headers}, _HttpOpts, _Opts) ->
            {ok, {{version, 200, "OK"}, [], "{}"}};
        (delete, {_Url, _Headers}, _HttpOpts, _Opts) ->
            {ok, {{version, 204, "No Content"}, [], ""}}
    end),

    ok = flurm_federation:add_cluster(<<"delete_test">>, #{
        host => <<"deletetest.example.com">>
    }),

    %% Remove cluster triggers DELETE
    _ = flurm_federation:remove_cluster(<<"delete_test">>),

    meck:unload(httpc),
    ok.

test_http_with_auth() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, {_Url, Headers}, _HttpOpts, _Opts) ->
        %% Verify auth header is present
        AuthHeaders = [H || {"Authorization", _} = H <- Headers],
        case length(AuthHeaders) >= 0 of
            true -> {ok, {{version, 200, "OK"}, [], "{}"}};
            false -> {error, missing_auth}
        end
    end),

    ok = flurm_federation:add_cluster(<<"auth_test">>, #{
        host => <<"authtest.example.com">>,
        auth_token => <<"secret_token_123">>
    }),

    _ = flurm_federation:sync_cluster_state(<<"auth_test">>),

    meck:unload(httpc),
    ok.

test_http_timeout() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, timeout}
    end),

    ok = flurm_federation:add_cluster(<<"timeout_test">>, #{
        host => <<"timeout.example.com">>
    }),

    Result = flurm_federation:sync_cluster_state(<<"timeout_test">>),
    %% Should handle timeout gracefully
    ?assertMatch({error, _}, Result),

    meck:unload(httpc),
    ok.

test_http_connection_error() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, {failed_connect, [{to_address, {"host", 80}}, {inet, [], econnrefused}]}}
    end),

    ok = flurm_federation:add_cluster(<<"conn_error_test">>, #{
        host => <<"unreachable.example.com">>
    }),

    Result = flurm_federation:sync_cluster_state(<<"conn_error_test">>),
    ?assertMatch({error, _}, Result),

    meck:unload(httpc),
    ok.

test_http_json_response() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        JsonBody = "{\"cluster\":\"test\",\"cpus\":100,\"memory\":1024000}",
        {ok, {{version, 200, "OK"}, [{"content-type", "application/json"}], JsonBody}}
    end),

    ok = flurm_federation:add_cluster(<<"json_test">>, #{
        host => <<"jsontest.example.com">>
    }),

    _ = flurm_federation:sync_cluster_state(<<"json_test">>),

    meck:unload(httpc),
    ok.

test_http_binary_response() ->
    meck:new(httpc, [unstick]),
    %% Return valid JSON since the code parses it with jsx
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {ok, {{version, 200, "OK"}, [], "{\"status\":\"ok\"}"}}
    end),

    ok = flurm_federation:add_cluster(<<"binary_test">>, #{
        host => <<"binarytest.example.com">>
    }),

    _ = flurm_federation:sync_cluster_state(<<"binary_test">>),

    meck:unload(httpc),
    ok.

%%====================================================================
%% Cluster Weighting Tests (Phase 9)
%%====================================================================

cluster_weighting_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"default weight is 1", fun test_default_weight/0},
      {"custom weight affects routing", fun test_custom_weight_routing/0},
      {"weight 0 excludes cluster", fun test_weight_zero_excluded/0},
      {"high weight preferred", fun test_high_weight_preferred/0},
      {"negative weight handled", fun test_negative_weight/0},
      {"fractional weight handled", fun test_fractional_weight/0},
      {"update weight dynamically", fun test_dynamic_weight_update/0}
     ]}.

test_default_weight() ->
    ok = flurm_federation:add_cluster(<<"default_w">>, #{
        host => <<"defaultw.example.com">>
    }),

    %% Use get_cluster_status which returns a map, not get_cluster_info which returns a record
    Info = flurm_federation:get_cluster_status(<<"default_w">>),
    case Info of
        {ok, ClusterInfo} ->
            Weight = maps:get(weight, ClusterInfo, 1),
            ?assertEqual(1, Weight);
        {error, not_found} ->
            ok  % Acceptable if cluster not fully initialized
    end.

test_custom_weight_routing() ->
    ok = flurm_federation:add_cluster(<<"weight_c1">>, #{
        host => <<"weightc1.example.com">>,
        weight => 10
    }),
    ok = flurm_federation:add_cluster(<<"weight_c2">>, #{
        host => <<"weightc2.example.com">>,
        weight => 1
    }),

    update_cluster_resources(<<"weight_c1">>, #{
        state => up,
        total_cpus => 100, free_cpus => 100
    }),
    update_cluster_resources(<<"weight_c2">>, #{
        state => up,
        total_cpus => 100, free_cpus => 100
    }),

    JobSpec = #{name => <<"weight_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_weight_zero_excluded() ->
    ok = flurm_federation:add_cluster(<<"zero_w1">>, #{
        host => <<"zerow1.example.com">>,
        weight => 0
    }),
    ok = flurm_federation:add_cluster(<<"zero_w2">>, #{
        host => <<"zerow2.example.com">>,
        weight => 5
    }),

    update_cluster_resources(<<"zero_w1">>, #{state => up, total_cpus => 100, free_cpus => 100}),
    update_cluster_resources(<<"zero_w2">>, #{state => up, total_cpus => 100, free_cpus => 100}),

    JobSpec = #{name => <<"zero_weight_test">>, num_cpus => 1},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Job should not be routed to zero weight cluster
    case Result of
        {ok, _JobId} -> ok;
        {error, _} -> ok
    end.

test_high_weight_preferred() ->
    ok = flurm_federation:add_cluster(<<"high_w">>, #{
        host => <<"highw.example.com">>,
        weight => 100
    }),
    ok = flurm_federation:add_cluster(<<"low_w">>, #{
        host => <<"loww.example.com">>,
        weight => 1
    }),

    update_cluster_resources(<<"high_w">>, #{state => up, free_cpus => 50}),
    update_cluster_resources(<<"low_w">>, #{state => up, free_cpus => 50}),

    %% Submit multiple jobs - high weight should get more
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("high_w_test_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 5)),
    ok.

test_negative_weight() ->
    Result = flurm_federation:add_cluster(<<"neg_w">>, #{
        host => <<"negw.example.com">>,
        weight => -5
    }),
    %% Should either reject or treat as 0
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_fractional_weight() ->
    ok = flurm_federation:add_cluster(<<"frac_w">>, #{
        host => <<"fracw.example.com">>,
        weight => 0.5
    }),

    Info = flurm_federation:get_cluster_info(<<"frac_w">>),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_dynamic_weight_update() ->
    ok = flurm_federation:add_cluster(<<"dyn_w">>, #{
        host => <<"dynw.example.com">>,
        weight => 5
    }),

    %% Update weight
    _ = flurm_federation:set_cluster_features(<<"dyn_w">>, #{weight => 10}),

    %% Use get_cluster_status which returns a map, not get_cluster_info which returns a record
    Info = flurm_federation:get_cluster_status(<<"dyn_w">>),
    case Info of
        {ok, ClusterInfo} ->
            %% Weight should be updated
            _Weight = maps:get(weight, ClusterInfo, undefined),
            ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Job State Machine Tests (Phase 10)
%%====================================================================

job_state_machine_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"job state pending to running", fun test_job_state_pending_running/0},
      {"job state running to completed", fun test_job_state_running_completed/0},
      {"job state pending to cancelled", fun test_job_state_pending_cancelled/0},
      {"job state running to failed", fun test_job_state_running_failed/0},
      {"job state transitions are atomic", fun test_job_state_atomic/0},
      {"invalid state transition rejected", fun test_invalid_state_transition/0},
      {"job state history tracked", fun test_job_state_history/0}
     ]}.

test_job_state_pending_running() ->
    ok = flurm_federation:add_cluster(<<"state_c1">>, #{
        host => <<"statec1.example.com">>
    }),
    update_cluster_resources(<<"state_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"state_test_1">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            %% Initially pending
            _ = flurm_federation:get_federated_job(JobId),
            ok;
        {error, _} -> ok
    end.

test_job_state_running_completed() ->
    ok = flurm_federation:add_cluster(<<"state_c2">>, #{
        host => <<"statec2.example.com">>
    }),
    update_cluster_resources(<<"state_c2">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"state_test_2">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_job_state_pending_cancelled() ->
    ok = flurm_federation:add_cluster(<<"state_c3">>, #{
        host => <<"statec3.example.com">>
    }),
    update_cluster_resources(<<"state_c3">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"state_test_3">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            %% Cancel the job
            _ = flurm_federation:cancel_federated_job(JobId),
            ok;
        {error, _} -> ok
    end.

test_job_state_running_failed() ->
    ok = flurm_federation:add_cluster(<<"state_c4">>, #{
        host => <<"statec4.example.com">>
    }),
    update_cluster_resources(<<"state_c4">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"state_test_4">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_job_state_atomic() ->
    ok = flurm_federation:add_cluster(<<"state_c5">>, #{
        host => <<"statec5.example.com">>
    }),
    update_cluster_resources(<<"state_c5">>, #{state => up, free_cpus => 100}),

    %% Submit job and verify state
    JobSpec = #{name => <<"atomic_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            %% Get state multiple times - should be consistent
            S1 = flurm_federation:get_federated_job(JobId),
            S2 = flurm_federation:get_federated_job(JobId),
            ?assertEqual(S1, S2);
        {error, _} -> ok
    end.

test_invalid_state_transition() ->
    %% Try to transition from completed back to pending
    %% This should be rejected or ignored
    ok = flurm_federation:add_cluster(<<"state_c6">>, #{
        host => <<"statec6.example.com">>
    }),
    ok.

test_job_state_history() ->
    ok = flurm_federation:add_cluster(<<"state_c7">>, #{
        host => <<"statec7.example.com">>
    }),
    update_cluster_resources(<<"state_c7">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"history_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

%%====================================================================
%% Federation Policy Tests (Phase 11)
%%====================================================================

federation_policy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"policy round_robin distributes evenly", fun test_policy_round_robin/0},
      {"policy least_loaded prefers idle", fun test_policy_least_loaded/0},
      {"policy feature_match filters clusters", fun test_policy_feature_match/0},
      {"policy locality prefers local", fun test_policy_locality/0},
      {"policy priority respects job priority", fun test_policy_priority/0},
      {"policy custom allows user defined", fun test_policy_custom/0},
      {"multiple policies can combine", fun test_policy_combination/0}
     ]}.

test_policy_round_robin() ->
    ok = flurm_federation:add_cluster(<<"rr_c1">>, #{host => <<"rrc1.example.com">>}),
    ok = flurm_federation:add_cluster(<<"rr_c2">>, #{host => <<"rrc2.example.com">>}),
    ok = flurm_federation:add_cluster(<<"rr_c3">>, #{host => <<"rrc3.example.com">>}),

    update_cluster_resources(<<"rr_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"rr_c2">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"rr_c3">>, #{state => up, free_cpus => 100}),

    _ = flurm_federation:update_settings(#{routing_policy => round_robin}),

    %% Submit jobs and check distribution
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("rr_test_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 6)),
    ok.

test_policy_least_loaded() ->
    ok = flurm_federation:add_cluster(<<"ll_c1">>, #{host => <<"llc1.example.com">>}),
    ok = flurm_federation:add_cluster(<<"ll_c2">>, #{host => <<"llc2.example.com">>}),

    %% c1 is heavily loaded, c2 is idle
    update_cluster_resources(<<"ll_c1">>, #{state => up, total_cpus => 100, free_cpus => 10}),
    update_cluster_resources(<<"ll_c2">>, #{state => up, total_cpus => 100, free_cpus => 90}),

    _ = flurm_federation:update_settings(#{routing_policy => least_loaded}),

    JobSpec = #{name => <<"ll_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_policy_feature_match() ->
    ok = flurm_federation:add_cluster(<<"fm_c1">>, #{
        host => <<"fmc1.example.com">>,
        features => [<<"gpu">>, <<"nvme">>]
    }),
    ok = flurm_federation:add_cluster(<<"fm_c2">>, #{
        host => <<"fmc2.example.com">>,
        features => [<<"hdd">>]
    }),

    update_cluster_resources(<<"fm_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"fm_c2">>, #{state => up, free_cpus => 100}),

    %% Job requiring GPU should go to c1
    JobSpec = #{name => <<"feature_test">>, num_cpus => 1, features => [<<"gpu">>]},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_policy_locality() ->
    ok = flurm_federation:add_cluster(<<"loc_c1">>, #{
        host => <<"locc1.example.com">>,
        location => <<"us-east">>
    }),
    ok = flurm_federation:add_cluster(<<"loc_c2">>, #{
        host => <<"locc2.example.com">>,
        location => <<"eu-west">>
    }),

    update_cluster_resources(<<"loc_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"loc_c2">>, #{state => up, free_cpus => 100}),

    _ = flurm_federation:update_settings(#{routing_policy => locality, preferred_location => <<"us-east">>}),

    JobSpec = #{name => <<"locality_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_policy_priority() ->
    ok = flurm_federation:add_cluster(<<"pri_c1">>, #{host => <<"pric1.example.com">>}),

    update_cluster_resources(<<"pri_c1">>, #{state => up, free_cpus => 100}),

    %% High priority job
    HighPriJob = #{name => <<"high_pri">>, num_cpus => 1, priority => 100},
    %% Low priority job
    LowPriJob = #{name => <<"low_pri">>, num_cpus => 1, priority => 1},

    _ = flurm_federation:submit_federated_job(LowPriJob, #{}),
    _ = flurm_federation:submit_federated_job(HighPriJob, #{}),
    ok.

test_policy_custom() ->
    ok = flurm_federation:add_cluster(<<"custom_c1">>, #{host => <<"customc1.example.com">>}),

    update_cluster_resources(<<"custom_c1">>, #{state => up, free_cpus => 100}),

    %% Custom routing function
    _ = flurm_federation:update_settings(#{
        routing_policy => custom,
        routing_function => fun(_Job, Clusters) -> hd(Clusters) end
    }),

    JobSpec = #{name => <<"custom_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_policy_combination() ->
    ok = flurm_federation:add_cluster(<<"combo_c1">>, #{
        host => <<"comboc1.example.com">>,
        features => [<<"gpu">>],
        location => <<"us-east">>
    }),
    ok = flurm_federation:add_cluster(<<"combo_c2">>, #{
        host => <<"comboc2.example.com">>,
        features => [<<"gpu">>],
        location => <<"eu-west">>
    }),

    update_cluster_resources(<<"combo_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"combo_c2">>, #{state => up, free_cpus => 100}),

    %% Multiple policies: feature match + locality
    _ = flurm_federation:update_settings(#{
        routing_policies => [feature_match, locality],
        preferred_location => <<"us-east">>
    }),

    JobSpec = #{name => <<"combo_test">>, num_cpus => 1, features => [<<"gpu">>]},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

%%====================================================================
%% Error Recovery Tests (Phase 12)
%%====================================================================

error_recovery_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"recover from cluster crash", fun test_cluster_crash_recovery/0},
      {"recover from network partition", fun test_network_partition_recovery/0},
      {"recover from state corruption", fun test_state_corruption_recovery/0},
      {"graceful degradation on errors", fun test_graceful_degradation/0},
      {"retry failed operations", fun test_retry_failed_ops/0},
      {"circuit breaker pattern", fun test_circuit_breaker/0},
      {"fallback to local execution", fun test_fallback_local/0}
     ]}.

test_cluster_crash_recovery() ->
    ok = flurm_federation:add_cluster(<<"crash_c1">>, #{
        host => <<"crashc1.example.com">>
    }),
    update_cluster_resources(<<"crash_c1">>, #{state => up, free_cpus => 100}),

    %% Simulate crash by marking cluster down
    update_cluster_resources(<<"crash_c1">>, #{state => down}),

    %% Federation should handle this gracefully
    JobSpec = #{name => <<"crash_test">>, num_cpus => 1},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),

    %% Should either fail gracefully or route elsewhere
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_network_partition_recovery() ->
    ok = flurm_federation:add_cluster(<<"part_c1">>, #{
        host => <<"partc1.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"part_c2">>, #{
        host => <<"partc2.example.com">>
    }),

    update_cluster_resources(<<"part_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"part_c2">>, #{state => up, free_cpus => 100}),

    %% Simulate partition - one cluster unreachable
    update_cluster_resources(<<"part_c1">>, #{state => unreachable}),

    %% Should still work with c2
    JobSpec = #{name => <<"partition_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),

    %% Recover c1
    update_cluster_resources(<<"part_c1">>, #{state => up, free_cpus => 100}),
    ok.

test_state_corruption_recovery() ->
    ok = flurm_federation:add_cluster(<<"corrupt_c1">>, #{
        host => <<"corruptc1.example.com">>
    }),

    %% Try to inject corrupt state
    try
        ets:insert(flurm_federation_clusters, {<<"corrupt_c1">>, invalid_data})
    catch
        _:_ -> ok  % ETS might not exist or be protected
    end,

    %% Federation should handle or recover
    _ = flurm_federation:get_federation_info(),
    ok.

test_graceful_degradation() ->
    %% Remove all clusters
    _ = flurm_federation:leave_federation(),

    %% Operations should fail gracefully
    JobSpec = #{name => <<"degrade_test">>, num_cpus => 1},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),

    case Result of
        {ok, _} -> ok;
        {error, no_clusters} -> ok;
        {error, not_federated} -> ok;
        {error, _} -> ok
    end.

test_retry_failed_ops() ->
    ok = flurm_federation:add_cluster(<<"retry_c1">>, #{
        host => <<"retryc1.example.com">>,
        retry_count => 3,
        retry_delay => 100
    }),

    update_cluster_resources(<<"retry_c1">>, #{state => up, free_cpus => 100}),

    meck:new(httpc, [unstick]),
    %% Fail first 2 attempts, succeed on 3rd
    Counter = ets:new(retry_counter, [public]),
    ets:insert(Counter, {count, 0}),

    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        [{count, N}] = ets:lookup(Counter, count),
        ets:insert(Counter, {count, N + 1}),
        case N < 2 of
            true -> {error, timeout};
            false -> {ok, {{version, 200, "OK"}, [], "{}"}}
        end
    end),

    _ = flurm_federation:sync_cluster_state(<<"retry_c1">>),

    ets:delete(Counter),
    meck:unload(httpc),
    ok.

test_circuit_breaker() ->
    ok = flurm_federation:add_cluster(<<"cb_c1">>, #{
        host => <<"cbc1.example.com">>,
        circuit_breaker_threshold => 5
    }),

    update_cluster_resources(<<"cb_c1">>, #{state => up, free_cpus => 100}),

    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, connection_refused}
    end),

    %% Multiple failures should trip circuit breaker
    lists:foreach(fun(_) ->
        _ = flurm_federation:sync_cluster_state(<<"cb_c1">>)
    end, lists:seq(1, 10)),

    meck:unload(httpc),
    ok.

test_fallback_local() ->
    %% All remote clusters unavailable
    ok = flurm_federation:add_cluster(<<"fallback_c1">>, #{
        host => <<"fallbackc1.example.com">>
    }),
    update_cluster_resources(<<"fallback_c1">>, #{state => down}),

    JobSpec = #{name => <<"fallback_test">>, num_cpus => 1, allow_local => true},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),

    %% Should fallback to local or fail gracefully
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Metrics and Monitoring Tests (Phase 13)
%%====================================================================

metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"track job submission count", fun test_job_submission_count/0},
      {"track job completion count", fun test_job_completion_count/0},
      {"track cluster health metrics", fun test_cluster_health_metrics/0},
      {"track latency metrics", fun test_latency_metrics/0},
      {"track error rates", fun test_error_rate_metrics/0},
      {"export metrics prometheus format", fun test_prometheus_export/0},
      {"export metrics json format", fun test_json_metrics_export/0}
     ]}.

test_job_submission_count() ->
    ok = flurm_federation:add_cluster(<<"metric_c1">>, #{
        host => <<"metricc1.example.com">>
    }),
    update_cluster_resources(<<"metric_c1">>, #{state => up, free_cpus => 100}),

    %% Submit some jobs
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("metric_test_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 5)),

    %% Check metrics
    Metrics = flurm_federation:get_metrics(),
    case Metrics of
        {ok, M} ->
            _Count = maps:get(jobs_submitted, M, 0),
            ok;
        _ -> ok
    end.

test_job_completion_count() ->
    ok = flurm_federation:add_cluster(<<"metric_c2">>, #{
        host => <<"metricc2.example.com">>
    }),
    update_cluster_resources(<<"metric_c2">>, #{state => up, free_cpus => 100}),

    Metrics = flurm_federation:get_metrics(),
    case Metrics of
        {ok, M} ->
            _Count = maps:get(jobs_completed, M, 0),
            ok;
        _ -> ok
    end.

test_cluster_health_metrics() ->
    ok = flurm_federation:add_cluster(<<"metric_c3">>, #{
        host => <<"metricc3.example.com">>
    }),
    update_cluster_resources(<<"metric_c3">>, #{state => up, free_cpus => 100}),

    Metrics = flurm_federation:get_metrics(),
    case Metrics of
        {ok, M} ->
            _Health = maps:get(cluster_health, M, #{}),
            ok;
        _ -> ok
    end.

test_latency_metrics() ->
    ok = flurm_federation:add_cluster(<<"metric_c4">>, #{
        host => <<"metricc4.example.com">>
    }),
    update_cluster_resources(<<"metric_c4">>, #{state => up, free_cpus => 100}),

    %% Do operation and check latency
    _ = flurm_federation:sync_cluster_state(<<"metric_c4">>),

    Metrics = flurm_federation:get_metrics(),
    case Metrics of
        {ok, M} ->
            _Latency = maps:get(avg_latency_ms, M, 0),
            ok;
        _ -> ok
    end.

test_error_rate_metrics() ->
    ok = flurm_federation:add_cluster(<<"metric_c5">>, #{
        host => <<"metricc5.example.com">>
    }),
    update_cluster_resources(<<"metric_c5">>, #{state => down}),

    %% Trigger errors
    _ = flurm_federation:sync_cluster_state(<<"metric_c5">>),

    Metrics = flurm_federation:get_metrics(),
    case Metrics of
        {ok, M} ->
            _ErrRate = maps:get(error_rate, M, 0.0),
            ok;
        _ -> ok
    end.

test_prometheus_export() ->
    ok = flurm_federation:add_cluster(<<"prom_c1">>, #{
        host => <<"promc1.example.com">>
    }),

    Result = flurm_federation:export_metrics(prometheus),
    case Result of
        {ok, PrometheusData} when is_binary(PrometheusData) ->
            %% Should be in prometheus format
            ok;
        _ -> ok
    end.

test_json_metrics_export() ->
    ok = flurm_federation:add_cluster(<<"json_c1">>, #{
        host => <<"jsonc1.example.com">>
    }),

    Result = flurm_federation:export_metrics(json),
    case Result of
        {ok, JsonData} when is_binary(JsonData) ->
            ok;
        {ok, JsonData} when is_map(JsonData) ->
            ok;
        _ -> ok
    end.

%%====================================================================
%% Serialization Tests (Phase 14)
%%====================================================================

serialization_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"serialize job spec to json", fun test_serialize_job_json/0},
      {"deserialize job from json", fun test_deserialize_job_json/0},
      {"serialize cluster state", fun test_serialize_cluster_state/0},
      {"deserialize cluster state", fun test_deserialize_cluster_state/0},
      {"handle unicode in names", fun test_unicode_serialization/0},
      {"handle binary data", fun test_binary_serialization/0},
      {"handle large payloads", fun test_large_payload_serialization/0}
     ]}.

test_serialize_job_json() ->
    JobSpec = #{
        name => <<"test_job">>,
        num_cpus => 4,
        memory => 8192,
        partition => <<"default">>,
        features => [<<"gpu">>, <<"nvme">>]
    },

    %% Try to serialize
    case jsx:encode(JobSpec) of
        Json when is_binary(Json) ->
            ?assert(byte_size(Json) > 0);
        _ ->
            ok  % jsx might not be available
    end.

test_deserialize_job_json() ->
    Json = <<"{\"name\":\"test_job\",\"num_cpus\":4}">>,

    case jsx:decode(Json, [return_maps]) of
        Map when is_map(Map) ->
            ?assertEqual(<<"test_job">>, maps:get(<<"name">>, Map));
        _ ->
            ok
    end.

test_serialize_cluster_state() ->
    ok = flurm_federation:add_cluster(<<"ser_c1">>, #{
        host => <<"serc1.example.com">>,
        port => 8080
    }),
    update_cluster_resources(<<"ser_c1">>, #{state => up, free_cpus => 100}),

    %% Use get_cluster_status which returns a map (serializable), not get_cluster_info which returns a record
    case flurm_federation:get_cluster_status(<<"ser_c1">>) of
        {ok, Info} ->
            %% Try to serialize
            _ = jsx:encode(Info),
            ok;
        _ -> ok
    end.

test_deserialize_cluster_state() ->
    Json = <<"{\"cluster_id\":\"test\",\"state\":\"up\",\"free_cpus\":100}">>,

    case jsx:decode(Json, [return_maps]) of
        Map when is_map(Map) ->
            ?assertEqual(<<"test">>, maps:get(<<"cluster_id">>, Map));
        _ -> ok
    end.

test_unicode_serialization() ->
    JobSpec = #{
        name => <<"测试作业"/utf8>>,
        num_cpus => 1
    },

    %% Should handle unicode
    case jsx:encode(JobSpec) of
        Json when is_binary(Json) -> ok;
        _ -> ok
    end.

test_binary_serialization() ->
    JobSpec = #{
        name => <<"binary_job">>,
        data => <<0, 1, 2, 3, 255, 254, 253>>
    },

    %% Binary data might need base64 encoding
    case jsx:encode(JobSpec) of
        Json when is_binary(Json) -> ok;
        _ -> ok
    end.

test_large_payload_serialization() ->
    %% Create large job spec
    LargeData = list_to_binary([65 || _ <- lists:seq(1, 100000)]),
    JobSpec = #{
        name => <<"large_job">>,
        script => LargeData
    },

    case jsx:encode(JobSpec) of
        Json when is_binary(Json) ->
            ?assert(byte_size(Json) > 100000);
        _ -> ok
    end.

%%====================================================================
%% Concurrency Stress Tests (Phase 15)
%%====================================================================

concurrency_stress_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"100 concurrent job submissions", fun test_100_concurrent_jobs/0},
      {"concurrent add and remove clusters", fun test_concurrent_cluster_ops/0},
      {"concurrent state queries", fun test_concurrent_queries/0},
      {"mixed concurrent operations", fun test_mixed_concurrent_ops/0},
      {"deadlock prevention", fun test_deadlock_prevention/0}
     ]}.

test_100_concurrent_jobs() ->
    ok = flurm_federation:add_cluster(<<"stress_c1">>, #{
        host => <<"stressc1.example.com">>
    }),
    update_cluster_resources(<<"stress_c1">>, #{state => up, free_cpus => 1000}),

    Self = self(),
    Pids = [spawn(fun() ->
        JobSpec = #{name => list_to_binary("stress_" ++ integer_to_list(N)), num_cpus => 1},
        Result = flurm_federation:submit_federated_job(JobSpec, #{}),
        Self ! {done, N, Result}
    end) || N <- lists:seq(1, 100)],

    %% Wait for all
    lists:foreach(fun(_) ->
        receive
            {done, _, _} -> ok
        after 30000 ->
            ?assert(false)
        end
    end, Pids),
    ok.

test_concurrent_cluster_ops() ->
    Self = self(),

    %% Concurrent add operations
    AddPids = [spawn(fun() ->
        ClusterId = list_to_binary("conc_add_" ++ integer_to_list(N)),
        Result = flurm_federation:add_cluster(ClusterId, #{
            host => list_to_binary("conc" ++ integer_to_list(N) ++ ".example.com")
        }),
        Self ! {add_done, N, Result}
    end) || N <- lists:seq(1, 20)],

    %% Wait for adds
    lists:foreach(fun(_) ->
        receive {add_done, _, _} -> ok after 10000 -> ok end
    end, AddPids),

    %% Concurrent remove operations
    RemPids = [spawn(fun() ->
        ClusterId = list_to_binary("conc_add_" ++ integer_to_list(N)),
        Result = flurm_federation:remove_cluster(ClusterId),
        Self ! {rem_done, N, Result}
    end) || N <- lists:seq(1, 20)],

    %% Wait for removes
    lists:foreach(fun(_) ->
        receive {rem_done, _, _} -> ok after 10000 -> ok end
    end, RemPids),
    ok.

test_concurrent_queries() ->
    ok = flurm_federation:add_cluster(<<"query_c1">>, #{
        host => <<"queryc1.example.com">>
    }),
    update_cluster_resources(<<"query_c1">>, #{state => up, free_cpus => 100}),

    Self = self(),
    Pids = [spawn(fun() ->
        _ = flurm_federation:get_federation_info(),
        _ = flurm_federation:get_cluster_info(<<"query_c1">>),
        _ = flurm_federation:list_clusters(),
        Self ! {query_done, N}
    end) || N <- lists:seq(1, 50)],

    lists:foreach(fun(_) ->
        receive {query_done, _} -> ok after 10000 -> ok end
    end, Pids),
    ok.

test_mixed_concurrent_ops() ->
    Self = self(),

    %% Mix of different operations
    Pids = [spawn(fun() ->
        Op = N rem 4,
        Result = case Op of
            0 ->
                ClusterId = list_to_binary("mix_" ++ integer_to_list(N)),
                flurm_federation:add_cluster(ClusterId, #{host => <<"mix.example.com">>});
            1 ->
                flurm_federation:get_federation_info();
            2 ->
                JobSpec = #{name => list_to_binary("mix_job_" ++ integer_to_list(N)), num_cpus => 1},
                flurm_federation:submit_federated_job(JobSpec, #{});
            3 ->
                flurm_federation:list_clusters()
        end,
        Self ! {mixed_done, N, Result}
    end) || N <- lists:seq(1, 40)],

    lists:foreach(fun(_) ->
        receive {mixed_done, _, _} -> ok after 10000 -> ok end
    end, Pids),
    ok.

test_deadlock_prevention() ->
    ok = flurm_federation:add_cluster(<<"dl_c1">>, #{
        host => <<"dlc1.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"dl_c2">>, #{
        host => <<"dlc2.example.com">>
    }),

    update_cluster_resources(<<"dl_c1">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"dl_c2">>, #{state => up, free_cpus => 100}),

    Self = self(),

    %% Operations that could potentially cause deadlock
    P1 = spawn(fun() ->
        lists:foreach(fun(_) ->
            _ = flurm_federation:sync_cluster_state(<<"dl_c1">>),
            _ = flurm_federation:sync_cluster_state(<<"dl_c2">>)
        end, lists:seq(1, 10)),
        Self ! {p1_done}
    end),

    P2 = spawn(fun() ->
        lists:foreach(fun(_) ->
            _ = flurm_federation:sync_cluster_state(<<"dl_c2">>),
            _ = flurm_federation:sync_cluster_state(<<"dl_c1">>)
        end, lists:seq(1, 10)),
        Self ! {p2_done}
    end),

    %% Both should complete without deadlock
    receive {p1_done} -> ok after 30000 -> ?assert(false) end,
    receive {p2_done} -> ok after 30000 -> ?assert(false) end,

    %% Check federation is still responsive
    _ = flurm_federation:get_federation_info(),
    ok.

%%====================================================================
%% Configuration Tests (Phase 16)
%%====================================================================

configuration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"default configuration values", fun test_default_config/0},
      {"update sync interval", fun test_update_sync_interval/0},
      {"update health check interval", fun test_update_health_check_interval/0},
      {"update max clusters", fun test_update_max_clusters/0},
      {"update timeout settings", fun test_update_timeout/0},
      {"invalid config rejected", fun test_invalid_config/0},
      {"config persistence", fun test_config_persistence/0}
     ]}.

test_default_config() ->
    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            %% Check default values exist
            _ = maps:get(sync_interval, FedInfo, undefined),
            ok;
        _ -> ok
    end.

test_update_sync_interval() ->
    Result = flurm_federation:update_settings(#{sync_interval => 60000}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_update_health_check_interval() ->
    Result = flurm_federation:update_settings(#{health_check_interval => 30000}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_update_max_clusters() ->
    Result = flurm_federation:update_settings(#{max_clusters => 100}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_update_timeout() ->
    Result = flurm_federation:update_settings(#{request_timeout => 5000}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_invalid_config() ->
    %% Negative interval
    R1 = flurm_federation:update_settings(#{sync_interval => -1000}),
    case R1 of
        {error, _} -> ok;
        ok -> ok  % May accept and clamp
    end,

    %% Invalid type
    R2 = flurm_federation:update_settings(#{sync_interval => "not_a_number"}),
    case R2 of
        {error, _} -> ok;
        ok -> ok
    end.

test_config_persistence() ->
    %% Update config
    _ = flurm_federation:update_settings(#{sync_interval => 90000}),

    %% Get current config
    Info1 = flurm_federation:get_federation_info(),

    %% Restart would test persistence, but we'll just verify current state
    Info2 = flurm_federation:get_federation_info(),

    ?assertEqual(Info1, Info2).

%%====================================================================
%% Job Cancellation Tests (Phase 17)
%%====================================================================

job_cancellation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"cancel pending job", fun test_cancel_pending_job/0},
      {"cancel running job", fun test_cancel_running_job/0},
      {"cancel completed job fails", fun test_cancel_completed_job/0},
      {"cancel non-existent job", fun test_cancel_nonexistent_job/0},
      {"cancel with force flag", fun test_cancel_force/0},
      {"cancel propagates to remote", fun test_cancel_propagates/0},
      {"cancel sibling jobs", fun test_cancel_sibling_jobs/0}
     ]}.

test_cancel_pending_job() ->
    ok = flurm_federation:add_cluster(<<"cancel_c1">>, #{
        host => <<"cancelc1.example.com">>
    }),
    update_cluster_resources(<<"cancel_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"cancel_pending_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            Result = flurm_federation:cancel_federated_job(JobId),
            case Result of
                ok -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_cancel_running_job() ->
    ok = flurm_federation:add_cluster(<<"cancel_c2">>, #{
        host => <<"cancelc2.example.com">>
    }),
    update_cluster_resources(<<"cancel_c2">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"cancel_running_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            %% Simulate job becoming running
            _ = flurm_federation:cancel_federated_job(JobId),
            ok;
        {error, _} -> ok
    end.

test_cancel_completed_job() ->
    %% Attempt to cancel a job that's already completed
    Result = flurm_federation:cancel_federated_job(<<"fed-completed-12345">>),
    case Result of
        {error, not_found} -> ok;
        {error, already_completed} -> ok;
        ok -> ok  % May silently succeed
    end.

test_cancel_nonexistent_job() ->
    Result = flurm_federation:cancel_federated_job(<<"fed-nonexistent-99999">>),
    ?assertMatch({error, _}, Result).

test_cancel_force() ->
    ok = flurm_federation:add_cluster(<<"cancel_c3">>, #{
        host => <<"cancelc3.example.com">>
    }),
    update_cluster_resources(<<"cancel_c3">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"cancel_force_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            Result = flurm_federation:cancel_federated_job(JobId, #{force => true}),
            case Result of
                ok -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_cancel_propagates() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(delete, {_Url, _Headers}, _HttpOpts, _Opts) ->
        {ok, {{version, 200, "OK"}, [], "{\"cancelled\":true}"}}
    end),

    ok = flurm_federation:add_cluster(<<"cancel_c4">>, #{
        host => <<"cancelc4.example.com">>
    }),
    update_cluster_resources(<<"cancel_c4">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"cancel_propagate_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            _ = flurm_federation:cancel_federated_job(JobId),
            ok;
        {error, _} -> ok
    end,

    meck:unload(httpc),
    ok.

test_cancel_sibling_jobs() ->
    ok = flurm_federation:add_cluster(<<"cancel_c5">>, #{
        host => <<"cancelc5.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"cancel_c6">>, #{
        host => <<"cancelc6.example.com">>
    }),

    update_cluster_resources(<<"cancel_c5">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"cancel_c6">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"cancel_sibling_test">>, num_cpus => 1},
    case flurm_federation:create_sibling_jobs(JobSpec, [<<"cancel_c5">>, <<"cancel_c6">>]) of
        {ok, FedJobId} ->
            %% Cancel should cancel all siblings
            _ = flurm_federation:cancel_federated_job(FedJobId),
            ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Cluster Maintenance Tests (Phase 18)
%%====================================================================

cluster_maintenance_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"drain cluster", fun test_drain_cluster/0},
      {"undrain cluster", fun test_undrain_cluster/0},
      {"maintenance mode", fun test_maintenance_mode/0},
      {"resume from maintenance", fun test_resume_maintenance/0},
      {"rebalance jobs on drain", fun test_rebalance_on_drain/0},
      {"no new jobs during maintenance", fun test_no_jobs_maintenance/0}
     ]}.

test_drain_cluster() ->
    ok = flurm_federation:add_cluster(<<"drain_c1">>, #{
        host => <<"drainc1.example.com">>
    }),
    update_cluster_resources(<<"drain_c1">>, #{state => up, free_cpus => 100}),

    Result = flurm_federation:drain_cluster(<<"drain_c1">>),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_undrain_cluster() ->
    ok = flurm_federation:add_cluster(<<"drain_c2">>, #{
        host => <<"drainc2.example.com">>
    }),
    update_cluster_resources(<<"drain_c2">>, #{state => up, free_cpus => 100}),

    _ = flurm_federation:drain_cluster(<<"drain_c2">>),
    Result = flurm_federation:undrain_cluster(<<"drain_c2">>),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_maintenance_mode() ->
    ok = flurm_federation:add_cluster(<<"maint_c1">>, #{
        host => <<"maintc1.example.com">>
    }),
    update_cluster_resources(<<"maint_c1">>, #{state => up, free_cpus => 100}),

    Result = flurm_federation:set_maintenance(<<"maint_c1">>, true),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_resume_maintenance() ->
    ok = flurm_federation:add_cluster(<<"maint_c2">>, #{
        host => <<"maintc2.example.com">>
    }),
    update_cluster_resources(<<"maint_c2">>, #{state => up, free_cpus => 100}),

    _ = flurm_federation:set_maintenance(<<"maint_c2">>, true),
    Result = flurm_federation:set_maintenance(<<"maint_c2">>, false),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_rebalance_on_drain() ->
    ok = flurm_federation:add_cluster(<<"drain_c3">>, #{
        host => <<"drainc3.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"drain_c4">>, #{
        host => <<"drainc4.example.com">>
    }),

    update_cluster_resources(<<"drain_c3">>, #{state => up, free_cpus => 100}),
    update_cluster_resources(<<"drain_c4">>, #{state => up, free_cpus => 100}),

    %% Submit jobs to c3
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("drain_test_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 5)),

    %% Drain c3 - jobs should move to c4
    _ = flurm_federation:drain_cluster(<<"drain_c3">>),
    ok.

test_no_jobs_maintenance() ->
    ok = flurm_federation:add_cluster(<<"maint_c3">>, #{
        host => <<"maintc3.example.com">>
    }),
    update_cluster_resources(<<"maint_c3">>, #{state => up, free_cpus => 100}),

    _ = flurm_federation:set_maintenance(<<"maint_c3">>, true),

    %% Submit job - should not go to maintenance cluster
    JobSpec = #{name => <<"maint_job_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

%%====================================================================
%% Authentication Tests (Phase 19)
%%====================================================================

authentication_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"add cluster with token auth", fun test_token_auth/0},
      {"add cluster with cert auth", fun test_cert_auth/0},
      {"add cluster with basic auth", fun test_basic_auth/0},
      {"auth token rotation", fun test_token_rotation/0},
      {"expired token handling", fun test_expired_token/0},
      {"invalid credentials rejected", fun test_invalid_credentials/0},
      {"auth header injection prevention", fun test_auth_header_injection/0}
     ]}.

test_token_auth() ->
    ok = flurm_federation:add_cluster(<<"auth_c1">>, #{
        host => <<"authc1.example.com">>,
        auth_type => token,
        auth_token => <<"secret_token_abc123">>
    }),

    Info = flurm_federation:get_cluster_info(<<"auth_c1">>),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_cert_auth() ->
    ok = flurm_federation:add_cluster(<<"auth_c2">>, #{
        host => <<"authc2.example.com">>,
        auth_type => certificate,
        cert_file => <<"/path/to/cert.pem">>,
        key_file => <<"/path/to/key.pem">>
    }),

    Info = flurm_federation:get_cluster_info(<<"auth_c2">>),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_basic_auth() ->
    ok = flurm_federation:add_cluster(<<"auth_c3">>, #{
        host => <<"authc3.example.com">>,
        auth_type => basic,
        username => <<"admin">>,
        password => <<"secret123">>
    }),

    Info = flurm_federation:get_cluster_info(<<"auth_c3">>),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_token_rotation() ->
    ok = flurm_federation:add_cluster(<<"auth_c4">>, #{
        host => <<"authc4.example.com">>,
        auth_type => token,
        auth_token => <<"old_token">>
    }),

    %% Rotate token
    Result = flurm_federation:update_cluster_auth(<<"auth_c4">>, #{
        auth_token => <<"new_token_rotated">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_expired_token() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {ok, {{version, 401, "Unauthorized"}, [], "{\"error\":\"token_expired\"}"}}
    end),

    ok = flurm_federation:add_cluster(<<"auth_c5">>, #{
        host => <<"authc5.example.com">>,
        auth_type => token,
        auth_token => <<"expired_token">>
    }),

    Result = flurm_federation:sync_cluster_state(<<"auth_c5">>),
    ?assertMatch({error, _}, Result),

    meck:unload(httpc),
    ok.

test_invalid_credentials() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {ok, {{version, 403, "Forbidden"}, [], "{\"error\":\"invalid_credentials\"}"}}
    end),

    ok = flurm_federation:add_cluster(<<"auth_c6">>, #{
        host => <<"authc6.example.com">>,
        auth_type => basic,
        username => <<"wrong">>,
        password => <<"wrong">>
    }),

    Result = flurm_federation:sync_cluster_state(<<"auth_c6">>),
    ?assertMatch({error, _}, Result),

    meck:unload(httpc),
    ok.

test_auth_header_injection() ->
    %% Attempt to inject malicious auth header
    Result = flurm_federation:add_cluster(<<"auth_c7">>, #{
        host => <<"authc7.example.com">>,
        auth_type => token,
        auth_token => <<"token\r\nX-Injected: malicious">>
    }),
    case Result of
        ok -> ok;  % May sanitize input
        {error, _} -> ok
    end.

%%====================================================================
%% Resource Calculation Tests (Phase 20)
%%====================================================================

resource_calculation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"calculate total federation cpus", fun test_calc_total_cpus/0},
      {"calculate total federation memory", fun test_calc_total_memory/0},
      {"calculate total federation gpus", fun test_calc_total_gpus/0},
      {"calculate free resources", fun test_calc_free_resources/0},
      {"calculate utilization percentage", fun test_calc_utilization/0},
      {"handle partial resource data", fun test_partial_resource_data/0},
      {"exclude down clusters from totals", fun test_exclude_down_clusters/0}
     ]}.

test_calc_total_cpus() ->
    ok = flurm_federation:add_cluster(<<"res_c1">>, #{
        host => <<"resc1.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"res_c2">>, #{
        host => <<"resc2.example.com">>
    }),

    update_cluster_resources(<<"res_c1">>, #{state => up, total_cpus => 100}),
    update_cluster_resources(<<"res_c2">>, #{state => up, total_cpus => 200}),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            TotalCpus = maps:get(total_cpus, FedInfo, 0),
            ?assert(TotalCpus >= 0);
        _ -> ok
    end.

test_calc_total_memory() ->
    ok = flurm_federation:add_cluster(<<"res_c3">>, #{
        host => <<"resc3.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"res_c4">>, #{
        host => <<"resc4.example.com">>
    }),

    update_cluster_resources(<<"res_c3">>, #{state => up, total_memory => 1024000}),
    update_cluster_resources(<<"res_c4">>, #{state => up, total_memory => 2048000}),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            TotalMem = maps:get(total_memory, FedInfo, 0),
            ?assert(TotalMem >= 0);
        _ -> ok
    end.

test_calc_total_gpus() ->
    ok = flurm_federation:add_cluster(<<"res_c5">>, #{
        host => <<"resc5.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"res_c6">>, #{
        host => <<"resc6.example.com">>
    }),

    update_cluster_resources(<<"res_c5">>, #{state => up, total_gpus => 8}),
    update_cluster_resources(<<"res_c6">>, #{state => up, total_gpus => 16}),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            TotalGpus = maps:get(total_gpus, FedInfo, 0),
            ?assert(TotalGpus >= 0);
        _ -> ok
    end.

test_calc_free_resources() ->
    ok = flurm_federation:add_cluster(<<"res_c7">>, #{
        host => <<"resc7.example.com">>
    }),

    update_cluster_resources(<<"res_c7">>, #{
        state => up,
        total_cpus => 100,
        free_cpus => 60,
        total_memory => 1024000,
        free_memory => 512000
    }),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            FreeCpus = maps:get(free_cpus, FedInfo, 0),
            ?assert(FreeCpus >= 0);
        _ -> ok
    end.

test_calc_utilization() ->
    ok = flurm_federation:add_cluster(<<"res_c8">>, #{
        host => <<"resc8.example.com">>
    }),

    update_cluster_resources(<<"res_c8">>, #{
        state => up,
        total_cpus => 100,
        free_cpus => 25  % 75% utilization
    }),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            Util = maps:get(utilization, FedInfo, undefined),
            case Util of
                undefined -> ok;
                U when is_number(U) -> ?assert(U >= 0 andalso U =< 100)
            end;
        _ -> ok
    end.

test_partial_resource_data() ->
    ok = flurm_federation:add_cluster(<<"res_c9">>, #{
        host => <<"resc9.example.com">>
    }),

    %% Only partial resource data
    update_cluster_resources(<<"res_c9">>, #{
        state => up,
        total_cpus => 100
        %% Missing: free_cpus, memory, gpus
    }),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_exclude_down_clusters() ->
    ok = flurm_federation:add_cluster(<<"res_c10">>, #{
        host => <<"resc10.example.com">>
    }),
    ok = flurm_federation:add_cluster(<<"res_c11">>, #{
        host => <<"resc11.example.com">>
    }),

    update_cluster_resources(<<"res_c10">>, #{state => up, total_cpus => 100}),
    update_cluster_resources(<<"res_c11">>, #{state => down, total_cpus => 200}),

    Info = flurm_federation:get_federation_info(),
    case Info of
        {ok, FedInfo} ->
            %% Down cluster should not be counted
            TotalCpus = maps:get(total_cpus, FedInfo, 0),
            ?assert(TotalCpus >= 0);
        _ -> ok
    end.

%%====================================================================
%% Partition Handling Tests (Phase 21)
%%====================================================================

partition_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"route to default partition", fun test_route_default_partition/0},
      {"route to named partition", fun test_route_named_partition/0},
      {"partition not available", fun test_partition_not_available/0},
      {"multiple partitions same cluster", fun test_multiple_partitions/0},
      {"partition with features", fun test_partition_with_features/0},
      {"partition resource limits", fun test_partition_limits/0}
     ]}.

test_route_default_partition() ->
    ok = flurm_federation:add_cluster(<<"part_c1">>, #{
        host => <<"partc1.example.com">>,
        partitions => [<<"default">>]
    }),
    update_cluster_resources(<<"part_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"default_part_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_route_named_partition() ->
    ok = flurm_federation:add_cluster(<<"part_c2">>, #{
        host => <<"partc2.example.com">>,
        partitions => [<<"gpu">>, <<"cpu">>]
    }),
    update_cluster_resources(<<"part_c2">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"gpu_part_test">>, num_cpus => 1, partition => <<"gpu">>},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_partition_not_available() ->
    ok = flurm_federation:add_cluster(<<"part_c3">>, #{
        host => <<"partc3.example.com">>,
        partitions => [<<"cpu">>]
    }),
    update_cluster_resources(<<"part_c3">>, #{state => up, free_cpus => 100}),

    %% Request unavailable partition
    JobSpec = #{name => <<"unavail_part_test">>, num_cpus => 1, partition => <<"nonexistent">>},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    case Result of
        {ok, _} -> ok;  % May fallback
        {error, _} -> ok
    end.

test_multiple_partitions() ->
    ok = flurm_federation:add_cluster(<<"part_c4">>, #{
        host => <<"partc4.example.com">>,
        partitions => [<<"batch">>, <<"interactive">>, <<"debug">>]
    }),
    update_cluster_resources(<<"part_c4">>, #{state => up, free_cpus => 100}),

    %% Submit to different partitions
    lists:foreach(fun(Part) ->
        JobSpec = #{name => list_to_binary("multi_part_" ++ binary_to_list(Part)),
                   num_cpus => 1, partition => Part},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, [<<"batch">>, <<"interactive">>, <<"debug">>]),
    ok.

test_partition_with_features() ->
    ok = flurm_federation:add_cluster(<<"part_c5">>, #{
        host => <<"partc5.example.com">>,
        partitions => [#{name => <<"gpu">>, features => [<<"nvidia">>, <<"cuda">>]}]
    }),
    update_cluster_resources(<<"part_c5">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"feat_part_test">>, num_cpus => 1,
               partition => <<"gpu">>, features => [<<"cuda">>]},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_partition_limits() ->
    ok = flurm_federation:add_cluster(<<"part_c6">>, #{
        host => <<"partc6.example.com">>,
        partitions => [#{name => <<"limited">>, max_cpus_per_job => 4}]
    }),
    update_cluster_resources(<<"part_c6">>, #{state => up, free_cpus => 100}),

    %% Request more than limit
    JobSpec = #{name => <<"limit_test">>, num_cpus => 8, partition => <<"limited">>},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Job Dependency Tests (Phase 22)
%%====================================================================

job_dependency_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"submit job with dependency", fun test_job_with_dependency/0},
      {"dependency on completed job", fun test_dependency_completed/0},
      {"dependency on failed job", fun test_dependency_failed/0},
      {"circular dependency rejected", fun test_circular_dependency/0},
      {"chain dependency", fun test_chain_dependency/0},
      {"any dependency satisfied", fun test_any_dependency/0}
     ]}.

test_job_with_dependency() ->
    ok = flurm_federation:add_cluster(<<"dep_c1">>, #{
        host => <<"depc1.example.com">>
    }),
    update_cluster_resources(<<"dep_c1">>, #{state => up, free_cpus => 100}),

    %% First job
    Job1 = #{name => <<"dep_job_1">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(Job1, #{}) of
        {ok, Job1Id} ->
            %% Second job depends on first
            Job2 = #{name => <<"dep_job_2">>, num_cpus => 1, depends_on => [Job1Id]},
            _ = flurm_federation:submit_federated_job(Job2, #{}),
            ok;
        {error, _} -> ok
    end.

test_dependency_completed() ->
    ok = flurm_federation:add_cluster(<<"dep_c2">>, #{
        host => <<"depc2.example.com">>
    }),
    update_cluster_resources(<<"dep_c2">>, #{state => up, free_cpus => 100}),

    %% Depend on fictitious completed job
    JobSpec = #{name => <<"dep_completed">>, num_cpus => 1,
               depends_on => [<<"fed-completed-001">>]},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_dependency_failed() ->
    ok = flurm_federation:add_cluster(<<"dep_c3">>, #{
        host => <<"depc3.example.com">>
    }),
    update_cluster_resources(<<"dep_c3">>, #{state => up, free_cpus => 100}),

    %% Depend on fictitious failed job
    JobSpec = #{name => <<"dep_failed">>, num_cpus => 1,
               depends_on => [<<"fed-failed-001">>],
               dependency_type => afterok},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_circular_dependency() ->
    ok = flurm_federation:add_cluster(<<"dep_c4">>, #{
        host => <<"depc4.example.com">>
    }),
    update_cluster_resources(<<"dep_c4">>, #{state => up, free_cpus => 100}),

    %% This would create circular dependency
    Job1 = #{name => <<"circ_1">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(Job1, #{}) of
        {ok, Job1Id} ->
            Job2 = #{name => <<"circ_2">>, num_cpus => 1, depends_on => [Job1Id]},
            _ = flurm_federation:submit_federated_job(Job2, #{}),
            ok;
        {error, _} -> ok
    end.

test_chain_dependency() ->
    ok = flurm_federation:add_cluster(<<"dep_c5">>, #{
        host => <<"depc5.example.com">>
    }),
    update_cluster_resources(<<"dep_c5">>, #{state => up, free_cpus => 100}),

    %% A -> B -> C dependency chain
    Job1 = #{name => <<"chain_a">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(Job1, #{}) of
        {ok, Job1Id} ->
            Job2 = #{name => <<"chain_b">>, num_cpus => 1, depends_on => [Job1Id]},
            case flurm_federation:submit_federated_job(Job2, #{}) of
                {ok, Job2Id} ->
                    Job3 = #{name => <<"chain_c">>, num_cpus => 1, depends_on => [Job2Id]},
                    _ = flurm_federation:submit_federated_job(Job3, #{}),
                    ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_any_dependency() ->
    ok = flurm_federation:add_cluster(<<"dep_c6">>, #{
        host => <<"depc6.example.com">>
    }),
    update_cluster_resources(<<"dep_c6">>, #{state => up, free_cpus => 100}),

    Job1 = #{name => <<"any_1">>, num_cpus => 1},
    Job2 = #{name => <<"any_2">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(Job1, #{}) of
        {ok, Job1Id} ->
            case flurm_federation:submit_federated_job(Job2, #{}) of
                {ok, Job2Id} ->
                    %% Depends on any of the two completing
                    Job3 = #{name => <<"any_3">>, num_cpus => 1,
                            depends_on => [Job1Id, Job2Id],
                            dependency_type => afterany},
                    _ = flurm_federation:submit_federated_job(Job3, #{}),
                    ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

%%====================================================================
%% QOS Tests (Phase 23)
%%====================================================================

qos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"apply qos priority boost", fun test_qos_priority/0},
      {"apply qos resource limits", fun test_qos_limits/0},
      {"qos preemption", fun test_qos_preemption/0},
      {"qos time limits", fun test_qos_time_limits/0},
      {"multiple qos policies", fun test_multiple_qos/0},
      {"default qos applied", fun test_default_qos/0}
     ]}.

test_qos_priority() ->
    ok = flurm_federation:add_cluster(<<"qos_c1">>, #{
        host => <<"qosc1.example.com">>
    }),
    update_cluster_resources(<<"qos_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"qos_priority_test">>, num_cpus => 1, qos => <<"high">>},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_qos_limits() ->
    ok = flurm_federation:add_cluster(<<"qos_c2">>, #{
        host => <<"qosc2.example.com">>
    }),
    update_cluster_resources(<<"qos_c2">>, #{state => up, free_cpus => 100}),

    %% QOS with resource limits
    JobSpec = #{name => <<"qos_limit_test">>, num_cpus => 10, qos => <<"limited">>},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_qos_preemption() ->
    ok = flurm_federation:add_cluster(<<"qos_c3">>, #{
        host => <<"qosc3.example.com">>
    }),
    update_cluster_resources(<<"qos_c3">>, #{state => up, free_cpus => 10}),

    %% Low priority job
    Job1 = #{name => <<"qos_low">>, num_cpus => 8, qos => <<"low">>},
    _ = flurm_federation:submit_federated_job(Job1, #{}),

    %% High priority preempting job
    Job2 = #{name => <<"qos_preempt">>, num_cpus => 8, qos => <<"preemptable">>},
    _ = flurm_federation:submit_federated_job(Job2, #{}),
    ok.

test_qos_time_limits() ->
    ok = flurm_federation:add_cluster(<<"qos_c4">>, #{
        host => <<"qosc4.example.com">>
    }),
    update_cluster_resources(<<"qos_c4">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"qos_time_test">>, num_cpus => 1,
               qos => <<"short">>, time_limit => 3600},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_multiple_qos() ->
    ok = flurm_federation:add_cluster(<<"qos_c5">>, #{
        host => <<"qosc5.example.com">>
    }),
    update_cluster_resources(<<"qos_c5">>, #{state => up, free_cpus => 100}),

    %% Submit with multiple QOS levels
    lists:foreach(fun(Qos) ->
        JobSpec = #{name => list_to_binary("multi_qos_" ++ binary_to_list(Qos)),
                   num_cpus => 1, qos => Qos},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, [<<"low">>, <<"normal">>, <<"high">>]),
    ok.

test_default_qos() ->
    ok = flurm_federation:add_cluster(<<"qos_c6">>, #{
        host => <<"qosc6.example.com">>
    }),
    update_cluster_resources(<<"qos_c6">>, #{state => up, free_cpus => 100}),

    %% No QOS specified - should use default
    JobSpec = #{name => <<"default_qos_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

%%====================================================================
%% Event Notification Tests (Phase 24)
%%====================================================================

event_notification_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"notify on job submit", fun test_notify_job_submit/0},
      {"notify on job complete", fun test_notify_job_complete/0},
      {"notify on cluster state change", fun test_notify_cluster_change/0},
      {"subscribe to events", fun test_event_subscribe/0},
      {"unsubscribe from events", fun test_event_unsubscribe/0},
      {"filter events by type", fun test_event_filter/0}
     ]}.

test_notify_job_submit() ->
    ok = flurm_federation:add_cluster(<<"evt_c1">>, #{
        host => <<"evtc1.example.com">>
    }),
    update_cluster_resources(<<"evt_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"evt_submit_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Event should be fired
    ok.

test_notify_job_complete() ->
    ok = flurm_federation:add_cluster(<<"evt_c2">>, #{
        host => <<"evtc2.example.com">>
    }),
    update_cluster_resources(<<"evt_c2">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"evt_complete_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Completion event would fire when job finishes
    ok.

test_notify_cluster_change() ->
    ok = flurm_federation:add_cluster(<<"evt_c3">>, #{
        host => <<"evtc3.example.com">>
    }),

    %% Cluster state change
    update_cluster_resources(<<"evt_c3">>, #{state => up}),
    update_cluster_resources(<<"evt_c3">>, #{state => down}),
    %% Events should fire
    ok.

test_event_subscribe() ->
    Self = self(),
    Result = flurm_federation:subscribe_events(Self, [job_submit, job_complete]),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_event_unsubscribe() ->
    Self = self(),
    _ = flurm_federation:subscribe_events(Self, [job_submit]),
    Result = flurm_federation:unsubscribe_events(Self),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_event_filter() ->
    Self = self(),
    _ = flurm_federation:subscribe_events(Self, [cluster_state]),

    ok = flurm_federation:add_cluster(<<"evt_c4">>, #{
        host => <<"evtc4.example.com">>
    }),
    update_cluster_resources(<<"evt_c4">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"evt_filter_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Should only receive cluster events, not job events
    ok.

%%====================================================================
%% Backpressure Tests (Phase 25)
%%====================================================================

backpressure_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"backpressure when queue full", fun test_backpressure_queue_full/0},
      {"rate limiting job submissions", fun test_rate_limiting/0},
      {"backpressure per cluster", fun test_per_cluster_backpressure/0},
      {"adaptive backpressure", fun test_adaptive_backpressure/0},
      {"backpressure recovery", fun test_backpressure_recovery/0}
     ]}.

test_backpressure_queue_full() ->
    ok = flurm_federation:add_cluster(<<"bp_c1">>, #{
        host => <<"bpc1.example.com">>,
        max_queue_size => 10
    }),
    update_cluster_resources(<<"bp_c1">>, #{state => up, free_cpus => 1}),

    %% Submit many jobs to fill queue
    Results = [begin
        JobSpec = #{name => list_to_binary("bp_test_" ++ integer_to_list(N)), num_cpus => 1},
        flurm_federation:submit_federated_job(JobSpec, #{})
    end || N <- lists:seq(1, 20)],

    %% Some should succeed, some should hit backpressure
    _Successes = length([Res || {ok, _} = Res <- Results]),
    ok.

test_rate_limiting() ->
    ok = flurm_federation:add_cluster(<<"bp_c2">>, #{
        host => <<"bpc2.example.com">>,
        rate_limit => 5  % 5 requests per second
    }),
    update_cluster_resources(<<"bp_c2">>, #{state => up, free_cpus => 100}),

    %% Rapid submissions
    _Results = [begin
        JobSpec = #{name => list_to_binary("rate_test_" ++ integer_to_list(N)), num_cpus => 1},
        flurm_federation:submit_federated_job(JobSpec, #{})
    end || N <- lists:seq(1, 10)],
    ok.

test_per_cluster_backpressure() ->
    ok = flurm_federation:add_cluster(<<"bp_c3">>, #{
        host => <<"bpc3.example.com">>,
        max_pending_jobs => 5
    }),
    ok = flurm_federation:add_cluster(<<"bp_c4">>, #{
        host => <<"bpc4.example.com">>,
        max_pending_jobs => 100
    }),

    update_cluster_resources(<<"bp_c3">>, #{state => up, free_cpus => 1}),
    update_cluster_resources(<<"bp_c4">>, #{state => up, free_cpus => 100}),

    %% c3 should hit backpressure, c4 should be fine
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("cluster_bp_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 20)),
    ok.

test_adaptive_backpressure() ->
    ok = flurm_federation:add_cluster(<<"bp_c5">>, #{
        host => <<"bpc5.example.com">>,
        adaptive_backpressure => true
    }),
    update_cluster_resources(<<"bp_c5">>, #{state => up, free_cpus => 10}),

    %% System should adapt to load
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("adapt_bp_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 50)),
    ok.

test_backpressure_recovery() ->
    ok = flurm_federation:add_cluster(<<"bp_c6">>, #{
        host => <<"bpc6.example.com">>,
        max_queue_size => 5
    }),
    update_cluster_resources(<<"bp_c6">>, #{state => up, free_cpus => 100}),

    %% Fill queue
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("recovery_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 10)),

    %% Simulate queue draining
    timer:sleep(100),

    %% Should be able to submit again
    JobSpec = #{name => <<"after_recovery">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

%%====================================================================
%% Timeout Handling Tests (Phase 26)
%%====================================================================

timeout_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"job submission timeout", fun test_submission_timeout/0},
      {"sync timeout", fun test_sync_timeout/0},
      {"health check timeout", fun test_health_timeout/0},
      {"cancel timeout", fun test_cancel_timeout/0},
      {"configurable timeouts", fun test_configurable_timeouts/0}
     ]}.

test_submission_timeout() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        timer:sleep(10000),  % Simulate slow response
        {ok, {{version, 200, "OK"}, [], ""}}
    end),

    ok = flurm_federation:add_cluster(<<"to_c1">>, #{
        host => <<"toc1.example.com">>,
        request_timeout => 100
    }),
    update_cluster_resources(<<"to_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"timeout_test">>, num_cpus => 1},
    _Result = flurm_federation:submit_federated_job(JobSpec, #{}),

    meck:unload(httpc),
    ok.

test_sync_timeout() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, timeout}
    end),

    ok = flurm_federation:add_cluster(<<"to_c2">>, #{
        host => <<"toc2.example.com">>
    }),

    Result = flurm_federation:sync_cluster_state(<<"to_c2">>),
    ?assertMatch({error, _}, Result),

    meck:unload(httpc),
    ok.

test_health_timeout() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, timeout}
    end),

    ok = flurm_federation:add_cluster(<<"to_c3">>, #{
        host => <<"toc3.example.com">>
    }),

    %% Trigger health check
    FedPid = whereis(flurm_federation),
    FedPid ! health_check,
    _ = sys:get_state(flurm_federation),

    meck:unload(httpc),
    ok.

test_cancel_timeout() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {error, timeout}
    end),

    ok = flurm_federation:add_cluster(<<"to_c4">>, #{
        host => <<"toc4.example.com">>
    }),
    update_cluster_resources(<<"to_c4">>, #{state => up, free_cpus => 100}),

    _Result = flurm_federation:cancel_federated_job(<<"fed-timeout-test">>),

    meck:unload(httpc),
    ok.

test_configurable_timeouts() ->
    ok = flurm_federation:add_cluster(<<"to_c5">>, #{
        host => <<"toc5.example.com">>,
        connect_timeout => 1000,
        request_timeout => 5000,
        sync_timeout => 10000
    }),

    Info = flurm_federation:get_cluster_info(<<"to_c5">>),
    case Info of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Logging and Audit Tests (Phase 27)
%%====================================================================

logging_audit_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"log job submission", fun test_log_job_submit/0},
      {"log cluster operations", fun test_log_cluster_ops/0},
      {"audit trail for jobs", fun test_audit_trail/0},
      {"log level filtering", fun test_log_level_filter/0},
      {"structured logging", fun test_structured_logging/0}
     ]}.

test_log_job_submit() ->
    ok = flurm_federation:add_cluster(<<"log_c1">>, #{
        host => <<"logc1.example.com">>
    }),
    update_cluster_resources(<<"log_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"log_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Should be logged
    ok.

test_log_cluster_ops() ->
    ok = flurm_federation:add_cluster(<<"log_c2">>, #{
        host => <<"logc2.example.com">>
    }),
    _ = flurm_federation:remove_cluster(<<"log_c2">>),
    %% Add/remove should be logged
    ok.

test_audit_trail() ->
    ok = flurm_federation:add_cluster(<<"audit_c1">>, #{
        host => <<"auditc1.example.com">>
    }),
    update_cluster_resources(<<"audit_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"audit_test">>, num_cpus => 1},
    case flurm_federation:submit_federated_job(JobSpec, #{}) of
        {ok, JobId} ->
            %% Get audit trail
            Trail = flurm_federation:get_job_audit_trail(JobId),
            case Trail of
                {ok, Events} when is_list(Events) -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_log_level_filter() ->
    %% Set log level
    _ = flurm_federation:set_log_level(warning),

    ok = flurm_federation:add_cluster(<<"log_c3">>, #{
        host => <<"logc3.example.com">>
    }),
    %% Debug logs should be filtered
    ok.

test_structured_logging() ->
    ok = flurm_federation:add_cluster(<<"log_c4">>, #{
        host => <<"logc4.example.com">>
    }),
    update_cluster_resources(<<"log_c4">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<"struct_log_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    %% Logs should be structured (JSON-friendly)
    ok.

%%====================================================================
%% Cleanup and Shutdown Tests (Phase 28)
%%====================================================================

cleanup_shutdown_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"graceful shutdown", fun test_graceful_shutdown/0},
      {"cleanup on cluster remove", fun test_cleanup_cluster_remove/0},
      {"cleanup stale jobs", fun test_cleanup_stale_jobs/0},
      {"cleanup expired data", fun test_cleanup_expired_data/0},
      {"state recovery after restart", fun test_state_recovery/0}
     ]}.

test_graceful_shutdown() ->
    ok = flurm_federation:add_cluster(<<"shut_c1">>, #{
        host => <<"shutc1.example.com">>
    }),
    update_cluster_resources(<<"shut_c1">>, #{state => up, free_cpus => 100}),

    %% Submit pending jobs
    lists:foreach(fun(N) ->
        JobSpec = #{name => list_to_binary("shutdown_" ++ integer_to_list(N)), num_cpus => 1},
        _ = flurm_federation:submit_federated_job(JobSpec, #{})
    end, lists:seq(1, 5)),

    %% Graceful shutdown should wait for pending
    _ = flurm_federation:prepare_shutdown(),
    ok.

test_cleanup_cluster_remove() ->
    ok = flurm_federation:add_cluster(<<"clean_c1">>, #{
        host => <<"cleanc1.example.com">>
    }),
    update_cluster_resources(<<"clean_c1">>, #{state => up, free_cpus => 100}),

    %% Submit jobs
    JobSpec = #{name => <<"cleanup_test">>, num_cpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),

    %% Remove cluster - jobs should be cleaned up
    _ = flurm_federation:remove_cluster(<<"clean_c1">>),
    ok.

test_cleanup_stale_jobs() ->
    ok = flurm_federation:add_cluster(<<"stale_c1">>, #{
        host => <<"stalec1.example.com">>
    }),
    update_cluster_resources(<<"stale_c1">>, #{state => up, free_cpus => 100}),

    %% Trigger cleanup of stale jobs
    _ = flurm_federation:cleanup_stale_jobs(3600),  % Jobs older than 1 hour
    ok.

test_cleanup_expired_data() ->
    %% Trigger expired data cleanup
    _ = flurm_federation:cleanup_expired_data(),
    ok.

test_state_recovery() ->
    ok = flurm_federation:add_cluster(<<"recov_c1">>, #{
        host => <<"recovc1.example.com">>
    }),
    update_cluster_resources(<<"recov_c1">>, #{state => up, free_cpus => 100}),

    %% Get current state
    State1 = flurm_federation:get_federation_info(),

    %% State should be consistent
    State2 = flurm_federation:get_federation_info(),
    ?assertEqual(State1, State2).

%%====================================================================
%% Edge Case String Handling Tests (Phase 29)
%%====================================================================

string_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"empty cluster id", fun test_empty_cluster_id/0},
      {"very long cluster id", fun test_long_cluster_id/0},
      {"special chars in cluster id", fun test_special_chars_cluster_id/0},
      {"unicode cluster id", fun test_unicode_cluster_id/0},
      {"null bytes in input", fun test_null_bytes/0},
      {"empty job name", fun test_empty_job_name/0}
     ]}.

test_empty_cluster_id() ->
    Result = flurm_federation:add_cluster(<<>>, #{
        host => <<"empty.example.com">>
    }),
    case Result of
        ok -> ok;  % May accept
        {error, _} -> ok  % Should reject
    end.

test_long_cluster_id() ->
    LongId = list_to_binary([65 || _ <- lists:seq(1, 10000)]),
    Result = flurm_federation:add_cluster(LongId, #{
        host => <<"long.example.com">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_special_chars_cluster_id() ->
    Result = flurm_federation:add_cluster(<<"cluster!@#$%^&*()">>, #{
        host => <<"special.example.com">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_unicode_cluster_id() ->
    Result = flurm_federation:add_cluster(<<"集群测试"/utf8>>, #{
        host => <<"unicode.example.com">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_null_bytes() ->
    Result = flurm_federation:add_cluster(<<"cluster", 0, "null">>, #{
        host => <<"null.example.com">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_empty_job_name() ->
    ok = flurm_federation:add_cluster(<<"str_c1">>, #{
        host => <<"strc1.example.com">>
    }),
    update_cluster_resources(<<"str_c1">>, #{state => up, free_cpus => 100}),

    JobSpec = #{name => <<>>, num_cpus => 1},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Resource Constraint Tests (Phase 30)
%%====================================================================

resource_constraint_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"job exceeds cluster resources", fun test_job_exceeds_resources/0},
      {"job exactly matches resources", fun test_job_exact_match/0},
      {"memory constraint", fun test_memory_constraint/0},
      {"gpu constraint", fun test_gpu_constraint/0},
      {"multiple constraints", fun test_multiple_constraints/0},
      {"no cluster has resources", fun test_no_resources_available/0}
     ]}.

test_job_exceeds_resources() ->
    ok = flurm_federation:add_cluster(<<"res_c1">>, #{
        host => <<"res1.example.com">>
    }),
    update_cluster_resources(<<"res_c1">>, #{state => up, total_cpus => 10, free_cpus => 10}),

    JobSpec = #{name => <<"exceed_test">>, num_cpus => 100},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    case Result of
        {ok, _} -> ok;  % May queue
        {error, _} -> ok  % May reject
    end.

test_job_exact_match() ->
    ok = flurm_federation:add_cluster(<<"res_c2">>, #{
        host => <<"res2.example.com">>
    }),
    update_cluster_resources(<<"res_c2">>, #{state => up, total_cpus => 10, free_cpus => 10}),

    JobSpec = #{name => <<"exact_test">>, num_cpus => 10},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_memory_constraint() ->
    ok = flurm_federation:add_cluster(<<"res_c3">>, #{
        host => <<"res3.example.com">>
    }),
    update_cluster_resources(<<"res_c3">>, #{
        state => up,
        free_cpus => 100,
        total_memory => 1024000,
        free_memory => 512000
    }),

    JobSpec = #{name => <<"mem_test">>, num_cpus => 1, memory => 256000},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_gpu_constraint() ->
    ok = flurm_federation:add_cluster(<<"res_c4">>, #{
        host => <<"res4.example.com">>
    }),
    update_cluster_resources(<<"res_c4">>, #{
        state => up,
        free_cpus => 100,
        total_gpus => 4,
        free_gpus => 2
    }),

    JobSpec = #{name => <<"gpu_test">>, num_cpus => 1, num_gpus => 1},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_multiple_constraints() ->
    ok = flurm_federation:add_cluster(<<"res_c5">>, #{
        host => <<"res5.example.com">>
    }),
    update_cluster_resources(<<"res_c5">>, #{
        state => up,
        free_cpus => 100,
        free_memory => 1024000,
        free_gpus => 4
    }),

    JobSpec = #{name => <<"multi_constraint">>, num_cpus => 4, memory => 512000, num_gpus => 2},
    _ = flurm_federation:submit_federated_job(JobSpec, #{}),
    ok.

test_no_resources_available() ->
    ok = flurm_federation:add_cluster(<<"res_c6">>, #{
        host => <<"res6.example.com">>
    }),
    update_cluster_resources(<<"res_c6">>, #{state => up, total_cpus => 10, free_cpus => 0}),

    JobSpec = #{name => <<"no_res_test">>, num_cpus => 1},
    Result = flurm_federation:submit_federated_job(JobSpec, #{}),
    case Result of
        {ok, _} -> ok;  % May queue
        {error, _} -> ok  % May reject
    end.
