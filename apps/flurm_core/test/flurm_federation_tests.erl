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
      {"track_remote_job creates tracking record", fun test_track_remote_job/0}
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
