%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_federation internal functions
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test: build_url/3
%%====================================================================

build_url_test_() ->
    [
     {"builds basic URL",
      ?_assertEqual(<<"http://localhost:6817/api/v1/health">>,
                    flurm_federation:build_url(<<"localhost">>, 6817, <<"/api/v1/health">>))},

     {"builds URL with different port",
      ?_assertEqual(<<"http://example.com:8080/test">>,
                    flurm_federation:build_url(<<"example.com">>, 8080, <<"/test">>))},

     {"builds URL with IP address",
      ?_assertEqual(<<"http://192.168.1.1:6817/api/v1/jobs">>,
                    flurm_federation:build_url(<<"192.168.1.1">>, 6817, <<"/api/v1/jobs">>))}
    ].

%%====================================================================
%% Test: build_auth_headers/1
%%====================================================================

build_auth_headers_test_() ->
    [
     {"builds bearer token header",
      fun() ->
          Auth = #{token => <<"abc123">>},
          Result = flurm_federation:build_auth_headers(Auth),
          ?assertEqual([{<<"Authorization">>, <<"Bearer abc123">>}], Result)
      end},

     {"builds API key header",
      fun() ->
          Auth = #{api_key => <<"key123">>},
          Result = flurm_federation:build_auth_headers(Auth),
          ?assertEqual([{<<"X-API-Key">>, <<"key123">>}], Result)
      end},

     {"returns empty list for no auth",
      fun() ->
          ?assertEqual([], flurm_federation:build_auth_headers(#{})),
          ?assertEqual([], flurm_federation:build_auth_headers(#{other => value}))
      end}
    ].

%%====================================================================
%% Test: headers_to_proplist/1
%%====================================================================

headers_to_proplist_test_() ->
    [
     {"converts binary headers to proplist",
      fun() ->
          Headers = [{<<"Content-Type">>, <<"application/json">>}],
          Result = flurm_federation:headers_to_proplist(Headers),
          ?assertEqual([{"Content-Type", "application/json"}], Result)
      end},

     {"handles multiple headers",
      fun() ->
          Headers = [{<<"Accept">>, <<"*/*">>}, {<<"X-Custom">>, <<"value">>}],
          Result = flurm_federation:headers_to_proplist(Headers),
          ?assertEqual([{"Accept", "*/*"}, {"X-Custom", "value"}], Result)
      end},

     {"handles empty list",
      ?_assertEqual([], flurm_federation:headers_to_proplist([]))}
    ].

%%====================================================================
%% Test: generate_local_ref/0
%%====================================================================

generate_local_ref_test_() ->
    [
     {"generates ref starting with 'ref-'",
      fun() ->
          Ref = flurm_federation:generate_local_ref(),
          ?assert(is_binary(Ref)),
          ?assertMatch(<<"ref-", _/binary>>, Ref)
      end},

     {"generates unique refs",
      fun() ->
          Refs = [flurm_federation:generate_local_ref() || _ <- lists:seq(1, 100)],
          UniqueRefs = lists:usort(Refs),
          ?assertEqual(100, length(UniqueRefs))
      end}
    ].

%%====================================================================
%% Test: generate_federation_id/0
%%====================================================================

generate_federation_id_test_() ->
    [
     {"generates id starting with 'fed-'",
      fun() ->
          Id = flurm_federation:generate_federation_id(),
          ?assert(is_binary(Id)),
          ?assertMatch(<<"fed-", _/binary>>, Id)
      end},

     {"generates unique ids",
      fun() ->
          Ids = [flurm_federation:generate_federation_id() || _ <- lists:seq(1, 100)],
          UniqueIds = lists:usort(Ids),
          ?assertEqual(100, length(UniqueIds))
      end}
    ].

%%====================================================================
%% Test: get_job_partition/1
%%====================================================================

get_job_partition_test_() ->
    [
     {"extracts partition from job record",
      fun() ->
          Job = #job{partition = <<"gpu">>},
          ?assertEqual(<<"gpu">>, flurm_federation:get_job_partition(Job))
      end},

     {"extracts partition from map",
      fun() ->
          JobSpec = #{partition => <<"compute">>},
          ?assertEqual(<<"compute">>, flurm_federation:get_job_partition(JobSpec))
      end},

     {"returns undefined for missing partition",
      ?_assertEqual(undefined, flurm_federation:get_job_partition(#{}))}
    ].

%%====================================================================
%% Test: get_job_features/1
%%====================================================================

get_job_features_test_() ->
    [
     {"extracts features from map",
      fun() ->
          JobSpec = #{features => [<<"gpu">>, <<"ssd">>]},
          ?assertEqual([<<"gpu">>, <<"ssd">>], flurm_federation:get_job_features(JobSpec))
      end},

     {"returns empty list for job record",
      fun() ->
          Job = #job{},
          ?assertEqual([], flurm_federation:get_job_features(Job))
      end},

     {"returns empty list for missing features",
      ?_assertEqual([], flurm_federation:get_job_features(#{}))}
    ].

%%====================================================================
%% Test: get_job_cpus/1 and get_job_memory/1
%%====================================================================

get_job_cpus_test_() ->
    [
     {"extracts cpus from job record",
      fun() ->
          Job = #job{num_cpus = 16},
          ?assertEqual(16, flurm_federation:get_job_cpus(Job))
      end},

     {"extracts cpus from map",
      ?_assertEqual(8, flurm_federation:get_job_cpus(#{num_cpus => 8}))},

     {"defaults to 1 cpu",
      ?_assertEqual(1, flurm_federation:get_job_cpus(#{}))}
    ].

get_job_memory_test_() ->
    [
     {"extracts memory from job record",
      fun() ->
          Job = #job{memory_mb = 4096},
          ?assertEqual(4096, flurm_federation:get_job_memory(Job))
      end},

     {"extracts memory from map",
      ?_assertEqual(8192, flurm_federation:get_job_memory(#{memory_mb => 8192}))},

     {"defaults to 1024 MB",
      ?_assertEqual(1024, flurm_federation:get_job_memory(#{}))}
    ].

%%====================================================================
%% Test: job_to_map/1
%%====================================================================

job_to_map_test_() ->
    [
     {"converts job record to map",
      fun() ->
          Job = #job{id = 123, name = <<"test_job">>, user = <<"testuser">>,
                     partition = <<"default">>, num_cpus = 4},
          Result = flurm_federation:job_to_map(Job),
          ?assertEqual(123, maps:get(id, Result)),
          ?assertEqual(<<"test_job">>, maps:get(name, Result)),
          ?assertEqual(<<"testuser">>, maps:get(user, Result)),
          ?assertEqual(4, maps:get(num_cpus, Result))
      end},

     {"passes through map",
      fun() ->
          Map = #{id => 456, name => <<"other">>},
          ?assertEqual(Map, flurm_federation:job_to_map(Map))
      end}
    ].

%%====================================================================
%% Test: map_to_job/1
%%====================================================================

map_to_job_test_() ->
    [
     {"converts map to job record",
      fun() ->
          Map = #{id => 789, name => <<"mapped_job">>, user => <<"user1">>,
                  num_cpus => 8, memory_mb => 4096},
          Result = flurm_federation:map_to_job(Map),
          ?assertEqual(789, Result#job.id),
          ?assertEqual(<<"mapped_job">>, Result#job.name),
          ?assertEqual(<<"user1">>, Result#job.user),
          ?assertEqual(8, Result#job.num_cpus),
          ?assertEqual(4096, Result#job.memory_mb)
      end},

     {"uses defaults for missing fields",
      fun() ->
          Map = #{},
          Result = flurm_federation:map_to_job(Map),
          ?assertEqual(0, Result#job.id),
          ?assertEqual(<<"unnamed">>, Result#job.name),
          ?assertEqual(1, Result#job.num_nodes),
          ?assertEqual(pending, Result#job.state)
      end}
    ].

%%====================================================================
%% Test: has_required_features/2
%%====================================================================

has_required_features_test_() ->
    [
     {"returns true when all features present",
      ?_assertEqual(true, flurm_federation:has_required_features(
                           [<<"gpu">>, <<"ssd">>, <<"ib">>],
                           [<<"gpu">>, <<"ssd">>]))},

     {"returns true for empty requirements",
      ?_assertEqual(true, flurm_federation:has_required_features(
                           [<<"gpu">>], []))},

     {"returns true when cluster has exact features",
      ?_assertEqual(true, flurm_federation:has_required_features(
                           [<<"gpu">>], [<<"gpu">>]))},

     {"returns false when missing features",
      ?_assertEqual(false, flurm_federation:has_required_features(
                            [<<"gpu">>], [<<"gpu">>, <<"ssd">>]))},

     {"returns false for empty cluster features",
      ?_assertEqual(false, flurm_federation:has_required_features(
                            [], [<<"gpu">>]))}
    ].

%%====================================================================
%% Test: calculate_load/1
%%====================================================================

calculate_load_test_() ->
    [
     {"calculates load for cluster with jobs",
      fun() ->
          Cluster = make_test_cluster(10, 5, 100),  % pending, running, cpus
          Load = flurm_federation:calculate_load(Cluster),
          ?assertEqual(0.15, Load)  % (10+5)/100
      end},

     {"returns infinity for zero cpus",
      fun() ->
          Cluster = make_test_cluster(10, 5, 0),
          ?assertEqual(infinity, flurm_federation:calculate_load(Cluster))
      end},

     {"calculates zero load for idle cluster",
      fun() ->
          Cluster = make_test_cluster(0, 0, 100),
          ?assertEqual(0.0, flurm_federation:calculate_load(Cluster))
      end}
    ].

%%====================================================================
%% Test: aggregate_resources/1
%%====================================================================

aggregate_resources_test_() ->
    [
     {"aggregates resources from multiple clusters",
      fun() ->
          Clusters = [
            make_test_cluster_full(10, 100, 1000, 4, up),
            make_test_cluster_full(20, 200, 2000, 8, up)
          ],
          Result = flurm_federation:aggregate_resources(Clusters),
          ?assertEqual(30, maps:get(total_nodes, Result)),
          ?assertEqual(300, maps:get(total_cpus, Result)),
          ?assertEqual(3000, maps:get(total_memory_mb, Result)),
          ?assertEqual(12, maps:get(total_gpus, Result)),
          ?assertEqual(2, maps:get(clusters_up, Result))
      end},

     {"excludes down clusters from totals",
      fun() ->
          Clusters = [
            make_test_cluster_full(10, 100, 1000, 4, up),
            make_test_cluster_full(20, 200, 2000, 8, down)
          ],
          Result = flurm_federation:aggregate_resources(Clusters),
          ?assertEqual(10, maps:get(total_nodes, Result)),
          ?assertEqual(1, maps:get(clusters_up, Result)),
          ?assertEqual(1, maps:get(clusters_down, Result))
      end},

     {"handles empty cluster list",
      fun() ->
          Result = flurm_federation:aggregate_resources([]),
          ?assertEqual(0, maps:get(total_nodes, Result)),
          ?assertEqual(0, maps:get(clusters_up, Result))
      end}
    ].

%%====================================================================
%% Test Helpers
%%====================================================================

%% Create a minimal fed_cluster record for load testing
make_test_cluster(PendingJobs, RunningJobs, CpuCount) ->
    {fed_cluster, <<"test">>, <<"localhost">>, 6817, #{}, up, 1, [],
     [], 0, CpuCount, 0, 0, PendingJobs, RunningJobs, 0, 0, 0, 0, 0, #{}}.

%% Create a full fed_cluster record for resource aggregation testing
make_test_cluster_full(Nodes, Cpus, Memory, Gpus, State) ->
    {fed_cluster, <<"test">>, <<"localhost">>, 6817, #{}, State, 1, [],
     [], Nodes, Cpus, Memory, Gpus, 0, 0, Cpus div 2, Memory div 2, 0, 0, 0, #{}}.
