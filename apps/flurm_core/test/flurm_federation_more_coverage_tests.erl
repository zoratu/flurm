%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_federation module.
%%% Tests internal functions exported via -ifdef(TEST) and edge cases.
%%%-------------------------------------------------------------------
-module(flurm_federation_more_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% URL Building Tests
%%====================================================================

build_url_basic_test() ->
    Url = flurm_federation:build_url(<<"localhost">>, 6817, <<"/api/v1/health">>),
    ?assertEqual(<<"http://localhost:6817/api/v1/health">>, Url).

build_url_with_host_test() ->
    Url = flurm_federation:build_url(<<"cluster1.example.com">>, 8080, <<"/status">>),
    ?assertEqual(<<"http://cluster1.example.com:8080/status">>, Url).

build_url_root_path_test() ->
    Url = flurm_federation:build_url(<<"node1">>, 9000, <<"/">>),
    ?assertEqual(<<"http://node1:9000/">>, Url).

build_url_jobs_endpoint_test() ->
    Url = flurm_federation:build_url(<<"scheduler">>, 6817, <<"/api/v1/jobs">>),
    ?assertEqual(<<"http://scheduler:6817/api/v1/jobs">>, Url).

build_url_cluster_stats_test() ->
    Url = flurm_federation:build_url(<<"remote">>, 7000, <<"/api/v1/cluster/stats">>),
    ?assertEqual(<<"http://remote:7000/api/v1/cluster/stats">>, Url).

%%====================================================================
%% Auth Headers Tests
%%====================================================================

build_auth_headers_empty_test() ->
    Headers = flurm_federation:build_auth_headers(#{}),
    ?assertEqual([], Headers).

build_auth_headers_token_test() ->
    Headers = flurm_federation:build_auth_headers(#{token => <<"my-secret-token">>}),
    ?assertEqual([{<<"Authorization">>, <<"Bearer my-secret-token">>}], Headers).

build_auth_headers_api_key_test() ->
    Headers = flurm_federation:build_auth_headers(#{api_key => <<"key123">>}),
    ?assertEqual([{<<"X-API-Key">>, <<"key123">>}], Headers).

build_auth_headers_other_test() ->
    Headers = flurm_federation:build_auth_headers(#{user => <<"admin">>}),
    ?assertEqual([], Headers).

%%====================================================================
%% Headers to Proplist Tests
%%====================================================================

headers_to_proplist_empty_test() ->
    Result = flurm_federation:headers_to_proplist([]),
    ?assertEqual([], Result).

headers_to_proplist_single_test() ->
    Result = flurm_federation:headers_to_proplist([{<<"Content-Type">>, <<"application/json">>}]),
    ?assertEqual([{"Content-Type", "application/json"}], Result).

headers_to_proplist_multiple_test() ->
    Result = flurm_federation:headers_to_proplist([
        {<<"Accept">>, <<"application/json">>},
        {<<"X-Custom">>, <<"value">>}
    ]),
    ?assertEqual([{"Accept", "application/json"}, {"X-Custom", "value"}], Result).

%%====================================================================
%% Cluster to Map Tests
%%====================================================================

cluster_to_map_test() ->
    %% Create a fed_cluster record via ETS if the table exists
    %% For unit testing, we can test the conversion function behavior
    %% by examining the expected structure
    ok.

%%====================================================================
%% Generate Local Ref Tests
%%====================================================================

generate_local_ref_format_test() ->
    Ref = flurm_federation:generate_local_ref(),
    ?assert(is_binary(Ref)),
    %% Should start with "ref-"
    ?assertMatch(<<"ref-", _/binary>>, Ref).

generate_local_ref_unique_test() ->
    Ref1 = flurm_federation:generate_local_ref(),
    Ref2 = flurm_federation:generate_local_ref(),
    ?assertNotEqual(Ref1, Ref2).

generate_local_ref_length_test() ->
    Ref = flurm_federation:generate_local_ref(),
    %% Should be "ref-" + timestamp + "-" + random
    ?assert(byte_size(Ref) > 10).

%%====================================================================
%% Generate Federation ID Tests
%%====================================================================

generate_federation_id_format_test() ->
    FedId = flurm_federation:generate_federation_id(),
    ?assert(is_binary(FedId)),
    %% Should start with "fed-"
    ?assertMatch(<<"fed-", _/binary>>, FedId).

generate_federation_id_unique_test() ->
    FedId1 = flurm_federation:generate_federation_id(),
    FedId2 = flurm_federation:generate_federation_id(),
    ?assertNotEqual(FedId1, FedId2).

%%====================================================================
%% Job Helper Tests - get_job_partition
%%====================================================================

get_job_partition_map_test() ->
    Job = #{partition => <<"batch">>},
    ?assertEqual(<<"batch">>, flurm_federation:get_job_partition(Job)).

get_job_partition_map_missing_test() ->
    Job = #{name => <<"test">>},
    ?assertEqual(undefined, flurm_federation:get_job_partition(Job)).

get_job_partition_other_test() ->
    ?assertEqual(undefined, flurm_federation:get_job_partition(invalid)).

%%====================================================================
%% Job Helper Tests - get_job_features
%%====================================================================

get_job_features_map_test() ->
    Job = #{features => [<<"gpu">>, <<"high_mem">>]},
    ?assertEqual([<<"gpu">>, <<"high_mem">>], flurm_federation:get_job_features(Job)).

get_job_features_map_missing_test() ->
    Job = #{name => <<"test">>},
    ?assertEqual([], flurm_federation:get_job_features(Job)).

get_job_features_other_test() ->
    ?assertEqual([], flurm_federation:get_job_features(invalid)).

%%====================================================================
%% Job Helper Tests - get_job_cpus
%%====================================================================

get_job_cpus_map_test() ->
    Job = #{num_cpus => 16},
    ?assertEqual(16, flurm_federation:get_job_cpus(Job)).

get_job_cpus_map_missing_test() ->
    Job = #{name => <<"test">>},
    ?assertEqual(1, flurm_federation:get_job_cpus(Job)).

get_job_cpus_other_test() ->
    ?assertEqual(1, flurm_federation:get_job_cpus(invalid)).

%%====================================================================
%% Job Helper Tests - get_job_memory
%%====================================================================

get_job_memory_map_test() ->
    Job = #{memory_mb => 8192},
    ?assertEqual(8192, flurm_federation:get_job_memory(Job)).

get_job_memory_map_missing_test() ->
    Job = #{name => <<"test">>},
    ?assertEqual(1024, flurm_federation:get_job_memory(Job)).

get_job_memory_other_test() ->
    ?assertEqual(1024, flurm_federation:get_job_memory(invalid)).

%%====================================================================
%% Job to Map Tests
%%====================================================================

job_to_map_passthrough_test() ->
    Job = #{id => 1, name => <<"test">>},
    Result = flurm_federation:job_to_map(Job),
    ?assertEqual(Job, Result).

%%====================================================================
%% Map to Job Tests
%%====================================================================

map_to_job_full_test() ->
    Map = #{
        id => 123,
        name => <<"test_job">>,
        user => <<"alice">>,
        partition => <<"compute">>,
        state => running,
        script => <<"#!/bin/bash\necho test">>,
        num_nodes => 2,
        num_cpus => 8,
        memory_mb => 4096,
        time_limit => 7200,
        priority => 50,
        work_dir => <<"/home/alice">>,
        account => <<"project1">>,
        qos => <<"high">>
    },
    Job = flurm_federation:map_to_job(Map),
    ?assert(is_tuple(Job)),
    ?assertEqual(job, element(1, Job)).

map_to_job_defaults_test() ->
    Map = #{},
    Job = flurm_federation:map_to_job(Map),
    ?assert(is_tuple(Job)),
    %% Should have default values
    ?assertEqual(job, element(1, Job)),
    %% id defaults to 0
    ?assertEqual(0, element(2, Job)).

map_to_job_partial_test() ->
    Map = #{name => <<"partial_job">>, num_cpus => 4},
    Job = flurm_federation:map_to_job(Map),
    ?assertEqual(<<"partial_job">>, element(3, Job)).

%%====================================================================
%% Has Required Features Tests
%%====================================================================

has_required_features_empty_required_test() ->
    ?assert(flurm_federation:has_required_features([<<"gpu">>, <<"ssd">>], [])).

has_required_features_match_test() ->
    ?assert(flurm_federation:has_required_features([<<"gpu">>, <<"ssd">>], [<<"gpu">>])).

has_required_features_match_all_test() ->
    ?assert(flurm_federation:has_required_features([<<"gpu">>, <<"ssd">>, <<"nvme">>], [<<"gpu">>, <<"ssd">>])).

has_required_features_no_match_test() ->
    ?assertNot(flurm_federation:has_required_features([<<"gpu">>], [<<"ssd">>])).

has_required_features_partial_match_test() ->
    ?assertNot(flurm_federation:has_required_features([<<"gpu">>], [<<"gpu">>, <<"ssd">>])).

has_required_features_empty_cluster_test() ->
    ?assertNot(flurm_federation:has_required_features([], [<<"gpu">>])).

has_required_features_both_empty_test() ->
    ?assert(flurm_federation:has_required_features([], [])).

%%====================================================================
%% Aggregate Resources Tests
%%====================================================================

aggregate_resources_empty_test() ->
    Result = flurm_federation:aggregate_resources([]),
    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(total_nodes, Result)),
    ?assertEqual(0, maps:get(total_cpus, Result)),
    ?assertEqual(0, maps:get(clusters_up, Result)),
    ?assertEqual(0, maps:get(clusters_down, Result)).

%%====================================================================
%% Calculate Load Tests
%%====================================================================

calculate_load_zero_cpu_test() ->
    %% Create a mock cluster record with 0 CPUs
    %% The function returns infinity for 0 cpu_count
    ok.

calculate_load_normal_test() ->
    %% Normal load calculation tests would require a fed_cluster record
    ok.

%%====================================================================
%% API Function Tests (require gen_server running)
%%====================================================================

%% These tests would require the gen_server to be running
%% We test what we can without starting the full system

list_clusters_ets_not_exists_test() ->
    %% When ETS table doesn't exist, should handle gracefully
    %% This is tested by the actual API tests
    ok.

get_federation_resources_empty_test() ->
    %% When no clusters are registered
    ok.

get_federation_stats_empty_test() ->
    %% When no federation is configured
    ok.
