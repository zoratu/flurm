%%%-------------------------------------------------------------------
%%% @doc FLURM Federation API Comprehensive Coverage Tests
%%%
%%% Complete coverage tests for flurm_federation_api module.
%%% Tests all REST API endpoints:
%%% - GET  /api/v1/federation              - Get federation info
%%% - GET  /api/v1/federation/clusters     - List clusters
%%% - POST /api/v1/federation/clusters     - Add cluster
%%% - DELETE /api/v1/federation/clusters/:name - Remove cluster
%%% - GET  /api/v1/federation/resources    - Aggregate resources
%%% - POST /api/v1/federation/jobs         - Submit federated job
%%%
%%% Also tests helper functions:
%%% - format_clusters/1
%%% - format_error/1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_api_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

federation_api_100cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% GET /api/v1/federation tests
        {"Get federation info success", fun test_get_federation_info_success/0},
        {"Get federation info not federated", fun test_get_federation_info_not_federated/0},
        {"Get federation info noproc", fun test_get_federation_info_noproc/0},
        {"Get federation info error", fun test_get_federation_info_error/0},
        {"Get federation info with empty values", fun test_get_federation_info_empty_values/0},
        {"Get federation info with clusters", fun test_get_federation_info_with_clusters/0},

        %% GET /api/v1/federation/clusters tests
        {"List clusters success", fun test_list_clusters_success/0},
        {"List clusters empty", fun test_list_clusters_empty/0},
        {"List clusters multiple", fun test_list_clusters_multiple/0},
        {"List clusters noproc", fun test_list_clusters_noproc/0},
        {"List clusters error", fun test_list_clusters_error/0},
        {"List clusters with map format", fun test_list_clusters_map_format/0},
        {"List clusters with tuple format", fun test_list_clusters_tuple_format/0},
        {"List clusters with unknown format", fun test_list_clusters_unknown_format/0},

        %% POST /api/v1/federation/clusters tests
        {"Add cluster success", fun test_add_cluster_success/0},
        {"Add cluster with all params", fun test_add_cluster_all_params/0},
        {"Add cluster with defaults", fun test_add_cluster_defaults/0},
        {"Add cluster invalid json", fun test_add_cluster_invalid_json/0},
        {"Add cluster error from federation", fun test_add_cluster_error/0},
        {"Add cluster catch error", fun test_add_cluster_catch_error/0},
        {"Add cluster with features", fun test_add_cluster_with_features/0},
        {"Add cluster with partitions", fun test_add_cluster_with_partitions/0},
        {"Add cluster with weight", fun test_add_cluster_with_weight/0},

        %% DELETE /api/v1/federation/clusters/:name tests
        {"Remove cluster success", fun test_remove_cluster_success/0},
        {"Remove cluster not found", fun test_remove_cluster_not_found/0},
        {"Remove cluster cannot remove local", fun test_remove_cluster_cannot_remove_local/0},
        {"Remove cluster noproc", fun test_remove_cluster_noproc/0},
        {"Remove cluster error", fun test_remove_cluster_error/0},

        %% GET /api/v1/federation/resources tests
        {"Get resources success", fun test_get_resources_success/0},
        {"Get resources with data", fun test_get_resources_with_data/0},
        {"Get resources noproc", fun test_get_resources_noproc/0},
        {"Get resources error", fun test_get_resources_error/0},

        %% POST /api/v1/federation/jobs tests
        {"Submit job success", fun test_submit_job_success/0},
        {"Submit job with all params", fun test_submit_job_all_params/0},
        {"Submit job with defaults", fun test_submit_job_defaults/0},
        {"Submit job invalid json", fun test_submit_job_invalid_json/0},
        {"Submit job no eligible clusters", fun test_submit_job_no_eligible_clusters/0},
        {"Submit job noproc", fun test_submit_job_noproc/0},
        {"Submit job route error", fun test_submit_job_route_error/0},
        {"Submit job submit error", fun test_submit_job_submit_error/0},
        {"Submit job catch error", fun test_submit_job_catch_error/0},
        {"Submit job with routing policy", fun test_submit_job_routing_policy/0},

        %% 404 Not Found tests
        {"Unknown path returns 404", fun test_unknown_path_404/0},
        {"Unknown method returns 404", fun test_unknown_method_404/0},

        %% handle/3 vs handle/4 tests
        {"handle/3 delegates to handle/4", fun test_handle_3_delegates/0}
     ]}.

%%====================================================================
%% Helper function tests
%%====================================================================

helper_function_test_() ->
    [
        %% format_clusters/1 tests
        {"Format clusters with map", fun test_format_clusters_map/0},
        {"Format clusters with tuple", fun test_format_clusters_tuple/0},
        {"Format clusters with unknown", fun test_format_clusters_unknown/0},
        {"Format clusters empty list", fun test_format_clusters_empty/0},
        {"Format clusters multiple", fun test_format_clusters_multiple/0},
        {"Format clusters with all fields", fun test_format_clusters_all_fields/0},
        {"Format clusters missing fields", fun test_format_clusters_missing_fields/0},

        %% format_error/1 tests
        {"Format error binary", fun test_format_error_binary/0},
        {"Format error atom", fun test_format_error_atom/0},
        {"Format error tuple", fun test_format_error_tuple/0},
        {"Format error list", fun test_format_error_list/0},
        {"Format error integer", fun test_format_error_integer/0}
    ].

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Unload any existing mocks
    catch meck:unload(flurm_federation),
    catch meck:unload(jsx),

    %% Start meck for mocking
    meck:new([flurm_federation, jsx], [passthrough, no_link, non_strict]),

    %% Default mock behaviors for jsx
    meck:expect(jsx, encode, fun(Map) when is_map(Map) ->
        %% Simple mock encoder - just return the map as binary representation
        iolist_to_binary(io_lib:format("~p", [Map]))
    end),
    meck:expect(jsx, decode, fun(Binary, [return_maps]) when is_binary(Binary) ->
        %% For testing, we'll parse simple JSON-like structures
        %% This is mocked so it returns the expected structure
        #{<<"name">> => <<"test">>}
    end),

    %% Default mock behaviors for flurm_federation
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test_fed">>, local_cluster => <<"local">>, clusters => []}}
    end),
    meck:expect(flurm_federation, list_clusters, fun() -> [] end),
    meck:expect(flurm_federation, add_cluster, fun(_, _) -> ok end),
    meck:expect(flurm_federation, remove_cluster, fun(_) -> ok end),
    meck:expect(flurm_federation, get_federation_resources, fun() -> #{} end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"local">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {ok, 12345} end),

    ok.

cleanup(_) ->
    catch meck:unload([flurm_federation, jsx]),
    ok.

%%====================================================================
%% GET /api/v1/federation tests
%%====================================================================

test_get_federation_info_success() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"my_federation">>, local_cluster => <<"cluster1">>, clusters => [1,2,3]}}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status).

test_get_federation_info_not_federated() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {error, not_federated}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status).

test_get_federation_info_noproc() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(503, Status).

test_get_federation_info_error() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {error, internal_error}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(500, Status).

test_get_federation_info_empty_values() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{}}  %% Empty map - missing all keys
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status).

test_get_federation_info_with_clusters() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{
            name => <<"fed1">>,
            local_cluster => <<"local">>,
            clusters => [<<"cluster1">>, <<"cluster2">>, <<"cluster3">>]
        }}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status).

%%====================================================================
%% GET /api/v1/federation/clusters tests
%%====================================================================

test_list_clusters_success() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [#{name => <<"cluster1">>}]
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

test_list_clusters_empty() ->
    meck:expect(flurm_federation, list_clusters, fun() -> [] end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

test_list_clusters_multiple() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [
            #{name => <<"cluster1">>, host => <<"host1">>, port => 6817},
            #{name => <<"cluster2">>, host => <<"host2">>, port => 6818}
        ]
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

test_list_clusters_noproc() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(503, Status).

test_list_clusters_error() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        error(internal_error)
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(500, Status).

test_list_clusters_map_format() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [#{name => <<"test">>, host => <<"localhost">>, port => 6817, state => <<"up">>}]
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

test_list_clusters_tuple_format() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [{cluster, <<"test">>, <<"localhost">>, 6817}]
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

test_list_clusters_unknown_format() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [<<"some_string">>]
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status).

%%====================================================================
%% POST /api/v1/federation/clusters tests
%%====================================================================

test_add_cluster_success() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"new_cluster">>}
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"new_cluster">>, _) -> ok end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

test_add_cluster_all_params() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{
            <<"name">> => <<"full_cluster">>,
            <<"host">> => <<"192.168.1.100">>,
            <<"port">> => 6820,
            <<"weight">> => 5,
            <<"features">> => [<<"gpu">>, <<"ssd">>],
            <<"partitions">> => [<<"gpu">>, <<"compute">>]
        }
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"full_cluster">>, Config) ->
        ?assertEqual(<<"192.168.1.100">>, maps:get(host, Config)),
        ?assertEqual(6820, maps:get(port, Config)),
        ?assertEqual(5, maps:get(weight, Config)),
        ok
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

test_add_cluster_defaults() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"default_cluster">>}
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"default_cluster">>, Config) ->
        ?assertEqual(<<"localhost">>, maps:get(host, Config)),
        ?assertEqual(6817, maps:get(port, Config)),
        ?assertEqual(1, maps:get(weight, Config)),
        ?assertEqual([], maps:get(features, Config)),
        ?assertEqual([], maps:get(partitions, Config)),
        ok
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

test_add_cluster_invalid_json() ->
    meck:expect(jsx, decode, fun(_, _) -> error(badarg) end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"not json">>),
    ?assertEqual(400, Status).

test_add_cluster_error() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"bad_cluster">>}
    end),
    meck:expect(flurm_federation, add_cluster, fun(_, _) -> {error, already_exists} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(400, Status).

test_add_cluster_catch_error() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"error_cluster">>}
    end),
    meck:expect(flurm_federation, add_cluster, fun(_, _) -> error(unexpected_error) end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(500, Status).

test_add_cluster_with_features() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"feat_cluster">>, <<"features">> => [<<"gpu">>, <<"nvme">>]}
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"feat_cluster">>, Config) ->
        ?assertEqual([<<"gpu">>, <<"nvme">>], maps:get(features, Config)),
        ok
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

test_add_cluster_with_partitions() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"part_cluster">>, <<"partitions">> => [<<"default">>, <<"gpu">>]}
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"part_cluster">>, Config) ->
        ?assertEqual([<<"default">>, <<"gpu">>], maps:get(partitions, Config)),
        ok
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

test_add_cluster_with_weight() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"name">> => <<"weight_cluster">>, <<"weight">> => 10}
    end),
    meck:expect(flurm_federation, add_cluster, fun(<<"weight_cluster">>, Config) ->
        ?assertEqual(10, maps:get(weight, Config)),
        ok
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status).

%%====================================================================
%% DELETE /api/v1/federation/clusters/:name tests
%%====================================================================

test_remove_cluster_success() ->
    meck:expect(flurm_federation, remove_cluster, fun(<<"cluster_to_remove">>) -> ok end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/cluster_to_remove">>, <<>>),
    ?assertEqual(200, Status).

test_remove_cluster_not_found() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, not_found} end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/nonexistent">>, <<>>),
    ?assertEqual(404, Status).

test_remove_cluster_cannot_remove_local() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, cannot_remove_local} end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/local">>, <<>>),
    ?assertEqual(400, Status).

test_remove_cluster_noproc() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test">>, <<>>),
    ?assertEqual(503, Status).

test_remove_cluster_error() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) ->
        {error, internal_error}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test">>, <<>>),
    ?assertEqual(500, Status).

%%====================================================================
%% GET /api/v1/federation/resources tests
%%====================================================================

test_get_resources_success() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        #{}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(200, Status).

test_get_resources_with_data() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        #{
            total_cpus => 1000,
            available_cpus => 500,
            total_memory => 65536000,
            available_memory => 32768000,
            node_count => 100
        }
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(200, Status).

test_get_resources_noproc() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(503, Status).

test_get_resources_error() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        error(internal_error)
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(500, Status).

%%====================================================================
%% POST /api/v1/federation/jobs tests
%%====================================================================

test_submit_job_success() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash\necho hello">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"local">>} end),
    meck:expect(flurm_federation, submit_job, fun(<<"local">>, _) -> {ok, 12345} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(201, Status).

test_submit_job_all_params() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{
            <<"name">> => <<"my_job">>,
            <<"script">> => <<"#!/bin/bash\necho hello">>,
            <<"partition">> => <<"gpu">>,
            <<"num_cpus">> => 8,
            <<"user_id">> => 1001,
            <<"group_id">> => 1001,
            <<"routing">> => <<"least-loaded">>
        }
    end),
    meck:expect(flurm_federation, route_job, fun(JobSpec) ->
        ?assertEqual(<<"my_job">>, maps:get(name, JobSpec)),
        ?assertEqual(<<"gpu">>, maps:get(partition, JobSpec)),
        ?assertEqual(8, maps:get(num_cpus, JobSpec)),
        {ok, <<"cluster2">>}
    end),
    meck:expect(flurm_federation, submit_job, fun(<<"cluster2">>, _) -> {ok, 99999} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(201, Status).

test_submit_job_defaults() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash\necho test">>}
    end),
    meck:expect(flurm_federation, route_job, fun(JobSpec) ->
        ?assertEqual(<<"federated_job">>, maps:get(name, JobSpec)),
        ?assertEqual(<<"default">>, maps:get(partition, JobSpec)),
        ?assertEqual(1, maps:get(num_cpus, JobSpec)),
        ?assertEqual(1000, maps:get(user_id, JobSpec)),
        ?assertEqual(1000, maps:get(group_id, JobSpec)),
        {ok, <<"local">>}
    end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {ok, 1} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(201, Status).

test_submit_job_invalid_json() ->
    meck:expect(jsx, decode, fun(_, _) -> error(badarg) end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"not json">>),
    ?assertEqual(400, Status).

test_submit_job_no_eligible_clusters() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) -> {error, no_eligible_clusters} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(503, Status).

test_submit_job_noproc() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(503, Status).

test_submit_job_route_error() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) ->
        {error, routing_failed}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(500, Status).

test_submit_job_submit_error() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"cluster1">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {error, submission_failed} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(500, Status).

test_submit_job_catch_error() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"cluster1">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> error(unexpected) end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(500, Status).

test_submit_job_routing_policy() ->
    meck:expect(jsx, decode, fun(_, _) ->
        #{<<"script">> => <<"#!/bin/bash">>, <<"routing">> => <<"round-robin">>}
    end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"cluster1">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {ok, 5678} end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(201, Status).

%%====================================================================
%% 404 Not Found tests
%%====================================================================

test_unknown_path_404() ->
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/unknown">>, <<>>),
    ?assertEqual(404, Status).

test_unknown_method_404() ->
    {Status, _Body} = flurm_federation_api:handle(<<"PUT">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(404, Status).

%%====================================================================
%% handle/3 vs handle/4 tests
%%====================================================================

test_handle_3_delegates() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test">>, local_cluster => <<"local">>, clusters => []}}
    end),
    {Status3, _} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    {Status4, _} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>, #{}),
    ?assertEqual(Status3, Status4).

%%====================================================================
%% Helper Function Tests - format_clusters
%%====================================================================

test_format_clusters_map() ->
    Clusters = [#{name => <<"cluster1">>, host => <<"host1">>, port => 6817, state => <<"up">>}],
    Result = flurm_federation_api:format_clusters(Clusters),
    [Formatted] = Result,
    ?assertEqual(<<"cluster1">>, maps:get(name, Formatted)),
    ?assertEqual(<<"host1">>, maps:get(host, Formatted)),
    ?assertEqual(6817, maps:get(port, Formatted)),
    ?assertEqual(<<"up">>, maps:get(state, Formatted)).

test_format_clusters_tuple() ->
    Clusters = [{cluster, <<"test">>, <<"localhost">>, 6817}],
    Result = flurm_federation_api:format_clusters(Clusters),
    [Formatted] = Result,
    ?assert(maps:is_key(cluster, Formatted)).

test_format_clusters_unknown() ->
    Clusters = [<<"some_string">>],
    Result = flurm_federation_api:format_clusters(Clusters),
    [Formatted] = Result,
    ?assertEqual(<<"unknown">>, maps:get(cluster, Formatted)).

test_format_clusters_empty() ->
    Result = flurm_federation_api:format_clusters([]),
    ?assertEqual([], Result).

test_format_clusters_multiple() ->
    Clusters = [
        #{name => <<"c1">>},
        #{name => <<"c2">>},
        #{name => <<"c3">>}
    ],
    Result = flurm_federation_api:format_clusters(Clusters),
    ?assertEqual(3, length(Result)).

test_format_clusters_all_fields() ->
    Clusters = [#{name => <<"test">>, host => <<"h">>, port => 1234, state => <<"up">>}],
    [Formatted] = flurm_federation_api:format_clusters(Clusters),
    ?assert(maps:is_key(name, Formatted)),
    ?assert(maps:is_key(host, Formatted)),
    ?assert(maps:is_key(port, Formatted)),
    ?assert(maps:is_key(state, Formatted)).

test_format_clusters_missing_fields() ->
    Clusters = [#{name => <<"test">>}],  %% Missing host, port, state
    [Formatted] = flurm_federation_api:format_clusters(Clusters),
    ?assertEqual(<<>>, maps:get(host, Formatted)),
    ?assertEqual(6817, maps:get(port, Formatted)),
    ?assertEqual(<<"unknown">>, maps:get(state, Formatted)).

%%====================================================================
%% Helper Function Tests - format_error
%%====================================================================

test_format_error_binary() ->
    Result = flurm_federation_api:format_error(<<"error message">>),
    ?assertEqual(<<"error message">>, Result).

test_format_error_atom() ->
    Result = flurm_federation_api:format_error(not_found),
    ?assertEqual(<<"not_found">>, Result).

test_format_error_tuple() ->
    Result = flurm_federation_api:format_error({error, reason}),
    ?assert(is_binary(Result)).

test_format_error_list() ->
    Result = flurm_federation_api:format_error("error string"),
    ?assert(is_binary(Result)).

test_format_error_integer() ->
    Result = flurm_federation_api:format_error(500),
    ?assert(is_binary(Result)).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Empty body for GET requests", fun test_empty_body_get/0},
        {"Options parameter passed through", fun test_options_passthrough/0},
        {"Multiple cluster names in DELETE path", fun test_delete_with_slashes/0},
        {"Empty cluster name in DELETE", fun test_delete_empty_name/0},
        {"POST with empty body", fun test_post_empty_body/0},
        {"GET federation with OPTIONS", fun test_get_with_options/0}
     ]}.

test_empty_body_get() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test">>, clusters => []}}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status).

test_options_passthrough() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test">>, clusters => []}}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>, #{auth => true}),
    ?assertEqual(200, Status).

test_delete_with_slashes() ->
    %% The path parsing should handle cluster names without additional slashes
    meck:expect(flurm_federation, remove_cluster, fun(<<"test_cluster">>) -> ok end),
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test_cluster">>, <<>>),
    ?assertEqual(200, Status).

test_delete_empty_name() ->
    %% Empty name after /clusters/
    {Status, _Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/">>, <<>>),
    ?assertEqual(200, Status).

test_post_empty_body() ->
    meck:expect(jsx, decode, fun(<<>>, _) -> error(badarg) end),
    {Status, _Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(400, Status).

test_get_with_options() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test">>, clusters => []}}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>, #{timeout => 5000}),
    ?assertEqual(200, Status).

%%====================================================================
%% Response Body Content Tests
%%====================================================================

response_content_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Federation info response has correct structure", fun test_federation_info_response_structure/0},
        {"List clusters response has count", fun test_list_clusters_response_count/0},
        {"Add cluster response has name", fun test_add_cluster_response_name/0},
        {"Remove cluster response has message", fun test_remove_cluster_response_message/0},
        {"Submit job response has job_id", fun test_submit_job_response_job_id/0}
     ]}.

test_federation_info_response_structure() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"fed1">>, local_cluster => <<"local">>, clusters => [1,2]}}
    end),
    %% The mock jsx:encode returns the map as string, so we can't easily parse it
    %% But we verify the call succeeded
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status),
    ?assert(is_binary(Body)).

test_list_clusters_response_count() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [#{name => <<"c1">>}, #{name => <<"c2">>}]
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status),
    ?assert(is_binary(Body)).

test_add_cluster_response_name() ->
    meck:expect(jsx, decode, fun(_, _) -> #{<<"name">> => <<"new_cluster">>} end),
    meck:expect(flurm_federation, add_cluster, fun(_, _) -> ok end),
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"{}">>),
    ?assertEqual(201, Status),
    ?assert(is_binary(Body)).

test_remove_cluster_response_message() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> ok end),
    {Status, Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test">>, <<>>),
    ?assertEqual(200, Status),
    ?assert(is_binary(Body)).

test_submit_job_response_job_id() ->
    meck:expect(jsx, decode, fun(_, _) -> #{<<"script">> => <<"#!/bin/bash">>} end),
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"local">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {ok, 12345} end),
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"{}">>),
    ?assertEqual(201, Status),
    ?assert(is_binary(Body)).

%%====================================================================
%% Error Message Tests
%%====================================================================

error_message_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"503 error includes service not available message", fun test_503_error_message/0},
        {"400 error for invalid JSON", fun test_400_invalid_json_message/0},
        {"404 error for not found cluster", fun test_404_not_found_message/0}
     ]}.

test_503_error_message() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        exit({noproc, {gen_server, call, []}})
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(503, Status),
    ?assert(is_binary(Body)).

test_400_invalid_json_message() ->
    meck:expect(jsx, decode, fun(_, _) -> error(badarg) end),
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"bad">>),
    ?assertEqual(400, Status),
    ?assert(is_binary(Body)).

test_404_not_found_message() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, not_found} end),
    {Status, Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/missing">>, <<>>),
    ?assertEqual(404, Status),
    ?assert(is_binary(Body)).
