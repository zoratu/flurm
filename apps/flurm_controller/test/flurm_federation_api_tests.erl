%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_federation_api
%%%
%%% Covers all federation REST API endpoints with mocked
%%% flurm_federation backend.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_federation_api_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_api_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

federation_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% GET /federation
        {"GET federation info ok",
         fun test_get_federation_info_ok/0},
        {"GET federation info not_federated",
         fun test_get_federation_info_not_federated/0},
        {"GET federation info noproc returns 503",
         fun test_get_federation_info_noproc/0},
        {"GET federation info unexpected error returns 500",
         fun test_get_federation_info_error/0},
        %% GET /federation/clusters
        {"GET clusters ok",
         fun test_list_clusters_ok/0},
        {"GET clusters empty list",
         fun test_list_clusters_empty/0},
        {"GET clusters noproc returns 503",
         fun test_list_clusters_noproc/0},
        {"GET clusters with map format",
         fun test_list_clusters_map_format/0},
        {"GET clusters with tuple format",
         fun test_list_clusters_tuple_format/0},
        %% POST /federation/clusters
        {"POST add cluster success returns 201",
         fun test_add_cluster_success/0},
        {"POST add cluster with optional fields",
         fun test_add_cluster_optional_fields/0},
        {"POST add cluster failure returns 400",
         fun test_add_cluster_failure/0},
        {"POST add cluster invalid JSON returns 400",
         fun test_add_cluster_invalid_json/0},
        {"POST add cluster missing name returns error",
         fun test_add_cluster_missing_name/0},
        %% DELETE /federation/clusters/:name
        {"DELETE cluster success returns 200",
         fun test_remove_cluster_success/0},
        {"DELETE cluster not_found returns 404",
         fun test_remove_cluster_not_found/0},
        {"DELETE cluster cannot_remove_local returns 400",
         fun test_remove_cluster_cannot_remove_local/0},
        {"DELETE cluster noproc returns 503",
         fun test_remove_cluster_noproc/0},
        %% GET /federation/resources
        {"GET resources ok",
         fun test_get_resources_ok/0},
        {"GET resources noproc returns 503",
         fun test_get_resources_noproc/0},
        %% POST /federation/jobs
        {"POST federated job success returns 201",
         fun test_submit_job_success/0},
        {"POST federated job no_eligible_clusters returns 503",
         fun test_submit_job_no_clusters/0},
        {"POST federated job submit failure returns 500",
         fun test_submit_job_submit_failure/0},
        {"POST federated job invalid JSON returns 400",
         fun test_submit_job_invalid_json/0},
        %% Unknown
        {"unknown path returns 404",
         fun test_unknown_path/0},
        {"unknown method returns 404",
         fun test_unknown_method/0},
        %% handle/3 delegates to handle/4
        {"handle/3 delegates to handle/4",
         fun test_handle3_delegates/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    catch meck:unload(flurm_federation),
    meck:new(flurm_federation, [non_strict]),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_federation),
    ok.

%%====================================================================
%% Helper
%%====================================================================

decode_body(Bin) ->
    jsx:decode(Bin, [return_maps]).

%%====================================================================
%% Tests - GET /federation
%%====================================================================

test_get_federation_info_ok() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"test_fed">>, local_cluster => <<"local1">>, clusters => [a, b]}}
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Decoded)),
    ?assertEqual(<<"test_fed">>, maps:get(<<"federation_name">>, Decoded)),
    ?assertEqual(2, maps:get(<<"cluster_count">>, Decoded)).

test_get_federation_info_not_federated() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {error, not_federated}
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"not_federated">>, maps:get(<<"status">>, Decoded)),
    ?assertEqual(0, maps:get(<<"cluster_count">>, Decoded)).

test_get_federation_info_noproc() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        exit({noproc, {gen_server, call, [flurm_federation, get_info]}})
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(503, Status).

test_get_federation_info_error() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {error, some_unexpected_error}
    end),
    {Status, _Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(500, Status).

%%====================================================================
%% Tests - GET /federation/clusters
%%====================================================================

test_list_clusters_ok() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [#{name => <<"cluster1">>, host => <<"host1">>, port => 6817, state => <<"active">>}]
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(1, maps:get(<<"count">>, Decoded)),
    ?assert(is_list(maps:get(<<"clusters">>, Decoded))).

test_list_clusters_empty() ->
    meck:expect(flurm_federation, list_clusters, fun() -> [] end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(0, maps:get(<<"count">>, Decoded)),
    ?assertEqual([], maps:get(<<"clusters">>, Decoded)).

test_list_clusters_noproc() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        exit({noproc, {gen_server, call, [flurm_federation, list]}})
    end),
    {Status, _} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assertEqual(503, Status).

test_list_clusters_map_format() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [#{name => <<"c1">>, host => <<"h1">>, port => 1234, state => <<"up">>}]
    end),
    {200, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    Decoded = decode_body(Body),
    [Cluster] = maps:get(<<"clusters">>, Decoded),
    ?assertEqual(<<"c1">>, maps:get(<<"name">>, Cluster)),
    ?assertEqual(<<"h1">>, maps:get(<<"host">>, Cluster)).

test_list_clusters_tuple_format() ->
    meck:expect(flurm_federation, list_clusters, fun() ->
        [{cluster_record, <<"tuple_cluster">>}]
    end),
    {200, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    Decoded = decode_body(Body),
    [Cluster] = maps:get(<<"clusters">>, Decoded),
    ?assertNotEqual(undefined, maps:get(<<"cluster">>, Cluster, undefined)).

%%====================================================================
%% Tests - POST /federation/clusters
%%====================================================================

test_add_cluster_success() ->
    meck:expect(flurm_federation, add_cluster, fun(<<"new_cluster">>, _Config) -> ok end),
    ReqBody = jsx:encode(#{<<"name">> => <<"new_cluster">>, <<"host">> => <<"host1">>, <<"port">> => 6817}),
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, ReqBody),
    ?assertEqual(201, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Decoded)),
    ?assertEqual(<<"new_cluster">>, maps:get(<<"name">>, Decoded)).

test_add_cluster_optional_fields() ->
    meck:expect(flurm_federation, add_cluster, fun(<<"opt_cluster">>, Config) ->
        %% Verify optional fields passed through
        ?assertEqual(5, maps:get(weight, Config)),
        ok
    end),
    ReqBody = jsx:encode(#{<<"name">> => <<"opt_cluster">>, <<"weight">> => 5,
                           <<"features">> => [<<"gpu">>], <<"partitions">> => [<<"batch">>]}),
    {Status, _} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, ReqBody),
    ?assertEqual(201, Status).

test_add_cluster_failure() ->
    meck:expect(flurm_federation, add_cluster, fun(_Name, _Config) ->
        {error, already_exists}
    end),
    ReqBody = jsx:encode(#{<<"name">> => <<"dup_cluster">>}),
    {Status, _} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, ReqBody),
    ?assertEqual(400, Status).

test_add_cluster_invalid_json() ->
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<"not json">>),
    ?assertEqual(400, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"Invalid JSON">>, maps:get(<<"error">>, Decoded)).

test_add_cluster_missing_name() ->
    ReqBody = jsx:encode(#{<<"host">> => <<"noname">>}),
    {Status, _} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, ReqBody),
    ?assert(Status >= 400).

%%====================================================================
%% Tests - DELETE /federation/clusters/:name
%%====================================================================

test_remove_cluster_success() ->
    meck:expect(flurm_federation, remove_cluster, fun(<<"old_cluster">>) -> ok end),
    {Status, Body} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/old_cluster">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Decoded)).

test_remove_cluster_not_found() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, not_found} end),
    {Status, _} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/missing">>, <<>>),
    ?assertEqual(404, Status).

test_remove_cluster_cannot_remove_local() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) -> {error, cannot_remove_local} end),
    {Status, _} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/local">>, <<>>),
    ?assertEqual(400, Status).

test_remove_cluster_noproc() ->
    meck:expect(flurm_federation, remove_cluster, fun(_) ->
        exit({noproc, {gen_server, call, [flurm_federation, remove]}})
    end),
    {Status, _} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test">>, <<>>),
    ?assertEqual(503, Status).

%%====================================================================
%% Tests - GET /federation/resources
%%====================================================================

test_get_resources_ok() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        #{total_nodes => 10, total_cpus => 100, available_cpus => 50}
    end),
    {Status, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(200, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Decoded)),
    Resources = maps:get(<<"resources">>, Decoded),
    ?assertEqual(10, maps:get(<<"total_nodes">>, Resources)).

test_get_resources_noproc() ->
    meck:expect(flurm_federation, get_federation_resources, fun() ->
        exit({noproc, {gen_server, call, [flurm_federation, resources]}})
    end),
    {Status, _} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assertEqual(503, Status).

%%====================================================================
%% Tests - POST /federation/jobs
%%====================================================================

test_submit_job_success() ->
    meck:expect(flurm_federation, route_job, fun(_JobSpec) ->
        {ok, <<"cluster1">>}
    end),
    meck:expect(flurm_federation, submit_job, fun(<<"cluster1">>, _JobSpec) ->
        {ok, 42}
    end),
    ReqBody = jsx:encode(#{<<"name">> => <<"my_job">>, <<"script">> => <<"#!/bin/bash\necho hi">>}),
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, ReqBody),
    ?assertEqual(201, Status),
    Decoded = decode_body(Body),
    ?assertEqual(42, maps:get(<<"job_id">>, Decoded)),
    ?assertEqual(<<"cluster1">>, maps:get(<<"cluster">>, Decoded)).

test_submit_job_no_clusters() ->
    meck:expect(flurm_federation, route_job, fun(_) ->
        {error, no_eligible_clusters}
    end),
    ReqBody = jsx:encode(#{<<"name">> => <<"job1">>, <<"script">> => <<"echo">>}),
    {Status, _} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, ReqBody),
    ?assertEqual(503, Status).

test_submit_job_submit_failure() ->
    meck:expect(flurm_federation, route_job, fun(_) -> {ok, <<"c1">>} end),
    meck:expect(flurm_federation, submit_job, fun(_, _) -> {error, queue_full} end),
    ReqBody = jsx:encode(#{<<"name">> => <<"job2">>, <<"script">> => <<"echo">>}),
    {Status, _} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, ReqBody),
    ?assertEqual(500, Status).

test_submit_job_invalid_json() ->
    {Status, Body} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, <<"bad json">>),
    ?assertEqual(400, Status),
    Decoded = decode_body(Body),
    ?assertEqual(<<"Invalid JSON">>, maps:get(<<"error">>, Decoded)).

%%====================================================================
%% Tests - Unknown
%%====================================================================

test_unknown_path() ->
    {Status, _} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/unknown">>, <<>>),
    ?assertEqual(404, Status).

test_unknown_method() ->
    {Status, _} = flurm_federation_api:handle(<<"PATCH">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(404, Status).

test_handle3_delegates() ->
    meck:expect(flurm_federation, get_federation_info, fun() ->
        {ok, #{name => <<"fed">>, local_cluster => <<"local">>, clusters => []}}
    end),
    {S1, B1} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    {S2, B2} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>, #{}),
    ?assertEqual(S1, S2),
    ?assertEqual(B1, B2).
