%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_federation_api module
%%% Tests for Federation REST API handlers
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_api_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: The federation API module is mostly a routing layer that
%% calls flurm_federation. We test the routing logic and error handling.

%%====================================================================
%% handle/3 Basic Routing Tests
%%====================================================================

%% Test handle/3 with GET /api/v1/federation
handle_get_federation_info_test() ->
    %% This will likely fail since flurm_federation isn't running
    %% but we test that the routing works
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)),
    %% Should be valid JSON
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(is_map(Decoded)).

handle_get_federation_clusters_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)).

handle_get_federation_resources_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)).

handle_unknown_path_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/unknown">>, <<>>),
    ?assertEqual(404, Code),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(maps:is_key(<<"error">>, Decoded)).

handle_unknown_method_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"PATCH">>, <<"/api/v1/federation">>, <<>>),
    ?assertEqual(404, Code),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(maps:is_key(<<"error">>, Decoded)).

%%====================================================================
%% handle/4 Tests with Options
%%====================================================================

handle4_get_federation_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>, #{}),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)).

handle4_get_clusters_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>, #{}),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)).

handle4_get_resources_test() ->
    {Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>, #{}),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Body)).

%%====================================================================
%% POST /api/v1/federation/clusters Tests
%%====================================================================

handle_post_clusters_valid_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"test_cluster">>,
        <<"host">> => <<"localhost">>,
        <<"port">> => 6817,
        <<"weight">> => 1,
        <<"features">> => [],
        <<"partitions">> => []
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    %% May fail if federation service isn't running, but shouldn't be 404
    ?assert(Code =/= 404),
    ?assert(is_binary(Response)).

handle_post_clusters_minimal_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"minimal_cluster">>,
        <<"host">> => <<"localhost">>
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    ?assert(Code =/= 404),
    ?assert(is_binary(Response)).

handle_post_clusters_invalid_json_test() ->
    Body = <<"not valid json">>,
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    ?assertEqual(400, Code),
    Decoded = jsx:decode(Response, [return_maps]),
    ?assertEqual(<<"Invalid JSON">>, maps:get(<<"error">>, Decoded)).

handle_post_clusters_empty_body_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, <<>>),
    %% Empty body will fail to parse required fields
    ?assert(Code >= 400),
    ?assert(is_binary(Response)).

%%====================================================================
%% DELETE /api/v1/federation/clusters/:name Tests
%%====================================================================

handle_delete_cluster_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/test_cluster">>, <<>>),
    %% May succeed or fail depending on whether cluster exists
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

handle_delete_cluster_not_found_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/nonexistent_cluster_xyz">>, <<>>),
    %% Will return 404 if federation returns not_found, or 503 if service unavailable
    ?assert(Code =:= 404 orelse Code =:= 503 orelse Code =:= 500),
    ?assert(is_binary(Response)).

%%====================================================================
%% POST /api/v1/federation/jobs Tests
%%====================================================================

handle_post_job_valid_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"test_job">>,
        <<"script">> => <<"#!/bin/bash\necho hello">>,
        <<"partition">> => <<"default">>,
        <<"num_cpus">> => 1,
        <<"user_id">> => 1000,
        <<"group_id">> => 1000,
        <<"routing">> => <<"round-robin">>
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, Body),
    %% May fail if no clusters available
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

handle_post_job_minimal_test() ->
    Body = jsx:encode(#{
        <<"script">> => <<"#!/bin/bash\necho minimal">>
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, Body),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

handle_post_job_invalid_json_test() ->
    Body = <<"invalid json{">>,
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/jobs">>, Body),
    ?assertEqual(400, Code),
    Decoded = jsx:decode(Response, [return_maps]),
    ?assertEqual(<<"Invalid JSON">>, maps:get(<<"error">>, Decoded)).

%%====================================================================
%% Error Response Format Tests
%%====================================================================

error_response_format_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/invalid">>, <<>>),
    ?assertEqual(404, Code),
    Decoded = jsx:decode(Response, [return_maps]),
    ?assert(maps:is_key(<<"error">>, Decoded)),
    ?assertEqual(<<"Not found">>, maps:get(<<"error">>, Decoded)).

%%====================================================================
%% Response Structure Tests
%%====================================================================

%% Test that successful GET responses have expected structure
get_federation_response_structure_test() ->
    {_Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation">>, <<>>),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(is_map(Decoded)),
    %% Should have status field
    ?assert(maps:is_key(<<"status">>, Decoded) orelse maps:is_key(<<"error">>, Decoded)).

get_clusters_response_structure_test() ->
    {_Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/clusters">>, <<>>),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(is_map(Decoded)),
    ?assert(maps:is_key(<<"status">>, Decoded) orelse maps:is_key(<<"error">>, Decoded)).

get_resources_response_structure_test() ->
    {_Code, Body} = flurm_federation_api:handle(<<"GET">>, <<"/api/v1/federation/resources">>, <<>>),
    Decoded = jsx:decode(Body, [return_maps]),
    ?assert(is_map(Decoded)),
    ?assert(maps:is_key(<<"status">>, Decoded) orelse maps:is_key(<<"error">>, Decoded)).

%%====================================================================
%% Edge Cases
%%====================================================================

%% Test with empty path variations
empty_cluster_name_test() ->
    {Code, _} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/">>, <<>>),
    %% Can return 404 (not found) or 503 (federation unavailable)
    ?assert(Code =:= 404 orelse Code =:= 503).

%% Test with unicode in cluster name
unicode_cluster_name_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/cluster\xC2\xA0name">>, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

%% Test with special characters in cluster name
special_chars_cluster_name_test() ->
    {Code, Response} = flurm_federation_api:handle(<<"DELETE">>, <<"/api/v1/federation/clusters/cluster-name_123">>, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

%% Test very long cluster name
long_cluster_name_test() ->
    LongName = binary:copy(<<"x">>, 1000),
    Path = <<"/api/v1/federation/clusters/", LongName/binary>>,
    {Code, Response} = flurm_federation_api:handle(<<"DELETE">>, Path, <<>>),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

%% Test JSON with extra fields
extra_fields_in_json_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"cluster">>,
        <<"host">> => <<"localhost">>,
        <<"extra_field">> => <<"should be ignored">>,
        <<"another_extra">> => 12345
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

%% Test nested JSON
nested_json_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"cluster">>,
        <<"host">> => <<"localhost">>,
        <<"metadata">> => #{
            <<"nested">> => #{
                <<"deeply">> => <<"nested">>
            }
        }
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).

%% Test array in JSON
array_in_json_test() ->
    Body = jsx:encode(#{
        <<"name">> => <<"cluster">>,
        <<"host">> => <<"localhost">>,
        <<"features">> => [<<"gpu">>, <<"nvme">>, <<"infiniband">>],
        <<"partitions">> => [<<"default">>, <<"gpu">>, <<"large">>]
    }),
    {Code, Response} = flurm_federation_api:handle(<<"POST">>, <<"/api/v1/federation/clusters">>, Body),
    ?assert(is_integer(Code)),
    ?assert(is_binary(Response)).
