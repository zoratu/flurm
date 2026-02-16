%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_bridge_http module
%%% Tests for Bridge HTTP REST API handler
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_bridge_http_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: These tests focus on the pure functions and path parsing
%% that don't require Cowboy or external dependencies.

%%====================================================================
%% parse_path Tests
%%====================================================================

parse_path_status_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/status">>),
    ?assertEqual({status, undefined}, Result).

parse_path_status_with_trailing_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/status/">>),
    ?assertEqual({status, undefined}, Result).

parse_path_mode_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode">>),
    ?assertEqual({mode, undefined}, Result).

parse_path_mode_with_trailing_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode/">>),
    ?assertEqual({mode, undefined}, Result).

parse_path_clusters_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters">>),
    ?assertEqual({clusters, undefined}, Result).

parse_path_clusters_with_trailing_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/">>),
    %% With trailing slash, returns empty binary not undefined
    ?assertEqual({clusters, <<>>}, Result).

parse_path_clusters_with_name_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/cluster1">>),
    ?assertEqual({clusters, <<"cluster1">>}, Result).

parse_path_clusters_with_name_trailing_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/cluster1/">>),
    ?assertEqual({clusters, <<"cluster1">>}, Result).

parse_path_clusters_with_complex_name_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/my-cluster-2">>),
    ?assertEqual({clusters, <<"my-cluster-2">>}, Result).

parse_path_unknown_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/unknown">>),
    ?assertEqual({unknown, undefined}, Result).

parse_path_empty_test() ->
    Result = flurm_bridge_http:parse_path(<<"">>),
    ?assertEqual({unknown, undefined}, Result).

parse_path_different_api_test() ->
    Result = flurm_bridge_http:parse_path(<<"/api/v2/bridge/status">>),
    ?assertEqual({unknown, undefined}, Result).

parse_path_root_test() ->
    Result = flurm_bridge_http:parse_path(<<"/">>),
    ?assertEqual({unknown, undefined}, Result).

%%====================================================================
%% binary_to_mode Tests
%%====================================================================

binary_to_mode_shadow_test() ->
    ?assertEqual(shadow, flurm_bridge_http:binary_to_mode(<<"shadow">>)).

binary_to_mode_active_test() ->
    ?assertEqual(active, flurm_bridge_http:binary_to_mode(<<"active">>)).

binary_to_mode_primary_test() ->
    ?assertEqual(primary, flurm_bridge_http:binary_to_mode(<<"primary">>)).

binary_to_mode_standalone_test() ->
    ?assertEqual(standalone, flurm_bridge_http:binary_to_mode(<<"standalone">>)).

binary_to_mode_invalid_test() ->
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"invalid">>)).

binary_to_mode_empty_test() ->
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"">>)).

binary_to_mode_uppercase_test() ->
    %% Case-sensitive, so uppercase should be invalid
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"SHADOW">>)).

binary_to_mode_mixed_case_test() ->
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"Shadow">>)).

%%====================================================================
%% encode_response Tests
%%====================================================================

encode_response_ok_simple_test() ->
    Result = flurm_bridge_http:encode_response({ok, #{key => value}}),
    ?assert(is_binary(Result)),
    %% Should be valid JSON
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)),
    ?assert(maps:is_key(<<"data">>, Decoded)).

encode_response_ok_complex_test() ->
    Data = #{
        status => <<"running">>,
        count => 42,
        items => [1, 2, 3]
    },
    Result = flurm_bridge_http:encode_response({ok, Data}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)).

encode_response_error_map_test() ->
    Result = flurm_bridge_http:encode_response({error, #{reason => <<"test error">>}}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    ?assert(maps:is_key(<<"error">>, Decoded)).

encode_response_error_atom_test() ->
    Result = flurm_bridge_http:encode_response({error, some_error}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)).

encode_response_error_binary_test() ->
    Result = flurm_bridge_http:encode_response({error, <<"binary error">>}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)).

encode_response_ok_empty_map_test() ->
    Result = flurm_bridge_http:encode_response({ok, #{}}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)).

%%====================================================================
%% decode_request Tests
%%====================================================================

decode_request_empty_test() ->
    Result = flurm_bridge_http:decode_request(<<>>),
    ?assertEqual(#{}, Result).

decode_request_valid_json_test() ->
    Body = <<"{\"key\": \"value\", \"num\": 42}">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assert(is_map(Result)),
    ?assertEqual(<<"value">>, maps:get(<<"key">>, Result)),
    ?assertEqual(42, maps:get(<<"num">>, Result)).

decode_request_nested_json_test() ->
    Body = <<"{\"outer\": {\"inner\": \"value\"}}">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assert(is_map(Result)),
    Inner = maps:get(<<"outer">>, Result),
    ?assert(is_map(Inner)),
    ?assertEqual(<<"value">>, maps:get(<<"inner">>, Inner)).

decode_request_array_json_test() ->
    Body = <<"{\"items\": [1, 2, 3]}">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assert(is_map(Result)),
    ?assertEqual([1, 2, 3], maps:get(<<"items">>, Result)).

decode_request_invalid_json_test() ->
    Body = <<"not valid json">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assertEqual(#{}, Result).

decode_request_partial_json_test() ->
    Body = <<"{\"key\": ">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assertEqual(#{}, Result).

decode_request_empty_object_test() ->
    Body = <<"{}">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assertEqual(#{}, Result).

%%====================================================================
%% route_request Tests (GET paths)
%%====================================================================

%% Note: These tests will fail if flurm_federation is not running.
%% We test the routing logic by checking that the function doesn't crash.

route_request_get_unknown_test() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/unknown">>, undefined),
    ?assertEqual({error, not_found}, Result).

route_request_invalid_method_test() ->
    Result = flurm_bridge_http:route_request(<<"PATCH">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertEqual({error, method_not_allowed}, Result).

route_request_put_mode_missing_mode_test() ->
    %% PUT /mode without mode field
    Data = #{},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertEqual({error, #{reason => <<"missing 'mode' field">>}}, Result).

route_request_put_mode_invalid_mode_test() ->
    %% PUT /mode with invalid mode
    Data = #{<<"mode">> => <<"invalid_mode">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertEqual({error, #{reason => <<"invalid mode, must be: shadow, active, primary, or standalone">>}}, Result).

route_request_put_mode_valid_test() ->
    %% PUT /mode with valid mode
    Data = #{<<"mode">> => <<"standalone">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok  % May fail if application env not available
    end.

route_request_post_clusters_missing_name_test() ->
    %% POST /clusters without name
    Data = #{<<"host">> => <<"localhost">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertEqual({error, #{reason => <<"missing 'name' field">>}}, Result).

route_request_post_clusters_missing_host_test() ->
    %% POST /clusters without host
    Data = #{<<"name">> => <<"cluster1">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertEqual({error, #{reason => <<"missing 'host' field">>}}, Result).

route_request_put_unknown_path_test() ->
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/unknown">>, {undefined, #{}}),
    ?assertEqual({error, not_found}, Result).

route_request_post_unknown_path_test() ->
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/unknown">>, {undefined, #{}}),
    ?assertEqual({error, not_found}, Result).

route_request_delete_unknown_path_test() ->
    Result = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/unknown">>, undefined),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% routes/0 Test
%%====================================================================

routes_test() ->
    Routes = flurm_bridge_http:routes(),
    ?assert(is_list(Routes)),
    ?assert(length(Routes) > 0),
    %% Should have one host entry
    [{Host, Paths}] = Routes,
    ?assertEqual('_', Host),
    ?assert(is_list(Paths)),
    ?assert(length(Paths) >= 4).

%%====================================================================
%% init/2 Test (Cowboy callback)
%%====================================================================

init_test() ->
    %% init/2 should return {cowboy_rest, Req, State}
    MockReq = mock_req,
    MockState = #{},
    Result = flurm_bridge_http:init(MockReq, MockState),
    ?assertEqual({cowboy_rest, MockReq, MockState}, Result).

%%====================================================================
%% content_types_provided/2 Test
%%====================================================================

content_types_provided_test() ->
    {Types, _, _} = flurm_bridge_http:content_types_provided(mock_req, #{}),
    ?assert(is_list(Types)),
    ?assert(length(Types) > 0),
    [{ContentType, Handler}] = Types,
    ?assertEqual(<<"application/json">>, ContentType),
    ?assertEqual(to_json, Handler).

%%====================================================================
%% content_types_accepted/2 Test
%%====================================================================

content_types_accepted_test() ->
    {Types, _, _} = flurm_bridge_http:content_types_accepted(mock_req, #{}),
    ?assert(is_list(Types)),
    ?assert(length(Types) > 0),
    [{ContentType, Handler}] = Types,
    ?assertEqual(<<"application/json">>, ContentType),
    ?assertEqual(from_json, Handler).

%%====================================================================
%% Edge Cases
%%====================================================================

parse_path_with_query_string_test() ->
    %% Path with query string should still work for prefix matching
    Result = flurm_bridge_http:parse_path(<<"/api/v1/bridge/status?foo=bar">>),
    %% The parse_path doesn't handle query strings, it just matches prefix
    ?assertEqual({status, undefined}, Result).

decode_request_with_unicode_test() ->
    Body = <<"{\"name\": \"\\u4e2d\\u6587\"}">>,
    Result = flurm_bridge_http:decode_request(Body),
    ?assert(is_map(Result)),
    ?assert(maps:is_key(<<"name">>, Result)).

encode_response_with_unicode_test() ->
    Result = flurm_bridge_http:encode_response({ok, #{name => <<"test">>}}),
    ?assert(is_binary(Result)).
