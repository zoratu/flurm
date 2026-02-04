%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_bridge_http REST API handler
%%%
%%% Tests cover:
%%% - Path parsing and routing
%%% - JSON encoding/decoding
%%% - Mode validation
%%% - JWT authentication
%%% - Error handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_bridge_http_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Store original values to restore later
    Keys = [bridge_api_auth, bridge_mode, jwt_secret, jwt_expiry],
    OriginalValues = [{K, application:get_env(flurm_controller, K)} || K <- Keys],
    %% Disable auth for most tests
    application:set_env(flurm_controller, bridge_api_auth, disabled),
    OriginalValues.

cleanup(OriginalValues) ->
    lists:foreach(
        fun({Key, undefined}) ->
                application:unset_env(flurm_controller, Key);
           ({Key, {ok, Value}}) ->
                application:set_env(flurm_controller, Key, Value)
        end, OriginalValues).

%%====================================================================
%% Path Parsing Tests
%%====================================================================

parse_path_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"parse_path recognizes status endpoint",
       fun parse_path_status/0},
      {"parse_path recognizes mode endpoint",
       fun parse_path_mode/0},
      {"parse_path recognizes clusters list endpoint",
       fun parse_path_clusters_list/0},
      {"parse_path recognizes clusters with name endpoint",
       fun parse_path_clusters_name/0},
      {"parse_path handles unknown paths",
       fun parse_path_unknown/0}
     ]}.

parse_path_status() ->
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status">>)),
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status/">>)).

parse_path_mode() ->
    ?assertEqual({mode, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode">>)),
    ?assertEqual({mode, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode/">>)).

parse_path_clusters_list() ->
    ?assertEqual({clusters, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters">>)),
    ?assertEqual({clusters, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/">>)).

parse_path_clusters_name() ->
    ?assertEqual({clusters, <<"cluster1">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/cluster1">>)),
    ?assertEqual({clusters, <<"my-cluster">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/my-cluster">>)),
    ?assertEqual({clusters, <<"test">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/test/">>)).

parse_path_unknown() ->
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/other">>)),
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"/api/v2/bridge/status">>)),
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"">>)).

%%====================================================================
%% JSON Encoding/Decoding Tests
%%====================================================================

json_encoding_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"encode_response handles success",
       fun encode_response_success/0},
      {"encode_response handles error with map",
       fun encode_response_error_map/0},
      {"encode_response handles error with atom",
       fun encode_response_error_atom/0},
      {"decode_request handles valid JSON",
       fun decode_request_valid/0},
      {"decode_request handles empty body",
       fun decode_request_empty/0},
      {"decode_request handles invalid JSON",
       fun decode_request_invalid/0}
     ]}.

encode_response_success() ->
    Result = flurm_bridge_http:encode_response({ok, #{key => value}}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)),
    ?assert(maps:is_key(<<"data">>, Decoded)).

encode_response_error_map() ->
    Result = flurm_bridge_http:encode_response({error, #{reason => <<"test error">>}}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    ?assert(maps:is_key(<<"error">>, Decoded)).

encode_response_error_atom() ->
    Result = flurm_bridge_http:encode_response({error, some_error}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    Error = maps:get(<<"error">>, Decoded),
    ?assertEqual(<<"some_error">>, maps:get(<<"reason">>, Error)).

decode_request_valid() ->
    Body = jsx:encode(#{<<"name">> => <<"test">>, <<"port">> => 6817}),
    Decoded = flurm_bridge_http:decode_request(Body),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Decoded)),
    ?assertEqual(6817, maps:get(<<"port">>, Decoded)).

decode_request_empty() ->
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<>>)).

decode_request_invalid() ->
    %% Invalid JSON should return empty map
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<"not valid json">>)).

%%====================================================================
%% Mode Validation Tests
%%====================================================================

mode_validation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"binary_to_mode converts shadow",
       fun mode_shadow/0},
      {"binary_to_mode converts active",
       fun mode_active/0},
      {"binary_to_mode converts primary",
       fun mode_primary/0},
      {"binary_to_mode converts standalone",
       fun mode_standalone/0},
      {"binary_to_mode rejects invalid",
       fun mode_invalid/0}
     ]}.

mode_shadow() ->
    ?assertEqual(shadow, flurm_bridge_http:binary_to_mode(<<"shadow">>)).

mode_active() ->
    ?assertEqual(active, flurm_bridge_http:binary_to_mode(<<"active">>)).

mode_primary() ->
    ?assertEqual(primary, flurm_bridge_http:binary_to_mode(<<"primary">>)).

mode_standalone() ->
    ?assertEqual(standalone, flurm_bridge_http:binary_to_mode(<<"standalone">>)).

mode_invalid() ->
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"invalid_mode">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"SHADOW">>)).

%%====================================================================
%% Routes Configuration Tests
%%====================================================================

routes_test_() ->
    [
     {"routes returns valid cowboy routes",
      fun routes_valid/0},
     {"routes includes all required endpoints",
      fun routes_includes_endpoints/0}
    ].

routes_valid() ->
    Routes = flurm_bridge_http:routes(),
    ?assert(is_list(Routes)),
    ?assert(length(Routes) > 0),
    %% First element should be {Host, PathList}
    [{'_', PathList}] = Routes,
    ?assert(is_list(PathList)).

routes_includes_endpoints() ->
    [{'_', PathList}] = flurm_bridge_http:routes(),
    Paths = [Path || {Path, _, _} <- PathList],
    ?assert(lists:member("/api/v1/bridge/status", Paths)),
    ?assert(lists:member("/api/v1/bridge/mode", Paths)),
    ?assert(lists:member("/api/v1/bridge/clusters", Paths)),
    ?assert(lists:member("/api/v1/bridge/clusters/:name", Paths)).

%%====================================================================
%% JWT Integration Tests
%%====================================================================

jwt_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, jwt_secret, <<"test-secret-key">>),
         application:set_env(flurm_controller, jwt_expiry, 3600)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, jwt_secret),
         application:unset_env(flurm_controller, jwt_expiry)
     end,
     [
      {"generate and verify token",
       fun jwt_generate_verify/0},
      {"verify rejects expired token",
       fun jwt_expired/0},
      {"verify rejects invalid signature",
       fun jwt_invalid_signature/0},
      {"decode returns claims without verification",
       fun jwt_decode/0},
      {"token includes cluster_id claim",
       fun jwt_cluster_id/0},
      {"verify/2 accepts custom secret",
       fun jwt_custom_secret/0}
     ]}.

jwt_generate_verify() ->
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    ?assert(is_binary(Token)),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assertEqual(<<"testuser">>, maps:get(<<"sub">>, Claims)).

jwt_expired() ->
    %% Set very short expiry
    application:set_env(flurm_controller, jwt_expiry, -1),
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    application:set_env(flurm_controller, jwt_expiry, 3600),
    ?assertEqual({error, expired}, flurm_jwt:verify(Token)).

jwt_invalid_signature() ->
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    %% Tamper with the token
    TamperedToken = <<Token/binary, "x">>,
    Result = flurm_jwt:verify(TamperedToken),
    ?assertMatch({error, _}, Result).

jwt_decode() ->
    {ok, Token} = flurm_jwt:generate(<<"testuser">>, #{<<"role">> => <<"admin">>}),
    {ok, Claims} = flurm_jwt:decode(Token),
    ?assertEqual(<<"testuser">>, maps:get(<<"sub">>, Claims)),
    ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)).

jwt_cluster_id() ->
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    {ok, Claims} = flurm_jwt:decode(Token),
    ?assert(maps:is_key(<<"cluster_id">>, Claims)).

jwt_custom_secret() ->
    CustomSecret = <<"my-custom-secret-key">>,
    application:set_env(flurm_controller, jwt_secret, CustomSecret),
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    %% Verify with the same secret
    {ok, Claims} = flurm_jwt:verify(Token, CustomSecret),
    ?assertEqual(<<"testuser">>, maps:get(<<"sub">>, Claims)),
    %% Verify with different secret should fail
    Result = flurm_jwt:verify(Token, <<"wrong-secret">>),
    ?assertEqual({error, invalid_signature}, Result).

%%====================================================================
%% Route Request Tests (Mocked)
%%====================================================================

route_request_test_() ->
    {setup,
     fun() ->
         %% Start meck for flurm_federation
         meck:new(flurm_federation, [passthrough, non_strict]),
         meck:expect(flurm_federation, get_federation_stats, fun() ->
             #{clusters_total => 2, clusters_healthy => 2, clusters_unhealthy => 0}
         end),
         meck:expect(flurm_federation, get_federation_resources, fun() ->
             #{total_nodes => 10, total_cpus => 100}
         end),
         meck:expect(flurm_federation, is_federated, fun() -> true end),
         meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local">> end),
         meck:expect(flurm_federation, list_clusters, fun() ->
             [#{name => <<"cluster1">>}, #{name => <<"cluster2">>}]
         end),
         meck:expect(flurm_federation, get_cluster_status, fun(Name) ->
             case Name of
                 <<"cluster1">> -> {ok, #{name => <<"cluster1">>, state => up}};
                 _ -> {error, not_found}
             end
         end),
         meck:expect(flurm_federation, add_cluster, fun(Name, _Config) ->
             case Name of
                 <<"newcluster">> -> ok;
                 _ -> {error, already_exists}
             end
         end),
         meck:expect(flurm_federation, remove_cluster, fun(Name) ->
             case Name of
                 <<"cluster1">> -> ok;
                 <<"local">> -> {error, cannot_remove_local};
                 _ -> {error, not_found}
             end
         end),
         application:set_env(flurm_controller, bridge_mode, standalone),
         ok
     end,
     fun(_) ->
         meck:unload(flurm_federation),
         application:unset_env(flurm_controller, bridge_mode)
     end,
     [
      {"GET /status returns status",
       fun route_get_status/0},
      {"GET /mode returns current mode",
       fun route_get_mode/0},
      {"PUT /mode sets mode",
       fun route_put_mode/0},
      {"PUT /mode rejects invalid mode",
       fun route_put_mode_invalid/0},
      {"GET /clusters lists clusters",
       fun route_get_clusters/0},
      {"GET /clusters/:name returns cluster",
       fun route_get_cluster/0},
      {"POST /clusters adds cluster",
       fun route_post_cluster/0},
      {"POST /clusters validates required fields",
       fun route_post_cluster_validation/0},
      {"DELETE /clusters/:name removes cluster",
       fun route_delete_cluster/0}
     ]}.

route_get_status() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertMatch({ok, _}, Result),
    {ok, Status} = Result,
    ?assert(maps:is_key(mode, Status)),
    ?assert(maps:is_key(is_federated, Status)).

route_get_mode() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),
    ?assertMatch({ok, #{mode := _}}, Result).

route_put_mode() ->
    Data = #{<<"mode">> => <<"shadow">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({ok, #{mode := shadow, status := <<"updated">>}}, Result).

route_put_mode_invalid() ->
    Data = #{<<"mode">> => <<"invalid_mode">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({error, #{reason := _}}, Result).

route_get_clusters() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),
    ?assertMatch({ok, #{clusters := _, count := _}}, Result),
    {ok, #{count := Count}} = Result,
    ?assertEqual(2, Count).

route_get_cluster() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters/cluster1">>, undefined),
    ?assertMatch({ok, #{name := <<"cluster1">>}}, Result).

route_post_cluster() ->
    Data = #{<<"name">> => <<"newcluster">>, <<"host">> => <<"host1.example.com">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({ok, #{name := <<"newcluster">>, status := <<"added">>}}, Result).

route_post_cluster_validation() ->
    %% Missing name
    Data1 = #{<<"host">> => <<"host1.example.com">>},
    Result1 = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data1}),
    ?assertMatch({error, #{reason := <<"missing 'name' field">>}}, Result1),

    %% Missing host
    Data2 = #{<<"name">> => <<"cluster">>},
    Result2 = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data2}),
    ?assertMatch({error, #{reason := <<"missing 'host' field">>}}, Result2).

route_delete_cluster() ->
    Result = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/clusters/cluster1">>, undefined),
    ?assertMatch({ok, #{name := <<"cluster1">>, status := <<"removed">>}}, Result).

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"route_request handles unknown method",
       fun error_unknown_method/0},
      {"route_request handles unknown path",
       fun error_unknown_path/0}
     ]}.

error_unknown_method() ->
    Result = flurm_bridge_http:route_request(<<"PATCH">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertEqual({error, method_not_allowed}, Result).

error_unknown_path() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/unknown">>, undefined),
    ?assertEqual({error, not_found}, Result).
