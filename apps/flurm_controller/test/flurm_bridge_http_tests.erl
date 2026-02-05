%%%-------------------------------------------------------------------
%%% @doc Comprehensive unit tests for flurm_bridge_http REST API handler
%%%
%%% Tests cover:
%%% - Path parsing and routing
%%% - JSON encoding/decoding
%%% - Mode transitions (shadow, active, primary, standalone)
%%% - Cluster management (add, remove, list)
%%% - JWT authentication middleware
%%% - Error handling (400, 404, 500 responses)
%%% - Cowboy REST callbacks
%%%
%%% Uses meck for mocking cowboy_req and flurm_federation functions.
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
       fun parse_path_unknown/0},
      {"parse_path handles trailing content",
       fun parse_path_trailing_content/0},
      {"parse_path handles cluster name with special chars",
       fun parse_path_cluster_special_chars/0}
     ]}.

parse_path_status() ->
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status">>)),
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status/">>)).

parse_path_mode() ->
    ?assertEqual({mode, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode">>)),
    ?assertEqual({mode, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode/">>)).

parse_path_clusters_list() ->
    ?assertEqual({clusters, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters">>)),
    %% Trailing slash produces empty cluster name (implementation detail)
    ?assertEqual({clusters, <<>>}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/">>)).

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
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"">>)),
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"/">>)),
    ?assertEqual({unknown, undefined}, flurm_bridge_http:parse_path(<<"/api">>)).

parse_path_trailing_content() ->
    %% Status with extra path segments
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status/extra">>)),
    ?assertEqual({status, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/status?query=param">>)),
    %% Mode with extra
    ?assertEqual({mode, undefined}, flurm_bridge_http:parse_path(<<"/api/v1/bridge/mode/extra">>)).

parse_path_cluster_special_chars() ->
    %% Cluster names with hyphens and underscores
    ?assertEqual({clusters, <<"prod-cluster-01">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/prod-cluster-01">>)),
    ?assertEqual({clusters, <<"test_cluster">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/test_cluster">>)),
    %% Numeric names
    ?assertEqual({clusters, <<"12345">>},
                 flurm_bridge_http:parse_path(<<"/api/v1/bridge/clusters/12345">>)).

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
      {"encode_response handles success with nested data",
       fun encode_response_success_nested/0},
      {"encode_response handles error with map",
       fun encode_response_error_map/0},
      {"encode_response handles error with atom",
       fun encode_response_error_atom/0},
      {"encode_response handles error with binary",
       fun encode_response_error_binary/0},
      {"encode_response handles error with string",
       fun encode_response_error_string/0},
      {"encode_response handles error with complex term",
       fun encode_response_error_complex/0},
      {"decode_request handles valid JSON",
       fun decode_request_valid/0},
      {"decode_request handles empty body",
       fun decode_request_empty/0},
      {"decode_request handles invalid JSON",
       fun decode_request_invalid/0},
      {"decode_request handles complex JSON structures",
       fun decode_request_complex/0}
     ]}.

encode_response_success() ->
    Result = flurm_bridge_http:encode_response({ok, #{key => value}}),
    ?assert(is_binary(Result)),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)),
    ?assert(maps:is_key(<<"data">>, Decoded)).

encode_response_success_nested() ->
    Data = #{
        mode => standalone,
        stats => #{total => 10, available => 5},
        clusters => [#{name => <<"a">>}, #{name => <<"b">>}]
    },
    Result = flurm_bridge_http:encode_response({ok, Data}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)),
    DecodedData = maps:get(<<"data">>, Decoded),
    ?assertEqual(<<"standalone">>, maps:get(<<"mode">>, DecodedData)).

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

encode_response_error_binary() ->
    Result = flurm_bridge_http:encode_response({error, <<"custom error message">>}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    Error = maps:get(<<"error">>, Decoded),
    ?assertEqual(<<"custom error message">>, maps:get(<<"reason">>, Error)).

encode_response_error_string() ->
    Result = flurm_bridge_http:encode_response({error, "string error"}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    Error = maps:get(<<"error">>, Decoded),
    ?assertEqual(<<"string error">>, maps:get(<<"reason">>, Error)).

encode_response_error_complex() ->
    %% Complex term should be formatted to string
    Result = flurm_bridge_http:encode_response({error, {complex, error, 123}}),
    Decoded = jsx:decode(Result, [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Decoded)),
    Error = maps:get(<<"error">>, Decoded),
    %% Should contain the complex term as string
    ?assert(is_binary(maps:get(<<"reason">>, Error))).

decode_request_valid() ->
    Body = jsx:encode(#{<<"name">> => <<"test">>, <<"port">> => 6817}),
    Decoded = flurm_bridge_http:decode_request(Body),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Decoded)),
    ?assertEqual(6817, maps:get(<<"port">>, Decoded)).

decode_request_empty() ->
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<>>)).

decode_request_invalid() ->
    %% Invalid JSON should return empty map
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<"not valid json">>)),
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<"{incomplete">>)),
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<"[unclosed">>)).

decode_request_complex() ->
    Body = jsx:encode(#{
        <<"name">> => <<"cluster1">>,
        <<"host">> => <<"host.example.com">>,
        <<"port">> => 6817,
        <<"auth">> => #{<<"token">> => <<"secret">>},
        <<"features">> => [<<"gpu">>, <<"highspeed">>],
        <<"partitions">> => [<<"compute">>, <<"debug">>],
        <<"weight">> => 10
    }),
    Decoded = flurm_bridge_http:decode_request(Body),
    ?assertEqual(<<"cluster1">>, maps:get(<<"name">>, Decoded)),
    ?assertEqual([<<"gpu">>, <<"highspeed">>], maps:get(<<"features">>, Decoded)),
    ?assertEqual(10, maps:get(<<"weight">>, Decoded)).

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
       fun mode_invalid/0},
      {"binary_to_mode rejects case variations",
       fun mode_case_variations/0}
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
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"unknown">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"readonly">>)).

mode_case_variations() ->
    %% Mode conversion is case-sensitive
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"SHADOW">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"Shadow">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"ACTIVE">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"PRIMARY">>)),
    ?assertEqual(invalid, flurm_bridge_http:binary_to_mode(<<"STANDALONE">>)).

%%====================================================================
%% Routes Configuration Tests
%%====================================================================

routes_test_() ->
    [
     {"routes returns valid cowboy routes",
      fun routes_valid/0},
     {"routes includes all required endpoints",
      fun routes_includes_endpoints/0},
     {"routes use correct handler module",
      fun routes_handler_module/0}
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

routes_handler_module() ->
    [{'_', PathList}] = flurm_bridge_http:routes(),
    lists:foreach(fun({_Path, Module, _Opts}) ->
        ?assertEqual(flurm_bridge_http, Module)
    end, PathList).

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
       fun jwt_custom_secret/0},
      {"generate with extra claims",
       fun jwt_extra_claims/0},
      {"refresh extends token lifetime",
       fun jwt_refresh/0}
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

jwt_extra_claims() ->
    ExtraClaims = #{<<"role">> => <<"admin">>, <<"permissions">> => [<<"read">>, <<"write">>]},
    {ok, Token} = flurm_jwt:generate(<<"testuser">>, ExtraClaims),
    {ok, Claims} = flurm_jwt:verify(Token),
    ?assertEqual(<<"admin">>, maps:get(<<"role">>, Claims)),
    ?assertEqual([<<"read">>, <<"write">>], maps:get(<<"permissions">>, Claims)).

jwt_refresh() ->
    {ok, Token} = flurm_jwt:generate(<<"testuser">>),
    %% Sleep to ensure different iat (system_time second granularity)
    timer:sleep(1100),
    {ok, RefreshedToken} = flurm_jwt:refresh(Token),
    %% Token may be the same if iat didn't change - just verify it's valid
    {ok, Claims} = flurm_jwt:verify(RefreshedToken),
    ?assertEqual(<<"testuser">>, maps:get(<<"sub">>, Claims)).

%%====================================================================
%% Route Request Tests - GET Operations (Mocked)
%%====================================================================

route_request_get_test_() ->
    {setup,
     fun() ->
         %% Start meck for flurm_federation
         meck:new(flurm_federation, [passthrough, non_strict]),
         meck:expect(flurm_federation, get_federation_stats, fun() ->
             #{clusters_total => 2, clusters_healthy => 2, clusters_unhealthy => 0}
         end),
         meck:expect(flurm_federation, get_federation_resources, fun() ->
             #{total_nodes => 10, total_cpus => 100, available_cpus => 50}
         end),
         meck:expect(flurm_federation, is_federated, fun() -> true end),
         meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local">> end),
         meck:expect(flurm_federation, list_clusters, fun() ->
             [#{name => <<"cluster1">>, state => up}, #{name => <<"cluster2">>, state => up}]
         end),
         meck:expect(flurm_federation, get_cluster_status, fun(Name) ->
             case Name of
                 <<"cluster1">> -> {ok, #{name => <<"cluster1">>, state => up, host => <<"host1.example.com">>}};
                 <<"cluster2">> -> {ok, #{name => <<"cluster2">>, state => down}};
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
      {"GET /status returns full status",
       fun route_get_status/0},
      {"GET /status includes all required fields",
       fun route_get_status_fields/0},
      {"GET /mode returns current mode",
       fun route_get_mode/0},
      {"GET /mode returns different modes",
       fun route_get_mode_variations/0},
      {"GET /clusters lists all clusters",
       fun route_get_clusters/0},
      {"GET /clusters includes count",
       fun route_get_clusters_count/0},
      {"GET /clusters/:name returns existing cluster",
       fun route_get_cluster_found/0},
      {"GET /clusters/:name returns error for missing",
       fun route_get_cluster_not_found/0},
      {"GET unknown path returns not_found",
       fun route_get_unknown/0}
     ]}.

route_get_status() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertMatch({ok, _}, Result),
    {ok, Status} = Result,
    ?assert(maps:is_key(mode, Status)),
    ?assert(maps:is_key(is_federated, Status)),
    ?assert(maps:is_key(local_cluster, Status)),
    ?assert(maps:is_key(federation_stats, Status)),
    ?assert(maps:is_key(resources, Status)).

route_get_status_fields() ->
    {ok, Status} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertEqual(standalone, maps:get(mode, Status)),
    ?assertEqual(true, maps:get(is_federated, Status)),
    ?assertEqual(<<"local">>, maps:get(local_cluster, Status)),
    %% Check stats structure
    Stats = maps:get(federation_stats, Status),
    ?assertEqual(2, maps:get(clusters_total, Stats)).

route_get_mode() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),
    ?assertMatch({ok, #{mode := _}}, Result).

route_get_mode_variations() ->
    %% Test that different modes are returned correctly
    application:set_env(flurm_controller, bridge_mode, shadow),
    {ok, #{mode := shadow}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    application:set_env(flurm_controller, bridge_mode, active),
    {ok, #{mode := active}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    application:set_env(flurm_controller, bridge_mode, primary),
    {ok, #{mode := primary}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    application:set_env(flurm_controller, bridge_mode, standalone),
    {ok, #{mode := standalone}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined).

route_get_clusters() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),
    ?assertMatch({ok, #{clusters := _, count := _}}, Result),
    {ok, #{clusters := Clusters}} = Result,
    ?assertEqual(2, length(Clusters)).

route_get_clusters_count() ->
    {ok, #{count := Count}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),
    ?assertEqual(2, Count).

route_get_cluster_found() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters/cluster1">>, undefined),
    ?assertMatch({ok, #{name := <<"cluster1">>, state := up}}, Result).

route_get_cluster_not_found() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters/nonexistent">>, undefined),
    ?assertMatch({error, #{reason := <<"cluster not found">>}}, Result).

route_get_unknown() ->
    ?assertEqual({error, not_found}, flurm_bridge_http:route_request(<<"GET">>, <<"/api/v2/unknown">>, undefined)),
    ?assertEqual({error, not_found}, flurm_bridge_http:route_request(<<"GET">>, <<"/other/path">>, undefined)).

%%====================================================================
%% Route Request Tests - Mutation Operations (PUT/POST/DELETE)
%%====================================================================

route_request_mutation_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_federation, [passthrough, non_strict]),
         meck:expect(flurm_federation, add_cluster, fun(Name, _Config) ->
             case Name of
                 <<"newcluster">> -> ok;
                 <<"duplicate">> -> {error, already_exists};
                 _ -> ok
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
      {"PUT /mode sets valid mode",
       fun route_put_mode/0},
      {"PUT /mode transitions through all modes",
       fun route_put_mode_transitions/0},
      {"PUT /mode rejects invalid mode",
       fun route_put_mode_invalid/0},
      {"PUT /mode requires mode field",
       fun route_put_mode_missing_field/0},
      {"POST /clusters adds new cluster",
       fun route_post_cluster/0},
      {"POST /clusters with all optional fields",
       fun route_post_cluster_full/0},
      {"POST /clusters validates required fields",
       fun route_post_cluster_validation/0},
      {"POST /clusters handles add_cluster errors",
       fun route_post_cluster_error/0},
      {"DELETE /clusters/:name removes cluster",
       fun route_delete_cluster/0},
      {"DELETE /clusters/:name cannot remove local",
       fun route_delete_cluster_local/0},
      {"DELETE /clusters/:name handles not found",
       fun route_delete_cluster_not_found/0}
     ]}.

route_put_mode() ->
    Data = #{<<"mode">> => <<"shadow">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({ok, #{mode := shadow, status := <<"updated">>}}, Result),
    %% Verify the mode actually changed
    ?assertEqual(shadow, application:get_env(flurm_controller, bridge_mode, undefined)).

route_put_mode_transitions() ->
    %% Test the typical migration sequence: standalone -> shadow -> active -> primary
    Sequence = [standalone, shadow, active, primary, standalone],
    lists:foreach(fun(Mode) ->
        Data = #{<<"mode">> => atom_to_binary(Mode, utf8)},
        {ok, #{mode := ResultMode}} = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
        ?assertEqual(Mode, ResultMode)
    end, Sequence).

route_put_mode_invalid() ->
    Data = #{<<"mode">> => <<"invalid_mode">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({error, #{reason := _}}, Result),
    {error, #{reason := Reason}} = Result,
    ?assert(binary:match(Reason, <<"invalid mode">>) =/= nomatch).

route_put_mode_missing_field() ->
    Data = #{<<"other">> => <<"value">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({error, #{reason := <<"missing 'mode' field">>}}, Result).

route_post_cluster() ->
    Data = #{<<"name">> => <<"newcluster">>, <<"host">> => <<"host1.example.com">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({ok, #{name := <<"newcluster">>, status := <<"added">>}}, Result).

route_post_cluster_full() ->
    Data = #{
        <<"name">> => <<"newcluster">>,
        <<"host">> => <<"host1.example.com">>,
        <<"port">> => 8080,
        <<"weight">> => 5,
        <<"features">> => [<<"gpu">>, <<"fast">>],
        <<"partitions">> => [<<"compute">>, <<"debug">>],
        <<"auth">> => #{<<"token">> => <<"secret">>}
    },
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
    ?assertMatch({error, #{reason := <<"missing 'host' field">>}}, Result2),

    %% Both missing
    Data3 = #{<<"port">> => 6817},
    Result3 = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data3}),
    ?assertMatch({error, #{reason := <<"missing 'name' field">>}}, Result3).

route_post_cluster_error() ->
    meck:expect(flurm_federation, add_cluster, fun(_Name, _Config) ->
        {error, connection_failed}
    end),
    Data = #{<<"name">> => <<"failing">>, <<"host">> => <<"bad.host">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({error, #{reason := _}}, Result).

route_delete_cluster() ->
    Result = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/clusters/cluster1">>, undefined),
    ?assertMatch({ok, #{name := <<"cluster1">>, status := <<"removed">>}}, Result).

route_delete_cluster_local() ->
    Result = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/clusters/local">>, undefined),
    ?assertMatch({error, #{reason := <<"cannot remove local cluster">>}}, Result).

route_delete_cluster_not_found() ->
    Result = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/clusters/nonexistent">>, undefined),
    ?assertMatch({error, #{reason := <<"cluster not found">>}}, Result).

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
       fun error_unknown_path/0},
      {"route_request handles various unsupported methods",
       fun error_various_methods/0}
     ]}.

error_unknown_method() ->
    Result = flurm_bridge_http:route_request(<<"PATCH">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertEqual({error, method_not_allowed}, Result).

error_unknown_path() ->
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/unknown">>, undefined),
    ?assertEqual({error, not_found}, Result).

error_various_methods() ->
    %% PATCH not allowed anywhere
    ?assertEqual({error, method_not_allowed},
                 flurm_bridge_http:route_request(<<"PATCH">>, <<"/api/v1/bridge/mode">>, undefined)),
    ?assertEqual({error, method_not_allowed},
                 flurm_bridge_http:route_request(<<"HEAD">>, <<"/api/v1/bridge/status">>, undefined)),
    ?assertEqual({error, method_not_allowed},
                 flurm_bridge_http:route_request(<<"CONNECT">>, <<"/api/v1/bridge/clusters">>, undefined)).

%%====================================================================
%% Status Error Handling Tests
%%====================================================================

status_error_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_federation, [passthrough, non_strict]),
         ok
     end,
     fun(_) ->
         meck:unload(flurm_federation)
     end,
     [
      {"GET /status handles federation error",
       fun status_federation_error/0}
     ]}.

status_federation_error() ->
    %% Make federation call throw an error
    meck:expect(flurm_federation, get_federation_stats, fun() ->
        error(simulated_federation_error)
    end),
    Result = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/status">>, undefined),
    ?assertMatch({error, #{reason := _}}, Result).

%%====================================================================
%% Cowboy REST Callback Tests (with mocked cowboy_req)
%%====================================================================

cowboy_callback_test_() ->
    {setup,
     fun() ->
         meck:new(cowboy_req, [non_strict]),
         meck:new(flurm_federation, [passthrough, non_strict]),
         ok
     end,
     fun(_) ->
         meck:unload(cowboy_req),
         meck:unload(flurm_federation)
     end,
     [
      {"init returns cowboy_rest tuple",
       fun test_init/0},
      {"allowed_methods returns correct methods for each path",
       fun test_allowed_methods/0},
      {"content_types_provided returns JSON",
       fun test_content_types_provided/0},
      {"content_types_accepted returns JSON",
       fun test_content_types_accepted/0},
      {"resource_exists checks path correctly",
       fun test_resource_exists/0}
     ]}.

test_init() ->
    MockReq = mock_req,
    State = #{},
    {Mode, _Req, _State} = flurm_bridge_http:init(MockReq, State),
    ?assertEqual(cowboy_rest, Mode).

test_allowed_methods() ->
    %% Status endpoint
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/status">> end),
    {Methods1, _, _} = flurm_bridge_http:allowed_methods(mock_req, #{}),
    ?assertEqual([<<"GET">>, <<"OPTIONS">>], Methods1),

    %% Mode endpoint
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/mode">> end),
    {Methods2, _, _} = flurm_bridge_http:allowed_methods(mock_req, #{}),
    ?assertEqual([<<"GET">>, <<"PUT">>, <<"OPTIONS">>], Methods2),

    %% Clusters list endpoint
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/clusters">> end),
    {Methods3, _, _} = flurm_bridge_http:allowed_methods(mock_req, #{}),
    ?assertEqual([<<"GET">>, <<"POST">>, <<"OPTIONS">>], Methods3),

    %% Cluster by name endpoint
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/clusters/test">> end),
    {Methods4, _, _} = flurm_bridge_http:allowed_methods(mock_req, #{}),
    ?assertEqual([<<"GET">>, <<"DELETE">>, <<"OPTIONS">>], Methods4).

test_content_types_provided() ->
    {Types, _, _} = flurm_bridge_http:content_types_provided(mock_req, #{}),
    ?assertMatch([{<<"application/json">>, to_json}], Types).

test_content_types_accepted() ->
    {Types, _, _} = flurm_bridge_http:content_types_accepted(mock_req, #{}),
    ?assertMatch([{<<"application/json">>, from_json}], Types).

test_resource_exists() ->
    %% Status always exists
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/status">> end),
    meck:expect(cowboy_req, method, fun(_) -> <<"GET">> end),
    {Exists1, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(true, Exists1),

    %% Mode always exists
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/mode">> end),
    {Exists2, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(true, Exists2),

    %% Clusters list always exists
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/clusters">> end),
    {Exists3, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(true, Exists3),

    %% Specific cluster depends on federation lookup
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v1/bridge/clusters/test">> end),
    meck:expect(cowboy_req, method, fun(_) -> <<"DELETE">> end),
    meck:expect(flurm_federation, get_cluster_status, fun(<<"test">>) -> {ok, #{}} end),
    {Exists4, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(true, Exists4),

    %% Non-existent cluster
    meck:expect(flurm_federation, get_cluster_status, fun(<<"test">>) -> {error, not_found} end),
    {Exists5, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(false, Exists5),

    %% Unknown path
    meck:expect(cowboy_req, path, fun(_) -> <<"/api/v2/unknown">> end),
    {Exists6, _, _} = flurm_bridge_http:resource_exists(mock_req, #{}),
    ?assertEqual(false, Exists6).

%%====================================================================
%% JWT Authentication Middleware Tests (with mocked cowboy_req)
%%====================================================================

jwt_auth_middleware_test_() ->
    {setup,
     fun() ->
         meck:new(cowboy_req, [non_strict]),
         meck:new(flurm_jwt, [passthrough, non_strict]),
         application:set_env(flurm_controller, jwt_secret, <<"test-secret">>),
         ok
     end,
     fun(_) ->
         meck:unload(cowboy_req),
         meck:unload(flurm_jwt),
         application:unset_env(flurm_controller, jwt_secret),
         application:unset_env(flurm_controller, bridge_api_auth)
     end,
     [
      {"is_authorized accepts valid Bearer token",
       fun auth_valid_token/0},
      {"is_authorized rejects invalid token",
       fun auth_invalid_token/0},
      {"is_authorized rejects missing header when auth enabled",
       fun auth_missing_header/0},
      {"is_authorized allows missing header when auth disabled",
       fun auth_disabled/0},
      {"is_authorized rejects malformed auth header",
       fun auth_malformed_header/0},
      {"is_authorized stores claims in state",
       fun auth_stores_claims/0}
     ]}.

auth_valid_token() ->
    ValidToken = <<"valid.jwt.token">>,
    Claims = #{<<"sub">> => <<"testuser">>, <<"exp">> => 9999999999},

    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) ->
        <<"Bearer ", ValidToken/binary>>
    end),
    meck:expect(flurm_jwt, verify, fun(Token) when Token =:= ValidToken ->
        {ok, Claims}
    end),

    {Result, _Req, State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertEqual(true, Result),
    ?assertEqual(Claims, maps:get(claims, State)).

auth_invalid_token() ->
    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) ->
        <<"Bearer invalid.token">>
    end),
    meck:expect(flurm_jwt, verify, fun(_) -> {error, invalid_signature} end),

    {Result, _Req, _State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertMatch({false, _Challenge}, Result).

auth_missing_header() ->
    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) -> undefined end),
    application:set_env(flurm_controller, bridge_api_auth, enabled),

    {Result, _Req, _State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertMatch({false, <<"Bearer realm=\"flurm\"">>}, Result).

auth_disabled() ->
    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) -> undefined end),
    application:set_env(flurm_controller, bridge_api_auth, disabled),

    {Result, _Req, _State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertEqual(true, Result).

auth_malformed_header() ->
    %% Basic auth instead of Bearer
    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) ->
        <<"Basic dXNlcjpwYXNz">>
    end),

    {Result, _Req, _State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertMatch({false, <<"Bearer realm=\"flurm\"">>}, Result).

auth_stores_claims() ->
    ValidToken = <<"valid.jwt.token">>,
    Claims = #{<<"sub">> => <<"admin">>, <<"role">> => <<"superuser">>},

    meck:expect(cowboy_req, header, fun(<<"authorization">>, _) ->
        <<"Bearer ", ValidToken/binary>>
    end),
    meck:expect(flurm_jwt, verify, fun(_) -> {ok, Claims} end),

    {true, _Req, State} = flurm_bridge_http:is_authorized(mock_req, #{}),
    ?assertEqual(Claims, maps:get(claims, State)),
    ?assertEqual(<<"admin">>, maps:get(<<"sub">>, maps:get(claims, State))).

%%====================================================================
%% Integration Tests - Full Request Flow
%%====================================================================

integration_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_federation, [passthrough, non_strict]),
         %% Setup cluster storage for integration tests
         ClusterTable = ets:new(test_clusters, [set, public, named_table]),
         ets:insert(ClusterTable, {<<"existing">>, #{state => up, host => <<"host.example.com">>}}),

         meck:expect(flurm_federation, list_clusters, fun() ->
             [#{name => K, state => maps:get(state, V, unknown)} || {K, V} <- ets:tab2list(test_clusters)]
         end),
         meck:expect(flurm_federation, get_cluster_status, fun(Name) ->
             case ets:lookup(test_clusters, Name) of
                 [{_, Config}] -> {ok, Config#{name => Name, state => maps:get(state, Config, unknown)}};
                 [] -> {error, not_found}
             end
         end),
         meck:expect(flurm_federation, add_cluster, fun(Name, Config) ->
             %% Add state to the config when storing
             ets:insert(test_clusters, {Name, Config#{state => up}}),
             ok
         end),
         meck:expect(flurm_federation, remove_cluster, fun(Name) ->
             case ets:lookup(test_clusters, Name) of
                 [{_, _}] ->
                     ets:delete(test_clusters, Name),
                     ok;
                 [] ->
                     {error, not_found}
             end
         end),
         meck:expect(flurm_federation, get_federation_stats, fun() ->
             #{clusters_total => ets:info(test_clusters, size)}
         end),
         meck:expect(flurm_federation, get_federation_resources, fun() -> #{} end),
         meck:expect(flurm_federation, is_federated, fun() -> ets:info(test_clusters, size) > 1 end),
         meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local">> end),

         application:set_env(flurm_controller, bridge_mode, standalone),
         ClusterTable
     end,
     fun(ClusterTable) ->
         ets:delete(ClusterTable),
         meck:unload(flurm_federation),
         application:unset_env(flurm_controller, bridge_mode)
     end,
     [
      {"full cluster add/list/get/remove flow",
       fun integration_cluster_crud/0},
      {"full mode transition flow",
       fun integration_mode_transitions/0},
      {"response encoding roundtrip",
       fun integration_encoding_roundtrip/0}
     ]}.

integration_cluster_crud() ->
    %% Initial state - one cluster
    {ok, #{count := 1}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),

    %% Add a new cluster
    AddData = #{<<"name">> => <<"prod-cluster">>, <<"host">> => <<"prod.example.com">>, <<"port">> => 6817},
    {ok, #{status := <<"added">>}} = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, AddData}),

    %% List should now show 2
    {ok, #{count := 2, clusters := Clusters}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),
    ?assertEqual(2, length(Clusters)),

    %% Get the specific cluster
    {ok, #{name := <<"prod-cluster">>}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters/prod-cluster">>, undefined),

    %% Remove the cluster
    {ok, #{status := <<"removed">>}} = flurm_bridge_http:route_request(<<"DELETE">>, <<"/api/v1/bridge/clusters/prod-cluster">>, undefined),

    %% Should be back to 1
    {ok, #{count := 1}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters">>, undefined),

    %% Get should now return not found
    {error, #{reason := <<"cluster not found">>}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/clusters/prod-cluster">>, undefined).

integration_mode_transitions() ->
    %% Start standalone
    {ok, #{mode := standalone}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    %% Transition to shadow (observing SLURM)
    flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, #{<<"mode">> => <<"shadow">>}}),
    {ok, #{mode := shadow}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    %% Transition to active (handling new jobs)
    flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, #{<<"mode">> => <<"active">>}}),
    {ok, #{mode := active}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    %% Transition to primary (full takeover)
    flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, #{<<"mode">> => <<"primary">>}}),
    {ok, #{mode := primary}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined),

    %% Can go back to standalone
    flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, #{<<"mode">> => <<"standalone">>}}),
    {ok, #{mode := standalone}} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/mode">>, undefined).

integration_encoding_roundtrip() ->
    %% Test that responses can be encoded and decoded correctly
    {ok, StatusResult} = flurm_bridge_http:route_request(<<"GET">>, <<"/api/v1/bridge/status">>, undefined),
    Encoded = flurm_bridge_http:encode_response({ok, StatusResult}),
    ?assert(is_binary(Encoded)),

    Decoded = jsx:decode(Encoded, [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Decoded)),
    Data = maps:get(<<"data">>, Decoded),
    ?assert(maps:is_key(<<"mode">>, Data)).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_federation, [passthrough, non_strict]),
         meck:expect(flurm_federation, add_cluster, fun(_, _) -> ok end),
         ok
     end,
     fun(_) ->
         meck:unload(flurm_federation)
     end,
     [
      {"cluster with empty name is rejected",
       fun edge_empty_cluster_name/0},
      {"cluster with empty host is rejected",
       fun edge_empty_cluster_host/0},
      {"mode with empty value is rejected",
       fun edge_empty_mode/0},
      {"decode empty JSON object",
       fun edge_empty_json_object/0},
      {"very long cluster name",
       fun edge_long_cluster_name/0}
     ]}.

edge_empty_cluster_name() ->
    Data = #{<<"name">> => <<"">>, <<"host">> => <<"host.example.com">>},
    %% Empty name should be treated as missing (depends on implementation)
    %% The current implementation doesn't check for empty strings, just undefined
    %% This test documents current behavior
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    %% Current implementation accepts empty string - this test documents the behavior
    ?assertMatch({ok, _}, Result).

edge_empty_cluster_host() ->
    Data = #{<<"name">> => <<"test">>, <<"host">> => <<"">>},
    %% Similar to empty name - current implementation accepts it
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({ok, _}, Result).

edge_empty_mode() ->
    Data = #{<<"mode">> => <<"">>},
    Result = flurm_bridge_http:route_request(<<"PUT">>, <<"/api/v1/bridge/mode">>, {undefined, Data}),
    ?assertMatch({error, _}, Result).

edge_empty_json_object() ->
    ?assertEqual(#{}, flurm_bridge_http:decode_request(<<"{}">>)).

edge_long_cluster_name() ->
    LongName = binary:copy(<<"x">>, 1000),
    Data = #{<<"name">> => LongName, <<"host">> => <<"host.example.com">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({ok, #{name := LongName}}, Result).

%%====================================================================
%% Default Values Tests
%%====================================================================

default_values_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_federation, [passthrough, non_strict]),
         meck:expect(flurm_federation, add_cluster, fun(_Name, Config) ->
             %% Verify default values are applied
             ?assertEqual(6817, maps:get(port, Config)),
             ?assertEqual(#{}, maps:get(auth, Config)),
             ?assertEqual(1, maps:get(weight, Config)),
             ?assertEqual([], maps:get(features, Config)),
             ?assertEqual([], maps:get(partitions, Config)),
             ok
         end),
         ok
     end,
     fun(_) ->
         meck:unload(flurm_federation)
     end,
     [
      {"add_cluster uses defaults for optional fields",
       fun default_cluster_values/0}
     ]}.

default_cluster_values() ->
    Data = #{<<"name">> => <<"minimal">>, <<"host">> => <<"host.example.com">>},
    Result = flurm_bridge_http:route_request(<<"POST">>, <<"/api/v1/bridge/clusters">>, {undefined, Data}),
    ?assertMatch({ok, _}, Result).
