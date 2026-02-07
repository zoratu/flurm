%%%-------------------------------------------------------------------
%%% @doc E2E Integration Tests for FLURM Bridge REST API
%%%
%%% These tests start a real Cowboy HTTP server and make actual HTTP
%%% requests to test the complete request/response cycle.
%%%
%%% Tests cover:
%%% - GET/PUT /api/v1/bridge/status
%%% - GET/PUT /api/v1/bridge/mode
%%% - GET/POST/DELETE /api/v1/bridge/clusters
%%% - JWT authentication flow
%%% - Error handling with real HTTP status codes
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_bridge_http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Status tests
    test_get_status/1,
    test_get_status_includes_all_fields/1,

    %% Mode tests
    test_get_mode/1,
    test_put_mode_shadow/1,
    test_put_mode_active/1,
    test_put_mode_primary/1,
    test_put_mode_standalone/1,
    test_put_mode_invalid/1,
    test_put_mode_missing_field/1,
    test_mode_transition_sequence/1,

    %% Cluster tests
    test_list_clusters_empty/1,
    test_add_cluster/1,
    test_add_cluster_missing_name/1,
    test_add_cluster_missing_host/1,
    test_get_cluster/1,
    test_get_cluster_not_found/1,
    test_delete_cluster/1,
    test_delete_cluster_not_found/1,
    test_cluster_crud_flow/1,

    %% Auth tests
    test_auth_no_token_when_enabled/1,
    test_auth_invalid_token/1,
    test_auth_valid_token/1,
    test_auth_expired_token/1,
    test_auth_disabled/1,

    %% Error handling tests
    test_invalid_json/1,
    test_method_not_allowed/1,
    test_not_found_endpoint/1
]).

-define(HTTP_PORT, 19080).
-define(BASE_URL, "http://localhost:" ++ integer_to_list(?HTTP_PORT)).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, status_tests},
        {group, mode_tests},
        {group, cluster_tests},
        {group, auth_tests},
        {group, error_tests}
    ].

groups() ->
    [
        {status_tests, [sequence], [
            test_get_status,
            test_get_status_includes_all_fields
        ]},
        {mode_tests, [sequence], [
            test_get_mode,
            test_put_mode_shadow,
            test_put_mode_active,
            test_put_mode_primary,
            test_put_mode_standalone,
            test_put_mode_invalid,
            test_put_mode_missing_field,
            test_mode_transition_sequence
        ]},
        {cluster_tests, [sequence], [
            test_list_clusters_empty,
            test_add_cluster,
            test_add_cluster_missing_name,
            test_add_cluster_missing_host,
            test_get_cluster,
            test_get_cluster_not_found,
            test_delete_cluster,
            test_delete_cluster_not_found,
            test_cluster_crud_flow
        ]},
        {auth_tests, [sequence], [
            test_auth_disabled,
            test_auth_no_token_when_enabled,
            test_auth_invalid_token,
            test_auth_valid_token,
            test_auth_expired_token
        ]},
        {error_tests, [sequence], [
            test_invalid_json,
            test_method_not_allowed,
            test_not_found_endpoint
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    application:ensure_all_started(lager),
    application:ensure_all_started(ranch),
    application:ensure_all_started(cowlib),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(jsx),
    application:ensure_all_started(inets),
    application:ensure_all_started(meck),

    %% Load the flurm_federation module so meck can mock it
    code:ensure_loaded(flurm_federation),

    %% Start mock federation server
    start_mock_federation(),

    %% Configure and start Cowboy with our routes
    Dispatch = cowboy_router:compile(flurm_bridge_http:routes()),
    {ok, _} = cowboy:start_clear(
        test_http_listener,
        [{port, ?HTTP_PORT}],
        #{env => #{dispatch => Dispatch}}
    ),

    %% Configure JWT
    application:set_env(flurm_controller, jwt_secret, <<"test-e2e-secret-key">>),
    application:set_env(flurm_controller, jwt_expiry, 3600),
    application:set_env(flurm_controller, jwt_issuer, <<"flurm-test">>),

    %% Disable auth by default
    application:set_env(flurm_controller, bridge_api_auth, disabled),

    %% Set initial mode
    application:set_env(flurm_controller, bridge_mode, standalone),

    Config.

end_per_suite(_Config) ->
    cowboy:stop_listener(test_http_listener),
    stop_mock_federation(),
    ok.

init_per_group(auth_tests, Config) ->
    %% Reset auth state before auth tests
    application:set_env(flurm_controller, bridge_api_auth, disabled),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Reset mode to standalone before each test
    application:set_env(flurm_controller, bridge_mode, standalone),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Status Tests
%%====================================================================

test_get_status(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_get("/api/v1/bridge/status"),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    ?assert(maps:is_key(<<"data">>, Response)).

test_get_status_includes_all_fields(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_get("/api/v1/bridge/status"),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    Data = maps:get(<<"data">>, Response),
    %% All required fields should be present
    ?assert(maps:is_key(<<"mode">>, Data)),
    ?assert(maps:is_key(<<"is_federated">>, Data)),
    ?assert(maps:is_key(<<"local_cluster">>, Data)),
    ?assert(maps:is_key(<<"federation_stats">>, Data)),
    ?assert(maps:is_key(<<"resources">>, Data)).

%%====================================================================
%% Mode Tests
%%====================================================================

test_get_mode(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_get("/api/v1/bridge/mode"),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"standalone">>, maps:get(<<"mode">>, Data)).

test_put_mode_shadow(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => <<"shadow">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"shadow">>, maps:get(<<"mode">>, Data)),
    ?assertEqual(<<"updated">>, maps:get(<<"status">>, Data)).

test_put_mode_active(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => <<"active">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"active">>, maps:get(<<"mode">>, Data)).

test_put_mode_primary(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => <<"primary">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"primary">>, maps:get(<<"mode">>, Data)).

test_put_mode_standalone(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => <<"standalone">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"standalone">>, maps:get(<<"mode">>, Data)).

test_put_mode_invalid(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => <<"invalid_mode">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Response)),
    ?assert(maps:is_key(<<"error">>, Response)).

test_put_mode_missing_field(_Config) ->
    {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"other">> => <<"value">>}),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Response)),
    Error = maps:get(<<"error">>, Response),
    ?assertEqual(<<"missing 'mode' field">>, maps:get(<<"reason">>, Error)).

test_mode_transition_sequence(_Config) ->
    %% Test the typical migration sequence: standalone -> shadow -> active -> primary
    Modes = [<<"shadow">>, <<"active">>, <<"primary">>, <<"standalone">>],
    lists:foreach(fun(Mode) ->
        {ok, {{_, 200, _}, _Headers, Body}} = http_put("/api/v1/bridge/mode", #{<<"mode">> => Mode}),
        Response = jsx:decode(list_to_binary(Body), [return_maps]),
        ?assertEqual(true, maps:get(<<"success">>, Response)),
        Data = maps:get(<<"data">>, Response),
        ?assertEqual(Mode, maps:get(<<"mode">>, Data))
    end, Modes).

%%====================================================================
%% Cluster Tests
%%====================================================================

test_list_clusters_empty(_Config) ->
    %% Verify ETS table exists
    ?assert(ets:info(mock_clusters, size) =/= undefined),
    %% Verify meck is still mocking flurm_federation
    ?assert(meck:validate(flurm_federation)),
    ct:pal("Meck validate passed for flurm_federation"),
    clear_mock_clusters(),
    %% Test the mock directly
    MockResult = flurm_federation:list_clusters(),
    ct:pal("Direct mock call result: ~p", [MockResult]),
    %% Call endpoint
    case http_get("/api/v1/bridge/clusters") of
        {ok, {{_, 200, _}, _Headers, Body}} ->
            Response = jsx:decode(list_to_binary(Body), [return_maps]),
            ?assertEqual(true, maps:get(<<"success">>, Response)),
            Data = maps:get(<<"data">>, Response),
            ?assertEqual(0, maps:get(<<"count">>, Data));
        {ok, {{_, StatusCode, _}, _, ErrorBody}} ->
            ct:fail({unexpected_status, StatusCode, ErrorBody})
    end.

test_add_cluster(_Config) ->
    clear_mock_clusters(),
    ClusterData = #{
        <<"name">> => <<"test-cluster">>,
        <<"host">> => <<"test.example.com">>,
        <<"port">> => 6817
    },
    {ok, {{_, 200, _}, _Headers, Body}} = http_post("/api/v1/bridge/clusters", ClusterData),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"test-cluster">>, maps:get(<<"name">>, Data)),
    ?assertEqual(<<"added">>, maps:get(<<"status">>, Data)).

test_add_cluster_missing_name(_Config) ->
    ClusterData = #{<<"host">> => <<"test.example.com">>},
    {ok, {{_, 200, _}, _Headers, Body}} = http_post("/api/v1/bridge/clusters", ClusterData),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Response)),
    Error = maps:get(<<"error">>, Response),
    ?assertEqual(<<"missing 'name' field">>, maps:get(<<"reason">>, Error)).

test_add_cluster_missing_host(_Config) ->
    ClusterData = #{<<"name">> => <<"test-cluster">>},
    {ok, {{_, 200, _}, _Headers, Body}} = http_post("/api/v1/bridge/clusters", ClusterData),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(false, maps:get(<<"success">>, Response)),
    Error = maps:get(<<"error">>, Response),
    ?assertEqual(<<"missing 'host' field">>, maps:get(<<"reason">>, Error)).

test_get_cluster(_Config) ->
    %% First add a cluster
    add_mock_cluster(<<"get-test-cluster">>, <<"host.example.com">>),

    {ok, {{_, 200, _}, _Headers, Body}} = http_get("/api/v1/bridge/clusters/get-test-cluster"),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"get-test-cluster">>, maps:get(<<"name">>, Data)).

test_get_cluster_not_found(_Config) ->
    {ok, {{_, 404, _}, _Headers, _Body}} = http_get("/api/v1/bridge/clusters/nonexistent-cluster"),
    ok.

test_delete_cluster(_Config) ->
    %% First add a cluster
    add_mock_cluster(<<"delete-test-cluster">>, <<"host.example.com">>),

    {ok, {{_, 200, _}, _Headers, Body}} = http_delete("/api/v1/bridge/clusters/delete-test-cluster"),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)),
    Data = maps:get(<<"data">>, Response),
    ?assertEqual(<<"removed">>, maps:get(<<"status">>, Data)).

test_delete_cluster_not_found(_Config) ->
    {ok, {{_, 404, _}, _Headers, _Body}} = http_delete("/api/v1/bridge/clusters/nonexistent-cluster"),
    ok.

test_cluster_crud_flow(_Config) ->
    clear_mock_clusters(),

    %% Create
    ClusterData = #{<<"name">> => <<"crud-cluster">>, <<"host">> => <<"crud.example.com">>},
    {ok, {{_, 200, _}, _, Body1}} = http_post("/api/v1/bridge/clusters", ClusterData),
    Resp1 = jsx:decode(list_to_binary(Body1), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Resp1)),

    %% Read
    {ok, {{_, 200, _}, _, Body2}} = http_get("/api/v1/bridge/clusters/crud-cluster"),
    Resp2 = jsx:decode(list_to_binary(Body2), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Resp2)),

    %% List (should have 1)
    {ok, {{_, 200, _}, _, Body3}} = http_get("/api/v1/bridge/clusters"),
    Resp3 = jsx:decode(list_to_binary(Body3), [return_maps]),
    Data3 = maps:get(<<"data">>, Resp3),
    ?assertEqual(1, maps:get(<<"count">>, Data3)),

    %% Delete
    {ok, {{_, 200, _}, _, Body4}} = http_delete("/api/v1/bridge/clusters/crud-cluster"),
    Resp4 = jsx:decode(list_to_binary(Body4), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Resp4)),

    %% Verify deleted
    {ok, {{_, 404, _}, _, _}} = http_get("/api/v1/bridge/clusters/crud-cluster"),
    ok.

%%====================================================================
%% Auth Tests
%%====================================================================

test_auth_disabled(_Config) ->
    application:set_env(flurm_controller, bridge_api_auth, disabled),
    %% Should work without token
    {ok, {{_, StatusCode, _}, _Headers, Body}} = http_get("/api/v1/bridge/status"),
    ct:pal("Status code: ~p, Body: ~p", [StatusCode, Body]),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ct:pal("Decoded response: ~p", [Response]),
    ?assertEqual(true, maps:get(<<"success">>, Response)).

test_auth_no_token_when_enabled(_Config) ->
    application:set_env(flurm_controller, bridge_api_auth, enabled),
    {ok, {{_, 401, _}, _Headers, _Body}} = http_get("/api/v1/bridge/status"),
    ok.

test_auth_invalid_token(_Config) ->
    application:set_env(flurm_controller, bridge_api_auth, enabled),
    {ok, {{_, 401, _}, _Headers, _Body}} = http_get_with_auth("/api/v1/bridge/status", "invalid.token.here"),
    ok.

test_auth_valid_token(_Config) ->
    application:set_env(flurm_controller, bridge_api_auth, enabled),
    %% Generate a valid token
    {ok, Token} = flurm_jwt:generate(<<"test-user">>),
    {ok, {{_, 200, _}, _Headers, Body}} = http_get_with_auth("/api/v1/bridge/status", binary_to_list(Token)),
    Response = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(true, maps:get(<<"success">>, Response)).

test_auth_expired_token(_Config) ->
    application:set_env(flurm_controller, bridge_api_auth, enabled),
    %% Generate an expired token
    application:set_env(flurm_controller, jwt_expiry, -1),
    {ok, Token} = flurm_jwt:generate(<<"test-user">>),
    application:set_env(flurm_controller, jwt_expiry, 3600),

    {ok, {{_, 401, _}, _Headers, _Body}} = http_get_with_auth("/api/v1/bridge/status", binary_to_list(Token)),
    ok.

%%====================================================================
%% Error Tests
%%====================================================================

test_invalid_json(_Config) ->
    %% Send malformed JSON
    {ok, {{_, StatusCode, _}, _Headers, _Body}} = http_post_raw("/api/v1/bridge/mode", "not valid json"),
    %% Should handle gracefully (400 or 200 with error in body)
    ?assert(StatusCode >= 200 andalso StatusCode < 500).

test_method_not_allowed(_Config) ->
    %% DELETE is not allowed on /status endpoint
    %% (httpc doesn't support PATCH, so we test with DELETE instead)
    {ok, {{_, 405, _}, _Headers, _Body}} = http_delete("/api/v1/bridge/status"),
    ok.

test_not_found_endpoint(_Config) ->
    {ok, {{_, 404, _}, _Headers, _Body}} = http_get("/api/v1/nonexistent"),
    ok.

%%====================================================================
%% HTTP Helpers
%%====================================================================

http_get(Path) ->
    URL = ?BASE_URL ++ Path,
    httpc:request(get, {URL, []}, [], [{body_format, string}]).

http_get_with_auth(Path, Token) ->
    URL = ?BASE_URL ++ Path,
    Headers = [{"Authorization", "Bearer " ++ Token}],
    httpc:request(get, {URL, Headers}, [], [{body_format, string}]).

http_put(Path, Data) ->
    URL = ?BASE_URL ++ Path,
    Body = jsx:encode(Data),
    httpc:request(put, {URL, [], "application/json", Body}, [], [{body_format, string}]).

http_post(Path, Data) ->
    URL = ?BASE_URL ++ Path,
    Body = jsx:encode(Data),
    httpc:request(post, {URL, [], "application/json", Body}, [], [{body_format, string}]).

http_post_raw(Path, Body) ->
    URL = ?BASE_URL ++ Path,
    httpc:request(post, {URL, [], "application/json", Body}, [], [{body_format, string}]).

http_delete(Path) ->
    URL = ?BASE_URL ++ Path,
    httpc:request(delete, {URL, []}, [], [{body_format, string}]).

http_request(Method, Path, Body) ->
    URL = ?BASE_URL ++ Path,
    MethodAtom = list_to_atom(string:lowercase(Method)),
    case Body of
        <<>> ->
            httpc:request(MethodAtom, {URL, []}, [], [{body_format, string}]);
        _ ->
            httpc:request(MethodAtom, {URL, [], "application/json", Body}, [], [{body_format, string}])
    end.

%%====================================================================
%% Mock Federation Server
%%====================================================================

start_mock_federation() ->
    %% Create ETS table for mock cluster storage
    %% Spawn a process to own the table so it survives across test cases
    Self = self(),
    TableOwner = spawn(fun() ->
        ets:new(mock_clusters, [set, public, named_table]),
        Self ! {ets_ready, self()},
        receive stop -> ok end
    end),
    receive
        {ets_ready, TableOwner} -> ok
    after 5000 ->
        error(ets_table_creation_timeout)
    end,
    register(mock_clusters_owner, TableOwner),

    %% Start meck for flurm_federation - use no_link to prevent crashes from killing the mock
    meck:new(flurm_federation, [passthrough, non_strict, no_link]),
    ct:pal("Meck created for flurm_federation"),

    meck:expect(flurm_federation, get_federation_stats, fun() ->
        #{clusters_total => ets:info(mock_clusters, size),
          clusters_healthy => ets:info(mock_clusters, size),
          clusters_unhealthy => 0}
    end),

    meck:expect(flurm_federation, get_federation_resources, fun() ->
        #{total_nodes => 10, total_cpus => 100, available_cpus => 50}
    end),

    meck:expect(flurm_federation, is_federated, fun() ->
        ets:info(mock_clusters, size) > 0
    end),

    meck:expect(flurm_federation, get_local_cluster, fun() -> <<"local">> end),

    meck:expect(flurm_federation, list_clusters, fun() ->
        case ets:info(mock_clusters, size) of
            undefined ->
                error({ets_table_not_found, mock_clusters});
            _ ->
                [maps:merge(#{name => K}, V) || {K, V} <- ets:tab2list(mock_clusters)]
        end
    end),

    meck:expect(flurm_federation, get_cluster_status, fun(Name) ->
        case ets:lookup(mock_clusters, Name) of
            [{_, Config}] -> {ok, maps:merge(#{name => Name}, Config)};
            [] -> {error, not_found}
        end
    end),

    meck:expect(flurm_federation, add_cluster, fun(Name, Config) ->
        ets:insert(mock_clusters, {Name, Config#{state => up}}),
        ok
    end),

    meck:expect(flurm_federation, remove_cluster, fun(Name) ->
        case ets:lookup(mock_clusters, Name) of
            [{_, _}] ->
                ets:delete(mock_clusters, Name),
                ok;
            [] ->
                {error, not_found}
        end
    end),

    ok.

stop_mock_federation() ->
    catch (mock_clusters_owner ! stop),
    catch ets:delete(mock_clusters),
    catch meck:unload(flurm_federation),
    ok.

clear_mock_clusters() ->
    ets:delete_all_objects(mock_clusters).

add_mock_cluster(Name, Host) ->
    ets:insert(mock_clusters, {Name, #{host => Host, state => up}}).
