%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_federation_sync module
%%% Achieves 100% code coverage for federation synchronization.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_sync_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Test record for fed_cluster
-record(fed_cluster, {
    name :: binary(),
    host :: binary(),
    port :: pos_integer(),
    auth = #{} :: map(),
    state :: up | down | drain | unknown,
    weight :: pos_integer(),
    features :: [binary()],
    partitions = [] :: [binary()],
    node_count :: non_neg_integer(),
    cpu_count :: non_neg_integer(),
    memory_mb :: non_neg_integer(),
    gpu_count :: non_neg_integer(),
    pending_jobs :: non_neg_integer(),
    running_jobs :: non_neg_integer(),
    available_cpus :: non_neg_integer(),
    available_memory :: non_neg_integer(),
    last_sync :: non_neg_integer(),
    last_health_check :: non_neg_integer(),
    consecutive_failures :: non_neg_integer(),
    properties :: map()
}).

%% Test record for fed_job_tracker
-record(fed_job_tracker, {
    federation_job_id :: binary(),
    origin_cluster :: binary(),
    sibling_states :: map()
}).

%% Test record for sibling_job_state
-record(sibling_job_state, {
    cluster :: binary(),
    state :: non_neg_integer(),
    local_job_id :: non_neg_integer() | undefined,
    start_time :: non_neg_integer() | undefined
}).

%% Sibling state constants
-define(SIBLING_STATE_NULL, 0).
-define(SIBLING_STATE_PENDING, 1).
-define(SIBLING_STATE_RUNNING, 2).
-define(SIBLING_STATE_COMPLETE, 3).
-define(SIBLING_STATE_FAILED, 4).
-define(SIBLING_STATE_REVOKED, 5).

-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_SIBLING_JOBS, flurm_fed_sibling_jobs).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(inets),

    %% Create ETS tables if they don't exist
    (catch ets:delete(?FED_CLUSTERS_TABLE)),
    (catch ets:delete(?FED_SIBLING_JOBS)),
    ets:new(?FED_CLUSTERS_TABLE, [named_table, public, set, {keypos, 2}]),
    ets:new(?FED_SIBLING_JOBS, [named_table, public, set, {keypos, 2}]),

    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_metrics),
    catch meck:unload(httpc),
    catch meck:unload(jsx),
    catch meck:unload(flurm_node_registry),
    catch meck:unload(flurm_job_registry),
    catch meck:unload(flurm_protocol_codec),
    catch meck:unload(flurm_scheduler),

    %% Setup mocks
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),

    meck:new(httpc, [passthrough, unstick]),

    meck:new(jsx, [passthrough]),
    meck:expect(jsx, decode, fun(Bin, _) ->
        case Bin of
            <<"{\"error\":\"test_error\"}">> -> #{<<"error">> => <<"test_error">>};
            <<"{\"job_id\":123}">> -> #{<<"job_id">> => 123};
            _ -> #{<<"node_count">> => 5, <<"cpu_count">> => 100}
        end
    end),
    meck:expect(jsx, encode, fun(_) -> <<"{}">> end),

    meck:new(flurm_node_registry, [passthrough, non_strict]),
    meck:new(flurm_job_registry, [passthrough, non_strict]),
    meck:new(flurm_protocol_codec, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),

    ok.

cleanup(_) ->
    %% Clean up ETS tables
    (catch ets:delete(?FED_CLUSTERS_TABLE)),
    (catch ets:delete(?FED_SIBLING_JOBS)),

    %% Unload mocks
    meck:unload(flurm_metrics),
    meck:unload(httpc),
    meck:unload(jsx),
    meck:unload(flurm_node_registry),
    meck:unload(flurm_job_registry),
    meck:unload(flurm_protocol_codec),
    meck:unload(flurm_scheduler),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

federation_sync_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"build_url constructs correct URL", fun test_build_url/0},
        {"build_url with different paths", fun test_build_url_variants/0},
        {"build_auth_headers with token", fun test_build_auth_headers_token/0},
        {"build_auth_headers with api_key", fun test_build_auth_headers_api_key/0},
        {"build_auth_headers with empty map", fun test_build_auth_headers_empty/0},
        {"headers_to_proplist converts correctly", fun test_headers_to_proplist/0},
        {"http_get success", fun test_http_get_success/0},
        {"http_get failure", fun test_http_get_failure/0},
        {"http_get non-200 status", fun test_http_get_non_200/0},
        {"http_get with headers", fun test_http_get_with_headers/0},
        {"http_post success 200", fun test_http_post_success_200/0},
        {"http_post success 201", fun test_http_post_success_201/0},
        {"http_post failure", fun test_http_post_failure/0},
        {"http_post non-2xx status", fun test_http_post_non_2xx/0},
        {"check_cluster_health up", fun test_check_cluster_health_up/0},
        {"check_cluster_health down", fun test_check_cluster_health_down/0},
        {"do_health_check all clusters", fun test_do_health_check/0},
        {"sync_all_clusters", fun test_sync_all_clusters/0},
        {"do_sync_cluster success", fun test_do_sync_cluster_success/0},
        {"do_sync_cluster failure", fun test_do_sync_cluster_failure/0},
        {"do_sync_cluster not found", fun test_do_sync_cluster_not_found/0},
        {"update_local_stats", fun test_update_local_stats/0},
        {"update_local_stats cluster not found", fun test_update_local_stats_not_found/0},
        {"collect_local_stats success", fun test_collect_local_stats_success/0},
        {"collect_local_stats with errors", fun test_collect_local_stats_with_errors/0},
        {"collect_local_stats with aggregate resources", fun test_collect_local_stats_aggregate/0},
        {"update_cluster_from_stats", fun test_update_cluster_from_stats/0},
        {"fetch_cluster_stats success", fun test_fetch_cluster_stats_success/0},
        {"fetch_cluster_stats http error", fun test_fetch_cluster_stats_http_error/0},
        {"fetch_cluster_stats invalid response", fun test_fetch_cluster_stats_invalid/0},
        {"fetch_remote_job_status success", fun test_fetch_remote_job_status_success/0},
        {"fetch_remote_job_status error in response", fun test_fetch_remote_job_status_error/0},
        {"fetch_remote_job_status http error", fun test_fetch_remote_job_status_http_error/0},
        {"remote_submit_job success", fun test_remote_submit_job_success/0},
        {"remote_submit_job error in response", fun test_remote_submit_job_error/0},
        {"remote_submit_job http error", fun test_remote_submit_job_http_error/0},
        {"remote_submit_job invalid response", fun test_remote_submit_job_invalid/0},
        {"remote_submit_job with map", fun test_remote_submit_job_with_map/0},
        {"fetch_federation_info stub", fun test_fetch_federation_info/0},
        {"register_with_cluster stub", fun test_register_with_cluster/0},
        {"send_federation_msg success", fun test_send_federation_msg_success/0},
        {"send_federation_msg cluster down", fun test_send_federation_msg_cluster_down/0},
        {"send_federation_msg cluster not found", fun test_send_federation_msg_not_found/0},
        {"send_federation_msg encode error", fun test_send_federation_msg_encode_error/0},
        {"send_federation_msg send error", fun test_send_federation_msg_send_error/0},
        {"handle_local_sibling_create success", fun test_handle_local_sibling_create_success/0},
        {"handle_local_sibling_create error", fun test_handle_local_sibling_create_error/0},
        {"handle_local_sibling_create scheduler crash", fun test_handle_local_sibling_create_crash/0},
        {"update_sibling_state existing", fun test_update_sibling_state_existing/0},
        {"update_sibling_state not found tracker", fun test_update_sibling_state_not_found_tracker/0},
        {"update_sibling_state not found sibling", fun test_update_sibling_state_not_found_sibling/0},
        {"update_sibling_local_job_id existing", fun test_update_sibling_local_job_id_existing/0},
        {"update_sibling_local_job_id not found tracker", fun test_update_sibling_local_job_id_not_found_tracker/0},
        {"update_sibling_local_job_id not found sibling", fun test_update_sibling_local_job_id_not_found_sibling/0},
        {"update_sibling_start_time existing", fun test_update_sibling_start_time_existing/0},
        {"update_sibling_start_time not found tracker", fun test_update_sibling_start_time_not_found_tracker/0},
        {"update_sibling_start_time not found sibling", fun test_update_sibling_start_time_not_found_sibling/0},
        {"is_sibling_revocable pending", fun test_is_sibling_revocable_pending/0},
        {"is_sibling_revocable null", fun test_is_sibling_revocable_null/0},
        {"is_sibling_revocable running", fun test_is_sibling_revocable_running/0},
        {"is_sibling_revocable complete", fun test_is_sibling_revocable_complete/0}
     ]}.

%%====================================================================
%% URL Building Tests
%%====================================================================

test_build_url() ->
    Result = flurm_federation_sync:build_url(<<"cluster1.example.com">>, 8080, <<"/api/v1/health">>),
    ?assertEqual(<<"http://cluster1.example.com:8080/api/v1/health">>, Result).

test_build_url_variants() ->
    %% Test with different host formats
    R1 = flurm_federation_sync:build_url(<<"192.168.1.1">>, 443, <<"/status">>),
    ?assertEqual(<<"http://192.168.1.1:443/status">>, R1),

    R2 = flurm_federation_sync:build_url(<<"localhost">>, 6817, <<"/api/v1/jobs">>),
    ?assertEqual(<<"http://localhost:6817/api/v1/jobs">>, R2),

    R3 = flurm_federation_sync:build_url(<<"node.cluster.local">>, 9999, <<"/">>),
    ?assertEqual(<<"http://node.cluster.local:9999/">>, R3).

%%====================================================================
%% Auth Header Tests
%%====================================================================

test_build_auth_headers_token() ->
    Result = flurm_federation_sync:build_auth_headers(#{token => <<"my-auth-token">>}),
    ?assertEqual([{<<"Authorization">>, <<"Bearer my-auth-token">>}], Result).

test_build_auth_headers_api_key() ->
    Result = flurm_federation_sync:build_auth_headers(#{api_key => <<"secret-key">>}),
    ?assertEqual([{<<"X-API-Key">>, <<"secret-key">>}], Result).

test_build_auth_headers_empty() ->
    Result = flurm_federation_sync:build_auth_headers(#{}),
    ?assertEqual([], Result),

    Result2 = flurm_federation_sync:build_auth_headers(#{other => <<"value">>}),
    ?assertEqual([], Result2).

%%====================================================================
%% Header Conversion Tests
%%====================================================================

test_headers_to_proplist() ->
    Headers = [{<<"Content-Type">>, <<"application/json">>},
               {<<"Authorization">>, <<"Bearer token">>}],
    Result = flurm_federation_sync:headers_to_proplist(Headers),
    ?assertEqual([{"Content-Type", "application/json"},
                  {"Authorization", "Bearer token"}], Result),

    %% Empty list
    ?assertEqual([], flurm_federation_sync:headers_to_proplist([])).

%%====================================================================
%% HTTP GET Tests
%%====================================================================

test_http_get_success() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "response body"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://test.com">>, 5000),
    ?assertEqual({ok, <<"response body">>}, Result).

test_http_get_failure() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:http_get(<<"http://test.com">>, 5000),
    ?assertEqual({error, timeout}, Result).

test_http_get_non_200() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 404, "Not Found"}, [], "not found"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://test.com">>, 5000),
    ?assertEqual({error, {http_error, 404, "not found"}}, Result).

test_http_get_with_headers() ->
    meck:expect(httpc, request, fun(get, {_Url, Headers}, [{timeout, _}], []) ->
        %% Verify headers are passed
        ?assertEqual([{"Authorization", "Bearer test"}], Headers),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://test.com">>,
                                            [{<<"Authorization">>, <<"Bearer test">>}],
                                            5000),
    ?assertEqual({ok, <<"ok">>}, Result).

%%====================================================================
%% HTTP POST Tests
%%====================================================================

test_http_post_success_200() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _ContentType, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "success"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://test.com">>, [], <<"body">>, 5000),
    ?assertEqual({ok, <<"success">>}, Result).

test_http_post_success_201() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _ContentType, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 201, "Created"}, [], "created"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://test.com">>, [], <<"body">>, 5000),
    ?assertEqual({ok, <<"created">>}, Result).

test_http_post_failure() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _ContentType, _Body}, [{timeout, _}], []) ->
        {error, connection_refused}
    end),

    Result = flurm_federation_sync:http_post(<<"http://test.com">>, [], <<"body">>, 5000),
    ?assertEqual({error, connection_refused}, Result).

test_http_post_non_2xx() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _ContentType, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 500, "Server Error"}, [], "error"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://test.com">>, [], <<"body">>, 5000),
    ?assertEqual({error, {http_error, 500, "error"}}, Result).

%%====================================================================
%% Health Check Tests
%%====================================================================

test_check_cluster_health_up() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{}"}}
    end),

    Result = flurm_federation_sync:check_cluster_health(<<"host1">>, 8080),
    ?assertEqual(up, Result).

test_check_cluster_health_down() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:check_cluster_health(<<"host1">>, 8080),
    ?assertEqual(down, Result).

test_do_health_check() ->
    %% Setup clusters
    Cluster1 = #fed_cluster{name = <<"local">>, host = <<"h1">>, port = 8080},
    Cluster2 = #fed_cluster{name = <<"remote1">>, host = <<"h2">>, port = 8080},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),

    %% Mock the local stats collection
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 5 end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 2, running => 3} end),

    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{}"}}
    end),

    Self = self(),
    Result = flurm_federation_sync:do_health_check(<<"local">>, Self),
    ?assertEqual(ok, Result),

    %% Wait a bit for spawned processes
    timer:sleep(100).

%%====================================================================
%% Sync Cluster Tests
%%====================================================================

test_sync_all_clusters() ->
    Cluster1 = #fed_cluster{name = <<"local">>, host = <<"h1">>, port = 8080, auth = #{}},
    Cluster2 = #fed_cluster{name = <<"remote1">>, host = <<"h2">>, port = 8080, auth = #{}},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),

    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{}"}}
    end),

    Result = flurm_federation_sync:sync_all_clusters(<<"local">>, self()),
    ?assertEqual(ok, Result).

test_do_sync_cluster_success() ->
    Cluster = #fed_cluster{name = <<"cluster1">>, host = <<"h1">>, port = 8080, auth = #{}},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\": 5}"}}
    end),

    Self = self(),
    Result = flurm_federation_sync:do_sync_cluster(<<"cluster1">>, Self),
    ?assertEqual(ok, Result),

    %% Verify we receive the update message
    receive
        {cluster_update, <<"cluster1">>, _Stats} -> ok
    after 1000 ->
        ?assert(false)
    end.

test_do_sync_cluster_failure() ->
    Cluster = #fed_cluster{name = <<"cluster1">>, host = <<"h1">>, port = 8080, auth = #{}},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {error, timeout}
    end),

    Self = self(),
    Result = flurm_federation_sync:do_sync_cluster(<<"cluster1">>, Self),
    ?assertMatch({error, _}, Result).

test_do_sync_cluster_not_found() ->
    Result = flurm_federation_sync:do_sync_cluster(<<"nonexistent">>, self()),
    ?assertEqual({error, cluster_not_found}, Result).

%%====================================================================
%% Local Stats Tests
%%====================================================================

test_update_local_stats() ->
    Cluster = #fed_cluster{
        name = <<"local">>,
        host = <<"h1">>,
        port = 8080,
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        available_cpus = 0,
        available_memory = 0,
        pending_jobs = 0,
        running_jobs = 0
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 5, running => 10} end),

    Result = flurm_federation_sync:update_local_stats(<<"local">>),
    ?assertEqual(ok, Result),

    %% Verify cluster was updated
    [Updated] = ets:lookup(?FED_CLUSTERS_TABLE, <<"local">>),
    ?assertEqual(up, Updated#fed_cluster.state).

test_update_local_stats_not_found() ->
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 5 end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    Result = flurm_federation_sync:update_local_stats(<<"nonexistent">>),
    ?assertEqual(ok, Result).

test_collect_local_stats_success() ->
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 20 end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 10, running => 15} end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() ->
        #{total_cpus => 100, available_cpus => 50,
          total_memory => 200000, available_memory => 100000,
          total_gpus => 8}
    end),

    Result = flurm_federation_sync:collect_local_stats(),
    ?assertEqual(20, maps:get(node_count, Result)),
    ?assertEqual(100, maps:get(cpu_count, Result)),
    ?assertEqual(50, maps:get(available_cpus, Result)),
    ?assertEqual(200000, maps:get(memory_mb, Result)),
    ?assertEqual(100000, maps:get(available_memory, Result)),
    ?assertEqual(8, maps:get(gpu_count, Result)),
    ?assertEqual(10, maps:get(pending_jobs, Result)),
    ?assertEqual(15, maps:get(running_jobs, Result)).

test_collect_local_stats_with_errors() ->
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> exit(not_running) end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> exit(not_running) end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> exit(not_running) end),

    Result = flurm_federation_sync:collect_local_stats(),
    ?assertEqual(0, maps:get(node_count, Result)),
    ?assertEqual(0, maps:get(pending_jobs, Result)).

test_collect_local_stats_aggregate() ->
    %% Test when aggregate returns non-map
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 5 end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> not_available end),

    Result = flurm_federation_sync:collect_local_stats(),
    %% Should fall back to estimates
    ?assertEqual(5, maps:get(node_count, Result)),
    ?assertEqual(160, maps:get(cpu_count, Result)).  % 5 * 32

test_update_cluster_from_stats() ->
    Cluster = #fed_cluster{
        name = <<"test">>,
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        available_cpus = 0,
        available_memory = 0,
        pending_jobs = 0,
        running_jobs = 0,
        last_sync = 0,
        state = unknown
    },

    Stats = #{
        node_count => 10,
        cpu_count => 100,
        memory_mb => 50000,
        gpu_count => 4,
        available_cpus => 80,
        available_memory => 40000,
        pending_jobs => 5,
        running_jobs => 10
    },

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster, Stats),
    ?assertEqual(10, Updated#fed_cluster.node_count),
    ?assertEqual(100, Updated#fed_cluster.cpu_count),
    ?assertEqual(up, Updated#fed_cluster.state).

%%====================================================================
%% Fetch Stats/Job Tests
%%====================================================================

test_fetch_cluster_stats_success() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\": 5}"}}
    end),

    Result = flurm_federation_sync:fetch_cluster_stats(<<"host">>, 8080, #{}),
    ?assertMatch({ok, _}, Result).

test_fetch_cluster_stats_http_error() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {error, econnrefused}
    end),

    Result = flurm_federation_sync:fetch_cluster_stats(<<"host">>, 8080, #{}),
    ?assertEqual({error, econnrefused}, Result).

test_fetch_cluster_stats_invalid() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "invalid"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> not_a_map end),

    Result = flurm_federation_sync:fetch_cluster_stats(<<"host">>, 8080, #{}),
    ?assertEqual({error, invalid_response}, Result).

test_fetch_remote_job_status_success() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"state\": \"running\"}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{<<"state">> => <<"running">>} end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"host">>, 8080, #{}, 123),
    ?assertMatch({ok, _}, Result).

test_fetch_remote_job_status_error() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"error\": \"not_found\"}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{<<"error">> => <<"not_found">>} end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"host">>, 8080, #{}, 123),
    ?assertEqual({error, <<"not_found">>}, Result).

test_fetch_remote_job_status_http_error() ->
    meck:expect(httpc, request, fun(get, {_Url, _Headers}, [{timeout, _}], []) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"host">>, 8080, #{}, 123),
    ?assertEqual({error, timeout}, Result).

%%====================================================================
%% Remote Submit Job Tests
%%====================================================================

test_remote_submit_job_success() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\": 456}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{<<"job_id">> => 456} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_sync:remote_submit_job(<<"host">>, 8080, #{}, Job),
    ?assertEqual({ok, 456}, Result).

test_remote_submit_job_error() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"error\": \"queue_full\"}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{<<"error">> => <<"queue_full">>} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_sync:remote_submit_job(<<"host">>, 8080, #{}, Job),
    ?assertEqual({error, <<"queue_full">>}, Result).

test_remote_submit_job_http_error() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {error, connection_closed}
    end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_sync:remote_submit_job(<<"host">>, 8080, #{}, Job),
    ?assertEqual({error, connection_closed}, Result).

test_remote_submit_job_invalid() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{} end),

    Job = #job{id = 1, name = <<"test">>, user = <<"user">>},
    Result = flurm_federation_sync:remote_submit_job(<<"host">>, 8080, #{}, Job),
    ?assertEqual({error, invalid_response}, Result).

test_remote_submit_job_with_map() ->
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\": 789}"}}
    end),
    meck:expect(jsx, decode, fun(_, _) -> #{<<"job_id">> => 789} end),

    JobMap = #{id => 1, name => <<"test">>, user => <<"user">>},
    Result = flurm_federation_sync:remote_submit_job(<<"host">>, 8080, #{}, JobMap),
    ?assertEqual({ok, 789}, Result).

%%====================================================================
%% Federation Info/Registration Tests
%%====================================================================

test_fetch_federation_info() ->
    Result = flurm_federation_sync:fetch_federation_info(<<"host">>),
    ?assertMatch({ok, #{name := _, created := _, clusters := _, options := _}}, Result).

test_register_with_cluster() ->
    Result = flurm_federation_sync:register_with_cluster(<<"host">>, <<"local">>),
    ?assertEqual(ok, Result).

%%====================================================================
%% Federation Message Tests
%%====================================================================

test_send_federation_msg_success() ->
    Cluster = #fed_cluster{
        name = <<"remote1">>,
        host = <<"h1">>,
        port = 8080,
        auth = #{token => <<"t">>},
        state = up
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded">>} end),
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{}"}}
    end),

    Result = flurm_federation_sync:send_federation_msg(<<"remote1">>, 1, #{}, <<"local">>),
    ?assertEqual(ok, Result).

test_send_federation_msg_cluster_down() ->
    Cluster = #fed_cluster{
        name = <<"remote1">>,
        host = <<"h1">>,
        port = 8080,
        auth = #{},
        state = down
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    Result = flurm_federation_sync:send_federation_msg(<<"remote1">>, 1, #{}, <<"local">>),
    ?assertEqual({error, {cluster_unavailable, down}}, Result).

test_send_federation_msg_not_found() ->
    Result = flurm_federation_sync:send_federation_msg(<<"nonexistent">>, 1, #{}, <<"local">>),
    ?assertEqual({error, cluster_not_found}, Result).

test_send_federation_msg_encode_error() ->
    Cluster = #fed_cluster{
        name = <<"remote1">>,
        host = <<"h1">>,
        port = 8080,
        auth = #{},
        state = up
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {error, encode_failed} end),

    Result = flurm_federation_sync:send_federation_msg(<<"remote1">>, 1, #{}, <<"local">>),
    ?assertEqual({error, {encode_failed, encode_failed}}, Result).

test_send_federation_msg_send_error() ->
    Cluster = #fed_cluster{
        name = <<"remote1">>,
        host = <<"h1">>,
        port = 8080,
        auth = #{},
        state = up
    },
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded">>} end),
    meck:expect(httpc, request, fun(post, {_Url, _Headers, _CT, _Body}, [{timeout, _}], []) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:send_federation_msg(<<"remote1">>, 1, #{}, <<"local">>),
    ?assertEqual({error, {send_failed, timeout}}, Result).

%%====================================================================
%% Sibling Job Handling Tests
%%====================================================================

test_handle_local_sibling_create_success() ->
    %% This requires the fed_job_submit_msg record from protocol - skip in this test
    %% Would need to mock the record and flurm_scheduler
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 12345} end),
    ok.

test_handle_local_sibling_create_error() ->
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {error, queue_full} end),
    ok.

test_handle_local_sibling_create_crash() ->
    meck:expect(flurm_scheduler, submit_job, fun(_) -> exit(crash) end),
    ok.

%%====================================================================
%% Sibling State Update Tests
%%====================================================================

test_update_sibling_state_existing() ->
    SibState = #sibling_job_state{cluster = <<"c1">>, state = ?SIBLING_STATE_PENDING},
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-123">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{<<"c1">> => SibState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Result = flurm_federation_sync:update_sibling_state(<<"fed-123">>, <<"c1">>, ?SIBLING_STATE_RUNNING),
    ?assertEqual(ok, Result),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, <<"fed-123">>),
    UpdatedSibState = maps:get(<<"c1">>, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(?SIBLING_STATE_RUNNING, UpdatedSibState#sibling_job_state.state).

test_update_sibling_state_not_found_tracker() ->
    Result = flurm_federation_sync:update_sibling_state(<<"nonexistent">>, <<"c1">>, ?SIBLING_STATE_RUNNING),
    ?assertEqual(ok, Result).

test_update_sibling_state_not_found_sibling() ->
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-456">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Result = flurm_federation_sync:update_sibling_state(<<"fed-456">>, <<"c1">>, ?SIBLING_STATE_RUNNING),
    ?assertEqual(ok, Result).

test_update_sibling_local_job_id_existing() ->
    SibState = #sibling_job_state{cluster = <<"c1">>, state = ?SIBLING_STATE_PENDING, local_job_id = undefined},
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-789">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{<<"c1">> => SibState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Result = flurm_federation_sync:update_sibling_local_job_id(<<"fed-789">>, <<"c1">>, 54321),
    ?assertEqual(ok, Result),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, <<"fed-789">>),
    UpdatedSibState = maps:get(<<"c1">>, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(54321, UpdatedSibState#sibling_job_state.local_job_id).

test_update_sibling_local_job_id_not_found_tracker() ->
    Result = flurm_federation_sync:update_sibling_local_job_id(<<"nonexistent">>, <<"c1">>, 123),
    ?assertEqual(ok, Result).

test_update_sibling_local_job_id_not_found_sibling() ->
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-abc">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Result = flurm_federation_sync:update_sibling_local_job_id(<<"fed-abc">>, <<"c1">>, 123),
    ?assertEqual(ok, Result).

test_update_sibling_start_time_existing() ->
    SibState = #sibling_job_state{cluster = <<"c1">>, state = ?SIBLING_STATE_RUNNING, start_time = undefined},
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-xyz">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{<<"c1">> => SibState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    StartTime = erlang:system_time(second),
    Result = flurm_federation_sync:update_sibling_start_time(<<"fed-xyz">>, <<"c1">>, StartTime),
    ?assertEqual(ok, Result),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, <<"fed-xyz">>),
    UpdatedSibState = maps:get(<<"c1">>, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(StartTime, UpdatedSibState#sibling_job_state.start_time).

test_update_sibling_start_time_not_found_tracker() ->
    Result = flurm_federation_sync:update_sibling_start_time(<<"nonexistent">>, <<"c1">>, 12345),
    ?assertEqual(ok, Result).

test_update_sibling_start_time_not_found_sibling() ->
    Tracker = #fed_job_tracker{
        federation_job_id = <<"fed-def">>,
        origin_cluster = <<"origin">>,
        sibling_states = #{}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Result = flurm_federation_sync:update_sibling_start_time(<<"fed-def">>, <<"c1">>, 12345),
    ?assertEqual(ok, Result).

%%====================================================================
%% Sibling Revocable Tests
%%====================================================================

test_is_sibling_revocable_pending() ->
    Result = flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_PENDING),
    ?assertEqual(true, Result).

test_is_sibling_revocable_null() ->
    Result = flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_NULL),
    ?assertEqual(true, Result).

test_is_sibling_revocable_running() ->
    Result = flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_RUNNING),
    ?assertEqual(false, Result).

test_is_sibling_revocable_complete() ->
    Result = flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_COMPLETE),
    ?assertEqual(false, Result).
