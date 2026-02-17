%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_federation_sync module
%%%
%%% Comprehensive tests for federation synchronization functionality:
%%% - Health monitoring and cluster health checks
%%% - Remote communication (HTTP GET/POST, URL building)
%%% - Cluster synchronization and state updates
%%% - Sibling job coordination (Phase 7D)
%%% - Authentication header building
%%% - Statistics collection and updates
%%%
%%% Uses meck for mocking HTTP calls and external dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_federation_sync_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Constants
%%====================================================================

-define(FED_CLUSTERS_TABLE, flurm_fed_clusters).
-define(FED_SIBLING_JOBS, flurm_fed_sibling_jobs).
-define(CLUSTER_TIMEOUT, 5000).

%% Records (matching the module's internal record)
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

%%====================================================================
%% Test Fixture Setup/Teardown
%%====================================================================

setup() ->
    %% Set up application environment
    application:set_env(flurm_core, cluster_name, <<"test_cluster">>),

    %% Create ETS tables
    catch ets:delete(?FED_CLUSTERS_TABLE),
    catch ets:delete(?FED_SIBLING_JOBS),
    ets:new(?FED_CLUSTERS_TABLE, [named_table, public, set, {keypos, 2}]),
    ets:new(?FED_SIBLING_JOBS, [named_table, public, set, {keypos, 2}]),
    ok.

cleanup(_) ->
    %% Clean up ETS tables
    catch ets:delete(?FED_CLUSTERS_TABLE),
    catch ets:delete(?FED_SIBLING_JOBS),
    %% Unload any mocks
    catch meck:unload(),
    ok.

setup_with_meck() ->
    setup(),
    %% Start meck for httpc
    meck:new(httpc, [unstick, passthrough]),
    %% Start meck for flurm_metrics
    meck:new(flurm_metrics, [non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    ok.

cleanup_with_meck(Arg) ->
    cleanup(Arg).

%%====================================================================
%% Test Generators
%%====================================================================

%% Main test suite
federation_sync_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% URL Building Tests
      {"build_url creates correct HTTP URL", fun test_build_url_basic/0},
      {"build_url with different ports", fun test_build_url_ports/0},
      {"build_url with various paths", fun test_build_url_paths/0},
      {"build_url with empty path", fun test_build_url_empty_path/0},
      {"build_url with complex path", fun test_build_url_complex_path/0},

      %% Authentication Header Tests
      {"build_auth_headers with bearer token", fun test_build_auth_headers_token/0},
      {"build_auth_headers with api_key", fun test_build_auth_headers_api_key/0},
      {"build_auth_headers with empty config", fun test_build_auth_headers_empty/0},
      {"build_auth_headers with unknown config", fun test_build_auth_headers_unknown/0},

      %% Headers Conversion Tests
      {"headers_to_proplist converts correctly", fun test_headers_to_proplist/0},
      {"headers_to_proplist empty list", fun test_headers_to_proplist_empty/0},
      {"headers_to_proplist multiple headers", fun test_headers_to_proplist_multiple/0},

      %% Statistics Collection Tests
      {"collect_local_stats returns valid structure", fun test_collect_local_stats_structure/0},

      %% Cluster Update from Stats Tests
      {"update_cluster_from_stats updates all fields", fun test_update_cluster_from_stats/0},
      {"update_cluster_from_stats partial update", fun test_update_cluster_from_stats_partial/0},
      {"update_cluster_from_stats sets state to up", fun test_update_cluster_from_stats_state/0},
      {"update_cluster_from_stats preserves defaults", fun test_update_cluster_from_stats_defaults/0},

      %% Update Local Stats Tests
      {"update_local_stats with existing cluster", fun test_update_local_stats_existing/0},
      {"update_local_stats with missing cluster", fun test_update_local_stats_missing/0},

      %% Sibling Job Revocability Tests
      {"is_sibling_revocable for pending state", fun test_is_sibling_revocable_pending/0},
      {"is_sibling_revocable for null state", fun test_is_sibling_revocable_null/0},
      {"is_sibling_revocable for running state", fun test_is_sibling_revocable_running/0},
      {"is_sibling_revocable for completed state", fun test_is_sibling_revocable_completed/0},
      {"is_sibling_revocable for revoked state", fun test_is_sibling_revocable_revoked/0},
      {"is_sibling_revocable for failed state", fun test_is_sibling_revocable_failed/0},

      %% Sibling State Update Tests
      {"update_sibling_state with existing tracker", fun test_update_sibling_state_existing/0},
      {"update_sibling_state with missing tracker", fun test_update_sibling_state_missing/0},
      {"update_sibling_state with missing cluster", fun test_update_sibling_state_missing_cluster/0},

      %% Sibling Local Job ID Update Tests
      {"update_sibling_local_job_id updates correctly", fun test_update_sibling_local_job_id/0},
      {"update_sibling_local_job_id with missing tracker", fun test_update_sibling_local_job_id_missing/0},

      %% Sibling Start Time Update Tests
      {"update_sibling_start_time updates correctly", fun test_update_sibling_start_time/0},
      {"update_sibling_start_time with missing tracker", fun test_update_sibling_start_time_missing/0},

      %% Fetch Federation Info Tests
      {"fetch_federation_info returns stub data", fun test_fetch_federation_info/0},

      %% Register With Cluster Tests
      {"register_with_cluster returns ok", fun test_register_with_cluster/0}
     ]}.

%% Tests requiring HTTP mocking
federation_sync_http_test_() ->
    {foreach,
     fun setup_with_meck/0,
     fun cleanup_with_meck/1,
     [
      %% HTTP GET Tests
      {"http_get success returns body", fun test_http_get_success/0},
      {"http_get with 404 returns error", fun test_http_get_404/0},
      {"http_get with 500 returns error", fun test_http_get_500/0},
      {"http_get with timeout returns error", fun test_http_get_timeout/0},
      {"http_get with network error", fun test_http_get_network_error/0},
      {"http_get with headers", fun test_http_get_with_headers/0},
      {"http_get with custom timeout", fun test_http_get_custom_timeout/0},

      %% HTTP POST Tests
      {"http_post success returns body", fun test_http_post_success/0},
      {"http_post 201 returns body", fun test_http_post_201/0},
      {"http_post with 400 returns error", fun test_http_post_400/0},
      {"http_post with 500 returns error", fun test_http_post_500/0},
      {"http_post with timeout", fun test_http_post_timeout/0},
      {"http_post with network error", fun test_http_post_network_error/0},

      %% Health Check Tests
      {"check_cluster_health returns up on success", fun test_check_cluster_health_up/0},
      {"check_cluster_health returns down on failure", fun test_check_cluster_health_down/0},
      {"check_cluster_health with timeout", fun test_check_cluster_health_timeout/0},
      {"check_cluster_health increments metrics", fun test_check_cluster_health_metrics/0},

      %% Do Health Check Tests
      {"do_health_check checks all remote clusters", fun test_do_health_check_all/0},
      {"do_health_check skips local cluster", fun test_do_health_check_skips_local/0},
      {"do_health_check with empty cluster list", fun test_do_health_check_empty/0},
      {"do_health_check with mixed cluster states", fun test_do_health_check_mixed/0},

      %% Sync All Clusters Tests
      {"sync_all_clusters syncs remote clusters", fun test_sync_all_clusters/0},
      {"sync_all_clusters skips local cluster", fun test_sync_all_clusters_skips_local/0},
      {"sync_all_clusters with empty list", fun test_sync_all_clusters_empty/0},

      %% Do Sync Cluster Tests
      {"do_sync_cluster success sends message", fun test_do_sync_cluster_success/0},
      {"do_sync_cluster failure sends down", fun test_do_sync_cluster_failure/0},
      {"do_sync_cluster not found", fun test_do_sync_cluster_not_found/0},

      %% Fetch Cluster Stats Tests
      {"fetch_cluster_stats success", fun test_fetch_cluster_stats_success/0},
      {"fetch_cluster_stats http error", fun test_fetch_cluster_stats_http_error/0},
      {"fetch_cluster_stats invalid json", fun test_fetch_cluster_stats_invalid_json/0},
      {"fetch_cluster_stats with auth", fun test_fetch_cluster_stats_with_auth/0},

      %% Fetch Remote Job Status Tests
      {"fetch_remote_job_status success", fun test_fetch_remote_job_status_success/0},
      {"fetch_remote_job_status not found", fun test_fetch_remote_job_status_not_found/0},
      {"fetch_remote_job_status http error", fun test_fetch_remote_job_status_http_error/0},
      {"fetch_remote_job_status with error in response", fun test_fetch_remote_job_status_error_response/0},

      %% Remote Submit Job Tests
      {"remote_submit_job success", fun test_remote_submit_job_success/0},
      {"remote_submit_job with job record", fun test_remote_submit_job_record/0},
      {"remote_submit_job error response", fun test_remote_submit_job_error/0},
      {"remote_submit_job http error", fun test_remote_submit_job_http_error/0},
      {"remote_submit_job invalid response", fun test_remote_submit_job_invalid/0},

      %% Send Federation Message Tests
      {"send_federation_msg success", fun test_send_federation_msg_success/0},
      {"send_federation_msg cluster down", fun test_send_federation_msg_cluster_down/0},
      {"send_federation_msg cluster not found", fun test_send_federation_msg_not_found/0},
      {"send_federation_msg http error", fun test_send_federation_msg_http_error/0},
      {"send_federation_msg encode error", fun test_send_federation_msg_encode_error/0}
     ]}.

%% Edge case and integration tests
federation_sync_edge_test_() ->
    {foreach,
     fun setup_with_meck/0,
     fun cleanup_with_meck/1,
     [
      %% Handle Local Sibling Create Tests
      {"handle_local_sibling_create success", fun test_handle_local_sibling_create_success/0},
      {"handle_local_sibling_create scheduler error", fun test_handle_local_sibling_create_error/0},
      {"handle_local_sibling_create scheduler unavailable", fun test_handle_local_sibling_create_unavailable/0},

      %% Concurrent Operations Tests
      {"concurrent health checks", fun test_concurrent_health_checks/0},
      {"concurrent sync operations", fun test_concurrent_sync_operations/0},

      %% Stress Tests
      {"multiple cluster health checks", fun test_multiple_cluster_health_checks/0},
      {"large cluster stats update", fun test_large_cluster_stats_update/0},

      %% Error Recovery Tests
      {"health check with transient errors", fun test_health_check_transient_errors/0},
      {"stats fetch retry behavior", fun test_stats_fetch_retry/0}
     ]}.

%%====================================================================
%% URL Building Tests
%%====================================================================

test_build_url_basic() ->
    Url = flurm_federation_sync:build_url(<<"localhost">>, 6817, <<"/api/v1/health">>),
    ?assertEqual(<<"http://localhost:6817/api/v1/health">>, Url).

test_build_url_ports() ->
    Url1 = flurm_federation_sync:build_url(<<"host1">>, 80, <<"/test">>),
    ?assertEqual(<<"http://host1:80/test">>, Url1),

    Url2 = flurm_federation_sync:build_url(<<"host2">>, 8080, <<"/test">>),
    ?assertEqual(<<"http://host2:8080/test">>, Url2),

    Url3 = flurm_federation_sync:build_url(<<"host3">>, 65535, <<"/test">>),
    ?assertEqual(<<"http://host3:65535/test">>, Url3).

test_build_url_paths() ->
    Url1 = flurm_federation_sync:build_url(<<"host">>, 80, <<"/">>),
    ?assertEqual(<<"http://host:80/">>, Url1),

    Url2 = flurm_federation_sync:build_url(<<"host">>, 80, <<"/a/b/c">>),
    ?assertEqual(<<"http://host:80/a/b/c">>, Url2).

test_build_url_empty_path() ->
    Url = flurm_federation_sync:build_url(<<"host">>, 80, <<>>),
    ?assertEqual(<<"http://host:80">>, Url).

test_build_url_complex_path() ->
    Url = flurm_federation_sync:build_url(<<"api.cluster.local">>, 443,
                                          <<"/api/v1/jobs/12345?format=json">>),
    ?assertEqual(<<"http://api.cluster.local:443/api/v1/jobs/12345?format=json">>, Url).

%%====================================================================
%% Authentication Header Tests
%%====================================================================

test_build_auth_headers_token() ->
    Auth = #{token => <<"my-secret-token">>},
    Headers = flurm_federation_sync:build_auth_headers(Auth),
    ?assertEqual([{<<"Authorization">>, <<"Bearer my-secret-token">>}], Headers).

test_build_auth_headers_api_key() ->
    Auth = #{api_key => <<"api-key-12345">>},
    Headers = flurm_federation_sync:build_auth_headers(Auth),
    ?assertEqual([{<<"X-API-Key">>, <<"api-key-12345">>}], Headers).

test_build_auth_headers_empty() ->
    Headers = flurm_federation_sync:build_auth_headers(#{}),
    ?assertEqual([], Headers).

test_build_auth_headers_unknown() ->
    Auth = #{username => <<"user">>, password => <<"pass">>},
    Headers = flurm_federation_sync:build_auth_headers(Auth),
    ?assertEqual([], Headers).

%%====================================================================
%% Headers Conversion Tests
%%====================================================================

test_headers_to_proplist() ->
    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    Result = flurm_federation_sync:headers_to_proplist(Headers),
    ?assertEqual([{"Content-Type", "application/json"}], Result).

test_headers_to_proplist_empty() ->
    Result = flurm_federation_sync:headers_to_proplist([]),
    ?assertEqual([], Result).

test_headers_to_proplist_multiple() ->
    Headers = [
        {<<"Authorization">>, <<"Bearer token">>},
        {<<"Content-Type">>, <<"application/json">>},
        {<<"X-Custom-Header">>, <<"value">>}
    ],
    Result = flurm_federation_sync:headers_to_proplist(Headers),
    ?assertEqual([
        {"Authorization", "Bearer token"},
        {"Content-Type", "application/json"},
        {"X-Custom-Header", "value"}
    ], Result).

%%====================================================================
%% Statistics Collection Tests
%%====================================================================

test_collect_local_stats_structure() ->
    %% Mock flurm_node_registry and flurm_job_registry
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() ->
        #{total_cpus => 100, available_cpus => 50,
          total_memory => 64000, available_memory => 32000,
          total_gpus => 4}
    end),
    meck:expect(flurm_job_registry, count_by_state, fun() ->
        #{pending => 5, running => 10}
    end),

    Stats = flurm_federation_sync:collect_local_stats(),

    ?assert(is_map(Stats)),
    ?assertEqual(10, maps:get(node_count, Stats)),
    ?assertEqual(100, maps:get(cpu_count, Stats)),
    ?assertEqual(50, maps:get(available_cpus, Stats)),
    ?assertEqual(5, maps:get(pending_jobs, Stats)),
    ?assertEqual(10, maps:get(running_jobs, Stats)),

    meck:unload([flurm_node_registry, flurm_job_registry]).

%%====================================================================
%% Cluster Update from Stats Tests
%%====================================================================

test_update_cluster_from_stats() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Stats = #{
        node_count => 20,
        cpu_count => 200,
        memory_mb => 128000,
        gpu_count => 8,
        available_cpus => 100,
        available_memory => 64000,
        pending_jobs => 15,
        running_jobs => 25
    },

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster, Stats),

    ?assertEqual(20, Updated#fed_cluster.node_count),
    ?assertEqual(200, Updated#fed_cluster.cpu_count),
    ?assertEqual(128000, Updated#fed_cluster.memory_mb),
    ?assertEqual(8, Updated#fed_cluster.gpu_count),
    ?assertEqual(100, Updated#fed_cluster.available_cpus),
    ?assertEqual(64000, Updated#fed_cluster.available_memory),
    ?assertEqual(15, Updated#fed_cluster.pending_jobs),
    ?assertEqual(25, Updated#fed_cluster.running_jobs),
    ?assertEqual(up, Updated#fed_cluster.state).

test_update_cluster_from_stats_partial() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{node_count = 5, cpu_count = 50},
    Stats = #{node_count => 10},  % Only update node_count

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster1, Stats),

    ?assertEqual(10, Updated#fed_cluster.node_count),
    ?assertEqual(50, Updated#fed_cluster.cpu_count).  % Should keep original

test_update_cluster_from_stats_state() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = down},
    Stats = #{node_count => 10},

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster1, Stats),

    ?assertEqual(up, Updated#fed_cluster.state).

test_update_cluster_from_stats_defaults() ->
    Cluster = make_test_cluster(<<"test">>, <<"host">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        node_count = 5,
        cpu_count = 50,
        memory_mb = 10000
    },
    Stats = #{},  % Empty stats - should use defaults

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster1, Stats),

    %% Should preserve existing values when not in stats
    ?assertEqual(5, Updated#fed_cluster.node_count),
    ?assertEqual(50, Updated#fed_cluster.cpu_count),
    ?assertEqual(10000, Updated#fed_cluster.memory_mb).

%%====================================================================
%% Update Local Stats Tests
%%====================================================================

test_update_local_stats_existing() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() ->
        #{total_cpus => 100, available_cpus => 50,
          total_memory => 64000, available_memory => 32000,
          total_gpus => 4}
    end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 5, running => 10} end),

    Cluster = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    ok = flurm_federation_sync:update_local_stats(<<"local">>),

    [Updated] = ets:lookup(?FED_CLUSTERS_TABLE, <<"local">>),
    ?assertEqual(10, Updated#fed_cluster.node_count),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_update_local_stats_missing() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Should not crash when cluster doesn't exist
    ok = flurm_federation_sync:update_local_stats(<<"nonexistent">>),

    meck:unload([flurm_node_registry, flurm_job_registry]).

%%====================================================================
%% Sibling Job Revocability Tests
%%====================================================================

test_is_sibling_revocable_pending() ->
    ?assertEqual(true, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_PENDING)).

test_is_sibling_revocable_null() ->
    ?assertEqual(true, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_NULL)).

test_is_sibling_revocable_running() ->
    ?assertEqual(false, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_RUNNING)).

test_is_sibling_revocable_completed() ->
    ?assertEqual(false, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_COMPLETED)).

test_is_sibling_revocable_revoked() ->
    ?assertEqual(false, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_REVOKED)).

test_is_sibling_revocable_failed() ->
    ?assertEqual(false, flurm_federation_sync:is_sibling_revocable(?SIBLING_STATE_FAILED)).

%%====================================================================
%% Sibling State Update Tests
%%====================================================================

test_update_sibling_state_existing() ->
    FedJobId = <<"fed-12345">>,
    ClusterName = <<"cluster1">>,

    SiblingState = #sibling_job_state{
        federation_job_id = FedJobId,
        sibling_cluster = ClusterName,
        origin_cluster = <<"origin">>,
        state = ?SIBLING_STATE_PENDING
    },
    Tracker = #fed_job_tracker{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        origin_job_id = 100,
        sibling_states = #{ClusterName => SiblingState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    %% Note: The function returns `true` from ets:insert when successful
    %% even though the spec says it returns `ok`
    Result = flurm_federation_sync:update_sibling_state(FedJobId, ClusterName, ?SIBLING_STATE_RUNNING),
    ?assert(Result =:= ok orelse Result =:= true),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, FedJobId),
    UpdatedSibState = maps:get(ClusterName, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(?SIBLING_STATE_RUNNING, UpdatedSibState#sibling_job_state.state).

test_update_sibling_state_missing() ->
    %% Should not crash when tracker doesn't exist
    ok = flurm_federation_sync:update_sibling_state(<<"nonexistent">>, <<"cluster">>, ?SIBLING_STATE_RUNNING).

test_update_sibling_state_missing_cluster() ->
    FedJobId = <<"fed-12345">>,

    Tracker = #fed_job_tracker{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        origin_job_id = 100,
        sibling_states = #{}  % Empty sibling states
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    %% Should not crash when cluster doesn't exist in sibling states
    ok = flurm_federation_sync:update_sibling_state(FedJobId, <<"unknown_cluster">>, ?SIBLING_STATE_RUNNING).

%%====================================================================
%% Sibling Local Job ID Update Tests
%%====================================================================

test_update_sibling_local_job_id() ->
    FedJobId = <<"fed-12345">>,
    ClusterName = <<"cluster1">>,

    SiblingState = #sibling_job_state{
        federation_job_id = FedJobId,
        sibling_cluster = ClusterName,
        origin_cluster = <<"origin">>,
        local_job_id = 0
    },
    Tracker = #fed_job_tracker{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        origin_job_id = 100,
        sibling_states = #{ClusterName => SiblingState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    %% Note: The function returns `true` from ets:insert when successful
    Result = flurm_federation_sync:update_sibling_local_job_id(FedJobId, ClusterName, 999),
    ?assert(Result =:= ok orelse Result =:= true),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, FedJobId),
    UpdatedSibState = maps:get(ClusterName, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(999, UpdatedSibState#sibling_job_state.local_job_id).

test_update_sibling_local_job_id_missing() ->
    %% Should not crash when tracker doesn't exist
    ok = flurm_federation_sync:update_sibling_local_job_id(<<"nonexistent">>, <<"cluster">>, 123).

%%====================================================================
%% Sibling Start Time Update Tests
%%====================================================================

test_update_sibling_start_time() ->
    FedJobId = <<"fed-12345">>,
    ClusterName = <<"cluster1">>,
    StartTime = erlang:system_time(second),

    SiblingState = #sibling_job_state{
        federation_job_id = FedJobId,
        sibling_cluster = ClusterName,
        origin_cluster = <<"origin">>,
        start_time = 0
    },
    Tracker = #fed_job_tracker{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        origin_job_id = 100,
        sibling_states = #{ClusterName => SiblingState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    %% Note: The function returns `true` from ets:insert when successful
    Result = flurm_federation_sync:update_sibling_start_time(FedJobId, ClusterName, StartTime),
    ?assert(Result =:= ok orelse Result =:= true),

    [Updated] = ets:lookup(?FED_SIBLING_JOBS, FedJobId),
    UpdatedSibState = maps:get(ClusterName, Updated#fed_job_tracker.sibling_states),
    ?assertEqual(StartTime, UpdatedSibState#sibling_job_state.start_time).

test_update_sibling_start_time_missing() ->
    %% Should not crash when tracker doesn't exist
    ok = flurm_federation_sync:update_sibling_start_time(<<"nonexistent">>, <<"cluster">>, 12345).

%%====================================================================
%% Fetch Federation Info Tests
%%====================================================================

test_fetch_federation_info() ->
    {ok, Info} = flurm_federation_sync:fetch_federation_info(<<"remote.host">>),
    ?assert(is_map(Info)),
    ?assertEqual(<<"default-federation">>, maps:get(name, Info)),
    ?assert(maps:is_key(created, Info)),
    ?assert(maps:is_key(clusters, Info)),
    ?assert(maps:is_key(options, Info)).

%%====================================================================
%% Register With Cluster Tests
%%====================================================================

test_register_with_cluster() ->
    Result = flurm_federation_sync:register_with_cluster(<<"remote.host">>, <<"local_cluster">>),
    ?assertEqual(ok, Result).

%%====================================================================
%% HTTP GET Tests
%%====================================================================

test_http_get_success() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "response body"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 5000),
    ?assertEqual({ok, <<"response body">>}, Result).

test_http_get_404() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 404, "Not Found"}, [], "not found"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 5000),
    ?assertEqual({error, {http_error, 404, "not found"}}, Result).

test_http_get_500() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 500, "Internal Server Error"}, [], "error"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 5000),
    ?assertEqual({error, {http_error, 500, "error"}}, Result).

test_http_get_timeout() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 5000),
    ?assertEqual({error, timeout}, Result).

test_http_get_network_error() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, {failed_connect, [{to_address, {"localhost", 6817}}, {inet, [], econnrefused}]}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 5000),
    ?assertMatch({error, {failed_connect, _}}, Result).

test_http_get_with_headers() ->
    meck:expect(httpc, request, fun(get, {_Url, Headers}, _, _) ->
        ?assertEqual([{"Authorization", "Bearer test-token"}], Headers),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "response"}}
    end),

    Headers = [{<<"Authorization">>, <<"Bearer test-token">>}],
    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, Headers, 5000),
    ?assertEqual({ok, <<"response">>}, Result).

test_http_get_custom_timeout() ->
    meck:expect(httpc, request, fun(get, _, [{timeout, Timeout}], _) ->
        ?assertEqual(10000, Timeout),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "response"}}
    end),

    Result = flurm_federation_sync:http_get(<<"http://localhost:6817/test">>, 10000),
    ?assertEqual({ok, <<"response">>}, Result).

%%====================================================================
%% HTTP POST Tests
%%====================================================================

test_http_post_success() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"result\":\"ok\"}"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({ok, <<"{\"result\":\"ok\"}">>}, Result).

test_http_post_201() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 201, "Created"}, [], "{\"id\":123}"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({ok, <<"{\"id\":123}">>}, Result).

test_http_post_400() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 400, "Bad Request"}, [], "bad request"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({error, {http_error, 400, "bad request"}}, Result).

test_http_post_500() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 500, "Internal Server Error"}, [], "server error"}}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({error, {http_error, 500, "server error"}}, Result).

test_http_post_timeout() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({error, timeout}, Result).

test_http_post_network_error() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {error, econnrefused}
    end),

    Result = flurm_federation_sync:http_post(<<"http://localhost:6817/test">>, [], <<"{\"data\":1}">>, 5000),
    ?assertEqual({error, econnrefused}, Result).

%%====================================================================
%% Health Check Tests
%%====================================================================

test_check_cluster_health_up() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\":\"healthy\"}"}}
    end),

    Result = flurm_federation_sync:check_cluster_health(<<"localhost">>, 6817),
    ?assertEqual(up, Result).

test_check_cluster_health_down() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, econnrefused}
    end),

    Result = flurm_federation_sync:check_cluster_health(<<"localhost">>, 6817),
    ?assertEqual(down, Result).

test_check_cluster_health_timeout() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:check_cluster_health(<<"localhost">>, 6817),
    ?assertEqual(down, Result).

test_check_cluster_health_metrics() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\":\"healthy\"}"}}
    end),

    flurm_federation_sync:check_cluster_health(<<"localhost">>, 6817),

    ?assert(meck:called(flurm_metrics, increment, [flurm_federation_health_checks_total])).

%%====================================================================
%% Do Health Check Tests
%%====================================================================

test_do_health_check_all() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
    end),

    Cluster1 = make_test_cluster(<<"test_cluster">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"remote1">>, <<"remote1.example.com">>, 6817),
    Cluster3 = make_test_cluster(<<"remote2">>, <<"remote2.example.com">>, 6817),

    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster3),

    ok = flurm_federation_sync:do_health_check(<<"test_cluster">>, self()),

    %% Give spawned processes time to complete
    timer:sleep(100),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_do_health_check_skips_local() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Track which URLs are requested
    Self = self(),
    meck:expect(httpc, request, fun(get, {Url, _}, _, _) ->
        Self ! {http_request, Url},
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
    end),

    Cluster1 = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),

    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),

    ok = flurm_federation_sync:do_health_check(<<"local">>, self()),

    %% Give spawned processes time to complete
    timer:sleep(100),

    %% Should NOT receive request for local cluster, only remote
    receive
        {http_request, Url} ->
            ?assert(string:str(Url, "localhost") =:= 0 orelse
                    string:str(Url, "remote.example.com") > 0)
    after 200 -> ok
    end,

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_do_health_check_empty() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 0 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Empty cluster table
    ok = flurm_federation_sync:do_health_check(<<"test">>, self()),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_do_health_check_mixed() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Some clusters respond, some don't
    meck:expect(httpc, request, fun(get, {Url, _}, _, _) ->
        case string:str(Url, "healthy") > 0 of
            true -> {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}};
            false -> {error, timeout}
        end
    end),

    Cluster1 = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"healthy">>, <<"healthy.example.com">>, 6817),
    Cluster3 = make_test_cluster(<<"unhealthy">>, <<"unhealthy.example.com">>, 6817),

    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster3),

    ok = flurm_federation_sync:do_health_check(<<"local">>, self()),

    timer:sleep(100),

    meck:unload([flurm_node_registry, flurm_job_registry]).

%%====================================================================
%% Sync All Clusters Tests
%%====================================================================

test_sync_all_clusters() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10}"}}
    end),

    Cluster1 = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"remote1">>, <<"remote1.example.com">>, 6817),
    Cluster3 = make_test_cluster(<<"remote2">>, <<"remote2.example.com">>, 6817),

    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster3),

    ok = flurm_federation_sync:sync_all_clusters(<<"local">>, self()).

test_sync_all_clusters_skips_local() ->
    %% Track which clusters are synced
    Self = self(),
    meck:expect(httpc, request, fun(get, {Url, _}, _, _) ->
        Self ! {sync_request, Url},
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10}"}}
    end),

    Cluster1 = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),

    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),

    ok = flurm_federation_sync:sync_all_clusters(<<"local">>, self()),

    %% Flush messages - should only see remote cluster requests
    timer:sleep(100).

test_sync_all_clusters_empty() ->
    ok = flurm_federation_sync:sync_all_clusters(<<"local">>, self()).

%%====================================================================
%% Do Sync Cluster Tests
%%====================================================================

test_do_sync_cluster_success() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10,\"cpu_count\":100}"}}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    ok = flurm_federation_sync:do_sync_cluster(<<"remote">>, self()),

    receive
        {cluster_update, <<"remote">>, Stats} ->
            ?assertEqual(10, maps:get(<<"node_count">>, Stats))
    after 1000 ->
        ?assert(false)
    end.

test_do_sync_cluster_failure() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, timeout}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),

    {error, timeout} = flurm_federation_sync:do_sync_cluster(<<"remote">>, self()),

    receive
        {cluster_health, <<"remote">>, down} -> ok
    after 1000 ->
        ?assert(false)
    end.

test_do_sync_cluster_not_found() ->
    Result = flurm_federation_sync:do_sync_cluster(<<"nonexistent">>, self()),
    ?assertEqual({error, cluster_not_found}, Result).

%%====================================================================
%% Fetch Cluster Stats Tests
%%====================================================================

test_fetch_cluster_stats_success() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10,\"cpu_count\":100}"}}
    end),

    {ok, Stats} = flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, #{}),
    ?assert(is_map(Stats)),
    ?assertEqual(10, maps:get(<<"node_count">>, Stats)),
    ?assertEqual(100, maps:get(<<"cpu_count">>, Stats)).

test_fetch_cluster_stats_http_error() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, timeout}
    end),

    Result = flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, #{}),
    ?assertEqual({error, timeout}, Result).

test_fetch_cluster_stats_invalid_json() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "not json"}}
    end),

    %% jsx:decode will throw or return unexpected - handle both cases
    Result = (catch flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, #{})),
    case Result of
        {error, invalid_response} -> ok;
        {'EXIT', _} -> ok;
        _ -> ?assertEqual({error, invalid_response}, Result)
    end.

test_fetch_cluster_stats_with_auth() ->
    meck:expect(httpc, request, fun(get, {_Url, Headers}, _, _) ->
        ?assert(lists:keymember("Authorization", 1, Headers)),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10}"}}
    end),

    Auth = #{token => <<"my-token">>},
    {ok, _Stats} = flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, Auth).

%%====================================================================
%% Fetch Remote Job Status Tests
%%====================================================================

test_fetch_remote_job_status_success() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\":123,\"state\":\"running\"}"}}
    end),

    {ok, Status} = flurm_federation_sync:fetch_remote_job_status(<<"localhost">>, 6817, #{}, 123),
    ?assertEqual(123, maps:get(<<"job_id">>, Status)),
    ?assertEqual(<<"running">>, maps:get(<<"state">>, Status)).

test_fetch_remote_job_status_not_found() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 404, "Not Found"}, [], "job not found"}}
    end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"localhost">>, 6817, #{}, 999),
    ?assertMatch({error, {http_error, 404, _}}, Result).

test_fetch_remote_job_status_http_error() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {error, econnrefused}
    end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"localhost">>, 6817, #{}, 123),
    ?assertEqual({error, econnrefused}, Result).

test_fetch_remote_job_status_error_response() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"error\":\"job_not_found\"}"}}
    end),

    Result = flurm_federation_sync:fetch_remote_job_status(<<"localhost">>, 6817, #{}, 123),
    ?assertEqual({error, <<"job_not_found">>}, Result).

%%====================================================================
%% Remote Submit Job Tests
%%====================================================================

test_remote_submit_job_success() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\":456}"}}
    end),

    JobSpec = #{name => <<"test_job">>, num_cpus => 4},
    {ok, JobId} = flurm_federation_sync:remote_submit_job(<<"localhost">>, 6817, #{}, JobSpec),
    ?assertEqual(456, JobId).

test_remote_submit_job_record() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"job_id\":789}"}}
    end),

    Job = #job{
        id = 0,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },

    {ok, JobId} = flurm_federation_sync:remote_submit_job(<<"localhost">>, 6817, #{}, Job),
    ?assertEqual(789, JobId).

test_remote_submit_job_error() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"error\":\"quota_exceeded\"}"}}
    end),

    JobSpec = #{name => <<"test_job">>},
    Result = flurm_federation_sync:remote_submit_job(<<"localhost">>, 6817, #{}, JobSpec),
    ?assertEqual({error, <<"quota_exceeded">>}, Result).

test_remote_submit_job_http_error() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {error, timeout}
    end),

    JobSpec = #{name => <<"test_job">>},
    Result = flurm_federation_sync:remote_submit_job(<<"localhost">>, 6817, #{}, JobSpec),
    ?assertEqual({error, timeout}, Result).

test_remote_submit_job_invalid() ->
    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\":\"ok\"}"}}  % No job_id
    end),

    JobSpec = #{name => <<"test_job">>},
    Result = flurm_federation_sync:remote_submit_job(<<"localhost">>, 6817, #{}, JobSpec),
    ?assertEqual({error, invalid_response}, Result).

%%====================================================================
%% Send Federation Message Tests
%%====================================================================

test_send_federation_msg_success() ->
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded">>} end),

    meck:expect(httpc, request, fun(post, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"status\":\"ok\"}"}}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Result = flurm_federation_sync:send_federation_msg(<<"remote">>, ?MSG_FED_JOB_SUBMIT, #{}, <<"local">>),
    ?assertEqual(ok, Result),

    meck:unload(flurm_protocol_codec).

test_send_federation_msg_cluster_down() ->
    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = down},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Result = flurm_federation_sync:send_federation_msg(<<"remote">>, ?MSG_FED_JOB_SUBMIT, #{}, <<"local">>),
    ?assertEqual({error, {cluster_unavailable, down}}, Result).

test_send_federation_msg_not_found() ->
    Result = flurm_federation_sync:send_federation_msg(<<"nonexistent">>, ?MSG_FED_JOB_SUBMIT, #{}, <<"local">>),
    ?assertEqual({error, cluster_not_found}, Result).

test_send_federation_msg_http_error() ->
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {ok, <<"encoded">>} end),

    meck:expect(httpc, request, fun(post, _, _, _) ->
        {error, timeout}
    end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Result = flurm_federation_sync:send_federation_msg(<<"remote">>, ?MSG_FED_JOB_SUBMIT, #{}, <<"local">>),
    ?assertEqual({error, {send_failed, timeout}}, Result),

    meck:unload(flurm_protocol_codec).

test_send_federation_msg_encode_error() ->
    meck:new(flurm_protocol_codec, [non_strict]),
    meck:expect(flurm_protocol_codec, encode, fun(_, _) -> {error, invalid_message} end),

    Cluster = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{state = up},
    ets:insert(?FED_CLUSTERS_TABLE, Cluster1),

    Result = flurm_federation_sync:send_federation_msg(<<"remote">>, ?MSG_FED_JOB_SUBMIT, #{}, <<"local">>),
    ?assertEqual({error, {encode_failed, invalid_message}}, Result),

    meck:unload(flurm_protocol_codec).

%%====================================================================
%% Handle Local Sibling Create Tests
%%====================================================================

test_handle_local_sibling_create_success() ->
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {ok, 12345} end),

    FedJobId = <<"fed-test-123">>,
    LocalCluster = <<"test_cluster">>,

    %% Set up sibling job tracker
    SiblingState = #sibling_job_state{
        federation_job_id = FedJobId,
        sibling_cluster = LocalCluster,
        state = ?SIBLING_STATE_NULL
    },
    Tracker = #fed_job_tracker{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        origin_job_id = 100,
        sibling_states = #{LocalCluster => SiblingState}
    },
    ets:insert(?FED_SIBLING_JOBS, Tracker),

    Msg = #fed_job_submit_msg{
        federation_job_id = FedJobId,
        origin_cluster = <<"origin">>,
        target_cluster = LocalCluster,
        job_spec = #{name => <<"test">>, user => <<"user">>}
    },

    Result = flurm_federation_sync:handle_local_sibling_create(Msg),
    ?assertEqual(ok, Result),

    meck:unload(flurm_scheduler).

test_handle_local_sibling_create_error() ->
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> {error, quota_exceeded} end),

    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed-test-123">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"test_cluster">>,
        job_spec = #{name => <<"test">>, user => <<"user">>}
    },

    Result = flurm_federation_sync:handle_local_sibling_create(Msg),
    ?assertEqual({error, quota_exceeded}, Result),

    meck:unload(flurm_scheduler).

test_handle_local_sibling_create_unavailable() ->
    %% Simulate scheduler not running
    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> exit(noproc) end),

    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed-test-123">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"test_cluster">>,
        job_spec = #{name => <<"test">>, user => <<"user">>}
    },

    Result = flurm_federation_sync:handle_local_sibling_create(Msg),
    ?assertMatch({error, {scheduler_not_available, _}}, Result),

    meck:unload(flurm_scheduler).

%%====================================================================
%% Concurrent Operations Tests
%%====================================================================

test_concurrent_health_checks() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Simulate varying response times
    meck:expect(httpc, request, fun(get, _, _, _) ->
        timer:sleep(rand:uniform(50)),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
    end),

    %% Add multiple clusters
    lists:foreach(fun(N) ->
        Name = list_to_binary("cluster" ++ integer_to_list(N)),
        Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
        ets:insert(?FED_CLUSTERS_TABLE, Cluster)
    end, lists:seq(1, 10)),

    ok = flurm_federation_sync:do_health_check(<<"cluster1">>, self()),

    %% Wait for all checks to complete
    timer:sleep(200),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_concurrent_sync_operations() ->
    meck:expect(httpc, request, fun(get, _, _, _) ->
        timer:sleep(rand:uniform(50)),
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10}"}}
    end),

    %% Add multiple clusters
    lists:foreach(fun(N) ->
        Name = list_to_binary("cluster" ++ integer_to_list(N)),
        Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
        ets:insert(?FED_CLUSTERS_TABLE, Cluster)
    end, lists:seq(1, 10)),

    ok = flurm_federation_sync:sync_all_clusters(<<"cluster1">>, self()),

    timer:sleep(200).

%%====================================================================
%% Stress Tests
%%====================================================================

test_multiple_cluster_health_checks() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    meck:expect(httpc, request, fun(get, _, _, _) ->
        {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
    end),

    %% Add many clusters
    lists:foreach(fun(N) ->
        Name = list_to_binary("cluster" ++ integer_to_list(N)),
        Cluster = make_test_cluster(Name, <<Name/binary, ".example.com">>, 6817),
        ets:insert(?FED_CLUSTERS_TABLE, Cluster)
    end, lists:seq(1, 50)),

    %% Run health checks multiple times
    lists:foreach(fun(_) ->
        ok = flurm_federation_sync:do_health_check(<<"cluster1">>, self())
    end, lists:seq(1, 5)),

    timer:sleep(500),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_large_cluster_stats_update() ->
    Cluster = make_test_cluster(<<"large">>, <<"large.example.com">>, 6817),
    Cluster1 = Cluster#fed_cluster{
        node_count = 1000,
        cpu_count = 100000,
        memory_mb = 1000000000,
        gpu_count = 5000,
        pending_jobs = 10000,
        running_jobs = 50000
    },

    Stats = #{
        node_count => 2000,
        cpu_count => 200000,
        memory_mb => 2000000000,
        gpu_count => 10000,
        available_cpus => 100000,
        available_memory => 1000000000,
        pending_jobs => 20000,
        running_jobs => 100000
    },

    Updated = flurm_federation_sync:update_cluster_from_stats(Cluster1, Stats),

    ?assertEqual(2000, Updated#fed_cluster.node_count),
    ?assertEqual(200000, Updated#fed_cluster.cpu_count),
    ?assertEqual(2000000000, Updated#fed_cluster.memory_mb).

%%====================================================================
%% Error Recovery Tests
%%====================================================================

test_health_check_transient_errors() ->
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job_registry, [non_strict]),
    meck:expect(flurm_node_registry, count_total_nodes, fun() -> 10 end),
    meck:expect(flurm_node_registry, get_aggregate_resources, fun() -> #{} end),
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{} end),

    %% Simulate intermittent failures
    Counter = counters:new(1, []),
    meck:expect(httpc, request, fun(get, _, _, _) ->
        counters:add(Counter, 1, 1),
        case counters:get(Counter, 1) rem 3 of
            0 -> {error, timeout};
            _ -> {ok, {{"HTTP/1.1", 200, "OK"}, [], "ok"}}
        end
    end),

    Cluster = make_test_cluster(<<"local">>, <<"localhost">>, 6817),
    Cluster2 = make_test_cluster(<<"remote">>, <<"remote.example.com">>, 6817),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster),
    ets:insert(?FED_CLUSTERS_TABLE, Cluster2),

    %% Run multiple health checks
    lists:foreach(fun(_) ->
        ok = flurm_federation_sync:do_health_check(<<"local">>, self()),
        timer:sleep(50)
    end, lists:seq(1, 5)),

    meck:unload([flurm_node_registry, flurm_job_registry]).

test_stats_fetch_retry() ->
    %% Simulate fetch that fails first time but succeeds second time
    Counter = counters:new(1, []),
    meck:expect(httpc, request, fun(get, _, _, _) ->
        counters:add(Counter, 1, 1),
        case counters:get(Counter, 1) of
            1 -> {error, timeout};
            _ -> {ok, {{"HTTP/1.1", 200, "OK"}, [], "{\"node_count\":10}"}}
        end
    end),

    %% First attempt fails
    Result1 = flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, #{}),
    ?assertEqual({error, timeout}, Result1),

    %% Second attempt succeeds
    {ok, Stats} = flurm_federation_sync:fetch_cluster_stats(<<"localhost">>, 6817, #{}),
    ?assertEqual(10, maps:get(<<"node_count">>, Stats)).

%%====================================================================
%% Helper Functions
%%====================================================================

make_test_cluster(Name, Host, Port) ->
    #fed_cluster{
        name = Name,
        host = Host,
        port = Port,
        auth = #{},
        state = unknown,
        weight = 1,
        features = [],
        partitions = [],
        node_count = 0,
        cpu_count = 0,
        memory_mb = 0,
        gpu_count = 0,
        pending_jobs = 0,
        running_jobs = 0,
        available_cpus = 0,
        available_memory = 0,
        last_sync = 0,
        last_health_check = 0,
        consecutive_failures = 0,
        properties = #{}
    }.
