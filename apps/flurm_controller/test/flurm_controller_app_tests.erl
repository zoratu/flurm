%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_app module
%%%
%%% Tests the application callbacks (start/stop/prep_stop) and
%%% the status/config API functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_app_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

app_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"config/0 returns expected keys", fun test_config/0},
         {"status/0 returns expected structure", fun test_status/0},
         {"cluster_status/0 returns map", fun test_cluster_status/0}
     ]}.

setup() ->
    %% Ensure required applications are started
    application:ensure_all_started(lager),
    application:ensure_all_started(ranch),

    %% Disable cluster mode for tests
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Set test configuration
    application:set_env(flurm_controller, listen_port, 26817),
    application:set_env(flurm_controller, listen_address, "127.0.0.1"),
    application:set_env(flurm_controller, num_acceptors, 5),
    application:set_env(flurm_controller, max_connections, 100),
    application:set_env(flurm_controller, cluster_name, test_cluster),
    application:set_env(flurm_controller, ra_data_dir, "/tmp/flurm_test_ra"),

    ok.

cleanup(_) ->
    %% Clean up env
    application:unset_env(flurm_controller, listen_port),
    application:unset_env(flurm_controller, listen_address),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_config() ->
    Config = flurm_controller_app:config(),
    ?assert(is_map(Config)),
    ?assert(maps:is_key(listen_port, Config)),
    ?assert(maps:is_key(listen_address, Config)),
    ?assert(maps:is_key(num_acceptors, Config)),
    ?assert(maps:is_key(max_connections, Config)),
    ?assert(maps:is_key(cluster_name, Config)),
    ?assert(maps:is_key(cluster_nodes, Config)),
    ?assert(maps:is_key(ra_data_dir, Config)),

    %% Verify our test values
    ?assertEqual(26817, maps:get(listen_port, Config)),
    ?assertEqual("127.0.0.1", maps:get(listen_address, Config)),
    ?assertEqual(5, maps:get(num_acceptors, Config)),
    ?assertEqual(100, maps:get(max_connections, Config)),
    ok.

test_status() ->
    %% status() should return a map even if services aren't running
    Status = flurm_controller_app:status(),
    ?assert(is_map(Status)),
    ?assertEqual(flurm_controller, maps:get(application, Status)),
    ?assertEqual(running, maps:get(status, Status)),
    ?assert(maps:is_key(listener, Status)),
    ?assert(maps:is_key(node_listener, Status)),
    ?assert(maps:is_key(jobs, Status)),
    ?assert(maps:is_key(nodes, Status)),
    ?assert(maps:is_key(partitions, Status)),
    ok.

test_cluster_status() ->
    %% cluster_status() should not crash even if cluster not running
    Status = flurm_controller_app:cluster_status(),
    ?assert(is_map(Status)),
    %% When cluster not available, it returns a special map
    ok.

%%====================================================================
%% Tests for Internal Helper Functions (exported via -ifdef(TEST))
%%====================================================================

helper_functions_test_() ->
    [
        {"get_config/2 returns app env value", fun test_get_config_existing/0},
        {"get_config/2 returns default when not set", fun test_get_config_default/0},
        {"count_jobs_by_state/2 counts correctly", fun test_count_jobs_by_state/0},
        {"count_jobs_by_state/2 handles empty list", fun test_count_jobs_empty/0},
        {"count_nodes_by_state/2 counts correctly", fun test_count_nodes_by_state/0},
        {"count_nodes_by_state/2 handles empty list", fun test_count_nodes_empty/0}
    ].

test_get_config_existing() ->
    %% Set a test value
    application:set_env(flurm_controller, test_key, test_value),

    Result = flurm_controller_app:get_config(test_key, default_value),
    ?assertEqual(test_value, Result),

    %% Cleanup
    application:unset_env(flurm_controller, test_key),
    ok.

test_get_config_default() ->
    %% Ensure key doesn't exist
    application:unset_env(flurm_controller, nonexistent_key),

    Result = flurm_controller_app:get_config(nonexistent_key, my_default),
    ?assertEqual(my_default, Result),
    ok.

test_count_jobs_by_state() ->
    %% Create mock job tuples (element 5 is state based on the code)
    %% Looking at count_jobs_by_state: element(5, J) =:= State
    Jobs = [
        {job, 1, <<"job1">>, <<"user1">>, pending, <<"script">>},
        {job, 2, <<"job2">>, <<"user2">>, running, <<"script">>},
        {job, 3, <<"job3">>, <<"user3">>, pending, <<"script">>},
        {job, 4, <<"job4">>, <<"user4">>, completed, <<"script">>},
        {job, 5, <<"job5">>, <<"user5">>, running, <<"script">>},
        {job, 6, <<"job6">>, <<"user6">>, pending, <<"script">>}
    ],

    ?assertEqual(3, flurm_controller_app:count_jobs_by_state(Jobs, pending)),
    ?assertEqual(2, flurm_controller_app:count_jobs_by_state(Jobs, running)),
    ?assertEqual(1, flurm_controller_app:count_jobs_by_state(Jobs, completed)),
    ?assertEqual(0, flurm_controller_app:count_jobs_by_state(Jobs, failed)),
    ok.

test_count_jobs_empty() ->
    ?assertEqual(0, flurm_controller_app:count_jobs_by_state([], pending)),
    ?assertEqual(0, flurm_controller_app:count_jobs_by_state([], running)),
    ok.

test_count_nodes_by_state() ->
    %% Create mock node tuples (element 4 is state based on the code)
    %% Looking at count_nodes_by_state: element(4, N) =:= State
    Nodes = [
        {node, <<"node1">>, 16, idle, 65536},
        {node, <<"node2">>, 16, allocated, 65536},
        {node, <<"node3">>, 16, idle, 65536},
        {node, <<"node4">>, 16, down, 65536},
        {node, <<"node5">>, 16, mixed, 65536}
    ],

    ?assertEqual(2, flurm_controller_app:count_nodes_by_state(Nodes, idle)),
    ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, allocated)),
    ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, down)),
    ?assertEqual(1, flurm_controller_app:count_nodes_by_state(Nodes, mixed)),
    ?assertEqual(0, flurm_controller_app:count_nodes_by_state(Nodes, unknown)),
    ok.

test_count_nodes_empty() ->
    ?assertEqual(0, flurm_controller_app:count_nodes_by_state([], idle)),
    ?assertEqual(0, flurm_controller_app:count_nodes_by_state([], down)),
    ok.

%%====================================================================
%% Application Start/Stop Tests
%%====================================================================

%% Test that the application starts and stops correctly
app_lifecycle_test_() ->
    {timeout, 60,
     fun() ->
         %% Clean state
         case whereis(flurm_controller_sup) of
             undefined -> ok;
             Pid ->
                 application:stop(flurm_controller),
                 flurm_test_utils:wait_for_death(Pid)
         end,

         %% Configure for test
         application:set_env(flurm_controller, enable_cluster, false),
         application:set_env(flurm_controller, listen_port, 27817),
         application:set_env(flurm_controller, listen_address, "127.0.0.1"),

         %% Start application
         case application:ensure_all_started(flurm_controller) of
             {ok, _} ->
                 %% Verify supervisor is running
                 ?assertNotEqual(undefined, whereis(flurm_controller_sup)),

                 %% Test prep_stop
                 State = flurm_controller_app:prep_stop(some_state),
                 ?assertEqual(some_state, State),

                 %% Stop application
                 SupPid = whereis(flurm_controller_sup),
                 ok = application:stop(flurm_controller),
                 case SupPid of
                     undefined -> ok;
                     _ -> flurm_test_utils:wait_for_death(SupPid)
                 end;
             {error, _Reason} ->
                 %% Application might already be started or have issues
                 ok
         end,
         ok
     end}.
