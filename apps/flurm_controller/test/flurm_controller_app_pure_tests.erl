%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_controller_app
%%%
%%% Tests the pure/exported functions without mocking.
%%% These tests verify the config/0 function which returns application
%%% configuration values.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_app_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Store original values to restore later
    Keys = [listen_port, listen_address, num_acceptors, max_connections,
            cluster_name, cluster_nodes, ra_data_dir],
    OriginalValues = [{K, application:get_env(flurm_controller, K)} || K <- Keys],
    OriginalValues.

cleanup(OriginalValues) ->
    %% Restore original values
    lists:foreach(
        fun({Key, undefined}) ->
                application:unset_env(flurm_controller, Key);
           ({Key, {ok, Value}}) ->
                application:set_env(flurm_controller, Key, Value)
        end, OriginalValues).

%%====================================================================
%% Test Generators
%%====================================================================

config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"config returns default values when not configured",
       fun config_returns_defaults/0},
      {"config returns custom values when configured",
       fun config_returns_custom_values/0},
      {"config returns correct map structure",
       fun config_returns_correct_structure/0}
     ]}.

%%====================================================================
%% Config Tests
%%====================================================================

config_returns_defaults() ->
    %% Clear all config values to get defaults
    Keys = [listen_port, listen_address, num_acceptors, max_connections,
            cluster_name, cluster_nodes, ra_data_dir],
    lists:foreach(fun(K) -> application:unset_env(flurm_controller, K) end, Keys),

    Config = flurm_controller_app:config(),

    ?assertEqual(6817, maps:get(listen_port, Config)),
    ?assertEqual("0.0.0.0", maps:get(listen_address, Config)),
    ?assertEqual(10, maps:get(num_acceptors, Config)),
    ?assertEqual(1000, maps:get(max_connections, Config)),
    ?assertEqual(flurm, maps:get(cluster_name, Config)),
    ?assertEqual("/var/lib/flurm/ra", maps:get(ra_data_dir, Config)).

config_returns_custom_values() ->
    %% Set custom values
    application:set_env(flurm_controller, listen_port, 7000),
    application:set_env(flurm_controller, listen_address, "127.0.0.1"),
    application:set_env(flurm_controller, num_acceptors, 20),
    application:set_env(flurm_controller, max_connections, 5000),
    application:set_env(flurm_controller, cluster_name, my_cluster),
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    application:set_env(flurm_controller, ra_data_dir, "/tmp/ra"),

    Config = flurm_controller_app:config(),

    ?assertEqual(7000, maps:get(listen_port, Config)),
    ?assertEqual("127.0.0.1", maps:get(listen_address, Config)),
    ?assertEqual(20, maps:get(num_acceptors, Config)),
    ?assertEqual(5000, maps:get(max_connections, Config)),
    ?assertEqual(my_cluster, maps:get(cluster_name, Config)),
    ?assertEqual([node1, node2], maps:get(cluster_nodes, Config)),
    ?assertEqual("/tmp/ra", maps:get(ra_data_dir, Config)).

config_returns_correct_structure() ->
    Config = flurm_controller_app:config(),

    ?assert(is_map(Config)),
    ?assertEqual(7, maps:size(Config)),
    ?assert(maps:is_key(listen_port, Config)),
    ?assert(maps:is_key(listen_address, Config)),
    ?assert(maps:is_key(num_acceptors, Config)),
    ?assert(maps:is_key(max_connections, Config)),
    ?assert(maps:is_key(cluster_name, Config)),
    ?assert(maps:is_key(cluster_nodes, Config)),
    ?assert(maps:is_key(ra_data_dir, Config)).

%%====================================================================
%% Cluster Status Tests
%%====================================================================

cluster_status_test_() ->
    [
     {"cluster_status returns not_available when cluster not running",
      fun cluster_status_when_not_running/0}
    ].

cluster_status_when_not_running() ->
    %% Without cluster running, flurm_controller_cluster:cluster_status/0
    %% catches the noproc exit and returns #{status => not_started}
    Status = flurm_controller_app:cluster_status(),
    ?assert(is_map(Status)),
    %% The status will be not_started (from cluster module) or not_available (from catch)
    StatusValue = maps:get(status, Status),
    ?assert(StatusValue =:= not_started orelse StatusValue =:= not_available).

%%====================================================================
%% Stop Callback Test
%%====================================================================

stop_callback_test() ->
    %% stop/1 should return ok
    ?assertEqual(ok, flurm_controller_app:stop(undefined)).
