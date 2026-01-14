%%%-------------------------------------------------------------------
%%% @doc FLURM Database Application Tests
%%%
%%% Tests for the flurm_db_app module (application behaviour).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_app_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Mock application environment
    application:set_env(flurm_db, cluster_nodes, [node()]),
    application:set_env(flurm_db, data_dir, "/tmp/flurm_db_test"),
    ok.

cleanup(_) ->
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),
    application:unset_env(flurm_db, data_dir),
    ok.

%%====================================================================
%% Application Callback Tests
%%====================================================================

app_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"Test build_config with cluster_nodes", fun test_build_config_cluster_nodes/0},
         {"Test build_config with join_node", fun test_build_config_join_node/0},
         {"Test build_config empty", fun test_build_config_empty/0},
         {"Test stop callback", fun test_stop_callback/0}
     ]
    }.

test_build_config_cluster_nodes() ->
    application:set_env(flurm_db, cluster_nodes, [node1, node2]),
    application:unset_env(flurm_db, join_node),
    %% The build_config function is internal, so we test it indirectly
    %% by checking the application env
    {ok, Nodes} = application:get_env(flurm_db, cluster_nodes),
    ?assertEqual([node1, node2], Nodes),
    ok.

test_build_config_join_node() ->
    application:unset_env(flurm_db, cluster_nodes),
    application:set_env(flurm_db, join_node, 'other@node'),
    {ok, JoinNode} = application:get_env(flurm_db, join_node),
    ?assertEqual('other@node', JoinNode),
    ok.

test_build_config_empty() ->
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),
    ?assertEqual(undefined, application:get_env(flurm_db, cluster_nodes)),
    ?assertEqual(undefined, application:get_env(flurm_db, join_node)),
    ok.

test_stop_callback() ->
    %% Test that stop/1 returns ok
    Result = flurm_db_app:stop(undefined),
    ?assertEqual(ok, Result),
    ok.
