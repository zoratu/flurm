%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_app module
%%%
%%% These tests call flurm_db_app functions directly to get code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_app_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

app_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Start application", fun start_test/0},
      {"Stop application - cluster running", fun stop_cluster_running_test/0},
      {"Stop application - cluster not running", fun stop_cluster_not_running_test/0}
     ]}.

build_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Build config - no env", fun build_config_no_env_test/0},
      {"Build config - with cluster_nodes", fun build_config_cluster_nodes_test/0},
      {"Build config - with join_node", fun build_config_join_node_test/0},
      {"Build config - with both", fun build_config_both_test/0},
      {"Build config - invalid cluster_nodes", fun build_config_invalid_cluster_nodes_test/0},
      {"Build config - invalid join_node", fun build_config_invalid_join_node_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    meck:new(flurm_db_sup, [passthrough]),
    meck:new(flurm_db_cluster, [passthrough]),
    meck:new(lager, [passthrough]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    %% Clear any existing app env
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),
    ok.

cleanup(_) ->
    meck:unload(flurm_db_sup),
    meck:unload(flurm_db_cluster),
    meck:unload(lager),
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),
    ok.

%%====================================================================
%% Application Callback Tests
%%====================================================================

start_test() ->
    meck:expect(flurm_db_sup, start_link, fun(_) -> {ok, self()} end),

    {ok, Pid} = flurm_db_app:start(normal, []),
    ?assert(is_pid(Pid)),
    ?assert(meck:called(flurm_db_sup, start_link, '_')).

stop_cluster_running_test() ->
    meck:expect(flurm_db_cluster, status, fun() -> {ok, #{state => leader}} end),
    meck:expect(flurm_db_cluster, leave_cluster, fun() -> ok end),

    ok = flurm_db_app:stop(state),
    ?assert(meck:called(flurm_db_cluster, leave_cluster, [])).

stop_cluster_not_running_test() ->
    meck:expect(flurm_db_cluster, status, fun() -> {error, not_started} end),

    ok = flurm_db_app:stop(state),
    ?assertNot(meck:called(flurm_db_cluster, leave_cluster, [])).

%%====================================================================
%% Build Config Tests
%%====================================================================

build_config_no_env_test() ->
    %% Clear env
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        %% Config should be empty map
        ?assertEqual(#{}, Config),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).

build_config_cluster_nodes_test() ->
    application:set_env(flurm_db, cluster_nodes, [node(), 'other@node']),
    application:unset_env(flurm_db, join_node),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        ?assertEqual([node(), 'other@node'], maps:get(cluster_nodes, Config)),
        ?assertNot(maps:is_key(join_node, Config)),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).

build_config_join_node_test() ->
    application:unset_env(flurm_db, cluster_nodes),
    application:set_env(flurm_db, join_node, 'other@node'),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        ?assertEqual('other@node', maps:get(join_node, Config)),
        ?assertNot(maps:is_key(cluster_nodes, Config)),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).

build_config_both_test() ->
    application:set_env(flurm_db, cluster_nodes, [node()]),
    application:set_env(flurm_db, join_node, 'other@node'),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        ?assertEqual([node()], maps:get(cluster_nodes, Config)),
        ?assertEqual('other@node', maps:get(join_node, Config)),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).

build_config_invalid_cluster_nodes_test() ->
    %% Test with invalid cluster_nodes (not a list)
    application:set_env(flurm_db, cluster_nodes, not_a_list),
    application:unset_env(flurm_db, join_node),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        ?assertNot(maps:is_key(cluster_nodes, Config)),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).

build_config_invalid_join_node_test() ->
    %% Test with invalid join_node (not an atom)
    application:unset_env(flurm_db, cluster_nodes),
    application:set_env(flurm_db, join_node, "not_an_atom"),

    meck:expect(flurm_db_sup, start_link, fun(Config) ->
        ?assertNot(maps:is_key(join_node, Config)),
        {ok, self()}
    end),

    {ok, _} = flurm_db_app:start(normal, []).
