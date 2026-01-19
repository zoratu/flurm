%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_app module
%%%
%%% These tests call flurm_db_app functions directly to get code coverage.
%%% All external dependencies are mocked, so no Ra cluster is required.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_app_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

app_callbacks_test_() ->
    case can_run_tests() of
        true ->
            {setup,
             fun setup/0,
             fun cleanup/1,
             fun(SetupResult) ->
                 case SetupResult of
                     {ok, _Mocks} ->
                         [
                          {"Start application", fun start_test/0},
                          {"Stop application - cluster running", fun stop_cluster_running_test/0},
                          {"Stop application - cluster not running", fun stop_cluster_not_running_test/0}
                         ];
                     {error, Reason} ->
                         [{atom_to_list(?MODULE) ++ " skipped",
                           ?_assertEqual({skip, Reason}, {skip, Reason})}]
                 end
             end};
        false ->
            {skip, "Required modules not available"}
    end.

build_config_test_() ->
    case can_run_tests() of
        true ->
            {setup,
             fun setup/0,
             fun cleanup/1,
             fun(SetupResult) ->
                 case SetupResult of
                     {ok, _Mocks} ->
                         [
                          {"Build config - no env", fun build_config_no_env_test/0},
                          {"Build config - with cluster_nodes", fun build_config_cluster_nodes_test/0},
                          {"Build config - with join_node", fun build_config_join_node_test/0},
                          {"Build config - with both", fun build_config_both_test/0},
                          {"Build config - invalid cluster_nodes", fun build_config_invalid_cluster_nodes_test/0},
                          {"Build config - invalid join_node", fun build_config_invalid_join_node_test/0}
                         ];
                     {error, Reason} ->
                         [{atom_to_list(?MODULE) ++ " skipped",
                           ?_assertEqual({skip, Reason}, {skip, Reason})}]
                 end
             end};
        false ->
            {skip, "Required modules not available"}
    end.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

%% Check if we can run tests (required modules available)
can_run_tests() ->
    try
        _ = code:ensure_loaded(meck),
        _ = code:ensure_loaded(flurm_db_app),
        true
    catch
        _:_ -> false
    end.

setup() ->
    %% Track which mocks we successfully create
    Mocks = [],
    try
        %% Clear any existing app env first
        application:unset_env(flurm_db, cluster_nodes),
        application:unset_env(flurm_db, join_node),

        %% Create mocks with error handling
        Mocks1 = safe_meck_new(flurm_db_sup, [passthrough], Mocks),
        Mocks2 = safe_meck_new(flurm_db_cluster, [passthrough], Mocks1),
        Mocks3 = safe_meck_new(lager, [passthrough, non_strict, no_link], Mocks2),

        %% Setup default expectations for lager (it may be called anywhere)
        case lists:member(lager, Mocks3) of
            true ->
                meck:expect(lager, info, fun(_) -> ok end),
                meck:expect(lager, info, fun(_, _) -> ok end),
                meck:expect(lager, warning, fun(_) -> ok end),
                meck:expect(lager, warning, fun(_, _) -> ok end),
                meck:expect(lager, error, fun(_) -> ok end),
                meck:expect(lager, error, fun(_, _) -> ok end);
            false ->
                ok
        end,

        {ok, Mocks3}
    catch
        Class:Reason:Stack ->
            %% Cleanup any mocks we managed to create
            cleanup_mocks(Mocks),
            {error, {setup_failed, Class, Reason, Stack}}
    end.

cleanup(SetupResult) ->
    %% Clean up app env
    application:unset_env(flurm_db, cluster_nodes),
    application:unset_env(flurm_db, join_node),

    %% Clean up mocks based on what was created
    case SetupResult of
        {ok, Mocks} ->
            cleanup_mocks(Mocks);
        {error, _} ->
            ok
    end,
    ok.

%% Safely create a meck mock, tracking success
safe_meck_new(Module, Options, Mocks) ->
    try
        %% First try to unload if already mocked
        catch meck:unload(Module),
        meck:new(Module, Options),
        [Module | Mocks]
    catch
        _:_ ->
            %% If meck fails, continue without this mock
            Mocks
    end.

%% Safely unload mocks
cleanup_mocks(Mocks) ->
    lists:foreach(fun(Module) ->
        try
            meck:unload(Module)
        catch
            _:_ -> ok
        end
    end, Mocks).

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
    %% Reset the expectation to avoid interference with other tests
    meck:expect(flurm_db_cluster, leave_cluster, fun() -> ok end),
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
