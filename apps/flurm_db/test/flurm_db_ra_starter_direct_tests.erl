%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_ra_starter module
%%%
%%% These tests call flurm_db_ra_starter gen_server callbacks directly
%%% to get code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_starter_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Init returns state with config", fun init_test/0},
      {"Handle call returns error for unknown request", fun handle_call_test/0},
      {"Handle cast returns noreply", fun handle_cast_test/0},
      {"Handle info for unknown message", fun handle_info_unknown_test/0},
      {"Terminate returns ok", fun terminate_test/0},
      {"Code change returns ok", fun code_change_test/0}
     ]}.

init_cluster_nonode_test_() ->
    {setup,
     fun setup_nonode/0,
     fun cleanup_nonode/1,
     [
      {"Init cluster in non-distributed mode", fun init_cluster_nonode_test/0}
     ]}.

init_cluster_distributed_test_() ->
    {setup,
     fun setup_distributed/0,
     fun cleanup_distributed/1,
     [
      {"Init cluster with join_node", fun init_cluster_join_node_test/0},
      {"Init cluster without join_node - single node", fun init_cluster_single_node_test/0},
      {"Init cluster without join_node - multi node", fun init_cluster_multi_node_test/0},
      {"Init cluster - Ra init fails", fun init_cluster_ra_init_fails_test/0},
      {"Init cluster - join fails", fun init_cluster_join_fails_test/0},
      {"Init cluster - start fails", fun init_cluster_start_fails_test/0},
      {"Init cluster - node not in list", fun init_cluster_node_not_in_list_test/0},
      {"Find cluster - reachable with Ra", fun find_cluster_reachable_with_ra_test/0}
     ]}.

find_existing_cluster_test_() ->
    {setup,
     fun setup_distributed/0,
     fun cleanup_distributed/1,
     [
      {"Find existing cluster - empty list", fun find_cluster_empty_test/0},
      {"Find existing cluster - node unreachable", fun find_cluster_unreachable_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    meck:new(lager, [passthrough]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(lager),
    ok.

setup_nonode() ->
    setup(),
    ok.

cleanup_nonode(_) ->
    cleanup(ok).

setup_distributed() ->
    setup(),
    meck:new(flurm_db_cluster, [passthrough]),
    meck:new(net_adm, [passthrough]),
    meck:new(ra, [passthrough]),
    ok.

cleanup_distributed(_) ->
    meck:unload(flurm_db_cluster),
    meck:unload(net_adm),
    meck:unload(ra),
    cleanup(ok).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

init_test() ->
    Config = #{cluster_nodes => [node()]},
    {ok, State} = flurm_db_ra_starter:init(Config),

    %% State should contain config
    ?assertMatch({state, #{cluster_nodes := [_]}}, State).

handle_call_test() ->
    State = {state, #{}},
    {reply, {error, unknown_request}, State} =
        flurm_db_ra_starter:handle_call(unknown_request, {self(), ref}, State).

handle_cast_test() ->
    State = {state, #{}},
    {noreply, State} = flurm_db_ra_starter:handle_cast(some_msg, State).

handle_info_unknown_test() ->
    State = {state, #{}},
    {noreply, State} = flurm_db_ra_starter:handle_info(unknown_msg, State).

terminate_test() ->
    State = {state, #{}},
    ok = flurm_db_ra_starter:terminate(normal, State).

code_change_test() ->
    State = {state, #{}},
    {ok, State} = flurm_db_ra_starter:code_change(old_vsn, State, extra).

%%====================================================================
%% Init Cluster Tests (Non-distributed mode)
%%====================================================================

init_cluster_nonode_test() ->
    %% When node() is nonode@nohost, should stop normally
    case node() of
        nonode@nohost ->
            State = {state, #{cluster_nodes => [node()]}},
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State);
        _ ->
            %% Skip this test on named nodes
            ok
    end.

%%====================================================================
%% Init Cluster Tests (Distributed mode)
%%====================================================================

init_cluster_join_node_test() ->
    %% Test joining an existing cluster
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, join_cluster, fun(_) -> ok end),

    State = {state, #{join_node => 'other@node'}},

    case node() of
        nonode@nohost ->
            %% Non-distributed mode - will take different path
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_single_node_test() ->
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, start_cluster, fun([_]) -> ok end),

    State = {state, #{cluster_nodes => [node()]}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_multi_node_test() ->
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, server_id, fun(_) -> {flurm_db_ra, node()} end),
    meck:expect(net_adm, ping, fun(_) -> pang end),  %% Other nodes not reachable
    meck:expect(flurm_db_cluster, start_cluster, fun(_) -> ok end),

    State = {state, #{cluster_nodes => [node(), 'other@node']}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_ra_init_fails_test() ->
    meck:expect(flurm_db_cluster, init_ra, fun() -> {error, failed} end),

    State = {state, #{cluster_nodes => [node()]}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            %% Should stop normally (graceful degradation to ETS)
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

%%====================================================================
%% Find Existing Cluster Tests
%%====================================================================

find_cluster_empty_test() ->
    %% Empty list should return not_found
    %% We test this by triggering init_or_join_cluster with empty list
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, start_cluster, fun([_]) -> ok end),

    State = {state, #{}},  %% No cluster_nodes specified

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

find_cluster_unreachable_test() ->
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(net_adm, ping, fun(_) -> pang end),
    meck:expect(flurm_db_cluster, start_cluster, fun(_) -> ok end),

    State = {state, #{cluster_nodes => [node(), 'unreachable@node']}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_join_fails_test() ->
    %% Test when joining an existing cluster fails
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, join_cluster, fun(_) -> {error, connection_refused} end),

    State = {state, #{join_node => 'other@node'}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            %% Should fall back to ETS and stop normally
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_start_fails_test() ->
    %% Test when starting cluster fails
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(flurm_db_cluster, start_cluster, fun(_) -> {error, timeout} end),

    State = {state, #{cluster_nodes => [node()]}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            %% Should fall back to ETS and stop normally
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

init_cluster_node_not_in_list_test() ->
    %% Test when local node is not in cluster_nodes list
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(net_adm, ping, fun(_) -> pang end),  %% Can't reach other nodes

    State = {state, #{cluster_nodes => ['other1@node', 'other2@node']}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            %% Should fail since we can't find any cluster
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.

find_cluster_reachable_with_ra_test() ->
    %% Test when we can reach a node with Ra running
    meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
    meck:expect(net_adm, ping, fun('other@node') -> pong end),
    meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
    meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, 'other@node'}], {flurm_db_ra, 'other@node'}} end),
    meck:expect(flurm_db_cluster, join_cluster, fun(_) -> ok end),

    State = {state, #{cluster_nodes => [node(), 'other@node']}},

    case node() of
        nonode@nohost ->
            ok;
        _ ->
            {stop, normal, State} =
                flurm_db_ra_starter:handle_info(init_cluster, State)
    end.
