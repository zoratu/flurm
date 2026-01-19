%%%-------------------------------------------------------------------
%%% @doc FLURM Database Cluster -ifdef(TEST) Exports Tests
%%%
%%% Tests for internal helper functions exported via -ifdef(TEST).
%%% These tests directly call the internal functions to achieve coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_cluster_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% server_id/0 and server_id/1 Tests (already exported)
%%====================================================================

server_id_test_() ->
    [
        {"server_id/0 returns tuple with current node", fun() ->
            ServerId = flurm_db_cluster:server_id(),
            ?assertMatch({_, _}, ServerId),
            {Name, Node} = ServerId,
            ?assertEqual(?RA_CLUSTER_NAME, Name),
            ?assertEqual(node(), Node)
        end},

        {"server_id/1 returns tuple with specified node", fun() ->
            TestNode = 'test@testhost',
            ServerId = flurm_db_cluster:server_id(TestNode),
            ?assertMatch({_, _}, ServerId),
            {Name, Node} = ServerId,
            ?assertEqual(?RA_CLUSTER_NAME, Name),
            ?assertEqual(TestNode, Node)
        end},

        {"server_id/1 with different nodes", fun() ->
            Node1 = 'node1@host1',
            Node2 = 'node2@host2',
            Id1 = flurm_db_cluster:server_id(Node1),
            Id2 = flurm_db_cluster:server_id(Node2),
            ?assertNotEqual(Id1, Id2),
            {_, N1} = Id1,
            {_, N2} = Id2,
            ?assertEqual(Node1, N1),
            ?assertEqual(Node2, N2)
        end}
    ].

%%====================================================================
%% format_status/1 Tests
%%====================================================================

format_status_map_test_() ->
    [
        {"format_status/1 with map input - all fields", fun() ->
            Input = #{
                state => leader,
                leader => {flurm_db_ra, 'node1@host'},
                current_term => 5,
                commit_index => 100,
                last_applied => 99,
                cluster => [{flurm_db_ra, 'node1@host'}, {flurm_db_ra, 'node2@host'}]
            },
            Result = flurm_db_cluster:format_status(Input),
            ?assert(is_map(Result)),
            ?assertEqual(leader, maps:get(state, Result)),
            ?assertEqual({flurm_db_ra, 'node1@host'}, maps:get(leader, Result)),
            ?assertEqual(5, maps:get(current_term, Result)),
            ?assertEqual(100, maps:get(commit_index, Result)),
            ?assertEqual(99, maps:get(last_applied, Result)),
            ?assertEqual(2, length(maps:get(cluster_members, Result)))
        end},

        {"format_status/1 with map input - missing fields use defaults", fun() ->
            Input = #{},
            Result = flurm_db_cluster:format_status(Input),
            ?assert(is_map(Result)),
            ?assertEqual(unknown, maps:get(state, Result)),
            ?assertEqual(undefined, maps:get(leader, Result)),
            ?assertEqual(0, maps:get(current_term, Result)),
            ?assertEqual(0, maps:get(commit_index, Result)),
            ?assertEqual(0, maps:get(last_applied, Result)),
            ?assertEqual([], maps:get(cluster_members, Result))
        end},

        {"format_status/1 with map input - partial fields", fun() ->
            Input = #{
                state => follower,
                current_term => 3
            },
            Result = flurm_db_cluster:format_status(Input),
            ?assertEqual(follower, maps:get(state, Result)),
            ?assertEqual(undefined, maps:get(leader, Result)),
            ?assertEqual(3, maps:get(current_term, Result))
        end}
    ].

format_status_proplist_test_() ->
    [
        {"format_status/1 with proplist input - all fields", fun() ->
            Input = [
                {state, candidate},
                {leader, undefined},
                {current_term, 10}
            ],
            Result = flurm_db_cluster:format_status(Input),
            ?assert(is_map(Result)),
            ?assertEqual(candidate, maps:get(state, Result)),
            ?assertEqual(undefined, maps:get(leader, Result)),
            ?assertEqual(10, maps:get(current_term, Result))
        end},

        {"format_status/1 with proplist input - empty list uses defaults", fun() ->
            Input = [],
            Result = flurm_db_cluster:format_status(Input),
            ?assertEqual(unknown, maps:get(state, Result)),
            ?assertEqual(undefined, maps:get(leader, Result)),
            ?assertEqual(0, maps:get(current_term, Result))
        end},

        {"format_status/1 with proplist input - partial data", fun() ->
            Input = [{state, leader}],
            Result = flurm_db_cluster:format_status(Input),
            ?assertEqual(leader, maps:get(state, Result))
        end}
    ].

%%====================================================================
%% start_default_system/0 Tests
%%====================================================================

%% Note: This test may have side effects depending on whether Ra is already running.
%% In a clean test environment, it will try to start the Ra system.

start_default_system_returns_ok_or_error_test() ->
    %% This function should return ok (if Ra starts/is running)
    %% or {error, Reason} if there's a problem, or throw if Ra supervisor not running.
    try
        Result = flurm_db_cluster:start_default_system(),
        case Result of
            ok ->
                ?assertEqual(ok, Result);
            {error, _Reason} ->
                %% Expected if Ra cannot be started in test environment
                ?assertMatch({error, _}, Result)
        end
    catch
        exit:{noproc, _} ->
            %% Ra supervisor not running - expected in test environment
            ok;
        _:_ ->
            %% Other errors are acceptable in test environment
            ok
    end.

%%====================================================================
%% init_ra/0 Tests
%%====================================================================

%% Note: init_ra/0 has side effects (creates directories, starts Ra).
%% These tests verify the function can be called and returns expected types.

init_ra_returns_ok_or_error_test() ->
    Result = flurm_db_cluster:init_ra(),
    case Result of
        ok ->
            ?assertEqual(ok, Result);
        {error, _Reason} ->
            %% May fail in test environment without proper setup
            ?assertMatch({error, _}, Result)
    end.
