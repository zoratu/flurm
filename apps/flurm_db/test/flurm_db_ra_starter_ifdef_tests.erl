%%%-------------------------------------------------------------------
%%% @doc Tests for internal functions exported via -ifdef(TEST)
%%% in flurm_db_ra_starter module.
%%%
%%% Note: These functions interact with external cluster functions and
%%% network operations. Tests here use meck to mock these dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_starter_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% init_or_join_cluster/1 Tests
%%====================================================================

init_or_join_cluster_test_() ->
    {"init_or_join_cluster/1 handles various node configurations",
     {foreach,
      fun() ->
          meck:new(flurm_db_cluster, [passthrough]),
          meck:new(net_adm, [unstick, passthrough]),
          meck:new(ra, [unstick, passthrough])
      end,
      fun(_) ->
          meck:unload(flurm_db_cluster),
          meck:unload(net_adm),
          meck:unload(ra)
      end,
      [
       {"empty node list starts single-node cluster",
        fun() ->
            meck:expect(flurm_db_cluster, start_cluster, fun([N]) when N =:= node() -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster([]))
        end},

       {"single-node list with current node starts cluster",
        fun() ->
            CurrentNode = node(),
            meck:expect(flurm_db_cluster, start_cluster, fun([N]) when N =:= CurrentNode -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster([CurrentNode]))
        end},

       {"returns error when start_cluster fails",
        fun() ->
            meck:expect(flurm_db_cluster, start_cluster, fun(_) -> {error, ra_not_started} end),
            ?assertEqual({error, ra_not_started}, flurm_db_ra_starter:init_or_join_cluster([]))
        end},

       {"multi-node list starts new cluster when no existing cluster found",
        fun() ->
            CurrentNode = node(),
            OtherNode = 'other@host',
            Nodes = [CurrentNode, OtherNode],
            %% Mock net_adm:ping to return pang (node not reachable)
            meck:expect(net_adm, ping, fun(_) -> pang end),
            meck:expect(flurm_db_cluster, start_cluster, fun(N) when N =:= Nodes -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster(Nodes))
        end},

       {"multi-node list joins existing cluster when found",
        fun() ->
            CurrentNode = node(),
            OtherNode = 'other@host',
            Nodes = [CurrentNode, OtherNode],
            %% Mock net_adm:ping to return pong for other node
            meck:expect(net_adm, ping, fun(N) when N =:= OtherNode -> pong; (_) -> pang end),
            %% Mock server_id to return a valid server id
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            %% Mock ra:members to return success
            meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, OtherNode}], OtherNode} end),
            %% Mock join_cluster
            meck:expect(flurm_db_cluster, join_cluster, fun(N) when N =:= OtherNode -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster(Nodes))
        end},

       {"node not in list joins first available cluster node",
        fun() ->
            OtherNode = 'other@host',
            Nodes = [OtherNode],  % Current node not in list
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, OtherNode}], OtherNode} end),
            meck:expect(flurm_db_cluster, join_cluster, fun(N) when N =:= OtherNode -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster(Nodes))
        end},

       {"node not in list returns error when no cluster available",
        fun() ->
            OtherNode = 'other@host',
            Nodes = [OtherNode],  % Current node not in list
            meck:expect(net_adm, ping, fun(_) -> pang end),
            ?assertEqual({error, no_cluster_available}, flurm_db_ra_starter:init_or_join_cluster(Nodes))
        end}
      ]}}.

%%====================================================================
%% find_existing_cluster/1 Tests
%%====================================================================

find_existing_cluster_test_() ->
    {"find_existing_cluster/1 searches for existing clusters",
     {foreach,
      fun() ->
          meck:new(flurm_db_cluster, [passthrough]),
          meck:new(net_adm, [unstick, passthrough]),
          meck:new(ra, [unstick, passthrough])
      end,
      fun(_) ->
          meck:unload(flurm_db_cluster),
          meck:unload(net_adm),
          meck:unload(ra)
      end,
      [
       {"returns not_found for empty list",
        fun() ->
            ?assertEqual(not_found, flurm_db_ra_starter:find_existing_cluster([]))
        end},

       {"returns not_found when all nodes unreachable",
        fun() ->
            Nodes = ['node1@host', 'node2@host', 'node3@host'],
            meck:expect(net_adm, ping, fun(_) -> pang end),
            ?assertEqual(not_found, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"returns not_found when reachable nodes have no ra server",
        fun() ->
            Nodes = ['node1@host', 'node2@host'],
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {error, noproc} end),
            ?assertEqual(not_found, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"returns first node with existing cluster",
        fun() ->
            Node1 = 'node1@host',
            Node2 = 'node2@host',
            Nodes = [Node1, Node2],
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun({_, N}) when N =:= Node1 ->
                {ok, [{flurm_db_ra, Node1}], Node1};
                                        (_) -> {error, noproc}
                                     end),
            ?assertEqual({ok, Node1}, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"skips unreachable nodes and finds reachable cluster",
        fun() ->
            Node1 = 'node1@host',  % unreachable
            Node2 = 'node2@host',  % has cluster
            Nodes = [Node1, Node2],
            meck:expect(net_adm, ping, fun(N) when N =:= Node1 -> pang;
                                          (_) -> pong
                                       end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, Node2}], Node2} end),
            ?assertEqual({ok, Node2}, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"handles ra:members returning error tuple",
        fun() ->
            Node1 = 'node1@host',
            Nodes = [Node1],
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {error, timeout} end),
            ?assertEqual(not_found, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"handles ra:members throwing exception",
        fun() ->
            Node1 = 'node1@host',
            Nodes = [Node1],
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> throw(badarg) end),
            ?assertEqual(not_found, flurm_db_ra_starter:find_existing_cluster(Nodes))
        end},

       {"searches nodes in order",
        fun() ->
            Node1 = 'node1@host',
            Node2 = 'node2@host',
            Node3 = 'node3@host',
            Nodes = [Node1, Node2, Node3],
            CheckOrder = [],
            %% Use process dictionary to track call order
            meck:expect(net_adm, ping, fun(N) ->
                put(ping_order, get(ping_order) ++ [N]),
                case N of
                    Node3 -> pong;
                    _ -> pang
                end
            end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, Node3}], Node3} end),
            put(ping_order, []),
            Result = flurm_db_ra_starter:find_existing_cluster(Nodes),
            PingOrder = get(ping_order),
            ?assertEqual({ok, Node3}, Result),
            ?assertEqual([Node1, Node2, Node3], PingOrder)
        end}
      ]}}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {"edge cases for cluster functions",
     {foreach,
      fun() ->
          meck:new(flurm_db_cluster, [passthrough]),
          meck:new(net_adm, [unstick, passthrough]),
          meck:new(ra, [unstick, passthrough])
      end,
      fun(_) ->
          meck:unload(flurm_db_cluster),
          meck:unload(net_adm),
          meck:unload(ra)
      end,
      [
       {"handles single remote node that is current node",
        fun() ->
            CurrentNode = node(),
            meck:expect(flurm_db_cluster, start_cluster, fun([N]) when N =:= CurrentNode -> ok end),
            ?assertEqual(ok, flurm_db_ra_starter:init_or_join_cluster([CurrentNode]))
        end},

       {"handles join_cluster error",
        fun() ->
            OtherNode = 'other@host',
            Nodes = [OtherNode],
            meck:expect(net_adm, ping, fun(_) -> pong end),
            meck:expect(flurm_db_cluster, server_id, fun(N) -> {flurm_db_ra, N} end),
            meck:expect(ra, members, fun(_) -> {ok, [], OtherNode} end),
            meck:expect(flurm_db_cluster, join_cluster, fun(_) -> {error, already_member} end),
            ?assertEqual({error, already_member}, flurm_db_ra_starter:init_or_join_cluster(Nodes))
        end}
      ]}}.
