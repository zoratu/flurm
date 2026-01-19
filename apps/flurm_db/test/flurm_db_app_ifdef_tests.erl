%%%-------------------------------------------------------------------
%%% @doc Tests for internal functions exported via -ifdef(TEST)
%%% in flurm_db_app module.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_app_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% build_config/0 Tests
%%====================================================================

build_config_test_() ->
    {"build_config/0 builds configuration from application environment",
     {setup,
      fun() ->
          %% Save original env values
          {application:get_env(flurm_db, cluster_nodes),
           application:get_env(flurm_db, join_node)}
      end,
      fun({OrigClusterNodes, OrigJoinNode}) ->
          %% Restore original env values
          case OrigClusterNodes of
              {ok, Value1} ->
                  application:set_env(flurm_db, cluster_nodes, Value1);
              undefined ->
                  application:unset_env(flurm_db, cluster_nodes)
          end,
          case OrigJoinNode of
              {ok, Value2} ->
                  application:set_env(flurm_db, join_node, Value2);
              undefined ->
                  application:unset_env(flurm_db, join_node)
          end
      end,
      [
       {"returns empty map when no env vars set",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{}, Config)
        end},

       {"includes cluster_nodes when configured",
        fun() ->
            Nodes = ['node1@host', 'node2@host', 'node3@host'],
            application:set_env(flurm_db, cluster_nodes, Nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{cluster_nodes => Nodes}, Config)
        end},

       {"includes join_node when configured",
        fun() ->
            Node = 'master@host',
            application:unset_env(flurm_db, cluster_nodes),
            application:set_env(flurm_db, join_node, Node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{join_node => Node}, Config)
        end},

       {"includes both cluster_nodes and join_node when configured",
        fun() ->
            Nodes = ['node1@host', 'node2@host'],
            JoinNode = 'master@host',
            application:set_env(flurm_db, cluster_nodes, Nodes),
            application:set_env(flurm_db, join_node, JoinNode),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{cluster_nodes => Nodes, join_node => JoinNode}, Config)
        end},

       {"ignores non-list cluster_nodes",
        fun() ->
            application:set_env(flurm_db, cluster_nodes, not_a_list),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{}, Config)
        end},

       {"ignores non-atom join_node",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:set_env(flurm_db, join_node, "not_an_atom"),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{}, Config)
        end},

       {"handles empty cluster_nodes list",
        fun() ->
            application:set_env(flurm_db, cluster_nodes, []),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{cluster_nodes => []}, Config)
        end},

       {"handles single-node cluster_nodes list",
        fun() ->
            Nodes = ['single@host'],
            application:set_env(flurm_db, cluster_nodes, Nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(#{cluster_nodes => Nodes}, Config)
        end},

       {"result is always a map",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assert(is_map(Config))
        end},

       {"result does not contain undefined values",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertNot(maps:is_key(cluster_nodes, Config)),
            ?assertNot(maps:is_key(join_node, Config))
        end}
      ]}}.

%%====================================================================
%% Type Validation Tests
%%====================================================================

build_config_type_validation_test_() ->
    {"build_config/0 type validation",
     {setup,
      fun() ->
          {application:get_env(flurm_db, cluster_nodes),
           application:get_env(flurm_db, join_node)}
      end,
      fun({OrigClusterNodes, OrigJoinNode}) ->
          case OrigClusterNodes of
              {ok, Value1} ->
                  application:set_env(flurm_db, cluster_nodes, Value1);
              undefined ->
                  application:unset_env(flurm_db, cluster_nodes)
          end,
          case OrigJoinNode of
              {ok, Value2} ->
                  application:set_env(flurm_db, join_node, Value2);
              undefined ->
                  application:unset_env(flurm_db, join_node)
          end
      end,
      [
       {"cluster_nodes must be a list",
        fun() ->
            application:set_env(flurm_db, cluster_nodes, {tuple, invalid, list}),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertNot(maps:is_key(cluster_nodes, Config))
        end},

       {"join_node must be an atom",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:set_env(flurm_db, join_node, <<"binary_not_atom">>),
            Config = flurm_db_app:build_config(),
            ?assertNot(maps:is_key(join_node, Config))
        end},

       {"cluster_nodes accepts list of atoms",
        fun() ->
            Nodes = [node1, node2, node3],
            application:set_env(flurm_db, cluster_nodes, Nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(Nodes, maps:get(cluster_nodes, Config))
        end},

       {"join_node accepts any atom",
        fun() ->
            Node = some_random_atom,
            application:unset_env(flurm_db, cluster_nodes),
            application:set_env(flurm_db, join_node, Node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(Node, maps:get(join_node, Config))
        end}
      ]}}.

%%====================================================================
%% Integration-style Tests
%%====================================================================

build_config_integration_test_() ->
    {"build_config/0 integration tests",
     {setup,
      fun() ->
          {application:get_env(flurm_db, cluster_nodes),
           application:get_env(flurm_db, join_node)}
      end,
      fun({OrigClusterNodes, OrigJoinNode}) ->
          case OrigClusterNodes of
              {ok, Value1} ->
                  application:set_env(flurm_db, cluster_nodes, Value1);
              undefined ->
                  application:unset_env(flurm_db, cluster_nodes)
          end,
          case OrigJoinNode of
              {ok, Value2} ->
                  application:set_env(flurm_db, join_node, Value2);
              undefined ->
                  application:unset_env(flurm_db, join_node)
          end
      end,
      [
       {"config is suitable for flurm_db_sup:start_link/1",
        fun() ->
            application:set_env(flurm_db, cluster_nodes, ['n1@h', 'n2@h']),
            application:set_env(flurm_db, join_node, 'master@h'),
            Config = flurm_db_app:build_config(),
            %% Verify config has expected keys
            ?assert(maps:is_key(cluster_nodes, Config)),
            ?assert(maps:is_key(join_node, Config)),
            %% Verify values can be retrieved with maps:get
            ?assertEqual(['n1@h', 'n2@h'], maps:get(cluster_nodes, Config)),
            ?assertEqual('master@h', maps:get(join_node, Config))
        end},

       {"config works with maps:get/3 default",
        fun() ->
            application:unset_env(flurm_db, cluster_nodes),
            application:unset_env(flurm_db, join_node),
            Config = flurm_db_app:build_config(),
            ?assertEqual(undefined, maps:get(cluster_nodes, Config, undefined)),
            ?assertEqual(undefined, maps:get(join_node, Config, undefined))
        end}
      ]}}.
