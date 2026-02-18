%%%-------------------------------------------------------------------
%%% @doc FLURM DB Ra Starter 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_db_ra_starter module covering:
%%% - Gen_server callbacks
%%% - Cluster initialization logic
%%% - Find existing cluster
%%% - Init or join cluster
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_starter_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(flurm_db_cluster),
    catch meck:unload(net_adm),
    catch meck:unload(lager),
    catch meck:unload(ra),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"start_link starts gen_server",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),
             %% Mock to return normal for non-distributed mode
             meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
             meck:expect(flurm_db_cluster, start_cluster, fun(_) -> ok end),

             Config = #{},
             {ok, Pid} = flurm_db_ra_starter:start_link(Config),
             ?assert(is_pid(Pid)),
             %% Wait for init_cluster message to be processed
             timer:sleep(100),
             %% Process should have stopped (transient)
             ?assertNot(is_process_alive(Pid))
         end}
     ]}.

%%====================================================================
%% Gen_server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init returns ok with state",
         fun() ->
             Config = #{test => value},
             {ok, State} = flurm_db_ra_starter:init(Config),
             ?assertEqual(Config, element(2, State))
         end},
        {"handle_call unknown request returns error",
         fun() ->
             State = {state, #{}},
             {reply, Result, NewState} = flurm_db_ra_starter:handle_call(unknown_request, {self(), make_ref()}, State),
             ?assertEqual({error, unknown_request}, Result),
             ?assertEqual(State, NewState)
         end},
        {"handle_cast returns noreply",
         fun() ->
             State = {state, #{}},
             {noreply, NewState} = flurm_db_ra_starter:handle_cast(any_message, State),
             ?assertEqual(State, NewState)
         end},
        {"handle_info unknown message returns noreply",
         fun() ->
             State = {state, #{}},
             {noreply, NewState} = flurm_db_ra_starter:handle_info(unknown_info, State),
             ?assertEqual(State, NewState)
         end},
        {"terminate returns ok",
         fun() ->
             State = {state, #{}},
             Result = flurm_db_ra_starter:terminate(normal, State),
             ?assertEqual(ok, Result)
         end},
        {"code_change returns ok with state",
         fun() ->
             State = {state, #{}},
             {ok, NewState} = flurm_db_ra_starter:code_change("1.0", State, []),
             ?assertEqual(State, NewState)
         end}
     ]}.

%%====================================================================
%% Init Or Join Cluster Tests
%%====================================================================

init_or_join_cluster_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init_or_join_cluster with empty list starts single node",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(flurm_db_cluster, start_cluster, fun([Node]) when Node =:= node() -> ok end),

             Result = flurm_db_ra_starter:init_or_join_cluster([]),
             ?assertEqual(ok, Result)
         end},
        {"init_or_join_cluster with only current node",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(flurm_db_cluster, start_cluster, fun([Node]) when Node =:= node() -> ok end),

             Result = flurm_db_ra_starter:init_or_join_cluster([node()]),
             ?assertEqual(ok, Result)
         end},
        {"init_or_join_cluster joins existing cluster",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(ra, [passthrough]),

             OtherNode = 'other@host',
             meck:expect(net_adm, ping, fun(Node) when Node =:= OtherNode -> pong end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_cluster, join_cluster, fun(Node) when Node =:= OtherNode -> ok end),

             Result = flurm_db_ra_starter:init_or_join_cluster([node(), OtherNode]),
             ?assertEqual(ok, Result)
         end},
        {"init_or_join_cluster starts new cluster when no existing found",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(ra, [passthrough]),

             OtherNode = 'other@host',
             meck:expect(net_adm, ping, fun(Node) when Node =:= OtherNode -> pang end),
             meck:expect(flurm_db_cluster, start_cluster, fun(Nodes) ->
                 ?assertEqual([node(), OtherNode], Nodes),
                 ok
             end),

             Result = flurm_db_ra_starter:init_or_join_cluster([node(), OtherNode]),
             ?assertEqual(ok, Result)
         end},
        {"init_or_join_cluster when not in cluster list joins first available",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(ra, [passthrough]),

             OtherNode = 'other@host',
             meck:expect(net_adm, ping, fun(Node) when Node =:= OtherNode -> pong end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_cluster, join_cluster, fun(Node) when Node =:= OtherNode -> ok end),

             %% Current node is NOT in the list
             Result = flurm_db_ra_starter:init_or_join_cluster([OtherNode]),
             ?assertEqual(ok, Result)
         end},
        {"init_or_join_cluster returns error when no cluster available",
         fun() ->
             meck:new(net_adm, [passthrough, unstick]),

             OtherNode = 'other@host',
             meck:expect(net_adm, ping, fun(_) -> pang end),

             %% Current node is NOT in the list and no nodes are reachable
             Result = flurm_db_ra_starter:init_or_join_cluster([OtherNode]),
             ?assertEqual({error, no_cluster_available}, Result)
         end}
     ]}.

%%====================================================================
%% Find Existing Cluster Tests
%%====================================================================

find_existing_cluster_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"find_existing_cluster with empty list",
         fun() ->
             Result = flurm_db_ra_starter:find_existing_cluster([]),
             ?assertEqual(not_found, Result)
         end},
        {"find_existing_cluster finds reachable node with Ra",
         fun() ->
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(ra, [passthrough]),

             OtherNode = 'other@host',
             meck:expect(net_adm, ping, fun(Node) when Node =:= OtherNode -> pong end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun(_) -> {ok, [], {?RA_CLUSTER_NAME, OtherNode}} end),

             Result = flurm_db_ra_starter:find_existing_cluster([OtherNode]),
             ?assertEqual({ok, OtherNode}, Result)
         end},
        {"find_existing_cluster skips unreachable nodes",
         fun() ->
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(ra, [passthrough]),

             Node1 = 'node1@host',
             Node2 = 'node2@host',
             meck:expect(net_adm, ping, fun(Node) ->
                 case Node of
                     Node1 -> pang;
                     Node2 -> pong
                 end
             end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun({_, Node}) when Node =:= Node2 -> {ok, [], node()} end),

             Result = flurm_db_ra_starter:find_existing_cluster([Node1, Node2]),
             ?assertEqual({ok, Node2}, Result)
         end},
        {"find_existing_cluster skips nodes without Ra",
         fun() ->
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(ra, [passthrough]),

             Node1 = 'node1@host',
             Node2 = 'node2@host',
             meck:expect(net_adm, ping, fun(_) -> pong end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun({_, Node}) ->
                 case Node of
                     Node1 -> {error, noproc};
                     Node2 -> {ok, [], node()}
                 end
             end),

             Result = flurm_db_ra_starter:find_existing_cluster([Node1, Node2]),
             ?assertEqual({ok, Node2}, Result)
         end},
        {"find_existing_cluster returns not_found when all fail",
         fun() ->
             meck:new(net_adm, [passthrough, unstick]),
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(ra, [passthrough]),

             Node1 = 'node1@host',
             Node2 = 'node2@host',
             meck:expect(net_adm, ping, fun(_) -> pong end),
             meck:expect(flurm_db_cluster, server_id, fun(Node) -> {?RA_CLUSTER_NAME, Node} end),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_ra_starter:find_existing_cluster([Node1, Node2]),
             ?assertEqual(not_found, Result)
         end}
     ]}.

%%====================================================================
%% Handle Info Init Cluster Tests
%%====================================================================

handle_info_init_cluster_test_() ->
    {foreach,
     fun() ->
         meck_setup(),
         meck:new(lager, [passthrough]),
         meck:expect(lager, info, fun(_, _) -> ok end),
         meck:expect(lager, warning, fun(_, _) -> ok end),
         ok
     end,
     fun meck_cleanup/1,
     [
        {"init_cluster with join_node config",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
             meck:expect(flurm_db_cluster, join_cluster, fun('join@node') -> ok end),

             State = {state, #{join_node => 'join@node'}},
             {stop, normal, _NewState} = flurm_db_ra_starter:handle_info(init_cluster, State)
         end},
        {"init_cluster join fails gracefully",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
             meck:expect(flurm_db_cluster, join_cluster, fun(_) -> {error, connection_refused} end),

             State = {state, #{join_node => 'join@node'}},
             {stop, normal, _NewState} = flurm_db_ra_starter:handle_info(init_cluster, State)
         end},
        {"init_cluster ra init fails gracefully",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:expect(flurm_db_cluster, init_ra, fun() -> {error, ra_start_failed} end),

             State = {state, #{}},
             {stop, normal, _NewState} = flurm_db_ra_starter:handle_info(init_cluster, State)
         end},
        {"init_cluster with cluster_nodes config",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(net_adm, [passthrough, unstick]),
             meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
             meck:expect(flurm_db_cluster, start_cluster, fun(_) -> ok end),
             meck:expect(net_adm, ping, fun(_) -> pang end),

             Nodes = [node(), 'other@node'],
             State = {state, #{cluster_nodes => Nodes}},
             {stop, normal, _NewState} = flurm_db_ra_starter:handle_info(init_cluster, State)
         end},
        {"init_cluster cluster init fails",
         fun() ->
             meck:new(flurm_db_cluster, [passthrough]),
             meck:new(net_adm, [passthrough, unstick]),
             meck:expect(flurm_db_cluster, init_ra, fun() -> ok end),
             meck:expect(flurm_db_cluster, start_cluster, fun(_) -> {error, cluster_start_failed} end),
             meck:expect(net_adm, ping, fun(_) -> pang end),

             State = {state, #{cluster_nodes => [node()]}},
             %% Should stop normally even on failure (graceful degradation)
             {stop, normal, _NewState} = flurm_db_ra_starter:handle_info(init_cluster, State)
         end}
     ]}.
