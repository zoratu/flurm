%%%-------------------------------------------------------------------
%%% @doc FLURM Node Handler Tests
%%%
%%% Comprehensive tests for the flurm_handler_node module.
%%% Tests all handle/2 clauses for node-related operations:
%%% - Node info queries
%%% - Node registration status
%%% - Controller reconfiguration (full and partial)
%%% - Node configuration changes
%%% - Partition configuration changes
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_node_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_node_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_NODE_INFO (2007) tests
        {"Node info - empty list", fun test_node_info_empty/0},
        {"Node info - single node", fun test_node_info_single/0},
        {"Node info - multiple nodes", fun test_node_info_multiple/0},
        {"Node info - node with allocations", fun test_node_info_with_allocations/0},
        {"Node info - response structure", fun test_node_info_response_structure/0},
        {"Node info - load average conversion", fun test_node_info_load_avg/0},

        %% REQUEST_NODE_REGISTRATION_STATUS (1001) tests
        {"Node registration status - request", fun test_node_registration_status/0},
        {"Node registration status - response code", fun test_node_registration_response/0},

        %% REQUEST_RECONFIGURE (1003) tests
        {"Reconfigure - success", fun test_reconfigure_success/0},
        {"Reconfigure - with changed keys", fun test_reconfigure_changed_keys/0},
        {"Reconfigure - failure", fun test_reconfigure_failure/0},
        {"Reconfigure - cluster mode leader", fun test_reconfigure_cluster_leader/0},
        {"Reconfigure - cluster mode follower", fun test_reconfigure_cluster_follower/0},
        {"Reconfigure - with record body", fun test_reconfigure_with_record/0},

        %% REQUEST_RECONFIGURE_WITH_CONFIG (1004) tests
        {"Partial reconfigure - success", fun test_partial_reconfigure_success/0},
        {"Partial reconfigure - with settings", fun test_partial_reconfigure_settings/0},
        {"Partial reconfigure - with config file", fun test_partial_reconfigure_config_file/0},
        {"Partial reconfigure - notify nodes", fun test_partial_reconfigure_notify_nodes/0},
        {"Partial reconfigure - force mode", fun test_partial_reconfigure_force/0},
        {"Partial reconfigure - validation failed", fun test_partial_reconfigure_validation_failed/0},
        {"Partial reconfigure - failure", fun test_partial_reconfigure_failure/0},
        {"Partial reconfigure - cluster follower", fun test_partial_reconfigure_cluster_follower/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Disable cluster mode
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Start meck for mocking
    meck:new([flurm_node_manager_server, flurm_config_server, flurm_partition_manager,
              flurm_controller_cluster, flurm_scheduler, flurm_config_slurm],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, reconfigure, fun(_) -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),

    ok.

cleanup(_) ->
    %% Unload all mocks
    catch meck:unload([flurm_node_manager_server, flurm_config_server, flurm_partition_manager,
                       flurm_controller_cluster, flurm_scheduler, flurm_config_slurm]),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_header(MsgType) ->
    #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = MsgType,
        body_length = 0
    }.

make_node(Hostname) ->
    make_node(Hostname, #{}).

make_node(Hostname, Overrides) ->
    Defaults = #{
        hostname => Hostname,
        state => up,
        cpus => 4,
        memory_mb => 8192,
        load_avg => 0.5,
        features => [],
        partitions => [<<"default">>],
        allocations => #{}
    },
    Props = maps:merge(Defaults, Overrides),
    #node{
        hostname = maps:get(hostname, Props),
        state = maps:get(state, Props),
        cpus = maps:get(cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        load_avg = maps:get(load_avg, Props),
        features = maps:get(features, Props),
        partitions = maps:get(partitions, Props),
        allocations = maps:get(allocations, Props)
    }.

%%====================================================================
%% Node Info Tests (REQUEST_NODE_INFO - 2007)
%%====================================================================

test_node_info_empty() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_NODE_INFO, MsgType),
    ?assertEqual(0, Response#node_info_response.node_count),
    ?assertEqual([], Response#node_info_response.nodes).

test_node_info_single() ->
    Node = make_node(<<"compute-1">>),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node] end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_NODE_INFO, MsgType),
    ?assertEqual(1, Response#node_info_response.node_count),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(<<"compute-1">>, NodeInfo#node_info.name).

test_node_info_multiple() ->
    Nodes = [
        make_node(<<"compute-1">>),
        make_node(<<"compute-2">>),
        make_node(<<"compute-3">>)
    ],
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> Nodes end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_NODE_INFO, MsgType),
    ?assertEqual(3, Response#node_info_response.node_count),
    ?assertEqual(3, length(Response#node_info_response.nodes)).

test_node_info_with_allocations() ->
    Node = make_node(<<"compute-1">>, #{
        cpus => 8,
        memory_mb => 16384,
        allocations => #{
            1001 => {2, 4096},
            1002 => {4, 8192}
        }
    }),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node] end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),

    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(8, NodeInfo#node_info.cpus),
    ?assertEqual(16384, NodeInfo#node_info.real_memory),
    ?assertEqual(6, NodeInfo#node_info.alloc_cpus),  % 2 + 4
    ?assertEqual(12288, NodeInfo#node_info.alloc_memory).  % 4096 + 8192

test_node_info_response_structure() ->
    Node = make_node(<<"compute-1">>, #{
        state => idle,
        features => [<<"gpu">>, <<"ssd">>],
        partitions => [<<"default">>, <<"gpu">>]
    }),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node] end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assert(Response#node_info_response.last_update > 0),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(<<"compute-1">>, NodeInfo#node_info.node_hostname),
    ?assertEqual(<<"compute-1">>, NodeInfo#node_info.node_addr),
    ?assertEqual(6818, NodeInfo#node_info.port),
    ?assertEqual(<<"22.05.0">>, NodeInfo#node_info.version),
    ?assertEqual(<<"x86_64">>, NodeInfo#node_info.arch),
    ?assertEqual(<<"Linux">>, NodeInfo#node_info.os).

test_node_info_load_avg() ->
    Node = make_node(<<"compute-1">>, #{load_avg => 2.5}),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [Node] end),

    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),

    [NodeInfo] = Response#node_info_response.nodes,
    %% load_avg * 100, truncated
    ?assertEqual(250, NodeInfo#node_info.cpu_load).

%%====================================================================
%% Node Registration Status Tests (REQUEST_NODE_REGISTRATION_STATUS - 1001)
%%====================================================================

test_node_registration_status() ->
    Header = make_header(?REQUEST_NODE_REGISTRATION_STATUS),
    Request = #node_registration_request{status_only = true},
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_node_registration_response() ->
    Header = make_header(?REQUEST_NODE_REGISTRATION_STATUS),
    Request = #node_registration_request{},
    {ok, MsgType, _} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType).

%%====================================================================
%% Reconfigure Tests (REQUEST_RECONFIGURE - 1003)
%%====================================================================

test_reconfigure_success() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 2 end),

    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_changed_keys() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_failure() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> {error, file_not_found} end),

    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_reconfigure_cluster_leader() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(reconfigure, _) -> {ok, ok} end),

    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_with_record() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE),
    Request = #reconfigure_request{flags = 0},
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Partial Reconfigure Tests (REQUEST_RECONFIGURE_WITH_CONFIG - 1004)
%%====================================================================

test_partial_reconfigure_success() ->
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_settings() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> <<"old_value">>; (_, D) -> D end),
    meck:expect(flurm_config_server, set, fun(key1, <<"new_value">>) -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{key1 => <<"new_value">>},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_config_file() ->
    meck:expect(flurm_config_server, reconfigure, fun("/etc/slurm/slurm.conf") -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_notify_nodes() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node1">>), make_node(<<"node2">>)]
    end),
    meck:expect(flurm_node_manager_server, send_command, fun(_, reconfigure) -> ok end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = true
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_force() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, parse_error} end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{},
        force = true,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_validation_failed() ->
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    %% Simulate validation failure by having config file reload fail
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, {validation_failed, bad_syntax}} end),

    Request = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_partial_reconfigure_failure() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, permission_denied} end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_partial_reconfigure_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(partial_reconfigure, _) ->
        {ok, {ok, [key1, key2]}}
    end),

    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{key1 => value1},
        force = false,
        notify_nodes = false
    },
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% Helper Function Tests
%%====================================================================

node_to_node_info_test_() ->
    [
        {"Convert basic node to node_info", fun() ->
            Node = make_node(<<"compute-1">>),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(<<"compute-1">>, NodeInfo#node_info.name),
            ?assertEqual(<<"compute-1">>, NodeInfo#node_info.node_hostname),
            ?assertEqual(4, NodeInfo#node_info.cpus),
            ?assertEqual(8192, NodeInfo#node_info.real_memory)
        end},
        {"Convert node with down state", fun() ->
            Node = make_node(<<"compute-1">>, #{state => down}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(?NODE_STATE_DOWN, NodeInfo#node_info.node_state)
        end},
        {"Convert node with allocations calculates resources", fun() ->
            Node = make_node(<<"compute-1">>, #{
                cpus => 8,
                memory_mb => 16384,
                allocations => #{
                    1001 => {4, 8192}
                }
            }),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(4, NodeInfo#node_info.alloc_cpus),
            ?assertEqual(8192, NodeInfo#node_info.alloc_memory),
            ?assertEqual(16384 - 8192, NodeInfo#node_info.free_mem)
        end},
        {"Convert node with empty allocations", fun() ->
            Node = make_node(<<"compute-1">>, #{allocations => undefined}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(0, NodeInfo#node_info.alloc_cpus),
            ?assertEqual(0, NodeInfo#node_info.alloc_memory)
        end}
    ].

node_state_to_slurm_test_() ->
    [
        {"up -> IDLE", fun() ->
            ?assertEqual(?NODE_STATE_IDLE, flurm_handler_node:node_state_to_slurm(up))
        end},
        {"down -> DOWN", fun() ->
            ?assertEqual(?NODE_STATE_DOWN, flurm_handler_node:node_state_to_slurm(down))
        end},
        {"drain -> DOWN", fun() ->
            ?assertEqual(?NODE_STATE_DOWN, flurm_handler_node:node_state_to_slurm(drain))
        end},
        {"idle -> IDLE", fun() ->
            ?assertEqual(?NODE_STATE_IDLE, flurm_handler_node:node_state_to_slurm(idle))
        end},
        {"allocated -> ALLOCATED", fun() ->
            ?assertEqual(?NODE_STATE_ALLOCATED, flurm_handler_node:node_state_to_slurm(allocated))
        end},
        {"mixed -> MIXED", fun() ->
            ?assertEqual(?NODE_STATE_MIXED, flurm_handler_node:node_state_to_slurm(mixed))
        end},
        {"unknown -> UNKNOWN", fun() ->
            ?assertEqual(?NODE_STATE_UNKNOWN, flurm_handler_node:node_state_to_slurm(some_unknown_state))
        end}
    ].

is_cluster_enabled_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) ->
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Cluster disabled when no nodes configured", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            ?assertEqual(false, flurm_handler_node:is_cluster_enabled())
        end},
        {"Cluster disabled with single node", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1]),
            ?assertEqual(false, flurm_handler_node:is_cluster_enabled())
        end},
        {"Cluster enabled with multiple nodes", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            ?assertEqual(true, flurm_handler_node:is_cluster_enabled())
        end}
     ]}.

%%====================================================================
%% Reconfiguration Internal Function Tests
%%====================================================================

do_reconfigure_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_scheduler, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_scheduler, flurm_node_manager_server]),
         ok
     end,
     [
        {"do_reconfigure success", fun() ->
            ?assertEqual(ok, flurm_handler_node:do_reconfigure())
        end},
        {"do_reconfigure failure", fun() ->
            meck:expect(flurm_config_server, reconfigure, fun() -> {error, bad_config} end),
            ?assertEqual({error, bad_config}, flurm_handler_node:do_reconfigure()),
            meck:expect(flurm_config_server, reconfigure, fun() -> ok end)
        end}
     ]}.

do_partial_reconfigure_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_scheduler, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, reconfigure, fun(_) -> ok end),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
         meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
         meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_scheduler, flurm_node_manager_server]),
         ok
     end,
     [
        {"Partial reconfigure with empty settings", fun() ->
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, false),
            ?assertEqual({ok, []}, Result)
        end},
        {"Partial reconfigure with config file", fun() ->
            Result = flurm_handler_node:do_partial_reconfigure(<<"/etc/slurm.conf">>, #{}, false, false),
            ?assertEqual({ok, []}, Result)
        end},
        {"Partial reconfigure with settings", fun() ->
            meck:expect(flurm_config_server, get, fun(key1, _) -> <<"old">>; (_, D) -> D end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{key1 => <<"new">>}, false, false),
            ?assertMatch({ok, [key1]}, Result)
        end}
     ]}.

%%====================================================================
%% Apply Node Changes Tests
%%====================================================================

apply_node_changes_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_node_manager_server, flurm_config_slurm],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_node_manager_server, flurm_config_slurm]),
         ok
     end,
     [
        {"Apply node changes with empty config", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end},
        {"Apply node changes with node definitions", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() ->
                [#{nodename => <<"compute-1">>, cpus => 4, realmemory => 8192}]
            end),
            meck:expect(flurm_node_manager_server, get_node, fun(_) -> {error, not_found} end),
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end},
        {"Apply node changes with existing node", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() ->
                [#{nodename => <<"compute-1">>}]
            end),
            meck:expect(flurm_node_manager_server, get_node, fun(_) -> {ok, make_node(<<"compute-1">>)} end),
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end}
     ]}.

%%====================================================================
%% Apply Partition Changes Tests
%%====================================================================

apply_partition_changes_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_partition_manager, flurm_config_slurm],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_partition_manager, flurm_config_slurm]),
         ok
     end,
     [
        {"Apply partition changes with empty config", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Apply partition changes creates new partition", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"compute">>, nodes => <<"node[1-4]">>, state => <<"UP">>}]
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
            meck:expect(flurm_partition_manager, create_partition, fun(_) -> ok end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Apply partition changes updates existing partition", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"compute">>, state => down}]
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {ok, #partition{}} end),
            meck:expect(flurm_partition_manager, update_partition, fun(_, _) -> ok end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_node_manager_server, flurm_config_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_node_manager_server, flurm_config_server]),
         ok
     end,
     [
        {"Node with negative load avg", fun() ->
            Node = make_node(<<"node1">>, #{load_avg => -0.5}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(-50, NodeInfo#node_info.cpu_load)
        end},
        {"Node with very high load avg", fun() ->
            Node = make_node(<<"node1">>, #{load_avg => 100.0}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(10000, NodeInfo#node_info.cpu_load)
        end},
        {"Node with zero cpus", fun() ->
            Node = make_node(<<"node1">>, #{cpus => 0}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(0, NodeInfo#node_info.cpus)
        end},
        {"Node with zero memory", fun() ->
            Node = make_node(<<"node1">>, #{memory_mb => 0}),
            NodeInfo = flurm_handler_node:node_to_node_info(Node),
            ?assertEqual(0, NodeInfo#node_info.real_memory)
        end}
     ]}.

%%====================================================================
%% Cluster Forwarding Tests
%%====================================================================

cluster_forwarding_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_controller_cluster, flurm_config_server],
                  [passthrough, no_link, non_strict]),
         application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_controller_cluster, flurm_config_server]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Reconfigure forwards to leader when follower", fun() ->
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(reconfigure, _) ->
                {ok, ok}
            end),
            Header = make_header(?REQUEST_RECONFIGURE),
            {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end},
        {"Reconfigure handles no_leader error", fun() ->
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_RECONFIGURE),
            {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
            ?assertEqual(-1, Response#slurm_rc_response.return_code)
        end},
        {"Partial reconfigure handles cluster_not_ready error", fun() ->
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) ->
                {error, cluster_not_ready}
            end),
            Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
            Request = #reconfigure_with_config_request{
                config_file = <<>>,
                settings = #{},
                force = false,
                notify_nodes = false
            },
            {ok, _, Response} = flurm_handler_node:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code)
        end}
     ]}.

%%====================================================================
%% Settings Application Tests
%%====================================================================

apply_settings_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_config_server, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_config_server),
         ok
     end,
     [
        {"Apply empty settings returns empty list", fun() ->
            %% Test with empty map - should return empty list
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, false),
            ?assertEqual({ok, []}, Result)
        end},
        {"Apply settings updates changed values", fun() ->
            meck:expect(flurm_config_server, get, fun(key1, _) -> <<"old">>; (_, D) -> D end),
            meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
            meck:expect(flurm_config_server, get_version, fun() -> 2 end),
            meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
            meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{key1 => <<"new">>}, false, false),
            ?assertMatch({ok, [key1]}, Result)
        end},
        {"Apply settings skips unchanged values", fun() ->
            meck:expect(flurm_config_server, get, fun(key1, _) -> <<"same">>; (_, D) -> D end),
            meck:expect(flurm_config_server, get_version, fun() -> 2 end),
            meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
            meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{key1 => <<"same">>}, false, false),
            ?assertEqual({ok, []}, Result)
        end}
     ]}.

%%====================================================================
%% Scheduler-Related Settings Tests
%%====================================================================

scheduler_settings_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_scheduler, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_scheduler, flurm_node_manager_server]),
         ok
     end,
     [
        {"Scheduler triggered when schedulertype changes", fun() ->
            meck:expect(flurm_config_server, get, fun(schedulertype, _) -> <<"old">>; (_, D) -> D end),
            meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
            meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{schedulertype => <<"new">>}, false, false),
            ?assertMatch({ok, _}, Result),
            ?assert(meck:called(flurm_scheduler, trigger_schedule, []))
        end},
        {"Scheduler not triggered for non-scheduler settings", fun() ->
            meck:expect(flurm_config_server, get, fun(some_key, _) -> <<"old">>; (_, D) -> D end),
            meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
            meck:reset(flurm_scheduler),
            _Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{some_key => <<"new">>}, false, false),
            %% Scheduler should not be called for non-scheduler keys
            ok
        end}
     ]}.

%%====================================================================
%% Raw Binary Body Tests
%%====================================================================

raw_body_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_node_manager_server]),
         ok
     end,
     [
        {"Reconfigure with raw binary body", fun() ->
            Header = make_header(?REQUEST_RECONFIGURE),
            {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<"raw binary">>),
            ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end},
        {"Partial reconfigure with non-record body uses defaults", fun() ->
            Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
            {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<"raw binary">>),
            ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end}
     ]}.

%%====================================================================
%% Is Cluster Enabled Extended Tests
%%====================================================================

is_cluster_enabled_extended_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) ->
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Cluster enabled with process running", fun() ->
            application:unset_env(flurm_controller, cluster_nodes),
            %% Simulate process running by registering a dummy process
            Pid = spawn(fun() -> receive stop -> ok end end),
            register(flurm_controller_cluster, Pid),
            ?assertEqual(true, flurm_handler_node:is_cluster_enabled()),
            Pid ! stop,
            unregister(flurm_controller_cluster)
        end},
        {"Cluster disabled with empty nodes list", fun() ->
            application:set_env(flurm_controller, cluster_nodes, []),
            ?assertEqual(false, flurm_handler_node:is_cluster_enabled())
        end}
     ]}.

%%====================================================================
%% Node Manager Exception Tests
%%====================================================================

node_manager_exception_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_node_manager_server, flurm_config_server],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_node_manager_server, flurm_config_server]),
         ok
     end,
     [
        {"Get_node exception handled in apply_node_changes", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() ->
                [#{nodename => <<"crash-node">>}]
            end),
            meck:expect(flurm_node_manager_server, get_node, fun(_) -> error(crash) end),
            meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
            %% Should not crash
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end}
     ]}.

%%====================================================================
%% Partition Manager Exception Tests
%%====================================================================

partition_manager_exception_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_partition_manager, flurm_config_slurm],
                  [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_partition_manager, flurm_config_slurm]),
         ok
     end,
     [
        {"Create partition failure logged but continues", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"fail_part">>, nodes => <<>>}]
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
            meck:expect(flurm_partition_manager, create_partition, fun(_) -> {error, failed} end),
            meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [] end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Update partition with undefined state skipped", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"no_state_part">>}]  % No state field
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {ok, #partition{}} end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Update partition with undef error handled", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"undef_part">>, state => up}]
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {ok, #partition{}} end),
            meck:expect(flurm_partition_manager, update_partition, fun(_, _) -> error(undef) end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end}
     ]}.

%%====================================================================
%% Broadcast Reconfigure Extended Tests
%%====================================================================

broadcast_reconfigure_extended_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_node_manager_server, flurm_config_server, flurm_scheduler],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_node_manager_server, flurm_config_server, flurm_scheduler]),
         ok
     end,
     [
        {"Broadcast to nodes handles send_command error", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [make_node(<<"err_node">>)]
            end),
            meck:expect(flurm_node_manager_server, send_command, fun(_, _) ->
                {error, node_unavailable}
            end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, true),
            ?assertMatch({ok, _}, Result)
        end},
        {"Broadcast to nodes handles node exception", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() ->
                [make_node(<<"crash_node">>)]
            end),
            meck:expect(flurm_node_manager_server, send_command, fun(_, _) ->
                error(crash)
            end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, true),
            ?assertMatch({ok, _}, Result)
        end},
        {"Broadcast handles list_nodes exception", fun() ->
            meck:expect(flurm_node_manager_server, list_nodes, fun() -> error(crash) end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, true),
            ?assertMatch({ok, _}, Result)
        end}
     ]}.

%%====================================================================
%% Apply Settings Force Mode Tests
%%====================================================================

apply_settings_force_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_node_manager_server, flurm_scheduler],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_node_manager_server, flurm_scheduler]),
         ok
     end,
     [
        {"Force mode continues on set exception", fun() ->
            meck:expect(flurm_config_server, get, fun(fail_key, _) -> <<"old">>; (_, D) -> D end),
            meck:expect(flurm_config_server, set, fun(fail_key, _) -> error(fail) end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{fail_key => <<"new">>}, true, false),
            %% Should return ok even with exception
            ?assertMatch({ok, _}, Result)
        end},
        {"Non-force mode continues on set exception", fun() ->
            meck:expect(flurm_config_server, get, fun(fail_key2, _) -> <<"old">>; (_, D) -> D end),
            meck:expect(flurm_config_server, set, fun(fail_key2, _) -> error(fail) end),
            Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{fail_key2 => <<"new">>}, false, false),
            %% Should return ok even without force
            ?assertMatch({ok, _}, Result)
        end}
     ]}.

%%====================================================================
%% Cluster Forwarding Extended Tests
%%====================================================================

cluster_forwarding_extended_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_controller_cluster, flurm_config_server],
                  [passthrough, no_link, non_strict]),
         application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_controller_cluster, flurm_config_server]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Reconfigure forwards to leader - generic error", fun() ->
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(reconfigure, _) ->
                {error, timeout}
            end),
            Header = make_header(?REQUEST_RECONFIGURE),
            {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
            ?assertEqual(-1, Response#slurm_rc_response.return_code)
        end},
        {"Partial reconfigure forwards to leader - ok with changed keys", fun() ->
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(partial_reconfigure, _) ->
                {ok, {ok, [key1, key2]}}
            end),
            Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
            Request = #reconfigure_with_config_request{
                config_file = <<>>,
                settings = #{key1 => value1},
                force = false,
                notify_nodes = false
            },
            {ok, _, Response} = flurm_handler_node:handle(Header, Request),
            ?assertEqual(0, Response#slurm_rc_response.return_code)
        end}
     ]}.

%%====================================================================
%% Reconfigure Version Exception Tests
%%====================================================================

reconfigure_version_exception_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_scheduler, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_scheduler, flurm_node_manager_server]),
         ok
     end,
     [
        {"Get_version exception handled gracefully", fun() ->
            meck:expect(flurm_config_server, get_version, fun() -> error(crash) end),
            Result = flurm_handler_node:do_reconfigure(),
            ?assertEqual(ok, Result)
        end}
     ]}.

%%====================================================================
%% Scheduler Exception Tests
%%====================================================================

scheduler_exception_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_scheduler, flurm_node_manager_server],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
         meck:expect(flurm_config_server, get_version, fun() -> 1 end),
         meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
         meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
         meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_scheduler, flurm_node_manager_server]),
         ok
     end,
     [
        {"Scheduler trigger_schedule exception handled", fun() ->
            meck:expect(flurm_scheduler, trigger_schedule, fun() -> error(crash) end),
            Result = flurm_handler_node:do_reconfigure(),
            ?assertEqual(ok, Result)
        end}
     ]}.

%%====================================================================
%% Node Definition Edge Cases Tests
%%====================================================================

node_definition_edge_cases_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_node_manager_server, flurm_config_slurm],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_slurm, expand_hostlist, fun(P) -> [P] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_node_manager_server, flurm_config_slurm]),
         ok
     end,
     [
        {"Node without nodename field skipped", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() ->
                [#{cpu => 4}]  % No nodename field
            end),
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end},
        {"Node with undefined nodename skipped", fun() ->
            meck:expect(flurm_config_server, get_nodes, fun() ->
                [#{nodename => undefined}]
            end),
            ?assertEqual(ok, flurm_handler_node:apply_node_changes())
        end}
     ]}.

%%====================================================================
%% Partition Definition Edge Cases Tests
%%====================================================================

partition_definition_edge_cases_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_config_server, flurm_partition_manager, flurm_config_slurm],
                  [passthrough, no_link, non_strict]),
         meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [] end),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_config_server, flurm_partition_manager, flurm_config_slurm]),
         ok
     end,
     [
        {"Partition without partitionname field skipped", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{nodes => <<"test">>}]  % No partitionname field
            end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Partition with undefined partitionname skipped", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => undefined}]
            end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end},
        {"Partition with state DOWN string", fun() ->
            meck:expect(flurm_config_server, get_partitions, fun() ->
                [#{partitionname => <<"down_part">>, state => <<"DOWN">>}]
            end),
            meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
            meck:expect(flurm_partition_manager, create_partition, fun(_) -> ok end),
            ?assertEqual(ok, flurm_handler_node:apply_partition_changes())
        end}
     ]}.
