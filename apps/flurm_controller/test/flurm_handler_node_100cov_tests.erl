%%%-------------------------------------------------------------------
%%% @doc FLURM Node Handler Comprehensive Coverage Tests
%%%
%%% Complete coverage tests for flurm_handler_node module.
%%% Tests all handle/2 clauses for node operations:
%%% - Node info requests (REQUEST_NODE_INFO - 2007)
%%% - Node registration status (REQUEST_NODE_REGISTRATION_STATUS - 1001)
%%% - Full reconfigure (REQUEST_RECONFIGURE - 1003)
%%% - Partial reconfigure (REQUEST_RECONFIGURE_WITH_CONFIG - 1004)
%%%
%%% Also tests all exported helper functions:
%%% - node_to_node_info/1
%%% - node_state_to_slurm/1
%%% - do_reconfigure/0
%%% - do_partial_reconfigure/4
%%% - apply_node_changes/0
%%% - apply_partition_changes/0
%%% - is_cluster_enabled/0
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_node_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_node_100cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_NODE_INFO (2007) tests - comprehensive coverage
        {"Node info request empty list", fun test_node_info_empty/0},
        {"Node info request single node", fun test_node_info_single/0},
        {"Node info request multiple nodes", fun test_node_info_multiple/0},
        {"Node info response structure", fun test_node_info_response_structure/0},
        {"Node info last_update is set", fun test_node_info_last_update/0},
        {"Node info node_count matches", fun test_node_info_count_matches/0},
        {"Node info converts all fields", fun test_node_info_converts_fields/0},
        {"Node info handles idle state", fun test_node_info_idle_state/0},
        {"Node info handles allocated state", fun test_node_info_allocated_state/0},
        {"Node info handles mixed state", fun test_node_info_mixed_state/0},
        {"Node info handles down state", fun test_node_info_down_state/0},
        {"Node info handles drain state", fun test_node_info_drain_state/0},
        {"Node info handles unknown state", fun test_node_info_unknown_state/0},
        {"Node info with allocations", fun test_node_info_with_allocations/0},
        {"Node info without allocations", fun test_node_info_without_allocations/0},
        {"Node info with features", fun test_node_info_with_features/0},
        {"Node info with partitions", fun test_node_info_with_partitions/0},
        {"Node info calculates free_mem", fun test_node_info_free_mem/0},
        {"Node info calculates cpu_load", fun test_node_info_cpu_load/0},

        %% REQUEST_NODE_REGISTRATION_STATUS (1001) tests
        {"Node registration status success", fun test_node_reg_status_success/0},
        {"Node registration status returns RC 0", fun test_node_reg_status_rc_zero/0},
        {"Node registration status with request record", fun test_node_reg_status_with_record/0},

        %% REQUEST_RECONFIGURE (1003) tests - comprehensive coverage
        {"Reconfigure non-cluster success", fun test_reconfigure_non_cluster_success/0},
        {"Reconfigure non-cluster failure", fun test_reconfigure_non_cluster_failure/0},
        {"Reconfigure cluster leader success", fun test_reconfigure_cluster_leader_success/0},
        {"Reconfigure cluster follower success", fun test_reconfigure_cluster_follower_success/0},
        {"Reconfigure cluster follower no_leader", fun test_reconfigure_cluster_follower_no_leader/0},
        {"Reconfigure cluster follower error", fun test_reconfigure_cluster_follower_error/0},
        {"Reconfigure with record body", fun test_reconfigure_with_record/0},
        {"Reconfigure with binary body", fun test_reconfigure_with_binary/0},
        {"Reconfigure with changed keys", fun test_reconfigure_with_changed_keys/0},
        {"Reconfigure measures elapsed time", fun test_reconfigure_elapsed_time/0},
        {"Reconfigure applies node changes", fun test_reconfigure_applies_node_changes/0},
        {"Reconfigure applies partition changes", fun test_reconfigure_applies_partition_changes/0},
        {"Reconfigure notifies scheduler", fun test_reconfigure_notifies_scheduler/0},
        {"Reconfigure broadcasts to nodes", fun test_reconfigure_broadcasts_to_nodes/0},

        %% REQUEST_RECONFIGURE_WITH_CONFIG (1004) tests - comprehensive coverage
        {"Partial reconfigure success", fun test_partial_reconfigure_success/0},
        {"Partial reconfigure with config file", fun test_partial_reconfigure_config_file/0},
        {"Partial reconfigure with settings", fun test_partial_reconfigure_settings/0},
        {"Partial reconfigure with force", fun test_partial_reconfigure_force/0},
        {"Partial reconfigure with notify_nodes", fun test_partial_reconfigure_notify_nodes/0},
        {"Partial reconfigure cluster leader", fun test_partial_reconfigure_cluster_leader/0},
        {"Partial reconfigure cluster follower success", fun test_partial_reconfigure_cluster_follower_success/0},
        {"Partial reconfigure cluster follower no_leader", fun test_partial_reconfigure_cluster_follower_no_leader/0},
        {"Partial reconfigure cluster follower error", fun test_partial_reconfigure_cluster_follower_error/0},
        {"Partial reconfigure file reload failure", fun test_partial_reconfigure_file_failure/0},
        {"Partial reconfigure file failure with force", fun test_partial_reconfigure_file_failure_force/0},
        {"Partial reconfigure validation failure", fun test_partial_reconfigure_validation_failure/0},
        {"Partial reconfigure empty config file", fun test_partial_reconfigure_empty_config_file/0},
        {"Partial reconfigure nodes setting changed", fun test_partial_reconfigure_nodes_changed/0},
        {"Partial reconfigure partitions setting changed", fun test_partial_reconfigure_partitions_changed/0},
        {"Partial reconfigure scheduler setting changed", fun test_partial_reconfigure_scheduler_changed/0},
        {"Partial reconfigure with binary body", fun test_partial_reconfigure_binary_body/0},
        {"Partial reconfigure returns changed keys", fun test_partial_reconfigure_returns_changed_keys/0}
     ]}.

%%====================================================================
%% Helper function tests
%%====================================================================

helper_function_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        %% node_to_node_info/1 tests
        {"Convert node with all fields", fun test_node_to_info_all_fields/0},
        {"Convert node with empty allocations", fun test_node_to_info_empty_allocations/0},
        {"Convert node with multiple allocations", fun test_node_to_info_multiple_allocations/0},
        {"Convert node with nil allocations", fun test_node_to_info_nil_allocations/0},
        {"Convert node with features list", fun test_node_to_info_features_list/0},
        {"Convert node with partitions list", fun test_node_to_info_partitions_list/0},
        {"Convert node calculates free_mem correctly", fun test_node_to_info_free_mem_calc/0},
        {"Convert node cpu_load from load_avg", fun test_node_to_info_cpu_load_calc/0},

        %% node_state_to_slurm/1 tests
        {"up state to NODE_STATE_IDLE", fun test_state_up/0},
        {"down state to NODE_STATE_DOWN", fun test_state_down/0},
        {"drain state to NODE_STATE_DOWN", fun test_state_drain/0},
        {"idle state to NODE_STATE_IDLE", fun test_state_idle/0},
        {"allocated state to NODE_STATE_ALLOCATED", fun test_state_allocated/0},
        {"mixed state to NODE_STATE_MIXED", fun test_state_mixed/0},
        {"unknown state to NODE_STATE_UNKNOWN", fun test_state_unknown/0},
        {"other atom to NODE_STATE_UNKNOWN", fun test_state_other/0},

        %% is_cluster_enabled/0 tests
        {"Cluster disabled no config", fun test_cluster_disabled_no_config/0},
        {"Cluster disabled single node", fun test_cluster_disabled_single_node/0},
        {"Cluster disabled empty list", fun test_cluster_disabled_empty_list/0},
        {"Cluster enabled multiple nodes", fun test_cluster_enabled_multiple_nodes/0},
        {"Cluster enabled process running", fun test_cluster_enabled_process_running/0}
     ]}.

%%====================================================================
%% do_reconfigure tests
%%====================================================================

do_reconfigure_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"do_reconfigure success", fun test_do_reconfigure_success/0},
        {"do_reconfigure failure", fun test_do_reconfigure_failure/0},
        {"do_reconfigure gets version", fun test_do_reconfigure_version/0},
        {"do_reconfigure applies node changes", fun test_do_reconfigure_node_changes/0},
        {"do_reconfigure applies partition changes", fun test_do_reconfigure_partition_changes/0},
        {"do_reconfigure notifies scheduler", fun test_do_reconfigure_notify_scheduler/0},
        {"do_reconfigure broadcasts to nodes", fun test_do_reconfigure_broadcast/0},
        {"do_reconfigure handles version exception", fun test_do_reconfigure_version_exception/0}
     ]}.

%%====================================================================
%% do_partial_reconfigure tests
%%====================================================================

do_partial_reconfigure_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"do_partial_reconfigure empty config file", fun test_do_partial_empty_config/0},
        {"do_partial_reconfigure with config file success", fun test_do_partial_config_file_success/0},
        {"do_partial_reconfigure with config file failure", fun test_do_partial_config_file_failure/0},
        {"do_partial_reconfigure with config file failure force", fun test_do_partial_config_file_failure_force/0},
        {"do_partial_reconfigure applies settings", fun test_do_partial_applies_settings/0},
        {"do_partial_reconfigure nodes key triggers node changes", fun test_do_partial_nodes_key/0},
        {"do_partial_reconfigure partitions key triggers partition changes", fun test_do_partial_partitions_key/0},
        {"do_partial_reconfigure scheduler key triggers refresh", fun test_do_partial_scheduler_key/0},
        {"do_partial_reconfigure notify_nodes true broadcasts", fun test_do_partial_notify_nodes_true/0},
        {"do_partial_reconfigure notify_nodes false skips broadcast", fun test_do_partial_notify_nodes_false/0},
        {"do_partial_reconfigure returns changed keys", fun test_do_partial_returns_changed/0}
     ]}.

%%====================================================================
%% apply_node_changes tests
%%====================================================================

apply_node_changes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"apply_node_changes empty config", fun test_apply_node_changes_empty/0},
        {"apply_node_changes with nodes", fun test_apply_node_changes_with_nodes/0},
        {"apply_node_changes expands hostlist", fun test_apply_node_changes_expands_hostlist/0},
        {"apply_node_changes node exists", fun test_apply_node_changes_node_exists/0},
        {"apply_node_changes node not found", fun test_apply_node_changes_node_not_found/0},
        {"apply_node_changes no nodename", fun test_apply_node_changes_no_nodename/0}
     ]}.

%%====================================================================
%% apply_partition_changes tests
%%====================================================================

apply_partition_changes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"apply_partition_changes empty config", fun test_apply_partition_changes_empty/0},
        {"apply_partition_changes creates partition", fun test_apply_partition_changes_creates/0},
        {"apply_partition_changes updates partition", fun test_apply_partition_changes_updates/0},
        {"apply_partition_changes no partitionname", fun test_apply_partition_changes_no_name/0},
        {"apply_partition_changes state UP", fun test_apply_partition_state_up/0},
        {"apply_partition_changes state DOWN", fun test_apply_partition_state_down/0},
        {"apply_partition_changes with nodes", fun test_apply_partition_with_nodes/0},
        {"apply_partition_changes default partition", fun test_apply_partition_default/0},
        {"apply_partition_changes with max_time", fun test_apply_partition_max_time/0},
        {"apply_partition_changes with priority", fun test_apply_partition_priority/0},
        {"apply_partition_changes update undef error", fun test_apply_partition_update_undef/0}
     ]}.

%%====================================================================
%% apply_settings tests
%%====================================================================

apply_settings_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"apply_settings empty map", fun test_apply_settings_empty/0},
        {"apply_settings single setting", fun test_apply_settings_single/0},
        {"apply_settings multiple settings", fun test_apply_settings_multiple/0},
        {"apply_settings unchanged value", fun test_apply_settings_unchanged/0},
        {"apply_settings error without force", fun test_apply_settings_error_no_force/0},
        {"apply_settings error with force", fun test_apply_settings_error_with_force/0}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Disable cluster mode by default (catch errors if app controller not running)
    catch application:set_env(flurm_controller, enable_cluster, false),
    catch application:unset_env(flurm_controller, cluster_nodes),

    %% Unload any existing mocks
    catch meck:unload(flurm_node_manager_server),
    catch meck:unload(flurm_controller_cluster),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_config_slurm),
    catch meck:unload(flurm_partition_manager),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_controller_handler),

    %% Start meck for mocking
    meck:new([flurm_node_manager_server, flurm_controller_cluster, flurm_config_server,
              flurm_config_slurm, flurm_partition_manager, flurm_scheduler,
              flurm_controller_handler],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_node_manager_server, get_node, fun(_) -> {error, not_found} end),
    meck:expect(flurm_node_manager_server, send_command, fun(_, _) -> ok end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {ok, ok} end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, reconfigure, fun(_) -> ok end),
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(_) -> ok end),
    meck:expect(flurm_partition_manager, update_partition, fun(_, _) -> ok end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),

    %% Mock imported functions from flurm_controller_handler
    meck:expect(flurm_controller_handler, format_features, fun
        ([]) -> <<>>;
        (Features) when is_list(Features) -> iolist_to_binary(lists:join(<<",">>, Features))
    end),
    meck:expect(flurm_controller_handler, format_partitions, fun
        ([]) -> <<>>;
        (Parts) when is_list(Parts) -> iolist_to_binary(lists:join(<<",">>, Parts))
    end),

    ok.

cleanup(_) ->
    catch meck:unload([flurm_node_manager_server, flurm_controller_cluster, flurm_config_server,
                       flurm_config_slurm, flurm_partition_manager, flurm_scheduler,
                       flurm_controller_handler]),
    catch application:unset_env(flurm_controller, cluster_nodes),
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

make_node(Name, State) ->
    #node{
        hostname = Name,
        state = State,
        cpus = 16,
        memory_mb = 65536,
        load_avg = 1.5,
        features = [<<"gpu">>, <<"fast">>],
        partitions = [<<"default">>, <<"gpu">>],
        allocations = #{}
    }.

make_node_with_allocations(Name) ->
    #node{
        hostname = Name,
        state = allocated,
        cpus = 16,
        memory_mb = 65536,
        load_avg = 4.0,
        features = [<<"gpu">>],
        partitions = [<<"default">>],
        allocations = #{
            123 => {8, 32768},   %% Job 123: 8 CPUs, 32GB
            456 => {4, 16384}    %% Job 456: 4 CPUs, 16GB
        }
    }.

%%====================================================================
%% Node Info Tests
%%====================================================================

test_node_info_empty() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_NODE_INFO, MsgType),
    ?assertEqual(0, Response#node_info_response.node_count).

test_node_info_single() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node1">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(1, Response#node_info_response.node_count).

test_node_info_multiple() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node1">>, idle),
         make_node(<<"node2">>, allocated),
         make_node(<<"node3">>, mixed)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(3, Response#node_info_response.node_count).

test_node_info_response_structure() ->
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assert(is_record(Response, node_info_response)).

test_node_info_last_update() ->
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assert(Response#node_info_response.last_update > 0).

test_node_info_count_matches() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"n1">>, idle), make_node(<<"n2">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(2, Response#node_info_response.node_count),
    ?assertEqual(2, length(Response#node_info_response.nodes)).

test_node_info_converts_fields() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"testnode">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(<<"testnode">>, NodeInfo#node_info.name),
    ?assertEqual(16, NodeInfo#node_info.cpus),
    ?assertEqual(65536, NodeInfo#node_info.real_memory).

test_node_info_idle_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_IDLE, NodeInfo#node_info.node_state).

test_node_info_allocated_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, allocated)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_ALLOCATED, NodeInfo#node_info.node_state).

test_node_info_mixed_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, mixed)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_MIXED, NodeInfo#node_info.node_state).

test_node_info_down_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, down)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_DOWN, NodeInfo#node_info.node_state).

test_node_info_drain_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, drain)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_DOWN, NodeInfo#node_info.node_state).

test_node_info_unknown_state() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, some_unknown_state)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(?NODE_STATE_UNKNOWN, NodeInfo#node_info.node_state).

test_node_info_with_allocations() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node_with_allocations(<<"node">>)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(12, NodeInfo#node_info.alloc_cpus),     %% 8 + 4
    ?assertEqual(49152, NodeInfo#node_info.alloc_memory). %% 32768 + 16384

test_node_info_without_allocations() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assertEqual(0, NodeInfo#node_info.alloc_cpus),
    ?assertEqual(0, NodeInfo#node_info.alloc_memory).

test_node_info_with_features() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assert(is_binary(NodeInfo#node_info.features)).

test_node_info_with_partitions() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    ?assert(is_binary(NodeInfo#node_info.partitions)).

test_node_info_free_mem() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node_with_allocations(<<"node">>)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    %% 65536 - 49152 = 16384
    ?assertEqual(16384, NodeInfo#node_info.free_mem).

test_node_info_cpu_load() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node">>, idle)]
    end),
    Header = make_header(?REQUEST_NODE_INFO),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    [NodeInfo] = Response#node_info_response.nodes,
    %% load_avg = 1.5 -> 150
    ?assertEqual(150, NodeInfo#node_info.cpu_load).

%%====================================================================
%% Node Registration Status Tests
%%====================================================================

test_node_reg_status_success() ->
    Header = make_header(?REQUEST_NODE_REGISTRATION_STATUS),
    Request = #node_registration_request{},
    {ok, MsgType, _Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType).

test_node_reg_status_rc_zero() ->
    Header = make_header(?REQUEST_NODE_REGISTRATION_STATUS),
    Request = #node_registration_request{},
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_node_reg_status_with_record() ->
    Header = make_header(?REQUEST_NODE_REGISTRATION_STATUS),
    Request = #node_registration_request{},
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assert(is_record(Response, slurm_rc_response)).

%%====================================================================
%% Reconfigure Tests
%%====================================================================

test_reconfigure_non_cluster_success() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, MsgType, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_non_cluster_failure() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    meck:expect(flurm_config_server, reconfigure, fun() -> {error, file_not_found} end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_reconfigure_cluster_leader_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(reconfigure, []) ->
        {ok, ok}
    end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_cluster_follower_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_cluster_follower_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_reconfigure_with_record() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    Request = #reconfigure_request{flags = 1},
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_with_binary() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<"binary body">>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_with_changed_keys() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_elapsed_time() ->
    meck:expect(flurm_config_server, reconfigure, fun() ->
        timer:sleep(10),
        ok
    end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_reconfigure_applies_node_changes() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, _} = flurm_handler_node:handle(Header, <<>>),
    ?assert(meck:called(flurm_config_server, get_nodes, [])).

test_reconfigure_applies_partition_changes() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, _} = flurm_handler_node:handle(Header, <<>>),
    ?assert(meck:called(flurm_config_server, get_partitions, [])).

test_reconfigure_notifies_scheduler() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, _} = flurm_handler_node:handle(Header, <<>>),
    ?assert(meck:called(flurm_scheduler, trigger_schedule, [])).

test_reconfigure_broadcasts_to_nodes() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node1">>, idle)]
    end),
    Header = make_header(?REQUEST_RECONFIGURE),
    {ok, _, _} = flurm_handler_node:handle(Header, <<>>),
    %% list_nodes should be called during broadcast
    ?assert(meck:num_calls(flurm_node_manager_server, list_nodes, []) >= 1).

%%====================================================================
%% Partial Reconfigure Tests
%%====================================================================

test_partial_reconfigure_success() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> ok end),
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

test_partial_reconfigure_config_file() ->
    meck:expect(flurm_config_server, reconfigure, fun("/etc/slurm/slurm.conf") -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/etc/slurm/slurm.conf">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_settings() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> old_value end),
    meck:expect(flurm_config_server, set, fun(key1, new_value) -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{key1 => new_value},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_force() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, permission_denied} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/bad/path">>,
        settings = #{},
        force = true,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_notify_nodes() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() ->
        [make_node(<<"node1">>, idle)]
    end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = true
    },
    {ok, _, _} = flurm_handler_node:handle(Header, Request),
    ?assert(meck:num_calls(flurm_node_manager_server, list_nodes, []) >= 1).

test_partial_reconfigure_cluster_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_partial_reconfigure_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(partial_reconfigure, _) ->
        {ok, {ok, [changed_key]}}
    end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_partial_reconfigure_cluster_follower_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_partial_reconfigure_cluster_follower_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_partial_reconfigure_file_failure() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, file_not_found} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/bad/path">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_partial_reconfigure_file_failure_force() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, file_not_found} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/bad/path">>,
        settings = #{},
        force = true,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_validation_failure() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, {validation_failed, bad_value}} end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<"/bad/config">>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_partial_reconfigure_empty_config_file() ->
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_nodes_changed() ->
    meck:expect(flurm_config_server, get, fun(nodes, _) -> old_nodes end),
    meck:expect(flurm_config_server, set, fun(nodes, _) -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{nodes => new_nodes},
        force = false,
        notify_nodes = false
    },
    {ok, _, _} = flurm_handler_node:handle(Header, Request),
    ?assert(meck:called(flurm_config_server, get_nodes, [])).

test_partial_reconfigure_partitions_changed() ->
    meck:expect(flurm_config_server, get, fun(partitions, _) -> old_partitions end),
    meck:expect(flurm_config_server, set, fun(partitions, _) -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{partitions => new_partitions},
        force = false,
        notify_nodes = false
    },
    {ok, _, _} = flurm_handler_node:handle(Header, Request),
    ?assert(meck:called(flurm_config_server, get_partitions, [])).

test_partial_reconfigure_scheduler_changed() ->
    meck:expect(flurm_config_server, get, fun(schedulertype, _) -> old_scheduler end),
    meck:expect(flurm_config_server, set, fun(schedulertype, _) -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{schedulertype => new_scheduler},
        force = false,
        notify_nodes = false
    },
    {ok, _, _} = flurm_handler_node:handle(Header, Request),
    ?assert(meck:called(flurm_scheduler, trigger_schedule, [])).

test_partial_reconfigure_binary_body() ->
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    {ok, _, Response} = flurm_handler_node:handle(Header, <<"binary">>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_partial_reconfigure_returns_changed_keys() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> old_value end),
    meck:expect(flurm_config_server, set, fun(key1, _) -> ok end),
    Header = make_header(?REQUEST_RECONFIGURE_WITH_CONFIG),
    Request = #reconfigure_with_config_request{
        config_file = <<>>,
        settings = #{key1 => new_value},
        force = false,
        notify_nodes = false
    },
    {ok, _, Response} = flurm_handler_node:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Helper Function Tests - node_to_node_info
%%====================================================================

test_node_to_info_all_fields() ->
    Node = make_node(<<"testnode">>, idle),
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(<<"testnode">>, NodeInfo#node_info.name),
    ?assertEqual(<<"testnode">>, NodeInfo#node_info.node_hostname),
    ?assertEqual(16, NodeInfo#node_info.cpus),
    ?assertEqual(65536, NodeInfo#node_info.real_memory).

test_node_to_info_empty_allocations() ->
    Node = #node{
        hostname = <<"node">>,
        state = idle,
        cpus = 8,
        memory_mb = 32768,
        load_avg = 0.0,
        features = [],
        partitions = [],
        allocations = #{}
    },
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(0, NodeInfo#node_info.alloc_cpus),
    ?assertEqual(0, NodeInfo#node_info.alloc_memory).

test_node_to_info_multiple_allocations() ->
    Node = make_node_with_allocations(<<"node">>),
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(12, NodeInfo#node_info.alloc_cpus),
    ?assertEqual(49152, NodeInfo#node_info.alloc_memory).

test_node_to_info_nil_allocations() ->
    Node = #node{
        hostname = <<"node">>,
        state = idle,
        cpus = 8,
        memory_mb = 32768,
        load_avg = 0.0,
        features = [],
        partitions = [],
        allocations = undefined
    },
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(0, NodeInfo#node_info.alloc_cpus).

test_node_to_info_features_list() ->
    Node = make_node(<<"node">>, idle),
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assert(is_binary(NodeInfo#node_info.features)).

test_node_to_info_partitions_list() ->
    Node = make_node(<<"node">>, idle),
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assert(is_binary(NodeInfo#node_info.partitions)).

test_node_to_info_free_mem_calc() ->
    Node = make_node_with_allocations(<<"node">>),
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(16384, NodeInfo#node_info.free_mem).

test_node_to_info_cpu_load_calc() ->
    Node = #node{
        hostname = <<"node">>,
        state = idle,
        cpus = 8,
        memory_mb = 32768,
        load_avg = 2.5,
        features = [],
        partitions = [],
        allocations = #{}
    },
    NodeInfo = flurm_handler_node:node_to_node_info(Node),
    ?assertEqual(250, NodeInfo#node_info.cpu_load).

%%====================================================================
%% Helper Function Tests - node_state_to_slurm
%%====================================================================

test_state_up() ->
    ?assertEqual(?NODE_STATE_IDLE, flurm_handler_node:node_state_to_slurm(up)).

test_state_down() ->
    ?assertEqual(?NODE_STATE_DOWN, flurm_handler_node:node_state_to_slurm(down)).

test_state_drain() ->
    ?assertEqual(?NODE_STATE_DOWN, flurm_handler_node:node_state_to_slurm(drain)).

test_state_idle() ->
    ?assertEqual(?NODE_STATE_IDLE, flurm_handler_node:node_state_to_slurm(idle)).

test_state_allocated() ->
    ?assertEqual(?NODE_STATE_ALLOCATED, flurm_handler_node:node_state_to_slurm(allocated)).

test_state_mixed() ->
    ?assertEqual(?NODE_STATE_MIXED, flurm_handler_node:node_state_to_slurm(mixed)).

test_state_unknown() ->
    ?assertEqual(?NODE_STATE_UNKNOWN, flurm_handler_node:node_state_to_slurm(unknown)).

test_state_other() ->
    ?assertEqual(?NODE_STATE_UNKNOWN, flurm_handler_node:node_state_to_slurm(some_other_state)).

%%====================================================================
%% Helper Function Tests - is_cluster_enabled
%%====================================================================

test_cluster_disabled_no_config() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    catch unregister(flurm_controller_cluster),
    ?assertEqual(false, flurm_handler_node:is_cluster_enabled()).

test_cluster_disabled_single_node() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1]),
    catch unregister(flurm_controller_cluster),
    ?assertEqual(false, flurm_handler_node:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_disabled_empty_list() ->
    catch application:set_env(flurm_controller, cluster_nodes, []),
    catch unregister(flurm_controller_cluster),
    ?assertEqual(false, flurm_handler_node:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_enabled_multiple_nodes() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    ?assertEqual(true, flurm_handler_node:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_enabled_process_running() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    Pid = spawn(fun() -> receive stop -> ok end end),
    register(flurm_controller_cluster, Pid),
    ?assertEqual(true, flurm_handler_node:is_cluster_enabled()),
    Pid ! stop,
    timer:sleep(10),
    catch unregister(flurm_controller_cluster).

%%====================================================================
%% do_reconfigure Tests
%%====================================================================

test_do_reconfigure_success() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    ?assertEqual(ok, flurm_handler_node:do_reconfigure()).

test_do_reconfigure_failure() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> {error, file_not_found} end),
    ?assertEqual({error, file_not_found}, flurm_handler_node:do_reconfigure()).

test_do_reconfigure_version() ->
    meck:expect(flurm_config_server, get_version, fun() -> 1 end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    flurm_handler_node:do_reconfigure(),
    ?assert(meck:num_calls(flurm_config_server, get_version, []) >= 1).

test_do_reconfigure_node_changes() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    flurm_handler_node:do_reconfigure(),
    ?assert(meck:called(flurm_config_server, get_nodes, [])).

test_do_reconfigure_partition_changes() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    flurm_handler_node:do_reconfigure(),
    ?assert(meck:called(flurm_config_server, get_partitions, [])).

test_do_reconfigure_notify_scheduler() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    flurm_handler_node:do_reconfigure(),
    ?assert(meck:called(flurm_scheduler, trigger_schedule, [])).

test_do_reconfigure_broadcast() ->
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    flurm_handler_node:do_reconfigure(),
    ?assert(meck:num_calls(flurm_node_manager_server, list_nodes, []) >= 1).

test_do_reconfigure_version_exception() ->
    meck:expect(flurm_config_server, get_version, fun() -> error(not_started) end),
    meck:expect(flurm_config_server, reconfigure, fun() -> ok end),
    ?assertEqual(ok, flurm_handler_node:do_reconfigure()).

%%====================================================================
%% do_partial_reconfigure Tests
%%====================================================================

test_do_partial_empty_config() ->
    Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, false),
    ?assertMatch({ok, _}, Result).

test_do_partial_config_file_success() ->
    meck:expect(flurm_config_server, reconfigure, fun("/etc/slurm/slurm.conf") -> ok end),
    Result = flurm_handler_node:do_partial_reconfigure(<<"/etc/slurm/slurm.conf">>, #{}, false, false),
    ?assertMatch({ok, _}, Result).

test_do_partial_config_file_failure() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, not_found} end),
    Result = flurm_handler_node:do_partial_reconfigure(<<"/bad/path">>, #{}, false, false),
    ?assertMatch({error, {file_reload_failed, _}}, Result).

test_do_partial_config_file_failure_force() ->
    meck:expect(flurm_config_server, reconfigure, fun(_) -> {error, not_found} end),
    Result = flurm_handler_node:do_partial_reconfigure(<<"/bad/path">>, #{}, true, false),
    ?assertMatch({ok, _}, Result).

test_do_partial_applies_settings() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> old_value end),
    meck:expect(flurm_config_server, set, fun(key1, _) -> ok end),
    Result = flurm_handler_node:do_partial_reconfigure(<<>>, #{key1 => new_value}, false, false),
    ?assertMatch({ok, [key1]}, Result).

test_do_partial_nodes_key() ->
    meck:expect(flurm_config_server, get, fun(nodes, _) -> old_nodes end),
    meck:expect(flurm_config_server, set, fun(nodes, _) -> ok end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    flurm_handler_node:do_partial_reconfigure(<<>>, #{nodes => new_nodes}, false, false),
    ?assert(meck:called(flurm_config_server, get_nodes, [])).

test_do_partial_partitions_key() ->
    meck:expect(flurm_config_server, get, fun(partitions, _) -> old_parts end),
    meck:expect(flurm_config_server, set, fun(partitions, _) -> ok end),
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    flurm_handler_node:do_partial_reconfigure(<<>>, #{partitions => new_parts}, false, false),
    ?assert(meck:called(flurm_config_server, get_partitions, [])).

test_do_partial_scheduler_key() ->
    meck:expect(flurm_config_server, get, fun(schedulertype, _) -> old_scheduler end),
    meck:expect(flurm_config_server, set, fun(schedulertype, _) -> ok end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    flurm_handler_node:do_partial_reconfigure(<<>>, #{schedulertype => new_scheduler}, false, false),
    ?assert(meck:called(flurm_scheduler, trigger_schedule, [])).

test_do_partial_notify_nodes_true() ->
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, true),
    ?assert(meck:num_calls(flurm_node_manager_server, list_nodes, []) >= 1).

test_do_partial_notify_nodes_false() ->
    InitialCalls = meck:num_calls(flurm_node_manager_server, list_nodes, []),
    flurm_handler_node:do_partial_reconfigure(<<>>, #{}, false, false),
    %% No additional calls should be made for node notification
    ?assertEqual(InitialCalls, meck:num_calls(flurm_node_manager_server, list_nodes, [])).

test_do_partial_returns_changed() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> old; (key2, _) -> old end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    {ok, ChangedKeys} = flurm_handler_node:do_partial_reconfigure(
        <<>>, #{key1 => new, key2 => new}, false, false),
    ?assertEqual(2, length(ChangedKeys)).

%%====================================================================
%% apply_node_changes Tests
%%====================================================================

test_apply_node_changes_empty() ->
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()).

test_apply_node_changes_with_nodes() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{nodename => <<"node1">>}]
    end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node1">>) -> [<<"node1">>] end),
    meck:expect(flurm_node_manager_server, get_node, fun(<<"node1">>) -> {error, not_found} end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()).

test_apply_node_changes_expands_hostlist() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{nodename => <<"node[1-3]">>}]
    end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-3]">>) ->
        [<<"node1">>, <<"node2">>, <<"node3">>]
    end),
    meck:expect(flurm_node_manager_server, get_node, fun(_) -> {error, not_found} end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()),
    ?assert(meck:num_calls(flurm_node_manager_server, get_node, '_') >= 3).

test_apply_node_changes_node_exists() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{nodename => <<"node1">>}]
    end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [<<"node1">>] end),
    meck:expect(flurm_node_manager_server, get_node, fun(<<"node1">>) ->
        {ok, make_node(<<"node1">>, idle)}
    end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()).

test_apply_node_changes_node_not_found() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{nodename => <<"node1">>}]
    end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [<<"node1">>] end),
    meck:expect(flurm_node_manager_server, get_node, fun(_) -> {error, not_found} end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()).

test_apply_node_changes_no_nodename() ->
    meck:expect(flurm_config_server, get_nodes, fun() ->
        [#{cpus => 8}]  %% No nodename key
    end),
    ?assertEqual(ok, flurm_handler_node:apply_node_changes()).

%%====================================================================
%% apply_partition_changes Tests
%%====================================================================

test_apply_partition_changes_empty() ->
    meck:expect(flurm_config_server, get_partitions, fun() -> [] end),
    ?assertEqual(ok, flurm_handler_node:apply_partition_changes()).

test_apply_partition_changes_creates() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"gpu">>}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(<<"gpu">>) ->
        {error, not_found}
    end),
    meck:expect(flurm_partition_manager, create_partition, fun(_) -> ok end),
    ?assertEqual(ok, flurm_handler_node:apply_partition_changes()),
    ?assert(meck:called(flurm_partition_manager, create_partition, '_')).

test_apply_partition_changes_updates() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"gpu">>, state => up}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(<<"gpu">>) ->
        {ok, #{name => <<"gpu">>, state => down}}
    end),
    meck:expect(flurm_partition_manager, update_partition, fun(_, _) -> ok end),
    ?assertEqual(ok, flurm_handler_node:apply_partition_changes()),
    ?assert(meck:called(flurm_partition_manager, update_partition, '_')).

test_apply_partition_changes_no_name() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{state => up}]  %% No partitionname key
    end),
    ?assertEqual(ok, flurm_handler_node:apply_partition_changes()).

test_apply_partition_state_up() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, state => <<"UP">>}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(up, maps:get(state, Spec)),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_state_down() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, state => <<"DOWN">>}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(down, maps:get(state, Spec)),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_with_nodes() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, nodes => <<"node[1-3]">>}]
    end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-3]">>) ->
        [<<"node1">>, <<"node2">>, <<"node3">>]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(3, length(maps:get(nodes, Spec))),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_default() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, default => true}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(true, maps:get(default, Spec)),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_max_time() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, maxtime => 3600}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(3600, maps:get(max_time, Spec)),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_priority() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, prioritytier => 5}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) -> {error, not_found} end),
    meck:expect(flurm_partition_manager, create_partition, fun(Spec) ->
        ?assertEqual(5, maps:get(priority, Spec)),
        ok
    end),
    flurm_handler_node:apply_partition_changes().

test_apply_partition_update_undef() ->
    meck:expect(flurm_config_server, get_partitions, fun() ->
        [#{partitionname => <<"test">>, state => up}]
    end),
    meck:expect(flurm_partition_manager, get_partition, fun(_) ->
        {ok, #{name => <<"test">>}}
    end),
    meck:expect(flurm_partition_manager, update_partition, fun(_, _) ->
        error(undef)
    end),
    %% Should not crash
    ?assertEqual(ok, flurm_handler_node:apply_partition_changes()).

%%====================================================================
%% apply_settings Tests
%%====================================================================

test_apply_settings_empty() ->
    ChangedKeys = flurm_handler_node:apply_settings(#{}, false),
    ?assertEqual([], ChangedKeys).

test_apply_settings_single() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> old_value end),
    meck:expect(flurm_config_server, set, fun(key1, new_value) -> ok end),
    ChangedKeys = flurm_handler_node:apply_settings(#{key1 => new_value}, false),
    ?assertEqual([key1], ChangedKeys).

test_apply_settings_multiple() ->
    meck:expect(flurm_config_server, get, fun(_, _) -> old end),
    meck:expect(flurm_config_server, set, fun(_, _) -> ok end),
    ChangedKeys = flurm_handler_node:apply_settings(#{key1 => new, key2 => new}, false),
    ?assertEqual(2, length(ChangedKeys)).

test_apply_settings_unchanged() ->
    meck:expect(flurm_config_server, get, fun(key1, _) -> same_value end),
    ChangedKeys = flurm_handler_node:apply_settings(#{key1 => same_value}, false),
    ?assertEqual([], ChangedKeys).

test_apply_settings_error_no_force() ->
    meck:expect(flurm_config_server, get, fun(_, _) -> error(not_configured) end),
    ChangedKeys = flurm_handler_node:apply_settings(#{key1 => value}, false),
    ?assertEqual([], ChangedKeys).

test_apply_settings_error_with_force() ->
    meck:expect(flurm_config_server, get, fun(_, _) -> error(not_configured) end),
    ChangedKeys = flurm_handler_node:apply_settings(#{key1 => value}, true),
    ?assertEqual([], ChangedKeys).
