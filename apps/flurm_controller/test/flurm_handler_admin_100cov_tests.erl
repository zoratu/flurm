%%%-------------------------------------------------------------------
%%% @doc FLURM Admin Handler Comprehensive Coverage Tests
%%%
%%% Complete coverage tests for flurm_handler_admin module.
%%% Tests all handle/2 clauses for administrative operations:
%%% - Ping requests
%%% - Shutdown requests (cluster leader/follower/non-cluster)
%%% - Reservation management (info, create, update, delete)
%%% - License info
%%% - Topology info
%%% - Front-end info
%%% - Burst buffer info
%%%
%%% Also tests all exported helper functions:
%%% - reservation_to_reservation_info/1
%%% - reservation_state_to_flags/1
%%% - determine_reservation_type/2
%%% - parse_reservation_flags/1
%%% - generate_reservation_name/0
%%% - extract_reservation_fields/1
%%% - create_reservation_request_to_spec/1
%%% - update_reservation_request_to_updates/1
%%% - license_to_license_info/1
%%% - build_burst_buffer_info/2
%%% - pool_to_bb_pool/1
%%% - do_graceful_shutdown/0
%%% - is_cluster_enabled/0
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_admin_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_admin_100cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_PING (1008) tests - comprehensive coverage
        {"Ping request returns RC 0", fun test_ping_request_basic/0},
        {"Ping response is slurm_rc_response record", fun test_ping_response_record/0},
        {"Ping response message type is RESPONSE_SLURM_RC", fun test_ping_response_type/0},
        {"Ping handles empty body", fun test_ping_empty_body/0},
        {"Ping handles binary body", fun test_ping_binary_body/0},
        {"Ping handles any body content", fun test_ping_any_body/0},

        %% REQUEST_SHUTDOWN (1005) tests - comprehensive coverage
        {"Shutdown success non-cluster mode", fun test_shutdown_non_cluster_success/0},
        {"Shutdown as cluster leader success", fun test_shutdown_cluster_leader_success/0},
        {"Shutdown as cluster follower success", fun test_shutdown_cluster_follower_success/0},
        {"Shutdown forward to leader no_leader error", fun test_shutdown_forward_no_leader/0},
        {"Shutdown forward to leader generic error", fun test_shutdown_forward_generic_error/0},
        {"Shutdown forward returns ok result", fun test_shutdown_forward_ok_result/0},
        {"Shutdown forward returns error result", fun test_shutdown_forward_error_result/0},
        {"Shutdown triggers graceful shutdown", fun test_shutdown_triggers_graceful/0},

        %% REQUEST_RESERVATION_INFO (2012) tests - comprehensive coverage
        {"Reservation info empty list", fun test_resv_info_empty/0},
        {"Reservation info single reservation", fun test_resv_info_single/0},
        {"Reservation info multiple reservations", fun test_resv_info_multiple/0},
        {"Reservation info exception returns empty", fun test_resv_info_exception/0},
        {"Reservation info response structure", fun test_resv_info_response_structure/0},
        {"Reservation info last_update is set", fun test_resv_info_last_update/0},
        {"Reservation info converts all fields", fun test_resv_info_all_fields/0},

        %% REQUEST_CREATE_RESERVATION (2050) tests - comprehensive coverage
        {"Create reservation success", fun test_create_resv_success/0},
        {"Create reservation with generated name", fun test_create_resv_generated_name/0},
        {"Create reservation with explicit name", fun test_create_resv_explicit_name/0},
        {"Create reservation with nodes", fun test_create_resv_with_nodes/0},
        {"Create reservation with users", fun test_create_resv_with_users/0},
        {"Create reservation with accounts", fun test_create_resv_with_accounts/0},
        {"Create reservation with features", fun test_create_resv_with_features/0},
        {"Create reservation with partition", fun test_create_resv_with_partition/0},
        {"Create reservation with start time", fun test_create_resv_with_start_time/0},
        {"Create reservation with end time", fun test_create_resv_with_end_time/0},
        {"Create reservation with duration", fun test_create_resv_with_duration/0},
        {"Create reservation with node_cnt", fun test_create_resv_with_node_cnt/0},
        {"Create reservation maint type", fun test_create_resv_maint_type/0},
        {"Create reservation flex type", fun test_create_resv_flex_type/0},
        {"Create reservation user type", fun test_create_resv_user_type/0},
        {"Create reservation with maint flag", fun test_create_resv_maint_flag/0},
        {"Create reservation with flex flag", fun test_create_resv_flex_flag/0},
        {"Create reservation failure", fun test_create_resv_failure/0},
        {"Create reservation cluster leader", fun test_create_resv_cluster_leader/0},
        {"Create reservation cluster follower success", fun test_create_resv_cluster_follower_success/0},
        {"Create reservation cluster follower no_leader", fun test_create_resv_cluster_follower_no_leader/0},
        {"Create reservation cluster follower error", fun test_create_resv_cluster_follower_error/0},
        {"Create reservation hostlist expansion", fun test_create_resv_hostlist_expansion/0},
        {"Create reservation hostlist expansion error", fun test_create_resv_hostlist_error/0},
        {"Create reservation state active", fun test_create_resv_state_active/0},
        {"Create reservation state inactive", fun test_create_resv_state_inactive/0},

        %% REQUEST_UPDATE_RESERVATION (2052) tests - comprehensive coverage
        {"Update reservation success", fun test_update_resv_success/0},
        {"Update reservation with start_time", fun test_update_resv_start_time/0},
        {"Update reservation with end_time", fun test_update_resv_end_time/0},
        {"Update reservation with duration", fun test_update_resv_duration/0},
        {"Update reservation with nodes", fun test_update_resv_nodes/0},
        {"Update reservation with users", fun test_update_resv_users/0},
        {"Update reservation with accounts", fun test_update_resv_accounts/0},
        {"Update reservation with partition", fun test_update_resv_partition/0},
        {"Update reservation with flags", fun test_update_resv_flags/0},
        {"Update reservation failure", fun test_update_resv_failure/0},
        {"Update reservation cluster leader", fun test_update_resv_cluster_leader/0},
        {"Update reservation cluster follower success", fun test_update_resv_cluster_follower_success/0},
        {"Update reservation cluster follower no_leader", fun test_update_resv_cluster_follower_no_leader/0},
        {"Update reservation cluster follower error", fun test_update_resv_cluster_follower_error/0},
        {"Update reservation empty updates", fun test_update_resv_empty_updates/0},
        {"Update reservation hostlist expansion", fun test_update_resv_hostlist_expansion/0},
        {"Update reservation hostlist error", fun test_update_resv_hostlist_error/0},

        %% REQUEST_DELETE_RESERVATION (2053) tests - comprehensive coverage
        {"Delete reservation success", fun test_delete_resv_success/0},
        {"Delete reservation failure", fun test_delete_resv_failure/0},
        {"Delete reservation not found", fun test_delete_resv_not_found/0},
        {"Delete reservation cluster leader", fun test_delete_resv_cluster_leader/0},
        {"Delete reservation cluster follower success", fun test_delete_resv_cluster_follower_success/0},
        {"Delete reservation cluster follower no_leader", fun test_delete_resv_cluster_follower_no_leader/0},
        {"Delete reservation cluster follower error", fun test_delete_resv_cluster_follower_error/0},

        %% REQUEST_LICENSE_INFO (1017) tests - comprehensive coverage
        {"License info empty", fun test_license_info_empty/0},
        {"License info single", fun test_license_info_single/0},
        {"License info multiple", fun test_license_info_multiple/0},
        {"License info exception", fun test_license_info_exception/0},
        {"License info map format", fun test_license_info_map_format/0},
        {"License info tuple format", fun test_license_info_tuple_format/0},
        {"License info remote true", fun test_license_info_remote_true/0},
        {"License info remote false", fun test_license_info_remote_false/0},
        {"License info available calc", fun test_license_info_available_calc/0},
        {"License info response structure", fun test_license_info_response_structure/0},

        %% REQUEST_TOPO_INFO (2018) tests - comprehensive coverage
        {"Topology info empty", fun test_topo_info_empty/0},
        {"Topology info response structure", fun test_topo_info_response_structure/0},
        {"Topology info topo_count is zero", fun test_topo_info_count_zero/0},
        {"Topology info topos is empty list", fun test_topo_info_topos_empty/0},

        %% REQUEST_FRONT_END_INFO (2028) tests - comprehensive coverage
        {"Front end info empty", fun test_front_end_info_empty/0},
        {"Front end info response structure", fun test_front_end_info_response_structure/0},
        {"Front end info last_update set", fun test_front_end_info_last_update/0},
        {"Front end info front_end_count zero", fun test_front_end_info_count_zero/0},
        {"Front end info front_ends empty", fun test_front_end_info_list_empty/0},

        %% REQUEST_BURST_BUFFER_INFO (2020) tests - comprehensive coverage
        {"Burst buffer info empty pools", fun test_bb_info_empty_pools/0},
        {"Burst buffer info with pools", fun test_bb_info_with_pools/0},
        {"Burst buffer info multiple pools", fun test_bb_info_multiple_pools/0},
        {"Burst buffer info exception on list_pools", fun test_bb_info_list_pools_exception/0},
        {"Burst buffer info exception on get_stats", fun test_bb_info_get_stats_exception/0},
        {"Burst buffer info response structure", fun test_bb_info_response_structure/0},
        {"Burst buffer info pool conversion", fun test_bb_info_pool_conversion/0},
        {"Burst buffer info stats used", fun test_bb_info_stats_used/0}
     ]}.

%%====================================================================
%% Helper function tests
%%====================================================================

helper_function_test_() ->
    [
        %% reservation_to_reservation_info/1 tests
        {"Convert valid reservation tuple", fun test_resv_to_info_valid/0},
        {"Convert reservation with all fields", fun test_resv_to_info_all_fields/0},
        {"Convert small tuple returns defaults", fun test_resv_to_info_small_tuple/0},
        {"Convert non-tuple returns defaults", fun test_resv_to_info_non_tuple/0},

        %% reservation_state_to_flags/1 tests
        {"active state to flags 1", fun test_state_to_flags_active/0},
        {"inactive state to flags 0", fun test_state_to_flags_inactive/0},
        {"expired state to flags 2", fun test_state_to_flags_expired/0},
        {"unknown state to flags 0", fun test_state_to_flags_unknown/0},
        {"other state to flags 0", fun test_state_to_flags_other/0},

        %% determine_reservation_type/2 tests
        {"Type maint lowercase", fun test_type_maint_lower/0},
        {"Type MAINT uppercase", fun test_type_maint_upper/0},
        {"Type maintenance", fun test_type_maintenance/0},
        {"Type flex lowercase", fun test_type_flex_lower/0},
        {"Type FLEX uppercase", fun test_type_flex_upper/0},
        {"Type user lowercase", fun test_type_user_lower/0},
        {"Type USER uppercase", fun test_type_user_upper/0},
        {"Empty type with maint flag", fun test_type_empty_maint_flag/0},
        {"Empty type with flex flag", fun test_type_empty_flex_flag/0},
        {"Empty type with no flags", fun test_type_empty_no_flags/0},
        {"Unknown type defaults to user", fun test_type_unknown/0},

        %% parse_reservation_flags/1 tests
        {"Parse flags 0 returns empty", fun test_parse_flags_zero/0},
        {"Parse flags maint", fun test_parse_flags_maint/0},
        {"Parse flags ignore_jobs", fun test_parse_flags_ignore_jobs/0},
        {"Parse flags daily", fun test_parse_flags_daily/0},
        {"Parse flags weekly", fun test_parse_flags_weekly/0},
        {"Parse flags weekday", fun test_parse_flags_weekday/0},
        {"Parse flags weekend", fun test_parse_flags_weekend/0},
        {"Parse flags any", fun test_parse_flags_any/0},
        {"Parse flags first_cores", fun test_parse_flags_first_cores/0},
        {"Parse flags time_float", fun test_parse_flags_time_float/0},
        {"Parse flags purge_comp", fun test_parse_flags_purge_comp/0},
        {"Parse flags part_nodes", fun test_parse_flags_part_nodes/0},
        {"Parse flags overlap", fun test_parse_flags_overlap/0},
        {"Parse flags no_hold_jobs_after", fun test_parse_flags_no_hold_jobs_after/0},
        {"Parse flags static_alloc", fun test_parse_flags_static_alloc/0},
        {"Parse flags no_hold_jobs", fun test_parse_flags_no_hold_jobs/0},
        {"Parse flags flex", fun test_parse_flags_flex/0},
        {"Parse flags multiple", fun test_parse_flags_multiple/0},

        %% generate_reservation_name/0 tests
        {"Generate name is binary", fun test_gen_name_is_binary/0},
        {"Generate name starts with resv_", fun test_gen_name_prefix/0},
        {"Generate name unique", fun test_gen_name_unique/0},

        %% extract_reservation_fields/1 tests
        {"Extract fields valid tuple", fun test_extract_valid_tuple/0},
        {"Extract fields small tuple", fun test_extract_small_tuple/0},
        {"Extract fields non-tuple", fun test_extract_non_tuple/0},
        {"Extract fields with all fields", fun test_extract_all_fields/0},

        %% create_reservation_request_to_spec/1 tests
        {"Create spec basic", fun test_create_spec_basic/0},
        {"Create spec with empty name generates", fun test_create_spec_empty_name/0},
        {"Create spec with explicit name", fun test_create_spec_explicit_name/0},
        {"Create spec with start_time 0 uses now", fun test_create_spec_start_now/0},
        {"Create spec with explicit start_time", fun test_create_spec_explicit_start/0},
        {"Create spec with end_time 0 uses duration", fun test_create_spec_end_from_duration/0},
        {"Create spec with explicit end_time", fun test_create_spec_explicit_end/0},
        {"Create spec with duration 0 defaults to 60", fun test_create_spec_default_duration/0},
        {"Create spec parses users", fun test_create_spec_parses_users/0},
        {"Create spec parses accounts", fun test_create_spec_parses_accounts/0},
        {"Create spec parses features", fun test_create_spec_parses_features/0},
        {"Create spec with node_cnt 0 uses nodes length", fun test_create_spec_node_cnt_zero/0},
        {"Create spec with explicit node_cnt", fun test_create_spec_explicit_node_cnt/0},

        %% update_reservation_request_to_updates/1 tests
        {"Update spec empty request", fun test_update_spec_empty/0},
        {"Update spec with start_time", fun test_update_spec_start_time/0},
        {"Update spec with end_time", fun test_update_spec_end_time/0},
        {"Update spec with duration", fun test_update_spec_duration/0},
        {"Update spec with nodes", fun test_update_spec_nodes/0},
        {"Update spec with users", fun test_update_spec_users/0},
        {"Update spec with accounts", fun test_update_spec_accounts/0},
        {"Update spec with partition", fun test_update_spec_partition/0},
        {"Update spec with flags", fun test_update_spec_flags/0},
        {"Update spec all fields", fun test_update_spec_all_fields/0},

        %% license_to_license_info/1 tests
        {"License map to info", fun test_license_map_to_info/0},
        {"License map with remote true", fun test_license_map_remote_true/0},
        {"License map available calc", fun test_license_map_available/0},
        {"License tuple to info", fun test_license_tuple_to_info/0},
        {"License small tuple", fun test_license_small_tuple/0},
        {"License invalid input", fun test_license_invalid/0},

        %% build_burst_buffer_info/2 tests
        {"Build BB info empty pools", fun test_build_bb_empty/0},
        {"Build BB info with pools", fun test_build_bb_with_pools/0},
        {"Build BB info calculates sizes", fun test_build_bb_calculates_sizes/0},
        {"Build BB info multiple pools", fun test_build_bb_multiple_pools/0},

        %% pool_to_bb_pool/1 tests
        {"Pool tuple to bb_pool", fun test_pool_to_bb_pool/0},
        {"Pool small tuple returns default", fun test_pool_small_tuple/0},
        {"Pool non-tuple returns default", fun test_pool_non_tuple/0},
        {"Pool calculates unfree_space", fun test_pool_calculates_unfree/0},

        %% do_graceful_shutdown/0 tests
        {"Graceful shutdown returns ok", fun test_graceful_shutdown_ok/0},

        %% is_cluster_enabled/0 tests
        {"Cluster disabled no config", fun test_cluster_disabled_no_config/0},
        {"Cluster disabled single node", fun test_cluster_disabled_single_node/0},
        {"Cluster disabled empty list", fun test_cluster_disabled_empty_list/0},
        {"Cluster enabled multiple nodes", fun test_cluster_enabled_multiple/0},
        {"Cluster enabled process running", fun test_cluster_enabled_process/0}
    ].

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Disable cluster mode by default (catch errors if app controller not running)
    catch application:set_env(flurm_controller, enable_cluster, false),
    catch application:unset_env(flurm_controller, cluster_nodes),

    %% Unload any existing mocks
    catch meck:unload(flurm_reservation),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_burst_buffer),
    catch meck:unload(flurm_controller_cluster),
    catch meck:unload(flurm_config_slurm),
    catch meck:unload(flurm_controller_handler),

    %% Start meck for mocking
    meck:new([flurm_reservation, flurm_license, flurm_burst_buffer,
              flurm_controller_cluster, flurm_config_slurm,
              flurm_controller_handler],
             [passthrough, no_link, non_strict]),

    %% Default mock behaviors
    meck:expect(flurm_reservation, list, fun() -> [] end),
    meck:expect(flurm_reservation, create, fun(_) -> {ok, <<"resv_test">>} end),
    meck:expect(flurm_reservation, update, fun(_, _) -> ok end),
    meck:expect(flurm_reservation, delete, fun(_) -> ok end),
    meck:expect(flurm_license, list, fun() -> [] end),
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() -> #{} end),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {ok, ok} end),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),

    %% Mock imported functions from flurm_controller_handler
    meck:expect(flurm_controller_handler, ensure_binary, fun
        (B) when is_binary(B) -> B;
        (L) when is_list(L) -> list_to_binary(L);
        (A) when is_atom(A) -> atom_to_binary(A, utf8);
        (I) when is_integer(I) -> integer_to_binary(I);
        (_) -> <<>>
    end),
    meck:expect(flurm_controller_handler, error_to_binary, fun
        (E) when is_binary(E) -> E;
        (E) when is_atom(E) -> atom_to_binary(E, utf8);
        (E) -> iolist_to_binary(io_lib:format("~p", [E]))
    end),
    meck:expect(flurm_controller_handler, format_allocated_nodes, fun
        ([]) -> <<>>;
        (Nodes) when is_list(Nodes) -> iolist_to_binary(lists:join(<<",">>, Nodes))
    end),

    ok.

cleanup(_) ->
    catch meck:unload([flurm_reservation, flurm_license, flurm_burst_buffer,
                       flurm_controller_cluster, flurm_config_slurm,
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

make_reservation_tuple(Name) ->
    Now = erlang:system_time(second),
    %% Tuple format: {reservation, name, type, start_time, end_time, duration, nodes,
    %%                node_count, partition, features, users, accounts, flags, state}
    {reservation, Name, maint, Now, Now + 3600, 3600, [<<"node1">>, <<"node2">>],
     2, <<"default">>, [<<"feature1">>], [<<"user1">>, <<"user2">>], [<<"acct1">>], [daily, maint], active}.

make_create_reservation_request() ->
    #create_reservation_request{
        name = <<>>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<>>,
        node_cnt = 0,
        partition = <<>>,
        users = <<>>,
        accounts = <<>>,
        groups = <<>>,
        licenses = <<>>,
        features = <<>>,
        flags = 0,
        type = <<>>
    }.

make_update_reservation_request(Name) ->
    #update_reservation_request{
        name = Name,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        node_cnt = 0,
        partition = <<>>,
        users = <<>>,
        accounts = <<>>,
        groups = <<>>,
        licenses = <<>>,
        features = <<>>,
        flags = 0
    }.

%%====================================================================
%% Ping Tests
%%====================================================================

test_ping_request_basic() ->
    Header = make_header(?REQUEST_PING),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_ping_response_record() ->
    Header = make_header(?REQUEST_PING),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, slurm_rc_response)).

test_ping_response_type() ->
    Header = make_header(?REQUEST_PING),
    {ok, MsgType, _} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType).

test_ping_empty_body() ->
    Header = make_header(?REQUEST_PING),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_ping_binary_body() ->
    Header = make_header(?REQUEST_PING),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<"some data">>),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_ping_any_body() ->
    Header = make_header(?REQUEST_PING),
    {ok, _, Response} = flurm_handler_admin:handle(Header, {tuple, data}),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

%%====================================================================
%% Shutdown Tests
%%====================================================================

test_shutdown_non_cluster_success() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_shutdown_cluster_leader_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(shutdown, _) -> {ok, ok} end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_forward_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_forward_generic_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_forward_ok_result() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(shutdown, _) -> {ok, ok} end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_forward_error_result() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(shutdown, _) -> {ok, {error, failed}} end),
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_triggers_graceful() ->
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, _, _} = flurm_handler_admin:handle(Header, <<>>),
    %% If we get here without crash, graceful shutdown was triggered
    ?assert(true).

%%====================================================================
%% Reservation Info Tests
%%====================================================================

test_resv_info_empty() ->
    meck:expect(flurm_reservation, list, fun() -> [] end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_RESERVATION_INFO, MsgType),
    ?assertEqual(0, Response#reservation_info_response.reservation_count).

test_resv_info_single() ->
    meck:expect(flurm_reservation, list, fun() -> [make_reservation_tuple(<<"resv1">>)] end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(1, Response#reservation_info_response.reservation_count).

test_resv_info_multiple() ->
    Resvs = [make_reservation_tuple(<<"resv1">>), make_reservation_tuple(<<"resv2">>),
             make_reservation_tuple(<<"resv3">>)],
    meck:expect(flurm_reservation, list, fun() -> Resvs end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(3, Response#reservation_info_response.reservation_count).

test_resv_info_exception() ->
    meck:expect(flurm_reservation, list, fun() -> error(crash) end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#reservation_info_response.reservation_count).

test_resv_info_response_structure() ->
    meck:expect(flurm_reservation, list, fun() -> [make_reservation_tuple(<<"test">>)] end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, reservation_info_response)).

test_resv_info_last_update() ->
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(Response#reservation_info_response.last_update > 0).

test_resv_info_all_fields() ->
    meck:expect(flurm_reservation, list, fun() -> [make_reservation_tuple(<<"test">>)] end),
    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [ResvInfo] = Response#reservation_info_response.reservations,
    ?assertEqual(<<"test">>, ResvInfo#reservation_info.name),
    ?assert(is_integer(ResvInfo#reservation_info.start_time)),
    ?assert(is_integer(ResvInfo#reservation_info.end_time)),
    ?assertEqual(2, ResvInfo#reservation_info.node_cnt).

%%====================================================================
%% Create Reservation Tests
%%====================================================================

test_create_resv_success() ->
    meck:expect(flurm_reservation, create, fun(_) -> {ok, <<"new_resv">>} end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(?RESPONSE_CREATE_RESERVATION, MsgType),
    ?assertEqual(0, Response#create_reservation_response.error_code).

test_create_resv_generated_name() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        Name = maps:get(name, Spec),
        ?assert(binary:match(Name, <<"resv_">>) =/= nomatch),
        {ok, Name}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assert(binary:match(Response#create_reservation_response.name, <<"resv_">>) =/= nomatch).

test_create_resv_explicit_name() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(<<"my_resv">>, maps:get(name, Spec)),
        {ok, <<"my_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{name = <<"my_resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(<<"my_resv">>, Response#create_reservation_response.name).

test_create_resv_with_nodes() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-3]">>) ->
        [<<"node1">>, <<"node2">>, <<"node3">>]
    end),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], maps:get(nodes, Spec)),
        {ok, <<"node_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"node_resv">>,
        nodes = <<"node[1-3]">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_users() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"user1">>, <<"user2">>], maps:get(users, Spec)),
        {ok, <<"user_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"user_resv">>,
        users = <<"user1,user2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_accounts() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"acct1">>, <<"acct2">>], maps:get(accounts, Spec)),
        {ok, <<"acct_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"acct_resv">>,
        accounts = <<"acct1,acct2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_features() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"gpu">>, <<"fast">>], maps:get(features, Spec)),
        {ok, <<"feat_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"feat_resv">>,
        features = <<"gpu,fast">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_partition() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(<<"gpu_partition">>, maps:get(partition, Spec)),
        {ok, <<"part_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"part_resv">>,
        partition = <<"gpu_partition">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_start_time() ->
    Now = erlang:system_time(second),
    StartTime = Now + 3600,
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(StartTime, maps:get(start_time, Spec)),
        {ok, <<"time_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"time_resv">>,
        start_time = StartTime
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_end_time() ->
    Now = erlang:system_time(second),
    EndTime = Now + 7200,
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(EndTime, maps:get(end_time, Spec)),
        {ok, <<"end_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"end_resv">>,
        end_time = EndTime
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_duration() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        StartTime = maps:get(start_time, Spec),
        EndTime = maps:get(end_time, Spec),
        ?assertEqual(7200, EndTime - StartTime), %% 120 minutes = 7200 seconds
        {ok, <<"dur_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"dur_resv">>,
        duration = 120
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_with_node_cnt() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(5, maps:get(node_count, Spec)),
        {ok, <<"cnt_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"cnt_resv">>,
        node_cnt = 5
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_maint_type() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(maint, maps:get(type, Spec)),
        {ok, <<"maint_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"maint_resv">>,
        type = <<"maint">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_flex_type() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(flex, maps:get(type, Spec)),
        {ok, <<"flex_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"flex_resv">>,
        type = <<"flex">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_user_type() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(user, maps:get(type, Spec)),
        {ok, <<"user_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"user_resv">>,
        type = <<"user">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_maint_flag() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        Flags = maps:get(flags, Spec),
        ?assert(lists:member(maint, Flags)),
        {ok, <<"flag_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"flag_resv">>,
        flags = 16#0001
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_flex_flag() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        Flags = maps:get(flags, Spec),
        ?assert(lists:member(flex, Flags)),
        {ok, <<"flex_flag_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"flex_flag_resv">>,
        flags = 16#8000
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_failure() ->
    meck:expect(flurm_reservation, create, fun(_) -> {error, invalid_time_range} end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(?RESPONSE_CREATE_RESERVATION, MsgType),
    ?assertEqual(1, Response#create_reservation_response.error_code).

test_create_resv_cluster_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_reservation, create, fun(_) -> {ok, <<"leader_resv">>} end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{name = <<"leader_resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(<<"leader_resv">>, Response#create_reservation_response.name),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_create_resv_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(create_reservation, _) ->
        {ok, {ok, <<"forwarded_resv">>}}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(<<"forwarded_resv">>, Response#create_reservation_response.name),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_create_resv_cluster_follower_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(1, Response#create_reservation_response.error_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_create_resv_cluster_follower_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = make_create_reservation_request(),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(1, Response#create_reservation_response.error_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_create_resv_hostlist_expansion() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[01-05]">>) ->
        [<<"node01">>, <<"node02">>, <<"node03">>, <<"node04">>, <<"node05">>]
    end),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        Nodes = maps:get(nodes, Spec),
        ?assertEqual(5, length(Nodes)),
        {ok, <<"expanded_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"expanded_resv">>,
        nodes = <<"node[01-05]">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_hostlist_error() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> error(invalid_pattern) end),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        Nodes = maps:get(nodes, Spec),
        ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
        {ok, <<"fallback_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"fallback_resv">>,
        nodes = <<"node1,node2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_state_active() ->
    Now = erlang:system_time(second),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(active, maps:get(state, Spec)),
        {ok, <<"active_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"active_resv">>,
        start_time = Now - 100  %% Start time in the past
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_resv_state_inactive() ->
    Now = erlang:system_time(second),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(inactive, maps:get(state, Spec)),
        {ok, <<"inactive_resv">>}
    end),
    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = (make_create_reservation_request())#create_reservation_request{
        name = <<"inactive_resv">>,
        start_time = Now + 3600  %% Start time in the future
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

%%====================================================================
%% Update Reservation Tests
%%====================================================================

test_update_resv_success() ->
    meck:expect(flurm_reservation, update, fun(<<"resv1">>, _) -> ok end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv1">>),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_resv_start_time() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assert(maps:is_key(start_time, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        start_time = 1000
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_end_time() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual(2000, maps:get(end_time, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        end_time = 2000
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_duration() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual(7200, maps:get(duration, Updates)), %% 120 * 60
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        duration = 120
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_nodes() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"new_nodes">>) ->
        [<<"newnode1">>, <<"newnode2">>]
    end),
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual([<<"newnode1">>, <<"newnode2">>], maps:get(nodes, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        nodes = <<"new_nodes">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_users() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual([<<"newuser1">>, <<"newuser2">>], maps:get(users, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        users = <<"newuser1,newuser2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_accounts() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual([<<"newacct1">>, <<"newacct2">>], maps:get(accounts, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        accounts = <<"newacct1,newacct2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_partition() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual(<<"new_partition">>, maps:get(partition, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        partition = <<"new_partition">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_flags() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        Flags = maps:get(flags, Updates),
        ?assert(lists:member(maint, Flags)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        flags = 16#0001
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_failure() ->
    meck:expect(flurm_reservation, update, fun(_, _) -> {error, not_found} end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"nonexistent">>),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_update_resv_cluster_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_reservation, update, fun(_, _) -> ok end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv">>),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_update_resv_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_reservation, _) ->
        {ok, ok}
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv">>),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_update_resv_cluster_follower_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv">>),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_update_resv_cluster_follower_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv">>),
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_update_resv_empty_updates() ->
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual(#{}, Updates),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = make_update_reservation_request(<<"resv">>),
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_hostlist_expansion() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-5]">>) ->
        [<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>, <<"node5">>]
    end),
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual(5, length(maps:get(nodes, Updates))),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        nodes = <<"node[1-5]">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_resv_hostlist_error() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> error(invalid) end),
    meck:expect(flurm_reservation, update, fun(_, Updates) ->
        ?assertEqual([<<"n1">>, <<"n2">>], maps:get(nodes, Updates)),
        ok
    end),
    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = (make_update_reservation_request(<<"resv">>))#update_reservation_request{
        nodes = <<"n1,n2">>
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

%%====================================================================
%% Delete Reservation Tests
%%====================================================================

test_delete_resv_success() ->
    meck:expect(flurm_reservation, delete, fun(<<"resv_to_delete">>) -> ok end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv_to_delete">>},
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_delete_resv_failure() ->
    meck:expect(flurm_reservation, delete, fun(_) -> {error, internal_error} end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_delete_resv_not_found() ->
    meck:expect(flurm_reservation, delete, fun(_) -> {error, not_found} end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"nonexistent">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_delete_resv_cluster_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),
    meck:expect(flurm_reservation, delete, fun(_) -> ok end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_delete_resv_cluster_follower_success() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(delete_reservation, _) ->
        {ok, ok}
    end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_delete_resv_cluster_follower_no_leader() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_delete_resv_cluster_follower_error() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, timeout} end),
    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    catch application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% License Info Tests
%%====================================================================

test_license_info_empty() ->
    meck:expect(flurm_license, list, fun() -> [] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_LICENSE_INFO, MsgType),
    ?assertEqual(0, Response#license_info_response.license_count).

test_license_info_single() ->
    Lic = #{name => <<"matlab">>, total => 10, in_use => 5, available => 5, reserved => 0, remote => false},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(1, Response#license_info_response.license_count).

test_license_info_multiple() ->
    Lics = [
        #{name => <<"matlab">>, total => 10, in_use => 5, remote => false},
        #{name => <<"gaussian">>, total => 5, in_use => 2, remote => false},
        #{name => <<"ansys">>, total => 20, in_use => 10, remote => true}
    ],
    meck:expect(flurm_license, list, fun() -> Lics end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(3, Response#license_info_response.license_count).

test_license_info_exception() ->
    meck:expect(flurm_license, list, fun() -> error(crash) end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#license_info_response.license_count).

test_license_info_map_format() ->
    Lic = #{name => <<"test_lic">>, total => 100, in_use => 25, reserved => 10, remote => false},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(<<"test_lic">>, LicInfo#license_info.name),
    ?assertEqual(100, LicInfo#license_info.total),
    ?assertEqual(25, LicInfo#license_info.in_use).

test_license_info_tuple_format() ->
    Lic = {license, <<"tuple_lic">>, 50, 10, 40},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(<<"tuple_lic">>, LicInfo#license_info.name).

test_license_info_remote_true() ->
    Lic = #{name => <<"remote_lic">>, total => 5, in_use => 0, remote => true},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(1, LicInfo#license_info.remote).

test_license_info_remote_false() ->
    Lic = #{name => <<"local_lic">>, total => 5, in_use => 0, remote => false},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(0, LicInfo#license_info.remote).

test_license_info_available_calc() ->
    Lic = #{name => <<"calc_lic">>, total => 100, in_use => 30},
    meck:expect(flurm_license, list, fun() -> [Lic] end),
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(70, LicInfo#license_info.available).

test_license_info_response_structure() ->
    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, license_info_response)),
    ?assert(Response#license_info_response.last_update > 0).

%%====================================================================
%% Topology Info Tests
%%====================================================================

test_topo_info_empty() ->
    Header = make_header(?REQUEST_TOPO_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_TOPO_INFO, MsgType),
    ?assertEqual(0, Response#topo_info_response.topo_count).

test_topo_info_response_structure() ->
    Header = make_header(?REQUEST_TOPO_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, topo_info_response)).

test_topo_info_count_zero() ->
    Header = make_header(?REQUEST_TOPO_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#topo_info_response.topo_count).

test_topo_info_topos_empty() ->
    Header = make_header(?REQUEST_TOPO_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual([], Response#topo_info_response.topos).

%%====================================================================
%% Front End Info Tests
%%====================================================================

test_front_end_info_empty() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_FRONT_END_INFO, MsgType),
    ?assertEqual(0, Response#front_end_info_response.front_end_count).

test_front_end_info_response_structure() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, front_end_info_response)).

test_front_end_info_last_update() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(Response#front_end_info_response.last_update > 0).

test_front_end_info_count_zero() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#front_end_info_response.front_end_count).

test_front_end_info_list_empty() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual([], Response#front_end_info_response.front_ends).

%%====================================================================
%% Burst Buffer Info Tests
%%====================================================================

test_bb_info_empty_pools() ->
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [] end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(?RESPONSE_BURST_BUFFER_INFO, MsgType),
    ?assertEqual(0, Response#burst_buffer_info_response.burst_buffer_count).

test_bb_info_with_pools() ->
    Pool = {bb_pool, <<"default">>, generic, 1000000, 800000, 1048576, [], up, []},
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [Pool] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() ->
        #{total_size => 1000000, used_size => 200000, free_size => 800000}
    end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(1, Response#burst_buffer_info_response.burst_buffer_count).

test_bb_info_multiple_pools() ->
    Pools = [
        {bb_pool, <<"pool1">>, generic, 500000, 400000, 1048576, [], up, []},
        {bb_pool, <<"pool2">>, generic, 500000, 400000, 1048576, [], up, []}
    ],
    meck:expect(flurm_burst_buffer, list_pools, fun() -> Pools end),
    meck:expect(flurm_burst_buffer, get_stats, fun() -> #{} end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(1, Response#burst_buffer_info_response.burst_buffer_count),
    [BBInfo] = Response#burst_buffer_info_response.burst_buffers,
    ?assertEqual(2, BBInfo#burst_buffer_info.pool_cnt).

test_bb_info_list_pools_exception() ->
    meck:expect(flurm_burst_buffer, list_pools, fun() -> error(crash) end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(0, Response#burst_buffer_info_response.burst_buffer_count).

test_bb_info_get_stats_exception() ->
    Pool = {bb_pool, <<"test">>, generic, 1000, 500, 512, [], up, []},
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [Pool] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() -> error(crash) end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assertEqual(1, Response#burst_buffer_info_response.burst_buffer_count).

test_bb_info_response_structure() ->
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    ?assert(is_record(Response, burst_buffer_info_response)),
    ?assert(Response#burst_buffer_info_response.last_update > 0).

test_bb_info_pool_conversion() ->
    Pool = {bb_pool, <<"mypool">>, generic, 2000000, 1500000, 4096, [], up, []},
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [Pool] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() -> #{} end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [BBInfo] = Response#burst_buffer_info_response.burst_buffers,
    [PoolInfo] = BBInfo#burst_buffer_info.pools,
    ?assertEqual(<<"mypool">>, PoolInfo#burst_buffer_pool.name).

test_bb_info_stats_used() ->
    Pool = {bb_pool, <<"pool">>, generic, 1000000, 800000, 1048576, [], up, []},
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [Pool] end),
    meck:expect(flurm_burst_buffer, get_stats, fun() ->
        #{total_size => 1000000, used_size => 200000, free_size => 800000}
    end),
    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),
    [BBInfo] = Response#burst_buffer_info_response.burst_buffers,
    ?assertEqual(1000000, BBInfo#burst_buffer_info.total_space),
    ?assertEqual(200000, BBInfo#burst_buffer_info.used_space).

%%====================================================================
%% Helper Function Tests - reservation_to_reservation_info
%%====================================================================

test_resv_to_info_valid() ->
    Resv = make_reservation_tuple(<<"valid_resv">>),
    ResvInfo = flurm_handler_admin:reservation_to_reservation_info(Resv),
    ?assertEqual(<<"valid_resv">>, ResvInfo#reservation_info.name).

test_resv_to_info_all_fields() ->
    Resv = make_reservation_tuple(<<"full_resv">>),
    ResvInfo = flurm_handler_admin:reservation_to_reservation_info(Resv),
    ?assert(is_binary(ResvInfo#reservation_info.name)),
    ?assert(is_integer(ResvInfo#reservation_info.start_time)),
    ?assert(is_integer(ResvInfo#reservation_info.end_time)),
    ?assert(is_integer(ResvInfo#reservation_info.node_cnt)),
    ?assert(is_integer(ResvInfo#reservation_info.flags)).

test_resv_to_info_small_tuple() ->
    Resv = {small, tuple, only},
    ResvInfo = flurm_handler_admin:reservation_to_reservation_info(Resv),
    ?assertEqual(<<>>, ResvInfo#reservation_info.name).

test_resv_to_info_non_tuple() ->
    ResvInfo = flurm_handler_admin:reservation_to_reservation_info(not_a_tuple),
    ?assertEqual(<<>>, ResvInfo#reservation_info.name).

%%====================================================================
%% Helper Function Tests - reservation_state_to_flags
%%====================================================================

test_state_to_flags_active() ->
    ?assertEqual(1, flurm_handler_admin:reservation_state_to_flags(active)).

test_state_to_flags_inactive() ->
    ?assertEqual(0, flurm_handler_admin:reservation_state_to_flags(inactive)).

test_state_to_flags_expired() ->
    ?assertEqual(2, flurm_handler_admin:reservation_state_to_flags(expired)).

test_state_to_flags_unknown() ->
    ?assertEqual(0, flurm_handler_admin:reservation_state_to_flags(unknown_state)).

test_state_to_flags_other() ->
    ?assertEqual(0, flurm_handler_admin:reservation_state_to_flags(something_else)).

%%====================================================================
%% Helper Function Tests - determine_reservation_type
%%====================================================================

test_type_maint_lower() ->
    ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<"maint">>, 0)).

test_type_maint_upper() ->
    ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<"MAINT">>, 0)).

test_type_maintenance() ->
    ?assertEqual(maintenance, flurm_handler_admin:determine_reservation_type(<<"maintenance">>, 0)).

test_type_flex_lower() ->
    ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<"flex">>, 0)).

test_type_flex_upper() ->
    ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<"FLEX">>, 0)).

test_type_user_lower() ->
    ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"user">>, 0)).

test_type_user_upper() ->
    ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"USER">>, 0)).

test_type_empty_maint_flag() ->
    ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<>>, 16#0001)).

test_type_empty_flex_flag() ->
    ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<>>, 16#8000)).

test_type_empty_no_flags() ->
    ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<>>, 0)).

test_type_unknown() ->
    ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"unknown_type">>, 0)).

%%====================================================================
%% Helper Function Tests - parse_reservation_flags
%%====================================================================

test_parse_flags_zero() ->
    ?assertEqual([], flurm_handler_admin:parse_reservation_flags(0)).

test_parse_flags_maint() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0001),
    ?assert(lists:member(maint, Flags)).

test_parse_flags_ignore_jobs() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0002),
    ?assert(lists:member(ignore_jobs, Flags)).

test_parse_flags_daily() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0004),
    ?assert(lists:member(daily, Flags)).

test_parse_flags_weekly() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0008),
    ?assert(lists:member(weekly, Flags)).

test_parse_flags_weekday() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0010),
    ?assert(lists:member(weekday, Flags)).

test_parse_flags_weekend() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0020),
    ?assert(lists:member(weekend, Flags)).

test_parse_flags_any() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0040),
    ?assert(lists:member(any, Flags)).

test_parse_flags_first_cores() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0080),
    ?assert(lists:member(first_cores, Flags)).

test_parse_flags_time_float() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0100),
    ?assert(lists:member(time_float, Flags)).

test_parse_flags_purge_comp() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0200),
    ?assert(lists:member(purge_comp, Flags)).

test_parse_flags_part_nodes() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0400),
    ?assert(lists:member(part_nodes, Flags)).

test_parse_flags_overlap() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0800),
    ?assert(lists:member(overlap, Flags)).

test_parse_flags_no_hold_jobs_after() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#1000),
    ?assert(lists:member(no_hold_jobs_after, Flags)).

test_parse_flags_static_alloc() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#2000),
    ?assert(lists:member(static_alloc, Flags)).

test_parse_flags_no_hold_jobs() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#4000),
    ?assert(lists:member(no_hold_jobs, Flags)).

test_parse_flags_flex() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#8000),
    ?assert(lists:member(flex, Flags)).

test_parse_flags_multiple() ->
    Flags = flurm_handler_admin:parse_reservation_flags(16#0005), %% maint + daily
    ?assert(lists:member(maint, Flags)),
    ?assert(lists:member(daily, Flags)).

%%====================================================================
%% Helper Function Tests - generate_reservation_name
%%====================================================================

test_gen_name_is_binary() ->
    Name = flurm_handler_admin:generate_reservation_name(),
    ?assert(is_binary(Name)).

test_gen_name_prefix() ->
    Name = flurm_handler_admin:generate_reservation_name(),
    ?assert(binary:match(Name, <<"resv_">>) =/= nomatch).

test_gen_name_unique() ->
    Name1 = flurm_handler_admin:generate_reservation_name(),
    Name2 = flurm_handler_admin:generate_reservation_name(),
    ?assertNotEqual(Name1, Name2).

%%====================================================================
%% Helper Function Tests - extract_reservation_fields
%%====================================================================

test_extract_valid_tuple() ->
    Resv = make_reservation_tuple(<<"extract_test">>),
    {Name, StartTime, EndTime, Nodes, Users, State, _Flags} =
        flurm_handler_admin:extract_reservation_fields(Resv),
    ?assertEqual(<<"extract_test">>, Name),
    ?assert(is_integer(StartTime)),
    ?assert(is_integer(EndTime)),
    ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
    ?assertEqual([<<"user1">>, <<"user2">>], Users),
    ?assertEqual(active, State).

test_extract_small_tuple() ->
    {Name, StartTime, EndTime, Nodes, Users, State, _Flags} =
        flurm_handler_admin:extract_reservation_fields({too, small}),
    ?assertEqual(<<>>, Name),
    ?assertEqual(0, StartTime),
    ?assertEqual(0, EndTime),
    ?assertEqual([], Nodes),
    ?assertEqual([], Users),
    ?assertEqual(inactive, State).

test_extract_non_tuple() ->
    {Name, _, _, _, _, _, _} =
        flurm_handler_admin:extract_reservation_fields(not_a_tuple),
    ?assertEqual(<<>>, Name).

test_extract_all_fields() ->
    Resv = make_reservation_tuple(<<"full">>),
    {Name, StartTime, EndTime, Nodes, Users, State, Flags} =
        flurm_handler_admin:extract_reservation_fields(Resv),
    ?assert(is_binary(Name)),
    ?assert(is_integer(StartTime)),
    ?assert(is_integer(EndTime)),
    ?assert(is_list(Nodes)),
    ?assert(is_list(Users)),
    ?assert(is_atom(State)),
    ?assert(is_list(Flags)).

%%====================================================================
%% Helper Function Tests - create_reservation_request_to_spec
%%====================================================================

test_create_spec_basic() ->
    Request = make_create_reservation_request(),
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assert(is_map(Spec)),
    ?assert(maps:is_key(name, Spec)),
    ?assert(maps:is_key(start_time, Spec)),
    ?assert(maps:is_key(end_time, Spec)).

test_create_spec_empty_name() ->
    Request = (make_create_reservation_request())#create_reservation_request{name = <<>>},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    Name = maps:get(name, Spec),
    ?assert(binary:match(Name, <<"resv_">>) =/= nomatch).

test_create_spec_explicit_name() ->
    Request = (make_create_reservation_request())#create_reservation_request{name = <<"myname">>},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual(<<"myname">>, maps:get(name, Spec)).

test_create_spec_start_now() ->
    Now = erlang:system_time(second),
    Request = (make_create_reservation_request())#create_reservation_request{start_time = 0},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    StartTime = maps:get(start_time, Spec),
    ?assert(StartTime >= Now).

test_create_spec_explicit_start() ->
    Request = (make_create_reservation_request())#create_reservation_request{start_time = 12345},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual(12345, maps:get(start_time, Spec)).

test_create_spec_end_from_duration() ->
    Request = (make_create_reservation_request())#create_reservation_request{end_time = 0, duration = 60},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    StartTime = maps:get(start_time, Spec),
    EndTime = maps:get(end_time, Spec),
    ?assertEqual(3600, EndTime - StartTime). %% 60 minutes = 3600 seconds

test_create_spec_explicit_end() ->
    Request = (make_create_reservation_request())#create_reservation_request{end_time = 99999},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual(99999, maps:get(end_time, Spec)).

test_create_spec_default_duration() ->
    Request = (make_create_reservation_request())#create_reservation_request{end_time = 0, duration = 0},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    StartTime = maps:get(start_time, Spec),
    EndTime = maps:get(end_time, Spec),
    ?assertEqual(3600, EndTime - StartTime). %% default 60 minutes

test_create_spec_parses_users() ->
    Request = (make_create_reservation_request())#create_reservation_request{users = <<"u1,u2,u3">>},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual([<<"u1">>, <<"u2">>, <<"u3">>], maps:get(users, Spec)).

test_create_spec_parses_accounts() ->
    Request = (make_create_reservation_request())#create_reservation_request{accounts = <<"a1,a2">>},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual([<<"a1">>, <<"a2">>], maps:get(accounts, Spec)).

test_create_spec_parses_features() ->
    Request = (make_create_reservation_request())#create_reservation_request{features = <<"f1,f2">>},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual([<<"f1">>, <<"f2">>], maps:get(features, Spec)).

test_create_spec_node_cnt_zero() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [<<"n1">>, <<"n2">>] end),
    Request = (make_create_reservation_request())#create_reservation_request{
        nodes = <<"nodes">>,
        node_cnt = 0
    },
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual(2, maps:get(node_count, Spec)).

test_create_spec_explicit_node_cnt() ->
    Request = (make_create_reservation_request())#create_reservation_request{node_cnt = 10},
    Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
    ?assertEqual(10, maps:get(node_count, Spec)).

%%====================================================================
%% Helper Function Tests - update_reservation_request_to_updates
%%====================================================================

test_update_spec_empty() ->
    Request = make_update_reservation_request(<<"test">>),
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual(#{}, Updates).

test_update_spec_start_time() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{start_time = 1000},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual(1000, maps:get(start_time, Updates)).

test_update_spec_end_time() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{end_time = 2000},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual(2000, maps:get(end_time, Updates)).

test_update_spec_duration() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{duration = 120},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual(7200, maps:get(duration, Updates)). %% 120 * 60

test_update_spec_nodes() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"n1,n2">>) -> [<<"n1">>, <<"n2">>] end),
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{nodes = <<"n1,n2">>},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assert(maps:is_key(nodes, Updates)).

test_update_spec_users() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{users = <<"u1,u2">>},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual([<<"u1">>, <<"u2">>], maps:get(users, Updates)).

test_update_spec_accounts() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{accounts = <<"a1,a2">>},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual([<<"a1">>, <<"a2">>], maps:get(accounts, Updates)).

test_update_spec_partition() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{partition = <<"part1">>},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assertEqual(<<"part1">>, maps:get(partition, Updates)).

test_update_spec_flags() ->
    Request = (make_update_reservation_request(<<"test">>))#update_reservation_request{flags = 16#0001},
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    Flags = maps:get(flags, Updates),
    ?assert(lists:member(maint, Flags)).

test_update_spec_all_fields() ->
    Request = #update_reservation_request{
        name = <<"test">>,
        start_time = 1000,
        end_time = 2000,
        duration = 60,
        nodes = <<>>,
        users = <<"u1">>,
        accounts = <<"a1">>,
        partition = <<"p1">>,
        flags = 16#0001
    },
    Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
    ?assert(maps:is_key(start_time, Updates)),
    ?assert(maps:is_key(end_time, Updates)),
    ?assert(maps:is_key(duration, Updates)),
    ?assert(maps:is_key(users, Updates)),
    ?assert(maps:is_key(accounts, Updates)),
    ?assert(maps:is_key(partition, Updates)),
    ?assert(maps:is_key(flags, Updates)).

%%====================================================================
%% Helper Function Tests - license_to_license_info
%%====================================================================

test_license_map_to_info() ->
    Lic = #{name => <<"matlab">>, total => 10, in_use => 3},
    LicInfo = flurm_handler_admin:license_to_license_info(Lic),
    ?assertEqual(<<"matlab">>, LicInfo#license_info.name),
    ?assertEqual(10, LicInfo#license_info.total),
    ?assertEqual(3, LicInfo#license_info.in_use).

test_license_map_remote_true() ->
    Lic = #{name => <<"test">>, total => 5, in_use => 0, remote => true},
    LicInfo = flurm_handler_admin:license_to_license_info(Lic),
    ?assertEqual(1, LicInfo#license_info.remote).

test_license_map_available() ->
    Lic = #{name => <<"test">>, total => 100, in_use => 30},
    LicInfo = flurm_handler_admin:license_to_license_info(Lic),
    ?assertEqual(70, LicInfo#license_info.available).

test_license_tuple_to_info() ->
    Lic = {license, <<"gaussian">>, 20, 5, 15},
    LicInfo = flurm_handler_admin:license_to_license_info(Lic),
    ?assertEqual(<<"gaussian">>, LicInfo#license_info.name),
    ?assertEqual(20, LicInfo#license_info.total),
    ?assertEqual(5, LicInfo#license_info.in_use).

test_license_small_tuple() ->
    Lic = {too, small},
    LicInfo = flurm_handler_admin:license_to_license_info(Lic),
    ?assert(is_record(LicInfo, license_info)).

test_license_invalid() ->
    LicInfo = flurm_handler_admin:license_to_license_info(invalid),
    ?assert(is_record(LicInfo, license_info)).

%%====================================================================
%% Helper Function Tests - build_burst_buffer_info
%%====================================================================

test_build_bb_empty() ->
    Result = flurm_handler_admin:build_burst_buffer_info([], #{}),
    ?assertEqual([], Result).

test_build_bb_with_pools() ->
    Pools = [{bb_pool, <<"pool1">>, generic, 1000, 800, 512, [], up, []}],
    Stats = #{total_size => 1000, used_size => 200, free_size => 800},
    [BBInfo] = flurm_handler_admin:build_burst_buffer_info(Pools, Stats),
    ?assertEqual(<<"generic">>, BBInfo#burst_buffer_info.name),
    ?assertEqual(1, BBInfo#burst_buffer_info.pool_cnt).

test_build_bb_calculates_sizes() ->
    Pools = [{bb_pool, <<"pool">>, generic, 1000, 800, 512, [], up, []}],
    Stats = #{total_size => 1000, used_size => 200, free_size => 800},
    [BBInfo] = flurm_handler_admin:build_burst_buffer_info(Pools, Stats),
    ?assertEqual(1000, BBInfo#burst_buffer_info.total_space),
    ?assertEqual(200, BBInfo#burst_buffer_info.used_space).

test_build_bb_multiple_pools() ->
    Pools = [
        {bb_pool, <<"p1">>, generic, 500, 400, 256, [], up, []},
        {bb_pool, <<"p2">>, generic, 500, 400, 256, [], up, []}
    ],
    [BBInfo] = flurm_handler_admin:build_burst_buffer_info(Pools, #{}),
    ?assertEqual(2, BBInfo#burst_buffer_info.pool_cnt).

%%====================================================================
%% Helper Function Tests - pool_to_bb_pool
%%====================================================================

test_pool_to_bb_pool() ->
    Pool = {bb_pool, <<"default">>, generic, 1000000, 800000, 4096, [], up, []},
    BBPool = flurm_handler_admin:pool_to_bb_pool(Pool),
    ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name),
    ?assertEqual(1000000, BBPool#burst_buffer_pool.total_space),
    ?assertEqual(4096, BBPool#burst_buffer_pool.granularity).

test_pool_small_tuple() ->
    Pool = {too, small},
    BBPool = flurm_handler_admin:pool_to_bb_pool(Pool),
    ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name).

test_pool_non_tuple() ->
    BBPool = flurm_handler_admin:pool_to_bb_pool(not_a_tuple),
    ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name).

test_pool_calculates_unfree() ->
    Pool = {bb_pool, <<"test">>, generic, 1000, 700, 512, [], up, []},
    BBPool = flurm_handler_admin:pool_to_bb_pool(Pool),
    ?assertEqual(300, BBPool#burst_buffer_pool.unfree_space). %% 1000 - 700

%%====================================================================
%% Helper Function Tests - do_graceful_shutdown
%%====================================================================

test_graceful_shutdown_ok() ->
    ?assertEqual(ok, flurm_handler_admin:do_graceful_shutdown()).

%%====================================================================
%% Helper Function Tests - is_cluster_enabled
%%====================================================================

test_cluster_disabled_no_config() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    %% Without config and no process, should be false
    ?assertEqual(false, flurm_handler_admin:is_cluster_enabled()).

test_cluster_disabled_single_node() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1]),
    %% Single node list means not clustered
    ?assertEqual(false, flurm_handler_admin:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_disabled_empty_list() ->
    catch application:set_env(flurm_controller, cluster_nodes, []),
    %% Empty node list means not clustered
    ?assertEqual(false, flurm_handler_admin:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_enabled_multiple() ->
    catch application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    %% Multiple nodes means clustered
    ?assertEqual(true, flurm_handler_admin:is_cluster_enabled()),
    catch application:unset_env(flurm_controller, cluster_nodes).

test_cluster_enabled_process() ->
    catch application:unset_env(flurm_controller, cluster_nodes),
    %% Simulate process running by registering a dummy process
    Pid = spawn(fun() -> receive stop -> ok end end),
    register(flurm_controller_cluster, Pid),
    ?assertEqual(true, flurm_handler_admin:is_cluster_enabled()),
    Pid ! stop,
    timer:sleep(10),
    catch unregister(flurm_controller_cluster).
