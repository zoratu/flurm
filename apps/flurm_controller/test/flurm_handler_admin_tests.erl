%%%-------------------------------------------------------------------
%%% @doc FLURM Admin Handler Tests
%%%
%%% Comprehensive tests for the flurm_handler_admin module.
%%% Tests all handle/2 clauses for administrative operations:
%%% - Ping requests
%%% - Shutdown requests
%%% - Reservation management (info, create, update, delete)
%%% - License info
%%% - Topology info
%%% - Front-end info
%%% - Burst buffer info
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_admin_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

handler_admin_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% REQUEST_PING (1008) tests
        {"Ping request", fun test_ping_request/0},
        {"Ping response structure", fun test_ping_response_structure/0},

        %% REQUEST_SHUTDOWN (1005) tests
        {"Shutdown - success", fun test_shutdown_success/0},
        {"Shutdown - cluster leader", fun test_shutdown_cluster_leader/0},
        {"Shutdown - cluster follower", fun test_shutdown_cluster_follower/0},
        {"Shutdown - failure", fun test_shutdown_failure/0},

        %% REQUEST_RESERVATION_INFO (2012) tests
        {"Reservation info - empty", fun test_reservation_info_empty/0},
        {"Reservation info - multiple", fun test_reservation_info_multiple/0},
        {"Reservation info - response structure", fun test_reservation_info_structure/0},

        %% REQUEST_CREATE_RESERVATION (2050) tests
        {"Create reservation - success", fun test_create_reservation_success/0},
        {"Create reservation - with name", fun test_create_reservation_with_name/0},
        {"Create reservation - with nodes", fun test_create_reservation_with_nodes/0},
        {"Create reservation - with users", fun test_create_reservation_with_users/0},
        {"Create reservation - failure", fun test_create_reservation_failure/0},
        {"Create reservation - cluster follower", fun test_create_reservation_cluster_follower/0},

        %% REQUEST_UPDATE_RESERVATION (2052) tests
        {"Update reservation - success", fun test_update_reservation_success/0},
        {"Update reservation - time change", fun test_update_reservation_time/0},
        {"Update reservation - nodes change", fun test_update_reservation_nodes/0},
        {"Update reservation - failure", fun test_update_reservation_failure/0},
        {"Update reservation - cluster follower", fun test_update_reservation_cluster_follower/0},

        %% REQUEST_DELETE_RESERVATION (2053) tests
        {"Delete reservation - success", fun test_delete_reservation_success/0},
        {"Delete reservation - not found", fun test_delete_reservation_not_found/0},
        {"Delete reservation - cluster follower", fun test_delete_reservation_cluster_follower/0},

        %% REQUEST_LICENSE_INFO (1017) tests
        {"License info - empty", fun test_license_info_empty/0},
        {"License info - multiple", fun test_license_info_multiple/0},
        {"License info - response structure", fun test_license_info_structure/0},

        %% REQUEST_TOPO_INFO (2018) tests
        {"Topology info - empty", fun test_topo_info_empty/0},

        %% REQUEST_FRONT_END_INFO (2028) tests
        {"Front end info - empty", fun test_front_end_info_empty/0},
        {"Front end info - response structure", fun test_front_end_info_structure/0},

        %% REQUEST_BURST_BUFFER_INFO (2020) tests
        {"Burst buffer info - empty", fun test_burst_buffer_info_empty/0},
        {"Burst buffer info - with pools", fun test_burst_buffer_info_with_pools/0},
        {"Burst buffer info - response structure", fun test_burst_buffer_info_structure/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Disable cluster mode
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Start meck for mocking
    meck:new([flurm_reservation, flurm_license, flurm_burst_buffer,
              flurm_controller_cluster, flurm_config_slurm],
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
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),

    ok.

cleanup(_) ->
    %% Unload all mocks
    catch meck:unload([flurm_reservation, flurm_license, flurm_burst_buffer,
                       flurm_controller_cluster, flurm_config_slurm]),
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
    %%                node_count, partition, features, users, accounts, flags, state, ...}
    {reservation, Name, maint, Now, Now + 3600, 3600, [<<"node1">>],
     1, <<"default">>, [], [<<"user1">>], [<<"acct1">>], [], active}.

%%====================================================================
%% Ping Tests (REQUEST_PING - 1008)
%%====================================================================

test_ping_request() ->
    Header = make_header(?REQUEST_PING),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_ping_response_structure() ->
    Header = make_header(?REQUEST_PING),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assert(is_record(Response, slurm_rc_response)).

%%====================================================================
%% Shutdown Tests (REQUEST_SHUTDOWN - 1005)
%%====================================================================

test_shutdown_success() ->
    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_shutdown_cluster_leader() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> true end),

    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(shutdown, _) -> {ok, ok} end),

    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

test_shutdown_failure() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(_, _) -> {error, no_leader} end),

    Header = make_header(?REQUEST_SHUTDOWN),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% Reservation Info Tests (REQUEST_RESERVATION_INFO - 2012)
%%====================================================================

test_reservation_info_empty() ->
    meck:expect(flurm_reservation, list, fun() -> [] end),

    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_RESERVATION_INFO, MsgType),
    ?assertEqual(0, Response#reservation_info_response.reservation_count).

test_reservation_info_multiple() ->
    Resvs = [make_reservation_tuple(<<"resv1">>), make_reservation_tuple(<<"resv2">>)],
    meck:expect(flurm_reservation, list, fun() -> Resvs end),

    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_RESERVATION_INFO, MsgType),
    ?assertEqual(2, Response#reservation_info_response.reservation_count).

test_reservation_info_structure() ->
    Resvs = [make_reservation_tuple(<<"test_resv">>)],
    meck:expect(flurm_reservation, list, fun() -> Resvs end),

    Header = make_header(?REQUEST_RESERVATION_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assert(Response#reservation_info_response.last_update > 0),
    [ResvInfo] = Response#reservation_info_response.reservations,
    ?assertEqual(<<"test_resv">>, ResvInfo#reservation_info.name),
    ?assert(ResvInfo#reservation_info.start_time > 0),
    ?assert(ResvInfo#reservation_info.end_time > 0).

%%====================================================================
%% Create Reservation Tests (REQUEST_CREATE_RESERVATION - 2050)
%%====================================================================

test_create_reservation_success() ->
    meck:expect(flurm_reservation, create, fun(_) -> {ok, <<"new_resv">>} end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"new_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<>>,
        node_cnt = 0,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_CREATE_RESERVATION, MsgType),
    ?assertEqual(<<"new_resv">>, Response#create_reservation_response.name),
    ?assertEqual(0, Response#create_reservation_response.error_code).

test_create_reservation_with_name() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual(<<"my_resv">>, maps:get(name, Spec)),
        {ok, <<"my_resv">>}
    end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"my_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<>>,
        node_cnt = 0,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(<<"my_resv">>, Response#create_reservation_response.name).

test_create_reservation_with_nodes() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[1-3]">>) ->
        [<<"node1">>, <<"node2">>, <<"node3">>]
    end),
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], maps:get(nodes, Spec)),
        {ok, <<"node_resv">>}
    end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"node_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<"node[1-3]">>,
        node_cnt = 3,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_reservation_with_users() ->
    meck:expect(flurm_reservation, create, fun(Spec) ->
        ?assertEqual([<<"user1">>, <<"user2">>], maps:get(users, Spec)),
        {ok, <<"user_resv">>}
    end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"user_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<>>,
        node_cnt = 0,
        users = <<"user1,user2">>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_create_reservation_failure() ->
    meck:expect(flurm_reservation, create, fun(_) -> {error, invalid_time_range} end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"bad_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        node_cnt = 0,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_CREATE_RESERVATION, MsgType),
    ?assertEqual(<<>>, Response#create_reservation_response.name),
    ?assertEqual(1, Response#create_reservation_response.error_code).

test_create_reservation_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(create_reservation, _) ->
        {ok, {ok, <<"forwarded_resv">>}}
    end),

    Header = make_header(?REQUEST_CREATE_RESERVATION),
    Request = #create_reservation_request{
        name = <<"cluster_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 60,
        nodes = <<>>,
        node_cnt = 0,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        features = <<>>,
        type = <<>>,
        flags = 0
    },
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(<<"forwarded_resv">>, Response#create_reservation_response.name),
    application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% Update Reservation Tests (REQUEST_UPDATE_RESERVATION - 2052)
%%====================================================================

test_update_reservation_success() ->
    meck:expect(flurm_reservation, update, fun(<<"resv1">>, _) -> ok end),

    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = #update_reservation_request{
        name = <<"resv1">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_reservation_time() ->
    Now = erlang:system_time(second),
    meck:expect(flurm_reservation, update, fun(<<"resv2">>, Updates) ->
        ?assertEqual(Now + 7200, maps:get(end_time, Updates)),
        ok
    end),

    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = #update_reservation_request{
        name = <<"resv2">>,
        start_time = 0,
        end_time = Now + 7200,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_update_reservation_nodes() ->
    meck:expect(flurm_config_slurm, expand_hostlist, fun(<<"node[5-8]">>) ->
        [<<"node5">>, <<"node6">>, <<"node7">>, <<"node8">>]
    end),
    meck:expect(flurm_reservation, update, fun(<<"resv3">>, Updates) ->
        ?assertEqual([<<"node5">>, <<"node6">>, <<"node7">>, <<"node8">>], maps:get(nodes, Updates)),
        ok
    end),

    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = #update_reservation_request{
        name = <<"resv3">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<"node[5-8]">>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    {ok, _, _} = flurm_handler_admin:handle(Header, Request).

test_update_reservation_failure() ->
    meck:expect(flurm_reservation, update, fun(_, _) -> {error, not_found} end),

    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = #update_reservation_request{
        name = <<"nonexistent">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_update_reservation_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_reservation, _) ->
        {ok, ok}
    end),

    Header = make_header(?REQUEST_UPDATE_RESERVATION),
    Request = #update_reservation_request{
        name = <<"cluster_resv">>,
        start_time = 0,
        end_time = 0,
        duration = 0,
        nodes = <<>>,
        users = <<>>,
        accounts = <<>>,
        partition = <<>>,
        flags = 0
    },
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% Delete Reservation Tests (REQUEST_DELETE_RESERVATION - 2053)
%%====================================================================

test_delete_reservation_success() ->
    meck:expect(flurm_reservation, delete, fun(<<"resv_to_delete">>) -> ok end),

    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"resv_to_delete">>},
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(0, Response#slurm_rc_response.return_code).

test_delete_reservation_not_found() ->
    meck:expect(flurm_reservation, delete, fun(_) -> {error, not_found} end),

    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"nonexistent">>},
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(?RESPONSE_SLURM_RC, MsgType),
    ?assertEqual(-1, Response#slurm_rc_response.return_code).

test_delete_reservation_cluster_follower() ->
    application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
    meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
    meck:expect(flurm_controller_cluster, forward_to_leader, fun(delete_reservation, <<"cluster_resv">>) ->
        {ok, ok}
    end),

    Header = make_header(?REQUEST_DELETE_RESERVATION),
    Request = #delete_reservation_request{name = <<"cluster_resv">>},
    {ok, _, Response} = flurm_handler_admin:handle(Header, Request),

    ?assertEqual(0, Response#slurm_rc_response.return_code),
    application:unset_env(flurm_controller, cluster_nodes).

%%====================================================================
%% License Info Tests (REQUEST_LICENSE_INFO - 1017)
%%====================================================================

test_license_info_empty() ->
    meck:expect(flurm_license, list, fun() -> [] end),

    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_LICENSE_INFO, MsgType),
    ?assertEqual(0, Response#license_info_response.license_count).

test_license_info_multiple() ->
    Licenses = [
        #{name => <<"matlab">>, total => 10, in_use => 5, available => 5, reserved => 0, remote => false},
        #{name => <<"gaussian">>, total => 5, in_use => 2, available => 3, reserved => 0, remote => false}
    ],
    meck:expect(flurm_license, list, fun() -> Licenses end),

    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_LICENSE_INFO, MsgType),
    ?assertEqual(2, Response#license_info_response.license_count).

test_license_info_structure() ->
    Licenses = [#{name => <<"test_lic">>, total => 100, in_use => 25, reserved => 10, remote => true}],
    meck:expect(flurm_license, list, fun() -> Licenses end),

    Header = make_header(?REQUEST_LICENSE_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assert(Response#license_info_response.last_update > 0),
    [LicInfo] = Response#license_info_response.licenses,
    ?assertEqual(<<"test_lic">>, LicInfo#license_info.name),
    ?assertEqual(100, LicInfo#license_info.total),
    ?assertEqual(25, LicInfo#license_info.in_use),
    ?assertEqual(75, LicInfo#license_info.available),
    ?assertEqual(10, LicInfo#license_info.reserved),
    ?assertEqual(1, LicInfo#license_info.remote).

%%====================================================================
%% Topology Info Tests (REQUEST_TOPO_INFO - 2018)
%%====================================================================

test_topo_info_empty() ->
    Header = make_header(?REQUEST_TOPO_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_TOPO_INFO, MsgType),
    ?assertEqual(0, Response#topo_info_response.topo_count),
    ?assertEqual([], Response#topo_info_response.topos).

%%====================================================================
%% Front End Info Tests (REQUEST_FRONT_END_INFO - 2028)
%%====================================================================

test_front_end_info_empty() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_FRONT_END_INFO, MsgType),
    ?assertEqual(0, Response#front_end_info_response.front_end_count).

test_front_end_info_structure() ->
    Header = make_header(?REQUEST_FRONT_END_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assert(Response#front_end_info_response.last_update > 0),
    ?assertEqual([], Response#front_end_info_response.front_ends).

%%====================================================================
%% Burst Buffer Info Tests (REQUEST_BURST_BUFFER_INFO - 2020)
%%====================================================================

test_burst_buffer_info_empty() ->
    meck:expect(flurm_burst_buffer, list_pools, fun() -> [] end),

    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_BURST_BUFFER_INFO, MsgType),
    ?assertEqual(0, Response#burst_buffer_info_response.burst_buffer_count).

test_burst_buffer_info_with_pools() ->
    %% Pool tuple format: {bb_pool, name, type, total_size, free_size, granularity, nodes, state, properties}
    Pools = [{bb_pool, <<"default">>, generic, 1000000000, 800000000, 1048576, [], up, []}],
    meck:expect(flurm_burst_buffer, list_pools, fun() -> Pools end),
    meck:expect(flurm_burst_buffer, get_stats, fun() ->
        #{total_size => 1000000000, used_size => 200000000, free_size => 800000000}
    end),

    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assertEqual(?RESPONSE_BURST_BUFFER_INFO, MsgType),
    ?assertEqual(1, Response#burst_buffer_info_response.burst_buffer_count).

test_burst_buffer_info_structure() ->
    Pools = [{bb_pool, <<"pool1">>, generic, 500000000, 400000000, 1048576, [], up, []}],
    meck:expect(flurm_burst_buffer, list_pools, fun() -> Pools end),
    meck:expect(flurm_burst_buffer, get_stats, fun() ->
        #{total_size => 500000000, used_size => 100000000, free_size => 400000000}
    end),

    Header = make_header(?REQUEST_BURST_BUFFER_INFO),
    {ok, _, Response} = flurm_handler_admin:handle(Header, <<>>),

    ?assert(Response#burst_buffer_info_response.last_update > 0),
    [BBInfo] = Response#burst_buffer_info_response.burst_buffers,
    ?assertEqual(<<"generic">>, BBInfo#burst_buffer_info.name),
    ?assertEqual(<<"default">>, BBInfo#burst_buffer_info.default_pool),
    ?assertEqual(500000000, BBInfo#burst_buffer_info.total_space),
    ?assertEqual(100000000, BBInfo#burst_buffer_info.used_space).

%%====================================================================
%% Helper Function Tests
%%====================================================================

reservation_to_reservation_info_test_() ->
    [
        {"Convert reservation tuple to info", fun() ->
            Resv = make_reservation_tuple(<<"test_resv">>),
            ResvInfo = flurm_handler_admin:reservation_to_reservation_info(Resv),
            ?assertEqual(<<"test_resv">>, ResvInfo#reservation_info.name),
            ?assert(ResvInfo#reservation_info.start_time > 0),
            ?assert(ResvInfo#reservation_info.end_time > 0),
            ?assertEqual(1, ResvInfo#reservation_info.node_cnt)
        end},
        {"Convert empty reservation", fun() ->
            Resv = {small_tuple, only, two},
            ResvInfo = flurm_handler_admin:reservation_to_reservation_info(Resv),
            ?assertEqual(<<>>, ResvInfo#reservation_info.name)
        end}
    ].

reservation_state_to_flags_test_() ->
    [
        {"active -> 1", fun() ->
            ?assertEqual(1, flurm_handler_admin:reservation_state_to_flags(active))
        end},
        {"inactive -> 0", fun() ->
            ?assertEqual(0, flurm_handler_admin:reservation_state_to_flags(inactive))
        end},
        {"expired -> 2", fun() ->
            ?assertEqual(2, flurm_handler_admin:reservation_state_to_flags(expired))
        end},
        {"unknown -> 0", fun() ->
            ?assertEqual(0, flurm_handler_admin:reservation_state_to_flags(something_else))
        end}
    ].

determine_reservation_type_test_() ->
    [
        {"maint type", fun() ->
            ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<"maint">>, 0))
        end},
        {"MAINT type", fun() ->
            ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<"MAINT">>, 0))
        end},
        {"flex type", fun() ->
            ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<"flex">>, 0))
        end},
        {"user type", fun() ->
            ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"user">>, 0))
        end},
        {"empty with maint flag", fun() ->
            ?assertEqual(maint, flurm_handler_admin:determine_reservation_type(<<>>, 16#0001))
        end},
        {"empty with flex flag", fun() ->
            ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<>>, 16#8000))
        end},
        {"empty with no flags", fun() ->
            ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<>>, 0))
        end}
    ].

parse_reservation_flags_test_() ->
    [
        {"No flags", fun() ->
            ?assertEqual([], flurm_handler_admin:parse_reservation_flags(0))
        end},
        {"Maint flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0001),
            ?assert(lists:member(maint, Flags))
        end},
        {"Flex flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#8000),
            ?assert(lists:member(flex, Flags))
        end},
        {"Multiple flags", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0005),  % maint + daily
            ?assert(lists:member(maint, Flags)),
            ?assert(lists:member(daily, Flags))
        end}
    ].

generate_reservation_name_test_() ->
    [
        {"Generates unique name", fun() ->
            Name1 = flurm_handler_admin:generate_reservation_name(),
            Name2 = flurm_handler_admin:generate_reservation_name(),
            ?assert(is_binary(Name1)),
            ?assertNotEqual(Name1, Name2)
        end},
        {"Name starts with resv_", fun() ->
            Name = flurm_handler_admin:generate_reservation_name(),
            ?assert(binary:match(Name, <<"resv_">>) =/= nomatch)
        end}
    ].

license_to_license_info_test_() ->
    [
        {"Convert map license to info", fun() ->
            Lic = #{name => <<"matlab">>, total => 10, in_use => 3, available => 7, reserved => 0, remote => false},
            LicInfo = flurm_handler_admin:license_to_license_info(Lic),
            ?assertEqual(<<"matlab">>, LicInfo#license_info.name),
            ?assertEqual(10, LicInfo#license_info.total),
            ?assertEqual(3, LicInfo#license_info.in_use),
            ?assertEqual(7, LicInfo#license_info.available),
            ?assertEqual(0, LicInfo#license_info.remote)
        end},
        {"Convert map license with remote true", fun() ->
            Lic = #{name => <<"remote_lic">>, total => 5, in_use => 0, remote => true},
            LicInfo = flurm_handler_admin:license_to_license_info(Lic),
            ?assertEqual(1, LicInfo#license_info.remote)
        end},
        {"Convert tuple license to info", fun() ->
            Lic = {license, <<"gaussian">>, 20, 5, 15},
            LicInfo = flurm_handler_admin:license_to_license_info(Lic),
            ?assertEqual(<<"gaussian">>, LicInfo#license_info.name),
            ?assertEqual(20, LicInfo#license_info.total),
            ?assertEqual(5, LicInfo#license_info.in_use)
        end},
        {"Convert invalid input returns empty record", fun() ->
            LicInfo = flurm_handler_admin:license_to_license_info(invalid),
            ?assert(is_record(LicInfo, license_info))
        end}
    ].

pool_to_bb_pool_test_() ->
    [
        {"Convert pool tuple to bb_pool", fun() ->
            Pool = {bb_pool, <<"default">>, generic, 1000000, 800000, 4096, [], up, []},
            BBPool = flurm_handler_admin:pool_to_bb_pool(Pool),
            ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name),
            ?assertEqual(1000000, BBPool#burst_buffer_pool.total_space),
            ?assertEqual(4096, BBPool#burst_buffer_pool.granularity),
            ?assertEqual(200000, BBPool#burst_buffer_pool.unfree_space)
        end},
        {"Convert small tuple returns default", fun() ->
            Pool = {too, small},
            BBPool = flurm_handler_admin:pool_to_bb_pool(Pool),
            ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name)
        end},
        {"Convert non-tuple returns default", fun() ->
            BBPool = flurm_handler_admin:pool_to_bb_pool(not_a_tuple),
            ?assertEqual(<<"default">>, BBPool#burst_buffer_pool.name)
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
            ?assertEqual(false, flurm_handler_admin:is_cluster_enabled())
        end},
        {"Cluster disabled with single node", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1]),
            ?assertEqual(false, flurm_handler_admin:is_cluster_enabled())
        end},
        {"Cluster enabled with multiple nodes", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            ?assertEqual(true, flurm_handler_admin:is_cluster_enabled())
        end}
     ]}.

%%====================================================================
%% Create Reservation Request Conversion Tests
%%====================================================================

create_reservation_request_to_spec_test_() ->
    [
        {"Basic request conversion", fun() ->
            Now = erlang:system_time(second),
            Request = #create_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<"default">>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual(<<"test">>, maps:get(name, Spec)),
            ?assertEqual(<<"default">>, maps:get(partition, Spec)),
            ?assert(maps:get(start_time, Spec) >= Now),
            ?assert(maps:get(end_time, Spec) > maps:get(start_time, Spec))
        end},
        {"Request with specific start and end time", fun() ->
            Now = erlang:system_time(second),
            Request = #create_reservation_request{
                name = <<"timed">>,
                start_time = Now + 3600,
                end_time = Now + 7200,
                duration = 0,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual(Now + 3600, maps:get(start_time, Spec)),
            ?assertEqual(Now + 7200, maps:get(end_time, Spec))
        end},
        {"Request with users and accounts", fun() ->
            Request = #create_reservation_request{
                name = <<"multi">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<"user1,user2,user3">>,
                accounts = <<"acct1,acct2">>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual([<<"user1">>, <<"user2">>, <<"user3">>], maps:get(users, Spec)),
            ?assertEqual([<<"acct1">>, <<"acct2">>], maps:get(accounts, Spec))
        end}
    ].

%%====================================================================
%% Update Reservation Request Conversion Tests
%%====================================================================

update_reservation_request_to_updates_test_() ->
    [
        {"Empty request returns empty map", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual(#{}, Updates)
        end},
        {"Request with time changes", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 1000,
                end_time = 2000,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual(1000, maps:get(start_time, Updates)),
            ?assertEqual(2000, maps:get(end_time, Updates))
        end},
        {"Request with duration", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 120,  % 120 minutes
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual(7200, maps:get(duration, Updates))  % 120 * 60 seconds
        end}
    ].

%%====================================================================
%% Build Burst Buffer Info Tests
%%====================================================================

build_burst_buffer_info_test_() ->
    [
        {"Empty pools returns empty list", fun() ->
            Result = flurm_handler_admin:build_burst_buffer_info([], #{}),
            ?assertEqual([], Result)
        end},
        {"With pools and stats", fun() ->
            Pools = [{bb_pool, <<"pool1">>, generic, 1000, 800, 512, [], up, []}],
            Stats = #{total_size => 1000, used_size => 200, free_size => 800},
            [BBInfo] = flurm_handler_admin:build_burst_buffer_info(Pools, Stats),
            ?assertEqual(1000, BBInfo#burst_buffer_info.total_space),
            ?assertEqual(200, BBInfo#burst_buffer_info.used_space),
            ?assertEqual(1, BBInfo#burst_buffer_info.pool_cnt)
        end}
    ].

%%====================================================================
%% Extract Reservation Fields Tests
%%====================================================================

extract_reservation_fields_test_() ->
    [
        {"Extract fields from valid tuple", fun() ->
            Resv = make_reservation_tuple(<<"test">>),
            {Name, StartTime, EndTime, Nodes, Users, State, _Flags} =
                flurm_handler_admin:extract_reservation_fields(Resv),
            ?assertEqual(<<"test">>, Name),
            ?assert(is_integer(StartTime)),
            ?assert(is_integer(EndTime)),
            ?assertEqual([<<"node1">>], Nodes),
            ?assertEqual([<<"user1">>], Users),
            ?assertEqual(active, State)
        end},
        {"Extract fields from small tuple returns defaults", fun() ->
            {Name, StartTime, EndTime, Nodes, Users, State, _Flags} =
                flurm_handler_admin:extract_reservation_fields({too, small}),
            ?assertEqual(<<>>, Name),
            ?assertEqual(0, StartTime),
            ?assertEqual(0, EndTime),
            ?assertEqual([], Nodes),
            ?assertEqual([], Users),
            ?assertEqual(inactive, State)
        end},
        {"Extract fields from non-tuple returns defaults", fun() ->
            {Name, _StartTime, _EndTime, _Nodes, _Users, _State, _Flags} =
                flurm_handler_admin:extract_reservation_fields(not_a_tuple),
            ?assertEqual(<<>>, Name)
        end}
    ].

%%====================================================================
%% Graceful Shutdown Tests
%%====================================================================

do_graceful_shutdown_test_() ->
    [
        {"Graceful shutdown returns ok", fun() ->
            ?assertEqual(ok, flurm_handler_admin:do_graceful_shutdown())
        end}
    ].

%%====================================================================
%% Cluster Forwarding Extended Tests
%%====================================================================

cluster_forwarding_extended_test_() ->
    {setup,
     fun() ->
         meck:new([flurm_controller_cluster, flurm_reservation], [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload([flurm_controller_cluster, flurm_reservation]),
         application:unset_env(flurm_controller, cluster_nodes),
         ok
     end,
     [
        {"Create reservation cluster forward no_leader error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(create_reservation, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_CREATE_RESERVATION),
            Request = #create_reservation_request{
                name = <<"err_resv">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
            ?assertEqual(1, Response#create_reservation_response.error_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Create reservation cluster forward generic error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(create_reservation, _) ->
                {error, timeout}
            end),
            Header = make_header(?REQUEST_CREATE_RESERVATION),
            Request = #create_reservation_request{
                name = <<"timeout_resv">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
            ?assertEqual(1, Response#create_reservation_response.error_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Update reservation cluster forward no_leader error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(update_reservation, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_UPDATE_RESERVATION),
            Request = #update_reservation_request{
                name = <<"update_err">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end},
        {"Delete reservation cluster forward no_leader error", fun() ->
            application:set_env(flurm_controller, cluster_nodes, [node1, node2]),
            meck:expect(flurm_controller_cluster, is_leader, fun() -> false end),
            meck:expect(flurm_controller_cluster, forward_to_leader, fun(delete_reservation, _) ->
                {error, no_leader}
            end),
            Header = make_header(?REQUEST_DELETE_RESERVATION),
            Request = #delete_reservation_request{name = <<"del_err">>},
            {ok, _, Response} = flurm_handler_admin:handle(Header, Request),
            ?assertEqual(-1, Response#slurm_rc_response.return_code),
            application:unset_env(flurm_controller, cluster_nodes)
        end}
     ]}.

%%====================================================================
%% Reservation Info Exception Handling Tests
%%====================================================================

reservation_info_exception_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_reservation, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_reservation),
         ok
     end,
     [
        {"Reservation list exception returns empty list", fun() ->
            meck:expect(flurm_reservation, list, fun() -> error(crash) end),
            Header = make_header(?REQUEST_RESERVATION_INFO),
            {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_RESERVATION_INFO, MsgType),
            ?assertEqual(0, Response#reservation_info_response.reservation_count)
        end}
     ]}.

%%====================================================================
%% License Info Exception Handling Tests
%%====================================================================

license_info_exception_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_license, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_license),
         ok
     end,
     [
        {"License list exception returns empty list", fun() ->
            meck:expect(flurm_license, list, fun() -> error(crash) end),
            Header = make_header(?REQUEST_LICENSE_INFO),
            {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_LICENSE_INFO, MsgType),
            ?assertEqual(0, Response#license_info_response.license_count)
        end}
     ]}.

%%====================================================================
%% Burst Buffer Exception Handling Tests
%%====================================================================

burst_buffer_exception_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_burst_buffer, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_burst_buffer),
         ok
     end,
     [
        {"Burst buffer list_pools exception returns empty list", fun() ->
            meck:expect(flurm_burst_buffer, list_pools, fun() -> error(crash) end),
            meck:expect(flurm_burst_buffer, get_stats, fun() -> #{} end),
            Header = make_header(?REQUEST_BURST_BUFFER_INFO),
            {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_BURST_BUFFER_INFO, MsgType),
            ?assertEqual(0, Response#burst_buffer_info_response.burst_buffer_count)
        end},
        {"Burst buffer get_stats exception uses empty map", fun() ->
            meck:expect(flurm_burst_buffer, list_pools, fun() ->
                [{bb_pool, <<"test">>, generic, 1000, 500, 512, [], up, []}]
            end),
            meck:expect(flurm_burst_buffer, get_stats, fun() -> error(crash) end),
            Header = make_header(?REQUEST_BURST_BUFFER_INFO),
            {ok, MsgType, Response} = flurm_handler_admin:handle(Header, <<>>),
            ?assertEqual(?RESPONSE_BURST_BUFFER_INFO, MsgType),
            ?assertEqual(1, Response#burst_buffer_info_response.burst_buffer_count)
        end}
     ]}.

%%====================================================================
%% Determine Reservation Type Extended Tests
%%====================================================================

determine_reservation_type_extended_test_() ->
    [
        {"maintenance type lowercase", fun() ->
            ?assertEqual(maintenance, flurm_handler_admin:determine_reservation_type(<<"maintenance">>, 0))
        end},
        {"FLEX type uppercase", fun() ->
            ?assertEqual(flex, flurm_handler_admin:determine_reservation_type(<<"FLEX">>, 0))
        end},
        {"USER type uppercase", fun() ->
            ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"USER">>, 0))
        end},
        {"Unknown type string defaults to user", fun() ->
            ?assertEqual(user, flurm_handler_admin:determine_reservation_type(<<"unknown_type">>, 0))
        end}
    ].

%%====================================================================
%% Create Reservation Spec Extended Tests
%%====================================================================

create_reservation_spec_extended_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_config_slurm, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_config_slurm),
         ok
     end,
     [
        {"Spec with empty name generates name", fun() ->
            Request = #create_reservation_request{
                name = <<>>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            Name = maps:get(name, Spec),
            ?assertNotEqual(<<>>, Name),
            ?assert(binary:match(Name, <<"resv_">>) =/= nomatch)
        end},
        {"Spec with node_cnt uses explicit count", fun() ->
            meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> [<<"n1">>, <<"n2">>] end),
            Request = #create_reservation_request{
                name = <<"cnt_resv">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<"nodes">>,
                node_cnt = 5,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual(5, maps:get(node_count, Spec))
        end},
        {"Spec with features parses comma-separated list", fun() ->
            Request = #create_reservation_request{
                name = <<"feat_resv">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<>>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<"gpu,fast,large">>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual([<<"gpu">>, <<"fast">>, <<"large">>], maps:get(features, Spec))
        end},
        {"Spec with expand_hostlist exception falls back to split", fun() ->
            meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> error(invalid_pattern) end),
            Request = #create_reservation_request{
                name = <<"fallback_resv">>,
                start_time = 0,
                end_time = 0,
                duration = 60,
                nodes = <<"node1,node2">>,
                node_cnt = 0,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                features = <<>>,
                type = <<>>,
                flags = 0
            },
            Spec = flurm_handler_admin:create_reservation_request_to_spec(Request),
            ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Spec))
        end}
     ]}.

%%====================================================================
%% Update Reservation Updates Extended Tests
%%====================================================================

update_reservation_updates_extended_test_() ->
    {setup,
     fun() ->
         meck:new(flurm_config_slurm, [passthrough, no_link, non_strict]),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_config_slurm),
         ok
     end,
     [
        {"Updates with users list", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<"user1,user2">>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual([<<"user1">>, <<"user2">>], maps:get(users, Updates))
        end},
        {"Updates with accounts list", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<"acct1,acct2">>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual([<<"acct1">>, <<"acct2">>], maps:get(accounts, Updates))
        end},
        {"Updates with partition", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<"gpu">>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual(<<"gpu">>, maps:get(partition, Updates))
        end},
        {"Updates with flags", fun() ->
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<>>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 16#0001
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assert(lists:member(maint, maps:get(flags, Updates)))
        end},
        {"Updates with expand_hostlist exception falls back to split", fun() ->
            meck:expect(flurm_config_slurm, expand_hostlist, fun(_) -> error(invalid_pattern) end),
            Request = #update_reservation_request{
                name = <<"test">>,
                start_time = 0,
                end_time = 0,
                duration = 0,
                nodes = <<"node1,node2">>,
                users = <<>>,
                accounts = <<>>,
                partition = <<>>,
                flags = 0
            },
            Updates = flurm_handler_admin:update_reservation_request_to_updates(Request),
            ?assertEqual([<<"node1">>, <<"node2">>], maps:get(nodes, Updates))
        end}
     ]}.

%%====================================================================
%% Parse Reservation Flags Extended Tests
%%====================================================================

parse_reservation_flags_extended_test_() ->
    [
        {"ignore_jobs flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0002),
            ?assert(lists:member(ignore_jobs, Flags))
        end},
        {"weekly flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0008),
            ?assert(lists:member(weekly, Flags))
        end},
        {"weekday flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0010),
            ?assert(lists:member(weekday, Flags))
        end},
        {"weekend flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0020),
            ?assert(lists:member(weekend, Flags))
        end},
        {"any flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0040),
            ?assert(lists:member(any, Flags))
        end},
        {"first_cores flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0080),
            ?assert(lists:member(first_cores, Flags))
        end},
        {"time_float flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0100),
            ?assert(lists:member(time_float, Flags))
        end},
        {"overlap flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#0800),
            ?assert(lists:member(overlap, Flags))
        end},
        {"static_alloc flag", fun() ->
            Flags = flurm_handler_admin:parse_reservation_flags(16#2000),
            ?assert(lists:member(static_alloc, Flags))
        end}
    ].

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
            ?assertEqual(true, flurm_handler_admin:is_cluster_enabled()),
            Pid ! stop,
            unregister(flurm_controller_cluster)
        end},
        {"Cluster disabled with empty nodes list", fun() ->
            application:set_env(flurm_controller, cluster_nodes, []),
            ?assertEqual(false, flurm_handler_admin:is_cluster_enabled())
        end}
     ]}.
