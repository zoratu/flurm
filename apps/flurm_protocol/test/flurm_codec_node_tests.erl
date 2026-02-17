%%%-------------------------------------------------------------------
%%% @doc Comprehensive unit tests for flurm_codec_node module.
%%%
%%% Tests for all node-related message encoding and decoding functions.
%%% Coverage target: ~90% of flurm_codec_node.erl (270 lines)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_node_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_codec_node_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% decode_body/2 tests
      {"decode REQUEST_NODE_REGISTRATION_STATUS - with status_only true", fun decode_node_reg_status_only_true_test/0},
      {"decode REQUEST_NODE_REGISTRATION_STATUS - with status_only false", fun decode_node_reg_status_only_false_test/0},
      {"decode REQUEST_NODE_REGISTRATION_STATUS - empty", fun decode_node_reg_empty_test/0},
      {"decode REQUEST_NODE_REGISTRATION_STATUS - with trailing data", fun decode_node_reg_trailing_test/0},
      {"decode unsupported message type", fun decode_unsupported_test/0},

      %% encode_body/2 tests - REQUEST_NODE_REGISTRATION_STATUS
      {"encode REQUEST_NODE_REGISTRATION_STATUS - status_only true", fun encode_node_reg_status_only_true_test/0},
      {"encode REQUEST_NODE_REGISTRATION_STATUS - status_only false", fun encode_node_reg_status_only_false_test/0},
      {"encode REQUEST_NODE_REGISTRATION_STATUS - undefined", fun encode_node_reg_undefined_test/0},
      {"encode REQUEST_NODE_REGISTRATION_STATUS - default", fun encode_node_reg_default_test/0},

      %% encode_body/2 tests - REQUEST_NODE_INFO
      {"encode REQUEST_NODE_INFO - with record", fun encode_node_info_request_record_test/0},
      {"encode REQUEST_NODE_INFO - with empty node name", fun encode_node_info_request_empty_name_test/0},
      {"encode REQUEST_NODE_INFO - with specific node", fun encode_node_info_request_specific_node_test/0},
      {"encode REQUEST_NODE_INFO - default", fun encode_node_info_request_default_test/0},

      %% encode_body/2 tests - RESPONSE_NODE_INFO
      {"encode RESPONSE_NODE_INFO - empty", fun encode_node_info_response_empty_test/0},
      {"encode RESPONSE_NODE_INFO - with nodes", fun encode_node_info_response_with_nodes_test/0},
      {"encode RESPONSE_NODE_INFO - with multiple nodes", fun encode_node_info_response_multiple_nodes_test/0},
      {"encode RESPONSE_NODE_INFO - default", fun encode_node_info_response_default_test/0},
      {"encode RESPONSE_NODE_INFO - with full node details", fun encode_node_info_response_full_node_test/0},

      %% encode_body/2 tests - REQUEST_PARTITION_INFO
      {"encode REQUEST_PARTITION_INFO - with record", fun encode_partition_info_request_record_test/0},
      {"encode REQUEST_PARTITION_INFO - with partition name", fun encode_partition_info_request_with_name_test/0},
      {"encode REQUEST_PARTITION_INFO - default", fun encode_partition_info_request_default_test/0},

      %% encode_body/2 tests - RESPONSE_PARTITION_INFO
      {"encode RESPONSE_PARTITION_INFO - empty", fun encode_partition_info_response_empty_test/0},
      {"encode RESPONSE_PARTITION_INFO - with partitions", fun encode_partition_info_response_with_parts_test/0},
      {"encode RESPONSE_PARTITION_INFO - with multiple partitions", fun encode_partition_info_response_multiple_parts_test/0},
      {"encode RESPONSE_PARTITION_INFO - default", fun encode_partition_info_response_default_test/0},
      {"encode RESPONSE_PARTITION_INFO - with full details", fun encode_partition_info_response_full_test/0},

      %% encode_body/2 tests - Unsupported
      {"encode unsupported message type", fun encode_unsupported_test/0},

      %% encode_node_inx tests
      {"encode node_inx - empty", fun encode_node_inx_empty_test/0},
      {"encode node_inx - with ranges", fun encode_node_inx_with_ranges_test/0},
      {"encode node_inx - negative terminator", fun encode_node_inx_negative_test/0},

      %% Roundtrip tests
      {"roundtrip node registration request", fun roundtrip_node_reg_test/0},

      %% ensure_binary tests
      {"ensure_binary - undefined", fun ensure_binary_undefined_test/0},
      {"ensure_binary - null", fun ensure_binary_null_test/0},
      {"ensure_binary - binary", fun ensure_binary_binary_test/0},
      {"ensure_binary - list", fun ensure_binary_list_test/0},
      {"ensure_binary - other", fun ensure_binary_other_test/0},

      %% Edge case tests
      {"encode node info with undefined fields", fun encode_node_info_undefined_fields_test/0},
      {"encode partition info with undefined fields", fun encode_partition_info_undefined_fields_test/0},
      {"decode with malformed binary", fun decode_malformed_test/0},

      %% Boundary value tests
      {"decode with max values", fun decode_max_values_test/0},
      {"encode with empty strings", fun encode_empty_strings_test/0},
      {"encode with large strings", fun encode_large_strings_test/0}
     ]}.

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_NODE_REGISTRATION_STATUS
%%%===================================================================

decode_node_reg_status_only_true_test() ->
    Binary = <<1:32/big>>,
    {ok, Body} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertEqual(true, Body#node_registration_request.status_only).

decode_node_reg_status_only_false_test() ->
    Binary = <<0:32/big>>,
    {ok, Body} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertEqual(false, Body#node_registration_request.status_only).

decode_node_reg_empty_test() ->
    {ok, Body} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<>>),
    ?assertMatch(#node_registration_request{}, Body).

decode_node_reg_trailing_test() ->
    Binary = <<1:32/big, "extra data">>,
    {ok, Body} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertEqual(true, Body#node_registration_request.status_only).

%%%===================================================================
%%% decode_body/2 Tests - Unsupported
%%%===================================================================

decode_unsupported_test() ->
    Result = flurm_codec_node:decode_body(99999, <<"data">>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_NODE_REGISTRATION_STATUS
%%%===================================================================

encode_node_reg_status_only_true_test() ->
    Req = #node_registration_request{status_only = true},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual(<<1:32/big>>, Binary).

encode_node_reg_status_only_false_test() ->
    Req = #node_registration_request{status_only = false},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual(<<0:32/big>>, Binary).

encode_node_reg_undefined_test() ->
    Req = #node_registration_request{status_only = undefined},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual(<<0:32/big>>, Binary).

encode_node_reg_default_test() ->
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, #{}),
    ?assertEqual(<<0:32/big>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_NODE_INFO
%%%===================================================================

encode_node_info_request_record_test() ->
    Req = #node_info_request{show_flags = 0, node_name = <<>>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)),
    <<ShowFlags:32/big, _/binary>> = Binary,
    ?assertEqual(0, ShowFlags).

encode_node_info_request_empty_name_test() ->
    Req = #node_info_request{show_flags = 1, node_name = <<>>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)),
    <<ShowFlags:32/big, _/binary>> = Binary,
    ?assertEqual(1, ShowFlags).

encode_node_info_request_specific_node_test() ->
    Req = #node_info_request{show_flags = 0, node_name = <<"node001">>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 4).

encode_node_info_request_default_test() ->
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, #{}),
    ?assertEqual(<<0:32/big, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_NODE_INFO
%%%===================================================================

encode_node_info_response_empty_test() ->
    Resp = #node_info_response{
        last_update = 1700000000,
        node_count = 0,
        nodes = []
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_info_response_with_nodes_test() ->
    Node = #node_info{
        name = <<"node001">>,
        node_state = 2,  % IDLE
        cpus = 16,
        sockets = 2,
        cores = 4,
        threads = 2,
        real_memory = 65536,
        tmp_disk = 100000
    },
    Resp = #node_info_response{
        last_update = 1700000000,
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 12).

encode_node_info_response_multiple_nodes_test() ->
    Node1 = #node_info{name = <<"node001">>, node_state = 2, cpus = 16},
    Node2 = #node_info{name = <<"node002">>, node_state = 2, cpus = 32},
    Node3 = #node_info{name = <<"node003">>, node_state = 3, cpus = 64},
    Resp = #node_info_response{
        last_update = 1700000000,
        node_count = 3,
        nodes = [Node1, Node2, Node3]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_info_response_default_test() ->
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, #{}),
    ?assertEqual(<<0:64, 0:32>>, Binary).

encode_node_info_response_full_node_test() ->
    Node = #node_info{
        name = <<"node001">>,
        node_hostname = <<"node001.cluster.local">>,
        node_addr = <<"192.168.1.1">>,
        bcast_address = <<"192.168.1.255">>,
        port = 6818,
        node_state = 2,
        version = <<"22.05.9">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>,
        boards = 1,
        sockets = 2,
        cores = 8,
        threads = 2,
        cpus = 32,
        cpu_load = 100,
        free_mem = 32768,
        real_memory = 65536,
        tmp_disk = 100000,
        weight = 1,
        owner = 0,
        features = <<"intel,gpu">>,
        features_act = <<"intel">>,
        gres = <<"gpu:2">>,
        gres_drain = <<>>,
        gres_used = <<"gpu:1">>,
        partitions = <<"compute,gpu">>,
        reason = <<>>,
        reason_time = 0,
        reason_uid = 0,
        boot_time = 1699999000,
        last_busy = 1700000000,
        slurmd_start_time = 1699999000,
        alloc_cpus = 16,
        alloc_memory = 32768,
        tres_fmt_str = <<"cpu=32,mem=65536M">>,
        mcs_label = <<>>,
        cpu_spec_list = <<>>,
        core_spec_cnt = 0,
        mem_spec_limit = 0
    },
    Resp = #node_info_response{
        last_update = 1700000000,
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_PARTITION_INFO
%%%===================================================================

encode_partition_info_request_record_test() ->
    Req = #partition_info_request{show_flags = 0, partition_name = <<>>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_PARTITION_INFO, Req),
    ?assert(is_binary(Binary)),
    <<ShowFlags:32/big, _/binary>> = Binary,
    ?assertEqual(0, ShowFlags).

encode_partition_info_request_with_name_test() ->
    Req = #partition_info_request{show_flags = 1, partition_name = <<"compute">>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_PARTITION_INFO, Req),
    ?assert(is_binary(Binary)),
    <<ShowFlags:32/big, _/binary>> = Binary,
    ?assertEqual(1, ShowFlags).

encode_partition_info_request_default_test() ->
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_PARTITION_INFO, #{}),
    ?assertEqual(<<0:32/big, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_PARTITION_INFO
%%%===================================================================

encode_partition_info_response_empty_test() ->
    Resp = #partition_info_response{
        last_update = 1700000000,
        partition_count = 0,
        partitions = []
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_response_with_parts_test() ->
    Part = #partition_info{
        name = <<"compute">>,
        state_up = 1,
        total_nodes = 10,
        total_cpus = 160,
        max_time = 86400,
        nodes = <<"node[001-010]">>
    },
    Resp = #partition_info_response{
        last_update = 1700000000,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_response_multiple_parts_test() ->
    Part1 = #partition_info{name = <<"compute">>, state_up = 1, total_nodes = 10},
    Part2 = #partition_info{name = <<"gpu">>, state_up = 1, total_nodes = 4},
    Part3 = #partition_info{name = <<"debug">>, state_up = 1, total_nodes = 2},
    Resp = #partition_info_response{
        last_update = 1700000000,
        partition_count = 3,
        partitions = [Part1, Part2, Part3]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_response_default_test() ->
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, #{}),
    ?assertEqual(<<0:64, 0:32>>, Binary).

encode_partition_info_response_full_test() ->
    Part = #partition_info{
        name = <<"compute">>,
        allow_accounts = <<"all">>,
        allow_alloc_nodes = <<>>,
        allow_groups = <<>>,
        allow_qos = <<"normal,high">>,
        alternate = <<>>,
        billing_weights_str = <<>>,
        def_mem_per_cpu = 0,
        def_mem_per_node = 0,
        default_time = 3600,
        deny_accounts = <<>>,
        deny_qos = <<>>,
        flags = 0,
        grace_time = 0,
        max_cpus_per_node = 16#FFFFFFFF,
        max_mem_per_cpu = 0,
        max_mem_per_node = 0,
        max_nodes = 100,
        max_share = 1,
        max_time = 604800,
        min_nodes = 1,
        nodes = <<"node[001-100]">>,
        over_subscribe = 0,
        over_time_limit = 0,
        preempt_mode = 0,
        priority_job_factor = 1,
        priority_tier = 1,
        qos_char = <<"normal">>,
        state_up = 1,
        total_cpus = 1600,
        total_nodes = 100,
        tres_fmt_str = <<"cpu=1600,mem=6553600M">>,
        node_inx = [0, 99, -1]
    },
    Resp = #partition_info_response{
        last_update = 1700000000,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

%%%===================================================================
%%% encode_body/2 Tests - Unsupported
%%%===================================================================

encode_unsupported_test() ->
    Result = flurm_codec_node:encode_body(99999, #{}),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% encode_node_inx Tests
%%%===================================================================

encode_node_inx_empty_test() ->
    %% Test via partition_info which uses encode_node_inx
    Part = #partition_info{name = <<"test">>, node_inx = []},
    Resp = #partition_info_response{
        last_update = 1,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_inx_with_ranges_test() ->
    Part = #partition_info{name = <<"test">>, node_inx = [0, 9, 20, 29, -1]},
    Resp = #partition_info_response{
        last_update = 1,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_inx_negative_test() ->
    Part = #partition_info{name = <<"test">>, node_inx = [0, 4, -1]},
    Resp = #partition_info_response{
        last_update = 1,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_node_reg_test() ->
    %% Test status_only = true
    Req1 = #node_registration_request{status_only = true},
    {ok, Encoded1} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req1),
    {ok, Decoded1} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Encoded1),
    ?assertEqual(Req1#node_registration_request.status_only, Decoded1#node_registration_request.status_only),

    %% Test status_only = false
    Req2 = #node_registration_request{status_only = false},
    {ok, Encoded2} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req2),
    {ok, Decoded2} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Encoded2),
    ?assertEqual(Req2#node_registration_request.status_only, Decoded2#node_registration_request.status_only).

%%%===================================================================
%%% ensure_binary Tests
%%%===================================================================

ensure_binary_undefined_test() ->
    %% Test via node_info with undefined fields
    Node = #node_info{name = undefined},
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_null_test() ->
    %% Test via node_info with null fields
    Node = #node_info{name = null},
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_binary_test() ->
    %% Test via node_info with binary fields
    Node = #node_info{name = <<"node001">>},
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_list_test() ->
    %% Test via node_info with list fields
    Node = #node_info{name = "node001"},
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_other_test() ->
    %% Test via node_info with atom fields (other type)
    Node = #node_info{name = node001},
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

encode_node_info_undefined_fields_test() ->
    Node = #node_info{
        name = undefined,
        node_hostname = undefined,
        node_addr = undefined,
        bcast_address = undefined,
        version = undefined,
        arch = undefined,
        os = undefined,
        features = undefined,
        features_act = undefined,
        gres = undefined,
        gres_drain = undefined,
        gres_used = undefined,
        partitions = undefined,
        reason = undefined,
        tres_fmt_str = undefined,
        mcs_label = undefined,
        cpu_spec_list = undefined
    },
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_undefined_fields_test() ->
    Part = #partition_info{
        name = undefined,
        nodes = undefined,
        allow_accounts = undefined,
        allow_alloc_nodes = undefined,
        allow_groups = undefined,
        allow_qos = undefined,
        alternate = undefined,
        billing_weights_str = undefined,
        deny_accounts = undefined,
        deny_qos = undefined,
        qos_char = undefined,
        tres_fmt_str = undefined
    },
    Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

decode_malformed_test() ->
    %% Short binary
    Result1 = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<1, 2>>),
    ?assertMatch({ok, #node_registration_request{}}, Result1).

%%%===================================================================
%%% Boundary Value Tests
%%%===================================================================

decode_max_values_test() ->
    %% Max uint32 value for status_only (should still be treated as true)
    Binary = <<16#FFFFFFFF:32/big>>,
    {ok, Body} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertEqual(true, Body#node_registration_request.status_only).

encode_empty_strings_test() ->
    Node = #node_info{
        name = <<>>,
        node_hostname = <<>>,
        node_addr = <<>>,
        arch = <<>>,
        os = <<>>,
        features = <<>>,
        partitions = <<>>
    },
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_large_strings_test() ->
    LargeStr = binary:copy(<<"A">>, 1000),
    Node = #node_info{
        name = LargeStr,
        node_hostname = LargeStr,
        partitions = LargeStr
    },
    Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 3000).

%%%===================================================================
%%% Additional Coverage Tests
%%%===================================================================

%% Test encode_single_node_info with non-record
encode_single_node_info_invalid_test_() ->
    [
     {"invalid node info", fun() ->
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [invalid]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test encode_single_partition_info with non-record
encode_single_partition_info_invalid_test_() ->
    [
     {"invalid partition info", fun() ->
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [invalid]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test various node states
node_states_test_() ->
    [
     {"node state UNKNOWN", fun() ->
        Node = #node_info{name = <<"n1">>, node_state = ?NODE_STATE_UNKNOWN},
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"node state DOWN", fun() ->
        Node = #node_info{name = <<"n1">>, node_state = ?NODE_STATE_DOWN},
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"node state IDLE", fun() ->
        Node = #node_info{name = <<"n1">>, node_state = ?NODE_STATE_IDLE},
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"node state ALLOCATED", fun() ->
        Node = #node_info{name = <<"n1">>, node_state = ?NODE_STATE_ALLOCATED},
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"node state MIXED", fun() ->
        Node = #node_info{name = <<"n1">>, node_state = ?NODE_STATE_MIXED},
        Resp = #node_info_response{last_update = 1, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test partition state values
partition_states_test_() ->
    [
     {"partition state UP", fun() ->
        Part = #partition_info{name = <<"p1">>, state_up = 1},
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"partition state DOWN", fun() ->
        Part = #partition_info{name = <<"p1">>, state_up = 0},
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"partition state DRAIN", fun() ->
        Part = #partition_info{name = <<"p1">>, state_up = 2},
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test numeric field values
numeric_fields_test_() ->
    [
     {"zero values", fun() ->
        Node = #node_info{
            name = <<"n1">>,
            cpus = 0,
            sockets = 0,
            cores = 0,
            threads = 0,
            real_memory = 0,
            tmp_disk = 0,
            cpu_load = 0,
            free_mem = 0
        },
        Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"large values", fun() ->
        Node = #node_info{
            name = <<"n1">>,
            cpus = 65535,
            sockets = 8,
            cores = 64,
            threads = 128,
            real_memory = 16#FFFFFFFFFFFF,
            tmp_disk = 16#FFFFFFFF,
            cpu_load = 16#FFFFFFFF,
            free_mem = 16#FFFFFFFFFFFF
        },
        Resp = #node_info_response{last_update = 16#FFFFFFFFFFFF, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test with show_flags variations
show_flags_test_() ->
    [
     {"show_flags = 0", fun() ->
        Req = #node_info_request{show_flags = 0, node_name = <<>>},
        {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
        <<Flags:32/big, _/binary>> = Binary,
        ?assertEqual(0, Flags)
      end},
     {"show_flags = 1", fun() ->
        Req = #node_info_request{show_flags = 1, node_name = <<>>},
        {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
        <<Flags:32/big, _/binary>> = Binary,
        ?assertEqual(1, Flags)
      end},
     {"show_flags = 0xFFFF", fun() ->
        Req = #node_info_request{show_flags = 16#FFFF, node_name = <<>>},
        {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
        <<Flags:32/big, _/binary>> = Binary,
        ?assertEqual(16#FFFF, Flags)
      end}
    ].

%% Test priority fields in partitions
partition_priority_test_() ->
    [
     {"priority values", fun() ->
        Part = #partition_info{
            name = <<"p1">>,
            priority_job_factor = 100,
            priority_tier = 5
        },
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test time fields
time_fields_test_() ->
    [
     {"partition time limits", fun() ->
        Part = #partition_info{
            name = <<"p1">>,
            max_time = 604800,  % 1 week
            default_time = 3600  % 1 hour
        },
        Resp = #partition_info_response{last_update = 1, partition_count = 1, partitions = [Part]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"node boot times", fun() ->
        Node = #node_info{
            name = <<"n1">>,
            boot_time = 1700000000,
            last_busy = 1700001000,
            slurmd_start_time = 1700000500,
            reason_time = 0
        },
        Resp = #node_info_response{last_update = 1700002000, node_count = 1, nodes = [Node]},
        {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary))
      end}
    ].
