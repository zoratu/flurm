%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_codec_node module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_codec_node_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_codec_node_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode tests
      {"decode_body REQUEST_NODE_REGISTRATION_STATUS with status_only true",
       fun decode_node_registration_status_only_true/0},
      {"decode_body REQUEST_NODE_REGISTRATION_STATUS with status_only false",
       fun decode_node_registration_status_only_false/0},
      {"decode_body REQUEST_NODE_REGISTRATION_STATUS empty binary",
       fun decode_node_registration_empty/0},
      {"decode_body REQUEST_NODE_REGISTRATION_STATUS short binary",
       fun decode_node_registration_short/0},
      {"decode_body unsupported message type",
       fun decode_unsupported_msg_type/0},

      %% Encode tests - REQUEST_NODE_REGISTRATION_STATUS
      {"encode_body REQUEST_NODE_REGISTRATION_STATUS status_only true",
       fun encode_node_registration_status_true/0},
      {"encode_body REQUEST_NODE_REGISTRATION_STATUS status_only false",
       fun encode_node_registration_status_false/0},
      {"encode_body REQUEST_NODE_REGISTRATION_STATUS non-boolean status_only",
       fun encode_node_registration_status_other/0},
      {"encode_body REQUEST_NODE_REGISTRATION_STATUS non-record",
       fun encode_node_registration_non_record/0},

      %% Encode tests - REQUEST_NODE_INFO
      {"encode_body REQUEST_NODE_INFO with record",
       fun encode_node_info_request_record/0},
      {"encode_body REQUEST_NODE_INFO with empty node name",
       fun encode_node_info_request_empty_name/0},
      {"encode_body REQUEST_NODE_INFO non-record",
       fun encode_node_info_request_non_record/0},

      %% Encode tests - RESPONSE_NODE_INFO
      {"encode_body RESPONSE_NODE_INFO with nodes",
       fun encode_node_info_response_with_nodes/0},
      {"encode_body RESPONSE_NODE_INFO empty nodes",
       fun encode_node_info_response_empty_nodes/0},
      {"encode_body RESPONSE_NODE_INFO non-record",
       fun encode_node_info_response_non_record/0},
      {"encode single node info record",
       fun encode_single_node_info_test/0},
      {"encode single node info non-record",
       fun encode_single_node_info_non_record/0},

      %% Encode tests - REQUEST_PARTITION_INFO
      {"encode_body REQUEST_PARTITION_INFO with record",
       fun encode_partition_info_request_record/0},
      {"encode_body REQUEST_PARTITION_INFO non-record",
       fun encode_partition_info_request_non_record/0},

      %% Encode tests - RESPONSE_PARTITION_INFO
      {"encode_body RESPONSE_PARTITION_INFO with partitions",
       fun encode_partition_info_response_with_partitions/0},
      {"encode_body RESPONSE_PARTITION_INFO empty partitions",
       fun encode_partition_info_response_empty_partitions/0},
      {"encode_body RESPONSE_PARTITION_INFO non-record",
       fun encode_partition_info_response_non_record/0},
      {"encode single partition info record",
       fun encode_single_partition_info_test/0},
      {"encode single partition info non-record",
       fun encode_single_partition_info_non_record/0},

      %% Encode node_inx tests
      {"encode_node_inx empty list",
       fun encode_node_inx_empty/0},
      {"encode_node_inx with ranges",
       fun encode_node_inx_with_ranges/0},

      %% Unsupported message type
      {"encode_body unsupported message type",
       fun encode_unsupported_msg_type/0},

      %% ensure_binary tests
      {"ensure_binary undefined",
       fun ensure_binary_undefined/0},
      {"ensure_binary null",
       fun ensure_binary_null/0},
      {"ensure_binary binary",
       fun ensure_binary_binary/0},
      {"ensure_binary list",
       fun ensure_binary_list/0},
      {"ensure_binary other",
       fun ensure_binary_other/0},

      %% Edge cases
      {"decode with extra trailing data",
       fun decode_with_extra_data/0},
      {"encode with all node info fields populated",
       fun encode_full_node_info/0},
      {"encode with all partition info fields populated",
       fun encode_full_partition_info/0},

      %% Roundtrip tests
      {"roundtrip node registration request",
       fun roundtrip_node_registration/0},

      %% Additional coverage tests
      {"encode multiple nodes in response",
       fun encode_multiple_nodes/0},
      {"encode multiple partitions in response",
       fun encode_multiple_partitions/0},
      {"encode node info with string list values",
       fun encode_node_info_string_values/0},
      {"encode partition info with node_inx ranges",
       fun encode_partition_with_node_inx/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Decode Tests
%%%===================================================================

decode_node_registration_status_only_true() ->
    %% StatusOnly = 1 (non-zero)
    Binary = <<1:32/big, 0, 0, 0, 0>>,
    Result = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertMatch({ok, #node_registration_request{status_only = true}}, Result).

decode_node_registration_status_only_false() ->
    %% StatusOnly = 0
    Binary = <<0:32/big, 0, 0, 0, 0>>,
    Result = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertMatch({ok, #node_registration_request{status_only = false}}, Result).

decode_node_registration_empty() ->
    Result = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<>>),
    ?assertMatch({ok, #node_registration_request{}}, Result).

decode_node_registration_short() ->
    %% Less than 4 bytes - should return default
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertMatch({ok, #node_registration_request{}}, Result).

decode_unsupported_msg_type() ->
    Result = flurm_codec_node:decode_body(99999, <<1, 2, 3>>),
    ?assertEqual(unsupported, Result).

decode_with_extra_data() ->
    %% Extra trailing data should be ignored
    Binary = <<1:32/big, "extra", "data", "here">>,
    Result = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary),
    ?assertMatch({ok, #node_registration_request{status_only = true}}, Result).

%%%===================================================================
%%% Encode Node Registration Tests
%%%===================================================================

encode_node_registration_status_true() ->
    Req = #node_registration_request{status_only = true},
    Result = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual({ok, <<1:32/big>>}, Result).

encode_node_registration_status_false() ->
    Req = #node_registration_request{status_only = false},
    Result = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual({ok, <<0:32/big>>}, Result).

encode_node_registration_status_other() ->
    %% Non-boolean value treated as false
    Req = #node_registration_request{status_only = undefined},
    Result = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    ?assertEqual({ok, <<0:32/big>>}, Result).

encode_node_registration_non_record() ->
    Result = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, not_a_record),
    ?assertEqual({ok, <<0:32/big>>}, Result).

%%%===================================================================
%%% Encode Node Info Request Tests
%%%===================================================================

encode_node_info_request_record() ->
    Req = #node_info_request{show_flags = 123, node_name = <<"node1">>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)),
    %% Check show_flags is at the beginning
    <<ShowFlags:32/big, _Rest/binary>> = Binary,
    ?assertEqual(123, ShowFlags).

encode_node_info_request_empty_name() ->
    Req = #node_info_request{show_flags = 0, node_name = <<>>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)).

encode_node_info_request_non_record() ->
    Result = flurm_codec_node:encode_body(?REQUEST_NODE_INFO, not_a_record),
    ?assertEqual({ok, <<0:32/big, 0:32>>}, Result).

%%%===================================================================
%%% Encode Node Info Response Tests
%%%===================================================================

encode_node_info_response_with_nodes() ->
    Node = #node_info{
        name = <<"node1">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>,
        cpus = 8,
        sockets = 1,
        cores = 4,
        threads = 2,
        real_memory = 16384,
        node_state = ?NODE_STATE_IDLE
    },
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 0).

encode_node_info_response_empty_nodes() ->
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 0,
        nodes = []
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_info_response_non_record() ->
    Result = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, not_a_record),
    ?assertEqual({ok, <<0:64, 0:32>>}, Result).

encode_single_node_info_test() ->
    Node = #node_info{
        name = <<"node1">>,
        node_hostname = <<"node1.local">>,
        node_addr = <<"192.168.1.1">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>,
        cpus = 8,
        sockets = 1,
        cores = 4,
        threads = 2,
        real_memory = 16384,
        node_state = ?NODE_STATE_IDLE,
        version = <<"22.05.0">>,
        features = <<"feature1,feature2">>,
        gres = <<"gpu:1">>
    },
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100). % Should be substantial

encode_single_node_info_non_record() ->
    %% Create a response with non-record node
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 1,
        nodes = [not_a_record]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_full_node_info() ->
    %% Test with all fields populated
    Node = #node_info{
        name = <<"compute01">>,
        node_hostname = <<"compute01.cluster.local">>,
        node_addr = <<"10.0.0.1">>,
        bcast_address = <<"10.0.0.255">>,
        port = 6818,
        node_state = ?NODE_STATE_ALLOCATED,
        version = <<"22.05.9">>,
        arch = <<"x86_64">>,
        os = <<"Linux 5.4.0">>,
        boards = 1,
        sockets = 2,
        cores = 8,
        threads = 2,
        cpus = 32,
        cpu_load = 50,
        free_mem = 8192,
        real_memory = 65536,
        tmp_disk = 100000,
        weight = 1,
        owner = 0,
        features = <<"gpu,fast">>,
        features_act = <<"gpu">>,
        gres = <<"gpu:tesla:4">>,
        gres_drain = <<>>,
        gres_used = <<"gpu:tesla:2">>,
        partitions = <<"default,gpu">>,
        reason = <<>>,
        reason_time = 0,
        reason_uid = 0,
        boot_time = 1234567890,
        last_busy = 1234567900,
        slurmd_start_time = 1234567895,
        alloc_cpus = 16,
        alloc_memory = 32768,
        tres_fmt_str = <<"cpu=32,mem=65536M">>,
        mcs_label = <<>>,
        cpu_spec_list = <<>>,
        core_spec_cnt = 0,
        mem_spec_limit = 0
    },
    Resp = #node_info_response{
        last_update = erlang:system_time(second),
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 200).

encode_multiple_nodes() ->
    Nodes = [
        #node_info{name = <<"node1">>, cpus = 8, node_state = ?NODE_STATE_IDLE},
        #node_info{name = <<"node2">>, cpus = 16, node_state = ?NODE_STATE_ALLOCATED},
        #node_info{name = <<"node3">>, cpus = 32, node_state = ?NODE_STATE_MIXED}
    ],
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 3,
        nodes = Nodes
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_info_string_values() ->
    %% Test with list strings instead of binaries
    Node = #node_info{
        name = "node1",  % list instead of binary
        arch = "x86_64",
        os = "Linux"
    },
    Resp = #node_info_response{
        last_update = 1234567890,
        node_count = 1,
        nodes = [Node]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode Partition Info Request Tests
%%%===================================================================

encode_partition_info_request_record() ->
    Req = #partition_info_request{show_flags = 456, partition_name = <<"default">>},
    {ok, Binary} = flurm_codec_node:encode_body(?REQUEST_PARTITION_INFO, Req),
    ?assert(is_binary(Binary)),
    <<ShowFlags:32/big, _Rest/binary>> = Binary,
    ?assertEqual(456, ShowFlags).

encode_partition_info_request_non_record() ->
    Result = flurm_codec_node:encode_body(?REQUEST_PARTITION_INFO, not_a_record),
    ?assertEqual({ok, <<0:32/big, 0:32>>}, Result).

%%%===================================================================
%%% Encode Partition Info Response Tests
%%%===================================================================

encode_partition_info_response_with_partitions() ->
    Part = #partition_info{
        name = <<"default">>,
        nodes = <<"node[1-10]">>,
        total_cpus = 80,
        total_nodes = 10,
        state_up = 1
    },
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_response_empty_partitions() ->
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 0,
        partitions = []
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_info_response_non_record() ->
    Result = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, not_a_record),
    ?assertEqual({ok, <<0:64, 0:32>>}, Result).

encode_single_partition_info_test() ->
    Part = #partition_info{
        name = <<"gpu">>,
        nodes = <<"gpu[01-04]">>,
        total_cpus = 128,
        total_nodes = 4,
        max_time = 86400,
        default_time = 3600,
        max_nodes = 4,
        min_nodes = 1,
        priority_job_factor = 100,
        priority_tier = 1,
        state_up = 1,
        node_inx = [0, 3, -1]
    },
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_partition_info_non_record() ->
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 1,
        partitions = [not_a_record]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_full_partition_info() ->
    Part = #partition_info{
        name = <<"compute">>,
        allow_accounts = <<"acct1,acct2">>,
        allow_alloc_nodes = <<"all">>,
        allow_groups = <<"users">>,
        allow_qos = <<"normal,high">>,
        alternate = <<>>,
        billing_weights_str = <<"cpu=1.0">>,
        def_mem_per_cpu = 2048,
        def_mem_per_node = 0,
        default_time = 3600,
        deny_accounts = <<>>,
        deny_qos = <<>>,
        flags = 0,
        grace_time = 300,
        max_cpus_per_node = 64,
        max_mem_per_cpu = 4096,
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
        total_cpus = 6400,
        total_nodes = 100,
        tres_fmt_str = <<"cpu=6400,mem=6553600M">>,
        node_inx = [0, 99, -1]
    },
    Resp = #partition_info_response{
        last_update = erlang:system_time(second),
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_multiple_partitions() ->
    Parts = [
        #partition_info{name = <<"default">>, total_cpus = 100, state_up = 1},
        #partition_info{name = <<"gpu">>, total_cpus = 64, state_up = 1},
        #partition_info{name = <<"maintenance">>, total_cpus = 32, state_up = 0}
    ],
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 3,
        partitions = Parts
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_partition_with_node_inx() ->
    Part = #partition_info{
        name = <<"test">>,
        nodes = <<"node[1-5,10-15,20]">>,
        node_inx = [0, 4, 9, 14, 19, 19, -1]
    },
    Resp = #partition_info_response{
        last_update = 1234567890,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode node_inx Tests
%%%===================================================================

encode_node_inx_empty() ->
    Part = #partition_info{
        name = <<"test">>,
        node_inx = []
    },
    Resp = #partition_info_response{
        last_update = 0,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_node_inx_with_ranges() ->
    Part = #partition_info{
        name = <<"test">>,
        node_inx = [0, 9, 20, 29, -1]
    },
    Resp = #partition_info_response{
        last_update = 0,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Unsupported Message Type Tests
%%%===================================================================

encode_unsupported_msg_type() ->
    Result = flurm_codec_node:encode_body(99999, some_body),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% ensure_binary Tests
%%%===================================================================

ensure_binary_undefined() ->
    %% Test via node info encoding
    Node = #node_info{name = undefined},
    Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_null() ->
    Node = #node_info{name = null},
    Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_binary() ->
    Node = #node_info{name = <<"binary_name">>},
    Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_list() ->
    Node = #node_info{name = "list_name"},
    Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

ensure_binary_other() ->
    %% Integer should be converted to empty binary
    Node = #node_info{name = 12345},
    Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
    {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_node_registration() ->
    %% Encode then decode
    OrigReq = #node_registration_request{status_only = true},
    {ok, Encoded} = flurm_codec_node:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, OrigReq),
    {ok, Decoded} = flurm_codec_node:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Encoded),
    ?assertEqual(OrigReq#node_registration_request.status_only,
                 Decoded#node_registration_request.status_only).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test with all possible node states
node_state_test_() ->
    States = [
        {?NODE_STATE_UNKNOWN, "unknown"},
        {?NODE_STATE_DOWN, "down"},
        {?NODE_STATE_IDLE, "idle"},
        {?NODE_STATE_ALLOCATED, "allocated"},
        {?NODE_STATE_ERROR, "error"},
        {?NODE_STATE_MIXED, "mixed"},
        {?NODE_STATE_FUTURE, "future"}
    ],
    [
        {lists:flatten(io_lib:format("encode node with state ~s", [Name])),
         fun() ->
             Node = #node_info{name = <<"node">>, node_state = State},
             Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
             {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
             ?assert(is_binary(Binary))
         end}
    || {State, Name} <- States].

%% Test large node count
large_node_count_test_() ->
    {"encode response with many nodes",
     fun() ->
         Nodes = [#node_info{name = list_to_binary("node" ++ integer_to_list(N))}
                  || N <- lists:seq(1, 100)],
         Resp = #node_info_response{
             last_update = 1234567890,
             node_count = 100,
             nodes = Nodes
         },
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
         ?assert(is_binary(Binary)),
         ?assert(byte_size(Binary) > 1000)
     end}.

%% Test large partition count
large_partition_count_test_() ->
    {"encode response with many partitions",
     fun() ->
         Parts = [#partition_info{name = list_to_binary("part" ++ integer_to_list(N))}
                  || N <- lists:seq(1, 50)],
         Resp = #partition_info_response{
             last_update = 1234567890,
             partition_count = 50,
             partitions = Parts
         },
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test with unicode strings
unicode_string_test_() ->
    {"encode node info with unicode characters",
     fun() ->
         Node = #node_info{
             name = <<"node-", 195, 169, 97, 115, 116, "-01">>,  % contains e-acute
             reason = <<"maintenance ", 226, 128, 147, " scheduled">>  % en-dash
         },
         Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test empty strings in all fields
empty_strings_test_() ->
    {"encode node info with all empty strings",
     fun() ->
         Node = #node_info{
             name = <<>>,
             node_hostname = <<>>,
             node_addr = <<>>,
             bcast_address = <<>>,
             version = <<>>,
             arch = <<>>,
             os = <<>>,
             features = <<>>,
             features_act = <<>>,
             gres = <<>>,
             gres_drain = <<>>,
             gres_used = <<>>,
             partitions = <<>>,
             reason = <<>>,
             tres_fmt_str = <<>>,
             mcs_label = <<>>,
             cpu_spec_list = <<>>
         },
         Resp = #node_info_response{last_update = 0, node_count = 1, nodes = [Node]},
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test maximum values
max_values_test_() ->
    {"encode node info with maximum integer values",
     fun() ->
         Node = #node_info{
             name = <<"maxnode">>,
             cpus = 16#FFFF,
             sockets = 16#FFFF,
             cores = 16#FFFF,
             threads = 16#FFFF,
             real_memory = 16#FFFFFFFFFFFFFFFF,
             free_mem = 16#FFFFFFFFFFFFFFFF,
             cpu_load = 16#FFFFFFFF,
             node_state = 16#FFFFFFFF,
             weight = 16#FFFFFFFF
         },
         Resp = #node_info_response{last_update = 16#FFFFFFFFFFFFFFFF, node_count = 1, nodes = [Node]},
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test partition with large node_inx
large_node_inx_test_() ->
    {"encode partition with many node index ranges",
     fun() ->
         %% Create a large node_inx list
         NodeInx = lists:flatten([[Start, Start + 9] || Start <- lists:seq(0, 990, 10)]) ++ [-1],
         Part = #partition_info{
             name = <<"large">>,
             node_inx = NodeInx
         },
         Resp = #partition_info_response{last_update = 0, partition_count = 1, partitions = [Part]},
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%%%===================================================================
%%% Integration-style Tests
%%%===================================================================

%% Test a complete sinfo-style response
sinfo_style_response_test_() ->
    {"encode complete sinfo-style node response",
     fun() ->
         Nodes = [
             #node_info{
                 name = <<"compute001">>,
                 node_hostname = <<"compute001.cluster.local">>,
                 node_addr = <<"192.168.1.1">>,
                 node_state = ?NODE_STATE_IDLE,
                 cpus = 32,
                 sockets = 2,
                 cores = 8,
                 threads = 2,
                 real_memory = 128000,
                 partitions = <<"default,batch">>,
                 features = <<"centos7,avx2,sse4">>
             },
             #node_info{
                 name = <<"compute002">>,
                 node_hostname = <<"compute002.cluster.local">>,
                 node_addr = <<"192.168.1.2">>,
                 node_state = ?NODE_STATE_ALLOCATED,
                 cpus = 32,
                 sockets = 2,
                 cores = 8,
                 threads = 2,
                 real_memory = 128000,
                 alloc_cpus = 16,
                 partitions = <<"default,batch">>,
                 features = <<"centos7,avx2,sse4">>
             },
             #node_info{
                 name = <<"gpu001">>,
                 node_hostname = <<"gpu001.cluster.local">>,
                 node_addr = <<"192.168.2.1">>,
                 node_state = ?NODE_STATE_MIXED,
                 cpus = 64,
                 sockets = 2,
                 cores = 16,
                 threads = 2,
                 real_memory = 256000,
                 alloc_cpus = 32,
                 partitions = <<"gpu">>,
                 features = <<"centos7,avx2,gpu">>,
                 gres = <<"gpu:tesla:4">>,
                 gres_used = <<"gpu:tesla:2">>
             }
         ],
         Resp = #node_info_response{
             last_update = erlang:system_time(second),
             node_count = 3,
             nodes = Nodes
         },
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_NODE_INFO, Resp),
         ?assert(is_binary(Binary)),
         ?assert(byte_size(Binary) > 500)
     end}.

%% Test a complete scontrol show partition response
scontrol_partition_response_test_() ->
    {"encode complete scontrol partition response",
     fun() ->
         Parts = [
             #partition_info{
                 name = <<"batch">>,
                 nodes = <<"compute[001-100]">>,
                 total_cpus = 3200,
                 total_nodes = 100,
                 max_time = 604800,  % 1 week
                 default_time = 3600,
                 max_nodes = 100,
                 min_nodes = 1,
                 state_up = 1,
                 priority_job_factor = 1,
                 priority_tier = 1,
                 node_inx = [0, 99, -1]
             },
             #partition_info{
                 name = <<"interactive">>,
                 nodes = <<"login[01-02]">>,
                 total_cpus = 16,
                 total_nodes = 2,
                 max_time = 28800,  % 8 hours
                 default_time = 1800,
                 max_nodes = 1,
                 min_nodes = 1,
                 state_up = 1,
                 priority_job_factor = 2,
                 priority_tier = 2,
                 node_inx = [100, 101, -1]
             }
         ],
         Resp = #partition_info_response{
             last_update = erlang:system_time(second),
             partition_count = 2,
             partitions = Parts
         },
         {ok, Binary} = flurm_codec_node:encode_body(?RESPONSE_PARTITION_INFO, Resp),
         ?assert(is_binary(Binary)),
         ?assert(byte_size(Binary) > 200)
     end}.
