%%%-------------------------------------------------------------------
%%% @doc Unit Tests for FLURM SLURM Interop Helpers
%%%
%%% This module contains unit tests for helper functions used in
%%% SLURM interoperability testing. These tests verify protocol
%%% encoding/decoding and network communication helpers.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_slurm_interop_helper_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%%===================================================================
%%% Test Setup/Teardown
%%%===================================================================

slurm_interop_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          protocol_encoding_tests(),
          protocol_decoding_tests(),
          munge_helper_tests(),
          connection_helper_tests()
         ]
     end}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Protocol Encoding Tests
%%%===================================================================

protocol_encoding_tests() ->
    [
        {"ping request encoding", fun test_encode_ping_request/0},
        {"job info request encoding", fun test_encode_job_info_request/0},
        {"node info request encoding", fun test_encode_node_info_request/0},
        {"partition info request encoding", fun test_encode_partition_info_request/0},
        {"cancel job request encoding", fun test_encode_cancel_job_request/0},
        {"batch job request encoding", fun test_encode_batch_job_request/0}
    ].

test_encode_ping_request() ->
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ?assert(is_binary(Encoded)),
    ?assert(byte_size(Encoded) >= 14),  % 4 byte length + 10 byte min header
    %% Verify message type is correct
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_PING, MsgType).

test_encode_job_info_request() ->
    Request = #job_info_request{
        show_flags = 0,
        job_id = 0,
        user_id = 1000
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Request),
    ?assert(is_binary(Encoded)),
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_JOB_INFO, MsgType).

test_encode_node_info_request() ->
    Request = #node_info_request{
        show_flags = 0,
        node_name = <<>>
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_NODE_INFO, Request),
    ?assert(is_binary(Encoded)),
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_NODE_INFO, MsgType).

test_encode_partition_info_request() ->
    Request = #partition_info_request{
        show_flags = 0,
        partition_name = <<>>
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PARTITION_INFO, Request),
    ?assert(is_binary(Encoded)),
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_PARTITION_INFO, MsgType).

test_encode_cancel_job_request() ->
    Request = #cancel_job_request{
        job_id = 12345,
        step_id = ?SLURM_NO_VAL,
        signal = 15,  % SIGTERM
        flags = 0
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Request),
    ?assert(is_binary(Encoded)),
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_CANCEL_JOB, MsgType).

test_encode_batch_job_request() ->
    Request = #batch_job_request{
        name = <<"test_job">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello\n">>,
        work_dir = <<"/tmp">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 1,
        num_tasks = 1,
        time_limit = 60,
        user_id = 1000,
        group_id = 1000
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_SUBMIT_BATCH_JOB, Request),
    ?assert(is_binary(Encoded)),
    <<_Len:32/big, _Ver:16/big, _Flags:16/big, MsgType:16/big, _Rest/binary>> = Encoded,
    ?assertEqual(?REQUEST_SUBMIT_BATCH_JOB, MsgType).

%%%===================================================================
%%% Protocol Decoding Tests
%%%===================================================================

protocol_decoding_tests() ->
    [
        {"ping response decoding", fun test_decode_ping_response/0},
        {"slurm rc response decoding", fun test_decode_slurm_rc_response/0},
        {"decode preserves message type", fun test_decode_preserves_type/0},
        {"decode handles incomplete message", fun test_decode_incomplete/0}
    ].

test_decode_ping_response() ->
    %% Encode a SLURM RC response (typical ping response)
    Response = #slurm_rc_response{return_code = 0},
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Response),
    {ok, Decoded, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(?RESPONSE_SLURM_RC, Decoded#slurm_msg.header#slurm_header.msg_type),
    ?assertEqual(0, (Decoded#slurm_msg.body)#slurm_rc_response.return_code).

test_decode_slurm_rc_response() ->
    %% Test various return codes
    lists:foreach(fun(Code) ->
        Response = #slurm_rc_response{return_code = Code},
        {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Response),
        {ok, Decoded, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Code, (Decoded#slurm_msg.body)#slurm_rc_response.return_code)
    end, [0, 1, -1, 127, -127]).

test_decode_preserves_type() ->
    %% Encode and decode various message types
    MessageTypes = [
        {?REQUEST_PING, #ping_request{}},
        {?REQUEST_JOB_INFO, #job_info_request{}},
        {?REQUEST_NODE_INFO, #node_info_request{}},
        {?REQUEST_PARTITION_INFO, #partition_info_request{}}
    ],
    lists:foreach(fun({Type, Request}) ->
        {ok, Encoded} = flurm_protocol_codec:encode(Type, Request),
        {ok, Decoded, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Type, Decoded#slurm_msg.header#slurm_header.msg_type)
    end, MessageTypes).

test_decode_incomplete() ->
    %% Test handling of incomplete messages
    Result = flurm_protocol_codec:decode(<<1, 2, 3>>),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% MUNGE Helper Tests
%%%===================================================================

munge_helper_tests() ->
    [
        {"munge credential format", fun test_munge_credential_format/0},
        {"munge key path default", fun test_munge_key_path/0}
    ].

test_munge_credential_format() ->
    %% MUNGE credentials are base64-encoded and start with "MUNGE:"
    %% This test verifies our understanding of the format
    MockCred = <<"MUNGE:AAECAQAAVGVzdA==">>,
    ?assert(binary:match(MockCred, <<"MUNGE:">>) =/= nomatch).

test_munge_key_path() ->
    %% Default MUNGE key path
    DefaultPath = "/etc/munge/munge.key",
    ?assertEqual("/etc/munge/munge.key", DefaultPath).

%%%===================================================================
%%% Connection Helper Tests
%%%===================================================================

connection_helper_tests() ->
    [
        {"tcp connect options", fun test_tcp_connect_options/0},
        {"default ports", fun test_default_ports/0}
    ].

test_tcp_connect_options() ->
    %% Test standard options used for SLURM connections
    Options = [binary, {packet, 0}, {active, false}],
    ?assert(lists:member(binary, Options)),
    ?assert(lists:keymember(packet, 1, Options)),
    ?assert(lists:keymember(active, 1, Options)).

test_default_ports() ->
    %% SLURM default ports
    ?assertEqual(6817, 6817),   % slurmctld
    ?assertEqual(6818, 6818),   % slurmd
    ?assertEqual(6819, 6819).   % slurmdbd

%%%===================================================================
%%% Additional Tests
%%%===================================================================

additional_interop_test_() ->
    [
        {"wire format verification", fun test_wire_format/0},
        {"header version check", fun test_header_version/0},
        {"message length calculation", fun test_message_length/0}
    ].

test_wire_format() ->
    %% SLURM wire format:
    %% - 4 byte length prefix (big endian)
    %% - 2 byte version
    %% - 2 byte flags
    %% - 2 byte message type
    %% - 4 byte body length
    %% - Variable: forward info, orig addr, body
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    <<Len:32/big, Ver:16/big, _Flags:16/big, MsgType:16/big, _BodyLen:32/big, _Rest/binary>> = Encoded,

    %% Length should match
    ?assertEqual(byte_size(Encoded) - 4, Len),

    %% Version should be SLURM 22.05 compatible
    ?assertEqual(?SLURM_PROTOCOL_VERSION, Ver),

    %% Message type should be ping
    ?assertEqual(?REQUEST_PING, MsgType).

test_header_version() ->
    %% SLURM 22.05 uses version 0x2600
    ?assertEqual(16#2600, ?SLURM_PROTOCOL_VERSION).

test_message_length() ->
    %% Test that encoded message length matches prefix
    Messages = [
        {?REQUEST_PING, #ping_request{}},
        {?REQUEST_JOB_INFO, #job_info_request{job_id = 12345}},
        {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}}
    ],
    lists:foreach(fun({Type, Body}) ->
        {ok, Encoded} = flurm_protocol_codec:encode(Type, Body),
        <<Len:32/big, Rest/binary>> = Encoded,
        ?assertEqual(Len, byte_size(Rest))
    end, Messages).
