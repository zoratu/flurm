%%%-------------------------------------------------------------------
%%% @doc EUnit Tests for FLURM Protocol Codec
%%%
%%% Tests for:
%%% - Header encoding/decoding round-trips
%%% - Message encoding/decoding round-trips
%%% - Pack utilities
%%% - Edge cases (empty strings, max values)
%%% - Invalid input handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Header Tests
%%%===================================================================

header_roundtrip_test_() ->
    [
        {"default header roundtrip", fun() ->
            Header = #slurm_header{},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Header#slurm_header.version, Decoded#slurm_header.version),
            ?assertEqual(Header#slurm_header.flags, Decoded#slurm_header.flags),
            ?assertEqual(Header#slurm_header.msg_index, Decoded#slurm_header.msg_index),
            ?assertEqual(Header#slurm_header.msg_type, Decoded#slurm_header.msg_type),
            ?assertEqual(Header#slurm_header.body_length, Decoded#slurm_header.body_length)
        end},

        {"custom header roundtrip", fun() ->
            Header = #slurm_header{
                version = 22050,
                flags = 16#0003,
                msg_index = 42,
                msg_type = ?REQUEST_SUBMIT_BATCH_JOB,
                body_length = 1024
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Header, Decoded)
        end},

        {"header with trailing data", fun() ->
            Header = #slurm_header{msg_type = ?REQUEST_PING},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            BinaryWithExtra = <<Binary/binary, "extra data">>,
            {ok, Decoded, <<"extra data">>} = flurm_protocol_header:parse_header(BinaryWithExtra),
            ?assertEqual(Header#slurm_header.msg_type, Decoded#slurm_header.msg_type)
        end},

        {"header size is correct", fun() ->
            ?assertEqual(12, flurm_protocol_header:header_size())
        end}
    ].

header_error_test_() ->
    [
        {"incomplete header", fun() ->
            Binary = <<1, 2, 3, 4, 5>>,  % Only 5 bytes
            Result = flurm_protocol_header:parse_header(Binary),
            ?assertMatch({error, {incomplete_header, 5, 12}}, Result)
        end},

        {"empty header", fun() ->
            Result = flurm_protocol_header:parse_header(<<>>),
            ?assertMatch({error, {incomplete_header, 0, 12}}, Result)
        end},

        {"invalid header record", fun() ->
            Result = flurm_protocol_header:encode_header(not_a_record),
            ?assertMatch({error, invalid_header_record}, Result)
        end}
    ].

%%%===================================================================
%%% Pack Utility Tests
%%%===================================================================

string_pack_test_() ->
    [
        {"empty string roundtrip", fun() ->
            Binary = flurm_protocol_pack:pack_string(<<>>),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(<<>>, Decoded)
        end},

        {"simple string roundtrip", fun() ->
            Str = <<"hello world">>,
            Binary = flurm_protocol_pack:pack_string(Str),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(Str, Decoded)
        end},

        {"unicode string roundtrip", fun() ->
            Str = <<"hello, world - \xC3\xA9\xC3\xB1\xC3\xBC"/utf8>>,  % UTF-8 encoded
            Binary = flurm_protocol_pack:pack_string(Str),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(Str, Decoded)
        end},

        {"undefined string", fun() ->
            Binary = flurm_protocol_pack:pack_string(undefined),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(undefined, Decoded)
        end},

        {"string with trailing data", fun() ->
            Str = <<"test">>,
            Binary = flurm_protocol_pack:pack_string(Str),
            BinaryWithExtra = <<Binary/binary, "extra">>,
            {ok, Decoded, <<"extra">>} = flurm_protocol_pack:unpack_string(BinaryWithExtra),
            ?assertEqual(Str, Decoded)
        end}
    ].

integer_pack_test_() ->
    [
        {"uint8 roundtrip", fun() ->
            Value = 255,
            Binary = flurm_protocol_pack:pack_uint8(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_uint8(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"uint16 roundtrip", fun() ->
            Value = 65535,
            Binary = flurm_protocol_pack:pack_uint16(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_uint16(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"uint32 roundtrip", fun() ->
            Value = 4294967295,
            Binary = flurm_protocol_pack:pack_uint32(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_uint32(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"uint32 undefined", fun() ->
            Binary = flurm_protocol_pack:pack_uint32(undefined),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_uint32(Binary),
            ?assertEqual(undefined, Decoded)
        end},

        {"uint64 roundtrip", fun() ->
            Value = 18446744073709551615,
            Binary = flurm_protocol_pack:pack_uint64(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_uint64(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"int32 positive roundtrip", fun() ->
            Value = 2147483647,
            Binary = flurm_protocol_pack:pack_int32(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_int32(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"int32 negative roundtrip", fun() ->
            Value = -2147483648,
            Binary = flurm_protocol_pack:pack_int32(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_int32(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"int32 zero roundtrip", fun() ->
            Value = 0,
            Binary = flurm_protocol_pack:pack_int32(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_int32(Binary),
            ?assertEqual(Value, Decoded)
        end}
    ].

bool_pack_test_() ->
    [
        {"true roundtrip", fun() ->
            Binary = flurm_protocol_pack:pack_bool(true),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_bool(Binary),
            ?assertEqual(true, Decoded)
        end},

        {"false roundtrip", fun() ->
            Binary = flurm_protocol_pack:pack_bool(false),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_bool(Binary),
            ?assertEqual(false, Decoded)
        end}
    ].

time_pack_test_() ->
    [
        {"timestamp roundtrip", fun() ->
            Timestamp = 1704067200,  % 2024-01-01 00:00:00 UTC
            Binary = flurm_protocol_pack:pack_time(Timestamp),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_time(Binary),
            ?assertEqual(Timestamp, Decoded)
        end},

        {"zero timestamp", fun() ->
            Binary = flurm_protocol_pack:pack_time(0),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_time(Binary),
            ?assertEqual(0, Decoded)
        end},

        {"undefined timestamp", fun() ->
            Binary = flurm_protocol_pack:pack_time(undefined),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_time(Binary),
            ?assertEqual(undefined, Decoded)
        end},

        {"erlang timestamp tuple", fun() ->
            ErlTimestamp = {1704, 67200, 0},  % {MegaSec, Sec, MicroSec}
            Expected = 1704 * 1000000 + 67200,
            Binary = flurm_protocol_pack:pack_time(ErlTimestamp),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_time(Binary),
            ?assertEqual(Expected, Decoded)
        end}
    ].

double_pack_test_() ->
    [
        {"positive float roundtrip", fun() ->
            Value = 3.14159265359,
            Binary = flurm_protocol_pack:pack_double(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Binary),
            ?assert(abs(Value - Decoded) < 0.0000001)
        end},

        {"negative float roundtrip", fun() ->
            Value = -123.456,
            Binary = flurm_protocol_pack:pack_double(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Binary),
            ?assert(abs(Value - Decoded) < 0.001)
        end},

        {"zero float", fun() ->
            Value = 0.0,
            Binary = flurm_protocol_pack:pack_double(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Binary),
            ?assertEqual(Value, Decoded)
        end},

        {"integer as float", fun() ->
            Value = 42,
            Binary = flurm_protocol_pack:pack_double(Value),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Binary),
            ?assertEqual(42.0, Decoded)
        end}
    ].

list_pack_test_() ->
    [
        {"empty list", fun() ->
            List = [],
            Binary = flurm_protocol_pack:pack_list(List, fun flurm_protocol_pack:pack_uint32/1),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_list(Binary, fun flurm_protocol_pack:unpack_uint32/1),
            ?assertEqual([], Decoded)
        end},

        {"integer list", fun() ->
            List = [1, 2, 3, 4, 5],
            Binary = flurm_protocol_pack:pack_list(List, fun flurm_protocol_pack:pack_uint32/1),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_list(Binary, fun flurm_protocol_pack:unpack_uint32/1),
            ?assertEqual(List, Decoded)
        end},

        {"string list", fun() ->
            List = [<<"one">>, <<"two">>, <<"three">>],
            Binary = flurm_protocol_pack:pack_string_array(List),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string_array(Binary),
            ?assertEqual(List, Decoded)
        end}
    ].

%%%===================================================================
%%% Codec Message Tests
%%%===================================================================

ping_message_test_() ->
    [
        {"ping request encode/decode roundtrip", fun() ->
            Req = #ping_request{},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertMatch(#ping_request{}, Msg#slurm_msg.body)
        end}
    ].

slurm_rc_message_test_() ->
    [
        {"slurm_rc success roundtrip", fun() ->
            Resp = #slurm_rc_response{return_code = 0},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertEqual(0, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end},

        {"slurm_rc error roundtrip", fun() ->
            Resp = #slurm_rc_response{return_code = -1},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(-1, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end},

        {"slurm_rc positive code", fun() ->
            Resp = #slurm_rc_response{return_code = 42},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(42, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
        end}
    ].

job_info_request_test_() ->
    [
        {"job info request roundtrip", fun() ->
            Req = #job_info_request{
                show_flags = 1,
                job_id = 12345,
                user_id = 1000
            },
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_JOB_INFO, Msg#slurm_msg.header#slurm_header.msg_type),
            Body = Msg#slurm_msg.body,
            ?assertEqual(1, Body#job_info_request.show_flags),
            ?assertEqual(12345, Body#job_info_request.job_id),
            ?assertEqual(1000, Body#job_info_request.user_id)
        end},

        {"job info request defaults", fun() ->
            Req = #job_info_request{},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            Body = Msg#slurm_msg.body,
            ?assertEqual(0, Body#job_info_request.show_flags),
            ?assertEqual(0, Body#job_info_request.job_id),
            ?assertEqual(0, Body#job_info_request.user_id)
        end}
    ].

cancel_job_request_test_() ->
    [
        {"cancel job request roundtrip", fun() ->
            Req = #cancel_job_request{
                job_id = 54321,
                job_id_str = <<"54321">>,
                step_id = 0,
                signal = 9,
                flags = 0
            },
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_CANCEL_JOB, Msg#slurm_msg.header#slurm_header.msg_type),
            Body = Msg#slurm_msg.body,
            ?assertEqual(54321, Body#cancel_job_request.job_id),
            ?assertEqual(<<"54321">>, Body#cancel_job_request.job_id_str),
            ?assertEqual(9, Body#cancel_job_request.signal)
        end}
    ].

batch_job_response_test_() ->
    [
        {"batch job response roundtrip", fun() ->
            Resp = #batch_job_response{
                job_id = 99999,
                step_id = 0,
                error_code = 0,
                job_submit_user_msg = <<"Job submitted successfully">>
            },
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, Msg#slurm_msg.header#slurm_header.msg_type),
            Body = Msg#slurm_msg.body,
            ?assertEqual(99999, Body#batch_job_response.job_id),
            ?assertEqual(0, Body#batch_job_response.step_id),
            ?assertEqual(0, Body#batch_job_response.error_code),
            ?assertEqual(<<"Job submitted successfully">>, Body#batch_job_response.job_submit_user_msg)
        end},

        {"batch job response minimal", fun() ->
            Resp = #batch_job_response{job_id = 1},
            {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            Body = Msg#slurm_msg.body,
            ?assertEqual(1, Body#batch_job_response.job_id)
        end}
    ].

node_registration_request_test_() ->
    [
        {"node registration status only false", fun() ->
            Req = #node_registration_request{status_only = false},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_NODE_REGISTRATION_STATUS, Msg#slurm_msg.header#slurm_header.msg_type),
            ?assertEqual(false, (Msg#slurm_msg.body)#node_registration_request.status_only)
        end},

        {"node registration status only true", fun() ->
            Req = #node_registration_request{status_only = true},
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Req),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(true, (Msg#slurm_msg.body)#node_registration_request.status_only)
        end}
    ].

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

edge_case_test_() ->
    [
        {"max uint16 in header", fun() ->
            Header = #slurm_header{
                version = 65535,
                flags = 65535,
                msg_index = 65535,
                msg_type = 65535,
                body_length = 65535
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Header, Decoded)
        end},

        {"zero values in header", fun() ->
            Header = #slurm_header{
                version = 0,
                flags = 0,
                msg_index = 0,
                msg_type = 0,
                body_length = 0
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Header, Decoded)
        end},

        {"empty body message", fun() ->
            {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
            {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
            ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type)
        end},

        {"large string", fun() ->
            LargeStr = list_to_binary(lists:duplicate(10000, $A)),
            Binary = flurm_protocol_pack:pack_string(LargeStr),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(LargeStr, Decoded)
        end},

        {"binary with null bytes", fun() ->
            Str = <<0, 1, 2, 0, 3, 4, 0>>,
            Binary = flurm_protocol_pack:pack_string(Str),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(Str, Decoded)
        end}
    ].

%%%===================================================================
%%% Invalid Input Tests
%%%===================================================================

invalid_input_test_() ->
    [
        {"decode incomplete length prefix", fun() ->
            Result = flurm_protocol_codec:decode(<<1, 2>>),
            ?assertMatch({error, {incomplete_length_prefix, 2}}, Result)
        end},

        {"decode incomplete message", fun() ->
            %% Length says 100 bytes but we only have 10 + header
            Result = flurm_protocol_codec:decode(<<100:32/big, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>),
            ?assertMatch({error, {incomplete_message, 100, _}}, Result)
        end},

        {"decode invalid message length", fun() ->
            %% Length < header size (10)
            Result = flurm_protocol_codec:decode(<<5:32/big, 1, 2, 3, 4, 5>>),
            ?assertMatch({error, {invalid_message_length, 5}}, Result)
        end},

        {"unpack incomplete string", fun() ->
            Result = flurm_protocol_pack:unpack_string(<<10:32/big, "abc">>),
            ?assertMatch({error, {insufficient_string_data, 10}}, Result)
        end},

        {"unpack incomplete uint32", fun() ->
            Result = flurm_protocol_pack:unpack_uint32(<<1, 2>>),
            ?assertMatch({error, {incomplete_uint32, 2}}, Result)
        end},

        {"unpack incomplete time", fun() ->
            Result = flurm_protocol_pack:unpack_time(<<1, 2, 3, 4>>),
            ?assertMatch({error, {incomplete_timestamp, 4}}, Result)
        end}
    ].

%%%===================================================================
%%% Message Type Helper Tests
%%%===================================================================

message_type_helper_test_() ->
    [
        {"message type name known", fun() ->
            ?assertEqual(request_ping, flurm_protocol_codec:message_type_name(?REQUEST_PING)),
            ?assertEqual(request_job_info, flurm_protocol_codec:message_type_name(?REQUEST_JOB_INFO)),
            ?assertEqual(response_slurm_rc, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC))
        end},

        {"message type name unknown", fun() ->
            ?assertMatch({unknown, 99999}, flurm_protocol_codec:message_type_name(99999))
        end},

        {"is_request", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_PING)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO)),
            ?assert(flurm_protocol_codec:is_request(?REQUEST_CANCEL_JOB)),
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC))
        end},

        {"is_response", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC)),
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_INFO)),
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_PING))
        end}
    ].
