%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for FLURM Protocol Header Module
%%%
%%% Tests for header encoding/decoding functions with focus on:
%%% - Success cases for all record fields
%%% - Error handling paths
%%% - Edge cases (boundary values, invalid inputs)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_header_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Constants Tests
%%%===================================================================

constants_test_() ->
    [
        {"protocol_version returns correct value", fun() ->
            Version = flurm_protocol_header:protocol_version(),
            ?assertEqual(?SLURM_PROTOCOL_VERSION, Version)
        end},

        {"header_size returns 10 bytes", fun() ->
            Size = flurm_protocol_header:header_size(),
            ?assertEqual(10, Size),
            ?assertEqual(?SLURM_HEADER_SIZE, Size)
        end}
    ].

%%%===================================================================
%%% parse_header Tests
%%%===================================================================

parse_header_success_test_() ->
    [
        {"parse default header", fun() ->
            Binary = <<0:16/big, 0:16/big, 0:16/big, 0:32/big>>,
            {ok, Header, Rest} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(0, Header#slurm_header.version),
            ?assertEqual(0, Header#slurm_header.flags),
            ?assertEqual(0, Header#slurm_header.msg_type),
            ?assertEqual(0, Header#slurm_header.body_length),
            ?assertEqual(<<>>, Rest)
        end},

        {"parse header with protocol version", fun() ->
            Binary = <<?SLURM_PROTOCOL_VERSION:16/big, 0:16/big, 0:16/big, 0:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(?SLURM_PROTOCOL_VERSION, Header#slurm_header.version)
        end},

        {"parse header with flags", fun() ->
            Binary = <<0:16/big, 16#1234:16/big, 0:16/big, 0:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(16#1234, Header#slurm_header.flags)
        end},

        {"parse header with msg_type", fun() ->
            Binary = <<0:16/big, 0:16/big, ?REQUEST_PING:16/big, 0:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(?REQUEST_PING, Header#slurm_header.msg_type)
        end},

        {"parse header with body_length", fun() ->
            Binary = <<0:16/big, 0:16/big, 0:16/big, 12345:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(12345, Header#slurm_header.body_length)
        end},

        {"parse header with max uint16 values", fun() ->
            Binary = <<65535:16/big, 65535:16/big, 65535:16/big, 0:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(65535, Header#slurm_header.version),
            ?assertEqual(65535, Header#slurm_header.flags),
            ?assertEqual(65535, Header#slurm_header.msg_type)
        end},

        {"parse header with max uint32 body_length", fun() ->
            Binary = <<0:16/big, 0:16/big, 0:16/big, 16#FFFFFFFF:32/big>>,
            {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(16#FFFFFFFF, Header#slurm_header.body_length)
        end},

        {"parse header with trailing data returns rest", fun() ->
            Binary = <<0:16/big, 0:16/big, 0:16/big, 0:32/big, "trailing data">>,
            {ok, Header, Rest} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(0, Header#slurm_header.msg_type),
            ?assertEqual(<<"trailing data">>, Rest)
        end},

        {"parse header with large trailing data", fun() ->
            TrailingData = list_to_binary(lists:duplicate(10000, $x)),
            Binary = <<0:16/big, 0:16/big, 0:16/big, 0:32/big, TrailingData/binary>>,
            {ok, _Header, Rest} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(TrailingData, Rest)
        end},

        {"parse all message types", fun() ->
            MsgTypes = [
                ?REQUEST_NODE_REGISTRATION_STATUS,
                ?REQUEST_PING,
                ?REQUEST_JOB_INFO,
                ?REQUEST_SUBMIT_BATCH_JOB,
                ?REQUEST_CANCEL_JOB,
                ?RESPONSE_SLURM_RC,
                ?RESPONSE_JOB_INFO,
                ?RESPONSE_SUBMIT_BATCH_JOB
            ],
            lists:foreach(fun(MsgType) ->
                Binary = <<0:16/big, 0:16/big, MsgType:16/big, 0:32/big>>,
                {ok, Header, <<>>} = flurm_protocol_header:parse_header(Binary),
                ?assertEqual(MsgType, Header#slurm_header.msg_type)
            end, MsgTypes)
        end}
    ].

parse_header_error_test_() ->
    [
        {"parse empty binary returns incomplete error", fun() ->
            Result = flurm_protocol_header:parse_header(<<>>),
            ?assertMatch({error, {incomplete_header, 0, 10}}, Result)
        end},

        {"parse 1 byte returns incomplete error", fun() ->
            Result = flurm_protocol_header:parse_header(<<1>>),
            ?assertMatch({error, {incomplete_header, 1, 10}}, Result)
        end},

        {"parse 5 bytes returns incomplete error", fun() ->
            Result = flurm_protocol_header:parse_header(<<1, 2, 3, 4, 5>>),
            ?assertMatch({error, {incomplete_header, 5, 10}}, Result)
        end},

        {"parse 9 bytes returns incomplete error", fun() ->
            Result = flurm_protocol_header:parse_header(<<1, 2, 3, 4, 5, 6, 7, 8, 9>>),
            ?assertMatch({error, {incomplete_header, 9, 10}}, Result)
        end},

        {"parse non-binary returns invalid error", fun() ->
            Result = flurm_protocol_header:parse_header(not_a_binary),
            ?assertMatch({error, invalid_header_data}, Result)
        end},

        {"parse integer returns invalid error", fun() ->
            Result = flurm_protocol_header:parse_header(12345),
            ?assertMatch({error, invalid_header_data}, Result)
        end},

        {"parse list returns invalid error", fun() ->
            Result = flurm_protocol_header:parse_header([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ?assertMatch({error, invalid_header_data}, Result)
        end}
    ].

%%%===================================================================
%%% encode_header Tests
%%%===================================================================

encode_header_success_test_() ->
    [
        {"encode default header", fun() ->
            Header = #slurm_header{},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            ?assertEqual(10, byte_size(Binary))
        end},

        {"encode header with protocol version", fun() ->
            Header = #slurm_header{version = ?SLURM_PROTOCOL_VERSION},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<?SLURM_PROTOCOL_VERSION:16/big, _Rest/binary>> = Binary,
            ?assertEqual(10, byte_size(Binary))
        end},

        {"encode header preserves all fields", fun() ->
            Header = #slurm_header{
                version = 16#1234,
                flags = 16#5678,
                msg_type = ?REQUEST_PING,
                body_length = 12345
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<Ver:16/big, Flags:16/big, MsgType:16/big, BodyLen:32/big>> = Binary,
            ?assertEqual(16#1234, Ver),
            ?assertEqual(16#5678, Flags),
            ?assertEqual(?REQUEST_PING, MsgType),
            ?assertEqual(12345, BodyLen)
        end},

        {"encode header with zero values", fun() ->
            Header = #slurm_header{
                version = 0,
                flags = 0,
                msg_type = 0,
                body_length = 0
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            ?assertEqual(<<0:80>>, Binary)
        end},

        {"encode header with max uint16 values", fun() ->
            Header = #slurm_header{
                version = 65535,
                flags = 65535,
                msg_type = 65535,
                body_length = 0
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<65535:16/big, 65535:16/big, 65535:16/big, 0:32/big>> = Binary,
            ?assert(true)
        end},

        {"encode header with max uint32 body_length", fun() ->
            Header = #slurm_header{
                version = 0,
                flags = 0,
                msg_type = 0,
                body_length = 4294967295
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<0:48, 16#FFFFFFFF:32/big>> = Binary,
            ?assert(true)
        end},

        {"encoded header is always 10 bytes", fun() ->
            Headers = [
                #slurm_header{},
                #slurm_header{version = 65535},
                #slurm_header{body_length = 4294967295},
                #slurm_header{version = 1, flags = 2, msg_type = 3, body_length = 4}
            ],
            lists:foreach(fun(H) ->
                {ok, Binary} = flurm_protocol_header:encode_header(H),
                ?assertEqual(10, byte_size(Binary))
            end, Headers)
        end}
    ].

encode_header_error_test_() ->
    [
        {"encode non-record returns error", fun() ->
            Result = flurm_protocol_header:encode_header(not_a_record),
            ?assertMatch({error, invalid_header_record}, Result)
        end},

        {"encode wrong record type returns error", fun() ->
            Result = flurm_protocol_header:encode_header({slurm_msg, #slurm_header{}, <<>>}),
            ?assertMatch({error, invalid_header_record}, Result)
        end},

        {"encode tuple returns error", fun() ->
            Result = flurm_protocol_header:encode_header({1, 2, 3, 4, 5}),
            ?assertMatch({error, invalid_header_record}, Result)
        end},

        {"encode atom returns error", fun() ->
            Result = flurm_protocol_header:encode_header(undefined),
            ?assertMatch({error, invalid_header_record}, Result)
        end},

        {"encode header with negative version returns error", fun() ->
            Header = #slurm_header{version = -1},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with version > 65535 returns error", fun() ->
            Header = #slurm_header{version = 65536},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with negative flags returns error", fun() ->
            Header = #slurm_header{flags = -1},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with flags > 65535 returns error", fun() ->
            Header = #slurm_header{flags = 65536},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with negative msg_type returns error", fun() ->
            Header = #slurm_header{msg_type = -1},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with msg_type > 65535 returns error", fun() ->
            Header = #slurm_header{msg_type = 65536},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with negative body_length returns error", fun() ->
            Header = #slurm_header{body_length = -1},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with body_length > 4294967295 returns error", fun() ->
            Header = #slurm_header{body_length = 4294967296},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with non-integer version returns error", fun() ->
            Header = #slurm_header{version = <<"not_int">>},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end},

        {"encode header with float body_length returns error", fun() ->
            Header = #slurm_header{body_length = 3.14},
            Result = flurm_protocol_header:encode_header(Header),
            ?assertMatch({error, {invalid_header_values, _}}, Result)
        end}
    ].

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_test_() ->
    [
        {"default header roundtrip", fun() ->
            Original = #slurm_header{},
            {ok, Binary} = flurm_protocol_header:encode_header(Original),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Original#slurm_header.version, Decoded#slurm_header.version),
            ?assertEqual(Original#slurm_header.flags, Decoded#slurm_header.flags),
            ?assertEqual(Original#slurm_header.msg_type, Decoded#slurm_header.msg_type),
            ?assertEqual(Original#slurm_header.body_length, Decoded#slurm_header.body_length)
        end},

        {"custom header roundtrip", fun() ->
            Original = #slurm_header{
                version = ?SLURM_PROTOCOL_VERSION,
                flags = 16#1234,
                msg_type = ?REQUEST_SUBMIT_BATCH_JOB,
                body_length = 9999
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Original),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Original#slurm_header.version, Decoded#slurm_header.version),
            ?assertEqual(Original#slurm_header.flags, Decoded#slurm_header.flags),
            ?assertEqual(Original#slurm_header.msg_type, Decoded#slurm_header.msg_type),
            ?assertEqual(Original#slurm_header.body_length, Decoded#slurm_header.body_length)
        end},

        {"max values roundtrip", fun() ->
            Original = #slurm_header{
                version = 65535,
                flags = 65535,
                msg_type = 65535,
                body_length = 4294967295
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Original),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Original#slurm_header.version, Decoded#slurm_header.version),
            ?assertEqual(Original#slurm_header.flags, Decoded#slurm_header.flags),
            ?assertEqual(Original#slurm_header.msg_type, Decoded#slurm_header.msg_type),
            ?assertEqual(Original#slurm_header.body_length, Decoded#slurm_header.body_length)
        end},

        {"zero values roundtrip", fun() ->
            Original = #slurm_header{
                version = 0,
                flags = 0,
                msg_type = 0,
                body_length = 0
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Original),
            {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
            ?assertEqual(Original, Decoded)
        end},

        {"multiple roundtrips are consistent", fun() ->
            Original = #slurm_header{
                version = 12345,
                flags = 111,
                msg_type = 4003,
                body_length = 87654321
            },
            lists:foreach(fun(_) ->
                {ok, Binary} = flurm_protocol_header:encode_header(Original),
                {ok, Decoded, <<>>} = flurm_protocol_header:parse_header(Binary),
                ?assertEqual(Original#slurm_header.version, Decoded#slurm_header.version),
                ?assertEqual(Original#slurm_header.flags, Decoded#slurm_header.flags),
                ?assertEqual(Original#slurm_header.msg_type, Decoded#slurm_header.msg_type),
                ?assertEqual(Original#slurm_header.body_length, Decoded#slurm_header.body_length)
            end, lists:seq(1, 10))
        end}
    ].

%%%===================================================================
%%% Wire Format Tests (Big Endian)
%%%===================================================================

wire_format_test_() ->
    [
        {"header is big endian (version)", fun() ->
            Header = #slurm_header{version = 16#0102},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<V1, V2, _Rest/binary>> = Binary,
            ?assertEqual(1, V1),
            ?assertEqual(2, V2)
        end},

        {"header is big endian (body_length)", fun() ->
            Header = #slurm_header{body_length = 16#01020304},
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            <<_:48, B1, B2, B3, B4>> = Binary,
            ?assertEqual(1, B1),
            ?assertEqual(2, B2),
            ?assertEqual(3, B3),
            ?assertEqual(4, B4)
        end},

        {"header byte layout is correct", fun() ->
            Header = #slurm_header{
                version = 16#1122,
                flags = 16#3344,
                msg_type = 16#5566,
                body_length = 16#778899AA
            },
            {ok, Binary} = flurm_protocol_header:encode_header(Header),
            Expected = <<16#11, 16#22, 16#33, 16#44, 16#55, 16#66, 16#77, 16#88, 16#99, 16#AA>>,
            ?assertEqual(Expected, Binary)
        end}
    ].
