%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_protocol_header module
%%%
%%% These tests do NOT use meck - they test the module directly with
%%% various inputs to achieve high coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_header_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test: protocol_version/0
%%%===================================================================

protocol_version_test() ->
    %% Should return the default protocol version
    Version = flurm_protocol_header:protocol_version(),
    ?assert(is_integer(Version)),
    ?assertEqual(?SLURM_PROTOCOL_VERSION, Version),
    %% Version should be 0x2600 for SLURM 22.05
    ?assertEqual(16#2600, Version).

%%%===================================================================
%%% Test: header_size/0
%%%===================================================================

header_size_test() ->
    %% Should return the header size constant
    Size = flurm_protocol_header:header_size(),
    ?assert(is_integer(Size)),
    ?assertEqual(?SLURM_HEADER_SIZE, Size),
    %% Header size should be 10 bytes
    ?assertEqual(10, Size).

%%%===================================================================
%%% Test: parse_header/1 - Success cases
%%%===================================================================

parse_header_basic_test() ->
    %% Create a valid 10-byte header
    Version = 16#2600,
    Flags = 16#0001,
    MsgType = 1008,  % REQUEST_PING
    BodyLength = 100,

    HeaderBin = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLength:32/big>>,

    {ok, Header, Rest} = flurm_protocol_header:parse_header(HeaderBin),

    ?assertEqual(Version, Header#slurm_header.version),
    ?assertEqual(Flags, Header#slurm_header.flags),
    ?assertEqual(0, Header#slurm_header.msg_index),  % Always 0
    ?assertEqual(MsgType, Header#slurm_header.msg_type),
    ?assertEqual(BodyLength, Header#slurm_header.body_length),
    ?assertEqual(<<>>, Rest).

parse_header_with_remaining_data_test() ->
    %% Header with extra data after it
    Version = 16#2500,
    Flags = 0,
    MsgType = 2003,  % REQUEST_JOB_INFO
    BodyLength = 50,
    ExtraData = <<"extra data here">>,

    HeaderBin = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLength:32/big, ExtraData/binary>>,

    {ok, Header, Rest} = flurm_protocol_header:parse_header(HeaderBin),

    ?assertEqual(Version, Header#slurm_header.version),
    ?assertEqual(Flags, Header#slurm_header.flags),
    ?assertEqual(MsgType, Header#slurm_header.msg_type),
    ?assertEqual(BodyLength, Header#slurm_header.body_length),
    ?assertEqual(ExtraData, Rest).

parse_header_zero_values_test() ->
    %% All zero values should be valid
    HeaderBin = <<0:16/big, 0:16/big, 0:16/big, 0:32/big>>,

    {ok, Header, <<>>} = flurm_protocol_header:parse_header(HeaderBin),

    ?assertEqual(0, Header#slurm_header.version),
    ?assertEqual(0, Header#slurm_header.flags),
    ?assertEqual(0, Header#slurm_header.msg_type),
    ?assertEqual(0, Header#slurm_header.body_length).

parse_header_max_values_test() ->
    %% Maximum values for each field
    MaxVersion = 16#FFFF,
    MaxFlags = 16#FFFF,
    MaxMsgType = 16#FFFF,
    MaxBodyLength = 16#FFFFFFFF,

    HeaderBin = <<MaxVersion:16/big, MaxFlags:16/big, MaxMsgType:16/big, MaxBodyLength:32/big>>,

    {ok, Header, <<>>} = flurm_protocol_header:parse_header(HeaderBin),

    ?assertEqual(MaxVersion, Header#slurm_header.version),
    ?assertEqual(MaxFlags, Header#slurm_header.flags),
    ?assertEqual(MaxMsgType, Header#slurm_header.msg_type),
    ?assertEqual(MaxBodyLength, Header#slurm_header.body_length).

parse_header_various_msg_types_test() ->
    %% Test various message types
    MsgTypes = [
        ?REQUEST_NODE_REGISTRATION_STATUS,
        ?REQUEST_PING,
        ?REQUEST_JOB_INFO,
        ?REQUEST_SUBMIT_BATCH_JOB,
        ?REQUEST_CANCEL_JOB,
        ?REQUEST_KILL_JOB,
        ?RESPONSE_SLURM_RC,
        ?RESPONSE_JOB_INFO,
        ?RESPONSE_SUBMIT_BATCH_JOB
    ],

    lists:foreach(fun(MsgType) ->
        HeaderBin = <<16#2600:16/big, 0:16/big, MsgType:16/big, 0:32/big>>,
        {ok, Header, <<>>} = flurm_protocol_header:parse_header(HeaderBin),
        ?assertEqual(MsgType, Header#slurm_header.msg_type)
    end, MsgTypes).

%%%===================================================================
%%% Test: parse_header/1 - Error cases
%%%===================================================================

parse_header_incomplete_test() ->
    %% Too short - less than 10 bytes
    {error, {incomplete_header, 0, 10}} = flurm_protocol_header:parse_header(<<>>),
    {error, {incomplete_header, 1, 10}} = flurm_protocol_header:parse_header(<<1>>),
    {error, {incomplete_header, 5, 10}} = flurm_protocol_header:parse_header(<<1,2,3,4,5>>),
    {error, {incomplete_header, 9, 10}} = flurm_protocol_header:parse_header(<<1,2,3,4,5,6,7,8,9>>).

parse_header_invalid_data_test() ->
    %% Non-binary input
    {error, invalid_header_data} = flurm_protocol_header:parse_header(not_binary),
    {error, invalid_header_data} = flurm_protocol_header:parse_header(123),
    {error, invalid_header_data} = flurm_protocol_header:parse_header([1,2,3,4,5,6,7,8,9,10]),
    {error, invalid_header_data} = flurm_protocol_header:parse_header({header, data}).

%%%===================================================================
%%% Test: encode_header/1 - Success cases
%%%===================================================================

encode_header_basic_test() ->
    Header = #slurm_header{
        version = 16#2600,
        flags = 16#0001,
        msg_index = 0,  % Not used but must be present
        msg_type = 1008,
        body_length = 100
    },

    {ok, Binary} = flurm_protocol_header:encode_header(Header),

    ?assertEqual(10, byte_size(Binary)),
    ?assertEqual(<<16#2600:16/big, 16#0001:16/big, 1008:16/big, 100:32/big>>, Binary).

encode_header_zero_values_test() ->
    Header = #slurm_header{
        version = 0,
        flags = 0,
        msg_index = 0,
        msg_type = 0,
        body_length = 0
    },

    {ok, Binary} = flurm_protocol_header:encode_header(Header),

    ?assertEqual(10, byte_size(Binary)),
    ?assertEqual(<<0:16/big, 0:16/big, 0:16/big, 0:32/big>>, Binary).

encode_header_max_values_test() ->
    Header = #slurm_header{
        version = 65535,
        flags = 65535,
        msg_index = 0,
        msg_type = 65535,
        body_length = 4294967295
    },

    {ok, Binary} = flurm_protocol_header:encode_header(Header),

    Expected = <<65535:16/big, 65535:16/big, 65535:16/big, 4294967295:32/big>>,
    ?assertEqual(Expected, Binary).

encode_header_various_message_types_test() ->
    %% Test encoding various message types
    MsgTypes = [
        {?REQUEST_PING, <<"ping">>},
        {?REQUEST_JOB_INFO, <<"job_info">>},
        {?REQUEST_SUBMIT_BATCH_JOB, <<"submit">>},
        {?RESPONSE_SLURM_RC, <<"rc">>}
    ],

    lists:foreach(fun({MsgType, _Name}) ->
        Header = #slurm_header{
            version = 16#2600,
            flags = 0,
            msg_index = 0,
            msg_type = MsgType,
            body_length = 0
        },
        {ok, Binary} = flurm_protocol_header:encode_header(Header),
        ?assertEqual(10, byte_size(Binary)),
        %% Verify we can parse it back
        {ok, ParsedHeader, <<>>} = flurm_protocol_header:parse_header(Binary),
        ?assertEqual(MsgType, ParsedHeader#slurm_header.msg_type)
    end, MsgTypes).

%%%===================================================================
%%% Test: encode_header/1 - Error cases
%%%===================================================================

encode_header_invalid_version_test() ->
    %% Version too large (> 65535)
    Header1 = #slurm_header{
        version = 65536,
        flags = 0,
        msg_index = 0,
        msg_type = 1008,
        body_length = 0
    },
    {error, {invalid_header_values, Header1}} = flurm_protocol_header:encode_header(Header1),

    %% Negative version
    Header2 = #slurm_header{
        version = -1,
        flags = 0,
        msg_index = 0,
        msg_type = 1008,
        body_length = 0
    },
    {error, {invalid_header_values, Header2}} = flurm_protocol_header:encode_header(Header2).

encode_header_invalid_flags_test() ->
    %% Flags too large
    Header = #slurm_header{
        version = 16#2600,
        flags = 65536,
        msg_index = 0,
        msg_type = 1008,
        body_length = 0
    },
    {error, {invalid_header_values, Header}} = flurm_protocol_header:encode_header(Header).

encode_header_invalid_msg_type_test() ->
    %% MsgType too large
    Header = #slurm_header{
        version = 16#2600,
        flags = 0,
        msg_index = 0,
        msg_type = 65536,
        body_length = 0
    },
    {error, {invalid_header_values, Header}} = flurm_protocol_header:encode_header(Header).

encode_header_invalid_body_length_test() ->
    %% Body length too large (> 4GB)
    Header = #slurm_header{
        version = 16#2600,
        flags = 0,
        msg_index = 0,
        msg_type = 1008,
        body_length = 4294967296
    },
    {error, {invalid_header_values, Header}} = flurm_protocol_header:encode_header(Header).

encode_header_invalid_record_test() ->
    %% Non-record input
    {error, invalid_header_record} = flurm_protocol_header:encode_header(not_a_record),
    {error, invalid_header_record} = flurm_protocol_header:encode_header(<<1,2,3>>),
    %% Note: {slurm_header, 1, 2, 3, 4, 5} is structurally valid as a record
    %% so we test with a different tuple
    {error, invalid_header_record} = flurm_protocol_header:encode_header({other_record, 1, 2}),
    {error, invalid_header_record} = flurm_protocol_header:encode_header(#{version => 1}).

encode_header_non_integer_values_test() ->
    %% Non-integer values
    Header1 = #slurm_header{
        version = "version",
        flags = 0,
        msg_index = 0,
        msg_type = 1008,
        body_length = 0
    },
    {error, {invalid_header_values, Header1}} = flurm_protocol_header:encode_header(Header1),

    Header2 = #slurm_header{
        version = 16#2600,
        flags = not_integer,
        msg_index = 0,
        msg_type = 1008,
        body_length = 0
    },
    {error, {invalid_header_values, Header2}} = flurm_protocol_header:encode_header(Header2).

%%%===================================================================
%%% Test: Round-trip encode/parse
%%%===================================================================

roundtrip_header_test() ->
    %% Test that encoding then parsing returns the same values
    Headers = [
        #slurm_header{version = 16#2600, flags = 0, msg_index = 0, msg_type = 1008, body_length = 0},
        #slurm_header{version = 16#2500, flags = 1, msg_index = 0, msg_type = 4003, body_length = 1024},
        #slurm_header{version = 9472, flags = 4, msg_index = 0, msg_type = 8001, body_length = 67108864},
        #slurm_header{version = 0, flags = 0, msg_index = 0, msg_type = 0, body_length = 0},
        #slurm_header{version = 65535, flags = 65535, msg_index = 0, msg_type = 65535, body_length = 4294967295}
    ],

    lists:foreach(fun(Header) ->
        {ok, Binary} = flurm_protocol_header:encode_header(Header),
        {ok, ParsedHeader, <<>>} = flurm_protocol_header:parse_header(Binary),

        %% Note: msg_index is set to 0 on parse regardless of input
        ?assertEqual(Header#slurm_header.version, ParsedHeader#slurm_header.version),
        ?assertEqual(Header#slurm_header.flags, ParsedHeader#slurm_header.flags),
        ?assertEqual(Header#slurm_header.msg_type, ParsedHeader#slurm_header.msg_type),
        ?assertEqual(Header#slurm_header.body_length, ParsedHeader#slurm_header.body_length)
    end, Headers).

roundtrip_with_extra_data_test() ->
    %% Roundtrip with extra data preserved
    Header = #slurm_header{
        version = 16#2600,
        flags = 0,
        msg_index = 0,
        msg_type = 2003,
        body_length = 256
    },
    ExtraData = <<"this is extra data that should be preserved">>,

    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    FullBin = <<HeaderBin/binary, ExtraData/binary>>,

    {ok, ParsedHeader, Rest} = flurm_protocol_header:parse_header(FullBin),

    ?assertEqual(Header#slurm_header.version, ParsedHeader#slurm_header.version),
    ?assertEqual(Header#slurm_header.msg_type, ParsedHeader#slurm_header.msg_type),
    ?assertEqual(ExtraData, Rest).

%%%===================================================================
%%% Test: Edge cases
%%%===================================================================

edge_case_boundary_values_test() ->
    %% Test boundary values

    %% Version at boundary
    Header1 = #slurm_header{version = 65535, flags = 0, msg_index = 0, msg_type = 0, body_length = 0},
    {ok, _} = flurm_protocol_header:encode_header(Header1),

    %% Flags at boundary
    Header2 = #slurm_header{version = 0, flags = 65535, msg_index = 0, msg_type = 0, body_length = 0},
    {ok, _} = flurm_protocol_header:encode_header(Header2),

    %% MsgType at boundary
    Header3 = #slurm_header{version = 0, flags = 0, msg_index = 0, msg_type = 65535, body_length = 0},
    {ok, _} = flurm_protocol_header:encode_header(Header3),

    %% Body length at boundary
    Header4 = #slurm_header{version = 0, flags = 0, msg_index = 0, msg_type = 0, body_length = 4294967295},
    {ok, _} = flurm_protocol_header:encode_header(Header4).

edge_case_exact_header_size_test() ->
    %% Exactly 10 bytes with no remainder
    HeaderBin = <<16#2600:16/big, 0:16/big, 1008:16/big, 100:32/big>>,
    ?assertEqual(10, byte_size(HeaderBin)),

    {ok, Header, Rest} = flurm_protocol_header:parse_header(HeaderBin),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(16#2600, Header#slurm_header.version).

%%%===================================================================
%%% Test: Multiple sequential operations
%%%===================================================================

multiple_encode_decode_test() ->
    %% Multiple encodes and decodes in sequence
    lists:foreach(fun(I) ->
        Header = #slurm_header{
            version = 16#2600,
            flags = I rem 65536,
            msg_index = 0,
            msg_type = I rem 65536,
            body_length = I
        },
        {ok, Binary} = flurm_protocol_header:encode_header(Header),
        {ok, ParsedHeader, <<>>} = flurm_protocol_header:parse_header(Binary),
        ?assertEqual(Header#slurm_header.msg_type, ParsedHeader#slurm_header.msg_type),
        ?assertEqual(Header#slurm_header.body_length, ParsedHeader#slurm_header.body_length)
    end, lists:seq(0, 100)).

concatenated_headers_test() ->
    %% Parse multiple concatenated headers
    Header1 = #slurm_header{version = 16#2600, flags = 1, msg_index = 0, msg_type = 1008, body_length = 10},
    Header2 = #slurm_header{version = 16#2600, flags = 2, msg_index = 0, msg_type = 2003, body_length = 20},
    Header3 = #slurm_header{version = 16#2600, flags = 3, msg_index = 0, msg_type = 4003, body_length = 30},

    {ok, Bin1} = flurm_protocol_header:encode_header(Header1),
    {ok, Bin2} = flurm_protocol_header:encode_header(Header2),
    {ok, Bin3} = flurm_protocol_header:encode_header(Header3),

    ConcatBin = <<Bin1/binary, Bin2/binary, Bin3/binary>>,

    {ok, P1, Rest1} = flurm_protocol_header:parse_header(ConcatBin),
    ?assertEqual(1008, P1#slurm_header.msg_type),

    {ok, P2, Rest2} = flurm_protocol_header:parse_header(Rest1),
    ?assertEqual(2003, P2#slurm_header.msg_type),

    {ok, P3, Rest3} = flurm_protocol_header:parse_header(Rest2),
    ?assertEqual(4003, P3#slurm_header.msg_type),

    ?assertEqual(<<>>, Rest3).
