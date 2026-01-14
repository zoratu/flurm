%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_protocol_header module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_header_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_protocol_header_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"protocol_version", fun protocol_version_test/0},
      {"header_size", fun header_size_test/0},
      {"parse_header success", fun parse_header_success_test/0},
      {"parse_header with extra data", fun parse_header_extra_data_test/0},
      {"parse_header errors", fun parse_header_errors_test/0},
      {"encode_header success", fun encode_header_success_test/0},
      {"encode_header errors", fun encode_header_errors_test/0},
      {"encode/parse roundtrip", fun encode_parse_roundtrip_test/0},
      {"boundary values", fun boundary_values_test/0}
     ]}.

%%%===================================================================
%%% Constants Tests
%%%===================================================================

protocol_version_test() ->
    Version = flurm_protocol_header:protocol_version(),
    ?assert(is_integer(Version)),
    ?assertEqual(?SLURM_PROTOCOL_VERSION, Version),
    ok.

header_size_test() ->
    Size = flurm_protocol_header:header_size(),
    ?assert(is_integer(Size)),
    ?assertEqual(?SLURM_HEADER_SIZE, Size),
    ?assertEqual(10, Size),
    ok.

%%%===================================================================
%%% Parse Header Tests
%%%===================================================================

parse_header_success_test() ->
    %% Create a valid header binary
    Version = ?SLURM_PROTOCOL_VERSION,
    Flags = 0,
    MsgType = ?REQUEST_PING,
    BodyLength = 100,

    HeaderBin = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLength:32/big>>,
    Result = flurm_protocol_header:parse_header(HeaderBin),

    ?assertMatch({ok, #slurm_header{}, <<>>}, Result),
    {ok, Header, Rest} = Result,
    ?assertEqual(Version, Header#slurm_header.version),
    ?assertEqual(Flags, Header#slurm_header.flags),
    ?assertEqual(MsgType, Header#slurm_header.msg_type),
    ?assertEqual(BodyLength, Header#slurm_header.body_length),
    ?assertEqual(0, Header#slurm_header.msg_index),
    ?assertEqual(<<>>, Rest),
    ok.

parse_header_extra_data_test() ->
    %% Create header with extra data after it
    Version = ?SLURM_PROTOCOL_VERSION,
    Flags = 16#1234,
    MsgType = ?REQUEST_JOB_INFO,
    BodyLength = 500,

    ExtraData = <<"extra data after header">>,
    HeaderBin = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLength:32/big, ExtraData/binary>>,

    Result = flurm_protocol_header:parse_header(HeaderBin),
    ?assertMatch({ok, #slurm_header{}, _}, Result),
    {ok, Header, Rest} = Result,
    ?assertEqual(Version, Header#slurm_header.version),
    ?assertEqual(Flags, Header#slurm_header.flags),
    ?assertEqual(MsgType, Header#slurm_header.msg_type),
    ?assertEqual(BodyLength, Header#slurm_header.body_length),
    ?assertEqual(ExtraData, Rest),
    ok.

parse_header_errors_test() ->
    %% Test incomplete header (too short)
    ShortBin = <<1:16/big, 2:16/big, 3:16/big>>,  % Only 6 bytes, need 10
    Result1 = flurm_protocol_header:parse_header(ShortBin),
    ?assertMatch({error, {incomplete_header, _, _}}, Result1),

    %% Test very short binary
    VeryShortBin = <<1, 2>>,
    Result2 = flurm_protocol_header:parse_header(VeryShortBin),
    ?assertMatch({error, {incomplete_header, _, _}}, Result2),

    %% Test empty binary
    EmptyBin = <<>>,
    Result3 = flurm_protocol_header:parse_header(EmptyBin),
    ?assertMatch({error, {incomplete_header, _, _}}, Result3),

    %% Test invalid data type
    Result4 = flurm_protocol_header:parse_header(not_binary),
    ?assertMatch({error, invalid_header_data}, Result4),
    ok.

%%%===================================================================
%%% Encode Header Tests
%%%===================================================================

encode_header_success_test() ->
    %% Test encoding a valid header
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = ?REQUEST_PING,
        body_length = 0
    },
    Result = flurm_protocol_header:encode_header(Header),
    ?assertMatch({ok, _}, Result),
    {ok, Bin} = Result,
    ?assert(is_binary(Bin)),
    ?assertEqual(10, byte_size(Bin)),

    %% Test with various flag values
    Header2 = Header#slurm_header{flags = 16#FFFF},
    Result2 = flurm_protocol_header:encode_header(Header2),
    ?assertMatch({ok, _}, Result2),

    %% Test with maximum body length
    Header3 = Header#slurm_header{body_length = 4294967295},
    Result3 = flurm_protocol_header:encode_header(Header3),
    ?assertMatch({ok, _}, Result3),
    ok.

encode_header_errors_test() ->
    %% Test with invalid header record (version out of range)
    InvalidHeader1 = #slurm_header{
        version = 100000,  % Too large for uint16
        flags = 0,
        msg_index = 0,
        msg_type = 0,
        body_length = 0
    },
    Result1 = flurm_protocol_header:encode_header(InvalidHeader1),
    ?assertMatch({error, {invalid_header_values, _}}, Result1),

    %% Test with invalid flags (out of range)
    InvalidHeader2 = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 100000,  % Too large for uint16
        msg_index = 0,
        msg_type = 0,
        body_length = 0
    },
    Result2 = flurm_protocol_header:encode_header(InvalidHeader2),
    ?assertMatch({error, {invalid_header_values, _}}, Result2),

    %% Test with invalid msg_type (out of range)
    InvalidHeader3 = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = 100000,  % Too large for uint16
        body_length = 0
    },
    Result3 = flurm_protocol_header:encode_header(InvalidHeader3),
    ?assertMatch({error, {invalid_header_values, _}}, Result3),

    %% Test with invalid body_length (out of range)
    InvalidHeader4 = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_index = 0,
        msg_type = 0,
        body_length = 5000000000  % Too large for uint32
    },
    Result4 = flurm_protocol_header:encode_header(InvalidHeader4),
    ?assertMatch({error, {invalid_header_values, _}}, Result4),

    %% Test with negative values
    InvalidHeader5 = #slurm_header{
        version = -1,
        flags = 0,
        msg_index = 0,
        msg_type = 0,
        body_length = 0
    },
    Result5 = flurm_protocol_header:encode_header(InvalidHeader5),
    ?assertMatch({error, {invalid_header_values, _}}, Result5),

    %% Test with non-header input
    Result6 = flurm_protocol_header:encode_header({invalid, tuple, header}),
    ?assertMatch({error, invalid_header_record}, Result6),

    Result7 = flurm_protocol_header:encode_header("string"),
    ?assertMatch({error, invalid_header_record}, Result7),
    ok.

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

encode_parse_roundtrip_test() ->
    %% Test multiple headers with various values
    TestCases = [
        #slurm_header{
            version = ?SLURM_PROTOCOL_VERSION,
            flags = 0,
            msg_index = 0,
            msg_type = ?REQUEST_PING,
            body_length = 0
        },
        #slurm_header{
            version = ?SLURM_PROTOCOL_VERSION,
            flags = 16#FFFF,
            msg_index = 0,
            msg_type = ?RESPONSE_SLURM_RC,
            body_length = 1000
        },
        #slurm_header{
            version = ?SLURM_PROTOCOL_VERSION,
            flags = 1,
            msg_index = 0,
            msg_type = ?REQUEST_SUBMIT_BATCH_JOB,
            body_length = 4294967295
        },
        #slurm_header{
            version = 16#FFFF,
            flags = 0,
            msg_index = 0,
            msg_type = 0,
            body_length = 0
        }
    ],
    lists:foreach(fun(OrigHeader) ->
        {ok, Bin} = flurm_protocol_header:encode_header(OrigHeader),
        {ok, ParsedHeader, <<>>} = flurm_protocol_header:parse_header(Bin),
        ?assertEqual(OrigHeader#slurm_header.version, ParsedHeader#slurm_header.version),
        ?assertEqual(OrigHeader#slurm_header.flags, ParsedHeader#slurm_header.flags),
        ?assertEqual(OrigHeader#slurm_header.msg_type, ParsedHeader#slurm_header.msg_type),
        ?assertEqual(OrigHeader#slurm_header.body_length, ParsedHeader#slurm_header.body_length)
    end, TestCases),
    ok.

%%%===================================================================
%%% Boundary Values Tests
%%%===================================================================

boundary_values_test() ->
    %% Test boundary values for all fields

    %% Version: 0 to 65535
    lists:foreach(fun(V) ->
        Header = #slurm_header{version = V, flags = 0, msg_type = 0, body_length = 0},
        ?assertMatch({ok, _}, flurm_protocol_header:encode_header(Header))
    end, [0, 1, 32767, 65534, 65535]),

    %% Flags: 0 to 65535
    lists:foreach(fun(F) ->
        Header = #slurm_header{version = ?SLURM_PROTOCOL_VERSION, flags = F, msg_type = 0, body_length = 0},
        ?assertMatch({ok, _}, flurm_protocol_header:encode_header(Header))
    end, [0, 1, 32767, 65534, 65535]),

    %% MsgType: 0 to 65535
    lists:foreach(fun(M) ->
        Header = #slurm_header{version = ?SLURM_PROTOCOL_VERSION, flags = 0, msg_type = M, body_length = 0},
        ?assertMatch({ok, _}, flurm_protocol_header:encode_header(Header))
    end, [0, 1, 1008, 8001, 65534, 65535]),

    %% BodyLength: 0 to 4294967295
    lists:foreach(fun(B) ->
        Header = #slurm_header{version = ?SLURM_PROTOCOL_VERSION, flags = 0, msg_type = 0, body_length = B},
        ?assertMatch({ok, _}, flurm_protocol_header:encode_header(Header))
    end, [0, 1, 65535, 16777215, 4294967294, 4294967295]),
    ok.
